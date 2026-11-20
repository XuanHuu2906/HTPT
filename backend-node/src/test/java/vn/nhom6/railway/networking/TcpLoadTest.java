package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV3-D20: Comprehensive TCP/Socket Lifecycle and Concurrent Load Test Suite.
 *
 * <p>Validates under concurrent client contention:
 * <ul>
 *   <li>6 concurrent clients firing 120 requests (mix of GET_SEATS, BOOK_TICKET, GET_REQUEST_RESULT) without deadlocks.</li>
 *   <li>Socket connection lifecycle and thread leak detection over 50 rapid sequential/concurrent cycles.</li>
 *   <li>Slow client isolation ensuring fast clients are not starved or blocked.</li>
 *   <li>Abrupt disconnect, zero-byte EOF, TCP RST, and framing bound violation handling.</li>
 *   <li>Queue overload and in-flight backpressure with bounded thread pools.</li>
 * </ul>
 */
@DisplayName("TCP Socket Lifecycle & Concurrent Load Tests (TV3-D20)")
@Timeout(value = 35, unit = TimeUnit.SECONDS)
public class TcpLoadTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        resources.clear();
    }

    @AfterEach
    public void tearDown() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {
            }
        }
        resources.clear();
    }

    private void awaitConnectionCountZero(TcpServer server, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (server.getActiveConnectionCount() == 0) {
                return;
            }
            Thread.sleep(25);
        }
        assertEquals(0, server.getActiveConnectionCount(), "Active connection count did not drop to 0 within timeout");
    }

    @Test
    @DisplayName("Kịch bản 1: 6 clients đồng thời bắn 120 requests hỗn hợp - 100% correlation match, zero deadlock")
    public void testSixClientsConcurrentContentionAndThroughput() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "cluster-load", "127.0.0.1", 0, "127.0.0.1", 0, null, 16, 15_000);
        FakeBookingService fakeService = new FakeBookingService("node-1");
        fakeService.setLeader(true, null);

        NodeMain node = new NodeMain(config, fakeService);
        resources.add(node);
        node.start();

        int clientCount = 6;
        int requestsPerClient = 20;
        int totalRequests = clientCount * requestsPerClient; // 120 requests >= 100 requests requirement

        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        resources.add(executor::shutdownNow);

        CountDownLatch readyLatch = new CountDownLatch(clientCount);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<List<MessageEnvelope<JsonNode>>>> futures = new ArrayList<>();
        List<TcpClient> clients = new ArrayList<>();

        for (int c = 1; c <= clientCount; c++) {
            final int clientIndex = c;
            TcpClient client = new TcpClient("127.0.0.1", node.getClientPort(), 5000, 5000);
            resources.add(client);
            clients.add(client);
            client.connect();
            assertTrue(client.isConnected(), "Client " + clientIndex + " must be connected");

            futures.add(executor.submit(() -> {
                readyLatch.countDown();
                assertTrue(startLatch.await(5, TimeUnit.SECONDS), "Start latch timed out");

                List<MessageEnvelope<JsonNode>> responses = new ArrayList<>();
                String lastRequestId = UUID.randomUUID().toString();

                for (int i = 0; i < requestsPerClient; i++) {
                    MessageEnvelope<JsonNode> response;
                    if (i % 3 == 0) {
                        // 1. GET_SEATS
                        GetSeatsRequestDto getSeatsDto = new GetSeatsRequestDto("SE-001");
                        MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                                MessageType.GET_SEATS, "client-" + clientIndex, getSeatsDto);
                        response = client.sendAndReceive(req);
                        assertNotNull(response, "GET_SEATS response must not be null");
                        assertEquals(req.getMessageId(), response.getCorrelationId(), "Correlation must match request messageId");
                        assertEquals(MessageType.GET_SEATS_RESPONSE, response.getType());
                    } else if (i % 3 == 1) {
                        // 2. BOOK_TICKET
                        lastRequestId = UUID.randomUUID().toString();
                        String seatId = String.format("G%02d", (i % 10) + 1);
                        BookTicketRequestDto bookDto = new BookTicketRequestDto(
                                lastRequestId, "SE-001", seatId, "Passenger-" + clientIndex + "-" + i);
                        MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                                MessageType.BOOK_TICKET, "client-" + clientIndex, bookDto);
                        response = client.sendAndReceive(req);
                        assertNotNull(response, "BOOK_TICKET response must not be null");
                        assertEquals(req.getMessageId(), response.getCorrelationId(), "Correlation must match request messageId");
                        assertEquals(MessageType.BOOK_TICKET_RESPONSE, response.getType());
                    } else {
                        // 3. GET_REQUEST_RESULT
                        GetRequestResultRequestDto lookupDto = new GetRequestResultRequestDto(lastRequestId);
                        MessageEnvelope<GetRequestResultRequestDto> req = MessageEnvelope.createRequest(
                                MessageType.GET_REQUEST_RESULT, "client-" + clientIndex, lookupDto);
                        response = client.sendAndReceive(req);
                        assertNotNull(response, "GET_REQUEST_RESULT response must not be null");
                        assertEquals(req.getMessageId(), response.getCorrelationId(), "Correlation must match request messageId");
                        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, response.getType());
                    }
                    responses.add(response);
                }
                return responses;
            }));
        }

        assertTrue(readyLatch.await(5, TimeUnit.SECONDS), "All 6 clients must reach ready state");
        long startNanos = System.nanoTime();
        startLatch.countDown();

        int completedRequests = 0;
        int confirmedBookings = 0;
        int rejectedBookings = 0;

        for (Future<List<MessageEnvelope<JsonNode>>> future : futures) {
            List<MessageEnvelope<JsonNode>> resList = future.get(10, TimeUnit.SECONDS);
            assertEquals(requestsPerClient, resList.size(), "Each client must receive all expected responses");
            completedRequests += resList.size();

            for (MessageEnvelope<JsonNode> env : resList) {
                if (env.getType() == MessageType.BOOK_TICKET_RESPONSE) {
                    BookTicketResponseDto bookRes = MessageCodec.decodePayload(env, BookTicketResponseDto.class);
                    if ("CONFIRMED".equals(bookRes.getOutcome())) {
                        confirmedBookings++;
                    } else if ("REJECTED".equals(bookRes.getOutcome())) {
                        rejectedBookings++;
                    }
                }
            }
        }

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        assertEquals(totalRequests, completedRequests, "All 120 requests must complete successfully");
        assertTrue(confirmedBookings <= 10, "Cannot confirm more than 10 seats for SE-001");
        assertTrue(confirmedBookings > 0, "At least some bookings must be confirmed");
        System.out.printf("TV3-D20 LOAD_RESULT: clients=6 totalRequests=%d completed=%d confirmed=%d rejected=%d elapsedMs=%d%n",
                totalRequests, completedRequests, confirmedBookings, rejectedBookings, elapsedMs);

        // Disconnect clients and verify connection cleanup
        for (TcpClient c : clients) {
            c.close();
        }
        awaitConnectionCountZero(node.getClientServer(), 2000);
        assertEquals(0, node.getClientServer().getActiveConnectionCount(), "Active connections must drop to 0");
    }

    @Test
    @DisplayName("Kịch bản 2: Vòng đời kết nối và phòng chống rò rỉ Socket/Thread qua 50 chu kỳ kết nối")
    public void testSocketConnectionLifecycleAndLeakDetection() throws Exception {
        int maxThreads = 8;
        TcpServer server = new TcpServer("127.0.0.1", 0, maxThreads, 5000);
        resources.add(server::stop);
        server.setFrameHandler(payload -> payload); // Echo frame handler
        server.start();

        int port = server.getPort();

        // 1. Sequential connections (25 cycles)
        int sequentialCount = 25;
        for (int i = 0; i < sequentialCount; i++) {
            try (TcpClient client = new TcpClient("127.0.0.1", port, 2000, 2000)) {
                client.connect();
                byte[] echoData = ("test-data-" + i).getBytes(StandardCharsets.UTF_8);
                byte[] reply = client.sendAndReceive(echoData);
                assertArrayEquals(echoData, reply);
            }
        }

        // 2. Concurrent connections (25 cycles from 10 client threads)
        int concurrentCount = 25;
        ExecutorService pool = Executors.newFixedThreadPool(10);
        resources.add(pool::shutdownNow);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < concurrentCount; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                try (TcpClient client = new TcpClient("127.0.0.1", port, 3000, 3000)) {
                    client.connect();
                    byte[] echoData = ("concurrent-data-" + idx).getBytes(StandardCharsets.UTF_8);
                    byte[] reply = client.sendAndReceive(echoData);
                    assertArrayEquals(echoData, reply);
                }
                return null;
            }));
        }

        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS), "Client test pool must terminate");

        awaitConnectionCountZero(server, 2000);
        assertEquals(0, server.getActiveConnectionCount(), "All active sockets must be closed");

        // Verify active threads specifically belonging to this TcpServer instance
        long serverWorkerThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().startsWith("tcp-worker-" + port))
                .count();
        long serverAcceptorThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().startsWith("tcp-acceptor-" + port))
                .count();

        assertTrue(serverWorkerThreads <= maxThreads,
                "Worker threads must not exceed maxThreads (" + maxThreads + "), observed: " + serverWorkerThreads);
        assertTrue(serverAcceptorThreads <= 1,
                "Acceptor thread must be at most 1, observed: " + serverAcceptorThreads);

        System.out.printf("TV3-D20 LEAK_CHECK: cycles=50 serverWorkerThreads=%d (max=%d) serverAcceptorThreads=%d activeSockets=%d%n",
                serverWorkerThreads, maxThreads, serverAcceptorThreads, server.getActiveConnectionCount());

        // Stop server and verify its worker and acceptor threads are cleanly released
        server.stop();
        Thread.sleep(100);
        long remainingServerThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && (t.getName().startsWith("tcp-worker-" + port) || t.getName().startsWith("tcp-acceptor-" + port)))
                .count();
        assertEquals(0, remainingServerThreads, "All server threads must terminate after stop()");
    }

    @Test
    @DisplayName("Kịch bản 3: Slow Client không làm nghẽn hoặc cạn kiệt tài nguyên của Fast Clients")
    public void testSlowClientDoesNotStarveFastClients() throws Exception {
        TcpServer server = new TcpServer("127.0.0.1", 0, 4, 5000);
        resources.add(server::stop);
        server.setFrameHandler(payload -> payload); // Echo frame handler
        server.start();

        CountDownLatch slowClientStarted = new CountDownLatch(1);
        CountDownLatch slowClientDone = new CountDownLatch(1);

        // Slow client thread: writes 1 byte every 15ms
        Thread slowThread = new Thread(() -> {
            try (Socket slowSocket = new Socket("127.0.0.1", server.getPort())) {
                slowSocket.setSoTimeout(5000);
                slowSocket.setTcpNoDelay(true);

                byte[] rawFrame = FrameCodec.frame("slow-client-message".getBytes(StandardCharsets.UTF_8));
                OutputStream out = slowSocket.getOutputStream();

                slowClientStarted.countDown();
                // Send byte by byte with sleep
                for (byte b : rawFrame) {
                    out.write(b);
                    out.flush();
                    Thread.sleep(15);
                }

                // Wait before reading response
                Thread.sleep(200);
                byte[] response = FrameCodec.readFrame(slowSocket.getInputStream());
                assertNotNull(response);
            } catch (Exception ignored) {
            } finally {
                slowClientDone.countDown();
            }
        }, "slow-client-thread");
        slowThread.setDaemon(true);
        slowThread.start();

        assertTrue(slowClientStarted.await(2, TimeUnit.SECONDS), "Slow client must start");

        // Concurrently run 5 fast clients, each sending 10 requests (50 requests total)
        int fastClientCount = 5;
        int requestsPerFastClient = 10;
        ExecutorService fastExecutor = Executors.newFixedThreadPool(fastClientCount);
        resources.add(fastExecutor::shutdownNow);

        long fastStart = System.nanoTime();
        List<Future<Integer>> fastFutures = new ArrayList<>();

        for (int c = 1; c <= fastClientCount; c++) {
            final int clientIdx = c;
            fastFutures.add(fastExecutor.submit(() -> {
                int success = 0;
                try (TcpClient fastClient = new TcpClient("127.0.0.1", server.getPort(), 3000, 3000)) {
                    fastClient.connect();
                    for (int r = 0; r < requestsPerFastClient; r++) {
                        byte[] data = ("fast-" + clientIdx + "-" + r).getBytes(StandardCharsets.UTF_8);
                        byte[] res = fastClient.sendAndReceive(data);
                        assertArrayEquals(data, res);
                        success++;
                    }
                }
                return success;
            }));
        }

        int totalFastSuccess = 0;
        for (Future<Integer> f : fastFutures) {
            totalFastSuccess += f.get(3, TimeUnit.SECONDS);
        }

        long fastElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fastStart);
        assertEquals(fastClientCount * requestsPerFastClient, totalFastSuccess);
        assertTrue(fastElapsedMs < 2500, "Fast clients must complete promptly without starvation, elapsed: " + fastElapsedMs + "ms");

        assertTrue(slowClientDone.await(3, TimeUnit.SECONDS), "Slow client must complete");
        awaitConnectionCountZero(server, 2000);
        assertEquals(0, server.getActiveConnectionCount());
        System.out.printf("TV3-D20 SLOW_CLIENT_TEST: fastRequests=%d fastElapsedMs=%d%n", totalFastSuccess, fastElapsedMs);
    }

    @Test
    @DisplayName("Kịch bản 4: Xử lý an toàn khi Client ngắt kết nối đột ngột, RST, hoặc frame dị dạng")
    public void testAbruptClientDisconnectionAndEofHandling() throws Exception {
        TcpServer server = new TcpServer("127.0.0.1", 0, 8, 3000);
        resources.add(server::stop);
        server.setFrameHandler(payload -> payload);
        server.start();

        // 1. Zero-byte EOF: connect and immediate close
        try (Socket s = new Socket("127.0.0.1", server.getPort())) {
            s.setTcpNoDelay(true);
            // close immediately
        }

        // 2. Truncated frame header: write 2 bytes of 4-byte length prefix then close
        try (Socket s = new Socket("127.0.0.1", server.getPort())) {
            s.setTcpNoDelay(true);
            s.getOutputStream().write(new byte[]{0x00, 0x00});
            s.getOutputStream().flush();
        }

        // 3. Truncated frame body: length header declares 100 bytes, but send only 5 bytes then close
        try (Socket s = new Socket("127.0.0.1", server.getPort())) {
            s.setTcpNoDelay(true);
            OutputStream out = s.getOutputStream();
            out.write(ByteBuffer.allocate(4).putInt(100).array());
            out.write(new byte[]{1, 2, 3, 4, 5});
            out.flush();
        }

        // 4. TCP RST: setSoLinger(true, 0) and close immediately (abrupt hard abort)
        try (Socket s = new Socket("127.0.0.1", server.getPort())) {
            s.setTcpNoDelay(true);
            s.setSoLinger(true, 0); // causes RST upon close
            OutputStream out = s.getOutputStream();
            out.write(ByteBuffer.allocate(4).putInt(20).array());
            out.write(new byte[]{1, 2, 3});
            out.flush();
        }

        // 5. Malformed oversized frame: length header > 1 MiB framing bound per SPEC-03 Section 2
        try (Socket s = new Socket("127.0.0.1", server.getPort())) {
            s.setTcpNoDelay(true);
            OutputStream out = s.getOutputStream();
            out.write(ByteBuffer.allocate(4).putInt(2 * 1024 * 1024).array()); // 2 MiB > 1 MiB
            out.flush();
        }

        // Verify active socket count returns to 0
        awaitConnectionCountZero(server, 2000);
        assertEquals(0, server.getActiveConnectionCount(), "All broken sockets must be cleanly closed and dereferenced");

        // Verify server is still completely functional and healthy
        try (TcpClient healthyClient = new TcpClient("127.0.0.1", server.getPort(), 2000, 2000)) {
            healthyClient.connect();
            byte[] msg = "healthy-check".getBytes(StandardCharsets.UTF_8);
            byte[] reply = healthyClient.sendAndReceive(msg);
            assertArrayEquals(msg, reply);
        }
        awaitConnectionCountZero(server, 1000);
        assertEquals(0, server.getActiveConnectionCount());
    }

    @Test
    @DisplayName("Kịch bản 5: Quá tải hàng đợi và giới hạn in-flight với thread pool nhỏ - phản hồi kiểm soát, không deadlock")
    public void testQueueOverloadAndInFlightBackpressure() throws Exception {
        // Small thread pool (2 threads) and low in-flight limit (5 requests)
        HandlerRegistry registry = new HandlerRegistry();
        MessageDispatcher dispatcher = new MessageDispatcher(
                registry, MessageDispatcher.ChannelType.CLIENT, "node-1", null, 5);

        // Register delay handler
        registry.register(MessageType.GET_SEATS, env -> {
            try {
                Thread.sleep(40); // Small processing delay to induce in-flight queuing
            } catch (InterruptedException ignored) {
            }
            return MessageEnvelope.createResponse(
                    env.getMessageId(),
                    MessageType.GET_SEATS_RESPONSE,
                    "node-1",
                    new GetSeatsResponseDto("SE-001", List.of(), 1L, "node-1")
            );
        });

        TcpServer server = new TcpServer("127.0.0.1", 0, 2, 5000);
        resources.add(server::stop);
        server.setMessageHandler(dispatcher::dispatch);
        server.start();

        int clientCount = 10;
        int requestsPerClient = 3;
        int totalRequests = clientCount * requestsPerClient; // 30 requests

        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        resources.add(executor::shutdownNow);

        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<List<MessageEnvelope<JsonNode>>>> futures = new ArrayList<>();

        for (int c = 1; c <= clientCount; c++) {
            final int cIdx = c;
            futures.add(executor.submit(() -> {
                startLatch.await();
                List<MessageEnvelope<JsonNode>> results = new ArrayList<>();
                try (TcpClient client = new TcpClient("127.0.0.1", server.getPort(), 3000, 3000)) {
                    client.connect();
                    for (int r = 0; r < requestsPerClient; r++) {
                        GetSeatsRequestDto dto = new GetSeatsRequestDto("SE-001");
                        MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                                MessageType.GET_SEATS, "client-" + cIdx, dto);
                        try {
                            MessageEnvelope<JsonNode> res = client.sendAndReceive(req);
                            if (res != null) {
                                results.add(res);
                            }
                        } catch (Exception ignored) {
                            // Client read timeout or connection reset under extreme load is acceptable
                        }
                    }
                }
                return results;
            }));
        }

        // Release all 10 client threads simultaneously
        startLatch.countDown();

        int validResponses = 0;
        int inFlightRejections = 0;

        for (Future<List<MessageEnvelope<JsonNode>>> f : futures) {
            List<MessageEnvelope<JsonNode>> resList = f.get(6, TimeUnit.SECONDS); // Deadlock assertion
            for (MessageEnvelope<JsonNode> res : resList) {
                if (res.getType() == MessageType.GET_SEATS_RESPONSE) {
                    validResponses++;
                } else if (res.getType() == MessageType.ERROR_RESPONSE) {
                    inFlightRejections++;
                }
            }
        }

        assertTrue(validResponses > 0, "Some requests must be processed successfully");
        System.out.printf("TV3-D20 BACKPRESSURE: totalRequests=%d validResponses=%d inFlightRejections=%d%n",
                totalRequests, validResponses, inFlightRejections);

        // Await connection cleanup
        awaitConnectionCountZero(server, 2000);
        assertEquals(0, server.getActiveConnectionCount());
    }
}
