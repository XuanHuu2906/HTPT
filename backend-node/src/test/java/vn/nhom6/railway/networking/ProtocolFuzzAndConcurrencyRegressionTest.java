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
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV3-D27: Protocol framing fuzz, multi-connection concurrency, timeout/retry,
 * requestId correlation, and per-node exception/log inspection regression suite.
 *
 * <p>Runs on isolated ephemeral loopback ports without depending on TV6-D27.
 */
@DisplayName("Protocol Framing Fuzz, Concurrency & Per-Node Log/Exception Regression (TV3-D27)")
@Timeout(value = 40, unit = TimeUnit.SECONDS)
public class ProtocolFuzzAndConcurrencyRegressionTest {

    private static final List<String> NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Set<String> VOTER_SET = Set.copyOf(NODE_IDS);

    private final List<AutoCloseable> resources = new ArrayList<>();
    private final CapturingLogHandler logCapture = new CapturingLogHandler();

    @BeforeEach
    void attachLoggers() {
        Logger.getLogger(MessageDispatcher.class.getName()).addHandler(logCapture);
        Logger.getLogger(TcpServer.class.getName()).addHandler(logCapture);
    }

    @AfterEach
    void tearDown() {
        Logger.getLogger(MessageDispatcher.class.getName()).removeHandler(logCapture);
        Logger.getLogger(TcpServer.class.getName()).removeHandler(logCapture);
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ignored) {}
        }
        resources.clear();
        logCapture.clear();
    }

    @Test
    @DisplayName("TC-D27-01: Framing fuzz (negative/zero/oversize/truncated/corrupt JSON) never crashes any of 5 nodes and logs cleanly")
    void testFramingFuzzAcrossFiveNodesNeverCrashesAndLogsCleanly() throws Exception {
        Map<String, NodeMain> cluster = startCluster(true);

        int[] invalidLengths = {
                -1,
                -1024,
                Integer.MIN_VALUE,
                0,
                FrameCodec.MAX_FRAME_LENGTH + 1,
                10_000_000,
                Integer.MAX_VALUE
        };

        for (String nodeId : NODE_IDS) {
            NodeMain node = cluster.get(nodeId);
            int clientPort = node.getClientPort();
            int raftPort = node.getRaftPort();

            // 1. Negative, zero, and oversize frame headers on BOTH Client and Peer ports
            for (int badLen : invalidLengths) {
                sendRawBytesAndExpectClose("127.0.0.1", clientPort, intToBigEndian(badLen));
                sendRawBytesAndExpectClose("127.0.0.1", raftPort, intToBigEndian(badLen));
            }

            // 2. Truncated 2-byte header and truncated payload (claims 128 bytes, sends 8)
            sendRawBytesAndExpectClose("127.0.0.1", clientPort, new byte[]{0x00, 0x01});
            byte[] truncatedPayload = new byte[12];
            ByteBuffer.wrap(truncatedPayload).putInt(128).putLong(0x1122334455667788L);
            sendRawBytesAndExpectClose("127.0.0.1", clientPort, truncatedPayload);

            // 3. Valid framing bounds carrying garbage bytes, invalid JSON, duplicate keys, unsupported protocolVersion
            sendRawBytesAndExpectClose("127.0.0.1", clientPort,
                    FrameCodec.frame(new byte[]{(byte) 0xFF, (byte) 0xFE, 0x00, 0x01}));
            sendRawBytesAndExpectClose("127.0.0.1", clientPort,
                    FrameCodec.frame("{corrupt-json".getBytes(StandardCharsets.UTF_8)));

            String dupKeyJson = "{\"protocolVersion\":1,\"messageId\":\"" + UUID.randomUUID() + "\","
                    + "\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"GET_SEATS\",\"senderId\":\"fuzz\",\"sentAt\":\"2026-11-27T08:00:00Z\",\"payload\":{\"tripId\":\"SE-001\"}}";
            sendRawBytesAndExpectClose("127.0.0.1", clientPort,
                    FrameCodec.frame(dupKeyJson.getBytes(StandardCharsets.UTF_8)));

            String badVersionJson = "{\"protocolVersion\":99,\"messageId\":\"" + UUID.randomUUID() + "\","
                    + "\"correlationId\":null,\"type\":\"GET_SEATS\",\"senderId\":\"fuzz\",\"sentAt\":\"2026-11-27T08:00:00Z\",\"payload\":{\"tripId\":\"SE-001\"}}";
            sendRawBytesAndExpectClose("127.0.0.1", clientPort,
                    FrameCodec.frame(badVersionJson.getBytes(StandardCharsets.UTF_8)));

            // Valid v1 JSON carrying unexpected response type onto request dispatcher -> returns correlated ERROR_RESPONSE
            String unexpectedRespJson = "{\"protocolVersion\":1,\"messageId\":\"" + UUID.randomUUID() + "\","
                    + "\"correlationId\":\"" + UUID.randomUUID() + "\",\"type\":\"BOOK_TICKET_RESPONSE\",\"senderId\":\"fuzz\",\"sentAt\":\"2026-11-27T08:00:00Z\",\"payload\":{}}";
            try (TcpClient client = new TcpClient("127.0.0.1", clientPort, 1000, 2000)) {
                client.connect();
                byte[] resp = client.sendAndReceive(unexpectedRespJson.getBytes(StandardCharsets.UTF_8));
                MessageEnvelope<ErrorResponseDto> err = MessageCodec.decode(resp, ErrorResponseDto.class);
                assertEquals(MessageType.ERROR_RESPONSE, err.getType());
                assertEquals("INVALID_REQUEST", err.getPayload().getErrorCode());
            }

            // Verify per-node server remained healthy, closed sockets, logged warnings, and still handles PING + BOOK_TICKET
            awaitZeroConnections(node.getClientServer());
            awaitZeroConnections(node.getRaftServer());
            assertTrue(node.isRunning(), nodeId + " must stay alive after fuzz storm");
            assertTrue(node.getClientServer().getMalformedFrameCount() >= invalidLengths.length + 6,
                    nodeId + " clientServer must record all malformed frames");
            assertTrue(node.getRaftServer().getMalformedFrameCount() >= invalidLengths.length,
                    nodeId + " raftServer must record malformed peer frames");
            assertTrue(logCapture.containsWarning("[" + nodeId + "]")
                    || logCapture.containsWarning("Malformed frame received"),
                    "Per-node logs must record WARNING for malformed frames");

            try (TcpClient verify = new TcpClient("127.0.0.1", clientPort, 1000, 2000)) {
                verify.connect();
                assertEquals("PONG", new String(verify.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @DisplayName("TC-D27-02: 12 concurrent connections mixed with malformed frames preserve 100% correlation & requestId")
    void testMultiNodeConcurrencyWithFuzzPreservesCorrelationAndRequestId() throws Exception {
        Map<String, NodeMain> cluster = startCluster(true);
        NodeMain leader = cluster.get("node-1");

        int totalClients = 12;
        int requestsPerClient = 15;
        ExecutorService pool = Executors.newFixedThreadPool(totalClients);
        resources.add(pool::shutdownNow);

        CountDownLatch ready = new CountDownLatch(totalClients);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int c = 0; c < totalClients; c++) {
            final int clientIdx = c;
            futures.add(pool.submit(() -> {
                ready.countDown();
                assertTrue(go.await(5, TimeUnit.SECONDS));

                if (clientIdx % 4 == 3) {
                    // Fuzz attacker clients firing bad frame lengths concurrently alongside valid bookings
                    for (int k = 0; k < 6; k++) {
                        sendRawBytesAndExpectClose("127.0.0.1", leader.getClientPort(), intToBigEndian(-50 - k));
                        sendRawBytesAndExpectClose("127.0.0.1", leader.getClientPort(),
                                intToBigEndian(FrameCodec.MAX_FRAME_LENGTH + 4096));
                    }
                    return null;
                }

                try (TcpClient client = new TcpClient("127.0.0.1", leader.getClientPort(), 2000, 5000)) {
                    client.connect();
                    for (int i = 0; i < requestsPerClient; i++) {
                        if (i % 2 == 0) {
                            GetSeatsRequestDto dto = new GetSeatsRequestDto("SE-001");
                            MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                                    MessageType.GET_SEATS, "conc-client-" + clientIdx, dto);
                            MessageEnvelope<GetSeatsResponseDto> resp =
                                    MessageCodec.decode(client.sendAndReceive(MessageCodec.encode(req)), GetSeatsResponseDto.class);
                            assertEquals(req.getMessageId(), resp.getCorrelationId(),
                                    "GET_SEATS correlationId mismatch under concurrency");
                            assertEquals("SE-001", resp.getPayload().getTripId());
                        } else {
                            String reqId = UUID.randomUUID().toString();
                            String seatId = String.format("G%02d", ((clientIdx + i) % 10) + 1);
                            BookTicketRequestDto book = new BookTicketRequestDto(reqId, "SE-001", seatId, "Conc-" + clientIdx);
                            MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                                    MessageType.BOOK_TICKET, "conc-client-" + clientIdx, book);
                            MessageEnvelope<BookTicketResponseDto> resp =
                                    MessageCodec.decode(client.sendAndReceive(MessageCodec.encode(req)), BookTicketResponseDto.class);
                            assertEquals(req.getMessageId(), resp.getCorrelationId(),
                                    "BOOK_TICKET correlationId mismatch under concurrency");
                            assertEquals(reqId, resp.getPayload().getRequestId(),
                                    "requestId must stay invariant per client");
                        }
                    }
                }
                return null;
            }));
        }

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        for (Future<?> f : futures) {
            f.get(25, TimeUnit.SECONDS);
        }

        awaitZeroConnections(leader.getClientServer());
        MessageDispatcher.DispatcherMetrics metrics = leader.getClientDispatcher().getMetrics();
        assertTrue(metrics.completedCount() >= 9 * requestsPerClient);
        assertEquals(0, metrics.currentInFlightCount(), "All in-flight entries must be released");
        assertTrue(metrics.peakInFlightCount() >= 1);
        assertTrue(leader.getClientServer().getMalformedFrameCount() >= 3 * 12);
    }

    @Test
    @DisplayName("TC-D27-03: Concurrent timeout, response-lost retry & GET_REQUEST_RESULT keep requestId intact")
    void testConcurrentTimeoutAndRetryPreservesRequestIdCorrelation() throws Exception {
        NodeConfig cfg = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 16, 5000);
        FakeBookingService bookingService = new FakeBookingService("node-1");
        bookingService.setLeader(true, "node-1");

        NodeMain node = new NodeMain(cfg, bookingService);
        resources.add(node);

        TestNetworkFilter filter = new TestNetworkFilter();
        // Drop first response for each of seats G01..G05 on BOOK_TICKET_RESPONSE, then allow subsequent queries/retries
        Map<String, Boolean> droppedFirstByReqId = new java.util.concurrent.ConcurrentHashMap<>();
        filter.addResponseDropPredicate((link, resp) -> {
            if (resp != null && resp.getType() == MessageType.BOOK_TICKET_RESPONSE
                    && resp.getPayload() instanceof BookTicketResponseDto dto) {
                return droppedFirstByReqId.putIfAbsent(dto.getRequestId(), Boolean.TRUE) == null;
            }
            return false;
        });
        filter.attachTo(node);
        node.start();

        int workerCount = 5;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        resources.add(pool::shutdownNow);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < workerCount; i++) {
            final int seatNum = i + 1;
            futures.add(pool.submit(() -> {
                RetryPolicy policy = new RetryPolicy(2, 10, 30, 1.5, 1000, 150);
                try (ConnectionManager cm = new ConnectionManager("127.0.0.1", node.getClientPort(), policy)) {
                    String reqId = UUID.randomUUID().toString();
                    String seatId = String.format("G%02d", seatNum);
                    MessageEnvelope<BookTicketRequestDto> bookReq = MessageEnvelope.createRequest(
                            MessageType.BOOK_TICKET,
                            "retry-client-" + seatNum,
                            new BookTicketRequestDto(reqId, "SE-001", seatId, "RetryUser-" + seatNum)
                    );

                    // 1. First attempt: response lost -> UNKNOWN (TIMEOUT_UNKNOWN) preserving reqId
                    MessageEnvelope<BookTicketResponseDto> first = cm.sendAndReceive(bookReq, BookTicketResponseDto.class);
                    assertEquals("UNKNOWN", first.getPayload().getOutcome());
                    assertEquals("TIMEOUT_UNKNOWN", first.getPayload().getReasonCode());
                    assertEquals(reqId, first.getPayload().getRequestId());

                    // 2. Lookup via GET_REQUEST_RESULT with SAME requestId recovers APPLIED_SUCCESS
                    MessageEnvelope<GetRequestResultRequestDto> lookupReq = MessageEnvelope.createRequest(
                            MessageType.GET_REQUEST_RESULT,
                            "retry-client-" + seatNum,
                            new GetRequestResultRequestDto(reqId)
                    );
                    MessageEnvelope<GetRequestResultResponseDto> lookupResp =
                            cm.sendAndReceive(lookupReq, GetRequestResultResponseDto.class);
                    assertEquals(lookupReq.getMessageId(), lookupResp.getCorrelationId());
                    assertEquals(reqId, lookupResp.getPayload().getRequestId());
                    assertEquals("APPLIED_SUCCESS", lookupResp.getPayload().getState());
                    assertNotNull(lookupResp.getPayload().getTicketId());
                }
                return null;
            }));
        }

        for (Future<?> f : futures) {
            f.get(15, TimeUnit.SECONDS);
        }

        assertEquals(5, bookingService.getStateMachine().lastApplied(), "Exactly 5 seats applied without duplicates");
        assertTrue(node.getClientDispatcher().getMetrics().droppedCount() >= 5);
        assertEquals(0, node.getClientDispatcher().getActiveInFlightCount());
    }

    @Test
    @DisplayName("TC-D27-04: Per-node exception & log audit captures exact nodeId and metrics without crashing peer nodes")
    void testPerNodeExceptionAndLogAudit() throws Exception {
        Map<String, NodeMain> cluster = startCluster(false);

        // Configure node-2 handler to throw RuntimeException on GET_SEATS to audit node-specific exception logging
        NodeMain node2 = cluster.get("node-2");
        node2.getClientRegistry().register(MessageType.GET_SEATS, req -> {
            throw new IllegalStateException("Injected fault in node-2 handler");
        });
        NodeMain node4 = cluster.get("node-4");

        // 1. Trigger handler exception specifically on node-2
        try (TcpClient c2 = new TcpClient("127.0.0.1", node2.getClientPort(), 1000, 2000)) {
            c2.connect();
            MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                    MessageType.GET_SEATS, "audit-client", new GetSeatsRequestDto("SE-001"));
            MessageEnvelope<ErrorResponseDto> err = MessageCodec.decode(
                    c2.sendAndReceive(MessageCodec.encode(req)), ErrorResponseDto.class);
            assertEquals(req.getMessageId(), err.getCorrelationId());
            assertEquals("INTERNAL_ERROR", err.getPayload().getErrorCode());
        }

        // 2. Trigger channel violation (TC-TCP-04) & unauthorized peer rejection specifically on node-4
        try (TcpClient c4 = new TcpClient("127.0.0.1", node4.getClientPort(), 1000, 2000);
             TcpClient p4 = new TcpClient("127.0.0.1", node4.getRaftPort(), 1000, 2000)) {
            c4.connect();
            p4.connect();

            MessageEnvelope<?> badClientRpc = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE, "node-1", new RequestVoteRequestDto(1L, "node-1", 0L, 0L));
            assertEquals("INVALID_REQUEST", MessageCodec.decode(
                    c4.sendAndReceive(MessageCodec.encode(badClientRpc)), ErrorResponseDto.class).getPayload().getErrorCode());

            MessageEnvelope<?> badPeerSender = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE, "rogue-peer", new RequestVoteRequestDto(1L, "rogue-peer", 0L, 0L));
            assertEquals("INVALID_REQUEST", MessageCodec.decode(
                    p4.sendAndReceive(MessageCodec.encode(badPeerSender)), ErrorResponseDto.class).getPayload().getErrorCode());
        }

        // Audit per-node metrics and logs cleanly isolated by nodeId
        MessageDispatcher.DispatcherMetrics m2 = node2.getClientDispatcher().getMetrics();
        MessageDispatcher.DispatcherMetrics m4Client = node4.getClientDispatcher().getMetrics();
        MessageDispatcher.DispatcherMetrics m4Peer = node4.getRaftDispatcher().getMetrics();
        MessageDispatcher.DispatcherMetrics m1 = cluster.get("node-1").getClientDispatcher().getMetrics();

        assertEquals(1, m2.exceptionCount(), "node-2 must record exactly 1 exception");
        assertEquals(0, m1.exceptionCount(), "node-1 must remain unaffected");
        assertEquals(1, m4Client.rejectedCount(), "node-4 client dispatcher must record 1 channel violation");
        assertEquals(1, m4Peer.rejectedCount(), "node-4 peer dispatcher must record 1 allowlist rejection");

        assertTrue(logCapture.containsWarning("[node-2] Exception while handling GET_SEATS"));
        assertTrue(logCapture.containsWarning("[node-4] Rejected request (CLIENT"));
        assertTrue(logCapture.containsWarning("[node-4] Rejected request (PEER"));
    }

    private Map<String, NodeMain> startCluster(boolean leaderForAll) throws IOException {
        Map<String, NodeMain> cluster = new LinkedHashMap<>();
        for (String nodeId : NODE_IDS) {
            NodeConfig cfg = new NodeConfig(nodeId, "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 16, 4000);
            FakeBookingService booking = new FakeBookingService(nodeId);
            booking.setLeader(leaderForAll || "node-1".equals(nodeId), "node-1");

            NodeMain node = new NodeMain(cfg, booking);
            FakeRaftRequestHandler raft = new FakeRaftRequestHandler(1L);
            node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(raft, nodeId));
            node.getRaftRegistry().register(MessageType.APPEND_ENTRIES, new AppendEntriesRpcHandler(raft, nodeId));
            node.getRaftDispatcher().setAllowedPeerIds(VOTER_SET);
            node.start();

            cluster.put(nodeId, node);
            resources.add(node);
        }
        return cluster;
    }

    private static byte[] intToBigEndian(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static void sendRawBytesAndExpectClose(String host, int port, byte[] rawBytes) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(1500);
            OutputStream out = socket.getOutputStream();
            out.write(rawBytes);
            out.flush();
            // Server closes malformed connection per SPEC-03 §2 (read returns -1 or SocketException)
            try {
                int read = socket.getInputStream().read();
                assertEquals(-1, read, "Server must close connection without corrupt response on malformed frame");
            } catch (IOException expectedOnResetOrClose) {
                // Connection reset or closed cleanly by server
            }
        }
    }

    private static void awaitZeroConnections(TcpServer server) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            if (server.getActiveConnectionCount() == 0) {
                return;
            }
            Thread.sleep(15);
        }
        assertEquals(0, server.getActiveConnectionCount(), "Server active connections did not drop to 0");
    }

    private static final class CapturingLogHandler extends Handler {
        private final List<String> entries = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            if (record != null && record.getLevel().intValue() >= Level.WARNING.intValue() && record.getMessage() != null) {
                entries.add(record.getLevel().getName() + ": " + record.getMessage());
            }
        }

        boolean containsWarning(String needle) {
            for (String entry : entries) {
                if (entry.contains(needle)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        void clear() {
            entries.clear();
        }

        @SuppressWarnings("unused")
        List<String> snapshot() {
            return Collections.unmodifiableList(entries);
        }
    }
}
