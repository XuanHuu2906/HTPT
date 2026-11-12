package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftRpcTcpIntegrationTest {

    private int portNode1;
    private int portNode2;

    private TcpServer serverNode1;
    private TcpServer serverNode2;

    private MessageDispatcher dispatcherNode1;
    private MessageDispatcher dispatcherNode2;

    private HandlerRegistry registryNode1;
    private HandlerRegistry registryNode2;

    private RaftRpcClient clientNode1;
    private RaftRpcClient clientNode2;

    @BeforeEach
    void setUp() throws IOException {
        // Node 1 Server & Dispatcher
        registryNode1 = new HandlerRegistry();
        dispatcherNode1 = new MessageDispatcher(registryNode1, MessageDispatcher.ChannelType.PEER, "node-1");
        serverNode1 = new TcpServer("127.0.0.1", 0).setMessageHandler(dispatcherNode1::dispatch);
        serverNode1.start();
        portNode1 = serverNode1.getPort();

        // Node 2 Server & Dispatcher
        registryNode2 = new HandlerRegistry();
        dispatcherNode2 = new MessageDispatcher(registryNode2, MessageDispatcher.ChannelType.PEER, "node-2");
        serverNode2 = new TcpServer("127.0.0.1", 0).setMessageHandler(dispatcherNode2::dispatch);
        serverNode2.start();
        portNode2 = serverNode2.getPort();

        // Node 1 RaftRpcClient
        Map<String, InetSocketAddress> peers1 = Map.of(
                "node-2", new InetSocketAddress("127.0.0.1", portNode2)
        );
        clientNode1 = new RaftRpcClient("node-1", peers1, 64, 5000, Duration.ofSeconds(3));

        // Node 2 RaftRpcClient
        Map<String, InetSocketAddress> peers2 = Map.of(
                "node-1", new InetSocketAddress("127.0.0.1", portNode1)
        );
        clientNode2 = new RaftRpcClient("node-2", peers2, 64, 5000, Duration.ofSeconds(3));
    }

    @AfterEach
    void tearDown() {
        if (clientNode1 != null) clientNode1.close();
        if (clientNode2 != null) clientNode2.close();
        if (serverNode1 != null) serverNode1.stop();
        if (serverNode2 != null) serverNode2.stop();
    }

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @Test
    @DisplayName("Test 1: Node 1 sends REQUEST_VOTE to Node 2 and receives typed REQUEST_VOTE_RESPONSE")
    void testExchangeRequestVoteOverTcp() throws Exception {
        // Register RaftHandler on Node 2 to handle REQUEST_VOTE
        registryNode2.register(MessageType.REQUEST_VOTE, req -> {
            RequestVoteRequestDto voteReq = MessageCodec.decodePayload(req, RequestVoteRequestDto.class);
            assertEquals("node-1", voteReq.getCandidateId());
            assertEquals(1L, voteReq.getTerm());

            RequestVoteResponseDto voteResp = RequestVoteResponseDto.granted(voteReq.getTerm());
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2", voteResp);
        });

        RequestVoteRequestDto request = new RequestVoteRequestDto(1L, "node-1", 0L, 0L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                clientNode1.sendRequestVote("node-2", request);

        MessageEnvelope<RequestVoteResponseDto> response = future.get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, response.getType());
        assertEquals("node-2", response.getSenderId());
        assertNotNull(response.getCorrelationId());
        assertEquals(1L, response.getPayload().getTerm());
        assertTrue(response.getPayload().isVoteGranted());
        assertTrue(clientNode1.isConnectedTo("node-2"), "Connection to node-2 must be reused and active");
    }

    @Test
    @DisplayName("Test 2: Node 1 sends APPEND_ENTRIES heartbeat to Node 2 and receives typed response")
    void testExchangeAppendEntriesHeartbeatOverTcp() throws Exception {
        registryNode2.register(MessageType.APPEND_ENTRIES, req -> {
            AppendEntriesRequestDto appendReq = MessageCodec.decodePayload(req, AppendEntriesRequestDto.class);
            assertEquals("node-1", appendReq.getLeaderId());
            assertTrue(appendReq.isHeartbeat());

            AppendEntriesResponseDto resp = AppendEntriesResponseDto.success(appendReq.getTerm(), appendReq.getPrevLogIndex());
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, "node-2", resp);
        });

        AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(2L, "node-1", 10L, 2L, 10L);
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> future =
                clientNode1.sendAppendEntries("node-2", heartbeat);

        MessageEnvelope<AppendEntriesResponseDto> response = future.get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());
        assertEquals("node-2", response.getSenderId());
        assertEquals(2L, response.getPayload().getTerm());
        assertTrue(response.getPayload().isSuccess());
        assertEquals(10L, response.getPayload().getMatchIndex());
    }

    @Test
    @DisplayName("Test 3: Bi-directional exchange: Node 1 sends VOTE to Node 2 and Node 2 sends APPEND to Node 1")
    void testBiDirectionalRaftRpcExchange() throws Exception {
        // Node 2 handles vote
        registryNode2.register(MessageType.REQUEST_VOTE, req -> {
            RequestVoteRequestDto voteReq = MessageCodec.decodePayload(req, RequestVoteRequestDto.class);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2",
                    RequestVoteResponseDto.granted(voteReq.getTerm()));
        });

        // Node 1 handles append
        registryNode1.register(MessageType.APPEND_ENTRIES, req -> {
            AppendEntriesRequestDto appendReq = MessageCodec.decodePayload(req, AppendEntriesRequestDto.class);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, "node-1",
                    AppendEntriesResponseDto.success(appendReq.getTerm(), 50L));
        });

        // Node 1 sends vote to Node 2
        RequestVoteRequestDto voteReq = new RequestVoteRequestDto(3L, "node-1", 5L, 2L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> voteFuture =
                clientNode1.sendRequestVote("node-2", voteReq);

        // Node 2 sends append to Node 1
        AppendEntriesRequestDto appendReq = AppendEntriesRequestDto.createHeartbeat(3L, "node-2", 50L, 3L, 50L);
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> appendFuture =
                clientNode2.sendAppendEntries("node-1", appendReq);

        CompletableFuture.allOf(voteFuture, appendFuture).get(3, TimeUnit.SECONDS);

        assertTrue(voteFuture.get().getPayload().isVoteGranted());
        assertTrue(appendFuture.get().getPayload().isSuccess());
        assertEquals(50L, appendFuture.get().getPayload().getMatchIndex());
    }

    @Test
    @DisplayName("Test 4: High concurrency: 100 interleaved RPCs over single reused connection")
    void testConcurrentRpcsOverReusedConnection() throws Exception {
        AtomicInteger voteCount = new AtomicInteger(0);
        AtomicInteger appendCount = new AtomicInteger(0);

        registryNode2.register(MessageType.REQUEST_VOTE, req -> {
            voteCount.incrementAndGet();
            RequestVoteRequestDto r = MessageCodec.decodePayload(req, RequestVoteRequestDto.class);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2",
                    RequestVoteResponseDto.granted(r.getTerm()));
        });

        registryNode2.register(MessageType.APPEND_ENTRIES, req -> {
            appendCount.incrementAndGet();
            AppendEntriesRequestDto a = MessageCodec.decodePayload(req, AppendEntriesRequestDto.class);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, "node-2",
                    AppendEntriesResponseDto.success(a.getTerm(), a.getPrevLogIndex()));
        });

        int totalRequests = 100;
        List<CompletableFuture<?>> futures = new ArrayList<>(totalRequests);
        Map<String, Long> expectedTerms = new ConcurrentHashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < totalRequests; i++) {
            final long term = i + 1;
            final boolean isVote = (i % 2 == 0);
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    if (isVote) {
                        RequestVoteRequestDto req = new RequestVoteRequestDto(term, "node-1", 0L, 0L);
                        var future = clientNode1.sendRequestVote("node-2", req);
                        var resp = future.get(5, TimeUnit.SECONDS);
                        assertEquals(term, resp.getPayload().getTerm());
                        assertTrue(resp.getPayload().isVoteGranted());
                    } else {
                        AppendEntriesRequestDto req = AppendEntriesRequestDto.createHeartbeat(term, "node-1", term, term, term);
                        var future = clientNode1.sendAppendEntries("node-2", req);
                        var resp = future.get(5, TimeUnit.SECONDS);
                        assertEquals(term, resp.getPayload().getTerm());
                        assertTrue(resp.getPayload().isSuccess());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, pool));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(50, voteCount.get());
        assertEquals(50, appendCount.get());
        assertEquals(0, clientNode1.getCorrelationRegistry().getInFlightCount("node-2"));
        assertEquals(0, clientNode1.getCorrelationRegistry().getTotalInFlightCount());
    }

    @Test
    @DisplayName("Test 5: Timeout when peer handler is slow / unresponsive")
    void testTimeoutOnUnresponsivePeer() {
        // Node 2 sleeps 1000ms
        registryNode2.register(MessageType.REQUEST_VOTE, req -> {
            Thread.sleep(1000);
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2",
                    RequestVoteResponseDto.granted(1L));
        });

        RequestVoteRequestDto req = new RequestVoteRequestDto(1L, "node-1", 0L, 0L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                clientNode1.sendRequestVote("node-2", req, Duration.ofMillis(100));

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("timed out after 100 ms"));
    }

    @Test
    @DisplayName("Test 6: Disconnect and reconnect resilience")
    void testDisconnectAndReconnectResilience() throws Exception {
        registryNode2.register(MessageType.REQUEST_VOTE, req -> MessageEnvelope.createResponse(
                req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2", RequestVoteResponseDto.granted(1L)));

        // 1. Initial successful RPC
        var resp1 = clientNode1.sendRequestVote("node-2", new RequestVoteRequestDto(1L, "node-1", 0L, 0L)).get();
        assertTrue(resp1.getPayload().isVoteGranted());
        assertTrue(clientNode1.isConnectedTo("node-2"));

        // 2. Stop Node 2 server abruptly
        serverNode2.stop();

        // Verify that immediate subsequent call detects disconnect or fails
        assertThrows(Exception.class, () -> {
            clientNode1.sendRequestVote("node-2", new RequestVoteRequestDto(2L, "node-1", 0L, 0L), Duration.ofMillis(300)).get();
        });

        // 3. Restart Node 2 on a fresh port and update peer endpoint
        serverNode2 = new TcpServer("127.0.0.1", 0).setMessageHandler(dispatcherNode2::dispatch);
        serverNode2.start();
        int newPortNode2 = serverNode2.getPort();
        clientNode1.setPeerEndpoint("node-2", "127.0.0.1", newPortNode2);

        // 4. Send new RPC -> client automatically re-establishes connection and succeeds
        var resp2 = clientNode1.sendRequestVote("node-2", new RequestVoteRequestDto(3L, "node-1", 0L, 0L)).get(3, TimeUnit.SECONDS);
        assertNotNull(resp2);
        assertTrue(resp2.getPayload().isVoteGranted());
        assertTrue(clientNode1.isConnectedTo("node-2"));
    }

    @Test
    @DisplayName("Test 7: Backpressure triggers fast failure when in-flight limit is reached")
    void testBackpressureEnforcement() throws Exception {
        // Create client with strict in-flight limit of 2
        Map<String, InetSocketAddress> peers = Map.of("node-2", new InetSocketAddress("localhost", portNode2));
        try (RaftRpcClient restrictedClient = new RaftRpcClient("node-1", peers, 2, 5000, Duration.ofSeconds(5))) {

            CountDownLatch pauseLatch = new CountDownLatch(1);
            registryNode2.register(MessageType.REQUEST_VOTE, req -> {
                pauseLatch.await();
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2",
                        RequestVoteResponseDto.granted(1L));
            });

            // Send 2 requests (reaches limit 2)
            var f1 = restrictedClient.sendRequestVote("node-2", new RequestVoteRequestDto(1L, "node-1", 0L, 0L));
            var f2 = restrictedClient.sendRequestVote("node-2", new RequestVoteRequestDto(1L, "node-1", 0L, 0L));

            // 3rd request must fail with RpcBackpressureException
            var f3 = restrictedClient.sendRequestVote("node-2", new RequestVoteRequestDto(1L, "node-1", 0L, 0L));

            ExecutionException ex = assertThrows(ExecutionException.class, f3::get);
            assertInstanceOf(RpcBackpressureException.class, ex.getCause());
            RpcBackpressureException bpEx = (RpcBackpressureException) ex.getCause();
            assertEquals("node-2", bpEx.getPeerId());
            assertEquals(2, bpEx.getInFlightCount());
            assertEquals(2, bpEx.getMaxInFlight());

            // Release handler and finish pending
            pauseLatch.countDown();
            assertTrue(f1.get().getPayload().isVoteGranted());
            assertTrue(f2.get().getPayload().isVoteGranted());
        }
    }

    @Test
    @DisplayName("Test 8: Two separate Java processes exchange VOTE and APPEND over TCP (Pass Criterion)")
    void testTwoJavaProcessesExchangeVoteAndAppendOverTcp() throws Exception {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "vn.nhom6.railway.networking.ProcessRaftNode",
                "0",
                "child-node"
        );
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process childProcess = pb.start();
        try {
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(childProcess.getInputStream()));
            String line;
            int childPort = -1;
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && (line = stdoutReader.readLine()) != null) {
                if (line.startsWith("READY:")) {
                    childPort = Integer.parseInt(line.substring("READY:".length()).trim());
                    break;
                }
            }
            assertTrue(childPort > 0, "Child Java process must report a valid listening port");

            // Register child node in parent RaftRpcClient
            clientNode1.setPeerEndpoint("child-node", "127.0.0.1", childPort);

            // 1. Send REQUEST_VOTE to child process
            RequestVoteRequestDto voteReq = new RequestVoteRequestDto(7L, "parent-node", 15L, 6L);
            CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> voteFuture =
                    clientNode1.sendRequestVote("child-node", voteReq);

            MessageEnvelope<RequestVoteResponseDto> voteResp = voteFuture.get(5, TimeUnit.SECONDS);
            assertNotNull(voteResp);
            assertEquals("child-node", voteResp.getSenderId());
            assertEquals(7L, voteResp.getPayload().getTerm());
            assertTrue(voteResp.getPayload().isVoteGranted());

            // 2. Send APPEND_ENTRIES to child process
            AppendEntriesRequestDto.LogEntryDto entry = new AppendEntriesRequestDto.LogEntryDto(
                    16L, 7L, "BOOK_TICKET", Map.of("requestId", "req-123"));
            AppendEntriesRequestDto appendReq = new AppendEntriesRequestDto(
                    7L, "parent-node", 15L, 6L, List.of(entry), 15L);
            CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> appendFuture =
                    clientNode1.sendAppendEntries("child-node", appendReq);

            MessageEnvelope<AppendEntriesResponseDto> appendResp = appendFuture.get(5, TimeUnit.SECONDS);
            assertNotNull(appendResp);
            assertEquals("child-node", appendResp.getSenderId());
            assertEquals(7L, appendResp.getPayload().getTerm());
            assertTrue(appendResp.getPayload().isSuccess());
            assertEquals(16L, appendResp.getPayload().getMatchIndex());
        } finally {
            childProcess.destroyForcibly();
            childProcess.waitFor(2, TimeUnit.SECONDS);
        }
    }
}
