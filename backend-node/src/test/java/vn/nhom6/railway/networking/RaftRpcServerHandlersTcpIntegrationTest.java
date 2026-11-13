package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftRpcServerHandlersTcpIntegrationTest {

    private TcpServer server;
    private int serverPort;
    private HandlerRegistry registry;
    private MessageDispatcher dispatcher;
    private FakeRaftRequestHandler fakeProcessor;
    private RaftRpcClient client;

    private final String localNodeId = "node-1";
    private final String remoteNodeId = "node-2";

    @BeforeEach
    void setUp() throws IOException {
        fakeProcessor = new FakeRaftRequestHandler(1L);

        RequestVoteRpcHandler voteHandler = new RequestVoteRpcHandler(fakeProcessor, localNodeId);
        AppendEntriesRpcHandler appendHandler = new AppendEntriesRpcHandler(fakeProcessor, localNodeId);

        registry = new HandlerRegistry();
        registry.register(MessageType.REQUEST_VOTE, voteHandler);
        registry.register(MessageType.APPEND_ENTRIES, appendHandler);

        dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, localNodeId);

        server = new TcpServer("127.0.0.1", 0).setMessageHandler(dispatcher);
        server.start();
        serverPort = server.getPort();

        Map<String, InetSocketAddress> peers = Map.of(
                localNodeId, new InetSocketAddress("127.0.0.1", serverPort)
        );
        client = new RaftRpcClient(remoteNodeId, peers, 64, 5000, Duration.ofSeconds(3));
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @DisplayName("Should receive and handle REQUEST_VOTE inbound over TCP socket")
    void testInboundRequestVoteOverTcp() throws Exception {
        fakeProcessor.setVoteGranted(true);
        fakeProcessor.setCurrentTerm(2L);

        RequestVoteRequestDto voteReq = new RequestVoteRequestDto(2L, remoteNodeId, 10L, 2L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                client.sendRequestVote(localNodeId, voteReq);

        MessageEnvelope<RequestVoteResponseDto> response = future.get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, response.getType());
        assertEquals(localNodeId, response.getSenderId());
        assertNotNull(response.getCorrelationId());

        RequestVoteResponseDto payload = response.getPayload();
        assertTrue(payload.isVoteGranted());
        assertEquals(2L, payload.getTerm());

        assertEquals(1, fakeProcessor.getVoteInvocations().size());
        assertEquals(remoteNodeId, fakeProcessor.getVoteInvocations().get(0).senderId());
    }

    @Test
    @DisplayName("Should receive and handle APPEND_ENTRIES heartbeat inbound over TCP socket")
    void testInboundAppendEntriesHeartbeatOverTcp() throws Exception {
        fakeProcessor.setAppendSuccess(true);
        fakeProcessor.setCurrentTerm(3L);

        AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(
                3L, remoteNodeId, 20L, 2L, 20L);
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> future =
                client.sendAppendEntries(localNodeId, heartbeat);

        MessageEnvelope<AppendEntriesResponseDto> response = future.get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());
        assertEquals(localNodeId, response.getSenderId());

        AppendEntriesResponseDto payload = response.getPayload();
        assertTrue(payload.isSuccess());
        assertEquals(3L, payload.getTerm());
        assertEquals(20L, payload.getMatchIndex());

        assertEquals(1, fakeProcessor.getAppendInvocations().size());
    }

    @Test
    @DisplayName("Should receive and handle APPEND_ENTRIES containing log entries over TCP socket")
    void testInboundAppendEntriesWithLogArrayOverTcp() throws Exception {
        fakeProcessor.setAppendSuccess(true);
        fakeProcessor.setCurrentTerm(4L);

        AppendEntriesRequestDto.LogEntryDto entry1 = new AppendEntriesRequestDto.LogEntryDto(
                21L, 4L, "BOOK_TICKET", Map.of("requestId", "req-101"));
        AppendEntriesRequestDto.LogEntryDto entry2 = new AppendEntriesRequestDto.LogEntryDto(
                22L, 4L, "BOOK_TICKET", Map.of("requestId", "req-102"));
        AppendEntriesRequestDto.LogEntryDto entry3 = new AppendEntriesRequestDto.LogEntryDto(
                23L, 4L, "BOOK_TICKET", Map.of("requestId", "req-103"));

        AppendEntriesRequestDto appendReq = new AppendEntriesRequestDto(
                4L, remoteNodeId, 20L, 3L, List.of(entry1, entry2, entry3), 20L);
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> future =
                client.sendAppendEntries(localNodeId, appendReq);

        MessageEnvelope<AppendEntriesResponseDto> response = future.get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());

        AppendEntriesResponseDto payload = response.getPayload();
        assertTrue(payload.isSuccess());
        assertEquals(4L, payload.getTerm());
        assertEquals(23L, payload.getMatchIndex()); // 20 + 3 = 23
    }

    @Test
    @DisplayName("Should reject sender mismatch over TCP with ERROR_RESPONSE")
    void testInboundSenderMismatchOverTcp() {
        // Client sends with remoteNodeId ("node-2"), but candidateId in payload is "node-3"
        RequestVoteRequestDto voteReq = new RequestVoteRequestDto(2L, "node-3", 10L, 2L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                client.sendRequestVote(localNodeId, voteReq);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
        assertInstanceOf(IOException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("INVALID_REQUEST"));
    }

    @Test
    @DisplayName("Should handle 100 concurrent interleaved RPCs (50 VOTE + 50 APPEND) over reused TCP connection")
    void testConcurrentInboundRpcsOverTcp() throws Exception {
        int totalRequests = 100;
        // This test measures server multiplexing; permit the full submitted burst.
        // Backpressure at a lower bound is verified separately in RpcCorrelationRegistryTest.
        client.close();
        client = new RaftRpcClient(remoteNodeId,
                Map.of(localNodeId, new InetSocketAddress("127.0.0.1", serverPort)),
                totalRequests, 5000, Duration.ofSeconds(3));
        List<CompletableFuture<?>> futures = new ArrayList<>(totalRequests);

        for (int i = 0; i < totalRequests; i++) {
            final long term = i + 1;
            final boolean isVote = (i % 2 == 0);
            if (isVote) {
                RequestVoteRequestDto req = new RequestVoteRequestDto(term, remoteNodeId, 0L, 0L);
                futures.add(client.sendRequestVote(localNodeId, req).thenAccept(resp -> {
                    assertEquals(MessageType.REQUEST_VOTE_RESPONSE, resp.getType());
                    assertTrue(resp.getPayload().isVoteGranted());
                }));
            } else {
                AppendEntriesRequestDto req = AppendEntriesRequestDto.createHeartbeat(term, remoteNodeId, term, term, term);
                futures.add(client.sendAppendEntries(localNodeId, req).thenAccept(resp -> {
                    assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, resp.getType());
                    assertTrue(resp.getPayload().isSuccess());
                }));
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        assertEquals(50, fakeProcessor.getVoteInvocations().size());
        assertEquals(50, fakeProcessor.getAppendInvocations().size());
    }

    @Test
    @DisplayName("Should safely handle raw malformed JSON frame inbound over TCP without crashing server")
    void testInboundMalformedJsonHandledSafelyOverTcp() throws Exception {
        try (TcpClient rawClient = new TcpClient("127.0.0.1", serverPort, 2000, 5000)) {
            rawClient.connect();

            // Send malformed frame payload
            String corruptJson = "{\"protocolVersion\":1,\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"REQUEST_VOTE\",\"corrupt\"}";
            byte[] rawBytes = corruptJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            // Server may return an ERROR_RESPONSE or close malformed frame per SPEC-03 Section 2
            try {
                byte[] responseBytes = rawClient.sendAndReceive(rawBytes);
                assertNotNull(responseBytes);
                MessageEnvelope<?> respEnvelope = MessageCodec.decode(responseBytes);
                assertEquals(MessageType.ERROR_RESPONSE, respEnvelope.getType());
            } catch (IOException eofOrReset) {
                // Connection closed per SPEC-03 Section 2 is also an acceptable defense
                assertNotNull(eofOrReset);
            }
        }

        // Verify server is still completely alive and accepting valid RPCs
        RequestVoteRequestDto validReq = new RequestVoteRequestDto(10L, remoteNodeId, 0L, 0L);
        var resp = client.sendRequestVote(localNodeId, validReq).get(3, TimeUnit.SECONDS);
        assertNotNull(resp);
        assertTrue(resp.getPayload().isVoteGranted());
    }
}
