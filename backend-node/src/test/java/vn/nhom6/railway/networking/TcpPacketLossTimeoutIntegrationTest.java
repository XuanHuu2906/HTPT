package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV3-D26: Standalone TCP packet loss, response lost, timeout, correlation, and UNKNOWN invariant tests
 * using {@link TestNetworkFilter} (D21), {@link MessageDispatcher} (D25/D26), and loopback TCP sockets.
 *
 * <p>Does NOT claim physical 5-laptop LAN execution; physical LAN verification belongs to Gate G6.
 */
@DisplayName("TCP Packet Loss, Response Lost, Timeout & Correlation Fault Tests (TV3-D26)")
public class TcpPacketLossTimeoutIntegrationTest {

    private static final Logger LOGGER = Logger.getLogger(TcpPacketLossTimeoutIntegrationTest.class.getName());
    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Set<String> VOTER_SET = Set.copyOf(VOTERS);

    private final List<AutoCloseable> resources = new ArrayList<>();
    private TestNetworkFilter filter;

    @BeforeEach
    void setUp() {
        filter = new TestNetworkFilter();
    }

    @AfterEach
    void tearDown() {
        if (filter != null) {
            filter.heal();
        }
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    @Test
    @DisplayName("TC-D26-01: Request packet loss -> SocketTimeout -> UNKNOWN (never REJECTED) -> Heal & retry preserves requestId")
    void testRequestPacketLossCausesTimeoutUnknownAndSafeRetryPreservesRequestId() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        FakeBookingService bookingService = new FakeBookingService("node-1");
        bookingService.setLeader(true, "node-1");

        NodeMain node = new NodeMain(config, bookingService);
        resources.add(node);
        filter.attachTo(node);
        node.start();

        // Inject ingress packet loss from desktop-client-d26 -> node-1
        filter.blockLink("desktop-client-d26", "node-1");

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 1000, 180);
        ConnectionManager conn = new ConnectionManager("127.0.0.1", node.getClientPort(), policy);
        resources.add(conn);

        String requestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(requestId, "SE-001", "G03", "D26 Request Loss");
        MessageEnvelope<BookTicketRequestDto> req1 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "desktop-client-d26", bookDto
        );

        // 1. Timeout must yield UNKNOWN (TIMEOUT_UNKNOWN), NEVER REJECTED
        MessageEnvelope<BookTicketResponseDto> timedOutResp = conn.sendAndReceive(req1, BookTicketResponseDto.class);
        assertNotNull(timedOutResp);
        assertEquals(req1.getMessageId(), timedOutResp.getCorrelationId());
        BookTicketResponseDto timedOutDto = timedOutResp.getPayload();
        assertEquals("UNKNOWN", timedOutDto.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", timedOutDto.getReasonCode());
        assertEquals(requestId, timedOutDto.getRequestId());
        assertFalse(timedOutDto.isRejected(), "Timeout MUST NOT convert into REJECTED");
        assertFalse(conn.isConnected(), "Poisoned timed-out socket must be closed cleanly");
        assertEquals("AVAILABLE", bookingService.getStateMachine().seatStatus("G03").orElse(null),
                "Dropped request must not have booked seat G03");
        assertEquals(0, bookingService.getStateMachine().lastApplied(),
                "Dropped request must not have applied any command");
        assertEquals(1, filter.getDroppedCount("desktop-client-d26", "node-1"));
        assertEquals(0, node.getClientDispatcher().getActiveInFlightCount(), "No in-flight slot leak after request drop");

        // 2. Heal network & retry with EXACT SAME requestId (fresh messageId) -> CONFIRMED
        filter.heal();
        MessageEnvelope<BookTicketRequestDto> retryReq = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "desktop-client-d26", bookDto
        );
        assertNotEquals(req1.getMessageId(), retryReq.getMessageId(), "Retry must issue fresh transport messageId");

        MessageEnvelope<BookTicketResponseDto> okResp = conn.sendAndReceive(retryReq, BookTicketResponseDto.class);
        assertEquals(retryReq.getMessageId(), okResp.getCorrelationId());
        assertTrue(okResp.getPayload().isConfirmed());
        assertEquals(requestId, okResp.getPayload().getRequestId());
        assertEquals("G03", okResp.getPayload().getSeatId());
        assertEquals(0, node.getClientDispatcher().getActiveInFlightCount());
    }

    @Test
    @DisplayName("TC-D26-02: Response lost after server apply -> Client UNKNOWN -> GET_REQUEST_RESULT recovers original ticket without duplicate")
    void testResponseLostAfterBookingApplyRecoversSameTicketWithoutDuplicate() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        FakeBookingService bookingService = new FakeBookingService("node-1");
        bookingService.setLeader(true, "node-1");

        NodeMain node = new NodeMain(config, bookingService);
        resources.add(node);
        filter.attachTo(node);
        node.start();

        // Drop BOOK_TICKET_RESPONSE AFTER server handler executes & applies booking (TC-AC10 response lost)
        filter.dropResponseType(MessageType.BOOK_TICKET_RESPONSE);

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 1000, 180);
        ConnectionManager conn = new ConnectionManager("127.0.0.1", node.getClientPort(), policy);
        resources.add(conn);

        String requestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(requestId, "SE-001", "G04", "D26 Response Lost");
        MessageEnvelope<BookTicketRequestDto> bookReq = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "desktop-client-d26", bookDto
        );

        // 1. Server commits G04, but response is dropped on the return path -> Client receives UNKNOWN
        MessageEnvelope<BookTicketResponseDto> unknownResp = conn.sendAndReceive(bookReq, BookTicketResponseDto.class);
        assertEquals("UNKNOWN", unknownResp.getPayload().getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", unknownResp.getPayload().getReasonCode());
        assertEquals(requestId, unknownResp.getPayload().getRequestId());
        assertEquals(1, filter.getResponseDroppedCount());
        assertEquals(0, node.getClientDispatcher().getActiveInFlightCount(),
                "Dispatcher finally block must release in-flight entry even when response is dropped");

        // Server actually persisted/applied the ticket locally
        assertEquals("BOOKED", bookingService.getStateMachine().seatStatus("G04").orElse(null));
        String serverTicketId = bookingService.getStateMachine().seatCurrentTicketId("G04").orElseThrow();

        // 2. Client queries GET_REQUEST_RESULT using the EXACT SAME requestId (fresh messageId)
        MessageEnvelope<GetRequestResultRequestDto> lookupReq = MessageEnvelope.createRequest(
                MessageType.GET_REQUEST_RESULT,
                "desktop-client-d26",
                new GetRequestResultRequestDto(requestId)
        );
        MessageEnvelope<GetRequestResultResponseDto> lookupResp = conn.sendAndReceive(lookupReq, GetRequestResultResponseDto.class);
        assertEquals(lookupReq.getMessageId(), lookupResp.getCorrelationId());

        GetRequestResultResponseDto lookupDto = lookupResp.getPayload();
        assertEquals(requestId, lookupDto.getRequestId());
        assertEquals("APPLIED_SUCCESS", lookupDto.getState());
        assertEquals(serverTicketId, lookupDto.getTicketId());

        // 3. Also verify ConnectionManager with ResultLookup automatically recovers CONFIRMED on response drop
        filter.heal();
        filter.dropResponseType(MessageType.BOOK_TICKET_RESPONSE);
        conn.setResultLookup(bookingService);

        String requestId2 = UUID.randomUUID().toString();
        MessageEnvelope<BookTicketRequestDto> bookReq2 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET,
                "desktop-client-d26",
                new BookTicketRequestDto(requestId2, "SE-001", "G05", "D26 Auto Recovery")
        );
        MessageEnvelope<BookTicketResponseDto> recoveredResp = conn.sendAndReceive(bookReq2, BookTicketResponseDto.class);
        assertEquals(bookReq2.getMessageId(), recoveredResp.getCorrelationId());
        assertTrue(recoveredResp.getPayload().isConfirmed());
        assertEquals(requestId2, recoveredResp.getPayload().getRequestId());
        assertEquals(bookingService.getStateMachine().seatCurrentTicketId("G05").orElseThrow(),
                recoveredResp.getPayload().getTicketId());
        assertEquals(2, bookingService.getStateMachine().lastApplied(), "Must only have 2 applied entries, zero duplicates");
    }

    @Test
    @DisplayName("TC-D26-03: Strict correlation repair in MessageDispatcher and stale/late response rejection in RpcCorrelationRegistry")
    void testCorrelationInvariantEnforcedUnderMismatchedHandlerAndLateResponses() throws Exception {
        // Part A: MessageDispatcher repairs mismatched correlationId from handler
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.NODE_STATUS, req -> {
            // Faulty handler returns wrong correlationId
            return MessageEnvelope.createResponse(
                    UUID.randomUUID().toString(),
                    MessageType.ERROR_RESPONSE,
                    "node-1",
                    new ErrorResponseDto("INTERNAL_ERROR", "Corrupted handler correlation", null, null, null)
            );
        });
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");
        MessageEnvelope<JsonNode> req = MessageCodec.decode(
                MessageCodec.encode(MessageEnvelope.createRequest(MessageType.NODE_STATUS, "client-1", Map.of())),
                JsonNode.class
        );
        MessageEnvelope<?> fixed = dispatcher.dispatch(req);
        assertNotNull(fixed);
        assertEquals(req.getMessageId(), fixed.getCorrelationId(),
                "Dispatcher must rewrite mismatched correlationId to request.messageId");

        // Part B: RpcCorrelationRegistry drops late/unmatched correlationId after timeout without leaking slots
        try (RpcCorrelationRegistry corrRegistry = new RpcCorrelationRegistry(4)) {
            String timedOutMsgId = UUID.randomUUID().toString();
            CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> f1 = corrRegistry.register(
                    timedOutMsgId, "node-2", Duration.ofMillis(80), RequestVoteResponseDto.class
            );
            assertEquals(1, corrRegistry.getInFlightCount("node-2"));

            ExecutionException timeoutErr = assertThrows(ExecutionException.class, () -> f1.get(1, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, timeoutErr.getCause());
            assertEquals(0, corrRegistry.getInFlightCount("node-2"), "Timeout must release in-flight counter");

            // Late response for timedOutMsgId arrives -> must be discarded cleanly (returns false)
            MessageEnvelope<JsonNode> lateEnvelope = MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(timedOutMsgId, MessageType.REQUEST_VOTE_RESPONSE,
                            "node-2", new RequestVoteResponseDto(1L, true))
            ), JsonNode.class);
            assertFalse(corrRegistry.complete(timedOutMsgId, lateEnvelope),
                    "Late response after timeout must be discarded");

            // Concurrent valid RPC with a different correlationId is unaffected by late reply above
            String validMsgId = UUID.randomUUID().toString();
            CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> f2 = corrRegistry.register(
                    validMsgId, "node-2", Duration.ofSeconds(2), RequestVoteResponseDto.class
            );
            assertFalse(corrRegistry.complete(timedOutMsgId, lateEnvelope));
            assertFalse(f2.isDone(), "Late correlationId must never complete another pending RPC");

            MessageEnvelope<JsonNode> validEnvelope = MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(validMsgId, MessageType.REQUEST_VOTE_RESPONSE,
                            "node-2", new RequestVoteResponseDto(2L, true))
            ), JsonNode.class);
            assertTrue(corrRegistry.complete(validMsgId, validEnvelope));
            assertTrue(f2.get(1, TimeUnit.SECONDS).getPayload().isVoteGranted());
            assertEquals(0, corrRegistry.getInFlightCount("node-2"));
        }
    }

    @Test
    @DisplayName("TC-D26-04: 5-Node Loopback Cluster with D25 peer allowlist + TestNetworkFilter packet loss & heal")
    void testFiveNodeLoopbackPacketLossAndHealWithPeerConfig() throws Exception {
        Map<String, NodeMain> cluster = new LinkedHashMap<>();
        Map<String, Integer> peerPorts = new LinkedHashMap<>();

        for (String nodeId : VOTERS) {
            NodeConfig cfg = new NodeConfig(nodeId, "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 16, 5000);
            NodeMain node = new NodeMain(cfg, new FakeBookingService(nodeId));
            FakeRaftRequestHandler raftProcessor = new FakeRaftRequestHandler(4L);
            node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(raftProcessor, nodeId));
            node.getRaftRegistry().register(MessageType.APPEND_ENTRIES, new AppendEntriesRpcHandler(raftProcessor, nodeId));
            // Enforce D25 peer allowlist on PEER channel
            node.getRaftDispatcher().setAllowedPeerIds(VOTER_SET);

            node.start();
            cluster.put(nodeId, node);
            resources.add(node);
            peerPorts.put(nodeId, node.getRaftPort());
        }
        filter.attachToCluster(cluster);

        Map<String, InetSocketAddress> leaderPeers = new LinkedHashMap<>();
        for (String pid : VOTERS) {
            if (!"node-1".equals(pid)) {
                leaderPeers.put(pid, new InetSocketAddress("127.0.0.1", peerPorts.get(pid)));
            }
        }
        RaftRpcClient leaderRpc = new RaftRpcClient("node-1", leaderPeers, 16, 1000, Duration.ofMillis(250));
        resources.add(leaderRpc);

        // 1. Inject selective APPEND_ENTRIES packet loss from node-1 -> node-3 AND response loss node-4 -> node-1
        filter.dropMessageTypeOnLink("node-1", "node-3", MessageType.APPEND_ENTRIES);
        filter.dropResponseType(MessageType.APPEND_ENTRIES_RESPONSE);
        // Limit response drop only to node-4 -> node-1 so node-2 & node-5 succeed cleanly
        filter.heal();
        filter.dropMessageTypeOnLink("node-1", "node-3", MessageType.APPEND_ENTRIES);
        filter.addResponseDropPredicate((link, resp) ->
                "node-4".equals(link.from()) && "node-1".equals(link.to())
                        && resp != null && resp.getType() == MessageType.APPEND_ENTRIES_RESPONSE);

        AppendEntriesRequestDto heartbeat = new AppendEntriesRequestDto(4L, "node-1", 0L, 0L, List.of(), 0L);

        // node-2 & node-5 succeed with matching correlationIds
        MessageEnvelope<AppendEntriesResponseDto> r2 = leaderRpc
                .sendAppendEntries("node-2", heartbeat, Duration.ofMillis(800))
                .get(2, TimeUnit.SECONDS);
        MessageEnvelope<AppendEntriesResponseDto> r5 = leaderRpc
                .sendAppendEntries("node-5", heartbeat, Duration.ofMillis(800))
                .get(2, TimeUnit.SECONDS);
        assertTrue(r2.getPayload().isSuccess());
        assertTrue(r5.getPayload().isSuccess());

        // node-3 (request lost) and node-4 (response lost) both fail with TimeoutException
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> f3 =
                leaderRpc.sendAppendEntries("node-3", heartbeat, Duration.ofMillis(180));
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> f4 =
                leaderRpc.sendAppendEntries("node-4", heartbeat, Duration.ofMillis(180));

        ExecutionException ex3 = assertThrows(ExecutionException.class, () -> f3.get(2, TimeUnit.SECONDS));
        ExecutionException ex4 = assertThrows(ExecutionException.class, () -> f4.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, ex3.getCause());
        assertInstanceOf(TimeoutException.class, ex4.getCause());
        assertTrue(filter.getDroppedCount("node-1", "node-3") >= 1);
        assertTrue(filter.getResponseDroppedCount() >= 1);

        // Verify all 5 JVM nodes remain alive (no fake process kill) and activeInFlight == 0
        for (NodeMain node : cluster.values()) {
            assertTrue(node.isRunning());
            assertEquals(0, node.getRaftDispatcher().getActiveInFlightCount());
        }

        // 2. Heal network -> both node-3 and node-4 recover immediately
        filter.heal();
        assertTrue(leaderRpc.sendAppendEntries("node-3", heartbeat, Duration.ofMillis(800))
                .get(2, TimeUnit.SECONDS).getPayload().isSuccess());
        assertTrue(leaderRpc.sendAppendEntries("node-4", heartbeat, Duration.ofMillis(800))
                .get(2, TimeUnit.SECONDS).getPayload().isSuccess());
    }

    @Test
    @DisplayName("TC-D26-05: Repeated response-lost timeouts never corrupt next TCP frame on reconnect")
    void testRepeatedFaultAndTimeoutNeverCorruptsNextFramedExchange() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        FakeBookingService bookingService = new FakeBookingService("node-1");
        bookingService.setLeader(true, "node-1");

        NodeMain node = new NodeMain(config, bookingService);
        resources.add(node);
        filter.attachTo(node);
        node.start();

        AtomicInteger attempt = new AtomicInteger(0);
        // Drop first 2 BOOK_TICKET_RESPONSE replies, let 3rd succeed
        filter.addResponseDropPredicate((link, resp) ->
                resp != null && resp.getType() == MessageType.BOOK_TICKET_RESPONSE && attempt.incrementAndGet() <= 2);

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 1000, 150);
        ConnectionManager conn = new ConnectionManager("127.0.0.1", node.getClientPort(), policy);
        resources.add(conn);

        String req1 = UUID.randomUUID().toString();
        String req2 = UUID.randomUUID().toString();
        String req3 = UUID.randomUUID().toString();

        assertEquals("UNKNOWN", conn.sendAndReceive(
                MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-loop",
                        new BookTicketRequestDto(req1, "SE-001", "G06", "Drop1")),
                BookTicketResponseDto.class).getPayload().getOutcome());
        assertEquals("UNKNOWN", conn.sendAndReceive(
                MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-loop",
                        new BookTicketRequestDto(req2, "SE-001", "G07", "Drop2")),
                BookTicketResponseDto.class).getPayload().getOutcome());

        MessageEnvelope<BookTicketResponseDto> ok = conn.sendAndReceive(
                MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-loop",
                        new BookTicketRequestDto(req3, "SE-001", "G08", "Pass3")),
                BookTicketResponseDto.class);
        assertTrue(ok.getPayload().isConfirmed());
        assertEquals(req3, ok.getPayload().getRequestId());
    }

    @Test
    @DisplayName("TC-D26-06: Channel isolation (TC-TCP-04) & D25 allowlist remain intact under active fault filter")
    void testPeerConfigIsolationRemainsIntactUnderPacketLoss() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        NodeMain node = new NodeMain(config, new FakeBookingService("node-1"));
        resources.add(node);
        node.getRaftDispatcher().setAllowedPeerIds(VOTER_SET);
        filter.attachTo(node);
        filter.blockLink("node-2", "node-3"); // Unrelated active fault rule
        node.start();

        // Client port must still reject REQUEST_VOTE
        try (TcpClient client = new TcpClient("127.0.0.1", node.getClientPort(), 1000, 2000)) {
            client.connect();
            MessageEnvelope<?> leakedVote = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE, "node-2", new RequestVoteRequestDto(1L, "node-2", 0L, 0L));
            MessageEnvelope<ErrorResponseDto> err = MessageCodec.decode(
                    client.sendAndReceive(MessageCodec.encode(leakedVote)), ErrorResponseDto.class);
            assertEquals("INVALID_REQUEST", err.getPayload().getErrorCode());
        }

        // Peer port must still reject unknown senderId outside D25 allowlist
        try (TcpClient peer = new TcpClient("127.0.0.1", node.getRaftPort(), 1000, 2000)) {
            peer.connect();
            MessageEnvelope<?> rogueVote = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE, "unknown-peer", new RequestVoteRequestDto(1L, "unknown-peer", 0L, 0L));
            MessageEnvelope<ErrorResponseDto> err = MessageCodec.decode(
                    peer.sendAndReceive(MessageCodec.encode(rogueVote)), ErrorResponseDto.class);
            assertEquals("INVALID_REQUEST", err.getPayload().getErrorCode());
        }

        LOGGER.info("D26_LOOPBACK_FAULT_TESTS=PASS; PHYSICAL_5_LAPTOP_G6=NOT_RUN (DEFERRED_TO_G6)");
    }
}
