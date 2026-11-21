package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PeerConnectionManager Unit & Integration Tests (TV3-D22)")
public class PeerConnectionManagerTest {

    private static final Logger LOGGER = Logger.getLogger(PeerConnectionManagerTest.class.getName());

    private final List<AutoCloseable> resources = new ArrayList<>();
    private final Map<String, NodeMain> nodes = new LinkedHashMap<>();
    private final Map<String, FakeBookingService> bookingServices = new LinkedHashMap<>();
    private final Map<String, InetSocketAddress> peerEndpoints = new LinkedHashMap<>();

    private BookingStateMachine sharedReplicatedStateMachine;
    private PeerConnectionManager connectionManager;

    @BeforeEach
    void setUp() throws IOException {
        sharedReplicatedStateMachine = new BookingStateMachine();

        // Start 3 test nodes on ephemeral ports (port 0)
        List<String> nodeIds = List.of("node-1", "node-2", "node-3");
        for (String id : nodeIds) {
            NodeConfig config = new NodeConfig(
                    id,
                    "railway-demo-cluster",
                    "127.0.0.1",
                    0,
                    "127.0.0.1",
                    0,
                    null,
                    8,
                    5_000
            );

            // Replicated state machine simulates Raft replication
            FakeBookingService service = new FakeBookingService(id, sharedReplicatedStateMachine);
            bookingServices.put(id, service);

            NodeMain node = new NodeMain(config, service);
            node.start();
            nodes.put(id, node);
            resources.add(node::stop);

            peerEndpoints.put(id, new InetSocketAddress("127.0.0.1", node.getClientPort()));
        }

        // Initially node-1 is the Leader, node-2 & node-3 are Followers
        bookingServices.get("node-1").setLeader(true, "node-1");
        bookingServices.get("node-2").setLeader(false, "node-1");
        bookingServices.get("node-3").setLeader(false, "node-1");

        RetryPolicy fastRetryPolicy = new RetryPolicy(2, 20L, 50L, 1.5, 1500, 1500);
        connectionManager = new PeerConnectionManager("client-test", peerEndpoints, fastRetryPolicy);
        connectionManager.onElectionCompleted("node-1", 1L);
        resources.add(connectionManager);
    }

    @AfterEach
    void tearDown() {
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ignored) {}
        }
        resources.clear();
        nodes.clear();
        bookingServices.clear();
        peerEndpoints.clear();
    }

    @Test
    @DisplayName("Post-Election Routing Refresh: Closes stale socket to old leader and switches to new leader")
    void testPostElectionRoutingRefreshClosesStaleAndSwitchesLeader() throws Exception {
        // 1. Initial request goes to node-1 (Leader)
        MessageEnvelope<GetSeatsRequestDto> seatsReq = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-test", new GetSeatsRequestDto("SE-001")
        );
        MessageEnvelope<GetSeatsResponseDto> resp1 = connectionManager.sendToLeader(seatsReq, GetSeatsResponseDto.class);
        assertNotNull(resp1);
        assertEquals(MessageType.GET_SEATS_RESPONSE, resp1.getType());
        assertEquals("node-1", connectionManager.getCurrentLeaderId());
        assertTrue(connectionManager.hasActiveConnection("node-1"), "Connection to node-1 must be active");

        // 2. Election occurs: node-2 becomes new Leader (term 2)
        bookingServices.get("node-1").setLeader(false, "node-2");
        bookingServices.get("node-2").setLeader(true, "node-2");
        connectionManager.onElectionCompleted("node-2", 2L);

        assertEquals("node-2", connectionManager.getCurrentLeaderId());
        assertEquals(2L, connectionManager.getCurrentTerm());
        assertFalse(connectionManager.hasActiveConnection("node-1"), "Stale connection to node-1 must be closed");

        // 3. Next request automatically routes directly to node-2
        MessageEnvelope<GetSeatsRequestDto> seatsReq2 = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-test", new GetSeatsRequestDto("SE-001")
        );
        MessageEnvelope<GetSeatsResponseDto> resp2 = connectionManager.sendToLeader(seatsReq2, GetSeatsResponseDto.class);
        assertNotNull(resp2);
        assertEquals(MessageType.GET_SEATS_RESPONSE, resp2.getType());
        assertTrue(connectionManager.hasActiveConnection("node-2"), "Connection to node-2 must now be active");
    }

    @Test
    @DisplayName("Pass Criteria: Crash Leader mid-flight -> New Leader elected -> Query GET_REQUEST_RESULT succeeds without changing requestId")
    void testLeaderCrashMidFlightResolvedByGetRequestResultSuccess() throws Exception {
        String requestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(requestId, "SE-001", "G01", "Passenger Failover");

        // Step 1: Pre-apply the booking to the replicated state machine (as Raft would before leader crash)
        var appliedResult = bookingServices.get("node-1").bookTicket(requestId, "SE-001", "G01", "Passenger Failover");
        assertNotNull(appliedResult.ticketId());
        String expectedTicketId = appliedResult.ticketId();

        // Step 2: Crash node-1 (stop TCP server simulating network crash)
        nodes.get("node-1").stop();
        bookingServices.get("node-1").setLeader(false, null);

        // Step 3: Election occurs -> node-2 is elected new Leader (term 2)
        bookingServices.get("node-2").setLeader(true, "node-2");
        connectionManager.setRoutingProvider(() -> new PeerConnectionManager.LeaderRoutingProvider.LeaderInfo("node-2", 2L));
        connectionManager.onElectionCompleted("node-1", 1L);

        // Step 4: Client executes booking with failover
        // Invariant: MUST NOT issue a new BOOK_TICKET; must query GET_REQUEST_RESULT with original requestId!
        BookTicketResponseDto response = connectionManager.executeBookingWithLeaderFailover(bookDto);

        assertNotNull(response);
        response.validate();
        assertEquals("CONFIRMED", response.getOutcome(), "Outcome must be recovered as CONFIRMED");
        assertEquals(requestId, response.getRequestId(), "Business requestId must be 100% preserved");
        assertEquals(expectedTicketId, response.getTicketId(), "Must recover original ticketId without double-booking");

        // Step 5: Verify in state machine that only 1 ticket was booked for G01
        var seatStatus = sharedReplicatedStateMachine.seatStatus("G01");
        assertEquals(Optional.of("BOOKED"), seatStatus);
        assertEquals(Optional.of(expectedTicketId), sharedReplicatedStateMachine.seatCurrentTicketId("G01"));
    }

    @Test
    @DisplayName("Crash before commit -> GET_REQUEST_RESULT returns UNKNOWN -> Retries with exact same requestId")
    void testLeaderCrashBeforeCommitRetriesWithExactSameRequestId() throws Exception {
        String requestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(requestId, "SE-001", "G02", "Passenger Retry");

        // Crash node-1 BEFORE applying anything
        nodes.get("node-1").stop();
        bookingServices.get("node-1").setLeader(false, null);

        // New election elects node-2
        bookingServices.get("node-2").setLeader(true, "node-2");
        connectionManager.setRoutingProvider(() -> new PeerConnectionManager.LeaderRoutingProvider.LeaderInfo("node-2", 2L));
        connectionManager.onElectionCompleted("node-1", 1L);

        // Execute booking: GET_REQUEST_RESULT will return UNKNOWN, then it resubmits with the SAME requestId
        BookTicketResponseDto response = connectionManager.executeBookingWithLeaderFailover(bookDto);

        assertNotNull(response);
        response.validate();
        assertEquals("CONFIRMED", response.getOutcome());
        assertEquals(requestId, response.getRequestId(), "RequestId must be preserved on retry");
        assertNotNull(response.getTicketId());

        // Verify seat is now booked with that ticket
        assertEquals(Optional.of("BOOKED"), sharedReplicatedStateMachine.seatStatus("G02"));
    }

    @Test
    @DisplayName("Follower NOT_LEADER redirect: Automatically refreshes routing to leaderHint and preserves requestId")
    void testFollowerNotLeaderRedirectRefreshesRoutingAndPreservesRequestId() throws Exception {
        String requestId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(requestId, "SE-001", "G03", "Passenger Redirect");

        // Make node-2 the true Leader, node-1 is Follower with hint="node-2"
        bookingServices.get("node-1").setLeader(false, "node-2");
        bookingServices.get("node-2").setLeader(true, "node-2");

        // PCM initially pointed to node-1
        connectionManager.onElectionCompleted("node-1", 1L);

        MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-test", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> resp = connectionManager.sendToLeader(req, BookTicketResponseDto.class);

        assertNotNull(resp);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
        BookTicketResponseDto payload = resp.getPayload();
        payload.validate();
        assertEquals("CONFIRMED", payload.getOutcome());
        assertEquals(requestId, payload.getRequestId(), "RequestId must be strictly preserved across redirect");

        // Verify routing was updated to node-2
        assertEquals("node-2", connectionManager.getCurrentLeaderId());
    }

    @Test
    @DisplayName("Successive leader crashes: node-1 crashes -> node-2 elected -> node-2 crashes -> node-3 elected")
    void testRepeatedLeaderCrashesFailoverSafety() throws Exception {
        String req1 = UUID.randomUUID().toString();
        BookTicketRequestDto book1 = new BookTicketRequestDto(req1, "SE-001", "G04", "Passenger 1");

        // 1. Book on node-1
        BookTicketResponseDto resp1 = connectionManager.executeBookingWithLeaderFailover(book1);
        assertEquals("CONFIRMED", resp1.getOutcome());
        assertEquals(req1, resp1.getRequestId());

        // 2. node-1 crashes, node-2 elected
        nodes.get("node-1").stop();
        bookingServices.get("node-2").setLeader(true, "node-2");
        connectionManager.onElectionCompleted("node-2", 2L);

        String req2 = UUID.randomUUID().toString();
        BookTicketRequestDto book2 = new BookTicketRequestDto(req2, "SE-001", "G05", "Passenger 2");
        BookTicketResponseDto resp2 = connectionManager.executeBookingWithLeaderFailover(book2);
        assertEquals("CONFIRMED", resp2.getOutcome());
        assertEquals(req2, resp2.getRequestId());

        // 3. node-2 crashes, node-3 elected
        nodes.get("node-2").stop();
        bookingServices.get("node-3").setLeader(true, "node-3");
        connectionManager.onElectionCompleted("node-3", 3L);

        String req3 = UUID.randomUUID().toString();
        BookTicketRequestDto book3 = new BookTicketRequestDto(req3, "SE-001", "G06", "Passenger 3");
        BookTicketResponseDto resp3 = connectionManager.executeBookingWithLeaderFailover(book3);
        assertEquals("CONFIRMED", resp3.getOutcome());
        assertEquals(req3, resp3.getRequestId());

        // All 3 requests preserved unique requestIds and booked 3 distinct seats
        assertEquals(Optional.of(resp1.getTicketId()), sharedReplicatedStateMachine.seatCurrentTicketId("G04"));
        assertEquals(Optional.of(resp2.getTicketId()), sharedReplicatedStateMachine.seatCurrentTicketId("G05"));
        assertEquals(Optional.of(resp3.getTicketId()), sharedReplicatedStateMachine.seatCurrentTicketId("G06"));
    }

    @Test
    @DisplayName("Direct queryRequestResult: Maps typed outcomes (APPLIED_SUCCESS, APPLIED_REJECTED, UNKNOWN)")
    void testDirectQueryRequestResultOutcomes() throws Exception {
        String successReqId = UUID.randomUUID().toString();
        String rejectedReqId = UUID.randomUUID().toString();
        String unknownReqId = UUID.randomUUID().toString();

        // 1. Book G07 successfully
        bookingServices.get("node-1").bookTicket(successReqId, "SE-001", "G07", "Passenger Success");
        var successLookup = connectionManager.queryRequestResult(successReqId);
        assertTrue(successLookup.isSuccess());
        assertEquals("APPLIED_SUCCESS", successLookup.state());
        assertNotNull(successLookup.ticketId());
        assertEquals(successReqId, successLookup.requestId());

        // 2. Book G07 again (will be rejected with SEAT_ALREADY_BOOKED)
        bookingServices.get("node-1").bookTicket(rejectedReqId, "SE-001", "G07", "Passenger Conflict");
        var rejectedLookup = connectionManager.queryRequestResult(rejectedReqId);
        assertTrue(rejectedLookup.isRejected());
        assertEquals("APPLIED_REJECTED", rejectedLookup.state());
        assertEquals("SEAT_ALREADY_BOOKED", rejectedLookup.reasonCode());
        assertEquals(rejectedReqId, rejectedLookup.requestId());

        // 3. Query unsubmitted requestId
        var unknownLookup = connectionManager.queryRequestResult(unknownReqId);
        assertTrue(unknownLookup.isUnknown());
        assertEquals("UNKNOWN", unknownLookup.state());
        assertEquals(unknownReqId, unknownLookup.requestId());
    }
}
