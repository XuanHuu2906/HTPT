package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.Role;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for LeaderRouter (TASK TV3-D13)")
public class LeaderRouterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private MessageEnvelope<JsonNode> createBookRequest(String senderId, String requestId) throws Exception {
        JsonNode json = mapper.readTree(
                "{\"requestId\":\"" + requestId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G01\",\"passengerName\":\"Test\"}"
        );
        return MessageEnvelope.createRequest(MessageType.BOOK_TICKET, senderId, json);
    }

    @Test
    @DisplayName("Should execute locally when local node is LEADER")
    public void testLeaderExecutesLocally() throws Exception {
        LeaderRouter router = new LeaderRouter("node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 2L, "node-1"));

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createBookRequest("client-1", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(req);
        assertNotNull(outcome);
        assertTrue(outcome.shouldExecuteLocally());
        assertFalse(outcome.isHandled());
        assertNull(outcome.getResponse());
    }

    @Test
    @DisplayName("Should return NOT_LEADER redirect when local node is FOLLOWER with known leader")
    public void testFollowerRedirectWithKnownLeader() throws Exception {
        LeaderRouter router = new LeaderRouter("node-2", LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 5L, "node-1"));

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createBookRequest("client-1", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(req);
        assertNotNull(outcome);
        assertFalse(outcome.shouldExecuteLocally());
        assertTrue(outcome.isHandled());

        MessageEnvelope<?> resp = outcome.getResponse();
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals("node-2", resp.getSenderId());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(5L, err.getTerm());
        assertEquals(reqId, err.getRequestId(), "requestId must be strictly preserved");
    }

    @Test
    @DisplayName("Should fallback to next cluster voter when leaderHint is unknown (null or blank)")
    public void testFollowerFallbackWhenLeaderUnknown() throws Exception {
        LeaderRouter router = new LeaderRouter("node-2", LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 1L, null),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"));

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createBookRequest("client-1", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(req);
        assertTrue(outcome.isHandled());

        ErrorResponseDto err = (ErrorResponseDto) outcome.getResponse().getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertNotNull(err.getLeaderHint(), "Fallback node must be chosen");
        assertNotEquals("node-2", err.getLeaderHint(), "Fallback must NEVER be localNodeId");
        assertTrue(List.of("node-1", "node-3", "node-4", "node-5").contains(err.getLeaderHint()));
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("Should prevent self-redirect loop if leaderHint points to local node while not LEADER")
    public void testSelfRedirectLoopPrevention() throws Exception {
        // Stale state: node stepped down to FOLLOWER but leaderId variable still has "node-3"
        LeaderRouter router = new LeaderRouter("node-3", LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 3L, "node-3"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"));

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createBookRequest("client-1", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(req);
        assertTrue(outcome.isHandled());

        ErrorResponseDto err = (ErrorResponseDto) outcome.getResponse().getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertNotEquals("node-3", err.getLeaderHint(), "Self-hint must be strictly prevented");
        assertTrue(List.of("node-1", "node-2", "node-4", "node-5").contains(err.getLeaderHint()));
    }

    @Test
    @DisplayName("Should forward request to leader and return response in FORWARD mode")
    public void testForwardingModeSuccess() throws Exception {
        AtomicInteger forwardCount = new AtomicInteger(0);
        String reqId = UUID.randomUUID().toString();

        LeaderRouter.RequestForwarder mockForwarder = (target, request) -> {
            forwardCount.incrementAndGet();
            assertEquals("node-1", target);
            // Simulate response from leader
            BookTicketResponseDto responseDto = new BookTicketResponseDto(
                    reqId, "CONFIRMED", "TKT-12345", "SE-001", "G01", null, 15L
            );
            return MessageEnvelope.createResponse(request.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, target, responseDto);
        };

        LeaderRouter router = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 4L, "node-1"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                mockForwarder
        );

        MessageEnvelope<JsonNode> req = createBookRequest("client-desktop", reqId);
        LeaderRouter.RoutingOutcome outcome = router.route(req);

        assertTrue(outcome.isHandled());
        assertEquals(1, forwardCount.get(), "Request must be forwarded exactly 1 time");

        MessageEnvelope<?> resp = outcome.getResponse();
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        BookTicketResponseDto confirm = (BookTicketResponseDto) resp.getPayload();
        assertTrue(confirm.isConfirmed());
        assertEquals(reqId, confirm.getRequestId(), "Forwarded response must preserve requestId");
    }

    @Test
    @DisplayName("Should prevent forward loop when request originated from peer cluster voter")
    public void testForwardingLoopPreventionFromPeer() throws Exception {
        AtomicInteger forwardCount = new AtomicInteger(0);
        LeaderRouter.RequestForwarder mockForwarder = (target, request) -> {
            forwardCount.incrementAndGet();
            return null;
        };

        LeaderRouter router = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 4L, "node-1"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                mockForwarder
        );

        // Request comes from peer voter "node-3" (already hopped once)
        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> peerReq = createBookRequest("node-3", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(peerReq);

        assertTrue(outcome.isHandled());
        assertEquals(0, forwardCount.get(), "Must not forward if request comes from peer voter (loop prevention)");

        ErrorResponseDto err = (ErrorResponseDto) outcome.getResponse().getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("Should prevent forward loop when same requestId is forwarded twice")
    public void testForwardingLoopPreventionDuplicateRequestId() throws Exception {
        AtomicInteger forwardCount = new AtomicInteger(0);
        LeaderRouter.RequestForwarder mockForwarder = (target, request) -> {
            forwardCount.incrementAndGet();
            return MessageEnvelope.createResponse(request.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, target,
                    new BookTicketResponseDto(ErrorReplyFactory.extractRequestId(request), "CONFIRMED", "TKT-1", "SE-001", "G01", null, 1L));
        };

        LeaderRouter router = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 2L, "node-1"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                mockForwarder
        );

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> firstReq = createBookRequest("client-1", reqId);
        LeaderRouter.RoutingOutcome outcome1 = router.route(firstReq);
        assertTrue(outcome1.isHandled());
        assertEquals(1, forwardCount.get());

        // Same requestId sent again for forwarding
        MessageEnvelope<JsonNode> secondReq = createBookRequest("client-1", reqId);
        LeaderRouter.RoutingOutcome outcome2 = router.route(secondReq);
        assertTrue(outcome2.isHandled());
        assertEquals(1, forwardCount.get(), "Must not forward duplicate requestId again");

        ErrorResponseDto err = (ErrorResponseDto) outcome2.getResponse().getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("Should fallback to NOT_LEADER error response if forwarding throws exception")
    public void testForwardingFailureFallsBackToNotLeader() throws Exception {
        LeaderRouter.RequestForwarder failingForwarder = (target, request) -> {
            throw new java.io.IOException("Connection refused to " + target);
        };

        LeaderRouter router = new LeaderRouter(
                "node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 3L, "node-1"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
                LeaderRouter.Mode.FORWARD,
                failingForwarder
        );

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createBookRequest("client-1", reqId);

        LeaderRouter.RoutingOutcome outcome = router.route(req);
        assertTrue(outcome.isHandled());

        ErrorResponseDto err = (ErrorResponseDto) outcome.getResponse().getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("Should adapt RoleProvider from BookingService seamlessly")
    public void testRoleProviderFromBookingService() throws Exception {
        FakeBookingService fakeService = new FakeBookingService("node-1");
        fakeService.setLeader(false, "node-3");

        LeaderRouter.RoleProvider provider = LeaderRouter.RoleProvider.fromBookingService(fakeService);
        LeaderRouter.RoleSnapshot snapshot = provider.getRole();

        assertEquals(Role.FOLLOWER, snapshot.role());
        assertEquals("node-3", snapshot.leaderHint());
    }
}
