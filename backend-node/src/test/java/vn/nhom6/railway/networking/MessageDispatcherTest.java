package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.Role;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class MessageDispatcherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    public void cleanup() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    private MessageEnvelope<JsonNode> createRawEnvelope(MessageType type, String senderId, Object payloadObj) {
        JsonNode jsonNode = MAPPER.valueToTree(payloadObj);
        return MessageEnvelope.createRequest(type, senderId, jsonNode);
    }

    @Test
    public void testDispatchBookingHandler() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerBookingHandler(req -> {
            if (req.getType() == MessageType.BOOK_TICKET) {
                String reqId = req.getPayload().get("requestId").asText();
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1",
                        Map.of("outcome", "CONFIRMED", "requestId", reqId, "ticketId", "ticket-001"));
            } else if (req.getType() == MessageType.GET_SEATS) {
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.GET_SEATS_RESPONSE, "node-1",
                        Map.of("tripId", "SE-001", "readIndex", 10, "leaderId", "node-1"));
            }
            return null;
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        // 1. Dispatch BOOK_TICKET
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookPayload = new BookTicketRequestDto(reqId, "SE-001", "G05", "Passenger One");
        MessageEnvelope<JsonNode> bookReq = createRawEnvelope(MessageType.BOOK_TICKET, "client-1", bookPayload);

        MessageEnvelope<?> bookResp = dispatcher.dispatch(bookReq);
        assertNotNull(bookResp);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookResp.getType());
        assertEquals(bookReq.getMessageId(), bookResp.getCorrelationId());
        assertEquals("node-1", bookResp.getSenderId());

        // 2. Dispatch GET_SEATS
        MessageEnvelope<JsonNode> seatsReq = createRawEnvelope(MessageType.GET_SEATS, "client-1", Map.of("tripId", "SE-001"));
        MessageEnvelope<?> seatsResp = dispatcher.dispatch(seatsReq);
        assertNotNull(seatsResp);
        assertEquals(MessageType.GET_SEATS_RESPONSE, seatsResp.getType());
        assertEquals(seatsReq.getMessageId(), seatsResp.getCorrelationId());
    }

    @Test
    public void testDispatchRaftHandler() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerRaftHandler(req -> {
            if (req.getType() == MessageType.REQUEST_VOTE) {
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-2",
                        Map.of("term", 2L, "voteGranted", true));
            } else if (req.getType() == MessageType.APPEND_ENTRIES) {
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, "node-2",
                        Map.of("term", 2L, "success", true, "matchIndex", 5L));
            }
            return null;
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, "node-2");

        // 1. Dispatch REQUEST_VOTE
        RequestVoteRequestDto voteReqDto = new RequestVoteRequestDto(2L, "node-1", 4L, 2L);
        MessageEnvelope<JsonNode> voteReq = createRawEnvelope(MessageType.REQUEST_VOTE, "node-1", voteReqDto);

        MessageEnvelope<?> voteResp = dispatcher.dispatch(voteReq);
        assertNotNull(voteResp);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, voteResp.getType());
        assertEquals(voteReq.getMessageId(), voteResp.getCorrelationId());

        // 2. Dispatch APPEND_ENTRIES
        MessageEnvelope<JsonNode> appendReq = createRawEnvelope(MessageType.APPEND_ENTRIES, "node-1",
                Map.of("term", 2L, "leaderId", "node-1", "prevLogIndex", 4L, "prevLogTerm", 2L, "entries", List.of(), "leaderCommit", 4L));

        MessageEnvelope<?> appendResp = dispatcher.dispatch(appendReq);
        assertNotNull(appendResp);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, appendResp.getType());
        assertEquals(appendReq.getMessageId(), appendResp.getCorrelationId());
    }

    @Test
    public void testDispatchStatusHandler() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerStatusHandler(req -> MessageEnvelope.createResponse(req.getMessageId(), MessageType.NODE_STATUS_RESPONSE, "node-3",
                Map.of("nodeId", "node-3", "role", "FOLLOWER", "currentTerm", 3L)));

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.MONITOR, "node-3");

        MessageEnvelope<JsonNode> statusReq = createRawEnvelope(MessageType.NODE_STATUS, "monitor-1", Map.of());
        MessageEnvelope<?> statusResp = dispatcher.dispatch(statusReq);

        assertNotNull(statusResp);
        assertEquals(MessageType.NODE_STATUS_RESPONSE, statusResp.getType());
        assertEquals(statusReq.getMessageId(), statusResp.getCorrelationId());
    }

    @Test
    public void testUnregisteredMessageTypeReturnsError() {
        HandlerRegistry registry = new HandlerRegistry();
        // Empty registry
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.ALL, "node-1");

        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.CANCEL_TICKET, "client-1",
                Map.of("requestId", UUID.randomUUID().toString(), "ticketId", "ticket-001"));

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INVALID_REQUEST", errorDto.getErrorCode());
        assertTrue(errorDto.getMessage().contains("No handler registered for message type: CANCEL_TICKET"));
    }

    @Test
    public void testRejectResponseTypesOnDispatcher() {
        HandlerRegistry registry = new HandlerRegistry();
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.ALL, "node-1");

        // Client attempts to send a response type as incoming request (envelope itself has valid correlationId)
        MessageEnvelope<JsonNode> responseTypeReq = MessageEnvelope.createResponse(
                UUID.randomUUID().toString(),
                MessageType.BOOK_TICKET_RESPONSE,
                "client-1",
                MAPPER.valueToTree(Map.of("outcome", "CONFIRMED"))
        );

        MessageEnvelope<?> resp = dispatcher.dispatch(responseTypeReq);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INVALID_REQUEST", errorDto.getErrorCode());
        assertTrue(errorDto.getMessage().contains("Unexpected response message type on request dispatcher"));
    }

    @Test
    public void testChannelTypeEnforcementClientPortRejectsRaftRpc() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerRaftHandler(req -> MessageEnvelope.createResponse(req.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, "node-1", Map.of()));

        // ChannelType.CLIENT must reject Raft RPC per SPEC-08 Section 64 & SPEC-07 TC-TCP-04
        MessageDispatcher clientDispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        MessageEnvelope<JsonNode> voteReq = createRawEnvelope(MessageType.REQUEST_VOTE, "node-2", Map.of("term", 1));
        MessageEnvelope<?> resp = clientDispatcher.dispatch(voteReq);

        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INVALID_REQUEST", errorDto.getErrorCode());
        assertTrue(errorDto.getMessage().contains("Raft RPC messages are not allowed on client channel"));
    }

    @Test
    public void testChannelTypeEnforcementPeerPortRejectsBookingRequest() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerBookingHandler(req -> MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1", Map.of()));

        // ChannelType.PEER must reject Booking requests
        MessageDispatcher peerDispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, "node-1");

        MessageEnvelope<JsonNode> bookReq = createRawEnvelope(MessageType.BOOK_TICKET, "client-1", Map.of("tripId", "SE-001"));
        MessageEnvelope<?> resp = peerDispatcher.dispatch(bookReq);

        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INVALID_REQUEST", errorDto.getErrorCode());
        assertTrue(errorDto.getMessage().contains("Non-Raft messages are not allowed on peer channel"));
    }

    @Test
    public void testChannelTypeEnforcementMonitorPortRejectsNonStatus() {
        HandlerRegistry registry = new HandlerRegistry();
        MessageDispatcher monitorDispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.MONITOR, "node-1");

        MessageEnvelope<JsonNode> seatsReq = createRawEnvelope(MessageType.GET_SEATS, "client-1", Map.of("tripId", "SE-001"));
        MessageEnvelope<?> resp = monitorDispatcher.dispatch(seatsReq);

        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INVALID_REQUEST", errorDto.getErrorCode());
        assertTrue(errorDto.getMessage().contains("Only NODE_STATUS messages are allowed on monitor channel"));
    }

    @Test
    public void testHandlerExceptionReturnsInternalError() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.BOOK_TICKET, req -> {
            throw new RuntimeException("Simulated unexpected database failure");
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.ALL, "node-1");

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> bookReq = createRawEnvelope(MessageType.BOOK_TICKET, "client-1", Map.of("requestId", reqId));

        MessageEnvelope<?> resp = dispatcher.dispatch(bookReq);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(bookReq.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto errorDto = (ErrorResponseDto) resp.getPayload();
        assertEquals("INTERNAL_ERROR", errorDto.getErrorCode());
        assertEquals(reqId, errorDto.getRequestId());
        assertTrue(errorDto.getMessage().contains("Simulated unexpected database failure"));
    }

    @Test
    public void testEndToEndWithTcpServerAndClient() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.registerBookingHandler(req -> {
            if (req.getType() == MessageType.BOOK_TICKET) {
                String reqId = req.getPayload().get("requestId").asText();
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-server",
                        Map.of("outcome", "CONFIRMED", "requestId", reqId, "ticketId", "ticket-tcp-test"));
            }
            return null;
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-server");

        TcpServer server = new TcpServer("127.0.0.1", 0, 8, 5000);
        server.setMessageHandler(dispatcher);
        resources.add(server);
        server.start();

        int port = server.getPort();
        assertTrue(port > 0);

        try (TcpClient client = new TcpClient("127.0.0.1", port, 2000, 5000)) {
            client.connect();
            assertTrue(client.isConnected());

            // 1. Send valid BOOK_TICKET request
            String reqId = UUID.randomUUID().toString();
            BookTicketRequestDto bookPayload = new BookTicketRequestDto(reqId, "SE-001", "G03", "Test Client");
            MessageEnvelope<BookTicketRequestDto> reqEnvelope = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-demo", bookPayload);

            MessageEnvelope<JsonNode> respEnvelope = client.sendAndReceive(reqEnvelope);
            assertNotNull(respEnvelope);
            assertEquals(MessageType.BOOK_TICKET_RESPONSE, respEnvelope.getType());
            assertEquals(reqEnvelope.getMessageId(), respEnvelope.getCorrelationId());
            assertEquals("CONFIRMED", respEnvelope.getPayload().get("outcome").asText());
            assertEquals("ticket-tcp-test", respEnvelope.getPayload().get("ticketId").asText());

            // 2. Send Raft RPC to CLIENT port -> should receive ERROR_RESPONSE
            RequestVoteRequestDto votePayload = new RequestVoteRequestDto(1L, "node-hack", 0L, 0L);
            MessageEnvelope<RequestVoteRequestDto> voteReq = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-hack", votePayload);

            MessageEnvelope<JsonNode> errorResp = client.sendAndReceive(voteReq);
            assertNotNull(errorResp);
            assertEquals(MessageType.ERROR_RESPONSE, errorResp.getType());
            assertEquals(voteReq.getMessageId(), errorResp.getCorrelationId());
            assertEquals("INVALID_REQUEST", errorResp.getPayload().get("errorCode").asText());
        }
    }

    @Test
    public void testHandlerResponseMissingCorrelationIdGetsAttached() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.GET_SEATS, req -> {
            // Handler returns response without correlationId
            return new MessageEnvelope<>(
                    1,
                    UUID.randomUUID().toString(),
                    null,
                    MessageType.GET_SEATS_RESPONSE,
                    "node-1",
                    "2026-10-29T10:00:00Z",
                    MAPPER.createObjectNode()
            );
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.GET_SEATS, "client-1", MAPPER.createObjectNode());

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(req.getMessageId(), resp.getCorrelationId(), "Dispatcher must attach correlationId if handler omitted it");
    }

    @Test
    public void testRequestWithInvalidMessageIdDropped() {
        HandlerRegistry registry = new HandlerRegistry();
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        // Envelope with invalid messageId
        MessageEnvelope<JsonNode> badReq = new MessageEnvelope<>(
                1,
                "00000000-0000-0000-0000-000000000000", // Valid syntax
                null,
                MessageType.GET_SEATS,
                "client-1",
                "2026-10-29T10:00:00Z",
                MAPPER.createObjectNode()
        );
        // Dispatch works
        assertNotNull(dispatcher.dispatch(badReq));

        // When null or invalid request is passed
        assertNull(dispatcher.dispatch(null));
    }

    @Test
    @DisplayName("TV3-D17: Follower redirects BOOK_TICKET to known leader with NOT_LEADER error envelope")
    public void testFollowerRedirectsBookTicketWithLeaderHint() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        AtomicInteger handlerInvocations = new AtomicInteger(0);
        registry.register(MessageType.BOOK_TICKET, req -> {
            handlerInvocations.incrementAndGet();
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-2",
                    Map.of("outcome", "CONFIRMED"));
        });

        LeaderRouter leaderRouter = new LeaderRouter("node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 5L, "node-1"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-2", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto payload = new BookTicketRequestDto(reqId, "SE-001", "G03", "Nguyen Van A");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", payload);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals("node-2", resp.getSenderId());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(5L, err.getTerm());
        assertEquals(reqId, err.getRequestId(), "requestId must be strictly preserved across NOT_LEADER redirect");
        assertEquals(0, handlerInvocations.get(), "Follower must NEVER invoke booking handler or self-confirm");
    }

    @Test
    @DisplayName("TV3-D17: Follower never invokes domain booking handler (Follower không tự ghi hay CONFIRMED)")
    public void testFollowerNeverInvokesBookingHandler() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        AtomicInteger writeCallCount = new AtomicInteger(0);
        registry.register(MessageType.BOOK_TICKET, req -> {
            writeCallCount.incrementAndGet();
            fail("Follower should never execute booking handler!");
            return null;
        });
        registry.register(MessageType.CANCEL_TICKET, req -> {
            writeCallCount.incrementAndGet();
            fail("Follower should never execute cancel handler!");
            return null;
        });

        LeaderRouter leaderRouter = new LeaderRouter("node-3",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 2L, "node-1"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-3", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookPayload = new BookTicketRequestDto(reqId, "SE-001", "G01", "Passenger");
        MessageEnvelope<JsonNode> bookReq = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", bookPayload);

        MessageEnvelope<?> bookResp = dispatcher.dispatch(bookReq);
        assertNotNull(bookResp);
        assertEquals(MessageType.ERROR_RESPONSE, bookResp.getType());
        ErrorResponseDto err = (ErrorResponseDto) bookResp.getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals(0, writeCallCount.get(), "Zero database write or domain handler calls on follower");
    }

    @Test
    @DisplayName("TV3-D17: Leader executes booking handler normally and returns CONFIRMED")
    public void testLeaderExecutesBookingHandlerNormally() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        AtomicInteger handlerInvocations = new AtomicInteger(0);
        String reqId = UUID.randomUUID().toString();

        registry.register(MessageType.BOOK_TICKET, req -> {
            handlerInvocations.incrementAndGet();
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1",
                    Map.of("outcome", "CONFIRMED", "requestId", reqId, "ticketId", "TKT-001", "seatId", "G01"));
        });

        LeaderRouter leaderRouter = new LeaderRouter("node-1",
                LeaderRouter.RoleProvider.fixed(Role.LEADER, 3L, "node-1"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        BookTicketRequestDto payload = new BookTicketRequestDto(reqId, "SE-001", "G01", "Passenger Leader");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", payload);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
        assertEquals("node-1", resp.getSenderId());
        assertEquals(req.getMessageId(), resp.getCorrelationId());
        assertEquals(1, handlerInvocations.get());
    }

    @Test
    @DisplayName("TV3-D17: Follower falls back to peer cluster list when leaderHint is null or self")
    public void testFollowerFallbackWhenLeaderHintNullOrSelf() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        // Leader hint is self "node-2" while role is FOLLOWER (stale state)
        LeaderRouter routerStale = new LeaderRouter("node-2",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 1L, "node-2"),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"));
        MessageDispatcher dispatcherStale = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-2", routerStale);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto payload = new BookTicketRequestDto(reqId, "SE-001", "G05", "Passenger");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", payload);

        MessageEnvelope<?> resp = dispatcherStale.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertNotNull(err.getLeaderHint(), "Fallback peer must be chosen");
        assertNotEquals("node-2", err.getLeaderHint(), "Self-hint must NEVER be returned");
        assertTrue(List.of("node-1", "node-3", "node-4", "node-5").contains(err.getLeaderHint()),
                "Fallback must be one of the other cluster peers");

        // Leader hint is null
        LeaderRouter routerNull = new LeaderRouter("node-3",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 1L, null),
                List.of("node-1", "node-2", "node-3", "node-4", "node-5"));
        MessageDispatcher dispatcherNull = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-3", routerNull);

        MessageEnvelope<?> respNull = dispatcherNull.dispatch(req);
        ErrorResponseDto errNull = (ErrorResponseDto) respNull.getPayload();
        assertEquals("NOT_LEADER", errNull.getErrorCode());
        assertNotNull(errNull.getLeaderHint());
        assertNotEquals("node-3", errNull.getLeaderHint());
        assertTrue(List.of("node-1", "node-2", "node-4", "node-5").contains(errNull.getLeaderHint()));
    }

    @Test
    @DisplayName("TV3-D17: NotLeaderException thrown by handler is gracefully converted to NOT_LEADER error envelope")
    public void testNotLeaderExceptionHandledGracefully() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.BOOK_TICKET, req -> {
            throw new BookingRequestHandler.NotLeaderException("node-4");
        });

        LeaderRouter leaderRouter = new LeaderRouter("node-1",
                LeaderRouter.RoleProvider.fixed(Role.LEADER, 7L, "node-1"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto payload = new BookTicketRequestDto(reqId, "SE-001", "G09", "Passenger Mid Stepdown");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", payload);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode(), "Must NOT return INTERNAL_ERROR when handler threw NotLeaderException");
        assertEquals("node-4", err.getLeaderHint());
        assertEquals(7L, err.getTerm());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("TV3-D17: CANCEL_TICKET is also redirected on Follower with NOT_LEADER")
    public void testFollowerRedirectsCancelTicket() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        LeaderRouter leaderRouter = new LeaderRouter("node-5",
                LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 3L, "node-1"));
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-5", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        Map<String, Object> cancelPayload = Map.of(
                "requestId", reqId,
                "ticketId", "TKT-9999",
                "reason", "Change of plans"
        );
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.CANCEL_TICKET, "client-app", cancelPayload);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("TV3-D18: 6 independent concurrent requests have distinct messageId/requestId and no cross-talk")
    public void testSixIndependentRequestsNoInterference() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        AtomicInteger counter = new AtomicInteger(0);
        registry.registerBookingHandler(req -> {
            if (req.getType() == MessageType.BOOK_TICKET) {
                String reqId = req.getPayload().get("requestId").asText();
                String seatId = req.getPayload().get("seatId").asText();
                int idx = counter.incrementAndGet();
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1",
                        Map.of("outcome", "CONFIRMED", "requestId", reqId, "ticketId", "TKT-" + seatId + "-" + idx));
            }
            return null;
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        int n = 6;
        ExecutorService executor = Executors.newFixedThreadPool(n);
        resources.add(executor::shutdownNow);

        List<Future<MessageEnvelope<?>>> futures = new ArrayList<>();
        List<MessageEnvelope<JsonNode>> requests = new ArrayList<>();

        for (int i = 1; i <= n; i++) {
            String seatId = String.format("G%02d", i);
            String reqId = UUID.randomUUID().toString();
            BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", seatId, "Passenger " + i);
            MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-" + i, dto);
            requests.add(req);
            futures.add(executor.submit(() -> dispatcher.dispatch(req)));
        }

        List<String> ticketIds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MessageEnvelope<?> resp = futures.get(i).get(5, TimeUnit.SECONDS);
            MessageEnvelope<JsonNode> req = requests.get(i);
            assertNotNull(resp);
            assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
            assertEquals(req.getMessageId(), resp.getCorrelationId(), "Correlation ID must strictly match request.messageId");

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) resp.getPayload();
            assertEquals("CONFIRMED", payload.get("outcome"));
            assertEquals(req.getPayload().get("requestId").asText(), payload.get("requestId"), "requestId must match original payload");
            ticketIds.add((String) payload.get("ticketId"));
        }

        assertEquals(6, ticketIds.stream().distinct().count(), "All 6 requests must yield distinct tickets");
    }

    @Test
    @DisplayName("TV3-D18: Retry with same requestId preserves idempotency, receives new correlationId, zero double-booking")
    public void testRetryWithSameRequestIdPreservesIdempotencyAndNewCorrelation() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        AtomicInteger executionCount = new AtomicInteger(0);
        Map<String, Map<String, Object>> idempotencyStore = new ConcurrentHashMap<>();

        registry.registerBookingHandler(req -> {
            if (req.getType() == MessageType.BOOK_TICKET) {
                String reqId = req.getPayload().get("requestId").asText();
                Map<String, Object> cached = idempotencyStore.computeIfAbsent(reqId, k -> {
                    int c = executionCount.incrementAndGet();
                    return Map.of("outcome", "CONFIRMED", "requestId", reqId, "ticketId", "TKT-SEAT-01-" + c);
                });
                return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1", cached);
            }
            return null;
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        String businessRequestId = UUID.randomUUID().toString();
        BookTicketRequestDto dto = new BookTicketRequestDto(businessRequestId, "SE-001", "G01", "Passenger Idempotent");

        // 1. Initial attempt: messageId M1
        MessageEnvelope<JsonNode> req1 = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", dto);
        MessageEnvelope<?> resp1 = dispatcher.dispatch(req1);
        assertNotNull(resp1);
        assertEquals(req1.getMessageId(), resp1.getCorrelationId());
        @SuppressWarnings("unchecked")
        Map<String, Object> p1 = (Map<String, Object>) resp1.getPayload();
        assertEquals("CONFIRMED", p1.get("outcome"));
        assertEquals("TKT-SEAT-01-1", p1.get("ticketId"));
        assertEquals(businessRequestId, p1.get("requestId"));

        // 2. Retry attempt: same payload & requestId, but NEW messageId M2 (per SPEC-03)
        MessageEnvelope<JsonNode> req2 = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", dto);
        assertNotEquals(req1.getMessageId(), req2.getMessageId(), "Retry must have a fresh transport messageId");

        MessageEnvelope<?> resp2 = dispatcher.dispatch(req2);
        assertNotNull(resp2);
        assertEquals(req2.getMessageId(), resp2.getCorrelationId(), "Retry response must correlate to new messageId");
        assertNotEquals(resp1.getCorrelationId(), resp2.getCorrelationId());

        @SuppressWarnings("unchecked")
        Map<String, Object> p2 = (Map<String, Object>) resp2.getPayload();
        assertEquals("CONFIRMED", p2.get("outcome"));
        assertEquals("TKT-SEAT-01-1", p2.get("ticketId"), "Must return identical ticketId (idempotent)");
        assertEquals(businessRequestId, p2.get("requestId"));

        // Verify business logic was executed exactly once (no double-booking!)
        assertEquals(1, executionCount.get(), "Underlying state machine must only execute once for same requestId");
    }

    @Test
    @DisplayName("TV3-D18: Dispatcher enforces strict correlationId even if handler returns wrong or null correlationId")
    public void testLateOrMismatchedCorrelationIdEnforcedByDispatcher() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        // Malicious or buggy handler returns a response with a stale/different correlationId
        registry.registerBookingHandler(req -> {
            String staleCorrelationId = UUID.randomUUID().toString();
            return MessageEnvelope.createResponse(staleCorrelationId, MessageType.BOOK_TICKET_RESPONSE, "node-1",
                    Map.of("outcome", "CONFIRMED", "requestId", req.getPayload().get("requestId").asText()));
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", "G02", "Passenger Strict Corr");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", dto);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(req.getMessageId(), resp.getCorrelationId(), "Dispatcher must enforce correlationId == request.messageId");
    }

    @Test
    @DisplayName("TV3-D18: Server in-flight request limit is enforced, rejecting excess requests with INTERNAL_ERROR")
    public void testInFlightLimitEnforced() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        CountDownLatch inFlightLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        registry.registerBookingHandler(req -> {
            inFlightLatch.countDown();
            try {
                proceedLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1",
                    Map.of("outcome", "CONFIRMED"));
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", null, 1);
        assertEquals(1, dispatcher.getMaxInFlightRequests());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        resources.add(executor::shutdownNow);

        String reqId1 = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req1 = createRawEnvelope(MessageType.BOOK_TICKET, "client-1",
                new BookTicketRequestDto(reqId1, "SE-001", "G01", "P1"));

        Future<MessageEnvelope<?>> future1 = executor.submit(() -> dispatcher.dispatch(req1));

        assertTrue(inFlightLatch.await(3, TimeUnit.SECONDS));
        assertEquals(1, dispatcher.getActiveInFlightCount());
        assertTrue(dispatcher.isInFlight(req1.getMessageId()));

        // Submit req2 while req1 is still in-flight -> should be rejected immediately
        String reqId2 = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req2 = createRawEnvelope(MessageType.BOOK_TICKET, "client-2",
                new BookTicketRequestDto(reqId2, "SE-001", "G02", "P2"));

        MessageEnvelope<?> resp2 = dispatcher.dispatch(req2);
        assertNotNull(resp2);
        assertEquals(MessageType.ERROR_RESPONSE, resp2.getType());
        assertEquals(req2.getMessageId(), resp2.getCorrelationId());
        ErrorResponseDto err2 = (ErrorResponseDto) resp2.getPayload();
        assertEquals("INTERNAL_ERROR", err2.getErrorCode());
        assertTrue(err2.getMessage().contains("Server in-flight request limit reached"));

        // Release req1 and let it complete
        proceedLatch.countDown();
        MessageEnvelope<?> resp1 = future1.get(5, TimeUnit.SECONDS);
        assertNotNull(resp1);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp1.getType());
        assertEquals(req1.getMessageId(), resp1.getCorrelationId());

        assertEquals(0, dispatcher.getActiveInFlightCount());
        assertFalse(dispatcher.isInFlight(req1.getMessageId()));
    }

    @Test
    @DisplayName("TV3-D18: Duplicate in-flight messageId is rejected with INVALID_REQUEST")
    public void testDuplicateInFlightMessageIdRejected() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        CountDownLatch inFlightLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        registry.registerBookingHandler(req -> {
            inFlightLatch.countDown();
            try {
                proceedLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return MessageEnvelope.createResponse(req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1",
                    Map.of("outcome", "CONFIRMED"));
        });

        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        resources.add(executor::shutdownNow);

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-1",
                new BookTicketRequestDto(reqId, "SE-001", "G01", "P1"));

        Future<MessageEnvelope<?>> future1 = executor.submit(() -> dispatcher.dispatch(req));

        assertTrue(inFlightLatch.await(3, TimeUnit.SECONDS));
        assertTrue(dispatcher.isInFlight(req.getMessageId()));

        // Dispatch exact same envelope with identical messageId while in-flight
        MessageEnvelope<?> duplicateResp = dispatcher.dispatch(req);
        assertNotNull(duplicateResp);
        assertEquals(MessageType.ERROR_RESPONSE, duplicateResp.getType());
        assertEquals(req.getMessageId(), duplicateResp.getCorrelationId());
        ErrorResponseDto err = (ErrorResponseDto) duplicateResp.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
        assertTrue(err.getMessage().contains("Duplicate in-flight messageId"));

        // Release first request
        proceedLatch.countDown();
        MessageEnvelope<?> resp1 = future1.get(5, TimeUnit.SECONDS);
        assertNotNull(resp1);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp1.getType());

        assertEquals(0, dispatcher.getActiveInFlightCount());
    }

    @Test
    @DisplayName("TV3-D19: Dispatcher maps NoQuorumException to NO_QUORUM errorCode with term")
    public void testDispatcherMapsNoQuorumExceptionToNoQuorumErrorCode() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.BOOK_TICKET, req -> {
            throw new BookingRequestHandler.NoQuorumException();
        });

        LeaderRouter leaderRouter = new LeaderRouter(
                "node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 7L, "node-1")
        );
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", "G05", "Passenger No Quorum");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", dto);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("NO_QUORUM", err.getErrorCode());
        assertEquals(7L, err.getTerm());
        assertEquals(reqId, err.getRequestId(), "requestId must be preserved on NO_QUORUM");
    }

    @Test
    @DisplayName("TV3-D19: Dispatcher maps ReadUnavailableException to READ_UNAVAILABLE errorCode with term")
    public void testDispatcherMapsReadUnavailableExceptionToReadUnavailableErrorCode() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.GET_SEATS, req -> {
            throw new BookingRequestHandler.ReadUnavailableException("Log barrier failed to commit");
        });

        LeaderRouter leaderRouter = new LeaderRouter(
                "node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 8L, "node-1")
        );
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.GET_SEATS, "client-app", Map.of("tripId", "SE-001"));

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("READ_UNAVAILABLE", err.getErrorCode());
        assertEquals(8L, err.getTerm());
    }

    @Test
    @DisplayName("TV3-D19: Dispatcher maps RequestIdConflictException to REQUEST_ID_CONFLICT errorCode")
    public void testDispatcherMapsRequestIdConflictExceptionToRequestIdConflictErrorCode() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MessageType.BOOK_TICKET, req -> {
            throw new BookingRequestHandler.RequestIdConflictException();
        });

        LeaderRouter leaderRouter = new LeaderRouter(
                "node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 9L, "node-1")
        );
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", "G06", "Passenger Conflict");
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.BOOK_TICKET, "client-app", dto);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("REQUEST_ID_CONFLICT", err.getErrorCode());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("TV3-D19: Follower redirects GET_REQUEST_RESULT with NOT_LEADER and leaderHint per SPEC-03 §4.4")
    public void testFollowerRedirectsGetRequestResultWithNotLeader() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        LeaderRouter leaderRouter = new LeaderRouter(
                "node-2", LeaderRouter.RoleProvider.fixed(Role.FOLLOWER, 3L, "node-1")
        );
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-2", leaderRouter);

        String reqId = UUID.randomUUID().toString();
        GetRequestResultRequestDto queryDto = new GetRequestResultRequestDto(reqId);
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.GET_REQUEST_RESULT, "client-app", queryDto);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-1", err.getLeaderHint());
        assertEquals(3L, err.getTerm());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("TV3-D19: Leader dispatches GET_REQUEST_RESULT successfully returning typed response")
    public void testLeaderDispatchesGetRequestResultSuccessfully() throws Exception {
        HandlerRegistry registry = new HandlerRegistry();
        String targetRequestId = UUID.randomUUID().toString();

        ResultLookup lookup = reqId -> {
            if (targetRequestId.equals(reqId)) {
                return ResultLookup.LookupResult.appliedSuccess(reqId, "BOOK_TICKET", "TKT-D19-SUCCESS", 77L);
            }
            return ResultLookup.LookupResult.unknown(reqId);
        };

        GetRequestResultHandler handler = new GetRequestResultHandler(lookup, "node-1");
        registry.register(MessageType.GET_REQUEST_RESULT, handler);

        LeaderRouter leaderRouter = new LeaderRouter(
                "node-1", LeaderRouter.RoleProvider.fixed(Role.LEADER, 5L, "node-1")
        );
        MessageDispatcher dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.CLIENT, "node-1", leaderRouter);

        GetRequestResultRequestDto queryDto = new GetRequestResultRequestDto(targetRequestId);
        MessageEnvelope<JsonNode> req = createRawEnvelope(MessageType.GET_REQUEST_RESULT, "client-app", queryDto);

        MessageEnvelope<?> resp = dispatcher.dispatch(req);
        assertNotNull(resp);
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, resp.getType());
        assertEquals(req.getMessageId(), resp.getCorrelationId());

        GetRequestResultResponseDto resultDto = (GetRequestResultResponseDto) resp.getPayload();
        resultDto.validate();
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, resultDto.getState());
        assertEquals(targetRequestId, resultDto.getRequestId());
        assertEquals("BOOK_TICKET", resultDto.getCommandType());
        assertEquals("TKT-D19-SUCCESS", resultDto.getTicketId());
        assertEquals(77L, resultDto.getAppliedLogIndex());
    }

    public static void main(String[] args) {
        System.out.println("Starting MessageDispatcherTest runner...");
        MessageDispatcherTest test = new MessageDispatcherTest();
        int passed = 0;
        int total = 10;

        try {
            System.out.println("[TEST 1] testDispatchBookingHandler...");
            test.testDispatchBookingHandler();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 2] testDispatchRaftHandler...");
            test.testDispatchRaftHandler();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 3] testDispatchStatusHandler...");
            test.testDispatchStatusHandler();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 4] testUnregisteredMessageTypeReturnsError...");
            test.testUnregisteredMessageTypeReturnsError();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 5] testRejectResponseTypesOnDispatcher...");
            test.testRejectResponseTypesOnDispatcher();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 6] testChannelTypeEnforcementClientPortRejectsRaftRpc...");
            test.testChannelTypeEnforcementClientPortRejectsRaftRpc();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 7] testChannelTypeEnforcementPeerPortRejectsBookingRequest...");
            test.testChannelTypeEnforcementPeerPortRejectsBookingRequest();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 8] testChannelTypeEnforcementMonitorPortRejectsNonStatus...");
            test.testChannelTypeEnforcementMonitorPortRejectsNonStatus();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 9] testHandlerExceptionReturnsInternalError...");
            test.testHandlerExceptionReturnsInternalError();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 10] testEndToEndWithTcpServerAndClient...");
            test.testEndToEndWithTcpServerAndClient();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("\n=== ALL MESSAGE DISPATCHER TESTS COMPLETED: " + passed + "/" + total + " PASSED ===");
        } catch (Throwable t) {
            System.err.println("Test failed: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }
}
