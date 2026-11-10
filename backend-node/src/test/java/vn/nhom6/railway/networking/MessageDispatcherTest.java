package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
