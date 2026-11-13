package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for ErrorReplyFactory (SPEC-03 §7)")
public class ErrorReplyFactoryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("Should create standard NOT_LEADER error envelope with all 5 fields")
    public void testCreateNotLeader() {
        String corrId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createNotLeader(
                corrId, reqId, "node-2", "node-1", 4L
        );

        assertNotNull(env);
        assertEquals(1, env.getProtocolVersion());
        assertEquals(MessageType.ERROR_RESPONSE, env.getType());
        assertEquals("node-2", env.getSenderId());
        assertEquals(corrId, env.getCorrelationId());
        assertNotNull(env.getMessageId());
        assertTrue(MessageEnvelope.isValidUuid(env.getMessageId()));

        ErrorResponseDto dto = env.getPayload();
        assertNotNull(dto);
        dto.validate(); // Must pass SPEC-03 validation

        assertEquals("NOT_LEADER", dto.getErrorCode());
        assertEquals("node-1", dto.getLeaderHint());
        assertEquals(4L, dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
        assertTrue(dto.getMessage().contains("node-2"));
        assertTrue(dto.getMessage().contains("node-1"));
        assertTrue(dto.getMessage().contains("term 4"));
    }

    @Test
    @DisplayName("Should handle null leaderHint and term in NOT_LEADER without validation failure")
    public void testCreateNotLeaderNulls() {
        String corrId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createNotLeader(
                corrId, reqId, "node-3", null, null
        );

        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("NOT_LEADER", dto.getErrorCode());
        assertNull(dto.getLeaderHint());
        assertNull(dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("Should create INVALID_REQUEST error envelope with null term and leaderHint")
    public void testCreateInvalidRequest() {
        String corrId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createInvalidRequest(
                corrId, reqId, "node-1", "Malformed request schema"
        );

        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("INVALID_REQUEST", dto.getErrorCode());
        assertEquals("Malformed request schema", dto.getMessage());
        assertNull(dto.getLeaderHint());
        assertNull(dto.getTerm());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("Should create NO_QUORUM error envelope preserving term and leaderHint")
    public void testCreateNoQuorum() {
        String corrId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createNoQuorum(
                corrId, reqId, "node-1", "Quorum unreachable", 7L, "node-1"
        );

        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("NO_QUORUM", dto.getErrorCode());
        assertEquals(7L, dto.getTerm());
        assertEquals("node-1", dto.getLeaderHint());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("Should create READ_UNAVAILABLE error envelope with null requestId")
    public void testCreateReadUnavailable() {
        String corrId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createReadUnavailable(
                corrId, "node-2", "Read barrier unavailable", 3L, "node-1"
        );

        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("READ_UNAVAILABLE", dto.getErrorCode());
        assertNull(dto.getRequestId());
        assertEquals(3L, dto.getTerm());
        assertEquals("node-1", dto.getLeaderHint());
    }

    @Test
    @DisplayName("Should create INTERNAL_ERROR error envelope")
    public void testCreateInternalError() {
        String corrId = UUID.randomUUID().toString();
        String reqId = UUID.randomUUID().toString();

        MessageEnvelope<ErrorResponseDto> env = ErrorReplyFactory.createInternalError(
                corrId, reqId, "node-4", "Storage disk full", 2L, null
        );

        ErrorResponseDto dto = env.getPayload();
        dto.validate();
        assertEquals("INTERNAL_ERROR", dto.getErrorCode());
        assertEquals(reqId, dto.getRequestId());
    }

    @Test
    @DisplayName("Should extract requestId from JsonNode payload")
    public void testExtractRequestIdFromJsonNode() throws Exception {
        String reqId = UUID.randomUUID().toString();
        JsonNode json = mapper.readTree("{\"requestId\":\"" + reqId + "\",\"seatId\":\"G01\"}");
        MessageEnvelope<JsonNode> env = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", json);

        String extracted = ErrorReplyFactory.extractRequestId(env);
        assertEquals(reqId, extracted);
    }

    @Test
    @DisplayName("Should extract requestId from typed BookTicketRequestDto")
    public void testExtractRequestIdFromDto() {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto dto = new BookTicketRequestDto(reqId, "SE-001", "G03", "Test Passenger");
        MessageEnvelope<BookTicketRequestDto> env = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", dto);

        String extracted = ErrorReplyFactory.extractRequestId(env);
        assertEquals(reqId, extracted);
    }

    @Test
    @DisplayName("Should return null for non-UUID or missing requestId")
    public void testExtractRequestIdInvalid() throws Exception {
        assertNull(ErrorReplyFactory.extractRequestId(null));

        JsonNode empty = mapper.readTree("{}");
        MessageEnvelope<JsonNode> env1 = MessageEnvelope.createRequest(MessageType.GET_SEATS, "client-1", empty);
        assertNull(ErrorReplyFactory.extractRequestId(env1));

        JsonNode notUuid = mapper.readTree("{\"requestId\":\"invalid-not-uuid\"}");
        MessageEnvelope<JsonNode> env2 = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", notUuid);
        assertNull(ErrorReplyFactory.extractRequestId(env2));
    }
}
