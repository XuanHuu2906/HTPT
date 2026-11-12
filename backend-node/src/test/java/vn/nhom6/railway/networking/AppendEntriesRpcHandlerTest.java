package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AppendEntriesRpcHandlerTest {

    private FakeRaftRequestHandler fakeProcessor;
    private AppendEntriesRpcHandler handler;
    private final String localNodeId = "node-1";

    @BeforeEach
    void setUp() {
        fakeProcessor = new FakeRaftRequestHandler(2L);
        handler = new AppendEntriesRpcHandler(fakeProcessor, localNodeId);
    }

    @Test
    @DisplayName("Should successfully handle valid heartbeat (empty entries list)")
    void testHandleHeartbeatSuccess() throws Exception {
        fakeProcessor.setAppendSuccess(true);
        fakeProcessor.setCurrentTerm(2L);

        AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(
                2L, "node-2", 15L, 2L, 15L);
        MessageEnvelope<AppendEntriesRequestDto> request = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, "node-2", heartbeat);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());
        assertEquals(localNodeId, response.getSenderId());

        AppendEntriesResponseDto respPayload = (AppendEntriesResponseDto) response.getPayload();
        assertTrue(respPayload.isSuccess());
        assertEquals(2L, respPayload.getTerm());
        assertEquals(15L, respPayload.getMatchIndex());

        assertEquals(1, fakeProcessor.getAppendInvocations().size());
        assertEquals("node-2", fakeProcessor.getAppendInvocations().get(0).senderId());
    }

    @Test
    @DisplayName("Should successfully handle append entries containing log entries and advance matchIndex")
    void testHandleAppendEntriesWithLogsSuccess() throws Exception {
        fakeProcessor.setAppendSuccess(true);
        fakeProcessor.setCurrentTerm(3L);

        AppendEntriesRequestDto.LogEntryDto entry1 = new AppendEntriesRequestDto.LogEntryDto(
                16L, 3L, "BOOK_TICKET", Map.of("requestId", "req-1"));
        AppendEntriesRequestDto.LogEntryDto entry2 = new AppendEntriesRequestDto.LogEntryDto(
                17L, 3L, "BOOK_TICKET", Map.of("requestId", "req-2"));

        AppendEntriesRequestDto appendReq = new AppendEntriesRequestDto(
                3L, "node-2", 15L, 2L, List.of(entry1, entry2), 15L);
        MessageEnvelope<AppendEntriesRequestDto> request = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, "node-2", appendReq);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        AppendEntriesResponseDto respPayload = (AppendEntriesResponseDto) response.getPayload();
        assertTrue(respPayload.isSuccess());
        assertEquals(3L, respPayload.getTerm());
        assertEquals(17L, respPayload.getMatchIndex()); // 15 + 2 = 17
    }

    @Test
    @DisplayName("Should handle log mismatch rejection from processor")
    void testHandleLogMismatchRejection() throws Exception {
        fakeProcessor.setAppendSuccess(false);
        fakeProcessor.setCurrentTerm(2L);

        AppendEntriesRequestDto appendReq = AppendEntriesRequestDto.createHeartbeat(
                2L, "node-2", 50L, 2L, 50L);
        MessageEnvelope<AppendEntriesRequestDto> request = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, "node-2", appendReq);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        AppendEntriesResponseDto respPayload = (AppendEntriesResponseDto) response.getPayload();
        assertFalse(respPayload.isSuccess());
        assertEquals(2L, respPayload.getTerm());
        assertEquals(0L, respPayload.getMatchIndex());
    }

    @Test
    @DisplayName("Should reject request when senderId does not match leaderId")
    void testRejectMismatchedSender() throws Exception {
        AppendEntriesRequestDto appendReq = AppendEntriesRequestDto.createHeartbeat(
                2L, "node-3", 10L, 2L, 10L);
        // senderId is node-2, leaderId is node-3
        MessageEnvelope<AppendEntriesRequestDto> request = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, "node-2", appendReq);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
        assertTrue(err.getMessage().contains("does not match leaderId"));
    }

    @Test
    @DisplayName("Should return INVALID_REQUEST when payload violates validation")
    void testRejectInvalidPayload() throws Exception {
        // Negative term
        String invalidJson = "{\"protocolVersion\":1,\"messageId\":\"" + UUID.randomUUID() + "\",\"correlationId\":null,\"type\":\"APPEND_ENTRIES\",\"senderId\":\"node-2\",\"sentAt\":\"2026-11-12T10:00:00Z\",\"payload\":{\"term\":-1,\"leaderId\":\"node-2\",\"prevLogIndex\":0,\"prevLogTerm\":0,\"entries\":[],\"leaderCommit\":0}}";
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(invalidJson);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
    }

    @Test
    @DisplayName("Should return INTERNAL_ERROR when processor throws unexpected exception")
    void testHandleProcessorFailure() throws Exception {
        fakeProcessor.setFailWithException(true, "Raft log persistence error");

        AppendEntriesRequestDto appendReq = AppendEntriesRequestDto.createHeartbeat(
                2L, "node-2", 10L, 2L, 10L);
        MessageEnvelope<AppendEntriesRequestDto> request = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, "node-2", appendReq);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INTERNAL_ERROR", err.getErrorCode());
        assertTrue(err.getMessage().contains("Raft log persistence error"));
    }

    @Test
    @DisplayName("Should return null for null request")
    void testNullOrEmptyEnvelope() throws Exception {
        assertNull(handler.handle(null));
    }
}
