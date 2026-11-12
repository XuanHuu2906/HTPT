package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RequestVoteRpcHandlerTest {

    private FakeRaftRequestHandler fakeProcessor;
    private RequestVoteRpcHandler handler;
    private final String localNodeId = "node-1";

    @BeforeEach
    void setUp() {
        fakeProcessor = new FakeRaftRequestHandler(1L);
        handler = new RequestVoteRpcHandler(fakeProcessor, localNodeId);
    }

    @Test
    @DisplayName("Should successfully handle valid vote request and grant vote")
    void testHandleValidVoteGranted() throws Exception {
        fakeProcessor.setVoteGranted(true);
        fakeProcessor.setCurrentTerm(2L);

        RequestVoteRequestDto payload = new RequestVoteRequestDto(2L, "node-2", 10L, 2L);
        MessageEnvelope<RequestVoteRequestDto> request = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, "node-2", payload);

        // Convert to generic JsonNode envelope
        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());
        assertEquals(localNodeId, response.getSenderId());

        RequestVoteResponseDto respPayload = (RequestVoteResponseDto) response.getPayload();
        assertTrue(respPayload.isVoteGranted());
        assertEquals(2L, respPayload.getTerm());

        assertEquals(1, fakeProcessor.getVoteInvocations().size());
        assertEquals("node-2", fakeProcessor.getVoteInvocations().get(0).senderId());
    }

    @Test
    @DisplayName("Should handle valid vote request when vote is rejected by consensus")
    void testHandleValidVoteRejected() throws Exception {
        fakeProcessor.setVoteGranted(false);
        fakeProcessor.setCurrentTerm(5L);

        RequestVoteRequestDto payload = new RequestVoteRequestDto(3L, "node-2", 10L, 2L);
        MessageEnvelope<RequestVoteRequestDto> request = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, "node-2", payload);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        RequestVoteResponseDto respPayload = (RequestVoteResponseDto) response.getPayload();
        assertFalse(respPayload.isVoteGranted());
        assertEquals(5L, respPayload.getTerm());
    }

    @Test
    @DisplayName("Should reject request when senderId does not match candidateId")
    void testRejectMismatchedSender() throws Exception {
        RequestVoteRequestDto payload = new RequestVoteRequestDto(2L, "node-3", 10L, 2L);
        // Sender is node-2, but candidateId is node-3
        MessageEnvelope<RequestVoteRequestDto> request = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, "node-2", payload);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
        assertTrue(err.getMessage().contains("does not match candidateId"));
    }

    @Test
    @DisplayName("Should reject request when senderId is localNodeId (remote vote for self)")
    void testRejectSelfVote() throws Exception {
        RequestVoteRequestDto payload = new RequestVoteRequestDto(2L, localNodeId, 10L, 2L);
        MessageEnvelope<RequestVoteRequestDto> request = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, localNodeId, payload);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
        assertTrue(err.getMessage().contains("cannot process remote vote request for itself"));
    }

    @Test
    @DisplayName("Should return INVALID_REQUEST when payload violates validation")
    void testRejectInvalidPayload() throws Exception {
        // Negative term
        String invalidJson = "{\"protocolVersion\":1,\"messageId\":\"" + UUID.randomUUID() + "\",\"correlationId\":null,\"type\":\"REQUEST_VOTE\",\"senderId\":\"node-2\",\"sentAt\":\"2026-11-12T10:00:00Z\",\"payload\":{\"term\":-1,\"candidateId\":\"node-2\",\"lastLogIndex\":0,\"lastLogTerm\":0}}";
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
        fakeProcessor.setFailWithException(true, "Database disk failure");

        RequestVoteRequestDto payload = new RequestVoteRequestDto(2L, "node-2", 10L, 2L);
        MessageEnvelope<RequestVoteRequestDto> request = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, "node-2", payload);

        byte[] bytes = MessageCodec.encode(request);
        MessageEnvelope<JsonNode> rawRequest = MessageCodec.decode(bytes);

        MessageEnvelope<?> response = handler.handle(rawRequest);

        assertNotNull(response);
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(request.getMessageId(), response.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) response.getPayload();
        assertEquals("INTERNAL_ERROR", err.getErrorCode());
        assertTrue(err.getMessage().contains("Database disk failure"));
    }

    @Test
    @DisplayName("Should return null for null request")
    void testNullOrEmptyEnvelope() throws Exception {
        assertNull(handler.handle(null));
    }
}
