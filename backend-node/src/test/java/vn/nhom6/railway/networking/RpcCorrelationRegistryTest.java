package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class RpcCorrelationRegistryTest {

    private RpcCorrelationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new RpcCorrelationRegistry(4); // limit 4 for testing backpressure
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    @DisplayName("Should register, complete successfully and decrement in-flight counter")
    void testRegisterAndCompleteSuccessfully() throws Exception {
        String msgId = UUID.randomUUID().toString();
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                registry.register(msgId, "node-2", Duration.ofSeconds(2), RequestVoteResponseDto.class);

        assertEquals(1, registry.getInFlightCount("node-2"));
        assertEquals(1, registry.getTotalInFlightCount());
        assertTrue(registry.hasPending(msgId));

        // Create response envelope
        RequestVoteResponseDto payload = RequestVoteResponseDto.granted(5);
        MessageEnvelope<RequestVoteResponseDto> responseEnvelope =
                MessageEnvelope.createResponse(msgId, MessageType.REQUEST_VOTE_RESPONSE, "node-2", payload);

        boolean completed = registry.complete(msgId, responseEnvelope);
        assertTrue(completed);

        MessageEnvelope<RequestVoteResponseDto> received = future.get();
        assertNotNull(received);
        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, received.getType());
        assertEquals(msgId, received.getCorrelationId());
        assertEquals(5, received.getPayload().getTerm());
        assertTrue(received.getPayload().isVoteGranted());

        assertEquals(0, registry.getInFlightCount("node-2"));
        assertEquals(0, registry.getTotalInFlightCount());
        assertFalse(registry.hasPending(msgId));
    }

    @Test
    @DisplayName("Should timeout when peer does not respond within deadline")
    void testTimeoutThrowsTimeoutException() throws Exception {
        String msgId = UUID.randomUUID().toString();
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                registry.register(msgId, "node-2", Duration.ofMillis(50), RequestVoteResponseDto.class);

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(TimeoutException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("timed out after 50 ms"));

        assertEquals(0, registry.getInFlightCount("node-2"));
        assertFalse(registry.hasPending(msgId));
    }

    @Test
    @DisplayName("Should reject requests when in-flight limit per peer is exceeded")
    void testBackpressureRejection() throws Exception {
        // Limit is 4
        for (int i = 0; i < 4; i++) {
            registry.register(UUID.randomUUID().toString(), "node-2", Duration.ofSeconds(5), RequestVoteResponseDto.class);
        }
        assertEquals(4, registry.getInFlightCount("node-2"));

        // 5th request should fail immediately with RpcBackpressureException
        RpcBackpressureException ex = assertThrows(RpcBackpressureException.class, () ->
                registry.register(UUID.randomUUID().toString(), "node-2", Duration.ofSeconds(5), RequestVoteResponseDto.class));

        assertEquals("node-2", ex.getPeerId());
        assertEquals(4, ex.getInFlightCount());
        assertEquals(4, ex.getMaxInFlight());

        // Other peer "node-3" is unaffected
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> futureNode3 =
                registry.register(UUID.randomUUID().toString(), "node-3", Duration.ofSeconds(5), RequestVoteResponseDto.class);
        assertNotNull(futureNode3);
        assertEquals(1, registry.getInFlightCount("node-3"));
    }

    @Test
    @DisplayName("Should fail all pending requests for a disconnected peer")
    void testFailAllWhenPeerDisconnects() throws Exception {
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> f1 =
                registry.register(id1, "node-2", Duration.ofSeconds(5), RequestVoteResponseDto.class);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> f2 =
                registry.register(id2, "node-2", Duration.ofSeconds(5), RequestVoteResponseDto.class);

        assertEquals(2, registry.getInFlightCount("node-2"));

        IOException disconnectError = new IOException("Connection reset by peer");
        registry.failAll("node-2", disconnectError);

        ExecutionException ex1 = assertThrows(ExecutionException.class, f1::get);
        assertSame(disconnectError, ex1.getCause());

        ExecutionException ex2 = assertThrows(ExecutionException.class, f2::get);
        assertSame(disconnectError, ex2.getCause());

        assertEquals(0, registry.getInFlightCount("node-2"));
        assertEquals(0, registry.getTotalInFlightCount());
    }

    @Test
    @DisplayName("Should safely ignore stale or unknown responses")
    void testStaleOrUnknownResponseIgnored() {
        String unknownId = UUID.randomUUID().toString();
        RequestVoteResponseDto payload = RequestVoteResponseDto.granted(1);
        MessageEnvelope<RequestVoteResponseDto> envelope =
                MessageEnvelope.createResponse(unknownId, MessageType.REQUEST_VOTE_RESPONSE, "node-2", payload);

        boolean completed = registry.complete(unknownId, envelope);
        assertFalse(completed);
    }

    @Test
    @DisplayName("Should complete exceptionally when response is an ERROR_RESPONSE")
    void testErrorResponseFailsFuture() throws Exception {
        String msgId = UUID.randomUUID().toString();
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                registry.register(msgId, "node-2", Duration.ofSeconds(5), RequestVoteResponseDto.class);

        MessageEnvelope<ErrorResponseDto> errorResp = MessageCodec.createErrorResponse(
                msgId,
                "NOT_LEADER",
                "Node 2 is follower",
                "node-3",
                2L,
                null,
                "node-2"
        );

        boolean completed = registry.complete(msgId, errorResp);
        assertTrue(completed);

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IOException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("NOT_LEADER"));
    }

    @Test
    @DisplayName("Closing registry should cancel all pending futures")
    void testCloseCancelsPending() throws Exception {
        String msgId = UUID.randomUUID().toString();
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                registry.register(msgId, "node-2", Duration.ofSeconds(10), RequestVoteResponseDto.class);

        registry.close();

        assertThrows(CancellationException.class, future::get);
    }
}
