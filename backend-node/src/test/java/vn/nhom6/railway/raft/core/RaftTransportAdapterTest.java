package vn.nhom6.railway.raft.core;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RaftTransportAdapter} (SPEC-03 sections 1-3, 6 and 6.1; SPEC-04 sections
 * 3.5, 4 and 10).
 *
 * <p><strong>Honesty boundary.</strong> Every test uses {@code FakeRaftRpcClient} and a manual
 * {@link ManualScheduler}; no TCP socket, real clock, sleep, thread, PostgreSQL, majority count,
 * commit, replication or {@code RaftNode} wiring takes part. The real asynchronous peer client is
 * TV3-D11, so the TCP check is deferred until after D11. Passing this suite means only that the
 * adapter contract works with a fake RPC client.
 */
class RaftTransportAdapterTest {

    private static final String LOCAL = "node-1";
    private static final String PEER = "node-2";
    private static final String THIRD = "node-3";

    private static final String BOOK_COMMAND = "{\"passengerName\":\"Khach demo\","
            + "\"requestId\":\"r-1\",\"seatId\":\"G05\",\"ticketId\":\"t-1\",\"tripId\":\"SE-001\"}";

    private final FakeRaftRpcClient rpc = new FakeRaftRpcClient();
    private final ManualScheduler scheduler = new ManualScheduler();
    private final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
    private final List<Exception> failures = new ArrayList<>();
    private final List<RaftTransportAdapter.PeerAppendResult> results = new ArrayList<>();

    // -------------------------------------------------------------------------------------------
    // Fake RPC client and manual clock
    // -------------------------------------------------------------------------------------------

    /** Records the wire requests and lets each test complete the future explicitly. */
    static final class FakeRaftRpcClient implements RaftTransportAdapter.RaftRpcClient {

        final List<String> peers = new ArrayList<>();
        final List<MessageEnvelope<AppendEntriesRequestDto>> requests = new ArrayList<>();
        final List<CompletableFuture<MessageEnvelope<JsonNode>>> responses = new ArrayList<>();
        Object lockTarget;
        boolean lockHeldDuringExchange;
        RuntimeException throwOnExchange;
        boolean returnNull;

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peerId,
                MessageEnvelope<AppendEntriesRequestDto> request) {
            if (lockTarget != null) {
                lockHeldDuringExchange = Thread.holdsLock(lockTarget);
            }
            peers.add(peerId);
            requests.add(request);
            if (throwOnExchange != null) {
                throw throwOnExchange;
            }
            if (returnNull) {
                return null;
            }
            var future = new CompletableFuture<MessageEnvelope<JsonNode>>();
            responses.add(future);
            return future;
        }

        void respond(int index, String sender, String correlation, MessageType type, Object payload)
                throws IOException {
            responses.get(index).complete(response(sender, correlation, type, payload));
        }

        void respondEnvelope(int index, MessageEnvelope<JsonNode> response) {
            responses.get(index).complete(response);
        }

        void fail(int index, Throwable failure) {
            responses.get(index).completeExceptionally(failure);
        }

        static MessageEnvelope<JsonNode> response(String sender, String correlation,
                MessageType type, Object payload) throws IOException {
            // Encode and decode like the real wire so the payload arrives as a JsonNode tree.
            return MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(correlation, type, sender, payload)));
        }
    }

    /** Deterministic deadline scheduler; tests advance time explicitly. */
    static final class ManualScheduler implements RaftTransportAdapter.TimeoutScheduler {

        private final List<Task> tasks = new ArrayList<>();
        private long now;

        @Override
        public Cancellable schedule(Runnable task, long delayMillis) {
            if (delayMillis < 0L) {
                throw new IllegalArgumentException("delay must not be negative");
            }
            Task scheduled = new Task(task, now + delayMillis);
            tasks.add(scheduled);
            return scheduled;
        }

        void advance(long millis) {
            now += millis;
            while (true) {
                Task next = null;
                for (Task task : tasks) {
                    if (!task.cancelled && task.dueAt <= now
                            && (next == null || task.dueAt < next.dueAt)) {
                        next = task;
                    }
                }
                if (next == null) {
                    return;
                }
                tasks.remove(next);
                next.action.run();
            }
        }

        int pending() {
            int count = 0;
            for (Task task : tasks) {
                if (!task.cancelled) {
                    count++;
                }
            }
            return count;
        }

        static final class Task implements Cancellable {
            private final Runnable action;
            private final long dueAt;
            private boolean cancelled;

            Task(Runnable action, long dueAt) {
                this.action = action;
                this.dueAt = dueAt;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private RaftTransportAdapter adapter(long timeoutMillis) {
        RaftTransportAdapter adapter = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofMillis(timeoutMillis), loop::add, scheduler, failures::add);
        rpc.lockTarget = adapter;
        return adapter;
    }

    private void send(RaftTransportAdapter adapter, String peer,
            RaftNode.AppendEntriesRequest request) {
        adapter.sendAppendEntries(peer, request, results::add);
    }

    private void drain() {
        Runnable task;
        while ((task = loop.poll()) != null) {
            task.run();
        }
    }

    private static RaftNode.AppendEntriesRequest heartbeat(long term, long prevLogIndex,
            long prevLogTerm, long leaderCommit) {
        return new RaftNode.AppendEntriesRequest(term, LOCAL, prevLogIndex, prevLogTerm,
                List.of(), leaderCommit);
    }

    private static RaftNode.AppendEntriesRequest withEntries(long term, long prevLogIndex,
            long prevLogTerm, long leaderCommit, RaftLogEntry... entries) {
        return new RaftNode.AppendEntriesRequest(term, LOCAL, prevLogIndex, prevLogTerm,
                List.of(entries), leaderCommit);
    }

    private static RaftLogEntry book(long index, long term, String payload) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static Map<String, Object> appendResponse(long term, boolean success, long matchIndex) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("term", term);
        payload.put("success", success);
        payload.put("matchIndex", matchIndex);
        return payload;
    }

    private static RaftTransportAdapter.Failed failedOf(
            RaftTransportAdapter.PeerAppendResult result) {
        return assertInstanceOf(RaftTransportAdapter.Failed.class, result);
    }

    // -------------------------------------------------------------------------------------------
    // Envelope shape and DTO mapping
    // -------------------------------------------------------------------------------------------

    @Test
    void heartbeatEnvelopeShape() {
        var adapter = adapter(5_000L);

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        assertEquals(List.of(PEER), rpc.peers);
        MessageEnvelope<AppendEntriesRequestDto> envelope = rpc.requests.get(0);
        assertEquals(MessageEnvelope.CURRENT_PROTOCOL_VERSION, envelope.getProtocolVersion());
        assertEquals(MessageType.APPEND_ENTRIES, envelope.getType());
        assertEquals(LOCAL, envelope.getSenderId());
        assertNull(envelope.getCorrelationId(), "a request has no correlationId");
        assertTrue(MessageEnvelope.isValidUuid(envelope.getMessageId()));
        AppendEntriesRequestDto dto = envelope.getPayload();
        assertEquals(2L, dto.getTerm());
        assertEquals(LOCAL, dto.getLeaderId());
        assertEquals(0L, dto.getPrevLogIndex());
        assertEquals(0L, dto.getPrevLogTerm());
        assertEquals(0L, dto.getLeaderCommit());
        assertTrue(dto.getEntries().isEmpty());
    }

    @Test
    void entriesMapToDtoWithJsonCommand() throws Exception {
        var adapter = adapter(5_000L);

        send(adapter, PEER, withEntries(2L, 0L, 0L, 0L, book(1L, 2L, BOOK_COMMAND)));

        AppendEntriesRequestDto dto = rpc.requests.get(0).getPayload();
        assertEquals(1, dto.getEntries().size());
        AppendEntriesRequestDto.LogEntryDto entry = dto.getEntries().get(0);
        assertEquals(1L, entry.getIndex());
        assertEquals(2L, entry.getTerm());
        assertEquals(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, entry.getCommandType());
        assertInstanceOf(Map.class, entry.getCommand(), "command must be a JSON object, not a string");

        MessageEnvelope<AppendEntriesRequestDto> roundTripped = MessageCodec.decode(
                MessageCodec.encode(rpc.requests.get(0)), AppendEntriesRequestDto.class);
        assertEquals(dto, roundTripped.getPayload(), "wire round-trip must preserve the request");
    }

    @Test
    void everySendUsesANewMessageId() throws Exception {
        var adapter = adapter(5_000L);

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        String firstMessageId = rpc.requests.get(0).getMessageId();
        rpc.respond(0, PEER, firstMessageId, MessageType.APPEND_ENTRIES_RESPONSE,
                appendResponse(2L, true, 0L));
        drain();
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        assertNotEquals(firstMessageId, rpc.requests.get(1).getMessageId(),
                "a retry is a new RPC and must use a fresh messageId");
    }

    // -------------------------------------------------------------------------------------------
    // Pre-send validation
    // -------------------------------------------------------------------------------------------

    @Test
    void nonCanonicalPayloadIsRejectedBeforeSending() {
        var adapter = adapter(5_000L);
        List<String> nonCanonical = List.of(
                "{\"b\":1,\"a\":2}",
                "{\"a\": 1}",
                "{\"a\":0.10}",
                "{\n  \"a\": 1\n}");

        for (String payload : nonCanonical) {
            assertThrows(IllegalArgumentException.class,
                    () -> send(adapter, PEER, withEntries(2L, 0L, 0L, 0L, book(1L, 2L, payload))),
                    "non-canonical payload must be rejected: " + payload);
        }
        assertTrue(rpc.requests.isEmpty(), "no exchange may happen for a rejected payload");

        send(adapter, PEER, withEntries(2L, 0L, 0L, 0L, book(1L, 2L, BOOK_COMMAND)));
        assertEquals(1, rpc.requests.size(), "a rejected payload must not occupy the peer slot");
    }

    @Test
    void oversizedEnvelopeIsRejectedBeforeSending() {
        var adapter = adapter(5_000L);
        String oversized = "{\"blob\":\"" + "x".repeat(1_100_000) + "\"}";

        assertThrows(IllegalArgumentException.class,
                () -> send(adapter, PEER, withEntries(2L, 0L, 0L, 0L, book(1L, 2L, oversized))));
        assertTrue(rpc.requests.isEmpty(), "an oversized envelope must never reach the client");

        String fits = "{\"blob\":\"" + "y".repeat(1_000_000) + "\"}";
        send(adapter, PEER, withEntries(2L, 0L, 0L, 0L, book(1L, 2L, fits)));
        assertEquals(1, rpc.requests.size(), "an envelope below 1 MiB must be sent normally");
    }

    @Test
    void argumentValidation() {
        var adapter = adapter(5_000L);

        assertThrows(IllegalArgumentException.class,
                () -> send(adapter, "node-9", heartbeat(2L, 0L, 0L, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> send(adapter, LOCAL, heartbeat(2L, 0L, 0L, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> adapter.sendAppendEntries(PEER,
                        new RaftNode.AppendEntriesRequest(2L, "node-2", 0L, 0L, List.of(), 0L),
                        results::add));
        assertThrows(IllegalArgumentException.class,
                () -> send(adapter, PEER, withEntries(2L, 0L, 0L, 0L,
                        RaftLogEntry.noOp(2L, 2L))));
        assertThrows(NullPointerException.class,
                () -> adapter.sendAppendEntries(PEER, null, results::add));
        assertThrows(NullPointerException.class,
                () -> adapter.sendAppendEntries(PEER, heartbeat(2L, 0L, 0L, 0L), null));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftTransportAdapter("node-9", rpc, Duration.ofMillis(5L),
                        loop::add, scheduler, failures::add));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftTransportAdapter(LOCAL, rpc, Duration.ZERO,
                        loop::add, scheduler, failures::add));
        assertTrue(rpc.requests.isEmpty(), "invalid arguments must not call the client");
    }

    // -------------------------------------------------------------------------------------------
    // Reply correlation and validation
    // -------------------------------------------------------------------------------------------

    @Test
    void successfulReplyIsDeliveredOnTheRaftLoopOnly() throws Exception {
        var adapter = adapter(5_000L);
        RaftNode.AppendEntriesRequest request =
                withEntries(2L, 0L, 0L, 0L, book(1L, 2L, BOOK_COMMAND));

        send(adapter, PEER, request);
        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 1L));

        assertTrue(results.isEmpty(), "the reply must be queued, not executed inline");
        assertEquals(1, loop.size());
        drain();

        assertEquals(1, results.size());
        RaftTransportAdapter.Replied replied =
                assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));
        assertSame(request, replied.request());
        assertEquals(new RaftNode.AppendEntriesResponse(2L, true, 1L), replied.response());
        assertEquals(Optional.of(true), adapter.peerConnectivity(PEER));
    }

    @Test
    void logMismatchReplyNormalizesMatchIndexToZero() throws Exception {
        var adapter = adapter(5_000L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, false, 999L));
        drain();

        RaftTransportAdapter.Replied replied =
                assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));
        assertEquals(new RaftNode.AppendEntriesResponse(2L, false, 0L), replied.response(),
                "SPEC-03 section 6: matchIndex is ignored and normalized to 0 on failure");
        assertEquals(Optional.of(true), adapter.peerConnectivity(PEER));
    }

    @Test
    void higherTermRejectionIsDelivered() throws Exception {
        var adapter = adapter(5_000L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(9L, false, 0L));
        drain();

        RaftTransportAdapter.Replied replied =
                assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));
        assertEquals(new RaftNode.AppendEntriesResponse(9L, false, 0L), replied.response());
    }

    @Test
    void invalidResponsesNeverAck() throws Exception {
        var adapter = adapter(5_000L);
        String wrongUuid = "00000000-0000-0000-0000-000000000000";
        List<ResponseFactory> invalid = List.of(
                id -> FakeRaftRpcClient.response(THIRD, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L)),
                id -> FakeRaftRpcClient.response(PEER, wrongUuid,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.REQUEST_VOTE_RESPONSE, Map.of("term", 2L, "voteGranted", true)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, Map.of("term", 2L, "success", true)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE,
                        Map.of("term", 2L, "success", true, "matchIndex", 0L, "extra", 1)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE,
                        Map.of("term", "2", "success", true, "matchIndex", 0L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(-1L, false, 0L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(1L, false, 0L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 5L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(9L, true, 0L)),
                id -> FakeRaftRpcClient.response(PEER, id,
                        MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, false, -1L)));

        for (ResponseFactory factory : invalid) {
            int index = rpc.requests.size();
            send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
            String messageId = rpc.requests.get(index).getMessageId();
            rpc.respondEnvelope(index, factory.create(messageId));
            drain();

            assertEquals(1, results.size(), "each invalid response yields exactly one outcome");
            assertEquals(RaftTransportAdapter.FailureKind.INVALID_RESPONSE,
                    failedOf(results.get(0)).kind());
            assertEquals(Optional.of(false), adapter.peerConnectivity(PEER));
            results.clear();
        }
    }

    @Test
    void peerErrorResponseIsPeerError() throws Exception {
        var adapter = adapter(5_000L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.ERROR_RESPONSE,
                Map.of("errorCode", "INTERNAL_ERROR", "message", "boom",
                        "leaderHint", LOCAL, "term", 2L,
                        "requestId", "11111111-1111-1111-1111-111111111111"));
        drain();

        assertEquals(RaftTransportAdapter.FailureKind.PEER_ERROR, failedOf(results.get(0)).kind());
        assertEquals(Optional.of(false), adapter.peerConnectivity(PEER));
    }

    @Test
    void transportFailuresAreFailuresNotAcks() {
        var adapter = adapter(5_000L);

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        rpc.fail(0, new IOException("peer disconnected"));
        assertTrue(results.isEmpty(), "an exceptional future must still be queued");
        drain();
        assertEquals(RaftTransportAdapter.FailureKind.TRANSPORT_ERROR,
                failedOf(results.get(0)).kind());
        results.clear();

        rpc.throwOnExchange = new RuntimeException("connect refused");
        send(adapter, THIRD, heartbeat(2L, 0L, 0L, 0L));
        assertTrue(results.isEmpty(), "a synchronous client failure must not run inline");
        drain();
        assertEquals(RaftTransportAdapter.FailureKind.TRANSPORT_ERROR,
                failedOf(results.get(0)).kind());
        results.clear();

        rpc.throwOnExchange = null;
        rpc.returnNull = true;
        send(adapter, "node-4", heartbeat(2L, 0L, 0L, 0L));
        assertTrue(results.isEmpty());
        drain();
        assertEquals(RaftTransportAdapter.FailureKind.TRANSPORT_ERROR,
                failedOf(results.get(0)).kind());
        assertTrue(failures.isEmpty(), "transport errors are RPC outcomes, not lifecycle failures");
    }

    // -------------------------------------------------------------------------------------------
    // Timeout, late response and one in-flight RPC
    // -------------------------------------------------------------------------------------------

    @Test
    void timeoutDeliversExactlyOneFailedAndFreesTheSlot() {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        scheduler.advance(99L);
        assertTrue(results.isEmpty(), "the deadline must not fire early");
        scheduler.advance(1L);
        drain();

        assertEquals(1, results.size());
        assertEquals(RaftTransportAdapter.FailureKind.TIMEOUT, failedOf(results.get(0)).kind());
        assertEquals(Optional.of(false), adapter.peerConnectivity(PEER));

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        assertEquals(2, rpc.requests.size(), "a timed-out RPC must free the peer slot");
    }

    @Test
    void lateValidSuccessAfterTimeoutNeverCountsAsAck() throws Exception {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        String messageId = rpc.requests.get(0).getMessageId();
        scheduler.advance(100L);
        drain();
        results.clear();

        rpc.respond(0, PEER, messageId, MessageType.APPEND_ENTRIES_RESPONSE,
                appendResponse(2L, true, 0L));
        drain();

        assertTrue(results.isEmpty(), "a late success must never become an ACK");
        assertEquals(Optional.of(false), adapter.peerConnectivity(PEER),
                "a late response must not change the connectivity observation");
    }

    @Test
    void lateHigherTermAfterTimeoutIsSurfacedOnce() throws Exception {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        String messageId = rpc.requests.get(0).getMessageId();
        scheduler.advance(100L);
        drain();
        results.clear();

        rpc.respond(0, PEER, messageId, MessageType.APPEND_ENTRIES_RESPONSE,
                appendResponse(5L, false, 0L));
        drain();

        assertEquals(List.of(new RaftTransportAdapter.LateHigherTerm(PEER, 5L)), results,
                "a late higher term must be surfaced exactly once, never as an ACK");
        assertEquals(Optional.of(false), adapter.peerConnectivity(PEER));

        // A second completion of the same future is rejected by CompletableFuture and adds nothing.
        assertFalse(rpc.responses.get(0).complete(FakeRaftRpcClient.response(PEER, messageId,
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(7L, false, 0L))));
        drain();
        assertEquals(1, results.size());
    }

    @Test
    void lateLowerOrEqualTermAfterTimeoutIsIgnored() throws Exception {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        String messageId = rpc.requests.get(0).getMessageId();
        scheduler.advance(100L);
        drain();
        results.clear();

        rpc.respond(0, PEER, messageId, MessageType.APPEND_ENTRIES_RESPONSE,
                appendResponse(2L, false, 0L));
        drain();

        assertTrue(results.isEmpty(), "a late term that is not higher is simply dropped");
    }

    @Test
    void lateUncorrelatedResponseAfterTimeoutIsIgnored() throws Exception {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        String messageId = rpc.requests.get(0).getMessageId();
        scheduler.advance(100L);
        drain();
        results.clear();

        rpc.respond(0, THIRD, messageId, MessageType.APPEND_ENTRIES_RESPONSE,
                appendResponse(9L, false, 0L));
        drain();

        assertTrue(results.isEmpty(), "a late response from another sender must be ignored");
    }

    @Test
    void replyAndTimeoutRaceCompletesOnce() throws Exception {
        var adapter = adapter(100L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();
        assertEquals(1, results.size());
        assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));

        scheduler.advance(100L);
        drain();

        assertEquals(1, results.size(), "a timeout after the reply must not add a second outcome");
        assertEquals(0, scheduler.pending(), "finishing the RPC cancels its deadline");
    }

    @Test
    void onlyOneAppendInFlightPerPeer() throws Exception {
        var adapter = adapter(5_000L);
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        assertThrows(IllegalStateException.class,
                () -> send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L)));
        assertEquals(1, rpc.requests.size(), "a second append must not reach the client");

        send(adapter, THIRD, heartbeat(2L, 0L, 0L, 0L));
        assertEquals(2, rpc.requests.size(), "another peer has its own slot");

        rpc.respond(1, THIRD, rpc.requests.get(1).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();
        send(adapter, THIRD, heartbeat(2L, 0L, 0L, 0L));
        assertEquals(3, rpc.requests.size(), "a resolved peer may be used again");

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();
        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        assertEquals(4, rpc.requests.size());
    }

    // -------------------------------------------------------------------------------------------
    // Locking, loop discipline and lifecycle failures
    // -------------------------------------------------------------------------------------------

    @Test
    void noLockHeldDuringExchangeAndNothingRunsInline() throws Exception {
        var adapter = adapter(5_000L);

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));

        assertFalse(rpc.lockHeldDuringExchange,
                "the adapter must not hold a monitor while calling the network");
        assertTrue(results.isEmpty());

        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        assertTrue(results.isEmpty(), "the reply must not run inside the completion thread");
        assertEquals(1, loop.size(), "the outcome must be queued on the Raft loop");
        drain();
        assertEquals(1, results.size());
    }

    @Test
    void replyCallbackFailureReachesOnFailureAndKeepsTheAdapterUsable() throws Exception {
        var adapter = adapter(5_000L);

        adapter.sendAppendEntries(PEER, heartbeat(2L, 0L, 0L, 0L), result -> {
            throw new IllegalStateException("raft loop callback failed");
        });
        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();

        assertEquals(1, failures.size());
        assertInstanceOf(IllegalStateException.class, failures.get(0));
        assertEquals("raft loop callback failed", failures.get(0).getMessage());

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        assertEquals(2, rpc.requests.size(), "a callback failure must not poison the adapter");
    }

    @Test
    void rejectedRaftLoopTaskReachesOnFailure() throws Exception {
        Executor closedLoop = task -> {
            throw new RejectedExecutionException("raft loop closed");
        };
        var adapter = new RaftTransportAdapter(LOCAL, rpc, Duration.ofMillis(5_000L),
                closedLoop, scheduler, failures::add);

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));

        assertEquals(1, failures.size());
        assertInstanceOf(RejectedExecutionException.class, failures.get(0));
        assertTrue(results.isEmpty(), "no reply can run when the Raft loop rejects it");
    }

    // -------------------------------------------------------------------------------------------
    // peerConnectivity
    // -------------------------------------------------------------------------------------------

    @Test
    void peerConnectivityTracksTheLastObservation() throws Exception {
        var adapter = adapter(100L);
        assertEquals(Optional.empty(), adapter.peerConnectivity(PEER));
        assertEquals(Optional.empty(), adapter.peerConnectivity(THIRD));

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        rpc.respond(0, PEER, rpc.requests.get(0).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();
        assertEquals(Optional.of(true), adapter.peerConnectivity(PEER));
        assertEquals(Optional.empty(), adapter.peerConnectivity(THIRD), "peers are independent");

        send(adapter, THIRD, heartbeat(2L, 0L, 0L, 0L));
        scheduler.advance(100L);
        drain();
        assertEquals(Optional.of(false), adapter.peerConnectivity(THIRD));
        assertEquals(Optional.of(true), adapter.peerConnectivity(PEER));

        rpc.respond(1, THIRD, rpc.requests.get(1).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(6L, false, 0L));
        drain();
        assertEquals(Optional.of(false), adapter.peerConnectivity(THIRD),
                "a late higher term does not change the observation");

        send(adapter, PEER, heartbeat(2L, 0L, 0L, 0L));
        rpc.respond(2, THIRD, rpc.requests.get(2).getMessageId(),
                MessageType.APPEND_ENTRIES_RESPONSE, appendResponse(2L, true, 0L));
        drain();
        assertEquals(Optional.of(false), adapter.peerConnectivity(PEER),
                "an invalid response is a failed observation");

        assertThrows(IllegalArgumentException.class, () -> adapter.peerConnectivity("node-9"));
    }

    @FunctionalInterface
    private interface ResponseFactory {
        MessageEnvelope<JsonNode> create(String messageId) throws Exception;
    }
}
