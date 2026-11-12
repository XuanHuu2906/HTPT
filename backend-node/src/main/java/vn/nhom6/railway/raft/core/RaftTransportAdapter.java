package vn.nhom6.railway.raft.core;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

/**
 * Leader-side transport adapter for one {@code APPEND_ENTRIES} RPC (SPEC-03 sections 1-3,
 * 6 and 6.1; SPEC-04 sections 3.5, 4 and 10).
 *
 * <p><strong>Scope.</strong> This class only builds the wire request from a
 * {@link RaftNode.AppendEntriesRequest}, sends it through a {@link RaftRpcClient}, applies a
 * deadline, correlates the response and hands the outcome back to the Raft event loop. It is not a
 * Leader, not a replicator and not a commit counter: it does not compute {@code nextIndex}, count
 * votes, advance {@code matchIndex}/{@code commitIndex}, persist anything or apply the state
 * machine. Real peer TCP belongs to TV3-D11; the unit suite uses {@code FakeRaftRpcClient} and a
 * manual clock only.
 *
 * <p><strong>Honesty boundary.</strong> The tests of this class prove the adapter contract with a
 * fake RPC client. They do <strong>not</strong> prove TCP, network partitions, replication,
 * majority, Raft commit or PostgreSQL durability; the real transport check is deferred until
 * TV3-D11 hands over an asynchronous client.
 *
 * <p><strong>One RPC per peer.</strong> SPEC-04 section 4 allows at most one pending
 * {@code APPEND_ENTRIES} per peer. A second send while the first is unresolved throws
 * {@link IllegalStateException} instead of occupying the peer twice. The slot is released when the
 * RPC ends by reply, timeout or transport error.
 *
 * <p><strong>Loop discipline.</strong> No lock is held while {@link RaftRpcClient#exchange} runs.
 * Every outcome is queued with {@link Executor#execute}; {@code raftLoop} must enqueue and must
 * never execute inline. A callback failure is reported to {@code onFailure}; it is not retried or
 * swallowed here.
 *
 * <p><strong>Late responses.</strong> After a timeout the original future is still observed, but a
 * late response can never become a positive ACK. A late, correctly correlated response with
 * {@code term > request.term} is surfaced exactly once as {@link LateHigherTerm}; every other late
 * response is ignored.
 */
public final class RaftTransportAdapter {

    private static final Pattern VOTER_ID = Pattern.compile("node-[1-5]");

    /**
     * Canonical serializer used by TV4-D09 when it stores the opaque {@code command} object: compact
     * JSON with map keys ordered. The adapter rejects a payload that is not already in this form so
     * that Leader and Follower persist identical bytes and a failover cannot turn the same
     * {@code (index, term)} into two different payload strings.
     */
    private static final ObjectMapper COMMAND_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    /**
     * Sends one Raft RPC to a peer. The production implementation (TV3-D11) must be asynchronous per
     * RPC, correlate by {@code messageId}, complete exceptionally when the peer connection breaks,
     * use the peer channel rather than the client port, and never block the Raft event loop.
     */
    @FunctionalInterface
    public interface RaftRpcClient {

        /**
         * Sends the request and returns the correlated response, without blocking the caller.
         *
         * @param peerId target voter, {@code node-1}..{@code node-5}
         * @param request request envelope with a fresh {@code messageId}
         * @return future completed with the peer response, or completed exceptionally on failure
         */
        CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request);
    }

    /** Receives the single outcome of one RPC on the Raft event loop. */
    @FunctionalInterface
    public interface AppendReply {

        /**
         * Consumes the outcome. Called through {@code raftLoop}, never on the network thread and
         * never inline from {@link #sendAppendEntries}.
         *
         * @param result correlated reply, failure or late higher term
         * @throws Exception when Raft lifecycle handling fails; reported to {@code onFailure}
         */
        void receive(PeerAppendResult result) throws Exception;
    }

    /** Single terminal outcome of one {@code APPEND_ENTRIES} RPC. */
    public sealed interface PeerAppendResult permits Replied, Failed, LateHigherTerm {
    }

    /**
     * A correlated, schema-valid response was received within the deadline.
     *
     * @param peerId peer that answered
     * @param request original request, useful to the Leader for {@code nextIndex} handling
     * @param response validated response; {@code matchIndex} is normalized to 0 on failure
     */
    public record Replied(String peerId, RaftNode.AppendEntriesRequest request,
            RaftNode.AppendEntriesResponse response) implements PeerAppendResult {
    }

    /**
     * The RPC ended without a usable ACK.
     *
     * @param peerId peer involved
     * @param request original request
     * @param kind why no ACK exists
     */
    public record Failed(String peerId, RaftNode.AppendEntriesRequest request,
            FailureKind kind) implements PeerAppendResult {
    }

    /**
     * A late response after the deadline carried a term higher than the request term. The Leader
     * must still step down, but must never count the late response as an ACK.
     *
     * @param peerId peer that answered
     * @param term higher term observed
     */
    public record LateHigherTerm(String peerId, long term) implements PeerAppendResult {
    }

    /** Why an RPC produced no positive ACK. */
    public enum FailureKind {
        /** No response before the adapter deadline. */
        TIMEOUT,
        /** The client threw, returned {@code null} or completed exceptionally. */
        TRANSPORT_ERROR,
        /** Response envelope or payload violated the Raft contract. */
        INVALID_RESPONSE,
        /** The peer answered with {@code ERROR_RESPONSE} for this correlation. */
        PEER_ERROR
    }

    /** Test seam for deadlines; production wraps a {@link ScheduledExecutorService}. */
    interface TimeoutScheduler {

        Cancellable schedule(Runnable task, long delayMillis);

        interface Cancellable {
            void cancel();
        }
    }

    private final String localId;
    private final RaftRpcClient client;
    private final Executor raftLoop;
    private final Consumer<Exception> onFailure;
    private final TimeoutScheduler scheduler;
    private final long timeoutMillis;
    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> connectivity = new ConcurrentHashMap<>();

    /**
     * Production constructor.
     *
     * @param localId this Leader's configured voter ID
     * @param client asynchronous peer RPC client supplied by TV3-D11
     * @param timeout per-RPC deadline in milliseconds; must be positive
     * @param raftLoop executor that enqueues onto the single Raft event loop
     * @param scheduler scheduler used for deadlines
     * @param onFailure reports failures that escape the Raft loop or its reply callback
     */
    public RaftTransportAdapter(String localId, RaftRpcClient client, Duration timeout,
            Executor raftLoop, ScheduledExecutorService scheduler, Consumer<Exception> onFailure) {
        this(localId, client, timeout, raftLoop, scheduled(scheduler), onFailure);
    }

    /** Test/package constructor with an injectable clock seam; no real time is used by tests. */
    RaftTransportAdapter(String localId, RaftRpcClient client, Duration timeout,
            Executor raftLoop, TimeoutScheduler scheduler, Consumer<Exception> onFailure) {
        requireVoter(localId);
        this.localId = localId;
        this.client = Objects.requireNonNull(client, "client");
        this.raftLoop = Objects.requireNonNull(raftLoop, "raftLoop");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.onFailure = Objects.requireNonNull(onFailure, "onFailure");
        this.timeoutMillis = Objects.requireNonNull(timeout, "timeout").toMillis();
        if (timeoutMillis <= 0L) {
            throw new IllegalArgumentException("timeout must be positive milliseconds");
        }
    }

    /**
     * Validates, snapshots and sends one {@code APPEND_ENTRIES} RPC to {@code peerId}.
     *
     * <p>Every argument and payload problem throws before the peer slot is taken and before the
     * client is called. A second unresolved send to the same peer throws
     * {@link IllegalStateException}. A synchronous client failure is reported through
     * {@code reply} on the Raft loop and returns normally.
     *
     * @param peerId remote voter ID
     * @param request request to replicate; not mutated or retained by reference beyond its immutable
     *                record values
     * @param reply outcome consumer, always invoked through {@code raftLoop}
     * @throws IllegalArgumentException when the peer, request, payload or envelope size is invalid
     * @throws IllegalStateException    when another append is still pending for this peer
     */
    public void sendAppendEntries(String peerId, RaftNode.AppendEntriesRequest request,
            AppendReply reply) {
        requireRemoteVoter(peerId);
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(reply, "reply");
        if (!localId.equals(request.leaderId())) {
            throw new IllegalArgumentException("leaderId must equal the local voter");
        }

        AppendEntriesRequestDto dto = toDto(request);
        dto.validate();
        MessageEnvelope<AppendEntriesRequestDto> envelope = createEnvelope(dto);
        byte[] encoded;
        try {
            encoded = MessageCodec.encode(envelope);
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot encode APPEND_ENTRIES envelope", failure);
        }
        if (encoded.length > FrameCodec.MAX_FRAME_LENGTH) {
            // SPEC-03 section 6.1: the Leader splits batches; the adapter never truncates.
            throw new IllegalArgumentException("APPEND_ENTRIES envelope is " + encoded.length
                    + " bytes, above the " + FrameCodec.MAX_FRAME_LENGTH + "-byte frame limit; split the batch");
        }

        InFlight operation = new InFlight(peerId, envelope.getMessageId(), request, reply);
        if (inFlight.putIfAbsent(peerId, operation) != null) {
            throw new IllegalStateException("append already in flight to " + peerId);
        }

        CompletableFuture<MessageEnvelope<JsonNode>> pending;
        try {
            pending = Objects.requireNonNull(client.exchange(peerId, envelope),
                    "RPC client returned a null future");
        } catch (RuntimeException transportFailure) {
            abort(operation);
            return;
        }

        try {
            operation.timeout = scheduler.schedule(() -> onTimeout(operation), timeoutMillis);
        } catch (RuntimeException schedulingFailure) {
            abort(operation);
            return;
        }

        // A private dependent future leaves the client's future untouched and still observes a
        // response that arrives after the timeout, so a higher term is not lost.
        pending.copy().whenComplete((response, failure) -> {
            if (operation.completed.compareAndSet(false, true)) {
                finish(operation, failure != null
                        ? failed(operation, FailureKind.TRANSPORT_ERROR)
                        : evaluate(operation, response));
                return;
            }
            if (failure == null) {
                Long higherTerm = lateHigherTerm(operation, response);
                if (higherTerm != null) {
                    deliver(operation, new LateHigherTerm(operation.peerId, higherTerm));
                }
            }
        });
    }

    /**
     * Last connectivity observation for a peer, for {@code NODE_STATUS.peerConnectivity}
     * (SPEC-03 section 5). Empty means this adapter has not observed that peer yet. It is not a
     * quorum proof and must not be used to confirm writes.
     *
     * @param peerId voter ID
     * @return {@code true} after a valid reply, {@code false} after a failure, empty if unobserved
     * @throws IllegalArgumentException when the ID is not a configured voter
     */
    public Optional<Boolean> peerConnectivity(String peerId) {
        requireVoter(peerId);
        return Optional.ofNullable(connectivity.get(peerId));
    }

    private void onTimeout(InFlight operation) {
        if (operation.completed.compareAndSet(false, true)) {
            finish(operation, failed(operation, FailureKind.TIMEOUT));
        }
    }

    /** Ends an RPC that failed before any response future could be observed. */
    private void abort(InFlight operation) {
        if (operation.completed.compareAndSet(false, true)) {
            finish(operation, failed(operation, FailureKind.TRANSPORT_ERROR));
        }
    }

    private void finish(InFlight operation, PeerAppendResult result) {
        cancelTimeout(operation);
        // Publish the observation before releasing the peer slot; the next RPC for this peer can
        // only start after the slot is free, so observations stay ordered per peer.
        connectivity.put(operation.peerId, result instanceof Replied);
        inFlight.remove(operation.peerId, operation);
        deliver(operation, result);
    }

    private void deliver(InFlight operation, PeerAppendResult result) {
        try {
            raftLoop.execute(() -> {
                try {
                    operation.reply.receive(result);
                } catch (Exception callbackFailure) {
                    onFailure.accept(callbackFailure);
                }
            });
        } catch (RuntimeException rejected) {
            onFailure.accept(rejected);
        }
    }

    private static void cancelTimeout(InFlight operation) {
        TimeoutScheduler.Cancellable timeout = operation.timeout;
        if (timeout != null) {
            timeout.cancel();
        }
    }

    private static Failed failed(InFlight operation, FailureKind kind) {
        return new Failed(operation.peerId, operation.request, kind);
    }

    private PeerAppendResult evaluate(InFlight operation, MessageEnvelope<JsonNode> response) {
        if (response == null) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }
        try {
            response.validate();
        } catch (RuntimeException invalidEnvelope) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }

        MessageType type = response.getType();
        if (type != MessageType.APPEND_ENTRIES_RESPONSE && type != MessageType.ERROR_RESPONSE) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }
        if (!operation.peerId.equals(response.getSenderId())
                || !operation.messageId.equals(response.getCorrelationId())) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }
        if (type == MessageType.ERROR_RESPONSE) {
            return failed(operation, FailureKind.PEER_ERROR);
        }

        JsonNode payload = response.getPayload();
        if (!hasAppendResponseShape(payload)) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }
        long term = payload.get("term").longValue();
        boolean success = payload.get("success").booleanValue();
        long matchIndex = payload.get("matchIndex").longValue();
        if (term < operation.request.term()) {
            return failed(operation, FailureKind.INVALID_RESPONSE);
        }
        if (success) {
            if (term != operation.request.term()) {
                return failed(operation, FailureKind.INVALID_RESPONSE);
            }
            long expectedMatchIndex;
            try {
                expectedMatchIndex = Math.addExact(operation.request.prevLogIndex(),
                        operation.request.entries().size());
            } catch (ArithmeticException overflow) {
                return failed(operation, FailureKind.INVALID_RESPONSE);
            }
            if (matchIndex != expectedMatchIndex) {
                return failed(operation, FailureKind.INVALID_RESPONSE);
            }
        } else {
            // SPEC-03 section 6: the Leader ignores matchIndex on failure; normalize it to 0.
            matchIndex = 0L;
        }
        return new Replied(operation.peerId, operation.request,
                new RaftNode.AppendEntriesResponse(term, success, matchIndex));
    }

    /** Returns the higher term of a late but well-correlated response, or {@code null}. */
    private Long lateHigherTerm(InFlight operation, MessageEnvelope<JsonNode> response) {
        if (response == null) {
            return null;
        }
        try {
            response.validate();
            if (response.getType() != MessageType.APPEND_ENTRIES_RESPONSE
                    || !operation.peerId.equals(response.getSenderId())
                    || !operation.messageId.equals(response.getCorrelationId())) {
                return null;
            }
            JsonNode payload = response.getPayload();
            if (!hasAppendResponseShape(payload)) {
                return null;
            }
            long term = payload.get("term").longValue();
            boolean success = payload.get("success").booleanValue();
            // A success cannot be valid above the request term; do not ACK or advance from it.
            if (success || term <= operation.request.term()) {
                return null;
            }
            return term;
        } catch (RuntimeException invalid) {
            return null;
        }
    }

    private static boolean hasAppendResponseShape(JsonNode payload) {
        return payload != null
                && payload.isObject()
                && payload.size() == 3
                && isNonNegativeLong(payload.get("term"))
                && payload.get("success") != null
                && payload.get("success").isBoolean()
                && isNonNegativeLong(payload.get("matchIndex"));
    }

    private static boolean isNonNegativeLong(JsonNode node) {
        return node != null && node.isIntegralNumber() && node.canConvertToLong()
                && node.longValue() >= 0L;
    }

    private static MessageEnvelope<AppendEntriesRequestDto> createEnvelope(
            AppendEntriesRequestDto dto) {
        return MessageEnvelope.createRequest(MessageType.APPEND_ENTRIES, dto.getLeaderId(), dto);
    }

    /**
     * Snapshots the internal request into a fresh wire DTO. The opaque command string is parsed once
     * and must already be canonical compact JSON with ordered keys, matching TV4-D09.
     */
    private static AppendEntriesRequestDto toDto(RaftNode.AppendEntriesRequest request) {
        long expectedIndex = firstEntryIndex(request.prevLogIndex());
        List<AppendEntriesRequestDto.LogEntryDto> entries =
                new ArrayList<>(request.entries().size());
        for (RaftLogEntry entry : request.entries()) {
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException("entry at position " + entries.size()
                        + " must have index " + expectedIndex + " (prevLogIndex + 1 + position) but was "
                        + entry.index());
            }
            entries.add(new AppendEntriesRequestDto.LogEntryDto(
                    entry.index(), entry.term(), entry.commandType(),
                    canonicalCommand(entry.commandPayload())));
            try {
                expectedIndex = Math.addExact(expectedIndex, 1L);
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("entry index chain overflows", overflow);
            }
        }
        return new AppendEntriesRequestDto(request.term(), request.leaderId(),
                request.prevLogIndex(), request.prevLogTerm(), entries, request.leaderCommit());
    }

    private static long firstEntryIndex(long prevLogIndex) {
        try {
            return Math.addExact(prevLogIndex, 1L);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("prevLogIndex is too large: " + prevLogIndex, overflow);
        }
    }

    /**
     * Parses the opaque payload and requires it to equal its canonical serialization. This is the
     * only place that decides which Leader payloads may reach the wire; it runs before the slot is
     * taken.
     */
    private static Object canonicalCommand(String commandPayload) {
        if (commandPayload == null) {
            throw new IllegalArgumentException("commandPayload must not be null");
        }
        final Object command;
        try {
            command = COMMAND_MAPPER.readValue(commandPayload, Object.class);
        } catch (IOException invalidJson) {
            throw new IllegalArgumentException("commandPayload must be valid JSON", invalidJson);
        }
        try {
            String canonical = COMMAND_MAPPER.writeValueAsString(command);
            if (!commandPayload.equals(canonical)) {
                throw new IllegalArgumentException(
                        "commandPayload must be canonical JSON (compact, ordered keys) so that every "
                                + "Node stores identical bytes; got: " + commandPayload);
            }
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("commandPayload cannot be serialized as JSON", failure);
        }
        return command;
    }

    private void requireRemoteVoter(String peerId) {
        requireVoter(peerId);
        if (localId.equals(peerId)) {
            throw new IllegalArgumentException("remote voter required");
        }
    }

    private static void requireVoter(String id) {
        if (id == null || !VOTER_ID.matcher(id).matches()) {
            throw new IllegalArgumentException("configured voter required");
        }
    }

    private static TimeoutScheduler scheduled(ScheduledExecutorService scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        return (task, delayMillis) -> {
            ScheduledFuture<?> future = scheduler.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
            return () -> future.cancel(false);
        };
    }

    /** Per-RPC state; the CAS flag makes reply and timeout race a single outcome. */
    private static final class InFlight {
        private final String peerId;
        private final String messageId;
        private final RaftNode.AppendEntriesRequest request;
        private final AppendReply reply;
        private final AtomicBoolean completed = new AtomicBoolean();
        private volatile TimeoutScheduler.Cancellable timeout;

        private InFlight(String peerId, String messageId, RaftNode.AppendEntriesRequest request,
                AppendReply reply) {
            this.peerId = peerId;
            this.messageId = messageId;
            this.request = request;
            this.reply = reply;
        }
    }
}
