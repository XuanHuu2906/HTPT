package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionTimer;
import vn.nhom6.railway.raft.election.PersistentElectionState;

/**
 * Follower-side handler for one {@code APPEND_ENTRIES} RPC (SPEC-03 sections 6 and 6.1,
 * SPEC-04 sections 3.1, 3.5, 4, 6, 9 and 10).
 *
 * <p>The mandatory order of work is:
 * <ol>
 *   <li>schema and peer validation, before any state, storage or timer change;</li>
 *   <li>term handling: a stale term is answered {@code success=false}; a higher term is
 *       <strong>persisted before</strong> the log is touched (SPEC-04 section 9);</li>
 *   <li>durable log handling through the existing D08 {@link LogConflictResolver}
 *       (D05 conflict rule + D07 persist-before-ACK), which is not reimplemented here;</li>
 *   <li>mirroring the durable change into {@link RaftState} only after the resolver succeeded,
 *       so the in-memory model used by election and commit stays equal to the durable log;</li>
 *   <li>advancing {@code commitIndex} with {@code min(leaderCommit, matchIndex)} of this RPC,
 *       never with the local {@code lastLogIndex} and never downwards (SPEC-04 section 4);</li>
 *   <li>resetting the election timer for every valid request with a non-stale term, including a
 *       log-mismatch rejection (SPEC-04 section 3.1);</li>
 *   <li>returning the response built from the resolver's outcome.</li>
 * </ol>
 *
 * <p><strong>Scope.</strong> This class does not replace {@link LogConsistencyChecker},
 * {@link PersistentLogAppender} or {@link LogConflictResolver}, and it introduces no second
 * persistence mechanism, no second conflict rule and no second in-memory log. It owns the term
 * transition only by delegating to {@link PersistentElectionState} (TV5-D08). It is not wired into
 * {@link RaftNode}, does not use TCP, does not apply the state machine
 * ({@code markApplied}/{@code CommitListener}) and does not persist {@code commitIndex}
 * (it is reconstructed from {@code lastApplied} on restart, SPEC-04 section 6).
 *
 * <p><strong>Two in-memory logs.</strong> The D07 appender keeps the durable-and-visible log,
 * while {@link RaftState} keeps the model used by election and commit. This handler mirrors a
 * successful durable change into {@code RaftState} and then checks that both logs have the same
 * last index and last term. A mismatch fails closed instead of acknowledging a log the state does
 * not see.
 *
 * <p><strong>Fail closed.</strong> A storage failure, an unexpected failure from the resolver or
 * the mirror step, or a timer failure marks this handler unusable: it closes the timer and every
 * later {@link #handle} call throws {@link IllegalStateException}. The owning Node must stop Raft
 * activity and rebuild state, appender and handler from storage. A schema error
 * ({@link IllegalArgumentException}) is detected before any change and does not poison the
 * handler.
 *
 * <p><strong>Not thread-safe.</strong> Every method must run on the single serialized Raft event
 * loop of the Node, the same convention as {@link RaftState}, {@link PersistentLogAppender} and
 * {@link LogConflictResolver}. No threads, clock, random values, UUIDs or console logging are
 * used.
 *
 * <p><strong>Test scope.</strong> The unit suite {@code AppendEntriesHandlerTest} uses an
 * in-memory {@code FakeRaftStorage} and a recording {@code TimerHook} only. It proves the follower
 * logic, ordering and fail-closed behaviour of this class; it does <strong>not</strong> prove
 * durability on PostgreSQL, replication, Raft commitment, majority, state-machine apply or real
 * {@code APPEND_ENTRIES} RPC handling. {@code JdbcRaftStorage} belongs to TV2-D12 and the durable
 * PostgreSQL check is performed after that hand-over.
 */
public final class AppendEntriesHandler {

    private static final Set<String> VOTERS =
            Set.of("node-1", "node-2", "node-3", "node-4", "node-5");

    /**
     * Canonical serializer for the opaque {@code command} object of a wire DTO: compact JSON with
     * map keys ordered, so that the same command always produces the same payload string and the
     * same {@code (index, term)} comparison is stable. TV3 must confirm this convention.
     */
    private static final ObjectMapper COMMAND_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private final RaftState state;
    private final PersistentElectionState persistence;
    private final PersistentLogAppender appender;
    private final LogConflictResolver resolver;
    private final TimerHook timer;
    private boolean failed;

    /**
     * Production constructor. Uses the D08 resolver over the D07 appender and wraps the real
     * {@link ElectionTimer} behind the small {@link TimerHook} seam.
     *
     * @param persistence shared term/vote persistence boundary of this Node
     * @param appender    D07 appender that owns the durable log of this Node
     * @param timer       election timer of this Node
     * @throws NullPointerException     when an argument is {@code null}
     * @throws IllegalArgumentException when the state and appender logs already differ
     */
    public AppendEntriesHandler(
            PersistentElectionState persistence, PersistentLogAppender appender, ElectionTimer timer) {
        this(persistence, appender,
                new LogConflictResolver(Objects.requireNonNull(appender, "appender")),
                hookOf(timer));
    }

    /**
     * Test seam: accepts a custom resolver (for a failing planner) and a recording timer hook.
     *
     * @param persistence shared term/vote persistence boundary of this Node
     * @param appender    D07 appender that owns the durable log of this Node
     * @param resolver    D08 resolver, normally {@code new LogConflictResolver(appender)}
     * @param timer       timer seam of this Node
     * @throws NullPointerException     when an argument is {@code null}
     * @throws IllegalArgumentException when the state and appender logs already differ
     */
    AppendEntriesHandler(
            PersistentElectionState persistence,
            PersistentLogAppender appender,
            LogConflictResolver resolver,
            TimerHook timer) {
        this.persistence = Objects.requireNonNull(persistence, "persistence");
        this.state = this.persistence.state();
        this.appender = Objects.requireNonNull(appender, "appender");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.timer = Objects.requireNonNull(timer, "timer");
        verifyLogsMatch();
    }

    /**
     * Minimal timer boundary used by this handler. It hides the scheduler of the final
     * {@link ElectionTimer} so that tests can record calls without a real executor.
     */
    interface TimerHook {

        /** Called after every valid request whose term is not stale, including a log mismatch. */
        void onValidAppendEntries(long requestTerm);

        /** Arms the election deadline after a Leader steps down. */
        void start();

        /** Permanently cancels the deadline when this handler fails closed. */
        void close();
    }

    /**
     * Handles one validated internal {@code APPEND_ENTRIES} request.
     *
     * @param request  request values received from the Leader
     * @param senderId peer channel identity that delivered the request
     * @return the local term, the Raft outcome and this RPC's match index
     * @throws NullPointerException     when the request or sender is {@code null}
     * @throws IllegalArgumentException when the request violates schema or peer rules, before any
     *                                  mutation, storage call or timer change
     * @throws RaftStorageException     when required durable state cannot be stored; no response is
     *                                  returned and the handler fails closed
     * @throws IllegalStateException    when this handler previously failed closed, when a Leader
     *                                  receives another Leader for its own term, or when the
     *                                  durable log and {@link RaftState} diverge
     */
    public RaftNode.AppendEntriesResponse handle(
            RaftNode.AppendEntriesRequest request, String senderId) throws RaftStorageException {
        if (failed) {
            throw new IllegalStateException("append handler failed closed; recover the node before reuse");
        }
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(senderId, "senderId");
        validateRequest(request, senderId);

        long currentTerm = state.currentTerm();
        if (request.term() < currentTerm) {
            // Stale term: no storage call, no state change and no timer reset (SPEC-04 section 3.1).
            return new RaftNode.AppendEntriesResponse(currentTerm, false, 0L);
        }

        if (request.term() > currentTerm) {
            Role previousRole = state.role();
            try {
                // Persist the higher term and cleared vote before touching the log (SPEC-04 section 9).
                persistence.observeHigherTerm(request.term());
                state.becomeFollower(request.term(), request.leaderId());
                if (previousRole == Role.LEADER) {
                    // A Leader's timer was stopped; arm it again on step-down.
                    timer.start();
                }
            } catch (RaftStorageException failure) {
                failClosed();
                throw failure;
            } catch (RuntimeException failure) {
                failClosed();
                throw failure;
            }
        } else if (state.role() == Role.LEADER) {
            // Two Leaders in one term is impossible under Raft; refuse instead of guessing.
            throw new IllegalStateException("two leaders in the same term " + currentTerm);
        } else {
            // Equal term as Follower or Candidate: keep the vote, record the leader hint.
            state.becomeFollower(request.term(), request.leaderId());
        }

        LogConflictResolver.ConflictResolution resolution;
        try {
            resolution = resolver.resolve(request, state.currentTerm(), state.commitIndex());
        } catch (RaftStorageException failure) {
            failClosed();
            throw failure;
        } catch (IllegalArgumentException validation) {
            // Schema-style rejection from the D07/D08 contract: no mutation happened, keep the handler usable.
            throw validation;
        } catch (RuntimeException failure) {
            failClosed();
            throw failure;
        }

        try {
            if (resolution.response().success()) {
                mirrorIntoState(resolution);

                long matchIndex = resolution.response().matchIndex();
                long target = Math.max(state.commitIndex(),
                        Math.min(request.leaderCommit(), matchIndex));
                // Never use the local lastLogIndex: the Follower may still hold an unmatched suffix.
                state.advanceCommitIndex(target);
            }
        } catch (RuntimeException failure) {
            failClosed();
            if (failure instanceof IllegalStateException) {
                throw failure;
            }
            throw new IllegalStateException(
                    "append handler failed while mirroring the durable log into RaftState", failure);
        }

        try {
            // SPEC-04 section 3.1: a valid, non-stale request resets the timer even on log mismatch.
            timer.onValidAppendEntries(request.term());
        } catch (RuntimeException failure) {
            failClosed();
            throw failure;
        }

        return new RaftNode.AppendEntriesResponse(
                state.currentTerm(), resolution.response().success(), resolution.response().matchIndex());
    }

    /**
     * Handles one wire DTO by validating it and mapping every {@code LogEntryDto} to the core
     * {@link RaftLogEntry}.
     *
     * <p>The DTO {@code command} is an arbitrary JSON object. It is serialized once to a compact
     * JSON string with map keys ordered, which becomes the opaque {@code commandPayload} stored and
     * replicated by every Node. The same command content therefore always produces the same
     * payload string, so an idempotent re-send is not mistaken for a conflicting payload. This
     * canonical convention is proposed here and needs confirmation from TV3.
     *
     * @param dto      wire request, heartbeats allowed with an empty entry list
     * @param senderId peer channel identity that delivered the request
     * @return the result of {@link #handle(RaftNode.AppendEntriesRequest, String)}
     * @throws NullPointerException     when {@code dto} or {@code senderId} is {@code null}
     * @throws IllegalArgumentException when DTO validation, command serialization, entry mapping or
     *                                  the core schema/peer rules fail
     * @throws RaftStorageException     when required durable state cannot be stored
     * @throws IllegalStateException    when this handler previously failed closed
     */
    public RaftNode.AppendEntriesResponse handle(
            AppendEntriesRequestDto dto, String senderId) throws RaftStorageException {
        Objects.requireNonNull(dto, "dto");
        dto.validate();

        List<RaftLogEntry> entries = new ArrayList<>(dto.getEntries().size());
        for (AppendEntriesRequestDto.LogEntryDto logEntry : dto.getEntries()) {
            Objects.requireNonNull(logEntry, "logEntry");
            entries.add(new RaftLogEntry(
                    logEntry.getIndex(),
                    logEntry.getTerm(),
                    logEntry.getCommandType(),
                    canonicalCommand(logEntry.getCommand())));
        }

        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                dto.getTerm(),
                dto.getLeaderId(),
                dto.getPrevLogIndex(),
                dto.getPrevLogTerm(),
                entries,
                dto.getLeaderCommit());
        return handle(request, senderId);
    }

    /**
     * Mirrors a successful durable change into {@link RaftState}: truncate the same uncommitted
     * suffix, append only the entries the resolver really wrote, then require both logs to agree on
     * the last index and term. Called only after the D08 resolver returned {@code success=true}.
     */
    private void mirrorIntoState(LogConflictResolver.ConflictResolution resolution) {
        if (resolution.truncatedFromIndex() > 0L) {
            state.truncateSuffixFrom(resolution.truncatedFromIndex());
        }
        for (RaftLogEntry entry : resolution.appended()) {
            state.append(entry);
        }
        if (state.lastLogIndex() != appender.lastIndex() || state.lastLogTerm() != appender.lastTerm()) {
            throw new IllegalStateException("state and appender logs differ after mirroring: state=("
                    + state.lastLogIndex() + ", " + state.lastLogTerm() + "), appender=("
                    + appender.lastIndex() + ", " + appender.lastTerm() + ")");
        }
    }

    /** Checks schema and peer rules that must hold before any term, log or timer change. */
    private static void validateRequest(RaftNode.AppendEntriesRequest request, String senderId) {
        String leaderId = request.leaderId();
        if (leaderId == null || leaderId.isBlank()) {
            throw new IllegalArgumentException("leaderId must not be null or blank");
        }
        if (senderId.isBlank()) {
            throw new IllegalArgumentException("senderId must not be blank");
        }
        if (!leaderId.equals(senderId)) {
            throw new IllegalArgumentException("leaderId '" + leaderId
                    + "' must equal senderId '" + senderId + "'");
        }
        if (!VOTERS.contains(senderId)) {
            throw new IllegalArgumentException("senderId '" + senderId + "' is not one of the five voters");
        }
        if (request.term() < 0L) {
            throw new IllegalArgumentException("term must be >= 0 but was " + request.term());
        }
        if (request.prevLogIndex() < 0L) {
            throw new IllegalArgumentException("prevLogIndex must be >= 0 but was " + request.prevLogIndex());
        }
        if (request.prevLogTerm() < 0L) {
            throw new IllegalArgumentException("prevLogTerm must be >= 0 but was " + request.prevLogTerm());
        }
        if (request.prevLogIndex() == 0L && request.prevLogTerm() != 0L) {
            throw new IllegalArgumentException(
                    "prevLogIndex 0 requires prevLogTerm 0 but was " + request.prevLogTerm());
        }
        if (request.leaderCommit() < 0L) {
            throw new IllegalArgumentException("leaderCommit must be >= 0 but was " + request.leaderCommit());
        }

        List<RaftLogEntry> entries = request.entries();
        long expectedIndex;
        try {
            expectedIndex = Math.addExact(request.prevLogIndex(), 1L);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("prevLogIndex is too large: " + request.prevLogIndex(), overflow);
        }
        long previousTerm = request.prevLogTerm();
        for (int position = 0; position < entries.size(); position++) {
            RaftLogEntry entry = entries.get(position);
            if (entry == null) {
                throw new IllegalArgumentException("entry at position " + position + " must not be null");
            }
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException("entry at position " + position + " must have index "
                        + expectedIndex + " (prevLogIndex + 1 + position) but was " + entry.index());
            }
            if (entry.term() < previousTerm) {
                throw new IllegalArgumentException("entry term must not decrease at index " + entry.index()
                        + ": previous term " + previousTerm + ", entry term " + entry.term());
            }
            if (entry.term() > request.term()) {
                throw new IllegalArgumentException("entry term " + entry.term()
                        + " must not exceed request term " + request.term());
            }
            previousTerm = entry.term();
            try {
                expectedIndex = Math.addExact(expectedIndex, 1L);
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("entry index chain overflows at position " + position, overflow);
            }
        }
    }

    /** Serializes a wire command object to compact, key-ordered JSON. */
    private static String canonicalCommand(Object command) {
        try {
            String payload = COMMAND_MAPPER.writeValueAsString(command);
            if (payload.isBlank()) {
                throw new IllegalArgumentException("command must not serialize to a blank payload");
            }
            return payload;
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("command cannot be serialized to canonical JSON", failure);
        }
    }

    /** Requires the restored state and the recovered appender to see exactly the same log. */
    private void verifyLogsMatch() {
        List<RaftLogEntry> visible = new ArrayList<>();
        long lastIndex = appender.lastIndex();
        for (long index = RaftLogEntry.FIRST_INDEX; index <= lastIndex; index++) {
            long missingIndex = index;
            visible.add(appender.read(index).orElseThrow(() -> new IllegalArgumentException(
                    "state and appender logs differ: no visible entry at index " + missingIndex)));
        }
        if (!state.entries().equals(visible)) {
            throw new IllegalArgumentException("state and appender logs differ");
        }
    }

    /** Marks this handler unusable and permanently closes the election timer. */
    private void failClosed() {
        failed = true;
        timer.close();
    }

    private static TimerHook hookOf(ElectionTimer timer) {
        Objects.requireNonNull(timer, "timer");
        return new TimerHook() {
            @Override
            public void onValidAppendEntries(long requestTerm) {
                timer.onValidAppendEntries(requestTerm);
            }

            @Override
            public void start() {
                timer.start();
            }

            @Override
            public void close() {
                timer.close();
            }
        };
    }
}
