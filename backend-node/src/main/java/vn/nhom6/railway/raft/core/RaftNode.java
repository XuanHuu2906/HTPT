package vn.nhom6.railway.raft.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * Shared entry point for Raft election, replication, recovery, and state-machine
 * integration. This interface declares the boundary only; D02 provides no runtime loop.
 *
 * <p>On startup, the implementation loads {@link RaftStorage.PersistentState}, restores
 * {@link RaftState}, and starts as {@link Role#FOLLOWER} with {@code commitIndex} equal to
 * {@code lastApplied}. Every call into this interface is serialized on one Raft event loop.
 * Only the RaftNode implementation may mutate RaftState; election, replication, and recovery
 * collaborate through this boundary. Networking, booking, and monitor modules may use only
 * its read-only queries.
 *
 * <p>A {@link RaftStorageException} requires the Node to fail closed and stop confirming
 * writes. Durable persistence, Raft commitment, and state-machine application are distinct
 * milestones. D02 does not define a real event loop or lifecycle methods such as
 * {@code start()} and {@code stop()}.
 */
public interface RaftNode {

    /** Returns the current local Raft role. */
    Role role();

    /** Returns the current term. */
    long currentTerm();

    /** Returns the known leader hint, which may be absent or stale. */
    Optional<String> leaderId();

    /** Returns the highest index known committed by this Node. */
    long commitIndex();

    /** Returns the highest index durably applied by the state machine. */
    long lastApplied();

    /** Returns the last log index, or 0 for an empty log. */
    long lastLogIndex();

    /**
     * Appends and persists a command on the Leader.
     *
     * <p>{@link Accepted} means only that the entry was appended and persisted in the
     * Leader's local log. It does not mean committed or applied and must never be used to
     * report {@code CONFIRMED} to a Client. Majority and current-term commit rules are
     * implemented in a later task according to SPEC-04 section 5.
     *
     * @param commandType command type from the established Raft command contract
     * @param commandPayload opaque command payload
     * @return acceptance, redirection, or a storage-failure outcome
     */
    AppendOutcome appendCommand(String commandType, String commandPayload);

    /**
     * Handles a RequestVote request received by this Node.
     *
     * <p>Field names match SPEC-03 section 6; this is an internal contract type, not a wire
     * DTO. A granted vote must be persisted with {@link RaftStorage#saveTermAndVote(long,
     * String)} before the response is returned.
     *
     * @param request request values received from a peer
     * @return the local term and vote decision
     * @throws RaftStorageException if required durable state cannot be stored
     */
    RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws RaftStorageException;

    /**
     * Handles an AppendEntries request received by this Node.
     *
     * <p>Field names match SPEC-03 section 6; this is an internal contract type, not a wire
     * DTO. The implementation must enforce log matching and persist log changes before
     * returning {@code success=true}. On success, {@code matchIndex} is
     * {@code prevLogIndex + entries.size()} for this RPC; on failure it is 0 (SPEC-03
     * section 6 and SPEC-04 section 4).
     *
     * <p><strong>This signature requires confirmation from TV5 and TV3.</strong>
     *
     * @param request request values received from the Leader
     * @return the local term and result for this RPC
     * @throws RaftStorageException if required durable state cannot be stored
     */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) throws RaftStorageException;

    /**
     * Handles an election timeout. The RaftNode implementation starts an election through
     * RaftState, durably stores the new term and self-vote, and only then emits
     * {@code REQUEST_VOTE}. TV5 must not mutate RaftState directly.
     *
     * @throws RaftStorageException if the new term and vote cannot be durably stored
     */
    void onElectionTimeout() throws RaftStorageException;

    /**
     * Registers a consumer for newly committed entries.
     *
     * <p>Callbacks are delivered once per entry in increasing index order and only for
     * indexes at or below {@code commitIndex}. Committed does not mean applied or
     * {@code CONFIRMED}; a Leader may report {@code CONFIRMED} only after the StateMachine
     * apply transaction succeeds (SPEC-04 section 5). A listener must not throw exceptions
     * that escape or break the Raft event loop. Apply failures must retain the log entry for
     * retry/recovery, prevent confirmation, and be reported as operational errors.
     *
     * @param listener consumer of committed entries
     */
    void addCommitListener(CommitListener listener);

    /** Result of a Leader append attempt. */
    sealed interface AppendOutcome permits Accepted, NotLeader, StorageFailure {
    }

    /**
     * Entry appended and durably persisted to the local Leader log; not committed.
     *
     * @param index appended log index
     * @param term term in which the entry was appended
     */
    record Accepted(long index, long term) implements AppendOutcome {
    }

    /**
     * The receiving Node is not the Leader.
     *
     * @param leaderHint known Leader ID, or {@code null} if unknown
     */
    record NotLeader(String leaderHint) implements AppendOutcome {
    }

    /**
     * The append could not be durably stored.
     *
     * @param cause storage failure details for operational reporting
     */
    record StorageFailure(RaftStorageException cause) implements AppendOutcome {
        public StorageFailure {
            Objects.requireNonNull(cause, "cause");
        }
    }

    /**
     * RequestVote values matching the field names in SPEC-03 section 6.
     *
     * @param term candidate's term
     * @param candidateId candidate's Node ID
     * @param lastLogIndex candidate's last log index
     * @param lastLogTerm candidate's last log term
     */
    record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    }

    /**
     * RequestVote result matching the field names in SPEC-03 section 6.
     *
     * @param term receiver's current term after handling the request
     * @param voteGranted whether the vote was granted
     */
    record RequestVoteResponse(long term, boolean voteGranted) {
    }

    /**
     * AppendEntries values matching the field names in SPEC-03 section 6.
     *
     * @param term Leader's term
     * @param leaderId Leader's Node ID
     * @param prevLogIndex index immediately preceding {@code entries}
     * @param prevLogTerm term at {@code prevLogIndex}
     * @param entries entries to append
     * @param leaderCommit Leader's commit index
     */
    record AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<RaftLogEntry> entries,
            long leaderCommit) {
        public AppendEntriesRequest {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        }
    }

    /**
     * AppendEntries result matching the field names in SPEC-03 section 6.
     *
     * @param term receiver's current term after handling the request
     * @param success whether log matching and durable append succeeded
     * @param matchIndex index matched by this RPC, or 0 when unsuccessful
     */
    record AppendEntriesResponse(long term, boolean success, long matchIndex) {
    }

    /**
     * Receives committed log entries for state-machine integration.
     */
    @FunctionalInterface
    interface CommitListener {

        /**
         * Called once for each committed entry in increasing index order.
         *
         * @param entry committed entry; this does not assert that it has been applied
         */
        void onCommitted(RaftLogEntry entry);
    }
}
