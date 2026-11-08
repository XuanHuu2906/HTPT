package vn.nhom6.railway.raft.replication;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * ACK contract of Raft replication (SPEC-04 section 9, SPEC-03 section 6).
 *
 * <p>This class is the single place that turns a durable-storage result into an acknowledgement:
 * <ul>
 *   <li>Follower: {@link RaftNode.AppendEntriesResponse#success()} may only be {@code true} after
 *       {@link RaftLogRepositoryAdapter#appendEntries(long, long, List, long)} has returned normally
 *       with an accepted result, i.e. after {@link RaftStorage#replaceSuffix(long, List)} reported
 *       success. A storage failure therefore never produces a {@code success=true} response.</li>
 *   <li>Leader: {@link RaftNode.Accepted} may only be returned after
 *       {@link RaftLogRepositoryAdapter#appendLocal(RaftLogEntry)} has returned normally, i.e. after
 *       the entry is persisted in the Leader's local log.</li>
 * </ul>
 *
 * <p><strong>{@code Accepted} is not commit.</strong> It only means that the Leader persisted the
 * entry locally. Majority counting, Raft commitment, state-machine apply and a {@code CONFIRMED}
 * business response belong to other tasks (SPEC-04 section 5). This class does not decide commit,
 * does not manage {@code currentTerm}/{@code votedFor}/{@code lastApplied}, and is not wired into
 * {@link RaftNode} or {@link vn.nhom6.railway.raft.core.RaftState} yet.
 *
 * <p>All durable writes go through the injected {@link RaftStorage} via an internal
 * {@link RaftLogRepositoryAdapter}; this class adds no second persistence mechanism and no
 * copy-on-write logic. Swapping {@code FakeRaftStorage} for the JDBC implementation
 * ({@code JdbcRaftStorage}, TV2-D12) is only a constructor-argument change.
 *
 * <p><strong>Not thread-safe.</strong> Every method must run on the single serialized Raft event
 * loop of the Node, the same convention as {@link RaftStorage} and {@link RaftLogRepositoryAdapter}.
 * No locks, timers, threads, clock, random values, identifiers or console logging are used.
 *
 * <p><strong>Test scope.</strong> The unit suite {@code PersistenceAckTest} uses an in-memory
 * {@code FakeRaftStorage} only. It proves the ACK ordering and fail-closed behaviour of this class;
 * it does <strong>not</strong> prove durability on PostgreSQL, replication, commit or majority.
 * {@code JdbcRaftStorage} belongs to TV2-D12 and the durable PostgreSQL check is performed after
 * that hand-over.
 */
public final class PersistentLogAppender {

    private final RaftLogRepositoryAdapter adapter;

    /**
     * @param storage durable log storage of this Node, injected so that the JDBC implementation is
     *                replaceable without changing this class
     * @throws NullPointerException when {@code storage} is {@code null}
     */
    public PersistentLogAppender(RaftStorage storage) {
        this.adapter = new RaftLogRepositoryAdapter(Objects.requireNonNull(storage, "storage"));
    }

    /**
     * Loads durable state and rebuilds the adapter after startup or after a storage failure.
     *
     * @return the durable state exactly as loaded from storage
     * @throws RaftStorageException when storage cannot be read or the persisted state is
     *                              inconsistent
     */
    public RaftStorage.PersistentState recover() throws RaftStorageException {
        return adapter.recover();
    }

    /**
     * Index of the last persisted-and-visible entry, or 0 for an empty log.
     *
     * @throws IllegalStateException when {@link #recover()} has not succeeded yet
     */
    public long lastIndex() {
        return adapter.lastIndex();
    }

    /**
     * Term of the last persisted-and-visible entry, or 0 for an empty log.
     *
     * @throws IllegalStateException when {@link #recover()} has not succeeded yet
     */
    public long lastTerm() {
        return adapter.lastTerm();
    }

    /**
     * Reads the persisted-and-visible entry at {@code index}.
     *
     * @throws IllegalStateException when {@link #recover()} has not succeeded yet
     */
    public Optional<RaftLogEntry> read(long index) {
        return adapter.read(index);
    }

    /**
     * Follower-side handling of one {@code APPEND_ENTRIES} request: validates the request term and
     * entry terms, persists any required log change, and only then returns an ACK.
     *
     * <p>The caller ({@code RaftNode}, which owns {@code currentTerm} and {@code commitIndex}) must
     * have advanced and persisted its term before calling this method with a newer request term.
     * This appender does not manage term, vote or role:
     * <ol>
     *   <li>{@code request.term() > currentTerm} is a caller contract violation and throws
     *       {@link IllegalArgumentException} without touching storage (SPEC-04 section 3).</li>
     *   <li>{@code request.term() < currentTerm} returns {@code success=false} with the local term
     *       and {@code matchIndex=0} without touching storage.</li>
     *   <li>Every entry term must not exceed {@code request.term()} (SPEC-03 section 6); a violation
     *       throws {@link IllegalArgumentException} without touching storage.</li>
     *   <li>A {@code prevLog} mismatch is a normal {@code success=false} response without storage
     *       access. A valid batch is reconciled and persisted through the adapter.</li>
     * </ol>
     *
     * <p>{@code success=true} is constructed only after the adapter returns normally with an
     * accepted result. A heartbeat or an idempotent re-send is accepted without a storage write
     * because the adapter's in-memory log only ever contains data already confirmed by storage.
     * Any failure from the adapter (checked or unchecked) is propagated unchanged and no response
     * is created.
     *
     * @param request     request values received from the Leader
     * @param currentTerm receiver's current term after handling the request
     * @param commitIndex receiver's current commit index, within {@code [0, lastIndex()]}
     * @return the response whose {@code term} is {@code currentTerm}
     * @throws NullPointerException     when {@code request} is {@code null}
     * @throws IllegalArgumentException when {@code currentTerm} is negative, when the request term
     *                                  is ahead of {@code currentTerm}, or when an entry term
     *                                  exceeds the request term or the batch is malformed
     * @throws IllegalStateException    when the adapter is not recovered, requires recovery, or a
     *                                  Raft invariant would be violated
     * @throws RaftStorageException     when a required log change cannot be made durable
     */
    public RaftNode.AppendEntriesResponse acknowledge(
            RaftNode.AppendEntriesRequest request, long currentTerm, long commitIndex)
            throws RaftStorageException {
        Objects.requireNonNull(request, "request");
        if (currentTerm < 0L) {
            throw new IllegalArgumentException("currentTerm must be >= 0 but was " + currentTerm);
        }
        if (request.term() > currentTerm) {
            throw new IllegalArgumentException("request term " + request.term()
                    + " is ahead of currentTerm " + currentTerm
                    + "; the caller must advance and persist the term before acknowledging");
        }
        if (request.term() < currentTerm) {
            return new RaftNode.AppendEntriesResponse(currentTerm, false, 0L);
        }

        List<RaftLogEntry> entries = request.entries();
        for (RaftLogEntry entry : entries) {
            if (entry.term() > request.term()) {
                throw new IllegalArgumentException("entry " + entry.index() + " has term " + entry.term()
                        + " which exceeds the request term " + request.term());
            }
        }

        LogConsistencyChecker.ReconcileResult result = adapter.appendEntries(
                request.prevLogIndex(), request.prevLogTerm(), entries, commitIndex);
        if (!result.accepted()) {
            return new RaftNode.AppendEntriesResponse(currentTerm, false, 0L);
        }
        // success=true exists only after appendEntries returned normally and storage reported success.
        return new RaftNode.AppendEntriesResponse(currentTerm, true, result.matchIndex());
    }

    /**
     * Leader-side append of one command to the local durable log.
     *
     * <p>{@link RaftNode.Accepted} means only that the entry was persisted in the Leader's local
     * log. It is <strong>not</strong> committed and must never be used to report {@code CONFIRMED}
     * to a Client. Replication and current-term majority commit are implemented in other tasks
     * (SPEC-04 section 5).
     *
     * <p>A storage failure (checked, or the adapter's fail-closed {@link IllegalStateException}
     * before/after a failed write) is reported as {@link RaftNode.StorageFailure}; it never yields
     * {@link RaftNode.Accepted}. A malformed entry (for example {@code currentTerm} lower than the
     * current last term, an unsupported command type, or a blank payload) throws
     * {@link IllegalArgumentException} before storage is called.
     *
     * @param commandType    command type from the established Raft command contract
     * @param commandPayload opaque command payload
     * @param currentTerm    current term of this Leader
     * @return accepted, or a storage-failure outcome
     * @throws IllegalArgumentException when the entry contract is violated
     * @throws NullPointerException     when the command type or payload is {@code null}
     */
    public RaftNode.AppendOutcome appendLeaderCommand(
            String commandType, String commandPayload, long currentTerm) {
        RaftLogEntry entry;
        try {
            entry = new RaftLogEntry(
                    Math.addExact(lastIndex(), 1L), currentTerm, commandType, commandPayload);
        } catch (IllegalStateException notRecovered) {
            return notWritable(notRecovered);
        }

        try {
            adapter.appendLocal(entry);
        } catch (RaftStorageException failure) {
            return new RaftNode.StorageFailure(failure);
        } catch (IllegalStateException blocked) {
            return notWritable(blocked);
        }
        // Accepted exists only after appendLocal returned normally, i.e. after the durable write.
        return new RaftNode.Accepted(entry.index(), entry.term());
    }

    private static RaftNode.StorageFailure notWritable(IllegalStateException cause) {
        return new RaftNode.StorageFailure(
                new RaftStorageException("log appender is not writable; recover() required", cause));
    }
}
