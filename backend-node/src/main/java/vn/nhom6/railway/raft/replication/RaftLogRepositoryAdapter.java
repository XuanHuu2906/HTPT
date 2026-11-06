package vn.nhom6.railway.raft.replication;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * Bridge between the in-memory {@link InMemoryRaftLog} of one Node and the durable
 * {@link RaftStorage} boundary (SPEC-04 sections 4, 6 and 9).
 *
 * <p>The adapter owns the ordering rule "persist first, acknowledge afterwards": the visible
 * in-memory log is only replaced after {@link RaftStorage#replaceSuffix(long, List)} returns
 * successfully. A storage failure therefore never produces an {@code accepted=true} result and
 * never makes the in-memory log run ahead of durable storage. After such a failure the adapter
 * fails closed and refuses further writes until {@link #recover()} rebuilds its state from
 * {@link RaftStorage#load()}.
 *
 * <p>This class decides no Raft semantics: it does not apply the commit rule, does not count
 * majorities, does not manage {@code currentTerm}/{@code votedFor} and does not touch
 * {@code lastApplied} (TV2's apply transaction owns that checkpoint). It also does not use JDBC:
 * the durable implementation, {@code JdbcRaftStorage}, belongs to TV2-D12. The tests for this
 * adapter use an in-memory {@code FakeRaftStorage} only, so they prove the adapter's ordering and
 * fail-closed behaviour, <strong>not</strong> durability on PostgreSQL.
 *
 * <p><strong>Not thread-safe.</strong> Every method must be called from the single serialized Raft
 * event loop of the Node, the same convention as {@link RaftStorage}, {@link RaftState} and
 * {@link InMemoryRaftLog}. This class deliberately uses no locks, timers, threads, clock, random
 * values or identifiers.
 */
public final class RaftLogRepositoryAdapter {

    private final RaftStorage storage;

    private InMemoryRaftLog log = new InMemoryRaftLog();
    private boolean recovered;
    private boolean needsRecovery;

    /**
     * @param storage durable log storage of this Node, injected so the adapter stays independent
     *                of the JDBC implementation (TV2-D12)
     * @throws NullPointerException when {@code storage} is {@code null}
     */
    public RaftLogRepositoryAdapter(RaftStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    /**
     * Loads the persisted state and rebuilds the in-memory log after startup or after a storage
     * failure (SPEC-04 section 6).
     *
     * <p>The persisted log must be contiguous from index 1 with non-decreasing terms, and the
     * durable apply checkpoint must be within {@code [0, lastLogIndex]}. An inconsistent persisted
     * state fails closed with {@link RaftStorageException}; the adapter keeps its previous state
     * and stays unrecovered in that case. On success the returned state is ready for
     * {@link RaftState#restore(long, String, List, long)}.
     *
     * @return the durable state exactly as loaded from storage
     * @throws RaftStorageException when storage cannot be read or the persisted state is
     *                              inconsistent
     */
    public RaftStorage.PersistentState recover() throws RaftStorageException {
        RaftStorage.PersistentState persisted = storage.load();
        if (persisted == null) {
            throw new RaftStorageException("storage.load() returned null");
        }
        InMemoryRaftLog rebuilt = new InMemoryRaftLog();
        try {
            for (RaftLogEntry entry : persisted.log()) {
                rebuilt.append(entry);
            }
        } catch (IllegalArgumentException | NullPointerException invalidLog) {
            throw new RaftStorageException("persisted log is not a valid contiguous Raft log", invalidLog);
        }
        if (persisted.lastApplied() < RaftState.EMPTY_LOG_INDEX
                || persisted.lastApplied() > rebuilt.lastIndex()) {
            throw new RaftStorageException("persisted lastApplied " + persisted.lastApplied()
                    + " is outside [0, lastLogIndex=" + rebuilt.lastIndex() + "]");
        }

        this.log = rebuilt;
        this.recovered = true;
        this.needsRecovery = false;
        return persisted;
    }

    // ---------------------------------------------------------------------------------------
    // Queries (readable after recover(), including while a recovery is required)
    // ---------------------------------------------------------------------------------------

    /**
     * Returns the entry at {@code index}, or empty when it is not present (index 0 and indexes
     * beyond the last entry are empty queries).
     *
     * @throws IllegalStateException when {@link #recover()} has not succeeded yet
     */
    public Optional<RaftLogEntry> read(long index) {
        requireRecovered();
        return log.get(index);
    }

    /** Index of the last persisted-and-visible entry, or 0 for an empty log. */
    public long lastIndex() {
        requireRecovered();
        return log.lastIndex();
    }

    /** Term of the last persisted-and-visible entry, or 0 for an empty log. */
    public long lastTerm() {
        requireRecovered();
        return log.lastTerm();
    }

    /**
     * Term at {@code index}, with {@code termAt(0) == 0} for the empty prefix.
     *
     * @throws IllegalStateException     when {@link #recover()} has not succeeded yet
     * @throws IndexOutOfBoundsException when {@code index} is beyond the last entry
     */
    public long termAt(long index) {
        requireRecovered();
        return log.termAt(index);
    }

    /** Unmodifiable snapshot of the persisted-and-visible log, ordered by ascending index. */
    public List<RaftLogEntry> entries() {
        requireRecovered();
        return log.entries();
    }

    // ---------------------------------------------------------------------------------------
    // Writes
    // ---------------------------------------------------------------------------------------

    /**
     * Appends an entry created by this Node as Leader.
     *
     * <p>The entry is first added to a private copy of the log, then written with
     * {@link RaftStorage#replaceSuffix(long, List)}, and only a successful storage write publishes
     * the copy as the visible log. On {@link RaftStorageException} the visible log is unchanged,
     * the adapter requires {@link #recover()}, and the exception is rethrown.
     *
     * @param entry entry whose index is {@code lastIndex() + 1}
     * @throws IllegalStateException    when the adapter is not recovered or requires recovery
     * @throws IllegalArgumentException when the entry does not extend the log
     * @throws RaftStorageException     when the entry cannot be made durable
     */
    public void appendLocal(RaftLogEntry entry) throws RaftStorageException {
        requireWritable();
        InMemoryRaftLog updated = copyOf(log);
        updated.append(entry);

        try {
            storage.replaceSuffix(entry.index(), List.of(entry));
        } catch (RaftStorageException storageFailure) {
            needsRecovery = true;
            throw storageFailure;
        }
        this.log = updated;
    }

    /**
     * Applies one Follower-side {@code APPEND_ENTRIES} batch.
     *
     * <p>The {@link LogConsistencyChecker} runs on a private copy of the log. A rejected batch or a
     * fail-closed conflict leaves the visible log and storage untouched. When entries were really
     * appended, {@link RaftStorage#replaceSuffix(long, List)} persists the truncation and the new
     * entries, and only its success publishes the copy and returns an {@code accepted=true} result.
     *
     * <p>A caller may send {@code APPEND_ENTRIES_RESPONSE.success=true} only after this method
     * returns normally with {@code result.accepted() == true} (SPEC-04 section 9). On
     * {@link RaftStorageException} the method never returns a result: the visible log is unchanged,
     * the adapter requires {@link #recover()}, and the exception is rethrown.
     *
     * @param prevLogIndex index preceding the batch, {@code >= 0}
     * @param prevLogTerm  expected term at {@code prevLogIndex}, 0 only at index 0
     * @param entries      contiguous batch starting at {@code prevLogIndex + 1}, possibly empty
     * @param commitIndex  highest index known to be committed, within {@code [0, lastIndex()]}
     * @return the checker outcome once any required persistence has succeeded
     * @throws IllegalStateException    when the adapter is not recovered or requires recovery, or
     *                                  when an accepted result would truncate without appending
     * @throws IllegalArgumentException when the batch or {@code commitIndex} is malformed
     * @throws RaftStorageException     when the change cannot be made durable
     */
    public LogConsistencyChecker.ReconcileResult appendEntries(
            long prevLogIndex, long prevLogTerm, List<RaftLogEntry> entries, long commitIndex)
            throws RaftStorageException {
        requireWritable();
        InMemoryRaftLog updated = copyOf(log);
        LogConsistencyChecker.ReconcileResult result =
                new LogConsistencyChecker(updated).reconcile(prevLogIndex, prevLogTerm, entries, commitIndex);

        if (!result.accepted()) {
            return result;
        }
        if (result.appended().isEmpty()) {
            if (result.truncatedFromIndex() > RaftState.EMPTY_LOG_INDEX) {
                throw new IllegalStateException("accepted batch truncated the log at index "
                        + result.truncatedFromIndex() + " but appended no entry");
            }
            return result;
        }

        long fromIndex = result.appended().get(0).index();
        try {
            storage.replaceSuffix(fromIndex, result.appended());
        } catch (RaftStorageException storageFailure) {
            needsRecovery = true;
            throw storageFailure;
        }
        this.log = updated;
        return result;
    }

    // ---------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------

    private void requireRecovered() {
        if (!recovered) {
            throw new IllegalStateException(
                    "Raft log adapter has not been recovered; call recover() before querying");
        }
    }

    private void requireWritable() {
        requireRecovered();
        if (needsRecovery) {
            throw new IllegalStateException(
                    "Raft log adapter requires recover() after a storage failure before writing");
        }
    }

    private static InMemoryRaftLog copyOf(InMemoryRaftLog source) {
        InMemoryRaftLog copy = new InMemoryRaftLog();
        for (RaftLogEntry entry : source.entries()) {
            copy.append(entry);
        }
        return copy;
    }
}
