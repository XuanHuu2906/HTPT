package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * D08 coordination and diagnostics layer for safe suffix truncation (SPEC-04 sections 4, 5, 6, 9
 * and 10, SPEC-03 section 6).
 *
 * <p>This class does <strong>not</strong> replace {@link LogConsistencyChecker} (D05) or
 * {@link PersistentLogAppender} (D07) and it introduces no second persistence mechanism and no
 * second conflict rule. It only adds three things on top of the existing layers:
 * <ol>
 *   <li>It reports exactly which <strong>uncommitted</strong> entries were removed and replaced
 *       ({@link ConflictResolution#removed()} and {@link ConflictResolution#truncatedFromIndex()}),
 *       information that an {@code AppendEntriesResponse} alone cannot carry.</li>
 *   <li>It adds an independent {@code commitIndex} guard: even if the checker regresses, storage
 *       never receives a request that would erase a committed entry.</li>
 *   <li>It verifies after the durable write that the committed prefix {@code [1, commitIndex]} did
 *       not change and that the visible log is exactly the expected log.</li>
 * </ol>
 *
 * <p>{@link #resolve(RaftNode.AppendEntriesRequest, long, long)} runs the D05 conflict rule as a
 * dry run on a private copy of the visible log, then delegates the real write to
 * {@link PersistentLogAppender#acknowledge(RaftNode.AppendEntriesRequest, long, long)} and only
 * afterwards creates a {@link ConflictResolution}. A storage failure, a planner/appender
 * disagreement, or a failed post-write verification propagates or throws without returning a
 * resolution, so a caller can never turn such a failure into {@code success=true}.
 *
 * <p><strong>Scope.</strong> This class does not decide commit, does not count a majority, does not
 * manage {@code currentTerm}/{@code votedFor}, does not apply entries and does not choose the
 * Leader's {@code nextIndex} or a conflict hint. It is not wired into {@link RaftNode} or
 * {@code RaftState} yet. Wrapping it around {@link PersistentLogAppender} is the proposed hand-over
 * to TV5-D08.
 *
 * <p><strong>Not thread-safe.</strong> Every method must run on the single serialized Raft event
 * loop of the Node, the same convention as {@link LogConsistencyChecker},
 * {@link PersistentLogAppender} and {@code RaftState}. No locks, timers, threads, clock, random
 * values, UUIDs or console logging are used.
 *
 * <p><strong>Cost.</strong> A batch with entries takes an O(n) snapshot of the visible log and runs
 * the checker on a copy. Heartbeats and term-mismatch responses take the fast path and do not
 * snapshot; a rejected batch is snapshotted for the dry run but performs no durable write. This is
 * acceptable for the in-memory model; JDBC tuning belongs to a later task.
 *
 * <p><strong>Test scope.</strong> The unit suite {@code LogConflictResolverTest} uses an in-memory
 * {@code FakeRaftStorage} only. It proves the coordination, commit guard, reporting and
 * post-write check of this class; it does <strong>not</strong> prove durability on PostgreSQL,
 * replication, Raft commitment, majority, or real {@code APPEND_ENTRIES} RPC handling.
 * {@code JdbcRaftStorage} belongs to TV2-D12 and the durable PostgreSQL check is performed after
 * that hand-over.
 *
 * <p><strong>Ambiguous storage failure.</strong> If storage persisted a change and then reported
 * an error, the caller must call {@link PersistentLogAppender#recover()} and infer the new log from
 * storage. The {@link ConflictResolution#removed()} list of that attempt is lost; it cannot be
 * reconstructed from a later successful retry.
 */
public final class LogConflictResolver {

    private final PersistentLogAppender appender;
    private final Planner planner;

    /**
     * Creates a resolver using the D05 {@link LogConsistencyChecker} as the conflict planner.
     *
     * @param appender D07 appender that owns the durable write and the ACK ordering
     * @throws NullPointerException when {@code appender} is {@code null}
     */
    public LogConflictResolver(PersistentLogAppender appender) {
        this(appender, (copy, prevLogIndex, prevLogTerm, entries, commitIndex) ->
                new LogConsistencyChecker(copy).reconcile(prevLogIndex, prevLogTerm, entries, commitIndex));
    }

    /**
     * Test-only constructor that allows replacing the conflict planner.
     *
     * <p>It exists so that the commit guard, the planner/appender disagreement check and the
     * post-write verification can be exercised with a deliberately wrong planner. Production code
     * uses {@link #LogConflictResolver(PersistentLogAppender)}.
     *
     * @param appender D07 appender that owns the durable write and the ACK ordering
     * @param planner  dry-run conflict planner, normally {@link LogConsistencyChecker#reconcile}
     * @throws NullPointerException when an argument is {@code null}
     */
    LogConflictResolver(PersistentLogAppender appender, Planner planner) {
        this.appender = Objects.requireNonNull(appender, "appender");
        this.planner = Objects.requireNonNull(planner, "planner");
    }

    /**
     * Dry-run conflict planner over a private copy of the visible log. The production
     * implementation delegates to {@link LogConsistencyChecker#reconcile(long, long, List, long)}.
     * It must not touch storage and must not mutate the real log.
     */
    @FunctionalInterface
    interface Planner {

        /**
         * Plans one batch against {@code copy} without persisting anything.
         *
         * @param copy         private copy of the visible log, safe to mutate
         * @param prevLogIndex index of the entry preceding the batch
         * @param prevLogTerm  term expected at {@code prevLogIndex}
         * @param entries      entries of the batch, never empty on this path
         * @param commitIndex  caller's authoritative commit index
         * @return the D05 reconciliation outcome for the batch
         */
        LogConsistencyChecker.ReconcileResult plan(
                InMemoryRaftLog copy,
                long prevLogIndex,
                long prevLogTerm,
                List<RaftLogEntry> entries,
                long commitIndex);
    }

    /**
     * Resolves one {@code APPEND_ENTRIES} batch while keeping committed entries untouched
     * (SPEC-04 section 4 and 10, SPEC-03 section 6).
     *
     * <p>The processing order is deliberately fail closed:
     * <ol>
     *   <li>A term mismatch is delegated to the appender without planning. An old term returns
     *       {@code success=false}; a request term ahead of {@code currentTerm} is a caller contract
     *       violation and the appender throws {@link IllegalArgumentException}.</li>
     *   <li>A heartbeat takes the fast path: only the appender is called, no snapshot is taken.</li>
     *   <li>A batch with entries is planned on a private copy of the visible log. A rejected plan
     *       is turned into the appender's normal {@code success=false} response.</li>
     *   <li>Before any storage call, a plan that would truncate at or below {@code commitIndex} is
     *       rejected with {@link IllegalStateException}.</li>
     *   <li>The appender performs the durable write (D07 ordering). A storage failure propagates
     *       and no resolution is created.</li>
     *   <li>The visible log is read back and must equal the expected log; the committed prefix
     *       {@code [1, commitIndex]} must be identical to the snapshot. Any difference fails
     *       closed with {@link IllegalStateException} and no resolution.</li>
     * </ol>
     *
     * <p>A {@link ConflictResolution} therefore exists only after the appender returned normally
     * with {@code success=true} and the post-write verification passed.
     *
     * @param request     request values received from the Leader
     * @param currentTerm receiver's current term after handling the request
     * @param commitIndex receiver's current commit index, within {@code [0, lastIndex()]}
     * @return the response plus what was removed and appended
     * @throws NullPointerException     when {@code request} is {@code null}
     * @throws IllegalArgumentException when the appender rejects the term/entry contract
     * @throws IllegalStateException    when the log is not recovered, a committed entry would be
     *                                  touched, planner and appender disagree, or the post-write
     *                                  verification fails
     * @throws RaftStorageException     when the required durable write fails
     */
    public ConflictResolution resolve(
            RaftNode.AppendEntriesRequest request, long currentTerm, long commitIndex)
            throws RaftStorageException {
        Objects.requireNonNull(request, "request");

        if (request.term() != currentTerm) {
            // Delegate the established term rules to D07 instead of duplicating them.
            RaftNode.AppendEntriesResponse response = appender.acknowledge(request, currentTerm, commitIndex);
            return unchanged(response);
        }

        if (request.entries().isEmpty()) {
            // Heartbeat fast path: prevLog is still validated by acknowledge, but no O(n) snapshot.
            RaftNode.AppendEntriesResponse response = appender.acknowledge(request, currentTerm, commitIndex);
            return unchanged(response);
        }

        List<RaftLogEntry> before = snapshotVisibleLog();
        InMemoryRaftLog copy = copyOf(before);
        LogConsistencyChecker.ReconcileResult plan = Objects.requireNonNull(
                planner.plan(copy, request.prevLogIndex(), request.prevLogTerm(), request.entries(), commitIndex),
                "planner result");

        if (!plan.accepted()) {
            RaftNode.AppendEntriesResponse response = appender.acknowledge(request, currentTerm, commitIndex);
            if (response.success()) {
                throw new IllegalStateException(
                        "planner and appender disagree: the planner rejected the batch but the appender accepted it");
            }
            return unchanged(response);
        }

        long truncatedFromIndex = plan.truncatedFromIndex();
        if (truncatedFromIndex > 0L && truncatedFromIndex <= commitIndex) {
            throw new IllegalStateException("refusing to truncate at index " + truncatedFromIndex
                    + " because it is at or below commitIndex " + commitIndex);
        }

        List<RaftLogEntry> expected = expectedLog(before, truncatedFromIndex, plan.appended());
        List<RaftLogEntry> removed = removedEntries(before, truncatedFromIndex);

        RaftNode.AppendEntriesResponse response = appender.acknowledge(request, currentTerm, commitIndex);
        if (!response.success()) {
            throw new IllegalStateException(
                    "planner and appender disagree: the planner accepted the batch but the appender rejected it");
        }

        verifyAfterWrite(before, expected, commitIndex);
        return new ConflictResolution(response, truncatedFromIndex, removed, plan.appended());
    }

    /** Reads the visible log from index 1 through {@code lastIndex()}. */
    private List<RaftLogEntry> snapshotVisibleLog() {
        long lastIndex = appender.lastIndex();
        List<RaftLogEntry> before = new ArrayList<>();
        for (long index = RaftLogEntry.FIRST_INDEX; index <= lastIndex; index++) {
            long missingIndex = index;
            before.add(appender.read(index).orElseThrow(() -> new IllegalStateException(
                    "visible log has no entry at index " + missingIndex + " although lastIndex is "
                            + lastIndex)));
        }
        return before;
    }

    /** Builds the expected visible log after removing the suffix and appending the planned entries. */
    private static List<RaftLogEntry> expectedLog(
            List<RaftLogEntry> before, long truncatedFromIndex, List<RaftLogEntry> appended) {
        List<RaftLogEntry> expected = new ArrayList<>();
        for (RaftLogEntry entry : before) {
            if (truncatedFromIndex == 0L || entry.index() < truncatedFromIndex) {
                expected.add(entry);
            }
        }
        expected.addAll(appended);
        return expected;
    }

    /** Collects every local entry removed by a suffix truncation, including entries beyond the batch. */
    private static List<RaftLogEntry> removedEntries(
            List<RaftLogEntry> before, long truncatedFromIndex) {
        if (truncatedFromIndex == 0L) {
            return List.of();
        }
        List<RaftLogEntry> removed = new ArrayList<>();
        for (RaftLogEntry entry : before) {
            if (entry.index() >= truncatedFromIndex) {
                removed.add(entry);
            }
        }
        return removed;
    }

    /** Fails closed unless the visible log is exactly {@code expected} and the committed prefix held. */
    private void verifyAfterWrite(
            List<RaftLogEntry> before, List<RaftLogEntry> expected, long commitIndex) {
        List<RaftLogEntry> after = snapshotVisibleLog();
        List<RaftLogEntry> expectedCommitted = committedPrefix(expected, commitIndex);
        List<RaftLogEntry> beforeCommitted = committedPrefix(before, commitIndex);
        if (!expectedCommitted.equals(beforeCommitted)) {
            throw new IllegalStateException("post-write verification failed: the planned log changes the committed prefix [1, "
                    + commitIndex + "]");
        }
        if (!after.equals(expected)) {
            throw new IllegalStateException("post-write verification failed: the visible log is not the expected log");
        }
        List<RaftLogEntry> afterCommitted = committedPrefix(after, commitIndex);
        if (!afterCommitted.equals(beforeCommitted)) {
            throw new IllegalStateException("post-write verification failed: the committed prefix [1, "
                    + commitIndex + "] changed");
        }
    }

    /** Returns the entries of {@code log} whose index is at or below {@code commitIndex}. */
    private static List<RaftLogEntry> committedPrefix(List<RaftLogEntry> log, long commitIndex) {
        List<RaftLogEntry> prefix = new ArrayList<>();
        for (RaftLogEntry entry : log) {
            if (entry.index() <= commitIndex) {
                prefix.add(entry);
            }
        }
        return prefix;
    }

    private static InMemoryRaftLog copyOf(List<RaftLogEntry> entries) {
        InMemoryRaftLog copy = new InMemoryRaftLog();
        for (RaftLogEntry entry : entries) {
            copy.append(entry);
        }
        return copy;
    }

    private static ConflictResolution unchanged(RaftNode.AppendEntriesResponse response) {
        return new ConflictResolution(response, 0L, List.of(), List.of());
    }

    /**
     * Outcome of one successful {@link #resolve} call.
     *
     * <p>{@code removed} contains the <strong>uncommitted</strong> entries of the visible log that
     * were replaced by the batch, in ascending index order. It includes local entries beyond the
     * batch when Raft truncates the whole diverging suffix. {@code appended} contains only the
     * entries really written. Both lists are immutable snapshots.
     *
     * <p>A resolution with {@code response.success() == false} reports no change: every field is
     * zero or empty. When {@code success} is true, {@code truncatedFromIndex == 0} exactly when
     * {@code removed} is empty; otherwise the first removed entry has index
     * {@code truncatedFromIndex}, and both {@code removed} and {@code appended} are contiguous.
     *
     * @param response           response created by the D07 appender
     * @param truncatedFromIndex first removed index, or 0 when nothing was removed
     * @param removed            uncommitted local entries replaced by the batch, ascending
     * @param appended           entries actually written, contiguous
     */
    public record ConflictResolution(
            RaftNode.AppendEntriesResponse response,
            long truncatedFromIndex,
            List<RaftLogEntry> removed,
            List<RaftLogEntry> appended) {

        public ConflictResolution {
            Objects.requireNonNull(response, "response");
            removed = List.copyOf(removed);
            appended = List.copyOf(appended);

            if (!response.success()) {
                if (truncatedFromIndex != 0L || !removed.isEmpty() || !appended.isEmpty()) {
                    throw new IllegalArgumentException(
                            "an unsuccessful response must not report truncation, removed or appended entries");
                }
            } else {
                if (truncatedFromIndex < 0L) {
                    throw new IllegalArgumentException(
                            "truncatedFromIndex must be >= 0 but was " + truncatedFromIndex);
                }
                if (truncatedFromIndex == 0L && !removed.isEmpty()) {
                    throw new IllegalArgumentException("removed entries require a non-zero truncatedFromIndex");
                }
                if (truncatedFromIndex != 0L && removed.isEmpty()) {
                    throw new IllegalArgumentException("truncatedFromIndex " + truncatedFromIndex
                            + " requires at least one removed entry");
                }
                if (!removed.isEmpty()) {
                    if (removed.get(0).index() != truncatedFromIndex) {
                        throw new IllegalArgumentException("the first removed entry must have index "
                                + truncatedFromIndex + " but was " + removed.get(0).index());
                    }
                    requireContiguous(removed, "removed");
                }
                requireContiguous(appended, "appended");
            }
        }

        private static void requireContiguous(List<RaftLogEntry> entries, String name) {
            for (int position = 1; position < entries.size(); position++) {
                long expectedIndex = Math.addExact(entries.get(position - 1).index(), 1L);
                if (entries.get(position).index() != expectedIndex) {
                    throw new IllegalArgumentException(name + " entries must be contiguous: expected index "
                            + expectedIndex + " at position " + position + " but was "
                            + entries.get(position).index());
                }
            }
        }
    }
}
