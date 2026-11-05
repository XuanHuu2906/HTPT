package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;

/**
 * Follower-side {@code prevLog} check and log conflict resolution over an {@link InMemoryRaftLog}
 * (SPEC-04 sections 4 and 10, SPEC-03 section 6).
 *
 * <p>The checker answers two questions for one incoming {@code APPEND_ENTRIES} batch:
 * <ol>
 *   <li>{@link #validate(long, long)}: does the local log still hold the entry at
 *       {@code (prevLogIndex, prevLogTerm)}? A mismatch is a normal Raft {@code success=false}
 *       response, not an exception.</li>
 *   <li>{@link #reconcile(long, long, List, long)}: if it matches, remove only the
 *       <strong>uncommitted</strong> conflicting suffix and append the new entries, never touching
 *       the committed prefix. The whole batch is validated before the log is mutated, so a rejected
 *       or malformed batch leaves the log unchanged.</li>
 * </ol>
 *
 * <p>Conflict rules (SPEC-04 section 4):
 * <ul>
 *   <li>An incoming entry with the same {@code (index, term)} as the local entry must carry the
 *       same command; it is then skipped (idempotent retry). A different command for the same
 *       {@code (index, term)} is an invariant violation and fails closed with
 *       {@link IllegalStateException}.</li>
 *   <li>A different term at an uncommitted index is a divergence: the log is truncated from that
 *       index and the incoming entries are appended.</li>
 *   <li>A different term at an index {@code <= commitIndex} means committed data would be
 *       overwritten, which is a fatal violation and fails closed.</li>
 *   <li>A heartbeat ({@code entries} empty) or a batch shorter than the local tail never removes
 *       the tail.</li>
 * </ul>
 *
 * <p><strong>Pure in-memory model.</strong> No I/O, no persistence, no RPC, no timers, no threads,
 * no clock/random/UUID, and no commit or majority decision: {@code commitIndex} is supplied by the
 * caller. Persisting new entries before answering {@code success=true} is the duty of the durable
 * log layer (SPEC-04 section 9).
 *
 * <p><strong>Not thread-safe.</strong> Every method must be called from the single serialized Raft
 * event loop of the Node, the same convention as {@link RaftState} and {@link InMemoryRaftLog}. The
 * checker holds no state of its own beyond the log reference.
 */
public final class LogConsistencyChecker {

    private final InMemoryRaftLog log;

    /**
     * @param log log of this Node, used read-only by {@link #validate} and mutated only by
     *            {@link #reconcile}
     * @throws NullPointerException when {@code log} is {@code null}
     */
    public LogConsistencyChecker(InMemoryRaftLog log) {
        this.log = Objects.requireNonNull(log, "log");
    }

    /** Result of the {@code (prevLogIndex, prevLogTerm)} check (SPEC-04 section 4). */
    public enum PrevLogCheck {
        /** The local log holds the expected entry (or the empty prefix at index 0). */
        MATCH,
        /** {@code prevLogIndex} is beyond the local log, so the Leader must back off. */
        PREV_INDEX_MISSING,
        /** {@code prevLogIndex} exists but with a different term, so the Leader must back off. */
        PREV_TERM_MISMATCH
    }

    /**
     * Checks whether the local log matches {@code (prevLogIndex, prevLogTerm)}.
     *
     * <p>Only reads the log. {@code (prevLogIndex = 0, prevLogTerm = 0)} always matches because the
     * empty prefix is {@code (0, 0)} (SPEC-03 section 6). A negative argument, or
     * {@code prevLogIndex = 0} with a non-zero term, is a schema error and is rejected with
     * {@link IllegalArgumentException}; a valid request that does not match is reported through
     * {@link PrevLogCheck} because it is a normal Raft {@code success=false} response.
     *
     * @param prevLogIndex index of the entry preceding the batch, {@code >= 0}
     * @param prevLogTerm  term expected at {@code prevLogIndex}, {@code 0} only at index 0
     * @return the match verdict, never {@code null}
     * @throws IllegalArgumentException when an argument is negative, or when
     *                                  {@code prevLogIndex == 0} but {@code prevLogTerm != 0}
     */
    public PrevLogCheck validate(long prevLogIndex, long prevLogTerm) {
        if (prevLogIndex < RaftState.EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "prevLogIndex must be >= " + RaftState.EMPTY_LOG_INDEX + " but was " + prevLogIndex);
        }
        if (prevLogTerm < RaftState.EMPTY_LOG_TERM) {
            throw new IllegalArgumentException(
                    "prevLogTerm must be >= " + RaftState.EMPTY_LOG_TERM + " but was " + prevLogTerm);
        }
        if (prevLogIndex == RaftState.EMPTY_LOG_INDEX) {
            if (prevLogTerm != RaftState.EMPTY_LOG_TERM) {
                throw new IllegalArgumentException(
                        "prevLogIndex 0 requires prevLogTerm 0 but was term " + prevLogTerm);
            }
            return PrevLogCheck.MATCH;
        }
        if (prevLogIndex > log.lastIndex()) {
            return PrevLogCheck.PREV_INDEX_MISSING;
        }
        return log.termAt(prevLogIndex) == prevLogTerm
                ? PrevLogCheck.MATCH
                : PrevLogCheck.PREV_TERM_MISMATCH;
    }

    /**
     * Applies one {@code APPEND_ENTRIES} batch to the local log (SPEC-04 section 4).
     *
     * <p>The batch is processed in two phases. Phase 1 only reads: it validates
     * {@code commitIndex}, the {@code prevLog} pair, the shape of the entry chain, and every
     * potential conflict. If phase 1 throws or the {@code prevLog} does not match, the log is
     * untouched. Phase 2 truncates only the diverging uncommitted suffix (if any) and appends the
     * entries that are still missing. A heartbeat, an idempotent re-send, or a batch shorter than
     * the local tail leaves the existing tail in place.
     *
     * <p>The entry chain must start at {@code prevLogIndex + 1}, be contiguous, and have
     * non-decreasing terms whose first term is not lower than {@code prevLogTerm} (SPEC-03
     * section 6). {@code commitIndex} is the caller's authoritative commit point; no entry at
     * {@code index <= commitIndex} is ever truncated or overwritten.
     *
     * <p>If {@link InMemoryRaftLog#append(RaftLogEntry)} were to reject an entry during phase 2,
     * phase 1 would have been wrong: that is a bug and the exception is allowed to propagate. The
     * durability of the truncation and appended entries is outside this class (SPEC-04 section 9).
     *
     * @param prevLogIndex index of the entry preceding the batch, {@code >= 0}
     * @param prevLogTerm  term expected at {@code prevLogIndex}, {@code 0} only at index 0
     * @param entries      entries of the batch, possibly empty for a heartbeat
     * @param commitIndex  highest index known to be committed, within {@code [0, lastIndex()]}
     * @return the accepted/rejected outcome and what was actually changed
     * @throws NullPointerException     when {@code entries} or one of its elements is {@code null}
     * @throws IllegalArgumentException when {@code commitIndex} is out of range, when the
     *                                  {@code prevLog} pair is a schema error, or when the entry
     *                                  chain is malformed
     * @throws IllegalStateException    when the same {@code (index, term)} carries a different
     *                                  command, or when a conflict would rewrite a committed index
     */
    public ReconcileResult reconcile(
            long prevLogIndex, long prevLogTerm, List<RaftLogEntry> entries, long commitIndex) {
        Objects.requireNonNull(entries, "entries");

        // ---- Phase 1: read-only validation; any failure leaves the log untouched. ----
        long lastIndex = log.lastIndex();
        if (commitIndex < RaftState.EMPTY_LOG_INDEX || commitIndex > lastIndex) {
            throw new IllegalArgumentException(
                    "commitIndex must be within [0, " + lastIndex + "] but was " + commitIndex);
        }
        if (validate(prevLogIndex, prevLogTerm) != PrevLogCheck.MATCH) {
            return ReconcileResult.rejected();
        }
        validateChain(prevLogIndex, prevLogTerm, entries);

        long divergenceIndex = findDivergence(entries, lastIndex, commitIndex);

        // ---- Phase 2: mutate only the uncommitted suffix that actually diverges. ----
        if (divergenceIndex > RaftState.EMPTY_LOG_INDEX) {
            log.truncateSuffixFrom(divergenceIndex);
        }
        List<RaftLogEntry> appended = new ArrayList<>();
        for (RaftLogEntry entry : entries) {
            if (entry.index() > log.lastIndex()) {
                log.append(entry);
                appended.add(entry);
            }
        }

        return new ReconcileResult(
                true,
                Math.addExact(prevLogIndex, entries.size()),
                divergenceIndex,
                appended);
    }

    /** Validates the shape of the batch without mutating the log (SPEC-03 section 6). */
    private static void validateChain(long prevLogIndex, long prevLogTerm, List<RaftLogEntry> entries) {
        long expectedIndex = Math.addExact(prevLogIndex, 1L);
        long previousTerm = prevLogTerm;
        for (int position = 0; position < entries.size(); position++) {
            RaftLogEntry entry = Objects.requireNonNull(
                    entries.get(position), "entries must not contain null at position " + position);
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException("entry at position " + position + " must have index "
                        + expectedIndex + " (prevLogIndex + 1 + position) but was " + entry.index());
            }
            if (entry.term() < previousTerm) {
                throw new IllegalArgumentException("entry term must not decrease at index " + entry.index()
                        + ": previous term " + previousTerm + ", entry term " + entry.term());
            }
            previousTerm = entry.term();
            expectedIndex = Math.addExact(expectedIndex, 1L);
        }
    }

    /**
     * Returns the first uncommitted index whose local term differs from the incoming entry, or
     * {@code 0} when no such divergence exists. Committed conflicts, and content changes at the
     * same {@code (index, term)}, fail closed (SPEC-04 section 4).
     */
    private long findDivergence(List<RaftLogEntry> entries, long lastIndex, long commitIndex) {
        for (RaftLogEntry incoming : entries) {
            if (incoming.index() > lastIndex) {
                // Everything from here on is new; no local suffix of those indices exists.
                return RaftState.EMPTY_LOG_INDEX;
            }
            RaftLogEntry local = log.get(incoming.index()).orElseThrow(() -> new IllegalStateException(
                    "log lost entry " + incoming.index() + " while it was within lastIndex " + lastIndex));
            if (local.term() == incoming.term()) {
                if (!local.equals(incoming)) {
                    throw new IllegalStateException("same (index, term) with a different command at index "
                            + incoming.index() + ", term " + incoming.term());
                }
                continue;
            }
            if (incoming.index() <= commitIndex) {
                throw new IllegalStateException("incoming term " + incoming.term() + " at committed index "
                        + incoming.index() + " would overwrite committed term " + local.term());
            }
            return incoming.index();
        }
        return RaftState.EMPTY_LOG_INDEX;
    }

    /**
     * Outcome of one {@link #reconcile} call, shaped for the future
     * {@code APPEND_ENTRIES_RESPONSE} (SPEC-03 section 6).
     *
     * <p>On rejection all fields are zero/empty and the local log was not modified. On acceptance
     * {@code matchIndex = prevLogIndex + entries.size()}, {@code truncatedFromIndex} is the first
     * removed index ({@code 0} when nothing was removed), and {@code appended} lists only the
     * entries that were really written (an idempotent re-send yields an empty list).
     *
     * @param accepted           {@code true} when the {@code prevLog} matched and the batch was applied
     * @param matchIndex         {@code prevLogIndex + entries.size()} when accepted, otherwise {@code 0}
     * @param truncatedFromIndex first index removed from the uncommitted suffix, {@code 0} if none
     * @param appended           unmodifiable snapshot of the entries actually appended, in order
     */
    public record ReconcileResult(
            boolean accepted, long matchIndex, long truncatedFromIndex, List<RaftLogEntry> appended) {

        public ReconcileResult {
            appended = List.copyOf(appended);
            if (!accepted && (matchIndex != RaftState.EMPTY_LOG_INDEX
                    || truncatedFromIndex != RaftState.EMPTY_LOG_INDEX
                    || !appended.isEmpty())) {
                throw new IllegalArgumentException(
                        "a rejected result must have matchIndex=0, truncatedFromIndex=0 and no appended entries");
            }
        }

        private static ReconcileResult rejected() {
            return new ReconcileResult(false, RaftState.EMPTY_LOG_INDEX, RaftState.EMPTY_LOG_INDEX, List.of());
        }
    }
}
