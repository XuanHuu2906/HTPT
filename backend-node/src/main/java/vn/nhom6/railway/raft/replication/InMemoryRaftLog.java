package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;

/**
 * In-memory Raft log of one Node: a contiguous, 1-based sequence of immutable
 * {@link RaftLogEntry} values (SPEC-04 sections 2, 4 and 6).
 *
 * <p>This type models only the append/query surface needed by replication:
 * {@link #append(RaftLogEntry)}, {@link #get(long)}, {@link #lastIndex()} and {@link #lastTerm()}.
 * It performs no I/O, no persistence, no truncation, no commit/majority computation, no timers
 * and no threads. Persisting an entry before acknowledging an {@code APPEND_ENTRIES} success is
 * the responsibility of the durable log layer (SPEC-04 section 9).
 *
 * <p>An empty log is {@code (lastIndex = 0, lastTerm = 0)}; entry index 0 is never stored and the
 * first real entry has index 1 (SPEC-03 section 3.0). Every appended entry must extend the log by
 * exactly one index, so indices never repeat and never leave gaps. Terms must not decrease.
 *
 * <p><strong>Not thread-safe.</strong> Every method must be called from the single serialized
 * Raft event loop of the Node, the same convention as {@link RaftState}. This class deliberately
 * uses no locks.
 *
 * <p><strong>D04 scope.</strong> This log is a standalone structure. It is not wired into
 * {@link RaftState}/{@code RaftStorage} yet, so no second competing log mechanism runs inside the
 * Raft model until TV5 reviews the integration. There is intentionally no
 * {@code truncate}/{@code removeFrom}/{@code clear} and no mutation of an appended entry.
 */
public final class InMemoryRaftLog {

    private final List<RaftLogEntry> entries = new ArrayList<>();

    /** Creates an empty log: {@code lastIndex() == 0} and {@code lastTerm() == 0}. */
    public InMemoryRaftLog() {
        // Empty log; no sentinel entry and no I/O.
    }

    /**
     * Appends the next entry of the log.
     *
     * <p>The entry must extend the log by exactly one index, and its term must not be lower than
     * the term of the current last entry (Raft log matching; SPEC-04 section 4). A rejected append
     * leaves the log unchanged.
     *
     * @param entry entry whose {@code index} is {@code lastIndex() + 1}
     * @throws NullPointerException     when {@code entry} is {@code null}
     * @throws IllegalArgumentException when the index is not {@code lastIndex() + 1} (duplicate,
     *                                  gap or backwards) or the term decreases
     * @throws ArithmeticException      when {@code lastIndex()} is {@link Long#MAX_VALUE}
     */
    public void append(RaftLogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        long expectedIndex = Math.addExact(lastIndex(), 1L);
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException(
                    "entry index must be lastIndex + 1 (" + expectedIndex + ") but was " + entry.index());
        }
        if (!entries.isEmpty() && entry.term() < lastTerm()) {
            throw new IllegalArgumentException(
                    "entry term " + entry.term() + " must not be lower than previous lastTerm " + lastTerm());
        }
        entries.add(entry);
    }

    /**
     * Returns the entry stored at {@code index}, or {@link Optional#empty()} when the log holds no
     * entry there. Index 0 is the empty prefix and is therefore not an entry (SPEC-03 section 6);
     * an index beyond {@link #lastIndex()} is a valid "not present yet" query.
     *
     * @throws IllegalArgumentException when {@code index} is negative
     */
    public Optional<RaftLogEntry> get(long index) {
        if (index < RaftState.EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "index must be >= " + RaftState.EMPTY_LOG_INDEX + " but was " + index);
        }
        if (index == RaftState.EMPTY_LOG_INDEX || index > lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) Math.toIntExact(index - 1L)));
    }

    /**
     * Returns the term of the entry at {@code index}, with {@code termAt(0) == 0} for the empty
     * prefix so that {@code (prevLogIndex = 0, prevLogTerm = 0)} checks work without a sentinel
     * entry (SPEC-03 section 6). This mirrors {@link RaftState#termAt(long)}.
     *
     * @throws IllegalArgumentException  when {@code index} is negative
     * @throws IndexOutOfBoundsException when {@code index} is beyond {@link #lastIndex()}
     */
    public long termAt(long index) {
        if (index < RaftState.EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "index must be >= " + RaftState.EMPTY_LOG_INDEX + " but was " + index);
        }
        if (index == RaftState.EMPTY_LOG_INDEX) {
            return RaftState.EMPTY_LOG_TERM;
        }
        if (index > lastIndex()) {
            throw new IndexOutOfBoundsException("index " + index + " is beyond lastIndex " + lastIndex());
        }
        return entries.get((int) Math.toIntExact(index - 1L)).term();
    }

    /** Index of the last entry, or 0 for an empty log. */
    public long lastIndex() {
        return entries.isEmpty() ? RaftState.EMPTY_LOG_INDEX : entries.get(entries.size() - 1).index();
    }

    /** Term of the last entry, or 0 for an empty log. */
    public long lastTerm() {
        return entries.isEmpty() ? RaftState.EMPTY_LOG_TERM : entries.get(entries.size() - 1).term();
    }

    /** Number of entries currently stored. */
    public int size() {
        return entries.size();
    }

    /** {@code true} when no entry has been appended. */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Unmodifiable snapshot of the log, ordered by ascending index. The copy is independent of
     * later appends, so callers cannot mutate or observe aliasing of the internal list.
     */
    public List<RaftLogEntry> entries() {
        return List.copyOf(entries);
    }
}
