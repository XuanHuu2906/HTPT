package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;

/**
 * In-memory test double of {@link RaftStorage} used only by {@code RaftStorageAdapterTest}.
 *
 * <p>It is <strong>not</strong> durable, <strong>not</strong> JDBC and <strong>not</strong> a
 * PostgreSQL implementation: it keeps the state of one Node in lists so that tests can prove the
 * ordering and fail-closed behaviour of {@link RaftLogRepositoryAdapter}. The real durable storage,
 * {@code JdbcRaftStorage}, belongs to TV2-D12. This fake defines the behaviour that implementation
 * must satisfy:
 * <ul>
 *   <li>{@link #load()} returns a snapshot of a log that is contiguous from index 1;</li>
 *   <li>{@link #replaceSuffix(long, List)} validates its arguments and is atomic: on failure the
 *       stored state is unchanged;</li>
 *   <li>the term never decreases in {@link #saveTermAndVote(long, String)}.</li>
 * </ul>
 *
 * <p>Tests can inject failures with {@link #failNext(RaftStorageException)},
 * {@link #setFailing(boolean)} and {@link #persistThenFailNext()}. The last mode applies the
 * replacement and then reports a failure, simulating the ambiguous outcome where the disk write
 * succeeded but the caller saw an error.
 */
public final class FakeRaftStorage implements RaftStorage {

    private long currentTerm;
    private String votedFor;
    private final List<RaftLogEntry> log = new ArrayList<>();
    private long lastApplied;

    private final List<String> calls = new ArrayList<>();
    private RaftStorageException nextFailure;
    private boolean failing;
    private boolean persistThenFailNext;
    private boolean persistTermAndVoteThenFailNext;
    private Runnable beforeReplaceSuffixHook;

    @Override
    public PersistentState load() throws RaftStorageException {
        calls.add("load");
        takeFailure();
        return new PersistentState(currentTerm, votedFor, List.copyOf(log), lastApplied);
    }

    @Override
    public void saveTermAndVote(long term, String vote) throws RaftStorageException {
        calls.add("saveTermAndVote(" + term + ", " + vote + ")");
        takeFailure();
        if (term < currentTerm) {
            throw new IllegalArgumentException(
                    "stored term must not decrease: current " + currentTerm + ", requested " + term);
        }
        currentTerm = term;
        votedFor = vote;
        if (persistTermAndVoteThenFailNext) {
            persistTermAndVoteThenFailNext = false;
            throw new RaftStorageException("fake: term/vote persisted but reported as failed");
        }
    }

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException {
        Objects.requireNonNull(entries, "entries");
        calls.add("replaceSuffix(" + fromIndex + ", " + entries.size() + ")");

        long storedLastIndex = log.isEmpty() ? 0L : log.get(log.size() - 1).index();
        if (fromIndex < RaftLogEntry.FIRST_INDEX || fromIndex > storedLastIndex + 1L) {
            throw new IllegalArgumentException("fromIndex must be within [1, " + (storedLastIndex + 1L)
                    + "] but was " + fromIndex);
        }
        long expectedIndex = fromIndex;
        for (RaftLogEntry entry : entries) {
            Objects.requireNonNull(entry, "entries must not contain null");
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException("replacement entries must be contiguous from "
                        + fromIndex + "; expected " + expectedIndex + " but was " + entry.index());
            }
            expectedIndex = Math.addExact(expectedIndex, 1L);
        }

        if (beforeReplaceSuffixHook != null) {
            beforeReplaceSuffixHook.run();
        }

        boolean failAfterWrite = persistThenFailNext;
        if (!failAfterWrite) {
            takeFailure();
        }

        List<RaftLogEntry> replaced = new ArrayList<>();
        for (RaftLogEntry entry : log) {
            if (entry.index() < fromIndex) {
                replaced.add(entry);
            }
        }
        replaced.addAll(entries);
        log.clear();
        log.addAll(replaced);

        if (failAfterWrite) {
            persistThenFailNext = false;
            throw new RaftStorageException("fake: replacement persisted but reported as failed");
        }
    }

    @Override
    public long loadLastApplied() throws RaftStorageException {
        calls.add("loadLastApplied");
        takeFailure();
        return lastApplied;
    }

    // ---------------------------------------------------------------------------------------
    // Failure switches
    // ---------------------------------------------------------------------------------------

    /** Makes the next storage call fail once, leaving the stored state unchanged. */
    public void failNext(RaftStorageException failure) {
        this.nextFailure = Objects.requireNonNull(failure, "failure");
    }

    /** Makes every subsequent storage call fail until the flag is cleared. */
    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    /**
     * Makes the next {@link #replaceSuffix(long, List)} validate and apply the replacement and
     * then throw {@link RaftStorageException}, simulating an ambiguous durable-write result.
     */
    public void persistThenFailNext() {
        this.persistThenFailNext = true;
    }

    /** Makes the next successful term/vote write persist, then report an ambiguous failure. */
    public void persistTermAndVoteThenFailNext() {
        this.persistTermAndVoteThenFailNext = true;
    }

    /** Runs a read-only observer immediately before {@link #replaceSuffix} mutates the log. */
    public void setBeforeReplaceSuffixHook(Runnable hook) {
        this.beforeReplaceSuffixHook = hook;
    }

    private void takeFailure() throws RaftStorageException {
        if (nextFailure != null) {
            RaftStorageException failure = nextFailure;
            nextFailure = null;
            throw failure;
        }
        if (failing) {
            throw new RaftStorageException("fake: storage is failing");
        }
    }

    // ---------------------------------------------------------------------------------------
    // Test setup and inspection
    // ---------------------------------------------------------------------------------------

    /** Seeds the whole durable state as if it came from a previous process; not a storage call. */
    public void seedState(long term, String vote, List<RaftLogEntry> entries, long applied) {
        this.currentTerm = term;
        this.votedFor = vote;
        this.log.clear();
        this.log.addAll(entries);
        this.lastApplied = applied;
    }

    /** Seeds the stored log without counting a storage call. */
    public void seedLog(List<RaftLogEntry> entries) {
        this.log.clear();
        this.log.addAll(entries);
    }

    /** Seeds the durable apply checkpoint without counting a storage call. */
    public void seedLastApplied(long applied) {
        this.lastApplied = applied;
    }

    /** Storage calls since construction, in invocation order. */
    public List<String> calls() {
        return List.copyOf(calls);
    }

    /** Snapshot of the currently stored log. */
    public List<RaftLogEntry> storedLog() {
        return List.copyOf(log);
    }

    /** Number of {@link #replaceSuffix(long, List)} invocations, successful or not. */
    public int replaceSuffixCalls() {
        int count = 0;
        for (String call : calls) {
            if (call.startsWith("replaceSuffix(")) {
                count++;
            }
        }
        return count;
    }
}
