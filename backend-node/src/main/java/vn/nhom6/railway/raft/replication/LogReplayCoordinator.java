package vn.nhom6.railway.raft.replication;

import java.util.Objects;

import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;

/**
 * Restart-replay coordinator that reconciles the volatile Raft apply progress with the durable
 * state-machine checkpoint before a restarted Node resumes normal Raft work (SPEC-04 sections 6
 * and 8).
 *
 * <p><strong>Why this exists.</strong> {@link RaftState#restore(long, String, java.util.List, long)}
 * initializes {@code commitIndex = lastApplied}, because {@code commitIndex} is volatile and is
 * deliberately not persisted ({@link RaftStorage.PersistentState} carries only
 * {@code currentTerm}, {@code votedFor}, the log and the state-machine checkpoint). After a restart
 * a Node therefore does <em>not</em> know which log entries above {@code lastApplied} were
 * committed until it relearns that from a Leader ({@code APPEND_ENTRIES.leaderCommit}) or becomes
 * Leader and commits again. This coordinator therefore:
 * <ol>
 *   <li>reads the durable state-machine checkpoint through {@link DurableApplyCheckpoint};</li>
 *   <li>derives a committed <strong>floor</strong>: an entry already applied durably is
 *       necessarily committed, so {@code commitFloor = max(state.commitIndex(), durable)} and the
 *       Raft commit index may be raised to that floor;</li>
 *   <li>drains {@code state.lastApplied() + 1 .. commitFloor} through the existing
 *       {@link RaftStateMachineApplier}, which is idempotent per log index (redelivery returns the
 *       stored outcome instead of applying a second time);</li>
 *   <li><strong>never</strong> applies the log tail above {@code commitFloor}: those entries are
 *       not known to be committed and stay untouched in the log.</li>
 * </ol>
 *
 * <p><strong>This class does not replace learning {@code commitIndex} from a Leader.</strong> It
 * only recovers the floor that the durable state machine already proves. The tail is committed
 * later by the normal Raft path; after that the {@link RaftStateMachineApplier} applies it.
 *
 * <p><strong>Fail closed.</strong> The coordinator never guesses and never throws a failure out of
 * {@link #replayOnStartup()}:
 * <ul>
 *   <li>{@code durable > lastLogIndex} means the state machine applied an entry the local log no
 *       longer holds (log loss or corruption) &rarr; {@link Outcome#FAILED_CLOSED};</li>
 *   <li>{@code durable < state.lastApplied()} means the in-memory Raft checkpoint is <em>ahead</em>
 *       of the durable state machine. Continuing would silently skip entries because
 *       {@link RaftState} cannot lower {@code lastApplied}. Rebuild the state with
 *       {@link #restoreAligned(RaftStorage.PersistentState, long)} first &rarr;
 *       {@link Outcome#FAILED_CLOSED};</li>
 *   <li>a fatal applier failure (decode error, index hole, latched failure) &rarr;
 *       {@link Outcome#FAILED_CLOSED} and {@link #failed()} latches;</li>
 *   <li>a transient {@link BookingRepository.StorageException} (and a transient checkpoint read
 *       failure) &rarr; {@link Outcome#RETRYABLE_FAILURE}; the next call retries the same index.</li>
 * </ul>
 *
 * <p><strong>Scope.</strong> No JDBC, no network, no timers, no randomness, no majority
 * computation. {@link RaftState} is mutated only through {@link RaftState#advanceCommitIndex(long)}
 * (directly) and {@code markApplied} (indirectly, by the applier). The durable checkpoint is read
 * only through {@link DurableApplyCheckpoint}; this class must not inspect the database itself.
 *
 * <p><strong>Threading.</strong> Not thread-safe; every call belongs to the single serialized Raft
 * event loop. A reentrant call is ignored instead of recursing.
 *
 * <p><strong>Test scope.</strong> The in-memory suite uses {@code FakeRaftStorage}, an in-memory
 * repository, a real {@link RaftStateMachineApplier}/{@code CommittedCommandApplier} and a fake
 * command sink; the gated JDBC suite restarts the process model on one PostgreSQL schema. Neither
 * kills a real process, database or Docker container.
 */
public final class LogReplayCoordinator {

    /**
     * Read-only view of the durable state-machine apply checkpoint, normally
     * {@code BookingRepository::lastApplied}.
     *
     * <p>It returns the highest log index whose business effect and checkpoint are durably
     * committed. A failure to read it is a transient or fatal coordinator failure depending on the
     * exception type.
     */
    @FunctionalInterface
    public interface DurableApplyCheckpoint {

        /** @return the highest log index durably applied by the state machine */
        long lastApplied() throws Exception;
    }

    /** Coordinator result of one {@link #replayOnStartup()} call. */
    public enum Outcome {
        /** The Raft and durable checkpoints already agree; nothing was applied. */
        NOTHING_TO_REPLAY,
        /** At least one entry was (re)applied and the Raft checkpoint advanced. */
        REPLAYED,
        /** A transient failure stopped the replay before the commit floor; retry the same call. */
        RETRYABLE_FAILURE,
        /** A consistency or fatal applier failure; the coordinator latches and applies nothing more. */
        FAILED_CLOSED
    }

    /**
     * Immutable report of one replay attempt.
     *
     * <p>Fields are read from {@link RaftState} and the durable checkpoint. When the durable
     * checkpoint could not be read, {@code durableLastApplied}, {@code commitFloor} and
     * {@code uncommittedSuffixLength} are {@code -1} because they are unknown.
     *
     * @param outcome                  classification of this attempt
     * @param raftLastApplied          {@code state.lastApplied()} after the attempt
     * @param durableLastApplied       checkpoint read at the start, or {@code -1} when unreadable
     * @param commitFloor              {@code max(state.commitIndex(), durable)} before applying, or
     *                                 {@code -1} when unknown
     * @param lastLogIndex             {@code state.lastLogIndex()}
     * @param replayedFrom             first index this attempt asked the applier to apply, else 0
     * @param replayedTo               last index this attempt advanced to, else 0
     * @param redelivered              number of already durable indices handed to the sink again
     * @param uncommittedSuffixLength  {@code lastLogIndex - commitFloor}, or {@code -1} when unknown
     * @param reason                   human-readable explanation, never {@code null}
     */
    public record ReplayReport(
            Outcome outcome,
            long raftLastApplied,
            long durableLastApplied,
            long commitFloor,
            long lastLogIndex,
            long replayedFrom,
            long replayedTo,
            int redelivered,
            long uncommittedSuffixLength,
            String reason) {

        public ReplayReport {
            Objects.requireNonNull(outcome, "outcome");
            if (reason == null) {
                reason = "";
            }
        }
    }

    private final RaftState state;
    private final RaftStateMachineApplier applier;
    private final DurableApplyCheckpoint checkpoint;

    private boolean replaying;
    private boolean failed;
    private String failureReason = "";

    /**
     * @param state      the Node state whose log and apply progress are reconciled; must be the
     *                   same instance the applier mutates
     * @param applier    the ordered committed-entry applier of this Node
     * @param checkpoint durable state-machine checkpoint, normally
     *                   {@code BookingRepository::lastApplied}
     */
    public LogReplayCoordinator(
            RaftState state,
            RaftStateMachineApplier applier,
            DurableApplyCheckpoint checkpoint) {
        this.state = Objects.requireNonNull(state, "state");
        this.applier = Objects.requireNonNull(applier, "applier");
        this.checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
    }

    /**
     * Reconciles the volatile Raft apply progress with the durable state-machine checkpoint.
     *
     * <p>Idempotent: once both checkpoints and the commit knowledge agree, later calls return
     * {@link Outcome#NOTHING_TO_REPLAY} without touching the state machine. Never throws for an
     * expected failure; failures are reported and, when fatal, latch {@link #failed()}.
     *
     * @return what this attempt observed and changed
     */
    public ReplayReport replayOnStartup() {
        if (failed) {
            return report(Outcome.FAILED_CLOSED, -1L, -1L, 0L, 0L, 0,
                    "coordinator already latched failed closed: " + failureReason);
        }
        if (replaying) {
            return report(Outcome.NOTHING_TO_REPLAY, -1L, -1L, 0L, 0L, 0,
                    "reentrant replayOnStartup call ignored");
        }
        replaying = true;
        try {
            return replay();
        } catch (RuntimeException unexpected) {
            // Defensive only: RaftStateMachineApplier.applyCommitted must not throw, but a broken
            // invariant here must not escape into the Raft loop or leave the coordinator usable.
            return failClosed(-1L, -1L, "unexpected replay failure: " + describe(unexpected));
        } finally {
            replaying = false;
        }
    }

    /** {@code true} after a {@link Outcome#FAILED_CLOSED} result; no further apply is attempted. */
    public boolean failed() {
        return failed;
    }

    private ReplayReport replay() {
        long durable;
        try {
            durable = checkpoint.lastApplied();
        } catch (BookingRepository.StorageException transientFailure) {
            return report(Outcome.RETRYABLE_FAILURE, -1L, -1L, 0L, 0L, 0,
                    "durable apply checkpoint is temporarily unreadable: " + describe(transientFailure));
        } catch (Exception fatalFailure) {
            return failClosed(-1L, -1L,
                    "durable apply checkpoint is unreadable: " + describe(fatalFailure));
        }

        long lastLogIndex = state.lastLogIndex();
        if (durable < RaftState.EMPTY_LOG_INDEX) {
            return failClosed(durable, -1L,
                    "durable apply checkpoint must be >= 0 but was " + durable);
        }
        if (durable > lastLogIndex) {
            return failClosed(durable, -1L,
                    "durable apply checkpoint " + durable + " is beyond lastLogIndex " + lastLogIndex
                            + "; the log was lost or corrupted, refusing to apply anything");
        }

        long raftLastApplied = state.lastApplied();
        if (durable < raftLastApplied) {
            return failClosed(durable, -1L,
                    "Raft lastApplied " + raftLastApplied + " is ahead of the durable state-machine"
                            + " checkpoint " + durable + "; applying further would skip entries."
                            + " Rebuild the state with restoreAligned(persisted, " + durable
                            + ") and start a new coordinator");
        }

        long previousCommit = state.commitIndex();
        long commitFloor = Math.max(previousCommit, durable);
        if (commitFloor > previousCommit) {
            try {
                state.advanceCommitIndex(commitFloor);
            } catch (RuntimeException impossible) {
                return failClosed(durable, commitFloor,
                        "commit floor " + commitFloor + " cannot be applied to the local log: "
                                + describe(impossible));
            }
        }

        long from = state.lastApplied() + 1L;
        long to = commitFloor;
        int plannedRedeliveries = countRedeliveries(from, to, durable);

        try {
            applier.applyCommitted();
        } catch (RuntimeException unexpected) {
            return failClosed(durable, commitFloor,
                    "applier threw instead of failing closed: " + describe(unexpected));
        }

        long appliedTo = state.lastApplied();
        if (applier.failed()) {
            return failClosed(durable, commitFloor,
                    "applier latched a fatal failure while replaying index " + (appliedTo + 1L));
        }
        if (appliedTo < commitFloor) {
            return report(Outcome.RETRYABLE_FAILURE, durable, commitFloor,
                    from, appliedTo, countRedeliveries(from, appliedTo, durable),
                    "transient apply failure stopped replay at index " + (appliedTo + 1L)
                            + "; retry with the same coordinator");
        }

        long uncommittedSuffix = lastLogIndex - commitFloor;
        if (appliedTo == raftLastApplied) {
            return report(Outcome.NOTHING_TO_REPLAY, durable, commitFloor, 0L, 0L, 0,
                    "Raft and durable checkpoints already agree at " + appliedTo);
        }
        int redelivered = plannedRedeliveries;
        return report(Outcome.REPLAYED, durable, commitFloor, from, appliedTo, redelivered,
                "replayed committed range [" + from + ", " + appliedTo + "]; "
                        + uncommittedSuffix + " uncommitted log entries left untouched");
    }

    /**
     * Rebuilds the state from persisted data using the durable state-machine checkpoint as the
     * authoritative {@code lastApplied}.
     *
     * <p>This is the documented recovery when {@code durable < persisted.lastApplied()}: the
     * persisted Raft checkpoint is ahead of the state machine, so rebuilding with the durable value
     * is required before replay. The rebuilt state always has
     * {@code lastApplied = commitIndex = durableLastApplied} and role {@code FOLLOWER}.
     *
     * @throws IllegalArgumentException when the checkpoint is negative or beyond the persisted log
     */
    public static RaftState restoreAligned(
            RaftStorage.PersistentState persisted, long durableLastApplied) {
        Objects.requireNonNull(persisted, "persisted");
        long lastLogIndex = persisted.log().isEmpty()
                ? RaftState.EMPTY_LOG_INDEX
                : persisted.log().getLast().index();
        if (durableLastApplied < RaftState.EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "durableLastApplied must be >= 0 but was " + durableLastApplied);
        }
        if (durableLastApplied > lastLogIndex) {
            throw new IllegalArgumentException("durableLastApplied " + durableLastApplied
                    + " is beyond the persisted lastLogIndex " + lastLogIndex);
        }
        return RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), durableLastApplied);
    }

    private int countRedeliveries(long from, long to, long durable) {
        if (to < from || from < 1L) {
            return 0;
        }
        long lastRedelivered = Math.min(to, durable);
        if (lastRedelivered < from) {
            return 0;
        }
        long count = lastRedelivered - from + 1L;
        return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
    }

    private ReplayReport failClosed(long durable, long commitFloor, String reason) {
        failed = true;
        failureReason = reason;
        long suffix = commitFloor >= 0L ? Math.max(0L, state.lastLogIndex() - commitFloor) : -1L;
        return report(Outcome.FAILED_CLOSED, durable, commitFloor, 0L, 0L, 0, reason, suffix);
    }

    private ReplayReport report(
            Outcome outcome,
            long durable,
            long commitFloor,
            long replayedFrom,
            long replayedTo,
            int redelivered,
            String reason) {
        return report(outcome, durable, commitFloor, replayedFrom, replayedTo, redelivered, reason,
                commitFloor >= 0L ? Math.max(0L, state.lastLogIndex() - commitFloor) : -1L);
    }

    private ReplayReport report(
            Outcome outcome,
            long durable,
            long commitFloor,
            long replayedFrom,
            long replayedTo,
            int redelivered,
            String reason,
            long uncommittedSuffixLength) {
        return new ReplayReport(outcome, state.lastApplied(), durable, commitFloor,
                state.lastLogIndex(), replayedFrom, replayedTo, redelivered,
                uncommittedSuffixLength, reason);
    }

    private static String describe(Throwable failure) {
        String message = failure.getMessage();
        return failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
