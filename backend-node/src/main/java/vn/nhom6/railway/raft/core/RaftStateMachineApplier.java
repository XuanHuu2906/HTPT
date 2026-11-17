package vn.nhom6.railway.raft.core;

import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;

/**
 * Applies the committed Raft prefix into the Booking state machine in strict log order
 * (SPEC-04 sections 5 and 6).
 *
 * <p><strong>Pull, never push.</strong> A {@link RaftNode.CommitListener} callback is only a
 * trigger: {@link #onCommitted(RaftLogEntry)} ignores the entry it receives and drains
 * {@code lastApplied + 1 ... commitIndex} from {@link RaftState}. A dropped, duplicated or lying
 * callback therefore cannot skip or double-apply an entry, and an uncommitted suffix can never be
 * applied: the loop is capped by {@code commitIndex}, never by {@code lastLogIndex}.
 *
 * <p><strong>Milestones are distinct.</strong> Committed is not applied and applied is not
 * {@code CONFIRMED}. The Leader may report {@code CONFIRMED} to a Client only after the Booking
 * apply transaction succeeded ({@link CommittedCommandApplier} reports that result through its
 * {@code ApplyResultListener}). This class never computes a commit index, never calls JDBC or the
 * network and mutates {@link RaftState} only through {@link RaftState#markApplied(long)}, after the
 * sink returned successfully.
 *
 * <p><strong>NO_OP.</strong> A {@code NO_OP} entry also goes through the sink so the durable
 * checkpoint advances past it (SPEC-04 section 6), but it produces no business result: the sink's
 * {@code ApplyResultListener} is not invoked for it ({@link CommittedCommandApplier} contract) and
 * {@link Hooks#onApplied} receives an outcome that is not a {@code BusinessResult}. Applying a
 * {@code NO_OP} must never stall the checkpoint.
 *
 * <p><strong>Fail closed.</strong> Any {@link RuntimeException} other than a transient
 * {@link BookingRepository.StorageException} (decode error, index hole, UNKNOWN result,
 * {@link IllegalStateException}, a failing sink) stops the loop at that index without calling
 * {@code markApplied}, latches {@link #failed()} and refuses every later apply until a new applier
 * is constructed. A transient storage failure also stops at that index, but is <em>not</em> fatal:
 * the next {@link #applyCommitted()} retries the same index. Errors never escape
 * {@link #onCommitted(RaftLogEntry)}, as required by the {@link RaftNode.CommitListener} contract.
 *
 * <p><strong>Replay.</strong> After a crash, {@code RaftState.lastApplied()} may be behind the
 * durable repository checkpoint. Replaying from {@code lastApplied + 1} is safe because
 * {@link CommittedCommandApplier} returns the stored outcome for an already applied index instead
 * of applying it again; {@code markApplied} still walks one index at a time. Such a redelivery is
 * reported to {@link Hooks#onApplied} again for observability, so a hook must not treat it as a new
 * business event without checking the outcome's replay flag.
 *
 * <p><strong>Threading and reentrancy.</strong> Not thread-safe: every call belongs to the single
 * serialized Raft event loop. No locks are used. A nested {@link #applyCommitted()} call from a
 * hook or from the sink is ignored (returns {@code 0}) instead of recursing.
 *
 * <p><strong>Not production-wired yet.</strong> The {@link CommandDecoder} is injected because no
 * production {@code RaftLogEntry -> BookingCommand} codec exists in this repository; ownership of
 * that codec is still open between TV2 (state machine) and TV3 (codec). The applier is not yet
 * registered on {@code PersistentRaftNode}/{@code NodeMain}; wiring the listener and a Leader-side
 * apply after a commit advance belongs to the follow-up integration task.
 */
public final class RaftStateMachineApplier implements RaftNode.CommitListener {

    /**
     * Decodes the opaque Raft entry into a domain command.
     *
     * <p>Injected on purpose: this module must not introduce a second command contract or a
     * production JSON codec before TV2/TV3 agree on one. Tests use a local fixture decoder.
     */
    @FunctionalInterface
    public interface CommandDecoder {

        /**
         * @param entry committed log entry to decode
         * @return the domain command, never {@code null}
         * @throws RuntimeException when the entry cannot be decoded; treated as fatal
         */
        BookingCommand decode(RaftLogEntry entry);
    }

    /**
     * Observability hooks. Both are optional and default to no-ops. A hook must not throw and must
     * not call {@link #applyCommitted()} recursively; a throwing hook is swallowed so that a
     * listener failure cannot break the Raft loop.
     */
    public interface Hooks {

        /**
         * Called after the entry was applied and reported as applied by the sink.
         *
         * @param entry   the applied entry
         * @param outcome the sink outcome; empty when the sink replayed a non-business entry,
         *                {@code NoOpApplied}/{@code RequestIdConflict} for a non-business entry,
         *                {@code BusinessResult} only for a real BOOK/CANCEL result
         */
        default void onApplied(RaftLogEntry entry, Optional<BookingStateMachine.ApplyOutcome> outcome) {
        }

        /**
         * Called when the loop stopped at {@code entry}.
         *
         * @param entry   the entry that failed, or {@code null} when the committed index is missing
         *                from the local log (a fail-closed log integrity error)
         * @param failure the failure that stopped the loop
         * @param fatal   {@code true} when the applier latched {@link #failed()}; {@code false} for a
         *                transient {@link BookingRepository.StorageException} that may be retried
         */
        default void onApplyFailure(RaftLogEntry entry, RuntimeException failure, boolean fatal) {
        }
    }

    private static final Hooks NO_HOOKS = new Hooks() {
    };

    private final RaftState state;
    private final CommandDecoder decoder;
    private final CommittedCommandApplier.CommitListener sink;
    private final Hooks hooks;

    private boolean applying;
    private boolean failed;

    /** Creates an applier without observability hooks. */
    public RaftStateMachineApplier(
            RaftState state,
            CommandDecoder decoder,
            CommittedCommandApplier.CommitListener sink) {
        this(state, decoder, sink, NO_HOOKS);
    }

    /**
     * @param state   the Raft state of this Node, sharing its log, {@code commitIndex} and
     *                {@code lastApplied}; must be the same instance the Node mutates
     * @param decoder produces the domain command for one entry
     * @param sink    usually {@code new CommittedCommandApplier(repository, resultListener)
     *                .commitListener()}; the sink is the only durable apply boundary
     * @param hooks   optional observability hooks
     */
    public RaftStateMachineApplier(
            RaftState state,
            CommandDecoder decoder,
            CommittedCommandApplier.CommitListener sink,
            Hooks hooks) {
        this.state = Objects.requireNonNull(state, "state");
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.hooks = Objects.requireNonNull(hooks, "hooks");
    }

    /**
     * Applies every committed but not yet applied entry, from {@code lastApplied + 1} through
     * {@code commitIndex}, stopping on the first failure.
     *
     * <p>An entry is marked applied only after the sink returned successfully. NO_OP entries are
     * applied too, so the checkpoint does not stop before them. A nested call is ignored.
     *
     * @return number of entries whose apply advanced {@code lastApplied} during this call; {@code 0}
     *         when there is nothing to apply, the applier latched a fatal failure, or the call is
     *         reentrant
     */
    public int applyCommitted() {
        if (failed || applying) {
            return 0;
        }
        applying = true;
        int applied = 0;
        try {
            while (!failed) {
                long index = state.lastApplied() + 1;
                if (index > state.commitIndex()) {
                    break;
                }

                RaftLogEntry entry;
                try {
                    entry = state.entryAt(index);
                } catch (RuntimeException logFailure) {
                    // commitIndex beyond the local log is a log integrity violation: fail closed.
                    fail(null, logFailure);
                    break;
                }

                BookingCommand command;
                try {
                    command = Objects.requireNonNull(
                            decoder.decode(entry), "decoder must not return null");
                } catch (RuntimeException decodeFailure) {
                    fail(entry, decodeFailure);
                    break;
                }

                Optional<BookingStateMachine.ApplyOutcome> outcome;
                try {
                    outcome = Objects.requireNonNull(
                            sink.onCommitted(entry.index(), command),
                            "committed sink must not return null");
                } catch (BookingRepository.StorageException transientFailure) {
                    // The sink rolled back; keep the entry and retry the same index later.
                    reportFailure(entry, transientFailure, false);
                    break;
                } catch (RuntimeException applyFailure) {
                    fail(entry, applyFailure);
                    break;
                }

                try {
                    state.markApplied(entry.index());
                } catch (RuntimeException markFailure) {
                    // The sink succeeded but Raft could not record progress: stop, never skip.
                    fail(entry, markFailure);
                    break;
                }
                applied++;

                try {
                    hooks.onApplied(entry, outcome);
                } catch (RuntimeException hookFailure) {
                    // Applied already happened, but a broken hook must not let the loop continue
                    // touching state it cannot observe correctly.
                    fail(entry, hookFailure);
                    break;
                }
            }
        } finally {
            applying = false;
        }
        return applied;
    }

    /**
     * Drains the committed prefix. The entry argument is deliberately ignored: a callback may be
     * missing, duplicated or lie about its index, and the authoritative source is
     * {@link RaftState}. This method never throws, as required by {@link RaftNode.CommitListener}.
     */
    @Override
    public void onCommitted(RaftLogEntry entry) {
        applyCommitted();
    }

    /** {@code true} once a fatal failure latched; the applier then refuses to apply anything. */
    public boolean failed() {
        return failed;
    }

    private void fail(RaftLogEntry entry, RuntimeException failure) {
        failed = true;
        reportFailure(entry, failure, true);
    }

    private void reportFailure(RaftLogEntry entry, RuntimeException failure, boolean fatal) {
        try {
            hooks.onApplyFailure(entry, failure, fatal);
        } catch (RuntimeException hookFailure) {
            // A listener must never break the Raft event loop (RaftNode.CommitListener contract).
        }
    }
}
