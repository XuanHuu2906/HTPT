package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;

/**
 * Leader-side NO_OP read barrier of SPEC-04 section 7.
 *
 * <p><strong>Protocol.</strong> Every authoritative read appends <em>one new</em> {@code NO_OP}
 * entry in the current term, waits until that exact index is committed by a majority
 * ({@link CommitAdvancer}, owned elsewhere) and durably applied by the local state machine, and
 * only then returns its index as {@code readIndex}. The caller may read its database snapshot once
 * {@code lastApplied >= readIndex}. A previous or another reader's {@code NO_OP} is never reused.
 *
 * <p><strong>Safety checks at completion.</strong> The barrier succeeds only when, at the moment it
 * completes, this Node is still the Leader in the same term, the log entry at
 * {@code readIndex} still belongs to that term (it was not overwritten), and
 * {@code commitIndex >= readIndex}. A {@code NO_OP} appended by a Leader that is later isolated in a
 * minority never reaches that point: it stays pending until the caller's deadline and then fails
 * with {@link TimeoutException}; it can never return stale data as a successful barrier.
 *
 * <p><strong>Seams.</strong> {@link Port} is the small adapter over the Raft core and
 * {@link AppliedAwaiter} is the apply-progress seam (in production {@code RaftApplyAdapter::
 * awaitIndex}). The class does not read the database, does not compute commit or apply progress,
 * and never mutates {@code RaftState}. It is thread-safe: the pending set is concurrent, the
 * per-reader completion is a single compare-and-set, and no lock is held while calling the port or
 * the awaiter.
 *
 * <p><strong>Not production-wired.</strong> {@code NodeMain}/{@code MessageDispatcher} still
 * receive a null or fake {@link BookingRequestHandler.ReadBarrier}; connecting this class to the
 * Leader event loop, the commit advancer and the applier is a follow-up integration task. The
 * barrier itself is complete and tested against a real {@code RaftReplicationService} harness.
 */
public final class RaftReadBarrier implements BookingRequestHandler.ReadBarrier {

    /**
     * Read-only view of the Raft core needed by the barrier plus the Leader append operation.
     * Every method must be safe to call from a request thread.
     */
    public interface Port {

        /** Configured voter ID of this Node. */
        String localId();

        /** {@code true} only while this Node is the Leader. */
        boolean isLeader();

        /** Current term of this Node. */
        long currentTerm();

        /** Known Leader hint, which may be empty or stale. */
        Optional<String> leaderId();

        /** Appends one {@code NO_OP} entry ({@code NO_OP}/{@code "{}"}) in the current term. */
        RaftNode.AppendOutcome appendNoOp();

        /** Highest index known to be committed by this Node. */
        long commitIndex();

        /** Term of the local log entry at {@code index}, empty when there is no such entry. */
        OptionalLong termAt(long index);
    }

    /** Apply-progress seam; completes when the local durable {@code lastApplied >= index}. */
    @FunctionalInterface
    public interface AppliedAwaiter {

        CompletionStage<Void> awaitIndex(long index);
    }

    private final Port port;
    private final AppliedAwaiter awaiter;
    private final RaftReplicationService.Scheduler scheduler;
    private final Set<Pending> pending = ConcurrentHashMap.newKeySet();

    public RaftReadBarrier(
            Port port,
            AppliedAwaiter awaiter,
            RaftReplicationService.Scheduler scheduler) {
        this.port = Objects.requireNonNull(port, "port");
        this.awaiter = Objects.requireNonNull(awaiter, "awaiter");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletionStage<BookingRequestHandler.ReadBarrierResult> appendAndAwaitNoOp(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive but was " + timeout);
        }

        if (!port.isLeader()) {
            return CompletableFuture.failedFuture(
                    new BookingRequestHandler.NotLeaderException(port.leaderId().orElse(null)));
        }

        long term = port.currentTerm();
        RaftNode.AppendOutcome outcome;
        try {
            outcome = Objects.requireNonNull(port.appendNoOp(), "port.appendNoOp returned null");
        } catch (RuntimeException appendFailure) {
            return CompletableFuture.failedFuture(new BookingRequestHandler.ReadUnavailableException(
                    "NO_OP append failed: " + appendFailure.getMessage()));
        }

        if (outcome instanceof RaftNode.NotLeader notLeader) {
            return CompletableFuture.failedFuture(
                    new BookingRequestHandler.NotLeaderException(notLeader.leaderHint()));
        }
        if (outcome instanceof RaftNode.StorageFailure storageFailure) {
            return CompletableFuture.failedFuture(new BookingRequestHandler.ReadUnavailableException(
                    "NO_OP append could not be persisted: " + storageFailure.cause().getMessage()));
        }
        if (!(outcome instanceof RaftNode.Accepted accepted)) {
            return CompletableFuture.failedFuture(new BookingRequestHandler.ReadUnavailableException(
                    "unsupported append outcome: " + outcome));
        }
        if (accepted.index() < RaftLogEntry.FIRST_INDEX) {
            return CompletableFuture.failedFuture(new BookingRequestHandler.ReadUnavailableException(
                    "NO_OP append returned an invalid index " + accepted.index()));
        }
        if (accepted.term() != term) {
            // A successful append at a different term means this Node is no longer the Leader of
            // the term it read; the new entry cannot be used as a barrier for this request.
            return CompletableFuture.failedFuture(
                    new BookingRequestHandler.NotLeaderException(port.leaderId().orElse(null)));
        }

        Pending read = new Pending(accepted.index(), accepted.term());
        pending.add(read);

        long timeoutMillis = timeout.toMillis();
        try {
            read.timeout = scheduler.schedule(
                    () -> read.fail(new TimeoutException(
                            "read barrier at index " + read.readIndex + " did not complete within " + timeout)),
                    timeoutMillis <= 0L ? 1L : timeoutMillis);
        } catch (RuntimeException schedulingFailure) {
            read.fail(new BookingRequestHandler.ReadUnavailableException(
                    "read barrier timeout could not be scheduled: " + schedulingFailure.getMessage()));
            return read.result;
        }

        CompletionStage<Void> applied;
        try {
            applied = Objects.requireNonNull(
                    awaiter.awaitIndex(accepted.index()), "awaiter.awaitIndex returned null");
        } catch (RuntimeException awaitFailure) {
            read.fail(new BookingRequestHandler.ReadUnavailableException(
                    "could not wait for NO_OP apply: " + awaitFailure.getMessage()));
            return read.result;
        }

        applied.whenComplete((ignored, failure) -> {
            if (failure != null) {
                read.fail(new BookingRequestHandler.ReadUnavailableException(
                        "NO_OP apply wait failed: " + failure));
                return;
            }
            verify(read);
        });
        return read.result;
    }

    /**
     * Fails every pending barrier with {@link BookingRequestHandler.NotLeaderException}. Call on
     * step-down or when a higher term is observed. Different callers may race; the per-reader
     * compare-and-set makes every barrier settle exactly once.
     */
    public void onLeadershipLost() {
        String leaderHint;
        try {
            leaderHint = port.leaderId().orElse(null);
        } catch (RuntimeException ignored) {
            leaderHint = null;
        }
        BookingRequestHandler.NotLeaderException failure =
                new BookingRequestHandler.NotLeaderException(leaderHint);
        for (Pending read : List.copyOf(pending)) {
            read.fail(failure);
        }
    }

    /** Number of barriers still waiting for commit and apply. */
    public int pendingCount() {
        return pending.size();
    }

    /**
     * Re-checks the barrier preconditions after apply progress. The result is published exactly once
     * and only if the Leader, term and log entry are still valid at completion time.
     */
    private void verify(Pending read) {
        if (read.settled.get()) {
            return;
        }
        Throwable failure = null;
        BookingRequestHandler.ReadBarrierResult result = null;
        try {
            if (!port.isLeader()) {
                failure = new BookingRequestHandler.NotLeaderException(port.leaderId().orElse(null));
            } else if (port.currentTerm() != read.term) {
                failure = new BookingRequestHandler.NotLeaderException(port.leaderId().orElse(null));
            } else {
                OptionalLong entryTerm = Objects.requireNonNull(
                        port.termAt(read.readIndex), "port.termAt returned null");
                if (entryTerm.isEmpty() || entryTerm.getAsLong() != read.term) {
                    failure = new BookingRequestHandler.ReadUnavailableException(
                            "NO_OP at index " + read.readIndex + " is no longer part of term " + read.term);
                } else if (port.commitIndex() < read.readIndex) {
                    failure = new BookingRequestHandler.ReadUnavailableException(
                            "NO_OP at index " + read.readIndex + " is not committed");
                } else {
                    result = new BookingRequestHandler.ReadBarrierResult(read.readIndex, port.localId());
                }
            }
        } catch (RuntimeException verificationFailure) {
            failure = new BookingRequestHandler.ReadUnavailableException(
                    "read barrier verification failed: " + verificationFailure.getMessage());
        }

        if (failure != null) {
            read.fail(failure);
        } else {
            read.succeed(result);
        }
    }

    /** One pending read; settled exactly once by success, timeout, step-down or verification. */
    private final class Pending {

        private final long readIndex;
        private final long term;
        private final CompletableFuture<BookingRequestHandler.ReadBarrierResult> result = new CompletableFuture<>();
        private final AtomicBoolean settled = new AtomicBoolean(false);
        private volatile RaftReplicationService.Scheduler.Cancellable timeout;

        private Pending(long readIndex, long term) {
            this.readIndex = readIndex;
            this.term = term;
        }

        private void succeed(BookingRequestHandler.ReadBarrierResult value) {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            cleanUp();
            result.complete(value);
        }

        private void fail(Throwable failure) {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            cleanUp();
            result.completeExceptionally(failure);
        }

        private void cleanUp() {
            RaftReplicationService.Scheduler.Cancellable scheduled = timeout;
            timeout = null;
            if (scheduled != null) {
                scheduled.cancel();
            }
            pending.remove(this);
        }
    }
}
