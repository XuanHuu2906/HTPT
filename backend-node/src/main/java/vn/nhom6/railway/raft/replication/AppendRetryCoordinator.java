package vn.nhom6.railway.raft.replication;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Coordinates bounded AppendEntries backtracking and catch-up retries (SPEC-04 section 4).
 *
 * <p>Progress remains authoritative in {@link RaftState} through {@link ReplicationTracker}; this
 * coordinator only schedules retries and counts consecutive rejections. It neither changes the
 * AppendEntries contract nor calculates {@code commitIndex}. The sender remains responsible for
 * limiting each RPC batch.
 *
 * <p>All callbacks and state access are serialized on the injected Raft event loop. Scheduler
 * callbacks only enqueue work onto that loop.
 *
 * <p>After {@code onGiveUp}, a peer is not retried automatically until an ACK arrives or the caller
 * explicitly invokes {@code replicate(peerId)}.
 */
public final class AppendRetryCoordinator {

    public interface Hooks {
        void onHigherTerm(long term);

        void onGiveUp(String peerId, String reason);
    }

    private final RaftState state;
    private final ReplicationTracker tracker;
    private final RaftReplicationService sender;
    private final Executor raftLoop;
    private final RaftReplicationService.Scheduler scheduler;
    private final RaftReplicationService.Backoff backoff;
    private final int maxBacktrackAttempts;
    private final Hooks hooks;
    private final Map<String, Integer> backtrackAttempts = new HashMap<>();
    private final Map<String, RaftReplicationService.Scheduler.Cancellable> scheduledRetries = new HashMap<>();
    private final Map<String, Long> retryGeneration = new HashMap<>();
    private final Set<String> givenUp = new HashSet<>();
    private boolean stopped;

    public AppendRetryCoordinator(
            RaftState state,
            ReplicationTracker tracker,
            RaftReplicationService sender,
            Executor raftLoop,
            RaftReplicationService.Scheduler scheduler,
            RaftReplicationService.Backoff backoff,
            int maxBacktrackAttempts,
            Hooks hooks) {
        this.state = Objects.requireNonNull(state, "state");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.raftLoop = Objects.requireNonNull(raftLoop, "raftLoop");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.backoff = Objects.requireNonNull(backoff, "backoff");
        if (maxBacktrackAttempts < 0) {
            throw new IllegalArgumentException("maxBacktrackAttempts must be >= 0");
        }
        this.maxBacktrackAttempts = maxBacktrackAttempts;
        this.hooks = Objects.requireNonNull(hooks, "hooks");
        sender.attachRetryCoordinator(this);
    }

    /**
     * Handles a correlated rejection. A higher term is surfaced before stale-request checks so a
     * delayed reply can still trigger the existing step-down hook.
     */
    public void onRejected(
            String peerId,
            RaftNode.AppendEntriesRequest request,
            RaftNode.AppendEntriesResponse response) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(response, "response");
        requirePeer(peerId);

        if (response.term() > state.currentTerm()) {
            hooks.onHigherTerm(response.term());
            return;
        }
        if (stopped || response.success() || state.role() != Role.LEADER
                || state.currentTerm() != request.term() || response.term() != request.term()) {
            return;
        }

        long expectedNext;
        try {
            expectedNext = Math.addExact(request.prevLogIndex(), 1L);
        } catch (ArithmeticException overflow) {
            return;
        }
        long currentNext = tracker.nextIndexOf(peerId);
        if (expectedNext != currentNext) {
            return;
        }

        long minimumNext = Math.addExact(tracker.matchIndexOf(peerId), 1L);
        if (currentNext <= 1L || currentNext <= minimumNext) {
            giveUp(peerId, "cannot backtrack below nextIndex=" + currentNext
                    + " with matchIndex=" + tracker.matchIndexOf(peerId));
            return;
        }

        int attempts = Math.addExact(backtrackAttempts.getOrDefault(peerId, 0), 1);
        if (attempts > maxBacktrackAttempts) {
            backtrackAttempts.put(peerId, attempts);
            giveUp(peerId, "exceeded maxBacktrackAttempts=" + maxBacktrackAttempts);
            return;
        }

        long newNext = Math.max(minimumNext, Math.max(1L, currentNext - 1L));
        if (!tracker.lowerNextIndex(peerId, currentNext, newNext)) {
            return;
        }
        backtrackAttempts.put(peerId, attempts);
        scheduleRetry(peerId, backoff.delayFor(attempts));
    }

    /** Resets consecutive rejections after a valid ACK and schedules the next bounded batch. */
    public void onAckSuccess(String peerId) {
        requirePeer(peerId);
        if (stopped) {
            return;
        }
        backtrackAttempts.remove(peerId);
        givenUp.remove(peerId);
        cancelRetry(peerId);
        if (state.role() == Role.LEADER && tracker.nextIndexOf(peerId) <= state.lastLogIndex()) {
            enqueueReplication(peerId);
        }
    }

    public void stop() {
        stopped = true;
        for (RaftReplicationService.Scheduler.Cancellable retry : scheduledRetries.values()) {
            if (retry != null) {
                retry.cancel();
            }
        }
        scheduledRetries.clear();
        for (String peer : retryGeneration.keySet()) {
            retryGeneration.put(peer, Math.addExact(retryGeneration.get(peer), 1L));
        }
    }

    private void scheduleRetry(String peerId, long delayMillis) {
        if (stopped || givenUp.contains(peerId)) {
            return;
        }
        cancelRetry(peerId);
        long generation = retryGeneration.getOrDefault(peerId, 0L);
        RaftReplicationService.Scheduler.Cancellable retry = scheduler.schedule(() -> {
            if (stopped || retryGeneration.getOrDefault(peerId, 0L) != generation) {
                return;
            }
            scheduledRetries.remove(peerId);
            enqueueReplication(peerId);
        }, delayMillis);
        scheduledRetries.put(peerId, Objects.requireNonNull(retry, "scheduler returned null"));
    }

    private void enqueueReplication(String peerId) {
        if (stopped) {
            return;
        }
        raftLoop.execute(() -> {
            if (!stopped && state.role() == Role.LEADER) {
                sender.replicate(peerId);
            }
        });
    }

    private void cancelRetry(String peerId) {
        RaftReplicationService.Scheduler.Cancellable retry = scheduledRetries.remove(peerId);
        retryGeneration.put(peerId, Math.addExact(retryGeneration.getOrDefault(peerId, 0L), 1L));
        if (retry != null) {
            retry.cancel();
        }
    }

    private void giveUp(String peerId, String reason) {
        cancelRetry(peerId);
        if (givenUp.add(peerId)) {
            hooks.onGiveUp(peerId, reason);
        }
    }

    private void requirePeer(String peerId) {
        tracker.nextIndexOf(peerId);
    }
}
