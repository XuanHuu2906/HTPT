package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Coordinates heartbeat scheduling and commit propagation for the Leader.
 *
 * <p>This class sits between {@link LeaderHeartbeatScheduler} (TV5-D12) and the replication
 * commit interface (TV4-D16). It ensures:
 * <ul>
 *   <li>Heartbeats are sent periodically with the current {@code leaderCommit} index.</li>
 *   <li>Heartbeats never block append operations: the scheduler uses non-blocking sends.</li>
 *   <li>Leader timers are cancelled correctly when the Leader loses its role (step-down,
 *       higher-term response, or shutdown).</li>
 *   <li>The {@code leaderCommit} value is always the Leader's current commit index at send time.</li>
 * </ul>
 *
 * <p>All methods run on the serialized Raft event loop. The coordinator does not perform
 * majority commit computation or state-machine apply; those remain in the replication
 * module (TV4-D16). This coordinator only ensures the heartbeat RPC carries the correct
 * commit index and that the Leader's volatile state is cleaned up on role loss.
 *
 * <p>Tests use {@code FakeRPC} to verify heartbeat + leaderCommit propagation without
 * requiring a real cluster (TV4-D17).
 */
public final class HeartbeatStateCoordinator implements AutoCloseable {

    private final String localId;
    private final RaftState state;
    private final LeaderHeartbeatScheduler scheduler;
    private final CommitPropagationHook commitHook;
    private final CandidateElection.HigherTermHandler higherTermHandler;
    private final Consumer<Exception> onFailure;
    private volatile boolean closed;
    private long lastKnownCommitIndex = 0L;

    /**
     * Hook for the replication module to notify the coordinator when commitIndex advances.
     *
     * <p>The replication module (TV4) calls this when it advances commitIndex so that
     * subsequent heartbeats carry the updated value.
     */
    @FunctionalInterface
    public interface CommitPropagationHook {
        /**
         * Called when the Leader's commitIndex advances.
         *
         * @param newCommitIndex the new commit index
         */
        void onCommitIndexAdvanced(long newCommitIndex);
    }

    /**
     * Creates a new heartbeat state coordinator.
     *
     * @param localId            this node's ID (must be one of the five voters)
     * @param state              shared Raft state
     * @param scheduler          the heartbeat scheduler to coordinate
     * @param commitHook         hook for replication to notify commit index changes
     * @param higherTermHandler  handler for higher-term responses
     * @param onFailure          failure callback that fails the entire node
     */
    public HeartbeatStateCoordinator(String localId, RaftState state,
            LeaderHeartbeatScheduler scheduler, CommitPropagationHook commitHook,
            CandidateElection.HigherTermHandler higherTermHandler, Consumer<Exception> onFailure) {
        if (!Set.of("node-1", "node-2", "node-3", "node-4", "node-5")
                .contains(Objects.requireNonNull(localId))) {
            throw new IllegalArgumentException("localId must be a configured voter");
        }
        this.localId = localId;
        this.state = Objects.requireNonNull(state);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.commitHook = Objects.requireNonNull(commitHook);
        this.higherTermHandler = Objects.requireNonNull(higherTermHandler);
        this.onFailure = Objects.requireNonNull(onFailure);
    }

    /**
     * Starts or restarts the heartbeat scheduler for the current leadership term.
     *
     * <p>Idempotent within the same term. If the node is not Leader, stops any
     * existing scheduler. Called from {@link CandidateElection#promoteToLeader(Set)}
     * via the {@code onLeader} hook.
     */
    public void start() {
        if (closed) {
            throw new IllegalStateException("coordinator closed");
        }
        if (state.role() != Role.LEADER) {
            stop();
            return;
        }
        long currentTerm = state.currentTerm();
        // If already started for this term, no-op
        if (scheduler != null && !closed) {
            // The scheduler itself checks term/role; we just ensure commit index is fresh
            lastKnownCommitIndex = state.commitIndex();
            commitHook.onCommitIndexAdvanced(lastKnownCommitIndex);
        }
        scheduler.start();
    }

    /**
     * Stops the heartbeat scheduler without closing the coordinator.
     *
     * <p>Called when the Leader steps down (higher term, election loss, or explicit
     * shutdown). The coordinator can be restarted if this node becomes Leader again.
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    /**
     * Notifies the coordinator that the commit index has advanced.
     *
     * <p>Called by the replication module (TV4-D16) when a new entry is committed.
     * This ensures the next heartbeat carries the updated {@code leaderCommit}.
     *
     * @param newCommitIndex the new commit index (must be >= current lastKnownCommitIndex)
     */
    public void onCommitIndexAdvanced(long newCommitIndex) {
        if (closed) {
            return;
        }
        if (newCommitIndex < lastKnownCommitIndex) {
            // Stale notification; ignore. Commit index only advances monotonically.
            return;
        }
        lastKnownCommitIndex = newCommitIndex;
        commitHook.onCommitIndexAdvanced(newCommitIndex);
    }

    /**
     * Handles a higher-term AppendEntries response from a peer.
     *
     * <p>This is called by the heartbeat scheduler when it receives a response with
     * a term greater than the current term. It delegates to the shared higher-term
     * handler which persists the term, steps down, and restarts the election timer.
     *
     * @param response the AppendEntries response with a higher term
     */
    public void onHigherTermResponse(AppendEntriesResponse response) {
        if (closed) {
            return;
        }
        try {
            if (response.term() > state.currentTerm()) {
                stop();
                higherTermHandler.observe(response.term());
            }
        } catch (Exception failure) {
            fail(failure);
        }
    }

    /**
     * Returns the current leaderCommit index that will be sent in the next heartbeat.
     */
    public long currentLeaderCommit() {
        return lastKnownCommitIndex;
    }

    /**
     * Checks if the coordinator is currently active (Leader role, not closed).
     */
    public boolean isActive() {
        return !closed && state.role() == Role.LEADER;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        stop();
    }

    private void fail(Exception failure) {
        close();
        onFailure.accept(failure);
    }
}