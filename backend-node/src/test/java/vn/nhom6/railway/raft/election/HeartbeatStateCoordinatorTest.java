package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link HeartbeatStateCoordinator} using FakeRPC and virtual timer.
 * Verifies heartbeat + leaderCommit propagation without a real cluster (TV4-D17).
 */
class HeartbeatStateCoordinatorTest {

    private final TimerTest.FakeScheduler clock = new TimerTest.FakeScheduler();
    private final RaftState state = new RaftState();
    private final List<Sent> sent = new ArrayList<>();
    private final List<Exception> failures = new ArrayList<>();
    private final Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
    private long lastCommitHookValue = 0L;

    private record Sent(String peer, AppendEntriesRequestDto request,
            Consumer<AppendEntriesResponse> reply) { }

    private LeaderHeartbeatScheduler createScheduler() {
        return new LeaderHeartbeatScheduler("node-1", state, clock, ElectionConfig.defaults(),
                (peer, request, reply) -> sent.add(new Sent(peer, request, reply)),
                term -> state.becomeFollower(term), failures::add);
    }

    private HeartbeatStateCoordinator createCoordinator(LeaderHeartbeatScheduler scheduler) {
        return new HeartbeatStateCoordinator("node-1", state, scheduler,
                newCommitIndex -> lastCommitHookValue = newCommitIndex,
                term -> state.becomeFollower(term), failures::add);
    }

    private void promote() {
        state.startElection("node-1");
        state.becomeLeader(peers);
    }

    @Test
    void coordinatorStartsAndStopsWithScheduler() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        coordinator.start();
        assertTrue(coordinator.isActive());
        assertEquals(4, sent.size()); // First heartbeat sent immediately on start()

        clock.advance(150);
        assertEquals(8, sent.size()); // Second heartbeat sent after interval

        coordinator.stop();
        // isActive() checks role, not scheduler state; role is still LEADER
        assertTrue(coordinator.isActive());
        clock.advance(500);
        assertEquals(8, sent.size()); // No more heartbeats after stop
    }

    @Test
    void leaderCommitInHeartbeatReflectsCurrentCommitIndex() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        // Initial commit index is 0
        coordinator.start();
        assertEquals(4, sent.size()); // First heartbeat sent immediately
        for (Sent rpc : sent) {
            assertEquals(0, rpc.request.getLeaderCommit());
        }

        // Advance commit index
        state.append(new RaftLogEntry(1, 1, "NO_OP", "{}"));
        state.advanceCommitIndex(1);
        coordinator.onCommitIndexAdvanced(1);

        // Next heartbeat should carry leaderCommit = 1
        sent.clear();
        clock.advance(150);
        assertEquals(4, sent.size());
        for (Sent rpc : sent) {
            assertEquals(1, rpc.request.getLeaderCommit());
        }
    }

    @Test
    void commitHookNotifiedWhenCommitIndexAdvances() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        coordinator.start();

        assertEquals(0, lastCommitHookValue);
        coordinator.onCommitIndexAdvanced(5);
        assertEquals(5, lastCommitHookValue);

        // Stale notification ignored
        coordinator.onCommitIndexAdvanced(3);
        assertEquals(5, lastCommitHookValue);
    }

    @Test
    void higherTermResponseStopsCoordinatorAndInvokesHandler() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        coordinator.start();
        assertEquals(4, sent.size()); // First heartbeat sent immediately

        // Simulate higher-term response from a peer
        sent.get(0).reply.accept(new AppendEntriesResponse(10, false, 0));

        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(10, state.currentTerm());
        assertFalse(coordinator.isActive());
        clock.advance(500);
        assertEquals(4, sent.size()); // No more heartbeats
    }

    @Test
    void closePreventsFurtherStart() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        coordinator.start();
        coordinator.close();

        assertThrows(IllegalStateException.class, coordinator::start);
        assertFalse(coordinator.isActive());
    }

    @Test
    void coordinatorNotActiveWhenNotLeader() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        // Start as follower
        coordinator.start();
        assertFalse(coordinator.isActive());
        assertEquals(0, sent.size());

        // Become leader
        promote();
        coordinator.start();
        assertTrue(coordinator.isActive());
    }

    @Test
    void heartbeatUsesPerPeerPreviousIndexAndCurrentCommit() {
        LeaderHeartbeatScheduler scheduler = createScheduler();
        HeartbeatStateCoordinator coordinator = createCoordinator(scheduler);

        promote();
        state.append(new RaftLogEntry(1, 1, "NO_OP", "{}"));
        state.advanceCommitIndex(1);
        state.updateNextIndex("node-2", 2);
        coordinator.onCommitIndexAdvanced(1);

        coordinator.start();
        // First heartbeat sent immediately
        assertEquals(4, sent.size());

        for (Sent rpc : sent) {
            long previous = rpc.peer.equals("node-2") ? 1 : 0;
            assertEquals(previous, rpc.request.getPrevLogIndex());
            assertEquals(previous == 1 ? 1 : 0, rpc.request.getPrevLogTerm());
            assertEquals(1, rpc.request.getLeaderCommit());
            assertTrue(rpc.request.getEntries().isEmpty());
        }
    }

    @Test
    void failedHigherTermPersistenceFailsClosed() {
        // The scheduler's higherTerm handler is what gets called on higher-term responses
        List<Exception> schedulerFailures = new ArrayList<>();
        LeaderHeartbeatScheduler scheduler = new LeaderHeartbeatScheduler("node-1", state, clock, ElectionConfig.defaults(),
                (peer, request, reply) -> sent.add(new Sent(peer, request, reply)),
                term -> { throw new IllegalStateException("disk failed"); },
                schedulerFailures::add);
        HeartbeatStateCoordinator coordinator = new HeartbeatStateCoordinator("node-1", state, scheduler,
                newCommitIndex -> { },
                term -> state.becomeFollower(term), // coordinator's handler not used for this path
                failures::add);

        promote();
        coordinator.start();
        assertEquals(4, sent.size()); // First heartbeat sent immediately
        sent.get(0).reply.accept(new AppendEntriesResponse(2, false, 0));

        // Failure is captured by scheduler's handler
        assertEquals(1, schedulerFailures.size());
        assertThrows(IllegalStateException.class, coordinator::start);
    }
}