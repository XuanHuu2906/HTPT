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

/** Virtual timer and FakeRPC only: does not certify a real five-node cluster. */
class LeaderHeartbeatSchedulerTest {
    private final TimerTest.FakeScheduler clock = new TimerTest.FakeScheduler();
    private final RaftState state = new RaftState();
    private final List<Sent> sent = new ArrayList<>();
    private final List<Exception> failures = new ArrayList<>();
    private final Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
    private final LeaderHeartbeatScheduler scheduler = create(term -> state.becomeFollower(term));

    private record Sent(String peer, AppendEntriesRequestDto request,
            Consumer<AppendEntriesResponse> reply) { }

    private LeaderHeartbeatScheduler create(CandidateElection.HigherTermHandler higherTerm) {
        return new LeaderHeartbeatScheduler("node-1", state, clock, ElectionConfig.defaults(),
                (peer, request, reply) -> sent.add(new Sent(peer, request, reply)),
                higherTerm, failures::add);
    }

    private void promote() {
        state.startElection("node-1");
        state.becomeLeader(peers);
    }

    @Test void sendsImmediatelyAndAtConfiguredIntervalWithIdempotentStart() {
        scheduler.start();
        assertTrue(sent.isEmpty());
        promote();
        scheduler.start();
        scheduler.start();
        assertEquals(4, sent.size());
        assertEquals(peers, Set.copyOf(sent.stream().map(Sent::peer).toList()));
        for (Sent rpc : sent) {
            rpc.request.validate();
            assertTrue(rpc.request.isHeartbeat());
            assertEquals(1, rpc.request.getTerm());
            assertEquals("node-1", rpc.request.getLeaderId());
            assertEquals(0, rpc.request.getPrevLogIndex());
            assertEquals(0, rpc.request.getPrevLogTerm());
        }
        clock.advance(149);
        assertEquals(4, sent.size());
        clock.advance(1);
        assertEquals(8, sent.size());
        clock.advance(300); // Missing responses never stop periodic heartbeats.
        assertEquals(16, sent.size());
        assertTrue(failures.isEmpty());
    }

    @Test void stopRestartAndCloseCancelDeadlinesWithoutOwningExecutor() {
        promote();
        scheduler.start();
        scheduler.stop();
        scheduler.stop();
        clock.advance(500);
        assertEquals(4, sent.size());
        scheduler.start();
        assertEquals(8, sent.size());
        clock.advance(150);
        assertEquals(12, sent.size());
        scheduler.close();
        clock.advance(500);
        assertEquals(12, sent.size());
        assertFalse(clock.isShutdown());
        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test void roleOrTermChangeCancelsOldLeadershipAndAllowsNewStart() {
        promote();
        scheduler.start();
        state.becomeFollower(2);
        clock.advance(150);
        assertEquals(4, sent.size());
        promote();
        scheduler.start();
        assertEquals(8, sent.size());
        assertEquals(3, sent.get(4).request.getTerm());
    }

    @Test void higherTermReplyStopsAndInvokesDurableTransitionHookEvenAfterStop() {
        promote();
        scheduler.start();
        scheduler.stop();
        sent.get(0).reply.accept(new AppendEntriesResponse(5, false, 0));
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(5, state.currentTerm());
        assertNull(state.votedFor());
        clock.advance(500);
        assertEquals(4, sent.size());
        assertTrue(failures.isEmpty());
    }

    @Test void sameAndStaleTermRepliesNeverAdvanceReplicationOrCommit() {
        promote();
        scheduler.start();
        sent.get(0).reply.accept(new AppendEntriesResponse(1, true, 999));
        sent.get(1).reply.accept(new AppendEntriesResponse(0, false, 0));
        assertEquals(Role.LEADER, state.role());
        assertEquals(0, state.commitIndex());
        assertTrue(state.matchIndex().values().stream().allMatch(index -> index == 0));
        clock.advance(150);
        assertEquals(8, sent.size());
    }

    @Test void failedHigherTermPersistenceFailsClosed() {
        var broken = create(term -> { throw new IllegalStateException("disk failed"); });
        promote();
        broken.start();
        sent.get(0).reply.accept(new AppendEntriesResponse(2, false, 0));
        assertEquals(1, failures.size());
        clock.advance(500);
        assertEquals(4, sent.size());
        assertThrows(IllegalStateException.class, broken::start);
    }

    @Test void heartbeatUsesEachPeersPreviousIndexAndCurrentCommit() {
        promote();
        state.append(new RaftLogEntry(1, 1, "NO_OP", "{}"));
        state.advanceCommitIndex(1);
        state.updateNextIndex("node-2", 2);
        scheduler.start();
        for (Sent rpc : sent) {
            long previous = rpc.peer.equals("node-2") ? 1 : 0;
            assertEquals(previous, rpc.request.getPrevLogIndex());
            assertEquals(previous, rpc.request.getPrevLogTerm());
            assertEquals(1, rpc.request.getLeaderCommit());
            assertTrue(rpc.request.getEntries().isEmpty());
        }
    }

    @Test void rejectedSchedulingIsReportedAndStopsScheduler() {
        promote();
        clock.shutdown();
        scheduler.start();
        assertEquals(1, failures.size());
        assertThrows(IllegalStateException.class, scheduler::start);
    }
}
