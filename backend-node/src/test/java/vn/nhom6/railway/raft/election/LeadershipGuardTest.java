package vn.nhom6.railway.raft.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

class LeadershipGuardTest {

    @Test
    void staleTermCallbackIsIgnoredAfterStepDown() {
        RaftState state = new RaftState();
        state.startElection("node-1");
        state.becomeLeader(Set.of("node-2"));
        LeadershipGuard guard = new LeadershipGuard(state);
        long leaderTerm = guard.captureTerm();
        AtomicInteger callbacks = new AtomicInteger();

        state.becomeFollower(leaderTerm + 1L);

        assertFalse(guard.isCurrent(leaderTerm));
        assertFalse(guard.runIfCurrent(leaderTerm, callbacks::incrementAndGet));
        assertEquals(0, callbacks.get());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @Test
    void currentTermCallbackRunsOnlyWhileStillLeader() {
        RaftState state = new RaftState();
        state.startElection("node-1");
        state.becomeLeader(Set.of("node-2"));
        LeadershipGuard guard = new LeadershipGuard(state);
        AtomicInteger callbacks = new AtomicInteger();

        assertTrue(guard.runIfCurrent(guard.captureTerm(), callbacks::incrementAndGet));
        assertEquals(1, callbacks.get());
    }

    @Test
    void captureTermRejectsNonLeader() {
        LeadershipGuard guard = new LeadershipGuard(new RaftState());

        assertThrows(IllegalStateException.class, guard::captureTerm);
    }
}
