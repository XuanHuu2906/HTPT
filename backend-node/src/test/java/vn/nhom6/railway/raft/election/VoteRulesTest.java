package vn.nhom6.railway.raft.election;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/** Pure in-memory tests: no durable vote, RPC, election timer or live cluster. */
class VoteRulesTest {

    @Test
    void freshNodeGrantsFirstVoteWithEmptyLog() {
        RaftState state = new RaftState();

        assertTrue(VoteRules.grantVote(state, 1, "node-2", 0, 0));

        assertEquals(1, state.currentTerm());
        assertEquals("node-2", state.votedFor());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @ParameterizedTest
    @CsvSource({"1", "3", "4"})
    void staleRequestCannotChangeTermVoteOrLeaderHint(long candidateTerm) {
        RaftState state = RaftState.restore(5, "node-4", List.of(), 0);
        state.becomeFollower(5, "node-4");

        assertFalse(VoteRules.grantVote(state, candidateTerm, "node-2", 100, candidateTerm));

        assertEquals(5, state.currentTerm());
        assertEquals("node-4", state.votedFor());
        assertEquals("node-4", state.leaderId());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @ParameterizedTest
    @CsvSource({
        "0, 0, false",    // empty candidate versus non-empty local log
        "100, 2, false",  // longer log cannot compensate for an older last term
        "2, 3, false",    // same last term, shorter log
        "3, 3, true",     // equal log
        "4, 3, true",     // same last term, longer log
        "1, 4, true",     // newer last term wins even with a shorter log
        "9223372036854775807, 3, true"
    })
    void comparesLastLogTermBeforeLastLogIndex(long index, long term, boolean expected) {
        RaftState state = stateWithLog(null);
        List<RaftLogEntry> originalLog = state.entries();

        assertEquals(expected, VoteRules.grantVote(state, 5, "node-2", index, term));

        assertEquals(5, state.currentTerm());
        assertEquals(expected ? "node-2" : null, state.votedFor());
        assertEquals(originalLog, state.entries());
        assertEquals(1, state.commitIndex());
        assertEquals(1, state.lastApplied());
    }

    @Test
    void grantsNonEmptyCandidateLogOnAnEmptyNode() {
        assertTrue(VoteRules.grantVote(new RaftState(), 5, "node-3", 1, 4));
    }

    @Test
    void repeatVoteIsIdempotentButAnotherCandidateCannotTakeIt() {
        RaftState state = stateWithLog(null);

        assertTrue(VoteRules.grantVote(state, 5, "node-2", 3, 3));
        assertTrue(VoteRules.grantVote(state, 5, new String("node-2"), 3, 3));
        assertFalse(VoteRules.grantVote(state, 5, "node-3", 100, 5));
        assertTrue(VoteRules.grantVote(state, 5, "node-2", 3, 3));

        assertEquals(5, state.currentTerm());
        assertEquals("node-2", state.votedFor());
    }

    @Test
    void sameCandidateStillNeedsAnUpToDateLog() {
        RaftState state = stateWithLog("node-2");

        assertFalse(VoteRules.grantVote(state, 5, "node-2", 2, 3));

        assertEquals("node-2", state.votedFor());
    }

    @Test
    void rejectedOldLogDoesNotConsumeTheVote() {
        RaftState state = stateWithLog(null);

        assertFalse(VoteRules.grantVote(state, 5, "node-2", 100, 2));
        assertNull(state.votedFor());
        assertTrue(VoteRules.grantVote(state, 5, "node-3", 3, 3));
        assertEquals("node-3", state.votedFor());
    }

    @ParameterizedTest
    @CsvSource({"FOLLOWER", "CANDIDATE", "LEADER"})
    void higherTermClearsOldVoteAndStepsDownEvenWhenLogIsRejected(Role role) {
        RaftState state = stateWithLog("node-1");
        if (role != Role.FOLLOWER) {
            state.startElection("node-1");
            if (role == Role.LEADER) {
                state.becomeLeader(Set.of("node-2", "node-3", "node-4", "node-5"));
            }
        } else {
            state.becomeFollower(5, "node-1");
        }
        assertEquals(role, state.role());
        long nextTerm = state.currentTerm() + 1;
        List<RaftLogEntry> originalLog = state.entries();

        assertFalse(VoteRules.grantVote(state, nextTerm, "node-2", 100, 2));

        assertEquals(nextTerm, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(Role.FOLLOWER, state.role());
        assertNull(state.leaderId());
        assertTrue(state.nextIndex().isEmpty());
        assertTrue(state.matchIndex().isEmpty());
        assertEquals(originalLog, state.entries());
        assertEquals(1, state.commitIndex());
        assertEquals(1, state.lastApplied());
        assertTrue(VoteRules.grantVote(state, nextTerm, "node-3", 3, 3));
        assertEquals("node-3", state.votedFor());
    }

    @Test
    void higherTermCanGrantADifferentCandidateAndRejectDelayedOldRequests() {
        RaftState state = stateWithLog("node-2");

        assertTrue(VoteRules.grantVote(state, 6, "node-3", 3, 3));
        assertFalse(VoteRules.grantVote(state, 5, "node-2", 100, 5));

        assertEquals(6, state.currentTerm());
        assertEquals("node-3", state.votedFor());
    }

    @Test
    void splitVoteKeepsSelfVotesUntilANewTerm() {
        // Two candidates and two followers; the fifth voter is unavailable.
        // This is a deterministic sequence of model calls, not a cluster test.
        RaftState first = new RaftState();
        RaftState second = new RaftState();
        RaftState third = new RaftState();
        RaftState fourth = new RaftState();
        first.startElection("node-1");
        second.startElection("node-2");
        assertTrue(VoteRules.grantVote(third, 1, "node-1", 0, 0));
        assertTrue(VoteRules.grantVote(fourth, 1, "node-2", 0, 0));

        assertFalse(VoteRules.grantVote(first, 1, "node-2", 0, 0));
        assertFalse(VoteRules.grantVote(second, 1, "node-1", 0, 0));
        assertFalse(VoteRules.grantVote(third, 1, "node-2", 0, 0));
        assertFalse(VoteRules.grantVote(fourth, 1, "node-1", 0, 0));
        assertEquals(List.of("node-1", "node-2", "node-1", "node-2"),
                List.of(first.votedFor(), second.votedFor(), third.votedFor(), fourth.votedFor()));
        assertEquals(Role.CANDIDATE, first.role());
        assertEquals(Role.CANDIDATE, second.role());

        first.startElection("node-1");
        assertTrue(VoteRules.grantVote(second, 2, "node-1", 0, 0));
        assertTrue(VoteRules.grantVote(fourth, 2, "node-1", 0, 0));
        assertFalse(VoteRules.grantVote(fourth, 1, "node-2", 0, 0));
        assertEquals(2, fourth.currentTerm());
        assertEquals("node-1", fourth.votedFor());
        assertEquals(Role.FOLLOWER, second.role());
        assertEquals(Role.CANDIDATE, first.role(), "VoteRules does not count votes or elect a leader");
    }

    @Test
    void acceptsMaximumLongTermWithoutOverflow() {
        RaftState state = stateWithLog("node-2");
        assertTrue(VoteRules.grantVote(state, Long.MAX_VALUE, "node-3", 1, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, state.currentTerm());
        assertEquals("node-3", state.votedFor());
    }

    @ParameterizedTest
    @CsvSource({"-1, 0, 0", "0, 0, 0", "6, -1, 1", "6, 1, -1",
        "6, 0, 1", "6, 1, 0", "6, 1, 7"})
    void invalidMetadataDoesNotAdvanceTermOrClearVote(long term, long index, long logTerm) {
        RaftState state = stateWithLog("node-4");
        state.becomeFollower(5, "node-4");

        assertThrows(IllegalArgumentException.class,
                () -> VoteRules.grantVote(state, term, "node-2", index, logTerm));

        assertEquals(5, state.currentTerm());
        assertEquals("node-4", state.votedFor());
        assertEquals("node-4", state.leaderId());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void invalidCandidateIdDoesNotChangeState(String candidateId) {
        RaftState state = stateWithLog("node-4");
        Class<? extends Exception> expected = candidateId == null
                ? NullPointerException.class : IllegalArgumentException.class;
        assertThrows(expected, () -> VoteRules.grantVote(state, 6, candidateId, 3, 3));
        assertEquals(5, state.currentTerm());
        assertEquals("node-4", state.votedFor());
    }

    @Test
    void rejectsMissingState() {
        assertThrows(NullPointerException.class, () -> VoteRules.grantVote(null, 1, "node-2", 0, 0));
    }

    private static RaftState stateWithLog(String votedFor) {
        return RaftState.restore(5, votedFor, List.of(
                new RaftLogEntry(1, 1, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"),
                new RaftLogEntry(2, 3, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"),
                new RaftLogEntry(3, 3, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}")), 1);
    }
}
