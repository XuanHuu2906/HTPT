package vn.nhom6.railway.raft.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure model tests for {@link RaftState} and {@link RaftLogEntry} (SPEC-04 sections 2-6, 10).
 *
 * <p>No database, socket, timer or thread is involved, and no class from the booking package is
 * imported: this proves only the in-memory rules, never Raft replication or a real election.
 */
class RaftCoreModelTest {

    // -------------------------------------------------------------------------------------------
    // RaftLogEntry
    // -------------------------------------------------------------------------------------------

    @Test
    void logEntryRejectsIndexZeroAndNegativeIndex() {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(0L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(-1L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
    }

    @Test
    void logEntryRejectsTermZeroAndNegativeTerm() {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, 0L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, -3L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
    }

    @Test
    void logEntryRejectsUnknownCommandType() {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, 1L, "book_ticket", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, 1L, "BOOK", "{}"));
        assertThrows(NullPointerException.class,
                () -> new RaftLogEntry(1L, 1L, null, "{}"));
    }

    @Test
    void logEntryRejectsNullAndBlankPayload() {
        assertThrows(NullPointerException.class,
                () -> new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, null));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, "   "));
    }

    @Test
    void logEntryAcceptsEveryPersistedCommandTypeAndKeepsPayloadOpaque() {
        RaftLogEntry book = new RaftLogEntry(1L, 4L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"seatId\":\"G01\"}");
        RaftLogEntry cancel = new RaftLogEntry(2L, 4L,
                RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET, "{\"ticketId\":\"t-1\"}");
        RaftLogEntry noOp = new RaftLogEntry(3L, 4L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");

        assertEquals(1L, book.index());
        assertEquals(4L, book.term());
        assertEquals(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, book.commandType());
        assertEquals("{\"seatId\":\"G01\"}", book.commandPayload(), "payload must be stored byte for byte");
        assertEquals(RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET, cancel.commandType());
        assertEquals(RaftLogEntry.COMMAND_TYPE_NO_OP, noOp.commandType());
    }

    @Test
    void logEntryEqualityUsesEveryFieldWhileLogMatchingUsesIndexAndTerm() {
        RaftLogEntry first = new RaftLogEntry(7L, 3L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"a\":1}");
        RaftLogEntry sameValues = new RaftLogEntry(7L, 3L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"a\":1}");
        RaftLogEntry differentPayload = new RaftLogEntry(7L, 3L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"a\":2}");

        assertEquals(first, sameValues);
        assertNotEquals(first, differentPayload);
        assertTrue(first.sameIndexAndTerm(differentPayload));
        assertFalse(first.sameIndexAndTerm(new RaftLogEntry(8L, 3L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"a\":1}")));
    }

    @Test
    void noOpFactoryUsesEmptyJsonObject() {
        RaftLogEntry entry = RaftLogEntry.noOp(5L, 2L);
        assertEquals(RaftLogEntry.COMMAND_TYPE_NO_OP, entry.commandType());
        assertEquals("{}", entry.commandPayload());
    }

    // -------------------------------------------------------------------------------------------
    // Empty log and queries
    // -------------------------------------------------------------------------------------------

    @Test
    void newStateHasEmptyLogAndFollowerDefaults() {
        RaftState state = new RaftState();

        assertEquals(0L, state.lastLogIndex());
        assertEquals(0L, state.lastLogTerm());
        assertEquals(0L, state.termAt(0L), "empty prefix must report (0,0) without a sentinel entry");
        assertTrue(state.entries().isEmpty());
        assertEquals(0L, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(Role.FOLLOWER, state.role());
        assertNull(state.leaderId());
        assertEquals(0L, state.commitIndex());
        assertEquals(0L, state.lastApplied());
        assertTrue(state.nextIndex().isEmpty());
        assertTrue(state.matchIndex().isEmpty());
    }

    @Test
    void entryAndTermLookupsRejectPositionsOutsideTheLog() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L)), 0L);

        assertEquals(1L, state.entryAt(1L).index());
        assertEquals(1L, state.termAt(1L), "termAt(0) is 0 and termAt(lastLogIndex) is the entry term");
        assertEquals(0L, state.termAt(0L));
        assertThrows(IndexOutOfBoundsException.class, () -> state.entryAt(0L));
        assertThrows(IndexOutOfBoundsException.class, () -> state.entryAt(2L));
        assertThrows(IllegalArgumentException.class, () -> state.termAt(-1L));
        assertThrows(IndexOutOfBoundsException.class, () -> state.termAt(9L));
    }

    @Test
    void logSnapshotIsUnmodifiable() {
        RaftState state = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);
        List<RaftLogEntry> snapshot = state.entries();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(entry(2L, 1L)));
        assertEquals(1, state.entries().size(), "an external snapshot must not change the log");
    }

    // -------------------------------------------------------------------------------------------
    // Term and role transitions
    // -------------------------------------------------------------------------------------------

    @Test
    void becomeFollowerRejectsAnOlderTerm() {
        RaftState state = RaftState.restore(5L, null, List.of(entry(1L, 5L)), 0L);

        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(4L));
        assertEquals(5L, state.currentTerm(), "the term must never decrease");
    }

    @Test
    void higherTermClearsVoteAndReturnsToFollower() {
        RaftState state = RaftState.restore(3L, "node-2", List.of(entry(1L, 3L)), 0L);
        state.startElection("node-1");
        assertEquals(Role.CANDIDATE, state.role());
        assertEquals("node-1", state.votedFor());

        state.becomeFollower(5L, "node-3");

        assertEquals(5L, state.currentTerm());
        assertNull(state.votedFor(), "a new term must clear the previous vote");
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals("node-3", state.leaderId());
    }

    @Test
    void equalTermKeepsTheVoteButDropsCandidateOrLeaderRole() {
        RaftState state = RaftState.restore(3L, "node-2", List.of(entry(1L, 3L)), 0L);
        state.startElection("node-1");
        state.becomeFollower(state.currentTerm(), "node-3");

        assertEquals(4L, state.currentTerm());
        assertEquals("node-1", state.votedFor(), "an equal term must not silently vote again");
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals("node-3", state.leaderId());
    }

    @Test
    void becomeFollowerRejectsBlankLeaderHint() {
        RaftState state = new RaftState();
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(1L, " "));
    }

    @Test
    void startElectionIncrementsTermVotesForSelfAndBecomesCandidate() {
        RaftState state = RaftState.restore(2L, "node-2", List.of(entry(1L, 2L)), 0L);

        state.startElection("node-1");

        assertEquals(3L, state.currentTerm());
        assertEquals("node-1", state.votedFor());
        assertEquals(Role.CANDIDATE, state.role());
        assertNull(state.leaderId());
    }

    @Test
    void startElectionRefusesToOverflowTheTerm() {
        RaftState state = RaftState.restore(Long.MAX_VALUE, null, List.of(), 0L);

        assertThrows(ArithmeticException.class, () -> state.startElection("node-1"));
        assertEquals(Long.MAX_VALUE, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @Test
    void startElectionRequiresAnIdAndRejectsALeader() {
        RaftState state = new RaftState();
        assertThrows(NullPointerException.class, () -> state.startElection(null));
        assertThrows(IllegalArgumentException.class, () -> state.startElection("  "));

        RaftState leader = existingLeader(Set.of("node-2"));
        assertEquals(Role.LEADER, leader.role());
        assertThrows(IllegalStateException.class, () -> leader.startElection("node-1"));
    }

    // -------------------------------------------------------------------------------------------
    // RequestVote rules
    // -------------------------------------------------------------------------------------------

    @Test
    void refusesTwoDifferentCandidatesInTheSameTerm() {
        RaftState state = RaftState.restore(1L, null, List.of(), 0L);

        assertTrue(state.grantVote(1L, "node-2", 0L, 0L));
        assertFalse(state.grantVote(1L, "node-3", 0L, 0L), "only one candidate can be voted for per term");
        assertEquals("node-2", state.votedFor());
    }

    @Test
    void grantsTheSameCandidateAgainInTheSameTerm() {
        RaftState state = RaftState.restore(1L, null, List.of(), 0L);

        assertTrue(state.grantVote(1L, "node-2", 0L, 0L));
        assertTrue(state.grantVote(1L, "node-2", 0L, 0L), "a re-sent request from the same candidate is idempotent");
        assertEquals("node-2", state.votedFor());
    }

    @Test
    void refusesAStaleTermWithoutChangingLocalState() {
        RaftState state = RaftState.restore(3L, "node-2", List.of(entry(1L, 2L)), 0L);

        assertFalse(state.grantVote(2L, "node-3", 9L, 2L));
        assertEquals(3L, state.currentTerm());
        assertEquals("node-2", state.votedFor());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @Test
    void refusesCandidateWithLowerLastLogTerm() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 2L)), 0L);

        assertFalse(state.grantVote(3L, "node-2", 9L, 1L), "log term is compared before log index");
        assertEquals(3L, state.currentTerm(), "the term still advances even when the vote is refused");
        assertNull(state.votedFor());
    }

    @Test
    void refusesCandidateWithShorterLogAtTheSameLastLogTerm() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 1L)), 0L);

        assertFalse(state.grantVote(2L, "node-2", 1L, 1L));
        assertNull(state.votedFor());
    }

    @Test
    void grantsCandidateWhoseLogIsAtLeastAsUpToDate() {
        RaftState equalLog = RaftState.restore(2L, null, List.of(entry(1L, 2L)), 0L);
        assertTrue(equalLog.grantVote(2L, "node-2", 1L, 2L));
        assertEquals("node-2", equalLog.votedFor());

        RaftState longerLog = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 1L)), 0L);
        assertTrue(longerLog.grantVote(2L, "node-3", 5L, 1L));

        RaftState higherTerm = RaftState.restore(2L, null, List.of(entry(1L, 2L)), 0L);
        assertTrue(higherTerm.grantVote(2L, "node-4", 1L, 2L), "equal term and equal log is up to date");
    }

    @Test
    void aVoteRequestForANewerTermClearsThePreviousVote() {
        RaftState state = RaftState.restore(1L, "node-2", List.of(), 0L);

        assertTrue(state.grantVote(4L, "node-3", 0L, 0L));
        assertEquals(4L, state.currentTerm());
        assertEquals("node-3", state.votedFor());
    }

    @Test
    void aVoteRequestWithANewerTermStepsDownACandidateOrLeader() {
        RaftState candidate = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);
        candidate.startElection("node-1");
        assertEquals(Role.CANDIDATE, candidate.role());

        assertTrue(candidate.grantVote(candidate.currentTerm() + 1L, "node-2", 1L, 1L));

        assertEquals(Role.FOLLOWER, candidate.role());
        assertEquals(3L, candidate.currentTerm());
        assertEquals("node-2", candidate.votedFor());

        RaftState leader = leaderWithLog(2L, Set.of("node-2"), entry(1L, 1L), entry(2L, 2L));
        assertEquals(Role.LEADER, leader.role());

        leader.grantVote(leader.currentTerm() + 1L, "node-3", 2L, 2L);

        assertEquals(Role.FOLLOWER, leader.role());
        assertTrue(leader.matchIndex().isEmpty(), "leader-volatile state must be dropped on step down");
    }

    @Test
    void rejectsMalformedCandidateLogCoordinates() {
        RaftState state = new RaftState();

        assertThrows(IllegalArgumentException.class, () -> state.grantVote(2L, "node-2", 4L, 0L),
                "a non-zero last log index cannot have term 0");
        assertThrows(IllegalArgumentException.class, () -> state.grantVote(2L, "node-2", 0L, 1L),
                "index 0 cannot carry a non-zero term");
        assertThrows(IllegalArgumentException.class, () -> state.grantVote(2L, "node-2", 3L, 5L),
                "a log term cannot exceed the RPC term");
        assertThrows(IllegalArgumentException.class, () -> state.grantVote(-1L, "node-2", 0L, 0L));
        assertThrows(NullPointerException.class, () -> state.grantVote(1L, null, 0L, 0L));
    }

    // -------------------------------------------------------------------------------------------
    // Append
    // -------------------------------------------------------------------------------------------

    @Test
    void appendRequiresTheNextContiguousIndex() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L)), 0L);

        assertThrows(IllegalArgumentException.class, () -> state.append(entry(3L, 1L)), "index 2 is missing");
        assertThrows(IllegalArgumentException.class, () -> state.append(entry(1L, 1L)), "duplicate index");
        assertEquals(1L, state.lastLogIndex());
    }

    @Test
    void appendRejectsTermAboveCurrentTerm() {
        RaftState state = RaftState.restore(2L, null, List.of(), 0L);
        assertThrows(IllegalArgumentException.class, () -> state.append(entry(1L, 3L)));
        assertEquals(0L, state.lastLogIndex());
    }

    @Test
    void appendRejectsTermRegressionInsideTheLog() {
        RaftState state = RaftState.restore(3L, null, List.of(entry(1L, 2L)), 0L);

        assertThrows(IllegalArgumentException.class, () -> state.append(entry(2L, 1L)));
        assertEquals(1L, state.lastLogIndex());
    }

    @Test
    void appendAcceptsNonDecreasingTermsUpToTheCurrentTerm() {
        RaftState state = RaftState.restore(3L, null, List.of(), 0L);

        state.append(entry(1L, 1L));
        state.append(entry(2L, 1L));
        state.append(entry(3L, 3L));

        assertEquals(3L, state.lastLogIndex());
        assertEquals(3L, state.lastLogTerm());
        assertEquals(List.of(1L, 1L, 3L),
                state.entries().stream().map(RaftLogEntry::term).toList());
    }

    @Test
    void appendRejectsNullEntry() {
        RaftState state = new RaftState();
        assertThrows(NullPointerException.class, () -> state.append(null));
    }

    // -------------------------------------------------------------------------------------------
    // Truncate and conflict detection
    // -------------------------------------------------------------------------------------------

    @Test
    void truncateRemovesOnlyTheUncommittedSuffix() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L)), 1L);

        state.truncateSuffixFrom(3L);

        assertEquals(2L, state.lastLogIndex(), "entries before the truncate index stay untouched");
        assertEquals(1L, state.lastLogTerm());
    }

    @Test
    void truncateIsRejectedAtOrBeforeTheCommitIndex() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L)), 2L);

        assertThrows(IllegalStateException.class, () -> state.truncateSuffixFrom(2L),
                "a committed entry must never be truncated");
        assertThrows(IllegalStateException.class, () -> state.truncateSuffixFrom(1L),
                "a committed entry must never be truncated");
        assertEquals(3L, state.lastLogIndex(), "a refused truncate must not modify the log");
    }

    @Test
    void truncateRejectsPositionsOutsideTheLog() {
        RaftState state = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);

        assertThrows(IllegalArgumentException.class, () -> state.truncateSuffixFrom(0L));
        assertThrows(IllegalArgumentException.class, () -> state.truncateSuffixFrom(5L));
    }

    @Test
    void sameIndexAndTermWithDifferentPayloadIsAFailClosedViolation() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L)), 0L);
        RaftLogEntry conflicting = new RaftLogEntry(1L, 1L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"other\"}");

        assertThrows(IllegalStateException.class, () -> state.verifyIncomingEntry(conflicting));
        assertEquals(entry(1L, 1L), state.entryAt(1L), "the local entry must not be overwritten");
    }

    @Test
    void differentTermAtAnUncommittedIndexIsANormalConflict() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 1L)), 1L);

        state.verifyIncomingEntry(new RaftLogEntry(2L, 2L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"req-2b\"}"));
        state.truncateSuffixFrom(2L);

        assertEquals(1L, state.lastLogIndex());
    }

    @Test
    void rewritingACommittedIndexWithAnotherTermIsAFailClosedViolation() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L)), 1L);
        RaftLogEntry foreign = new RaftLogEntry(1L, 2L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"other\"}");

        assertThrows(IllegalStateException.class, () -> state.verifyIncomingEntry(foreign));
        assertEquals(entry(1L, 1L), state.entryAt(1L));
    }

    // -------------------------------------------------------------------------------------------
    // Commit index
    // -------------------------------------------------------------------------------------------

    @Test
    void commitIndexCannotJumpBeyondTheLog() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 2L)), 0L);

        assertThrows(IllegalArgumentException.class, () -> state.advanceCommitIndex(3L));
        assertThrows(IllegalArgumentException.class, () -> state.advanceCommitIndex(-1L));
        assertEquals(0L, state.commitIndex());
    }

    @Test
    void commitIndexIsMonotonicAndIgnoresLateValues() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 2L)), 0L);

        assertTrue(state.advanceCommitIndex(2L));
        assertEquals(2L, state.commitIndex());
        assertFalse(state.advanceCommitIndex(1L), "a late lower value must be ignored, not applied");
        assertFalse(state.advanceCommitIndex(2L), "an equal value is not an advance");
        assertEquals(2L, state.commitIndex());
    }

    // -------------------------------------------------------------------------------------------
    // Apply progress
    // -------------------------------------------------------------------------------------------

    @Test
    void applyMustFollowTheCommitPrefixExactlyOneIndexAtATime() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 2L)), 0L);
        state.advanceCommitIndex(2L);

        assertThrows(IllegalArgumentException.class, () -> state.markApplied(2L), "index 1 was skipped");
        assertThrows(IllegalArgumentException.class, () -> state.markApplied(0L));
        assertEquals(0L, state.lastApplied());

        state.markApplied(1L);
        state.markApplied(2L);
        assertEquals(2L, state.lastApplied());
    }

    @Test
    void applyCannotRunAheadOfTheCommitIndex() {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 2L)), 0L);
        state.advanceCommitIndex(1L);

        assertThrows(IllegalArgumentException.class, () -> state.markApplied(2L));
        assertEquals(0L, state.lastApplied());

        state.markApplied(1L);
        assertEquals(1L, state.lastApplied());
    }

    // -------------------------------------------------------------------------------------------
    // Restore after restart
    // -------------------------------------------------------------------------------------------

    @Test
    void restoreRebuildsTermVoteLogAndCheckpointAsFollower() {
        RaftState state = RaftState.restore(4L, "node-2",
                List.of(entry(1L, 3L), entry(2L, 4L), entry(3L, 4L)), 2L);

        assertEquals(4L, state.currentTerm());
        assertEquals("node-2", state.votedFor());
        assertEquals(3L, state.lastLogIndex());
        assertEquals(4L, state.lastLogTerm());
        assertEquals(Role.FOLLOWER, state.role());
        assertNull(state.leaderId());
        assertEquals(2L, state.commitIndex(), "commitIndex must equal the durable apply checkpoint");
        assertNotEquals(state.lastLogIndex(), state.commitIndex(),
                "commit knowledge beyond the checkpoint must not be invented");
        assertEquals(2L, state.lastApplied());
        assertTrue(state.nextIndex().isEmpty());
    }

    @Test
    void restoreRejectsAGapInThePersistedLog() {
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(3L, null,
                List.of(entry(1L, 1L), entry(3L, 2L)), 0L));
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(3L, null,
                List.of(entry(2L, 1L)), 0L));
    }

    @Test
    void restoreRejectsACheckpointBeyondThePersistedLog() {
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(3L, null,
                List.of(entry(1L, 1L)), 2L), "fail closed when the checkpoint is ahead of the log");
    }

    @Test
    void restoreRejectsCorruptTermAndLogOrdering() {
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(-1L, null, List.of(), 0L));
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(2L, null,
                List.of(entry(1L, 2L), entry(2L, 1L)), 0L), "the log term must not decrease");
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(2L, null,
                List.of(entry(1L, 3L)), 0L), "an entry term above currentTerm means corrupt state");
        assertThrows(IllegalArgumentException.class, () -> RaftState.restore(2L, " ", List.of(), 0L));
        assertThrows(NullPointerException.class, () -> RaftState.restore(2L, null, null, 0L));
    }

    @Test
    void restoredLogIsUnmodifiable() {
        RaftState state = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);
        List<RaftLogEntry> snapshot = state.entries();

        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(entry(2L, 1L)));
        assertEquals(1L, state.lastLogIndex());
    }

    // -------------------------------------------------------------------------------------------
    // Leader-volatile indices
    // -------------------------------------------------------------------------------------------

    @Test
    void becomeLeaderIsOnlyAllowedFromCandidate() {
        RaftState follower = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);
        assertThrows(IllegalStateException.class, () -> follower.becomeLeader(Set.of("node-2")));

        RaftState leader = existingLeader(Set.of("node-2"));
        assertThrows(IllegalStateException.class, () -> leader.becomeLeader(Set.of("node-2")),
                "an existing leader cannot re-initialize leader state as a candidate");
    }

    @Test
    void becomeLeaderInitializesPeerIndices() {
        RaftState state = RaftState.restore(2L, null,
                List.of(entry(1L, 1L), entry(2L, 2L)), 0L);

        assertThrows(IllegalStateException.class, () -> state.becomeLeader(Set.of("node-2")));

        state.startElection("node-1");
        state.becomeLeader(Set.of("node-2", "node-3"));

        assertEquals(Role.LEADER, state.role());
        assertEquals(Map.of("node-2", 3L, "node-3", 3L), state.nextIndex());
        assertEquals(Map.of("node-2", 0L, "node-3", 0L), state.matchIndex());
        assertEquals(3L, state.nextIndexOf("node-2"));
        assertEquals(0L, state.matchIndexOf("node-2"));
        assertThrows(UnsupportedOperationException.class,
                () -> state.nextIndex().put("node-4", 1L), "the exposed map is a snapshot");
    }

    @Test
    void matchIndexNeverDecreasesAndKeepsNextIndexAhead() {
        RaftState state = leaderWithLog(3L, Set.of("node-2"), entry(1L, 1L), entry(2L, 2L), entry(3L, 3L));
        assertEquals(4L, state.nextIndexOf("node-2"));

        state.advanceMatchIndex("node-2", 1L);
        assertEquals(1L, state.matchIndexOf("node-2"));
        state.updateNextIndex("node-2", 2L);
        assertEquals(2L, state.nextIndexOf("node-2"), "nextIndex may drop to matchIndex + 1 on conflict");
        assertThrows(IllegalArgumentException.class, () -> state.updateNextIndex("node-2", 1L));

        state.advanceMatchIndex("node-2", 3L);
        assertEquals(3L, state.matchIndexOf("node-2"));
        assertEquals(4L, state.nextIndexOf("node-2"), "nextIndex is raised with an overtaking matchIndex");

        assertThrows(IllegalArgumentException.class, () -> state.advanceMatchIndex("node-2", 2L));
        assertEquals(3L, state.matchIndexOf("node-2"), "a late lower matchIndex must be refused");
    }

    @Test
    void leaderIndicesStayWithinTheLocalLogAndKnownPeers() {
        RaftState state = leaderWithLog(2L, Set.of("node-2"), entry(1L, 1L), entry(2L, 2L));

        assertThrows(IllegalArgumentException.class, () -> state.advanceMatchIndex("node-2", 3L),
                "matchIndex cannot exceed lastLogIndex");
        assertThrows(IllegalArgumentException.class, () -> state.advanceMatchIndex("node-9", 1L),
                "unknown peers are rejected");
        assertThrows(IllegalArgumentException.class, () -> state.updateNextIndex("node-2", 4L),
                "nextIndex cannot exceed lastLogIndex + 1");
        assertThrows(IllegalArgumentException.class, () -> state.updateNextIndex("node-2", 0L));
        assertThrows(NullPointerException.class, () -> state.updateNextIndex(null, 1L));
    }

    @Test
    void leaderIndicesAreRejectedOnAFollowerAndClearedWhenSteppingDown() {
        RaftState follower = RaftState.restore(1L, null, List.of(entry(1L, 1L)), 0L);
        assertThrows(IllegalStateException.class, () -> follower.advanceMatchIndex("node-2", 1L));
        assertThrows(IllegalStateException.class, () -> follower.updateNextIndex("node-2", 1L));

        RaftState leader = leaderWithLog(2L, Set.of("node-2"), entry(1L, 1L), entry(2L, 2L));
        assertFalse(leader.matchIndex().isEmpty());

        leader.becomeFollower(3L, "node-2");

        assertTrue(leader.matchIndex().isEmpty(), "leader-volatile state must be cleared on step down");
        assertTrue(leader.nextIndex().isEmpty());
        assertEquals(Role.FOLLOWER, leader.role());
    }

    // -------------------------------------------------------------------------------------------
    // Helpers (no booking, persistence or networking type is involved)
    // -------------------------------------------------------------------------------------------

    private static RaftState existingLeader(Set<String> peers) {
        RaftState state = RaftState.restore(2L, null, List.of(entry(1L, 1L), entry(2L, 2L)), 0L);
        state.startElection("node-1");
        state.becomeLeader(peers);
        return state;
    }

    private static RaftState leaderWithLog(long currentTerm, Set<String> peers, RaftLogEntry... entries) {
        RaftState state = RaftState.restore(currentTerm, null, List.of(entries), 0L);
        state.startElection("node-1");
        state.becomeLeader(peers);
        return state;
    }

    private static RaftLogEntry entry(long index, long term) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                "{\"requestId\":\"req-" + index + "\",\"seatId\":\"G01\",\"issuedAt\":\"2026-10-29T10:00:00.000Z\"}");
    }
}
