package vn.nhom6.railway.raft.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.election.VoteRules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for the D02 boundary between {@link RaftNode}, {@link RaftStorage}, the booking
 * state machine and the election rules.
 *
 * <p><strong>Test double only.</strong> Every collaborator used here is a sequential, in-memory
 * fake nested in this file. Nothing exercises real Raft replication, a real election, a majority
 * count of 3/5, PostgreSQL/JDBC, TCP, threads or timers. These tests verify what the D02 contract
 * promises at its boundary and are <em>not</em> evidence that Raft is implemented.
 *
 * <p>{@link FakeRaftNode#simulateCommit(long)} is a harness switch that pretends a quorum already
 * committed an index. It does not implement or verify the SPEC-04 section 5 commit rule.
 *
 * <p>The JSON to {@link BookingCommand} conversion in {@link FixtureCommandCodec} is a test
 * fixture, not the production codec: no production converter between
 * {@code commandType + commandPayload} and {@code BookingCommand} exists yet.
 */
@DisplayName("Raft core contract (test double; không kiểm thử Raft thật)")
class RaftCoreContractTest {

    private static final String SELF_ID = "node-1";
    private static final String LEADER_ID = "node-3";
    private static final String PEER_A = "node-2";
    private static final String PEER_B = "node-4";
    private static final Set<String> PEERS = Set.of(PEER_A, PEER_B);

    private static final String REQ_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQ_2 = "22222222-2222-2222-2222-222222222222";
    private static final Instant ISSUED_AT = Instant.parse("2023-11-14T22:13:20.000Z");
    private static final Instant ISSUED_AT_LATER = Instant.parse("2023-11-14T22:13:30.000Z");

    // ------------------------------------------------------------------------------------------
    // A. Durability before responding (SPEC-04 section 9)
    // ------------------------------------------------------------------------------------------

    @Test
    void grantedVotePersistsTermAndVoteBeforeTheResponseIsReturned() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        RaftNode.RequestVoteResponse response = election.askVote(1L, PEER_A, 0L, 0L);

        assertTrue(response.voteGranted());
        assertEquals(1L, response.term());
        // The grant is already durable when the response value exists: no later write is needed.
        assertEquals(List.of("load", "saveTermAndVote(1, node-2)"), storage.events());
        assertEquals(1L, storage.currentTerm());
        assertEquals(PEER_A, storage.votedFor());
    }

    @Test
    void appendEntriesPersistsTheLogBeforeReportingSuccess() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(1L, null, List.of(), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);

        RaftNode.AppendEntriesResponse response = node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER_ID, 0L, 0L, List.of(noOp(1L, 1L)), 0L));

        assertTrue(response.success());
        assertEquals(1L, response.matchIndex());
        assertEquals(List.of("load", "replaceSuffix(1, 1)"), storage.events());
        assertEquals(List.of(noOp(1L, 1L)), storage.log());
    }

    @Test
    void appendEntriesStorageFailureFailsClosedWithoutAdvancingTheLog() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(1L, null, List.of(), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        storage.failNext(FakeRaftStorage.FailurePoint.REPLACE_SUFFIX);

        assertThrows(RaftStorageException.class, () -> node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER_ID, 0L, 0L, List.of(noOp(1L, 1L)), 0L)));

        assertEquals(0L, node.lastLogIndex());
        assertEquals(0L, node.commitIndex());
        assertTrue(storage.log().isEmpty());
        assertTrue(node.isDegraded());
    }

    @Test
    void appendCommandStorageFailureReturnsStorageFailureWithoutGrowingTheLog() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        storage.failNext(FakeRaftStorage.FailurePoint.REPLACE_SUFFIX);

        RaftNode.AppendOutcome outcome = node.appendCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));

        assertInstanceOf(RaftNode.StorageFailure.class, outcome);
        assertEquals(0L, node.lastLogIndex());
        assertTrue(storage.log().isEmpty());
        assertEquals(0L, node.commitIndex());
    }

    // ------------------------------------------------------------------------------------------
    // B. Vote and term rules (SPEC-04 section 3)
    // ------------------------------------------------------------------------------------------

    @Test
    void aNodeDoesNotVoteForTwoDifferentCandidatesInTheSameTerm() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(1L, null, List.of(), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        assertTrue(election.askVote(1L, PEER_A, 0L, 0L).voteGranted());
        List<String> eventsAfterFirstGrant = storage.events();

        assertFalse(election.askVote(1L, PEER_B, 0L, 0L).voteGranted());
        assertEquals(PEER_A, storage.votedFor());
        assertEquals(eventsAfterFirstGrant, storage.events());

        assertTrue(election.askVote(1L, PEER_A, 0L, 0L).voteGranted());
        assertEquals(PEER_A, storage.votedFor());
    }

    @Test
    void higherTermMakesTheNodeAFollowerAndPersistsTheClearedVote() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, PEER_A, List.of(noOp(1L, 1L), noOp(2L, 2L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        // Higher term but a stale candidate log, so the vote is denied after the term reset.
        RaftNode.RequestVoteResponse response = election.askVote(4L, PEER_B, 1L, 1L);

        assertFalse(response.voteGranted());
        assertEquals(4L, response.term());
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(4L, node.currentTerm());
        assertNull(storage.votedFor());
        assertEquals(List.of("load", "saveTermAndVote(4, null)"), storage.events());
    }

    @Test
    void requestVoteWithTermZeroIsRejectedWithoutPersistingAVote() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        // RaftState.grantVote rejects term 0 before mutating; D02 does not define a response
        // mapping for out-of-contract input, so the fake lets the documented exception escape.
        assertThrows(IllegalArgumentException.class, () -> election.askVote(0L, PEER_A, 0L, 0L));

        assertEquals(0L, node.currentTerm());
        assertNull(storage.votedFor());
        assertEquals(List.of("load"), storage.events());
    }

    @Test
    void candidateWithLowerLastLogTermIsRejected() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 2L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        assertFalse(election.askVote(2L, PEER_A, 5L, 1L).voteGranted());
        assertNull(storage.votedFor());
    }

    @Test
    void candidateWithShorterLogInTheSameTermIsRejected() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 2L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        assertFalse(election.askVote(2L, PEER_A, 1L, 2L).voteGranted());
        assertTrue(election.askVote(2L, PEER_B, 2L, 2L).voteGranted());
        assertEquals(PEER_B, storage.votedFor());
    }

    @Test
    void electionTimeoutPersistsTermAndSelfVoteBeforeAnyElectionResult() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeElection election = new FakeElection(node);

        election.timeout();

        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(1L, node.currentTerm());
        assertEquals(List.of("load", "saveTermAndVote(1, node-1)"), storage.events());
        assertEquals(SELF_ID, storage.votedFor());
        assertEquals(0L, node.lastLogIndex());
    }

    @Test
    void aLeaderDoesNotStartAnotherElection() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        List<String> eventsBeforeTimeout = storage.events();

        assertThrows(IllegalStateException.class, node::onElectionTimeout);

        assertEquals(Role.LEADER, node.role());
        assertEquals(1L, node.currentTerm());
        assertEquals(eventsBeforeTimeout, storage.events());
        assertEquals(storage.votedFor(), SELF_ID);
    }

    // ------------------------------------------------------------------------------------------
    // C. AppendEntries and log matching (SPEC-04 section 4)
    // ------------------------------------------------------------------------------------------

    @Test
    void staleAppendEntriesTermIsRejectedWithZeroMatchIndex() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 2L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        List<RaftLogEntry> logBefore = storage.log();

        RaftNode.AppendEntriesResponse response = node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER_ID, 2L, 2L, List.of(noOp(3L, 1L)), 3L));

        assertFalse(response.success());
        assertEquals(2L, response.term());
        assertEquals(0L, response.matchIndex());
        assertEquals(logBefore, storage.log());
        assertEquals(2L, node.lastLogIndex());
    }

    @Test
    void appendEntriesWithMismatchedPrevLogIsRejectedWithoutTouchingTheLog() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 2L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        List<RaftLogEntry> logBefore = storage.log();

        // Local entry 2 has term 2, so prevLogTerm=1 cannot match.
        RaftNode.AppendEntriesResponse response = node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(2L, LEADER_ID, 2L, 1L, List.of(noOp(3L, 2L)), 0L));

        assertFalse(response.success());
        assertEquals(0L, response.matchIndex());
        assertEquals(logBefore, storage.log());
        assertEquals(2L, node.lastLogIndex());
        assertFalse(storage.events().stream().anyMatch(event -> event.startsWith("replaceSuffix")));
    }

    @Test
    void matchingAppendEntriesAdvancesMatchIndexAndHeartbeatKeepsTheUnmatchedSuffix() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 1L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);

        RaftNode.AppendEntriesResponse first = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 2L, 1L, List.of(noOp(3L, 2L)), 0L));
        assertEquals(3L, first.matchIndex());

        RaftNode.AppendEntriesResponse second = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 2L, 1L, List.of(noOp(3L, 2L), noOp(4L, 2L), noOp(5L, 2L)), 0L));
        assertEquals(5L, second.matchIndex());
        assertEquals(5L, node.lastLogIndex());

        // Heartbeat ending at index 4 must keep the already matched (uncommitted) entry 5.
        RaftNode.AppendEntriesResponse heartbeat = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 4L, 2L, List.of(), 5L));

        assertTrue(heartbeat.success());
        assertEquals(4L, heartbeat.matchIndex());
        assertEquals(5L, node.lastLogIndex());
        assertEquals(5, storage.log().size());
        assertEquals(4L, node.commitIndex());
    }

    @Test
    void conflictingUncommittedSuffixIsTruncatedAndReplaced() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 1L), noOp(4L, 1L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);

        RaftNode.AppendEntriesResponse response = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 2L, 1L, List.of(noOp(3L, 2L), noOp(4L, 2L)), 0L));

        assertTrue(response.success());
        assertEquals(4L, response.matchIndex());
        assertEquals(4L, node.lastLogIndex());
        assertEquals(2L, node.entryAtForTest(3L).term());
        assertEquals(2L, node.entryAtForTest(4L).term());
        assertTrue(storage.events().contains("replaceSuffix(3, 2)"));
    }

    @Test
    void conflictingCommittedEntryIsRefusedInsteadOfBeingOverwritten() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        // The durable apply checkpoint at 3 means the prefix is already committed.
        storage.seed(2L, null,
                List.of(noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 1L), noOp(4L, 1L)), 3L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        assertEquals(3L, node.commitIndex());

        assertThrows(IllegalStateException.class, () -> node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(2L, LEADER_ID, 2L, 1L, List.of(noOp(3L, 2L)), 0L)));

        assertEquals(4L, node.lastLogIndex());
        assertEquals(1L, node.entryAtForTest(3L).term());
        assertFalse(storage.events().stream().anyMatch(event -> event.startsWith("replaceSuffix")));
    }

    @Test
    void sameIndexAndTermWithDifferentPayloadFailsClosed() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 1L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);

        RaftLogEntry differentPayload = bookEntry(
                2L, 1L, REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT);

        assertThrows(IllegalStateException.class, () -> node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(2L, LEADER_ID, 1L, 1L, List.of(differentPayload), 0L)));

        assertEquals(2L, node.lastLogIndex());
        assertEquals(RaftLogEntry.COMMAND_TYPE_NO_OP, node.entryAtForTest(2L).commandType());
        assertFalse(storage.events().stream().anyMatch(event -> event.startsWith("replaceSuffix")));
    }

    @Test
    void leaderCommitAdvancesOnlyUpToTheMatchedIndexAndNeverDecreases() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(2L, null, List.of(noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 1L)), 0L);
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);

        RaftNode.AppendEntriesResponse first = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 1L, 1L, List.of(noOp(2L, 1L), noOp(3L, 1L)), 99L));

        assertTrue(first.success());
        assertEquals(3L, first.matchIndex());
        assertEquals(3L, node.commitIndex());

        // A late, older heartbeat advertises leaderCommit=1; it must not lower commitIndex.
        RaftNode.AppendEntriesResponse stale = node.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                2L, LEADER_ID, 0L, 0L, List.of(), 1L));

        assertTrue(stale.success());
        assertEquals(0L, stale.matchIndex());
        assertEquals(3L, node.commitIndex());
    }

    // ------------------------------------------------------------------------------------------
    // D. Commit is not applied and is not CONFIRMED (SPEC-04 sections 5, 6 and 10)
    // ------------------------------------------------------------------------------------------

    @Test
    void anAcceptedEntryIsNeitherCommittedNorAppliedNorConfirmed() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);

        RaftNode.AppendOutcome outcome = node.appendCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));

        assertInstanceOf(RaftNode.Accepted.class, outcome);
        assertEquals(1L, node.lastLogIndex());
        assertEquals(0L, node.commitIndex());
        assertEquals(0L, node.lastApplied());
        assertEquals(0L, repository.lastApplied());
        assertTrue(adapter.reportedResults().isEmpty());
        assertEquals(0, repository.stateMachine().totalTicketCount());
    }

    @Test
    void committedEntriesReachTheListenerOnceInIncreasingIndexOrder() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        List<Long> deliveredIndexes = new ArrayList<>();
        node.addCommitListener(entry -> deliveredIndexes.add(entry.index()));
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");

        node.simulateCommit(2L);
        assertEquals(List.of(1L, 2L), deliveredIndexes);

        node.simulateCommit(3L);
        assertEquals(List.of(1L, 2L, 3L), deliveredIndexes);

        node.simulateCommit(3L);
        assertEquals(List.of(1L, 2L, 3L), deliveredIndexes);
    }

    @Test
    void theBookingAdapterAppliesOnceAndThenReportsOnApplied() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));

        node.simulateCommit(1L);

        assertEquals(1L, repository.lastApplied());
        assertEquals(1L, node.lastApplied());
        assertEquals(1, adapter.reportedResults().size());
        assertEquals(ApplyResult.Outcome.CONFIRMED, adapter.reportedResults().getFirst().outcome());
    }

    @Test
    void raftInvariantsHoldAfterEveryAppendCommitAndApplyStep() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        node.addCommitListener(new FakeBookingAdapter(node, repository));

        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_2, "G02", "Passenger", "ticket-b", ISSUED_AT_LATER));
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        assertRaftInvariants(node);

        node.simulateCommit(1L);
        assertEquals(1L, repository.lastApplied());
        assertRaftInvariants(node);

        node.simulateCommit(3L);
        assertEquals(3L, repository.lastApplied());
        assertRaftInvariants(node);

        node.simulateCommit(4L);
        assertEquals(4L, repository.lastApplied());
        assertRaftInvariants(node);
        assertEquals(node.lastApplied(), repository.lastApplied());
    }

    @Test
    void onAppliedRejectsSkippedAndUncommittedIndexes() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");

        // Nothing is committed yet: reporting an apply must fail closed.
        assertThrows(IllegalArgumentException.class, () -> node.onApplied(1L));

        node.simulateCommit(2L);
        // Skipping index 1 is not allowed.
        assertThrows(IllegalArgumentException.class, () -> node.onApplied(2L));
        assertEquals(0L, node.lastApplied());

        node.onApplied(1L);
        node.onApplied(2L);
        assertEquals(2L, node.lastApplied());
    }

    @Test
    void noOpEntriesAdvanceTheCheckpointWithoutABusinessResult() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));

        node.simulateCommit(2L);

        assertEquals(2L, repository.lastApplied());
        assertEquals(2L, node.lastApplied());
        assertEquals(1, adapter.reportedResults().size());
        assertInstanceOf(BookingStateMachine.ApplyOutcome.NoOpApplied.class,
                adapter.outcomes().getFirst());
    }

    // ------------------------------------------------------------------------------------------
    // E. Booking fake contract (TV2 logic itself is tested by TV2, not repeated here)
    // ------------------------------------------------------------------------------------------

    @Test
    void bookingTheSameSeatTwiceAppliesBothEntriesAndTheSecondIsRejected() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_2, "G01", "Passenger", "ticket-b", ISSUED_AT_LATER));

        node.simulateCommit(2L);

        assertEquals(2, adapter.reportedResults().size());
        assertEquals(ApplyResult.Outcome.CONFIRMED, adapter.reportedResults().get(0).outcome());
        assertEquals(ApplyResult.Outcome.REJECTED, adapter.reportedResults().get(1).outcome());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, adapter.reportedResults().get(1).reasonCode());
        assertEquals(2L, repository.lastApplied());
        assertEquals(1L, repository.stateMachine().activeTicketCount());
        assertTrue(repository.stateMachine().ticketStatus("ticket-b").isEmpty());
    }

    @Test
    void retryingTheSameRequestIdFromAnotherLeaderIsIdempotent() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));
        // A new Leader may re-append the same request with a different Leader-issued ticketId.
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-new-leader", ISSUED_AT_LATER));

        node.simulateCommit(2L);

        assertEquals(2L, repository.lastApplied());
        assertEquals(1, repository.stateMachine().totalTicketCount());
        assertEquals(1L, repository.stateMachine().activeTicketCount());
        ApplyResult first = ((BookingStateMachine.ApplyOutcome.BusinessResult) adapter.outcomes().get(0))
                .applyResult();
        BookingStateMachine.ApplyOutcome.BusinessResult replay =
                assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, adapter.outcomes().get(1));
        assertTrue(replay.isReplay());
        assertEquals(first, replay.applyResult());
        assertTrue(repository.stateMachine().ticketStatus("ticket-new-leader").isEmpty());
    }

    @Test
    void restartRestoresCommitIndexFromTheApplyCheckpointAndSkipsTheAppliedPrefix() throws Exception {
        List<RaftLogEntry> entries = List.of(
                bookEntry(1L, 1L, REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT),
                noOp(2L, 1L),
                bookEntry(3L, 1L, REQ_2, "G02", "Passenger", "ticket-b", ISSUED_AT_LATER));

        // Durable state: log 1..3 is stored, the apply checkpoint proves prefix 1..2 was applied.
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(1L, null, entries, 2L);

        FakeBookingRepository repository = new FakeBookingRepository();
        repository.applyAtomically(1L, commandOf(entries.get(0)));
        repository.applyAtomically(2L, new BookingCommand.NoOp());
        assertEquals(2L, repository.lastApplied());

        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        assertEquals(2L, node.commitIndex());
        assertEquals(2L, node.lastApplied());
        assertEquals(3L, node.lastLogIndex());

        List<Long> deliveredIndexes = new ArrayList<>();
        node.addCommitListener(entry -> deliveredIndexes.add(entry.index()));
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);

        // Re-announcing the durable checkpoint must not replay the applied prefix.
        node.simulateCommit(2L);
        assertTrue(deliveredIndexes.isEmpty());
        assertEquals(2L, repository.lastApplied());

        node.simulateCommit(3L);
        assertEquals(List.of(3L), deliveredIndexes);
        assertEquals(3L, repository.lastApplied());
        assertEquals(2, repository.stateMachine().totalTicketCount());
        assertEquals("ACTIVE", repository.stateMachine().ticketStatus("ticket-b").orElseThrow());
    }

    @Test
    void redeliveredCommittedEntryIsNotAppliedTwiceNorReportedTwice() throws Exception {
        List<RaftLogEntry> entries = List.of(
                bookEntry(1L, 1L, REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT),
                noOp(2L, 1L));

        // Models a crash after the apply transaction committed (lastApplied=2 is durable) but
        // before the in-memory onApplied observation. Restart restores lastApplied=2, then a
        // duplicate commit notification for index 1 arrives.
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seed(1L, null, entries, 2L);
        FakeBookingRepository repository = new FakeBookingRepository();
        repository.applyAtomically(1L, commandOf(entries.get(0)));
        repository.applyAtomically(2L, new BookingCommand.NoOp());

        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        int ticketsBeforeRedelivery = repository.stateMachine().totalTicketCount();

        adapter.onCommitted(entries.get(0));

        assertEquals(2L, node.lastApplied());
        assertEquals(2L, repository.lastApplied());
        assertEquals(ticketsBeforeRedelivery, repository.stateMachine().totalTicketCount());
        assertTrue(adapter.reportedResults().isEmpty());
    }

    @Test
    void applyFailureKeepsTheEntryForRetryAndNeverReportsConfirmed() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        FakeBookingRepository repository = new FakeBookingRepository();
        FakeBookingAdapter adapter = new FakeBookingAdapter(node, repository);
        node.addCommitListener(adapter);
        node.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(REQ_1, "G01", "Passenger", "ticket-a", ISSUED_AT));

        repository.failNextApply();
        assertThrows(BookingRepository.StorageException.class, () -> node.simulateCommit(1L));

        assertEquals(0L, node.lastApplied());
        assertEquals(1L, node.commitIndex());
        assertEquals(1L, node.lastLogIndex());
        assertTrue(adapter.reportedResults().isEmpty());
        assertTrue(repository.stateMachine().ticketStatus("ticket-a").isEmpty());

        // The committed entry is still in the log, so a retry applies it exactly once.
        node.simulateCommit(1L);
        assertEquals(1L, node.lastApplied());
        assertEquals(1, adapter.reportedResults().size());
        assertEquals(ApplyResult.Outcome.CONFIRMED, adapter.reportedResults().getFirst().outcome());
    }

    // ------------------------------------------------------------------------------------------
    // F. Fail closed
    // ------------------------------------------------------------------------------------------

    @Test
    void afterAStorageFailureTheNodeRefusesFurtherWrites() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        FakeRaftNode node = newLeader(storage);
        storage.failNext(FakeRaftStorage.FailurePoint.REPLACE_SUFFIX);

        assertInstanceOf(RaftNode.StorageFailure.class,
                node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
        assertTrue(node.isDegraded());
        int eventsAfterFailure = storage.events().size();

        // Fail closed: the next write is refused without another storage attempt.
        assertInstanceOf(RaftNode.StorageFailure.class,
                node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
        assertEquals(eventsAfterFailure, storage.events().size());
        assertEquals(0L, node.lastLogIndex());
        assertEquals(0L, node.commitIndex());
    }

    // ------------------------------------------------------------------------------------------
    // Fixtures and assertions
    // ------------------------------------------------------------------------------------------

    private static FakeRaftNode newLeader(FakeRaftStorage storage) throws RaftStorageException {
        FakeRaftNode node = new FakeRaftNode(SELF_ID, storage);
        new FakeElection(node).timeout();
        node.simulateWonElectionForTest(PEERS);
        return node;
    }

    private static RaftLogEntry noOp(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static RaftLogEntry bookEntry(long index, long term, String requestId, String seatId,
            String passengerName, String ticketId, Instant issuedAt) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(requestId, seatId, passengerName, ticketId, issuedAt));
    }

    private static String bookPayload(String requestId, String seatId, String passengerName,
            String ticketId, Instant issuedAt) {
        return "{\"requestId\":\"" + requestId
                + "\",\"tripId\":\"SE-001\""
                + ",\"seatId\":\"" + seatId
                + "\",\"passengerName\":\"" + passengerName
                + "\",\"ticketId\":\"" + ticketId
                + "\",\"issuedAt\":\"" + issuedAt + "\"}";
    }

    private static BookingCommand commandOf(RaftLogEntry entry) {
        return FixtureCommandCodec.toCommand(entry.commandType(), entry.commandPayload());
    }

    private static void assertRaftInvariants(RaftNode node) {
        assertTrue(node.lastApplied() >= 0L, "lastApplied must be >= 0");
        assertTrue(node.lastApplied() <= node.commitIndex(), "lastApplied must be <= commitIndex");
        assertTrue(node.commitIndex() <= node.lastLogIndex(), "commitIndex must be <= lastLogIndex");
    }

    // ------------------------------------------------------------------------------------------
    // Test doubles
    // ------------------------------------------------------------------------------------------

    /**
     * Fixture-only JSON decoder for the log command payload. It is deliberately not the production
     * codec: ownership of the real {@code commandType + commandPayload -> BookingCommand} mapping is
     * still an open question for TV3 (codec) and TV2 (state machine).
     */
    private static final class FixtureCommandCodec {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        private FixtureCommandCodec() {
        }

        static BookingCommand toCommand(String commandType, String commandPayload) {
            try {
                JsonNode node = MAPPER.readTree(commandPayload);
                return switch (commandType) {
                    case RaftLogEntry.COMMAND_TYPE_BOOK_TICKET -> new BookingCommand.BookTicket(
                            text(node, "requestId"),
                            text(node, "tripId"),
                            text(node, "seatId"),
                            text(node, "passengerName"),
                            text(node, "ticketId"),
                            Instant.parse(text(node, "issuedAt")));
                    case RaftLogEntry.COMMAND_TYPE_NO_OP -> new BookingCommand.NoOp();
                    case RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET -> throw new IllegalArgumentException(
                            "fixture codec does not build CANCEL_TICKET; P1 is out of scope for D03");
                    default -> throw new IllegalArgumentException("unknown commandType '" + commandType + "'");
                };
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "fixture JSON is invalid for commandType '" + commandType + "'", failure);
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null || !value.isTextual()) {
                throw new IllegalArgumentException("fixture JSON is missing text field '" + field + "'");
            }
            return value.asText();
        }
    }

    /**
     * In-memory durable storage fake. It records every call so tests can check durability ordering
     * and that a fail-closed node stops touching storage. {@code commitIndex} is volatile and not
     * visible to storage, so the fake rejects only indexes covered by the durable apply checkpoint;
     * {@link FakeRaftNode} enforces the full {@code commitIndex} refusal.
     */
    private static final class FakeRaftStorage implements RaftStorage {

        enum FailurePoint {
            NONE,
            LOAD,
            SAVE_TERM_AND_VOTE,
            REPLACE_SUFFIX
        }

        private long currentTerm;
        private String votedFor;
        private final List<RaftLogEntry> log = new ArrayList<>();
        private long lastApplied;
        private final List<String> events = new ArrayList<>();
        private FailurePoint failNext = FailurePoint.NONE;

        /** Fixture setup only: seeds durable state as if it came from a previous process. */
        void seed(long term, String vote, List<RaftLogEntry> entries, long applied) {
            this.currentTerm = term;
            this.votedFor = vote;
            this.log.clear();
            this.log.addAll(entries);
            this.lastApplied = applied;
        }

        void failNext(FailurePoint point) {
            this.failNext = point;
        }

        @Override
        public PersistentState load() throws RaftStorageException {
            events.add("load");
            if (takeFailure(FailurePoint.LOAD)) {
                throw new RaftStorageException("fixture: load failed");
            }
            return new PersistentState(currentTerm, votedFor, List.copyOf(log), lastApplied);
        }

        @Override
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            events.add("saveTermAndVote(" + term + ", " + vote + ")");
            if (takeFailure(FailurePoint.SAVE_TERM_AND_VOTE)) {
                throw new RaftStorageException("fixture: saveTermAndVote failed");
            }
            if (term < currentTerm) {
                throw new IllegalArgumentException("stored term must not decrease");
            }
            currentTerm = term;
            votedFor = vote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException {
            events.add("replaceSuffix(" + fromIndex + ", " + entries.size() + ")");
            if (takeFailure(FailurePoint.REPLACE_SUFFIX)) {
                throw new RaftStorageException("fixture: replaceSuffix failed");
            }
            if (fromIndex < RaftLogEntry.FIRST_INDEX) {
                throw new IllegalArgumentException("fromIndex must be >= 1 but was " + fromIndex);
            }
            if (fromIndex <= lastApplied) {
                throw new RaftStorageException("refusing to replace durably applied index " + fromIndex
                        + " (durable apply checkpoint " + lastApplied + ")");
            }
            long expectedIndex = fromIndex;
            for (RaftLogEntry entry : entries) {
                if (entry.index() != expectedIndex) {
                    throw new IllegalArgumentException("replacement entries must be contiguous from "
                            + fromIndex + "; expected " + expectedIndex + " but was " + entry.index());
                }
                expectedIndex = Math.addExact(expectedIndex, 1L);
            }
            while (!log.isEmpty() && log.getLast().index() >= fromIndex) {
                log.removeLast();
            }
            log.addAll(entries);
        }

        @Override
        public long loadLastApplied() {
            return lastApplied;
        }

        private boolean takeFailure(FailurePoint point) {
            if (failNext != point) {
                return false;
            }
            failNext = FailurePoint.NONE;
            return true;
        }

        long currentTerm() {
            return currentTerm;
        }

        String votedFor() {
            return votedFor;
        }

        List<RaftLogEntry> log() {
            return List.copyOf(log);
        }

        long lastApplied() {
            return lastApplied;
        }

        List<String> events() {
            return List.copyOf(events);
        }
    }

    /**
     * Thin, single-threaded {@link RaftNode} test double over {@link RaftState} and
     * {@link FakeRaftStorage}. It exists only to check the D02 contract; it is not a Raft
     * implementation and it never counts votes, replicates, persists an event loop or computes a
     * majority.
     */
    private static final class FakeRaftNode implements RaftNode {

        private final String selfId;
        private final FakeRaftStorage storage;
        private final List<CommitListener> listeners = new ArrayList<>();
        private RaftState state;
        private long deliveredThrough;
        private boolean degraded;
        private RaftStorageException degradedCause;

        FakeRaftNode(String selfId, FakeRaftStorage storage) throws RaftStorageException {
            this.selfId = Objects.requireNonNull(selfId, "selfId");
            this.storage = Objects.requireNonNull(storage, "storage");
            restoreFrom(storage.load());
        }

        private void restoreFrom(RaftStorage.PersistentState persisted) {
            state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                    persisted.log(), persisted.lastApplied());
            deliveredThrough = state.lastApplied();
        }

        @Override
        public Role role() {
            return state.role();
        }

        @Override
        public long currentTerm() {
            return state.currentTerm();
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.ofNullable(state.leaderId());
        }

        @Override
        public long commitIndex() {
            return state.commitIndex();
        }

        @Override
        public long lastApplied() {
            return state.lastApplied();
        }

        @Override
        public long lastLogIndex() {
            return state.lastLogIndex();
        }

        @Override
        public void onApplied(long index) {
            // RaftState.markApplied implements the documented fail-closed checks.
            state.markApplied(index);
        }

        @Override
        public AppendOutcome appendCommand(String commandType, String commandPayload) {
            if (degraded) {
                return new StorageFailure(degradedCause);
            }
            if (state.role() != Role.LEADER) {
                return new NotLeader(state.leaderId());
            }
            long index = Math.addExact(state.lastLogIndex(), 1L);
            RaftLogEntry entry = new RaftLogEntry(index, state.currentTerm(), commandType, commandPayload);
            try {
                // Appending at lastLogIndex + 1 replaces an empty suffix.
                storage.replaceSuffix(index, List.of(entry));
            } catch (RaftStorageException failure) {
                enterFailClosed(failure);
                return new StorageFailure(failure);
            }
            state.append(entry);
            return new Accepted(entry.index(), entry.term());
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws RaftStorageException {
            requireWritable();
            Objects.requireNonNull(request, "request");
            long termBefore = state.currentTerm();
            String voteBefore = state.votedFor();
            boolean granted = VoteRules.grantVote(state, request.term(), request.candidateId(),
                    request.lastLogIndex(), request.lastLogTerm());
            if (state.currentTerm() != termBefore || !Objects.equals(state.votedFor(), voteBefore)) {
                try {
                    // The grant (and any term change, including a rejection in a higher term)
                    // is durable before the response value exists.
                    storage.saveTermAndVote(state.currentTerm(), state.votedFor());
                } catch (RaftStorageException failure) {
                    throw enterFailClosed(failure);
                }
            }
            return new RequestVoteResponse(state.currentTerm(), granted);
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) throws RaftStorageException {
            requireWritable();
            Objects.requireNonNull(request, "request");
            if (request.prevLogIndex() < RaftState.EMPTY_LOG_INDEX) {
                throw new IllegalArgumentException("prevLogIndex must be >= 0 but was " + request.prevLogIndex());
            }
            if (request.term() < state.currentTerm()) {
                return new AppendEntriesResponse(state.currentTerm(), false, 0L);
            }
            if (request.term() > state.currentTerm()) {
                try {
                    // A higher term is durable before this node acts on it or replies.
                    storage.saveTermAndVote(request.term(), null);
                } catch (RaftStorageException failure) {
                    throw enterFailClosed(failure);
                }
                state.becomeFollower(request.term(), request.leaderId());
            } else if (state.role() != Role.FOLLOWER
                    || !Objects.equals(state.leaderId(), request.leaderId())) {
                state.becomeFollower(request.term(), request.leaderId());
            }

            if (request.prevLogIndex() > state.lastLogIndex()
                    || state.termAt(request.prevLogIndex()) != request.prevLogTerm()) {
                return new AppendEntriesResponse(state.currentTerm(), false, 0L);
            }

            List<RaftLogEntry> entries = request.entries();
            long expectedIndex = Math.addExact(request.prevLogIndex(), 1L);
            long previousTerm = request.prevLogTerm();
            for (RaftLogEntry entry : entries) {
                if (entry.index() != expectedIndex) {
                    throw new IllegalArgumentException("entries must be contiguous from prevLogIndex + 1;"
                            + " expected " + expectedIndex + " but was " + entry.index());
                }
                if (entry.term() < previousTerm || entry.term() > request.term()) {
                    throw new IllegalArgumentException("entry term " + entry.term() + " at index "
                            + entry.index() + " is invalid for prevLogTerm " + previousTerm
                            + " and RPC term " + request.term());
                }
                // Same (index, term) with a different command, or a rewrite of a committed
                // index, throws IllegalStateException: fail closed, never overwrite silently.
                state.verifyIncomingEntry(entry);
                previousTerm = entry.term();
                expectedIndex = Math.addExact(expectedIndex, 1L);
            }

            long firstWriteIndex = 0L;
            for (RaftLogEntry entry : entries) {
                if (entry.index() > state.lastLogIndex()
                        || state.termAt(entry.index()) != entry.term()) {
                    firstWriteIndex = entry.index();
                    break;
                }
            }
            if (firstWriteIndex > 0L) {
                if (firstWriteIndex <= state.commitIndex()) {
                    throw new IllegalStateException("refusing to overwrite committed index "
                            + firstWriteIndex + " (commitIndex=" + state.commitIndex() + ")");
                }
                int offset = Math.toIntExact(firstWriteIndex - entries.getFirst().index());
                List<RaftLogEntry> replacement = List.copyOf(entries.subList(offset, entries.size()));
                try {
                    storage.replaceSuffix(firstWriteIndex, replacement);
                } catch (RaftStorageException failure) {
                    throw enterFailClosed(failure);
                }
                if (firstWriteIndex <= state.lastLogIndex()) {
                    state.truncateSuffixFrom(firstWriteIndex);
                }
                replacement.forEach(state::append);
            }

            long lastMatchedIndex = Math.addExact(request.prevLogIndex(), entries.size());
            long candidateCommit = Math.min(request.leaderCommit(), lastMatchedIndex);
            if (candidateCommit > state.commitIndex()) {
                state.advanceCommitIndex(candidateCommit);
            }
            deliverCommittedEntries();
            return new AppendEntriesResponse(state.currentTerm(), true, lastMatchedIndex);
        }

        @Override
        public void onElectionTimeout() throws RaftStorageException {
            requireWritable();
            state.startElection(selfId);
            try {
                // term and self-vote are durable before any REQUEST_VOTE could be emitted.
                storage.saveTermAndVote(state.currentTerm(), state.votedFor());
            } catch (RaftStorageException failure) {
                throw enterFailClosed(failure);
            }
        }

        @Override
        public void addCommitListener(CommitListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
        }

        private void requireWritable() throws RaftStorageException {
            if (degraded) {
                throw new RaftStorageException(
                        "node is fail-closed after a durable storage failure; writes are refused",
                        degradedCause);
            }
        }

        private RaftStorageException enterFailClosed(RaftStorageException failure) {
            degraded = true;
            if (degradedCause == null) {
                degradedCause = failure;
            }
            try {
                // Reads must reflect durable truth, not the mutation that failed to persist.
                restoreFrom(storage.load());
            } catch (RaftStorageException reloadFailure) {
                // Stay fail-closed; the original failure remains the operational cause.
            }
            return failure;
        }

        private void deliverCommittedEntries() {
            if (listeners.isEmpty()) {
                return;
            }
            while (deliveredThrough < state.commitIndex()) {
                long nextIndex = Math.addExact(deliveredThrough, 1L);
                RaftLogEntry entry = state.entryAt(nextIndex);
                for (CommitListener listener : listeners) {
                    listener.onCommitted(entry);
                }
                deliveredThrough = nextIndex;
            }
        }

        /**
         * TEST HARNESS ONLY: pretends the entry at {@code index} is quorum-committed. It does not
         * implement, exercise or prove the SPEC-04 section 5 majority/current-term commit rule.
         */
        void simulateCommit(long index) {
            state.advanceCommitIndex(index);
            deliverCommittedEntries();
        }

        /**
         * TEST HARNESS ONLY: marks this double as LEADER of the current term without counting
         * votes. Election counting belongs to TV5 and is not implemented in D02/D03.
         */
        void simulateWonElectionForTest(Set<String> peers) {
            state.becomeLeader(peers);
        }

        RaftLogEntry entryAtForTest(long index) {
            return state.entryAt(index);
        }

        boolean isDegraded() {
            return degraded;
        }
    }

    /**
     * Test-only election driver: no timer and no thread. It only calls the D02 entry points, and
     * vote decisions come from the real {@link VoteRules} used by {@link FakeRaftNode}.
     */
    private static final class FakeElection {

        private final FakeRaftNode node;

        FakeElection(FakeRaftNode node) {
            this.node = Objects.requireNonNull(node, "node");
        }

        void timeout() throws RaftStorageException {
            node.onElectionTimeout();
        }

        RaftNode.RequestVoteResponse askVote(long term, String candidateId, long lastLogIndex, long lastLogTerm)
                throws RaftStorageException {
            return node.handleRequestVote(
                    new RaftNode.RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm));
        }
    }

    /**
     * Bridges {@link RaftNode.CommitListener} (void) to {@link CommittedCommandApplier}
     * ({@code Optional<ApplyOutcome>}) and reports {@link RaftNode#onApplied(long)} only after the
     * apply succeeded. Which production class owns this adapter is an open question for TV2/TV3.
     */
    private static final class FakeBookingAdapter implements RaftNode.CommitListener {

        private final FakeRaftNode node;
        private final CommittedCommandApplier applier;
        private final List<ApplyResult> reportedResults = new ArrayList<>();
        private final List<BookingStateMachine.ApplyOutcome> outcomes = new ArrayList<>();

        FakeBookingAdapter(FakeRaftNode node, FakeBookingRepository repository) {
            this.node = Objects.requireNonNull(node, "node");
            this.applier = new CommittedCommandApplier(
                    Objects.requireNonNull(repository, "repository"), reportedResults::add);
        }

        @Override
        public void onCommitted(RaftLogEntry entry) {
            BookingCommand command = FixtureCommandCodec.toCommand(entry.commandType(), entry.commandPayload());
            Optional<BookingStateMachine.ApplyOutcome> outcome = applier.applyCommitted(entry.index(), command);
            outcome.ifPresent(outcomes::add);
            // A redelivery of an already applied index (e.g. after restart) must not be reported
            // as a fresh apply; only genuine in-memory progress calls onApplied.
            if (entry.index() == node.lastApplied() + 1) {
                node.onApplied(entry.index());
            }
        }

        List<ApplyResult> reportedResults() {
            return List.copyOf(reportedResults);
        }

        List<BookingStateMachine.ApplyOutcome> outcomes() {
            return List.copyOf(outcomes);
        }
    }

    /**
     * In-memory fake of the TV2 transaction boundary: it stages a rebuilt state machine and only
     * publishes it when the whole apply succeeds, so a simulated storage failure rolls back.
     */
    private static final class FakeBookingRepository implements BookingRepository {

        private record AppliedEntry(long index, BookingCommand command) {
        }

        private BookingStateMachine stateMachine = new BookingStateMachine();
        private final List<AppliedEntry> appliedEntries = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();
        private boolean failNextApply;

        @Override
        public long lastApplied() {
            return stateMachine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            BookingStateMachine staged = new BookingStateMachine();
            for (AppliedEntry entry : appliedEntries) {
                staged.apply(entry.index(), entry.command());
            }
            BookingStateMachine.ApplyOutcome outcome = staged.apply(logIndex, command);
            if (failNextApply) {
                failNextApply = false;
                throw new StorageException("fixture: apply transaction failed before commit");
            }
            stateMachine = staged;
            appliedEntries.add(new AppliedEntry(logIndex, command));
            outcomes.put(logIndex, outcome);
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return stateMachine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomes.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return stateMachine.allSeatStatuses();
        }

        BookingStateMachine stateMachine() {
            return stateMachine;
        }

        void failNextApply() {
            failNextApply = true;
        }
    }
}
