package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/** Real candidate coordinator/state, fake core persistence and queued transport; no TCP/DB. */
class ElectionTest {
    private static final Set<String> PEERS = Set.of("node-2", "node-3", "node-4", "node-5");

    @Test
    void persistsIncrementedTermAndSelfVoteBeforeSendingToAllFourPeers() throws Exception {
        Fixture f = new Fixture("node-1", RaftState.restore(4, "node-2",
                List.of(new RaftLogEntry(1, 3, "NO_OP", "{}")), 0));

        assertEquals(5, f.election.startElection());

        assertSame(f.core.state, f.election.state());
        assertSame(f.core, f.election.node());
        assertEquals(Role.CANDIDATE, f.core.role());
        assertEquals("node-1", f.core.state.votedFor());
        assertEquals(List.of("persist:5:node-1", "send:node-2:5", "send:node-3:5",
                "send:node-4:5", "send:node-5:5"), f.events);
        assertEquals(4, f.sent.size());
        for (Sent sent : f.sent) {
            assertEquals(new RequestVoteRequestDto(5, "node-1", 1, 3), sent.request());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"node-1", "node-2", "node-3", "node-4", "node-5"})
    void threeDistinctVotesElectAnyNodeAndInitializeAllPeerIndices(String self) throws Exception {
        Fixture f = new Fixture(self, RaftState.restore(2, null,
                List.of(new RaftLogEntry(1, 2, "NO_OP", "{}")), 0));
        f.election.startElection();
        f.sent.get(0).reply().receive(new RaftNode.RequestVoteResponse(3, true));
        assertEquals(Role.CANDIDATE, f.core.role(), "self plus one peer is only 2/5");
        f.sent.get(1).reply().receive(new RaftNode.RequestVoteResponse(3, true));

        assertEquals(Role.LEADER, f.core.role());
        assertEquals(1, f.leaderStarts);
        assertEquals(4, f.core.state.nextIndex().size());
        assertFalse(f.core.state.nextIndex().containsKey(self));
        f.core.state.nextIndex().values().forEach(index -> assertEquals(2L, index));
        f.core.state.matchIndex().values().forEach(index -> assertEquals(0L, index));
        assertEquals(0, f.core.commitIndex(), "election does not commit entries");
        assertEquals(0, f.core.lastApplied());
        f.sent.get(2).reply().receive(new RaftNode.RequestVoteResponse(3, true));
        assertEquals(1, f.leaderStarts, "late grants do not start leadership twice");
    }

    @Test
    void twoVotesAndOfflinePeersNeverElectLeader() throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        assertEquals(Role.CANDIDATE, f.core.role());
        assertEquals(3, f.election.requiredVotes());
        assertEquals(0, f.leaderStarts);
        assertThrows(IllegalStateException.class, () -> f.election.promoteToLeader(PEERS));
    }

    @Test
    void duplicateSelfNonVoterAndRefusedResponsesCannotMakeQuorum() throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 1, false);
        assertInstanceOf(ElectionService.IgnoredVote.class,
                f.election.recordVoteResponse("node-1", 1, true));
        assertInstanceOf(ElectionService.IgnoredVote.class,
                f.election.recordVoteResponse("monitor-1", 99, true));
        assertEquals(1, f.core.currentTerm());
        assertEquals(Role.CANDIDATE, f.core.role());
        f.respond("node-4", 1, 1, true);
        assertEquals(Role.LEADER, f.core.role());
    }

    @Test
    void oldTermGrantsNeverWinAndNewCampaignDiscardsEarlierVotes() throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        assertEquals(2, f.election.startElection());
        f.respond("node-3", 1, 1, true);
        f.respond("node-4", 2, 1, true);
        f.respond("node-5", 2, 2, true);
        assertEquals(Role.CANDIDATE, f.core.role());
        f.respond("node-2", 2, 2, true);
        assertEquals(Role.LEADER, f.core.role());
        assertEquals(2, f.core.currentTerm());
    }

    @Test
    void responseToOldRequestCannotVoteInNewCampaignEvenWithCurrentResponseTerm() throws Exception {
        Fixture f = campaign();
        f.election.startElection();
        f.respond("node-2", 1, 2, true);
        f.respond("node-3", 1, 2, true);
        f.respond("node-4", 2, 2, true);
        assertEquals(Role.CANDIDATE, f.core.role());
        f.respond("node-5", 2, 2, true);
        assertEquals(Role.LEADER, f.core.role());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void higherTermGrantOrDenialPersistsClearedVoteAndStepsDown(boolean granted) throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 7, granted);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals(7, f.core.currentTerm());
        assertEquals(7, f.core.savedTerm);
        assertNull(f.core.savedVote);
        assertNull(f.core.state.votedFor());
        assertTrue(f.election.voteRequestFor(1).isEmpty());
        f.respond("node-4", 1, 1, true);
        f.respond("node-5", 1, 7, true);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals(8, f.election.startElection());
        f.respond("node-2", 8, 8, true);
        assertEquals(Role.CANDIDATE, f.core.role());
    }

    @Test
    void delayedResponseCanStepDownAnAlreadyElectedLeader() throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 1, true);
        f.respond("node-4", 1, 6, false);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals(6, f.core.savedTerm);
        assertTrue(f.core.state.nextIndex().isEmpty());
        assertEquals(1, f.leaderStarts);
    }

    @Test
    void higherTermFromEarlierCampaignStillUpdatesCore() throws Exception {
        Fixture f = campaign();
        f.election.startElection();
        f.respond("node-2", 1, 9, false);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals(9, f.core.savedTerm);
    }

    @Test
    void sameTermLeaderContactThroughCorePreventsLateVotesFromWinning() throws Exception {
        Fixture f = campaign();
        // Simulate TV4 accepting a valid AppendEntries in the shared state.
        f.core.state.becomeFollower(1, "node-4");
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 1, true);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals("node-4", f.core.state.leaderId());
        assertEquals("node-1", f.core.state.votedFor());
        assertEquals(0, f.leaderStarts);
    }

    @Test
    void sharedCoreChangingTermInvalidatesCampaign() throws Exception {
        Fixture f = campaign();
        f.core.observeHigherTerm(4);
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 4, true);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertTrue(f.election.voteRequestFor(4).isEmpty());
    }

    @Test
    void failedSelfVotePersistenceSendsNothingAndCannotBeReused() {
        Fixture f = new Fixture();
        f.core.failSave = true;
        assertThrows(RaftStorageException.class, f.election::startElection);
        assertTrue(f.sent.isEmpty());
        assertTrue(f.election.voteRequestFor(1).isEmpty());
        assertThrows(IllegalStateException.class, f.election::startElection);
        assertThrows(IllegalStateException.class, () -> f.election.promoteToLeader(PEERS));
    }

    @Test
    void failedHigherTermPersistenceBlocksEverySubsequentVoteAndCampaign() throws Exception {
        Fixture f = campaign();
        f.respond("node-2", 1, 1, true);
        f.core.failSave = true;
        assertThrows(RaftStorageException.class, () -> f.respond("node-3", 1, 2, false));
        f.respond("node-4", 1, 1, true);
        assertEquals(0, f.leaderStarts);
        assertTrue(f.election.voteRequestFor(1).isEmpty());
        assertThrows(IllegalStateException.class, f.election::startElection);
        assertThrows(IllegalStateException.class, () -> f.election.promoteToLeader(PEERS));
    }

    @Test
    void countsOnlyUntilExplicitPromotionWhenUsingElectionServiceBoundary() throws Exception {
        Fixture f = campaign();
        assertEquals(new ElectionService.VoteProgress(2, 3),
                f.election.recordVoteResponse("node-2", 1, true));
        assertEquals(new ElectionService.Won(3),
                f.election.recordVoteResponse("node-3", 1, true));
        assertEquals(Role.CANDIDATE, f.core.role());
        assertThrows(IllegalArgumentException.class,
                () -> f.election.promoteToLeader(Set.of("node-2", "node-3")));
        f.election.promoteToLeader(PEERS);
        assertEquals(Role.LEADER, f.core.role());
        assertThrows(IllegalStateException.class, f.election::startElection);
        assertThrows(IllegalStateException.class, () -> f.election.promoteToLeader(PEERS));
        assertEquals(1, f.leaderStarts);
    }

    @Test
    void countingHigherTermAbandonsCampaignUntilEventLoopUpdatesCore() throws Exception {
        Fixture f = campaign();
        assertEquals(new ElectionService.SteppedDown(3),
                f.election.recordVoteResponse("node-2", 3, true));
        assertEquals(1, f.core.currentTerm(), "pure counter must not persist or mutate core");
        assertTrue(f.election.voteRequestFor(1).isEmpty());
        assertInstanceOf(ElectionService.IgnoredVote.class,
                f.election.recordVoteResponse("node-3", 1, true));
        assertThrows(IllegalStateException.class, () -> f.election.promoteToLeader(PEERS));
    }

    @Test
    void incomingVoteDelegatesToCoreAndInvalidatesCampaignOnHigherTerm() throws Exception {
        Fixture f = campaign();
        var response = f.election.handleRequestVote(new RequestVoteRequestDto(3, "node-4", 0, 0));
        assertTrue(response.voteGranted());
        assertEquals(3, f.core.savedTerm);
        assertEquals("node-4", f.core.savedVote);
        f.respond("node-2", 1, 1, true);
        f.respond("node-3", 1, 1, true);
        assertEquals(Role.FOLLOWER, f.core.role());
        assertEquals(0, f.leaderStarts);
    }

    @Test
    void failedIncomingVotePersistenceClosesElection() throws Exception {
        Fixture f = campaign();
        f.core.failSave = true;
        assertThrows(RaftStorageException.class,
                () -> f.election.handleRequestVote(new RequestVoteRequestDto(3, "node-4", 0, 0)));
        assertThrows(IllegalStateException.class, f.election::startElection);
        assertTrue(f.election.voteRequestFor(3).isEmpty());
    }

    @Test
    void invalidMembersAndMalformedResponsesCannotChangeState() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new Fixture("monitor-1", new RaftState()));
        Fixture f = campaign();
        assertFalse(f.election.isVoter(null));
        assertEquals(5, f.election.voters().size());
        assertThrows(UnsupportedOperationException.class, () -> f.election.voters().clear());
        assertThrows(IllegalArgumentException.class,
                () -> f.election.handleRequestVote(new RequestVoteRequestDto(9, "node-6", 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> f.election.recordVoteResponse(null, 1, true));
        assertThrows(IllegalArgumentException.class,
                () -> f.election.recordVoteResponse("node-2", -1, true));
        assertEquals(1, f.core.currentTerm());
    }

    @Test
    void termOverflowCannotSendRequestsOrWrapTerm() {
        Fixture f = new Fixture("node-1", RaftState.restore(Long.MAX_VALUE, null, List.of(), 0));
        assertThrows(ArithmeticException.class, f.election::startElection);
        assertTrue(f.sent.isEmpty());
        assertEquals(Long.MAX_VALUE, f.core.currentTerm());
        assertEquals(Role.FOLLOWER, f.core.role());
    }

    private static Fixture campaign() throws Exception {
        Fixture f = new Fixture();
        f.election.startElection();
        return f;
    }

    private record Sent(String peer, RequestVoteRequestDto request, CandidateElection.VoteReply reply) { }

    private static final class Fixture {
        final List<String> events = new ArrayList<>();
        final List<Sent> sent = new ArrayList<>();
        final FakeCore core;
        final CandidateElection election;
        int leaderStarts;

        Fixture() { this("node-1", new RaftState()); }

        Fixture(String self, RaftState state) {
            core = new FakeCore(self, state, events);
            election = new CandidateElection(self, core, state, (peer, request, reply) -> {
                assertEquals(request.getTerm(), core.savedTerm, "persist before send");
                assertEquals(self, core.savedVote);
                events.add("send:" + peer + ":" + request.getTerm());
                sent.add(new Sent(peer, request, reply));
            }, core::observeHigherTerm, () -> {
                assertEquals(Role.LEADER, core.role());
                assertEquals(4, state.nextIndex().size(), "initialize before heartbeat hook");
                leaderStarts++;
            });
        }

        void respond(String peer, long requestTerm, long responseTerm, boolean granted)
                throws RaftStorageException {
            Sent request = sent.stream().filter(s -> s.peer().equals(peer)
                    && s.request().getTerm() == requestTerm).findFirst().orElseThrow();
            request.reply().receive(new RaftNode.RequestVoteResponse(responseTerm, granted));
        }
    }

    /** Test-only core/storage adapter. Recorded writes are not PostgreSQL durability evidence. */
    private static final class FakeCore implements RaftNode {
        final String self;
        final RaftState state;
        final List<String> events;
        long savedTerm;
        String savedVote;
        boolean failSave;

        FakeCore(String self, RaftState state, List<String> events) {
            this.self = self;
            this.state = state;
            this.events = events;
            savedTerm = state.currentTerm();
            savedVote = state.votedFor();
        }

        private void persist() throws RaftStorageException {
            if (failSave) {
                // Deliberately fail after the in-memory mutation to test fail-closed handling.
                throw new RaftStorageException("injected storage failure");
            }
            savedTerm = state.currentTerm();
            savedVote = state.votedFor();
            events.add("persist:" + savedTerm + ":" + savedVote);
        }

        void observeHigherTerm(long term) throws RaftStorageException {
            state.becomeFollower(term);
            persist();
        }

        @Override public void onElectionTimeout() throws RaftStorageException {
            state.startElection(self);
            persist();
        }

        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
                throws RaftStorageException {
            long beforeTerm = state.currentTerm();
            String beforeVote = state.votedFor();
            boolean grant = VoteRules.grantVote(state, request.term(), request.candidateId(),
                    request.lastLogIndex(), request.lastLogTerm());
            if (beforeTerm != state.currentTerm() || !Objects.equals(beforeVote, state.votedFor())) {
                persist();
            }
            return new RequestVoteResponse(state.currentTerm(), grant);
        }

        @Override public Role role() { return state.role(); }
        @Override public long currentTerm() { return state.currentTerm(); }
        @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
        @Override public long commitIndex() { return state.commitIndex(); }
        @Override public long lastApplied() { return state.lastApplied(); }
        @Override public long lastLogIndex() { return state.lastLogIndex(); }
        @Override public void onApplied(long index) { throw new UnsupportedOperationException(); }
        @Override public AppendOutcome appendCommand(String type, String payload) {
            throw new UnsupportedOperationException();
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public void addCommitListener(CommitListener listener) {
            throw new UnsupportedOperationException();
        }
    }
}
