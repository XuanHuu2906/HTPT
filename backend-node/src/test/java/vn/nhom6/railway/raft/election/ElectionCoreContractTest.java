package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract test for {@link ElectionService} joined with the TV4 Raft core, the TV5-D02
 * vote rules and the TV3-D02 wire DTO. The fake core delegates to the real
 * {@link RaftState} and {@link VoteRules}; only durability and transport are faked, so a
 * pass here proves the interface composes, not that a 5-Node cluster has been run.
 */
class ElectionCoreContractTest {

    private static final List<String> CLUSTER = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    // -----------------------------------------------------------------------------------
    // Contract compilation and shared-state identity
    // -----------------------------------------------------------------------------------

    @Test
    void compilesAgainstSharedCoreTypesWithoutSecondTermOrVote() throws Exception {
        ElectionService service = new FakeElectionService("node-1", new RaftState());

        // The interface must use the existing TV4/TV5/TV3 types, not private copies.
        assertInstanceOf(RaftState.class, service.state());
        assertInstanceOf(RaftNode.class, service.node());
        assertSame(service.state(), ((FakeRaftCore) service.node()).state,
                "election must not copy RaftState");
        assertEquals(CLUSTER, service.voters());

        long term = service.startElection();
        assertEquals(service.state().currentTerm(), term);
        assertEquals(term, service.node().currentTerm());
        assertEquals(service.state().votedFor(), service.localId());

        RequestVoteRequestDto dto = service.voteRequestFor(term).orElseThrow();
        assertEquals(term, dto.getTerm());
        assertEquals(service.localId(), dto.getCandidateId());
        assertEquals(service.state().lastLogIndex(), dto.getLastLogIndex());
        assertEquals(service.state().lastLogTerm(), dto.getLastLogTerm());
    }

    @Test
    void receivesWirePayloadIntoTheSharedVoteRules() throws Exception {
        ElectionService service = new FakeElectionService("node-1", new RaftState());

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(1, "node-2", 0, 0));

        assertEquals(1, response.term());
        assertTrue(response.voteGranted(), "node-2 log is up to date for an empty log");
        assertEquals("node-2", service.state().votedFor());
    }

    @Test
    void stateReturnsTheSameInstanceAsTheRaftNode() throws Exception {
        RaftState shared = RaftState.restore(4, null, List.of(), 0);
        ElectionService service = new FakeElectionService("node-3", shared);

        assertSame(shared, service.state());
        assertSame(shared, ((FakeRaftCore) service.node()).state);
        service.startElection();
        assertSame(shared, service.state());
        assertEquals(5, shared.currentTerm());
    }

    @Test
    void votersIncludeSelfAndSetMajorityAtThree() {
        ElectionService service = new FakeElectionService("node-2", new RaftState());

        assertEquals(CLUSTER, service.voters());
        assertTrue(service.isVoter("node-1"));
        assertTrue(service.isVoter("node-2"));
        assertFalse(service.isVoter("node-9"));
        assertFalse(service.isVoter(null));
        assertEquals(3, service.requiredVotes());
    }

    // -----------------------------------------------------------------------------------
    // Vote requests reuse VoteRules and the shared DTO
    // -----------------------------------------------------------------------------------

    @Test
    void forwardsGrantDecisionToVoteRulesWithPersistBeforeReply() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(1, "node-2", 0, 0));

        assertTrue(response.voteGranted());
        assertEquals(List.of("saveTermAndVote(1,node-2)"), core.durableWrites,
                "the vote must be durable before the reply is returned");
        assertEquals("node-2", service.state().votedFor());
    }

    @Test
    void rejectsNonVoterBeforeAnyStateChange() {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);

        assertThrows(IllegalArgumentException.class,
                () -> service.handleRequestVote(new RequestVoteRequestDto(9, "intruder", 0, 0)));

        assertEquals(0, service.state().currentTerm());
        assertNull(service.state().votedFor());
        assertTrue(core.durableWrites.isEmpty(), "a non-voter request must not be persisted");
    }

    @Test
    void validatesWirePayloadBeforeDelegating() {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);

        assertThrows(IllegalArgumentException.class,
                () -> service.handleRequestVote(new RequestVoteRequestDto(1, " ", 0, 0)));

        assertEquals(0, core.state.currentTerm());
        assertTrue(core.durableWrites.isEmpty());
    }

    @Test
    void staleRequestEchoesCurrentTermWithoutChangingTheVote() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1",
                RaftState.restore(5, "node-4", List.of(), 0));
        ElectionService service = new FakeElectionService(core);

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(3, "node-2", 0, 0));

        assertFalse(response.voteGranted());
        assertEquals(5, response.term());
        assertEquals("node-4", service.state().votedFor());
    }

    @Test
    void higherTermRejectedByLogStillPersistsTheNewTerm() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1", logState());
        ElectionService service = new FakeElectionService(core);

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(6, "node-2", 100, 2));

        assertFalse(response.voteGranted(), "older last term loses to the local log");
        assertEquals(6, response.term());
        assertEquals(List.of("saveTermAndVote(6,null)"), core.durableWrites,
                "the higher term must be durable even when the vote is refused");
    }

    @Test
    void duplicateVoteResponseDoesNotCountTwice() throws Exception {
        ElectionService service = campaign("node-1");

        assertInstanceOf(ElectionService.VoteProgress.class,
                service.recordVoteResponse("node-2", 1, true));
        ElectionService.ElectionOutcome second = service.recordVoteResponse("node-2", 1, true);

        assertInstanceOf(ElectionService.IgnoredVote.class, second);
    }

    // -----------------------------------------------------------------------------------
    // Majority counting: 3 distinct of 5 including self, no duplicates
    // -----------------------------------------------------------------------------------

    @Test
    void winsAtThreeDistinctVotesIncludingSelf() throws Exception {
        ElectionService service = campaign("node-1");

        assertProgress(service.recordVoteResponse("node-2", 1, true), 2);
        ElectionService.ElectionOutcome won = service.recordVoteResponse("node-3", 1, true);

        assertInstanceOf(ElectionService.Won.class, won);
        assertEquals(3, ((ElectionService.Won) won).votesGranted());
        assertEquals(Role.CANDIDATE, service.state().role(),
                "counting votes must not perform the Leader transition");
    }

    @Test
    void aRefusedVoteIsNotProgressTowardQuorum() throws Exception {
        ElectionService service = campaign("node-1");

        assertProgress(service.recordVoteResponse("node-2", 1, true), 2);
        ElectionService.ElectionOutcome refused = service.recordVoteResponse("node-3", 1, false);

        assertInstanceOf(ElectionService.IgnoredVote.class, refused);
        assertInstanceOf(ElectionService.Won.class,
                service.recordVoteResponse("node-4", 1, true),
                "a refusal must not consume the third distinct grant");
    }

    @Test
    void staleTermVoteResponseIsIgnored() throws Exception {
        ElectionService service = campaign("node-1");
        service.recordVoteResponse("node-2", 1, true);

        ElectionService.ElectionOutcome stale = service.recordVoteResponse("node-3", 0, true);

        assertInstanceOf(ElectionService.IgnoredVote.class, stale);
    }

    @Test
    void higherTermVoteResponseEndsTheCampaign() throws Exception {
        ElectionService service = campaign("node-1");

        ElectionService.ElectionOutcome outcome = service.recordVoteResponse("node-2", 2, true);

        assertInstanceOf(ElectionService.SteppedDown.class, outcome);
        assertEquals(2, ((ElectionService.SteppedDown) outcome).observedTerm());
        assertInstanceOf(ElectionService.IgnoredVote.class,
                service.recordVoteResponse("node-3", 1, true),
                "a stepped-down candidate cannot win its old term");
    }

    @Test
    void voteResponseFromNonVoterIsIgnored() throws Exception {
        ElectionService service = campaign("node-1");

        assertInstanceOf(ElectionService.IgnoredVote.class,
                service.recordVoteResponse("intruder", 1, true));
    }

    @Test
    void repeatedElectionResetsEarlierVotes() throws Exception {
        ElectionService service = campaign("node-1");
        service.recordVoteResponse("node-2", 1, true);
        service.recordVoteResponse("node-3", 1, true);

        long second = service.startElection();

        assertEquals(2, second);
        assertInstanceOf(ElectionService.VoteProgress.class,
                service.recordVoteResponse("node-4", 2, true));
        assertInstanceOf(ElectionService.Won.class,
                service.recordVoteResponse("node-5", 2, true));
        assertInstanceOf(ElectionService.IgnoredVote.class,
                service.recordVoteResponse("node-2", 1, true),
                "votes from the previous term must not be reused");
    }

    @Test
    void winningDoesNotPromoteWithoutTheEventLoopTransition() throws Exception {
        ElectionService service = campaign("node-1");
        service.recordVoteResponse("node-2", 1, true);
        service.recordVoteResponse("node-3", 1, true);

        assertEquals(Role.CANDIDATE, service.state().role());
        assertEquals(Role.CANDIDATE, service.node().role());
    }

    // -----------------------------------------------------------------------------------
    // Election timeout, campaign payload and Leader transition of the shared state
    // -----------------------------------------------------------------------------------

    @Test
    void startElectionAdvancesTermAndPersistsSelfVoteBeforeCampaigning() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);

        long term = service.startElection();

        assertEquals(1, term);
        assertEquals(Role.CANDIDATE, service.state().role());
        assertEquals("node-1", service.state().votedFor());
        RequestVoteRequestDto dto = service.voteRequestFor(term).orElseThrow();
        assertEquals(1, dto.getTerm());
        assertEquals("node-1", dto.getCandidateId());
        assertTrue(core.durableWrites.contains("saveTermAndVote(1,node-1)"));
    }

    @Test
    void voteRequestIsEmptyOutsideTheCampaignTerm() throws Exception {
        ElectionService service = campaign("node-1");

        assertTrue(service.voteRequestFor(2).isEmpty());
        assertTrue(service.voteRequestFor(0).isEmpty());
        assertTrue(service.voteRequestFor(1).isPresent());
    }

    @Test
    void aWonCandidateBecomesLeaderWithInitializedPeerIndices() throws Exception {
        ElectionService service = campaign("node-1");
        service.recordVoteResponse("node-2", 1, true);
        service.recordVoteResponse("node-3", 1, true);

        service.promoteToLeader(Set.of("node-2", "node-3", "node-4", "node-5"));

        RaftState state = service.state();
        assertEquals(Role.LEADER, state.role());
        assertEquals(Set.of("node-2", "node-3", "node-4", "node-5"), state.nextIndex().keySet());
        assertEquals(Set.of("node-2", "node-3", "node-4", "node-5"), state.matchIndex().keySet());
        state.nextIndex().forEach((peer, next) -> assertEquals(1L, next));
        state.matchIndex().forEach((peer, match) -> assertEquals(0L, match));
    }

    @Test
    void startElectionIsRejectedAfterPromotion() throws Exception {
        ElectionService service = campaign("node-1");
        service.recordVoteResponse("node-2", 1, true);
        service.recordVoteResponse("node-3", 1, true);
        service.promoteToLeader(Set.of("node-2", "node-3", "node-4", "node-5"));

        assertThrows(IllegalStateException.class, service::startElection);
        assertEquals(Role.LEADER, service.state().role());
    }

    @Test
    void candidateRefusesACompetingRequestOfItsOwnTermWithoutReverting() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);
        service.startElection();
        List<String> writesAfterCampaign = List.copyOf(core.durableWrites);

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(1, "node-2", 0, 0));

        assertFalse(response.voteGranted(), "the candidate already voted for itself");
        assertEquals(Role.CANDIDATE, service.state().role());
        assertEquals(writesAfterCampaign, core.durableWrites,
                "a refused same-term vote changes nothing durable");
    }

    @Test
    void leaderStepsDownOnHigherTermRequestVote() throws Exception {
        FakeRaftCore core = new FakeRaftCore("node-1", new RaftState());
        ElectionService service = new FakeElectionService(core);
        service.startElection();
        service.recordVoteResponse("node-2", 1, true);
        service.recordVoteResponse("node-3", 1, true);
        service.promoteToLeader(Set.of("node-2", "node-3", "node-4", "node-5"));
        assertEquals(Role.LEADER, service.state().role());

        RequestVoteResponse response = service.handleRequestVote(
                new RequestVoteRequestDto(2, "node-4", 1, 1));

        assertTrue(response.voteGranted());
        assertEquals(2, response.term());
        assertEquals(Role.FOLLOWER, service.state().role());
        assertEquals("node-4", service.state().votedFor());
        assertTrue(service.state().nextIndex().isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"node-1", "node-2", "node-3", "node-4", "node-5"})
    void anyVoterCanCampaignAndWinItsOwnMajority(String selfId) throws Exception {
        ElectionService service = campaign(selfId);
        List<String> others = CLUSTER.stream().filter(id -> !id.equals(selfId)).toList();

        assertProgress(service.recordVoteResponse(others.get(0), 1, true), 2);
        ElectionService.ElectionOutcome outcome = service.recordVoteResponse(others.get(1), 1, true);

        assertInstanceOf(ElectionService.Won.class, outcome);
        service.promoteToLeader(Set.copyOf(others));
        assertEquals(Role.LEADER, service.state().role());
        assertEquals(Set.copyOf(others), service.state().nextIndex().keySet());
    }

    // -----------------------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------------------

    private static ElectionService campaign(String selfId) throws Exception {
        FakeRaftCore core = new FakeRaftCore(selfId, new RaftState());
        ElectionService service = new FakeElectionService(core);
        service.startElection();
        return service;
    }

    private static void assertProgress(ElectionService.ElectionOutcome outcome, int expectedVotes) {
        assertInstanceOf(ElectionService.VoteProgress.class, outcome);
        ElectionService.VoteProgress progress = (ElectionService.VoteProgress) outcome;
        assertEquals(expectedVotes, progress.votesGranted());
        assertEquals(3, progress.votesRequired());
    }

    private static RaftState logState() {
        return RaftState.restore(5, "node-4", List.of(
                new RaftLogEntry(1, 1, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"),
                new RaftLogEntry(2, 3, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"),
                new RaftLogEntry(3, 3, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}")), 1);
    }

    /**
     * Fake RaftNode: the real core state and vote rules, no network and no database.
     * Every durable mutation is recorded, so tests can assert persist-before-reply.
     */
    private static final class FakeRaftCore implements RaftNode {

        private final String selfId;
        private final RaftState state;
        private final List<String> durableWrites = new ArrayList<>();
        private final List<CommitListener> listeners = new ArrayList<>();

        private FakeRaftCore(String selfId, RaftState state) {
            this.selfId = selfId;
            this.state = state;
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
        public void onApplied(long index) {
            throw new UnsupportedOperationException("apply is not part of the election contract");
        }

        @Override
        public long lastLogIndex() {
            return state.lastLogIndex();
        }

        @Override
        public AppendOutcome appendCommand(String commandType, String commandPayload) {
            throw new UnsupportedOperationException("replication is owned by TV4");
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
            long termBefore = state.currentTerm();
            String voteBefore = state.votedFor();
            boolean granted = VoteRules.grantVote(state,
                    request.term(), request.candidateId(), request.lastLogIndex(), request.lastLogTerm());
            if (state.currentTerm() != termBefore || !Objects.equals(state.votedFor(), voteBefore)) {
                durableWrites.add("saveTermAndVote(" + state.currentTerm() + ","
                        + (state.votedFor() == null ? "null" : state.votedFor()) + ")");
            }
            return new RequestVoteResponse(state.currentTerm(), granted);
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            throw new UnsupportedOperationException("replication is owned by TV4");
        }

        @Override
        public void onElectionTimeout() {
            state.startElection(selfId);
            durableWrites.add("saveTermAndVote(" + state.currentTerm() + "," + state.votedFor() + ")");
        }

        @Override
        public void addCommitListener(CommitListener listener) {
            listeners.add(listener);
        }
    }

    /**
     * Fake ElectionService: membership, wire conversion and vote counting over the fake
     * core. Vote counting is deliberately implemented with a map so duplicate voters and
     * stale terms are visibly rejected rather than assumed.
     */
    private static final class FakeElectionService implements ElectionService {

        private final FakeRaftCore core;
        private long campaignTerm;
        private final Set<String> votesGrantedBy = new LinkedHashSet<>();
        private boolean campaignAbandoned;

        private FakeElectionService(String selfId, RaftState state) {
            this(new FakeRaftCore(selfId, state));
        }

        private FakeElectionService(FakeRaftCore core) {
            this.core = core;
        }

        @Override
        public String localId() {
            return core.selfId;
        }

        @Override
        public RaftNode node() {
            return core;
        }

        @Override
        public RaftState state() {
            return core.state;
        }

        @Override
        public List<String> voters() {
            return CLUSTER;
        }

        @Override
        public boolean isVoter(String nodeId) {
            return nodeId != null && CLUSTER.contains(nodeId);
        }

        @Override
        public int requiredVotes() {
            return CLUSTER.size() / 2 + 1;
        }

        @Override
        public long startElection() throws RaftStorage.RaftStorageException {
            Objects.requireNonNull(core.selfId, "selfId");
            if (!isVoter(core.selfId)) {
                throw new IllegalStateException("local Node is not a voter");
            }
            if (core.state.role() == Role.LEADER) {
                throw new IllegalStateException("a leader cannot start an election");
            }
            core.onElectionTimeout();
            campaignTerm = core.state.currentTerm();
            votesGrantedBy.clear();
            votesGrantedBy.add(core.selfId);
            campaignAbandoned = false;
            return campaignTerm;
        }

        @Override
        public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
            if (core.state.role() != Role.CANDIDATE || core.state.currentTerm() != candidateTerm) {
                return Optional.empty();
            }
            return Optional.of(new RequestVoteRequestDto(
                    candidateTerm,
                    core.selfId,
                    core.state.lastLogIndex(),
                    core.state.lastLogTerm()));
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequestDto request)
                throws RaftStorage.RaftStorageException {
            Objects.requireNonNull(request, "request");
            request.validate();
            if (!isVoter(request.getCandidateId())) {
                throw new IllegalArgumentException(
                        "candidate '" + request.getCandidateId() + "' is not a voter");
            }
            return core.handleRequestVote(new RequestVoteRequest(
                    request.getTerm(),
                    request.getCandidateId(),
                    request.getLastLogIndex(),
                    request.getLastLogTerm()));
        }

        @Override
        public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
            Objects.requireNonNull(voterId, "voterId");
            if (voterId.isBlank()) {
                throw new IllegalArgumentException("voterId must not be blank");
            }
            if (term > campaignTerm) {
                campaignAbandoned = true;
                return new SteppedDown(term);
            }
            if (campaignAbandoned || term != campaignTerm || core.state.role() != Role.CANDIDATE
                    || !isVoter(voterId) || voterId.equals(core.selfId)) {
                return new IgnoredVote("not a vote for the current campaign");
            }
            if (!granted) {
                return new IgnoredVote("vote refused");
            }
            if (!votesGrantedBy.add(voterId)) {
                return new IgnoredVote("duplicate voter");
            }
            int count = votesGrantedBy.size();
            if (count >= requiredVotes()) {
                return new Won(count);
            }
            return new VoteProgress(count, requiredVotes());
        }

        @Override
        public void promoteToLeader(Set<String> peers) {
            Objects.requireNonNull(peers, "peers");
            for (String peer : peers) {
                if (!isVoter(peer)) {
                    throw new IllegalArgumentException("peer '" + peer + "' is not a voter");
                }
                if (peer.equals(core.selfId)) {
                    throw new IllegalArgumentException("the local Node is not its own peer");
                }
            }
            core.state.becomeLeader(peers);
        }
    }
}
