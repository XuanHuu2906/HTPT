package vn.nhom6.railway.raft.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

/**
 * Five-voter election/recovery contract over a deterministic fake network.
 *
 * <p>This proves election safety and durable term/vote transitions only. It is
 * deliberately not a substitute for the real TCP/PostgreSQL/Docker G4 test.
 */
class RaftElectionFivePeerFakeNetworkTest {
    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    @Test
    void splitVoteRetriesInNewTermAndElectsOneLeaderWithThreeVotes() throws Exception {
        FakeNetwork network = new FakeNetwork((from, to) -> true);

        for (String voter : VOTERS) {
            network.node(voter).startElection();
        }
        assertEquals(20, network.pendingCount());
        network.deliverAll();

        assertEquals(0, network.leaderCount());
        for (String voter : VOTERS) {
            assertEquals(1, network.node(voter).core.currentTerm());
            assertEquals(voter, network.node(voter).storage.vote);
        }

        network.node("node-1").startElection();
        network.deliver("node-1", "node-2", 2);
        assertEquals(Role.CANDIDATE, network.node("node-1").core.role());
        network.deliver("node-1", "node-3", 2);

        assertEquals(1, network.leaderCount());
        assertEquals(Role.LEADER, network.node("node-1").core.role());
        assertEquals(2, network.node("node-1").core.currentTerm());
        assertEquals(4, network.node("node-1").core.state.nextIndex().size());

        network.deliverAll();
        assertEquals(1, network.leaderCount(), "stale and duplicate traffic must not create another Leader");
        assertEquals(Role.LEADER, network.node("node-1").core.role());
    }

    @Test
    void majorityPartitionElectsLeaderButTwoNodeMinorityCannot() throws Exception {
        Set<String> majority = Set.of("node-1", "node-2", "node-3");
        FakeNetwork network = new FakeNetwork((from, to) ->
                majority.contains(from) == majority.contains(to));

        network.node("node-1").startElection();
        network.node("node-4").startElection();
        network.deliverAll();

        assertEquals(1, network.leaderCount());
        assertEquals(Role.LEADER, network.node("node-1").core.role());
        assertTrue(network.node("node-1").core.currentTerm() >= 1);
        assertFalse(network.node("node-4").core.role() == Role.LEADER);
        assertFalse(network.node("node-5").core.role() == Role.LEADER);
        assertTrue(network.dropped > 0);
        assertTrue(network.droppedAcrossPartition > 0);
    }

    @Test
    void higherTermDelayedResponseStepsDownLeaderAndPersistsClearedVote() throws Exception {
        FakeNetwork network = new FakeNetwork((from, to) -> true);
        network.node("node-1").startElection();
        network.deliver("node-1", "node-2", 1);
        network.deliver("node-1", "node-3", 1);
        assertEquals(Role.LEADER, network.node("node-1").core.role());

        Sent delayed = network.sent.stream()
                .filter(rpc -> rpc.from.equals("node-1") && rpc.request.getTerm() == 1)
                .findFirst().orElseThrow();
        delayed.reply.receive(new RaftNode.RequestVoteResponse(7, false));

        assertEquals(Role.FOLLOWER, network.node("node-1").core.role());
        assertEquals(7, network.node("node-1").core.currentTerm());
        assertNull(network.node("node-1").core.state.votedFor());
        assertEquals(7, network.node("node-1").storage.term);
        assertNull(network.node("node-1").storage.vote);
        assertEquals(0, network.node("node-1").core.state.nextIndex().size());
        assertInstanceOf(ElectionService.IgnoredVote.class,
                network.node("node-1").election.recordVoteResponse("node-4", 1, true));
    }

    @Test
    void restartRestoresTermAndVoteAndRejectsSecondCandidateInSameTerm() throws Exception {
        DurableMemoryStorage storage = new DurableMemoryStorage();
        RaftState firstState = RaftState.restore(0, null, List.of(), 0);
        PersistentElectionState first = new PersistentElectionState(firstState, storage);

        assertEquals(1, first.startElection("node-1"));
        assertEquals(1, storage.term);
        assertEquals("node-1", storage.vote);

        RaftStorage.PersistentState restored = storage.load();
        RaftState restartedState = RaftState.restore(restored.currentTerm(), restored.votedFor(),
                restored.log(), restored.lastApplied());
        PersistentElectionState restarted = new PersistentElectionState(restartedState, storage);

        assertFalse(restarted.grantVote(new RaftNode.RequestVoteRequest(1, "node-2", 0, 0)));
        assertEquals(1, restartedState.currentTerm());
        assertEquals("node-1", restartedState.votedFor());

        assertTrue(restarted.grantVote(new RaftNode.RequestVoteRequest(2, "node-2", 0, 0)));
        assertEquals(2, storage.term);
        assertEquals("node-2", storage.vote);

        RaftState restoredAgain = RaftState.restore(storage.load().currentTerm(), storage.load().votedFor(),
                storage.load().log(), storage.load().lastApplied());
        assertEquals(2, restoredAgain.currentTerm());
        assertEquals("node-2", restoredAgain.votedFor());
    }

    private record Sent(String from, String to, RequestVoteRequestDto request,
            CandidateElection.VoteReply reply) {
    }

    @FunctionalInterface
    private interface Link {
        boolean delivers(String from, String to);
    }

    private static final class FakeNetwork {
        final Map<String, Member> members = new LinkedHashMap<>();
        final List<Sent> sent = new ArrayList<>();
        final Deque<Sent> pending = new ArrayDeque<>();
        final Link link;
        int dropped;
        int droppedAcrossPartition;

        FakeNetwork(Link link) {
            this.link = Objects.requireNonNull(link);
            for (String voter : VOTERS) {
                members.put(voter, new Member(this, voter));
            }
        }

        Member node(String id) {
            return members.get(id);
        }

        int leaderCount() {
            return (int) members.values().stream()
                    .filter(member -> member.core.role() == Role.LEADER)
                    .count();
        }

        int pendingCount() {
            return pending.size();
        }

        void send(String from, String to, RequestVoteRequestDto request,
                CandidateElection.VoteReply reply) {
            Sent rpc = new Sent(from, to, request, reply);
            sent.add(rpc);
            if (link.delivers(from, to)) {
                pending.add(rpc);
            } else {
                dropped++;
                droppedAcrossPartition++;
            }
        }

        void deliver(String from, String to, long term) throws Exception {
            Sent rpc = pending.stream()
                    .filter(candidate -> candidate.from.equals(from)
                            && candidate.to.equals(to)
                            && candidate.request.getTerm() == term)
                    .findFirst()
                    .orElseThrow();
            pending.remove(rpc);
            RaftNode.RequestVoteResponse response =
                    node(to).election.handleRequestVote(rpc.request);
            rpc.reply.receive(response);
            assertTrue(leaderCount() <= 1, "one term must not produce two Leaders");
        }

        void deliverAll() throws Exception {
            while (!pending.isEmpty()) {
                Sent rpc = pending.removeFirst();
                RaftNode.RequestVoteResponse response =
                        node(rpc.to).election.handleRequestVote(rpc.request);
                rpc.reply.receive(response);
                assertTrue(leaderCount() <= 1, "one term must not produce two Leaders");
            }
        }
    }

    private static final class Member {
        final String id;
        final DurableMemoryStorage storage = new DurableMemoryStorage();
        final FakeCore core;
        final CandidateElection election;

        Member(FakeNetwork network, String id) {
            this.id = id;
            core = new FakeCore(id, storage);
            election = new CandidateElection(id, core, core.state,
                    (peer, request, reply) -> network.send(id, peer, request, reply),
                    term -> core.persistence.observeHigherTerm(term),
                    () -> {
                    });
        }

        void startElection() throws Exception {
            election.startElection();
        }
    }

    private static final class FakeCore implements RaftNode {
        final String id;
        final RaftState state = new RaftState();
        final PersistentElectionState persistence;

        FakeCore(String id, DurableMemoryStorage storage) {
            this.id = id;
            persistence = new PersistentElectionState(state, storage);
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
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastLogIndex() {
            return state.lastLogIndex();
        }

        @Override
        public AppendOutcome appendCommand(String commandType, String commandPayload) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
                throws RaftStorageException {
            boolean granted = persistence.grantVote(request);
            return new RequestVoteResponse(state.currentTerm(), granted);
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onElectionTimeout() throws RaftStorageException {
            persistence.startElection(id);
        }

        @Override
        public void addCommitListener(CommitListener listener) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class DurableMemoryStorage implements RaftStorage {
        long term;
        String vote;
        List<RaftLogEntry> log = List.of();

        @Override
        public PersistentState load() {
            return new PersistentState(term, vote, log, 0);
        }

        @Override
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (term < this.term) {
                throw new RaftStorageException("term regression");
            }
            this.term = term;
            this.vote = vote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            log = List.copyOf(entries);
        }

        @Override
        public long loadLastApplied() {
            return 0;
        }
    }
}
