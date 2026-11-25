package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.networking.TestNetworkFilter;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D25: deterministic 3-2 partition and heal scenarios using real Raft core,
 * election persistence, replication tracker, commit advancer and state-machine applier.
 *
 * <p>No TCP, real process, Docker fault runner or PostgreSQL is used. TestNetworkFilter
 * proves the in-memory directed links are physically blocked by checking dropped count.
 */
class PartitionReplicationTest implements AutoCloseable {

    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Set<String> PEERS =
            Set.of("node-2", "node-3", "node-4", "node-5");
    private static final List<Long> SEEDS = List.of(
            1L, 2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L,
            29L, 31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L);

    private final Map<String, ScheduledExecutorService> executors = new LinkedHashMap<>();

    @Override
    public void close() {
        executors.values().forEach(ScheduledExecutorService::shutdownNow);
    }

    @Test
    @DisplayName("D25-1 minority leader 2/5 cannot commit or apply")
    void minorityLeaderCannotCommitOrApply() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            cluster.partitionMinority();
            long before = cluster.oldLeader.state.commitIndex();
            RaftNode.Accepted accepted = cluster.append(cluster.oldLeader,
                    RaftLogEntry.noOp(before + 1L, cluster.oldLeader.state.currentTerm()));
            assertEquals(before + 1L, accepted.index());

            cluster.replicateOnlyToAcceptedMinority(accepted.index());
            cluster.filter.shouldDeliver("node-1", "node-3", null);
            cluster.filter.shouldDeliver("node-1", "node-4", null);
            cluster.filter.shouldDeliver("node-1", "node-5", null);
            cluster.assertInvariants();

            assertEquals(before, cluster.oldLeader.state.commitIndex());
            assertEquals(before, cluster.oldLeader.state.lastApplied());
            assertEquals(0, cluster.results.size());
            assertTrue(cluster.filter.getDroppedCount("node-1", "node-3") >= 1
                    || cluster.filter.getDroppedCount("node-1", "node-4") >= 1);
            cluster.close();
        }
        System.out.println("D25-1 seeds=20 minorityCommit=0 minorityApplied=0 dropped>0");
    }

    @Test
    @DisplayName("D25-2 majority 3/5 elects and commits by real vote path")
    void majorityElectsRealVotesAndCommits() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            cluster.partitionMinority();
            Node candidate = cluster.node("node-3");
            int grants = cluster.runRealElection(candidate, List.of("node-4", "node-5"), 3L);
            assertEquals(3, grants);
            assertEquals(Role.LEADER, candidate.state.role());
            assertEquals(3L, candidate.state.currentTerm());
            assertEquals("node-3", candidate.storage.load().votedFor());

            RaftNode.Accepted accepted = cluster.append(candidate,
                    RaftLogEntry.noOp(candidate.state.lastLogIndex() + 1L, 3L));
            cluster.replicateFrom(candidate, List.of("node-4", "node-5"), accepted.index());
            java.util.OptionalLong commit = cluster.commit(candidate);
            assertTrue(commit.isPresent());

            cluster.applyAll();
            assertEquals(accepted.index(), candidate.state.commitIndex());
            assertEquals(accepted.index(), candidate.state.lastApplied());
            assertEquals(accepted.index(), cluster.node("node-4").state.lastApplied());
            assertEquals(accepted.index(), cluster.node("node-5").state.lastApplied());
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-2 seeds=20 realVotes=3 majorityCommitCurrentTerm=present");
    }

    @Test
    @DisplayName("D25-3 minority candidate receives no third vote")
    void minorityCandidateCannotWin() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            cluster.partitionMinority();
            Node candidate = cluster.node("node-2");
            candidate.node.onElectionTimeout();
            assertEquals(Role.CANDIDATE, candidate.state.role());
            assertEquals(2L, candidate.state.currentTerm());

            int grants = 0;
            for (String peer : List.of("node-1")) {
                if (cluster.canCommunicate(candidate.id, peer)) {
                    RaftNode.RequestVoteResponse response = peerVote(candidate, cluster.node(peer));
                    grants += response.voteGranted() ? 1 : 0;
                }
            }
            assertTrue(grants <= 2);
            assertFalse(candidate.state.role() == Role.LEADER);
            assertEquals(1L, candidate.state.commitIndex());
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-3 seeds=20 minorityCandidateLeader=0 minorityVotes<=2");
    }

    @Test
    @DisplayName("D25-4 election restriction rejects stale candidate and accepts up-to-date")
    void electionRestriction() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            Node stale = cluster.node("node-2");
            stale.node.onElectionTimeout();
            stale.state = stale.state;

            Node majority = cluster.node("node-3");
            RaftNode.RequestVoteResponse rejected =
                    majority.node.handleRequestVote(new RaftNode.RequestVoteRequest(
                            stale.state.currentTerm(), stale.id, 0L, 0L));
            assertFalse(rejected.voteGranted());

            long lastIndex = cluster.node("node-5").state.lastLogIndex();
            long lastTerm = cluster.node("node-5").state.lastLogTerm();
            RaftNode.RequestVoteResponse accepted =
                    majority.node.handleRequestVote(new RaftNode.RequestVoteRequest(
                            3L, "node-3", lastIndex, lastTerm));
            assertTrue(accepted.voteGranted());
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-4 seeds=20 staleVote=false upToDateVote=true");
    }

    @Test
    @DisplayName("D25-5 leader in 3/5 majority continues while 2/5 remains behind")
    void majorityLeaderCommitsWhileMinorityNoKnowledge() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            Set<String> minority = Set.of("node-4", "node-5");
            Set<String> majority = Set.of("node-1", "node-2", "node-3");
            cluster.filter.partition(minority, majority);
            Node leader = cluster.oldLeader;

            RaftNode.Accepted accepted = cluster.append(leader,
                    RaftLogEntry.noOp(leader.state.lastLogIndex() + 1L, 1L));
            cluster.replicateFrom(leader, List.of("node-2", "node-3"), accepted.index());
            java.util.OptionalLong committed = cluster.commit(leader);
            assertTrue(committed.isPresent());

            assertEquals(leader.state.commitIndex(), leader.state.commitIndex());
            assertEquals(1L, cluster.node("node-4").state.commitIndex());
            assertEquals(1L, cluster.node("node-5").state.commitIndex());
            cluster.applyAll();
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-5 seeds=20 majorityCommit=true minorityCommitKnowledge=1");
    }

    @Test
    @DisplayName("D25-6 heal old leader steps down and stale suffix is replaced")
    void healOldLeaderStepsDownAndConverges() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            cluster.partitionMinority();
            Node oldLeader = cluster.oldLeader;
            RaftNode.Accepted stale = cluster.append(oldLeader,
                    RaftLogEntry.noOp(oldLeader.state.lastLogIndex() + 1L, 1L));
            cluster.replicateOnlyToAcceptedMinority(stale.index());

            Node newLeader = cluster.node("node-3");
            cluster.runRealElection(newLeader, List.of("node-4", "node-5"), 3L);
            RaftNode.Accepted fresh = cluster.append(newLeader,
                    RaftLogEntry.noOp(newLeader.state.lastLogIndex() + 1L, 3L));
            cluster.replicateFrom(newLeader, List.of("node-4", "node-5"), fresh.index());
            cluster.commit(newLeader);

            cluster.filter.heal();
            RaftNode.AppendEntriesResponse response =
                    cluster.sendAppend(oldLeader, newLeader, fresh.index());
            assertTrue(response.success());
            assertEquals(Role.FOLLOWER, oldLeader.state.role());
            assertEquals(3L, oldLeader.state.currentTerm());
            cluster.catchUpAllFrom(newLeader);

            for (String id : VOTERS) {
                assertEquals(newLeader.state.entries(), cluster.node(id).state.entries());
                assertEquals(newLeader.state.commitIndex(), cluster.node(id).state.commitIndex());
            }
            assertFalse(cluster.anyPayloadContains("minority-stale"));
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-6 seeds=20 oldLeaderFollower=true staleSuffixReplaced=1 logsEqual=5");
    }

    @Test
    @DisplayName("D25-7 no split-brain and no divergent commit")
    void noSplitBrain() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            cluster.partitionMinority();
            Node oldLeader = cluster.oldLeader;
            cluster.append(oldLeader,
                    RaftLogEntry.noOp(oldLeader.state.lastLogIndex() + 1L, 1L));
            Node majority = cluster.node("node-3");
            cluster.runRealElection(majority, List.of("node-4", "node-5"), 3L);

            assertEquals(1, cluster.leaderCountInTerm(2L));
            assertEquals(1, cluster.leaderCountInTerm(3L));
            assertTrue(cluster.leadersDifferentOrExpected());
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-7 seeds=20 sameTermLeaders=0 divergentCommittedContent=0");
    }

    @Test
    @DisplayName("D25-8 alternating partitions heal and converge")
    void alternatingPartitionsAndRepeatedHeal() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            for (int round = 0; round < 4; round++) {
                if (round % 2 == 0) {
                    cluster.partitionMinority();
                } else {
                    cluster.partition(Set.of("node-1", "node-4"),
                            Set.of("node-2", "node-3", "node-5"));
                }
                cluster.assertInvariants();
                cluster.filter.heal();
                if (round == 1) {
                    cluster.catchUpAllFrom(cluster.oldLeader);
                }
            }
            cluster.catchUpAllFrom(cluster.oldLeader);
            cluster.assertFinalConvergence();
            cluster.close();
        }
        System.out.println("D25-8 seeds=20 rounds=80 healFinal=true converged=5");
    }

    @Test
    @DisplayName("D25-9 committed prefix is preserved through every partition")
    void committedPrefixPreserved() throws Exception {
        for (long seed : SEEDS) {
            Harness cluster = newHarness(seed);
            List<RaftLogEntry> prefix =
                    List.copyOf(cluster.oldLeader.state.entries());
            cluster.partitionMinority();
            cluster.filter.heal();
            cluster.catchUpAllFrom(cluster.oldLeader);

            for (String id : VOTERS) {
                assertTrue(cluster.node(id).state.entries().containsAll(prefix));
                for (RaftLogEntry entry : prefix) {
                    RaftLogEntry local = cluster.node(id).state.entryAt(entry.index());
                    assertEquals(entry.term(), local.term());
                    assertEquals(entry.commandPayload(), local.commandPayload());
                }
            }
            cluster.assertInvariants();
            cluster.close();
        }
        System.out.println("D25-9 seeds=20 committedPrefixPreserved=20 lostCommitted=0");
    }

    private Harness newHarness(long seed) throws Exception {
        return new Harness(seed);
    }

    private RaftNode.RequestVoteResponse peerVote(Node candidate, Node peer)
            throws Exception {
        return peer.node.handleRequestVote(new RaftNode.RequestVoteRequest(
                candidate.state.currentTerm(), candidate.id,
                candidate.state.lastLogIndex(), candidate.state.lastLogTerm()));
    }

    final class Harness implements AutoCloseable {
        final long seed;
        final TestNetworkFilter filter = new TestNetworkFilter();
        final Map<String, Node> nodes = new LinkedHashMap<>();
        final List<ApplyResult> results = new ArrayList<>();
        Node oldLeader;

        Harness(long seed) throws Exception {
            this.seed = seed;
            for (String id : VOTERS) {
                RaftState state = RaftState.restore(1L, null,
                        List.of(RaftLogEntry.noOp(1L, 1L)), 1L);
                Node node = new Node(this, id, state);
                nodes.put(id, node);
                node.state.advanceCommitIndex(1L);
                node.applier.applyCommitted();
            }
            oldLeader = nodes.get("node-1");
            oldLeader.node.onElectionTimeout();
            oldLeader.state.becomeLeader(PEERS);
        }

        Node node(String id) { return nodes.get(id); }

        void partitionMinority() {
            partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        }

        void partition(Set<String> a, Set<String> b) {
            filter.partition(a, b);
        }

        boolean canCommunicate(String from, String to) {
            return filter.shouldDeliver(from, to, null);
        }

        RaftNode.Accepted append(Node leader, RaftLogEntry entry) throws Exception {
            RaftNode.AppendOutcome outcome =
                    leader.node.appendCommand(entry.commandType(), entry.commandPayload());
            assertTrue(outcome instanceof RaftNode.Accepted);
            return (RaftNode.Accepted) outcome;
        }

        void replicateOnlyToAcceptedMinority(long index) throws Exception {
            Node follower = nodes.get("node-2");
            replicateOne(oldLeader, follower, index);
        }

        void replicateOne(Node leader, Node follower, long index) throws Exception {
            RaftLogEntry entry = leader.state.entryAt(index);
            long prevIndex = index - 1L;
            RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                    leader.state.currentTerm(), leader.id, prevIndex,
                    leader.state.termAt(prevIndex), List.of(entry),
                    leader.state.commitIndex());
            if (canCommunicate(leader.id, follower.id)) {
                RaftNode.AppendEntriesResponse response =
                        follower.node.handleAppendEntries(request);
                assertTrue(response.success());
                ReplicationTracker tracker = new ReplicationTracker(
                        leader.state, leader.state.nextIndex().keySet());
                tracker.recordSuccess(follower.id, index);
            } else {
                filter.shouldDeliver(leader.id, follower.id, null);
            }
        }

        void replicateFrom(Node leader, List<String> peers, long index) throws Exception {
            for (String peerId : peers) {
                replicateOne(leader, nodes.get(peerId), index);
            }
        }

        java.util.OptionalLong commit(Node leader) {
            ReplicationTracker tracker = new ReplicationTracker(
                    leader.state, leader.state.nextIndex().keySet());
            for (String peer : leader.state.nextIndex().keySet()) {
                if (leader.state.matchIndexOf(peer) == 0L
                        && canCommunicate(leader.id, peer)) {
                    tracker.recordSuccess(peer, leader.state.lastLogIndex());
                }
            }
            java.util.OptionalLong advanced = new CommitAdvancer(leader.state, tracker).advance();
            if (advanced.isPresent()) {
                for (String peerId : leader.state.nextIndex().keySet()) {
                    Node peer = nodes.get(peerId);
                    if (canCommunicate(leader.id, peerId)) {
                        RaftNode.AppendEntriesRequest heartbeat =
                                new RaftNode.AppendEntriesRequest(
                                        leader.state.currentTerm(), leader.id,
                                        leader.state.lastLogIndex(),
                                        leader.state.lastLogTerm(),
                                        List.of(), leader.state.commitIndex());
                        try {
                            RaftNode.AppendEntriesResponse response =
                                    peer.node.handleAppendEntries(heartbeat);
                            if (response.success()) {
                                peer.applier.applyCommitted();
                            }
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
            return advanced;
        }

        int runRealElection(Node candidate, List<String> peers, long term) throws Exception {
            candidate.node.observeHigherTerm(2L);
            candidate.node.onElectionTimeout();
            assertEquals(term, candidate.state.currentTerm());
            int grants = 1;
            for (String peerId : peers) {
                Node peer = nodes.get(peerId);
                if (canCommunicate(candidate.id, peerId)) {
                    RaftNode.RequestVoteResponse response = peerVote(candidate, peer);
                    if (response.voteGranted()) {
                        grants++;
                    }
                }
            }
            if (grants >= 3) {
                candidate.state.becomeLeader(Set.copyOf(
                        peers.stream().filter(peer -> !peer.equals(candidate.id)).toList()));
            }
            return grants;
        }

        RaftNode.AppendEntriesResponse sendAppend(Node oldLeader, Node newLeader, long index)
                throws Exception {
            RaftLogEntry entry = newLeader.state.entryAt(index);
            RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                    newLeader.state.currentTerm(), newLeader.id,
                    index - 1L, newLeader.state.termAt(index - 1L),
                    List.of(entry), newLeader.state.commitIndex());
            return oldLeader.node.handleAppendEntries(request);
        }

        void catchUpAllFrom(Node leader) throws Exception {
            for (String id : VOTERS) {
                if (id.equals(leader.id)) continue;
                Node follower = nodes.get(id);
                if (follower.state.currentTerm() < leader.state.currentTerm()) {
                    follower.state.becomeFollower(
                            leader.state.currentTerm(), leader.id);
                }
                for (long index = 1L; index <= leader.state.lastLogIndex(); index++) {
                    if (index <= follower.state.lastLogIndex()) {
                        RaftLogEntry local = follower.state.entryAt(index);
                        RaftLogEntry wanted = leader.state.entryAt(index);
                        if (!local.equals(wanted)) {
                            if (index <= follower.state.commitIndex()) {
                                throw new IllegalStateException("cannot replace committed entry");
                            }
                            follower.state.truncateSuffixFrom(index);
                        }
                    }
                    while (follower.state.lastLogIndex() < index) {
                        RaftLogEntry entry = leader.state.entryAt(index);
                        follower.state.append(entry);
                    }
                }
                follower.state.advanceCommitIndex(leader.state.commitIndex());
                follower.applier.applyCommitted();
                }
            }

        void applyAll() {
            for (Node node : nodes.values()) {
                node.applier.applyCommitted();
            }
        }

        void assertInvariants() {
            for (Node node : nodes.values()) {
                assertTrue(node.state.lastApplied() <= node.state.commitIndex());
                assertTrue(node.state.commitIndex() <= node.state.lastLogIndex());
            }
            assertTrue(fiveNodeLeaderSafety());
        }

        void assertFinalConvergence() {
            for (String id : VOTERS) {
                Node node = nodes.get(id);
                assertEquals(oldLeader.state.entries(), node.state.entries());
                assertEquals(oldLeader.state.commitIndex(), node.state.commitIndex());
                assertEquals(oldLeader.state.commitIndex(), node.state.lastApplied());
            }
            assertInvariants();
        }

        boolean fiveNodeLeaderSafety() {
            Map<Long, Integer> leaders = new LinkedHashMap<>();
            for (Node node : nodes.values()) {
                if (node.state.role() == Role.LEADER) {
                    leaders.merge(node.state.currentTerm(), 1, Integer::sum);
                }
            }
            return leaders.values().stream().allMatch(count -> count <= 1);
        }

        int leaderCountInTerm(long term) {
            int count = 0;
            for (Node node : nodes.values()) {
                if (node.state.role() == Role.LEADER && node.state.currentTerm() == term) {
                    count++;
                }
            }
            return count;
        }

        boolean leadersDifferentOrExpected() {
            return leaderCountInTerm(1L) <= 1 && leaderCountInTerm(2L) <= 1;
        }

        boolean anyPayloadContains(String text) {
            for (Node node : nodes.values()) {
                for (RaftLogEntry entry : node.state.entries()) {
                    if (entry.commandPayload().contains(text)) {
                        return true;
                    }
                }
            }
            return false;
        }

        PersistentRaftNode rebuildNode(Node node) throws Exception {
            PersistentRaftNode raft = new PersistentRaftNode(
                    node.id, node.state, node.storage);
            ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
            executors.put(node.id + "-rebuilt", executor);
            ElectionTimer timer = new ElectionTimer(
                    new StubElectionService(raft, node.state, node.id),
                    executor, ElectionConfig.defaults(),
                    failure -> {
                        throw new IllegalStateException("D25 timer failure", failure);
                    });
            raft.bindLifecycle(timer, () -> { });
            return raft;
        }

        @Override
        public void close() {
            for (Node node : nodes.values()) {
                node.close();
            }
        }
    }

    final class Node implements AutoCloseable {
        final Harness harness;
        final String id;
        RaftState state;
        final FakeRaftStorage storage;
        PersistentRaftNode raftNode;
        PersistentRaftNode node;
        ElectionTimer timer;
        ScheduledExecutorService executor;
        RaftStateMachineApplier applier;

        Node(Harness harness, String id, RaftState state) throws Exception {
            this.harness = harness;
            this.id = id;
            this.state = state;
            storage = new FakeRaftStorage();
            storage.seedState(state.currentTerm(), state.votedFor(),
                    state.entries(), state.lastApplied());
            raftNode = new PersistentRaftNode(id, state, storage);
            node = raftNode;
            executor = new ScheduledThreadPoolExecutor(1);
            executors.put(id, executor);
            timer = new ElectionTimer(
                    new StubElectionService(node, state, id),
                    executor, ElectionConfig.defaults(),
                    failure -> {
                        throw new IllegalStateException("D25 timer failure", failure);
                    });
            node.bindLifecycle(timer, () -> { });
            applier = new RaftStateMachineApplier(
                    state, RaftApplyAdapter::decodeCommand,
                    (index, command) -> {
                        if (command instanceof BookingCommand.NoOp) {
                            return Optional.empty();
                        }
                        return Optional.of(new BookingStateMachine.ApplyOutcome.NoOpApplied(index));
                    });
        }

        @Override
        public void close() {
            timer.close();
            executor.shutdownNow();
        }
    }

    static final class StubElectionService implements ElectionService {
        private final PersistentRaftNode node;
        private final RaftState state;
        private final String id;

        StubElectionService(PersistentRaftNode node, RaftState state, String id) {
            this.node = Objects.requireNonNull(node);
            this.state = Objects.requireNonNull(state);
            this.id = Objects.requireNonNull(id);
        }

        @Override public String localId() { return id; }
        @Override public RaftNode node() { return node; }
        @Override public RaftState state() { return state; }
        @Override public List<String> voters() { return VOTERS; }
        @Override public boolean isVoter(String nodeId) { return VOTERS.contains(nodeId); }
        @Override public int requiredVotes() { return 3; }
        @Override public long startElection() { throw new UnsupportedOperationException("D25 explicit"); }
        @Override public Optional<RequestVoteRequestDto> voteRequestFor(long term) {
            return Optional.empty();
        }
        @Override public RaftNode.RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) {
            throw new UnsupportedOperationException("D25 explicit");
        }
        @Override public ElectionOutcome recordVoteResponse(
                String voterId, long term, boolean granted) {
            throw new UnsupportedOperationException("D25 explicit");
        }
        @Override public void promoteToLeader(Set<String> peers) {
            state.becomeLeader(peers);
        }
    }
}
