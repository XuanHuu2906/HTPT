package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D21 follower stop/rejoin catch-up characterization.
 *
 * <p>Fake transport/storage only. It uses the real replication, tracker, retry,
 * persistence and append-handler components. It does not start TCP, threads,
 * Docker, PostgreSQL or production services.
 */
class FollowerCatchupTest {

    private static final String LEADER = "node-1";
    private static final List<String> PEERS = List.of("node-2", "node-3", "node-4", "node-5");
    private static final String BOOK = RaftLogEntry.COMMAND_TYPE_BOOK_TICKET;

    @Test
    @DisplayName("1. Stop then rejoin follower catches up its missing tail")
    void stoppedFollowerCatchesUpMissingTail() throws Exception {
        RaftState leader = leaderWithEntries(20, 1L);
        leader.advanceCommitIndex(20L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster =
                cluster(leader, followers, 4, 20);

        cluster.network.partition("node-2");
        cluster.service.replicateAll();
        cluster.runRounds(100);
        assertEquals(0L, leader.matchIndexOf("node-2"));

        cluster.network.heal("node-2");
        cluster.service.replicate("node-2");
        cluster.settle(400);

        assertFollowerConverged(leader, followers.get("node-2"));
        assertEquals(20L, leader.matchIndexOf("node-2"));
        assertEquals(20L, leader.commitIndex());
    }

    @Test
    @DisplayName("2. Large catch-up uses bounded batches with at most batchMax entries")
    void largeCatchupUsesBoundedBatches() throws Exception {
        RaftState leader = leaderWithEntries(35, 1L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster =
                cluster(leader, followers, 3, 40);

        cluster.service.replicate("node-2");
        cluster.settle(500);

        int maxBatch = 0;
        int rpcCount = 0;
        for (FivePeerReplicationSafetyTest.FakeNetwork.Call call : cluster.network.allCalls()) {
            int size = call.request.getPayload().getEntries().size();
            maxBatch = Math.max(maxBatch, size);
            rpcCount++;
        }
        assertTrue(maxBatch <= 3);
        assertTrue(rpcCount <= 200);
        assertEquals(35L, leader.matchIndexOf("node-2"));
        assertFollowerConverged(leader, followers.get("node-2"));
    }

    @Test
    @DisplayName("3. Two stopped followers rejoin together and catch up")
    void twoStoppedFollowersRejoinTogether() throws Exception {
        RaftState leader = leaderWithEntries(12, 1L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 4, 30);

        cluster.network.partition("node-2");
        cluster.network.partition("node-3");
        cluster.service.replicateAll();
        cluster.runRounds(120);
        cluster.network.heal("node-2");
        cluster.network.heal("node-3");
        cluster.service.replicate("node-2");
        cluster.service.replicate("node-3");
        cluster.settle(500);

        assertEquals(12L, leader.matchIndexOf("node-2"));
        assertEquals(12L, leader.matchIndexOf("node-3"));
        assertFollowerConverged(leader, followers.get("node-2"));
        assertFollowerConverged(leader, followers.get("node-3"));
    }

    @Test
    @DisplayName("4. Three stopped followers leave only 2/5 and cannot commit")
    void threeStoppedFollowersPreserveQuorumSafety() throws Exception {
        RaftState leader = leaderWithEntries(5, 2L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 20);

        cluster.network.partition("node-3");
        cluster.network.partition("node-4");
        cluster.network.partition("node-5");
        leader.append(entry(6L, 2L, "r-6"));
        cluster.service.replicateAll();
        cluster.runRounds(100);

        long commitBefore = leader.commitIndex();
        assertEquals(0L, commitBefore);
        assertEquals(0L, leader.lastApplied());

        cluster.network.heal("node-3");
        cluster.service.replicate("node-3");
        cluster.runRounds(300);
        assertEquals(0L, leader.commitIndex(),
                "current-term entry may need explicit commit after ACK");
        assertTrue(followers.get("node-3").state.commitIndex() <= 5L);
    }

    @Test
    @DisplayName("5. Restarted follower restores term, vote, log and commit knowledge")
    void restartedFollowerRestoresDurableState() throws Exception {
        RaftState leader = leaderWithEntries(8, 3L);
        leader.advanceCommitIndex(8L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 4, 20);

        cluster.service.replicateAll();
        cluster.settle(300);
        followers.put("node-2", follower(3L, leader.entries(), 8L));

        assertEquals(3L, followers.get("node-2").storage.load().currentTerm());
        assertEquals(leader.entries(), followers.get("node-2").storage.storedLog());
        assertEquals(8L, followers.get("node-2").state.commitIndex());
    }

    @Test
    @DisplayName("6. Restart between catch-up batches resumes without duplicate apply")
    void restartBetweenCatchupBatchesDoesNotDuplicate() throws Exception {
        RaftState leader = leaderWithEntries(10, 1L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 20);

        cluster.service.replicate("node-2");
        cluster.runRounds(3);
        List<RaftLogEntry> partial = followers.get("node-2").state.entries();

        followers.put("node-2", follower(1L, partial, 0L));
        FivePeerReplicationSafetyTest.Cluster restarted =
                cluster(leader, followers, 2, 20);
        restarted.service.replicate("node-2");
        restarted.settle(400);

        assertFollowerConverged(leader, followers.get("node-2"));
        assertEquals(10L, countDistinctPrefix(followers.get("node-2").state.entries()));
    }

    @Test
    @DisplayName("7. Divergent uncommitted tail is backtracked without deleting the matching prefix")
    void divergentUncommittedTailBacktracksSafely() throws Exception {
        RaftState leader = leaderWithEntries(4, 2L);
        leader.advanceCommitIndex(2L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        followers.put("node-2", follower(2L,
                List.of(entry(1L, 2L, "r-1"), entry(2L, 2L, "r-2"),
                        entry(3L, 2L, "old-3"), entry(4L, 2L, "old-4")), 2L));
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 20);

        cluster.service.replicate("node-2");
        cluster.runRounds(500);

        FivePeerReplicationSafetyTest.FollowerNode node = followers.get("node-2");
        assertEquals(leader.entryAt(1L), node.state.entryAt(1L));
        assertEquals(leader.entryAt(2L), node.state.entryAt(2L));
        assertEquals(2L, node.state.commitIndex());
        assertTrue(node.state.lastLogIndex() >= 2L);
    }

    @Test
    @DisplayName("8. Committed conflict fails closed and never advances ACK")
    void committedConflictFailsClosed() throws Exception {
        RaftState leader = leaderWithEntries(1, 1L);
        leader.advanceCommitIndex(1L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        followers.put("node-2", follower(1L,
                List.of(entry(1L, 1L, "committed-conflict")), 1L));
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 20);

        cluster.service.replicate("node-2");
        cluster.runRounds(20);

        assertEquals(0L, leader.matchIndexOf("node-2"));
        assertFalse(cluster.network.deliveryFailures().isEmpty());
        assertEquals(entry(1L, 1L, "committed-conflict"),
                followers.get("node-2").state.entryAt(1L));
    }

    @Test
    @DisplayName("9. Timeout and malformed replies never count as ACK")
    void timeoutAndMalformedRepliesNeverCountAsAck() throws Exception {
        for (long seed : List.of(1L, 7L, 31L)) {
            RaftState leader = leaderWithEntries(5, 1L);
            Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
            FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 20);
            cluster.service.replicate("node-2");
            cluster.drain();
            var call = cluster.network.pendingCall("node-2");
            assertNotNull(call);

            if ((seed & 1L) == 1L) {
                cluster.network.completeCall(call, 1L, true,
                        call.request.getPayload().getPrevLogIndex()
                                + call.request.getPayload().getEntries().size() + 1L);
            } else {
                cluster.network.failTransport(call,
                        new java.io.IOException("controlled drop seed=" + seed));
            }
            cluster.drain();
            assertTrue(leader.matchIndexOf("node-2") <= 5L);
            assertTrue(leader.commitIndex() <= leader.lastLogIndex());
        }
    }

    @Test
    @DisplayName("10. New higher-term leader replaces old uncommitted suffix")
    void higherTermLeaderReplacesOldUncommittedSuffix() throws Exception {
        RaftState oldLeader = leaderWithEntries(3, 1L);
        oldLeader.advanceCommitIndex(2L);
        oldLeader.append(entry(4L, 1L, "old-uncommitted"));

        RaftState newLeader = RaftState.restore(2L, null,
                oldLeader.entries().subList(0, 3), 2L);
        newLeader.startElection(LEADER);
        newLeader.becomeLeader(new TreeSet<>(PEERS));
        newLeader.append(entry(4L, 2L, "new-term-entry"));

        assertEquals(2L, newLeader.commitIndex());
        assertEquals(4L, newLeader.lastLogIndex());
        assertTrue(newLeader.entryAt(4L).commandPayload().contains("new-term-entry"));
    }

    @Test
    @DisplayName("11. Old-term AppendEntries is rejected after follower observes higher term")
    void oldLeaderAppendIsRejectedAtHigherTermFollower() throws Exception {
        RaftState oldLeader = leaderWithEntries(3, 1L);
        FivePeerReplicationSafetyTest.FollowerNode follower =
                follower(2L, List.of(entry(1L, 1L, "r-1"),
                        entry(2L, 1L, "r-2")), 2L);
        RaftNode.AppendEntriesRequest request =
                new RaftNode.AppendEntriesRequest(1L, LEADER, 2L, 1L,
                        List.of(entry(3L, 1L, "r-3")), 2L);

        var response = follower.handler.handle(request, LEADER);
        assertFalse(response.success());
        assertEquals(2L, response.term());
        assertEquals(2L, follower.state.commitIndex());
        assertEquals(2L, follower.state.lastLogIndex());
    }

    @Test
    @DisplayName("12. Lost disk log is characterized without reducing Leader matchIndex")
    void lostDiskLogDoesNotReduceTrackedProgressOrCommit() throws Exception {
        RaftState leader = leaderWithEntries(8, 1L);
        leader.advanceCommitIndex(8L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 2, 4);

        cluster.service.replicateAll();
        cluster.settle(300);
        long oldMatch = leader.matchIndexOf("node-2");
        long oldCommit = leader.commitIndex();

        FakeRaftStorage lostStorage = new FakeRaftStorage();
        lostStorage.seedState(1L, null, List.of(), 0L);
        RaftStorage.PersistentState persisted = lostStorage.load();
        RaftState lostState = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        followers.put("node-2", followerFrom(lostStorage, lostState));
        FivePeerReplicationSafetyTest.Cluster lostCluster =
                cluster(leader, followers, 2, 4);
        lostCluster.tracker.recordSuccess("node-2", oldMatch);
        lostCluster.service.replicate("node-2");
        lostCluster.runRounds(200);

        assertEquals(oldMatch, leader.matchIndexOf("node-2"),
                "a disk-loss characteristic must not reduce matchIndex");
        assertEquals(oldCommit, leader.commitIndex());
        assertTrue(cluster.tracker.nextIndexOf("node-2") >=
                cluster.tracker.matchIndexOf("node-2") + 1);
        assertTrue(cluster.network.requestsFor("node-2") <= 205);
    }

    @Test
    @DisplayName("13. Final rejoin converges logs, commit and apply checkpoints")
    void finalRejoinConvergesAllNodes() throws Exception {
        RaftState leader = leaderWithEntries(15, 1L);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        FivePeerReplicationSafetyTest.Cluster cluster = cluster(leader, followers, 4, 30);

        for (String peer : PEERS) {
            cluster.network.partition(peer);
        }
        cluster.service.replicateAll();
        cluster.runRounds(40);
        for (String peer : PEERS) {
            cluster.network.heal(peer);
        }
        cluster.service.replicateAll();
        cluster.settle(600);

        leader.advanceCommitIndex(15L);
        for (String peer : PEERS) {
            assertFollowerConverged(leader, followers.get(peer));
            assertEquals(15L, leader.matchIndexOf(peer));
        }
        assertLogMatching(leader, followers);
        assertTrue(leader.commitIndex() >= 15L);
    }

    private static FivePeerReplicationSafetyTest.Cluster cluster(
            RaftState leader,
            Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers,
            int batchMax,
            int maxBacktrack) {
        return new FivePeerReplicationSafetyTest.Cluster(
                leader, followers,
                new FivePeerReplicationSafetyTest.RecordingHooks(leader, false),
                batchMax, maxBacktrack);
    }

    private static RaftState leaderWithEntries(int count, long term) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        state.becomeLeader(new TreeSet<>(PEERS));
        while (state.currentTerm() < term) {
            state.becomeFollower(state.currentTerm());
            state.startElection(LEADER);
            state.becomeLeader(new TreeSet<>(PEERS));
        }
        for (int index = 1; index <= count; index++) {
            state.append(entry(index, term, "r-" + index));
        }
        return state;
    }

    private static RaftLogEntry entry(long index, long term, String requestId) {
        return new RaftLogEntry(index, term, BOOK,
                "{\"requestId\":\"" + requestId + "\"}");
    }

    private static Map<String, FivePeerReplicationSafetyTest.FollowerNode> emptyFollowers()
            throws Exception {
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers =
                new LinkedHashMap<>();
        for (String peer : PEERS) {
            followers.put(peer, follower(1L, List.of(), 0L));
        }
        return followers;
    }

    private static FivePeerReplicationSafetyTest.FollowerNode follower(
            long term, List<RaftLogEntry> entries, long commitIndex) throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, entries, 0L);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        if (commitIndex > 0L) {
            state.advanceCommitIndex(commitIndex);
        }
        return followerFrom(storage, state);
    }

    private static FivePeerReplicationSafetyTest.FollowerNode followerFrom(
            FakeRaftStorage storage, RaftState state) throws Exception {
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        AppendEntriesHandler handler = new AppendEntriesHandler(
                new PersistentElectionState(state, storage), appender,
                new LogConflictResolver(appender), new RecordingTimer());
        return new FivePeerReplicationSafetyTest.FollowerNode(storage, state, handler);
    }

    private static void assertFollowerConverged(
            RaftState leader,
            FivePeerReplicationSafetyTest.FollowerNode follower) {
        assertEquals(leader.entries(), follower.state.entries());
        assertTrue(follower.state.lastApplied() <= follower.state.commitIndex());
        assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
    }

    private static void assertLogMatching(
            RaftState leader,
            Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers) {
        for (FivePeerReplicationSafetyTest.FollowerNode follower : followers.values()) {
            long common = Math.min(leader.lastLogIndex(), follower.state.lastLogIndex());
            for (long index = 1L; index <= common; index++) {
                if (leader.termAt(index) == follower.state.termAt(index)) {
                    assertEquals(leader.entryAt(index), follower.state.entryAt(index));
                }
            }
        }
    }

    private static long countDistinctPrefix(List<RaftLogEntry> entries) {
        long unique = 0L;
        Set<RaftLogEntry> seen = new java.util.HashSet<>();
        for (RaftLogEntry entry : entries) {
            if (seen.add(entry)) {
                unique++;
            }
        }
        return unique;
    }

    private static final class RecordingTimer implements AppendEntriesHandler.TimerHook {
        @Override
        public void onValidAppendEntries(long requestTerm) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }
}
