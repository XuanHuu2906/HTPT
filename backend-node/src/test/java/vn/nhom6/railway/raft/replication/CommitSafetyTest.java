package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D16 tests for the SPEC-04 section 5 majority/current-term commit rule.
 *
 * <p><strong>Honesty boundary.</strong> State, tracker, replication and follower handlers are
 * in-memory objects; transport is a fake network and storage is {@link FakeRaftStorage}. No TCP,
 * five real Nodes, Docker, PostgreSQL durability or state-machine apply is exercised here. The
 * commit advance is computed on the serialized test loop, and no JDBC or listener is called.
 */
class CommitSafetyTest {

    private static final String LEADER = "node-1";
    private static final Set<String> PEERS =
            new TreeSet<>(List.of("node-2", "node-3", "node-4", "node-5"));
    private static final String BOOK = RaftLogEntry.COMMAND_TYPE_BOOK_TICKET;

    // -------------------------------------------------------------------------------------------
    // Majority and current-term rule
    // -------------------------------------------------------------------------------------------

    @Test
    void commitsWithLeaderPlusTwoFollowersAtCurrentTerm() {
        RaftState state = leaderTermOne(3);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 3L);
        state.advanceMatchIndex("node-3", 3L);

        OptionalLong advanced = new CommitAdvancer(state, tracker).advance();

        assertTrue(advanced.isPresent(), "3/5 must commit");
        assertEquals(3L, advanced.getAsLong());
        assertEquals(3L, state.commitIndex());
    }

    @Test
    void doesNotCommitWithOnlyTwoOfFiveNodes() {
        RaftState state = leaderTermOne(3);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 3L);

        OptionalLong advanced = new CommitAdvancer(state, tracker).advance();

        assertTrue(advanced.isEmpty(), "Leader + 1 follower is only 2/5");
        assertEquals(0L, state.commitIndex());
    }

    @Test
    void oldTermEntryIsNotCommittedDirectly() {
        RaftState state = oldTermLeader(2);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 2L);
        state.advanceMatchIndex("node-3", 2L);

        OptionalLong advanced = new CommitAdvancer(state, tracker).advance();

        assertTrue(advanced.isEmpty(), "an old-term entry on 3/5 must not be committed directly");
        assertEquals(0L, state.commitIndex());
        assertEquals(1L, state.termAt(2L));
        assertEquals(2L, state.currentTerm());
    }

    @Test
    void currentTermEntryCommitsOldTermPrefixIndirectly() {
        RaftState state = oldTermLeader(2);
        state.append(entry(3L, state.currentTerm(), "r-3"));
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 3L);
        state.advanceMatchIndex("node-3", 3L);

        OptionalLong advanced = new CommitAdvancer(state, tracker).advance();

        assertTrue(advanced.isPresent());
        assertEquals(3L, advanced.getAsLong());
        assertEquals(3L, state.commitIndex(),
                "committing index 3 indirectly commits the term-1 prefix at indexes 1 and 2");
        assertEquals(1L, state.termAt(1L));
        assertEquals(1L, state.termAt(2L));
        assertEquals(2L, state.termAt(3L));
    }

    @Test
    void figure8OldEntryOnMajorityIsNeverCommittedByItsOldLeader() {
        RaftState oldLeader = oldTermLeader(2);
        ReplicationTracker oldTracker = new ReplicationTracker(oldLeader, PEERS);
        oldLeader.advanceMatchIndex("node-2", 2L);
        oldLeader.advanceMatchIndex("node-3", 2L);

        OptionalLong oldAdvance = new CommitAdvancer(oldLeader, oldTracker).advance();

        assertTrue(oldAdvance.isEmpty());
        assertEquals(0L, oldLeader.commitIndex());

        // A new Leader in term 3 has the same index 1 but overwrites index 2 with its own term.
        RaftState newLeader = RaftState.restore(2L, null, List.of(oldLeader.entryAt(1L)), 0L);
        newLeader.startElection(LEADER); // term 3
        newLeader.becomeLeader(PEERS);
        newLeader.append(entry(2L, 3L, "new-leader-r-2"));
        ReplicationTracker newTracker = new ReplicationTracker(newLeader, PEERS);
        newLeader.advanceMatchIndex("node-2", 2L);
        newLeader.advanceMatchIndex("node-3", 2L);

        OptionalLong newAdvance = new CommitAdvancer(newLeader, newTracker).advance();

        assertTrue(newAdvance.isPresent());
        assertEquals(2L, newAdvance.getAsLong());
        assertEquals(2L, newLeader.commitIndex());
        assertNotEquals(oldLeader.entryAt(2L), newLeader.entryAt(2L));
        assertEquals(0L, oldLeader.commitIndex(),
                "the old Leader's commitIndex never passed the overwritten entry");
    }

    @Test
    void nonLeaderNeverAdvancesCommit() {
        RaftState state = leaderTermOne(3);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 3L);
        state.advanceMatchIndex("node-3", 3L);
        state.becomeFollower(2L);

        CommitAdvancer advancer = new CommitAdvancer(state, tracker);
        assertTrue(advancer.advance().isEmpty());
        assertEquals(0L, state.commitIndex());
        assertEquals(Role.FOLLOWER, state.role());

        state.startElection(LEADER);
        assertEquals(Role.CANDIDATE, state.role());
        assertTrue(advancer.advance().isEmpty());
        assertEquals(0L, state.commitIndex());
    }

    @Test
    void commitIndexIsIdempotentMonotonicAndBoundedByLastLogIndex() {
        RaftState state = leaderTermOne(3);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        state.advanceMatchIndex("node-2", 3L);
        state.advanceMatchIndex("node-3", 3L);
        CommitAdvancer advancer = new CommitAdvancer(state, tracker);

        assertEquals(OptionalLong.of(3L), advancer.advance());
        for (int repeat = 0; repeat < 5; repeat++) {
            assertTrue(advancer.advance().isEmpty());
            assertEquals(3L, state.commitIndex());
            assertTrue(state.commitIndex() <= state.lastLogIndex());
        }
    }

    @Test
    void failedOrMalformedPeerReplyNeverContributesToQuorum() {
        RaftState state = leaderTermOne(2);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        CommitAdvancer advancer = new CommitAdvancer(state, tracker);
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = service(state, hooks);

        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                state.currentTerm(), LEADER, 0L, 0L, List.of(state.entryAt(1L)), 0L);
        service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Failed("node-2", request,
                        RaftTransportAdapter.FailureKind.TIMEOUT));
        assertEquals(0L, state.matchIndexOf("node-2"));
        assertTrue(advancer.advance().isEmpty(), "timeout is not an ACK");
        assertTrue(hooks.advanced.isEmpty(), "the match hook must not run on a failure");

        service.handleAppendResult("node-3", request,
                new RaftTransportAdapter.Replied("node-3", request,
                        new RaftNode.AppendEntriesResponse(state.currentTerm(), false, 0L)));
        assertEquals(0L, state.matchIndexOf("node-3"));
        assertTrue(advancer.advance().isEmpty(), "a rejection is not an ACK");
        assertTrue(hooks.advanced.isEmpty(), "the match hook must not run on a rejection");

        service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Replied("node-2", request,
                        new RaftNode.AppendEntriesResponse(
                                state.currentTerm(), true, 1L)));
        assertEquals(1L, state.matchIndexOf("node-2"));
        assertTrue(advancer.advance().isEmpty(), "Leader + 1 is only 2/5");

        service.handleAppendResult("node-3", request,
                new RaftTransportAdapter.Replied("node-3", request,
                        new RaftNode.AppendEntriesResponse(
                                state.currentTerm(), true, 1L)));
        assertEquals(1L, state.matchIndexOf("node-3"));
        assertEquals(OptionalLong.of(1L), advancer.advance());
        assertEquals(1L, state.commitIndex());
    }

    @Test
    void quorumFollowsClusterSizeAndIgnoresOneSlowPeer() {
        RaftState threeNode = leaderWithPeers(Set.of("node-2", "node-3"), 2);
        ReplicationTracker threeTracker = new ReplicationTracker(threeNode, Set.of("node-2", "node-3"));
        threeNode.advanceMatchIndex("node-2", 2L);
        assertEquals(OptionalLong.of(2L), new CommitAdvancer(threeNode, threeTracker).advance(),
                "2/3 is a quorum");
        assertEquals(2L, threeNode.commitIndex());

        RaftState oneOfThree = leaderWithPeers(Set.of("node-2", "node-3"), 2);
        ReplicationTracker oneTracker = new ReplicationTracker(oneOfThree, Set.of("node-2", "node-3"));
        assertTrue(new CommitAdvancer(oneOfThree, oneTracker).advance().isEmpty(),
                "1/3 must not commit");

        RaftState fiveNode = leaderTermOne(5);
        ReplicationTracker fiveTracker = new ReplicationTracker(fiveNode, PEERS);
        fiveNode.advanceMatchIndex("node-2", 5L);
        fiveNode.advanceMatchIndex("node-3", 5L);
        fiveNode.advanceMatchIndex("node-4", 2L);
        fiveNode.advanceMatchIndex("node-5", 0L);
        assertEquals(OptionalLong.of(5L), new CommitAdvancer(fiveNode, fiveTracker).advance(),
                "a slow peer must not block Leader + 2 followers");
        assertEquals(5L, fiveNode.commitIndex());
    }

    // -------------------------------------------------------------------------------------------
    // Hook contract (D16 section 3.3)
    // -------------------------------------------------------------------------------------------

    @Test
    void matchHookRunsOnlyAfterAVerifiedAck() {
        RaftState state = leaderTermOne(2);
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = service(state, hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                state.currentTerm(), LEADER, 0L, 0L, List.of(state.entryAt(1L)), 0L);

        service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Replied("node-2", request,
                        new RaftNode.AppendEntriesResponse(state.currentTerm(), true, 1L)));
        assertEquals(List.of("node-2:1"), hooks.advanced);

        hooks.advanced.clear();
        service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Failed("node-2", request,
                        RaftTransportAdapter.FailureKind.TRANSPORT_ERROR));
        assertTrue(hooks.advanced.isEmpty());

        service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Replied("node-2", request,
                        new RaftNode.AppendEntriesResponse(state.currentTerm(), false, 0L)));
        assertTrue(hooks.advanced.isEmpty());
    }

    // -------------------------------------------------------------------------------------------
    // Integration through the D15 fake network
    // -------------------------------------------------------------------------------------------

    @Test
    void partitionedClusterCommitsOnlyAfterThreeOfFiveAcknowledgements() throws Exception {
        RaftState leader = leaderTermOne(3);
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers = emptyFollowers();
        CommitHooks hooks = new CommitHooks();
        FivePeerReplicationSafetyTest.Cluster cluster =
                new FivePeerReplicationSafetyTest.Cluster(leader, followers, hooks);
        CommitAdvancer advancer = new CommitAdvancer(cluster.leader, cluster.tracker);
        hooks.advancer = advancer;

        cluster.network.partition("node-3");
        cluster.network.partition("node-4");
        cluster.network.partition("node-5");
        cluster.service.replicateAll();
        cluster.runRounds(60);

        assertEquals(3L, leader.matchIndexOf("node-2"));
        assertEquals(0L, leader.commitIndex(), "only 2/5 nodes hold the entries");

        cluster.network.heal("node-3");
        cluster.runRounds(60);
        assertEquals(3L, leader.matchIndexOf("node-3"));
        assertEquals(3L, leader.commitIndex(), "3/5 must commit");

        cluster.network.heal("node-4");
        cluster.network.heal("node-5");
        cluster.settle(300);
        for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
            assertEquals(3L, leader.matchIndexOf(peer));
        }

        cluster.service.replicateAll();
        cluster.settle(200);

        assertEquals(3L, leader.commitIndex());
        assertTrue(leader.commitIndex() <= leader.lastLogIndex());
        for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
            RaftState follower = followers.get(peer).state;
            assertEquals(Math.min(leader.commitIndex(), follower.lastLogIndex()),
                    follower.commitIndex(), "follower commit of " + peer);
            assertTrue(follower.commitIndex() <= follower.lastLogIndex());
            assertEquals(3L, follower.commitIndex());
        }
        assertFalse(hooks.advanced.isEmpty(), "the ACK hook drove the commit advance");
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static RaftState leaderTermOne(int entries) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        state.becomeLeader(PEERS);
        for (long index = 1; index <= entries; index++) {
            state.append(entry(index, 1L, "r-" + index));
        }
        return state;
    }

    private static RaftState leaderWithPeers(Set<String> peers, int entries) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        state.becomeLeader(new TreeSet<>(peers));
        for (long index = 1; index <= entries; index++) {
            state.append(entry(index, 1L, "r-" + index));
        }
        return state;
    }

    /** Leader in term 2 whose whole log is a term-1 prefix. */
    private static RaftState oldTermLeader(int oldEntries) {
        List<RaftLogEntry> prefix = new ArrayList<>();
        for (long index = 1; index <= oldEntries; index++) {
            prefix.add(entry(index, 1L, "r-" + index));
        }
        RaftState state = RaftState.restore(1L, null, prefix, 0L);
        state.startElection(LEADER); // term 2
        state.becomeLeader(PEERS);
        return state;
    }

    private static RaftLogEntry entry(long index, long term, String requestId) {
        return new RaftLogEntry(index, term, BOOK, "{\"requestId\":\"" + requestId + "\"}");
    }

    private static RaftReplicationService service(RaftState state, RecordingHooks hooks) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "d16-deadline");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        RaftTransportAdapter transport = new RaftTransportAdapter(LEADER,
                (peerId, request) -> new java.util.concurrent.CompletableFuture<>(),
                Duration.ofSeconds(30L), Runnable::run, scheduler, failure -> {
                });
        return new RaftReplicationService(LEADER, state, transport, PEERS, Runnable::run,
                new FivePeerReplicationSafetyTest.ManualRetryScheduler(), 64,
                new RaftReplicationService.Backoff(10L, 100L), hooks);
    }

    private static Map<String, FivePeerReplicationSafetyTest.FollowerNode> emptyFollowers()
            throws Exception {
        Map<String, FivePeerReplicationSafetyTest.FollowerNode> followers =
                new java.util.LinkedHashMap<>();
        for (String peer : PEERS) {
            FakeRaftStorage storage = new FakeRaftStorage();
            storage.seedState(1L, null, List.of(), 0L);
            RaftStorage.PersistentState persisted = storage.load();
            RaftState state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                    persisted.log(), persisted.lastApplied());
            PersistentLogAppender appender = new PersistentLogAppender(storage);
            appender.recover();
            AppendEntriesHandler handler = new AppendEntriesHandler(
                    new PersistentElectionState(state, storage), appender,
                    new LogConflictResolver(appender), new AppendEntriesHandler.TimerHook() {
                        @Override
                        public void onValidAppendEntries(long requestTerm) {
                        }

                        @Override
                        public void start() {
                        }

                        @Override
                        public void close() {
                        }
                    });
            followers.put(peer, new FivePeerReplicationSafetyTest.FollowerNode(storage, state, handler));
        }
        return followers;
    }

    private static class RecordingHooks implements RaftReplicationService.Hooks {

        final List<String> advanced = new ArrayList<>();

        @Override
        public void onMatchIndexAdvanced(String peerId, long matchIndex) {
            advanced.add(peerId + ":" + matchIndex);
        }
    }

    private static final class CommitHooks extends RecordingHooks {

        CommitAdvancer advancer;

        @Override
        public void onMatchIndexAdvanced(String peerId, long matchIndex) {
            super.onMatchIndexAdvanced(peerId, matchIndex);
            if (advancer != null) {
                advancer.advance();
            }
        }
    }
}
