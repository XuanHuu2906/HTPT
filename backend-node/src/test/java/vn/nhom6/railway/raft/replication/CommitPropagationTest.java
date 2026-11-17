package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;
import vn.nhom6.railway.raft.election.LeaderHeartbeatScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D17 tests for {@code leaderCommit} propagation from a Leader to its four Followers.
 *
 * <p><strong>Honesty boundary.</strong> Every Node is an in-memory object, the peer transport is a
 * deterministic fake network, follower storage is {@link FakeRaftStorage}, and both the RPC
 * deadline scheduler and the retry scheduler are manual (no real time, no sleeps, no threads).
 * This test uses <strong>no</strong> TCP sockets, Docker, PostgreSQL durability, five real
 * processes or a Booking state machine. "Apply" here means only that the real
 * {@link vn.nhom6.railway.raft.core.RaftNode.CommitListener} registered on a real
 * {@link PersistentRaftNode} received the entry; it does not mean {@code CONFIRMED} or a Booking
 * row.
 *
 * <p><strong>Scope.</strong> Production code is not modified. The Leader side is the real
 * {@link RaftState}, {@link ReplicationTracker}, {@link RaftReplicationService},
 * {@link AppendRetryCoordinator} and {@link CommitAdvancer} connected through the
 * {@code onMatchIndexAdvanced} hook. Each follower is a real {@link PersistentRaftNode} with a real
 * {@link AppendEntriesHandler}, real {@link PersistentElectionState} and
 * {@link PersistentLogAppender} over {@link FakeRaftStorage}; {@code deliverCommitted()} is
 * therefore the production code path that feeds the listeners.
 *
 * <p><strong>One deliberate divergence from the task sheet.</strong> The sheet says
 * {@code commitIndex = min(leaderCommit, lastLogIndex)}. The implemented and SPEC-04 rule is
 * {@code max(commitIndex, min(leaderCommit, matchIndex of this verified RPC))}, which is stricter:
 * a follower may still hold an unmatched suffix, so a heartbeat cannot commit past the prefix the
 * Leader proved. The tests below assert the stricter rule.
 */
class CommitPropagationTest {

    private static final String LEADER = "node-1";
    private static final List<String> PEERS = List.of("node-2", "node-3", "node-4", "node-5");
    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final String BOOK = RaftLogEntry.COMMAND_TYPE_BOOK_TICKET;
    private static final ElectionConfig HEARTBEAT_CONFIG = new ElectionConfig(1_000L, 2_000L, 4_000L);

    // -------------------------------------------------------------------------------------------
    // 1. Leader commits on 3/5, then the next round teaches all four followers
    // -------------------------------------------------------------------------------------------

    @Test
    void leaderCommitReachesAllFourFollowersOnTheNextAppendEntriesRound() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 64);

        cluster.service.replicateAll();
        cluster.settle(200);

        assertEquals(5L, leader.commitIndex(), "Leader + node-2 + node-3 is a 3/5 quorum");
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(5L, follower.state.lastLogIndex(), "log of " + peer + " is already complete");
            assertEquals(0L, follower.state.commitIndex(),
                    "an AppendEntries sent before the commit must not make " + peer + " think it is committed");
            assertTrue(follower.listener.entries().isEmpty(), "no entry may be applied before leaderCommit");
        }

        int callsBefore = cluster.network.allCalls().size();
        cluster.service.replicateAll();
        cluster.settle(200);

        List<FakeNetwork.Call> propagation = cluster.network.allCalls()
                .subList(callsBefore, cluster.network.allCalls().size());
        assertEquals(4, propagation.size());
        for (FakeNetwork.Call call : propagation) {
            assertEquals(5L, call.request.getPayload().getLeaderCommit(),
                    "the service must put state.commitIndex() into leaderCommit");
            assertTrue(call.request.getPayload().getEntries().isEmpty(),
                    "a caught-up follower only needs a heartbeat");
        }
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(5L, follower.state.commitIndex(), "commitIndex of " + peer);
            assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
            assertListenerDeliveredExactly(follower, leader.entries());
        }
        assertTrue(cluster.loopFailures.isEmpty(), cluster.loopFailures.toString());
    }

    // -------------------------------------------------------------------------------------------
    // 2. A successful append is not a commit and is not a booking success
    // -------------------------------------------------------------------------------------------

    @Test
    void appendSuccessWithoutQuorumNeverAppliesAnEntry() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 64);

        cluster.service.replicate("node-2");
        cluster.runRounds(20);

        FollowerNode follower = cluster.follower("node-2");
        assertEquals(5L, leader.matchIndexOf("node-2"), "the follower did store and ACK all entries");
        assertEquals(5L, follower.storage.storedLog().size(), "the durable log was written");
        assertEquals(0L, leader.commitIndex(), "Leader + 1 follower is only 2/5");
        assertEquals(0L, follower.state.commitIndex());
        assertTrue(follower.listener.entries().isEmpty(),
                "append success must never be reported to a CommitListener as committed");
        for (FakeNetwork.Call call : cluster.network.allCalls()) {
            assertEquals(0L, call.request.getPayload().getLeaderCommit(),
                    "no AppendEntries may claim a commit the Leader has not computed");
            assertNotNull(call.response);
            assertTrue(call.response.success(), "the fake follower answered success=true");
        }
    }

    // -------------------------------------------------------------------------------------------
    // 3. Commit is only learned through the next AppendEntries/heartbeat
    // -------------------------------------------------------------------------------------------

    @Test
    void followerDoesNotLearnCommitUntilTheNextSend() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 64);

        cluster.service.replicateAll();
        cluster.settle(200);
        assertEquals(5L, leader.commitIndex());

        for (String peer : PEERS) {
            assertEquals(0L, cluster.follower(peer).state.commitIndex(),
                    peer + " cannot guess a commit that was not sent yet");
            assertTrue(cluster.follower(peer).listener.entries().isEmpty());
        }

        cluster.service.replicateAll();
        cluster.settle(200);

        for (String peer : PEERS) {
            assertEquals(5L, cluster.follower(peer).state.commitIndex(), "after the next send: " + peer);
        }
    }

    // -------------------------------------------------------------------------------------------
    // 4. A lagging follower is capped at its own matchIndex, then catches up in batches
    // -------------------------------------------------------------------------------------------

    @Test
    void laggingFollowerCommitsOnlyUpToItsMatchIndexThenCatchesUpWithoutDuplicates() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 2);

        // Give node-4 exactly entries 1..2, then isolate it so it cannot advance.
        cluster.service.replicate("node-4");
        assertTrue(cluster.network.deliverPeer("node-4"), "the first bounded batch is delivered");
        cluster.network.partition("node-4");
        cluster.drain();
        FollowerNode lagging = cluster.follower("node-4");
        assertEquals(List.of(1L, 2L), indices(lagging.state.entries()));
        assertEquals(2L, leader.matchIndexOf("node-4"));

        for (String peer : List.of("node-2", "node-3", "node-5")) {
            cluster.service.replicate(peer);
        }
        cluster.runRounds(120);

        assertEquals(5L, leader.commitIndex(), "the other 3/5 nodes committed the log");
        assertEquals(2L, leader.matchIndexOf("node-4"));
        assertEquals(3L, leader.nextIndexOf("node-4"));
        assertEquals(0L, lagging.state.commitIndex());
        assertTrue(lagging.listener.entries().isEmpty());

        // A real heartbeat carries leaderCommit=5 but only proves the prefix through index 2.
        List<Heartbeat> heartbeats = cluster.sendHeartbeats(Set.of("node-4"));
        assertEquals(1, heartbeats.size());
        AppendEntriesRequestDto heartbeat = heartbeats.getFirst().request();
        assertTrue(heartbeat.getEntries().isEmpty());
        assertEquals(2L, heartbeat.getPrevLogIndex());
        assertEquals(5L, heartbeat.getLeaderCommit());
        assertEquals(2L, lagging.state.commitIndex(), "min(leaderCommit, matchIndex) caps the follower");
        assertTrue(lagging.state.commitIndex() <= lagging.state.lastLogIndex());
        assertListenerDeliveredExactly(lagging, leader.entries().subList(0, 2));

        // Heal, drop the stale partitioned RPC, and let the bounded batches fill 3..5.
        FakeNetwork.Call stale = cluster.network.pendingCall("node-4");
        assertNotNull(stale, "the partitioned request must still be queued");
        cluster.network.failTransport(stale, new IOException("fake partition ended"));
        cluster.drain();
        cluster.network.heal("node-4");

        cluster.service.replicateAll();
        cluster.settle(300);

        assertEquals(5L, lagging.state.commitIndex());
        assertTrue(lagging.state.commitIndex() <= lagging.state.lastLogIndex());
        assertListenerDeliveredExactly(lagging, leader.entries());
        assertEquals(5L, leader.commitIndex());
    }

    // -------------------------------------------------------------------------------------------
    // 5. A diverging uncommitted tail is never committed; only convergence is
    // -------------------------------------------------------------------------------------------

    @Test
    void divergingUncommittedTailIsNeverCommittedAndNeverDelivered() throws Exception {
        RaftState leader = leaderTermTwo(2, 3);
        Map<String, FollowerNode> followers = emptyFollowers(2L);
        RaftLogEntry local3 = entry(3L, 1L, "node-2-old-3");
        RaftLogEntry local4 = entry(4L, 1L, "node-2-old-4");
        followers.put("node-2", follower("node-2", 2L, List.of(
                leader.entryAt(1L), leader.entryAt(2L), local3, local4), 0L));

        Cluster cluster = new Cluster(leader, followers, 64);
        FollowerNode node2 = cluster.follower("node-2");

        // A heartbeat whose leaderCommit is far ahead must be rejected by the prevLog check.
        RaftNode.AppendEntriesResponse rejected = node2.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(2L, LEADER, 5L, 2L, List.of(), 5L));
        assertFalse(rejected.success());
        assertEquals(0L, node2.state.commitIndex());
        assertTrue(node2.listener.entries().isEmpty());
        assertTrue(node2.state.entries().contains(local3), "the local diverging tail is untouched");
        assertTrue(node2.state.entries().contains(local4));

        cluster.service.replicateAll();
        cluster.settle(200);

        assertEquals(5L, leader.commitIndex());
        assertEquals(leader.entries(), node2.state.entries(), "the conflicting suffix was replaced");
        assertEquals(leader.entries(), node2.storage.storedLog());
        for (String peer : PEERS) {
            assertEquals(0L, cluster.follower(peer).state.commitIndex(),
                    "the commit has not been sent to " + peer + " yet");
        }

        cluster.service.replicateAll();
        cluster.settle(200);

        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(5L, follower.state.commitIndex(), "commitIndex of " + peer);
            assertListenerDeliveredExactly(follower, leader.entries());
            assertFalse(follower.listener.entries().contains(local3),
                    "an overwritten entry must never reach the listener");
            assertFalse(follower.listener.entries().contains(local4));
        }
    }

    // -------------------------------------------------------------------------------------------
    // 6. Stale, duplicate and reordered leaderCommit never lowers or re-delivers
    // -------------------------------------------------------------------------------------------

    @Test
    void staleDuplicateAndReorderedLeaderCommitNeverLowerCommitOrRedeliver() throws Exception {
        RaftState leader = leaderTermOne(5);
        List<RaftLogEntry> entries = leader.entries();
        FollowerNode follower = follower("node-2", 1L, entries, 0L);

        RaftNode.AppendEntriesResponse first = follower.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER, 5L, 1L, List.of(), 5L));
        assertTrue(first.success());
        assertEquals(5L, follower.state.commitIndex());
        assertListenerDeliveredExactly(follower, entries);

        RaftNode.AppendEntriesResponse older = follower.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER, 5L, 1L, List.of(), 3L));
        assertTrue(older.success());
        assertEquals(5L, follower.state.commitIndex(), "a stale leaderCommit must not lower commitIndex");
        assertListenerDeliveredExactly(follower, entries);

        RaftNode.AppendEntriesResponse duplicate = follower.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER, 5L, 1L, List.of(), 5L));
        assertTrue(duplicate.success());
        assertEquals(5L, follower.state.commitIndex());
        assertListenerDeliveredExactly(follower, entries);

        RaftNode.AppendEntriesResponse zero = follower.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(1L, LEADER, 5L, 1L, List.of(), 0L));
        assertTrue(zero.success());
        assertEquals(5L, follower.state.commitIndex());
        assertListenerDeliveredExactly(follower, entries);
    }

    // -------------------------------------------------------------------------------------------
    // 7. A real heartbeat scheduler with an empty request carries the commit
    // -------------------------------------------------------------------------------------------

    @Test
    void realHeartbeatSchedulerCarriesCommitWithoutNewEntries() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 64);

        cluster.service.replicateAll();
        cluster.settle(200);
        assertEquals(5L, leader.commitIndex());
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(5L, follower.state.lastLogIndex());
            assertEquals(0L, follower.state.commitIndex());
        }

        List<Heartbeat> heartbeats = cluster.sendHeartbeats(Set.of());
        assertEquals(4, heartbeats.size(), "one heartbeat per peer, no more");
        for (Heartbeat heartbeat : heartbeats) {
            AppendEntriesRequestDto request = heartbeat.request();
            assertTrue(request.getEntries().isEmpty(), "no entry is re-sent by a heartbeat");
            assertEquals(5L, request.getPrevLogIndex());
            assertEquals(5L, request.getLeaderCommit());
            assertEquals(1L, request.getTerm());
        }
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(5L, follower.state.commitIndex(), "heartbeat taught " + peer + " the commit");
            assertEquals(5L, follower.state.lastLogIndex(), "no entry was appended");
            assertListenerDeliveredExactly(follower, leader.entries());
        }
    }

    // -------------------------------------------------------------------------------------------
    // 8. Two partitioned followers keep their old commit; all five converge after heal
    // -------------------------------------------------------------------------------------------

    @Test
    void partitionedFollowersKeepTheirCommitAndAllFiveConvergeAfterHeal() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 64);

        cluster.network.partition("node-4");
        cluster.network.partition("node-5");
        cluster.service.replicateAll();
        cluster.runRounds(30);
        assertEquals(5L, leader.commitIndex());

        cluster.service.replicateAll();
        cluster.runRounds(10);

        assertEquals(5L, cluster.follower("node-2").state.commitIndex());
        assertEquals(5L, cluster.follower("node-3").state.commitIndex());
        for (String peer : List.of("node-4", "node-5")) {
            FollowerNode partitioned = cluster.follower(peer);
            assertEquals(0L, partitioned.state.lastLogIndex(), peer + " received no entry");
            assertEquals(0L, partitioned.state.commitIndex());
            assertTrue(partitioned.listener.entries().isEmpty());
        }

        cluster.network.heal("node-4");
        cluster.network.heal("node-5");
        cluster.settle(300);
        for (String peer : List.of("node-4", "node-5")) {
            assertEquals(5L, cluster.follower(peer).state.lastLogIndex(), "log of " + peer);
            assertEquals(0L, cluster.follower(peer).state.commitIndex(),
                    "the queued AppendEntries was built with leaderCommit=0");
        }

        cluster.service.replicateAll();
        cluster.settle(200);

        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(leader.commitIndex(), follower.state.commitIndex(), "commitIndex of " + peer);
            assertEquals(leader.entries(), follower.state.entries());
            assertListenerDeliveredExactly(follower, leader.entries());
        }
        assertEquals(5L, leader.commitIndex());
    }

    // -------------------------------------------------------------------------------------------
    // 9. Five-node convergence with monotonic commit and identical delivery
    // -------------------------------------------------------------------------------------------

    @Test
    void allFiveNodesConvergeWithMonotonicCommitAndIdenticalDeliveredEntries() throws Exception {
        RaftState leader = leaderTermOne(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), 2);

        Map<String, Long> previous = commits(cluster);
        cluster.service.replicateAll();
        List<Long> leaderCommitHistory = new ArrayList<>();
        for (int round = 0; round < 120; round++) {
            cluster.runRound();
            Map<String, Long> current = commits(cluster);
            for (Map.Entry<String, Long> entry : current.entrySet()) {
                assertTrue(entry.getValue() >= previous.get(entry.getKey()),
                        "commitIndex of " + entry.getKey() + " decreased: "
                                + previous.get(entry.getKey()) + " -> " + entry.getValue());
            }
            previous = current;
            leaderCommitHistory.add(leader.commitIndex());
        }

        cluster.service.replicateAll();
        cluster.settle(300);

        assertEquals(5L, leader.commitIndex());
        assertTrue(leader.commitIndex() <= leader.lastLogIndex());
        assertFalse(leaderCommitHistory.isEmpty());
        for (int position = 1; position < leaderCommitHistory.size(); position++) {
            assertTrue(leaderCommitHistory.get(position) >= leaderCommitHistory.get(position - 1));
        }

        List<RaftLogEntry> expected = leader.entries();
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(leader.commitIndex(), follower.state.commitIndex(), "commitIndex of " + peer);
            assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
            assertEquals(expected, follower.listener.entries(), "delivered entries of " + peer);
            assertListenerDeliveredExactly(follower, expected);
        }
        for (String peer : PEERS) {
            assertEquals(cluster.follower("node-2").listener.entries(),
                    cluster.follower(peer).listener.entries(),
                    "every follower must receive the same (index, term, payload) list");
        }
    }

    // -------------------------------------------------------------------------------------------
    // 10. An uncommitted old-term entry is not carried by leaderCommit until a current-term quorum
    // -------------------------------------------------------------------------------------------

    @Test
    void oldTermEntryIsNotCommittedDirectlyAndOnlyTravelsBehindACurrentTermCommit() throws Exception {
        RaftState leader = leaderTermTwo(2, 0);
        Map<String, FollowerNode> followers = emptyFollowers(2L);
        for (String peer : PEERS) {
            followers.put(peer, follower(peer, 2L, leader.entries(), 0L));
        }
        Cluster cluster = new Cluster(leader, followers, 64);

        cluster.service.replicateAll();
        cluster.settle(200);

        assertEquals(2L, leader.lastLogIndex());
        assertEquals(1L, leader.termAt(2L));
        assertEquals(2L, leader.currentTerm());
        assertEquals(0L, leader.commitIndex(), "a term-1 entry on 3/5 is not committed directly");
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(0L, follower.state.commitIndex());
            assertTrue(follower.listener.entries().isEmpty(),
                    "the old-term entry must not be delivered to " + peer + " yet");
        }

        leader.append(entry(3L, 2L, "r-3"));
        cluster.service.replicateAll();
        cluster.settle(200);

        assertEquals(3L, leader.commitIndex(), "the current-term entry commits the old prefix indirectly");
        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(0L, follower.state.commitIndex(),
                    "the follower still has not received leaderCommit=3");
        }

        cluster.service.replicateAll();
        cluster.settle(200);

        for (String peer : PEERS) {
            FollowerNode follower = cluster.follower(peer);
            assertEquals(3L, follower.state.commitIndex());
            assertListenerDeliveredExactly(follower, leader.entries());
        }
    }

    // -------------------------------------------------------------------------------------------
    // 11. A stale-term (fake) Leader cannot raise a follower's commit
    // -------------------------------------------------------------------------------------------

    @Test
    void staleTermFakeLeaderIsRejectedAndCannotRaiseFollowerCommit() throws Exception {
        RaftState leader = leaderTermOne(5);
        FollowerNode follower = follower("node-2", 5L, leader.entries(), 0L);

        RaftNode.AppendEntriesResponse response = follower.node.handleAppendEntries(
                new RaftNode.AppendEntriesRequest(4L, "node-2", 5L, 1L, List.of(), 5L));

        assertFalse(response.success(), "a request from term 4 is stale for a term-5 follower");
        assertEquals(5L, response.term(), "the follower answers with its own term");
        assertEquals(5L, follower.state.currentTerm());
        assertEquals(0L, follower.state.commitIndex(), "a fake Leader cannot advance commitIndex");
        assertTrue(follower.listener.entries().isEmpty());
        assertEquals(leader.entries(), follower.state.entries(), "the local log is unchanged");
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static RaftState leaderTermOne(int count) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        state.becomeLeader(new TreeSet<>(PEERS));
        for (long index = 1L; index <= count; index++) {
            state.append(entry(index, 1L, "r-" + index));
        }
        return state;
    }

    /**
     * Leader in term 2 whose first {@code oldTermEntries} entries belong to term 1, followed by
     * {@code currentTermEntries} entries of term 2. The old prefix must not be committed directly.
     */
    private static RaftState leaderTermTwo(int oldTermEntries, int currentTermEntries) {
        List<RaftLogEntry> prefix = new ArrayList<>();
        for (long index = 1L; index <= oldTermEntries; index++) {
            prefix.add(entry(index, 1L, "r-" + index));
        }
        RaftState state = RaftState.restore(1L, null, prefix, 0L);
        state.startElection(LEADER); // term 2
        state.becomeLeader(new TreeSet<>(PEERS));
        long index = state.lastLogIndex();
        for (int position = 0; position < currentTermEntries; position++) {
            index++;
            state.append(entry(index, 2L, "r-" + index));
        }
        return state;
    }

    private static RaftLogEntry entry(long index, long term, String requestId) {
        return new RaftLogEntry(index, term, BOOK, "{\"requestId\":\"" + requestId + "\"}");
    }

    private static List<Long> indices(List<RaftLogEntry> entries) {
        List<Long> indices = new ArrayList<>(entries.size());
        for (RaftLogEntry entry : entries) {
            indices.add(entry.index());
        }
        return indices;
    }

    private static Map<String, FollowerNode> emptyFollowers() throws Exception {
        return emptyFollowers(1L);
    }

    private static Map<String, FollowerNode> emptyFollowers(long term) throws Exception {
        Map<String, FollowerNode> followers = new LinkedHashMap<>();
        for (String peer : PEERS) {
            followers.put(peer, follower(peer, term, List.of(), 0L));
        }
        return followers;
    }

    /**
     * Builds one follower as a real {@link PersistentRaftNode}: the same production code path that
     * calls {@code deliverCommitted()} and feeds {@link RaftNode.CommitListener}s.
     */
    private static FollowerNode follower(
            String id, long term, List<RaftLogEntry> entries, long lastApplied) throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, List.copyOf(entries), lastApplied);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());

        ManualEventLoop electionClock = new ManualEventLoop();
        PersistentRaftNode node = new PersistentRaftNode(id, state, storage);
        ElectionTimer timer = new ElectionTimer(
                new FollowerElectionService(node, state, id), electionClock, HEARTBEAT_CONFIG,
                failure -> {
                    throw new IllegalStateException("election timer failed in a D17 test", failure);
                });
        node.bindLifecycle(timer, () -> { });
        RecordingListener listener = new RecordingListener();
        node.addCommitListener(listener);
        return new FollowerNode(id, storage, state, node, timer, listener);
    }

    private static Map<String, Long> commits(Cluster cluster) {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        snapshot.put(LEADER, cluster.leader.commitIndex());
        for (String peer : PEERS) {
            snapshot.put(peer, cluster.follower(peer).state.commitIndex());
        }
        return snapshot;
    }

    private static void assertListenerDeliveredExactly(FollowerNode follower, List<RaftLogEntry> expected) {
        assertEquals(expected, follower.listener.entries(),
                "CommitListener delivery of " + follower.id);
        Set<Long> seen = new HashSet<>();
        for (RaftLogEntry entry : follower.listener.entries()) {
            assertTrue(seen.add(entry.index()),
                    "entry " + entry.index() + " was delivered more than once to " + follower.id);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Cluster fixture: real Leader services over a deterministic fake network
    // -------------------------------------------------------------------------------------------

    static final class Cluster {

        final RaftState leader;
        final ReplicationTracker tracker;
        final Map<String, FollowerNode> followers;
        final Set<String> peers;
        final FakeNetwork network;
        final RaftReplicationService service;
        final CommitAdvancer advancer;
        final CommitHooks hooks;
        final AppendRetryCoordinator coordinator;
        final ManualRetryScheduler retries = new ManualRetryScheduler();
        final ManualEventLoop adapterDeadlines = new ManualEventLoop();
        final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
        final List<Throwable> loopFailures = new ArrayList<>();

        Cluster(RaftState leader, Map<String, FollowerNode> followers, int batchMax) {
            this.leader = Objects.requireNonNull(leader);
            this.followers = new LinkedHashMap<>(followers);
            this.peers = new TreeSet<>(leader.matchIndex().keySet());
            this.tracker = new ReplicationTracker(leader, peers);
            this.advancer = new CommitAdvancer(leader, tracker);
            this.network = new FakeNetwork(this.followers);
            this.hooks = new CommitHooks(advancer);

            RaftTransportAdapter transport = new RaftTransportAdapter(
                    LEADER, network, Duration.ofSeconds(30L), loop::add, adapterDeadlines,
                    loopFailures::add);
            RaftReplicationService.Backoff backoff = new RaftReplicationService.Backoff(10L, 100L);
            this.service = new RaftReplicationService(LEADER, leader, transport, peers,
                    loop::add, retries, batchMax, backoff, hooks);
            this.coordinator = new AppendRetryCoordinator(leader, tracker, service, loop::add,
                    retries, backoff, 10, new AppendRetryCoordinator.Hooks() {
                        @Override
                        public void onHigherTerm(long term) {
                        }

                        @Override
                        public void onGiveUp(String peerId, String reason) {
                        }
                    });
        }

        FollowerNode follower(String peerId) {
            return followers.get(peerId);
        }

        void drain() {
            Runnable task;
            while ((task = loop.poll()) != null) {
                task.run();
            }
        }

        void runRound() {
            network.deliverAll();
            drain();
            retries.fireAll();
            drain();
        }

        void runRounds(int rounds) {
            for (int round = 0; round < rounds; round++) {
                runRound();
            }
        }

        void settle(int maxRounds) {
            for (int round = 0; round < maxRounds; round++) {
                boolean progress = network.deliverAll();
                drain();
                progress |= retries.fireAll();
                drain();
                if (!progress && network.pendingCount() == 0 && retries.pending() == 0) {
                    return;
                }
            }
            throw new AssertionError("cluster did not settle: pending=" + network.pendingCount()
                    + ", retries=" + retries.pending());
        }

        /**
         * Sends one real {@link LeaderHeartbeatScheduler} tick. {@code targets} limits which peers
         * actually receive the heartbeat; the others are observed but not delivered, so a test can
         * isolate a lagging follower without touching the fake network partition state.
         */
        List<Heartbeat> sendHeartbeats(Set<String> targets) {
            boolean everyone = targets.isEmpty();
            List<Heartbeat> sent = new ArrayList<>();
            ManualEventLoop heartbeatClock = new ManualEventLoop();
            LeaderHeartbeatScheduler scheduler = new LeaderHeartbeatScheduler(
                    LEADER, leader, heartbeatClock, HEARTBEAT_CONFIG,
                    (peer, request, reply) -> {
                        if (!everyone && !targets.contains(peer)) {
                            return;
                        }
                        sent.add(new Heartbeat(peer, request));
                        try {
                            reply.accept(follower(peer).node.handleAppendEntries(request, LEADER));
                        } catch (RaftStorage.RaftStorageException failure) {
                            throw new IllegalStateException(
                                    "follower " + peer + " failed in a D17 heartbeat test", failure);
                        }
                    },
                    term -> leader.becomeFollower(term),
                    failure -> {
                        throw new IllegalStateException("heartbeat scheduler failed in a D17 test", failure);
                    });
            scheduler.start();
            scheduler.close();
            return sent;
        }
    }

    record Heartbeat(String peerId, AppendEntriesRequestDto request) {
    }

    static final class FollowerNode implements AutoCloseable {

        final String id;
        final FakeRaftStorage storage;
        final RaftState state;
        final PersistentRaftNode node;
        final ElectionTimer timer;
        final RecordingListener listener;

        FollowerNode(String id, FakeRaftStorage storage, RaftState state, PersistentRaftNode node,
                ElectionTimer timer, RecordingListener listener) {
            this.id = id;
            this.storage = storage;
            this.state = state;
            this.node = node;
            this.timer = timer;
            this.listener = listener;
        }

        @Override
        public void close() {
            timer.close();
        }
    }

    static final class RecordingListener implements RaftNode.CommitListener {

        private final List<RaftLogEntry> entries = new ArrayList<>();

        @Override
        public void onCommitted(RaftLogEntry entry) {
            entries.add(entry);
        }

        List<RaftLogEntry> entries() {
            return List.copyOf(entries);
        }
    }

    static final class CommitHooks implements RaftReplicationService.Hooks {

        private final CommitAdvancer advancer;
        final List<String> advanced = new ArrayList<>();
        final List<Long> commits = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
        int rejections;
        int higherTermCalls;

        CommitHooks(CommitAdvancer advancer) {
            this.advancer = Objects.requireNonNull(advancer);
        }

        @Override
        public void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
            rejections++;
        }

        @Override
        public void onHigherTerm(long term) {
            higherTermCalls++;
        }

        @Override
        public void onFailure(String peerId, Throwable failure) {
            failures.add(failure);
        }

        @Override
        public void onMatchIndexAdvanced(String peerId, long matchIndex) {
            advanced.add(peerId + ":" + matchIndex);
            advancer.advance().ifPresent(commits::add);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Follower election boundary: only the service/timer seam, no campaign is expected
    // -------------------------------------------------------------------------------------------

    static final class FollowerElectionService implements ElectionService {

        private final PersistentRaftNode node;
        private final RaftState state;
        private final String localId;

        FollowerElectionService(PersistentRaftNode node, RaftState state, String localId) {
            this.node = Objects.requireNonNull(node);
            this.state = Objects.requireNonNull(state);
            this.localId = Objects.requireNonNull(localId);
        }

        @Override
        public String localId() {
            return localId;
        }

        @Override
        public RaftNode node() {
            return node;
        }

        @Override
        public RaftState state() {
            return state;
        }

        @Override
        public List<String> voters() {
            return VOTERS;
        }

        @Override
        public boolean isVoter(String nodeId) {
            return VOTERS.contains(nodeId);
        }

        @Override
        public int requiredVotes() {
            return 3;
        }

        @Override
        public long startElection() {
            throw new UnsupportedOperationException("D17 does not campaign; the manual clock never fires");
        }

        @Override
        public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
            return Optional.empty();
        }

        @Override
        public RaftNode.RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) {
            throw new UnsupportedOperationException("D17 does not handle REQUEST_VOTE");
        }

        @Override
        public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
            throw new UnsupportedOperationException("D17 does not count votes");
        }

        @Override
        public void promoteToLeader(Set<String> peers) {
            throw new UnsupportedOperationException("D17 never promotes a follower");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Deterministic fake network: real RaftTransportAdapter on top, real follower nodes below
    // -------------------------------------------------------------------------------------------

    static final class FakeNetwork implements RaftTransportAdapter.RaftRpcClient {

        static final class Call {

            final String peerId;
            final MessageEnvelope<AppendEntriesRequestDto> request;
            final CompletableFuture<MessageEnvelope<JsonNode>> future = new CompletableFuture<>();
            RaftNode.AppendEntriesResponse response;
            boolean released;

            Call(String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
                this.peerId = peerId;
                this.request = request;
            }
        }

        private final Map<String, FollowerNode> followers;
        private final Map<String, ArrayDeque<Call>> pending = new LinkedHashMap<>();
        private final Set<String> partitioned = new HashSet<>();
        private final List<Call> calls = new ArrayList<>();
        private final Map<String, Integer> activePerPeer = new LinkedHashMap<>();
        private final List<Throwable> deliveryFailures = new ArrayList<>();

        FakeNetwork(Map<String, FollowerNode> followers) {
            this.followers = Map.copyOf(followers);
            for (String peer : followers.keySet()) {
                pending.put(peer, new ArrayDeque<>());
            }
        }

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
            Call call = new Call(peerId, request);
            pending.computeIfAbsent(peerId, key -> new ArrayDeque<>()).add(call);
            calls.add(call);
            activePerPeer.merge(peerId, 1, Integer::sum);
            return call.future;
        }

        void partition(String peerId) {
            partitioned.add(peerId);
        }

        void heal(String peerId) {
            partitioned.remove(peerId);
        }

        boolean deliverAll() {
            boolean delivered = false;
            for (String peerId : List.copyOf(pending.keySet())) {
                if (partitioned.contains(peerId)) {
                    continue;
                }
                delivered |= deliverPeerOnce(peerId);
            }
            return delivered;
        }

        boolean deliverPeer(String peerId) {
            return deliverPeerOnce(peerId);
        }

        private boolean deliverPeerOnce(String peerId) {
            ArrayDeque<Call> queue = pending.get(peerId);
            if (queue == null || queue.isEmpty()) {
                return false;
            }
            deliver(queue.poll());
            return true;
        }

        private void deliver(Call call) {
            try {
                RaftNode.AppendEntriesResponse response = followers.get(call.peerId).node
                        .handleAppendEntries(call.request.getPayload(), LEADER);
                call.response = response;
                call.future.complete(responseEnvelope(
                        call, response.term(), response.success(), response.matchIndex()));
            } catch (Exception failure) {
                deliveryFailures.add(failure);
                call.future.completeExceptionally(failure);
            } finally {
                release(call);
            }
        }

        Call pendingCall(String peerId) {
            ArrayDeque<Call> queue = pending.get(peerId);
            return queue == null || queue.isEmpty() ? null : queue.peek();
        }

        void failTransport(Call call, Throwable failure) {
            removeQueued(call);
            call.future.completeExceptionally(failure);
            release(call);
        }

        private void removeQueued(Call call) {
            ArrayDeque<Call> queue = pending.get(call.peerId);
            if (queue != null) {
                queue.remove(call);
            }
        }

        private void release(Call call) {
            if (call.released) {
                return;
            }
            call.released = true;
            activePerPeer.computeIfPresent(call.peerId, (peer, active) -> active - 1);
        }

        int pendingCount() {
            int total = 0;
            for (ArrayDeque<Call> queue : pending.values()) {
                total += queue.size();
            }
            return total;
        }

        List<Call> allCalls() {
            return List.copyOf(calls);
        }

        List<Throwable> deliveryFailures() {
            return List.copyOf(deliveryFailures);
        }

        private static MessageEnvelope<JsonNode> responseEnvelope(
                Call call, long term, boolean success, long matchIndex) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("term", term);
            payload.put("success", success);
            payload.put("matchIndex", matchIndex);
            return envelope(call.request.getMessageId(),
                    MessageType.APPEND_ENTRIES_RESPONSE, call.peerId, payload);
        }
    }

    private static MessageEnvelope<JsonNode> envelope(
            String correlationId, MessageType type, String senderId, Object payload) {
        try {
            return MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(correlationId, type, senderId, payload)));
        } catch (IOException failure) {
            throw new UncheckedIOException("cannot encode fake response", failure);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Manual schedulers: no wall clock, no threads, no sleeping
    // -------------------------------------------------------------------------------------------

    static final class ManualRetryScheduler implements RaftReplicationService.Scheduler {

        static final class Task implements Cancellable {

            final Runnable runnable;
            boolean cancelled;

            Task(Runnable runnable) {
                this.runnable = runnable;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }

        private final List<Task> tasks = new ArrayList<>();

        @Override
        public Cancellable schedule(Runnable task, long delayMillis) {
            Task scheduled = new Task(task);
            tasks.add(scheduled);
            return scheduled;
        }

        boolean fireAll() {
            List<Task> due = new ArrayList<>(tasks);
            tasks.clear();
            boolean fired = false;
            for (Task task : due) {
                if (!task.cancelled) {
                    fired = true;
                    task.runnable.run();
                }
            }
            return fired;
        }

        int pending() {
            int count = 0;
            for (Task task : tasks) {
                if (!task.cancelled) {
                    count++;
                }
            }
            return count;
        }
    }

    /** Virtual-time scheduler: a task is recorded but only runs if a test explicitly does so. */
    static final class ManualEventLoop extends AbstractExecutorService
            implements ScheduledExecutorService {

        private final List<FakeScheduledTask> scheduled = new ArrayList<>();
        private boolean shutdown;

        int scheduledCount() {
            return scheduled.size();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (shutdown) {
                throw new RejectedExecutionException("manual event loop is shut down");
            }
            FakeScheduledTask task = new FakeScheduledTask(command);
            scheduled.add(task);
            return task;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("D17 never runs inline loop tasks");
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("D17 schedules Runnable deadlines only");
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are one-shot");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are one-shot");
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }

    static final class FakeScheduledTask extends FutureTask<Void> implements ScheduledFuture<Void> {

        FakeScheduledTask(Runnable command) {
            super(Objects.requireNonNull(command), null);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
