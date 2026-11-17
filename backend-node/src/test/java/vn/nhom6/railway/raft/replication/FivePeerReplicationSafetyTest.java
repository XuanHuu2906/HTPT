package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D15 5-peer replication safety tests over a deterministic {@link FakeNetwork}.
 *
 * <p><strong>Honesty boundary.</strong> Every Node is an in-memory object, the peer transport is
 * a fake queue, follower storage is {@link FakeRaftStorage}, and both the RPC deadline scheduler
 * and the retry scheduler are manual. This test does <strong>not</strong> use TCP sockets, real
 * threads, a real clock, Docker, five real Nodes, PostgreSQL durability or state-machine apply.
 * It does not verify a real cluster deployment; G4 remains TV6's responsibility.
 *
 * <p>The Leader side uses the real {@link RaftTransportAdapter}, {@link RaftReplicationService},
 * {@link ReplicationTracker} and {@link AppendRetryCoordinator}; every follower uses the real
 * {@link AppendEntriesHandler} with its own {@link FakeRaftStorage}. No production class is
 * modified by this task.
 */
class FivePeerReplicationSafetyTest {

    static final String LEADER = "node-1";
    static final List<String> PEERS = List.of("node-2", "node-3", "node-4", "node-5");
    private static final String BOOK = RaftLogEntry.COMMAND_TYPE_BOOK_TICKET;

    // -------------------------------------------------------------------------------------------
    // Scenario 1: five entries replicated to all four followers
    // -------------------------------------------------------------------------------------------

    @Test
    void replicatesFiveEntriesToAllFivePeersAndLogsConverge() throws Exception {
        RaftState leader = leaderTermOneWithEntries(5);
        Cluster cluster = new Cluster(leader, emptyFollowers(), new RecordingHooks(leader, false));

        cluster.service.replicateAll();
        cluster.settle(200);

        List<RaftLogEntry> expected = leader.entries();
        assertEquals(5, expected.size());
        for (String peer : PEERS) {
            FollowerNode follower = cluster.followers.get(peer);
            assertEquals(expected, follower.state.entries(), "state log of " + peer);
            assertEquals(expected, follower.storage.storedLog(), "durable log of " + peer);
            assertEquals(5L, leader.matchIndexOf(peer), "matchIndex of " + peer);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 2: Log Matching Property
    // -------------------------------------------------------------------------------------------

    @Test
    void logMatchingPropertyHoldsForEveryPairOfNodes() throws Exception {
        RaftState leader = leaderTermTwoWith(List.of(entry(1, 1, "r-1"), entry(2, 1, "r-2")), 2);

        Map<String, FollowerNode> followers = new LinkedHashMap<>();
        followers.put("node-2", follower(1, 0,
                entry(1, 1, "r-1"), entry(2, 1, "r-2"),
                entry(3, 1, "n2-local-3"), entry(4, 1, "n2-local-4")));
        followers.put("node-3", follower(1, 0, entry(1, 1, "r-1"), entry(2, 1, "r-2")));
        followers.put("node-4", follower(1, 0));
        followers.put("node-5", follower(2, 0,
                leader.entryAt(1), leader.entryAt(2), leader.entryAt(3)));

        List<RaftState> allNodes = new ArrayList<>();
        allNodes.add(leader);
        for (FollowerNode node : followers.values()) {
            allNodes.add(node.state);
        }

        assertLogMatching(allNodes);

        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));
        cluster.service.replicateAll();
        cluster.settle(300);

        assertLogMatching(allNodes);
        for (FollowerNode node : followers.values()) {
            assertEquals(leader.entries(), node.state.entries());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 3a: delayed follower catches up with a bounded batch
    // -------------------------------------------------------------------------------------------

    @Test
    void delayedFollowerCatchesUpInBoundedBatches() throws Exception {
        RaftState leader = leaderTermOneWithEntries(5);
        Map<String, FollowerNode> followers = new LinkedHashMap<>();
        followers.put("node-2", follower(1, 0));

        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false), 2, 10);
        cluster.service.replicate("node-2");
        cluster.settle(300);

        FollowerNode follower = followers.get("node-2");
        assertEquals(leader.entries(), follower.state.entries());
        assertEquals(leader.entries(), follower.storage.storedLog());
        assertEquals(5L, leader.matchIndexOf("node-2"));

        int maxBatch = 0;
        int nonEmptyBatches = 0;
        for (FakeNetwork.Call call : cluster.network.allCalls()) {
            int size = call.request.getPayload().getEntries().size();
            maxBatch = Math.max(maxBatch, size);
            if (size > 0) {
                nonEmptyBatches++;
            }
        }
        assertTrue(maxBatch <= 2, "batchMax=2 must be respected, saw " + maxBatch);
        assertTrue(nonEmptyBatches >= 3, "five entries with batchMax=2 need several batches");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 3b: uncommitted suffix converges without deleting a matching prefix
    // -------------------------------------------------------------------------------------------

    @Test
    void uncommittedConflictingSuffixConvergesWithoutDeletingMatchingPrefix() throws Exception {
        RaftState leader = leaderTermTwoWith(List.of(entry(1, 1, "r-1"), entry(2, 1, "r-2")), 2);
        RaftLogEntry matching1 = leader.entryAt(1);
        RaftLogEntry matching2 = leader.entryAt(2);

        Map<String, FollowerNode> followers = emptyFollowers();
        followers.put("node-2", follower(1, 0,
                matching1, matching2, entry(3, 1, "n2-old-3"), entry(4, 1, "n2-old-4")));

        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));
        cluster.service.replicateAll();
        cluster.settle(300);

        FollowerNode node2 = followers.get("node-2");
        assertEquals(leader.entries(), node2.state.entries());
        assertEquals(leader.entries(), node2.storage.storedLog());
        assertEquals(matching1, node2.state.entryAt(1));
        assertEquals(matching2, node2.state.entryAt(2));
        assertEquals(4L, leader.matchIndexOf("node-2"));
        assertEquals(0L, leader.commitIndex());
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 3c: committed conflicting entry fails closed
    // -------------------------------------------------------------------------------------------

    @Test
    void committedConflictFailsClosedAndKeepsLog() throws Exception {
        RaftState leader = leaderTermTwoWith(List.of(), 1);
        RaftLogEntry committedLocal = entry(1, 1, "n2-committed");

        Map<String, FollowerNode> followers = emptyFollowers();
        followers.put("node-2", follower(1, 1, committedLocal));

        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));
        cluster.service.replicate("node-2");
        cluster.runRounds(20);

        FollowerNode node2 = followers.get("node-2");
        assertEquals(List.of(committedLocal), node2.storage.storedLog());
        assertEquals(List.of(committedLocal), node2.state.entries());
        assertEquals(1L, node2.state.commitIndex());
        assertEquals(0L, leader.matchIndexOf("node-2"));
        assertEquals(0L, leader.commitIndex());
        assertFalse(cluster.network.deliveryFailures().isEmpty(),
                "the follower must have failed closed instead of acknowledging");
        assertThrows(IllegalStateException.class,
                () -> node2.handler.handle(heartbeat(leader.currentTerm(), 0),
                        LEADER),
                "the failed handler must refuse further requests");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 4: partition 2/4 then heal
    // -------------------------------------------------------------------------------------------

    @Test
    void partitionedFollowersDoNotBlockProgressAndCatchUpAfterHeal() throws Exception {
        RaftState leader = leaderTermOneWithEntries(5);
        Map<String, FollowerNode> followers = emptyFollowers();
        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));

        cluster.network.partition("node-4");
        cluster.network.partition("node-5");
        cluster.service.replicateAll();

        Map<String, Long> previous = matchIndices(leader);
        for (int round = 0; round < 60; round++) {
            cluster.runRound();
            assertMatchIndexMonotonic(previous, leader);
            previous = matchIndices(leader);
        }

        assertEquals(5L, leader.matchIndexOf("node-2"));
        assertEquals(5L, leader.matchIndexOf("node-3"));
        assertEquals(0L, leader.matchIndexOf("node-4"));
        assertEquals(0L, leader.matchIndexOf("node-5"));
        assertEquals(leader.entries(), followers.get("node-2").state.entries());
        assertEquals(leader.entries(), followers.get("node-3").state.entries());

        cluster.network.heal("node-4");
        cluster.network.heal("node-5");
        for (int round = 0; round < 120; round++) {
            cluster.runRound();
            assertMatchIndexMonotonic(previous, leader);
            previous = matchIndices(leader);
        }

        for (String peer : PEERS) {
            assertEquals(5L, leader.matchIndexOf(peer), "matchIndex of " + peer);
            assertEquals(leader.entries(), followers.get(peer).state.entries(), "log of " + peer);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 5: higher term and late replies
    // -------------------------------------------------------------------------------------------

    @Test
    void higherTermReplyStepsLeaderDown() throws Exception {
        RaftState leader = leaderTermOneWithEntries(2);
        Map<String, FollowerNode> followers = emptyFollowers();
        followers.put("node-2", follower(5, 0));
        RecordingHooks hooks = new RecordingHooks(leader, true);
        Cluster cluster = new Cluster(leader, followers, hooks);

        cluster.service.replicate("node-2");
        cluster.runRound();

        assertEquals(1, hooks.higherTermCalls());
        assertEquals(5L, hooks.lastHigherTerm());
        assertEquals(Role.FOLLOWER, leader.role());
        assertEquals(0L, leader.commitIndex());

        int callsBefore = cluster.network.allCalls().size();
        cluster.service.replicateAll();
        cluster.runRound();
        assertEquals(callsBefore, cluster.network.allCalls().size(),
                "a stepped-down Leader must not keep sending APPEND_ENTRIES");
    }

    @Test
    void lateHigherTermIsHandledButLateEqualTermIsIgnored() throws Exception {
        RaftState leader = leaderTermOneWithEntries(1);
        Map<String, FollowerNode> followers = emptyFollowers();
        RecordingHooks hooks = new RecordingHooks(leader, true);
        Cluster cluster = new Cluster(leader, followers, hooks);
        RaftNode.AppendEntriesRequest request = leaderRequest(leader);

        // RaftTransportAdapterTest already proves the wire-level late-reply rules. Here the real
        // RaftReplicationService receives the same outcomes the adapter would surface.
        cluster.service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.Failed("node-2", request,
                        RaftTransportAdapter.FailureKind.TIMEOUT));
        assertEquals(0, hooks.higherTermCalls(), "a timeout is not a higher term");
        assertEquals(Role.LEADER, leader.role());

        cluster.service.handleAppendResult("node-2", request,
                new RaftTransportAdapter.LateHigherTerm("node-2", 5L));
        assertEquals(1, hooks.higherTermCalls());
        assertEquals(5L, hooks.lastHigherTerm());
        assertEquals(Role.FOLLOWER, leader.role());
        assertEquals(0L, leader.commitIndex());

        RaftState equalTermLeader = leaderTermOneWithEntries(1);
        Map<String, FollowerNode> equalTermFollowers = emptyFollowers();
        RecordingHooks equalTermHooks = new RecordingHooks(equalTermLeader, true);
        Cluster equalTermCluster = new Cluster(equalTermLeader, equalTermFollowers, equalTermHooks);
        RaftNode.AppendEntriesRequest equalTermRequest = leaderRequest(equalTermLeader);

        equalTermCluster.service.handleAppendResult("node-2", equalTermRequest,
                new RaftTransportAdapter.Replied("node-2", equalTermRequest,
                        new RaftNode.AppendEntriesResponse(1L, false, 0L)));

        assertEquals(0, equalTermHooks.higherTermCalls());
        assertEquals(Role.LEADER, equalTermLeader.role());
        assertEquals(0L, equalTermLeader.matchIndexOf("node-2"));
        assertEquals(1, equalTermHooks.rejectionCount());
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 6: heartbeat
    // -------------------------------------------------------------------------------------------

    @Test
    void heartbeatPreservesFollowerLogAndDoesNotInflateMatchIndex() throws Exception {
        RaftState leader = leaderTermOneWithEntries(3);
        Map<String, FollowerNode> followers = emptyFollowers();
        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));

        cluster.service.replicateAll();
        cluster.settle(200);
        for (String peer : PEERS) {
            assertEquals(3L, leader.matchIndexOf(peer));
        }
        List<List<RaftLogEntry>> logsBefore = new ArrayList<>();
        for (String peer : PEERS) {
            logsBefore.add(followers.get(peer).state.entries());
        }
        int callsBefore = cluster.network.allCalls().size();

        cluster.service.replicateAll();
        cluster.settle(100);

        List<FakeNetwork.Call> heartbeats =
                cluster.network.allCalls().subList(callsBefore, cluster.network.allCalls().size());
        assertEquals(4, heartbeats.size());
        for (FakeNetwork.Call heartbeat : heartbeats) {
            AppendEntriesRequestDto payload = heartbeat.request.getPayload();
            assertTrue(payload.getEntries().isEmpty(), "a caught-up peer receives an empty heartbeat");
            assertEquals(3L, payload.getPrevLogIndex());
            assertEquals(1L, payload.getPrevLogTerm());
            assertEquals(0L, payload.getLeaderCommit());
        }
        int position = 0;
        for (String peer : PEERS) {
            FollowerNode follower = followers.get(peer);
            assertEquals(logsBefore.get(position++), follower.state.entries());
            assertEquals(3L, leader.matchIndexOf(peer), "heartbeat must not raise matchIndex");
            assertEquals(0L, follower.state.commitIndex());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 7: failures never count as ACK
    // -------------------------------------------------------------------------------------------

    @Test
    void transportTimeoutInvalidResponseAndPeerErrorNeverCountAsAck() throws Exception {
        for (FailureMode mode : FailureMode.values()) {
            RaftState leader = leaderTermOneWithEntries(2);
            Map<String, FollowerNode> followers = emptyFollowers();
            RecordingHooks hooks = new RecordingHooks(leader, false);
            Cluster cluster = new Cluster(leader, followers, hooks);

            cluster.service.replicate("node-2");
            cluster.drain();
            FakeNetwork.Call call = cluster.network.pendingCall("node-2");
            assertNotNull(call, "mode " + mode);

            switch (mode) {
                case TIMEOUT -> cluster.service.handleAppendResult("node-2", leaderRequest(leader),
                        new RaftTransportAdapter.Failed("node-2", leaderRequest(leader),
                                RaftTransportAdapter.FailureKind.TIMEOUT));
                case TRANSPORT_ERROR -> cluster.network.failTransport(call, new IOException("fake transport"));
                case INVALID_RESPONSE -> {
                    long expected = call.request.getPayload().getPrevLogIndex()
                            + call.request.getPayload().getEntries().size();
                    cluster.network.completeCall(call, leader.currentTerm(), true, expected + 1L);
                }
                case PEER_ERROR -> cluster.network.completePeerError(call);
            }
            cluster.drain();

            assertEquals(0L, leader.matchIndexOf("node-2"), mode + " must not advance matchIndex");
            assertEquals(0L, leader.commitIndex(), mode + " must not advance commitIndex");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 8: one in-flight RPC per peer, independent progress
    // -------------------------------------------------------------------------------------------

    @Test
    void enforcesOneInFlightAppendPerPeerWithIndependentProgress() throws Exception {
        RaftState leader = leaderTermOneWithEntries(5);
        Map<String, FollowerNode> followers = emptyFollowers();
        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false), 2, 10);

        cluster.service.replicateAll();
        cluster.drain();
        assertEquals(4, cluster.network.pendingCount());
        assertEquals(1, cluster.network.maxActivePerPeer());

        cluster.service.replicateAll();
        cluster.drain();
        assertEquals(4, cluster.network.pendingCount(),
                "a second replicate must not add a second in-flight RPC per peer");

        cluster.network.deliverPeer("node-2");
        cluster.drain();
        assertTrue(cluster.network.requestsFor("node-2") > 1,
                "requests to node-2: " + cluster.network.requestsFor("node-2"));
        assertEquals(1, cluster.network.pendingCountFor("node-3"));
        assertEquals(1, cluster.network.pendingCountFor("node-4"));
        assertEquals(1, cluster.network.pendingCountFor("node-5"));
        assertEquals(0L, leader.matchIndexOf("node-3"));
        assertEquals(0L, leader.matchIndexOf("node-4"));
        assertEquals(0L, leader.matchIndexOf("node-5"));

        cluster.settle(300);
        for (String peer : PEERS) {
            assertEquals(5L, leader.matchIndexOf(peer));
            assertEquals(leader.entries(), followers.get(peer).state.entries());
        }
        assertEquals(1, cluster.network.maxActivePerPeer());
    }

    // -------------------------------------------------------------------------------------------
    // Scenario 9: this task does not compute commit
    // -------------------------------------------------------------------------------------------

    @Test
    void replicationNeverAdvancesLeaderCommitAndFollowersNeverOvershootTheirOwnLog() throws Exception {
        RaftState leader = leaderTermOneWithEntries(5);
        leader.advanceCommitIndex(2L);

        Map<String, FollowerNode> followers = emptyFollowers();
        Cluster cluster = new Cluster(leader, followers, new RecordingHooks(leader, false));
        cluster.service.replicateAll();
        cluster.settle(300);

        assertEquals(2L, leader.commitIndex(),
                "D15 replication must not change the Leader commit index");
        for (String peer : PEERS) {
            FollowerNode follower = followers.get(peer);
            assertEquals(5L, follower.state.lastLogIndex());
            assertEquals(Math.min(2L, follower.state.lastLogIndex()),
                    follower.state.commitIndex(), "commit of " + peer);
            assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
            assertEquals(2L, follower.state.commitIndex());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static RaftState leaderTermOneWithEntries(int count) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        state.becomeLeader(new TreeSet<>(PEERS));
        for (int index = 1; index <= count; index++) {
            state.append(entry(index, 1, "r-" + index));
        }
        return state;
    }

    /**
     * Builds a Leader in term 2 whose first entries are the supplied term-1 prefix. This shape is
     * needed to exercise an old-term prefix and a future current-term commit.
     */
    private static RaftState leaderTermTwoWith(List<RaftLogEntry> termOnePrefix, int currentTermEntries) {
        RaftState state = RaftState.restore(1L, null, termOnePrefix, 0L);
        state.startElection(LEADER);
        state.becomeLeader(new TreeSet<>(PEERS));
        long index = state.lastLogIndex();
        for (int i = 0; i < currentTermEntries; i++) {
            index++;
            state.append(entry(index, 2, "r-" + index));
        }
        return state;
    }

    private static RaftLogEntry entry(long index, long term, String requestId) {
        return new RaftLogEntry(index, term, BOOK, "{\"requestId\":\"" + requestId + "\"}");
    }

    private static Map<String, FollowerNode> emptyFollowers() throws Exception {
        Map<String, FollowerNode> followers = new LinkedHashMap<>();
        for (String peer : PEERS) {
            followers.put(peer, follower(1L, 0L));
        }
        return followers;
    }

    private static FollowerNode follower(long term, long lastApplied, RaftLogEntry... entries)
            throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, List.of(entries), lastApplied);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        AppendEntriesHandler handler = new AppendEntriesHandler(
                new PersistentElectionState(state, storage), appender,
                new LogConflictResolver(appender), new RecordingTimer());
        return new FollowerNode(storage, state, handler);
    }

    private static RaftNode.AppendEntriesRequest heartbeat(long term, long prevLogIndex) {
        return new RaftNode.AppendEntriesRequest(term, LEADER, prevLogIndex, 1L, List.of(), 0L);
    }

    private static RaftNode.AppendEntriesRequest leaderRequest(RaftState leader) {
        long prevLogIndex = Math.max(0L, leader.lastLogIndex());
        return new RaftNode.AppendEntriesRequest(leader.currentTerm(), LEADER, prevLogIndex,
                leader.termAt(prevLogIndex), List.of(), leader.commitIndex());
    }

    private static Map<String, Long> matchIndices(RaftState leader) {
        Map<String, Long> snapshot = new TreeMap<>();
        for (String peer : PEERS) {
            snapshot.put(peer, leader.matchIndexOf(peer));
        }
        return snapshot;
    }

    private static void assertMatchIndexMonotonic(Map<String, Long> previous, RaftState leader) {
        for (String peer : PEERS) {
            assertTrue(leader.matchIndexOf(peer) >= previous.get(peer),
                    "matchIndex of " + peer + " decreased: " + previous.get(peer)
                            + " -> " + leader.matchIndexOf(peer));
        }
    }

    /**
     * Raft Log Matching Property: if two logs contain an entry with the same index and term, the
     * entry payload is equal and every preceding entry is equal too.
     */
    private static void assertLogMatching(Collection<RaftState> states) {
        List<RaftState> nodes = new ArrayList<>(states);
        for (int first = 0; first < nodes.size(); first++) {
            for (int second = first + 1; second < nodes.size(); second++) {
                RaftState left = nodes.get(first);
                RaftState right = nodes.get(second);
                long common = Math.min(left.lastLogIndex(), right.lastLogIndex());
                for (long index = 1; index <= common; index++) {
                    if (left.termAt(index) == right.termAt(index)) {
                        assertEquals(left.entryAt(index), right.entryAt(index),
                                "same (index, term) must carry the same payload at index " + index);
                        for (long prefix = 1; prefix <= index; prefix++) {
                            assertEquals(left.entryAt(prefix), right.entryAt(prefix),
                                    "matching (index, term) requires the same prefix at " + prefix);
                        }
                    }
                }
            }
        }
    }

    private enum FailureMode {
        TIMEOUT, TRANSPORT_ERROR, INVALID_RESPONSE, PEER_ERROR
    }

    // -------------------------------------------------------------------------------------------
    // Cluster fixture: real Leader-side services over a fake network and real follower handlers
    // -------------------------------------------------------------------------------------------

    static final class Cluster {

        final RaftState leader;
        final ReplicationTracker tracker;
        final Map<String, FollowerNode> followers;
        final Set<String> peers;
        final FakeNetwork network;
        final RaftReplicationService service;
        final ManualRetryScheduler retries = new ManualRetryScheduler();
        final ScheduledThreadPoolExecutor adapterScheduler = adapterScheduler();
        final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
        final List<Throwable> loopFailures = new ArrayList<>();
        private final AppendRetryCoordinator coordinator;

        private static ScheduledThreadPoolExecutor adapterScheduler() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
                Thread thread = new Thread(task, "d15-fake-adapter-deadline");
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }

        Cluster(RaftState leader, Map<String, FollowerNode> followers,
                RaftReplicationService.Hooks hooks) {
            this(leader, followers, hooks, 64, 10);
        }

        Cluster(RaftState leader, Map<String, FollowerNode> followers,
                RaftReplicationService.Hooks hooks, int batchMax, int maxBacktrackAttempts) {
            this.leader = leader;
            this.followers = Map.copyOf(followers);
            this.peers = new TreeSet<>(leader.matchIndex().keySet());
            this.tracker = new ReplicationTracker(leader, peers);
            this.network = new FakeNetwork(followers);
            RaftTransportAdapter transport = new RaftTransportAdapter(
                    LEADER, network, Duration.ofSeconds(30L), loop::add, adapterScheduler,
                    loopFailures::add);
            RaftReplicationService.Backoff backoff = new RaftReplicationService.Backoff(10L, 100L);
            this.service = new RaftReplicationService(LEADER, leader, transport, peers,
                    loop::add, retries, batchMax, backoff, hooks);
            this.coordinator = new AppendRetryCoordinator(leader, tracker, service, loop::add,
                    retries, backoff, maxBacktrackAttempts, new AppendRetryCoordinator.Hooks() {
                        @Override
                        public void onHigherTerm(long term) {
                            hooks.onHigherTerm(term);
                        }

                        @Override
                        public void onGiveUp(String peerId, String reason) {
                        }
                    });
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
            throw new AssertionError("cluster did not settle: pending="
                    + network.pendingCount() + ", retries=" + retries.pending());
        }
    }

    static final class FollowerNode {

        final FakeRaftStorage storage;
        final RaftState state;
        final AppendEntriesHandler handler;

        FollowerNode(FakeRaftStorage storage, RaftState state, AppendEntriesHandler handler) {
            this.storage = storage;
            this.state = state;
            this.handler = handler;
        }
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

    static class RecordingHooks implements RaftReplicationService.Hooks {

        private final RaftState state;
        private final boolean stepDownOnHigherTerm;
        private long lastHigherTerm = -1L;
        private int higherTermCalls;
        private int rejectionCount;
        private final List<Throwable> failures = new ArrayList<>();

        RecordingHooks(RaftState state, boolean stepDownOnHigherTerm) {
            this.state = state;
            this.stepDownOnHigherTerm = stepDownOnHigherTerm;
        }

        @Override
        public void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
            rejectionCount++;
        }

        @Override
        public void onHigherTerm(long term) {
            higherTermCalls++;
            lastHigherTerm = term;
            if (stepDownOnHigherTerm && state.role() == Role.LEADER) {
                state.becomeFollower(term);
            }
        }

        @Override
        public void onFailure(String peerId, Throwable failure) {
            failures.add(failure);
        }

        long lastHigherTerm() {
            return lastHigherTerm;
        }

        int higherTermCalls() {
            return higherTermCalls;
        }

        int rejectionCount() {
            return rejectionCount;
        }

        List<Throwable> failures() {
            return List.copyOf(failures);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fake network
    // -------------------------------------------------------------------------------------------

    static final class FakeNetwork implements RaftTransportAdapter.RaftRpcClient {

        static final class Call {

            final String peerId;
            final MessageEnvelope<AppendEntriesRequestDto> request;
            final CompletableFuture<MessageEnvelope<JsonNode>> future =
                    new CompletableFuture<>();
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
        private int maxActivePerPeer;

        FakeNetwork(Map<String, FollowerNode> followers) {
            this.followers = Map.copyOf(followers);
            for (String peer : followers.keySet()) {
                pending.put(peer, new ArrayDeque<>());
            }
        }

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peerId,
                MessageEnvelope<AppendEntriesRequestDto> request) {
            Call call = new Call(peerId, request);
            pending.computeIfAbsent(peerId, key -> new ArrayDeque<>()).add(call);
            calls.add(call);
            int active = activePerPeer.merge(peerId, 1, Integer::sum);
            maxActivePerPeer = Math.max(maxActivePerPeer, active);
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
                delivered |= deliverPeerNow(peerId);
            }
            return delivered;
        }

        boolean deliverPeer(String peerId) {
            return deliverPeerNow(peerId);
        }

        private boolean deliverPeerNow(String peerId) {
            ArrayDeque<Call> queue = pending.get(peerId);
            if (queue == null || queue.isEmpty()) {
                return false;
            }
            deliver(queue.poll());
            return true;
        }

        private void deliver(Call call) {
            try {
                RaftNode.AppendEntriesResponse response = followers.get(call.peerId)
                        .handler.handle(call.request.getPayload(), LEADER);
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

        void completeCall(Call call, long term, boolean success, long matchIndex) {
            removeQueued(call);
            call.future.complete(responseEnvelope(call, term, success, matchIndex));
            release(call);
        }

        void failTransport(Call call, Throwable failure) {
            removeQueued(call);
            call.future.completeExceptionally(failure);
            release(call);
        }

        void completePeerError(Call call) {
            removeQueued(call);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("errorCode", "PEER_FAILURE");
            payload.put("message", "fake peer error");
            call.future.complete(envelope(call.request.getMessageId(),
                    MessageType.ERROR_RESPONSE, call.peerId, payload));
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

        int pendingCountFor(String peerId) {
            ArrayDeque<Call> queue = pending.get(peerId);
            return queue == null ? 0 : queue.size();
        }

        int requestsFor(String peerId) {
            int count = 0;
            for (Call call : calls) {
                if (call.peerId.equals(peerId)) {
                    count++;
                }
            }
            return count;
        }

        int maxActivePerPeer() {
            return maxActivePerPeer;
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
    // Manual schedulers (no real time, no threads, no sleeping)
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
}
