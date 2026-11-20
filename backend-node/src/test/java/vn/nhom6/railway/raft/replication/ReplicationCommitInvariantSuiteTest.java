package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.BookingRaftBridge;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D20 replication/commit/apply/read-barrier invariant suite over a deterministic FakeNetwork.
 *
 * <p><strong>Honesty boundary.</strong> All five Nodes are in-memory objects on one JVM. Peer
 * transport is a deterministic fake queue; follower storage is {@link FakeRaftStorage}; the retry
 * scheduler, the RPC-deadline executor and the read-barrier timeout scheduler are manual. There
 * are <strong>no</strong> TCP sockets, real threads, sleeps, real clocks, Docker, PostgreSQL, real
 * crash/restart, real elections or five real processes. Elections are simulated by promoting a
 * follower through {@link RaftState#startElection}/{@link RaftState#becomeLeader}; that is a test
 * harness transition, not a proof of the TV5 election implementation. The apply sink is an
 * in-memory transactional repository; {@code CONFIRMED} here means "repository apply returned",
 * not a PostgreSQL commit. G5 remains TV6's responsibility.
 *
 * <p><strong>What is checked.</strong> After every step ({@code assertInvariants}) the suite checks
 * the nine invariants of the D20 plan: majority/current-term commit, Log Matching, monotonic
 * commit with {@code lastApplied <= commitIndex <= lastLogIndex}, state-machine safety, no
 * uncommitted apply with NO_OP not producing a business event, committed-entry durability,
 * fresh-term NO_OP read barriers with no stale read, no business result before commit+apply, and
 * monotonic {@code matchIndex}/valid {@code nextIndex}/one in-flight AppendEntries per peer with
 * failures never counted as ACK.
 *
 * <p>Production source is not modified. Every scenario runs the same hard-coded seed list so a
 * failure is reproducible: the assertion message includes the seed and scenario event.
 */
class ReplicationCommitInvariantSuiteTest {

    static final String LEADER = "node-1";
    static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    static final List<String> PEERS = List.of("node-2", "node-3", "node-4", "node-5");
    static final List<Long> SEEDS = List.of(
            1L, 2L, 3L, 7L, 11L, 13L, 17L, 19L, 23L, 29L,
            31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L, 71L);
    static final ElectionConfig ELECTION_CONFIG = new ElectionConfig(1_000L, 2_000L, 4_000L);
    static final String TRIP = "SE-001";
    static final Instant BASE_TIME = Instant.parse("2026-11-20T08:00:00Z");
    private static final com.fasterxml.jackson.databind.ObjectMapper CANONICAL =
            new com.fasterxml.jackson.databind.ObjectMapper();

    // -------------------------------------------------------------------------------------------
    // Scenario A: normal workload, all five converge
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("A. normal book/cancel/NO_OP workload converges on all five nodes")
    void scenarioA_normalWorkload() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runNormalWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario B: controlled drop/delay/reorder, then heal and converge
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("B. drop/delay/reorder keeps invariants and converges after heal")
    void scenarioB_faultyNetwork() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runFaultyWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario C: 3/5 commits, 2/5 does not, minority leader has no barrier, heal converges
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("C. 3/5 commits, 2/5 does not, minority barrier times out, heal converges")
    void scenarioC_partitions() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runPartitionWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario D: leader change keeps committed prefix, truncates old uncommitted suffix
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("D. new leader keeps committed entries and replaces the old uncommitted suffix")
    void scenarioD_leaderChange() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runLeaderChangeWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario E: crash/replay from lastApplied=0 with the repository kept
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("E. replay from lastApplied=0 keeps the repository and never double-books")
    void scenarioE_crashReplay() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runReplayWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario F: lagging follower with a divergent uncommitted tail keeps the matching prefix
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("F. divergent uncommitted tail converges without deleting the matching prefix")
    void scenarioF_laggingDivergentTail() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.divergentTail(seed)) {
                cluster.runDivergentTailWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario G: old-term entry on 3/5 is not committed directly (Figure 8)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("G. old-term entry is not committed directly; a current-term entry commits it")
    void scenarioG_oldTermIndirectCommit() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.oldTermPrefix(seed)) {
                cluster.runOldTermWorkload();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Scenario H: concurrent read barriers interleaved with writes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("H. concurrent read barriers are fresh current-term NO_OPs and never stale")
    void scenarioH_concurrentBarriers() throws Exception {
        for (long seed : SEEDS) {
            try (Harness cluster = Harness.normal(seed)) {
                cluster.runConcurrentBarrierWorkload();
            }
        }
    }

    // ===========================================================================================
    // Harness: five in-memory Nodes, real Raft/apply services, deterministic fake network
    // ===========================================================================================

    static final class Harness implements AutoCloseable {

        final long seed;
        final LinkedHashMap<String, Node> nodes = new LinkedHashMap<>();
        final FakeNetwork network;
        final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
        final List<Throwable> loopFailures = new ArrayList<>();
        final ManualRetryScheduler retries = new ManualRetryScheduler();
        final ManualEventLoop adapterDeadlines = new ManualEventLoop();
        final ManualBarrierScheduler barrierTimeouts = new ManualBarrierScheduler();
        final LeaderAwaiter awaiter = new LeaderAwaiter();

        final Map<String, Long> previousCommit = new LinkedHashMap<>();
        final Map<Long, RaftLogEntry> appliedCanonical = new LinkedHashMap<>();
        final Map<Long, RaftLogEntry> committedCanonical = new LinkedHashMap<>();
        final Map<String, Long> previousMatch = new LinkedHashMap<>();
        final List<ResultEvent> resultEvents = new ArrayList<>();
        final List<BarrierObservation> barrierObservations = new ArrayList<>();
        final List<CommitAdvance> commitAdvances = new ArrayList<>();

        Node leader;
        ReplicationTracker tracker;
        CommitAdvancer advancer;
        RaftReplicationService service;
        AppendRetryCoordinator coordinator;
        RaftReadBarrier barrier;

        private Harness(long seed) {
            this.seed = seed;
            this.network = new FakeNetwork(this);
        }

        static Harness normal(long seed) throws Exception {
            Harness harness = new Harness(seed);
            harness.installLeader(harness.buildLeader(1, List.of()), 0L);
            harness.installEmptyFollowers(1L);
            harness.finishInit();
            return harness;
        }

        /** Leader has {@code prefix} (term 1) and is promoted to {@code term}; no commit. */
        static Harness oldTermPrefix(long seed) throws Exception {
            Harness harness = new Harness(seed);
            List<RaftLogEntry> prefix = List.of(
                    bookEntry(1L, 1L, seed, 1, "G01"),
                    bookEntry(2L, 1L, seed, 2, "G02"));
            Harness.LeaderSeed seedState = harness.buildLeaderWithPrefix(2L, prefix);
            harness.installLeader(seedState.state, 0L);
            harness.installSeededFollowers(1L, prefix, Map.of(), Map.of());            harness.finishInit();
            return harness;
        }

        /** Leader term 3 with a term-1 matching prefix; one follower also has a term-2 tail. */
        static Harness divergentTail(long seed) throws Exception {
            Harness harness = new Harness(seed);
            List<RaftLogEntry> prefix = List.of(
                    bookEntry(1L, 1L, seed, 1, "G01"),
                    bookEntry(2L, 1L, seed, 2, "G02"));
            Harness.LeaderSeed seedState = harness.buildLeaderWithPrefix(3L, prefix);
            harness.installLeader(seedState.state, 0L);
            List<RaftLogEntry> staleTail = new ArrayList<>(prefix);
            staleTail.add(bookEntry(3L, 2L, seed, 91, "G03"));
            staleTail.add(bookEntry(4L, 2L, seed, 92, "G04"));
            harness.installSeededFollowers(1L, prefix,
                    Map.of("node-2", staleTail), Map.of("node-2", 2L));
            harness.finishInit();
            return harness;
        }

        private record LeaderSeed(RaftState state, long commit) {
        }

        private RaftState buildLeader(long term, List<RaftLogEntry> entries) {
            RaftState state = new RaftState();
            state.startElection(LEADER);
            state.becomeLeader(new TreeSet<>(PEERS));
            for (RaftLogEntry entry : entries) {
                state.append(entry);
            }
            while (state.currentTerm() < term) {
                long next = state.currentTerm() + 1L;
                state.becomeFollower(next - 1L);
                state.startElection(LEADER);
                state.becomeLeader(new TreeSet<>(PEERS));
            }
            return state;
        }

        private LeaderSeed buildLeaderWithPrefix(long term, List<RaftLogEntry> prefix) {
            RaftState state = buildLeader(term, prefix);
            return new LeaderSeed(state, 0L);
        }

        private void installLeader(RaftState leaderState, long commit) throws Exception {
            if (commit > 0L) {
                leaderState.advanceCommitIndex(commit);
            }
            Node node = newNode(LEADER, leaderState);
            nodes.put(LEADER, node);
            leader = node;
        }

        private void installEmptyFollowers(long term) throws Exception {
            for (String peer : PEERS) {
                RaftState state = RaftState.restore(term, null, List.of(), 0L);
                nodes.put(peer, newNode(peer, state));
            }
        }

        private void installSeededFollowers(long term,
                List<RaftLogEntry> defaultLog,
                Map<String, List<RaftLogEntry>> overrides,
                Map<String, Long> termOverrides) throws Exception {
            for (String peer : PEERS) {
                List<RaftLogEntry> log = overrides.getOrDefault(peer, defaultLog);
                long nodeTerm = termOverrides.getOrDefault(peer, term);
                RaftState state = RaftState.restore(nodeTerm, null, log, 0L);
                nodes.put(peer, newNode(peer, state));
            }
        }

        private Node newNode(String id, RaftState state) throws Exception {
            Node node = new Node(this, id, state);
            previousCommit.put(id, state.commitIndex());
            previousMatch.put(id, 0L);
            return node;
        }

        private void finishInit() {
            buildReplication();
            assertInvariants("init");
        }

        private void buildReplication() {
            if (service != null) {
                service.stop();
            }
            Set<String> peers = peersOf(leader.id);
            tracker = new ReplicationTracker(leader.state, peers);
            advancer = new CommitAdvancer(leader.state, tracker);
            network.setLeader(leader.id);
            RaftTransportAdapter transport = new RaftTransportAdapter(
                    leader.id, network, Duration.ofSeconds(30L), loop::add,
                    adapterDeadlines, loopFailures::add);
            hook = new Hook(this);
            RaftReplicationService.Backoff backoff = new RaftReplicationService.Backoff(10L, 100L);
            service = new RaftReplicationService(leader.id, leader.state, transport, peers,
                    loop::add, retries, 64, backoff, hook);
            coordinator = new AppendRetryCoordinator(leader.state, tracker, service, loop::add,
                    retries, backoff, 20, new AppendRetryCoordinator.Hooks() {
                        @Override
                        public void onHigherTerm(long term) {
                            if (leader.state.role() == Role.LEADER) {
                                leader.state.becomeFollower(term);
                            }
                            barrier.onLeadershipLost();
                        }

                        @Override
                        public void onGiveUp(String peerId, String reason) {
                        }
                    });
            awaiter.reset(leader.repo.lastApplied());
            barrier = new RaftReadBarrier(new LeaderPort(leader), awaiter::awaitIndex,
                    barrierTimeouts);
            previousMatch.replaceAll((id, value) -> 0L);
        }

        private Hook hook;

        private static Set<String> peersOf(String leaderId) {
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(leaderId);
            return peers;
        }

        Node node(String id) {
            return Objects.requireNonNull(nodes.get(id), id);
        }

        // ---------------------------------------------------------------------------------------
        // Workloads
        // ---------------------------------------------------------------------------------------

        void runNormalWorkload() {
            for (int step = 0; step < 24; step++) {
                if (step % 6 == 5) {
                    issueBarrier(10_000L);
                    step("A barrier step " + step);
                } else {
                    appendCommand(commandFor(step));
                    step("A command step " + step);
                }
                settleQuietly(120, "A step " + step);
            }
            propagate("A final");
            assertConverged("A");
            assertTrue(resultEvents.stream().anyMatch(event -> event.outcome() == ApplyResult.Outcome.CONFIRMED),
                    seedMessage("A must produce at least one CONFIRMED business result"));
            assertTrue(barrierObservations.stream().anyMatch(BarrierObservation::success),
                    seedMessage("A must produce at least one successful barrier"));
        }

        void runFaultyWorkload() {
            network.enableFaults(new Random(seed * 31L + 7L), 0.40, 0.40);
            for (int step = 0; step < 16; step++) {
                if (step % 4 == 3) {
                    appendNoOp();
                } else {
                    appendCommand(commandFor(step));
                }
                service.replicateAll();
                runRounds(3, "B faulty step " + step);
            }
            network.disableFaultsAndHeal();
            network.dropPending();
            service.stop();
            buildReplication();
            service.replicateAll();
            for (int attempt = 0; attempt < 8; attempt++) {
                if (leader.state.role() == Role.LEADER) {
                    service.replicateAll();
                }
                runRounds(120, "B heal round " + attempt);
            }
            settle(3_000, "B after heal");
            propagate("B final");
            assertConverged("B");
        }

        void runPartitionWorkload() {
            // Phase 1: Leader + node-2 + node-3 is exactly 3/5.
            network.blockPeers(Set.of("node-4", "node-5"));
            for (int step = 0; step < 3; step++) {
                appendCommand(commandFor(step));
                service.replicateAll();
                runRounds(60, "C phase-1 step " + step);
            }
            long phaseOneCommit = leader.state.commitIndex();
            assertTrue(phaseOneCommit >= 3L,
                    seedMessage("3/5 must commit, commitIndex=" + phaseOneCommit));
            assertEquals(0L, node("node-4").state.commitIndex(), "node-4 is in the 2/5 minority");
            assertEquals(0L, node("node-5").state.commitIndex(), "node-5 is in the 2/5 minority");

            // Phase 2: only node-5 is reachable, Leader + node-5 is 2/5.
            network.unblockAll();
            network.blockPeers(Set.of("node-2", "node-3", "node-4"));
            appendCommand(commandFor(10));
            service.replicateAll();
            runRounds(60, "C minority");
            assertEquals(phaseOneCommit, leader.state.commitIndex(),
                    "a 2/5 Leader must not advance commit");
            assertEquals(leader.state.lastApplied(), leader.state.commitIndex(),
                    "the uncommitted entry must not be applied");

            BarrierHandle barrierHandle = issueBarrier(5_000L);
            runRounds(30, "C minority barrier");
            assertFalse(barrierHandle.stage().toCompletableFuture().isDone(),
                    seedMessage("a minority Leader must not complete a read barrier"));
            barrierTimeouts.fireAll();
            assertTrue(barrierHandle.stage().toCompletableFuture().isCompletedExceptionally(),
                    seedMessage("the minority barrier must fail closed"));
            assertTrue(lastBarrierFailedWith(TimeoutException.class),
                    seedMessage("minority barrier must fail with TimeoutException but was "
                            + lastBarrierFailure()));

            // Heal: the 2/5 side and the delayed followers converge.
            network.unblockAll();
            for (int attempt = 0; attempt < 4; attempt++) {
                if (leader.state.role() == Role.LEADER) {
                    service.replicateAll();
                }
                runRounds(80, "C heal round " + attempt);
            }
            settle(1_400, "C after heal");
            propagate("C final");
            assertConverged("C");
            assertTrue(leader.state.commitIndex() > phaseOneCommit,
                    "the formerly isolated entry must commit after healing");
        }

        void runLeaderChangeWorkload() throws Exception {
            appendCommand(commandFor(0));
            appendCommand(commandFor(1));
            settle(400, "D initial commits");
            long committed = leader.state.commitIndex();
            assertTrue(committed >= 2L, seedMessage("D needs a committed prefix"));

            network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
            BookingCommand oldCommand = commandFor(2);
            long oldIndex = appendCommand(oldCommand);
            service.replicateAll();
            runRounds(40, "D isolated old leader");
            assertEquals(committed, leader.state.commitIndex(),
                    "an isolated old Leader must not commit");
            assertEquals(committed, leader.state.lastApplied(),
                    "an uncommitted old-Leader entry must not apply");

            promote("node-2");
            network.unblockAll();
            assertEquals(committed, leader.state.lastLogIndex(),
                    "the new Leader must keep the committed prefix");
            appendCommand(commandFor(3));
            settle(900, "D new leader convergence");
            propagate("D final");

            assertConverged("D");
            assertEquals(committed + 1L, leader.state.commitIndex(),
                    "the new term commits exactly one new entry");
            assertNotEquals(requestIdOf(oldCommand),
                    requestIdOf(leader.state.entryAt(oldIndex)),
                    "the old uncommitted entry must be replaced");
            for (Node node : nodes.values()) {
                assertFalse(node.repo.findResultByRequestId(requestIdOf(oldCommand)).isPresent(),
                        seedMessage("an overwritten entry must never reach the business state"));
            }
        }

        void runReplayWorkload() throws Exception {
            appendCommand(commandFor(0));
            appendCommand(commandFor(1));
            appendNoOp();
            settle(500, "E initial apply");
            Node victim = node("node-3");
            propagate("E before crash");
            long commit = victim.state.commitIndex();
            assertTrue(commit >= 3L, seedMessage("E needs a non-trivial applied prefix"));
            assertEquals(commit, victim.repo.lastApplied());

            Map<String, String> seatsBefore = new TreeMap<>(victim.repo.readSeatSnapshot());
            int resultsBefore = countResults("node-3");
            victim.replayFromDurableLog(leader);
            step("E replay");

            assertEquals(commit, victim.state.commitIndex(), "replay must relearn the same commit");
            assertEquals(commit, victim.state.lastApplied(), "replay must reapply the full prefix");
            assertEquals(commit, victim.repo.lastApplied(), "the repository checkpoint must stay");
            assertEquals(seatsBefore, new TreeMap<>(victim.repo.readSeatSnapshot()),
                    "replay must not double-book a seat");
            assertEquals(resultsBefore, countResults("node-3"),
                    "replay must not publish a duplicate business result");
            assertConverged("E");
        }

        void runDivergentTailWorkload() {
            List<RaftLogEntry> prefix = List.of(
                    leader.state.entryAt(1L), leader.state.entryAt(2L));
            List<String> staleRequestIds = List.of(
                    requestIdOf(node("node-2").state.entryAt(3L)),
                    requestIdOf(node("node-2").state.entryAt(4L)));

            service.replicateAll();
            runRounds(80, "F prefix heartbeat");
            assertEquals(prefix, node("node-2").state.entries().subList(0, 2),
                    "the matching prefix must not be deleted");

            appendCommand(commandFor(2));
            appendCommand(commandFor(3));
            service.replicateAll();
            runRounds(120, "F converge tail");
            service.replicateAll();
            settle(900, "F final");
            propagate("F final commit");

            assertConverged("F");
            assertEquals(prefix, node("node-2").state.entries().subList(0, 2),
                    "the matching prefix must survive convergence");
            for (String staleRequestId : staleRequestIds) {
                for (Node node : nodes.values()) {
                    assertFalse(node.repo.findResultByRequestId(staleRequestId).isPresent(),
                            seedMessage("a truncated entry must never produce a business result"));
                }
            }
        }

        void runOldTermWorkload() {
            service.replicateAll();
            settle(400, "G old-term quorum");
            assertEquals(0L, leader.state.commitIndex(),
                    "a term-1 entry on 3/5 must not be committed directly");
            for (Node node : nodes.values()) {
                assertEquals(0L, node.state.commitIndex());
            }

            appendCommand(commandFor(0));
            service.replicateAll();
            settle(500, "G current-term commit");
            assertEquals(3L, leader.state.commitIndex(),
                    "the term-2 entry commits the old prefix indirectly");
            assertEquals(leader.state.currentTerm(), leader.state.termAt(leader.state.commitIndex()),
                    "the advanced commit index must be a current-term entry");

            service.replicateAll();
            settle(600, "G propagate commit");
            propagate("G final");
            assertConverged("G");
            assertTrue(allAppliedAtLeast(3L), seedMessage("all five must apply the committed prefix"));
        }

        void runConcurrentBarrierWorkload() {
            List<BarrierHandle> handles = new ArrayList<>();
            long latestLogIndex = 0L;
            for (int step = 0; step < 6; step++) {
                handles.add(issueBarrier(10_000L));
                latestLogIndex = leader.state.lastLogIndex();
                appendCommand(commandFor(step));
                service.replicateAll();
                runRounds(20, "H barrier/write step " + step);
            }
            // A minority phase: every barrier must fail closed instead of reading stale data.
            network.blockPeers(Set.of("node-2", "node-3", "node-4"));
            BarrierHandle minority = issueBarrier(3_000L);
            runRounds(30, "H minority barrier");
            assertFalse(minority.stage().toCompletableFuture().isDone(),
                    seedMessage("a minority Leader must not complete a barrier"));
            barrierTimeouts.fireAll();
            assertTrue(lastBarrierFailedWith(TimeoutException.class),
                    seedMessage("H minority barrier must fail with TimeoutException but was "
                            + lastBarrierFailure()));

            network.unblockAll();
            settle(1_200, "H heal");
            propagate("H final");
            for (BarrierHandle handle : handles) {
                assertTrue(handle.stage().toCompletableFuture().isDone(),
                        seedMessage("a majority barrier must settle"));
            }
            assertConverged("H");

            long maxReadIndex = -1L;
            for (BarrierObservation observation : barrierObservations) {
                if (observation.success()) {
                    assertTrue(observation.readIndex() > observation.issueLastLogIndex(),
                            seedMessage("every successful barrier must use a new NO_OP"));
                    assertTrue(observation.readIndex() > maxReadIndex,
                            seedMessage("concurrent barriers must not share one NO_OP index"));
                    maxReadIndex = Math.max(maxReadIndex, observation.readIndex());
                }
            }
            assertTrue(maxReadIndex >= latestLogIndex,
                    seedMessage("a barrier must observe at least the writes issued before it"));
        }

        // ---------------------------------------------------------------------------------------
        // Command/append helpers
        // ---------------------------------------------------------------------------------------

        BookingCommand commandFor(int step) {
            int seat = (step % 10) + 1;
            if (step > 0 && step % 5 == 4) {
                int cancelSeat = ((step / 5) % 10) + 1;
                return cancelCommand(seed, step, ticketId(seed, cancelSeat));
            }
            return bookCommand(seed, step, String.format("G%02d", seat));
        }

        long appendCommand(BookingCommand command) {
            RaftNode.AppendOutcome outcome = leader.raftNode.appendCommand(
                    BookingRaftBridge.commandType(command),
                    canonicalPayload(command));
            RaftNode.Accepted accepted = assertInstanceOf(outcome);
            service.replicateAll();
            return accepted.index();
        }

        long appendNoOp() {
            RaftNode.AppendOutcome outcome = leader.raftNode.appendCommand(
                    RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
            long index = assertInstanceOf(outcome).index();
            service.replicateAll();
            return index;
        }

        private static RaftNode.Accepted assertInstanceOf(RaftNode.AppendOutcome outcome) {
            if (outcome instanceof RaftNode.Accepted accepted) {
                return accepted;
            }
            throw new AssertionError("expected Accepted append outcome but was " + outcome);
        }

        void replicateAll() {
            service.replicateAll();
        }

        // ---------------------------------------------------------------------------------------
        // Barrier helpers
        // ---------------------------------------------------------------------------------------

        record BarrierHandle(long issueLastLogIndex,
                CompletionStage<BookingRequestHandler.ReadBarrierResult> stage) {
        }

        BarrierHandle issueBarrier(long timeoutMillis) {
            long issueIndex = leader.state.lastLogIndex();
            CompletionStage<BookingRequestHandler.ReadBarrierResult> stage;
            try {
                stage = barrier.appendAndAwaitNoOp(Duration.ofMillis(timeoutMillis));
                service.replicateAll();
            } catch (RuntimeException synchronous) {
                CompletableFuture<BookingRequestHandler.ReadBarrierResult> failed =
                        new CompletableFuture<>();
                failed.completeExceptionally(synchronous);
                stage = failed;
            }
            BarrierHandle handle = new BarrierHandle(issueIndex, stage);
            stage.whenComplete((result, failure) -> recordBarrier(handle, result, failure));
            return handle;
        }

        private void recordBarrier(BarrierHandle handle,
                BookingRequestHandler.ReadBarrierResult result, Throwable failure) {
            Node current = leader;
            if (failure == null && result != null) {
                RaftLogEntry entry = null;
                if (result.readIndex() >= RaftLogEntry.FIRST_INDEX
                        && result.readIndex() <= current.state.lastLogIndex()) {
                    entry = current.state.entryAt(result.readIndex());
                }
                barrierObservations.add(new BarrierObservation(
                        true, result.readIndex(), handle.issueLastLogIndex(), current.id,
                        current.state.currentTerm(), current.state.commitIndex(),
                        current.repo.lastApplied(), current.state.role() == Role.LEADER,
                        entry == null ? "<missing>" : entry.commandType(),
                        entry == null ? -1L : entry.term(), null));
            } else {
                barrierObservations.add(new BarrierObservation(
                        false, -1L, handle.issueLastLogIndex(), current.id,
                        current.state.currentTerm(), current.state.commitIndex(),
                        current.repo.lastApplied(), current.state.role() == Role.LEADER,
                        "<none>", -1L, unwrap(failure)));
            }
        }

        private boolean lastBarrierFailedWith(Class<? extends Throwable> type) {
            Throwable failure = lastBarrierFailure();
            return failure != null && type.isInstance(failure);
        }

        private Throwable lastBarrierFailure() {
            if (barrierObservations.isEmpty()) {
                return null;
            }
            BarrierObservation last = barrierObservations.get(barrierObservations.size() - 1);
            return last.success() ? null : last.failure();
        }

        private static Throwable unwrap(Throwable failure) {
            Throwable current = failure;
            while (current instanceof CompletionException && current.getCause() != null) {
                current = current.getCause();
            }
            return current == null ? new AssertionError("missing barrier failure") : current;
        }

        // ---------------------------------------------------------------------------------------
        // Leader change and replay
        // ---------------------------------------------------------------------------------------

        void promote(String newLeaderId) throws Exception {
            Node target = node(newLeaderId);
            Node oldLeader = leader;
            if (service != null) {
                service.stop();
            }
            network.dropPending();

            long baseTerm = Math.max(oldLeader.state.currentTerm(), target.state.currentTerm());
            target.state.becomeFollower(baseTerm);
            target.state.startElection(newLeaderId);
            target.state.becomeLeader(new TreeSet<>(peersOf(newLeaderId)));
            target.rebindPersistent();
            oldLeader.state.becomeFollower(target.state.currentTerm());

            leader = target;
            buildReplication();
            step("promote " + newLeaderId);
        }

        // ---------------------------------------------------------------------------------------
        // Steps and invariants
        // ---------------------------------------------------------------------------------------

        void step(String label) {
            assertInvariants(label);
        }

        void runRound(String label) {
            network.step();
            drain();
            retries.fireAll();
            drain();
            assertInvariants(label);
        }

        void runRounds(int rounds, String label) {
            for (int round = 0; round < rounds; round++) {
                runRound(label + " round " + round);
            }
        }

        /** Sends extra heartbeats so followers learn the final commit index. */
        void propagate(String label) {
            for (int attempt = 0; attempt < 3; attempt++) {
                if (leader.state.role() == Role.LEADER) {
                    service.replicateAll();
                }
                settle(200, label + " heartbeat " + attempt);
            }
        }

        void settle(int maxRounds, String label) {
            for (int round = 0; round < maxRounds; round++) {
                boolean progress = network.step();
                drain();
                progress |= retries.fireAll();
                drain();
                if (!progress && network.pendingCount() == 0 && retries.pending() == 0) {
                    assertInvariants(label + " settled");
                    return;
                }
                assertInvariants(label + " round " + round);
            }
            throw new AssertionError(seedMessage("cluster did not settle at " + label
                    + ": pending=" + network.pendingCount() + ", retries=" + retries.pending()));
        }

        private void settleQuietly(int maxRounds, String label) {
            try {
                settle(maxRounds, label);
            } catch (AssertionError failure) {
                // Fault-free scenarios must settle; keep the seed in the failure.
                throw new AssertionError(seedMessage(failure.getMessage()), failure);
            }
        }

        void drain() {
            Runnable task;
            while ((task = loop.poll()) != null) {
                task.run();
            }
        }

        // ---------------------------------------------------------------------------------------
        // The nine D20 invariants
        // ---------------------------------------------------------------------------------------

        void assertInvariants(String label) {
            String context = seedMessage(label);
            // 3/5: progress bounds and monotonic commit; 4/5: applied prefix has a single history.
            for (Node node : nodes.values()) {
                RaftState state = node.state;
                assertTrue(state.lastApplied() <= state.commitIndex(),
                        context + ": lastApplied " + state.lastApplied()
                                + " > commitIndex " + state.commitIndex() + " on " + node.id);
                assertTrue(state.commitIndex() <= state.lastLogIndex(),
                        context + ": commitIndex " + state.commitIndex()
                                + " > lastLogIndex " + state.lastLogIndex() + " on " + node.id);
                long previous = previousCommit.getOrDefault(node.id, 0L);
                assertTrue(state.commitIndex() >= previous,
                        context + ": commitIndex decreased on " + node.id + " from "
                                + previous + " to " + state.commitIndex());
                previousCommit.put(node.id, state.commitIndex());

                assertEquals(state.lastApplied(), node.repo.lastApplied(),
                        context + ": Raft checkpoint and repository checkpoint diverge on " + node.id);

                for (long index = 1L; index <= state.lastApplied(); index++) {
                    RaftLogEntry appliedEntry = state.entryAt(index);
                    RaftLogEntry canonical = appliedCanonical.putIfAbsent(index, appliedEntry);
                    if (canonical != null) {
                        assertEquals(canonical, appliedEntry,
                                context + ": two Nodes applied different entries at index " + index);
                    }
                    if (RaftLogEntry.COMMAND_TYPE_NO_OP.equals(appliedEntry.commandType())) {
                        long noOpIndex = index;
                        BookingStateMachine.ApplyOutcome outcome =
                                node.repo.findAppliedOutcome(noOpIndex).orElseThrow(
                                        () -> new AssertionError(context
                                                + ": NO_OP at " + noOpIndex + " has no stored outcome"));
                        assertTrue(outcome instanceof BookingStateMachine.ApplyOutcome.NoOpApplied,
                                context + ": NO_OP at index " + noOpIndex
                                        + " produced a business outcome " + outcome);
                        assertTrue(resultEvents.stream().noneMatch(
                                        event -> event.index() == noOpIndex),
                                context + ": NO_OP at index " + noOpIndex
                                        + " produced a business result");
                    }
                }

                for (long index = 1L; index <= state.commitIndex(); index++) {
                    RaftLogEntry committedEntry = state.entryAt(index);
                    RaftLogEntry canonical = committedCanonical.putIfAbsent(index, committedEntry);
                    if (canonical != null) {
                        assertEquals(canonical, committedEntry,
                                context + ": committed index " + index + " was overwritten on " + node.id);
                    }
                }
            }

            // 6: once an index is committed anywhere, no Node may hold a different entry there.
            for (Map.Entry<Long, RaftLogEntry> committed : committedCanonical.entrySet()) {
                long index = committed.getKey();
                for (Node node : nodes.values()) {
                    if (node.state.lastLogIndex() >= index) {
                        assertEquals(committed.getValue(), node.state.entryAt(index),
                                context + ": committed index " + index
                                        + " differs on " + node.id);
                    }
                }
            }

            // 2: Log Matching between every pair.
            List<Node> all = new ArrayList<>(nodes.values());
            for (int first = 0; first < all.size(); first++) {
                for (int second = first + 1; second < all.size(); second++) {
                    assertLogMatching(all.get(first), all.get(second), context);
                }
            }

            // 1: the Leader only advanced commit to a current-term entry held by a majority.
            if (leader.state.role() == Role.LEADER) {
                for (CommitAdvance advance : commitAdvances) {
                    assertEquals(advance.termAtCommit(), advance.leaderTerm(),
                            context + ": Leader advanced commit to an old-term entry " + advance);
                }
                long commit = leader.state.commitIndex();
                for (long index = 1L; index <= commit; index++) {
                    int holders = 0;
                    RaftLogEntry entry = leader.state.entryAt(index);
                    for (Node node : nodes.values()) {
                        if (node.state.lastLogIndex() >= index
                                && node.state.entryAt(index).equals(entry)) {
                            holders++;
                        }
                    }
                    assertTrue(holders >= 3,
                            context + ": committed index " + index + " is held by only "
                                    + holders + "/5 Nodes");
                }

                // 9: matchIndex monotonic, nextIndex valid, ACKs only from successful replies.
                for (String peer : peersOf(leader.id)) {
                    long match = leader.state.matchIndexOf(peer);
                    long previous = previousMatch.getOrDefault(peer, 0L);
                    assertTrue(match >= previous,
                            context + ": matchIndex of " + peer + " decreased from "
                                    + previous + " to " + match);
                    previousMatch.put(peer, match);
                    assertTrue(leader.state.nextIndexOf(peer) >= match + 1L,
                            context + ": nextIndex < matchIndex + 1 for " + peer);
                    assertTrue(match <= network.maxAcked(peer),
                            context + ": matchIndex " + match + " of " + peer
                                    + " exceeds successful ACK evidence " + network.maxAcked(peer));
                }
                assertTrue(network.maxActivePerPeer() <= 1,
                        context + ": more than one AppendEntries in flight per peer");
            }

            // 4: Nodes with the same applied length must also expose the same seat snapshot.
            Map<Long, Map<String, String>> snapshots = new LinkedHashMap<>();
            for (Node node : nodes.values()) {
                Map<String, String> snapshot = new TreeMap<>(node.repo.readSeatSnapshot());
                Map<String, String> canonical = snapshots.putIfAbsent(node.state.lastApplied(), snapshot);
                if (canonical != null) {
                    assertEquals(canonical, snapshot,
                            context + ": seat snapshots differ at lastApplied="
                                    + node.state.lastApplied());
                }
            }

            // 7: every successful barrier is a fresh current-term NO_OP already applied locally.
            for (BarrierObservation observation : barrierObservations) {
                if (!observation.success()) {
                    continue;
                }
                assertTrue(observation.leaderAtCompletion(),
                        context + ": a non-Leader returned a barrier result");
                assertEquals(RaftLogEntry.COMMAND_TYPE_NO_OP, observation.entryType(),
                        context + ": a barrier read a non-NO_OP entry");
                assertEquals(observation.leaderTerm(), observation.entryTerm(),
                        context + ": a barrier read an entry from another term");
                assertTrue(observation.readIndex() > observation.issueLastLogIndex(),
                        context + ": a barrier reused an old entry as readIndex");
                assertTrue(observation.commitAtCompletion() >= observation.readIndex(),
                        context + ": a barrier returned before commit");
                assertTrue(observation.repoAppliedAtCompletion() >= observation.readIndex(),
                        context + ": a barrier returned before durable apply");
            }

            // 8: no business result may exist before commit and apply.
            for (ResultEvent event : resultEvents) {
                assertTrue(event.commitIndex() >= event.index(),
                        context + ": business result for index " + event.index()
                                + " appeared before commit");
                assertTrue(event.repoApplied() >= event.index(),
                        context + ": business result for index " + event.index()
                                + " appeared before apply");
                assertTrue(event.persistable(),
                        context + ": a non-persistable UNKNOWN result was published");
                assertNotEquals(ApplyResult.Outcome.UNKNOWN, event.outcome(),
                        context + ": UNKNOWN must not be a committed business result");
            }
        }

        private void assertLogMatching(Node left, Node right, String context) {
            long common = Math.min(left.state.lastLogIndex(), right.state.lastLogIndex());
            long firstTermDifference = 0L;
            for (long index = 1L; index <= common; index++) {
                if (left.state.termAt(index) != right.state.termAt(index)) {
                    firstTermDifference = index;
                    break;
                }
            }
            long equalTermUpTo = firstTermDifference == 0L ? common : firstTermDifference - 1L;
            for (long index = 1L; index <= equalTermUpTo; index++) {
                assertEquals(left.state.entryAt(index), right.state.entryAt(index),
                        context + ": same term prefix differs at index " + index
                                + " between " + left.id + " and " + right.id);
            }
            for (long index = firstTermDifference + 1L; index <= common; index++) {
                if (left.state.termAt(index) == right.state.termAt(index)) {
                    for (long prefix = 1L; prefix <= index; prefix++) {
                        assertEquals(left.state.entryAt(prefix), right.state.entryAt(prefix),
                                context + ": same (index, term) at " + index
                                        + " but prefix differs at " + prefix);
                    }
                    break;
                }
            }
        }

        private void assertConverged(String scenario) {
            List<RaftLogEntry> leaderEntries = leader.state.entries();
            long leaderCommit = leader.state.commitIndex();
            long leaderApplied = leader.state.lastApplied();
            for (Node node : nodes.values()) {
                assertEquals(leaderEntries, node.state.entries(),
                        seedMessage(scenario + ": log of " + node.id + " did not converge"));
                assertEquals(leaderCommit, node.state.commitIndex(),
                        seedMessage(scenario + ": commitIndex of " + node.id + " did not converge"));
                assertEquals(leaderApplied, node.state.lastApplied(),
                        seedMessage(scenario + ": lastApplied of " + node.id + " did not converge"));
            }
            assertEquals(leader.state.lastLogIndex(), leaderCommit,
                    seedMessage(scenario + ": the Leader still has an uncommitted suffix"));
        }

        private boolean allAppliedAtLeast(long index) {
            for (Node node : nodes.values()) {
                if (node.state.lastApplied() < index) {
                    return false;
                }
            }
            return true;
        }

        private int countResults(String nodeId) {
            int count = 0;
            for (ResultEvent event : resultEvents) {
                if (event.nodeId().equals(nodeId)) {
                    count++;
                }
            }
            return count;
        }

        private void recordResult(String nodeId, long index, ApplyResult result) {
            Node node = node(nodeId);
            assertTrue(index <= node.state.commitIndex(),
                    seedMessage("business result at " + index + " before commit on " + nodeId));
            boolean alreadyRecorded = resultEvents.stream()
                    .anyMatch(event -> event.nodeId().equals(nodeId) && event.index() == index);
            if (alreadyRecorded) {
                return;
            }
            resultEvents.add(new ResultEvent(nodeId, index, node.state.commitIndex(),
                    node.repo.lastApplied(), result.outcome(), result.isPersistable()));
        }

        String seedMessage(String message) {
            return "seed=" + seed + ": " + message;
        }

        @Override
        public void close() {
            if (service != null) {
                service.stop();
            }
            for (Node node : nodes.values()) {
                node.close();
            }
        }
    }

    // ===========================================================================================
    // Node: real RaftState/PersistentRaftNode + real apply pipeline + in-memory repository
    // ===========================================================================================

    static final class Node implements AutoCloseable {

        final Harness harness;
        final String id;
        RaftState state;
        FakeRaftStorage storage;
        PersistentRaftNode raftNode;
        ElectionTimer timer;
        final InMemoryBookingRepository repo = new InMemoryBookingRepository();
        RaftStateMachineApplier applier;

        Node(Harness harness, String id, RaftState state) throws Exception {
            this.harness = harness;
            this.id = id;
            this.state = state;
            buildPersistent();
        }

        private void buildPersistent() throws Exception {
            storage = new FakeRaftStorage();
            storage.seedState(state.currentTerm(), state.votedFor(), state.entries(),
                    state.lastApplied());
            raftNode = new PersistentRaftNode(id, state, storage);
            ManualEventLoop clock = new ManualEventLoop();
            timer = new ElectionTimer(
                    new StubElectionService(raftNode, state, id), clock, ELECTION_CONFIG,
                    failure -> {
                        throw new IllegalStateException("election timer failed in D20 harness", failure);
                    });
            raftNode.bindLifecycle(timer, () -> { });
            applier = newApplier();
            raftNode.addCommitListener(applier);
        }

        private RaftStateMachineApplier newApplier() {
            CommittedCommandApplier delegate = new CommittedCommandApplier(repo);
            CommittedCommandApplier.CommitListener sink = (index, command) -> {
                Optional<BookingStateMachine.ApplyOutcome> outcome =
                        delegate.applyCommitted(index, command);
                outcome.ifPresent(applied -> {
                    if (applied instanceof BookingStateMachine.ApplyOutcome.BusinessResult business) {
                        harness.recordResult(id, index, business.applyResult());
                    }
                });
                return outcome;
            };
            return new RaftStateMachineApplier(state, RaftApplyAdapter::decodeCommand, sink);
        }

        /** Re-seeds storage for a simulated leader change (test harness transition only). */
        void rebindPersistent() throws Exception {
            if (timer != null) {
                timer.close();
            }
            buildPersistent();
        }

        /**
         * Simulates a crash where Raft lost {@code lastApplied} but the repository kept its rows.
         * Rebuilds RaftState with lastApplied=0, then relearns the commit index through a real
         * heartbeat and replays through a new applier.
         */
        void replayFromDurableLog(Node currentLeader) throws Exception {
            if (timer != null) {
                timer.close();
            }
            List<RaftLogEntry> log = state.entries();
            long term = state.currentTerm();
            String vote = state.votedFor();
            long commit = state.commitIndex();

            FakeRaftStorage replayStorage = new FakeRaftStorage();
            replayStorage.seedState(term, vote, log, 0L);
            state = RaftState.restore(term, vote, log, 0L);
            storage = replayStorage;
            raftNode = new PersistentRaftNode(id, state, replayStorage);
            ManualEventLoop clock = new ManualEventLoop();
            timer = new ElectionTimer(
                    new StubElectionService(raftNode, state, id), clock, ELECTION_CONFIG,
                    failure -> {
                        throw new IllegalStateException("replay timer failed in D20 harness", failure);
                    });
            raftNode.bindLifecycle(timer, () -> { });
            applier = newApplier();
            raftNode.addCommitListener(applier);

            AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(
                    Math.max(currentLeader.state.currentTerm(), term),
                    currentLeader.id,
                    state.lastLogIndex(),
                    state.termAt(state.lastLogIndex()),
                    commit);
            raftNode.handleAppendEntries(heartbeat, currentLeader.id);
        }

        @Override
        public void close() {
            if (timer != null) {
                timer.close();
            }
        }
    }

    // ===========================================================================================
    // Hook: Leader-side commit advance + apply
    // ===========================================================================================

    static final class Hook implements RaftReplicationService.Hooks {

        private final Harness harness;

        Hook(Harness harness) {
            this.harness = harness;
        }

        @Override
        public void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
        }

        @Override
        public void onHigherTerm(long term) {
            if (harness.leader.state.role() == Role.LEADER) {
                harness.leader.state.becomeFollower(term);
            }
            harness.barrier.onLeadershipLost();
        }

        @Override
        public void onFailure(String peerId, Throwable failure) {
            harness.loopFailures.add(failure);
        }

        @Override
        public void onMatchIndexAdvanced(String peerId, long matchIndex) {
            if (harness.leader.state.role() != Role.LEADER) {
                return;
            }
            OptionalLong advanced = harness.advancer.advance();
            if (advanced.isPresent()) {
                long index = advanced.getAsLong();
                harness.commitAdvances.add(new CommitAdvance(harness.leader.id, index,
                        harness.leader.state.termAt(index),
                        harness.leader.state.currentTerm()));
            }
            harness.leader.applier.applyCommitted();
            harness.awaiter.notifyApplied(harness.leader.repo.lastApplied());
        }
    }

    // ===========================================================================================
    // Read-barrier seams
    // ===========================================================================================

    static final class LeaderPort implements RaftReadBarrier.Port {

        private final Node leader;

        LeaderPort(Node leader) {
            this.leader = leader;
        }

        @Override
        public String localId() {
            return leader.id;
        }

        @Override
        public boolean isLeader() {
            return leader.raftNode.role() == Role.LEADER;
        }

        @Override
        public long currentTerm() {
            return leader.raftNode.currentTerm();
        }

        @Override
        public Optional<String> leaderId() {
            return leader.raftNode.leaderId();
        }

        @Override
        public RaftNode.AppendOutcome appendNoOp() {
            return leader.raftNode.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        }

        @Override
        public long commitIndex() {
            return leader.state.commitIndex();
        }

        @Override
        public OptionalLong termAt(long index) {
            if (index < RaftLogEntry.FIRST_INDEX || index > leader.state.lastLogIndex()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(leader.state.termAt(index));
        }
    }

    static final class LeaderAwaiter {

        private final Map<Long, CompletableFuture<Void>> waiters = new LinkedHashMap<>();
        private long lastApplied;

        synchronized void reset(long applied) {
            lastApplied = applied;
            waiters.values().forEach(future -> future.complete(null));
            waiters.clear();
        }

        synchronized void notifyApplied(long applied) {
            lastApplied = Math.max(lastApplied, applied);
            List<Long> ready = new ArrayList<>();
            for (Long index : waiters.keySet()) {
                if (index <= lastApplied) {
                    ready.add(index);
                }
            }
            for (Long index : ready) {
                waiters.remove(index).complete(null);
            }
        }

        CompletionStage<Void> awaitIndex(long index) {
            synchronized (this) {
                if (lastApplied >= index) {
                    return CompletableFuture.completedFuture(null);
                }
                return waiters.computeIfAbsent(index, key -> new CompletableFuture<>());
            }
        }
    }

    // ===========================================================================================
    // Deterministic fake network with drop/delay/partition
    // ===========================================================================================

    static final class FakeNetwork implements RaftTransportAdapter.RaftRpcClient {

        private static final class Call {

            final String peerId;
            final String leaderId;
            final MessageEnvelope<AppendEntriesRequestDto> request;
            final CompletableFuture<MessageEnvelope<JsonNode>> future = new CompletableFuture<>();
            boolean released;

            Call(String peerId, String leaderId,
                    MessageEnvelope<AppendEntriesRequestDto> request) {
                this.peerId = peerId;
                this.leaderId = leaderId;
                this.request = request;
            }
        }

        private record DelayedCall(Call call, int remainingRounds) {
        }

        private final Harness harness;
        private final Map<String, Deque<Call>> pending = new LinkedHashMap<>();
        private final List<Call> failures = new ArrayList<>();
        private final List<DelayedCall> delayed = new ArrayList<>();
        private final List<Call> calls = new ArrayList<>();
        private final Set<String> blockedPeers = new LinkedHashSet<>();
        private final Map<String, Integer> activePerPeer = new LinkedHashMap<>();
        private final Map<String, Long> ackedMatch = new LinkedHashMap<>();
        private int maxActivePerPeer;
        private String leaderId = LEADER;
        private Random faultRandom = new Random(0L);
        private boolean faultsEnabled;
        private double dropRate;
        private double delayRate;

        FakeNetwork(Harness harness) {
            this.harness = harness;
            for (String peer : PEERS) {
                pending.put(peer, new ArrayDeque<>());
            }
        }

        void setLeader(String leaderId) {
            this.leaderId = leaderId;
            blockedPeers.clear();
            dropPending();
            for (String peer : VOTERS) {
                if (!peer.equals(leaderId)) {
                    pending.computeIfAbsent(peer, key -> new ArrayDeque<>());
                }
            }
            activePerPeer.clear();
            ackedMatch.clear();
            maxActivePerPeer = 0;
        }

        void enableFaults(Random random, double dropRate, double delayRate) {
            this.faultRandom = Objects.requireNonNull(random);
            this.dropRate = dropRate;
            this.delayRate = delayRate;
            this.faultsEnabled = true;
        }

        void disableFaultsAndHeal() {
            this.faultsEnabled = false;
            blockedPeers.clear();
            failures.clear();
            delayed.clear();
        }

        void blockPeers(Set<String> peerIds) {
            blockedPeers.addAll(peerIds);
        }

        void unblockAll() {
            blockedPeers.clear();
        }

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
            Call call = new Call(peerId, leaderId, request);
            calls.add(call);
            int active = activePerPeer.merge(peerId, 1, Integer::sum);
            maxActivePerPeer = Math.max(maxActivePerPeer, active);

            if (faultsEnabled && faultRandom.nextDouble() < dropRate) {
                failures.add(call);
                return call.future;
            }
            if (faultsEnabled && faultRandom.nextDouble() < delayRate) {
                delayed.add(new DelayedCall(call, 1 + faultRandom.nextInt(4)));
                return call.future;
            }
            pending.computeIfAbsent(peerId, key -> new ArrayDeque<>()).add(call);
            return call.future;
        }

        /** One deterministic round: complete failures, age delays, deliver one RPC per peer. */
        boolean step() {
            boolean progress = false;
            for (Call failed : new ArrayList<>(failures)) {
                release(failed);
                failed.future.completeExceptionally(new IOException("fake network drop"));
                progress = true;
            }
            failures.clear();

            List<DelayedCall> aged = new ArrayList<>();
            for (DelayedCall scheduled : delayed) {
                if (scheduled.remainingRounds() <= 1) {
                    pending.computeIfAbsent(scheduled.call().peerId, key -> new ArrayDeque<>())
                            .add(scheduled.call());
                    progress = true;
                } else {
                    aged.add(new DelayedCall(scheduled.call(), scheduled.remainingRounds() - 1));
                }
            }
            delayed.clear();
            delayed.addAll(aged);

            for (String peer : List.copyOf(pending.keySet())) {
                if (blockedPeers.contains(peer)) {
                    continue;
                }
                Deque<Call> queue = pending.get(peer);
                if (queue == null || queue.isEmpty()) {
                    continue;
                }
                deliver(queue.poll());
                progress = true;
            }
            return progress;
        }

        private void deliver(Call call) {
            try {
                Node follower = harness.nodes.get(call.peerId);
                RaftNode.AppendEntriesResponse response = follower.raftNode
                        .handleAppendEntries(call.request.getPayload(), call.leaderId);
                ackedMatch.merge(call.peerId, response.matchIndex(), Math::max);
                call.future.complete(responseEnvelope(call, response));
            } catch (Exception failure) {
                call.future.completeExceptionally(failure);
            } finally {
                release(call);
            }
        }

        void dropPending() {
            for (Deque<Call> queue : pending.values()) {
                while (!queue.isEmpty()) {
                    Call call = queue.poll();
                    release(call);
                    call.future.completeExceptionally(new IOException("fake network leader change"));
                }
            }
            for (DelayedCall scheduled : delayed) {
                release(scheduled.call());
                scheduled.call().future.completeExceptionally(
                        new IOException("fake network leader change"));
            }
            delayed.clear();
            for (Call failed : failures) {
                release(failed);
                failed.future.completeExceptionally(new IOException("fake network leader change"));
            }
            failures.clear();
        }

        private void release(Call call) {
            if (call.released) {
                return;
            }
            call.released = true;
            activePerPeer.computeIfPresent(call.peerId, (peer, active) -> active - 1);
        }

        int pendingCount() {
            int total = delayed.size() + failures.size();
            for (Deque<Call> queue : pending.values()) {
                total += queue.size();
            }
            return total;
        }

        int maxActivePerPeer() {
            return maxActivePerPeer;
        }

        long maxAcked(String peerId) {
            return ackedMatch.getOrDefault(peerId, 0L);
        }

        private static MessageEnvelope<JsonNode> responseEnvelope(
                Call call, RaftNode.AppendEntriesResponse response) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("term", response.term());
            payload.put("success", response.success());
            payload.put("matchIndex", response.matchIndex());
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
            throw new IllegalStateException("cannot encode fake response", failure);
        }
    }

    // ===========================================================================================
    // Manual schedulers: no wall clock, no threads, no sleeping
    // ===========================================================================================

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
            Task scheduled = new Task(Objects.requireNonNull(task));
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

    static final class ManualBarrierScheduler implements RaftReplicationService.Scheduler {

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
            Task scheduled = new Task(Objects.requireNonNull(task));
            tasks.add(scheduled);
            return scheduled;
        }

        void fireAll() {
            List<Task> due = new ArrayList<>(tasks);
            tasks.clear();
            for (Task task : due) {
                if (!task.cancelled) {
                    task.runnable.run();
                }
            }
        }
    }

    /** Virtual-time executor: records deadlines and never runs them implicitly. */
    static final class ManualEventLoop extends AbstractExecutorService
            implements ScheduledExecutorService {

        private final List<FakeScheduledTask> scheduled = new ArrayList<>();
        private boolean shutdown;

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
            throw new UnsupportedOperationException("the D20 harness drains its own loop queue");
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are Runnable-only");
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
            return Long.compare(getDelay(TimeUnit.MILLISECONDS),
                    other.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    // ===========================================================================================
    // Election boundary used only to bind the timer; the harness never campaigns
    // ===========================================================================================

    static final class StubElectionService implements ElectionService {

        private final PersistentRaftNode node;
        private final RaftState state;
        private final String localId;

        StubElectionService(PersistentRaftNode node, RaftState state, String localId) {
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
            throw new UnsupportedOperationException("D20 uses explicit harness promotions");
        }

        @Override
        public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
            return Optional.empty();
        }

        @Override
        public RaftNode.RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) {
            throw new UnsupportedOperationException("D20 does not run elections");
        }

        @Override
        public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
            throw new UnsupportedOperationException("D20 does not count votes");
        }

        @Override
        public void promoteToLeader(Set<String> peers) {
            throw new UnsupportedOperationException("D20 promotes explicitly in the harness");
        }
    }

    // ===========================================================================================
    // Booking fixtures
    // ===========================================================================================

    static BookingCommand.BookTicket bookCommand(long seed, int step, String seat) {
        return new BookingCommand.BookTicket(
                uuid(seed * 10_000L + step),
                TRIP,
                seat,
                "Passenger " + step,
                ticketId(seed, seatNumber(seat)),
                BASE_TIME.plusMillis(step));
    }

    static BookingCommand.CancelTicket cancelCommand(long seed, int step, String ticketId) {
        return new BookingCommand.CancelTicket(
                uuid(seed * 10_000L + 5_000L + step),
                ticketId,
                BASE_TIME.plusMillis(step));
    }

    static RaftLogEntry bookEntry(long index, long term, long seed, int step, String seat) {
        BookingCommand.BookTicket command = bookCommand(seed, step, seat);
        return new RaftLogEntry(index, term, BookingRaftBridge.commandType(command),
                canonicalPayload(command));
    }

    /**
     * Canonical JSON (compact, alphabetically ordered keys) required by {@link RaftTransportAdapter}.
     * Production {@link BookingRaftBridge#serializePayload(BookingCommand)} emits insertion order,
     * which the adapter rejects; that is a documented TV2/TV3 gap, not changed here.
     */
    static String canonicalPayload(BookingCommand command) {
        try {
            Map<String, Object> fields = new TreeMap<>();
            if (command instanceof BookingCommand.BookTicket book) {
                fields.put("requestId", book.requestId());
                fields.put("tripId", book.tripId());
                fields.put("seatId", book.seatId());
                fields.put("passengerName", book.passengerName());
                fields.put("ticketId", book.ticketId());
                fields.put("issuedAt", book.issuedAt().toString());
            } else if (command instanceof BookingCommand.CancelTicket cancel) {
                fields.put("requestId", cancel.requestId());
                fields.put("ticketId", cancel.ticketId());
                fields.put("cancelledAt", cancel.cancelledAt().toString());
            } else if (command instanceof BookingCommand.NoOp) {
                return "{}";
            } else {
                throw new IllegalArgumentException("unsupported command " + command);
            }
            return CANONICAL.writeValueAsString(fields);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("cannot serialize canonical payload", failure);
        }
    }

    static String requestIdOf(RaftLogEntry entry) {
        if (RaftLogEntry.COMMAND_TYPE_NO_OP.equals(entry.commandType())) {
            return "";
        }
        return requestIdOf(RaftApplyAdapter.decodeCommand(entry));
    }

    static String requestIdOf(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return book.requestId();
        }
        if (command instanceof BookingCommand.CancelTicket cancel) {
            return cancel.requestId();
        }
        return "";
    }

    static String ticketId(long seed, int seatNumber) {
        return "tk-" + seed + "-" + seatNumber;
    }

    static int seatNumber(String seat) {
        return Integer.parseInt(seat.substring(1));
    }

    static String uuid(long value) {
        return String.format("%08x-0000-4000-8000-%012x",
                value & 0xffff_ffffL, value & 0xffff_ffff_ffffL);
    }

    // ===========================================================================================
    // Event records used by invariant checks
    // ===========================================================================================

    record ResultEvent(String nodeId, long index, long commitIndex, long repoApplied,
            ApplyResult.Outcome outcome, boolean persistable) {
    }

    record CommitAdvance(String leaderId, long index, long termAtCommit, long leaderTerm) {
    }

    record BarrierObservation(boolean success, long readIndex, long issueLastLogIndex,
            String leaderId, long leaderTerm, long commitAtCompletion,
            long repoAppliedAtCompletion, boolean leaderAtCompletion,
            String entryType, long entryTerm, Throwable failure) {
    }

    // ===========================================================================================
    // Transactional in-memory repository (same replay contract as the JDBC repository)
    // ===========================================================================================

    static final class InMemoryBookingRepository implements BookingRepository {

        private record AppliedEntry(long index, BookingCommand command) {
        }

        private BookingStateMachine machine = new BookingStateMachine();
        private final List<AppliedEntry> applied = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();

        @Override
        public long lastApplied() {
            return machine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            BookingStateMachine staged = new BookingStateMachine();
            for (AppliedEntry entry : applied) {
                staged.apply(entry.index(), entry.command());
            }
            BookingStateMachine.ApplyOutcome outcome = staged.apply(logIndex, command);
            machine = staged;
            applied.add(new AppliedEntry(logIndex, command));
            outcomes.put(logIndex, outcome);
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return machine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomes.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return machine.allSeatStatuses();
        }
    }
}
