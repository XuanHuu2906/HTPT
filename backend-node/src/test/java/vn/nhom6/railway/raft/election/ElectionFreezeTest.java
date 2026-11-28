package vn.nhom6.railway.raft.election;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.RaftElectionService.RuntimeMode;
import vn.nhom6.railway.raft.election.RaftElectionService.TermLogEntry;
import vn.nhom6.railway.raft.election.RaftElectionService.TimeoutConfig;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityStatus;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D28: Release-Candidate Freeze Tests for RaftElectionService:
 * <ul>
 *   <li>Elimination of mock/no-op/simulation election paths in {@link RuntimeMode#PRODUCTION}.</li>
 *   <li>Complete structured event logging (audit trail, logger emission, listener hooks).</li>
 *   <li>Timeout configuration loading &amp; strict production Raft liveness/safety validation.</li>
 * </ul>
 */
@DisplayName("TV5-D28: Raft Election & Recovery Freeze Verification")
class ElectionFreezeTest {

    private static final Set<String> PEERS_OF_NODE1 = Set.of("node-2", "node-3", "node-4", "node-5");

    private static RaftState restoreFrom(FakeRaftStorage storage) throws RaftStorageException {
        PersistentState loaded = storage.load();
        return RaftState.restore(
                loaded.currentTerm(), loaded.votedFor(), loaded.log(), loaded.lastApplied());
    }

    private static WriteAvailabilityPolicy policyFor(String localId, RaftState state, Duration freshness) {
        Set<String> peers = new java.util.TreeSet<>(RaftElectionService.VOTERS);
        peers.remove(localId);
        ReplicationTracker tracker = new ReplicationTracker(state, peers);
        CommitAdvancer advancer = new CommitAdvancer(state, tracker);
        return new WriteAvailabilityPolicy(localId, state, peers, tracker, advancer, freshness);
    }

    @Test
    @DisplayName("1. PRODUCTION mode từ chối storage null, mock RaftNode, transport null/no-op, và fallback quorum")
    void productionMode_rejectsMockDependenciesAndSimulationHelpers() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode realCore = new PersistentRaftNode("node-1", state, storage);
        CandidateElection.VoteTransport realTransport = (peer, req, reply) -> {};

        // 1a. Missing storage -> rejected
        assertThrows(NullPointerException.class, () -> RaftElectionService.production(
                "node-1", realCore, state, null, realTransport, null, () -> {}, () -> {},
                TimeoutConfig.lanProductionDefaults()));

        // 1b. Mock / non-PersistentRaftNode -> rejected
        RaftNode mockNode = new RaftNode() {
            @Override public Role role() { return state.role(); }
            @Override public long currentTerm() { return state.currentTerm(); }
            @Override public java.util.Optional<String> leaderId() { return java.util.Optional.empty(); }
            @Override public long commitIndex() { return 0; }
            @Override public long lastApplied() { return 0; }
            @Override public long lastLogIndex() { return 0; }
            @Override public void onApplied(long index) {}
            @Override public void onElectionTimeout() {}
            @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
                return new RequestVoteResponse(0, false);
            }
            @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
                return new AppendEntriesResponse(0, false, 0);
            }
            @Override public AppendOutcome appendCommand(String type, String payload) {
                return new NotLeader(null);
            }
            @Override public void addCommitListener(CommitListener listener) {}
        };
        assertThrows(IllegalArgumentException.class, () -> new RaftElectionService(
                RuntimeMode.PRODUCTION, TimeoutConfig.lanProductionDefaults(),
                "node-1", mockNode, state, storage, realTransport, null, () -> {}, () -> {}, null));

        // 1c. Null / no-op transport -> rejected
        assertThrows(NullPointerException.class, () -> RaftElectionService.production(
                "node-1", realCore, state, storage, null, null, () -> {}, () -> {},
                TimeoutConfig.lanProductionDefaults()));

        // 1d. Enforced PRODUCTION via system property rejects 4-arg mock constructor
        System.setProperty(RaftElectionService.PROP_RAFT_MODE, "production");
        try {
            assertTrue(RaftElectionService.isProductionModeConfigured());
            assertThrows(IllegalStateException.class,
                    () -> new RaftElectionService("node-1", realCore, state, storage));
        } finally {
            System.clearProperty(RaftElectionService.PROP_RAFT_MODE);
        }

        // 1e. Valid PRODUCTION instance blocks ALL simulation/mock operations and rejects leader without WriteAvailabilityPolicy
        RaftElectionService prodSvc = RaftElectionService.production(
                "node-1", realCore, state, storage, realTransport, null, () -> {}, () -> {},
                TimeoutConfig.lanProductionDefaults());
        assertTrue(prodSvc.isProductionMode());
        assertEquals(RuntimeMode.PRODUCTION, prodSvc.runtimeMode());
        assertEquals(AvailabilityStatus.NOT_LEADER,
                prodSvc.checkWriteAvailability(Instant.now()).status());

        // If state were promoted to LEADER without policy, production fallback still fails closed
        state.startElection("node-1");
        state.becomeLeader(PEERS_OF_NODE1);
        assertThrows(IllegalStateException.class, () -> prodSvc.canCommitWrites(Instant.now()));

        assertThrows(UnsupportedOperationException.class, () -> prodSvc.isolatePeer("node-2"));
        assertThrows(UnsupportedOperationException.class, () -> prodSvc.healPeer("node-2"));
        assertThrows(UnsupportedOperationException.class, () -> prodSvc.partition(Set.of("node-2", "node-3")));
        assertThrows(UnsupportedOperationException.class, prodSvc::healAll);
        assertThrows(UnsupportedOperationException.class, () -> prodSvc.setClock(Clock.systemUTC()));
        assertThrows(UnsupportedOperationException.class, prodSvc::clearTermLog);
        assertThrows(UnsupportedOperationException.class,
                () -> prodSvc.simulateStopLeader("127.0.0.1:9101", "forbidden"));
        assertThrows(UnsupportedOperationException.class, prodSvc::simulateRestartAfterStop);
    }

    @Test
    @DisplayName("2. PRODUCTION mode thực thi bầu cử, tự gắn WriteAvailabilityPolicy, quorum và durable recovery không cần mock hooks")
    void productionMode_runsRealElectionVoteQuorumAndRecovery() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode realCore = new PersistentRaftNode("node-1", state, storage);

        List<String> peersSent = new ArrayList<>();
        RaftElectionService prodSvc = RaftElectionService.production(
                "node-1", realCore, state, storage,
                (peer, req, reply) -> {
                    peersSent.add(peer);
                    try {
                        if ("node-2".equals(peer) || "node-3".equals(peer)) {
                            reply.receive(new RequestVoteResponse(req.getTerm(), true));
                        } else {
                            reply.receive(new RequestVoteResponse(req.getTerm(), false));
                        }
                    } catch (RaftStorageException ex) {
                        throw new RuntimeException(ex);
                    }
                },
                null, () -> {}, () -> {}, TimeoutConfig.lanProductionDefaults());

        long term = prodSvc.startElection();
        assertEquals(1L, term);
        assertEquals(Role.LEADER, prodSvc.role());
        assertEquals(2, peersSent.size(), "Sync 3/5 quorum stops sending remaining vote requests cleanly");
        assertTrue(prodSvc.hasWonElection());
        assertNotNull(prodSvc.writeAvailabilityPolicy(),
                "PRODUCTION mode must auto-wire WriteAvailabilityPolicy upon LEADER promotion");

        // Quorum check requires 3/5 real peer acks in current term
        Instant now = Instant.parse("2026-11-28T12:00:00Z");
        assertEquals(AvailabilityStatus.NO_QUORUM, prodSvc.checkWriteAvailability(now).status());
        prodSvc.recordPeerAck("node-2", 0L, now);
        prodSvc.recordPeerAck("node-3", 0L, now);
        assertEquals(AvailabilityStatus.AVAILABLE, prodSvc.checkWriteAvailability(now).status());
        assertTrue(prodSvc.canCommitWrites(now));

        // Durable recovery succeeds and records RECOVERY_RESTORED
        PersistentState recovered = prodSvc.recoverFromStorage();
        assertEquals(1L, recovered.currentTerm());
        assertTrue(prodSvc.termLog().stream().anyMatch(
                e -> RaftElectionService.EVENT_RECOVERY_RESTORED.equals(e.eventType())));
        assertTrue(RaftElectionService.auditTermLog(prodSvc.termLog()).passed());
    }

    @Test
    @DisplayName("3. Event logs hoàn chỉnh: ghi đủ NODE_STARTED, TIMEOUT_CONFIGURED, VOTE_DENIED, LEADER_ELECTED, RECOVERY_FAILED, FAIL_CLOSED và emit Logger")
    void eventLogs_captureCompleteLifecycleAndEmitRuntimeLogger() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);

        Logger logger = Logger.getLogger(RaftElectionService.class.getName());
        List<LogRecord> capturedLoggerRecords = new CopyOnWriteArrayList<>();
        Handler logTap = new Handler() {
            @Override public void publish(LogRecord record) { capturedLoggerRecords.add(record); }
            @Override public void flush() {}
            @Override public void close() {}
        };
        logger.addHandler(logTap);
        try {
            List<TermLogEntry> listened = new ArrayList<>();
            java.util.function.Consumer<TermLogEntry> listener = listened::add;
            RaftElectionService svc = RaftElectionService.production(
                    "node-1", core, state, storage,
                    (peer, req, reply) -> {
                        try {
                            if ("node-2".equals(peer) || "node-3".equals(peer)) {
                                reply.receive(new RequestVoteResponse(req.getTerm(), true));
                            } else {
                                reply.receive(new RequestVoteResponse(req.getTerm(), false));
                            }
                        } catch (RaftStorageException ex) {
                            throw new RuntimeException(ex);
                        }
                    },
                    null, () -> {}, () -> {}, TimeoutConfig.lanProductionDefaults());
            svc.addEventListener(listener);

            // Start election -> node-2, node-3 grant -> LEADER_ELECTED
            svc.startElection();
            // Same-term rival vote request (already voted for self in term 1) -> VOTE_DENIED
            RequestVoteResponse denied = svc.handleRequestVote(new RequestVoteRequestDto(1L, "node-4", 0L, 0L));
            assertFalse(denied.voteGranted());

            // Corrupt storage on recovery -> RECOVERY_FAILED + FAIL_CLOSED
            storage.seedState(0L, "node-2", List.of(), 0L);
            assertThrows(RaftStorageException.class, svc::recoverFromStorage);

            List<String> eventTypes = svc.termLog().stream().map(TermLogEntry::eventType).toList();
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_NODE_STARTED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_TIMEOUT_CONFIGURED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_ELECTION_STARTED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_VOTE_GRANTED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_VOTE_DENIED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_LEADER_ELECTED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_RECOVERY_FAILED));
            assertTrue(eventTypes.contains(RaftElectionService.EVENT_FAIL_CLOSED));

            TermLogEntry electedEntry = svc.termLog().stream()
                    .filter(e -> RaftElectionService.EVENT_LEADER_ELECTED.equals(e.eventType()))
                    .findFirst().orElseThrow();
            assertEquals("node-1", electedEntry.leaderId(), "LEADER_ELECTED event must identify leaderId");

            List<String> exported = svc.exportEventLogs();
            assertEquals(svc.termLog().size(), exported.size());
            assertTrue(exported.stream().allMatch(line -> line.contains("mode=PRODUCTION") && line.contains("lastApplied=")));
            assertFalse(listened.isEmpty());
            assertTrue(capturedLoggerRecords.stream().anyMatch(
                    r -> r.getMessage() != null && r.getMessage().contains("[RAFT-ELECTION]")));

            svc.removeEventListener(listener);
        } finally {
            logger.removeHandler(logTap);
        }
    }

    @Test
    @DisplayName("4. Config timeout đọc chuẩn env/properties (dev/compose.dev/lan/election-test) và chặn cấu hình không an toàn ở PRODUCTION")
    void timeoutConfig_loadsFromEnvAndRejectsUnsafeProductionBounds() throws Exception {
        // 4a. Default / docker/lan.yml & docker/compose.dev.yml values
        TimeoutConfig lanEnv = TimeoutConfig.fromEnvironment(Map.of(
                RaftElectionService.ENV_HEARTBEAT_INTERVAL_MS, "150",
                RaftElectionService.ENV_ELECTION_TIMEOUT_MIN_MS, "500",
                RaftElectionService.ENV_ELECTION_TIMEOUT_MAX_MS, "1000",
                RaftElectionService.ENV_TCP_RESPONSE_DEADLINE_MS, "3000"
        ), null);
        lanEnv.validateForProduction();
        assertEquals(150L, lanEnv.electionConfig().heartbeatIntervalMillis());
        assertEquals(500L, lanEnv.electionConfig().electionTimeoutMinMillis());
        assertEquals(1000L, lanEnv.electionConfig().electionTimeoutMaxMillis());
        assertEquals(1000L, lanEnv.rpcTimeout().toMillis());
        assertEquals(1000L, lanEnv.quorumFreshnessWindow().toMillis());

        // 4b. docker/dev.yml fast dev profile (50 / 150 / 300)
        TimeoutConfig fastDev = TimeoutConfig.fromEnvironment(Map.of(
                RaftElectionService.ENV_HEARTBEAT_INTERVAL_MS, "50",
                RaftElectionService.ENV_ELECTION_TIMEOUT_MIN_MS, "150",
                RaftElectionService.ENV_ELECTION_TIMEOUT_MAX_MS, "300",
                RaftElectionService.ENV_TCP_RESPONSE_DEADLINE_MS, "3000"
        ), null);
        fastDev.validateForProduction();
        assertEquals(300L, fastDev.rpcTimeout().toMillis());

        // 4c. docker/compose.election-test.yml profile (150 / 700 / 1400, deadline=500)
        Properties props = new Properties();
        props.setProperty("raft.heartbeat.interval.ms", "150");
        props.setProperty("raft.election.timeout.min.ms", "700");
        props.setProperty("raft.election.timeout.max.ms", "1400");
        props.setProperty("tcp.response.deadline.ms", "500");
        TimeoutConfig dockerTest = TimeoutConfig.fromEnvironment(Map.of(), props);
        dockerTest.validateForProduction();
        assertEquals(500L, dockerTest.rpcTimeout().toMillis());

        // 4d. Unsafe production timeout margins MUST fail closed
        TimeoutConfig tooTightMin = new TimeoutConfig(
                new ElectionConfig(150, 300, 1000), Duration.ofMillis(300), Duration.ofMillis(1000));
        assertThrows(IllegalArgumentException.class, tooTightMin::validateForProduction);

        TimeoutConfig tooNarrowSpread = new TimeoutConfig(
                new ElectionConfig(150, 500, 550), Duration.ofMillis(300), Duration.ofMillis(1000));
        assertThrows(IllegalArgumentException.class, tooNarrowSpread::validateForProduction);

        TimeoutConfig rpcExceedsMax = new TimeoutConfig(
                new ElectionConfig(150, 500, 1000), Duration.ofMillis(1500), Duration.ofMillis(1000));
        assertThrows(IllegalArgumentException.class, rpcExceedsMax::validateForProduction);

        TimeoutConfig shortFreshness = new TimeoutConfig(
                new ElectionConfig(150, 500, 1000), Duration.ofMillis(400), Duration.ofMillis(300));
        assertThrows(IllegalArgumentException.class, shortFreshness::validateForProduction);

        // 4e. Runtime mode resolution defaults to PRODUCTION and rejects invalid mode values
        assertEquals(RuntimeMode.PRODUCTION, RaftElectionService.resolveRuntimeMode(Map.of(), null));
        assertEquals(RuntimeMode.PRODUCTION,
                RaftElectionService.resolveRuntimeMode(Map.of(RaftElectionService.ENV_RAFT_MODE, "lan"), null));
        assertEquals(RuntimeMode.TEST_SIMULATION,
                RaftElectionService.resolveRuntimeMode(Map.of(RaftElectionService.ENV_RAFT_MODE, "test_simulation"), null));
        assertThrows(IllegalArgumentException.class,
                () -> RaftElectionService.resolveRuntimeMode(Map.of(RaftElectionService.ENV_RAFT_MODE, "bogus"), null));

        // 4f. nextElectionTimeoutMillis respects configured bounds
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        RaftElectionService svc = RaftElectionService.fromEnvironment(
                "node-1", core, state, storage,
                (peer, req, reply) -> {}, null, () -> {}, () -> {},
                null,
                Map.of(RaftElectionService.ENV_RAFT_MODE, "production",
                        RaftElectionService.ENV_ELECTION_TIMEOUT_MIN_MS, "600",
                        RaftElectionService.ENV_ELECTION_TIMEOUT_MAX_MS, "900",
                        RaftElectionService.ENV_HEARTBEAT_INTERVAL_MS, "150"),
                null);
        assertTrue(svc.isProductionMode());
        for (int i = 0; i < 50; i++) {
            long sampled = svc.nextElectionTimeoutMillis();
            assertTrue(sampled >= 600L && sampled <= 900L);
        }
        assertNotNull(svc.electionConfig());
    }
}
