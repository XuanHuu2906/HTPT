package vn.nhom6.railway.raft.election;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.networking.ElectionRuntime;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.networking.MessageDispatcher;
import vn.nhom6.railway.networking.NodeConfig;
import vn.nhom6.railway.networking.NodeStatusHandler;
import vn.nhom6.railway.networking.TcpClient;
import vn.nhom6.railway.networking.TcpServer;
import vn.nhom6.railway.networking.TestNetworkFilter;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.RaftElectionService.StopLeaderAction;
import vn.nhom6.railway.raft.election.RaftElectionService.TermLogEntry;
import vn.nhom6.railway.raft.election.RaftElectionService.TraceAuditReport;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityResult;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityStatus;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutOutcome;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutVerificationSituation;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.WriteTimeoutResolution;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D25: LAN Failover Election Test Suite using Recorded Traces &amp; Fake TCP v1 Endpoints.
 *
 * <p><strong>Scope &amp; Honesty Boundary (AGENTS.md §0, §6; SPEC-07 §1; SPEC-08 §6):</strong>
 * <ul>
 *   <li>Uses D22 election/partition infrastructure ({@link TestNetworkFilter}, {@link RaftElectionService})
 *       and TV3-D23 / TV6-D24 LAN inventory templates ({@code configs/lan/node-1..5.properties},
 *       {@code configs/lan/PORTS.md}, {@code fixtures/lan-failover/*.json}).</li>
 *   <li>Does NOT wait for TV6-D25 physical cluster initialization.</li>
 *   <li>Exercises real loopback TCP v1 framing ({@link TcpServer}, {@link TcpClient},
 *       {@link MessageDispatcher}, {@link NodeStatusHandler}) for fake LAN endpoints,
 *       dynamic stop-leader action preparation, structured term logging, and recorded trace auditing.</li>
 *   <li><strong>Real 5-laptop physical LAN failover is NOT claimed here; it is executed at Gate G6.</strong></li>
 * </ul>
 */
@DisplayName("TV5-D25: Raft Election LAN Failover (Recorded Trace & Fake TCP v1 Endpoints)")
class RaftElectionLanTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();
    private static final Instant BASE_TIME = Instant.parse("2026-11-25T09:00:00.000Z");

    private FakeLanTestHarness harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new FakeLanTestHarness();
    }

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // 1. LAN Inventory & Config Integration (TV3-D23 / TV6-D24)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Tải đầy đủ inventory LAN placeholder (5 voter + Laptop 6 Monitor) & configs/lan mà không giả nhận G6 vật lý")
    void lanInventoryAndConfigs_loadAllFiveVotersAndNonVoterMonitor_withoutPhysicalClusterClaim() throws Exception {
        JsonNode inventory = readFixtureJson("fixtures/lan-failover/lan-inventory-placeholder.json");
        assertEquals("TV5-D25", inventory.get("taskRef").asText());
        assertEquals("PLACEHOLDER_TEMPLATE", inventory.get("mode").asText());
        assertFalse(inventory.get("physicalLanVerified").asBoolean(),
                "TV5-D25 must explicitly mark physical 5-laptop LAN failover as NOT yet run (G6 only)");
        assertEquals("G6", inventory.get("deferredToGate").asText());
        assertEquals(5, inventory.get("voterCount").asInt());
        assertEquals(3, inventory.get("quorumSize").asInt());
        assertEquals(9100, inventory.get("ports").get("clientTcp").asInt());
        assertEquals(9200, inventory.get("ports").get("peerRaftTcp").asInt());

        // Cross-check with configs/lan/node-1..5.properties and ElectionRuntime.parsePeers
        String peersString = inventory.get("nodePeersString").asText();
        Map<String, InetSocketAddress> parsedPeers = ElectionRuntime.parsePeers(peersString);
        assertEquals(Set.copyOf(VOTERS), parsedPeers.keySet());
        for (InetSocketAddress addr : parsedPeers.values()) {
            assertEquals(9200, addr.getPort());
        }

        Set<String> seenDbNames = new TreeSet<>();
        for (JsonNode voter : inventory.get("voters")) {
            String nodeId = voter.get("nodeId").asText();
            String configRel = voter.get("configFile").asText();
            File cfgFile = locateRepoFile(configRel);
            assertTrue(cfgFile.isFile(), "LAN config template must exist: " + configRel);

            NodeConfig cfg = NodeConfig.load(cfgFile.getPath());
            assertEquals(nodeId, cfg.nodeId());
            assertEquals(9100, cfg.clientPort());
            assertEquals(9200, cfg.raftPort());
            assertEquals(voter.get("database").asText(), cfg.dbConfig().dbName());
            assertTrue(seenDbNames.add(cfg.dbConfig().dbName()), "Each node must have an isolated DB");

            Properties props = new Properties();
            try (InputStream in = new FileInputStream(cfgFile)) {
                props.load(in);
            }
            assertEquals(peersString, props.getProperty("node.peers"),
                    "All 5 LAN configs must share identical NODE_PEERS string");
        }
        assertEquals(5, seenDbNames.size());

        // Laptop 6 is strictly non-voter
        JsonNode nonVoters = inventory.get("nonVoters");
        assertEquals(1, nonVoters.size());
        assertEquals("Laptop-6", nonVoters.get(0).get("laptop").asText());
        assertFalse(nonVoters.get(0).get("raftVoter").asBoolean());

        // Verify PORTS.md exists
        File portsDoc = locateRepoFile("configs/lan/PORTS.md");
        assertTrue(portsDoc.isFile());
        String portsContent = Files.readString(portsDoc.toPath(), StandardCharsets.UTF_8);
        assertTrue(portsContent.contains("9100") && portsContent.contains("9200"));
    }

    // -------------------------------------------------------------------------------------------
    // 2. Fake TCP v1 LAN Endpoints: Dynamic Leader Discovery (Non-node-1)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Fake TCP v1 endpoint phục vụ NODE_STATUS chuẩn SPEC-03 và phát hiện động Leader (node-3, không hardcode node-1)")
    void fakeTcpEndpoints_serveSpec03NodeStatus_andDynamicallyDiscoverNonNode1Leader() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-3", 1L);

        Map<String, NodeStatusDto> liveSnapshot = harness.probeAllTcpEndpoints();
        assertEquals(5, liveSnapshot.size(), "All 5 fake LAN TCP endpoints must reply with NODE_STATUS_RESPONSE");

        for (NodeStatusDto dto : liveSnapshot.values()) {
            dto.validate();
            assertEquals(1L, dto.getTerm());
            assertTrue(dto.isStorageHealthy());
            assertEquals(4, dto.getPeerConnectivity().size(), "Each node must report connectivity for its 4 peers");
            assertEquals("node-3", dto.getLeaderId());
        }
        assertEquals("LEADER", liveSnapshot.get("node-3").getRole());
        assertEquals("FOLLOWER", liveSnapshot.get("node-1").getRole());

        StopLeaderAction action = RaftElectionService.prepareStopLeaderFromStatuses(
                liveSnapshot.values(),
                harness.clientEndpointMap(),
                "TV5-D25 dynamic non-node-1 leader check");
        assertEquals("node-3", action.targetNodeId(), "Must dynamically target node-3 rather than hardcoding node-1");
        assertEquals(1L, action.observedTerm());
        assertTrue(action.dryRun());
        assertTrue(action.commandHint().contains("node-3"));
        assertEquals(harness.clientEndpointMap().get("node-3"), action.clientEndpoint());
    }

    // -------------------------------------------------------------------------------------------
    // 3. Stop-Leader Action + 4/5 Failover + Structured Term Logging
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Stop-Leader action động trên fake TCP endpoint -> 4/5 bầu Leader mới ở term cao hơn và ghi log term đầy đủ")
    void fakeTcpEndpoints_stopLeaderAction_triggers4of5FailoverAndLogsHigherTerm() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-3", 1L);

        // Discover current leader over real TCP v1 NODE_STATUS
        Map<String, NodeStatusDto> beforeStop = harness.probeAllTcpEndpoints();
        StopLeaderAction prepared = RaftElectionService.prepareStopLeaderFromStatuses(
                beforeStop.values(), harness.clientEndpointMap(), "TV5-D25 4/5 leader stop failover");
        assertEquals("node-3", prepared.targetNodeId());
        assertEquals(1L, prepared.observedTerm());

        // Execute stop-leader action: halts node-3 service (preserving storage) + shuts down TCP endpoint
        StopLeaderAction executed = harness.executeStopLeaderAction(prepared);
        assertFalse(executed.dryRun());
        assertEquals("node-3", executed.targetNodeId());
        assertTrue(harness.node("node-3").service.isStopped());
        assertFalse(harness.node("node-3").service.isLeader());

        // Stopped node's TCP endpoint is unreachable; 4/5 remain online
        Map<String, NodeStatusDto> afterStop = harness.probeAllTcpEndpoints();
        assertEquals(4, afterStop.size());
        assertFalse(afterStop.containsKey("node-3"), "Stopped leader endpoint must be OFFLINE");

        // Remaining 4/5 nodes timeout and elect node-1 in strictly higher term 2
        harness.electLeader("node-1", 2L);
        Map<String, NodeStatusDto> afterFailover = harness.probeAllTcpEndpoints();
        assertEquals(4, afterFailover.size());

        StopLeaderAction nextAction = RaftElectionService.prepareStopLeaderFromStatuses(
                afterFailover.values(), harness.clientEndpointMap(), "Post-failover verification");
        assertEquals("node-1", nextAction.targetNodeId());
        assertEquals(2L, nextAction.observedTerm());
        assertTrue(nextAction.observedTerm() > prepared.observedTerm(), "New Leader term must strictly exceed old Leader term");

        // Verify structured term log records every step and satisfies monotonicity & Election Safety
        List<TermLogEntry> combinedLog = harness.combinedTermLog();
        assertTrue(combinedLog.stream().anyMatch(e ->
                e.nodeId().equals("node-3") && RaftElectionService.EVENT_STOP_LEADER_PREPARED.equals(e.eventType())));
        assertTrue(combinedLog.stream().anyMatch(e ->
                e.nodeId().equals("node-3") && RaftElectionService.EVENT_STOP_LEADER_EXECUTED.equals(e.eventType())));
        assertTrue(combinedLog.stream().anyMatch(e ->
                e.nodeId().equals("node-1") && RaftElectionService.EVENT_LEADER_ELECTED.equals(e.eventType())
                        && e.currentTerm() == 2L));

        TraceAuditReport logAudit = RaftElectionService.auditTermLog(combinedLog);
        assertTrue(logAudit.passed(), "Term log audit must pass: " + logAudit.violations());
        assertEquals("node-3", logAudit.leaderByTerm().get(1L));
        assertEquals("node-1", logAudit.leaderByTerm().get(2L));

        TraceAuditReport statusAudit = RaftElectionService.auditRecordedStatusTrace(
                List.of(beforeStop, afterStop, afterFailover));
        assertTrue(statusAudit.passed(), "Status trace audit must pass: " + statusAudit.violations());
    }

    // -------------------------------------------------------------------------------------------
    // 4. 3/5 Quorum Survival & Re-election (TC-AC05B)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Mất 2 Node (còn 3/5): cụm vẫn bầu Leader hợp lệ ở term mới và WriteAvailability báo AVAILABLE (3/5)")
    void fakeTcpEndpoints_twoNodesDown_3of5MaintainsQuorumAndReelectsLeader() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-5", 1L);

        // Stop Leader node-5 and Follower node-4 -> 3/5 remain: {node-1, node-2, node-3}
        StopLeaderAction stopLeader = RaftElectionService.prepareStopLeaderFromStatuses(
                harness.probeAllTcpEndpoints().values(), harness.clientEndpointMap(), "Stop leader for 3/5 test");
        harness.executeStopLeaderAction(stopLeader);
        harness.stopFollower("node-4");

        Map<String, NodeStatusDto> threeOnline = harness.probeAllTcpEndpoints();
        assertEquals(Set.of("node-1", "node-2", "node-3"), threeOnline.keySet());

        // 3/5 elects node-2 at term 2
        harness.electLeader("node-2", 2L);
        FakeLanNode leader = harness.node("node-2");
        assertTrue(leader.service.isLeader());
        assertEquals(2L, leader.state.currentTerm());

        // Record fresh commit ACKs from the 2 online followers -> 3/5 quorum AVAILABLE
        leader.service.recordPeerAck("node-1", 1L, BASE_TIME);
        leader.service.recordPeerAck("node-3", 1L, BASE_TIME);
        AvailabilityResult availability = leader.service.checkWriteAvailability(BASE_TIME.plusMillis(50));
        assertEquals(AvailabilityStatus.AVAILABLE, availability.status());
        assertEquals(3, availability.activeVoters());
        assertTrue(leader.service.canCommitWrites(BASE_TIME.plusMillis(50)));

        // Verify live TCP status shows unique Leader node-2 at term 2
        Map<String, NodeStatusDto> after3of5 = harness.probeAllTcpEndpoints();
        assertEquals(3, after3of5.size());
        assertEquals("LEADER", after3of5.get("node-2").getRole());
        assertEquals(2L, after3of5.get("node-2").getTerm());
    }

    // -------------------------------------------------------------------------------------------
    // 5. 2/5 Quorum Loss: No New Leader Elected & NO_QUORUM (TC-AC06)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Mất 3 Node (chỉ còn 2/5): Candidate không thể đắc cử (tối đa 2/5 phiếu) và báo NO_QUORUM")
    void fakeTcpEndpoints_threeNodesDown_2of5CannotElectLeaderAndReportsNoQuorum() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-1", 1L);

        // Stop 3 nodes: node-1 (leader), node-4, node-5 -> only {node-2, node-3} remain (2/5)
        harness.executeStopLeaderAction(RaftElectionService.prepareStopLeaderFromStatuses(
                harness.probeAllTcpEndpoints().values(), harness.clientEndpointMap(), "Lose quorum to 2/5"));
        harness.stopFollower("node-4");
        harness.stopFollower("node-5");

        Map<String, NodeStatusDto> twoOnline = harness.probeAllTcpEndpoints();
        assertEquals(Set.of("node-2", "node-3"), twoOnline.keySet());

        // node-2 campaigns at term 2; only node-3 can respond -> 2/5 votes < quorum 3/5
        FakeLanNode node2 = harness.node("node-2");
        long campaignTerm = node2.service.startElection();
        assertEquals(2L, campaignTerm);
        harness.deliverAllPendingVotes();

        assertEquals(2, node2.service.grantedVoteCount(), "Only self + node-3 = 2/5 votes");
        assertFalse(node2.service.hasWonElection(), "2/5 must NOT win election");
        assertFalse(node2.service.isLeader());
        assertEquals(Role.CANDIDATE, node2.state.role());

        // Retry in term 3 still fails to elect a leader
        long retryTerm = node2.service.retryElection();
        assertEquals(3L, retryTerm);
        harness.deliverAllPendingVotes();
        assertFalse(node2.service.isLeader());

        // Live TCP probe sees 0 Leaders -> prepareStopLeaderFromStatuses fails closed
        Map<String, NodeStatusDto> minorityStatuses = harness.probeAllTcpEndpoints();
        assertTrue(minorityStatuses.values().stream().noneMatch(NodeStatusDto::isLeader));
        IllegalStateException noLeaderEx = assertThrows(IllegalStateException.class, () ->
                RaftElectionService.prepareStopLeaderFromStatuses(
                        minorityStatuses.values(), harness.clientEndpointMap(), "Should fail closed in 2/5"));
        assertTrue(noLeaderEx.getMessage().contains("NO_LEADER_FOUND"));

        // Verify WriteTimeoutResolution classifies verified minority as NO_QUORUM
        WriteTimeoutResolution res = node2.service.resolveWriteTimeout(
                0L, retryTerm, false, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, res.outcome());
        assertEquals("NO_QUORUM", res.errorCode());
    }

    // -------------------------------------------------------------------------------------------
    // 6. Old Leader Restart After Stop-Leader Action: Durable Term/Vote Recovery & Rejoin
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Leader cũ restart sau stop-leader: đọc storage bền vững, không giảm term, step-down và tái gia nhập 5/5")
    void fakeTcpEndpoints_oldLeaderRestartAfterStop_recoversTermVoteStepsDownAndRejoins() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-2", 1L);

        // Stop old leader node-2
        StopLeaderAction stopAction = RaftElectionService.prepareStopLeaderFromStatuses(
                harness.probeAllTcpEndpoints().values(), harness.clientEndpointMap(), "Stop node-2 before restart");
        harness.executeStopLeaderAction(stopAction);

        // Remaining 4/5 elect node-4 at term 2
        harness.electLeader("node-4", 2L);
        assertEquals(2L, harness.node("node-4").state.currentTerm());

        // Restart node-2: loads durable storage (term=1, votedFor=node-2), reopens TCP endpoint
        PersistentState loaded = harness.restartStoppedNode("node-2");
        assertNotNull(loaded);
        assertEquals(1L, loaded.currentTerm());
        assertEquals("node-2", loaded.votedFor());
        assertFalse(harness.node("node-2").service.isStopped());
        assertEquals(Role.FOLLOWER, harness.node("node-2").state.role());

        // Receives higher-term heartbeat/RPC from new leader node-4 (term 2)
        harness.node("node-2").service.observeHigherTerm(2L);
        harness.node("node-2").state.becomeFollower(2L, "node-4");
        assertEquals(2L, harness.node("node-2").state.currentTerm());
        assertEquals("node-4", harness.node("node-2").state.leaderId());
        assertEquals(2L, harness.node("node-2").storage.currentTerm);

        // All 5 fake TCP endpoints are now online and converged at term 2 with leader node-4
        Map<String, NodeStatusDto> converged = harness.probeAllTcpEndpoints();
        assertEquals(5, converged.size());
        for (NodeStatusDto dto : converged.values()) {
            assertEquals(2L, dto.getTerm());
            assertEquals("node-4", dto.getLeaderId());
        }
        harness.node("node-2").service.assertTermMonotonicity();
        assertTrue(harness.node("node-2").service.termLog().stream().anyMatch(
                e -> RaftElectionService.EVENT_NODE_RESTARTED.equals(e.eventType())));
        assertTrue(harness.node("node-2").service.termLog().stream().anyMatch(
                e -> RaftElectionService.EVENT_HIGHER_TERM_STEP_DOWN.equals(e.eventType())));
    }

    // -------------------------------------------------------------------------------------------
    // 7. Channel Isolation on Fake LAN Client Port (TC-TCP-04 & configs/lan/PORTS.md)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Phân tách kênh cổng Client 9100 (TC-TCP-04): từ chối REQUEST_VOTE bằng ERROR_RESPONSE(INVALID_REQUEST)")
    void fakeTcpEndpoints_channelIsolation_rejectsRaftRpcOnClientPort() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-1", 1L);

        FakeLanNode node1 = harness.node("node-1");
        RequestVoteRequestDto rogueVote = new RequestVoteRequestDto(99L, "node-2", 0L, 0L);
        MessageEnvelope<RequestVoteRequestDto> env =
                MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-2", rogueVote);

        try (TcpClient client = new TcpClient("127.0.0.1", node1.clientTcpPort(), 1000, 1000)) {
            MessageEnvelope<JsonNode> resp = client.sendAndReceive(env);
            assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
            assertEquals(env.getMessageId(), resp.getCorrelationId());

            ErrorResponseDto err = MAPPER.treeToValue(resp.getPayload(), ErrorResponseDto.class);
            assertEquals("INVALID_REQUEST", err.getErrorCode());
            assertTrue(err.getMessage().contains("Raft RPC messages are not allowed on client channel"));
        }

        // Raft state must NOT have been mutated by rogue RPC on client channel
        assertEquals(1L, node1.state.currentTerm());
        assertEquals(Role.LEADER, node1.state.role());
    }

    // -------------------------------------------------------------------------------------------
    // 8. 3-2 Network Partition via TV3-D21/TV5-D22 TestNetworkFilter + Heal
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Phân vùng mạng LAN 3–2 qua TestNetworkFilter (kế thừa D22): thiểu số NO_QUORUM, đa số bầu term mới, heal step-down")
    void fakeLanPartition3to2_viaTestNetworkFilter_minorityNoCommitMajorityFailoverAndHeal() throws Exception {
        harness.electLeader("node-1", 1L);
        FakeLanNode oldLeader = harness.node("node-1");
        oldLeader.service.recordPeerAck("node-2", 1L, BASE_TIME);
        oldLeader.service.recordPeerAck("node-3", 1L, BASE_TIME);
        assertEquals(AvailabilityStatus.AVAILABLE,
                oldLeader.service.checkWriteAvailability(BASE_TIME.plusMillis(100)).status());

        // Partition: minority {node-1, node-2} vs majority {node-3, node-4, node-5}
        Set<String> minority = Set.of("node-1", "node-2");
        Set<String> majority = Set.of("node-3", "node-4", "node-5");
        harness.applyPartition(minority, majority);

        // Old leader in minority has only 2/5 active voters -> NO_QUORUM
        AvailabilityResult minorityAvail = oldLeader.service.checkWriteAvailability(BASE_TIME.plusMillis(100));
        assertEquals(AvailabilityStatus.NO_QUORUM, minorityAvail.status());
        assertEquals(2, minorityAvail.activeVoters());
        assertFalse(oldLeader.service.canCommitWrites(BASE_TIME.plusMillis(100)));

        // Majority {node-3, node-4, node-5} elects node-4 at term 2
        harness.electLeader("node-4", 2L);
        FakeLanNode newLeader = harness.node("node-4");
        assertTrue(newLeader.service.isLeader());
        assertEquals(2L, newLeader.state.currentTerm());
        newLeader.service.recordPeerAck("node-3", 2L, BASE_TIME.plusSeconds(1));
        newLeader.service.recordPeerAck("node-5", 2L, BASE_TIME.plusSeconds(1));
        assertEquals(AvailabilityStatus.AVAILABLE,
                newLeader.service.checkWriteAvailability(BASE_TIME.plusSeconds(1)).status());

        // Heal partition -> old leader observes higher term 2 and steps down cleanly
        harness.healPartition();
        oldLeader.service.observeHigherTerm(2L);
        oldLeader.state.becomeFollower(2L, "node-4");
        harness.node("node-2").service.observeHigherTerm(2L);
        harness.node("node-2").state.becomeFollower(2L, "node-4");

        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());
        assertEquals("node-4", oldLeader.state.leaderId());

        // Audit combined term log across all 5 nodes
        TraceAuditReport audit = RaftElectionService.auditTermLog(harness.combinedTermLog());
        assertTrue(audit.passed(), "3-2 partition + heal term audit must pass: " + audit.violations());
        assertEquals("node-1", audit.leaderByTerm().get(1L));
        assertEquals("node-4", audit.leaderByTerm().get(2L));
        assertTrue(harness.networkFilter.getDroppedCount() > 0,
                "TestNetworkFilter must have intercepted cross-partition votes");
    }

    // -------------------------------------------------------------------------------------------
    // 9. Laptop 6 Monitor Non-Voter Invariance (TC-DEPLOY-02)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Tắt Laptop 6 (Monitor non-voter — TC-DEPLOY-02): không đổi tập 5 voter hay quorum 3/5")
    void laptop6MonitorOffline_doesNotAffectFiveVoterQuorumOrElection() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-3", 1L);

        // Laptop 6 monitor connects, polls NODE_STATUS, then disconnects ("Laptop 6 powered off")
        Map<String, NodeStatusDto> monitorPoll = harness.probeAllTcpEndpoints("monitor-laptop-6");
        assertEquals(5, monitorPoll.size());

        // Verify Monitor ID is strictly rejected as a voter across all 5 election services
        for (String id : VOTERS) {
            RaftElectionService svc = harness.node(id).service;
            assertFalse(svc.isVoter("monitor-laptop-6"));
            assertFalse(svc.isVoter("laptop-6"));
            assertFalse(svc.isVoter("client-6"));
            assertEquals(5, svc.clusterSize());
            assertEquals(3, svc.quorumSize());
        }

        // After Monitor disappears, stopping Leader node-3 still allows 4/5 to elect node-5 at term 2
        harness.executeStopLeaderAction(RaftElectionService.prepareStopLeaderFromStatuses(
                monitorPoll.values(), harness.clientEndpointMap(), "Failover while Laptop 6 offline"));
        harness.electLeader("node-5", 2L);
        assertTrue(harness.node("node-5").service.isLeader());
        assertEquals(2L, harness.node("node-5").state.currentTerm());
    }

    // -------------------------------------------------------------------------------------------
    // 10-13. Recorded Trace Fixtures Replay & Safety Auditing
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Replay recorded trace 4/5 failover (node-3 term 5 -> node-1 term 6 -> rejoin): PASS mọi invariant")
    void recordedTrace_leaderFailover4of5_replaysAndPassesAllSafetyAssertions() throws Exception {
        JsonNode root = readFixtureJson("fixtures/lan-failover/recorded-trace-leader-failover-4of5.json");
        assertFalse(root.get("physicalLanVerified").asBoolean());

        List<Map<String, NodeStatusDto>> steps = parseTraceSteps(root.get("steps"));
        assertEquals(4, steps.size());

        // Prepare stop-leader action from step 0 baseline
        StopLeaderAction stopAction = RaftElectionService.prepareStopLeaderFromStatuses(
                steps.get(0).values(), Map.of("node-3", "192.168.10.13:9100"), "Replay 4/5 failover");
        assertEquals(root.get("expectedStopLeaderNodeId").asText(), stopAction.targetNodeId());
        assertEquals(root.get("expectedInitialTerm").asLong(), stopAction.observedTerm());

        // Verify step 2 elected expected failover leader in higher term
        StopLeaderAction postFailover = RaftElectionService.prepareStopLeaderFromStatuses(
                steps.get(2).values(), Map.of("node-1", "192.168.10.11:9100"), "Verify post-failover leader");
        assertEquals(root.get("expectedFailoverLeaderNodeId").asText(), postFailover.targetNodeId());
        assertEquals(root.get("expectedFailoverTerm").asLong(), postFailover.observedTerm());
        assertTrue(postFailover.observedTerm() > stopAction.observedTerm());

        // Verify step 3 old leader node-3 rejoined as FOLLOWER at term 6 with caught-up commitIndex
        NodeStatusDto rejoined = steps.get(3).get("node-3");
        assertNotNull(rejoined);
        assertEquals("FOLLOWER", rejoined.getRole());
        assertEquals(6L, rejoined.getTerm());
        assertEquals("node-1", rejoined.getLeaderId());
        assertEquals(13L, rejoined.getLastApplied());

        TraceAuditReport report = RaftElectionService.auditRecordedStatusTrace(steps);
        assertTrue(report.passed(), "4/5 failover recorded trace must pass: " + report.violations());
        assertEquals(Map.of(5L, "node-3", 6L, "node-1"), report.leaderByTerm());
    }

    @Test
    @DisplayName("11. Replay recorded trace 3/5 -> 2/5 mất quorum -> 3/5 khôi phục: 2/5 không có Leader dù term tăng")
    void recordedTrace_3of5And2of5QuorumLoss_verifiesNoMinorityLeaderAndRecovery() throws Exception {
        JsonNode root = readFixtureJson("fixtures/lan-failover/recorded-trace-3of5-and-2of5-quorum-loss.json");
        List<Map<String, NodeStatusDto>> steps = parseTraceSteps(root.get("steps"));
        assertEquals(5, steps.size());

        // Step 1 (3/5): unique leader node-2 at term 3
        assertEquals(3, steps.get(1).size());
        StopLeaderAction leader3of5 = RaftElectionService.prepareStopLeaderFromStatuses(
                steps.get(1).values(), Map.of(), "3/5 leader check");
        assertEquals("node-2", leader3of5.targetNodeId());
        assertEquals(3L, leader3of5.observedTerm());

        // Steps 2 & 3 (2/5): 0 leaders while terms increase 4 -> 5 and commitIndex stays frozen at 7
        for (int idx : List.of(2, 3)) {
            Map<String, NodeStatusDto> step = steps.get(idx);
            assertEquals(2, step.size());
            assertTrue(step.values().stream().noneMatch(NodeStatusDto::isLeader),
                    "2/5 quorum-loss step must have 0 leaders");
            assertTrue(step.values().stream().allMatch(s -> s.getCommitIndex() == 7L),
                    "2/5 must not advance commitIndex");
        }

        // Step 4 (3/5 restored): leader node-3 at term 6, commitIndex advances to 8
        assertEquals(3, steps.get(4).size());
        StopLeaderAction restoredLeader = RaftElectionService.prepareStopLeaderFromStatuses(
                steps.get(4).values(), Map.of(), "3/5 restored check");
        assertEquals("node-3", restoredLeader.targetNodeId());
        assertEquals(6L, restoredLeader.observedTerm());

        TraceAuditReport report = RaftElectionService.auditRecordedStatusTrace(steps);
        assertTrue(report.passed(), "3/5 & 2/5 trace audit must pass: " + report.violations());
        assertEquals(Map.of(2L, "node-5", 3L, "node-2", 6L, "node-3"), report.leaderByTerm());
    }

    @Test
    @DisplayName("12. Replay recorded trace phân vùng 3–2 & heal: thiểu số không commit, đa số bầu term 4, heal hội tụ 5/5")
    void recordedTrace_partition3to2AndHeal_verifiesNoSplitBrainCommitAndConvergence() throws Exception {
        JsonNode root = readFixtureJson("fixtures/lan-failover/recorded-trace-partition-3-2-heal.json");
        List<Map<String, NodeStatusDto>> steps = parseTraceSteps(root.get("steps"));
        assertEquals(3, steps.size());

        // Step 1 (active 3-2 partition): node-2 (term 3) commitIndex stays 5 despite uncommitted lastLogIndex 6;
        // majority node-4 (term 4) commits up to 8.
        Map<String, NodeStatusDto> partitioned = steps.get(1);
        assertEquals(5L, partitioned.get("node-2").getCommitIndex());
        assertEquals(6L, partitioned.get("node-2").getLastLogIndex());
        assertEquals(8L, partitioned.get("node-4").getCommitIndex());
        assertEquals(4L, partitioned.get("node-4").getTerm());

        // Step 2 (healed): all 5 nodes at term 4, leader=node-4, commitIndex == lastApplied == 8
        Map<String, NodeStatusDto> healed = steps.get(2);
        for (NodeStatusDto dto : healed.values()) {
            assertEquals(4L, dto.getTerm());
            assertEquals("node-4", dto.getLeaderId());
            assertEquals(8L, dto.getCommitIndex());
            assertEquals(8L, dto.getLastApplied());
        }
        assertEquals("FOLLOWER", healed.get("node-2").getRole());

        TraceAuditReport report = RaftElectionService.auditRecordedStatusTrace(steps);
        assertTrue(report.passed(), "3-2 partition trace must pass Election Safety (distinct terms 3 and 4): "
                + report.violations());
        assertEquals(Map.of(3L, "node-2", 4L, "node-4"), report.leaderByTerm());
    }

    @Test
    @DisplayName("13. Negative recorded trace: phát hiện và fail-closed khi có Split-Brain cùng term hoặc tụt term sau restart")
    void recordedTrace_negativeSplitBrainAndTermRegression_failsClosed() throws Exception {
        JsonNode root = readFixtureJson("fixtures/lan-failover/recorded-trace-split-brain-negative.json");

        List<Map<String, NodeStatusDto>> splitBrain = parseTraceSteps(root.get("splitBrainSteps"));
        TraceAuditReport splitReport = RaftElectionService.auditRecordedStatusTrace(splitBrain);
        assertFalse(splitReport.passed(), "Split-brain trace (two leaders in term 7) MUST fail audit");
        assertTrue(splitReport.violations().stream().anyMatch(v -> v.contains("Election Safety violated")));

        List<Map<String, NodeStatusDto>> termRegression = parseTraceSteps(root.get("termRegressionSteps"));
        TraceAuditReport regReport = RaftElectionService.auditRecordedStatusTrace(termRegression);
        assertFalse(regReport.passed(), "Term regression (7 -> 6) MUST fail audit");
        assertTrue(regReport.violations().stream().anyMatch(v -> v.contains("term regression")));
    }

    // -------------------------------------------------------------------------------------------
    // 14. Stop-Leader Preparation Fail-Closed Guards
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("14. Chuẩn bị stop-leader fail-closed khi 0 Leader, >1 Leader (split/partition), hoặc Leader storageHealthy=false")
    void stopLeaderPreparation_failClosedOnZeroLeadersMultipleLeadersOrUnhealthyStorage() {
        NodeStatusDto follower = new NodeStatusDto(
                "node-1", "FOLLOWER", 4L, "node-2", 5L, 5L, 5L, true,
                Map.of("node-2", true, "node-3", true, "node-4", true, "node-5", true),
                "2026-11-25T09:40:00.000Z");

        IllegalStateException zeroEx = assertThrows(IllegalStateException.class, () ->
                RaftElectionService.prepareStopLeaderFromStatuses(List.of(follower), Map.of(), "test"));
        assertTrue(zeroEx.getMessage().contains("NO_LEADER_FOUND"));

        NodeStatusDto leaderA = new NodeStatusDto(
                "node-2", "LEADER", 3L, "node-2", 5L, 5L, 5L, true,
                Map.of("node-1", true, "node-3", false, "node-4", false, "node-5", false),
                "2026-11-25T09:40:00.000Z");
        NodeStatusDto leaderB = new NodeStatusDto(
                "node-4", "LEADER", 4L, "node-4", 8L, 8L, 8L, true,
                Map.of("node-1", false, "node-2", false, "node-3", true, "node-5", true),
                "2026-11-25T09:40:00.000Z");

        IllegalStateException multiEx = assertThrows(IllegalStateException.class, () ->
                RaftElectionService.prepareStopLeaderFromStatuses(List.of(leaderA, leaderB), Map.of(), "test"));
        assertTrue(multiEx.getMessage().contains("MULTIPLE_LEADERS_DETECTED"));

        NodeStatusDto unhealthyLeader = new NodeStatusDto(
                "node-2", "LEADER", 4L, "node-2", 5L, 5L, 5L, false,
                Map.of("node-1", true, "node-3", true, "node-4", true, "node-5", true),
                "2026-11-25T09:40:00.000Z");

        IllegalStateException unhealthyEx = assertThrows(IllegalStateException.class, () ->
                RaftElectionService.prepareStopLeaderFromStatuses(List.of(unhealthyLeader), Map.of(), "test"));
        assertTrue(unhealthyEx.getMessage().contains("UNHEALTHY_LEADER"));
    }

    // -------------------------------------------------------------------------------------------
    // 15. Firewall Timeout / Asymmetric Link Drop Simulation on Fake LAN
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("15. Mô phỏng firewall drop/timeout bất đối xứng trên LAN: cô lập 1 node, 4/5 vẫn ổn định và term log chuẩn xác")
    void firewallTimeoutAndAsymmetricDrop_onFakeLanEndpoints_preservesSafetyAndLogsEvents() throws Exception {
        harness.startAllTcpEndpoints();
        harness.electLeader("node-1", 1L);

        // Simulate firewall rule blocking all Raft traffic to/from node-1 (leader isolated by firewall)
        harness.networkFilter.isolate("node-1");
        harness.node("node-1").service.partition(Set.of("node-2", "node-3", "node-4", "node-5"));

        // Isolated leader loses commit quorum (1/5 < 3/5) -> NO_QUORUM
        AvailabilityResult isolatedAvail = harness.node("node-1").service.checkWriteAvailability(BASE_TIME);
        assertEquals(AvailabilityStatus.NO_QUORUM, isolatedAvail.status());
        assertEquals(1, isolatedAvail.activeVoters());

        // Remaining 4 nodes {node-2, node-3, node-4, node-5} elect node-3 at term 2
        harness.electLeader("node-3", 2L);
        assertTrue(harness.node("node-3").service.isLeader());
        assertEquals(2L, harness.node("node-3").state.currentTerm());

        // Firewall rule removed -> node-1 receives higher term 2 and steps down
        harness.networkFilter.deisolate("node-1");
        harness.node("node-1").service.healAll();
        harness.node("node-1").service.observeHigherTerm(2L);
        harness.node("node-1").state.becomeFollower(2L, "node-3");

        assertEquals(Role.FOLLOWER, harness.node("node-1").state.role());
        assertEquals(2L, harness.node("node-1").state.currentTerm());

        // Verify structured log lines are non-empty and monotonic across all 5 nodes
        List<TermLogEntry> allLogs = harness.combinedTermLog();
        assertFalse(allLogs.isEmpty());
        for (TermLogEntry entry : allLogs) {
            String rendered = entry.toLogLine();
            assertTrue(rendered.contains(entry.nodeId()));
            assertTrue(rendered.contains("event=" + entry.eventType()));
        }
        TraceAuditReport report = RaftElectionService.auditTermLog(allLogs);
        assertTrue(report.passed(), "Firewall failover term audit must pass: " + report.violations());
    }

    // -------------------------------------------------------------------------------------------
    // Fixture & File Helpers
    // -------------------------------------------------------------------------------------------

    private static File locateRepoFile(String relativePath) {
        File direct = new File(relativePath);
        if (direct.exists()) {
            return direct;
        }
        File parent = new File(".." + File.separator + relativePath);
        if (parent.exists()) {
            return parent;
        }
        return direct;
    }

    private static JsonNode readFixtureJson(String classpathResource) throws IOException {
        try (InputStream in = RaftElectionLanTest.class.getClassLoader().getResourceAsStream(classpathResource)) {
            assertNotNull(in, "Missing test fixture resource: " + classpathResource);
            return MAPPER.readTree(in);
        }
    }

    private static List<Map<String, NodeStatusDto>> parseTraceSteps(JsonNode stepsArray) throws IOException {
        assertNotNull(stepsArray);
        assertTrue(stepsArray.isArray());
        List<Map<String, NodeStatusDto>> parsed = new ArrayList<>();
        for (JsonNode stepNode : stepsArray) {
            JsonNode statusesNode = stepNode.get("statuses");
            Map<String, NodeStatusDto> map = new LinkedHashMap<>();
            var fields = statusesNode.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                NodeStatusDto dto = MAPPER.treeToValue(entry.getValue(), NodeStatusDto.class);
                map.put(entry.getKey(), dto);
            }
            parsed.add(map);
        }
        return parsed;
    }

    // -------------------------------------------------------------------------------------------
    // Fake 5-Node LAN Harness (Real Loopback TCP v1 Endpoints + RaftElectionService + TestNetworkFilter)
    // -------------------------------------------------------------------------------------------

    private static final class FakeLanTestHarness implements AutoCloseable {
        final TestNetworkFilter networkFilter = new TestNetworkFilter();
        final Map<String, FakeLanNode> nodes = new LinkedHashMap<>();
        final Deque<PendingVote> pendingVotes = new ArrayDeque<>();
        final Set<String> stoppedNodes = new TreeSet<>();

        FakeLanTestHarness() throws RaftStorageException {
            for (String id : VOTERS) {
                nodes.put(id, new FakeLanNode(id, this));
            }
        }

        FakeLanNode node(String id) {
            return Objects.requireNonNull(nodes.get(id), "Unknown node: " + id);
        }

        void startAllTcpEndpoints() throws IOException {
            for (FakeLanNode n : nodes.values()) {
                n.startTcpEndpoint();
            }
        }

        Map<String, String> clientEndpointMap() {
            Map<String, String> map = new LinkedHashMap<>();
            for (FakeLanNode n : nodes.values()) {
                map.put(n.nodeId, "127.0.0.1:" + n.clientTcpPort());
            }
            return Collections.unmodifiableMap(map);
        }

        Map<String, NodeStatusDto> probeAllTcpEndpoints() {
            return probeAllTcpEndpoints("monitor-lan-test");
        }

        Map<String, NodeStatusDto> probeAllTcpEndpoints(String senderId) {
            Map<String, NodeStatusDto> result = new LinkedHashMap<>();
            for (FakeLanNode n : nodes.values()) {
                int port = n.clientTcpPort();
                if (port <= 0) {
                    continue;
                }
                try (TcpClient client = new TcpClient("127.0.0.1", port, 300, 500)) {
                    MessageEnvelope<Map<String, Object>> req =
                            MessageEnvelope.createRequest(MessageType.NODE_STATUS, senderId, Map.of());
                    MessageEnvelope<NodeStatusDto> resp = client.sendAndReceive(req, NodeStatusDto.class);
                    assertEquals(MessageType.NODE_STATUS_RESPONSE, resp.getType());
                    assertEquals(req.getMessageId(), resp.getCorrelationId());
                    result.put(n.nodeId, resp.getPayload());
                } catch (IOException ignored) {
                    // Node endpoint is stopped/offline
                }
            }
            return result;
        }

        void electLeader(String candidateId, long expectedTerm) throws RaftStorageException {
            FakeLanNode candidate = node(candidateId);
            long term = candidate.service.startElection();
            assertEquals(expectedTerm, term, "Unexpected election term for " + candidateId);
            deliverAllPendingVotes();
            assertTrue(candidate.service.isLeader(),
                    "Candidate " + candidateId + " expected to win election at term " + expectedTerm);
            // Propagate leaderId hint & current term to all reachable peers (heartbeat effect)
            for (FakeLanNode peer : nodes.values()) {
                if (peer.nodeId.equals(candidateId) || stoppedNodes.contains(peer.nodeId)) {
                    continue;
                }
                MessageEnvelope<Map<String, Object>> hb =
                        MessageEnvelope.createRequest(MessageType.APPEND_ENTRIES, candidateId, Map.of());
                if (networkFilter.shouldDeliver(candidateId, peer.nodeId, hb)) {
                    if (peer.state.currentTerm() < expectedTerm) {
                        peer.service.observeHigherTerm(expectedTerm);
                    }
                    peer.state.becomeFollower(expectedTerm, candidateId);
                }
            }
        }

        void enqueueVote(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) {
            pendingVotes.addLast(new PendingVote(from, to, req, reply));
        }

        void deliverAllPendingVotes() throws RaftStorageException {
            while (!pendingVotes.isEmpty()) {
                PendingVote pv = pendingVotes.removeFirst();
                if (stoppedNodes.contains(pv.from) || stoppedNodes.contains(pv.to)) {
                    continue;
                }
                MessageEnvelope<RequestVoteRequestDto> env =
                        MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, pv.from, pv.request);
                if (!networkFilter.shouldDeliver(pv.from, pv.to, env)) {
                    continue;
                }
                FakeLanNode target = node(pv.to);
                RequestVoteResponse resp = target.service.handleRequestVote(pv.request);
                MessageEnvelope<Map<String, Object>> respEnv =
                        MessageEnvelope.createResponse(env.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, pv.to, Map.of());
                if (!networkFilter.shouldDeliver(pv.to, pv.from, respEnv)) {
                    continue;
                }
                if (!stoppedNodes.contains(pv.from)) {
                    pv.reply.receive(resp);
                }
            }
        }

        StopLeaderAction executeStopLeaderAction(StopLeaderAction prepared) {
            FakeLanNode target = node(prepared.targetNodeId());
            StopLeaderAction executed = target.service.simulateStopLeader(prepared.clientEndpoint(), prepared.reason());
            stoppedNodes.add(target.nodeId);
            target.stopTcpEndpoint();
            return executed;
        }

        void stopFollower(String nodeId) {
            FakeLanNode target = node(nodeId);
            stoppedNodes.add(nodeId);
            target.service.partition(target.service.peers());
            target.stopTcpEndpoint();
        }

        PersistentState restartStoppedNode(String nodeId) throws Exception {
            FakeLanNode target = node(nodeId);
            stoppedNodes.remove(nodeId);
            PersistentState loaded = target.service.simulateRestartAfterStop();
            target.startTcpEndpoint();
            return loaded;
        }

        void applyPartition(Set<String> partA, Set<String> partB) {
            networkFilter.partition(partA, partB);
            for (String a : partA) {
                node(a).service.partition(partB);
            }
            for (String b : partB) {
                node(b).service.partition(partA);
            }
        }

        void healPartition() {
            networkFilter.heal();
            for (FakeLanNode n : nodes.values()) {
                n.service.healAll();
            }
        }

        List<TermLogEntry> combinedTermLog() {
            List<TermLogEntry> all = new ArrayList<>();
            for (FakeLanNode n : nodes.values()) {
                all.addAll(n.service.termLog());
            }
            return all;
        }

        @Override
        public void close() {
            for (FakeLanNode n : nodes.values()) {
                n.stopTcpEndpoint();
            }
        }
    }

    private record PendingVote(
            String from,
            String to,
            RequestVoteRequestDto request,
            CandidateElection.VoteReply reply) {}

    private static final class FakeLanNode {
        final String nodeId;
        final FakeLanTestHarness harness;
        final RaftState state = new RaftState();
        final InMemoryDurableStorage storage = new InMemoryDurableStorage();
        final PersistentRaftNode coreNode;
        final RaftElectionService service;
        private TcpServer clientServer;
        private int lastPort = -1;

        FakeLanNode(String nodeId, FakeLanTestHarness harness) throws RaftStorageException {
            this.nodeId = nodeId;
            this.harness = harness;
            this.coreNode = new PersistentRaftNode(nodeId, state, storage);
            this.service = new RaftElectionService(
                    nodeId,
                    coreNode,
                    state,
                    storage,
                    (peer, req, reply) -> harness.enqueueVote(nodeId, peer, req, reply),
                    null,
                    () -> {},
                    () -> {});
            this.service.setClock(Clock.fixed(BASE_TIME, ZoneOffset.UTC));
        }

        void startTcpEndpoint() throws IOException {
            if (clientServer != null && clientServer.isRunning()) {
                return;
            }
            HandlerRegistry registry = new HandlerRegistry();
            NodeStatusHandler statusHandler = new NodeStatusHandler(nodeId, () -> {
                Map<String, Boolean> peerConn = new LinkedHashMap<>();
                for (String peer : VOTERS) {
                    if (!peer.equals(nodeId)) {
                        boolean reachable = !harness.stoppedNodes.contains(peer)
                                && !service.isolatedPeers().contains(peer);
                        peerConn.put(peer, reachable);
                    }
                }
                return new NodeStatusHandler.StatusSnapshot(
                        state.role() != null ? state.role().name() : null,
                        state.currentTerm(),
                        state.role() == Role.LEADER ? nodeId : state.leaderId(),
                        state.lastLogIndex(),
                        state.commitIndex(),
                        state.lastApplied(),
                        true,
                        peerConn);
            });
            registry.registerStatusHandler(statusHandler);

            // Enforce SPEC-08 / TC-TCP-04 CLIENT channel isolation on fake LAN client port
            MessageDispatcher dispatcher = new MessageDispatcher(
                    registry, MessageDispatcher.ChannelType.CLIENT, nodeId);

            clientServer = new TcpServer("127.0.0.1", 0, 4, 2000);
            clientServer.setMessageHandler(dispatcher);
            clientServer.start();
            lastPort = clientServer.getPort();
        }

        void stopTcpEndpoint() {
            if (clientServer != null) {
                clientServer.stop();
                clientServer = null;
            }
        }

        int clientTcpPort() {
            return clientServer != null && clientServer.isRunning() ? clientServer.getPort() : lastPort;
        }
    }

    private static final class InMemoryDurableStorage implements RaftStorage {
        long currentTerm = 0L;
        String votedFor = null;
        final List<RaftLogEntry> log = new ArrayList<>();

        @Override
        public PersistentState load() {
            return new PersistentState(currentTerm, votedFor, List.copyOf(log), 0L);
        }

        @Override
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (term < this.currentTerm) {
                throw new RaftStorageException("term regression: " + term + " < " + this.currentTerm);
            }
            this.currentTerm = term;
            this.votedFor = vote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            log.removeIf(e -> e.index() >= fromIndex);
            log.addAll(entries);
        }

        @Override
        public long loadLastApplied() {
            return 0L;
        }
    }
}
