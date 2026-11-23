package vn.nhom6.railway.tests;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.networking.AppendEntriesRpcHandler;
import vn.nhom6.railway.networking.FakeBookingService;
import vn.nhom6.railway.networking.FakeRaftRequestHandler;
import vn.nhom6.railway.networking.MessageDispatcher;
import vn.nhom6.railway.networking.NodeConfig;
import vn.nhom6.railway.networking.NodeMain;
import vn.nhom6.railway.networking.RaftRpcClient;
import vn.nhom6.railway.networking.RequestVoteRpcHandler;
import vn.nhom6.railway.networking.TcpClient;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV6-D22: 3–2 Network Partition Test Harness over Real 5-Node Loopback TCP Cluster.
 *
 * <p><strong>Scope &amp; Guarantees (TV6-D22, SPEC-03, SPEC-07 TC-AC09, SPEC-08 §7):</strong>
 * <ul>
 *   <li>Uses the TV3-D21 production filter hooks ({@link MessageDispatcher.MessageFilterHook} on
 *       peer dispatchers and {@link RaftRpcClient.RpcFilterHook} on peer RPC clients) to enforce
 *       peer-ID TCP partition boundaries without terminating processes or closing server sockets.</li>
 *   <li><strong>Partition != Crash:</strong> All 5 {@link NodeMain} processes remain alive
 *       ({@code isRunning() == true}) with their client TCP ports open and answering
 *       {@code PING}/{@code NODE_STATUS}/client requests on <strong>both</strong> sides of the
 *       3–2 partition at the same time.</li>
 *   <li><strong>Heal &amp; Recovery:</strong> Healing clears all 12 blocked cross-partition
 *       directed links without restarting any process, immediately restoring 20/20 full-mesh
 *       peer TCP connectivity and collecting responses from both previously partitioned sides.</li>
 *   <li><strong>Independent of TV5-D22:</strong> Validates the controlled TCP partition/heal
 *       harness and dual-side response collection independently; full Raft consensus &amp;
 *       PostgreSQL AC09 safety is verified at Gate G6.</li>
 * </ul>
 */
@DisplayName("TV6-D22: 3–2 Network Partition Harness (Live 5-Node Loopback TCP, Processes Alive)")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class PartitionHarness {

    public static final List<String> ALL_NODES = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Duration RPC_TIMEOUT = Duration.ofMillis(450);

    private ClusterFixture cluster;

    @BeforeEach
    void setUp() throws IOException {
        cluster = new ClusterFixture();
        cluster.start();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Baseline Full Mesh (20/20 Directed Peer Links + 5/5 Client NODE_STATUS)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Baseline trước phân vùng: 5/5 NodeMain sống, 20/20 đường TCP peer thông và 5/5 NODE_STATUS thành công")
    void baselineFullMesh_allFiveNodesAlive_peerRpcsAndClientResponsesSucceed() throws Exception {
        cluster.assertAllProcessesAlive();

        Map<String, NodeStatusDto> statuses = cluster.probeAllNodeStatuses();
        assertEquals(5, statuses.size());
        for (String nodeId : ALL_NODES) {
            NodeStatusDto status = statuses.get(nodeId);
            assertNotNull(status);
            assertEquals(nodeId, status.getNodeId());
            assertTrue(status.isStorageHealthy(), "Node " + nodeId + " phải khỏe trước phân vùng");
        }

        int succeededLinks = 0;
        for (String from : ALL_NODES) {
            for (String to : ALL_NODES) {
                if (from.equals(to)) continue;
                MessageEnvelope<AppendEntriesResponseDto> resp = cluster.sendHeartbeat(from, to, 1L);
                assertNotNull(resp);
                assertEquals(to, resp.getSenderId());
                assertTrue(resp.getPayload().isSuccess());
                succeededLinks++;
            }
        }
        assertEquals(20, succeededLinks, "Cụm 5 node full-mesh phải thông đủ 20/20 directed TCP links");
        log("BASELINE_PASS nodes=5/5 peerLinks=20/20 statusProbes=5/5");
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: 3–2 Partition Blocks Exactly 12 Cross Links While Keeping All 5 Processes Alive
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Phân vùng 3–2: Chặn đúng 12 đường TCP chéo, giữ sống 5/5 process (khác crash), nội bộ 2 phía thông 100%")
    void partition3vs2_blocksTwelveCrossLinks_whileIntraPartitionTrafficAndAllFiveProcessesStayAlive() throws Exception {
        Set<String> majority = Set.of("node-1", "node-2", "node-3");
        Set<String> minority = Set.of("node-4", "node-5");

        cluster.partition3vs2(majority, minority);

        // 1. Exactly 3 * 2 * 2 = 12 directed links blocked
        assertEquals(12, cluster.filter().getBlockedLinks().size(),
                "Phân vùng 3–2 phải chặn đúng 12 directed links giữa hai phía");

        // 2. All 5 processes remain ALIVE and answer client-port PING + NODE_STATUS (NOT crashed!)
        cluster.assertAllProcessesAlive();
        for (String nodeId : ALL_NODES) {
            assertEquals("PONG", cluster.sendPing(nodeId),
                    "Client port của " + nodeId + " phải tiếp tục trả PONG khi đang phân vùng");
        }
        Map<String, NodeStatusDto> duringPartitionStatuses = cluster.probeAllNodeStatuses();
        assertEquals(5, duringPartitionStatuses.size(),
                "Cả 5 node (3 đa số + 2 thiểu số) phải tiếp tục trả NODE_STATUS khi bị phân vùng");

        // 3. Intra-majority (6 directed links) & intra-minority (2 directed links) TCP RPCs succeed
        for (String from : majority) {
            for (String to : majority) {
                if (from.equals(to)) continue;
                MessageEnvelope<AppendEntriesResponseDto> resp = cluster.sendHeartbeat(from, to, 2L);
                assertTrue(resp.getPayload().isSuccess(), "Intra-majority " + from + "->" + to + " phải thành công");
            }
        }
        assertTrue(cluster.sendHeartbeat("node-4", "node-5", 2L).getPayload().isSuccess());
        assertTrue(cluster.sendHeartbeat("node-5", "node-4", 2L).getPayload().isSuccess());

        // 4. Cross-partition TCP RPCs (both directions) are dropped and time out cleanly
        cluster.assertPeerRpcDropped("node-1", "node-4", 2L);
        cluster.assertPeerRpcDropped("node-3", "node-5", 2L);
        cluster.assertPeerRpcDropped("node-4", "node-1", 2L);
        cluster.assertPeerRpcDropped("node-5", "node-2", 2L);

        assertTrue(cluster.filter().getDroppedCount() >= 4, "Filter phải ghi nhận các gói xuyên phân vùng bị drop");
        // Processes STILL alive after cross-partition timeouts!
        cluster.assertAllProcessesAlive();
        log("PARTITION_3_2_LIVENESS_PASS blockedLinks=12 processesAlive=5/5 dropped=" + cluster.filter().getDroppedCount());
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Collect Responses From BOTH Sides During 3–2 Partition -> Heal -> Full Convergence
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Chia 3–2, thu response đồng thời từ CẢ 2 PHÍA (đa số 3/5 & thiểu số 2/5), rồi heal và khôi phục 20/20 link")
    void partition3vs2_collectsResponsesFromBothSidesSimultaneously_thenHealsAndRestoresFullMesh() throws Exception {
        Set<String> majority = Set.of("node-1", "node-2", "node-3");
        Set<String> minority = Set.of("node-4", "node-5");

        // Configure roles to simulate split behavior on both sides without crashing any node:
        // Majority side: node-1 accepts leader requests; node-2/node-3 redirect with leaderHint=node-1
        cluster.bookingService("node-1").setLeader(true, "node-1");
        cluster.bookingService("node-2").setLeader(false, "node-1");
        cluster.bookingService("node-3").setLeader(false, "node-1");
        // Minority side: node-4 (stale leader without quorum -> returns UNKNOWN), node-5 (follower -> NOT_LEADER)
        cluster.bookingService("node-4").setLeader(true, "node-4");
        cluster.bookingService("node-4").setSimulateUnknown(true);
        cluster.bookingService("node-5").setLeader(false, "node-4");

        // Snapshot ports before partition to prove processes are NOT restarted during heal
        Map<String, Integer> clientPortsBefore = new LinkedHashMap<>(cluster.clientPorts);
        Map<String, Integer> raftPortsBefore = new LinkedHashMap<>(cluster.raftPorts);

        // Step A: Apply 3–2 partition
        cluster.partition3vs2(majority, minority);

        // Step B: Collect real TCP responses from BOTH sides while partitioned
        // --- Majority side (3/5) responses ---
        NodeStatusDto statusNode1 = cluster.probeNodeStatus("node-1");
        assertEquals("node-1", statusNode1.getNodeId());
        assertTrue(statusNode1.isStorageHealthy());

        GetSeatsResponseDto seatsMajority = cluster.sendGetSeats("node-1", "SE-001");
        assertEquals(10, seatsMajority.getSeats().size());

        String reqMajority = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> bookMajorityResp = cluster.sendBookTicket("node-1", reqMajority, "SE-001", "G05", "Majority-Alice");
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookMajorityResp.getType());
        BookTicketResponseDto majorityBookDto = MessageCodec.decodePayload(bookMajorityResp, BookTicketResponseDto.class);
        assertEquals("CONFIRMED", majorityBookDto.getOutcome());
        assertEquals(reqMajority, majorityBookDto.getRequestId());

        MessageEnvelope<JsonNode> followerMajorityResp = cluster.sendBookTicket(
                "node-2", UUID.randomUUID().toString(), "SE-001", "G06", "Redirect-Test");
        assertEquals(MessageType.ERROR_RESPONSE, followerMajorityResp.getType());
        ErrorResponseDto majorityRedirect = MessageCodec.decodePayload(followerMajorityResp, ErrorResponseDto.class);
        assertEquals("NOT_LEADER", majorityRedirect.getErrorCode());
        assertEquals("node-1", majorityRedirect.getLeaderHint());

        // --- Minority side (2/5) responses (proves minority nodes are ALIVE & reachable by clients!) ---
        NodeStatusDto statusNode4 = cluster.probeNodeStatus("node-4");
        NodeStatusDto statusNode5 = cluster.probeNodeStatus("node-5");
        assertEquals("node-4", statusNode4.getNodeId());
        assertEquals("node-5", statusNode5.getNodeId());
        assertTrue(statusNode4.isStorageHealthy());
        assertTrue(statusNode5.isStorageHealthy());

        String reqMinority = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> bookMinorityResp = cluster.sendBookTicket("node-4", reqMinority, "SE-001", "G05", "Minority-Bob");
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookMinorityResp.getType());
        BookTicketResponseDto minorityBookDto = MessageCodec.decodePayload(bookMinorityResp, BookTicketResponseDto.class);
        assertEquals("UNKNOWN", minorityBookDto.getOutcome(),
                "Phía thiểu số 2/5 phải trả response kiểm soát (UNKNOWN, không CONFIRMED)");

        MessageEnvelope<JsonNode> followerMinorityResp = cluster.sendBookTicket(
                "node-5", UUID.randomUUID().toString(), "SE-001", "G07", "Minority-Follower");
        assertEquals(MessageType.ERROR_RESPONSE, followerMinorityResp.getType());
        ErrorResponseDto minorityRedirect = MessageCodec.decodePayload(followerMinorityResp, ErrorResponseDto.class);
        assertEquals("NOT_LEADER", minorityRedirect.getErrorCode());
        assertEquals("node-4", minorityRedirect.getLeaderHint());

        // Cross-partition Raft RPC still blocked before heal
        cluster.assertPeerRpcDropped("node-1", "node-4", 3L);
        cluster.assertPeerRpcDropped("node-4", "node-1", 3L);

        // Step C: Heal partition WITHOUT restarting any process
        long droppedBeforeHeal = cluster.filter().getDroppedCount();
        cluster.heal();
        assertTrue(cluster.filter().getBlockedLinks().isEmpty());
        cluster.assertAllProcessesAlive();
        assertEquals(clientPortsBefore, cluster.clientPorts, "Heal không được khởi động lại tiến trình/đổi port");
        assertEquals(raftPortsBefore, cluster.raftPorts);

        // Step D: Cross-partition peer TCP RPCs immediately succeed in BOTH directions after heal
        MessageEnvelope<AppendEntriesResponseDto> heal1to4 = cluster.sendHeartbeat("node-1", "node-4", 4L);
        MessageEnvelope<AppendEntriesResponseDto> heal1to5 = cluster.sendHeartbeat("node-1", "node-5", 4L);
        MessageEnvelope<RequestVoteResponseDto> heal4to1 = cluster.sendRequestVote("node-4", "node-1", 4L);
        MessageEnvelope<RequestVoteResponseDto> heal5to3 = cluster.sendRequestVote("node-5", "node-3", 4L);

        assertTrue(heal1to4.getPayload().isSuccess());
        assertTrue(heal1to5.getPayload().isSuccess());
        assertTrue(heal4to1.getPayload().isVoteGranted());
        assertTrue(heal5to3.getPayload().isVoteGranted());
        assertEquals(droppedBeforeHeal, cluster.filter().getDroppedCount(),
                "Sau heal không được drop thêm bất kỳ gói tin nào");

        // Step E: Collect responses from all 5 nodes across both previously partitioned sides after heal
        Map<String, NodeStatusDto> postHealStatuses = cluster.probeAllNodeStatuses();
        assertEquals(5, postHealStatuses.size());
        log("PARTITION_AND_HEAL_DUAL_SIDE_PASS majorityOutcome=" + majorityBookDto.getOutcome()
                + " minorityOutcome=" + minorityBookDto.getOutcome()
                + " postHealNodes=5/5 droppedBeforeHeal=" + droppedBeforeHeal);
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Outbound (RaftRpcClient) + Inbound (MessageDispatcher) Hooks & Telemetry Accuracy
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Kiểm chứng filter hook 2 lớp (RaftRpcClient + MessageDispatcher) và telemetry drop/deliver")
    void partition3vs2_selectiveOutboundAndInboundFilterHooks_dropCrossPartitionWithoutKillingSockets() throws Exception {
        cluster.attachOutboundFilterToClients();
        cluster.filter().resetMetrics();

        cluster.partition3vs2(Set.of("node-1", "node-2", "node-3"), Set.of("node-4", "node-5"));

        // Intra-majority delivers
        assertTrue(cluster.sendHeartbeat("node-1", "node-2", 2L).getPayload().isSuccess());
        assertTrue(cluster.filter().getDeliveredCount() > 0);

        // Cross-partition drops at outbound hook before writing wire bytes
        cluster.assertPeerRpcDropped("node-1", "node-4", 2L);
        assertTrue(cluster.filter().getDroppedCount("node-1", "node-4") >= 1);

        // Heal and verify delivery resumes with zero additional drops
        long dropsAtHeal = cluster.filter().getDroppedCount();
        long deliveredAtHeal = cluster.filter().getDeliveredCount();
        cluster.heal();

        assertTrue(cluster.sendHeartbeat("node-1", "node-4", 3L).getPayload().isSuccess());
        assertEquals(dropsAtHeal, cluster.filter().getDroppedCount());
        assertTrue(cluster.filter().getDeliveredCount() > deliveredAtHeal);
        log("DUAL_HOOK_TELEMETRY_PASS drops=" + dropsAtHeal + " deliveredAfterHeal=" + cluster.filter().getDeliveredCount());
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: Asymmetric Unidirectional Link Block Preserves Reverse Direction
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Phân vùng bất đối xứng 1 chiều: Chặn node-1 -> node-4 nhưng chiều ngược node-4 -> node-1 vẫn thông")
    void asymmetricPartition_unidirectionalLinkBlock_preservesReverseDirectionAndProcessLiveness() throws Exception {
        cluster.filter().blockLink("node-1", "node-4");

        // Forward blocked
        cluster.assertPeerRpcDropped("node-1", "node-4", 2L);
        // Reverse still open
        MessageEnvelope<AppendEntriesResponseDto> reverse = cluster.sendHeartbeat("node-4", "node-1", 2L);
        assertTrue(reverse.getPayload().isSuccess());
        // Both processes remain alive
        assertEquals("PONG", cluster.sendPing("node-1"));
        assertEquals("PONG", cluster.sendPing("node-4"));

        // Restore link
        cluster.filter().allowLink("node-1", "node-4");
        assertTrue(cluster.sendHeartbeat("node-1", "node-4", 3L).getPayload().isSuccess());
        log("ASYMMETRIC_LINK_PASS");
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Selective Message Type Drop (Drop REQUEST_VOTE Only on Cross Link)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Lọc theo loại bản tin: Drop riêng REQUEST_VOTE trên node-1 -> node-4, APPEND_ENTRIES & NODE_STATUS vẫn thông")
    void selectiveMessageTypeDrop_blocksRequestVoteAcrossBoundary_whileAppendEntriesAndStatusContinue() throws Exception {
        cluster.filter().dropMessageTypeOnLink("node-1", "node-4", MessageType.REQUEST_VOTE);

        // REQUEST_VOTE on node-1 -> node-4 is dropped
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> blockedVote =
                cluster.rpcClient("node-1").sendRequestVote(
                        "node-4", new RequestVoteRequestDto(2L, "node-1", 1L, 1L), RPC_TIMEOUT);
        assertThrows(Exception.class, () -> blockedVote.get(1500, TimeUnit.MILLISECONDS));

        // APPEND_ENTRIES on the same link succeeds
        assertTrue(cluster.sendHeartbeat("node-1", "node-4", 2L).getPayload().isSuccess());
        // NODE_STATUS on node-4 succeeds
        assertEquals("node-4", cluster.probeNodeStatus("node-4").getNodeId());

        cluster.heal();
        assertTrue(cluster.sendRequestVote("node-1", "node-4", 3L).getPayload().isVoteGranted());
        log("SELECTIVE_TYPE_DROP_PASS");
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Successive Rotating 3–2 Partitions & Heals
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Phân vùng 3–2 xoay vòng 2 chu kỳ ({1,2,3}|{4,5} -> heal -> {1,2}|{3,4,5} -> heal) không rơi process")
    void successivePartitions_rotate3vs2Boundaries_andHealCleanlyEveryCycle() throws Exception {
        // Cycle 1: {1,2,3} vs {4,5}
        cluster.partition3vs2(Set.of("node-1", "node-2", "node-3"), Set.of("node-4", "node-5"));
        assertTrue(cluster.sendHeartbeat("node-1", "node-3", 2L).getPayload().isSuccess());
        assertTrue(cluster.sendHeartbeat("node-4", "node-5", 2L).getPayload().isSuccess());
        cluster.assertPeerRpcDropped("node-3", "node-4", 2L);
        assertEquals(5, cluster.probeAllNodeStatuses().size());

        cluster.heal();
        assertTrue(cluster.sendHeartbeat("node-3", "node-4", 3L).getPayload().isSuccess());

        // Cycle 2: {1,2} vs {3,4,5}
        cluster.partition3vs2(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        assertTrue(cluster.sendHeartbeat("node-1", "node-2", 4L).getPayload().isSuccess());
        assertTrue(cluster.sendHeartbeat("node-3", "node-5", 4L).getPayload().isSuccess());
        cluster.assertPeerRpcDropped("node-2", "node-3", 4L);
        cluster.assertPeerRpcDropped("node-5", "node-1", 4L);
        assertEquals(5, cluster.probeAllNodeStatuses().size());

        cluster.heal();
        // Verify full 20/20 mesh restored
        int restored = 0;
        for (String from : ALL_NODES) {
            for (String to : ALL_NODES) {
                if (from.equals(to)) continue;
                assertTrue(cluster.sendHeartbeat(from, to, 5L).getPayload().isSuccess());
                restored++;
            }
        }
        assertEquals(20, restored);
        cluster.assertAllProcessesAlive();
        log("ROTATING_PARTITIONS_PASS cycles=2 restoredLinks=20/20");
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: TV6-D21 Fault Scripts Presence & Volume-Safety Contract Check (G6 Readiness)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Kiểm tra sẵn sàng Gate G6: Bộ script TV6-D21 (stop-node, restart-node, select-leader) tồn tại và không xóa volume")
    void faultScriptsInventory_tv6D21ScriptsPresentAndExecutableContractVerified() throws Exception {
        Path repoRoot = resolveRepoRoot();
        List<String> requiredScripts = List.of(
                "scripts/stop-node.ps1",
                "scripts/restart-node.ps1",
                "scripts/select-leader.ps1"
        );
        for (String rel : requiredScripts) {
            Path p = repoRoot.resolve(rel);
            assertTrue(Files.isRegularFile(p), "Thiếu script TV6-D21: " + p);
            String content = Files.readString(p, StandardCharsets.UTF_8);
            assertFalse(content.isBlank(), "Script không được rỗng: " + rel);
            // Verify scripts never invoke destructive Docker volume commands
            assertFalse(content.contains("docker compose down -v"), rel + " không được chứa lệnh xóa volume");
            assertFalse(content.contains("docker volume rm"), rel + " không được chứa lệnh xóa volume");
        }
        log("G6_FAULT_SCRIPTS_CONTRACT_PASS scripts=3/3");
    }

    private static Path resolveRepoRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        if (Files.isDirectory(cwd.resolve("scripts"))) {
            return cwd;
        }
        Path parent = cwd.getParent();
        if (parent != null && Files.isDirectory(parent.resolve("scripts"))) {
            return parent;
        }
        throw new IllegalStateException("Cannot locate repository root containing scripts/ from " + cwd);
    }

    private static void log(String message) {
        System.out.println(Instant.now() + " [PartitionHarness] " + message);
    }

    // ===========================================================================================
    // Reusable Live 5-Node TCP Cluster Fixture & Controlled Peer-ID Network Filter
    // ===========================================================================================

    public static final class ClusterFixture implements AutoCloseable {
        private final PeerPartitionFilter filter = new PeerPartitionFilter();
        private final Map<String, NodeMain> nodes = new LinkedHashMap<>();
        private final Map<String, FakeBookingService> bookingServices = new LinkedHashMap<>();
        private final Map<String, FakeRaftRequestHandler> raftHandlers = new LinkedHashMap<>();
        private final Map<String, RaftRpcClient> rpcClients = new LinkedHashMap<>();
        final Map<String, Integer> clientPorts = new LinkedHashMap<>();
        final Map<String, Integer> raftPorts = new LinkedHashMap<>();

        public void start() throws IOException {
            for (String nodeId : ALL_NODES) {
                NodeConfig config = new NodeConfig(
                        nodeId,
                        "railway-partition-harness",
                        "127.0.0.1",
                        0,
                        "127.0.0.1",
                        0,
                        null,
                        32,
                        5_000
                );
                FakeBookingService bookingService = new FakeBookingService(nodeId);
                NodeMain node = new NodeMain(config, bookingService);

                FakeRaftRequestHandler processor = new FakeRaftRequestHandler(1L);
                node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(processor, nodeId));
                node.getRaftRegistry().register(MessageType.APPEND_ENTRIES, new AppendEntriesRpcHandler(processor, nodeId));

                // Attach peer-ID partition filter ONLY to Raft peer dispatcher so client ports remain reachable
                node.getRaftDispatcher().setFilterHook(filter);

                node.start();
                nodes.put(nodeId, node);
                bookingServices.put(nodeId, bookingService);
                raftHandlers.put(nodeId, processor);
                clientPorts.put(nodeId, node.getClientPort());
                raftPorts.put(nodeId, node.getRaftPort());
            }

            for (String localId : ALL_NODES) {
                Map<String, InetSocketAddress> endpoints = new LinkedHashMap<>();
                for (String peerId : ALL_NODES) {
                    if (!peerId.equals(localId)) {
                        endpoints.put(peerId, new InetSocketAddress("127.0.0.1", raftPorts.get(peerId)));
                    }
                }
                RaftRpcClient client = new RaftRpcClient(localId, endpoints, 32, 1_500, RPC_TIMEOUT);
                rpcClients.put(localId, client);
            }
        }

        public PeerPartitionFilter filter() {
            return filter;
        }

        public FakeBookingService bookingService(String nodeId) {
            return bookingServices.get(nodeId);
        }

        public RaftRpcClient rpcClient(String nodeId) {
            return rpcClients.get(nodeId);
        }

        public void attachOutboundFilterToClients() {
            rpcClients.values().forEach(c -> c.setFilterHook(filter));
        }

        public void partition3vs2(Set<String> sideA, Set<String> sideB) {
            assertEquals(5, sideA.size() + sideB.size(), "Phân vùng 3–2 phải phủ đủ 5 node");
            assertTrue((sideA.size() == 3 && sideB.size() == 2) || (sideA.size() == 2 && sideB.size() == 3),
                    "Phải chia đúng tỷ lệ 3–2");
            filter.partition(sideA, sideB);
        }

        public void heal() {
            filter.heal();
        }

        public void assertAllProcessesAlive() {
            for (String nodeId : ALL_NODES) {
                NodeMain node = nodes.get(nodeId);
                assertNotNull(node, "Node " + nodeId + " không tồn tại");
                assertTrue(node.isRunning(), "Node " + nodeId + " phải còn sống (partition != crash)");
                assertTrue(node.getClientPort() > 0 && node.getRaftPort() > 0);
            }
        }

        public String sendPing(String nodeId) throws IOException {
            try (TcpClient client = new TcpClient("127.0.0.1", clientPorts.get(nodeId), 1_500, 2_000)) {
                byte[] resp = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
                return new String(resp, StandardCharsets.UTF_8);
            }
        }

        public NodeStatusDto probeNodeStatus(String nodeId) throws IOException {
            try (TcpClient client = new TcpClient("127.0.0.1", clientPorts.get(nodeId), 1_500, 2_000)) {
                MessageEnvelope<Map<String, Object>> req = MessageEnvelope.createRequest(
                        MessageType.NODE_STATUS, "monitor-partition-harness", Map.of());
                MessageEnvelope<JsonNode> resp = client.sendAndReceive(req);
                assertEquals(MessageType.NODE_STATUS_RESPONSE, resp.getType());
                assertEquals(req.getMessageId(), resp.getCorrelationId());
                assertEquals(nodeId, resp.getSenderId());
                NodeStatusDto dto = MessageCodec.decodePayload(resp, NodeStatusDto.class);
                dto.validate();
                return dto;
            }
        }

        public Map<String, NodeStatusDto> probeAllNodeStatuses() throws IOException {
            Map<String, NodeStatusDto> map = new LinkedHashMap<>();
            for (String nodeId : ALL_NODES) {
                map.put(nodeId, probeNodeStatus(nodeId));
            }
            return map;
        }

        public GetSeatsResponseDto sendGetSeats(String nodeId, String tripId) throws IOException {
            try (TcpClient client = new TcpClient("127.0.0.1", clientPorts.get(nodeId), 1_500, 2_000)) {
                MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                        MessageType.GET_SEATS, "client-partition-harness", new GetSeatsRequestDto(tripId));
                MessageEnvelope<JsonNode> resp = client.sendAndReceive(req);
                assertEquals(MessageType.GET_SEATS_RESPONSE, resp.getType());
                return MessageCodec.decodePayload(resp, GetSeatsResponseDto.class);
            }
        }

        public MessageEnvelope<JsonNode> sendBookTicket(
                String nodeId, String requestId, String tripId, String seatId, String passengerName) throws IOException {
            try (TcpClient client = new TcpClient("127.0.0.1", clientPorts.get(nodeId), 1_500, 2_000)) {
                BookTicketRequestDto dto = new BookTicketRequestDto(requestId, tripId, seatId, passengerName);
                MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                        MessageType.BOOK_TICKET, "client-partition-harness", dto);
                return client.sendAndReceive(req);
            }
        }

        public MessageEnvelope<AppendEntriesResponseDto> sendHeartbeat(String from, String to, long term) throws Exception {
            AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(term, from, 0L, 0L, 0L);
            return rpcClients.get(from).sendAppendEntries(to, heartbeat, RPC_TIMEOUT).get(2, TimeUnit.SECONDS);
        }

        public MessageEnvelope<RequestVoteResponseDto> sendRequestVote(String from, String to, long term) throws Exception {
            RequestVoteRequestDto vote = new RequestVoteRequestDto(term, from, 0L, 0L);
            return rpcClients.get(from).sendRequestVote(to, vote, RPC_TIMEOUT).get(2, TimeUnit.SECONDS);
        }

        public void assertPeerRpcDropped(String from, String to, long term) {
            AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(term, from, 0L, 0L, 0L);
            CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> future =
                    rpcClients.get(from).sendAppendEntries(to, heartbeat, RPC_TIMEOUT);
            Exception ex = assertThrows(Exception.class, () -> future.get(1500, TimeUnit.MILLISECONDS),
                    "RPC xuyên phân vùng " + from + " -> " + to + " phải bị chặn và timeout");
            assertTrue(ex instanceof TimeoutException || ex instanceof ExecutionException,
                    "Kỳ vọng TimeoutException hoặc ExecutionException nhưng nhận " + ex.getClass().getSimpleName());
        }

        @Override
        public void close() {
            filter.heal();
            for (RaftRpcClient client : rpcClients.values()) {
                client.setFilterHook(null);
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            }
            rpcClients.clear();
            for (NodeMain node : nodes.values()) {
                node.getRaftDispatcher().setFilterHook(null);
                try {
                    node.stop();
                } catch (Exception ignored) {
                }
            }
            nodes.clear();
            bookingServices.clear();
            raftHandlers.clear();
            clientPorts.clear();
            raftPorts.clear();
        }
    }

    /**
     * Controlled peer-ID network filter implementing TV3-D21's {@link MessageDispatcher.MessageFilterHook}
     * and {@link RaftRpcClient.RpcFilterHook} interfaces.
     */
    public static final class PeerPartitionFilter
            implements MessageDispatcher.MessageFilterHook, RaftRpcClient.RpcFilterHook {

        public record DirectedLink(String from, String to) {
            public DirectedLink {
                Objects.requireNonNull(from, "from");
                Objects.requireNonNull(to, "to");
            }
        }

        private final Set<DirectedLink> blockedLinks = ConcurrentHashMap.newKeySet();
        private final Set<String> isolatedNodes = ConcurrentHashMap.newKeySet();
        private final List<BiPredicate<DirectedLink, MessageEnvelope<?>>> customPredicates = new CopyOnWriteArrayList<>();
        private final AtomicLong totalDropped = new AtomicLong(0);
        private final AtomicLong totalDelivered = new AtomicLong(0);
        private final ConcurrentMap<DirectedLink, AtomicLong> droppedByLink = new ConcurrentHashMap<>();

        @Override
        public boolean shouldDeliver(String from, String to, MessageEnvelope<?> message) {
            if (from == null || to == null) {
                totalDelivered.incrementAndGet();
                return true;
            }
            DirectedLink link = new DirectedLink(from, to);
            if (isolatedNodes.contains(from) || isolatedNodes.contains(to)) {
                recordDrop(link);
                return false;
            }
            if (blockedLinks.contains(link)) {
                recordDrop(link);
                return false;
            }
            for (BiPredicate<DirectedLink, MessageEnvelope<?>> p : customPredicates) {
                if (p.test(link, message)) {
                    recordDrop(link);
                    return false;
                }
            }
            totalDelivered.incrementAndGet();
            return true;
        }

        private void recordDrop(DirectedLink link) {
            totalDropped.incrementAndGet();
            droppedByLink.computeIfAbsent(link, k -> new AtomicLong(0)).incrementAndGet();
        }

        public void partition(Set<String> partA, Set<String> partB) {
            for (String a : partA) {
                for (String b : partB) {
                    blockLink(a, b);
                    blockLink(b, a);
                }
            }
        }

        public void blockLink(String from, String to) {
            blockedLinks.add(new DirectedLink(from, to));
        }

        public void allowLink(String from, String to) {
            blockedLinks.remove(new DirectedLink(from, to));
        }

        public void isolate(String nodeId) {
            isolatedNodes.add(nodeId);
        }

        public void deisolate(String nodeId) {
            isolatedNodes.remove(nodeId);
        }

        public void dropMessageTypeOnLink(String from, String to, MessageType type) {
            DirectedLink target = new DirectedLink(from, to);
            customPredicates.add((link, msg) -> link.equals(target) && msg != null && msg.getType() == type);
        }

        public void heal() {
            blockedLinks.clear();
            isolatedNodes.clear();
            customPredicates.clear();
        }

        public Set<DirectedLink> getBlockedLinks() {
            return Collections.unmodifiableSet(blockedLinks);
        }

        public long getDroppedCount() {
            return totalDropped.get();
        }

        public long getDeliveredCount() {
            return totalDelivered.get();
        }

        public long getDroppedCount(String from, String to) {
            AtomicLong c = droppedByLink.get(new DirectedLink(from, to));
            return c != null ? c.get() : 0L;
        }

        public void resetMetrics() {
            totalDropped.set(0);
            totalDelivered.set(0);
            droppedByLink.clear();
        }
    }
}
