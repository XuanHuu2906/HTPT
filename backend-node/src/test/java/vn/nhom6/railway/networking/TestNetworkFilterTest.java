package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TestNetworkFilter Network Partition & Fault Simulation Tests (TV3-D21)")
public class TestNetworkFilterTest {

    private static final Logger LOGGER = Logger.getLogger(TestNetworkFilterTest.class.getName());
    private static final List<String> CLUSTER_NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final Map<String, NodeMain> nodes = new LinkedHashMap<>();
    private final Map<String, FakeRaftRequestHandler> fakeProcessors = new LinkedHashMap<>();
    private final Map<String, RaftRpcClient> rpcClients = new LinkedHashMap<>();
    private final Map<String, Integer> clientPorts = new LinkedHashMap<>();
    private final Map<String, Integer> raftPorts = new LinkedHashMap<>();

    private TestNetworkFilter filter;

    @BeforeEach
    void setupCluster() throws IOException {
        filter = new TestNetworkFilter();

        // 1. Initialize 5 nodes on ephemeral ports (port 0)
        for (String nodeId : CLUSTER_NODE_IDS) {
            NodeConfig config = new NodeConfig(
                    nodeId,
                    "railway-demo-cluster",
                    "127.0.0.1",
                    0,
                    "127.0.0.1",
                    0,
                    null,
                    32,
                    15_000
            );

            FakeBookingService bookingService = new FakeBookingService(nodeId);
            NodeMain node = new NodeMain(config, bookingService);

            FakeRaftRequestHandler processor = new FakeRaftRequestHandler(1L);
            fakeProcessors.put(nodeId, processor);
            node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(processor, nodeId));
            node.getRaftRegistry().register(MessageType.APPEND_ENTRIES, new AppendEntriesRpcHandler(processor, nodeId));

            node.start();
            nodes.put(nodeId, node);
            clientPorts.put(nodeId, node.getClientPort());
            raftPorts.put(nodeId, node.getRaftPort());
        }

        // 2. Setup full-mesh RaftRpcClient instances with 1.5s default timeout
        for (String localId : CLUSTER_NODE_IDS) {
            Map<String, InetSocketAddress> peerEndpoints = new LinkedHashMap<>();
            for (String peerId : CLUSTER_NODE_IDS) {
                if (!peerId.equals(localId)) {
                    peerEndpoints.put(peerId, new InetSocketAddress("127.0.0.1", raftPorts.get(peerId)));
                }
            }
            RaftRpcClient client = new RaftRpcClient(localId, peerEndpoints, 32, 3000, Duration.ofMillis(1200));
            rpcClients.put(localId, client);
        }

        // 3. Attach filter to all dispatchers in cluster
        filter.attachToCluster(nodes);
    }

    @AfterEach
    void tearDownCluster() {
        if (filter != null) {
            filter.detachFromCluster(nodes);
            filter.detachFromClients(rpcClients);
            filter.heal();
        }

        for (RaftRpcClient client : rpcClients.values()) {
            try {
                client.close();
            } catch (Exception ignored) {}
        }
        rpcClients.clear();

        for (NodeMain node : nodes.values()) {
            try {
                node.stop();
            } catch (Exception ignored) {}
        }
        nodes.clear();
        fakeProcessors.clear();
        clientPorts.clear();
        raftPorts.clear();
    }

    @Test
    @DisplayName("Majority/Minority Partition: 3 vs 2 partition enforces network boundaries while keeping processes alive")
    void testMajorityMinorityPartitionOverTcpCluster() throws Exception {
        Set<String> majority = Set.of("node-1", "node-2", "node-3");
        Set<String> minority = Set.of("node-4", "node-5");

        filter.partition(majority, minority);

        // A. Intra-majority communication must succeed 100%
        RequestVoteRequestDto vote1to2 = new RequestVoteRequestDto(2L, "node-1", 10L, 2L);
        MessageEnvelope<RequestVoteResponseDto> resp1to2 = rpcClients.get("node-1")
                .sendRequestVote("node-2", vote1to2, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(resp1to2);
        assertEquals("node-2", resp1to2.getSenderId());
        assertTrue(resp1to2.getPayload().isVoteGranted());

        RequestVoteRequestDto vote2to3 = new RequestVoteRequestDto(2L, "node-2", 10L, 2L);
        MessageEnvelope<RequestVoteResponseDto> resp2to3 = rpcClients.get("node-2")
                .sendRequestVote("node-3", vote2to3, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(resp2to3);
        assertEquals("node-3", resp2to3.getSenderId());
        assertTrue(resp2to3.getPayload().isVoteGranted());

        // B. Intra-minority communication must succeed 100%
        RequestVoteRequestDto vote4to5 = new RequestVoteRequestDto(2L, "node-4", 10L, 2L);
        MessageEnvelope<RequestVoteResponseDto> resp4to5 = rpcClients.get("node-4")
                .sendRequestVote("node-5", vote4to5, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(resp4to5);
        assertEquals("node-5", resp4to5.getSenderId());
        assertTrue(resp4to5.getPayload().isVoteGranted());

        // C. Cross-partition communication must be dropped and time out
        RequestVoteRequestDto vote1to4 = new RequestVoteRequestDto(2L, "node-1", 10L, 2L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> futureCross1 =
                rpcClients.get("node-1").sendRequestVote("node-4", vote1to4, Duration.ofMillis(600));

        assertThrows(ExecutionException.class, () -> futureCross1.get(2, TimeUnit.SECONDS),
                "RPC from node-1 to node-4 must fail due to partition drop");

        RequestVoteRequestDto vote5to2 = new RequestVoteRequestDto(2L, "node-5", 10L, 2L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> futureCross2 =
                rpcClients.get("node-5").sendRequestVote("node-2", vote5to2, Duration.ofMillis(600));

        assertThrows(ExecutionException.class, () -> futureCross2.get(2, TimeUnit.SECONDS),
                "RPC from node-5 to node-2 must fail due to partition drop");

        // D. Verify dropped telemetry
        assertTrue(filter.getDroppedCount() >= 2, "Dropped count must be >= 2");
        assertTrue(filter.getDroppedCount("node-1", "node-4") >= 1, "node-1 -> node-4 drop recorded");
        assertTrue(filter.getDroppedCount("node-5", "node-2") >= 1, "node-5 -> node-2 drop recorded");

        // E. Verify all 5 node processes are still 100% alive
        for (String id : CLUSTER_NODE_IDS) {
            assertTrue(nodes.get(id).isRunning(), "Node " + id + " must remain running during partition");
        }
    }

    @Test
    @DisplayName("Partition Healing: heal() clears drop rules and immediately restores full-mesh communication")
    void testPartitionHealingRestoresCommunication() throws Exception {
        Set<String> majority = Set.of("node-1", "node-2", "node-3");
        Set<String> minority = Set.of("node-4", "node-5");

        filter.partition(majority, minority);

        // Verify link is initially blocked
        RequestVoteRequestDto req = new RequestVoteRequestDto(3L, "node-1", 10L, 3L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> blockedFuture =
                rpcClients.get("node-1").sendRequestVote("node-4", req, Duration.ofMillis(500));
        assertThrows(ExecutionException.class, () -> blockedFuture.get(2, TimeUnit.SECONDS));

        // Heal partition
        filter.heal();
        assertEquals(0, filter.getBlockedLinks().size(), "All blocked links must be cleared after heal");
        assertEquals(0, filter.getIsolatedNodes().size(), "All isolated nodes must be cleared after heal");

        // Immediately verify cross-partition RPC succeeds
        RequestVoteRequestDto healedReq = new RequestVoteRequestDto(3L, "node-1", 10L, 3L);
        MessageEnvelope<RequestVoteResponseDto> respHealed = rpcClients.get("node-1")
                .sendRequestVote("node-4", healedReq, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);

        assertNotNull(respHealed);
        assertEquals("node-4", respHealed.getSenderId());
        assertTrue(respHealed.getPayload().isVoteGranted());
    }

    @Test
    @DisplayName("Single Node Isolation: isolate() drops all inbound & outbound messages, other 4 nodes communicate")
    void testSingleNodeIsolation() throws Exception {
        filter.isolate("node-3");
        assertTrue(filter.getIsolatedNodes().contains("node-3"));

        // Healthy nodes (1, 2, 4, 5) communicate normally
        AppendEntriesRequestDto heartbeat = new AppendEntriesRequestDto(1L, "node-1", 0L, 0L, List.of(), 0L);
        MessageEnvelope<AppendEntriesResponseDto> healthyResp = rpcClients.get("node-1")
                .sendAppendEntries("node-2", heartbeat, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(healthyResp);
        assertTrue(healthyResp.getPayload().isSuccess());

        // Inbound to node-3 is dropped
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> inToIsolated =
                rpcClients.get("node-1").sendAppendEntries("node-3", heartbeat, Duration.ofMillis(500));
        assertThrows(ExecutionException.class, () -> inToIsolated.get(2, TimeUnit.SECONDS));

        // Outbound from node-3 is dropped
        AppendEntriesRequestDto outHeartbeat = new AppendEntriesRequestDto(1L, "node-3", 0L, 0L, List.of(), 0L);
        CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> outFromIsolated =
                rpcClients.get("node-3").sendAppendEntries("node-1", outHeartbeat, Duration.ofMillis(500));
        assertThrows(ExecutionException.class, () -> outFromIsolated.get(2, TimeUnit.SECONDS));

        // De-isolate node-3
        filter.deisolate("node-3");
        assertFalse(filter.getIsolatedNodes().contains("node-3"));

        MessageEnvelope<AppendEntriesResponseDto> restoredResp = rpcClients.get("node-1")
                .sendAppendEntries("node-3", heartbeat, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(restoredResp);
        assertTrue(restoredResp.getPayload().isSuccess());
    }

    @Test
    @DisplayName("Asymmetric Link Drop: Simulates unidirectional packet loss (A->B dropped, B->A succeeds)")
    void testAsymmetricUnidirectionalLinkDrop() throws Exception {
        filter.blockLink("node-1", "node-2");

        // node-1 -> node-2 is dropped
        RequestVoteRequestDto vote1to2 = new RequestVoteRequestDto(5L, "node-1", 10L, 5L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> droppedFuture =
                rpcClients.get("node-1").sendRequestVote("node-2", vote1to2, Duration.ofMillis(500));
        assertThrows(ExecutionException.class, () -> droppedFuture.get(2, TimeUnit.SECONDS));

        // node-2 -> node-1 SUCCEEDS
        RequestVoteRequestDto vote2to1 = new RequestVoteRequestDto(5L, "node-2", 10L, 5L);
        MessageEnvelope<RequestVoteResponseDto> successResp = rpcClients.get("node-2")
                .sendRequestVote("node-1", vote2to1, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(successResp);
        assertEquals("node-1", successResp.getSenderId());
        assertTrue(successResp.getPayload().isVoteGranted());

        assertEquals(1, filter.getDroppedCount("node-1", "node-2"));
        assertEquals(0, filter.getDroppedCount("node-2", "node-1"));
    }

    @Test
    @DisplayName("Selective RPC Type Drop: Drops REQUEST_VOTE but permits APPEND_ENTRIES heartbeats")
    void testSelectiveRpcTypeDrop() throws Exception {
        filter.dropMessageTypeOnLink("node-1", "node-2", MessageType.REQUEST_VOTE);

        // REQUEST_VOTE is dropped
        RequestVoteRequestDto voteReq = new RequestVoteRequestDto(6L, "node-1", 10L, 6L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> voteFuture =
                rpcClients.get("node-1").sendRequestVote("node-2", voteReq, Duration.ofMillis(500));
        assertThrows(ExecutionException.class, () -> voteFuture.get(2, TimeUnit.SECONDS));

        // APPEND_ENTRIES succeeds on the exact same link
        AppendEntriesRequestDto appendReq = new AppendEntriesRequestDto(6L, "node-1", 0L, 0L, List.of(), 0L);
        MessageEnvelope<AppendEntriesResponseDto> appendResp = rpcClients.get("node-1")
                .sendAppendEntries("node-2", appendReq, Duration.ofSeconds(2))
                .get(3, TimeUnit.SECONDS);
        assertNotNull(appendResp);
        assertTrue(appendResp.getPayload().isSuccess());
    }

    @Test
    @DisplayName("Process Lifecycle Invariant: All 5 nodes and 10 ports stay alive and bound during partition")
    void testAllProcessesStayAliveDuringPartition() {
        // Enforce split partition
        filter.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        for (String nodeId : CLUSTER_NODE_IDS) {
            NodeMain node = nodes.get(nodeId);
            assertNotNull(node);
            assertTrue(node.isRunning(), "Node " + nodeId + " must remain running");

            int clientPort = clientPorts.get(nodeId);
            int raftPort = raftPorts.get(nodeId);

            // Verify TCP sockets are still bound and accepting connections
            assertDoesNotThrow(() -> {
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress("127.0.0.1", clientPort), 500);
                    assertTrue(s.isConnected(), "Client socket on port " + clientPort + " must be connectable");
                }
            });

            assertDoesNotThrow(() -> {
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress("127.0.0.1", raftPort), 500);
                    assertTrue(s.isConnected(), "Raft socket on port " + raftPort + " must be connectable");
                }
            });
        }
    }

    @Test
    @DisplayName("Egress Filtering: RaftRpcClient filter hook drops before transmission with immediate timeout")
    void testClientEgressDropWithRaftRpcClientHook() {
        // Attach filter to clients instead of servers
        filter.detachFromCluster(nodes);
        filter.attachToClients(rpcClients);

        filter.blockLink("node-1", "node-4");

        RequestVoteRequestDto voteReq = new RequestVoteRequestDto(7L, "node-1", 10L, 7L);
        CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                rpcClients.get("node-1").sendRequestVote("node-4", voteReq, Duration.ofSeconds(2));

        // Should fail fast with SocketTimeoutException wrapped in ExecutionException
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertInstanceOf(java.net.SocketTimeoutException.class, ex.getCause(),
                "Cause must be SocketTimeoutException from RpcFilterHook");
        assertTrue(ex.getCause().getMessage().contains("Simulated network drop"));
    }
}
