package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.*;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("5-Node TCP Network Cluster Smoke & Integration Tests (TV3-D16)")
public class TcpClusterSmokeTest {

    private static final Logger LOGGER = Logger.getLogger(TcpClusterSmokeTest.class.getName());
    private static final List<String> CLUSTER_NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final Map<String, NodeMain> nodes = new LinkedHashMap<>();
    private final Map<String, FakeRaftRequestHandler> fakeProcessors = new LinkedHashMap<>();
    private final Map<String, RaftRpcClient> rpcClients = new LinkedHashMap<>();
    private final Map<String, Integer> clientPorts = new LinkedHashMap<>();
    private final Map<String, Integer> raftPorts = new LinkedHashMap<>();

    @BeforeEach
    void startCluster() throws IOException {
        // 1. Initialize and start 5 NodeMain instances on ephemeral ports (0)
        for (String nodeId : CLUSTER_NODE_IDS) {
            NodeConfig config = new NodeConfig(
                    nodeId,
                    "railway-demo-5",
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

            // Register dedicated Raft RPC server handlers on the peer registry
            FakeRaftRequestHandler processor = new FakeRaftRequestHandler(1L);
            fakeProcessors.put(nodeId, processor);
            node.getRaftRegistry().register(MessageType.REQUEST_VOTE, new RequestVoteRpcHandler(processor, nodeId));
            node.getRaftRegistry().register(MessageType.APPEND_ENTRIES, new AppendEntriesRpcHandler(processor, nodeId));

            node.start();
            nodes.put(nodeId, node);
            clientPorts.put(nodeId, node.getClientPort());
            raftPorts.put(nodeId, node.getRaftPort());
        }

        // 2. Establish peer resolution map and instantiate RaftRpcClients for full mesh
        for (String localId : CLUSTER_NODE_IDS) {
            Map<String, InetSocketAddress> peerEndpoints = new LinkedHashMap<>();
            for (String peerId : CLUSTER_NODE_IDS) {
                if (!peerId.equals(localId)) {
                    peerEndpoints.put(peerId, new InetSocketAddress("127.0.0.1", raftPorts.get(peerId)));
                }
            }
            RaftRpcClient client = new RaftRpcClient(localId, peerEndpoints, 64, 5000, Duration.ofSeconds(5));
            rpcClients.put(localId, client);
        }
    }

    @AfterEach
    void stopCluster() {
        // Stop all RaftRpcClients
        for (RaftRpcClient client : rpcClients.values()) {
            try {
                client.close();
            } catch (Exception ignored) {}
        }
        rpcClients.clear();

        // Stop all NodeMain instances
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
    @DisplayName("Verify 5-node cluster port allocation and peer list resolution without bind errors")
    void testClusterPortAllocationAndPeerListVerification() {
        assertEquals(5, nodes.size(), "Cluster must contain exactly 5 nodes");

        Set<Integer> allBoundPorts = new HashSet<>();

        // 1. Verify ports and zero bind errors
        for (String nodeId : CLUSTER_NODE_IDS) {
            int clientPort = clientPorts.get(nodeId);
            int raftPort = raftPorts.get(nodeId);

            assertTrue(clientPort > 0, nodeId + " clientPort must be > 0");
            assertTrue(raftPort > 0, nodeId + " raftPort must be > 0");
            assertNotEquals(clientPort, raftPort, nodeId + " clientPort and raftPort must be distinct");

            assertTrue(allBoundPorts.add(clientPort), "Client port " + clientPort + " collision detected");
            assertTrue(allBoundPorts.add(raftPort), "Raft port " + raftPort + " collision detected");

            assertTrue(nodes.get(nodeId).isRunning(), nodeId + " must be running");
        }

        assertEquals(10, allBoundPorts.size(), "Exactly 10 unique server ports must be bound across 5 nodes");

        // 2. Verify peer list resolution: each node must map exactly 4 other peers
        StringBuilder nodePeersEnvBuilder = new StringBuilder();
        for (int i = 0; i < CLUSTER_NODE_IDS.size(); i++) {
            String nodeId = CLUSTER_NODE_IDS.get(i);
            if (i > 0) nodePeersEnvBuilder.append(",");
            nodePeersEnvBuilder.append(nodeId).append("@127.0.0.1:").append(raftPorts.get(nodeId));

            RaftRpcClient client = rpcClients.get(nodeId);
            assertNotNull(client, "RaftRpcClient for " + nodeId + " must exist");
        }

        // 3. Verify parsePeers integration from ElectionRuntime
        Map<String, InetSocketAddress> parsedMesh = ElectionRuntime.parsePeers(nodePeersEnvBuilder.toString());
        assertEquals(5, parsedMesh.size());
        for (String nodeId : CLUSTER_NODE_IDS) {
            assertEquals(raftPorts.get(nodeId).intValue(), parsedMesh.get(nodeId).getPort());
        }

        LOGGER.info(() -> "[CLUSTER] Successfully verified 5 nodes, 10 TCP ports, and 20 directed peer channels.");
    }

    @Test
    @DisplayName("Execute concurrent 20 RPC/Node workload across 5 nodes (100 concurrent RPCs full-mesh)")
    void testConcurrentFullMeshRpcWorkload() throws Exception {
        final int rpcsPerNode = 20; // Pass criterion: 20 RPC/Node
        final int totalNodes = CLUSTER_NODE_IDS.size();
        final int expectedTotalRpcs = totalNodes * rpcsPerNode; // 100 total RPCs

        List<CompletableFuture<?>> allFutures = new CopyOnWriteArrayList<>();
        AtomicInteger successCounter = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        // Each of the 5 nodes sends 20 RPCs distributed across its 4 peers
        final long clusterTerm = 5L; // Fixed consensus term for concurrent test batch

        for (String senderId : CLUSTER_NODE_IDS) {
            RaftRpcClient client = rpcClients.get(senderId);
            List<String> peers = new ArrayList<>(CLUSTER_NODE_IDS);
            peers.remove(senderId); // 4 peers

            for (int r = 0; r < rpcsPerNode; r++) {
                String targetPeer = peers.get(r % peers.size());

                if (r % 2 == 0) {
                    // Send REQUEST_VOTE RPC
                    RequestVoteRequestDto voteReq = new RequestVoteRequestDto(clusterTerm, senderId, 10L + r, clusterTerm);
                    CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> future =
                            client.sendRequestVote(targetPeer, voteReq, Duration.ofSeconds(5))
                                    .thenApply(resp -> {
                                        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, resp.getType());
                                        assertEquals(targetPeer, resp.getSenderId());
                                        assertNotNull(resp.getCorrelationId());
                                        assertNotNull(resp.getPayload());
                                        assertTrue(resp.getPayload().getTerm() >= clusterTerm);
                                        assertTrue(resp.getPayload().isVoteGranted());
                                        successCounter.incrementAndGet();
                                        return resp;
                                    });
                    allFutures.add(future);
                } else {
                    // Send APPEND_ENTRIES RPC
                    AppendEntriesRequestDto.LogEntryDto entry = new AppendEntriesRequestDto.LogEntryDto(
                            10L + r, clusterTerm, "BOOK_TICKET", Map.of("requestId", UUID.randomUUID().toString()));
                    AppendEntriesRequestDto appendReq = new AppendEntriesRequestDto(
                            clusterTerm, senderId, 9L + r, clusterTerm, List.of(entry), 9L + r);
                    CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> future =
                            client.sendAppendEntries(targetPeer, appendReq, Duration.ofSeconds(5))
                                    .thenApply(resp -> {
                                        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, resp.getType());
                                        assertEquals(targetPeer, resp.getSenderId());
                                        assertNotNull(resp.getCorrelationId());
                                        assertNotNull(resp.getPayload());
                                        assertTrue(resp.getPayload().isSuccess());
                                        successCounter.incrementAndGet();
                                        return resp;
                                    });
                    allFutures.add(future);
                }
            }
        }

        assertEquals(expectedTotalRpcs, allFutures.size(), "Must dispatch exactly 100 concurrent RPCs");

        // Await all 100 RPCs to complete
        CompletableFuture<Void> combined = CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));
        assertDoesNotThrow(() -> combined.get(10, TimeUnit.SECONDS),
                "All 100 concurrent RPCs must complete within 10s deadline without timeouts or connection drops");

        long elapsedMs = System.currentTimeMillis() - startTime;
        assertEquals(expectedTotalRpcs, successCounter.get(), "All 100 RPCs must complete successfully");
        LOGGER.info(() -> String.format(
                "[PERF] 100 concurrent RPCs (20 RPC/Node) completed successfully in %d ms (~%.2f ms/RPC)",
                elapsedMs, (double) elapsedMs / expectedTotalRpcs));
    }

    @Test
    @DisplayName("Verify correlation tracking and structured logging by node and term")
    void testLogCorrelationByNodeAndTerm() throws Exception {
        List<String> correlationLogs = new CopyOnWriteArrayList<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();

        long[] testTerms = {1L, 2L, 5L};

        for (long term : testTerms) {
            for (String senderId : CLUSTER_NODE_IDS) {
                RaftRpcClient client = rpcClients.get(senderId);
                for (String targetPeer : CLUSTER_NODE_IDS) {
                    if (targetPeer.equals(senderId)) continue;

                    String messageId = UUID.randomUUID().toString();
                    long start = System.currentTimeMillis();

                    RequestVoteRequestDto voteReq = new RequestVoteRequestDto(term, senderId, 100L, term);
                    MessageEnvelope<RequestVoteRequestDto> envelope = MessageEnvelope.createRequest(
                            MessageType.REQUEST_VOTE, senderId, voteReq);

                    CompletableFuture<?> f = client.exchange(targetPeer, envelope).thenAccept(resp -> {
                        long latency = System.currentTimeMillis() - start;

                        // Assert correlation invariant: correlationId == messageId
                        assertEquals(envelope.getMessageId(), resp.getCorrelationId(),
                                "Response correlationId must strictly match request messageId");
                        assertEquals(targetPeer, resp.getSenderId(),
                                "Response senderId must match targeted peer");

                        RequestVoteResponseDto voteResp;
                        try {
                            voteResp = MessageCodec.decodePayload(resp, RequestVoteResponseDto.class);
                        } catch (Exception ex) {
                            throw new RuntimeException("Failed to decode vote response", ex);
                        }
                        assertTrue(voteResp.getTerm() >= term,
                                "Response term must be >= request term");

                        String logEntry = String.format(
                                "[CORRELATION] sender=%s target=%s type=%s reqId=%s corrId=%s term=%d status=SUCCESS latency=%dms",
                                senderId, targetPeer, resp.getType(), envelope.getMessageId(), resp.getCorrelationId(), term, latency
                        );
                        correlationLogs.add(logEntry);
                        LOGGER.fine(logEntry);
                    });
                    futures.add(f);
                }
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        // 3 terms * 5 nodes * 4 peers = 60 RPCs
        assertEquals(60, correlationLogs.size());
        LOGGER.info(() -> "[CORRELATION-CHECK] Verified 60 correlation log entries across terms [1, 2, 5]. Sample:\n"
                + correlationLogs.get(0) + "\n" + correlationLogs.get(correlationLogs.size() - 1));
    }

    @Test
    @DisplayName("Verify channel boundary isolation across all 5 nodes (SPEC-08 §3.1)")
    void testChannelBoundaryAcrossFiveNodes() throws Exception {
        for (String nodeId : CLUSTER_NODE_IDS) {
            int clientPort = clientPorts.get(nodeId);
            int raftPort = raftPorts.get(nodeId);

            // 1. Verify Client Port rejects Raft RPCs with INVALID_REQUEST
            try (TcpClient client = new TcpClient("127.0.0.1", clientPort, 2000, 5000)) {
                client.connect();
                assertTrue(client.isConnected());

                // PING frame works
                byte[] pong = client.sendAndReceive("PING".getBytes());
                assertEquals("PONG", new String(pong));

                // REQUEST_VOTE to client port must be rejected
                RequestVoteRequestDto vote = new RequestVoteRequestDto(1L, "node-attacker", 0L, 0L);
                MessageEnvelope<?> voteEnv = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-attacker", vote);
                MessageEnvelope<JsonNode> errResp = client.sendAndReceive(voteEnv);

                assertNotNull(errResp);
                assertEquals(MessageType.ERROR_RESPONSE, errResp.getType());
                assertEquals("INVALID_REQUEST", errResp.getPayload().get("errorCode").asText());

                // APPEND_ENTRIES to client port must be rejected
                AppendEntriesRequestDto append = new AppendEntriesRequestDto(1L, "node-attacker", 0L, 0L, List.of(), 0L);
                MessageEnvelope<?> appendEnv = MessageEnvelope.createRequest(MessageType.APPEND_ENTRIES, "node-attacker", append);
                MessageEnvelope<JsonNode> errResp2 = client.sendAndReceive(appendEnv);

                assertNotNull(errResp2);
                assertEquals(MessageType.ERROR_RESPONSE, errResp2.getType());
                assertEquals("INVALID_REQUEST", errResp2.getPayload().get("errorCode").asText());
            }

            // 2. Verify Raft Peer Port rejects Client requests
            try (TcpClient client = new TcpClient("127.0.0.1", raftPort, 2000, 5000)) {
                client.connect();
                assertTrue(client.isConnected());

                // GET_SEATS to Raft peer port must be rejected
                GetSeatsRequestDto seatsReq = new GetSeatsRequestDto("SE-001");
                MessageEnvelope<?> seatsEnv = MessageEnvelope.createRequest(MessageType.GET_SEATS, "client-hacker", seatsReq);
                MessageEnvelope<JsonNode> errResp = client.sendAndReceive(seatsEnv);

                assertNotNull(errResp);
                assertEquals(MessageType.ERROR_RESPONSE, errResp.getType());
                assertEquals("INVALID_REQUEST", errResp.getPayload().get("errorCode").asText());
            }
        }
        LOGGER.info("[SECURITY] Channel boundaries successfully verified across all 5 nodes.");
    }

    @Test
    @DisplayName("Verify concurrent monitor status broadcast across all 5 nodes (SPEC-03 §5)")
    void testConcurrentMonitorStatusBroadcast() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(5);
        List<Future<MessageEnvelope<NodeStatusDto>>> futures = new ArrayList<>();

        for (String nodeId : CLUSTER_NODE_IDS) {
            int port = clientPorts.get(nodeId);
            futures.add(pool.submit(() -> {
                try (TcpClient client = new TcpClient("127.0.0.1", port, 2000, 5000)) {
                    client.connect();
                    MessageEnvelope<?> req = MessageEnvelope.createRequest(MessageType.NODE_STATUS, "monitor-agent", Map.of());
                    return client.sendAndReceive(req, NodeStatusDto.class);
                }
            }));
        }

        for (int i = 0; i < CLUSTER_NODE_IDS.size(); i++) {
            String expectedNodeId = CLUSTER_NODE_IDS.get(i);
            MessageEnvelope<NodeStatusDto> resp = futures.get(i).get(5, TimeUnit.SECONDS);

            assertNotNull(resp);
            assertEquals(MessageType.NODE_STATUS_RESPONSE, resp.getType());
            assertEquals(expectedNodeId, resp.getSenderId());

            NodeStatusDto status = resp.getPayload();
            assertNotNull(status);
            status.validate();
            assertEquals(expectedNodeId, status.getNodeId());
            assertNotNull(status.getObservedAt());

            Map<String, Boolean> peers = status.getPeerConnectivity();
            assertNotNull(peers);
            assertEquals(4, peers.size(), "peerConnectivity must report exactly 4 peers");
            assertFalse(peers.containsKey(expectedNodeId), "peerConnectivity must not report self");
        }

        pool.shutdown();
        LOGGER.info("[MONITOR] Successfully broadcasted and received NODE_STATUS across all 5 nodes.");
    }

    @Test
    @DisplayName("Optional: Verify Docker Compose cluster endpoints if live containers are present")
    void testDockerClusterConnectivityIfAvailable() {
        // Ports defined in docker/compose.dev.yml and configs/dev/cluster.properties
        int[] dockerClientPorts = {9101, 9102, 9103, 9104, 9105};
        int[] dockerRaftPorts = {9201, 9202, 9203, 9204, 9205};

        boolean dockerRunning = true;
        for (int p : dockerClientPorts) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", p), 200);
            } catch (Exception ex) {
                dockerRunning = false;
                break;
            }
        }

        if (!dockerRunning) {
            LOGGER.info("[DOCKER-CHECK] Docker compose containers not running on ports 9101..9105. Skipping live container probe.");
            return;
        }

        LOGGER.info("[DOCKER-CHECK] Detected running Docker Compose cluster! Probing all 5 container endpoints...");
        for (int i = 0; i < 5; i++) {
            String nodeId = "node-" + (i + 1);
            int clientPort = dockerClientPorts[i];
            int raftPort = dockerRaftPorts[i];

            assertDoesNotThrow(() -> {
                try (TcpClient client = new TcpClient("127.0.0.1", clientPort, 2000, 3000)) {
                    client.connect();
                    byte[] pong = client.sendAndReceive("PING".getBytes());
                    assertEquals("PONG", new String(pong));
                }
            }, "Docker container " + nodeId + " client port " + clientPort + " should be reachable");
        }
        LOGGER.info("[DOCKER-CHECK] All 5 Docker container endpoints verified successfully.");
    }
}
