package vn.nhom6.railway.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.monitor.MonitorController.NodeRow;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;

class TcpNodeStatusAdapterTest {

    private final List<ServerSocket> openServers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ServerSocket server : openServers) {
            try {
                server.close();
            } catch (IOException ignored) { }
        }
        openServers.clear();
    }

    @Test
    void pollsRealTcpNodeStatusAndHandlesOfflineAndStorageError() throws Exception {
        Map<String, InetSocketAddress> endpoints = new LinkedHashMap<>();

        // node-1: LEADER ONLINE
        endpoints.put("node-1", startStubServer("node-1", "LEADER", 4L, "node-1", 12L, 12L, 12L, true, 0L));
        // node-2: FOLLOWER ONLINE catching up
        endpoints.put("node-2", startStubServer("node-2", "FOLLOWER", 4L, "node-1", 12L, 12L, 10L, true, 0L));
        // node-3: STORAGE_ERROR
        endpoints.put("node-3", startStubServer("node-3", "FOLLOWER", 4L, "node-1", 12L, 12L, 10L, false, 0L));
        // node-4: TIMEOUT (slow server)
        endpoints.put("node-4", startStubServer("node-4", "FOLLOWER", 4L, "node-1", 12L, 12L, 12L, true, 1200L));
        // node-5: OFFLINE (closed port)
        int closedPort = allocateUnusedPort();
        endpoints.put("node-5", new InetSocketAddress("127.0.0.1", closedPort));

        var adapter = new TcpNodeStatusAdapter(
                endpoints, Duration.ofMillis(300), Duration.ofMillis(350), "test-monitor");
        try (var client = new MonitorTcpClient(adapter, Duration.ofMillis(400))) {
            NodeRow r1 = client.poll("node-1").get(3, TimeUnit.SECONDS);
            NodeRow r2 = client.poll("node-2").get(3, TimeUnit.SECONDS);
            NodeRow r3 = client.poll("node-3").get(3, TimeUnit.SECONDS);
            NodeRow r4 = client.poll("node-4").get(3, TimeUnit.SECONDS);
            NodeRow r5 = client.poll("node-5").get(3, TimeUnit.SECONDS);

            assertEquals("LEADER", r1.role());
            assertEquals(4L, r1.term());
            assertEquals("ONLINE", r1.status());
            assertEquals(12L, r1.commitIndex());
            assertEquals(12L, r1.lastApplied());

            assertEquals("FOLLOWER", r2.role());
            assertEquals("ONLINE", r2.status());
            assertEquals(10L, r2.lastApplied());

            assertEquals("STORAGE_ERROR", r3.status());

            assertEquals("TIMEOUT", r4.status());
            assertNull(r4.role());

            assertEquals("OFFLINE", r5.status());
            assertNull(r5.role());

            // Verify MonitorEventLog correctly tracks leader, catching-up, and offline nodes
            var eventLog = new MonitorEventLog();
            eventLog.recordSnapshot(List.of(r1, r2, r3, r4, r5));
            assertEquals(MonitorEventLog.SyncState.SYNCED, eventLog.recoveryRowOf("node-1").syncState());
            assertEquals(MonitorEventLog.SyncState.CATCHING_UP, eventLog.recoveryRowOf("node-2").syncState());
            assertEquals(MonitorEventLog.SyncState.OFFLINE, eventLog.recoveryRowOf("node-5").syncState());
        }
    }

    @Test
    void loadsDevClusterPropertiesWhenPresent() throws Exception {
        Path devConfig = Path.of("../configs/dev/cluster.properties");
        if (!java.nio.file.Files.isRegularFile(devConfig)) {
            devConfig = Path.of("configs/dev/cluster.properties");
        }
        assertTrue(java.nio.file.Files.isRegularFile(devConfig), "configs/dev/cluster.properties should exist");
        var cfg = TcpNodeStatusAdapter.loadConfig(devConfig);
        assertEquals(5, cfg.endpoints().size());
        assertEquals(9101, cfg.endpoints().get("node-1").getPort());
        assertEquals(9105, cfg.endpoints().get("node-5").getPort());
    }

    private InetSocketAddress startStubServer(
            String nodeId, String role, long term, String leaderId,
            long lastLogIndex, long commitIndex, long lastApplied,
            boolean storageHealthy, long delayMs) throws IOException {
        ServerSocket server = new ServerSocket(0);
        openServers.add(server);
        Thread thread = new Thread(() -> {
            while (!server.isClosed()) {
                try (Socket socket = server.accept()) {
                    InputStream in = socket.getInputStream();
                    OutputStream out = new BufferedOutputStream(socket.getOutputStream());
                    byte[] frame = FrameCodec.readFrame(in);
                    MessageEnvelope<JsonNode> req = MessageCodec.decode(frame);
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                    Map<String, Boolean> peers = new LinkedHashMap<>();
                    for (String id : MonitorTcpClient.NODE_IDS) {
                        if (!id.equals(nodeId)) {
                            peers.put(id, true);
                        }
                    }
                    NodeStatusDto dto = new NodeStatusDto(
                            nodeId, role, term, leaderId, lastLogIndex, commitIndex, lastApplied,
                            storageHealthy, peers, Instant.now().toString());
                    MessageEnvelope<NodeStatusDto> resp = MessageEnvelope.createResponse(
                            req.getMessageId(), MessageType.NODE_STATUS_RESPONSE, nodeId, dto);
                    FrameCodec.writeMessage(out, resp);
                } catch (Exception ignored) {
                    if (server.isClosed()) {
                        break;
                    }
                }
            }
        }, "stub-node-status-" + nodeId);
        thread.setDaemon(true);
        thread.start();
        return new InetSocketAddress("127.0.0.1", server.getLocalPort());
    }

    private static int allocateUnusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
