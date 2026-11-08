package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NodeMainTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    public void cleanup() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    private static File findConfigFile(String relativePath) {
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

    @Test
    public void testNodeConfigFromPropertiesFile() throws Exception {
        File configFile = findConfigFile("configs/lan/node-1.properties");
        assertTrue(configFile.exists(), "configs/lan/node-1.properties must exist");

        NodeConfig config = NodeConfig.load(configFile.getPath());
        assertNotNull(config);
        assertEquals("node-1", config.nodeId());
        assertEquals("railway-demo-5", config.clusterId());
        assertEquals("0.0.0.0", config.clientHost());
        assertEquals(9100, config.clientPort());
        assertEquals("0.0.0.0", config.raftHost());
        assertEquals(9200, config.raftPort());
        assertNotNull(config.dbConfig());
        assertEquals("railway_node1", config.dbConfig().dbName());
        assertEquals("railway", config.dbConfig().user());
        assertEquals(5433, config.dbConfig().port());
    }

    @Test
    public void testNodeConfigDefaults() {
        NodeConfig config = NodeConfig.fromProperties(new Properties(), Map.of());
        assertEquals("node-1", config.nodeId());
        assertEquals("railway-demo-5", config.clusterId());
        assertEquals(9100, config.clientPort());
        assertEquals(9200, config.raftPort());
        assertNull(config.dbConfig());
    }

    @Test
    public void testNodeMainInProcessPingAndStatus() throws Exception {
        // Setup config with dynamic ports (0) to avoid any port conflicts
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        NodeMain node = new NodeMain(config);
        resources.add(node);

        node.start();
        assertTrue(node.isRunning());
        assertTrue(node.getClientPort() > 0);
        assertTrue(node.getRaftPort() > 0);
        assertNotEquals(node.getClientPort(), node.getRaftPort());

        // Connect client to Client TCP port
        try (TcpClient client = new TcpClient("127.0.0.1", node.getClientPort(), 2000, 5000)) {
            client.connect();
            assertTrue(client.isConnected());

            // 1. Verify PING -> PONG raw frame
            byte[] pingResp = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
            assertEquals("PONG", new String(pingResp, StandardCharsets.UTF_8));

            // 2. Verify NODE_STATUS -> NODE_STATUS_RESPONSE envelope
            MessageEnvelope<Map<String, Object>> statusReq = MessageEnvelope.createRequest(MessageType.NODE_STATUS, "monitor-1", Map.of());
            MessageEnvelope<JsonNode> statusResp = client.sendAndReceive(statusReq);

            assertNotNull(statusResp);
            assertEquals(MessageType.NODE_STATUS_RESPONSE, statusResp.getType());
            assertEquals(statusReq.getMessageId(), statusResp.getCorrelationId());
            assertEquals("node-1", statusResp.getPayload().get("nodeId").asText());
            assertEquals("FOLLOWER", statusResp.getPayload().get("role").asText());
            assertNotNull(statusResp.getPayload().get("observedAt"));

            // 3. Verify channel protection: Raft RPC sent to client port is rejected
            MessageEnvelope<Map<String, Object>> voteReq = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-2", Map.of("term", 1));
            MessageEnvelope<JsonNode> errorResp = client.sendAndReceive(voteReq);
            assertEquals(MessageType.ERROR_RESPONSE, errorResp.getType());
            assertEquals("INVALID_REQUEST", errorResp.getPayload().get("errorCode").asText());
        }

        // Clean stop
        node.stop();
        assertFalse(node.isRunning());
    }

    @Test
    public void testNodeMainSubprocessPingAndStatus() throws Exception {
        // Create a temporary configuration file with ephemeral ports
        File tempConfig = File.createTempFile("node-subprocess-", ".properties");
        tempConfig.deleteOnExit();

        try (FileOutputStream out = new FileOutputStream(tempConfig)) {
            Properties p = new Properties();
            p.setProperty("node.id", "node-sub-1");
            p.setProperty("cluster.id", "railway-demo-5");
            p.setProperty("client.host", "127.0.0.1");
            p.setProperty("client.port", "0");
            p.setProperty("raft.host", "127.0.0.1");
            p.setProperty("raft.port", "0");
            p.store(out, "Test Subprocess Config");
        }

        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "vn.nhom6.railway.networking.NodeMain",
                tempConfig.getAbsolutePath()
        );
        pb.redirectErrorStream(true);

        Process nodeProcess = pb.start();

        // Read output of subprocess to find the allocated client port
        BufferedReader reader = new BufferedReader(new InputStreamReader(nodeProcess.getInputStream()));
        String line;
        int clientPort = -1;
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline && (line = reader.readLine()) != null) {
            System.out.println("[Subprocess Node] " + line);
            if (line.contains("[READY]")) {
                int clientIdx = line.indexOf("client=");
                int spaceIdx = line.indexOf(' ', clientIdx);
                if (clientIdx >= 0 && spaceIdx > clientIdx) {
                    clientPort = Integer.parseInt(line.substring(clientIdx + 7, spaceIdx).trim());
                    break;
                }
            }
        }

        Thread drainThread = new Thread(() -> {
            try {
                String l;
                while ((l = reader.readLine()) != null) {
                    // drain
                }
            } catch (IOException ignored) {}
        });
        drainThread.setDaemon(true);
        drainThread.start();

        assertTrue(clientPort > 0, "Subprocess client port must be extracted from startup log");

        // Connect to subprocess and verify PING and STATUS
        try (TcpClient client = new TcpClient("127.0.0.1", clientPort, 2000, 5000)) {
            client.connect();
            assertTrue(client.isConnected());

            // 1. PING -> PONG
            byte[] pong = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
            assertEquals("PONG", new String(pong, StandardCharsets.UTF_8));

            // 2. STATUS -> NODE_STATUS_RESPONSE
            MessageEnvelope<Map<String, Object>> statusReq = MessageEnvelope.createRequest(MessageType.NODE_STATUS, "monitor-sub", Map.of());
            MessageEnvelope<JsonNode> statusResp = client.sendAndReceive(statusReq);

            assertNotNull(statusResp);
            assertEquals(MessageType.NODE_STATUS_RESPONSE, statusResp.getType());
            assertEquals("node-sub-1", statusResp.getPayload().get("nodeId").asText());
        }

        // Clean termination
        nodeProcess.destroy();
        boolean finished = nodeProcess.waitFor(5, TimeUnit.SECONDS);
        assertTrue(finished, "Subprocess node should terminate cleanly within timeout");
    }

    public static void main(String[] args) {
        System.out.println("Starting NodeMainTest runner...");
        NodeMainTest test = new NodeMainTest();
        int passed = 0;
        int total = 4;

        try {
            System.out.println("[TEST 1] testNodeConfigFromPropertiesFile...");
            test.testNodeConfigFromPropertiesFile();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 2] testNodeConfigDefaults...");
            test.testNodeConfigDefaults();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 3] testNodeMainInProcessPingAndStatus...");
            test.testNodeMainInProcessPingAndStatus();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 4] testNodeMainSubprocessPingAndStatus...");
            test.testNodeMainSubprocessPingAndStatus();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("\n=== ALL NODE MAIN TESTS COMPLETED: " + passed + "/" + total + " PASSED ===");
        } catch (Throwable t) {
            System.err.println("Test failed: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }
}
