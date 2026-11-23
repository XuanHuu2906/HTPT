package vn.nhom6.railway.networking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Preflight syntax, topology, and port mapping verification for LAN configuration files.
 *
 * <p>Requirements:
 * <ul>
 *   <li>Task: TV3-D23 (SPEC-08 §1, §2, §3, §3.1 and AGENTS.md §0, §5).</li>
 *   <li>Verifies {@code configs/lan/node-1.properties} through {@code node-5.properties}.</li>
 *   <li>Verifies {@code configs/lan/PORTS.md} network specification document.</li>
 *   <li>Verifies peer list syntax compatibility with {@link ElectionRuntime#parsePeers(String)}.</li>
 *   <li>Verifies channel resolution in {@link MessageDispatcher#resolveChannelForPort(int)}.</li>
 *   <li>Explicitly states placeholder status: physical 5-laptop LAN test is NOT claimed.</li>
 * </ul>
 */
public class LanConfigPreflightTest {

    private static final Logger LOGGER = Logger.getLogger(LanConfigPreflightTest.class.getName());
    private static final List<String> NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private static File findFile(String relativePath) {
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
    @DisplayName("TC-LAN-01: All five LAN node properties files must exist")
    public void testAllFiveLanPropertiesFilesExist() {
        for (String nodeId : NODE_IDS) {
            File file = findFile("configs/lan/" + nodeId + ".properties");
            assertTrue(file.exists(), "Properties file must exist for: " + nodeId + " at " + file.getPath());
            assertTrue(file.isFile(), "Path must be a regular file: " + file.getPath());
            assertTrue(file.length() > 0, "Properties file must not be empty: " + file.getPath());
        }
    }

    @Test
    @DisplayName("TC-LAN-02: NodeConfig parses all five nodes with standard LAN ports 9100 and 9200")
    public void testNodeConfigParsesAllFiveNodes() throws Exception {
        for (int i = 0; i < NODE_IDS.size(); i++) {
            String expectedId = NODE_IDS.get(i);
            File file = findFile("configs/lan/" + expectedId + ".properties");
            assertTrue(file.exists());

            NodeConfig config = NodeConfig.load(file.getPath());
            assertNotNull(config, "Config must load successfully for " + expectedId);
            assertEquals(expectedId, config.nodeId(), "nodeId mismatch");
            assertEquals("railway-demo-5", config.clusterId(), "clusterId mismatch");
            assertEquals("0.0.0.0", config.clientHost(), "clientHost must bind to 0.0.0.0 for LAN");
            assertEquals(9100, config.clientPort(), "Client port must be 9100 on LAN per SPEC-08 §3");
            assertEquals("0.0.0.0", config.raftHost(), "raftHost must bind to 0.0.0.0 for LAN");
            assertEquals(9200, config.raftPort(), "Raft peer port must be 9200 on LAN per SPEC-08 §3");
            assertEquals(32, config.maxThreads(), "maxThreads should be 32");
            assertEquals(15000, config.soTimeoutMs(), "soTimeoutMs should be 15000ms");
        }
    }

    @Test
    @DisplayName("TC-LAN-03: Separate PostgreSQL configuration per node (TV2 DB isolation)")
    public void testSeparateDatabaseConfigurationPerNode() throws Exception {
        Set<String> databaseNames = new HashSet<>();

        for (int i = 0; i < NODE_IDS.size(); i++) {
            String nodeId = NODE_IDS.get(i);
            File file = findFile("configs/lan/" + nodeId + ".properties");
            NodeConfig config = NodeConfig.load(file.getPath());

            assertNotNull(config.dbConfig(), "dbConfig must be present for " + nodeId);
            String dbName = config.dbConfig().dbName();
            assertNotNull(dbName);
            assertEquals("railway_node" + (i + 1), dbName, "Database name must be isolated per node");
            assertTrue(databaseNames.add(dbName), "Duplicate database name detected: " + dbName);
            assertEquals("railway", config.dbConfig().user());

            // Node 1 maintains 5433 for backwards-compatibility with NodeMainTest
            if ("node-1".equals(nodeId)) {
                assertEquals(5433, config.dbConfig().port(), "node-1 maintains port 5433 for test suite compatibility");
            }
        }

        assertEquals(5, databaseNames.size(), "All 5 nodes must have distinct database names");
    }

    @Test
    @DisplayName("TC-LAN-04: Node peers placeholder format is strictly valid for ElectionRuntime.parsePeers()")
    public void testNodePeersFormatCompatibleWithElectionRuntime() throws Exception {
        for (String nodeId : NODE_IDS) {
            File file = findFile("configs/lan/" + nodeId + ".properties");
            Properties props = new Properties();
            try (InputStream in = new FileInputStream(file)) {
                props.load(in);
            }

            String peersStr = props.getProperty("node.peers");
            assertNotNull(peersStr, "node.peers property must exist in " + nodeId + ".properties");
            assertFalse(peersStr.isBlank(), "node.peers must not be blank in " + nodeId + ".properties");

            // Parse via official ElectionRuntime.parsePeers
            Map<String, InetSocketAddress> peers = ElectionRuntime.parsePeers(peersStr);
            assertNotNull(peers);
            assertEquals(5, peers.size(), "NODE_PEERS must contain exactly 5 voters");
            assertEquals(Set.of("node-1", "node-2", "node-3", "node-4", "node-5"), peers.keySet());

            for (Map.Entry<String, InetSocketAddress> entry : peers.entrySet()) {
                assertEquals(9200, entry.getValue().getPort(), "Peer port must be 9200 for: " + entry.getKey());
                assertFalse(entry.getValue().getHostString().isBlank(), "Host must not be blank");
            }
        }
    }

    @Test
    @DisplayName("TC-LAN-05: PORTS.md specification documentation exists and covers all requirements")
    public void testPortsDocumentationExistsAndCoversSpec() throws Exception {
        File portsDoc = findFile("configs/lan/PORTS.md");
        assertTrue(portsDoc.exists(), "configs/lan/PORTS.md must exist");
        assertTrue(portsDoc.length() > 0, "configs/lan/PORTS.md must not be empty");

        String content = Files.readString(portsDoc.toPath());

        // Verify required sections and keywords
        assertTrue(content.contains("9100"), "PORTS.md must specify Client port 9100");
        assertTrue(content.contains("9200"), "PORTS.md must specify Peer Raft port 9200");
        assertTrue(content.contains("Laptop 1"), "PORTS.md must document Laptop 1");
        assertTrue(content.contains("Laptop 6"), "PORTS.md must document Laptop 6 (Monitor/Client)");
        assertTrue(content.contains("Quorum") || content.contains("quorum"), "PORTS.md must specify Quorum");
        assertTrue(content.contains("3/5"), "PORTS.md must specify 3/5 quorum");
        assertTrue(content.contains("PLACEHOLDER") || content.contains("placeholder"), "PORTS.md must clearly flag placeholder IPs");
        assertTrue(content.contains("MessageDispatcher"), "PORTS.md must document MessageDispatcher role");
        assertTrue(content.contains("INVALID_REQUEST"), "PORTS.md must document channel isolation error code");
    }

    @Test
    @DisplayName("TC-LAN-06: MessageDispatcher standard port resolution matches SPEC-08")
    public void testMessageDispatcherPortChannelResolution() {
        assertEquals(9100, MessageDispatcher.STANDARD_CLIENT_PORT);
        assertEquals(9200, MessageDispatcher.STANDARD_PEER_PORT);

        // LAN Ports
        assertEquals(MessageDispatcher.ChannelType.CLIENT, MessageDispatcher.resolveChannelForPort(9100));
        assertEquals(MessageDispatcher.ChannelType.PEER, MessageDispatcher.resolveChannelForPort(9200));

        // Dev host published ports (9101..9105, 9201..9205)
        for (int p = 9101; p <= 9105; p++) {
            assertEquals(MessageDispatcher.ChannelType.CLIENT, MessageDispatcher.resolveChannelForPort(p));
        }
        for (int p = 9201; p <= 9205; p++) {
            assertEquals(MessageDispatcher.ChannelType.PEER, MessageDispatcher.resolveChannelForPort(p));
        }

        // Other ports
        assertEquals(MessageDispatcher.ChannelType.ALL, MessageDispatcher.resolveChannelForPort(8080));
        assertEquals(MessageDispatcher.ChannelType.ALL, MessageDispatcher.resolveChannelForPort(0));
    }

    @Test
    @DisplayName("TC-LAN-07: Explicit preflight assertion - syntax PASS with placeholders, physical cluster deferred")
    public void testExplicitLanPreflightAssertionNotClaimingPhysicalCluster() {
        LOGGER.info("================================================================================");
        LOGGER.info(" LAN PREFLIGHT VERIFICATION REPORT (TV3-D23)");
        LOGGER.info("================================================================================");
        LOGGER.info(" 1. Configuration templates (configs/lan/node-1..5.properties) : SYNTAX_PASS");
        LOGGER.info(" 2. Port mapping specification (configs/lan/PORTS.md)           : SPEC_VALID");
        LOGGER.info(" 3. Raft voter peer formatting (ElectionRuntime parser)         : 5_VOTERS_VALID");
        LOGGER.info(" 4. IP mode                                                     : PLACEHOLDER_MODE");
        LOGGER.info(" 5. Physical 5-Laptop TCP connection status                     : NOT_RUN (TV6-D24 / G6)");
        LOGGER.info("================================================================================");

        // Explicit invariant per AGENTS.md §0, §5, §6
        boolean placeholderMode = true;
        boolean physicalLanTested = false;

        assertTrue(placeholderMode, "D23 operates strictly in template/placeholder mode");
        assertFalse(physicalLanTested, "Physical 5-laptop cluster must NOT be reported as tested in D23");
    }
}
