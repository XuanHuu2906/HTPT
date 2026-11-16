package vn.nhom6.railway.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterConnectivityCheck;
import vn.nhom6.railway.client.network.ClusterConnectivityCheck.ClusterCheckSummary;
import vn.nhom6.railway.client.network.ClusterConnectivityCheck.NodeCheckResult;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterNodeSelector;
import vn.nhom6.railway.client.network.DataSource;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.SelectionStrategy;
import vn.nhom6.railway.client.smoke.FakeNodeServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kiểm thử toàn diện cụm và bộ chọn Node (TASK TV1-D16).
 * <p>
 * Bao gồm đầy đủ 10 tiêu chí kỹ thuật:
 * 1. Nạp cấu hình từ file tạm với 5 Node.
 * 2. Đổi Node bằng cách sửa file mà không sửa Java.
 * 3. Thứ tự ưu tiên cấu hình 4 tầng.
 * 4. Validation fail-fast với thông báo rõ ràng.
 * 5. Cảnh báo host nội bộ Docker vs host 127.0.0.1/IP LAN.
 * 6. Các chiến lược lựa chọn: PREFERRED_FIRST, RANDOM, ROUND_ROBIN.
 * 7. Cơ chế failover và cooldown.
 * 8. Không hardcode Leader trong code/config.
 * 9. Đọc tệp UTF-8 có tiếng Việt và đường dẫn Windows chứa khoảng trắng.
 * 10. ClusterConnectivityCheck với FakeNodeServer.
 */
class ClusterNodeSelectorTest {

    // =========================================================================
    // 1. Nạp cấu hình từ file tạm với 5 Node
    // =========================================================================
    @Test
    @DisplayName("AC-D16-01: Nạp cấu hình từ file tạm (@TempDir) với 5 Node -> đúng endpoint từng Node")
    void testLoadConfigFromTempFileWith5Nodes(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("client-dev.properties");
        String content = """
                cluster.profile=dev
                client.selectionStrategy=ROUND_ROBIN
                client.connectTimeoutMs=2500
                client.readTimeoutMs=6000
                dev.cluster.nodes=node-1,node-2,node-3,node-4,node-5
                dev.node.node-1.client.host=127.0.0.1
                dev.node.node-1.client.port=9101
                dev.node.node-2.client.host=127.0.0.1
                dev.node.node-2.client.port=9102
                dev.node.node-3.client.host=127.0.0.1
                dev.node.node-3.client.port=9103
                dev.node.node-4.client.host=127.0.0.1
                dev.node.node-4.client.port=9104
                dev.node.node-5.client.host=127.0.0.1
                dev.node.node-5.client.port=9105
                """;
        Files.writeString(configFile, content, StandardCharsets.UTF_8);

        ClientConfig config = ClientConfig.fromFile(configFile);

        assertEquals("dev", config.getProfile());
        assertEquals(2500, config.getConnectTimeoutMs());
        assertEquals(6000, config.getReadTimeoutMs());
        assertEquals(SelectionStrategy.ROUND_ROBIN, config.getSelectionStrategy());
        assertEquals(5, config.getEndpoints().size());

        NodeEndpoint n1 = config.getEndpoint("node-1");
        assertNotNull(n1);
        assertEquals("127.0.0.1", n1.host());
        assertEquals(9101, n1.port());

        NodeEndpoint n5 = config.getEndpoint("node-5");
        assertNotNull(n5);
        assertEquals("127.0.0.1", n5.host());
        assertEquals(9105, n5.port());
    }

    // =========================================================================
    // 2. Đổi Node bằng cách đổi nội dung file mà không sửa Java
    // =========================================================================
    @Test
    @DisplayName("AC-D16-02: Đổi Node qua config mà không sửa Java -> endpoint đổi theo")
    void testSwitchNodeWithoutRecompilingJava(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("dynamic-client.properties");

        // Cấu hình A: Node 1 ở cổng 9101, Node 2 ở 9102
        Files.writeString(configFile, """
                cluster.profile=dev
                client.preferredNode=node-1
                dev.cluster.nodes=node-1,node-2
                dev.node.node-1.client.host=127.0.0.1
                dev.node.node-1.client.port=9101
                dev.node.node-2.client.host=127.0.0.1
                dev.node.node-2.client.port=9102
                """, StandardCharsets.UTF_8);

        ClientConfig configA = ClientConfig.fromFile(configFile);
        ClusterNodeSelector selector = new ClusterNodeSelector(configA);
        assertEquals("node-1", selector.endpointOf("node-1").nodeId());
        assertEquals(9101, selector.endpointOf("node-1").port());

        // Đổi cấu hình B: Node 1 đổi sang cổng 9999, đổi preferred sang node-2 (192.168.1.50:9100)
        Files.writeString(configFile, """
                cluster.profile=lan
                client.preferredNode=node-2
                lan.cluster.nodes=node-1,node-2
                lan.node.node-1.client.host=192.168.1.10
                lan.node.node-1.client.port=9999
                lan.node.node-2.client.host=192.168.1.50
                lan.node.node-2.client.port=9100
                """, StandardCharsets.UTF_8);

        ClientConfig configB = ClientConfig.fromFile(configFile);
        selector.reload(configB);

        assertEquals("lan", selector.getConfig().getProfile());
        assertEquals(9999, selector.endpointOf("node-1").port());
        assertEquals("192.168.1.10", selector.endpointOf("node-1").host());
        assertEquals("node-2", selector.getConfig().getPreferredNode());
    }

    // =========================================================================
    // 3. Thứ tự ưu tiên nguồn: SysProp > Env > File > Classpath
    // =========================================================================
    @Test
    @DisplayName("AC-D16-03: Thứ tự ưu tiên: System Property > Env Var > File > Classpath")
    void testConfigurationSourcePrecedence(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("test-precedence.properties");
        Files.writeString(file, """
                cluster.profile=dev
                client.connectTimeoutMs=3000
                dev.cluster.nodes=node-1
                dev.node.node-1.client.host=127.0.0.1
                dev.node.node-1.client.port=9101
                """, StandardCharsets.UTF_8);

        // Env ghi đè connectTimeoutMs thành 4000
        Map<String, String> env = new HashMap<>();
        env.put("CLIENT_CONNECT_TIMEOUT_MS", "4000");

        ClientConfig confEnv = ClientConfig.loadWithOverrides(file, null, env);
        assertEquals(4000, confEnv.getConnectTimeoutMs(), "Biến môi trường phải ghi đè giá trị trong file");

        // System property ghi đè connectTimeoutMs thành 5000 (ưu tiên cao nhất)
        Map<String, String> sysProps = new HashMap<>();
        sysProps.put("client.connectTimeoutMs", "5000");

        ClientConfig confSys = ClientConfig.loadWithOverrides(file, sysProps, env);
        assertEquals(5000, confSys.getConnectTimeoutMs(), "System Property phải ghi đè Biến môi trường và File");
    }

    // =========================================================================
    // 4. Validation fail-fast
    // =========================================================================
    @Test
    @DisplayName("AC-D16-04: Validation fail-fast: thiếu Node, nodeId trùng, port sai, host rỗng, profile sai")
    void testValidationFailFast() {
        // Lỗi 1: Profile không tồn tại / không có Node
        Properties p1 = new Properties();
        p1.setProperty("cluster.profile", "non_existing_profile");
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(p1, "test-p1"));
        assertTrue(ex1.getMessage().contains("non_existing_profile"));

        // Lỗi 2: NodeId trùng lặp
        Properties p2 = new Properties();
        p2.setProperty("cluster.profile", "dev");
        p2.setProperty("dev.cluster.nodes", "node-1,node-1");
        p2.setProperty("dev.node.node-1.client.host", "127.0.0.1");
        p2.setProperty("dev.node.node-1.client.port", "9101");
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(p2, "test-p2"));
        assertTrue(ex2.getMessage().contains("trùng lặp"));

        // Lỗi 3: Port ngoài khoảng [1, 65535]
        Properties p3 = new Properties();
        p3.setProperty("cluster.profile", "dev");
        p3.setProperty("dev.cluster.nodes", "node-1");
        p3.setProperty("dev.node.node-1.client.host", "127.0.0.1");
        p3.setProperty("dev.node.node-1.client.port", "70000");
        IllegalArgumentException ex3 = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(p3, "test-p3"));
        assertTrue(ex3.getMessage().contains("Port"));

        // Lỗi 4: Host rỗng
        Properties p4 = new Properties();
        p4.setProperty("cluster.profile", "dev");
        p4.setProperty("dev.cluster.nodes", "node-1");
        p4.setProperty("dev.node.node-1.client.host", "   ");
        p4.setProperty("dev.node.node-1.client.port", "9101");
        IllegalArgumentException ex4 = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(p4, "test-p4"));
        assertTrue(ex4.getMessage().contains("Host"));

        // Lỗi 5: preferredNode không có trong danh sách
        Properties p5 = new Properties();
        p5.setProperty("cluster.profile", "dev");
        p5.setProperty("client.preferredNode", "node-99");
        p5.setProperty("dev.cluster.nodes", "node-1");
        p5.setProperty("dev.node.node-1.client.host", "127.0.0.1");
        p5.setProperty("dev.node.node-1.client.port", "9101");
        IllegalArgumentException ex5 = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(p5, "test-p5"));
        assertTrue(ex5.getMessage().contains("node-99"));
    }

    // =========================================================================
    // 5. Cảnh báo host Docker nội bộ
    // =========================================================================
    @Test
    @DisplayName("AC-D16-05: Cảnh báo host Docker nội bộ; 127.0.0.1 và IP LAN không bị cảnh báo")
    void testDockerInternalHostnameWarning() {
        // Tên dịch vụ nội bộ Docker
        assertTrue(ClientConfig.isDockerInternalHost("backend-node1"));
        assertTrue(ClientConfig.isDockerInternalHost("backend-node-1"));
        assertTrue(ClientConfig.isDockerInternalHost("pg1"));
        assertTrue(ClientConfig.isDockerInternalHost("postgres"));
        assertTrue(ClientConfig.isDockerInternalHost("some-internal-container"));

        // Địa chỉ hợp lệ trên Windows host / LAN
        assertFalse(ClientConfig.isDockerInternalHost("127.0.0.1"));
        assertFalse(ClientConfig.isDockerInternalHost("localhost"));
        assertFalse(ClientConfig.isDockerInternalHost("192.168.1.101"));
        assertFalse(ClientConfig.isDockerInternalHost("10.0.0.5"));
    }

    // =========================================================================
    // 6. Các chiến lược lựa chọn: PREFERRED_FIRST, RANDOM, ROUND_ROBIN
    // =========================================================================
    @Test
    @DisplayName("AC-D16-06: Chiến lược PREFERRED_FIRST, RANDOM (tiêm Random), ROUND_ROBIN (xoay vòng đủ 5 Node)")
    void testSelectionStrategies() {
        List<NodeEndpoint> endpoints = List.of(
                new NodeEndpoint("node-1", "127.0.0.1", 9101),
                new NodeEndpoint("node-2", "127.0.0.1", 9102),
                new NodeEndpoint("node-3", "127.0.0.1", 9103),
                new NodeEndpoint("node-4", "127.0.0.1", 9104),
                new NodeEndpoint("node-5", "127.0.0.1", 9105)
        );

        // PREFERRED_FIRST
        ClientConfig confPref = new ClientConfig(
                "dev", 2000, 5000, 3, "node-3", SelectionStrategy.PREFERRED_FIRST, 15000L, endpoints, null, "test"
        );
        ClusterNodeSelector selectorPref = new ClusterNodeSelector(confPref);
        assertEquals("node-3", selectorPref.selectNode().nodeId());

        // RANDOM với Random được tiêm
        ClientConfig confRand = new ClientConfig(
                "dev", 2000, 5000, 3, null, SelectionStrategy.RANDOM, 15000L, endpoints, null, "test"
        );
        ClusterNodeSelector selectorRand = new ClusterNodeSelector(confRand, new Random(12345), System::currentTimeMillis);
        Set<String> chosenIds = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            chosenIds.add(selectorRand.selectNode().nodeId());
        }
        assertTrue(chosenIds.size() > 1, "RANDOM phải chọn ngẫu nhiên nhiều Node");

        // ROUND_ROBIN: xoay vòng tuần tự qua đủ 5 Node
        ClientConfig confRr = new ClientConfig(
                "dev", 2000, 5000, 3, null, SelectionStrategy.ROUND_ROBIN, 15000L, endpoints, null, "test"
        );
        ClusterNodeSelector selectorRr = new ClusterNodeSelector(confRr);
        List<String> round1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            round1.add(selectorRr.selectNode().nodeId());
        }
        assertEquals(List.of("node-1", "node-2", "node-3", "node-4", "node-5"), round1);

        // Vòng 2 tiếp tục quay lại node-1
        assertEquals("node-1", selectorRr.selectNode().nodeId());
    }

    // =========================================================================
    // 7. Failover và Cooldown
    // =========================================================================
    @Test
    @DisplayName("AC-D16-07: Failover: Node lỗi bị cooldown -> chọn Node tiếp theo; hết Node -> lỗi kết nối rõ ràng")
    void testFailoverAndCooldown() {
        List<NodeEndpoint> endpoints = List.of(
                new NodeEndpoint("node-1", "127.0.0.1", 9101),
                new NodeEndpoint("node-2", "127.0.0.1", 9102),
                new NodeEndpoint("node-3", "127.0.0.1", 9103)
        );
        AtomicLong virtualTime = new AtomicLong(1000L);

        ClientConfig config = new ClientConfig(
                "dev", 2000, 5000, 3, null, SelectionStrategy.ROUND_ROBIN, 5000L, endpoints, null, "test"
        );
        ClusterNodeSelector selector = new ClusterNodeSelector(config, new Random(), virtualTime::get);

        // Ban đầu cả 3 Node đều khả dụng
        assertEquals(3, selector.getAvailableEndpoints().size());

        // Node 1 gặp lỗi kết nối
        selector.markFailed("node-1");
        assertFalse(selector.isAvailable("node-1"));
        assertEquals(2, selector.getAvailableEndpoints().size());

        // Node được chọn tiếp theo không phải là node-1
        NodeEndpoint next = selector.selectNode();
        assertTrue(next.nodeId().equals("node-2") || next.nodeId().equals("node-3"));

        // Node 2 và Node 3 cũng gặp lỗi
        selector.markFailed("node-2");
        selector.markFailed("node-3");
        assertEquals(0, selector.getAvailableEndpoints().size());

        // Hết Node khả dụng -> ném ngoại lệ kết nối rõ ràng
        IllegalStateException ex = assertThrows(IllegalStateException.class, selector::selectNode);
        assertTrue(ex.getMessage().contains("Không còn Node nào khả dụng trong cụm"));

        // Thời gian trôi qua quá thời gian cooldown (5000ms)
        virtualTime.addAndGet(6000L);
        assertTrue(selector.isAvailable("node-1"));
        assertTrue(selector.isAvailable("node-2"));
        assertTrue(selector.isAvailable("node-3"));

        // Lại có thể chọn Node bình thường
        assertNotNull(selector.selectNode());
    }

    // =========================================================================
    // 8. Không có Leader trong config / grep
    // =========================================================================
    @Test
    @DisplayName("AC-D16-08: Không có thuộc tính 'leader' trong client.properties mặc định; không gán sẵn Leader")
    void testNoHardcodedLeaderOrDockerNameInCode() throws IOException {
        ClientConfig config = ClientConfig.loadDefault();
        Properties props = config.getRawProperties();

        // Đảm bảo không có key nào chứa ".leader"
        for (String key : props.stringPropertyNames()) {
            assertFalse(key.toLowerCase().contains("leader"),
                    "Cấu hình client.properties TUYỆT ĐỐI không được chứa từ khóa 'leader': " + key);
        }

        // LeaderAwareBookingClient ban đầu không có knownLeader
        ClusterEndpoints endpoints = ClusterEndpoints.fromClientConfig(config);
        try (LeaderAwareBookingClient client = new LeaderAwareBookingClient(endpoints)) {
            assertNull(client.getKnownLeaderId(), "Ban đầu knownLeaderId phải là null (chưa biết ai là Leader)");
        }
    }

    // =========================================================================
    // 9. Đường dẫn Windows có khoảng trắng & tệp UTF-8 có tiếng Việt
    // =========================================================================
    @Test
    @DisplayName("AC-D16-09: Đường dẫn Windows có khoảng trắng/ký tự đặc biệt, tệp UTF-8 tiếng Việt đọc đúng")
    void testWindowsPathWithSpacesAndUtf8VietnameseComments(@TempDir Path tempDir) throws IOException {
        Path specialDir = tempDir.resolve("Thu Muc Chon Node - Tàu Cao Tốc (Nhóm 6)");
        Files.createDirectories(specialDir);
        Path configFile = specialDir.resolve("cấu hình client.properties");

        String content = """
                # ====================================================================
                # CẤU HÌNH KIỂM THỬ TIẾNG VIỆT CÓ DẤU
                # Chuyến tàu: SE-001 Hà Nội - Sài Gòn
                # ====================================================================
                cluster.profile=dev
                client.preferredNode=node-1
                dev.cluster.nodes=node-1
                dev.node.node-1.client.host=127.0.0.1
                dev.node.node-1.client.port=9101
                """;
        Files.writeString(configFile, content, StandardCharsets.UTF_8);

        ClientConfig config = ClientConfig.fromFile(configFile);
        assertNotNull(config);
        assertEquals("dev", config.getProfile());
        assertEquals("node-1", config.getPreferredNode());
        assertEquals(1, config.getEndpoints().size());
        assertEquals("127.0.0.1", config.getEndpoint("node-1").host());
    }

    // =========================================================================
    // 10. ClusterConnectivityCheck với FakeNodeServer
    // =========================================================================
    @Test
    @DisplayName("AC-D16-10: ClusterConnectivityCheck với FakeNodeServer: node UP, node DOWN, lệch nodeId")
    void testClusterConnectivityCheckWithFakeNodeServers() throws Exception {
        // Khởi động 2 Fake Server trên cổng ngẫu nhiên
        try (FakeNodeServer server1 = new FakeNodeServer("node-1");
             FakeNodeServer server2 = new FakeNodeServer("node-2-real")) {

            int port1 = server1.getPort();
            int port2 = server2.getPort();
            int downPort = 65530; // Cổng không có server lắng nghe

            List<NodeEndpoint> testEndpoints = List.of(
                    new NodeEndpoint("node-1", "127.0.0.1", port1),
                    new NodeEndpoint("node-2", "127.0.0.1", port2), // Cấu hình là node-2 nhưng server trả về node-2-real -> lệch nodeId
                    new NodeEndpoint("node-3", "127.0.0.1", downPort) // Server tắt -> DOWN
            );

            ClusterCheckSummary summary = ClusterConnectivityCheck.checkCluster(
                    testEndpoints, 1000, DataSource.FAKE
            );

            assertEquals(3, summary.totalNodes());
            assertEquals(2, summary.reachableNodes());

            // Node 1: UP và khớp nodeId
            NodeCheckResult r1 = summary.results().get(0);
            assertTrue(r1.reachable());
            assertEquals("node-1", r1.reportedNodeId());
            assertTrue(r1.nodeIdMatched());
            assertEquals(DataSource.FAKE, r1.dataSource());

            // Node 2: UP nhưng lệch nodeId
            NodeCheckResult r2 = summary.results().get(1);
            assertTrue(r2.reachable());
            assertEquals("node-2-real", r2.reportedNodeId());
            assertFalse(r2.nodeIdMatched(), "Node 2 phải bị phát hiện lệch nodeId");

            // Node 3: DOWN
            NodeCheckResult r3 = summary.results().get(2);
            assertFalse(r3.reachable());
            assertNotNull(r3.errorMessage());
        }
    }

    // =========================================================================
    // Test với Cụm thật (chỉ chạy khi có biến môi trường CLUSTER_PROFILE=dev)
    // =========================================================================
    @Test
    @EnabledIfEnvironmentVariable(named = "CLUSTER_PROFILE", matches = "dev")
    @DisplayName("AC-D16-11: [REAL CLUSTER] Kiểm tra kết nối tới cụm dev thật khi có CLUSTER_PROFILE=dev")
    void testWithRealClusterWhenAvailable() {
        ClientConfig config = ClientConfig.loadDefault();
        ClusterCheckSummary summary = ClusterConnectivityCheck.checkCluster(
                config.getEndpoints(), config.getConnectTimeoutMs(), DataSource.REAL
        );
        System.out.println(summary.toFormattedSummary());
        assertEquals(5, summary.totalNodes());
        assertEquals(5, summary.reachableNodes(), "Yêu cầu cả 5 Node dev thật đều phản hồi");
    }
}
