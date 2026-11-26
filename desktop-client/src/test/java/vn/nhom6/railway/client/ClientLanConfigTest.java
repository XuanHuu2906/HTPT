package vn.nhom6.railway.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.NodeEndpoint;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử cấu hình LAN profile từ client-lan.properties và ClientConfig (TASK TV1-D25).
 * Tuân thủ AGENTS.md:
 * - 5 Node Voter: node-1..5, cổng Client chuẩn 9100.
 * - Nhận diện và từ chối IP placeholder chờ TV6-D24 khi chạy ở chế độ thật.
 * - Đảm bảo tính duy nhất của nguồn chân lý cấu hình LAN.
 */
public class ClientLanConfigTest {

    @Test
    @DisplayName("TC_LAN_CFG_01: Nạp thành công profile lan với đủ 5 node và cổng client 9100")
    void testLoadLanConfigFromClasspath() {
        ClientConfig config = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), null);

        assertNotNull(config, "ClientConfig không được null khi nạp profile lan");
        assertEquals("lan", config.getProfile(), "Profile phải là 'lan'");
        assertEquals(5, config.getEndpoints().size(), "Profile LAN phải cấu hình đủ 5 node");

        List<String> nodeIds = config.getNodeIds();
        assertTrue(nodeIds.containsAll(List.of("node-1", "node-2", "node-3", "node-4", "node-5")),
                "Phải chứa đầy đủ định danh node-1 đến node-5");

        for (NodeEndpoint ep : config.getEndpoints()) {
            assertEquals(9100, ep.port(), "Cổng kết nối Client của node " + ep.nodeId() + " bắt buộc phải là 9100");
        }
    }

    @Test
    @DisplayName("TC_LAN_CFG_02: Đối chiếu cổng Client và Node ID khớp với template configs/lan/node-1..5.properties")
    void testCompareWithNodePropertiesTemplates() throws Exception {
        Path baseDir = Paths.get("configs", "lan");
        if (!Files.exists(baseDir)) {
            baseDir = Paths.get("..", "configs", "lan");
        }
        assertTrue(Files.exists(baseDir), "Thư mục configs/lan phải tồn tại trong repository");

        ClientConfig clientConfig = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), null);

        for (int i = 1; i <= 5; i++) {
            String nodeId = "node-" + i;
            Path propFile = baseDir.resolve(nodeId + ".properties");
            assertTrue(Files.exists(propFile), "Tệp cấu hình node " + propFile + " phải tồn tại");

            Properties p = new Properties();
            try (BufferedReader reader = Files.newBufferedReader(propFile, StandardCharsets.UTF_8)) {
                p.load(reader);
            }

            assertEquals(nodeId, p.getProperty("node.id"), "node.id trong template phải khớp với " + nodeId);
            assertEquals("9100", p.getProperty("client.port"), "client.port trong template phải là 9100");

            NodeEndpoint ep = clientConfig.getEndpoint(nodeId);
            assertNotNull(ep, "NodeEndpoint " + nodeId + " phải tồn tại trong ClientConfig LAN");
            assertEquals(9100, ep.port(), "Port Client trong ClientConfig phải khớp với template (9100)");
        }
    }

    @Test
    @DisplayName("TC_LAN_CFG_03: Phát hiện và từ chối IP placeholder khi gọi validateNoPlaceholders")
    void testDetectPlaceholders() {
        ClientConfig config = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), null);

        assertTrue(config.hasPlaceholderEndpoints(), "client-lan.properties mặc định phải chứa IP placeholder");
        List<String> placeholders = config.getPlaceholderNodes();
        assertEquals(5, placeholders.size(), "Cả 5 node hiện tại phải ở trạng thái placeholder chờ TV6-D24");

        IllegalStateException ex = assertThrows(IllegalStateException.class, config::validateNoPlaceholders,
                "validateNoPlaceholders phải ném IllegalStateException khi còn IP placeholder");
        assertTrue(ex.getMessage().contains("TV6-D24"), "Thông điệp lỗi phải nêu rõ chờ TV6-D24 bàn giao");
        assertTrue(ex.getMessage().contains("placeholder"), "Thông điệp lỗi phải có từ khóa placeholder");
    }

    @Test
    @DisplayName("TC_LAN_CFG_04: Đảm bảo client.properties không còn danh sách lan.cluster.nodes trùng lặp")
    void testNoSecondLanClusterInClientProperties() throws Exception {
        Properties clientProps = new Properties();
        try (InputStream is = getClass().getResourceAsStream("/config/client.properties")) {
            assertNotNull(is, "Tệp /config/client.properties phải tồn tại trong classpath");
            try (InputStreamReader r = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                clientProps.load(r);
            }
        }

        assertNull(clientProps.getProperty("lan.cluster.nodes"),
                "client.properties không được định nghĩa lan.cluster.nodes (nguồn chân lý LAN đã chuyển sang client-lan.properties)");
    }

    @Test
    @DisplayName("TC_LAN_CFG_05: Ghi đè bằng IP thật hợp lệ thì validateNoPlaceholders thành công")
    void testOverridePlaceholderWithValidLanIp() {
        Map<String, String> realIpOverrides = new HashMap<>();
        realIpOverrides.put("cluster.profile", "lan");
        realIpOverrides.put("lan.node.node-1.client.host", "10.0.0.101");
        realIpOverrides.put("lan.node.node-2.client.host", "10.0.0.102");
        realIpOverrides.put("lan.node.node-3.client.host", "10.0.0.103");
        realIpOverrides.put("lan.node.node-4.client.host", "10.0.0.104");
        realIpOverrides.put("lan.node.node-5.client.host", "10.0.0.105");

        ClientConfig config = ClientConfig.loadWithOverrides(null, realIpOverrides, null);

        assertFalse(config.hasPlaceholderEndpoints(), "Sau khi ghi đè bằng IP thật thì không còn placeholder");
        assertEquals(0, config.getPlaceholderNodes().size(), "Danh sách placeholder phải rỗng");
        assertDoesNotThrow(config::validateNoPlaceholders, "validateNoPlaceholders không được ném lỗi khi IP hợp lệ");
    }
}
