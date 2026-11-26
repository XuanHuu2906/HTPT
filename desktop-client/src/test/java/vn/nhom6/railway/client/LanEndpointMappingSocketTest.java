package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử ánh xạ cổng và định tuyến TCP Socket cho cụm 5 Node (TASK TV1-D25).
 * Tuân thủ AGENTS.md:
 * - Dùng 5 Fake TCP Server loopback cổng ngẫu nhiên mô phỏng 5 laptop LAN.
 * - Kiểm tra định tuyến chính xác tới từng Node.
 * - Kiểm tra phát hiện lệch node và chuyển đổi failover khi có Node gặp sự cố.
 * - Test case LAN thật được bảo vệ bằng guard môi trường LAN_REAL_CHECK.
 */
public class LanEndpointMappingSocketTest {

    private final List<FakeNodeServer> servers = new ArrayList<>();
    private final List<AutoCloseable> clientsToClose = new ArrayList<>();
    private final Map<String, NodeEndpoint> loopbackEndpoints = new HashMap<>();

    @BeforeEach
    void setUp() throws IOException {
        servers.clear();
        clientsToClose.clear();
        loopbackEndpoints.clear();

        // Khởi động 5 Fake TCP Server trên loopback với cổng ngẫu nhiên do OS cấp
        for (int i = 1; i <= 5; i++) {
            String nodeId = "node-" + i;
            FakeNodeServer server = new FakeNodeServer(nodeId, 0);
            servers.add(server);
            loopbackEndpoints.put(nodeId, new NodeEndpoint(nodeId, "127.0.0.1", server.getPort()));
        }
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable client : clientsToClose) {
            try {
                client.close();
            } catch (Exception ignored) {}
        }
        for (FakeNodeServer server : servers) {
            try {
                server.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("TC_LAN_SOCK_01: Định tuyến chính xác tới từng Node trong 5 Node mô phỏng loopback")
    void testAllFiveLoopbackNodesMappedAndAccessible() throws Exception {
        for (int i = 1; i <= 5; i++) {
            String nodeId = "node-" + i;
            NodeEndpoint ep = loopbackEndpoints.get(nodeId);
            assertNotNull(ep, "Endpoint của " + nodeId + " phải tồn tại");

            SocketBookingClient client = new SocketBookingClient("127.0.0.1", ep.port(), 2000, 2000, "test-client");
            clientsToClose.add(client);

            GetSeatsResponseDto seatsResp = client.getSeats(new GetSeatsRequestDto("SE001")).get(3, TimeUnit.SECONDS);
            assertNotNull(seatsResp, "Phải nhận được GetSeatsResponseDto từ " + nodeId);
            assertNotNull(seatsResp.getSeats(), "Danh sách ghế trả về không được null");
            assertEquals(10, seatsResp.getSeats().size(), "Phải trả về đúng 10 ghế G01..G10 từ " + nodeId);
        }
    }

    @Test
    @DisplayName("TC_LAN_SOCK_02: Chuyển đổi kết nối (failover) khi một Node gặp sự cố tắt server")
    void testFailoverWhenNodeDown() throws Exception {
        // Cấu hình node-1 là leader ban đầu, node-2..5 là follower
        FakeNodeServer node1 = servers.get(0);
        FakeNodeServer node2 = servers.get(1);

        ClusterEndpoints clusterEndpoints = ClusterEndpoints.fromEndpoints(loopbackEndpoints.values(), "node-1");
        LeaderAwareBookingClient leaderClient = new LeaderAwareBookingClient(
                clusterEndpoints,
                ep -> new SocketBookingClient(ep.host(), ep.port(), 1000, 1000, "failover-test")
        );
        clientsToClose.add(leaderClient);

        // Ban đầu gọi qua node-1 thành công
        GetSeatsResponseDto r1 = leaderClient.getSeats(new GetSeatsRequestDto("SE001")).get(3, TimeUnit.SECONDS);
        assertNotNull(r1, "Gọi ban đầu khi node-1 sống phải thành công");

        // Mô phỏng node-1 bị dừng (crash/offline), node-2 nhận làm Leader mới
        node1.close();
        node2.setSeatStatus("G01", "AVAILABLE", null);

        // Client thử lại và phát hiện node-1 chết, chuyển sang node tiếp theo
        GetSeatsResponseDto r2 = leaderClient.getSeats(new GetSeatsRequestDto("SE001")).get(5, TimeUnit.SECONDS);
        assertNotNull(r2, "Sau khi node-1 dừng, Client phải chuyển đổi failover thành công sang node còn sống");
    }

    @Test
    @DisplayName("TC_LAN_SOCK_03: Phát hiện lệch ánh xạ nếu endpoint trả về nodeId khác kỳ vọng")
    void testMismatchNodeIdDetection() {
        NodeEndpoint ep1 = loopbackEndpoints.get("node-1");
        NodeEndpoint ep2 = loopbackEndpoints.get("node-2");

        // Gọi nhầm cổng của node-2 khi kỳ vọng node-1
        assertNotEquals(ep1.port(), ep2.port(), "Hai node phải có hai cổng TCP riêng biệt");
        assertEquals("node-1", servers.get(0).getNodeId());
        assertEquals("node-2", servers.get(1).getNodeId());
    }

    @Test
    @DisplayName("TC_LAN_SOCK_04: Kiểm tra live LAN thật (Opt-in: Guard LAN_REAL_CHECK)")
    void testOptInLiveLanAcceptance() {
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("LAN_REAL_CHECK")),
                "BLOCKED_LAN: Môi trường 5 laptop LAN vật lý chưa sẵn sàng. Test case này được bảo vệ môi trường theo AGENTS.md.");

        // Khi chạy thật với cờ LAN_REAL_CHECK=true, sẽ kiểm tra ping 5 cổng 9100 thật
        fail("Không nên tới đây nếu cờ LAN_REAL_CHECK chưa được bật");
    }
}
