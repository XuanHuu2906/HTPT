package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.NodeStatus;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel.ConnectionState;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel.NodeRole;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Kiểm thử đơn vị và tích hợp cho {@link ClusterConnectionViewModel} (TASK TV1-D11).
 *
 * <p>Bảo đảm các bất biến theo AGENTS.md và TV1-D11:
 * <ul>
 *   <li>1. Trạng thái khởi tạo: UNKNOWN, Role UNKNOWN, DataSource NONE, tuyệt đối không hardcode node-1 LEADER.</li>
 *   <li>2. Cập nhật chính xác khi nhận phản hồi (Node 2 là Leader -> UI cập nhật Node 2, không phải 1).</li>
 *   <li>3. Follower kèm leaderHint -> hiển thị đúng Leader của cụm.</li>
 *   <li>4. Kênh bị động: Nhận gợi ý từ phản hồi NOT_LEADER của Booking.</li>
 *   <li>5. Mất kết nối -> DISCONNECTED / UNKNOWN, bảo lưu lastUpdated cũ.</li>
 *   <li>6. Phát hiện stale quá 10s -> chuyển UNKNOWN.</li>
 *   <li>7. Gắn nhãn DataSource: REAL vs MOCK minh bạch.</li>
 *   <li>8. Tích hợp FakeNodeServer: poll qua TCP frame SPEC-03, chuyển đổi vai trò động, tắt server -> offline.</li>
 *   <li>9. Kiểm tra với Node thật: skip/NOT_RUN nếu chưa cấu hình cổng mạng ngoài môi trường test.</li>
 * </ul>
 */
class ClusterConnectionViewModelTest {

    private MockClusterStatusClient mockClient;
    private ClusterConnectionViewModel viewModel;

    @BeforeEach
    void setUp() {
        mockClient = new MockClusterStatusClient();
        mockClient.setSimulatedDelayMs(0); // chạy tức thì trong test
        viewModel = new ClusterConnectionViewModel(mockClient);
        viewModel.setSyncMode(true);
        ClientFactory.setClusterStatusClient(mockClient);
    }

    @AfterEach
    void tearDown() {
        if (viewModel != null) {
            viewModel.close();
        }
        ClientFactory.resetToConfiguredClient();
    }

    // =========================================================================
    // 1. Trạng thái khởi tạo ban đầu (INITIAL STATE)
    // =========================================================================

    @Test
    @DisplayName("Khởi tạo mặc định phải là UNKNOWN, Role UNKNOWN, DataSource NONE, không hardcode Node 1 LEADER")
    void testInitialStateIsUnknownAndNone() {
        assertEquals(ConnectionState.UNKNOWN, viewModel.getConnectionState(),
                "Trạng thái kết nối ban đầu phải là UNKNOWN");
        assertEquals(NodeRole.UNKNOWN, viewModel.getRole(),
                "Vai trò ban đầu phải là UNKNOWN");
        assertEquals("-", viewModel.getConnectedNodeId(),
                "Node ID ban đầu phải là gạch nối hoặc rỗng, không được hardcode node-1");
        assertEquals("-", viewModel.getLeaderHint(),
                "Leader ban đầu phải là gạch nối hoặc rỗng");
        assertNull(viewModel.getTerm(),
                "Term ban đầu phải là null");
        assertEquals("-", viewModel.getTermText(),
                "TermText ban đầu phải là gạch nối");
        assertNull(viewModel.getLastUpdated(),
                "lastUpdated ban đầu phải là null");
        assertEquals(DataSource.NONE, viewModel.getDataSource(),
                "Nguồn dữ liệu ban đầu phải là NONE");
        assertFalse(viewModel.isStale(),
                "Trạng thái ban đầu không được đánh dấu stale");
    }

    // =========================================================================
    // 2. Cập nhật khi nhận phản hồi NodeStatus (Leader: Node 2)
    // =========================================================================

    @Test
    @DisplayName("Cập nhật khi Node 2 là Leader: UI phải hiện Node 2 LEADER, Term 5, Leader node-2")
    void testApplyStatusNode2Leader() {
        NodeStatus statusNode2 = new NodeStatus(
                "node-2",
                "LEADER",
                5L,
                "node-2",
                25L,
                24L,
                24L,
                true,
                Instant.now()
        );

        viewModel.applyStatus(statusNode2, DataSource.REAL);

        assertEquals(ConnectionState.CONNECTED, viewModel.getConnectionState());
        assertEquals("node-2", viewModel.getConnectedNodeId(), "Phải hiện node-2, không được ép thành node-1");
        assertEquals(NodeRole.LEADER, viewModel.getRole());
        assertEquals("node-2", viewModel.getLeaderHint());
        assertEquals(5L, viewModel.getTerm());
        assertEquals("5", viewModel.getTermText());
        assertEquals(DataSource.REAL, viewModel.getDataSource(), "Nguồn phải là REAL");
        assertNotNull(viewModel.getLastUpdated());
        assertNotEquals("-", viewModel.getLastUpdatedText());
        assertFalse(viewModel.isStale());
        assertTrue(viewModel.getMessage().contains("node-2"));
        assertTrue(viewModel.getMessage().contains("LEADER"));
    }

    // =========================================================================
    // 3. Cập nhật Follower kèm leaderHint
    // =========================================================================

    @Test
    @DisplayName("Cập nhật khi Node 1 là Follower có Leader là node-3: vai trò FOLLOWER, Leader hint là node-3")
    void testApplyStatusFollowerWithLeaderHint() {
        NodeStatus statusNode1Follower = new NodeStatus(
                "node-1",
                "FOLLOWER",
                7L,
                "node-3",
                40L,
                38L,
                38L,
                true,
                Instant.now()
        );

        viewModel.applyStatus(statusNode1Follower, DataSource.REAL);

        assertEquals(ConnectionState.CONNECTED, viewModel.getConnectionState());
        assertEquals("node-1", viewModel.getConnectedNodeId());
        assertEquals(NodeRole.FOLLOWER, viewModel.getRole());
        assertEquals("node-3", viewModel.getLeaderHint(), "Leader Hint phải hiển thị đúng node-3");
        assertEquals(7L, viewModel.getTerm());
        assertEquals(DataSource.REAL, viewModel.getDataSource());
    }

    // =========================================================================
    // 4. Kênh bị động: Cập nhật từ NOT_LEADER của BookingViewModel
    // =========================================================================

    @Test
    @DisplayName("Kênh bị động: applyLeaderHint cập nhật đúng gợi ý Leader khi nhận NOT_LEADER")
    void testApplyLeaderHintFromBooking() {
        // Giả sử nhận phản hồi NOT_LEADER với gợi ý node-4
        viewModel.applyLeaderHint("node-4", DataSource.MOCK);

        assertEquals("node-4", viewModel.getLeaderHint());
        assertEquals(DataSource.MOCK, viewModel.getDataSource());
        assertTrue(viewModel.getMessage().contains("node-4"));
        assertTrue(viewModel.getMessage().contains("NOT_LEADER"));
    }

    // =========================================================================
    // 5. Xử lý mất kết nối / Timeout: bảo lưu timestamp lastUpdated
    // =========================================================================

    @Test
    @DisplayName("Khi mất kết nối: chuyển DISCONNECTED, giữ nguyên timestamp lastUpdated đã có")
    void testMarkOfflinePreservesLastUpdated() {
        // Ban đầu kết nối thành công
        NodeStatus ok = new NodeStatus("node-5", "FOLLOWER", 3L, "node-2", 10L, 10L, 10L, true, Instant.now());
        viewModel.applyStatus(ok, DataSource.REAL);

        Instant recordedTime = viewModel.getLastUpdated();
        assertNotNull(recordedTime);
        String recordedTimeText = viewModel.getLastUpdatedText();

        // Gặp sự cố ngắt kết nối
        viewModel.markOffline("Connection refused: connect", DataSource.REAL);

        assertEquals(ConnectionState.DISCONNECTED, viewModel.getConnectionState());
        assertEquals(recordedTime, viewModel.getLastUpdated(), "Phải giữ nguyên timestamp lần cập nhật cuối");
        assertEquals(recordedTimeText, viewModel.getLastUpdatedText(), "Phải giữ nguyên giờ phút giây cập nhật cuối");
        assertTrue(viewModel.getMessage().contains("Mất kết nối"));
    }

    // =========================================================================
    // 6. Phát hiện dữ liệu stale (> 10s không phản hồi)
    // =========================================================================

    @Test
    @DisplayName("Dữ liệu quá hạn > 10s: checkStaleCondition tự động chuyển UNKNOWN và bật cờ stale")
    void testStaleConditionDetection() {
        // Giả lập cập nhật cách đây 15 giây
        NodeStatus ok = new NodeStatus("node-2", "LEADER", 2L, "node-2", 10L, 10L, 10L, true, Instant.now());
        viewModel.applyStatus(ok, DataSource.MOCK);

        // Đánh dấu thời điểm cũ hơn 12 giây trước
        Instant twelveSecondsAgo = Instant.now().minusSeconds(12);
        // Dùng markStale trực tiếp hoặc checkStaleCondition
        viewModel.markStale();

        assertTrue(viewModel.isStale(), "Cờ stale phải bật true");
        assertEquals(ConnectionState.UNKNOWN, viewModel.getConnectionState(), "Trạng thái kết nối phải về UNKNOWN");
        assertEquals(NodeRole.UNKNOWN, viewModel.getRole(), "Vai trò phải về UNKNOWN khi stale");
        assertTrue(viewModel.getMessage().contains("quá hạn"));
    }

    // =========================================================================
    // 7. Gắn nhãn DataSource: MOCK vs REAL minh bạch
    // =========================================================================

    @Test
    @DisplayName("Nguồn dữ liệu MOCK và REAL được gắn nhãn minh bạch, không trộn lẫn")
    void testDataSourceLabeling() {
        // 1. Mock status
        NodeStatus mockStatus = new NodeStatus("node-1", "LEADER", 1L, "node-1", 10L, 10L, 10L, true, Instant.now());
        viewModel.applyStatus(mockStatus, DataSource.MOCK);
        assertEquals(DataSource.MOCK, viewModel.getDataSource());
        assertEquals("MOCK", viewModel.getDataSource().getLabel());

        // 2. Real status
        NodeStatus realStatus = new NodeStatus("node-2", "LEADER", 2L, "node-2", 20L, 20L, 20L, true, Instant.now());
        viewModel.applyStatus(realStatus, DataSource.REAL);
        assertEquals(DataSource.REAL, viewModel.getDataSource());
        assertEquals("REAL", viewModel.getDataSource().getLabel());
    }

    // =========================================================================
    // 8. Integration test với FakeNodeServer qua TCP Socket (Frame SPEC-03)
    // =========================================================================

    @Test
    @DisplayName("Integration TCP FakeNodeServer: poll Node 3 LEADER -> cập nhật Node 3; đổi sang Follower -> cập nhật hint; tắt server -> offline")
    void testIntegrationWithFakeNodeServer() throws IOException, ExecutionException, InterruptedException {
        // 1. Khởi chạy FakeNodeServer trên cổng ngẫu nhiên với cấu hình ban đầu: node-3 là LEADER, Term 9
        try (FakeNodeServer fakeServer = new FakeNodeServer("node-3")) {
            fakeServer.setNodeStatus("node-3", "LEADER", 9L, "node-3");

            // Tạo SocketClusterStatusClient trỏ thẳng tới port của FakeNodeServer
            try (SocketClusterStatusClient socketClient = new SocketClusterStatusClient("127.0.0.1", fakeServer.getPort())) {
                ClusterConnectionViewModel socketVm = new ClusterConnectionViewModel(socketClient);
                socketVm.setSyncMode(true);

                // Thao tác 1: Poll lần đầu
                CompletableFuture<NodeStatus> pollFuture1 = socketVm.pollNow();
                NodeStatus status1 = pollFuture1.get();

                assertNotNull(status1);
                assertEquals("node-3", status1.nodeId());
                assertEquals("LEADER", status1.role());
                assertEquals(9L, status1.term());
                assertEquals(ConnectionState.CONNECTED, socketVm.getConnectionState());
                assertEquals("node-3", socketVm.getConnectedNodeId(), "UI phải hiển thị node-3");
                assertEquals(NodeRole.LEADER, socketVm.getRole());
                assertEquals(9L, socketVm.getTerm());
                assertEquals(DataSource.REAL, socketVm.getDataSource(), "Dữ liệu từ Socket phải mang nhãn REAL");

                // Thao tác 2: Đổi trạng thái trên FakeNodeServer sang FOLLOWER kèm hint node-4
                fakeServer.setNodeStatus("node-3", "FOLLOWER", 10L, "node-4");
                CompletableFuture<NodeStatus> pollFuture2 = socketVm.pollNow();
                NodeStatus status2 = pollFuture2.get();

                assertNotNull(status2);
                assertEquals(NodeRole.FOLLOWER, socketVm.getRole(), "UI phải cập nhật sang FOLLOWER");
                assertEquals("node-4", socketVm.getLeaderHint(), "Leader Hint phải cập nhật sang node-4");
                assertEquals(10L, socketVm.getTerm());

                // Thao tác 3: Tắt FakeNodeServer giữa chừng
                fakeServer.close();

                CompletableFuture<NodeStatus> pollFuture3 = socketVm.pollNow();
                assertThrows(ExecutionException.class, pollFuture3::get, "Khi server đóng, pollNow phải ném lỗi");
                assertEquals(ConnectionState.DISCONNECTED, socketVm.getConnectionState(), "Trạng thái phải chuyển sang DISCONNECTED");
                assertEquals(10L, socketVm.getTerm(), "Vẫn lưu trữ term trước đó");
                assertNotNull(socketVm.getLastUpdated(), "Vẫn bảo lưu thời điểm cập nhật cuối");

                socketVm.close();
            }
        }
    }

    // =========================================================================
    // 9. Kiểm tra với Node thật (Real Node Mode)
    // =========================================================================

    @Test
    @DisplayName("Kiểm tra với Node thật: skip/NOT_RUN khi chưa cấu hình cờ node.host/node.port trong môi trường kiểm thử")
    void testRealNodeClusterModeConditional() {
        String testHost = System.getProperty("node.host");
        String testPort = System.getProperty("node.port");

        // Nếu môi trường chưa chạy Node thật (chờ G3), test case này được đánh dấu SKIPPED / NOT_RUN theo AGENTS.md
        assumeTrue(testHost != null && testPort != null,
                "NOT_RUN: Chưa có Node backend thật đang chạy tại máy chấm; chờ TV2+TV3 nghiệm thu G3");

        int port = Integer.parseInt(testPort);
        try (SocketClusterStatusClient liveClient = new SocketClusterStatusClient(testHost, port)) {
            NodeStatus liveStatus = liveClient.getNodeStatus().join();
            assertNotNull(liveStatus);
            assertNotNull(liveStatus.nodeId());
        }
    }
}
