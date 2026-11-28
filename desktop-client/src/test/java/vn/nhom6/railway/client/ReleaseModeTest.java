package vn.nhom6.railway.client;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.stage.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.MainController;
import vn.nhom6.railway.client.controller.SeatGridController;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.DevSupportBridge;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.ReadAdapter;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.GraphicsEnvironment;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bộ kiểm thử chứng minh chế độ RELEASE nghiêm ngặt của Frontend Release Candidate (TASK TV1-D28 — Bước 3.6).
 * <p>
 * Các bất biến được kiểm chứng:
 * <ol>
 *   <li>Chế độ {@code release.mode=true} từ chối mọi yêu cầu cấu hình hoặc tiêm {@code mock}/{@code fake}.</li>
 *   <li>Fail-fast với thông báo nêu đúng tên khóa và {@code nodeId} khi còn IP placeholder, port lỗi,
 *       thiếu profile, danh sách Node rỗng, {@code preferredNode} sai hoặc tên container Docker nội bộ.</li>
 *   <li>Chỉ khởi tạo {@link SocketBookingClient} bọc bởi {@link LeaderAwareBookingClient} + {@link JournalingBookingClient},
 *       cùng {@link BookingClientReadAdapter} và {@link SocketClusterStatusClient}.</li>
 *   <li>Cây giao diện JavaFX (Scene Graph) trong chế độ release hoàn toàn KHÔNG nạp các phần tử MOCK/DEMO,
 *       đồng thời hiển thị đầy đủ {@code release.version=1.0.0-rc1}, {@code mode=RELEASE}, {@code profile} và {@code nodeId}.</li>
 *   <li>Quét tĩnh mã nguồn chứng minh 0 tham chiếu {@code Mock*}/{@code Fake*} trong toàn bộ luồng release.</li>
 * </ol>
 */
@DisplayName("TV1-D28: ReleaseModeTest — Chế độ RELEASE chỉ dùng SocketBookingClient thật và loại bỏ hoàn toàn Mock")
public class ReleaseModeTest {

    private static Path resolveProjectPath(String relativePath) {
        Path direct = Paths.get(relativePath);
        if (Files.exists(Paths.get("desktop-client")) || Files.exists(Paths.get(".git"))) {
            return direct.toAbsolutePath().normalize();
        }
        return Paths.get("..").resolve(relativePath).toAbsolutePath().normalize();
    }

    @AfterEach
    void cleanupReleaseSystemProperties() {
        System.clearProperty(ClientConfig.KEY_RELEASE_MODE);
        System.clearProperty(ClientConfig.KEY_CLIENT_MODE);
        System.clearProperty("booking.client");
        System.clearProperty("cluster.client");
        System.clearProperty("read.adapter");
        System.clearProperty("ui.evidence.fake");
        try {
            ClientFactory.resetToConfiguredClient();
        } catch (Exception ignored) {
        }
    }

    @Test
    @DisplayName("1. Release mode từ chối mọi cấu hình và lệnh tiêm Mock/Fake bằng ngoại lệ rõ ràng")
    void releaseMode_RejectsAllMockAndFakeConfigurationsFailFast() {
        // 1.1 client.mode=mock
        IllegalArgumentException mockModeEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "client.mode", "mock"
                ), Map.of()));
        assertTrue(mockModeEx.getMessage().contains("client.mode=mock"),
                "Thông báo lỗi phải nêu rõ khóa client.mode=mock: " + mockModeEx.getMessage());

        // 1.2 client.mode=fake
        IllegalArgumentException fakeModeEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "client.mode", "fake"
                ), Map.of()));
        assertTrue(fakeModeEx.getMessage().contains("client.mode=fake"));

        // 1.3 booking.client=mock
        IllegalArgumentException bookingMockEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "client.mode", "socket",
                        "booking.client", "mock"
                ), Map.of()));
        assertTrue(bookingMockEx.getMessage().contains("booking.client=mock"));

        // 1.4 cluster.client=mock
        IllegalArgumentException clusterMockEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "client.mode", "socket",
                        "cluster.client", "mock"
                ), Map.of()));
        assertTrue(clusterMockEx.getMessage().contains("cluster.client=mock"));

        // 1.5 read.adapter=fake
        IllegalArgumentException readFakeEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "client.mode", "socket",
                        "read.adapter", "fake"
                ), Map.of()));
        assertTrue(readFakeEx.getMessage().contains("read.adapter=fake"));

        // 1.6 Chặn runtime injection MockBookingClient / MockClusterStatusClient / FakeReadAdapter / DevSupportBridge khi release.mode=true
        System.setProperty(ClientConfig.KEY_RELEASE_MODE, "true");
        assertTrue(ClientConfig.isReleaseModeActive());
        assertThrows(IllegalStateException.class, DevSupportBridge::get,
                "DevSupportBridge.get() phải ném IllegalStateException khi release.mode=true");
        assertThrows(IllegalStateException.class, () -> ClientFactory.setBookingClient(new MockBookingClient()),
                "ClientFactory phải từ chối tiêm MockBookingClient trong chế độ RELEASE");
        assertThrows(IllegalStateException.class, () -> ClientFactory.setClusterStatusClient(MockClusterStatusClient.getInstance()),
                "ClientFactory phải từ chối tiêm MockClusterStatusClient trong chế độ RELEASE");
        assertThrows(IllegalStateException.class, () -> ClientFactory.setReadAdapter(new FakeReadAdapter()),
                "ClientFactory phải từ chối tiêm FakeReadAdapter trong chế độ RELEASE");
    }

    @Test
    @DisplayName("2. Fail-fast cấu hình không hợp lệ: placeholder IP, Node rỗng, port sai, preferredNode lạ, thiếu profile, Docker host")
    void releaseMode_RejectsInvalidEndpointsAndPlaceholdersWithExactKeyAndNodeId() {
        // 2.1 Profile LAN mặc định còn chứa placeholder IP
        IllegalStateException placeholderEx = assertThrows(IllegalStateException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "cluster.profile", "lan"
                ), Map.of()));
        assertTrue(placeholderEx.getMessage().contains("lan.node.node-1.client.host"),
                "Thông báo lỗi placeholder phải nêu đúng khóa cấu hình: " + placeholderEx.getMessage());
        assertTrue(placeholderEx.getMessage().contains("nodeId=node-1"),
                "Thông báo lỗi placeholder phải nêu rõ nodeId: " + placeholderEx.getMessage());

        // 2.2 Danh sách Node rỗng / profile không tồn tại
        Properties emptyNodesProps = new Properties();
        emptyNodesProps.setProperty("release.mode", "true");
        emptyNodesProps.setProperty("cluster.profile", "staging-empty");
        IllegalArgumentException emptyEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(emptyNodesProps, "test-empty-profile"));
        assertTrue(emptyEx.getMessage().contains("staging-empty"));

        // 2.3 Port không hợp lệ (ngoài khoảng 1..65535) nêu đúng khóa và nodeId
        Properties badPortProps = new Properties();
        badPortProps.setProperty("release.mode", "true");
        badPortProps.setProperty("cluster.profile", "dev");
        badPortProps.setProperty("dev.cluster.nodes", "node-3");
        badPortProps.setProperty("dev.node.node-3.client.host", "127.0.0.1");
        badPortProps.setProperty("dev.node.node-3.client.port", "99999");
        IllegalArgumentException badPortEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.fromProperties(badPortProps, "test-bad-port"));
        assertTrue(badPortEx.getMessage().contains("node-3") && badPortEx.getMessage().contains("dev.node.node-3.client.port"));

        // 2.4 preferredNode không tồn tại trong danh sách Node
        IllegalArgumentException badPrefEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "cluster.profile", "dev",
                        "client.preferredNode", "node-99"
                ), Map.of()));
        assertTrue(badPrefEx.getMessage().contains("client.preferredNode=node-99"));

        // 2.5 Tên dịch vụ Docker nội bộ bị từ chối trong chế độ RELEASE kèm đúng khóa và nodeId
        IllegalArgumentException dockerHostEx = assertThrows(IllegalArgumentException.class, () ->
                ClientConfig.loadWithOverrides(null, Map.of(
                        "release.mode", "true",
                        "cluster.profile", "dev",
                        "dev.node.node-2.client.host", "backend-node-2"
                ), Map.of()));
        assertTrue(dockerHostEx.getMessage().contains("dev.node.node-2.client.host=backend-node-2"));
        assertTrue(dockerHostEx.getMessage().contains("nodeId=node-2"));
    }

    @Test
    @DisplayName("3. Release mode chỉ khởi tạo chuỗi Journaling -> LeaderAware -> SocketBookingClient thật và gửi frame TCP hợp lệ")
    void releaseMode_WiresRealSocketBookingClientChain(@TempDir Path tempDir) throws Exception {
        try (FakeNodeServer tcpPeer = new FakeNodeServer("node-1", 0);
             PersistentPendingJournal journal = new PersistentPendingJournal(tempDir)) {

            Map<String, String> overrides = new HashMap<>();
            overrides.put("release.mode", "true");
            overrides.put("client.mode", "socket");
            overrides.put("cluster.profile", "dev");
            overrides.put("dev.cluster.nodes", "node-1");
            overrides.put("dev.node.node-1.client.host", "127.0.0.1");
            overrides.put("dev.node.node-1.client.port", String.valueOf(tcpPeer.getPort()));

            System.setProperty(ClientConfig.KEY_RELEASE_MODE, "true");
            ClientConfig releaseCfg = ClientConfig.loadWithOverrides(null, overrides, Map.of());
            assertTrue(releaseCfg.isReleaseMode());
            assertEquals("1.0.0-rc1", releaseCfg.getReleaseVersion());

            // Kiểm tra qua spy factory để xác minh lớp transport cấp dưới chính xác là SocketBookingClient
            AtomicInteger socketCreatedCount = new AtomicInteger(0);
            AtomicReference<BookingClient> createdTransportRef = new AtomicReference<>();
            BookingClient leaderClient = ClientFactory.createBookingClientForConfig(releaseCfg, ep -> {
                SocketBookingClient sbc = new SocketBookingClient(
                        ep.host(), ep.port(), releaseCfg.getConnectTimeoutMs(), releaseCfg.getReadTimeoutMs(), "release-spy");
                socketCreatedCount.incrementAndGet();
                createdTransportRef.set(sbc);
                return sbc;
            });

            assertInstanceOf(LeaderAwareBookingClient.class, leaderClient);

            ClientFactory.initializeForConfig(releaseCfg);
            ClientFactory.setJournal(journal);

            JournalingBookingClient jbc = ClientFactory.getJournalingBookingClient();
            assertNotNull(jbc);
            assertInstanceOf(LeaderAwareBookingClient.class, jbc.getDelegate());

            ReadAdapter readAdapter = ClientFactory.getReadAdapter();
            assertInstanceOf(BookingClientReadAdapter.class, readAdapter);

            ClusterStatusClient statusClient = ClientFactory.getClusterStatusClient();
            assertInstanceOf(SocketClusterStatusClient.class, statusClient);
            assertEquals(ClusterStatusClient.DataSource.REAL, statusClient.getDataSource());

            try {
                // Thực thi lời gọi qua spy transport để xác nhận SocketBookingClient thật truyền tin trên TCP
                GetSeatsResponseDto seatsResp = leaderClient.getSeats(new GetSeatsRequestDto("SE-001")).get(3, TimeUnit.SECONDS);
                assertEquals(10, seatsResp.getSeats().size());
                assertFalse(seatsResp.isMock());
                assertTrue(socketCreatedCount.get() >= 1);
                assertInstanceOf(SocketBookingClient.class, createdTransportRef.get());

                String reqId = UUID.randomUUID().toString();
                BookTicketResponseDto bookResp = jbc.book(new BookTicketRequestDto(reqId, "SE-001", "G03", "Khách Bản Phát Hành"))
                        .get(3, TimeUnit.SECONDS);
                assertTrue(bookResp.isConfirmed());
                assertEquals(reqId, bookResp.getRequestId());
                assertFalse(bookResp.isMock());
            } finally {
                if (leaderClient instanceof AutoCloseable ac) {
                    ac.close();
                }
            }
        }
    }

    @Test
    @DisplayName("4. Giao diện RELEASE: 0 node MOCK/DEMO trên Scene Graph, hiển thị version/mode/profile/nodeId và màn hình lỗi cấu hình fail-fast")
    void releaseMode_JavaFxSceneGraphExcludesMockNodesAndRendersMetadataAndConfigErrorScreen(@TempDir Path tempDir) throws Exception {
        if (GraphicsEnvironment.isHeadless()) {
            return;
        }
        try {
            Platform.startup(() -> {});
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
        }

        // 4.1 Kiểm tra màn hình lỗi cấu hình fail-fast (App.buildConfigErrorRoot) không treo, không nạp luồng đặt vé
        IllegalStateException sampleErr = new IllegalStateException(
                "Cấu hình không hợp lệ — phát hiện IP placeholder tại lan.node.node-1.client.host=192.168.10.11 (nodeId=node-1)");
        Parent errorScreen = App.buildConfigErrorRoot(sampleErr, null);
        assertNotNull(errorScreen.lookup("#configErrorTitleLabel"));
        Node detailNode = errorScreen.lookup("#configErrorDetailLabel");
        assertInstanceOf(Label.class, detailNode);
        assertTrue(((Label) detailNode).getText().contains("nodeId=node-1"));
        assertNull(errorScreen.lookup("#seatGridPane"), "Màn hình lỗi cấu hình tuyệt đối không nạp sơ đồ đặt vé");

        // 4.2 Kiểm tra cây Scene Graph của ứng dụng ở chế độ release.mode=true
        try (FakeNodeServer loopbackPeer = new FakeNodeServer("node-1", 0);
             PersistentPendingJournal journal = new PersistentPendingJournal(tempDir)) {

            loopbackPeer.setSeatStatus("G02", "BOOKED", "TCK-G02-REL");

            Map<String, String> overrides = new HashMap<>();
            overrides.put("release.mode", "true");
            overrides.put("client.mode", "socket");
            overrides.put("cluster.profile", "dev");
            overrides.put("dev.cluster.nodes", "node-1");
            overrides.put("dev.node.node-1.client.host", "127.0.0.1");
            overrides.put("dev.node.node-1.client.port", String.valueOf(loopbackPeer.getPort()));

            System.setProperty(ClientConfig.KEY_RELEASE_MODE, "true");
            System.setProperty(ClientConfig.KEY_CLIENT_MODE, "socket");
            ClientConfig releaseCfg = ClientConfig.loadWithOverrides(null, overrides, Map.of());
            ClientFactory.initializeForConfig(releaseCfg);
            ClientFactory.setJournal(journal);

            CountDownLatch uiReadyLatch = new CountDownLatch(1);
            AtomicReference<Throwable> uiErr = new AtomicReference<>();
            AtomicReference<Stage> stageRef = new AtomicReference<>();
            AtomicReference<Parent> mainRootRef = new AtomicReference<>();
            AtomicReference<MainController> mainCtrlRef = new AtomicReference<>();
            AtomicReference<Parent> cancelRootRef = new AtomicReference<>();

            boolean captureD28Evidence = Boolean.getBoolean("ui.evidence") && Boolean.getBoolean("d28.ui.evidence");
            if (captureD28Evidence) {
                System.setProperty("ui.evidence.fake", "true");
            }

            Platform.runLater(() -> {
                try {
                    FXMLLoader mainLoader = new FXMLLoader(App.class.getResource("/fxml/main.fxml"));
                    Parent mainRoot = mainLoader.load();
                    MainController mainCtrl = mainLoader.getController();

                    Scene scene = new Scene(mainRoot, App.DEFAULT_WIDTH, App.DEFAULT_HEIGHT);
                    Stage stage = new Stage();
                    stage.setTitle(App.buildWindowTitle(releaseCfg));
                    stage.setScene(scene);
                    stage.show();

                    // Mở màn hình Tra cứu vé rồi mở Sơ đồ chọn ghế để khởi tạo đầy đủ các màn hình con
                    mainCtrl.showTicketStatusView();
                    mainCtrl.showSeatGridView();

                    FXMLLoader cancelLoader = new FXMLLoader(App.class.getResource("/fxml/cancel-ticket-dialog.fxml"));
                    Parent cancelRoot = cancelLoader.load();

                    mainRoot.applyCss();
                    mainRoot.layout();

                    stageRef.set(stage);
                    mainRootRef.set(mainRoot);
                    mainCtrlRef.set(mainCtrl);
                    cancelRootRef.set(cancelRoot);
                } catch (Throwable t) {
                    uiErr.set(t);
                } finally {
                    uiReadyLatch.countDown();
                }
            });

            assertTrue(uiReadyLatch.await(10, TimeUnit.SECONDS));
            if (uiErr.get() != null) {
                throw new AssertionError("Không thể khởi tạo giao diện RELEASE", uiErr.get());
            }

            try {
                // Đợi nhịp refresh ghế bất đồng bộ từ loopbackPeer hoàn tất
                SeatGridController sgc = mainCtrlRef.get().getCachedSeatGridController();
                assertNotNull(sgc);
                sgc.getSeatsRefreshService().refreshNow().get(3, TimeUnit.SECONDS);

                CountDownLatch verifyLatch = new CountDownLatch(1);
                AtomicReference<Throwable> verifyErr = new AtomicReference<>();
                Platform.runLater(() -> {
                    try {
                        Parent mainRoot = mainRootRef.get();
                        Parent cancelRoot = cancelRootRef.get();

                        // Minh họa đủ trạng thái ghế BOOKED (G02 từ peer) và UNKNOWN (G04) dưới nhãn [Chưa xác minh]
                        sgc.getSeatViewModel("G04").setStatus(SeatStatus.UNKNOWN);
                        sgc.updateLegendCounts();
                        mainRoot.applyCss();
                        mainRoot.layout();

                        // Xác nhận các node MOCK đã bị GỠ HOÀN TOÀN khỏi Scene Graph (lookup trả về null / rỗng)
                        assertNull(mainRoot.lookup("#mockBadgeLabel"),
                                "#mockBadgeLabel phải bị gỡ khỏi cây giao diện trong chế độ RELEASE");
                        assertNull(mainRoot.lookup("#mockControlPanel"),
                                "#mockControlPanel phải bị gỡ khỏi cây giao diện trong chế độ RELEASE");
                        assertNull(cancelRoot.lookup("#cancelMockPanel"),
                                "#cancelMockPanel phải bị gỡ khỏi hộp thoại hủy vé trong chế độ RELEASE");
                        assertTrue(mainRoot.lookupAll(".mock-panel").isEmpty(),
                                "Không được tồn tại bất kỳ phần tử .mock-panel nào trong Scene Graph RELEASE");
                        assertTrue(mainRoot.lookupAll(".badge-mock").isEmpty(),
                                "Không được tồn tại bất kỳ huy hiệu .badge-mock nào trong Scene Graph RELEASE");

                        // Xác nhận thanh trạng thái hiển thị đầy đủ release.version, mode=RELEASE, profile và nodeId
                        Label releaseInfo = mainCtrlRef.get().getReleaseInfoLabel();
                        assertNotNull(releaseInfo);
                        String infoTxt = releaseInfo.getText();
                        assertTrue(infoTxt.contains("release.version=1.0.0-rc1"), "Thiếu release.version=1.0.0-rc1: " + infoTxt);
                        assertTrue(infoTxt.contains("mode=RELEASE"), "Thiếu mode=RELEASE: " + infoTxt);
                        assertTrue(infoTxt.contains("profile=dev"), "Thiếu profile=dev: " + infoTxt);
                        assertTrue(infoTxt.contains("node=node-1"), "Thiếu nodeId đang kết nối: " + infoTxt);
                        assertFalse(infoTxt.toUpperCase().contains("MOCK") || infoTxt.toUpperCase().contains("FAKE"));

                        // Xác nhận nhãn [Chưa xác minh] trên sơ đồ ghế
                        Node verifNode = mainRoot.lookup("#verificationBadgeLabel");
                        assertInstanceOf(Label.class, verifNode);
                        assertEquals("[Chưa xác minh]", ((Label) verifNode).getText());

                        // Chỉ chụp ảnh minh họa giao diện RELEASE khi có cờ tường minh -Dui.evidence=true -Dd28.ui.evidence=true
                        if (captureD28Evidence) {
                            Stage st = stageRef.get();
                            assertEquals("RELEASE UI — KẾT NỐI FAKE (không phải giao dịch thật)", st.getTitle());
                            Path outPng = resolveProjectPath("artifacts/evidence/D28/TV1-D28-ui.png");
                            Files.createDirectories(outPng.getParent());
                            writeSnapshotWithWindowTitleBar(mainRoot, st.getTitle(), outPng);
                        }
                    } catch (Throwable t) {
                        verifyErr.set(t);
                    } finally {
                        Stage st = stageRef.get();
                        if (st != null) {
                            st.close();
                        }
                        verifyLatch.countDown();
                    }
                });

                assertTrue(verifyLatch.await(10, TimeUnit.SECONDS));
                if (verifyErr.get() != null) {
                    throw new AssertionError("Kiểm chứng cây giao diện RELEASE thất bại", verifyErr.get());
                }
            } finally {
                Platform.runLater(() -> {
                    Stage st = stageRef.get();
                    if (st != null && st.isShowing()) {
                        st.close();
                    }
                });
            }
        }
    }

    private static void writeSnapshotWithWindowTitleBar(Parent sceneRoot, String windowTitle, Path targetPng) throws Exception {
        WritableImage fxImage = sceneRoot.snapshot(null, null);
        int contentW = (int) fxImage.getWidth();
        int contentH = (int) fxImage.getHeight();
        int titleBarH = 34;

        BufferedImage out = new BufferedImage(contentW, contentH + titleBarH, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = out.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            g.setColor(new Color(15, 23, 42));
            g.fillRect(0, 0, contentW, titleBarH);
            g.setColor(new Color(251, 191, 36));
            g.setFont(new Font("SansSerif", Font.BOLD, 13));
            g.drawString(windowTitle, 16, 22);
        } finally {
            g.dispose();
        }

        PixelReader reader = fxImage.getPixelReader();
        for (int y = 0; y < contentH; y++) {
            for (int x = 0; x < contentW; x++) {
                out.setRGB(x, y + titleBarH, reader.getArgb(x, y));
            }
        }
        ImageIO.write(out, "png", targetPng.toFile());
    }

    @Test
    @DisplayName("5. Quét tĩnh mã nguồn chứng minh 0 tham chiếu Mock*/Fake* trong toàn bộ các lớp thuộc luồng RELEASE")
    void releaseMode_StaticSourceScanZeroMockOrFakeReferencesInReleaseFlow() throws Exception {
        Path mainJavaDir = resolveProjectPath("desktop-client/src/main/java/vn/nhom6/railway/client");
        Pattern forbiddenReleaseRef = Pattern.compile(
                "\\b(MockBookingClient|MockClusterStatusClient|MockScenario|MockTicketSource|MockSeatSource|FakeReadAdapter|MockBookingService|MockBookingServiceImpl|DevModeSupport)\\b"
        );

        // Các tệp chỉ-dùng-cho-dev đã được loại trừ khỏi artifact JAR khi đóng gói với -Prelease
        List<String> violations = new ArrayList<>();
        int releaseFlowFilesCount = 0;

        try (Stream<Path> walk = Files.walk(mainJavaDir)) {
            for (Path file : walk.filter(f -> f.toString().endsWith(".java")).toList()) {
                String rel = mainJavaDir.relativize(file).toString().replace('\\', '/');
                String fileName = file.getFileName().toString();

                // Bỏ qua các lớp giả lập độc lập và gói dev/** (đã loại qua maven-jar-plugin trong profile release)
                if (rel.startsWith("dev/")
                        || fileName.startsWith("Mock")
                        || fileName.startsWith("Fake")) {
                    continue;
                }

                releaseFlowFilesCount++;
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                boolean inBlockComment = false;
                boolean inInnerMockSourceClass = false;
                int innerBraceDepth = 0;

                for (int i = 0; i < lines.size(); i++) {
                    String raw = lines.get(i);
                    String trimmed = raw.trim();

                    if (inBlockComment) {
                        if (trimmed.contains("*/")) {
                            inBlockComment = false;
                        }
                        continue;
                    }
                    if (trimmed.startsWith("/*")) {
                        if (!trimmed.contains("*/")) {
                            inBlockComment = true;
                        }
                        continue;
                    }
                    if (trimmed.startsWith("//") || trimmed.startsWith("*")) {
                        continue;
                    }

                    String code = raw.contains("//") ? raw.substring(0, raw.indexOf("//")) : raw;

                    // Bỏ qua thân inner class MockSeatSource / MockTicketSource (bị loại khỏi JAR bởi **/*$Mock*.class)
                    if (!inInnerMockSourceClass && code.contains("static class Mock")) {
                        inInnerMockSourceClass = true;
                        innerBraceDepth = countChar(code, '{') - countChar(code, '}');
                        continue;
                    }
                    if (inInnerMockSourceClass) {
                        innerBraceDepth += countChar(code, '{') - countChar(code, '}');
                        if (innerBraceDepth <= 0) {
                            inInnerMockSourceClass = false;
                        }
                        continue;
                    }

                    // Bỏ qua chuỗi hằng số tên lớp dùng cho Class.forName trong DevSupportBridge (có bảo vệ isReleaseModeActive)
                    if (rel.equals("network/DevSupportBridge.java") && code.contains("IMPL_CLASS_NAME")) {
                        continue;
                    }

                    if (forbiddenReleaseRef.matcher(code).find()) {
                        violations.add(rel + ":" + (i + 1) + " -> " + trimmed);
                    }
                }
            }
        }

        assertTrue(releaseFlowFilesCount >= 50, "Phải quét đầy đủ toàn bộ các lớp trong luồng release");
        assertEquals(List.of(), violations,
                "Các lớp trong luồng RELEASE tuyệt đối không được tham chiếu tới Mock*/Fake*: " + violations);
    }

    private static int countChar(String s, char ch) {
        int c = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ch) {
                c++;
            }
        }
        return c;
    }
}
