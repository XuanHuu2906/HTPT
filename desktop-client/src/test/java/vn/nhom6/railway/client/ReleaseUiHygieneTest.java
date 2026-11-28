package vn.nhom6.railway.client;

import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.controller.SeatGridController;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.VerificationLevel;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bộ kiểm thử rà soát vệ sinh giao diện, CSS, thông báo và sơ đồ ghế trước khi đóng băng Release Candidate (TASK TV1-D28 — Bước 2.4).
 * <p>
 * Không sử dụng thêm bất kỳ dependency runtime nào ngoài JUnit 5 và JavaFX có sẵn.
 */
@DisplayName("TV1-D28: ReleaseUiHygieneTest — Kiểm tra CSS, thông báo chuẩn hóa và sơ đồ 10 ghế")
public class ReleaseUiHygieneTest {

    private static final List<String> ALL_FXML_RESOURCES = List.of(
            "/fxml/main.fxml",
            "/fxml/seat-grid.fxml",
            "/fxml/booking-dialog.fxml",
            "/fxml/cancel-ticket-dialog.fxml",
            "/fxml/ticket-status.fxml",
            "/fxml/pending-requests.fxml",
            "/fxml/cluster-status-bar.fxml"
    );

    private static final List<String> FORBIDDEN_UNKNOWN_PHRASES = List.of(
            "thành công",
            "thất bại",
            "bị từ chối",
            "đã đặt",
            "đã hủy"
    );

    private static Path resolveProjectPath(String relativePath) {
        Path direct = Paths.get(relativePath);
        if (Files.exists(Paths.get("desktop-client")) || Files.exists(Paths.get(".git"))) {
            return direct.toAbsolutePath().normalize();
        }
        return Paths.get("..").resolve(relativePath).toAbsolutePath().normalize();
    }

    private static String readClasspathResource(String resourcePath) throws IOException {
        try (InputStream is = App.class.getResourceAsStream(resourcePath)) {
            assertNotNull(is, "Không tìm thấy tài nguyên bắt buộc: " + resourcePath);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    @DisplayName("1. CSS: Mọi styleClass trong FXML/Mapper/Controller đều tồn tại trong railway.css; 0 hard-code màu #hex trong Java UI")
    void css_AllReferencedStyleClassesExistAndZeroHardcodedHexColorsInJava() throws Exception {
        String cssContent = readClasspathResource("/css/railway.css");

        // Trích xuất toàn bộ định nghĩa .class-name trong railway.css
        Set<String> definedClasses = new HashSet<>();
        Matcher cssClassMatcher = Pattern.compile("\\.([a-zA-Z0-9_-]+)").matcher(cssContent);
        while (cssClassMatcher.find()) {
            definedClasses.add(cssClassMatcher.group(1));
        }

        // 1.1 Kiểm tra mọi styleClass="..." khai báo trong cả 7 tệp FXML
        Pattern fxmlStyleClassPattern = Pattern.compile("styleClass=\"([^\"]+)\"");
        List<String> missingFxmlClasses = new ArrayList<>();
        for (String fxmlRes : ALL_FXML_RESOURCES) {
            String xml = readClasspathResource(fxmlRes);
            Matcher m = fxmlStyleClassPattern.matcher(xml);
            while (m.find()) {
                for (String token : m.group(1).split("[,\\s]+")) {
                    String cls = token.trim();
                    if (!cls.isEmpty() && !definedClasses.contains(cls)) {
                        missingFxmlClasses.add(fxmlRes + " -> ." + cls);
                    }
                }
            }
        }
        assertEquals(List.of(), missingFxmlClasses,
                "Mọi styleClass tham chiếu trong FXML phải được định nghĩa trong railway.css");

        // 1.2 Kiểm tra mọi styleClass từ Enum / Mapper / Controller động
        Set<String> requiredDynamicClasses = new HashSet<>();
        for (SeatStatus s : EnumSet.allOf(SeatStatus.class)) {
            requiredDynamicClasses.add(s.getStyleClass());
        }
        for (ScreenState s : EnumSet.allOf(ScreenState.class)) {
            requiredDynamicClasses.add(s.getStyleClass());
        }
        for (BookingViewModel.UiState s : EnumSet.allOf(BookingViewModel.UiState.class)) {
            requiredDynamicClasses.add(s.getStyleClass());
        }
        for (TicketStatus s : EnumSet.allOf(TicketStatus.class)) {
            requiredDynamicClasses.add(s.getStyleClass());
        }
        for (ClusterConnectionViewModel.ConnectionState s : EnumSet.allOf(ClusterConnectionViewModel.ConnectionState.class)) {
            requiredDynamicClasses.add(s.getDotStyleClass());
        }
        for (ClusterConnectionViewModel.NodeRole s : EnumSet.allOf(ClusterConnectionViewModel.NodeRole.class)) {
            requiredDynamicClasses.add(s.getBadgeStyleClass());
        }
        for (VerificationLevel v : EnumSet.allOf(VerificationLevel.class)) {
            requiredDynamicClasses.add(v.getStyleClass());
        }
        for (ClusterStatusClient.DataSource ds : EnumSet.allOf(ClusterStatusClient.DataSource.class)) {
            requiredDynamicClasses.add(ds.getStyleClass());
        }
        for (ClientErrorKind cek : EnumSet.allOf(ClientErrorKind.class)) {
            requiredDynamicClasses.add(cek.getStyleClass());
        }
        requiredDynamicClasses.addAll(List.of(
                ErrorPresentationMapper.STYLE_CONFIRMED,
                ErrorPresentationMapper.STYLE_REJECTED,
                ErrorPresentationMapper.STYLE_UNKNOWN,
                ErrorPresentationMapper.STYLE_ERROR,
                ErrorPresentationMapper.STYLE_DISCONNECTED,
                "badge-cluster-mode",
                "badge-release-mode",
                "version-status-label",
                "reconnect-connected",
                "reconnect-searching",
                "reconnect-no-leader",
                "pending-status-confirmed",
                "pending-status-rejected",
                "pending-status-error",
                "pending-status-unknown",
                "pending-source-real",
                "pending-source-fake",
                "pending-source-mock",
                "btn-pending-check",
                "btn-pending-retry",
                "config-error-root",
                "config-error-card",
                "config-error-title",
                "config-error-detail"
        ));

        List<String> missingDynamicClasses = requiredDynamicClasses.stream()
                .filter(cls -> !definedClasses.contains(cls))
                .sorted()
                .toList();
        assertEquals(List.of(), missingDynamicClasses,
                "Mọi styleClass động từ Mapper/ViewModel/Controller phải có trong railway.css");

        // 1.3 Kiểm tra trạng thái :hover, :focused, :disabled và sự phân biệt trực quan 4 trạng thái ghế + chưa xác minh
        assertTrue(cssContent.contains(".seat-available:hover") && cssContent.contains(".seat-available:focused"));
        assertTrue(cssContent.contains(".seat-selected:hover") && cssContent.contains(".seat-selected:focused"));
        assertTrue(cssContent.contains(".seat-booked:hover") && cssContent.contains(".seat-booked:focused"));
        assertTrue(cssContent.contains(".seat-unknown:hover") && cssContent.contains(".seat-unknown:focused"));
        assertTrue(cssContent.contains(".seat-button:disabled"));
        assertTrue(cssContent.contains("-fx-border-style: dashed"), "Ghế/huy hiệu UNKNOWN phải có viền nét đứt để không phụ thuộc duy nhất vào màu sắc");
        assertTrue(definedClasses.contains("badge-unverified") && definedClasses.contains("badge-verified"));

        // 1.4 Quét toàn bộ mã Java tầng UI (App, controller, viewmodel) chứng minh 0 hard-code màu #hex và 0 printStackTrace
        Path javaMainRoot = resolveProjectPath("desktop-client/src/main/java/vn/nhom6/railway/client");
        Pattern hexColorPattern = Pattern.compile("#[0-9a-fA-F]{6}\\b");
        List<String> uiViolations = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(javaMainRoot)) {
            for (Path p : walk.filter(f -> f.toString().endsWith(".java")).toList()) {
                String rel = javaMainRoot.relativize(p).toString().replace('\\', '/');
                if (rel.startsWith("dev/")) {
                    continue;
                }
                boolean isUiCode = rel.equals("App.java")
                        || rel.startsWith("controller/")
                        || rel.startsWith("viewmodel/");
                if (!isUiCode) {
                    continue;
                }
                List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i).trim();
                    if (line.startsWith("//") || line.startsWith("*") || line.startsWith("/*")) {
                        continue;
                    }
                    if (hexColorPattern.matcher(line).find()) {
                        uiViolations.add(rel + ":" + (i + 1) + " -> hard-code mã màu #hex trong Java");
                    }
                    if (line.contains("printStackTrace(")) {
                        uiViolations.add(rel + ":" + (i + 1) + " -> gọi printStackTrace");
                    }
                }
            }
        }
        assertEquals(List.of(), uiViolations, "Không được hard-code màu #hex hay in stack trace trong Java UI");
    }

    @Test
    @DisplayName("2. Thông báo & Tài nguyên: UNKNOWN không chứa từ cấm, khác biệt hoàn toàn với REJECTED; 0 MOCK/DEMO/FAKE/TODO/FIXME trong FXML")
    void messages_UnknownZeroForbiddenWordsDistinctFromRejectedAndCleanFxmlResources() throws Exception {
        // 2.1 Kiểm tra mọi thông điệp UNKNOWN từ đặt vé, đọc ghế, hủy vé và timeout mạng
        List<ErrorPresentation> unknownPresentations = List.of(
                ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, false),
                ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_READ_UNAVAILABLE, false),
                ErrorPresentationMapper.mapClientError(ClientErrorKind.READ_TIMEOUT, "Socket read timed out", false),
                ErrorPresentationMapper.mapBookingResponse(
                        new BookTicketResponseDto("req-u1", "UNKNOWN", null, "SE-001", "G01", "TIMEOUT_UNKNOWN", null, false, null, null)),
                ErrorPresentationMapper.mapCancelResponse(
                        new CancelTicketResponseDto("req-c1", "UNKNOWN", "TCK-01", "TIMEOUT_UNKNOWN", null, false, null, null))
        );

        for (ErrorPresentation up : unknownPresentations) {
            assertEquals(PresentationKind.UNKNOWN, up.kind());
            String combinedLower = (up.title() + " " + up.message() + " " + up.suggestedAction()).toLowerCase(Locale.ROOT);
            for (String forbidden : FORBIDDEN_UNKNOWN_PHRASES) {
                assertFalse(combinedLower.contains(forbidden),
                        "Thông báo UNKNOWN tuyệt đối không được chứa từ cấm '" + forbidden + "': " + up.message());
            }
        }

        // 2.2 Chứng minh UNKNOWN khác REJECTED ở cả trạng thái, style, tiêu đề, hành động gợi ý và cờ tra cứu
        ErrorPresentation unknownBook = ErrorPresentationMapper.mapBookingResponse(
                new BookTicketResponseDto("req-u", "UNKNOWN", null, "SE-001", "G03", "TIMEOUT_UNKNOWN", null, false, null, null));
        ErrorPresentation rejectedBook = ErrorPresentationMapper.mapBookingResponse(
                new BookTicketResponseDto("req-r", "REJECTED", null, "SE-001", "G03", "SEAT_ALREADY_BOOKED", 12L, false, null, null));

        assertNotEquals(unknownBook.kind(), rejectedBook.kind());
        assertNotEquals(unknownBook.styleClass(), rejectedBook.styleClass());
        assertNotEquals(unknownBook.title(), rejectedBook.title());
        assertNotEquals(unknownBook.suggestedAction(), rejectedBook.suggestedAction());
        assertTrue(unknownBook.retryable());
        assertFalse(rejectedBook.retryable());
        assertTrue(unknownBook.showRequestId());
        assertFalse(rejectedBook.showRequestId());

        // 2.3 Quét văn bản hiển thị trong cả 7 tệp FXML chứng minh 0 có MOCK, DEMO, FAKE, TODO, FIXME, Placeholder
        Pattern visibleTextAttr = Pattern.compile("(?:text|promptText)=\"([^\"]*)\"");
        Pattern bannedTokens = Pattern.compile("\\b(MOCK|DEMO|FAKE|TODO|FIXME|Placeholder)\\b", Pattern.CASE_INSENSITIVE);
        List<String> fxmlTextViolations = new ArrayList<>();

        for (String fxmlRes : ALL_FXML_RESOURCES) {
            String xml = readClasspathResource(fxmlRes);
            assertFalse(xml.contains("TODO") || xml.contains("FIXME"),
                    "Tệp FXML không được còn ghi chú TODO/FIXME: " + fxmlRes);
            Matcher attrMatcher = visibleTextAttr.matcher(xml);
            while (attrMatcher.find()) {
                String val = attrMatcher.group(1);
                if (bannedTokens.matcher(val).find()) {
                    fxmlTextViolations.add(fxmlRes + " -> \"" + val + "\"");
                }
            }
        }
        assertEquals(List.of(), fxmlTextViolations,
                "Văn bản hiển thị trong tài nguyên FXML không được chứa từ khóa MOCK/DEMO/FAKE/TODO/FIXME/Placeholder");
    }

    @Test
    @DisplayName("3. Ghế G01–G10: Đủ thứ tự, BOOKED khóa chọn, refresh giữ lựa chọn hợp lệ của người dùng, READ_UNAVAILABLE chuyển UNKNOWN")
    void seats_TenSeatsSelectionPreservedAcrossRefreshAndReadUnavailableHandled(@TempDir Path tempDir) throws Exception {
        // 3.1 Khởi tạo 10 ghế chưa xác minh
        ObservableList<SeatViewModel> seats = SeatViewModel.createInitialUnverifiedSeats();
        assertEquals(10, seats.size());
        for (int i = 1; i <= 10; i++) {
            String expectedId = String.format("G%02d", i);
            assertEquals(expectedId, seats.get(i - 1).getSeatId());
            assertEquals(SeatStatus.UNKNOWN, seats.get(i - 1).getStatus());
            assertEquals(VerificationLevel.UNVERIFIED, seats.get(i - 1).getVerification());
        }

        try (FakeNodeServer server = new FakeNodeServer("node-1", 0);
             PersistentPendingJournal journal = new PersistentPendingJournal(tempDir)) {
            server.setSeatStatus("G02", "BOOKED", "TCK-G02");

            NodeEndpoint ep = new NodeEndpoint("node-1", "127.0.0.1", server.getPort());
            LeaderAwareBookingClient client = new LeaderAwareBookingClient(
                    ClusterEndpoints.fromEndpoints(List.of(ep), "node-1"),
                    nodeEp -> new SocketBookingClient(nodeEp.host(), nodeEp.port(), 1500, 1500, "hygiene-test"),
                    new RedirectPolicy(2, 1500, 3000),
                    null,
                    "node-1"
            );
            SeatsRefreshService refreshService = new SeatsRefreshService(
                    new BookingClientReadAdapter(client), seats, journal);

            try {
                // 3.2 Refresh lần đầu: G02 BOOKED (bị khóa), 9 ghế còn lại AVAILABLE
                refreshService.refreshNow().get(3, TimeUnit.SECONDS);
                flushFxEventsIfStarted();
                assertTrue(seats.get(1).isBooked(), "G02 BOOKED phải bật cờ bookedProperty để disable nút");
                assertEquals(SeatStatus.AVAILABLE, seats.get(0).getStatus());

                // Người dùng chọn ghế G01 và G06 hợp lệ
                SeatGridController controller = new SeatGridController();
                controller.getSeats().setAll(seats);
                controller.setSeatsRefreshService(refreshService);
                controller.handleSeatClick("G01");
                controller.handleSeatClick("G06");
                assertEquals(List.of("G01", "G06"), controller.getSelectedSeats());

                // Chọn vào ghế BOOKED G02 bị từ chối
                controller.handleSeatClick("G02");
                assertEquals(List.of("G01", "G06"), controller.getSelectedSeats());

                // Bỏ chọn G06 -> chỉ còn G01 đang chọn
                controller.handleSeatClick("G06");
                assertEquals(List.of("G01"), controller.getSelectedSeats());

                // 3.3 Refresh ghế khi G01 vẫn AVAILABLE trên server -> KHÔNG làm mất lựa chọn SELECTED của người dùng
                server.setSeatStatus("G05", "BOOKED", "TCK-G05");
                refreshService.refreshNow().get(3, TimeUnit.SECONDS);
                flushFxEventsIfStarted();
                assertEquals(SeatStatus.SELECTED, controller.getSeatViewModel("G01").getStatus(),
                        "Làm mới sơ đồ ghế không được xóa lựa chọn hợp lệ G01 của người dùng");
                assertEquals(SeatStatus.BOOKED, controller.getSeatViewModel("G05").getStatus());

                // 3.4 READ_UNAVAILABLE -> các ghế chưa chọn chuyển sang UNKNOWN dưới nhãn Chưa xác minh
                controller.clearSelection();
                server.setForcedError("READ_UNAVAILABLE", "No read quorum");
                try {
                    refreshService.refreshNow().get(3, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
                flushFxEventsIfStarted();
                for (SeatViewModel s : controller.getSeats()) {
                    assertEquals(SeatStatus.UNKNOWN, s.getStatus());
                }
                assertEquals(VerificationLevel.UNVERIFIED, refreshService.getVerification());
                assertEquals("Chưa xác minh", refreshService.getVerification().getDisplayName());
            } finally {
                refreshService.close();
                client.close();
            }
        }
    }

    private static void flushFxEventsIfStarted() throws InterruptedException {
        if (Platform.isFxApplicationThread()) {
            return;
        }
        CountDownLatch fxLatch = new CountDownLatch(1);
        try {
            Platform.runLater(fxLatch::countDown);
            fxLatch.await(3, TimeUnit.SECONDS);
        } catch (IllegalStateException ignored) {
            // Toolkit chưa khởi chạy -> SeatsRefreshService đã cập nhật đồng bộ
        }
    }

    private static boolean isJavaFxDisplayAvailable() {
        if (GraphicsEnvironment.isHeadless() || Boolean.parseBoolean(System.getenv("HEADLESS"))) {
            return false;
        }
        if ("Monocle".equalsIgnoreCase(System.getProperty("glass.platform", ""))) {
            try {
                Class.forName("com.sun.glass.ui.monocle.MonoclePlatformFactory");
            } catch (ClassNotFoundException e) {
                return false;
            }
        }
        return true;
    }

    @Test
    @DisplayName("4. Kích thước cửa sổ tối thiểu >= 1040x720 và bố cục FXML nạp sạch ở khung tối thiểu")
    void windowLayout_MinDimensionsAtLeast1040x720() throws Exception {
        assertTrue(App.MIN_WIDTH >= 1040, "Chiều rộng tối thiểu phải >= 1040px để không cắt chữ");
        assertTrue(App.MIN_HEIGHT >= 720, "Chiều cao tối thiểu phải >= 720px để không che khuất bảng và thanh trạng thái");
        assertTrue(App.DEFAULT_WIDTH >= App.MIN_WIDTH);
        assertTrue(App.DEFAULT_HEIGHT >= App.MIN_HEIGHT);

        if (!isJavaFxDisplayAvailable()) {
            return;
        }
        try {
            Platform.startup(() -> {});
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Platform.runLater(() -> {
            Stage stage = null;
            try {
                FXMLLoader loader = new FXMLLoader(App.class.getResource("/fxml/main.fxml"));
                Parent root = loader.load();
                Scene scene = new Scene(root, App.MIN_WIDTH, App.MIN_HEIGHT);
                stage = new Stage();
                stage.setScene(scene);
                root.applyCss();
                root.layout();
                assertTrue(root.getLayoutBounds().getWidth() <= App.MIN_WIDTH + 1.0);
            } catch (Throwable t) {
                err.set(t);
            } finally {
                if (stage != null) {
                    stage.close();
                }
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError("Lỗi bố cục ở kích thước tối thiểu 1040x720", err.get());
        }
    }
}
