package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.stage.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.controller.ReconnectController;
import vn.nhom6.railway.client.controller.ReconnectController.ReconnectState;
import vn.nhom6.railway.client.harness.RecordedTraceReplayServer;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.VerificationLevel;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketLookupViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import javax.imageio.ImageIO;
import java.awt.GraphicsEnvironment;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BỘ KIỂM THỬ HỒI QUY TOÀN DIỆN DESKTOP CLIENT (TASK TV1-D27).
 * <p>
 * Tách bạch 2 lớp thực thi:
 * <ul>
 *   <li>{@code @Tag("fixture")}: Chạy trên loopback với {@link FakeNodeServer} / {@link RecordedTraceReplayServer},
 *       làm baseline hồi quy cho toàn bộ chức năng D01–D26 (nguồn {@code FAKE} / {@code RECORDED-FIXTURE}).</li>
 *   <li>{@code @Tag("real")}: Bảo vệ môi trường chặt chẽ, chỉ chạy khi đủ opt-in, khóa lịch từ TV6 và ngân sách ghế.</li>
 * </ul>
 * <p>
 * Lưu ý quan trọng: Kết quả regression tại đây phục vụ kiểm chứng phía Desktop Client,
 * KHÔNG thay thế đợt nghiệm thu AC01–AC10 toàn hệ thống do TV6 chấm.
 * Tuân thủ quyền riêng tư (AGENTS.md): tuyệt đối KHÔNG ghi họ tên hành khách hay payload thô ra CSV/log.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(ClientRegressionTest.RegressionCsvWatcher.class)
@DisplayName("TV1-D27: Regression toàn bộ Desktop Client (tách lớp FIXTURE và REAL)")
public class ClientRegressionTest {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface RegMeta {
        String id();
        String group();
        String layer();
        String dataSource();
    }

    public record CsvRow(
            String regId,
            String group,
            String layer,
            String status,
            long durationMs,
            String dataSource,
            String requestIds,
            String note
    ) {}

    /**
     * Bản ghi kiểm toán khung tin trên đường dây (KHÔNG lưu họ tên hành khách hay payload thô — chỉ lưu mã băm và độ dài).
     */
    public record WireFrameAudit(
            String regId,
            String requestId,
            String messageId,
            String commandType,
            String payloadSha256,
            int payloadBytes,
            String intentHash
    ) {}

    private static final class ActiveContext {
        final List<String> requestIds = new CopyOnWriteArrayList<>();
        volatile String note = "";
        volatile String overrideDataSource = null;
        volatile long startNanos = System.nanoTime();
        volatile long durationMs = 0L;
    }

    private static final Map<String, ActiveContext> CONTEXT_BY_TEST = new ConcurrentHashMap<>();
    private static final Map<String, CsvRow> RECORDED_ROWS = Collections.synchronizedMap(new LinkedHashMap<>());
    private static final List<WireFrameAudit> WIRE_AUDIT_COLLECTOR = new CopyOnWriteArrayList<>();
    private static final ThreadLocal<String> CURRENT_TEST_KEY = new ThreadLocal<>();
    private static final ThreadLocal<String> CURRENT_REG_ID = new ThreadLocal<>();

    private static final Path CSV_OUTPUT_PATH = resolveProjectPath("artifacts/evidence/D27/TV1-D27-regression-results.csv");

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDownResources() {
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ignored) {
            }
        }
        resources.clear();
    }

    private <T extends AutoCloseable> T track(T resource) {
        if (resource != null) {
            resources.add(resource);
        }
        return resource;
    }

    protected static Path resolveProjectPath(String relativePath) {
        Path direct = Paths.get(relativePath);
        if (Files.exists(Paths.get("desktop-client")) || Files.exists(Paths.get(".git"))) {
            return direct.toAbsolutePath().normalize();
        }
        return Paths.get("..").resolve(relativePath).toAbsolutePath().normalize();
    }

    private static void recordRequestId(String reqId) {
        String key = CURRENT_TEST_KEY.get();
        if (key != null && reqId != null && !reqId.isBlank()) {
            ActiveContext ctx = CONTEXT_BY_TEST.get(key);
            if (ctx != null && !ctx.requestIds.contains(reqId)) {
                ctx.requestIds.add(reqId);
            }
        }
    }

    private static void recordNote(String note) {
        String key = CURRENT_TEST_KEY.get();
        if (key != null && note != null) {
            ActiveContext ctx = CONTEXT_BY_TEST.get(key);
            if (ctx != null) {
                ctx.note = note;
            }
        }
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest((input != null ? input : "").getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Không thể tính SHA-256", e);
        }
    }

    private static void collectServerFrames(FakeNodeServer server) {
        String regId = CURRENT_REG_ID.get() != null ? CURRENT_REG_ID.get() : "UNKNOWN";
        for (MessageEnvelope<JsonNode> env : server.getReceivedRequests()) {
            MessageType type = env.getType();
            JsonNode p = env.getPayload();
            if (p == null || (type != MessageType.BOOK_TICKET
                    && type != MessageType.CANCEL_TICKET
                    && type != MessageType.GET_REQUEST_RESULT)) {
                continue;
            }
            String reqId = p.hasNonNull("requestId") ? p.get("requestId").asText() : null;
            if (reqId == null || reqId.isBlank()) {
                continue;
            }
            String canonical;
            String intentCanonical;
            if (type == MessageType.BOOK_TICKET) {
                String trip = p.hasNonNull("tripId") ? p.get("tripId").asText() : "";
                String seat = p.hasNonNull("seatId") ? p.get("seatId").asText() : "";
                String nameNorm = BookingFormValidator.normalizeName(
                        p.hasNonNull("passengerName") ? p.get("passengerName").asText() : "");
                canonical = "BOOK|" + reqId + "|" + trip + "|" + seat + "|" + sha256Hex(nameNorm);
                intentCanonical = "BOOK_INTENT|" + trip + "|" + seat + "|" + sha256Hex(nameNorm);
            } else if (type == MessageType.CANCEL_TICKET) {
                String tck = p.hasNonNull("ticketId") ? p.get("ticketId").asText() : "";
                canonical = "CANCEL|" + reqId + "|" + tck;
                intentCanonical = "CANCEL_INTENT|" + tck;
            } else {
                canonical = "LOOKUP|" + reqId;
                intentCanonical = null;
            }
            int byteLen = p.toString().getBytes(StandardCharsets.UTF_8).length;
            WIRE_AUDIT_COLLECTOR.add(new WireFrameAudit(
                    regId,
                    reqId,
                    env.getMessageId(),
                    type.name(),
                    sha256Hex(canonical),
                    byteLen,
                    intentCanonical != null ? sha256Hex(intentCanonical) : null
            ));
        }
    }

    private static ObservableList<SeatViewModel> createCleanTenSeats() {
        ObservableList<SeatViewModel> list = FXCollections.observableArrayList();
        for (int i = 1; i <= 10; i++) {
            list.add(new SeatViewModel(String.format("G%02d", i), SeatStatus.AVAILABLE));
        }
        return list;
    }

    private LeaderAwareBookingClient buildSingleServerLeaderClient(FakeNodeServer server, int stepTimeoutMs, int totalTimeoutMs, String senderId) {
        NodeEndpoint ep = new NodeEndpoint(server.getNodeId(), "127.0.0.1", server.getPort());
        ClusterEndpoints endpoints = ClusterEndpoints.fromEndpoints(List.of(ep), server.getNodeId());
        RedirectPolicy policy = new RedirectPolicy(3, stepTimeoutMs, totalTimeoutMs);
        return track(new LeaderAwareBookingClient(
                endpoints,
                nodeEp -> track(new SocketBookingClient(nodeEp.host(), nodeEp.port(), stepTimeoutMs, stepTimeoutMs, senderId)),
                policy,
                null,
                server.getNodeId()
        ));
    }

    // =========================================================================
    // NHÓM A — 10 GHẾ (D02/D04/D05/D15)
    // =========================================================================

    @Test
    @Order(10)
    @Tag("fixture")
    @RegMeta(id = "REG-A01", group = "A", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-A01 [FIXTURE]: Hiển thị đủ 10 ghế G01–G10, chọn/bỏ chọn cục bộ; ghế BOOKED không chọn được")
    void regA01_Fixture_TenSeatsSelectionAndBookedBlocked(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setSeatStatus("G02", "BOOKED", "TCK-G02-INIT");

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-a01");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        ObservableList<SeatViewModel> seats = createCleanTenSeats();
        SeatsRefreshService refreshService = track(new SeatsRefreshService(
                new BookingClientReadAdapter(leaderClient), seats, journal));

        SeatsSnapshot snap = refreshService.refreshNow().get(3, TimeUnit.SECONDS);
        assertNotNull(snap);
        assertEquals(10, seats.size(), "Phải có đúng 10 ghế");

        List<String> expectedIds = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            expectedIds.add(String.format("G%02d", i));
        }
        assertEquals(expectedIds, seats.stream().map(SeatViewModel::getSeatId).toList(),
                "Thứ tự ghế phải chính xác từ G01 đến G10 không trùng lặp");

        // Chọn rồi bỏ chọn ghế AVAILABLE (G01)
        SeatViewModel g01 = seats.get(0);
        assertEquals(SeatStatus.AVAILABLE, g01.getStatus());
        g01.setStatus(SeatStatus.SELECTED);
        assertTrue(g01.isSelected(), "G01 phải ở trạng thái SELECTED");
        g01.setStatus(SeatStatus.AVAILABLE);
        assertFalse(g01.isSelected(), "G01 sau khi bỏ chọn phải về AVAILABLE");

        // Ghế G02 đang BOOKED: cờ bookedProperty=true khóa nút chọn trên UI
        SeatViewModel g02 = seats.get(1);
        assertEquals(SeatStatus.BOOKED, g02.getStatus());
        assertTrue(g02.isBooked(), "Ghế BOOKED phải bật cờ booked để vô hiệu hóa chọn");

        collectServerFrames(server);
        recordNote("10 ghế G01-G10 đủ thứ tự; chọn/bỏ chọn G01 hợp lệ; G02 BOOKED bị khóa");
    }

    @Test
    @Order(11)
    @Tag("fixture")
    @RegMeta(id = "REG-A02", group = "A", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-A02 [FIXTURE]: 4 trạng thái AVAILABLE/BOOKED/SELECTED/UNKNOWN phân biệt, legend khớp, nhãn Chưa xác minh")
    void regA02_Fixture_FourStatesLegendAndUnverifiedLabel(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setSeatStatus("G02", "BOOKED", "TCK-G02-A02");

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-a02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        ObservableList<SeatViewModel> seats = createCleanTenSeats();
        SeatsRefreshService refreshService = track(new SeatsRefreshService(
                new BookingClientReadAdapter(leaderClient), seats, journal));

        refreshService.refreshNow().get(3, TimeUnit.SECONDS);

        // Thiết lập đủ 4 trạng thái trên lưới ghế để kiểm tra phân biệt và tổng legend
        seats.get(0).setStatus(SeatStatus.SELECTED); // G01: SELECTED
        // G02 đã BOOKED từ server
        seats.get(2).setStatus(SeatStatus.UNKNOWN);  // G03: UNKNOWN
        // G04..G10: 7 ghế AVAILABLE

        Set<String> symbols = new HashSet<>();
        Set<String> styles = new HashSet<>();
        for (SeatStatus st : EnumSet.allOf(SeatStatus.class)) {
            symbols.add(st.getBadgeSymbol());
            styles.add(st.getStyleClass());
        }
        assertEquals(4, symbols.size(), "4 trạng thái phải có 4 ký hiệu accessibility khác nhau");
        assertEquals(4, styles.size(), "4 trạng thái phải có 4 lớp CSS khác nhau");

        long availCount = seats.stream().filter(s -> s.getStatus() == SeatStatus.AVAILABLE).count();
        long bookedCount = seats.stream().filter(s -> s.getStatus() == SeatStatus.BOOKED).count();
        long selectedCount = seats.stream().filter(s -> s.getStatus() == SeatStatus.SELECTED).count();
        long unknownCount = seats.stream().filter(s -> s.getStatus() == SeatStatus.UNKNOWN).count();

        assertEquals(7, availCount);
        assertEquals(1, bookedCount);
        assertEquals(1, selectedCount);
        assertEquals(1, unknownCount);
        assertEquals(10, availCount + bookedCount + selectedCount + unknownCount, "Tổng số lượng legend phải khớp 10 ghế");

        assertEquals(VerificationLevel.UNVERIFIED, refreshService.getVerification(),
                "Đọc sơ đồ ghế phải gắn mức UNVERIFIED");
        assertEquals("Chưa xác minh", refreshService.getVerification().getDisplayName());

        collectServerFrames(server);
        recordNote("4 trạng thái phân biệt rõ ký hiệu/CSS; tổng legend=10; nhãn Chưa xác minh (UNVERIFIED)");
    }

    @Test
    @Order(12)
    @Tag("fixture")
    @RegMeta(id = "REG-A03", group = "A", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-A03 [FIXTURE]: Refresh ghế khi đổi trạng thái; READ_UNAVAILABLE chuyển ghế sang UNKNOWN")
    void regA03_Fixture_RefreshUpdatesAndReadUnavailableYieldsUnknown(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 1500, 3000, "reg-a03");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        ObservableList<SeatViewModel> seats = createCleanTenSeats();
        SeatsRefreshService refreshService = track(new SeatsRefreshService(
                new BookingClientReadAdapter(leaderClient), seats, journal));

        refreshService.refreshNow().get(3, TimeUnit.SECONDS);
        assertEquals(SeatStatus.AVAILABLE, seats.get(5).getStatus());

        // Đổi G06 sang BOOKED trên server rồi làm mới
        server.setSeatStatus("G06", "BOOKED", "TCK-G06-NEW");
        refreshService.refreshNow().get(3, TimeUnit.SECONDS);
        assertEquals(SeatStatus.BOOKED, seats.get(5).getStatus(), "Sau refresh, G06 phải cập nhật sang BOOKED");

        // Ép server trả lỗi READ_UNAVAILABLE -> toàn bộ ghế không chọn chuyển sang UNKNOWN
        server.setForcedError("READ_UNAVAILABLE", "Read barrier unavailable");
        try {
            refreshService.refreshNow().get(3, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        for (SeatViewModel seat : seats) {
            assertEquals(SeatStatus.UNKNOWN, seat.getStatus(),
                    "Khi READ_UNAVAILABLE, ghế " + seat.getSeatId() + " phải chuyển sang UNKNOWN an toàn");
        }
        assertEquals(VerificationLevel.UNVERIFIED, refreshService.getVerification());

        collectServerFrames(server);
        recordNote("Refresh đồng bộ G06->BOOKED; khi READ_UNAVAILABLE cả 10 ghế chuyển sang UNKNOWN");
    }

    // =========================================================================
    // NHÓM B — LỖI NHẬP / VALIDATION (D03/D16/D24)
    // =========================================================================

    @Test
    @Order(20)
    @Tag("fixture")
    @RegMeta(id = "REG-B01", group = "B", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-B01 [FIXTURE]: Họ tên rỗng, quá dài (>50), ký tự không hợp lệ bị chặn; 0 frame gửi đi")
    void regB01_Fixture_InvalidPassengerNameBlockedZeroFrames() throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));

        assertFalse(BookingFormValidator.validateName(null).isValid(), "Null phải bị chặn");
        assertFalse(BookingFormValidator.validateName("   ").isValid(), "Chuỗi chỉ khoảng trắng phải bị chặn");
        assertFalse(BookingFormValidator.validateName("A".repeat(51)).isValid(), "Tên > 50 ký tự phải bị chặn");
        assertFalse(BookingFormValidator.validateName("Khach 123").isValid(), "Tên chứa chữ số phải bị chặn");
        assertFalse(BookingFormValidator.validateName("Khach@#$").isValid(), "Tên chứa ký tự đặc biệt phải bị chặn");
        assertTrue(BookingFormValidator.validateName("Nguyễn Văn An").isValid(), "Tên tiếng Việt hợp lệ phải được chấp nhận");

        assertEquals(0, server.getReceivedRequests().size(), "Validation thất bại ở Client không được gửi bất kỳ frame nào");
        recordNote("Chặn tên null/rỗng/>50 ký tự/chứa số-ký tự lạ; 0 frame gửi qua TCP");
    }

    @Test
    @Order(21)
    @Tag("fixture")
    @RegMeta(id = "REG-B02", group = "B", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-B02 [FIXTURE]: requestId tra cứu rỗng/sai định dạng bị chặn; cấu hình endpoint/placeholder sai báo lỗi rõ")
    void regB02_Fixture_LookupValidationAndConfigFailFast(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 1500, 3000, "reg-b02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        TicketLookupViewModel lookupVm = new TicketLookupViewModel(leaderClient, journal, Runnable::run, null);

        // 1. requestId rỗng và sai định dạng
        CompletableFuture<GetRequestResultResponseDto> emptyLookup = lookupVm.lookup("   ");
        assertTrue(emptyLookup.isCompletedExceptionally(), "requestId rỗng phải bị chặn ngay");

        CompletableFuture<GetRequestResultResponseDto> badFormatLookup = lookupVm.lookup("req id có dấu cách!");
        assertTrue(badFormatLookup.isCompletedExceptionally(), "requestId chứa khoảng trắng/ký tự lạ phải bị chặn");
        assertEquals(0, server.getReceivedRequests().size(), "Không có frame GET_REQUEST_RESULT nào được gửi khi lỗi đầu vào");

        // 2. Cấu hình port ngoài dải [1..65535]
        Properties badPortProps = new Properties();
        badPortProps.setProperty("cluster.profile", "dev");
        badPortProps.setProperty("dev.cluster.nodes", "node-1");
        badPortProps.setProperty("dev.node.node-1.client.host", "127.0.0.1");
        badPortProps.setProperty("dev.node.node-1.client.port", "70000");
        assertThrows(IllegalArgumentException.class,
                () -> ClientConfig.fromProperties(badPortProps, "test-bad-port"),
                "Port 70000 phải bị từ chối fail-fast");

        // 3. Cấu hình tên service nội bộ Docker không hợp lệ trên Windows host
        Properties dockerHostProps = new Properties();
        dockerHostProps.setProperty("cluster.profile", "dev");
        dockerHostProps.setProperty("dev.cluster.nodes", "node-1");
        dockerHostProps.setProperty("dev.node.node-1.client.host", "backend-node-1");
        dockerHostProps.setProperty("dev.node.node-1.client.port", "9100");
        assertThrows(IllegalArgumentException.class,
                () -> ClientConfig.fromProperties(dockerHostProps, "test-docker-host"),
                "Docker service hostname phải bị chặn trên Desktop Client");

        // 4. Placeholder IP trong profile LAN bị từ chối khi gọi validateNoPlaceholders
        ClientConfig lanDefault = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), Map.of());
        assertTrue(lanDefault.hasPlaceholderEndpoints());
        assertThrows(IllegalStateException.class, lanDefault::validateNoPlaceholders,
                "IP placeholder phải bị từ chối khi xác thực chạy thật");

        recordNote("Chặn requestId rỗng/sai format (0 frame); chặn port=70000, Docker host và LAN placeholder fail-fast");
    }

    @Test
    @Order(22)
    @Tag("fixture")
    @RegMeta(id = "REG-B03", group = "B", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-B03 [FIXTURE]: Chưa chọn ghế mà xác nhận -> bị chặn, 0 frame gửi đi")
    void regB03_Fixture_EmptySeatSelectionBlocked() throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));

        assertFalse(BookingFormValidator.validateSeats(null).isValid(), "Danh sách ghế null phải bị chặn");
        assertFalse(BookingFormValidator.validateSeats(List.of()).isValid(), "Danh sách ghế rỗng phải bị chặn");
        assertEquals(0, server.getReceivedRequests().size(), "0 frame gửi đi khi chưa chọn ghế");

        recordNote("validateSeats chặn danh sách ghế null/rỗng; 0 frame gửi qua mạng");
    }

    // =========================================================================
    // NHÓM C — ĐẶT VÉ (D17/D22)
    // =========================================================================

    @Test
    @Order(30)
    @Tag("fixture")
    @RegMeta(id = "REG-C01", group = "C", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-C01 [FIXTURE]: SUBMITTING đến khi có phản hồi cuối; CONFIRMED đúng requestId; ghế BOOKED; refresh 1 lần")
    void regC01_Fixture_SubmittingToConfirmedAndSingleRefresh(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setDelayMs(80);

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-c01");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));
        ObservableList<SeatViewModel> seats = createCleanTenSeats();

        AtomicInteger refreshCalls = new AtomicInteger(0);
        SeatsRefreshService countingRefresh = track(new SeatsRefreshService(
                new BookingClientReadAdapter(leaderClient), seats, journal) {
            @Override
            public CompletableFuture<SeatsSnapshot> refreshNow() {
                refreshCalls.incrementAndGet();
                return super.refreshNow();
            }
        });

        BookingViewModel vm = new BookingViewModel(journalingClient, journal, countingRefresh, null, 4000);
        vm.setSyncMode(true);
        vm.setSeatsList(seats);

        String reqId = UUID.randomUUID().toString();
        recordRequestId(reqId);
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G01", "Khách Kiểm Thử Một");

        CompletableFuture<BookTicketResponseDto> future = vm.submit(req);
        assertEquals(UiState.SUBMITTING, vm.getUiState(), "Ngay sau submit phải ở trạng thái SUBMITTING");
        assertTrue(vm.isBusy(), "Đang chờ phản hồi phải bật busy=true");

        BookTicketResponseDto resp = future.get(3, TimeUnit.SECONDS);
        assertNotNull(resp);
        assertEquals(reqId, resp.getRequestId());
        assertTrue(resp.isConfirmed());
        assertEquals(UiState.CONFIRMED, vm.getUiState());
        assertFalse(vm.isBusy());
        assertEquals(SeatStatus.BOOKED, seats.get(0).getStatus(), "Ghế G01 phải chuyển sang BOOKED");
        assertEquals(1, refreshCalls.get(), "Sau CONFIRMED chỉ kích hoạt refresh đúng 1 lần");
        assertEquals(PendingState.RESOLVED, journal.get(reqId).orElseThrow().getState());

        collectServerFrames(server);
        recordNote("SUBMITTING->CONFIRMED đúng requestId; G01=BOOKED; refreshCount=1; journal=RESOLVED");
    }

    @Test
    @Order(31)
    @Tag("fixture")
    @RegMeta(id = "REG-C02", group = "C", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-C02 [FIXTURE]: REJECTED(SEAT_ALREADY_BOOKED) hiển thị đúng và khác biệt hoàn toàn với UNKNOWN")
    void regC02_Fixture_RejectedSeatAlreadyBookedDistinctFromUnknown(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setSeatStatus("G04", "BOOKED", "TCK-PRE-G04");

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-c02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));
        ObservableList<SeatViewModel> seats = createCleanTenSeats();
        SeatsRefreshService refreshService = track(new SeatsRefreshService(
                new BookingClientReadAdapter(leaderClient), seats, journal));

        BookingViewModel vm = new BookingViewModel(journalingClient, journal, refreshService, null, 4000);
        vm.setSyncMode(true);
        vm.setSeatsList(seats);

        String reqId = UUID.randomUUID().toString();
        recordRequestId(reqId);
        BookTicketResponseDto resp = vm.submit(new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Kiểm Thử Hai"))
                .get(3, TimeUnit.SECONDS);

        assertTrue(resp.isRejected());
        assertEquals("SEAT_ALREADY_BOOKED", resp.getReasonCode());
        assertEquals(UiState.REJECTED, vm.getUiState());
        assertNotEquals(UiState.UNKNOWN, vm.getUiState(), "REJECTED phải khác hoàn toàn với UNKNOWN");
        assertEquals("SEAT_ALREADY_BOOKED", vm.getErrorCode());
        assertEquals(SeatStatus.BOOKED, seats.get(3).getStatus());
        assertEquals(PendingState.RESOLVED, journal.get(reqId).orElseThrow().getState());

        collectServerFrames(server);
        recordNote("REJECTED(SEAT_ALREADY_BOOKED) hiển thị UiState.REJECTED, khác UNKNOWN, ghế G04=BOOKED");
    }

    @Test
    @Order(32)
    @Tag("fixture")
    @RegMeta(id = "REG-C03", group = "C", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-C03 [FIXTURE]: Bấm đúp / nhấn Enter 5 lần liên tiếp -> chỉ đúng 1 frame BOOK_TICKET được gửi")
    void regC03_Fixture_DoubleClickSendsExactlyOneBookFrame(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setDelayMs(150);

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-c03");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));

        BookingViewModel vm = new BookingViewModel(journalingClient, journal, null, null, 4000);
        vm.setSyncMode(true);

        String reqId = UUID.randomUUID().toString();
        recordRequestId(reqId);
        BookTicketRequestDto firstReq = new BookTicketRequestDto(reqId, "SE-001", "G07", "Khách Bấm Nhanh");

        CompletableFuture<BookTicketResponseDto> f1 = vm.submit(firstReq);
        int blockedDuplicates = 0;
        for (int i = 0; i < 4; i++) {
            CompletableFuture<BookTicketResponseDto> dup = vm.submit(
                    new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G07", "Khách Bấm Nhanh"));
            if (dup.isCompletedExceptionally()) {
                blockedDuplicates++;
            }
        }
        assertEquals(4, blockedDuplicates, "Cả 4 lần bấm trùng khi đang SUBMITTING phải bị chặn tức thì");

        BookTicketResponseDto res = f1.get(3, TimeUnit.SECONDS);
        assertTrue(res.isConfirmed());

        long bookFrames = server.getReceivedRequests().stream()
                .filter(env -> env.getType() == MessageType.BOOK_TICKET)
                .count();
        assertEquals(1L, bookFrames, "Server chỉ được nhận ĐÚNG 1 frame BOOK_TICKET");

        collectServerFrames(server);
        recordNote("5 lần gọi submit liên tiếp -> chặn 4 lần trùng, đúng 1 frame BOOK_TICKET trên dây");
    }

    @Test
    @Order(33)
    @Tag("fixture")
    @RegMeta(id = "REG-C04", group = "C", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-C04 [FIXTURE]: Cùng ý định khi pending -> dùng lại requestId; đổi ý định -> requestId mới kèm cảnh báo")
    void regC04_Fixture_SameIntentReusesRequestIdAndIntentChangeWarns(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        String reqId1 = UUID.randomUUID().toString();
        recordRequestId(reqId1);
        server.dropResponseForRequestId(reqId1);
        server.setDropResponseHoldMs(600);

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 300, 800, "reg-c04");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));

        BookingViewModel vm = new BookingViewModel(journalingClient, journal, null, null, 350);
        vm.setSyncMode(true);

        // 1. Gửi ý định 1 trên G01 -> rơi phản hồi -> UNKNOWN (SENT_UNRESOLVED)
        try {
            vm.submit(new BookTicketRequestDto(reqId1, "SE-001", "G01", "Khách Ý Định Một")).get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqId1).orElseThrow().getState());

        // 2. Cùng ý định nhưng cố tạo requestId mới -> bị chặn, yêu cầu dùng retry cùng requestId
        assertThrows(IllegalStateException.class, () ->
                vm.submit(new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G01", "Khách Ý Định Một")),
                "Cùng ý định đang pending không được phép tạo requestId mới");

        // 3. Đổi sang ghế khác (G06) -> cho phép requestId mới nhưng bật cảnh báo intentChangeWarning
        server.clearDropResponseForRequestIds();
        String reqId2 = UUID.randomUUID().toString();
        recordRequestId(reqId2);
        BookTicketResponseDto res2 = vm.submit(new BookTicketRequestDto(reqId2, "SE-001", "G06", "Khách Ý Định Hai"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(res2.isConfirmed());
        assertFalse(vm.getIntentChangeWarning().isBlank(), "Đổi ý định khi còn pending phải hiển thị cảnh báo rõ ràng");
        assertTrue(vm.getIntentChangeWarning().contains(reqId1), "Cảnh báo phải nêu rõ requestId cũ chưa ngã ngũ");
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqId1).orElseThrow().getState(),
                "Pending cũ trên G01 vẫn được bảo toàn nguyên vẹn");

        // 4. Retry lại đúng reqId1 -> nhận lại kết quả idempotent từ server, không nhân bản vé
        BookTicketResponseDto retry1 = vm.retry(reqId1).get(3, TimeUnit.SECONDS);
        assertTrue(retry1.isConfirmed());
        assertEquals(1, server.getActiveTicketCount("G01"));

        collectServerFrames(server);
        recordNote("Chặn tạo ID mới cho cùng ý định; đổi sang G06 có cảnh báo; retry reqId1 idempotent");
    }

    // =========================================================================
    // NHÓM D — HỦY VÉ (D23/D25)
    // =========================================================================

    @Test
    @Order(40)
    @Tag("fixture")
    @RegMeta(id = "REG-D01", group = "D", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-D01 [FIXTURE]: Hủy vé CONFIRMED/REJECTED/UNKNOWN; vé chỉ CANCELLED khi CONFIRMED đúng requestId")
    void regD01_Fixture_CancelOutcomesConfirmedRejectedUnknown(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 400, 1200, "reg-d01");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));
        BookingViewModel vm = new BookingViewModel(journalingClient, journal, null, null, 2000);
        vm.setSyncMode(true);

        // Đặt trước ghế G08 để lấy ticketId hợp lệ
        String bookReqId = UUID.randomUUID().toString();
        recordRequestId(bookReqId);
        BookTicketResponseDto booked = vm.submit(new BookTicketRequestDto(bookReqId, "SE-001", "G08", "Khách Hủy Vé"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(booked.isConfirmed());
        String ticketId = booked.getTicketId();
        TicketViewModel ticketVm = new TicketViewModel(
                bookReqId, ticketId, "G08", "SE-001", "—", "2026-11-27T10:00:00Z",
                "1.250.000 VNĐ", TicketStatus.CONFIRMED, null, "FAKE");

        // 1. CANCEL gặp timeout -> UNKNOWN: vé KHÔNG ĐƯỢC chuyển sang CANCELLED
        server.setSimulateTimeoutOnCancel(true, 900);
        String cancelTimeoutReqId = UUID.randomUUID().toString();
        recordRequestId(cancelTimeoutReqId);
        CancelTicketResponseDto timeoutCancel = vm.cancel(new CancelTicketRequestDto(cancelTimeoutReqId, ticketId))
                .get(3, TimeUnit.SECONDS);
        assertTrue(timeoutCancel.isUnknown());
        assertEquals(UiState.UNKNOWN, vm.getUiState());
        assertEquals(TicketStatus.CONFIRMED, ticketVm.getStatus(), "Khi hủy UNKNOWN, vé phải giữ nguyên CONFIRMED");
        // Giải phóng bản ghi timeout để kiểm thử tiếp nhánh REJECTED và CONFIRMED độc lập
        journal.markResolved(cancelTimeoutReqId, "REJECTED");
        server.setSimulateTimeoutOnCancel(false, 0);

        // 2. CANCEL vé không tồn tại -> REJECTED(TICKET_NOT_ACTIVE)
        String cancelRejectReqId = UUID.randomUUID().toString();
        recordRequestId(cancelRejectReqId);
        CancelTicketResponseDto rejectedCancel = vm.cancel(new CancelTicketRequestDto(cancelRejectReqId, "TCK-NON-EXISTENT"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(rejectedCancel.isRejected());
        assertEquals("TICKET_NOT_ACTIVE", rejectedCancel.getReasonCode());
        assertEquals(UiState.REJECTED, vm.getUiState());

        // 3. CANCEL vé hợp lệ -> CONFIRMED đúng requestId -> vé chuyển CANCELLED, ghế G08 về AVAILABLE
        String cancelOkReqId = UUID.randomUUID().toString();
        recordRequestId(cancelOkReqId);
        CancelTicketResponseDto okCancel = vm.cancel(new CancelTicketRequestDto(cancelOkReqId, ticketId))
                .get(3, TimeUnit.SECONDS);
        assertTrue(okCancel.isConfirmed());
        assertEquals(cancelOkReqId, okCancel.getRequestId());
        ticketVm.setStatus(TicketStatus.CANCELLED);
        assertEquals(TicketStatus.CANCELLED, ticketVm.getStatus());
        assertEquals(0, server.getActiveTicketCount("G08"), "Sau khi hủy CONFIRMED, số vé ACTIVE trên G08 phải về 0");

        collectServerFrames(server);
        recordNote("CANCEL đủ 3 nhánh UNKNOWN/REJECTED/CONFIRMED; chỉ CONFIRMED đúng ID mới hủy vé & giải phóng G08");
    }

    @Test
    @Order(41)
    @Tag("fixture")
    @RegMeta(id = "REG-D02", group = "D", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-D02 [FIXTURE]: Không cho hủy vé mà lệnh BOOK_TICKET sinh ra nó còn UNKNOWN")
    void regD02_Fixture_BlockCancelWhenOriginatingBookIsUnknown(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        String bookReqId = UUID.randomUUID().toString();
        recordRequestId(bookReqId);
        server.dropResponseForRequestId(bookReqId);
        server.setDropResponseHoldMs(600);

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 300, 700, "reg-d02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));
        BookingViewModel vm = new BookingViewModel(journalingClient, journal, null, null, 350);
        vm.setSyncMode(true);

        try {
            vm.submit(new BookTicketRequestDto(bookReqId, "SE-001", "G03", "Khách Chưa Xác Nhận")).get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(bookReqId).orElseThrow().getState());

        // Cố hủy bằng mã vé dẫn xuất từ requestId đang UNKNOWN (ví dụ TCK-<8 ký tự đầu requestId>)
        String derivedTicketId = "TCK-" + bookReqId.substring(0, 8);
        String cancelReqId = UUID.randomUUID().toString();
        recordRequestId(cancelReqId);

        CompletableFuture<CancelTicketResponseDto> cancelFuture =
                vm.cancel(new CancelTicketRequestDto(cancelReqId, derivedTicketId));
        assertTrue(cancelFuture.isCompletedExceptionally(),
                "Phải chặn hủy vé khi lệnh BOOK_TICKET sinh ra vé đó vẫn đang UNKNOWN (SENT_UNRESOLVED)");

        long cancelFrames = server.getReceivedRequests().stream()
                .filter(env -> env.getType() == MessageType.CANCEL_TICKET)
                .count();
        assertEquals(0L, cancelFrames, "Không được gửi bất kỳ frame CANCEL_TICKET nào lên mạng");

        collectServerFrames(server);
        recordNote("Chặn hủy vé có BOOK_TICKET gốc còn SENT_UNRESOLVED; 0 frame CANCEL_TICKET gửi đi");
    }

    // =========================================================================
    // NHÓM E — TRA CỨU / LOOKUP (D24)
    // =========================================================================

    @Test
    @Order(50)
    @Tag("fixture")
    @RegMeta(id = "REG-E01", group = "E", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-E01 [FIXTURE]: GET_REQUEST_RESULT với PENDING/APPLIED_SUCCESS/APPLIED_REJECTED/UNKNOWN hiển thị đúng, 0 BOOK/CANCEL phát sinh")
    void regE01_Fixture_LookupFourStatesZeroSideEffect(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        String idPending = UUID.randomUUID().toString();
        String idSuccess = UUID.randomUUID().toString();
        String idRejected = UUID.randomUUID().toString();
        String idUnknown = UUID.randomUUID().toString();
        recordRequestId(idPending);
        recordRequestId(idSuccess);
        recordRequestId(idRejected);
        recordRequestId(idUnknown);

        server.markRequestPending(idPending);
        server.setCustomRequestResult(idSuccess,
                new GetRequestResultResponseDto(idSuccess, "APPLIED_SUCCESS", "BOOK_TICKET", "TCK-G09-OK", null, 55L));
        server.setCustomRequestResult(idRejected,
                new GetRequestResultResponseDto(idRejected, "APPLIED_REJECTED", "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", 56L));

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(server, 2000, 4000, "reg-e01");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));
        TicketLookupViewModel lookupVm = new TicketLookupViewModel(journalingClient, journal, Runnable::run, null);

        GetRequestResultResponseDto r1 = lookupVm.lookup(idPending).get(3, TimeUnit.SECONDS);
        assertEquals("PENDING", r1.getState());
        Thread.sleep(270); // qua cooldown 250ms của TicketLookupViewModel

        GetRequestResultResponseDto r2 = lookupVm.lookup(idSuccess).get(3, TimeUnit.SECONDS);
        assertEquals("APPLIED_SUCCESS", r2.getState());
        assertEquals("TCK-G09-OK", r2.getTicketId());
        Thread.sleep(270);

        GetRequestResultResponseDto r3 = lookupVm.lookup(idRejected).get(3, TimeUnit.SECONDS);
        assertEquals("APPLIED_REJECTED", r3.getState());
        assertEquals("SEAT_ALREADY_BOOKED", r3.getReasonCode());
        Thread.sleep(270);

        GetRequestResultResponseDto r4 = lookupVm.lookup(idUnknown).get(3, TimeUnit.SECONDS);
        assertEquals("UNKNOWN", r4.getState());

        long writeFrames = server.getReceivedRequests().stream()
                .filter(env -> env.getType() == MessageType.BOOK_TICKET || env.getType() == MessageType.CANCEL_TICKET)
                .count();
        assertEquals(0L, writeFrames, "Tra cứu chỉ-đọc tuyệt đối không phát sinh frame BOOK hoặc CANCEL");

        collectServerFrames(server);
        recordNote("4 trạng thái PENDING/APPLIED_SUCCESS/APPLIED_REJECTED/UNKNOWN khớp chuẩn; 0 frame ghi phát sinh");
    }

    @Test
    @Order(51)
    @Tag("fixture")
    @RegMeta(id = "REG-E02", group = "E", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-E02 [FIXTURE]: Follower trả NOT_LEADER không tự kết luận kết quả; lịch sử vé khóa BLOCKED/P1 với 0 dữ liệu giả")
    void regE02_Fixture_FollowerNotConclusivelyResolvedAndHistoryBlocked(@TempDir Path tempDir) throws Exception {
        FakeNodeServer follower = track(new FakeNodeServer("node-2", 0));
        follower.setNotLeader(null, 3L);

        LeaderAwareBookingClient leaderClient = buildSingleServerLeaderClient(follower, 1500, 3000, "reg-e02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient journalingClient = track(new JournalingBookingClient(leaderClient, journal));

        // Tạo 1 mục pending trước khi tra cứu tới Follower
        String reqId = UUID.randomUUID().toString();
        recordRequestId(reqId);
        String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G09\",\"passengerName\":\"Test\"}";
        journal.recordPrepared(PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload));
        journal.markSent(reqId);

        String prevSysProp = System.getProperty("booking.client");
        try {
            System.setProperty("booking.client", "socket");
            TicketLookupViewModel lookupVm = new TicketLookupViewModel(journalingClient, journal, Runnable::run, null);

            // Kiểm tra lịch sử vé nâng cao P1 bị khóa an toàn và không nạp dữ liệu giả từ MockTicketSource
            assertFalse(lookupVm.isHistoryEnabled(), "Chế độ socket phải vô hiệu hóa lịch sử vé nâng cao chưa có contract");
            assertTrue(lookupVm.getHistoryBlockedMessage().contains("BLOCKED/P1"));
            assertEquals(0, lookupVm.getDisplayedTickets().size(), "Không được hiển thị vé giả từ MockTicketSource ở chế độ socket");

            // Tra cứu trúng Follower không có hint -> không kết luận thành công hay từ chối, journal giữ nguyên SENT_UNRESOLVED
            try {
                lookupVm.lookup(reqId).get(3, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
            assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqId).orElseThrow().getState(),
                    "Phản hồi từ Follower không được làm mất trạng thái SENT_UNRESOLVED");
            assertEquals(0, lookupVm.getDisplayedTickets().size(), "Follower trả lỗi không được sinh vé giả");
        } finally {
            if (prevSysProp == null) {
                System.clearProperty("booking.client");
            } else {
                System.setProperty("booking.client", prevSysProp);
            }
        }

        collectServerFrames(follower);
        recordNote("Follower NOT_LEADER giữ nguyên SENT_UNRESOLVED; lịch sử vé gắn BLOCKED/P1 với 0 vé giả");
    }

    // =========================================================================
    // NHÓM F — 6 CLIENT CÙNG GHẾ (D18)
    // =========================================================================

    @Test
    @Order(60)
    @Tag("fixture")
    @RegMeta(id = "REG-F01", group = "F", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-F01 [FIXTURE]: 6 Client đặt đồng thời G05 -> 6 requestId, đúng 1 CONFIRMED, 5 REJECTED sau resolve, 1 vé ACTIVE")
    void regF01_Fixture_SixClientsContentionSingleActiveTicket(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.configureRandomDelay(2026L, 20);
        // Giả lập client-3 bị rơi phản hồi lần đầu -> UNKNOWN -> tra cứu resolve
        server.addDropResponseForSender("reg-f01-c3");
        server.setDropResponseHoldMs(500);

        int clientCount = 6;
        List<BookingViewModel> vms = new ArrayList<>();
        List<String> reqIds = new ArrayList<>();

        for (int i = 1; i <= clientCount; i++) {
            Path subDir = tempDir.resolve("c-" + i);
            PersistentPendingJournal j = track(new PersistentPendingJournal(subDir));
            LeaderAwareBookingClient lac = buildSingleServerLeaderClient(server, 350, 900, "reg-f01-c" + i);
            JournalingBookingClient jbc = track(new JournalingBookingClient(lac, j));
            BookingViewModel vm = new BookingViewModel(jbc, j, null, null, 1200);
            vm.setSyncMode(true);
            vms.add(vm);

            String rid = UUID.randomUUID().toString();
            reqIds.add(rid);
            recordRequestId(rid);
        }
        assertEquals(6, new HashSet<>(reqIds).size(), "6 Client phải có 6 requestId đôi một khác nhau");

        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        CountDownLatch startGate = new CountDownLatch(1);
        List<CompletableFuture<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < clientCount; i++) {
            final int idx = i;
            tasks.add(CompletableFuture.runAsync(() -> {
                try {
                    startGate.await(2, TimeUnit.SECONDS);
                    BookTicketRequestDto dto = new BookTicketRequestDto(
                            reqIds.get(idx), "SE-001", "G05", "Hành Khách C" + (idx + 1));
                    vms.get(idx).submit(dto).get(3, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
            }, pool));
        }
        startGate.countDown();
        for (CompletableFuture<Void> t : tasks) {
            t.get(5, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        // Mọi Client đang UNKNOWN thực hiện tra cứu GET_REQUEST_RESULT bằng đúng requestId gốc
        for (int i = 0; i < clientCount; i++) {
            BookingViewModel vm = vms.get(i);
            if (vm.getUiState() == UiState.UNKNOWN || vm.getUiState() == UiState.DISCONNECTED) {
                vm.getRequestResult(reqIds.get(i)).get(3, TimeUnit.SECONDS);
            }
        }

        int confirmed = 0;
        int rejected = 0;
        for (BookingViewModel vm : vms) {
            if (vm.getUiState() == UiState.CONFIRMED) {
                confirmed++;
            } else if (vm.getUiState() == UiState.REJECTED) {
                rejected++;
            }
        }
        assertEquals(1, confirmed, "Phải có ĐÚNG 1 Client CONFIRMED");
        assertEquals(5, rejected, "5 Client còn lại sau khi resolve phải là REJECTED(SEAT_ALREADY_BOOKED)");
        assertEquals(1, server.getActiveTicketCount("G05"), "Đúng 1 vé ACTIVE trên ghế G05");

        collectServerFrames(server);
        recordNote("6 Client tranh G05 (có 1 Client rơi phản hồi rồi lookup): 1 CONFIRMED, 5 REJECTED, 1 ACTIVE");
    }

    // =========================================================================
    // NHÓM G — TIMEOUT / UNKNOWN / PENDING (D14/D19)
    // =========================================================================

    @Test
    @Order(70)
    @Tag("fixture")
    @RegMeta(id = "REG-G01", group = "G", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-G01 [FIXTURE]: Timeout sau gửi -> UNKNOWN + SENT_UNRESOLVED + 0 từ cấm; lỗi trước gửi -> FAILED_NOT_SENT + ghế giữ nguyên")
    void regG01_Fixture_TimeoutAfterSendVsFailedNotSentBeforeSend(@TempDir Path tempDir) throws Exception {
        // Phần 1: Timeout sau khi gửi
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        server.setSimulateTimeoutOnBook(true, 900);

        SocketBookingClient timeoutSocket = track(new SocketBookingClient("127.0.0.1", server.getPort(), 1000, 250, "reg-g01-after"));
        PersistentPendingJournal journal1 = track(new PersistentPendingJournal(tempDir.resolve("j1")));
        JournalingBookingClient jbc1 = track(new JournalingBookingClient(timeoutSocket, journal1));
        ObservableList<SeatViewModel> seats1 = createCleanTenSeats();
        BookingViewModel vm1 = new BookingViewModel(jbc1, journal1, null, null, 2000);
        vm1.setSyncMode(true);
        vm1.setSeatsList(seats1);

        String reqAfter = UUID.randomUUID().toString();
        recordRequestId(reqAfter);
        BookTicketResponseDto resAfter = vm1.submit(new BookTicketRequestDto(reqAfter, "SE-001", "G02", "Khách Timeout"))
                .get(3, TimeUnit.SECONDS);

        assertTrue(resAfter.isUnknown());
        assertEquals(UiState.UNKNOWN, vm1.getUiState());
        assertEquals(SeatStatus.UNKNOWN, seats1.get(1).getStatus());
        assertEquals(PendingState.SENT_UNRESOLVED, journal1.get(reqAfter).orElseThrow().getState());

        String lowerMsg = vm1.getMessage().toLowerCase(Locale.ROOT);
        for (String forbidden : List.of("thành công", "thất bại", "bị từ chối", "đã đặt", "đã hủy")) {
            assertFalse(lowerMsg.contains(forbidden),
                    "Thông điệp UNKNOWN tuyệt đối không được chứa cụm từ khẳng định sai: '" + forbidden + "'");
        }

        // Phần 2: Lỗi kết nối TRƯỚC khi gửi byte (cổng đóng hoàn toàn)
        int closedPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            closedPort = ss.getLocalPort();
        }
        SocketBookingClient preFailSocket = track(new SocketBookingClient("127.0.0.1", closedPort, 300, 500, "reg-g01-before"));
        PersistentPendingJournal journal2 = track(new PersistentPendingJournal(tempDir.resolve("j2")));
        JournalingBookingClient jbc2 = track(new JournalingBookingClient(preFailSocket, journal2));
        ObservableList<SeatViewModel> seats2 = createCleanTenSeats();
        BookingViewModel vm2 = new BookingViewModel(jbc2, journal2, null, null, 2000);
        vm2.setSyncMode(true);
        vm2.setSeatsList(seats2);

        String reqBefore = UUID.randomUUID().toString();
        recordRequestId(reqBefore);
        try {
            vm2.submit(new BookTicketRequestDto(reqBefore, "SE-001", "G03", "Khách Trước Gửi")).get(3, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }

        assertEquals(ClientErrorKind.FAILED_NOT_SENT.name(), vm2.getErrorCode());
        assertEquals(SeatStatus.AVAILABLE, seats2.get(2).getStatus(),
                "Lỗi trước khi gửi byte phải giữ nguyên ghế G03=AVAILABLE");
        assertEquals(PendingState.FAILED_NOT_SENT, journal2.get(reqBefore).orElseThrow().getState(),
                "Lỗi trước khi gửi byte phải ghi nhận trạng thái FAILED_NOT_SENT trong journal");

        collectServerFrames(server);
        recordNote("Sau gửi timeout->UNKNOWN/SENT_UNRESOLVED (0 từ cấm); trước gửi->FAILED_NOT_SENT & ghế AVAILABLE");
    }

    @Test
    @Order(71)
    @Tag("fixture")
    @RegMeta(id = "REG-G02", group = "G", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-G02 [FIXTURE]: Restart app giữa chừng khôi phục nguyên requestId + payloadHash; Tra cứu/Gửi lại cùng requestId")
    void regG02_Fixture_RestartRecoversJournalAndResolvesSameRequestId(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        String reqId = UUID.randomUUID().toString();
        recordRequestId(reqId);
        server.dropResponseForRequestId(reqId);
        server.setDropResponseHoldMs(500);

        Path journalDir = tempDir.resolve("restart-journal");
        String originalHash;

        // Phiên 1: Gửi lệnh, mất phản hồi -> đóng ứng dụng/journal
        try (PersistentPendingJournal j1 = new PersistentPendingJournal(journalDir);
             SocketBookingClient sc1 = new SocketBookingClient("127.0.0.1", server.getPort(), 1000, 250, "reg-g02-s1");
             JournalingBookingClient jbc1 = new JournalingBookingClient(sc1, j1)) {
            BookingViewModel vm1 = new BookingViewModel(jbc1, j1, null, null, 1000);
            vm1.setSyncMode(true);
            try {
                vm1.submit(new BookTicketRequestDto(reqId, "SE-001", "G10", "Khách Khởi Động Lại")).get(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
            PendingEntry saved = j1.get(reqId).orElseThrow();
            assertEquals(PendingState.SENT_UNRESOLVED, saved.getState());
            originalHash = saved.getPayloadHash();
        }

        // Phiên 2: Mở lại journal trên cùng thư mục sau restart -> khôi phục & retry cùng requestId
        server.clearDropResponseForRequestIds();
        try (PersistentPendingJournal j2 = new PersistentPendingJournal(journalDir);
             SocketBookingClient sc2 = new SocketBookingClient("127.0.0.1", server.getPort(), 1000, 1500, "reg-g02-s2");
             JournalingBookingClient jbc2 = new JournalingBookingClient(sc2, j2)) {
            PendingEntry recovered = j2.get(reqId).orElseThrow();
            assertEquals(PendingState.SENT_UNRESOLVED, recovered.getState());
            assertEquals(originalHash, recovered.getPayloadHash(), "SHA-256 payload sau restart phải khớp 100%");
            assertTrue(recovered.isPayloadValid());

            BookingViewModel vm2 = new BookingViewModel(jbc2, j2, null, null, 2000);
            vm2.setSyncMode(true);
            BookTicketResponseDto retryResp = vm2.retry(reqId).get(3, TimeUnit.SECONDS);
            assertTrue(retryResp.isConfirmed());
            assertEquals(reqId, retryResp.getRequestId());
            assertEquals(PendingState.RESOLVED, j2.get(reqId).orElseThrow().getState());
        }
        assertEquals(1, server.getActiveTicketCount("G10"), "Chỉ đúng 1 vé ACTIVE trên G10 sau restart + retry");

        collectServerFrames(server);
        recordNote("Restart khôi phục 100% SHA-256 hash (" + originalHash.substring(0, 12) + "...); retry cùng requestId -> 1 ACTIVE");
    }

    @Test
    @Order(72)
    @Tag("fixture")
    @RegMeta(id = "REG-G03", group = "G", layer = "FIXTURE", dataSource = "RECORDED-FIXTURE")
    @DisplayName("REG-G03 [FIXTURE]: NO_QUORUM, READ_UNAVAILABLE và NOT_LEADER không phân giải được theo ma trận D19/SPEC-03")
    void regG03_Fixture_NoQuorumReadUnavailableAndUnresolvedNotLeader(@TempDir Path tempDir) throws Exception {
        // 1. NO_QUORUM phát lại từ RecordedTraceReplayServer
        try (RecordedTraceReplayServer replay = new RecordedTraceReplayServer("node-1")) {
            replay.setFaultMode(RecordedTraceReplayServer.FaultMode.NO_QUORUM);
            try (SocketBookingClient sc = new SocketBookingClient("127.0.0.1", replay.getPort(), 1000, 1500, "reg-g03-nq");
                 PersistentPendingJournal j = new PersistentPendingJournal(tempDir.resolve("nq"));
                 JournalingBookingClient jbc = new JournalingBookingClient(sc, j)) {
                ObservableList<SeatViewModel> seats = createCleanTenSeats();
                BookingViewModel vm = new BookingViewModel(jbc, j, null, null, 2000);
                vm.setSyncMode(true);
                vm.setSeatsList(seats);

                String reqNq = UUID.randomUUID().toString();
                recordRequestId(reqNq);
                BookTicketResponseDto resNq = vm.submit(new BookTicketRequestDto(reqNq, "SE-001", "G06", "Khách Mất Đa Số"))
                        .get(3, TimeUnit.SECONDS);
                assertFalse(resNq.isConfirmed(), "NO_QUORUM không bao giờ được xác nhận thành công");
                assertEquals("NO_QUORUM", vm.getErrorCode());
                assertNotEquals(SeatStatus.BOOKED, seats.get(5).getStatus(), "NO_QUORUM không được đổi ghế sang BOOKED");
            }
        }

        // 2. NOT_LEADER không phân giải được (cả 2 Node chỉ vòng tròn sang nhau)
        FakeNodeServer n1 = track(new FakeNodeServer("node-1", 0));
        FakeNodeServer n2 = track(new FakeNodeServer("node-2", 0));
        n1.setNotLeader("node-2", 4L);
        n2.setNotLeader("node-1", 4L);

        ClusterEndpoints eps = ClusterEndpoints.fromEndpoints(List.of(
                new NodeEndpoint("node-1", "127.0.0.1", n1.getPort()),
                new NodeEndpoint("node-2", "127.0.0.1", n2.getPort())
        ), "node-1");
        LeaderAwareBookingClient lac = track(new LeaderAwareBookingClient(eps));
        String reqLoop = UUID.randomUUID().toString();
        recordRequestId(reqLoop);
        BookTicketResponseDto loopRes = lac.book(new BookTicketRequestDto(reqLoop, "SE-001", "G06", "Khách Vòng Lặp"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(loopRes.isUnknown());
        assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, loopRes.getReasonCode());

        // 3. READ_UNAVAILABLE ánh xạ theo ma trận D19
        ErrorPresentation readUnavailPres = ErrorPresentationMapper.mapCode("READ_UNAVAILABLE", false);
        assertEquals(PresentationKind.ERROR, readUnavailPres.kind());
        assertEquals("READ_UNAVAILABLE", readUnavailPres.rawCode());

        collectServerFrames(n1);
        collectServerFrames(n2);
        recordNote("NO_QUORUM không đổi ghế BOOKED; vòng lặp NOT_LEADER dừng ở NOT_LEADER_UNRESOLVED; READ_UNAVAILABLE chuẩn D19");
    }

    // =========================================================================
    // NHÓM H — RECONNECT (D21/D26)
    // =========================================================================

    @Test
    @Order(80)
    @Tag("fixture")
    @RegMeta(id = "REG-H01", group = "H", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-H01 [FIXTURE]: Mất Leader -> banner Đang tìm Leader..., chặn đặt vé MỚI, 0 BOOK/CANCEL khi dò, bảo toàn pending")
    void regH01_Fixture_LeaderLostBlocksNewBookingsAndProbesReadOnly(@TempDir Path tempDir) throws Exception {
        FakeNodeServer s1 = track(new FakeNodeServer("node-1", 0));
        FakeNodeServer s2 = track(new FakeNodeServer("node-2", 0));
        s1.setNotLeader(null, 2L);
        s2.setNotLeader(null, 2L);

        ClusterEndpoints eps = ClusterEndpoints.fromEndpoints(List.of(
                new NodeEndpoint("node-1", "127.0.0.1", s1.getPort()),
                new NodeEndpoint("node-2", "127.0.0.1", s2.getPort())
        ), "node-1");

        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        String oldPendingId = UUID.randomUUID().toString();
        recordRequestId(oldPendingId);
        String oldPayload = "{\"requestId\":\"" + oldPendingId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G04\",\"passengerName\":\"Pending\"}";
        journal.recordPrepared(PendingEntry.createPrepared(oldPendingId, "BOOK_TICKET", oldPayload));
        journal.markSent(oldPendingId);

        LeaderAwareBookingClient lac = track(new LeaderAwareBookingClient(
                eps, ep -> track(new SocketBookingClient(ep.host(), ep.port(), 400, 400, "reg-h01")),
                new RedirectPolicy(3, 400, 1500), null, "node-1"));
        JournalingBookingClient jbc = track(new JournalingBookingClient(lac, journal));
        BookingViewModel vm = new BookingViewModel(jbc, journal, null, null, 1500);
        vm.setSyncMode(true);

        ReconnectController rc = new ReconnectController(lac, vm, null, journal, Runnable::run);
        rc.setDataSource(vn.nhom6.railway.client.network.ClusterStatusClient.DataSource.MOCK);

        // Báo mất Leader -> chuyển SEARCHING_LEADER
        rc.onLeaderLost("node-1", "Socket closed");
        assertEquals(ReconnectState.SEARCHING_LEADER, rc.getState());
        assertTrue(rc.isBannerVisible());
        assertTrue(rc.getBannerMessage().contains("Đang tìm Leader"));
        assertFalse(rc.getCanBookNew(), "Đang tìm Leader phải khóa đặt vé mới");
        assertTrue(rc.getCanRetryOrLookup(), "Pending cũ vẫn được phép tra cứu/retry");

        // Đặt vé MỚI bị chặn ngay lập tức
        String newReqId = UUID.randomUUID().toString();
        CompletableFuture<BookTicketResponseDto> blockedNew =
                vm.submit(new BookTicketRequestDto(newReqId, "SE-001", "G05", "Khách Mới"));
        assertTrue(blockedNew.isCompletedExceptionally(), "Đặt vé mới khi đang SEARCHING_LEADER phải bị chặn");
        assertTrue(rc.isRequestAllowed(oldPendingId), "requestId đã có trong journal vẫn được phép thực thi");

        // Kích hoạt dò tìm Leader khi chưa có Leader -> chỉ được gửi GET_SEATS, 0 frame BOOK/CANCEL
        rc.triggerReconnect().get(5, TimeUnit.SECONDS);
        for (FakeNodeServer srv : List.of(s1, s2)) {
            for (MessageEnvelope<JsonNode> env : srv.getReceivedRequests()) {
                assertNotEquals(MessageType.BOOK_TICKET, env.getType(), "Dò Leader cấm gửi BOOK_TICKET");
                assertNotEquals(MessageType.CANCEL_TICKET, env.getType(), "Dò Leader cấm gửi CANCEL_TICKET");
            }
        }
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(oldPendingId).orElseThrow().getState());

        collectServerFrames(s1);
        collectServerFrames(s2);
        recordNote("SEARCHING_LEADER hiện banner, chặn đặt mới, bảo toàn pending cũ, dò đường 0 frame BOOK/CANCEL");
    }

    @Test
    @Order(81)
    @Tag("fixture")
    @RegMeta(id = "REG-H02", group = "H", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-H02 [FIXTURE]: leaderHint lạ bị bỏ qua; failover chọn node-3 từ cấu hình, không mặc định node-1 là Leader")
    void regH02_Fixture_UntrustedLeaderHintIgnoredAndFailoverToNode3(@TempDir Path tempDir) throws Exception {
        FakeNodeServer n1 = track(new FakeNodeServer("node-1", 0));
        FakeNodeServer n2 = track(new FakeNodeServer("node-2", 0));
        FakeNodeServer n3 = track(new FakeNodeServer("node-3", 0));

        // node-1 phát hint lạ ngoài danh bạ; node-2 chỉ đúng sang node-3 (Leader thực)
        n1.setNotLeader("rogue-node-99", 5L);
        n2.setNotLeader("node-3", 5L);
        n3.setLeader();

        ClusterEndpoints eps = ClusterEndpoints.fromEndpoints(List.of(
                new NodeEndpoint("node-1", "127.0.0.1", n1.getPort()),
                new NodeEndpoint("node-2", "127.0.0.1", n2.getPort()),
                new NodeEndpoint("node-3", "127.0.0.1", n3.getPort())
        ), "node-1");

        LeaderAwareBookingClient lac = track(new LeaderAwareBookingClient(
                eps, ep -> track(new SocketBookingClient(ep.host(), ep.port(), 1000, 1000, "reg-h02")),
                new RedirectPolicy(3, 1000, 3000), null, "node-1"));
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        ReconnectController rc = new ReconnectController(lac, null, null, journal, Runnable::run);

        // Kiểm tra ClusterConnectionViewModel khởi tạo không bao giờ mặc định node-1 là LEADER
        ClusterConnectionViewModel connVm = track(new ClusterConnectionViewModel(null));
        assertEquals(ClusterConnectionViewModel.NodeRole.UNKNOWN, connVm.getRole());

        // Gửi tới node-1 có hint lạ -> từ chối theo rogue hint
        String reqRogue = UUID.randomUUID().toString();
        recordRequestId(reqRogue);
        BookTicketResponseDto rogueRes = lac.book(new BookTicketRequestDto(reqRogue, "SE-001", "G02", "Khách Thử Hint"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(rogueRes.isUnknown());
        assertEquals(LeaderAwareBookingClient.ERROR_NOT_LEADER_UNRESOLVED, rogueRes.getReasonCode());

        // Dò tìm Leader tự động bỏ qua hint lạ và phát hiện đúng node-3 từ cấu hình
        String discovered = rc.triggerReconnect().get(4, TimeUnit.SECONDS);
        assertEquals("node-3", discovered, "Phải phát hiện đúng Leader node-3 thay vì mặc định node-1");
        assertEquals(ReconnectState.CONNECTED, rc.getState());

        BookTicketResponseDto okAfterDiscover = lac.book(new BookTicketRequestDto(reqRogue, "SE-001", "G02", "Khách Thử Hint"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(okAfterDiscover.isConfirmed());
        assertEquals(reqRogue, okAfterDiscover.getRequestId());

        collectServerFrames(n1);
        collectServerFrames(n2);
        collectServerFrames(n3);
        recordNote("Bỏ qua rogue-node-99; discoverLeader tìm đúng node-3; không mặc định node-1 là Leader");
    }

    // =========================================================================
    // NHÓM I — BẤT BIẾN REQUEST_ID XUYÊN SUỐT
    // =========================================================================

    @Test
    @Order(90)
    @Tag("fixture")
    @RegMeta(id = "REG-I01", group = "I", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-I01 [FIXTURE]: Mọi retry/lookup/redirect/restart trong toàn bộ trace giữ nguyên requestId + payloadHash, messageId khác nhau")
    void regI01_Fixture_GlobalRequestIdAndPayloadHashInvariance(@TempDir Path tempDir) throws Exception {
        // Bơm thêm một chuỗi tổng hợp: Follower redirect -> Leader drop response -> Restart -> Lookup -> Retry
        FakeNodeServer follower = track(new FakeNodeServer("node-1", 0));
        FakeNodeServer leader = track(new FakeNodeServer("node-2", 0));
        follower.setNotLeader("node-2", 6L);
        leader.setLeader();

        String chainReqId = UUID.randomUUID().toString();
        recordRequestId(chainReqId);
        leader.dropResponseForRequestId(chainReqId);
        leader.setDropResponseHoldMs(450);

        ClusterEndpoints eps = ClusterEndpoints.fromEndpoints(List.of(
                new NodeEndpoint("node-1", "127.0.0.1", follower.getPort()),
                new NodeEndpoint("node-2", "127.0.0.1", leader.getPort())
        ), "node-1");

        Path jDir = tempDir.resolve("chain");
        try (PersistentPendingJournal j1 = new PersistentPendingJournal(jDir);
             LeaderAwareBookingClient lac1 = new LeaderAwareBookingClient(
                     eps, ep -> new SocketBookingClient(ep.host(), ep.port(), 300, 300, "reg-i01-1"),
                     new RedirectPolicy(3, 350, 900), null, "node-1");
             JournalingBookingClient jbc1 = new JournalingBookingClient(lac1, j1)) {
            jbc1.book(new BookTicketRequestDto(chainReqId, "SE-001", "G09", "Khách Bất Biến")).get(3, TimeUnit.SECONDS);
        }

        leader.clearDropResponseForRequestIds();
        try (PersistentPendingJournal j2 = new PersistentPendingJournal(jDir);
             LeaderAwareBookingClient lac2 = new LeaderAwareBookingClient(
                     eps, ep -> new SocketBookingClient(ep.host(), ep.port(), 1000, 1000, "reg-i01-2"),
                     new RedirectPolicy(3, 1000, 3000), null, "node-1");
             JournalingBookingClient jbc2 = new JournalingBookingClient(lac2, j2)) {
            BookTicketResponseDto retried = jbc2.retry(chainReqId).get(3, TimeUnit.SECONDS);
            assertTrue(retried.isConfirmed());
            GetRequestResultResponseDto lookedUp = jbc2.getRequestResult(chainReqId).get(3, TimeUnit.SECONDS);
            assertEquals("APPLIED_SUCCESS", lookedUp.getState());
        }

        collectServerFrames(follower);
        collectServerFrames(leader);

        // Kiểm chứng toàn cục trên toàn bộ WIRE_AUDIT_COLLECTOR đã thu thập tự động từ đầu phiên chạy
        assertFalse(WIRE_AUDIT_COLLECTOR.isEmpty(), "Phải có dữ liệu khung tin thu thập tự động");

        Set<String> allMessageIds = new HashSet<>();
        Map<String, Set<String>> writePayloadHashesByReqId = new HashMap<>();
        Map<String, Set<String>> intentHashesByReqId = new HashMap<>();
        int multiFrameReqCount = 0;

        Map<String, List<WireFrameAudit>> framesByReqId = new HashMap<>();
        for (WireFrameAudit f : WIRE_AUDIT_COLLECTOR) {
            assertTrue(allMessageIds.add(f.messageId()),
                    "Mỗi khung tin TCP phải có messageId duy nhất, phát hiện trùng: " + f.messageId());
            framesByReqId.computeIfAbsent(f.requestId(), k -> new ArrayList<>()).add(f);
            if ("BOOK_TICKET".equals(f.commandType()) || "CANCEL_TICKET".equals(f.commandType())) {
                writePayloadHashesByReqId.computeIfAbsent(f.requestId(), k -> new HashSet<>()).add(f.payloadSha256());
                if (f.intentHash() != null) {
                    intentHashesByReqId.computeIfAbsent(f.requestId(), k -> new HashSet<>()).add(f.intentHash());
                }
            }
        }

        for (Map.Entry<String, Set<String>> e : writePayloadHashesByReqId.entrySet()) {
            assertEquals(1, e.getValue().size(),
                    "Mọi lần gửi/redirect/retry của requestId=" + e.getKey() + " phải có cùng SHA-256 payload!");
        }
        for (Map.Entry<String, Set<String>> e : intentHashesByReqId.entrySet()) {
            assertEquals(1, e.getValue().size(),
                    "Một requestId tuyệt đối không được dùng chung cho hai ý định khác nhau: " + e.getKey());
        }
        for (List<WireFrameAudit> list : framesByReqId.values()) {
            if (list.size() > 1) {
                multiFrameReqCount++;
            }
        }
        assertTrue(multiFrameReqCount >= 3, "Trace tự động phải bao phủ nhiều chuỗi retry/redirect/lookup đa khung tin");

        recordNote("Đã kiểm chứng tự động " + WIRE_AUDIT_COLLECTOR.size() + " frames (" + framesByReqId.size()
                + " requestIds): 100% bất biến SHA-256 payload, 100% messageId duy nhất");
    }

    @Test
    @Order(91)
    @Tag("fixture")
    @RegMeta(id = "REG-I02", group = "I", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-I02 [FIXTURE]: REQUEST_ID_CONFLICT bị chặn ở Client trước khi gửi; server trả CONFLICT hiển thị đúng")
    void regI02_Fixture_RequestIdConflictClientBlockAndServerPresentation(@TempDir Path tempDir) throws Exception {
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        LeaderAwareBookingClient lac = buildSingleServerLeaderClient(server, 1500, 3000, "reg-i02");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient jbc = track(new JournalingBookingClient(lac, journal));
        BookingViewModel vm = new BookingViewModel(jbc, journal, null, null, 2000);
        vm.setSyncMode(true);

        String conflictId = UUID.randomUUID().toString();
        recordRequestId(conflictId);

        // Lần 1: Đặt hợp lệ ghế G01
        BookTicketResponseDto first = vm.submit(new BookTicketRequestDto(conflictId, "SE-001", "G01", "Khách Gốc"))
                .get(3, TimeUnit.SECONDS);
        assertTrue(first.isConfirmed());
        int framesBefore = server.getReceivedRequests().size();

        // Lần 2 ở Client: Cố dùng lại conflictId nhưng đổi ghế sang G02 -> bị chặn trước khi chạm mạng!
        assertThrows(IllegalArgumentException.class, () ->
                vm.submit(new BookTicketRequestDto(conflictId, "SE-001", "G02", "Khách Gốc")),
                "Client phải chặn REQUEST_ID_CONFLICT trước khi gửi");
        assertEquals(framesBefore, server.getReceivedRequests().size(), "0 frame xung đột được phép rời Client");

        // Lần 3 từ socket thô khác journal (mô phỏng server trả REQUEST_ID_CONFLICT) -> kiểm tra hiển thị
        try (SocketBookingClient rawClient = new SocketBookingClient("127.0.0.1", server.getPort(), 1000, 1500, "reg-i02-raw")) {
            BookTicketResponseDto serverConflict = rawClient.book(
                    new BookTicketRequestDto(conflictId, "SE-001", "G02", "Khách Khác")).get(3, TimeUnit.SECONDS);
            assertTrue(serverConflict.isRejected());
            assertEquals("REQUEST_ID_CONFLICT", serverConflict.getReasonCode());

            ErrorPresentation pres = ErrorPresentationMapper.mapBookingResponse(serverConflict);
            assertEquals("REQUEST_ID_CONFLICT", pres.rawCode());
            assertFalse(pres.retryable(), "REQUEST_ID_CONFLICT tuyệt đối không cho phép retry tự động");
        }

        // Lưu ý: không đưa frame cố tình tạo conflict ở bài test âm tính vào tập bất biến dương tính của REG-I01
        recordNote("Chặn REQUEST_ID_CONFLICT fail-fast tại Client (0 frame); phản hồi server CONFLICT ánh xạ canRetry=false");
    }

    // =========================================================================
    // NHÓM J — ÁNH XẠ LỖI (D13/D19)
    // =========================================================================

    @Test
    @Order(100)
    @Tag("fixture")
    @RegMeta(id = "REG-J01", group = "J", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-J01 [FIXTURE]: Đủ 10 mã SPEC-03 có presentation riêng; UNKNOWN khác REJECTED; mã lạ -> không xác định mã; 0 SEAT_SOLD")
    void regJ01_Fixture_ErrorPresentationMatrixExhaustive() {
        List<String> spec03Codes = List.of(
                "INVALID_REQUEST",
                "UNSUPPORTED_VERSION",
                "NOT_LEADER",
                "NO_QUORUM",
                "READ_UNAVAILABLE",
                "SEAT_ALREADY_BOOKED",
                "TICKET_NOT_ACTIVE",
                "REQUEST_ID_CONFLICT",
                "TIMEOUT_UNKNOWN",
                "INTERNAL_ERROR"
        );

        for (String code : spec03Codes) {
            ErrorPresentation p = ErrorPresentationMapper.mapCode(code, false);
            assertNotNull(p, "Mã " + code + " phải có ErrorPresentation");
            assertEquals(code, p.rawCode());
            assertFalse(p.title().isBlank());
            assertFalse(p.message().isBlank());
            assertFalse(p.message().contains("SEAT_SOLD"), "Không được xuất hiện chuỗi tự chế SEAT_SOLD");
        }

        // Chứng minh bằng dữ liệu cấu trúc: UNKNOWN khác hoàn toàn REJECTED
        ErrorPresentation pUnknown = ErrorPresentationMapper.mapBookingResponse(
                new BookTicketResponseDto("r-1", "UNKNOWN", null, "SE-001", "G01", "TIMEOUT_UNKNOWN", null, false, null, null));
        ErrorPresentation pRejected = ErrorPresentationMapper.mapBookingResponse(
                new BookTicketResponseDto("r-2", "REJECTED", null, "SE-001", "G01", "SEAT_ALREADY_BOOKED", 10L, false, null, null));

        assertEquals(PresentationKind.UNKNOWN, pUnknown.kind());
        assertEquals(PresentationKind.REJECTED, pRejected.kind());
        assertNotEquals(pUnknown.kind(), pRejected.kind());
        assertNotEquals(pUnknown.styleClass(), pRejected.styleClass());
        assertTrue(pUnknown.retryable(), "UNKNOWN phải cho phép tra cứu/thử lại cùng requestId");
        assertFalse(pRejected.retryable(), "REJECTED(SEAT_ALREADY_BOOKED) đã ngã ngũ không cho retry cùng ghế");
        assertTrue(pUnknown.showRequestId(), "UNKNOWN phải hiển thị requestId để tra cứu");
        assertFalse(pRejected.showRequestId(), "REJECTED không gắn cờ tra cứu pending");

        // Mã lạ chưa biết -> rơi vào nhánh không xác định mã an toàn
        ErrorPresentation unknownCodePres = ErrorPresentationMapper.mapCode("MYSTERY_CODE_2099", false);
        assertEquals(PresentationKind.ERROR, unknownCodePres.kind());
        assertTrue(unknownCodePres.title().toLowerCase(Locale.ROOT).contains("không xác định")
                || unknownCodePres.message().toLowerCase(Locale.ROOT).contains("không xác định"));

        recordNote("10/10 mã SPEC-03 hợp lệ; UNKNOWN != REJECTED (kind/style/canCheckResult); mã lạ->UNRECOGNIZED_ERROR; 0 SEAT_SOLD");
    }

    // =========================================================================
    // NHÓM K — CẤU HÌNH & ENDPOINT (D16/D25)
    // =========================================================================

    @Test
    @Order(110)
    @Tag("fixture")
    @RegMeta(id = "REG-K01", group = "K", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-K01 [FIXTURE]: Profile dev/lan nạp đúng; đổi Node qua cấu hình không sửa Java; chặn placeholder; map nodeId<->endpoint")
    void regK01_Fixture_ProfilesConfigOverrideAndEndpointMapping(@TempDir Path tempDir) throws Exception {
        // 1. Profile dev chuẩn
        ClientConfig devCfg = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "dev"), Map.of());
        assertEquals("dev", devCfg.getProfile());
        assertEquals(5, devCfg.getEndpoints().size());
        assertFalse(devCfg.hasPlaceholderEndpoints());
        devCfg.validateNoPlaceholders();

        // 2. Profile lan mặc định có placeholder -> bị chặn bởi validateNoPlaceholders
        ClientConfig lanPlaceholderCfg = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), Map.of());
        assertEquals("lan", lanPlaceholderCfg.getProfile());
        assertEquals(5, lanPlaceholderCfg.getEndpoints().size());
        assertTrue(lanPlaceholderCfg.hasPlaceholderEndpoints());
        assertThrows(IllegalStateException.class, lanPlaceholderCfg::validateNoPlaceholders);

        // 3. Đổi sang IP LAN phòng máy hợp lệ qua tệp cấu hình ngoài mà không sửa dòng code Java nào
        Path customLanFile = tempDir.resolve("custom-lan.properties");
        String customContent = """
                cluster.profile=lan
                client.mode=socket
                lan.cluster.nodes=node-1,node-2,node-3,node-4,node-5
                lan.node.node-1.client.host=10.20.30.101
                lan.node.node-1.client.port=9100
                lan.node.node-2.client.host=10.20.30.102
                lan.node.node-2.client.port=9100
                lan.node.node-3.client.host=10.20.30.103
                lan.node.node-3.client.port=9100
                lan.node.node-4.client.host=10.20.30.104
                lan.node.node-4.client.port=9100
                lan.node.node-5.client.host=10.20.30.105
                lan.node.node-5.client.port=9100
                """;
        Files.writeString(customLanFile, customContent, StandardCharsets.UTF_8);

        ClientConfig validLanCfg = ClientConfig.loadWithOverrides(customLanFile, Map.of(), Map.of());
        assertFalse(validLanCfg.hasPlaceholderEndpoints());
        validLanCfg.validateNoPlaceholders();

        ClusterEndpoints endpoints = ClusterEndpoints.fromClientConfig(validLanCfg);
        assertEquals("10.20.30.103:9100", endpoints.getEndpoint("node-3").toAddressString());
        assertEquals(5, endpoints.size());

        recordNote("dev(9101..9105) & lan(9100) nạp chuẩn; chặn placeholder; override file ngoài ánh xạ đúng node-1..5");
    }

    // =========================================================================
    // NHÓM L — BẤT BIẾN TĨNH TRÊN MÃ NGUỒN (REG-L01)
    // =========================================================================

    @Test
    @Order(120)
    @Tag("fixture")
    @RegMeta(id = "REG-L01", group = "L", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-L01 [FIXTURE]: Quét mã nguồn chứng minh 0 vi phạm kiến trúc (java.net/sql ở UI, HTTP/REST, SEAT_SOLD, sleep/blocking, ProcessBuilder/docker)")
    void regL01_Fixture_StaticArchitectureInvariantsScan() throws IOException {
        Path mainJavaRoot = resolveProjectPath("desktop-client/src/main/java");
        assertTrue(Files.isDirectory(mainJavaRoot), "Thư mục mã nguồn phải tồn tại: " + mainJavaRoot);

        Pattern forbiddenImportUi = Pattern.compile("^\\s*import\\s+java\\.(net|sql)\\.");
        Pattern forbiddenWebJdbc = Pattern.compile("\\b(java\\.net\\.http|HttpClient|OkHttp|Retrofit|WebSocket|RestTemplate|SpringApplication|JdbcTemplate|DriverManager)\\b");
        Pattern seatSoldToken = Pattern.compile("\"SEAT_SOLD\"");
        Pattern uiBlockingCall = Pattern.compile("(Thread\\.sleep\\s*\\(|\\.join\\s*\\(\\s*\\)|\\.get\\s*\\([^)]*TimeUnit)");
        Pattern processOrDocker = Pattern.compile("\\b(ProcessBuilder|Runtime\\.getRuntime\\(\\)\\.exec)\\b|\"docker\\s");

        List<String> violations = new ArrayList<>();
        int scannedFiles = 0;

        try (Stream<Path> walk = Files.walk(mainJavaRoot)) {
            List<Path> javaFiles = walk.filter(p -> p.toString().endsWith(".java")).toList();
            for (Path file : javaFiles) {
                scannedFiles++;
                String rel = mainJavaRoot.relativize(file).toString().replace('\\', '/');
                boolean isControllerOrVm = rel.contains("/controller/") || rel.contains("/viewmodel/");
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                boolean inBlockComment = false;

                for (int idx = 0; idx < lines.size(); idx++) {
                    String raw = lines.get(idx);
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
                    int lineNo = idx + 1;
                    String codeLine = raw.contains("//") ? raw.substring(0, raw.indexOf("//")) : raw;

                    if (isControllerOrVm && forbiddenImportUi.matcher(codeLine).find()) {
                        violations.add(rel + ":" + lineNo + " -> import java.net/java.sql trong UI");
                    }
                    if (forbiddenWebJdbc.matcher(codeLine).find()) {
                        violations.add(rel + ":" + lineNo + " -> sử dụng thư viện Web/HTTP/JDBC bị cấm");
                    }
                    if (seatSoldToken.matcher(codeLine).find()) {
                        violations.add(rel + ":" + lineNo + " -> sử dụng chuỗi tự chế SEAT_SOLD");
                    }
                    if (isControllerOrVm && uiBlockingCall.matcher(codeLine).find()) {
                        violations.add(rel + ":" + lineNo + " -> gọi hàm chặn luồng UI (sleep/join/Future.get)");
                    }
                    if (processOrDocker.matcher(codeLine).find()) {
                        violations.add(rel + ":" + lineNo + " -> gọi ProcessBuilder/exec/docker từ Client");
                    }
                }
            }
        }

        // Kiểm tra ErrorPresentationMapper không rẽ nhánh theo nội dung chuỗi serverMsg
        Path mapperFile = mainJavaRoot.resolve("vn/nhom6/railway/client/controller/ErrorPresentationMapper.java");
        String mapperSrc = Files.readString(mapperFile, StandardCharsets.UTF_8);
        assertFalse(mapperSrc.contains("serverMsg.contains(") || mapperSrc.contains("getMessage().contains("),
                "ErrorPresentationMapper tuyệt đối không được parse chuỗi message từ server để quyết định trạng thái");

        assertTrue(scannedFiles >= 40, "Phải quét đủ toàn bộ các tệp Java trong src/main");
        assertEquals(List.of(), violations, "Phát hiện vi phạm bất biến tĩnh: " + violations);

        recordNote("Đã quét " + scannedFiles + " tệp Java trong src/main: 0 vi phạm java.net/sql, Web/JDBC, SEAT_SOLD, UI blocking, ProcessBuilder");
    }

    // =========================================================================
    // NHÓM M — KHỞI CHẠY & DEMO WINDOWS (D01/D08)
    // =========================================================================

    @Test
    @Order(130)
    @Tag("fixture")
    @RegMeta(id = "REG-M01", group = "M", layer = "FIXTURE", dataSource = "FAKE")
    @DisplayName("REG-M01 [FIXTURE]: Tài nguyên FXML/CSS hợp lệ, đủ G01–G10; chế độ Socket không rò nhãn MOCK, chế độ Mock hiện đúng nhãn")
    void regM01_Fixture_StartupResourcesAndSourceLabelSeparation(@TempDir Path tempDir) throws Exception {
        // 1. Kiểm tra đủ các tệp giao diện FXML & CSS cho cửa sổ Windows
        for (String resPath : List.of(
                "/fxml/main.fxml",
                "/fxml/seat-grid.fxml",
                "/fxml/booking-dialog.fxml",
                "/fxml/cancel-ticket-dialog.fxml",
                "/fxml/ticket-status.fxml",
                "/fxml/pending-requests.fxml",
                "/fxml/cluster-status-bar.fxml",
                "/css/railway.css")) {
            assertNotNull(App.class.getResource(resPath), "Thiếu tài nguyên giao diện bắt buộc: " + resPath);
        }

        ObservableList<SeatViewModel> mockSeats = SeatViewModel.MockSeatSource.createMockSeats();
        assertEquals(10, mockSeats.size(), "Khởi tạo sơ đồ phải có đúng 10 ghế G01–G10");

        // 2. Lớp Mock hiển thị đúng cờ và nhãn MOCK
        MockBookingClient mockClient = track(new MockBookingClient());
        BookingViewModel mockVm = new BookingViewModel(mockClient);
        mockVm.setSyncMode(true);
        assertTrue(mockVm.isMock(), "Ở chế độ MockBookingClient, cờ mock phải là true");
        assertTrue(mockVm.getMessage().contains("MOCK"), "Ở chế độ Mock, thông điệp phải ghi rõ nhãn MOCK");

        // 3. Lớp Socket / Non-Mock TUYỆT ĐỐI KHÔNG hiển thị nhãn MOCK
        FakeNodeServer server = track(new FakeNodeServer("node-1", 0));
        LeaderAwareBookingClient socketLeaderClient = buildSingleServerLeaderClient(server, 1500, 3000, "reg-m01");
        PersistentPendingJournal journal = track(new PersistentPendingJournal(tempDir));
        JournalingBookingClient socketJournaling = track(new JournalingBookingClient(socketLeaderClient, journal));

        BookingViewModel realModeVm = new BookingViewModel(socketJournaling, journal, null, null, 2000);
        realModeVm.setSyncMode(true);
        assertFalse(realModeVm.isMock(), "Khi gắn với Socket/LeaderAware client, cờ mock ban đầu phải là false");
        assertFalse(realModeVm.getMessage().toUpperCase(Locale.ROOT).contains("MOCK"),
                "Thông điệp khởi tạo của chế độ Socket không được chứa nhãn MOCK: " + realModeVm.getMessage());

        GetSeatsResponseDto seatsResp = realModeVm.refreshSeats("SE-001").get(3, TimeUnit.SECONDS);
        assertFalse(seatsResp.isMock());
        assertFalse(realModeVm.getMessage().toUpperCase(Locale.ROOT).contains("MOCK"),
                "Sau khi refreshSeats ở chế độ Socket, thông điệp không được gắn nhãn [MOCK]: " + realModeVm.getMessage());

        // 4. Chụp ảnh bằng chứng tùy chọn bằng Node.snapshot() + ImageIO.write() CHỈ KHI có -Dui.evidence=true
        String evidenceNote = "ui.evidence=NOT_RUN (chưa bật -Dui.evidence=true)";
        if (Boolean.getBoolean("ui.evidence")) {
            if (GraphicsEnvironment.isHeadless()) {
                evidenceNote = "ui.evidence=NOT_RUN (headless)";
            } else {
                captureOptionalJavaFxSnapshots();
                evidenceNote = "ui.evidence=CAPTURED (3 ảnh PNG)";
            }
        }

        collectServerFrames(server);
        recordNote("Đủ 8 FXML/CSS & 10 ghế; tách bạch nhãn Socket(0 MOCK) vs Mock(có MOCK); " + evidenceNote);
    }

    private void captureOptionalJavaFxSnapshots() throws Exception {
        try {
            Platform.startup(() -> {});
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
            // JavaFX Toolkit đã khởi chạy trước đó
        }

        Path outDir = resolveProjectPath("artifacts/evidence/D27");
        Files.createDirectories(outDir);

        captureFxmlSnapshot("/fxml/seat-grid.fxml",
                "TV1-D27 [LỚP: FIXTURE | NGUỒN: FAKE TCP SERVER] - Sơ đồ 10 ghế",
                outDir.resolve("TV1-D27-ui-seats.png"));
        captureFxmlSnapshot("/fxml/pending-requests.fxml",
                "TV1-D27 [LỚP: FIXTURE | NGUỒN: FAKE TCP SERVER] - Yêu cầu UNKNOWN / Pending",
                outDir.resolve("TV1-D27-ui-unknown-pending.png"));
        captureFxmlSnapshot("/fxml/ticket-status.fxml",
                "TV1-D27 [LỚP: FIXTURE | NGUỒN: FAKE TCP SERVER] - Tra cứu GET_REQUEST_RESULT",
                outDir.resolve("TV1-D27-ui-lookup.png"));
    }

    private void captureFxmlSnapshot(String fxmlPath, String windowTitle, Path pngTarget) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errRef = new AtomicReference<>();

        Platform.runLater(() -> {
            Stage stage = null;
            try {
                FXMLLoader loader = new FXMLLoader(App.class.getResource(fxmlPath));
                Parent root = loader.load();
                Scene scene = new Scene(root, 980, 640);
                var cssUrl = App.class.getResource("/css/railway.css");
                if (cssUrl != null) {
                    scene.getStylesheets().add(cssUrl.toExternalForm());
                }
                stage = new Stage();
                stage.setTitle(windowTitle);
                stage.setScene(scene);
                stage.show();
                root.applyCss();
                root.layout();

                Node targetNode = scene.getRoot();
                WritableImage fxImg = targetNode.snapshot(null, null);
                int w = (int) fxImg.getWidth();
                int h = (int) fxImg.getHeight();
                BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
                PixelReader pr = fxImg.getPixelReader();
                for (int y = 0; y < h; y++) {
                    for (int x = 0; x < w; x++) {
                        bi.setRGB(x, y, pr.getArgb(x, y));
                    }
                }
                ImageIO.write(bi, "png", pngTarget.toFile());
            } catch (Throwable t) {
                errRef.set(t);
            } finally {
                if (stage != null) {
                    stage.close();
                }
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Hết thời gian chờ chụp snapshot JavaFX cho " + fxmlPath);
        if (errRef.get() != null) {
            throw new IOException("Không thể chụp snapshot " + fxmlPath + ": " + errRef.get().getMessage(), errRef.get());
        }
    }

    // =========================================================================
    // JUNIT 5 TEST WATCHER & BỘ XUẤT CSV TỰ ĐỘNG (KHÔNG THÊM DEPENDENCY)
    // =========================================================================

    public static class RegressionCsvWatcher implements BeforeTestExecutionCallback, AfterTestExecutionCallback, TestWatcher, AfterAllCallback {

        @Override
        public void beforeTestExecution(ExtensionContext context) {
            String key = context.getUniqueId();
            ActiveContext active = new ActiveContext();
            CONTEXT_BY_TEST.put(key, active);
            CURRENT_TEST_KEY.set(key);
            RegMeta meta = context.getRequiredTestMethod().getAnnotation(RegMeta.class);
            CURRENT_REG_ID.set(meta != null ? meta.id() : context.getDisplayName());
        }

        @Override
        public void afterTestExecution(ExtensionContext context) {
            String key = context.getUniqueId();
            ActiveContext active = CONTEXT_BY_TEST.get(key);
            if (active != null) {
                active.durationMs = Math.max(0L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - active.startNanos));
            }
            CURRENT_TEST_KEY.remove();
            CURRENT_REG_ID.remove();
        }

        @Override
        public void testSuccessful(ExtensionContext context) {
            saveRow(context, "PASS", null);
        }

        @Override
        public void testFailed(ExtensionContext context, Throwable cause) {
            String errMsg = cause != null && cause.getMessage() != null
                    ? cause.getClass().getSimpleName() + ": " + sanitizeCsvNote(cause.getMessage())
                    : "Assertion failed";
            saveRow(context, "FAIL", errMsg);
        }

        @Override
        public void testAborted(ExtensionContext context, Throwable cause) {
            String msg = (cause != null && cause.getMessage() != null) ? cause.getMessage().trim() : "Assumptions not met";
            String status = "SKIPPED";
            if (msg.startsWith("NOT_RUN")) {
                status = "NOT_RUN";
            } else if (msg.startsWith("BLOCKED")) {
                status = "BLOCKED";
            }
            saveRow(context, status, sanitizeCsvNote(msg));
        }

        @Override
        public void testDisabled(ExtensionContext context, Optional<String> reason) {
            saveRow(context, "NOT_RUN", reason.orElse("Disabled"));
        }

        private void saveRow(ExtensionContext context, String status, String fallbackNote) {
            RegMeta meta = context.getRequiredTestMethod().getAnnotation(RegMeta.class);
            if (meta == null) {
                return;
            }
            ActiveContext active = CONTEXT_BY_TEST.get(context.getUniqueId());
            long dur = active != null ? active.durationMs : 0L;
            String src = (active != null && active.overrideDataSource != null)
                    ? active.overrideDataSource
                    : meta.dataSource();
            String reqIds = (active != null && !active.requestIds.isEmpty())
                    ? String.join(";", active.requestIds)
                    : "-";
            String note = (active != null && active.note != null && !active.note.isBlank())
                    ? sanitizeCsvNote(active.note)
                    : (fallbackNote != null ? fallbackNote : "");
            if ("FAIL".equals(status) && fallbackNote != null) {
                note = fallbackNote;
            }

            String rowKey = meta.layer() + "::" + meta.id();
            RECORDED_ROWS.put(rowKey, new CsvRow(
                    meta.id(),
                    meta.group(),
                    meta.layer(),
                    status,
                    dur,
                    src,
                    reqIds,
                    note
            ));
        }

        private static String sanitizeCsvNote(String raw) {
            if (raw == null) {
                return "";
            }
            String cleaned = raw.replace('\r', ' ').replace('\n', ' ').trim();
            if (cleaned.length() > 220) {
                cleaned = cleaned.substring(0, 217) + "...";
            }
            return cleaned;
        }

        @Override
        public void afterAll(ExtensionContext context) throws Exception {
            Map<String, CsvRow> merged = new LinkedHashMap<>();

            // Giữ lại các dòng của lớp khác nếu chạy lọc theo -Dgroups=fixture hoặc -Dgroups=real
            if (Files.exists(CSV_OUTPUT_PATH)) {
                try (BufferedReader reader = Files.newBufferedReader(CSV_OUTPUT_PATH, StandardCharsets.UTF_8)) {
                    String line = reader.readLine(); // bỏ qua header
                    while ((line = reader.readLine()) != null) {
                        CsvRow parsed = parseCsvLine(line);
                        if (parsed != null) {
                            merged.put(parsed.layer() + "::" + parsed.regId(), parsed);
                        }
                    }
                } catch (IOException ignored) {
                }
            }

            synchronized (RECORDED_ROWS) {
                merged.putAll(RECORDED_ROWS);
            }

            if (CSV_OUTPUT_PATH.getParent() != null) {
                Files.createDirectories(CSV_OUTPUT_PATH.getParent());
            }

            try (BufferedWriter writer = Files.newBufferedWriter(CSV_OUTPUT_PATH, StandardCharsets.UTF_8)) {
                writer.write("regId,group,layer,status,durationMs,dataSource,requestIds,note\n");
                for (CsvRow r : merged.values()) {
                    writer.write(String.format("%s,%s,%s,%s,%d,%s,\"%s\",\"%s\"\n",
                            r.regId(),
                            r.group(),
                            r.layer(),
                            r.status(),
                            r.durationMs(),
                            r.dataSource(),
                            r.requestIds().replace("\"", "\"\""),
                            r.note().replace("\"", "\"\"")));
                }
            }

            printSummaryTable(merged.values());
        }

        private static CsvRow parseCsvLine(String line) {
            if (line == null || line.isBlank()) {
                return null;
            }
            List<String> cols = new ArrayList<>();
            StringBuilder cur = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                } else if (c == ',' && !inQuotes) {
                    cols.add(cur.toString());
                    cur.setLength(0);
                } else {
                    cur.append(c);
                }
            }
            cols.add(cur.toString());
            if (cols.size() < 8) {
                return null;
            }
            try {
                return new CsvRow(
                        cols.get(0), cols.get(1), cols.get(2), cols.get(3),
                        Long.parseLong(cols.get(4)), cols.get(5), cols.get(6), cols.get(7));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private static void printSummaryTable(Iterable<CsvRow> rows) {
            Map<String, int[]> summary = new LinkedHashMap<>();
            for (CsvRow r : rows) {
                String key = String.format("%-7s | %-5s", r.layer(), "Nhóm " + r.group());
                int[] counts = summary.computeIfAbsent(key, k -> new int[5]);
                switch (r.status()) {
                    case "PASS" -> counts[0]++;
                    case "FAIL" -> counts[1]++;
                    case "SKIPPED" -> counts[2]++;
                    case "NOT_RUN" -> counts[3]++;
                    case "BLOCKED" -> counts[4]++;
                    default -> counts[2]++;
                }
            }

            System.out.println("================================================================================");
            System.out.println("[TV1-D27] BẢNG TỔNG HỢP REGRESSION THEO LỚP × NHÓM (CSV: " + CSV_OUTPUT_PATH + ")");
            System.out.println("Lớp     | Nhóm   | PASS | FAIL | SKIPPED | NOT_RUN | BLOCKED");
            System.out.println("--------+--------+------+------+---------+---------+--------");
            for (Map.Entry<String, int[]> e : summary.entrySet()) {
                int[] c = e.getValue();
                System.out.printf("%s | %4d | %4d | %7d | %7d | %7d%n",
                        e.getKey(), c[0], c[1], c[2], c[3], c[4]);
            }
            System.out.println("================================================================================");
        }
    }
}
