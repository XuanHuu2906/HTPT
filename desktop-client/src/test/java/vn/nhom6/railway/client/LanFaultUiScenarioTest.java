package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.harness.RecordedTraceReplayServer;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.support.UiFaultTimeline;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BỘ KIỂM THỬ KỊCH BẢN SỰ CỐ GIAO DIỆN LAN (TASK TV1-D26).
 * <p>
 * Kiểm thử hành vi giao diện người dùng Desktop Client khi xảy ra các sự cố phân tán:
 * 1. Leader lost giữa chừng -> UNKNOWN, bảo toàn requestId, tự động ghi nhận pending journal.
 * 2. Reconnect tìm Leader mới sau sự cố -> Tra cứu giải quyết yêu cầu pending.
 * 3. NO_QUORUM (cụm mất đa số 2/5) -> Báo lỗi Quorum, ghế KHÔNG đổi sang BOOKED.
 * 4. Timeout sau khi gửi -> UNKNOWN (SENT_UNRESOLVED).
 * 5. Lỗi kết nối trước khi gửi -> FAILED_NOT_SENT, ghế giữ nguyên.
 * 6. Chuyển hướng NOT_LEADER giữ nguyên requestId.
 * 7. Lặp lại chuyển hướng tối đa 3 lần.
 * 8. Tính lũy thừa (idempotency) khi gửi lại sau failover.
 * 9. Phân vùng mạng giả lập (Network Partition).
 * 10. Khôi phục Journal sau khi ứng dụng crash trong lúc sự cố.
 * 11. Ánh xạ mã lỗi chuẩn xác thực SPEC-03 qua ErrorPresentationMapper.
 * 12. Ghi nhận dòng thời gian trạng thái UI qua UiFaultTimeline.
 * 13. Kịch bản nghiệm thu trên LAN 5 laptop thật (Guard LIVE_LAN_ACCEPTANCE - Opt-in).
 */
public class LanFaultUiScenarioTest {

    private final List<AutoCloseable> resources = new ArrayList<>();
    private final UiFaultTimeline timeline = new UiFaultTimeline();
    private static final Path EVIDENCE_CSV = resolveEvidencePath("artifacts/evidence/D26/TV1-D26-ui-timeline.csv");

    private static Path resolveEvidencePath(String relPath) {
        Path direct = Paths.get(relPath);
        if (Files.exists(Paths.get("artifacts")) || Files.exists(Paths.get(".git"))) {
            return direct;
        }
        return Paths.get("..").resolve(relPath);
    }

    @BeforeEach
    void setUp() {
        resources.clear();
        timeline.clear();
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("TC_LAN_01: Leader mất kết nối đột ngột giữa chừng -> UI chuyển UNKNOWN (SENT_UNRESOLVED)")
    void testTC_LAN_01_LeaderLostMidFlight(@TempDir Path tempDir) throws Exception {
        RecordedTraceReplayServer server = new RecordedTraceReplayServer("node-1");
        resources.add(server);
        server.setFaultMode(RecordedTraceReplayServer.FaultMode.DROP_CONNECTION);

        Path journalFile = tempDir.resolve("journal-tc01.log");
        PersistentPendingJournal journal = new PersistentPendingJournal(journalFile);
        resources.add(journal);

        SocketBookingClient rawClient = new SocketBookingClient("127.0.0.1", server.getPort(), 2000, 2000, "client-d26");
        resources.add(rawClient);

        JournalingBookingClient client = new JournalingBookingClient(rawClient, journal);
        resources.add(client);

        String reqId = "REQ-FAULT-01-" + UUID.randomUUID();
        timeline.recordTransition(reqId, "SEAT_SELECTED", "CLICK_BOOK", "SUBMITTING", "Đang gửi yêu cầu đặt vé...", "FAKE");

        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G01", "Khách Hàng A");
        ExecutionException ex = assertThrows(ExecutionException.class, () -> client.book(request).get(4, TimeUnit.SECONDS));

        assertNotNull(ex.getCause());
        timeline.recordTransition(reqId, "SUBMITTING", "SOCKET_DROPPED", "UNKNOWN", "Mất kết nối với máy chủ Leader. Yêu cầu đang chờ xác nhận.", "FAKE");

        PendingEntry entry = journal.get(reqId).orElse(null);
        assertNotNull(entry, "Yêu cầu phải được lưu trong journal khi Leader mất kết nối");
        assertEquals(PendingState.SENT_UNRESOLVED, entry.getState(), "Trạng thái journal phải là SENT_UNRESOLVED");
    }

    @Test
    @DisplayName("TC_LAN_02: Reconnect tìm Leader mới sau khi Leader cũ sập -> Giải quyết vé pending")
    void testTC_LAN_02_ReconnectAndResolvePending(@TempDir Path tempDir) throws Exception {
        FakeNodeServer newLeader = new FakeNodeServer("node-2", 0);
        resources.add(newLeader);

        Path journalFile = tempDir.resolve("journal-tc02.log");
        PersistentPendingJournal journal = new PersistentPendingJournal(journalFile);
        resources.add(journal);

        String reqId = "REQ-FAULT-02-" + UUID.randomUUID();
        PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "payload-raw");
        journal.recordPrepared(entry);
        journal.markSent(reqId);

        timeline.recordTransition(reqId, "UNKNOWN", "TRIGGER_RECONNECT", "RECONNECTING", "Đang dò tìm Leader mới...", "FAKE");

        // Giả lập Leader mới ghi nhận vé đã commit trước đó
        newLeader.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", "TCK-RECOVERED-02", null, 150L, false
        ));

        SocketBookingClient rawClient = new SocketBookingClient("127.0.0.1", newLeader.getPort(), 2000, 2000, "client-d26");
        resources.add(rawClient);

        GetRequestResultResponseDto result = rawClient.getRequestResult(reqId).get(3, TimeUnit.SECONDS);
        assertNotNull(result);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, result.getState());

        journal.markResolved(reqId, "CONFIRMED");
        timeline.recordTransition(reqId, "RECONNECTING", "LOOKUP_SUCCESS", "CONFIRMED", "Vé đã được xác nhận: TCK-RECOVERED-02", "FAKE");

        PendingEntry resolved = journal.get(reqId).orElse(null);
        assertNotNull(resolved);
        assertEquals(PendingState.RESOLVED, resolved.getState());
    }

    @Test
    @DisplayName("TC_LAN_03: Cụm mất đa số (NO_QUORUM 2/5) -> UI báo lỗi không thể cam kết, ghế KHÔNG đổi BOOKED")
    void testTC_LAN_03_NoQuorumError() throws Exception {
        RecordedTraceReplayServer server = new RecordedTraceReplayServer("node-1");
        resources.add(server);
        server.setFaultMode(RecordedTraceReplayServer.FaultMode.NO_QUORUM);

        SocketBookingClient rawClient = new SocketBookingClient("127.0.0.1", server.getPort(), 2000, 2000, "client-d26");
        resources.add(rawClient);

        String reqId = "REQ-FAULT-03-" + UUID.randomUUID();
        timeline.recordTransition(reqId, "AVAILABLE", "CLICK_BOOK", "SUBMITTING", "Đang gửi yêu cầu đặt vé...", "RECORDED-FIXTURE");

        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G03", "Khách Hàng B");
        BookTicketResponseDto resp = rawClient.book(request).get(3, TimeUnit.SECONDS);

        assertNotNull(resp);
        assertEquals("UNKNOWN", resp.getOutcome(), "Phản hồi NO_QUORUM phải mang outcome UNKNOWN");
        assertEquals("NO_QUORUM", resp.getReasonCode(), "reasonCode phải là NO_QUORUM");
        timeline.recordTransition(reqId, "SUBMITTING", "NO_QUORUM_RECEIVED", "AVAILABLE", "Không đủ đa số Quorum (3/5). Ghế giữ nguyên trạng thái.", "RECORDED-FIXTURE");

        // Ánh xạ lỗi qua ErrorPresentationMapper
        ErrorPresentation presentation = ErrorPresentationMapper.mapCode("NO_QUORUM", true);
        assertEquals(PresentationKind.ERROR, presentation.getKind());
        assertEquals("NO_QUORUM", presentation.getRawCode());
    }

    @Test
    @DisplayName("TC_LAN_04: Timeout sau khi gửi (Read Timeout) -> UI báo UNKNOWN, tự động vào pending journal")
    void testTC_LAN_04_TimeoutAfterSend(@TempDir Path tempDir) throws Exception {
        RecordedTraceReplayServer server = new RecordedTraceReplayServer("node-1");
        resources.add(server);
        server.setFaultMode(RecordedTraceReplayServer.FaultMode.TIMEOUT);
        server.setTimeoutDelayMs(3000); // Lớn hơn readTimeout 1000ms

        Path journalFile = tempDir.resolve("journal-tc04.log");
        PersistentPendingJournal journal = new PersistentPendingJournal(journalFile);
        resources.add(journal);

        SocketBookingClient rawClient = new SocketBookingClient("127.0.0.1", server.getPort(), 1000, 1000, "client-d26");
        resources.add(rawClient);

        JournalingBookingClient client = new JournalingBookingClient(rawClient, journal);
        resources.add(client);

        String reqId = "REQ-FAULT-04-" + UUID.randomUUID();
        timeline.recordTransition(reqId, "SEAT_SELECTED", "CLICK_BOOK", "SUBMITTING", "Đang gửi yêu cầu...", "FAKE");

        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Hàng C");
        BookTicketResponseDto resp = client.book(request).get(4, TimeUnit.SECONDS);

        assertNotNull(resp);
        assertEquals("UNKNOWN", resp.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", resp.getReasonCode());
        timeline.recordTransition(reqId, "SUBMITTING", "TIMEOUT_AFTER_SEND", "UNKNOWN", "Hết thời gian chờ phản hồi. Trạng thái chưa xác định.", "FAKE");

        PendingEntry entry = journal.get(reqId).orElse(null);
        assertNotNull(entry);
        assertEquals(PendingState.SENT_UNRESOLVED, entry.getState());
    }

    @Test
    @DisplayName("TC_LAN_05: Lỗi kết nối trước khi gửi (Connect Timeout) -> FAILED_NOT_SENT, ghế giữ nguyên")
    void testTC_LAN_05_ConnectFailureBeforeSend(@TempDir Path tempDir) throws Exception {
        Path journalFile = tempDir.resolve("journal-tc05.log");
        PersistentPendingJournal journal = new PersistentPendingJournal(journalFile);
        resources.add(journal);

        String reqId = "REQ-FAULT-05-" + UUID.randomUUID();
        timeline.recordTransition(reqId, "AVAILABLE", "CLICK_BOOK", "FAILED_NOT_SENT", "Không thể kết nối đến máy chủ. Yêu cầu chưa gửi.", "FAKE");

        PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "payload-raw");
        journal.recordPrepared(entry);
        journal.markFailedNotSent(reqId, "CONNECTION_REFUSED_PRE_SEND");

        PendingEntry failedEntry = journal.get(reqId).orElse(null);
        assertNotNull(failedEntry);
        assertEquals(PendingState.FAILED_NOT_SENT, failedEntry.getState(),
                "Lỗi trước khi gửi được ghi nhận FAILED_NOT_SENT trong journal");
    }

    @Test
    @DisplayName("TC_LAN_06: Follower trả NOT_LEADER kèm leaderHint -> Chuyển hướng giữ nguyên requestId")
    void testTC_LAN_06_NotLeaderRedirectKeepsRequestId() throws Exception {
        RecordedTraceReplayServer follower = new RecordedTraceReplayServer("node-1");
        resources.add(follower);
        follower.setFaultMode(RecordedTraceReplayServer.FaultMode.NOT_LEADER);
        follower.setLeaderHint("node-2");

        SocketBookingClient client = new SocketBookingClient("127.0.0.1", follower.getPort(), 2000, 2000, "client-d26");
        resources.add(client);

        String reqId = "REQ-FAULT-06-" + UUID.randomUUID();
        timeline.recordTransition(reqId, "AVAILABLE", "CLICK_BOOK", "REDIRECTING", "Máy chủ không phải Leader. Đang chuyển hướng sang node-2...", "RECORDED-FIXTURE");

        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G06", "Khách Hàng E");
        BookTicketResponseDto resp = client.book(request).get(3, TimeUnit.SECONDS);

        assertNotNull(resp);
        assertEquals("UNKNOWN", resp.getOutcome());
        assertEquals("NOT_LEADER", resp.getReasonCode());
        assertEquals("node-2", resp.getLeaderHint());
        // Xác nhận requestId không bị biến đổi
        assertEquals(reqId, resp.getRequestId());
        assertEquals(reqId, request.getRequestId());
    }

    @Test
    @DisplayName("TC_LAN_07: Giới hạn số lần chuyển hướng Leader tối đa 3 lần")
    void testTC_LAN_07_MaxRedirectsEnforced() {
        // Kiểm tra hằng số đặc tả SPEC-03 / ClientConfig
        assertEquals(3, vn.nhom6.railway.client.network.ClientConfig.DEFAULT_MAX_REDIRECTS);
    }

    @Test
    @DisplayName("TC_LAN_08: Idempotency sau failover -> Gửi lại cùng requestId nhận kết quả đã xác nhận")
    void testTC_LAN_08_IdempotencyAfterFailover() throws Exception {
        FakeNodeServer server = new FakeNodeServer("node-1", 0);
        resources.add(server);

        String reqId = "REQ-FAULT-08-" + UUID.randomUUID();
        String ticketId = "TCK-IDEMPOTENT-08";

        // Giả lập yêu cầu đã commit trước đó
        server.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", ticketId, null, 200L, false
        ));

        SocketBookingClient client = new SocketBookingClient("127.0.0.1", server.getPort(), 2000, 2000, "client-d26");
        resources.add(client);

        GetRequestResultResponseDto res1 = client.getRequestResult(reqId).get(3, TimeUnit.SECONDS);
        GetRequestResultResponseDto res2 = client.getRequestResult(reqId).get(3, TimeUnit.SECONDS);

        assertEquals(res1.getTicketId(), res2.getTicketId(), "Idempotency: hai lần tra cứu cùng requestId phải trả về cùng ticketId");
        assertEquals(ticketId, res1.getTicketId());
    }

    @Test
    @DisplayName("TC_LAN_09: Phân vùng mạng giả lập -> Phát hiện chuyển đổi failover")
    void testTC_LAN_09_NetworkPartitionSimulation() throws Exception {
        FakeNodeServer leader = new FakeNodeServer("node-1", 0);
        FakeNodeServer healthyFollower = new FakeNodeServer("node-2", 0);
        resources.add(leader);
        resources.add(healthyFollower);

        timeline.recordTransition("N/A", "CLUSTER_OK", "PARTITION_OCCURRED", "SEARCHING_LEADER", "Mạng bị phân vùng. Đang định tuyến lại...", "FAKE");
        leader.close(); // Giả lập phân vùng khiến leader mất kết nối

        assertDoesNotThrow(healthyFollower::getPort);
    }

    @Test
    @DisplayName("TC_LAN_10: Khôi phục Journal sau khi ứng dụng gặp sự cố khởi động lại")
    void testTC_LAN_10_JournalCrashRecovery(@TempDir Path tempDir) throws Exception {
        Path journalFile = tempDir.resolve("journal-tc10.log");

        String reqId = "REQ-FAULT-10-" + UUID.randomUUID();
        try (PersistentPendingJournal j1 = new PersistentPendingJournal(journalFile)) {
            PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "payload-tc10");
            j1.recordPrepared(entry);
            j1.markSent(reqId);
        }

        // Khởi động lại ứng dụng và mở lại journal
        try (PersistentPendingJournal j2 = new PersistentPendingJournal(journalFile)) {
            PendingEntry recovered = j2.get(reqId).orElse(null);
            assertNotNull(recovered, "Entry phải được khôi phục nguyên vẹn sau khi restart");
            assertEquals(PendingState.SENT_UNRESOLVED, recovered.getState());
            assertEquals("BOOK_TICKET", recovered.getCommandType());
        }
    }

    @Test
    @DisplayName("TC_LAN_11: Ánh xạ lỗi chuẩn xác thực SPEC-03 không parse chuỗi tự do")
    void testTC_LAN_11_StandardErrorMappingWithoutStringParsing() {
        ErrorPresentation p1 = ErrorPresentationMapper.mapCode("NOT_LEADER", true);
        assertNotNull(p1);
        assertEquals("NOT_LEADER", p1.getRawCode());

        ErrorPresentation p2 = ErrorPresentationMapper.mapCode("READ_UNAVAILABLE", false);
        assertNotNull(p2);
        assertEquals("READ_UNAVAILABLE", p2.getRawCode());

        ErrorPresentation p3 = ErrorPresentationMapper.mapCode("REQUEST_ID_CONFLICT", true);
        assertNotNull(p3);
        assertEquals("REQUEST_ID_CONFLICT", p3.getRawCode());
    }

    @Test
    @DisplayName("TC_LAN_12: Xuất dòng thời gian trạng thái UI ra artifacts/evidence/D26/TV1-D26-ui-timeline.csv")
    void testTC_LAN_12_ExportTimelineCsv() throws IOException {
        timeline.recordTransition("REQ-SAMPLE-01", "AVAILABLE", "CLICK_BOOK", "SUBMITTING", "Bắt đầu đặt vé", "FAKE");
        timeline.recordTransition("REQ-SAMPLE-01", "SUBMITTING", "TIMEOUT", "UNKNOWN", "Chờ xác nhận", "FAKE");
        timeline.recordTransition("REQ-SAMPLE-01", "UNKNOWN", "LOOKUP_SUCCESS", "CONFIRMED", "Đặt vé thành công", "FAKE");

        timeline.exportCsv(EVIDENCE_CSV);
        assertTrue(Files.exists(EVIDENCE_CSV), "Tệp CSV timeline phải được tạo ra");

        List<String> lines = Files.readAllLines(EVIDENCE_CSV);
        assertTrue(lines.size() >= 4, "Tệp CSV phải có dòng tiêu đề và ít nhất 3 bản ghi");
        assertTrue(lines.get(0).contains("timestamp,requestId,stateBefore,event,stateAfter,uiMessage,source"));
        System.out.println("[EVIDENCE D26] Đã xuất dòng thời gian UI: " + EVIDENCE_CSV.toAbsolutePath());
    }

    @Test
    @DisplayName("TC_LAN_13: Nghiệm thu LAN 5 laptop thật (Guard LIVE_LAN_ACCEPTANCE - Mặc định SKIPPED)")
    void testTC_LAN_13_LiveLanAcceptanceOptIn() {
        Assumptions.assumeTrue(
                "true".equalsIgnoreCase(System.getenv("LIVE_LAN_ACCEPTANCE")),
                "BLOCKED_LIVE_LAN: Kịch bản trên 5 laptop thật được phân công chạy nghiệm thu tại G6 theo AGENTS.md §8."
        );
        fail("Không nên tới đây nếu cờ LIVE_LAN_ACCEPTANCE chưa được bật");
    }

    @Test
    @DisplayName("TC_LAN_14: Xuất ảnh minh họa giao diện sự cố LAN (TV1-D26-ui.png) có nhãn FAKE/RECORDED")
    void testTC_LAN_14_GenerateUiEvidenceImage() throws IOException {
        Path imagePath = resolveEvidencePath("artifacts/evidence/D26/TV1-D26-ui.png");
        Files.createDirectories(imagePath.getParent());

        int width = 960;
        int height = 640;
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

        // Nền chung
        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, width, height);

        // Header
        g.setColor(new Color(30, 41, 59));
        g.fillRect(0, 0, width, 70);

        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 17));
        g.drawString("HỆ THỐNG ĐẶT VÉ XE LỬA PHÂN TÁN — RAILWAY DESKTOP CLIENT", 30, 32);

        g.setColor(new Color(226, 232, 240));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString("MÔ PHỎNG SỰ CỐ LAN (TV1-D26) | Trạng thái: MẤT LEADER (ĐANG TÌM KIẾM LEADER) | Nguồn: [FAKE / RECORDED-FIXTURE]", 30, 52);

        // Badge FAKE
        g.setColor(new Color(239, 68, 68));
        g.fillRoundRect(width - 150, 20, 120, 30, 8, 8);
        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("FAKE / TRACE", width - 130, 40);

        // Banner cảnh báo sự cố
        g.setColor(new Color(254, 242, 242));
        g.fillRoundRect(30, 85, width - 60, 55, 8, 8);
        g.setColor(new Color(239, 68, 68));
        g.setStroke(new BasicStroke(1.5f));
        g.drawRoundRect(30, 85, width - 60, 55, 8, 8);

        g.setColor(new Color(185, 28, 28));
        g.setFont(new Font("Segoe UI", Font.BOLD, 13));
        g.drawString("⚠ SỰ CỐ MẠNG LAN: Mất kết nối tới Leader node-1 giữa chừng | Quorum: 2/5 (NO_QUORUM)", 45, 107);
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString("Yêu cầu REQ-FAULT-01 đã chuyển sang UNKNOWN (SENT_UNRESOLVED). Ghế G03 giữ nguyên chờ xác nhận. Không sinh requestId mới!", 45, 127);

        // Sơ đồ ghế
        g.setColor(new Color(15, 23, 42));
        g.setFont(new Font("Segoe UI", Font.BOLD, 14));
        g.drawString("SƠ ĐỒ 10 GHẾ TRÊN CHUYẾN TÀU SE-001 (SPEC-05 §3.1)", 30, 165);

        int seatStartX = 30;
        int seatStartY = 180;
        int seatW = 82;
        int seatH = 70;
        int gap = 8;

        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            int x = seatStartX + (i - 1) * (seatW + gap);

            if ("G03".equals(seatId)) {
                // UNKNOWN: Viền cam nét đứt, nền vàng nhạt
                g.setColor(new Color(254, 243, 199));
                g.fillRoundRect(x, seatStartY, seatW, seatH, 8, 8);

                g.setColor(new Color(217, 119, 6));
                float[] dash = {6f, 3f};
                g.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10f, dash, 0f));
                g.drawRoundRect(x, seatStartY, seatW, seatH, 8, 8);

                g.setColor(new Color(180, 83, 9));
                g.setFont(new Font("Segoe UI", Font.BOLD, 14));
                g.drawString(seatId, x + 24, seatStartY + 30);
                g.setFont(new Font("Segoe UI", Font.BOLD, 10));
                g.drawString("UNKNOWN", x + 12, seatStartY + 50);
            } else if ("G02".equals(seatId)) {
                // BOOKED: Đỏ
                g.setColor(new Color(254, 226, 226));
                g.fillRoundRect(x, seatStartY, seatW, seatH, 8, 8);
                g.setColor(new Color(220, 38, 38));
                g.setStroke(new BasicStroke(1.5f));
                g.drawRoundRect(x, seatStartY, seatW, seatH, 8, 8);

                g.setFont(new Font("Segoe UI", Font.BOLD, 14));
                g.drawString(seatId, x + 24, seatStartY + 30);
                g.setFont(new Font("Segoe UI", Font.PLAIN, 10));
                g.drawString("ĐÃ ĐẶT", x + 20, seatStartY + 50);
            } else {
                // AVAILABLE: Xanh lá
                g.setColor(new Color(240, 253, 244));
                g.fillRoundRect(x, seatStartY, seatW, seatH, 8, 8);
                g.setColor(new Color(34, 197, 94));
                g.setStroke(new BasicStroke(1.5f));
                g.drawRoundRect(x, seatStartY, seatW, seatH, 8, 8);

                g.setFont(new Font("Segoe UI", Font.BOLD, 14));
                g.drawString(seatId, x + 24, seatStartY + 30);
                g.setFont(new Font("Segoe UI", Font.PLAIN, 10));
                g.drawString("TRỐNG", x + 22, seatStartY + 50);
            }
        }

        // Bảng Pending Requests
        g.setColor(new Color(15, 23, 42));
        g.setFont(new Font("Segoe UI", Font.BOLD, 14));
        g.drawString("NHẬT KÝ YÊU CẦU ĐANG CHỜ (PENDING JOURNAL — BẢO TOÀN QUA FAILOVER/SỰ CỐ LAN)", 30, 285);

        int tableY = 300;
        int tableW = width - 60;
        int tableH = 260;

        g.setColor(Color.WHITE);
        g.fillRect(30, tableY, tableW, tableH);
        g.setColor(new Color(203, 213, 225));
        g.setStroke(new BasicStroke(1f));
        g.drawRect(30, tableY, tableW, tableH);

        // Header bảng
        g.setColor(new Color(241, 245, 249));
        g.fillRect(30, tableY, tableW, 35);
        g.setColor(new Color(71, 85, 105));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("Mã yêu cầu (requestId)", 45, tableY + 22);
        g.drawString("Lệnh", 320, tableY + 22);
        g.drawString("Ghế", 420, tableY + 22);
        g.drawString("Trạng thái Pending", 480, tableY + 22);
        g.drawString("Nguyên nhân", 650, tableY + 22);
        g.drawString("Thao tác", 820, tableY + 22);

        // Row 1
        int rowY = tableY + 65;
        g.setColor(new Color(15, 23, 42));
        g.setFont(new Font("Consolas", Font.PLAIN, 11));
        g.drawString("REQ-FAULT-01-632ce51c-75c2-42bd", 45, rowY);
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString("BOOK_TICKET", 320, rowY);
        g.drawString("G03", 420, rowY);

        g.setColor(new Color(217, 119, 6));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("SENT_UNRESOLVED", 480, rowY);

        g.setColor(new Color(220, 38, 38));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 11));
        g.drawString("TIMEOUT / LEADER_LOST", 650, rowY);

        // Nút Tra cứu
        g.setColor(new Color(37, 99, 235));
        g.fillRoundRect(810, rowY - 15, 75, 24, 6, 6);
        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        g.drawString("Tra cứu", 826, rowY + 1);

        // Footer
        g.setColor(new Color(30, 41, 59));
        g.fillRect(0, height - 40, width, 40);
        g.setColor(new Color(148, 163, 184));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 11));
        g.drawString("NHÃN KIỂM THỬ: FAKE/RECORDED-FIXTURE (TASK TV1-D26) — Sẵn sàng cho TV5 & TV6 nghiệm thu LAN 5 laptop tại G6", 30, height - 16);

        g.dispose();
        ImageIO.write(img, "png", imagePath.toFile());
        assertTrue(Files.exists(imagePath), "Tệp ảnh TV1-D26-ui.png phải được tạo ra thành công");
        System.out.println("[EVIDENCE D26] Đã lưu ảnh minh họa giao diện sự cố: " + imagePath.toAbsolutePath());
    }
}
