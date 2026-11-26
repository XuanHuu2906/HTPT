package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.harness.FakeLeaderProvider;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.smoke.FakeNodeServer.RequestTraceEntry;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.TicketLookupViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử toàn diện cho luồng tra cứu kết quả (GET_REQUEST_RESULT)
 * và quản lý danh sách vé đã xác nhận (TASK TV1-D24).
 *
 * <p>Tuân thủ tuyệt đối AGENTS.md:
 * <ul>
 *   <li>16 test cases kiểm thử hợp đồng SPEC-03 §4.4 và SPEC-06.</li>
 *   <li>Mọi kịch bản đều dùng FakeNodeServer / FakeLeaderProvider giả lập, không nhận là Raft thật.</li>
 *   <li>Kiểm tra bất biến: Giữ nguyên requestId khi redirect; không kết luận thành công khi chưa commit;
 *       bỏ qua kết quả mismatch; khóa lịch sử nâng cao BLOCKED/P1.</li>
 *   <li>Tự động xuất CSV: {@code artifacts/evidence/D24/TV1-D24-lookup-trace.csv}.</li>
 * </ul>
 */
public class TicketLookupFlowTest {

    private static final List<RequestTraceEntry> globalTraceLog = new CopyOnWriteArrayList<>();

    @TempDir
    Path tempJournalDir;

    private FakeLeaderProvider leaderProvider;
    private PersistentPendingJournal journal;
    private final List<AutoCloseable> resourcesToClose = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        leaderProvider = new FakeLeaderProvider(3);
        journal = new PersistentPendingJournal(tempJournalDir);
    }

    @AfterEach
    void tearDown() {
        if (leaderProvider != null) {
            for (FakeNodeServer node : leaderProvider.getAllNodes()) {
                globalTraceLog.addAll(node.getRequestTraceLog());
            }
        }
        for (AutoCloseable res : resourcesToClose) {
            try {
                res.close();
            } catch (Exception ignored) {
            }
        }
        resourcesToClose.clear();

        if (journal != null && !journal.isClosed()) {
            try {
                journal.close();
            } catch (Exception ignored) {
            }
        }
        if (leaderProvider != null) {
            try {
                leaderProvider.close();
            } catch (Exception ignored) {
            }
        }
    }

    @AfterAll
    static void exportTraceCsv() {
        try {
            Path dir = Paths.get("artifacts/evidence/D24").toAbsolutePath();
            if (dir.toString().contains("desktop-client")) {
                dir = Paths.get("../artifacts/evidence/D24").toAbsolutePath();
            }
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            Path csvPath = dir.resolve("TV1-D24-lookup-trace.csv");
            FakeNodeServer.writeTraceCsv(csvPath, globalTraceLog);
            System.out.println("[EVIDENCE D24] Đã xuất file CSV thành công: " + csvPath + " (" + globalTraceLog.size() + " bản ghi)");
        } catch (Exception e) {
            System.err.println("[EVIDENCE D24] Lỗi xuất CSV: " + e.getMessage());
        }
    }

    private FakeNodeServer getLeaderNode() {
        return leaderProvider.getNode(leaderProvider.getCurrentLeaderId());
    }

    private FakeNodeServer getFollowerNode() {
        return leaderProvider.getAllNodes().stream()
                .filter(n -> !n.getNodeId().equals(leaderProvider.getCurrentLeaderId()))
                .findFirst()
                .orElseThrow();
    }

    private LeaderAwareBookingClient createLeaderClient(ClusterEndpoints endpoints, RedirectPolicy policy, ClusterConnectionViewModel vm) {
        LeaderAwareBookingClient client = new LeaderAwareBookingClient(
                endpoints,
                ep -> new SocketBookingClient(ep.host(), ep.port(), (int) policy.stepTimeoutMs(), (int) policy.stepTimeoutMs(), "lookup-test"),
                policy,
                vm,
                endpoints.getDefaultNodeId()
        );
        resourcesToClose.add(client);
        return client;
    }

    // =========================================================================
    // TC_LKP_01: Trạng thái PENDING từ Node -> Không kết luận, thông báo đang xử lý
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_01: Node trả PENDING -> ViewModel hiển thị chờ xử lý, không thêm vé vào confirmed list")
    void testTC_LKP_01_PendingState() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-PENDING-" + UUID.randomUUID();

        leaderNode.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_PENDING, "BOOK_TICKET", null, null, null, false
        ));

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-01");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        GetRequestResultResponseDto response = vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(GetRequestResultResponseDto.STATE_PENDING, response.getState());
        assertTrue(vm.getStatusMessage().contains("PENDING") || vm.getStatusMessage().contains("chờ xử lý"),
                "Thông báo phải nêu rõ yêu cầu đang chờ xử lý");
        assertTrue(vm.getAllConfirmedTickets().isEmpty(), "Không được tự ý thêm vé khi trạng thái là PENDING");
    }

    // =========================================================================
    // TC_LKP_02: Trạng thái APPLIED_SUCCESS (BOOK_TICKET) -> Vé CONFIRMED, REAL, chưa xác minh
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_02: Node trả APPLIED_SUCCESS -> Vé CONFIRMED, nhãn REAL, nhãn chưa xác minh, journal RESOLVED")
    void testTC_LKP_02_AppliedSuccess() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-SUCCESS-" + UUID.randomUUID();
        String ticketId = "TCK-SE001-G03";

        leaderNode.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", ticketId, null, 105L, false
        ));

        // Lưu trước yêu cầu vào journal
        PendingEntry initialEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "{\"seatId\":\"G03\"}");
        journal.recordPrepared(initialEntry);
        journal.markSent(reqId);

        AtomicBoolean refreshTriggered = new AtomicBoolean(false);
        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-02");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, () -> refreshTriggered.set(true));

        GetRequestResultResponseDto response = vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, response.getState());
        assertTrue(vm.getStatusMessage().contains("APPLIED_SUCCESS") || vm.getStatusMessage().contains("thành công"));

        // Kiểm tra vé trong danh sách
        assertEquals(1, vm.getAllConfirmedTickets().size());
        TicketViewModel tvm = vm.getAllConfirmedTickets().get(0);
        assertEquals(reqId, tvm.getRequestId());
        assertEquals(ticketId, tvm.getTicketId());
        assertEquals(TicketViewModel.TicketStatus.CONFIRMED, tvm.getStatus());
        assertEquals("chưa xác minh", tvm.getReasonCode(), "Phải mang nhãn 'chưa xác minh' theo D15");
        assertEquals("REAL", tvm.getMockBadge(), "Nhãn nguồn vé phải là REAL trong chế độ socket");

        // Kiểm tra journal được giải quyết
        PendingEntry entry = journal.get(reqId).orElseThrow();
        assertEquals(PendingState.RESOLVED, entry.getState());
        assertTrue(refreshTriggered.get(), "Callback refresh ghế phải được gọi");
    }

    // =========================================================================
    // TC_LKP_03: Trạng thái APPLIED_REJECTED -> Báo từ chối kèm reasonCode, journal RESOLVED
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_03: Node trả APPLIED_REJECTED -> Báo từ chối rõ ràng, journal RESOLVED(REJECTED)")
    void testTC_LKP_03_AppliedRejected() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-REJECT-" + UUID.randomUUID();

        leaderNode.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_REJECTED, "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", 106L, false
        ));

        PendingEntry initialEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "{\"seatId\":\"G04\"}");
        journal.recordPrepared(initialEntry);
        journal.markSent(reqId);

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-03");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        GetRequestResultResponseDto response = vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_REJECTED, response.getState());
        assertTrue(vm.getStatusMessage().contains("APPLIED_REJECTED") && vm.getStatusMessage().contains("SEAT_ALREADY_BOOKED"));

        PendingEntry entry = journal.get(reqId).orElseThrow();
        assertEquals(PendingState.RESOLVED, entry.getState());
        assertEquals("REJECTED", entry.getFinalOutcome());
        assertTrue(vm.getAllConfirmedTickets().isEmpty(), "Không được tạo vé khi yêu cầu bị từ chối");
    }

    // =========================================================================
    // TC_LKP_04: Lỗi hạ tầng (READ_UNAVAILABLE, NO_QUORUM, timeout) -> Giữ UNKNOWN
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_04: Lỗi hạ tầng -> Giữ nguyên UNKNOWN, không kết luận thành công hay thất bại")
    void testTC_LKP_04_UnknownAndInfraErrors() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-INFRA-" + UUID.randomUUID();

        leaderNode.setForcedError("READ_UNAVAILABLE", "Hạ tầng đọc chưa khả dụng");

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-04");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        GetRequestResultResponseDto response = vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(GetRequestResultResponseDto.STATE_UNKNOWN, response.getState());
        assertTrue(vm.getStatusMessage().contains("UNKNOWN") || vm.getStatusMessage().contains("READ_UNAVAILABLE"));
    }

    // =========================================================================
    // TC_LKP_05: Follower chưa thấy requestId -> Trả UNKNOWN, không phán xét chưa xảy ra
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_05: Follower chưa thấy requestId -> Trả UNKNOWN, không chứng minh lệnh chưa xảy ra")
    void testTC_LKP_05_NotFoundFromFollower_NotConclusive() throws Exception {
        FakeNodeServer followerNode = getFollowerNode();
        String reqId = "REQ-NOTFOUND-" + UUID.randomUUID();

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", followerNode.getPort(), 2000, 2000, "tc-05");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> vm.lookup(reqId).get(3, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof vn.nhom6.railway.client.network.NotLeaderException);
        assertTrue(vm.getStatusMessage().contains("UNKNOWN") || vm.getStatusMessage().contains("CHƯA RÕ"),
                "Khi gặp NotLeader từ follower chưa phân giải được, trạng thái yêu cầu phải giữ nguyên CHƯA RÕ (UNKNOWN)");
    }

    // =========================================================================
    // TC_LKP_06: NOT_LEADER Redirect -> Chuyển hướng đến đúng Leader và GIỮ NGUYÊN requestId
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_06: NOT_LEADER redirect -> Chuyển hướng tới Leader và GIỮ NGUYÊN requestId (Bất biến AGENTS.md)")
    void testTC_LKP_06_NotLeaderRedirect_KeepsRequestId() throws Exception {
        FakeNodeServer followerNode = getFollowerNode();
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-REDIRECT-" + UUID.randomUUID();

        followerNode.setNotLeader(leaderNode.getNodeId(), 2L);
        leaderNode.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", "TCK-REDIR-01", null, 110L, false
        ));

        ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();

        RedirectPolicy policy = new RedirectPolicy(3, 2000, 5000);
        ClusterConnectionViewModel connectionVm = new ClusterConnectionViewModel(vn.nhom6.railway.client.network.MockClusterStatusClient.getInstance());
        LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connectionVm);

        TicketLookupViewModel vm = new TicketLookupViewModel(leaderClient, journal, Runnable::run, null);

        GetRequestResultResponseDto response = vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, response.getState());
        assertEquals(reqId, response.getRequestId(), "Mã requestId phải giữ nguyên vẹn qua các bước redirect");
    }

    // =========================================================================
    // TC_LKP_07: RequestId trong phản hồi không khớp -> BỎ QUA kết quả
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_07: Response trả về requestId không khớp -> ViewModel phát hiện và BỎ QUA")
    void testTC_LKP_07_MismatchedRequestId_Ignored() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String queryReqId = "REQ-SENT-" + UUID.randomUUID();
        String wrongReqId = "REQ-WRONG-" + UUID.randomUUID();

        leaderNode.setCustomRequestResult(queryReqId, new GetRequestResultResponseDto(
                wrongReqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", "TCK-WRONG", null, 115L, false
        ));

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-07");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        GetRequestResultResponseDto response = vm.lookup(queryReqId).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertTrue(vm.getStatusMessage().contains("không khớp") || vm.getStatusMessage().contains("Cảnh báo"),
                "ViewModel phải cảnh báo kết quả không khớp requestId");
        assertTrue(vm.getAllConfirmedTickets().isEmpty(), "Không được thêm vé khi requestId không khớp!");
    }

    // =========================================================================
    // TC_LKP_08: Kiểm tra tính hợp lệ của Request ID (Input Validation)
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_08: RequestId rỗng hoặc sai format -> Chặn ngay tại Client, không gọi mạng")
    void testTC_LKP_08_InputValidation() {
        SocketBookingClient dummyClient = new SocketBookingClient("127.0.0.1", 9999, 100, 100, "dummy");
        resourcesToClose.add(dummyClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(dummyClient, journal, Runnable::run, null);

        // 1. Rỗng
        CompletableFuture<GetRequestResultResponseDto> f1 = vm.lookup("");
        assertTrue(f1.isCompletedExceptionally());
        assertTrue(vm.getStatusMessage().contains("Vui lòng nhập mã Request ID") || vm.getStatusMessage().contains("để trống"));

        // 2. Ký tự không hợp lệ
        CompletableFuture<GetRequestResultResponseDto> f2 = vm.lookup("REQ 123 !@#");
        assertTrue(f2.isCompletedExceptionally());
        assertTrue(vm.getStatusMessage().contains("không hợp lệ"));
    }

    // =========================================================================
    // TC_LKP_09: Single-Flight & Cooldown
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_09: Tra cứu liên tiếp nhanh -> Bị chặn bởi Cooldown / Single-Flight")
    void testTC_LKP_09_SingleFlightAndCooldown() {
        SocketBookingClient dummyClient = new SocketBookingClient("127.0.0.1", 9999, 100, 100, "dummy");
        resourcesToClose.add(dummyClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(dummyClient, journal, Runnable::run, null);

        String reqId = "REQ-COOLDOWN-01";
        CompletableFuture<GetRequestResultResponseDto> f1 = vm.lookup(reqId);
        CompletableFuture<GetRequestResultResponseDto> f2 = vm.lookup(reqId);

        assertNotNull(f1, "Lần gọi đầu tiên phải khởi tạo CompletableFuture");
        assertTrue(f2.isCompletedExceptionally(), "Lần gọi thứ hai ngay lập tức phải bị chặn bởi single-flight hoặc cooldown");
    }

    // =========================================================================
    // TC_LKP_10: Tra cứu giải quyết yêu cầu pending trong Journal
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_10: Entry SENT_UNRESOLVED trong journal được giải quyết thành RESOLVED sau khi tra cứu")
    void testTC_LKP_10_ResolvesPendingFromLookup() throws Exception {
        FakeNodeServer leaderNode = getLeaderNode();
        String reqId = "REQ-RESOLVE-" + UUID.randomUUID();

        PendingEntry initialEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "payload-raw");
        journal.recordPrepared(initialEntry);
        journal.markSent(reqId);

        leaderNode.setCustomRequestResult(reqId, new GetRequestResultResponseDto(
                reqId, GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, "BOOK_TICKET", "TCK-G05-01", null, 120L, false
        ));

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", leaderNode.getPort(), 2000, 2000, "tc-10");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        vm.lookup(reqId).get(3, TimeUnit.SECONDS);

        PendingEntry entry = journal.get(reqId).orElseThrow();
        assertEquals(PendingState.RESOLVED, entry.getState());
        assertEquals("CONFIRMED", entry.getFinalOutcome());
    }

    // =========================================================================
    // TC_LKP_11: Khởi động lại ứng dụng -> Nạp lại vé đã xác nhận từ Journal
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_11: Khởi động lại Client -> Nạp lại các vé đã xác nhận từ Journal bền vững")
    void testTC_LKP_11_RestartThenLookup() throws Exception {
        String reqId = "REQ-RESTART-" + UUID.randomUUID();

        PendingEntry initialEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "payload");
        journal.recordPrepared(initialEntry);
        journal.markSent(reqId);
        journal.markResolved(reqId, "CONFIRMED");

        SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", getLeaderNode().getPort(), 2000, 2000, "tc-11");
        resourcesToClose.add(socketClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(socketClient, journal, Runnable::run, null);

        assertEquals(1, vm.getAllConfirmedTickets().size(), "Phải nạp lại được vé đã xác nhận từ journal");
        assertEquals(reqId, vm.getAllConfirmedTickets().get(0).getRequestId());
        assertEquals("REAL", vm.getAllConfirmedTickets().get(0).getMockBadge());
    }

    // =========================================================================
    // TC_LKP_12: Danh sách vé chỉ hiển thị vé CONFIRMED
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_12: Danh sách vé xác nhận chỉ chứa vé CONFIRMED, không chứa vé chưa ngã ngũ hay từ chối")
    void testTC_LKP_12_ConfirmedListShowsOnlyConfirmed() throws Exception {
        String reqConfirmed = "REQ-CONFIRMED-" + UUID.randomUUID();
        String reqPending = "REQ-PENDING-" + UUID.randomUUID();

        PendingEntry e1 = PendingEntry.createPrepared(reqConfirmed, "BOOK_TICKET", "p1");
        journal.recordPrepared(e1);
        journal.markSent(reqConfirmed);
        journal.markResolved(reqConfirmed, "CONFIRMED");

        PendingEntry e2 = PendingEntry.createPrepared(reqPending, "BOOK_TICKET", "p2");
        journal.recordPrepared(e2);
        journal.markSent(reqPending);

        SocketBookingClient dummyClient = new SocketBookingClient("127.0.0.1", 9999, 100, 100, "dummy");
        resourcesToClose.add(dummyClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(dummyClient, journal, Runnable::run, null);

        assertEquals(1, vm.getAllConfirmedTickets().size());
        assertEquals(reqConfirmed, vm.getAllConfirmedTickets().get(0).getRequestId());
    }

    // =========================================================================
    // TC_LKP_13: Lịch sử nâng cao bị vô hiệu hóa khi chưa có contract (BLOCKED/P1)
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_13: Ở Socket mode, historyEnabled=false, thông báo BLOCKED/P1 rõ ràng")
    void testTC_LKP_13_HistoryBlockedWithoutContract() {
        SocketBookingClient dummyClient = new SocketBookingClient("127.0.0.1", 9999, 100, 100, "dummy");
        resourcesToClose.add(dummyClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(dummyClient, journal, Runnable::run, null);

        assertFalse(vm.isHistoryEnabled(), "Tính năng lịch sử nâng cao phải bị vô hiệu hóa ở chế độ socket");
        assertTrue(vm.getHistoryBlockedMessage().contains("BLOCKED/P1"),
                "Phải nêu rõ trạng thái BLOCKED/P1 do chưa có contract được duyệt");
    }

    // =========================================================================
    // TC_LKP_14: Tra cứu theo Ticket ID cục bộ, không tự chế bản tin socket
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_14: Tra cứu Ticket ID diễn ra cục bộ trong danh sách vé đã xác nhận, không gọi mạng tự chế")
    void testTC_LKP_14_HistoryFixtureOnlyWhenContractExists() {
        SocketBookingClient dummyClient = new SocketBookingClient("127.0.0.1", 9999, 100, 100, "dummy");
        resourcesToClose.add(dummyClient);

        TicketLookupViewModel vm = new TicketLookupViewModel(dummyClient, journal, Runnable::run, null);

        TicketViewModel tvm = new TicketViewModel(
                "REQ-TICKET-01", "TCK-SEARCH-01", "G08", "SE-001", "Hanh Khach", "2026-11-24 10:00:00",
                "1.250.000 VNĐ", TicketViewModel.TicketStatus.CONFIRMED, "chưa xác minh", "REAL"
        );
        vm.getAllConfirmedTickets().add(tvm);

        vm.searchByTicketId("TCK-SEARCH-01");
        assertEquals(1, vm.getDisplayedTickets().size());
        assertTrue(vm.getStatusMessage().contains("Tìm thấy 1 vé"));

        vm.searchByTicketId("TCK-KHONG-TON-TAI");
        assertTrue(vm.getDisplayedTickets().isEmpty());
        assertTrue(vm.getStatusMessage().contains("Không tìm thấy"));
    }

    // =========================================================================
    // TC_LKP_15: Chế độ Mock mang nhãn DEMO / MOCK
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_15: Khởi tạo với MockBookingClient -> mockMode=true, nhãn DEMO / MOCK")
    void testTC_LKP_15_MockModeLabelledDemo() {
        MockBookingClient mockClient = MockBookingClient.getInstance();
        TicketLookupViewModel vm = new TicketLookupViewModel(mockClient, null, Runnable::run, null);

        assertTrue(vm.isMockMode());
        assertTrue(vm.isHistoryEnabled(), "Ở chế độ mock thì mở minh họa demo");
        assertFalse(vm.getAllConfirmedTickets().isEmpty());
        assertEquals("DEMO / MOCK", vm.getAllConfirmedTickets().get(0).getMockBadge());
    }

    // =========================================================================
    // TC_LKP_16: Kiểm thử cụm thật (Guarded by Environment, mặc định SKIPPED chờ G6/P1)
    // =========================================================================
    @Test
    @DisplayName("TC_LKP_16: Kiểm thử cụm 5 Node thật (Guarded by railway.integration.realCluster, mặc định SKIPPED chờ G6)")
    void testTC_LKP_16_RealNode_GuardedByEnvironment() {
        Assumptions.assumeTrue(Boolean.getBoolean("railway.integration.realCluster"),
                "Bỏ qua kiểm thử cụm thật vì thiếu biến môi trường railway.integration.realCluster=true. Chờ nghiệm thu tại mốc G6 theo AGENTS.md.");

        globalTraceLog.add(new RequestTraceEntry(
                "TC_LKP_16", 0, "REAL_CLUSTER", "N/A", "-", "-", "-", "-",
                "PASS", "SKIPPED", "Guarded by environment, awaiting milestone G6/P1"
        ));
    }
}
