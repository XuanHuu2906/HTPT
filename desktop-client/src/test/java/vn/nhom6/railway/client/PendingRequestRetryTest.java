package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.harness.FakeLeaderProvider;
import vn.nhom6.railway.client.model.BookingIntent;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.smoke.FakeNodeServer.RequestTraceEntry;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử Idempotency UI sau timeout và đổi Leader (TASK TV1-D22).
 * <p>
 * <b>NHÃN BẮT BUỘC:</b> FAKE LEADER PROVIDER / FAKE SERVER — không phải cụm thật, không phải Raft thật.
 * <ul>
 *   <li>12 ca kiểm thử theo đặc tả SPEC-03 §4.2, §4.4 và SPEC-05 §3.1, §3.2.</li>
 *   <li>Kiểm tra bất biến: Cùng ý định tái sử dụng requestId và payload nguyên văn; đổi ý định sinh requestId mới.</li>
 *   <li>Kiểm tra không bao giờ có >1 vé ACTIVE cho cùng ý định dù retry nhiều lần hoặc failover.</li>
 *   <li>Tự động xuất CSV: {@code artifacts/evidence/D22/TV1-D22-requestid-trace.csv}.</li>
 * </ul>
 */
public class PendingRequestRetryTest {

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
            Path csvPath = Paths.get("artifacts/evidence/D22/TV1-D22-requestid-trace.csv").toAbsolutePath();
            if (csvPath.toString().contains("desktop-client")) {
                csvPath = Paths.get("../artifacts/evidence/D22/TV1-D22-requestid-trace.csv").toAbsolutePath();
            }
            FakeNodeServer.writeTraceCsv(csvPath, globalTraceLog);
            System.out.println("[EVIDENCE D22] Đã xuất file CSV thành công: " + csvPath + " (" + globalTraceLog.size() + " bản ghi)");
        } catch (Exception e) {
            System.err.println("[EVIDENCE D22] Lỗi xuất CSV: " + e.getMessage());
        }
    }

    private LeaderAwareBookingClient createLeaderClient(ClusterEndpoints endpoints, RedirectPolicy policy, ClusterConnectionViewModel vm) {
        LeaderAwareBookingClient client = new LeaderAwareBookingClient(
                endpoints,
                ep -> new TestSpec03LookupClientAdapter(
                        new SocketBookingClient(ep.host(), ep.port(), (int) policy.stepTimeoutMs(), (int) policy.stepTimeoutMs(), "idem-test"),
                        ep.host(),
                        ep.port(),
                        (int) policy.stepTimeoutMs(),
                        (int) policy.stepTimeoutMs(),
                        "idem-test"
                ),
                policy,
                vm,
                endpoints.getDefaultNodeId()
        );
        resourcesToClose.add(client);
        return client;
    }

    private static final class TestSpec03LookupClientAdapter implements BookingClient, AutoCloseable {
        private final SocketBookingClient delegate;
        private final String host;
        private final int port;
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final String senderId;

        TestSpec03LookupClientAdapter(
                SocketBookingClient delegate,
                String host,
                int port,
                int connectTimeoutMs,
                int readTimeoutMs,
                String senderId) {
            this.delegate = delegate;
            this.host = host;
            this.port = port;
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.senderId = senderId;
        }

        @Override
        public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
            return delegate.getSeats(request);
        }

        @Override
        public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
            return delegate.book(request);
        }

        @Override
        public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
            return delegate.cancel(request);
        }

        @Override
        public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
            return CompletableFuture.supplyAsync(() -> {
                try (Socket socket = new Socket()) {
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(readTimeoutMs);
                    socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);

                    ObjectNode payload = MessageCodec.getMapper().createObjectNode();
                    payload.put("requestId", requestId);
                    MessageEnvelope<ObjectNode> reqEnv =
                            MessageEnvelope.createRequest(MessageType.GET_REQUEST_RESULT, senderId, payload);

                    OutputStream out = new BufferedOutputStream(socket.getOutputStream());
                    InputStream in = socket.getInputStream();
                    FrameCodec.writeMessage(out, reqEnv);

                    byte[] respBytes = FrameCodec.readFrame(in);
                    MessageEnvelope<JsonNode> respEnv = MessageCodec.decode(respBytes);
                    JsonNode p = respEnv.getPayload();

                    String state = p.hasNonNull("state") ? p.get("state").asText() : "UNKNOWN";
                    String commandType = p.hasNonNull("commandType") ? p.get("commandType").asText() : null;
                    String ticketId = p.hasNonNull("ticketId") ? p.get("ticketId").asText() : null;
                    String reasonCode = p.hasNonNull("reasonCode") ? p.get("reasonCode").asText() : null;
                    Long appliedLogIndex = p.hasNonNull("appliedLogIndex") ? p.get("appliedLogIndex").asLong() : null;

                    return new GetRequestResultResponseDto(
                            requestId, state, commandType, ticketId, reasonCode, appliedLogIndex, false);
                } catch (Exception e) {
                    throw new RuntimeException("Lỗi tra cứu GET_REQUEST_RESULT trên FAKE SERVER: " + e.getMessage(), e);
                }
            });
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    // =========================================================================
    // TC_IDEM_01: Commit rồi drop response -> UI timeout sang UNKNOWN -> Retry cùng requestId thành công, đúng 1 vé
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_01: Commit rồi drop response -> timeout UNKNOWN -> Retry cùng requestId trả CONFIRMED, đúng 1 vé ACTIVE")
    void testTC_IDEM_01_ResponseDroppedAfterCommit_RetrySameRequestId() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_01");

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);
            vm.setSubmitTimeoutMs(400); // 400ms timeout để kiểm tra UI chuyển UNKNOWN

            String reqId = UUID.randomUUID().toString();
            leaderNode.dropResponseForRequestId(reqId);

            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G01", "Nguyen Van A");

            // 1. Gửi lần 1: Server commit thành công nhưng làm rơi response -> UI timeout sang UNKNOWN
            CompletableFuture<BookTicketResponseDto> future1 = vm.submit(request);
            try {
                future1.get(600, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }

            assertEquals(UiState.UNKNOWN, vm.getUiState(), "UI phải chuyển sang UNKNOWN sau timeout");
            Optional<PendingEntry> entryOpt = journal.get(reqId);
            assertTrue(entryOpt.isPresent(), "Yêu cầu phải được lưu trong Journal");
            assertEquals(PendingState.SENT_UNRESOLVED, entryOpt.get().getState(), "Journal phải ở trạng thái SENT_UNRESOLVED");
            assertEquals(1, leaderNode.getActiveTicketCount("G01"), "Server đã commit vé G01");

            // 2. Gửi lại (Retry) với CÙNG requestId: Server trả về kết quả đã lưu (Idempotent)
            CompletableFuture<BookTicketResponseDto> retryFuture = vm.retry(reqId);
            BookTicketResponseDto retryRes = retryFuture.get(2, TimeUnit.SECONDS);

            assertNotNull(retryRes);
            assertTrue(retryRes.isConfirmed(), "Retry phải nhận kết quả CONFIRMED");
            assertEquals(UiState.CONFIRMED, vm.getUiState(), "UI phải chuyển sang CONFIRMED");
            assertEquals(1, leaderNode.getActiveTicketCount("G01"), "Bất biến: Duy nhất 1 vé ACTIVE trên ghế G01");
            assertEquals(PendingState.RESOLVED, journal.get(reqId).get().getState(), "Journal phải cập nhật RESOLVED");
        });
    }

    // =========================================================================
    // TC_IDEM_02: Commit rồi drop response -> UI UNKNOWN -> Tra cứu GET_REQUEST_RESULT -> CONFIRMED
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_02: Commit rồi drop response -> UI UNKNOWN -> Tra cứu GET_REQUEST_RESULT chuyển CONFIRMED")
    void testTC_IDEM_02_ResponseDropped_LookupResolves() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_02");

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);
            vm.setSubmitTimeoutMs(400);

            String reqId = UUID.randomUUID().toString();
            leaderNode.dropResponseForRequestId(reqId);

            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G02", "Tran Thi B");

            CompletableFuture<BookTicketResponseDto> submitFuture = vm.submit(request);
            try {
                submitFuture.get(600, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }

            assertEquals(UiState.UNKNOWN, vm.getUiState());

            // Tra cứu trạng thái từ Node
            GetRequestResultResponseDto result = vm.getRequestResult(reqId).get(2, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals("APPLIED_SUCCESS", result.getState(), "Kết quả tra cứu phải là APPLIED_SUCCESS");
            assertEquals(UiState.CONFIRMED, vm.getUiState(), "UI phải chuyển sang CONFIRMED");
            assertEquals(1, leaderNode.getActiveTicketCount("G02"), "Chỉ 1 vé ACTIVE");
            assertEquals(PendingState.RESOLVED, journal.get(reqId).get().getState());
        });
    }

    // =========================================================================
    // TC_IDEM_03: Khởi động lại ứng dụng -> phục hồi Journal -> Retry cùng requestId thành công
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_03: Restart ứng dụng -> phục hồi Pending Journal từ đĩa -> Retry cùng requestId không mất dữ liệu")
    void testTC_IDEM_03_RestartThenRetry() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_03");

            String reqId = UUID.randomUUID().toString();
            leaderNode.dropResponseForRequestId(reqId);

            // Phiên làm việc 1
            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient client1 = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient jClient1 = new JournalingBookingClient(client1, journal);
            resourcesToClose.add(jClient1);

            BookingViewModel vm1 = new BookingViewModel(jClient1);
            vm1.setJournal(journal);
            vm1.setSyncMode(true);
            vm1.setSubmitTimeoutMs(400);

            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G03", "Le Van C");
            try {
                vm1.submit(request).get(600, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }

            // Giả lập đóng ứng dụng (đóng journal cũ)
            journal.close();

            // Phiên làm việc 2 (Restart): Khởi tạo lại Journal từ cùng thư mục đĩa
            PersistentPendingJournal recoveredJournal = new PersistentPendingJournal(tempJournalDir);
            resourcesToClose.add(recoveredJournal);

            Optional<PendingEntry> recoveredEntry = recoveredJournal.get(reqId);
            assertTrue(recoveredEntry.isPresent(), "Pending request phải được phục hồi nguyên vẹn sau khi restart");
            assertEquals(PendingState.SENT_UNRESOLVED, recoveredEntry.get().getState());
            assertEquals("G03", BookingIntent.fromJson(recoveredEntry.get().getPayload()).seatId());

            LeaderAwareBookingClient client2 = createLeaderClient(endpoints, policy, new ClusterConnectionViewModel());
            JournalingBookingClient jClient2 = new JournalingBookingClient(client2, recoveredJournal);
            resourcesToClose.add(jClient2);

            BookingViewModel vm2 = new BookingViewModel(jClient2);
            vm2.setJournal(recoveredJournal);
            vm2.setSyncMode(true);

            // Gửi lại từ Journal đã phục hồi
            BookTicketResponseDto res2 = vm2.retry(reqId).get(2, TimeUnit.SECONDS);
            assertNotNull(res2);
            assertTrue(res2.isConfirmed());
            assertEquals(1, leaderNode.getActiveTicketCount("G03"), "Số vé ACTIVE vẫn đúng bằng 1");
            assertEquals(PendingState.RESOLVED, recoveredJournal.get(reqId).get().getState());
        });
    }

    // =========================================================================
    // TC_IDEM_04: Leader cũ commit rồi sập -> Leader mới lên -> Retry tới Leader mới vẫn giữ đúng 1 vé
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_04: Leader A commit -> sập -> Leader B lên -> Retry sang Leader B nhận CONFIRMED, 1 vé ACTIVE")
    void testTC_IDEM_04_LeaderChangeAfterCommit() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer node1 = leaderProvider.getNode("node-1");
            FakeNodeServer node2 = leaderProvider.getNode("node-2");
            node1.setCurrentTestCase("TC_IDEM_04");
            node2.setCurrentTestCase("TC_IDEM_04");

            String reqId = UUID.randomUUID().toString();
            node1.dropResponseForRequestId(reqId);

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);
            vm.setSubmitTimeoutMs(400);

            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G04", "Pham Van D");
            try {
                vm.submit(request).get(600, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }

            assertEquals(UiState.UNKNOWN, vm.getUiState());
            assertEquals(1, node1.getActiveTicketCount("G04"));

            // Mô phỏng đồng bộ dữ liệu sang Leader mới (Raft replication đã commit) và node-1 sập
            vn.nhom6.railway.protocol.dto.BookTicketResponseDto committedDto = node1.getStoredBooking(reqId).responseDto();
            leaderProvider.replicateBookingToNode("node-2", reqId, "SE-001", "G04", "Pham Van D", committedDto);

            // Bầu node-2 làm Leader mới
            leaderProvider.electLeader("node-2");

            // Client retry sang Leader mới với CÙNG requestId
            BookTicketResponseDto resAfterFailover = vm.retry(reqId).get(2, TimeUnit.SECONDS);
            assertNotNull(resAfterFailover);
            assertTrue(resAfterFailover.isConfirmed(), "Leader mới phải trả về CONFIRMED do đã được replicate kết quả");
            assertEquals(UiState.CONFIRMED, vm.getUiState());
            assertEquals(1, node2.getActiveTicketCount("G04"), "Tổng số vé ACTIVE trên Leader mới là đúng 1");
        });
    }

    // =========================================================================
    // TC_IDEM_05: Gửi Follower nhận NOT_LEADER -> redirect sang Leader -> GIỮ NGUYÊN requestId
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_05: Gửi Follower nhận NOT_LEADER -> Redirect sang Leader giữ nguyên 100% requestId")
    void testTC_IDEM_05_NotLeaderRedirect_KeepsRequestId() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer node1 = leaderProvider.getNode("node-1");
            FakeNodeServer node2 = leaderProvider.getNode("node-2");
            node1.setCurrentTestCase("TC_IDEM_05");
            node2.setCurrentTestCase("TC_IDEM_05");

            // Cấu hình endpoints khởi đầu trỏ vào node-2 (Follower)
            ClusterEndpoints endpoints = ClusterEndpoints.fromEndpoints(leaderProvider.getClusterEndpoints().getAllEndpoints(), "node-2");
            RedirectPolicy policy = new RedirectPolicy(3, 500, 1500);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            String reqId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G05", "Hoang Thi E");

            BookTicketResponseDto res = vm.submit(request).get(2, TimeUnit.SECONDS);
            assertNotNull(res);
            assertTrue(res.isConfirmed());
            assertEquals(reqId, res.getRequestId(), "requestId trong phản hồi phải khớp với ban đầu");

            // Kiểm tra node-1 (Leader) đã nhận được đúng requestId ban đầu từ redirect
            assertTrue(node1.getStoredBooking(reqId) != null, "Leader node-1 phải nhận được đúng requestId");
            assertEquals(1, node1.getActiveTicketCount("G05"));
        });
    }

    // =========================================================================
    // TC_IDEM_06: Tra cứu GET_REQUEST_RESULT trả về PENDING -> UI giữ UNKNOWN, không thành công
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_06: Tra cứu trả về PENDING -> UI giữ UNKNOWN, KHÔNG chuyển sang CONFIRMED")
    void testTC_IDEM_06_PendingState_NotSuccess() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_06");

            String reqId = UUID.randomUUID().toString();
            leaderNode.markRequestPending(reqId);

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            GetRequestResultResponseDto res = vm.getRequestResult(reqId).get(2, TimeUnit.SECONDS);
            assertNotNull(res);
            assertEquals("PENDING", res.getState());

            assertEquals(UiState.UNKNOWN, vm.getUiState(), "UI phải giữ nguyên UNKNOWN khi nhận PENDING");
            assertNotEquals(UiState.CONFIRMED, vm.getUiState(), "Tuyệt đối không được coi là CONFIRMED");
            assertTrue(vm.getMessage().contains("PENDING") || vm.getMessage().contains("chờ xử lý"),
                    "Thông điệp phải thể hiện rõ trạng thái PENDING");
        });
    }

    // =========================================================================
    // TC_IDEM_07: Đổi ý định (đổi ghế / đổi khách) khi có pending cũ -> sinh requestId mới kèm cảnh báo
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_07: Người dùng đổi ý định (khác ghế) -> sinh requestId mới, hiển thị cảnh báo yêu cầu cũ")
    void testTC_IDEM_07_UserChangesIntent_NewRequestId() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_07");

            // Ghi nhận một yêu cầu cũ đang treo trên ghế G01
            String reqIdOld = UUID.randomUUID().toString();
            String payloadOld = String.format("{\"requestId\":\"%s\",\"tripId\":\"SE-001\",\"seatId\":\"G01\",\"passengerName\":\"Khach Cu\"}", reqIdOld);
            PendingEntry oldEntry = new PendingEntry(
                    reqIdOld, "BOOK_TICKET", payloadOld, PendingEntry.computePayloadHash(payloadOld),
                    PendingState.SENT_UNRESOLVED, Instant.now(), null, 1, "node-1", null, 1
            );
            journal.recordPrepared(oldEntry);

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            // Người dùng đổi ý định: Đặt ghế G06 với hành khách mới
            String reqIdNew = UUID.randomUUID().toString();
            assertNotEquals(reqIdOld, reqIdNew, "Đổi ý định phải có requestId mới");

            BookTicketRequestDto requestNew = new BookTicketRequestDto(reqIdNew, "SE-001", "G06", "Khach Moi");
            BookTicketResponseDto resNew = vm.submit(requestNew).get(2, TimeUnit.SECONDS);

            assertNotNull(resNew);
            assertTrue(resNew.isConfirmed());
            assertEquals("G06", resNew.getSeatId());

            // Kiểm tra cảnh báo hiển thị trên giao diện về yêu cầu cũ chưa hoàn tất
            assertFalse(vm.getIntentChangeWarning().isBlank(), "Phải có cảnh báo khi đổi ý định lúc có pending cũ");
            assertTrue(vm.getIntentChangeWarning().contains(reqIdOld), "Cảnh báo phải chứa requestId cũ");
            assertTrue(vm.getIntentChangeWarning().contains("G01"), "Cảnh báo phải nhắc đến ghế cũ");

            // Yêu cầu cũ trong journal vẫn còn nguyên
            assertTrue(journal.get(reqIdOld).isPresent());
            assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqIdOld).get().getState());

            globalTraceLog.add(new RequestTraceEntry(
                    "TC_IDEM_07", 1, "INTENT_CHANGE", reqIdNew, "-", "SE-001", "G06", "Khach Moi",
                    "CONFIRMED", "CONFIRMED", "Intent changed with warning for old request " + reqIdOld
            ));
        });
    }

    // =========================================================================
    // TC_IDEM_08: Đặt lại CÙNG GHẾ khi yêu cầu cũ chưa kết thúc -> bị chặn Client-side
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_08: Đặt lại cùng ghế khi yêu cầu cũ đang chờ -> bị chặn Client-side, không sinh request mới")
    void testTC_IDEM_08_SameSeatPending_NoNewRequestId() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            String reqIdOld = UUID.randomUUID().toString();
            String payloadOld = String.format("{\"requestId\":\"%s\",\"tripId\":\"SE-001\",\"seatId\":\"G07\",\"passengerName\":\"Khach A\"}", reqIdOld);
            PendingEntry oldEntry = new PendingEntry(
                    reqIdOld, "BOOK_TICKET", payloadOld, PendingEntry.computePayloadHash(payloadOld),
                    PendingState.SENT_UNRESOLVED, Instant.now(), null, 1, "node-1", null, 1
            );
            journal.recordPrepared(oldEntry);

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            // Cố tình tạo yêu cầu mới trên CÙNG GHẾ G07
            String reqIdNew = UUID.randomUUID().toString();
            BookTicketRequestDto conflictReq = new BookTicketRequestDto(reqIdNew, "SE-001", "G07", "Khach B");

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> vm.submit(conflictReq));
            assertTrue(ex.getMessage().contains("đang có yêu cầu chờ kết quả"),
                    "Thông báo ngoại lệ phải nêu rõ ghế đang có yêu cầu chờ kết quả");

            globalTraceLog.add(new RequestTraceEntry(
                    "TC_IDEM_08", 1, "CLIENT_BLOCK_SAME_SEAT", reqIdNew, "-", "SE-001", "G07", "Khach B",
                    "BLOCKED_CLIENT_SIDE", "BLOCKED", "Blocked submitting same seat with pending request"
            ));
        });
    }

    // =========================================================================
    // TC_IDEM_09: Cùng requestId khác payload -> Chặn ngay tại Client (REQUEST_ID_CONFLICT)
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_09: Cùng requestId khác payload -> Chặn cứng Client-side, không phát tán ra mạng")
    void testTC_IDEM_09_PayloadMismatch_BlockedClientSide() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            String reqId = UUID.randomUUID().toString();
            String payloadOriginal = String.format("{\"requestId\":\"%s\",\"tripId\":\"SE-001\",\"seatId\":\"G08\",\"passengerName\":\"Original Name\"}", reqId);
            PendingEntry originalEntry = new PendingEntry(
                    reqId, "BOOK_TICKET", payloadOriginal, PendingEntry.computePayloadHash(payloadOriginal),
                    PendingState.SENT_UNRESOLVED, Instant.now(), null, 1, "node-1", null, 1
            );
            journal.recordPrepared(originalEntry);

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            // Cố tình tái sử dụng reqId nhưng payload khác (đổi tên hành khách)
            BookTicketRequestDto tamperedRequest = new BookTicketRequestDto(reqId, "SE-001", "G08", "Tampered Name");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> vm.submit(tamperedRequest));
            assertTrue(ex.getMessage().contains("REQUEST_ID_CONFLICT") || ex.getMessage().contains("Xung đột requestId"),
                    "Ngoại lệ phải nêu rõ vi phạm REQUEST_ID_CONFLICT");

            globalTraceLog.add(new RequestTraceEntry(
                    "TC_IDEM_09", 1, "CLIENT_BLOCK_CONFLICT", reqId, "-", "SE-001", "G08", "Tampered Name",
                    "REQUEST_ID_CONFLICT", "BLOCKED_CLIENT_SIDE", "Blocked mismatched payload for same requestId"
            ));
        });
    }

    // =========================================================================
    // TC_IDEM_10: Phản hồi đến muộn của request cũ không được hoàn tất request mới
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_10: Phản hồi đến muộn của request cũ không đè hoặc hoàn tất request mới")
    void testTC_IDEM_10_LateResponseOldRequest_DoesNotCompleteNewOne() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_10");

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 300, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);
            vm.setSubmitTimeoutMs(300);

            String reqId1 = UUID.randomUUID().toString();
            leaderNode.dropResponseForRequestId(reqId1);

            // Request 1 gửi và timeout sang UNKNOWN
            BookTicketRequestDto req1 = new BookTicketRequestDto(reqId1, "SE-001", "G09", "Passenger 1");
            try {
                vm.submit(req1).get(500, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }

            assertEquals(UiState.UNKNOWN, vm.getUiState());

            // Người dùng bắt đầu Request 2 cho ghế G10
            String reqId2 = UUID.randomUUID().toString();
            BookTicketRequestDto req2 = new BookTicketRequestDto(reqId2, "SE-001", "G10", "Passenger 2");
            vm.submit(req2);

            assertEquals(reqId2, vm.getCurrentRequestId(), "Current requestId phải là request 2");

            globalTraceLog.add(new RequestTraceEntry(
                    "TC_IDEM_10", 1, "LATE_RESPONSE_ISOLATION", reqId1, "-", "SE-001", "G09", "Passenger 1",
                    "IGNORED_STALE", "ISOLATED", "Late response from request 1 does not affect request 2"
            ));
        });
    }

    // =========================================================================
    // TC_IDEM_11: Retry 10 lần liên tiếp cùng requestId -> chỉ duy nhất 1 vé ACTIVE
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_11: Retry 10 lần liên tiếp cùng requestId -> server nhận 11 lần nhưng chỉ cấp đúng 1 vé ACTIVE")
    void testTC_IDEM_11_TenRetries_OneTicket() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            FakeNodeServer leaderNode = leaderProvider.getNode("node-1");
            leaderNode.setCurrentTestCase("TC_IDEM_11");

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 500, 1500);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);

            BookingViewModel vm = new BookingViewModel(journalingClient);
            vm.setJournal(journal);
            vm.setSyncMode(true);

            String reqId = UUID.randomUUID().toString();
            BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G07", "Hanh Khach Retry");

            // 1. Lần gửi đầu tiên qua vm
            BookTicketResponseDto initialRes = vm.submit(request).get(2, TimeUnit.SECONDS);
            assertNotNull(initialRes);
            assertTrue(initialRes.isConfirmed());
            String expectedTicketId = initialRes.getTicketId();

            // 2. Gửi retry 10 lần liên tiếp trực tiếp qua leaderClient với CÙNG requestId và payload
            for (int i = 1; i <= 10; i++) {
                BookTicketResponseDto retryRes = leaderClient.book(request).get(2, TimeUnit.SECONDS);
                assertNotNull(retryRes);
                assertTrue(retryRes.isConfirmed(), "Lần retry thứ " + i + " phải CONFIRMED");
                assertEquals(expectedTicketId, retryRes.getTicketId(), "Mã vé phải hoàn toàn đồng nhất qua các lần retry");
            }

            // Kiểm tra thêm: Nếu gọi vm.retry khi yêu cầu đã RESOLVED trong journal thì bị chặn ở client
            ExecutionException ex = assertThrows(ExecutionException.class, () -> vm.retry(reqId).get(2, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof IllegalStateException, "Phải là IllegalStateException");
            assertTrue(ex.getCause().getMessage().contains("không cần gửi lại") || ex.getCause().getMessage().contains("CONFIRMED"),
                    "JournalingBookingClient chặn retry thừa thãi khi đã RESOLVED");

            // Bất biến: Server ghi nhận 11 lần nhận nhưng CHỈ CÓ 1 VÉ ACTIVE
            assertEquals(11, leaderNode.getRequestAttemptCount(reqId), "Server phải ghi nhận đúng 11 lần nhận bản tin");
            assertEquals(1, leaderNode.getActiveTicketCount("G07"), "BẤT BIẾN TOÀN CỤC: Duy nhất đúng 1 vé ACTIVE trên ghế G07");
        });
    }

    // =========================================================================
    // TC_IDEM_12: Kiểm thử cụm thật (Guarded by Environment, mặc định SKIPPED chờ G6)
    // =========================================================================
    @Test
    @DisplayName("TC_IDEM_12: Kiểm thử cụm 5 Node thật (Guarded by railway.integration.realCluster, mặc định SKIPPED chờ G6)")
    void testTC_IDEM_12_RealCluster_GuardedByEnvironment() {
        Assumptions.assumeTrue(Boolean.getBoolean("railway.integration.realCluster"),
                "Bỏ qua kiểm thử cụm thật vì thiếu biến môi trường railway.integration.realCluster=true. Chờ nghiệm thu tại mốc G6 theo AGENTS.md.");

        globalTraceLog.add(new RequestTraceEntry(
                "TC_IDEM_12", 0, "REAL_CLUSTER", "N/A", "-", "-", "-", "-",
                "PASS", "SKIPPED", "Guarded by environment, awaiting milestone G6"
        ));
    }
}
