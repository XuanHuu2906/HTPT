package vn.nhom6.railway.client.controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ReconnectController.ReconnectState;
import vn.nhom6.railway.client.harness.FakeLeaderProvider;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ 10 ca kiểm thử chấp nhận Reconnect khi Leader lỗi và bảo toàn Pending Request (TASK TV1-D21).
 * <p>
 * <b>NHÃN BẮT BUỘC:</b> FAKE LEADER PROVIDER — không phải Raft thật, không phải cụm thật.
 * <p>
 * Bất biến an toàn:
 * <ul>
 *   <li>requestId duy nhất, bất biến; payload giữ nguyên văn từ Journal.</li>
 *   <li>Không tự gửi lại lệnh BOOK mới khi đang tìm Leader; chỉ cho phép tra cứu hoặc gửi lại request cũ đã có trong journal.</li>
 *   <li>Lỗi trước gửi: FAILED_NOT_SENT, ghế AVAILABLE.</li>
 *   <li>Lỗi sau gửi: TIMEOUT_UNKNOWN, ghế UNKNOWN (viền cam), journal SENT_UNRESOLVED.</li>
 *   <li>Dò tìm Leader tuyệt đối KHÔNG gửi BOOK/CANCEL; chỉ dùng read-only probe.</li>
 * </ul>
 */
@Timeout(value = 35, unit = TimeUnit.SECONDS)
public class ReconnectControllerTest {

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
        for (AutoCloseable res : resourcesToClose) {
            try {
                res.close();
            } catch (Exception ignored) {
            }
        }
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

    private LeaderAwareBookingClient createLeaderClient(ClusterEndpoints endpoints, RedirectPolicy policy, ClusterConnectionViewModel vm) {
        LeaderAwareBookingClient client = new LeaderAwareBookingClient(
                endpoints,
                ep -> new SocketBookingClient(ep.host(), ep.port(), (int) policy.stepTimeoutMs(), (int) policy.stepTimeoutMs(), "rec-test"),
                policy,
                vm,
                endpoints.getDefaultNodeId()
        );
        resourcesToClose.add(client);
        return client;
    }

    // =========================================================================
    // TC_REC_01: Leader sập đột ngột -> kích hoạt trạng thái SEARCHING_LEADER
    // =========================================================================
    @Test
    @DisplayName("TC_REC_01: Leader sập đột ngột -> ReconnectController hiển thị Đang tìm Leader, UI không treo")
    void testTC_REC_01_LeaderCrash_TriggersSearchingState() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 500, 1500);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);

            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);
            BookingViewModel bookingVm = new BookingViewModel(journalingClient);

            ReconnectController controller = new ReconnectController(leaderClient, bookingVm, connVm, journal);
            controller.setDataSource(DataSource.MOCK);

            // Ban đầu: CONNECTED tới node-1
            assertEquals(ReconnectState.CONNECTED, controller.getState());
            assertFalse(controller.isBannerVisible());
            assertTrue(controller.getCanBookNew());
            assertTrue(controller.getCanRetryOrLookup());

            // Node-1 (Leader) bị sập đột ngột
            leaderProvider.crashNode("node-1");

            // Kích hoạt phát hiện mất kết nối bằng cách gửi lệnh book tới node-1 bị sập
            BookTicketRequestDto req = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G01", "Hành Khách Test 1");
            BookTicketResponseDto response = leaderClient.book(req).join();
            assertNotNull(response, "Phản hồi từ cụm không được null");
            assertFalse(response.isConfirmed(), "Gửi tới Node Leader bị sập không thể CONFIRMED");

            // Xác nhận controller đã chuyển sang trạng thái SEARCHING_LEADER
            assertEquals(ReconnectState.SEARCHING_LEADER, controller.getState(), "Phải chuyển sang SEARCHING_LEADER khi mất Leader");
            assertTrue(controller.isBannerVisible(), "Banner phải hiển thị");
            assertTrue(controller.getBannerMessage().contains("Đang tìm Leader") || controller.getBannerMessage().contains("Mất kết nối với Leader"),
                    "Thông điệp banner phải thông báo đang tìm Leader: " + controller.getBannerMessage());
            assertFalse(controller.getCanBookNew(), "Chặn đặt vé mới khi đang tìm Leader");
            assertTrue(controller.getCanRetryOrLookup(), "Vẫn cho phép tra cứu hoặc gửi lại request cũ");
        });
    }

    // =========================================================================
    // TC_REC_02: Pending Request được bảo toàn 100% qua failover
    // =========================================================================
    @Test
    @DisplayName("TC_REC_02: Pending request giữ nguyên requestId, payload nguyên văn byte-for-byte, trạng thái SENT_UNRESOLVED")
    void testTC_REC_02_PendingPreservedAcrossFailover() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            String requestId = UUID.randomUUID().toString();
            String payload = "{\"requestId\":\"" + requestId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G02\",\"passengerName\":\"Trần Văn Bảo Toàn\"}";
            String payloadHash = PendingEntry.computePayloadHash(payload);

            // Ghi nhận trạng thái chuẩn bị và đã gửi vào Journal
            PendingEntry entry = new PendingEntry(
                    requestId,
                    "BOOK_TICKET",
                    payload,
                    payloadHash,
                    PendingState.PREPARED,
                    Instant.now(),
                    null,
                    1,
                    "node-1",
                    null,
                    1
            );
            journal.recordPrepared(entry);
            journal.markSent(requestId);

            // Leader sập
            leaderProvider.crashNode("node-1");

            // Kiểm tra tính toàn vẹn 100% của Pending Request trong Journal
            Optional<PendingEntry> retrievedOpt = journal.get(requestId);
            assertTrue(retrievedOpt.isPresent(), "Pending request phải còn nguyên trong journal");

            PendingEntry retrieved = retrievedOpt.get();
            assertEquals(requestId, retrieved.getRequestId(), "requestId phải giữ nguyên");
            assertEquals(PendingState.SENT_UNRESOLVED, retrieved.getState(), "Trạng thái phải là SENT_UNRESOLVED");
            assertEquals(payload, retrieved.getPayload(), "Payload nguyên văn chuỗi phải khớp 100%");
            assertArrayEquals(payload.getBytes(StandardCharsets.UTF_8), retrieved.getPayload().getBytes(StandardCharsets.UTF_8),
                    "Payload dạng byte UTF-8 phải trùng khớp tuyệt đối");
            assertEquals(payloadHash, retrieved.getPayloadHash(), "Payload hash phải khớp");
            assertEquals("node-1", retrieved.getLastKnownNodeId(), "Node mục tiêu ghi nhận chuẩn");
        });
    }

    // =========================================================================
    // TC_REC_03: Tìm thấy Leader mới -> tra cứu/gửi lại bằng đúng requestId gốc
    // =========================================================================
    @Test
    @DisplayName("TC_REC_03: Node 2 lên Leader mới -> ReconnectController báo CONNECTED -> tra cứu đúng requestId gốc")
    void testTC_REC_03_NewLeaderFound_ThenLookupByOriginalRequestId() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(3, 500, 2000);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);
            BookingViewModel bookingVm = new BookingViewModel(journalingClient);
            ReconnectController controller = new ReconnectController(leaderClient, bookingVm, connVm, journal);
            controller.setDataSource(DataSource.MOCK);

            // 1. Chuẩn bị pending request
            String origRequestId = UUID.randomUUID().toString();
            String payload = "{\"requestId\":\"" + origRequestId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G03\",\"passengerName\":\"Lê Thị C\"}";
            PendingEntry entry = new PendingEntry(origRequestId, "BOOK_TICKET", payload, PendingEntry.computePayloadHash(payload),
                    PendingState.PREPARED, Instant.now(), null, 1, "node-1", null, 1);
            journal.recordPrepared(entry);
            journal.markSent(origRequestId);

            // 2. Node-1 sập, Node-2 đắc cử Leader
            leaderProvider.crashNode("node-1");
            leaderProvider.electLeader("node-2");

            // 3. Kích hoạt dò tìm Leader
            String discoveredLeader = controller.triggerReconnect().get(5, TimeUnit.SECONDS);
            assertEquals("node-2", discoveredLeader, "Phải tìm thấy node-2 là Leader mới");
            assertEquals(ReconnectState.CONNECTED, controller.getState(), "Trạng thái controller phải trở về CONNECTED");
            assertFalse(controller.isBannerVisible(), "Banner phải ẩn khi đã có Leader");

            // 4. Tra cứu hoặc gửi lại với đúng requestId gốc tới Node-2
            FakeNodeServer node2 = leaderProvider.getNode("node-2");
            assertNotNull(node2);

            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", node2.getPort()), 1500);
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();

                GetRequestResultRequestDto reqDto = new GetRequestResultRequestDto(origRequestId);
                MessageEnvelope<GetRequestResultRequestDto> env = MessageEnvelope.createRequest(
                        MessageType.GET_REQUEST_RESULT,
                        "client-test",
                        reqDto
                );

                FrameCodec.writeMessage(out, env);
                byte[] resBytes = FrameCodec.readFrame(in);
                MessageEnvelope<JsonNode> resEnv = MessageCodec.decode(resBytes);

                assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, resEnv.getType());
                assertEquals(origRequestId, resEnv.getPayload().get("requestId").asText(),
                        "Bản tin tra cứu trả về đúng requestId gốc");
            }
        });
    }

    // =========================================================================
    // TC_REC_04: Leader cũ đã apply trước crash -> Leader mới trả kết quả đã lưu, không duplicate
    // =========================================================================
    @Test
    @DisplayName("TC_REC_04: Leader cũ đã apply trước crash -> Leader mới trả kết quả cũ (idempotent), không sinh vé trùng")
    void testTC_REC_04_OldLeaderAppliedBeforeCrash_NoDuplicate() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            String requestId = UUID.randomUUID().toString();
            String tripId = "SE-001";
            String seatId = "G04";
            String passenger = "Phạm Văn D";

            // Kết quả đã được Leader cũ áp dụng thành công
            vn.nhom6.railway.protocol.dto.BookTicketResponseDto appliedResponse =
                    vn.nhom6.railway.protocol.dto.BookTicketResponseDto.confirmed(
                            requestId,
                            tripId,
                            seatId,
                            "TCK-G04-PRE-APPLIED",
                            101L
                    );

            // Đồng bộ sang Node-2 trước khi sập (mô phỏng Raft log replicate)
            leaderProvider.replicateBookingToNode("node-2", requestId, tripId, seatId, passenger, appliedResponse);

            // Node-1 sập, Node-2 lên Leader
            leaderProvider.crashNode("node-1");
            leaderProvider.electLeader("node-2");

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(3, 500, 2000);
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, null);

            // Khám phá Leader mới (node-2) trước khi gửi lại
            String newLeader = leaderClient.discoverLeader().get(5, TimeUnit.SECONDS);
            assertEquals("node-2", newLeader, "LeaderAwareBookingClient phải phát hiện node-2 là Leader mới");

            // Client gửi lại (retry) với CÙNG requestId và CÙNG payload tới Leader mới
            BookTicketRequestDto retryReq = new BookTicketRequestDto(requestId, tripId, seatId, passenger);
            BookTicketResponseDto retryResponse = leaderClient.book(retryReq).get(5, TimeUnit.SECONDS);

            assertNotNull(retryResponse);
            assertTrue(retryResponse.isConfirmed(), "Phải trả về CONFIRMED từ kết quả đã lưu");
            assertEquals("TCK-G04-PRE-APPLIED", retryResponse.getTicketId(), "Phải trả về ĐÚNG ticketId cũ đã cấp");
            assertEquals(101L, retryResponse.getAppliedLogIndex(), "Log index phải bảo toàn");

            // Kiểm tra số vé trên ghế G04 tại Node-2: đúng 1 vé ACTIVE, không có vé thứ hai
            FakeNodeServer node2 = leaderProvider.getNode("node-2");
            assertEquals(1, node2.getBookingAuditLog().stream()
                    .filter(a -> a.requestId().equals(requestId))
                    .count(), "Phải chỉ ghi nhận đúng 1 lần xử lý nghiệp vụ cho requestId này");
        });
    }

    // =========================================================================
    // TC_REC_05: Cụm không có Leader -> backoff có giới hạn, không spam socket, không gửi BOOK/CANCEL
    // =========================================================================
    @Test
    @DisplayName("TC_REC_05: Cửa sổ bầu cử không có Leader -> backoff không flood socket, tuyệt đối không gửi BOOK/CANCEL")
    void testTC_REC_05_NoLeaderAvailable_BacksOffWithoutSpam() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            // Giả lập cửa sổ bầu cử: tất cả các node đều trả NOT_LEADER với hint null
            leaderProvider.setElectionWindow();

            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 200, 800);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            BookingViewModel bookingVm = new BookingViewModel(new JournalingBookingClient(leaderClient, journal));
            ReconnectController controller = new ReconnectController(leaderClient, bookingVm, connVm, journal);
            controller.setDataSource(DataSource.MOCK);

            // Đếm số request nhận được trên tất cả các node trước khi chạy
            int initialRequests = leaderProvider.getAllNodes().stream()
                    .mapToInt(n -> n.getReceivedRequests().size())
                    .sum();

            // Chạy dò tìm Leader
            String result = controller.triggerReconnect().get(5, TimeUnit.SECONDS);
            assertNull(result, "Không có Leader nào khả dụng");
            assertEquals(ReconnectState.NO_LEADER, controller.getState(), "Trạng thái controller phải là NO_LEADER");
            assertTrue(controller.isBannerVisible(), "Banner phải hiển thị cảnh báo không có Leader");
            assertTrue(controller.getBannerMessage().contains("Không tìm thấy Leader"),
                    "Thông điệp cảnh báo phải chuẩn: " + controller.getBannerMessage());

            // Kiểm tra các yêu cầu gửi tới các node trong quá trình dò tìm
            for (FakeNodeServer node : leaderProvider.getAllNodes()) {
                for (MessageEnvelope<JsonNode> env : node.getReceivedRequests()) {
                    assertNotEquals(MessageType.BOOK_TICKET, env.getType(),
                            "Dò tìm Leader TUYỆT ĐỐI KHÔNG được gửi lệnh BOOK_TICKET!");
                    assertNotEquals(MessageType.CANCEL_TICKET, env.getType(),
                            "Dò tìm Leader TUYỆT ĐỐI KHÔNG được gửi lệnh CANCEL_TICKET!");
                    assertTrue(env.getType() == MessageType.GET_SEATS || env.getType() == MessageType.NODE_STATUS,
                            "Chỉ được phép gửi bản tin read-only (GET_SEATS/NODE_STATUS) để thăm dò!");
                }
            }

            int finalRequests = leaderProvider.getAllNodes().stream()
                    .mapToInt(n -> n.getReceivedRequests().size())
                    .sum();
            int diff = finalRequests - initialRequests;
            assertTrue(diff <= 15, "Số lần thăm dò phải bị chặn trên theo backoff, không spam socket (nhận " + diff + " requests)");
        });
    }

    // =========================================================================
    // TC_REC_06: Chặn tạo booking MỚI khi đang tìm Leader; cho phép pending cũ
    // =========================================================================
    @Test
    @DisplayName("TC_REC_06: Chặn tạo booking mới khi đang tìm Leader, vẫn cho phép tra cứu/gửi lại pending request")
    void testTC_REC_06_NoNewBookingWhileSearching() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 500, 1500);
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, null);
            JournalingBookingClient journalingClient = new JournalingBookingClient(leaderClient, journal);
            resourcesToClose.add(journalingClient);
            BookingViewModel bookingVm = new BookingViewModel(journalingClient);
            ReconnectController controller = new ReconnectController(leaderClient, bookingVm, null, journal);

            // Chuyển controller sang trạng thái SEARCHING_LEADER
            controller.onSearchingLeader();
            assertEquals(ReconnectState.SEARCHING_LEADER, controller.getState());

            // 1. Kiểm tra với requestId MỚI chưa có trong Journal -> bị chặn
            String newReqId = UUID.randomUUID().toString();
            assertFalse(controller.isRequestAllowed(newReqId), "Yêu cầu mới phải bị chặn khi đang tìm Leader");

            BookTicketRequestDto newReq = new BookTicketRequestDto(newReqId, "SE-001", "G05", "Nguyễn Văn Mới");
            CompletableFuture<BookTicketResponseDto> submitFuture = bookingVm.submit(newReq);
            java.util.concurrent.ExecutionException ex = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> submitFuture.get(1, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof IllegalStateException, "Phải ném IllegalStateException khi bị chặn");
            assertTrue(ex.getCause().getMessage().contains("Không thể tạo yêu cầu đặt vé mới"),
                    "Thông báo lỗi phải chỉ rõ đang tìm Leader: " + ex.getCause().getMessage());

            // 2. Kiểm tra với requestId ĐÃ CÓ trong Journal (pending request cũ) -> ĐƯỢC PHÉP
            String pendingReqId = UUID.randomUUID().toString();
            String payload = "{\"requestId\":\"" + pendingReqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G06\",\"passengerName\":\"Cũ\"}";
            journal.recordPrepared(new PendingEntry(pendingReqId, "BOOK_TICKET", payload,
                    PendingEntry.computePayloadHash(payload), PendingState.SENT_UNRESOLVED, Instant.now(), null, 1, "node-1", null, 1));

            assertTrue(controller.isRequestAllowed(pendingReqId),
                    "Request đã có trong Journal phải được phép tiếp tục để phục vụ tra cứu / retry");
        });
    }

    // =========================================================================
    // TC_REC_07: Phân biệt rõ lỗi TRƯỚC GỬI (AVAILABLE) và SAU GỬI (UNKNOWN)
    // =========================================================================
    @Test
    @DisplayName("TC_REC_07: Lỗi trước gửi -> FAILED_NOT_SENT, ghế AVAILABLE; lỗi sau gửi -> TIMEOUT_UNKNOWN, ghế UNKNOWN")
    void testTC_REC_07_FailedNotSentVsUnknown() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            // 1. Giả lập lỗi trước gửi: FailedNotSentException (Connect refused trước khi gửi byte)
            String reqBefore = UUID.randomUUID().toString();
            BookTicketRequestDto reqDtoBefore = new BookTicketRequestDto(reqBefore, "SE-001", "G07", "Test Before");

            ObservableList<SeatViewModel> seatsBefore = FXCollections.observableArrayList(
                    new SeatViewModel("G07", SeatViewModel.SeatStatus.AVAILABLE)
            );
            BookingViewModel vmBefore = new BookingViewModel(new BookingClient() {
                @Override
                public CompletableFuture<GetSeatsResponseDto> getSeats(vn.nhom6.railway.protocol.dto.GetSeatsRequestDto request) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
                    CompletableFuture<BookTicketResponseDto> f = new CompletableFuture<>();
                    f.completeExceptionally(new vn.nhom6.railway.client.network.FailedNotSentException("Connect refused trước khi gửi"));
                    return f;
                }

                @Override
                public CompletableFuture<vn.nhom6.railway.client.network.CancelTicketResponseDto> cancel(vn.nhom6.railway.client.network.CancelTicketRequestDto request) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
                    return CompletableFuture.completedFuture(null);
                }
            });
            vmBefore.setSeatsList(seatsBefore);
            vmBefore.setJournal(journal);

            CompletableFuture<BookTicketResponseDto> fBefore = vmBefore.submit(reqDtoBefore);
            try {
                fBefore.get(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }

            assertEquals(UiState.DISCONNECTED, vmBefore.getUiState(), "UiState phải là DISCONNECTED khi chưa gửi được byte");
            assertEquals("FAILED_NOT_SENT", vmBefore.getErrorCode(), "Mã lỗi phải là FAILED_NOT_SENT");
            assertEquals(SeatViewModel.SeatStatus.AVAILABLE, seatsBefore.get(0).getStatus(),
                    "Ghế phải giữ nguyên AVAILABLE khi lỗi trước gửi");

            // 2. Giả lập lỗi sau gửi: Server nhận request nhưng timeout sau gửi
            String reqAfter = UUID.randomUUID().toString();
            BookTicketRequestDto reqDtoAfter = new BookTicketRequestDto(reqAfter, "SE-001", "G08", "Test After");

            ObservableList<SeatViewModel> seatsAfter = FXCollections.observableArrayList(
                    new SeatViewModel("G08", SeatViewModel.SeatStatus.AVAILABLE)
            );
            BookingViewModel vmAfter = new BookingViewModel(new BookingClient() {
                @Override
                public CompletableFuture<GetSeatsResponseDto> getSeats(vn.nhom6.railway.protocol.dto.GetSeatsRequestDto request) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
                    return CompletableFuture.completedFuture(
                            new BookTicketResponseDto(
                                    request.getRequestId(),
                                    "UNKNOWN",
                                    null,
                                    request.getTripId(),
                                    request.getSeatId(),
                                    "TIMEOUT_UNKNOWN",
                                    null,
                                    false,
                                    "node-1",
                                    "Hết thời gian chờ phản hồi"
                            )
                    );
                }

                @Override
                public CompletableFuture<vn.nhom6.railway.client.network.CancelTicketResponseDto> cancel(vn.nhom6.railway.client.network.CancelTicketRequestDto request) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
                    return CompletableFuture.completedFuture(null);
                }
            });
            vmAfter.setSeatsList(seatsAfter);
            vmAfter.setJournal(journal);

            CompletableFuture<BookTicketResponseDto> fAfter = vmAfter.submit(reqDtoAfter);
            fAfter.get(2, TimeUnit.SECONDS);

            assertEquals(UiState.UNKNOWN, vmAfter.getUiState(), "UiState phải là UNKNOWN khi lỗi sau gửi");
            assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsAfter.get(0).getStatus(),
                    "Ghế phải chuyển sang UNKNOWN (viền cam) khi lỗi sau gửi");
            assertEquals("TIMEOUT_UNKNOWN", vmAfter.getErrorCode(), "Mã lỗi phải là TIMEOUT_UNKNOWN");
        });
    }

    // =========================================================================
    // TC_REC_08: Restart ứng dụng trong lúc failover -> phục hồi pending 100%
    // =========================================================================
    @Test
    @DisplayName("TC_REC_08: Khởi động lại Client giữa chừng failover -> nạp lại 100% pending request từ đĩa")
    void testTC_REC_08_RestartDuringFailover() throws Exception {
        String requestId = UUID.randomUUID().toString();
        String payload = "{\"requestId\":\"" + requestId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G09\",\"passengerName\":\"Đỗ Văn Restart\"}";
        String payloadHash = PendingEntry.computePayloadHash(payload);

        // Ghi vào journal hiện tại
        journal.recordPrepared(new PendingEntry(requestId, "BOOK_TICKET", payload, payloadHash,
                PendingState.PREPARED, Instant.now(), null, 1, "node-1", null, 1));
        journal.markSent(requestId);

        // Giả lập đóng ứng dụng (close journal)
        journal.close();

        // Giả lập mở lại ứng dụng: Khởi tạo journal mới từ cùng thư mục đĩa
        try (PersistentPendingJournal newJournal = new PersistentPendingJournal(tempJournalDir)) {
            Optional<PendingEntry> recoveredOpt = newJournal.get(requestId);
            assertTrue(recoveredOpt.isPresent(), "Pending request phải được phục hồi từ đĩa");

            PendingEntry recovered = recoveredOpt.get();
            assertEquals(requestId, recovered.getRequestId());
            assertEquals(PendingState.SENT_UNRESOLVED, recovered.getState());
            assertEquals(payload, recovered.getPayload());
            assertEquals(payloadHash, recovered.getPayloadHash());

            // ReconnectController khởi tạo với journal mới phải nhận diện đúng
            ReconnectController newController = new ReconnectController(null, null, null, newJournal);
            newController.onSearchingLeader();
            assertTrue(newController.isRequestAllowed(requestId), "Request phục hồi từ đĩa phải được phép tra cứu");
            assertFalse(newController.isRequestAllowed("random-new-id"), "Request lạ mới sinh vẫn bị chặn");
        }
    }

    // =========================================================================
    // TC_REC_09: Hint từ Node lạ/ngoài danh sách bị bỏ qua, không mặc định Node 1 là Leader
    // =========================================================================
    @Test
    @DisplayName("TC_REC_09: leaderHint không có trong ClusterEndpoints bị bỏ qua; hiển thị đúng nhãn FAKE")
    void testTC_REC_09_HintFromNodeIsOnlyAHint() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            ClusterEndpoints endpoints = leaderProvider.getClusterEndpoints();
            RedirectPolicy policy = new RedirectPolicy(2, 500, 1500);
            ClusterConnectionViewModel connVm = new ClusterConnectionViewModel();
            LeaderAwareBookingClient leaderClient = createLeaderClient(endpoints, policy, connVm);
            ReconnectController controller = new ReconnectController(leaderClient, null, connVm, journal);
            controller.setDataSource(DataSource.MOCK);

            // Node-1 trả về NOT_LEADER với hint lạ "malicious-node-999" không có trong cụm
            FakeNodeServer node1 = leaderProvider.getNode("node-1");
            node1.setNotLeader("malicious-node-999", 5L);

            BookTicketRequestDto req = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G10", "Khách Test 9");
            BookTicketResponseDto res = leaderClient.book(req).join();

            // Client không crash, không chuyển hướng sang node lạ ngoài cấu hình
            assertNotNull(res);
            assertFalse(res.isConfirmed());
            assertEquals(DataSource.MOCK, controller.getDataSource(), "Nguồn dữ liệu phải hiển thị đúng MOCK");
            assertEquals("MOCK", controller.getDataSource().getLabel());
        });
    }

    // =========================================================================
    // TC_REC_10: Kiểm thử Docker thật kill Leader (Guard bằng assumeTrue, mặc định SKIPPED chờ G6)
    // =========================================================================
    @Test
    @DisplayName("TC_REC_10: Kill Leader trên Docker thật — guard bằng assumeTrue, SKIPPED / NOT_RUN chờ nghiệm thu G6")
    void testTC_REC_10_RealDockerKill_GuardedByEnvironment() {
        Assumptions.assumeTrue(
                "true".equalsIgnoreCase(System.getProperty("docker.real.test")),
                "Kịch bản Docker real kill Leader chỉ chạy khi có cờ -Ddocker.real.test=true. Mặc định SKIPPED chờ nghiệm thu tại G6."
        );

        // Kịch bản này chỉ thực thi khi chạy tích hợp cụm Docker thật tại mốc G6
        // với lệnh: docker compose -f docker/compose.dev.yml stop railway-node-1
        fail("Nếu assumeTrue vượt qua thì cần kết nối Docker thật cổng 9101-9105");
    }
}
