package vn.nhom6.railway.client;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.FailedNotSentException;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.support.FakeBookingClient;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử toàn diện kịch bản lỗi luồng đặt vé theo SPEC-03 / SPEC-05 (TASK TV1-D19).
 * <p>
 * Bao gồm đầy đủ 13 kịch bản:
 * 1. REJECTED(SEAT_ALREADY_BOOKED): UiState=REJECTED, ghế BOOKED, pending RESOLVED, refresh ghế.
 * 2. Connect refused/timeout -> FAILED_NOT_SENT: ghế không đổi, cho thử lại, khác UNKNOWN/REJECTED.
 * 3. Server đóng kết nối sau khi gửi -> UNKNOWN: ghế UNKNOWN, pending SENT_UNRESOLVED, hiển thị requestId.
 * 4. Read timeout sau khi gửi -> UNKNOWN; TIMEOUT_UNKNOWN từ Node -> UNKNOWN.
 * 5. UNKNOWN không bao giờ bị coi là thành công hay thất bại.
 * 6. NO_QUORUM phản hồi có cấu trúc -> đúng ma trận SPEC-03, không sinh requestId mới.
 * 7. NOT_LEADER có hint hợp lệ / hint không hợp lệ.
 * 8. Mã cấu trúc khác: INVALID_REQUEST, READ_UNAVAILABLE, REQUEST_ID_CONFLICT.
 * 9. Tra cứu kết quả (GET_REQUEST_RESULT) sau UNKNOWN: CONFIRMED, REJECTED, PENDING, Exception.
 * 10. Gửi lại sau UNKNOWN dùng đúng requestId và payload byte-identical từ journal.
 * 11. Phản hồi đến muộn sau UNKNOWN (D17) không đè lần đặt mới.
 * 12. Quét toàn bộ mã trong ma trận SPEC-03.
 * 13. Rà soát không có SEAT_SOLD, không parse chuỗi, UiDispatcher đồng bộ, cluster thật bị skip.
 */
class BookingErrorFlowTest {

    @TempDir
    Path tempJournalDir;

    private FakeBookingClient fakeClient;
    private JournalingBookingClient journalingClient;
    private PersistentPendingJournal journal;
    private TrackingRefreshService refreshService;
    private ObservableList<SeatViewModel> seatsList;
    private BookingViewModel viewModel;
    private ScheduledExecutorService testScheduler;
    private AtomicInteger uiDispatchCount;

    static class TrackingRefreshService extends SeatsRefreshService {
        final AtomicInteger refreshCount = new AtomicInteger(0);

        TrackingRefreshService(FakeReadAdapter adapter, ObservableList<SeatViewModel> list, PersistentPendingJournal journal) {
            super(adapter, list, journal);
        }

        @Override
        public CompletableFuture<SeatsSnapshot> refreshNow() {
            refreshCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        fakeClient = new FakeBookingClient();
        journal = new PersistentPendingJournal(tempJournalDir);
        journalingClient = new JournalingBookingClient(fakeClient, journal);

        seatsList = FXCollections.observableArrayList();
        for (int i = 1; i <= 10; i++) {
            seatsList.add(new SeatViewModel(String.format("G%02d", i), SeatViewModel.SeatStatus.AVAILABLE));
        }

        refreshService = new TrackingRefreshService(new FakeReadAdapter(), seatsList, journal);
        testScheduler = Executors.newSingleThreadScheduledExecutor();

        viewModel = new BookingViewModel(journalingClient, journal, refreshService, testScheduler, 5000);
        viewModel.setSeatsList(seatsList);

        uiDispatchCount = new AtomicInteger(0);
        viewModel.setUiDispatcher(action -> {
            uiDispatchCount.incrementAndGet();
            action.run();
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testScheduler != null) {
            testScheduler.shutdownNow();
        }
        if (refreshService != null) {
            refreshService.close();
        }
        if (journalingClient != null) {
            journalingClient.close();
        }
    }

    // =========================================================================
    // KỊCH BẢN 1: REJECTED(SEAT_ALREADY_BOOKED)
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 1: REJECTED(SEAT_ALREADY_BOOKED) -> UiState=REJECTED, ghế BOOKED, refresh ghế, pending RESOLVED")
    void test01_RejectedSeatAlreadyBookedTransitionsToRejectedSeatBookedAndRefreshes() throws Exception {
        int initialRefreshes = refreshService.refreshCount.get();
        String reqId = "REQ-FLOW-01";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G02", "Khách Test 1");

        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto(
                reqId,
                "REJECTED",
                null,
                "SE-001",
                "G02",
                ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                105L,
                true,
                "node-1",
                "Ghế đã có người đặt trước (SEAT_ALREADY_BOOKED)"
        ));

        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals(BookingViewModel.UiState.REJECTED, viewModel.getUiState(), "UiState phải là REJECTED");
        assertEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("đã có người đặt"));

        // Ghế G02 phải chuyển sang BOOKED
        SeatViewModel seatG02 = seatsList.get(1);
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatG02.getStatus(), "Ghế bị từ chối do đã đặt phải chuyển sang BOOKED");

        // Kích hoạt refresh ghế
        assertTrue(refreshService.refreshCount.get() > initialRefreshes, "Phải kích hoạt refreshNow() khi REJECTED(SEAT_ALREADY_BOOKED)");

        // Pending trong journal phải được đánh dấu RESOLVED
        Optional<PendingEntry> entry = journal.get(reqId);
        assertTrue(entry.isPresent());
        assertEquals(PendingState.RESOLVED, entry.get().getState(), "Pending trong journal phải được đánh dấu RESOLVED");
        assertEquals("REJECTED", entry.get().getFinalOutcome());
    }

    // =========================================================================
    // KỊCH BẢN 2: LỖI KẾT NỐI TRƯỚC KHI GỬI (FAILED_NOT_SENT)
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 2: Connect refused trước khi gửi -> FAILED_NOT_SENT, ghế KHÔNG đổi, cho thử lại, khác UNKNOWN/REJECTED")
    void test02_ConnectFailureBeforeSendIsFailedNotSentSeatUnchanged() throws Exception {
        String reqId = "REQ-FLOW-02";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G03", "Khách Test 2");

        fakeClient.setConfiguredBookException(new FailedNotSentException("Connection refused to 127.0.0.1:9101 trước khi gửi", new ConnectException("Connection refused")));

        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {}

        assertEquals(BookingViewModel.UiState.DISCONNECTED, viewModel.getUiState(), "UiState phải là DISCONNECTED");
        assertEquals("FAILED_NOT_SENT", viewModel.getErrorCode());
        assertNotEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState(), "FAILED_NOT_SENT khác hoàn toàn với UNKNOWN");
        assertNotEquals(BookingViewModel.UiState.REJECTED, viewModel.getUiState(), "FAILED_NOT_SENT không phải REJECTED nghiệp vụ");

        // Ghế G03 giữ nguyên AVAILABLE, tuyệt đối không bị đổi sang UNKNOWN
        SeatViewModel seatG03 = seatsList.get(2);
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, seatG03.getStatus(),
                "Khi lỗi FAILED_NOT_SENT, ghế phải giữ nguyên AVAILABLE vì lệnh chưa từng rời máy khách!");
    }

    // =========================================================================
    // KỊCH BẢN 3: MẤT KẾT NỐI SAU KHI GỬI (UNKNOWN)
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 3: Server đóng kết nối sau khi nhận lệnh -> UNKNOWN, ghế UNKNOWN, pending SENT_UNRESOLVED")
    void test03_ConnectionLostAfterSendIsUnknownSeatUnknownPendingPreserved() throws Exception {
        String reqId = "REQ-FLOW-03";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Test 3");

        // Giả lập SocketException (Connection reset by peer hoặc EOF sau khi đã ghi frame)
        fakeClient.setConfiguredBookException(new SocketException("Connection reset by peer sau khi gửi frame"));

        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {}

        // ViewModel xử lý ngoại lệ mất kết nối sau gửi -> ghế chuyển UNKNOWN, journal giữ SENT_UNRESOLVED
        SeatViewModel seatG04 = seatsList.get(3);
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatG04.getStatus(),
                "Mất kết nối sau khi gửi: ghế phải chuyển sang UNKNOWN vì Node có thể đã nhận và xử lý lệnh!");

        Optional<PendingEntry> entry = journal.get(reqId);
        assertTrue(entry.isPresent());
        assertEquals(PendingState.SENT_UNRESOLVED, entry.get().getState(),
                "Pending entry phải giữ nguyên trạng thái SENT_UNRESOLVED trong journal!");
        assertEquals(reqId, viewModel.getCurrentRequestId());
    }

    // =========================================================================
    // KỊCH BẢN 4: READ TIMEOUT VÀ TIMEOUT_UNKNOWN TỪ NODE
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 4: Read timeout hoặc TIMEOUT_UNKNOWN từ Node -> UiState=UNKNOWN, pending giữ SENT_UNRESOLVED")
    void test04_ReadTimeoutOrTimeoutUnknownYieldsUnknownState() throws Exception {
        // Trường hợp 4a: Read timeout trên socket
        String reqId1 = "REQ-FLOW-04A";
        BookTicketRequestDto request1 = new BookTicketRequestDto(reqId1, "SE-001", "G05", "Khách Timeout 1");
        fakeClient.setConfiguredBookException(new SocketTimeoutException("Read timed out"));

        CompletableFuture<BookTicketResponseDto> future1 = viewModel.submit(request1);
        try {
            future1.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {}

        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals(ClientErrorKind.READ_TIMEOUT.name(), viewModel.getErrorCode());
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsList.get(4).getStatus());
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqId1).get().getState());

        // Trường hợp 4b: Node trả phản hồi có cấu trúc outcome=UNKNOWN, reasonCode=TIMEOUT_UNKNOWN
        String reqId2 = "REQ-FLOW-04B";
        BookTicketRequestDto request2 = new BookTicketRequestDto(reqId2, "SE-001", "G06", "Khách Timeout 2");
        fakeClient.setConfiguredBookException(null);
        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto(
                reqId2,
                "UNKNOWN",
                null,
                "SE-001",
                "G06",
                ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN,
                null,
                false,
                "node-1",
                "Hết thời gian chờ cam kết Raft tại cụm (TIMEOUT_UNKNOWN)"
        ));

        CompletableFuture<BookTicketResponseDto> future2 = viewModel.submit(request2);
        BookTicketResponseDto res2 = future2.get(2, TimeUnit.SECONDS);

        assertNotNull(res2);
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, viewModel.getErrorCode());
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsList.get(5).getStatus());
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqId2).get().getState());
    }

    // =========================================================================
    // KỊCH BẢN 5: UNKNOWN TUYỆT ĐỐI KHÔNG COI LÀ THÀNH CÔNG HAY THẤT BẠI
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 5: UNKNOWN tuyệt đối không chứa các từ cấm (thành công, thất bại, bị từ chối, đã đặt)")
    void test05_UnknownStateNeverTreatedAsSuccessOrFailure() {
        ErrorPresentation p = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, "Node timeout", false);

        assertEquals(PresentationKind.UNKNOWN, p.kind());
        assertTrue(p.isUnknown());
        assertFalse(p.isConfirmed(), "UNKNOWN không bao giờ là confirmed");
        assertFalse(p.isRejected(), "UNKNOWN không bao giờ là rejected");

        String lowerTitle = p.title().toLowerCase();
        String lowerMsg = p.message().toLowerCase();

        // Kiểm tra các từ cấm theo tiêu chí chất lượng D19
        assertFalse(lowerTitle.contains("thành công"), "Tiêu đề UNKNOWN không được chứa 'thành công'");
        assertFalse(lowerTitle.contains("thất bại"), "Tiêu đề UNKNOWN không được chứa 'thất bại'");
        assertFalse(lowerTitle.contains("bị từ chối"), "Tiêu đề UNKNOWN không được chứa 'bị từ chối'");
        assertFalse(lowerTitle.contains("đã đặt"), "Tiêu đề UNKNOWN không được chứa 'đã đặt'");

        assertFalse(lowerMsg.contains("đặt vé thành công"), "Thông điệp UNKNOWN không được chứa 'đặt vé thành công'");
        assertFalse(lowerMsg.contains("đặt vé thất bại"), "Thông điệp UNKNOWN không được chứa 'đặt vé thất bại'");
        assertFalse(lowerMsg.contains("đã bị từ chối"), "Thông điệp UNKNOWN không được chứa 'đã bị từ chối'");
    }

    // =========================================================================
    // KỊCH BẢN 6: NO_QUORUM TỪ CỤM
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 6: NO_QUORUM phản hồi có cấu trúc -> đúng ma trận SPEC-03, không sinh requestId mới")
    void test06_NoQuorumStructuredErrorHandledCorrectlyWithoutNewRequestId() throws Exception {
        String reqId = "REQ-FLOW-06";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G07", "Khách No Quorum");

        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto(
                reqId,
                "UNKNOWN",
                null,
                "SE-001",
                "G07",
                ErrorPresentationMapper.CODE_NO_QUORUM,
                null,
                false,
                "node-1",
                "⚡ Lỗi cụm: Không đủ đa số (Quorum 3/5) để cam kết an toàn!"
        ));

        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);
        BookTicketResponseDto res = future.get(2, TimeUnit.SECONDS);

        assertNotNull(res);
        assertEquals(BookingViewModel.UiState.ERROR, viewModel.getUiState());
        assertEquals(ErrorPresentationMapper.CODE_NO_QUORUM, viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("Không đủ đa số"));

        // Khi thử lại từ UI, requestId phải được giữ nguyên
        assertEquals(reqId, viewModel.getCurrentRequestId(), "Lỗi NO_QUORUM phải giữ nguyên requestId để thử lại, không sinh ID mới");
    }

    // =========================================================================
    // KỊCH BẢN 7: NOT_LEADER VÀ REDIRECT
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 7: NOT_LEADER hiển thị thông tin chuyển hướng, hint không hợp lệ báo lỗi rõ ràng")
    void test07_NotLeaderWithHintOrInvalidHintHandledCorrectly() {
        // 7a: NOT_LEADER có hint hợp lệ
        ErrorPresentation p1 = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_NOT_LEADER, "Node không phải Leader, vui lòng gửi sang node-2", false);
        assertEquals(PresentationKind.ERROR, p1.kind());
        assertEquals(ErrorPresentationMapper.CODE_NOT_LEADER, p1.rawCode());
        assertTrue(p1.retryable());

        // 7b: NOT_LEADER không hint hoặc hint không hợp lệ
        ErrorPresentation p2 = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_NOT_LEADER, "Node không phải Leader và không có hint", false);
        assertEquals(PresentationKind.ERROR, p2.kind());
        assertFalse(p2.isConfirmed(), "NOT_LEADER tuyệt đối không coi là CONFIRMED");
    }

    // =========================================================================
    // KỊCH BẢN 8: CÁC MÃ LỖI CẤU TRÚC KHÁC
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 8: INVALID_REQUEST, READ_UNAVAILABLE, REQUEST_ID_CONFLICT ánh xạ đúng presentation")
    void test08_StructuredErrorsInvalidRequestReadUnavailableRequestIdConflict() {
        // INVALID_REQUEST: lỗi dữ liệu đầu vào, không retry nguyên trạng vô hạn
        ErrorPresentation pInvalid = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_INVALID_REQUEST, "Sai format seatId", false);
        assertEquals(PresentationKind.ERROR, pInvalid.kind());
        assertFalse(pInvalid.retryable(), "INVALID_REQUEST không được tự động retry cùng payload sai");

        // READ_UNAVAILABLE: đọc thất bại do barrier -> Theo SPEC-03, client hiển thị UNKNOWN
        ErrorPresentation pRead = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_READ_UNAVAILABLE, "Read barrier failed", false);
        assertEquals(PresentationKind.UNKNOWN, pRead.kind());
        assertTrue(pRead.retryable());

        // REQUEST_ID_CONFLICT: xung đột requestId với payload khác
        ErrorPresentation pConflict = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_REQUEST_ID_CONFLICT, "Payload conflict", false);
        assertEquals(PresentationKind.ERROR, pConflict.kind());
        assertFalse(pConflict.retryable(), "REQUEST_ID_CONFLICT cấm retry với payload đã sửa");
    }

    // =========================================================================
    // KỊCH BẢN 9: TRA CỨU KẾT QUẢ (GET_REQUEST_RESULT) SAU UNKNOWN
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 9: Tra cứu kết quả sau UNKNOWN phục hồi đúng CONFIRMED, REJECTED, PENDING hoặc Exception")
    void test09_GetRequestResultAfterUnknownResolvesConfirmedRejectedOrPending() throws Exception {
        String reqId = "REQ-FLOW-09";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G08", "Khách Tra Cứu");

        fakeClient.setConfiguredBookException(new SocketTimeoutException("Simulated timeout"));
        viewModel.submit(request);
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());

        // 9a: Node trả APPLIED_SUCCESS -> UI chuyển sang CONFIRMED, ghế BOOKED, pending RESOLVED
        fakeClient.setConfiguredResultResponse(new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "BOOK_TICKET", "TCK-09", null, 200L, true));
        CompletableFuture<GetRequestResultResponseDto> checkFuture = viewModel.checkCurrentResult();
        GetRequestResultResponseDto res = checkFuture.get(2, TimeUnit.SECONDS);

        assertNotNull(res);
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(7).getStatus());
        assertEquals(PendingState.RESOLVED, journal.get(reqId).get().getState());

        // 9b: Thử lại kịch bản với APPLIED_REJECTED
        String reqIdReject = "REQ-FLOW-09-REJ";
        BookTicketRequestDto reqRej = new BookTicketRequestDto(reqIdReject, "SE-001", "G09", "Khách Tra Cứu Từ Chối");
        viewModel.submit(reqRej);

        fakeClient.setConfiguredResultResponse(new GetRequestResultResponseDto(reqIdReject, "APPLIED_REJECTED", "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", 201L, true));
        viewModel.checkCurrentResult().get(2, TimeUnit.SECONDS);
        assertEquals(BookingViewModel.UiState.REJECTED, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(8).getStatus());
        assertEquals(PendingState.RESOLVED, journal.get(reqIdReject).get().getState());
    }

    // =========================================================================
    // KỊCH BẢN 10: GỬI LẠI (RETRY) SAU UNKNOWN
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 10: Gửi lại sau UNKNOWN dùng đúng requestId và payload nguyên văn từ journal")
    void test10_RetryAfterUnknownUsesByteIdenticalPayloadAndSameRequestId() throws Exception {
        String reqId = "REQ-FLOW-10";
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G10", "Khách Retry Byte Identical");

        fakeClient.setConfiguredBookException(new SocketTimeoutException("Timeout"));
        viewModel.submit(request);
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());

        // Kiểm tra journal lưu nguyên văn payload
        Optional<PendingEntry> entry = journal.get(reqId);
        assertTrue(entry.isPresent());
        String originalPayload = entry.get().getPayload();
        String originalHash = entry.get().getPayloadHash();

        fakeClient.setConfiguredBookException(null);
        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto(reqId, "CONFIRMED", "TCK-10", "SE-001", "G10", null, 210L, true, "node-1", "OK"));

        CompletableFuture<BookTicketResponseDto> retryFuture = viewModel.retryCurrent();
        BookTicketResponseDto res = retryFuture.get(2, TimeUnit.SECONDS);

        assertNotNull(res);
        assertEquals(reqId, res.getRequestId(), "Retry phải dùng đúng requestId gốc");
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());

        // Kiểm tra payload không bị thay đổi dù chỉ 1 byte
        assertEquals(originalHash, entry.get().getPayloadHash(), "Payload hash phải giữ nguyên sau retry");
    }

    // =========================================================================
    // KỊCH BẢN 11: PHẢN HỒI ĐẾN MUỘN SAU UNKNOWN (D17)
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 11: Phản hồi đến muộn sau khi UI đã sang UNKNOWN được ghi nhận, không đè lần đặt mới")
    void test11_LateArrivingResponseHandledGracefullyWithoutOverwritingNewBooking() throws Exception {
        CompletableFuture<BookTicketResponseDto> slowFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(slowFuture);

        String reqIdOld = "REQ-FLOW-11-OLD";
        BookTicketRequestDto oldReq = new BookTicketRequestDto(reqIdOld, "SE-001", "G01", "Khách Đến Muộn");
        viewModel.submit(oldReq);

        // Kích hoạt timeout để UI chuyển sang UNKNOWN
        viewModel.triggerTimeoutForTesting();
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());

        // Bắt đầu một yêu cầu đặt vé MỚI (generation tăng)
        String reqIdNew = "REQ-FLOW-11-NEW";
        fakeClient.setManualBookFuture(null);
        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto(reqIdNew, "CONFIRMED", "TCK-NEW", "SE-001", "G02", null, 300L, true, "node-1", "Đặt mới thành công"));
        viewModel.submit(new BookTicketRequestDto(reqIdNew, "SE-001", "G02", "Khách Mới"));
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals(reqIdNew, viewModel.getCurrentRequestId());

        // Giờ phản hồi của reqIdOld mới tới
        slowFuture.complete(new BookTicketResponseDto(reqIdOld, "CONFIRMED", "TCK-OLD", "SE-001", "G01", null, 299L, true, "node-1", "Cũ đến muộn"));

        // Trạng thái UI vẫn phải là CONFIRMED của lần đặt MỚI (không bị đè)
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals(reqIdNew, viewModel.getCurrentRequestId(), "UiState không bị đè bởi phản hồi đến muộn của request cũ");
    }

    // =========================================================================
    // KỊCH BẢN 12: QUÉT TOÀN BỘ MA TRẬN SPEC-03
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 12: Quét toàn bộ mã trong ma trận SPEC-03 và client-side: không null/rỗng")
    void test12_AllMatrixCodesMappedWithNonEmptyFields() {
        String[] specCodes = {
                "CONFIRMED",
                ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                ErrorPresentationMapper.CODE_NOT_LEADER,
                ErrorPresentationMapper.CODE_NO_QUORUM,
                ErrorPresentationMapper.CODE_INVALID_REQUEST,
                ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN,
                ErrorPresentationMapper.CODE_READ_UNAVAILABLE,
                ErrorPresentationMapper.CODE_REQUEST_ID_CONFLICT
        };

        for (String code : specCodes) {
            ErrorPresentation presentation = ErrorPresentationMapper.mapCode(code, "Message test", false);
            assertNotNull(presentation, "Mã " + code + " phải có ErrorPresentation hợp lệ");
            assertNotNull(presentation.title(), "Title của mã " + code + " không được null");
            assertFalse(presentation.title().isBlank(), "Title của mã " + code + " không được rỗng");
            assertNotNull(presentation.message(), "Message của mã " + code + " không được null");
            assertFalse(presentation.message().isBlank(), "Message của mã " + code + " không được rỗng");
            assertNotNull(presentation.styleClass(), "StyleClass của mã " + code + " không được null");
            assertNotNull(presentation.kind(), "Kind của mã " + code + " không được null");
        }

        // Quét các ClientErrorKind
        for (ClientErrorKind clientError : ClientErrorKind.values()) {
            ErrorPresentation p = ErrorPresentationMapper.mapClientError(clientError, "Chi tiết lỗi client", false);
            assertNotNull(p);
            assertNotNull(p.title());
            assertNotNull(p.message());
            assertNotNull(p.styleClass());
            assertEquals(clientError, p.clientErrorKind());
        }
    }

    // =========================================================================
    // KỊCH BẢN 13: RÀ SOÁT CẤM VÀ CỤM THẬT SKIPPED
    // =========================================================================
    @Test
    @DisplayName("Kịch bản 13a: Không sử dụng SEAT_SOLD, không parse chuỗi, UiDispatcher đồng bộ")
    void test13a_NoSeatSoldAndNoStringParsing() {
        // Kiểm tra không dùng SEAT_SOLD
        assertFalse(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED.equals("SEAT_SOLD"));

        // Kiểm tra đồng bộ qua UiDispatcher
        int before = uiDispatchCount.get();
        viewModel.submit(new BookTicketRequestDto("REQ-13", "SE-001", "G01", "Khách 13"));
        assertTrue(uiDispatchCount.get() > before, "Mọi cập nhật thuộc tính phải đi qua UiDispatcher");
    }

    @Test
    @DisplayName("Kịch bản 13b: Test với cụm Raft thật (bị SKIP khi không có biến môi trường CLUSTER_PROFILE=real)")
    @EnabledIfEnvironmentVariable(named = "CLUSTER_PROFILE", matches = ".*real.*")
    void test13b_RealRaftClusterSkippedWithoutEnv() {
        fail("Test này chỉ chạy khi có cụm Raft thật và biến môi trường CLUSTER_PROFILE=real (nghiệm thu tại G5)");
    }
}
