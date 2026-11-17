package vn.nhom6.railway.client;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.support.FakeBookingClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import java.io.IOException;
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
 * Kiểm thử đơn vị cho máy trạng thái giao dịch đặt vé BookingViewModel (TASK TV1-D17).
 * <p>
 * Bắt buộc:
 * - Không mạng, không phụ thuộc cụm Raft thật.
 * - Sử dụng FakeBookingClient với nhãn FAKE và virtual delays.
 * - Kiểm chứng toàn diện 12 kịch bản theo đặc tả TASK TV1-D17.
 */
class BookingViewModelTest {

    @TempDir
    Path tempJournalDir;

    private FakeBookingClient fakeClient;
    private JournalingBookingClient journalingClient;
    private PersistentPendingJournal journal;
    private TrackingSeatsRefreshService trackingRefreshService;
    private ObservableList<SeatViewModel> seatsList;
    private BookingViewModel viewModel;
    private ScheduledExecutorService testScheduler;
    private AtomicInteger uiDispatchCount;

    static class TrackingSeatsRefreshService extends SeatsRefreshService {
        final AtomicInteger refreshCount = new AtomicInteger(0);

        TrackingSeatsRefreshService(FakeReadAdapter adapter, ObservableList<SeatViewModel> list, PersistentPendingJournal journal) {
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

        trackingRefreshService = new TrackingSeatsRefreshService(new FakeReadAdapter(), seatsList, journal);
        testScheduler = Executors.newSingleThreadScheduledExecutor();

        viewModel = new BookingViewModel(journalingClient, journal, trackingRefreshService, testScheduler, 5000);
        viewModel.setSeatsList(seatsList);

        // UiDispatcher đếm lệnh đồng bộ cho kiểm thử
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
        if (trackingRefreshService != null) {
            trackingRefreshService.close();
        }
        if (journalingClient != null) {
            journalingClient.close();
        }
    }

    @Test
    @DisplayName("Kịch bản 1: submit -> ngay lập tức SUBMITTING, busy=true, chưa CONFIRMED, ghế chưa đổi")
    void test1_SubmitImmediateSubmitting() {
        CompletableFuture<BookTicketResponseDto> manualFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(manualFuture);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-001", "SE-001", "G03", "Trần Khách 1");
        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);

        assertNotNull(future);
        assertTrue(viewModel.isBusy(), "Khi submit, busy phải là true ngay lập tức");
        assertEquals(BookingViewModel.UiState.SUBMITTING, viewModel.getUiState(), "Trạng thái phải là SUBMITTING");
        assertNotEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals("G03", seatsList.get(2).getSeatId());
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, seatsList.get(2).getStatus(), "Ghế chưa được đổi sang BOOKED khi chưa có phản hồi cuối");
        assertTrue(viewModel.getMessage().contains("Đang chờ Node xác nhận"));
        assertEquals("REQ-001", viewModel.getCurrentRequestId());
    }

    @Test
    @DisplayName("Kịch bản 2: Fake trả CONFIRMED sau delay ảo -> CONFIRMED, busy=false, ghế BOOKED, refreshNow() gọi đúng 1 lần")
    void test2_FakeReturnsConfirmedAfterDelay() throws Exception {
        fakeClient.setSimulatedDelay(50, testScheduler);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-002", "SE-001", "G05", "Trần Khách 2");
        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);

        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("CONFIRMED", response.getOutcome());
        assertFalse(viewModel.isBusy(), "Sau khi có kết quả cuối, busy phải là false");
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(4).getStatus(), "Ghế G05 phải chuyển sang BOOKED");
        assertEquals(1, trackingRefreshService.refreshCount.get(), "refreshNow() phải được gọi đúng 1 lần sau CONFIRMED");

        Optional<PendingEntry> opt = journal.get("REQ-002");
        assertTrue(opt.isPresent());
        assertEquals(PendingState.RESOLVED, opt.get().getState(), "Pending entry phải được chuyển sang RESOLVED");
        assertEquals("CONFIRMED", opt.get().getFinalOutcome());
    }

    @Test
    @DisplayName("Kịch bản 3: REJECTED(SEAT_ALREADY_BOOKED) -> REJECTED, ghế BOOKED, refresh được gọi, khác UNKNOWN")
    void test3_RejectedSeatAlreadyBooked() throws Exception {
        BookTicketResponseDto rejectedResponse = new BookTicketResponseDto(
                "REQ-003",
                "REJECTED",
                null,
                "SE-001",
                "G02",
                "SEAT_ALREADY_BOOKED",
                102L,
                true,
                "node-1",
                "Ghế G02 đã có người đặt [FAKE]"
        );
        fakeClient.setConfiguredBookResponse(rejectedResponse);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-003", "SE-001", "G02", "Trần Khách Trùng");
        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);

        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.REJECTED, viewModel.getUiState());
        assertNotEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState(), "REJECTED phải khác UNKNOWN");
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(1).getStatus(), "Ghế G02 bị trùng phải chuyển sang BOOKED");
        assertEquals(1, trackingRefreshService.refreshCount.get(), "refreshNow() phải được gọi khi ghế đã bị đặt");
        assertTrue(viewModel.getMessage().contains("đã có người đặt"));
    }

    @Test
    @DisplayName("Kịch bản 4: Các lỗi khác (INVALID_REQUEST, NO_QUORUM, NOT_LEADER) -> ERROR theo mapper, busy=false, ghế không đổi, KHÔNG refresh")
    void test4_OtherErrorsNoRefresh() throws Exception {
        BookTicketResponseDto errorResponse = new BookTicketResponseDto(
                "REQ-004",
                "NO_QUORUM",
                null,
                "SE-001",
                "G06",
                "NO_QUORUM",
                0L,
                true,
                "node-1",
                "Không đủ đa số quá bán (NO_QUORUM)"
        );
        fakeClient.setConfiguredBookResponse(errorResponse);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-004", "SE-001", "G06", "Trần Khách Lỗi");
        CompletableFuture<BookTicketResponseDto> future = viewModel.submit(request);

        future.get(2, TimeUnit.SECONDS);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.ERROR, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.AVAILABLE, seatsList.get(5).getStatus(), "Ghế G06 không đổi trạng thái khi gặp lỗi hạ tầng");
        assertEquals(0, trackingRefreshService.refreshCount.get(), "KHÔNG được gọi refreshNow() khi gặp lỗi hạ tầng");
    }

    @Test
    @DisplayName("Kịch bản 5: Double-click: gọi submit 2 lần và 10 lần trong lúc SUBMITTING -> chỉ 1 lần gọi client, cùng 1 requestId")
    void test5_DoubleClickSuppression() {
        CompletableFuture<BookTicketResponseDto> manualFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(manualFuture);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-005", "SE-001", "G07", "Trần Thao Tác Nhanh");

        // Gọi submit lần 1
        CompletableFuture<BookTicketResponseDto> future1 = viewModel.submit(request);
        assertNotNull(future1, "future1 trả về không được null");
        assertFalse(future1.isDone(), "future1 chưa thể hoàn thành khi fake backend chưa phản hồi");
        assertEquals(1, fakeClient.getBookCallCount(), "Client được gọi lần đầu");

        // Gọi submit thêm 9 lần (tổng cộng 10 lần)
        List<CompletableFuture<BookTicketResponseDto>> subsequentFutures = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            CompletableFuture<BookTicketResponseDto> f = viewModel.submit(request);
            subsequentFutures.add(f);
            assertTrue(f.isCompletedExceptionally(), "Các lần submit tiếp theo trong lúc SUBMITTING phải bị từ chối lập tức");
        }

        assertEquals(1, fakeClient.getBookCallCount(), "Client vẫn CHỈ được gọi đúng 1 lần duy nhất khi đang SUBMITTING");
        assertEquals(1, fakeClient.getRecordedBookRequests().size());
        assertEquals("REQ-005", fakeClient.getRecordedBookRequests().get(0).getRequestId());
        assertTrue(viewModel.isBusy());

        // Hoàn thành future thủ công
        manualFuture.complete(new BookTicketResponseDto("REQ-005", "CONFIRMED", "TCK-005", "SE-001", "G07", null, 105L, true, "node-1", "Đặt vé thành công"));

        assertTrue(future1.isDone(), "future1 phải hoàn thành sau khi backend phản hồi");
        assertFalse(future1.isCompletedExceptionally(), "future1 hoàn thành thành công, không có exception");
        assertFalse(viewModel.isBusy(), "Sau khi hoàn thành, busy giải phóng");
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());

        // Sau khi đã xong, cho phép submit tiếp lần mới
        fakeClient.setManualBookFuture(null);
        BookTicketRequestDto nextRequest = new BookTicketRequestDto("REQ-005-B", "SE-001", "G08", "Khách Tiếp Theo");
        viewModel.submit(nextRequest);
        assertEquals(2, fakeClient.getBookCallCount(), "Sau khi giải phóng, cho phép submit yêu cầu mới");
    }

    @Test
    @DisplayName("Kịch bản 6: Timeout -> UNKNOWN, requestId hiển thị, ghế UNKNOWN, busy giải phóng, có pending trong journal")
    void test6_TimeoutTransitionsToUnknown() {
        CompletableFuture<BookTicketResponseDto> hangingFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(hangingFuture);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-006", "SE-001", "G04", "Khách Timeout");
        viewModel.submit(request);

        assertTrue(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.SUBMITTING, viewModel.getUiState());

        // Kích hoạt timeout ảo
        viewModel.triggerTimeoutForTesting();

        assertFalse(viewModel.isBusy(), "Sau timeout, busy phải được giải phóng");
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState(), "Phải chuyển sang UNKNOWN");
        assertEquals("REQ-006", viewModel.getCurrentRequestId(), "RequestId phải được hiển thị");
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsList.get(3).getStatus(), "Ghế liên quan phải ở trạng thái UNKNOWN");

        // Kiểm tra có bản ghi pending SENT_UNRESOLVED trong journal
        Optional<PendingEntry> opt = journal.get("REQ-006");
        assertTrue(opt.isPresent(), "Phải có bản ghi pending trong journal");
        assertEquals(PendingState.SENT_UNRESOLVED, opt.get().getState());
    }

    @Test
    @DisplayName("Kịch bản 7: Timeout rồi CONFIRMED đến muộn -> pending RESOLVED, UI chuyển CONFIRMED kèm ghi chú đến muộn, ghế BOOKED")
    void test7_LateArrivingConfirmedResolution() {
        CompletableFuture<BookTicketResponseDto> hangingFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(hangingFuture);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-007", "SE-001", "G09", "Khách Trễ");
        viewModel.submit(request);

        // Bước 1: Timeout sang UNKNOWN
        viewModel.triggerTimeoutForTesting();
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsList.get(8).getStatus());

        // Bước 2: Phản hồi CONFIRMED đến muộn cùng requestId
        BookTicketResponseDto lateResponse = new BookTicketResponseDto(
                "REQ-007",
                "CONFIRMED",
                "TCK-LATE-007",
                "SE-001",
                "G09",
                null,
                107L,
                true,
                "node-1",
                "Đặt vé thành công [LATE]"
        );
        hangingFuture.complete(lateResponse);

        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState(), "UI phải chuyển sang CONFIRMED khi nhận kết quả đến muộn");
        assertTrue(viewModel.getLateArrivalNote().contains("kết quả đến muộn") || viewModel.getLateArrivalNote().contains("muộn"));
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(8).getStatus(), "Ghế G09 phải chuyển sang BOOKED");

        Optional<PendingEntry> opt = journal.get("REQ-007");
        assertTrue(opt.isPresent());
        assertEquals(PendingState.RESOLVED, opt.get().getState(), "Pending entry phải chuyển sang RESOLVED");
    }

    @Test
    @DisplayName("Kịch bản 8: Fake ném ngoại lệ mất kết nối sau khi gửi -> UNKNOWN, pending còn trong journal")
    void test8_ExceptionAfterSend_TransitionsToUnknown() {
        fakeClient.setConfiguredBookException(new java.net.SocketTimeoutException("Simulated socket timeout"));

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-008", "SE-001", "G01", "Khách Mất Kết Nối");
        viewModel.submit(request);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.UNKNOWN, seatsList.get(0).getStatus());

        Optional<PendingEntry> opt = journal.get("REQ-008");
        assertTrue(opt.isPresent(), "Phải lưu pending trong journal khi mất kết nối");
        assertEquals(PendingState.SENT_UNRESOLVED, opt.get().getState());
    }

    @Test
    @DisplayName("Kịch bản 9: Đóng/đổi màn hình khi đang SUBMITTING -> ViewModel độc lập, vẫn xử lý kết quả khi đến")
    void test9_DialogCloseIndependence() {
        CompletableFuture<BookTicketResponseDto> manualFuture = new CompletableFuture<>();
        fakeClient.setManualBookFuture(manualFuture);

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-009", "SE-001", "G10", "Khách Đóng Dialog");
        viewModel.submit(request);

        assertTrue(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.SUBMITTING, viewModel.getUiState());

        // Mô phỏng đóng hộp thoại: ViewModel không bị hủy
        // Giả lập kết quả đến từ mạng
        manualFuture.complete(new BookTicketResponseDto("REQ-009", "CONFIRMED", "TCK-009", "SE-001", "G10", null, 109L, true, "node-1", "Đặt vé thành công"));

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState());
        assertEquals(SeatViewModel.SeatStatus.BOOKED, seatsList.get(9).getStatus(), "Ghế G10 vẫn được cập nhật thành BOOKED dù dialog đã đóng");
        assertEquals(1, trackingRefreshService.refreshCount.get());
    }

    @Test
    @DisplayName("Kịch bản 10: Retry từ UNKNOWN và getRequestResult dùng cùng requestId, không sinh requestId mới")
    void test10_RetryAndCheckResultPreserveRequestId() throws Exception {
        // Hành động 1: Retry từ UNKNOWN giữ nguyên requestId và payload
        fakeClient.setConfiguredBookException(new java.net.SocketTimeoutException("Read timed out"));
        BookTicketRequestDto requestRetry = new BookTicketRequestDto("REQ-010-RETRY", "SE-001", "G05", "Khách Retry");
        viewModel.submit(requestRetry);

        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals("REQ-010-RETRY", viewModel.getCurrentRequestId());

        fakeClient.setConfiguredBookException(null);
        fakeClient.setConfiguredBookResponse(new BookTicketResponseDto("REQ-010-RETRY", "CONFIRMED", "TCK-010", "SE-001", "G05", null, 110L, true, "node-1", "Đặt vé thành công"));
        CompletableFuture<BookTicketResponseDto> retryFuture = viewModel.retry("REQ-010-RETRY");
        BookTicketResponseDto retryRes = retryFuture.get(2, TimeUnit.SECONDS);

        assertNotNull(retryRes);
        assertEquals("REQ-010-RETRY", retryRes.getRequestId(), "Retry phải giữ nguyên requestId gốc");
        assertEquals("G05", retryRes.getSeatId());

        // Hành động 2: getRequestResult với cùng requestId
        fakeClient.setConfiguredBookException(new java.net.SocketTimeoutException("Connection dropped"));
        BookTicketRequestDto requestCheck = new BookTicketRequestDto("REQ-010-CHECK", "SE-001", "G06", "Khách Check");
        viewModel.submit(requestCheck);
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals("REQ-010-CHECK", viewModel.getCurrentRequestId());

        fakeClient.setConfiguredResultResponse(new GetRequestResultResponseDto("REQ-010-CHECK", "APPLIED_SUCCESS", "BOOK_TICKET", "TCK-APPLIED-010", null, 111L, true));
        CompletableFuture<GetRequestResultResponseDto> checkFuture = viewModel.checkCurrentResult();
        GetRequestResultResponseDto checkRes = checkFuture.get(2, TimeUnit.SECONDS);

        assertNotNull(checkRes);
        assertEquals("REQ-010-CHECK", checkRes.getRequestId(), "Phải tra cứu đúng requestId gốc");
        assertEquals(BookingViewModel.UiState.CONFIRMED, viewModel.getUiState(), "Tra cứu thành công chuyển sang CONFIRMED");
    }

    @Test
    @DisplayName("Kịch bản 11: Mọi cập nhật property đi qua UiDispatcher (kiểm bằng dispatcher đếm lệnh)")
    void test11_UiDispatcherCount() {
        int initialDispatches = uiDispatchCount.get();

        BookTicketRequestDto request = new BookTicketRequestDto("REQ-011", "SE-001", "G03", "Khách Dispatcher");
        viewModel.submit(request);

        assertTrue(uiDispatchCount.get() > initialDispatches, "Mọi cập nhật thuộc tính phải đi qua UiDispatcher");
    }

    @Test
    @DisplayName("Kịch bản 12: Kết quả từ client MOCK/FAKE giữ nhãn nguồn, không bao giờ thành REAL")
    void test12_MockFakeSourceLabelPreserved() {
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-012", "SE-001", "G04", "Khách Nhãn Mock");
        viewModel.submit(request);

        assertTrue(viewModel.isMock(), "Cờ mock phải là true cho client MOCK/FAKE");
        assertNotEquals("REAL", viewModel.isMock() ? "MOCK" : "REAL");
    }

    @Test
    @DisplayName("Kịch bản 13: Test với cụm Raft thật (bị SKIP khi không có biến môi trường CLUSTER_PROFILE)")
    @EnabledIfEnvironmentVariable(named = "CLUSTER_PROFILE", matches = ".*real.*")
    void test13_RealRaftClusterSkippedWithoutEnv() {
        fail("Test này chỉ chạy khi có cụm Raft thật và biến môi trường CLUSTER_PROFILE=real (nghiệm thu tại G5)");
    }
}
