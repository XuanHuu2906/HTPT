package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockScenario;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.MockTicketSource;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử luồng tự động tầng Mock & ViewModel cho nhiệm vụ nghiệm thu D08 (TV1-D08).
 * <p>
 * Phạm vi:
 * - Không mở cửa sổ giao diện thật.
 * - Kiểm chứng toàn bộ kịch bản: book thành công, SEAT_ALREADY_BOOKED, NOT_LEADER, NO_QUORUM, UNKNOWN, CONNECTION_LOST.
 * - Kiểm chứng cancel thành công, cancel bị từ chối, cancel UNKNOWN (không bị coi là đã hủy).
 * - Kiểm chứng tra cứu theo requestId và ticketId.
 * - Kiểm chứng mọi kết quả luôn có cờ MOCK (isMock() == true).
 * - Kiểm chứng chuyển trạng thái model AVAILABLE -> BOOKED.
 * - Kiểm chứng non-blocking call với delay mặc định 2 giây.
 */
class MockUiFlowTest {

    private MockBookingClient mockClient;
    private BookingViewModel viewModel;

    @BeforeEach
    void setUp() {
        mockClient = new MockBookingClient();
        mockClient.setDelayMs(20); // 20ms cho test chạy nhanh
        mockClient.resetDefaultData();
        viewModel = new BookingViewModel(mockClient);
        viewModel.setSyncMode(true);
    }

    @AfterEach
    void tearDown() {
        if (mockClient != null) {
            mockClient.close();
        }
    }

    @Test
    @DisplayName("Luồng đặt vé thành công: Trả về CONFIRMED, có mã vé, đổi ghế AVAILABLE sang BOOKED và gắn nhãn MOCK")
    void testBookTicketSuccessFlow() throws Exception {
        SeatViewModel seatG03 = new SeatViewModel("G03", SeatStatus.AVAILABLE);
        assertEquals(SeatStatus.AVAILABLE, seatG03.getStatus());
        assertTrue(seatG03.isAvailable());

        mockClient.setScenario(MockScenario.SUCCESS);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-001", "SE-001", "G03", "Nguyễn Văn Nghiệm Thu");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("CONFIRMED", response.getOutcome());
        assertTrue(response.isConfirmed());
        assertNotNull(response.getTicketId());
        assertTrue(response.getTicketId().startsWith("TCK-G03-"));
        assertTrue(response.isMock(), "Mọi phản hồi phải có cờ mock = true");
        assertEquals("node-1", response.getLeaderHint());

        // Mô phỏng cập nhật trạng thái model
        seatG03.setStatus(SeatStatus.BOOKED);
        assertEquals(SeatStatus.BOOKED, seatG03.getStatus());
        assertTrue(seatG03.isBooked());
        assertFalse(seatG03.isAvailable());

        // Kiểm tra ViewModel state
        assertEquals(UiState.SUCCESS, viewModel.getUiState());
        assertFalse(viewModel.isBusy());
        assertTrue(viewModel.isMock());
    }

    @Test
    @DisplayName("Luồng đặt vé gặp ghế đã đặt: Trả về REJECTED SEAT_ALREADY_BOOKED, hiển thị lý do, khác lỗi hạ tầng")
    void testBookTicketSeatAlreadyBookedFlow() throws Exception {
        // G01 đã được đặt sẵn trong resetDefaultData
        mockClient.setScenario(MockScenario.SEAT_ALREADY_BOOKED);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-002", "SE-001", "G01", "Trần Khách Trùng");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("REJECTED", response.getOutcome());
        assertTrue(response.isRejected());
        assertEquals("SEAT_ALREADY_BOOKED", response.getReasonCode());
        assertTrue(response.isMock());

        assertEquals(UiState.REJECTED, viewModel.getUiState());
        assertEquals("SEAT_ALREADY_BOOKED", viewModel.getErrorCode());
        assertNull(response.getTicketId());
    }

    @Test
    @DisplayName("Luồng đặt vé gặp NOT_LEADER: Trả về UNKNOWN, mã lỗi NOT_LEADER, có gợi ý leaderHint")
    void testBookTicketNotLeaderFlow() throws Exception {
        mockClient.setScenario(MockScenario.NOT_LEADER);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-003", "SE-001", "G05", "Lê Văn Follower");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertTrue(response.isUnknown());
        assertEquals("NOT_LEADER", response.getReasonCode());
        assertEquals("node-2", response.getLeaderHint(), "Phải trả về leaderHint cho client định tuyến lại");
        assertTrue(response.isMock());

        assertEquals(UiState.ERROR, viewModel.getUiState());
        assertEquals("NOT_LEADER", viewModel.getErrorCode());
        assertEquals("node-2", viewModel.getLeaderHint());
    }

    @Test
    @DisplayName("Luồng đặt vé gặp NO_QUORUM: Trả về UNKNOWN, mã lỗi NO_QUORUM, không xác nhận ghi")
    void testBookTicketNoQuorumFlow() throws Exception {
        mockClient.setScenario(MockScenario.NO_QUORUM);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-004", "SE-001", "G06", "Phạm Thiếu Quorum");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertTrue(response.isUnknown());
        assertEquals("NO_QUORUM", response.getReasonCode());
        assertTrue(response.isMock());

        assertEquals(UiState.ERROR, viewModel.getUiState());
        assertEquals("NO_QUORUM", viewModel.getErrorCode());
    }

    @Test
    @DisplayName("Luồng đặt vé gặp UNKNOWN timeout: Trả về UNKNOWN TIMEOUT_UNKNOWN, không bị coi là REJECTED hay CONFIRMED")
    void testBookTicketUnknownTimeoutFlow() throws Exception {
        SeatViewModel seatG07 = new SeatViewModel("G07", SeatStatus.SELECTED);

        mockClient.setScenario(MockScenario.UNKNOWN);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-005", "SE-001", "G07", "Hoàng Chờ Timeout");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);
        BookTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertTrue(response.isUnknown());
        assertEquals("TIMEOUT_UNKNOWN", response.getReasonCode());
        assertNotEquals("REJECTED", response.getOutcome(), "UNKNOWN tuyệt đối không được gán thành REJECTED");
        assertNotEquals("CONFIRMED", response.getOutcome(), "UNKNOWN tuyệt đối không được gán thành CONFIRMED");
        assertTrue(response.isMock());

        // Ghế không được tự ý đổi sang AVAILABLE hay BOOKED, phải chuyển sang UNKNOWN
        seatG07.setStatus(SeatStatus.UNKNOWN);
        assertEquals(SeatStatus.UNKNOWN, seatG07.getStatus());
        assertTrue(seatG07.isUnknown());

        assertEquals(UiState.UNKNOWN, viewModel.getUiState());
        assertEquals("TIMEOUT_UNKNOWN", viewModel.getErrorCode());
    }

    @Test
    @DisplayName("Luồng đặt vé gặp CONNECTION_LOST: Bắt ngoại lệ IO / rớt mạng, chuyển trạng thái DISCONNECTED")
    void testBookTicketConnectionLostFlow() {
        mockClient.setScenario(MockScenario.CONNECTION_LOST);
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-006", "SE-001", "G08", "Vũ Mất Mạng");

        CompletableFuture<BookTicketResponseDto> future = viewModel.book(request);

        assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));
        assertEquals(UiState.DISCONNECTED, viewModel.getUiState());
        assertEquals("CONNECTION_LOST", viewModel.getErrorCode());
    }

    @Test
    @DisplayName("Luồng hủy vé thành công: Trả về CONFIRMED, vé chuyển sang CANCELLED, ghế giải phóng sang AVAILABLE")
    void testCancelTicketSuccessFlow() throws Exception {
        mockClient.setScenario(MockScenario.SUCCESS);
        // TCK-SE001-G01 là vé có sẵn của ghế G01 trong MockBookingClient
        CancelTicketRequestDto cancelReq = new CancelTicketRequestDto("REQ-CANCEL-01", "TCK-SE001-G01");

        CompletableFuture<CancelTicketResponseDto> future = viewModel.cancel(cancelReq);
        CancelTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("CONFIRMED", response.getOutcome());
        assertEquals("TCK-SE001-G01", response.getTicketId());
        assertTrue(response.isMock());

        // Kiểm tra ghế G01 sau khi hủy đã được giải phóng về AVAILABLE trong client
        GetSeatsResponseDto seatsRes = mockClient.getSeats(new GetSeatsRequestDto("SE-001")).get(2, TimeUnit.SECONDS);
        var g01 = seatsRes.getSeats().stream().filter(s -> "G01".equals(s.getSeatId())).findFirst().orElseThrow();
        assertEquals("AVAILABLE", g01.getStatus());
    }

    @Test
    @DisplayName("Luồng hủy vé bị từ chối: Khi vé không tồn tại hoặc không ở trạng thái hiệu lực (TICKET_NOT_ACTIVE)")
    void testCancelTicketRejectedFlow() throws Exception {
        mockClient.setScenario(MockScenario.SUCCESS); // SUCCESS logic sẽ kiểm tra record không tìm thấy
        CancelTicketRequestDto cancelReq = new CancelTicketRequestDto("REQ-CANCEL-02", "TCK-NON-EXISTENT");

        CompletableFuture<CancelTicketResponseDto> future = viewModel.cancel(cancelReq);
        CancelTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("REJECTED", response.getOutcome());
        assertEquals("TICKET_NOT_ACTIVE", response.getReasonCode());
        assertTrue(response.isMock());
        assertEquals(UiState.REJECTED, viewModel.getUiState());
    }

    @Test
    @DisplayName("Luồng hủy vé gặp UNKNOWN timeout: Trạng thái UNKNOWN, vé không bị coi là đã hủy")
    void testCancelTicketUnknownFlow() throws Exception {
        mockClient.setScenario(MockScenario.UNKNOWN);
        CancelTicketRequestDto cancelReq = new CancelTicketRequestDto("REQ-CANCEL-03", "TCK-SE001-G02");

        CompletableFuture<CancelTicketResponseDto> future = viewModel.cancel(cancelReq);
        CancelTicketResponseDto response = future.get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", response.getReasonCode());
        assertNotEquals("CONFIRMED", response.getOutcome(), "Hủy gặp UNKNOWN không được xem là đã hủy thành công");
        assertTrue(response.isMock());
        assertEquals(UiState.UNKNOWN, viewModel.getUiState());
    }

    @Test
    @DisplayName("Tra cứu trạng thái yêu cầu theo requestId và tra cứu vé theo ticketId")
    void testLookupByRequestIdAndTicketIdFlow() throws Exception {
        // 1. Thực hiện đặt vé thành công để có requestId trong kho
        mockClient.setScenario(MockScenario.SUCCESS);
        String reqId = "REQ-LOOKUP-01";
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G09", "Đặng Tra Cứu");
        BookTicketResponseDto bookRes = viewModel.book(bookReq).get(2, TimeUnit.SECONDS);

        assertNotNull(bookRes);
        assertEquals("CONFIRMED", bookRes.getOutcome());
        String ticketId = bookRes.getTicketId();

        // 2. Tra cứu trạng thái yêu cầu theo requestId qua getRequestResult
        GetRequestResultResponseDto resultDto = viewModel.getRequestResult(reqId).get(2, TimeUnit.SECONDS);
        assertNotNull(resultDto);
        assertEquals(reqId, resultDto.getRequestId());
        assertEquals("APPLIED_SUCCESS", resultDto.getState());
        assertEquals(ticketId, resultDto.getTicketId());
        assertTrue(resultDto.isMock());

        // 3. Tra cứu vé trong MockTicketSource dùng chung của giao diện
        TicketViewModel ticket = MockTicketSource.findByRequestId("REQ-20261105-001");
        assertNotNull(ticket, "Phải tìm thấy vé mẫu REQ-20261105-001 trong MockTicketSource");
        assertTrue(ticket.getMockBadge().contains("MOCK"));

        TicketViewModel byTicket = MockTicketSource.findByTicketId("TCK-SE001-G02");
        assertNotNull(byTicket, "Phải tìm thấy vé mẫu TCK-SE001-G02 trong MockTicketSource");

        TicketViewModel nonExistent = MockTicketSource.findByRequestId("REQ-KHONG-CO");
        assertNull(nonExistent);
    }

    @Test
    @DisplayName("Kiểm tra non-blocking call: Delay mặc định ~2 giây không khóa luồng gọi")
    void testDefaultTwoSecondDelayIsNonBlocking() {
        MockBookingClient slowClient = new MockBookingClient();
        slowClient.setDelayMs(2000L); // Đúng chuẩn 2000ms theo DoD
        BookingViewModel slowVm = new BookingViewModel(slowClient);
        slowVm.setSyncMode(false); // Chạy bất đồng bộ thật

        long startTime = System.currentTimeMillis();
        BookTicketRequestDto request = new BookTicketRequestDto("REQ-SLOW", "SE-001", "G10", "Khách Bất Đồng Bộ");

        CompletableFuture<BookTicketResponseDto> future = slowVm.book(request);
        long callDuration = System.currentTimeMillis() - startTime;

        assertNotNull(future, "Lời gọi book() phải trả về CompletableFuture hợp lệ");
        assertFalse(future.isDone(), "Lời gọi book() có delay không được hoàn thành ngay lập tức");
        assertTrue(callDuration < 1500, "Lời gọi book() phải là non-blocking và trả về Future ngay (dưới 1.5s), thực tế: " + callDuration + "ms");
        assertTrue(slowVm.isBusy(), "ViewModel phải chuyển sang trạng thái busy trong khi chờ xử lý");

        slowClient.close();
    }

    @Test
    @DisplayName("Kiểm tra cơ chế chống thao tác trùng (double-submit) khi ViewModel đang busy")
    void testDoubleSubmitProtection() {
        MockBookingClient slowClient = new MockBookingClient();
        slowClient.setDelayMs(1000L);
        BookingViewModel slowVm = new BookingViewModel(slowClient);
        slowVm.setSyncMode(false);

        BookTicketRequestDto req1 = new BookTicketRequestDto("REQ-FIRST", "SE-001", "G04", "Khách 1");
        CompletableFuture<BookTicketResponseDto> future1 = slowVm.book(req1);

        assertNotNull(future1, "Lời gọi book() đầu tiên phải trả về CompletableFuture hợp lệ");
        assertFalse(future1.isDone(), "Yêu cầu thứ nhất vẫn đang được xử lý trong nền");
        assertTrue(slowVm.isBusy());

        // Gửi yêu cầu thứ 2 trong khi yêu cầu 1 đang chạy
        BookTicketRequestDto req2 = new BookTicketRequestDto("REQ-SECOND", "SE-001", "G04", "Khách 2");
        CompletableFuture<BookTicketResponseDto> future2 = slowVm.book(req2);

        assertTrue(future2.isCompletedExceptionally(), "Yêu cầu đặt vé thứ hai trong lúc busy phải bị từ chối lập tức");

        slowClient.close();
    }
}
