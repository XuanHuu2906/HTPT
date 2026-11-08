package vn.nhom6.railway.client.viewmodel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockScenario;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử đơn vị cho {@link BookingViewModel} (TASK D07).
 */
class BookingViewModelTest {

    private MockBookingClient mockClient;
    private BookingViewModel viewModel;

    @BeforeEach
    void setUp() {
        mockClient = new MockBookingClient();
        mockClient.setDelayMs(10); // 10ms cho test chạy nhanh
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
    @DisplayName("Đặt vé thành công: uiState chuyển sang SUCCESS, busy trở về false, mock=true")
    void testBookSuccessState() throws Exception {
        mockClient.setScenario(MockScenario.SUCCESS);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G03", "Trần Test");
        assertTrue(viewModel.isBusy(), "Khi đang gửi yêu cầu, cờ busy phải là true");
        assertEquals(BookingViewModel.UiState.LOADING, viewModel.getUiState());

        BookTicketResponseDto result = future.get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(viewModel.isBusy(), "Sau khi xử lý xong, busy phải là false");
        assertEquals(BookingViewModel.UiState.SUCCESS, viewModel.getUiState());
        assertTrue(viewModel.isMock(), "Cờ mock phải bật");
        assertTrue(viewModel.getMessage().contains("Đặt vé thành công"));
    }

    @Test
    @DisplayName("Đặt vé bị từ chối: uiState chuyển sang REJECTED, errorCode=SEAT_ALREADY_BOOKED")
    void testBookRejectedState() throws Exception {
        mockClient.setScenario(MockScenario.SEAT_ALREADY_BOOKED);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G04", "Trần Test");
        future.get(2, TimeUnit.SECONDS);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.REJECTED, viewModel.getUiState());
        assertEquals("SEAT_ALREADY_BOOKED", viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("đã có người đặt"));
    }

    @Test
    @DisplayName("Lỗi NOT_LEADER: uiState=ERROR, errorCode=NOT_LEADER, có leaderHint")
    void testBookNotLeaderState() throws Exception {
        mockClient.setScenario(MockScenario.NOT_LEADER);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G04", "Trần Test");
        future.get(2, TimeUnit.SECONDS);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.ERROR, viewModel.getUiState());
        assertEquals("NOT_LEADER", viewModel.getErrorCode());
        assertEquals("node-2", viewModel.getLeaderHint());
        assertTrue(viewModel.getMessage().contains("Node không phải Leader"));
    }

    @Test
    @DisplayName("Lỗi NO_QUORUM: uiState=ERROR, errorCode=NO_QUORUM")
    void testBookNoQuorumState() throws Exception {
        mockClient.setScenario(MockScenario.NO_QUORUM);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G04", "Trần Test");
        future.get(2, TimeUnit.SECONDS);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.ERROR, viewModel.getUiState());
        assertEquals("NO_QUORUM", viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("Không đủ đa số"));
    }

    @Test
    @DisplayName("Lỗi TIMEOUT_UNKNOWN: uiState=UNKNOWN, không bị coi là REJECTED hay SUCCESS")
    void testBookUnknownTimeoutState() throws Exception {
        mockClient.setScenario(MockScenario.UNKNOWN);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G04", "Trần Test");
        future.get(2, TimeUnit.SECONDS);

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.UNKNOWN, viewModel.getUiState());
        assertEquals("TIMEOUT_UNKNOWN", viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("Chưa xác định kết quả"));
    }

    @Test
    @DisplayName("Mất kết nối CONNECTION_LOST: uiState=DISCONNECTED")
    void testBookConnectionLostState() throws Exception {
        mockClient.setScenario(MockScenario.CONNECTION_LOST);

        CompletableFuture<BookTicketResponseDto> future = viewModel.book("G04", "Trần Test");
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Future hoàn thành có ngoại lệ
        }

        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.DISCONNECTED, viewModel.getUiState());
        assertEquals("CONNECTION_LOST", viewModel.getErrorCode());
        assertTrue(viewModel.getMessage().contains("Mất kết nối"));
    }

    @Test
    @DisplayName("Hủy vé thành công qua ViewModel")
    void testCancelSuccessState() throws Exception {
        mockClient.setScenario(MockScenario.SUCCESS);

        CompletableFuture<CancelTicketResponseDto> future = viewModel.cancel("TCK-SE001-G01");
        CancelTicketResponseDto result = future.get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(viewModel.isBusy());
        assertEquals(BookingViewModel.UiState.SUCCESS, viewModel.getUiState());
        assertTrue(viewModel.getMessage().contains("Hủy vé"));
    }

    @Test
    @DisplayName("Chứng minh thay nguồn dữ liệu qua interface: Tiêm stub AlwaysFailBookingClient mà không cần sửa Controller hay FXML")
    void testSwapDataSourceWithStubClient() {
        vn.nhom6.railway.client.network.BookingClient alwaysFailClient = new vn.nhom6.railway.client.network.BookingClient() {
            @Override
            public CompletableFuture<vn.nhom6.railway.client.network.GetSeatsResponseDto> getSeats(vn.nhom6.railway.protocol.dto.GetSeatsRequestDto request) {
                CompletableFuture<vn.nhom6.railway.client.network.GetSeatsResponseDto> f = new CompletableFuture<>();
                f.completeExceptionally(new java.io.IOException("STUB: Mất kết nối tới máy chủ"));
                return f;
            }

            @Override
            public CompletableFuture<BookTicketResponseDto> book(vn.nhom6.railway.protocol.dto.BookTicketRequestDto request) {
                CompletableFuture<BookTicketResponseDto> f = new CompletableFuture<>();
                f.completeExceptionally(new java.io.IOException("STUB: Không thể gửi yêu cầu"));
                return f;
            }

            @Override
            public CompletableFuture<CancelTicketResponseDto> cancel(vn.nhom6.railway.client.network.CancelTicketRequestDto request) {
                CompletableFuture<CancelTicketResponseDto> f = new CompletableFuture<>();
                f.completeExceptionally(new java.io.IOException("STUB: Không thể hủy"));
                return f;
            }

            @Override
            public CompletableFuture<vn.nhom6.railway.client.network.GetRequestResultResponseDto> getRequestResult(String requestId) {
                return CompletableFuture.completedFuture(new vn.nhom6.railway.client.network.GetRequestResultResponseDto(requestId, "UNKNOWN", null, null, null, null, false));
            }
        };

        vn.nhom6.railway.client.ClientFactory.setBookingClient(alwaysFailClient);
        BookingViewModel vm = vn.nhom6.railway.client.ClientFactory.getBookingViewModel();
        vm.setSyncMode(true);

        CompletableFuture<BookTicketResponseDto> future = vm.book("G05", "Khách Test");
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {}

        assertEquals(BookingViewModel.UiState.DISCONNECTED, vm.getUiState());
        assertTrue(vm.getMessage().contains("Mất kết nối"));

        // Phục hồi lại mock client mặc định
        vn.nhom6.railway.client.ClientFactory.setBookingClient(mockClient);
    }
}
