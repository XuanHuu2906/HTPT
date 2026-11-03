package vn.nhom6.railway.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Kiểm thử dịch vụ đặt vé giả lập (MockBookingServiceTest)")
class MockBookingServiceTest {

    private MockBookingService bookingService;
    private final List<String> defaultSeats = List.of("G01", "G03");

    @BeforeEach
    void setUp() {
        // Tắt giả lập delay để chạy test nhanh (không chặn)
        bookingService = new MockBookingServiceImpl(false);
        // Đảm bảo không bật chế độ random
        System.clearProperty(MockBookingServiceImpl.RANDOM_MODE_PROPERTY);
    }

    @Test
    @DisplayName("Tên bình thường -> CONFIRMED kèm mã tham chiếu MOCK-xxxxxx")
    void testSubmit_Confirmed() {
        BookingOutcome outcome = bookingService.submit("Nguyễn Văn A", defaultSeats);
        assertNotNull(outcome);
        assertEquals(BookingOutcome.BookingStatus.CONFIRMED, outcome.getStatus());
        assertTrue(outcome.isConfirmed());
        assertNull(outcome.getReasonCode());
        assertNotNull(outcome.getReferenceCode());
        assertTrue(outcome.getReferenceCode().startsWith("MOCK-"));
        assertEquals("Đặt vé thành công", outcome.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Nguyen Reject", "test-reject-user", "REJECT", "Trần Thị Reject An"})
    @DisplayName("Tên chứa 'reject' (không phân biệt hoa thường) -> REJECTED với reasonCode=SEAT_ALREADY_BOOKED")
    void testSubmit_Rejected(String name) {
        BookingOutcome outcome = bookingService.submit(name, defaultSeats);
        assertNotNull(outcome);
        assertEquals(BookingOutcome.BookingStatus.REJECTED, outcome.getStatus());
        assertTrue(outcome.isRejected());
        assertEquals(BookingOutcome.SEAT_ALREADY_BOOKED, outcome.getReasonCode());
        assertNull(outcome.getReferenceCode());
        assertEquals("Ghế đã có người đặt, vui lòng chọn ghế khác", outcome.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Nguyen Unknown", "demo_unknown_timeout", "UNKNOWN", "Lê UNKNOWN"})
    @DisplayName("Tên chứa 'unknown' (không phân biệt hoa thường) -> UNKNOWN")
    void testSubmit_Unknown(String name) {
        BookingOutcome outcome = bookingService.submit(name, defaultSeats);
        assertNotNull(outcome);
        assertEquals(BookingOutcome.BookingStatus.UNKNOWN, outcome.getStatus());
        assertTrue(outcome.isUnknown());
        assertNull(outcome.getReasonCode());
        assertNull(outcome.getReferenceCode());
        assertEquals("Chưa xác định được kết quả, vui lòng kiểm tra lại trước khi đặt lại", outcome.getMessage());
    }
}

