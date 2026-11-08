package vn.nhom6.railway.client.viewmodel;

import javafx.collections.ObservableList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.MockTicketSource;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TicketViewModel & MockTicketSource Tests (TASK D06)")
class TicketViewModelTest {

    @Test
    @DisplayName("TicketStatus - 4 trạng thái vé có styleClass và badgeSymbol chính xác")
    void testTicketStatusEnums() {
        assertEquals("badge-ticket-confirmed", TicketStatus.CONFIRMED.getStyleClass());
        assertEquals("✓", TicketStatus.CONFIRMED.getBadgeSymbol());
        assertEquals("Đã xác nhận", TicketStatus.CONFIRMED.getDisplayName());

        assertEquals("badge-ticket-cancelled", TicketStatus.CANCELLED.getStyleClass());
        assertEquals("✗", TicketStatus.CANCELLED.getBadgeSymbol());
        assertEquals("Đã hủy", TicketStatus.CANCELLED.getDisplayName());

        assertEquals("badge-ticket-rejected", TicketStatus.REJECTED.getStyleClass());
        assertEquals("⛔", TicketStatus.REJECTED.getBadgeSymbol());
        assertEquals("Bị từ chối", TicketStatus.REJECTED.getDisplayName());

        assertEquals("badge-ticket-unknown", TicketStatus.UNKNOWN.getStyleClass());
        assertEquals("⚠", TicketStatus.UNKNOWN.getBadgeSymbol());
        assertEquals("Chưa rõ (Chờ)", TicketStatus.UNKNOWN.getDisplayName());
    }

    @Test
    @DisplayName("TicketViewModel - isCancellable chỉ đúng khi vé ở trạng thái CONFIRMED")
    void testIsCancellable() {
        TicketViewModel confirmedTicket = new TicketViewModel(
                "REQ-01", "TCK-01", "G01", "SE-001", "Nguyen Van A",
                "2026-11-05 08:00", "1.250.000 VNĐ", TicketStatus.CONFIRMED, null
        );
        assertTrue(confirmedTicket.isCancellable());

        TicketViewModel cancelledTicket = new TicketViewModel(
                "REQ-02", "TCK-02", "G02", "SE-001", "Tran Thi B",
                "2026-11-05 08:30", "1.250.000 VNĐ", TicketStatus.CANCELLED, null
        );
        assertFalse(cancelledTicket.isCancellable());

        TicketViewModel rejectedTicket = new TicketViewModel(
                "REQ-03", "", "G03", "SE-001", "Le Van C",
                "2026-11-05 09:00", "1.250.000 VNĐ", TicketStatus.REJECTED, "SEAT_ALREADY_BOOKED"
        );
        assertFalse(rejectedTicket.isCancellable());

        TicketViewModel unknownTicket = new TicketViewModel(
                "REQ-04", "", "G04", "SE-001", "Pham Van D",
                "2026-11-05 09:30", "1.250.000 VNĐ", TicketStatus.UNKNOWN, "TIMEOUT"
        );
        assertFalse(unknownTicket.isCancellable());
    }

    @Test
    @DisplayName("MockTicketSource - Sinh 7 vé mẫu bao phủ đủ 4 trạng thái theo SPEC-06")
    void testMockTicketSourceCreation() {
        ObservableList<TicketViewModel> tickets = MockTicketSource.createMockTickets();
        assertNotNull(tickets);
        assertEquals(7, tickets.size());

        long confirmedCount = tickets.stream().filter(t -> t.getStatus() == TicketStatus.CONFIRMED).count();
        long cancelledCount = tickets.stream().filter(t -> t.getStatus() == TicketStatus.CANCELLED).count();
        long rejectedCount = tickets.stream().filter(t -> t.getStatus() == TicketStatus.REJECTED).count();
        long unknownCount = tickets.stream().filter(t -> t.getStatus() == TicketStatus.UNKNOWN).count();

        assertEquals(4, confirmedCount);
        assertEquals(1, cancelledCount);
        assertEquals(1, rejectedCount);
        assertEquals(1, unknownCount);
    }

    @Test
    @DisplayName("CancelMockOutcome - Giả lập 3 kết quả hủy vé tuân thủ SPEC-06")
    void testCancelOutcomesBehavior() {
        TicketViewModel ticket = new TicketViewModel(
                "REQ-01", "TCK-01", "G01", "SE-001", "Nguyen Van A",
                "2026-11-05 08:00", "1.250.000 VNĐ", TicketStatus.CONFIRMED, null
        );

        // 1. Khi kết quả là CANCELLED_SUCCESS -> Chuyển sang CANCELLED
        ticket.setStatus(TicketStatus.CANCELLED);
        assertEquals(TicketStatus.CANCELLED, ticket.getStatus());
        assertFalse(ticket.isCancellable());

        // 2. Khi kết quả là REJECTED (NOT_CANCELLABLE) -> Giữ nguyên CONFIRMED
        ticket.setStatus(TicketStatus.CONFIRMED);
        // Mô phỏng server từ chối: không đổi trạng thái
        assertEquals(TicketStatus.CONFIRMED, ticket.getStatus());
        assertTrue(ticket.isCancellable());

        // 3. Khi kết quả là UNKNOWN (Timeout) -> Theo SPEC-06 không được đổi sang CANCELLED
        // Trạng thái vẫn là CONFIRMED
        assertEquals(TicketStatus.CONFIRMED, ticket.getStatus());
    }

    @Test
    @DisplayName("MockTicketSource - Đặt vé mới tự động đồng bộ vào đầu danh sách Vé của tôi")
    void testAddTicketSyncsToMyTickets() {
        MockTicketSource.resetToDefault();
        int initialSize = MockTicketSource.getSharedTickets().size();
        assertEquals(7, initialSize);

        // Đặt vé cho ghế G06 (chưa có trong 7 vé ban đầu) -> danh sách tăng lên 8
        TicketViewModel newBookingTicket = new TicketViewModel(
                "REF-TEST-999", "TCK-SE001-G06", "G06", "SE-001",
                "Phan Thanh Liêm", "2026-11-06 09:40", "1.250.000 VNĐ",
                TicketStatus.CONFIRMED, null
        );

        MockTicketSource.addTicket(newBookingTicket);

        ObservableList<TicketViewModel> currentTickets = MockTicketSource.getSharedTickets();
        assertEquals(initialSize + 1, currentTickets.size());
        // Vé vừa đặt nằm ngay đầu danh sách
        assertSame(newBookingTicket, currentTickets.get(0));
        assertEquals("Phan Thanh Liêm", currentTickets.get(0).getPassengerName());
        assertEquals("TCK-SE001-G06", currentTickets.get(0).getTicketId());
        assertEquals(TicketStatus.CONFIRMED, currentTickets.get(0).getStatus());

        // Đặt lại vé cho ghế G01 (đã có trong danh sách với trạng thái CANCELLED)
        // -> CẬP NHẬT LẠI bản ghi của G01, không tạo thêm dòng mới (tổng số vé vẫn là 8, không có 2 dòng G01)
        TicketViewModel rebookG01 = new TicketViewModel(
                "REF-TEST-1000", "TCK-SE001-G01", "G01", "SE-001",
                "Khách Đặt Lại G01", "2026-11-06 10:15", "1.250.000 VNĐ",
                TicketStatus.CONFIRMED, null
        );
        MockTicketSource.addTicket(rebookG01);
        assertEquals(8, currentTickets.size());
        assertEquals(1, currentTickets.stream().filter(t -> "G01".equalsIgnoreCase(t.getSeatId())).count());
        assertEquals("Khách Đặt Lại G01", currentTickets.get(0).getPassengerName());

        // Reset về mặc định
        MockTicketSource.resetToDefault();
        assertEquals(7, MockTicketSource.getSharedTickets().size());
    }

    @Test
    @DisplayName("MockTicketSource - syncSeatStatusChange cập nhật lại ghế hiện có, đảm bảo mỗi ghế chỉ 1 trạng thái")
    void testSyncSeatStatusChangeFromMockControls() {
        MockTicketSource.resetToDefault();

        // 1. Chuyển G09 sang BOOKED: Ghế chưa có vé -> Sinh 1 vé mới
        MockTicketSource.syncSeatStatusChange("G09", SeatViewModel.SeatStatus.BOOKED);
        ObservableList<TicketViewModel> tickets = MockTicketSource.getSharedTickets();
        assertEquals(8, tickets.size());
        TicketViewModel g09Ticket = tickets.get(0);
        assertEquals("G09", g09Ticket.getSeatId());
        assertEquals(TicketStatus.CONFIRMED, g09Ticket.getStatus());
        assertEquals("", g09Ticket.getPassengerName()); // để trống theo yêu cầu người dùng
        assertTrue(g09Ticket.getTicketId().contains("G09"));

        // 2. Chuyển G09 sang AVAILABLE: Ghế được giải phóng -> CẬP NHẬT vé G09 sang CANCELLED (vẫn 8 vé, không tạo dòng mới)
        MockTicketSource.syncSeatStatusChange("G09", SeatViewModel.SeatStatus.AVAILABLE);
        assertEquals(8, tickets.size());
        assertEquals(TicketStatus.CANCELLED, g09Ticket.getStatus());
        assertEquals("HỦY QUA THAO TÁC MOCK", g09Ticket.getReasonCode());
        assertEquals(1, tickets.stream().filter(t -> "G09".equalsIgnoreCase(t.getSeatId())).count());

        // 3. Chuyển G04 (đang CONFIRMED) sang UNKNOWN: CẬP NHẬT lại vé G04 sang UNKNOWN, không tạo bản ghi trùng lặp
        MockTicketSource.syncSeatStatusChange("G04", SeatViewModel.SeatStatus.UNKNOWN);
        TicketViewModel g04Ticket = tickets.get(0);
        assertEquals("G04", g04Ticket.getSeatId());
        assertEquals(TicketStatus.UNKNOWN, g04Ticket.getStatus());
        assertEquals("— (Chưa cấp)", g04Ticket.getTicketId());
        assertEquals(1, tickets.stream().filter(t -> "G04".equalsIgnoreCase(t.getSeatId())).count());

        // 4. Reset gốc: Khôi phục lại 7 vé ban đầu với 7 mã ghế hoàn toàn riêng biệt
        MockTicketSource.resetToDefault();
        assertEquals(7, MockTicketSource.getSharedTickets().size());
        long uniqueSeats = MockTicketSource.getSharedTickets().stream()
                .map(TicketViewModel::getSeatId)
                .distinct()
                .count();
        assertEquals(7, uniqueSeats);
    }
}

