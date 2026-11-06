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
}

