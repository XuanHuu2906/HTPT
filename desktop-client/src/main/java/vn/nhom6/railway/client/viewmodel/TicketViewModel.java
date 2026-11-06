package vn.nhom6.railway.client.viewmodel;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.util.List;

/**
 * ViewModel đại diện cho một vé tàu mô phỏng (TASK D06).
 * <p>
 * Lưu trữ thông tin vé và trạng thái phản ứng để binding lên TableView và các
 * điều khiển UI.
 * Dữ liệu hoàn toàn cục bộ (MOCK), không tương tác DB hay network.
 */
public class TicketViewModel {

    /**
     * 4 trạng thái vé theo quy định D06 và SPEC-06.
     */
    public enum TicketStatus {
        CONFIRMED("Đã xác nhận", "badge-ticket-confirmed", "✓"),
        CANCELLED("Đã hủy", "badge-ticket-cancelled", "✗"),
        REJECTED("Bị từ chối", "badge-ticket-rejected", "⛔"),
        UNKNOWN("Chưa rõ (Chờ)", "badge-ticket-unknown", "⚠");

        private final String displayName;
        private final String styleClass;
        private final String badgeSymbol;

        TicketStatus(String displayName, String styleClass, String badgeSymbol) {
            this.displayName = displayName;
            this.styleClass = styleClass;
            this.badgeSymbol = badgeSymbol;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getStyleClass() {
            return styleClass;
        }

        public String getBadgeSymbol() {
            return badgeSymbol;
        }

        public String getFormattedText() {
            return badgeSymbol + " " + displayName;
        }
    }

    private final String requestId;
    private final String ticketId;
    private final String seatId;
    private final String tripCode;
    private final String passengerName;
    private final String createdAt;
    private final String priceFormatted;
    private final String mockBadge;

    private final ObjectProperty<TicketStatus> status = new SimpleObjectProperty<>();
    private final StringProperty reasonCode = new SimpleStringProperty();
    private final ReadOnlyStringWrapper statusDisplay = new ReadOnlyStringWrapper();

    public TicketViewModel(String requestId, String ticketId, String seatId, String tripCode,
            String passengerName, String createdAt, String priceFormatted,
            TicketStatus initialStatus, String reasonCode) {
        this.requestId = requestId;
        this.ticketId = (ticketId != null && !ticketId.isBlank()) ? ticketId : "— (Chưa cấp)";
        this.seatId = seatId;
        this.tripCode = tripCode;
        this.passengerName = passengerName;
        this.createdAt = createdAt;
        this.priceFormatted = priceFormatted;
        this.mockBadge = "DEMO / MOCK";

        this.status.addListener((obs, oldVal, newVal) -> syncStatusDisplay(newVal));
        this.status.set(initialStatus != null ? initialStatus : TicketStatus.UNKNOWN);
        this.reasonCode.set(reasonCode);
        syncStatusDisplay(this.status.get());
    }

    private void syncStatusDisplay(TicketStatus s) {
        if (s == null) {
            statusDisplay.set("—");
        } else {
            statusDisplay.set(s.getFormattedText());
        }
    }

    public String getRequestId() {
        return requestId;
    }

    public String getTicketId() {
        return ticketId;
    }

    public String getSeatId() {
        return seatId;
    }

    public String getTripCode() {
        return tripCode;
    }

    public String getPassengerName() {
        return passengerName;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getPriceFormatted() {
        return priceFormatted;
    }

    public String getMockBadge() {
        return mockBadge;
    }

    public TicketStatus getStatus() {
        return status.get();
    }

    public void setStatus(TicketStatus newStatus) {
        this.status.set(newStatus);
    }

    public ObjectProperty<TicketStatus> statusProperty() {
        return status;
    }

    public String getReasonCode() {
        return reasonCode.get();
    }

    public void setReasonCode(String reason) {
        this.reasonCode.set(reason);
    }

    public StringProperty reasonCodeProperty() {
        return reasonCode;
    }

    public String getStatusDisplay() {
        return statusDisplay.get();
    }

    public ReadOnlyStringProperty statusDisplayProperty() {
        return statusDisplay.getReadOnlyProperty();
    }

    /**
     * Vé chỉ có thể hủy khi đang ở trạng thái CONFIRMED (đã xác nhận thành công).
     */
    public boolean isCancellable() {
        return getStatus() == TicketStatus.CONFIRMED;
    }

    /**
     * Nguồn dữ liệu vé MOCK duy nhất (TASK D06).
     * Quản lý danh sách vé dùng chung giữa các màn hình Đặt vé và Tra cứu/Vé của
     * tôi.
     */
    public static class MockTicketSource {
        public static final String MOCK_NOTICE = "DEMO / MOCK — D06";

        private static final ObservableList<TicketViewModel> SHARED_TICKETS = FXCollections.observableArrayList();

        static {
            resetToDefault();
        }

        public static ObservableList<TicketViewModel> createMockTickets() {
            return getSharedTickets();
        }

        public static ObservableList<TicketViewModel> getSharedTickets() {
            if (SHARED_TICKETS.isEmpty()) {
                resetToDefault();
            }
            return SHARED_TICKETS;
        }

        /**
         * Thêm vé mới đặt thành công vào đầu danh sách để hiển thị ngay trong "Vé của
         * tôi".
         */
        public static void addTicket(TicketViewModel ticket) {
            if (ticket != null) {
                SHARED_TICKETS.add(0, ticket);
            }
        }

        public static void resetToDefault() {
            SHARED_TICKETS.clear();
            SHARED_TICKETS.addAll(
                    new TicketViewModel(
                            "REQ-20261105-001", "TCK-SE001-G02", "G02", "SE-001",
                            "Trần Thị Mai", "2026-11-05 08:15", "1.250.000 VNĐ",
                            TicketStatus.CONFIRMED, null),
                    new TicketViewModel(
                            "REQ-20261105-002", "TCK-SE001-G04", "G04", "SE-001",
                            "Lê Hoàng Nam", "2026-11-05 08:30", "1.250.000 VNĐ",
                            TicketStatus.CONFIRMED, null),
                    new TicketViewModel(
                            "REQ-20261105-003", "TCK-SE001-G08", "G08", "SE-001",
                            "Phạm Minh Tuấn", "2026-11-05 09:00", "1.250.000 VNĐ",
                            TicketStatus.CONFIRMED, null),
                    new TicketViewModel(
                            "REQ-20261105-004", "TCK-SE001-G01", "G01", "SE-001",
                            "Hoàng Đức Anh", "2026-11-05 09:45", "1.250.000 VNĐ",
                            TicketStatus.CANCELLED, "HỦY THEO YÊU CẦU KHÁCH HÀNG"),
                    new TicketViewModel(
                            "REQ-20261105-005", "", "G02", "SE-001",
                            "Vũ Hải Đăng", "2026-11-05 10:10", "1.250.000 VNĐ",
                            TicketStatus.REJECTED, "SEAT_ALREADY_BOOKED"),
                    new TicketViewModel(
                            "REQ-20261105-006", "", "G05", "SE-001",
                            "Đỗ Thu Thảo", "2026-11-05 10:30", "1.250.000 VNĐ",
                            TicketStatus.UNKNOWN, "TIMEOUT_AWAITING_LEADER"),
                    new TicketViewModel(
                            "REQ-20261105-007", "TCK-SE001-G07", "G07", "SE-001",
                            "Ngô Quang Vinh", "2026-11-05 11:00", "1.250.000 VNĐ",
                            TicketStatus.CONFIRMED, null));
        }
    }
}
