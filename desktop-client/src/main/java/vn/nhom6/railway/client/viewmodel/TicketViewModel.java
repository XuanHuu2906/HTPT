package vn.nhom6.railway.client.viewmodel;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

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
     * Quản lý danh sách vé dùng chung giữa các màn hình Đặt vé và Tra cứu/Vé của tôi.
     */
    public static class MockTicketSource {
        public static final String MOCK_NOTICE = "DEMO / MOCK — D06";

        private static final ObservableList<TicketViewModel> SHARED_TICKETS = FXCollections.observableArrayList(
                ticket -> new javafx.beans.Observable[]{ ticket.statusProperty(), ticket.reasonCodeProperty() }
        );

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
         * Thêm hoặc cập nhật vé vào danh sách dùng chung.
         * Đảm bảo mỗi ghế (seatId) chỉ có DUY NHẤT một bản ghi trạng thái trong danh sách.
         */
        public static void addTicket(TicketViewModel ticket) {
            if (ticket != null) {
                String seatId = ticket.getSeatId();
                if (seatId != null && !seatId.isBlank()) {
                    // Loại bỏ bản ghi cũ của cùng ghế này nếu đã có để không bao giờ bị trùng lặp 2 trạng thái
                    SHARED_TICKETS.removeIf(t -> seatId.equalsIgnoreCase(t.getSeatId()));
                }
                SHARED_TICKETS.add(0, ticket);
            }
        }

        /**
         * Tìm vé hiện tại theo mã ghế.
         */
        public static TicketViewModel findTicketBySeatId(String seatId) {
            if (seatId == null || seatId.isBlank()) {
                return null;
            }
            return SHARED_TICKETS.stream()
                    .filter(t -> seatId.equalsIgnoreCase(t.getSeatId()))
                    .findFirst()
                    .orElse(null);
        }

        /**
         * Đồng bộ sự thay đổi trạng thái của một ghế sang danh sách "Vé của tôi".
         * Mỗi ghế chỉ có DUY NHẤT một trạng thái. Trường hợp ghế đó được thay đổi thì
         * CẬP NHẬT LẠI bản ghi của ghế đó chứ không tạo thêm bản ghi trùng lặp.
         */
        public static void syncSeatStatusChange(String seatId, SeatViewModel.SeatStatus newStatus) {
            if (seatId == null || newStatus == null) {
                return;
            }

            String nowTime = java.time.LocalDateTime.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

            TicketViewModel existing = findTicketBySeatId(seatId);

            switch (newStatus) {
                case BOOKED -> {
                    String pName = (existing != null && existing.getPassengerName() != null && !existing.getPassengerName().isBlank())
                            ? existing.getPassengerName()
                            : "";
                    String reqId = (existing != null && existing.getRequestId() != null && !existing.getRequestId().isBlank())
                            ? existing.getRequestId()
                            : "REQ-MOCK-" + seatId;
                    TicketViewModel updated = new TicketViewModel(
                            reqId,
                            "TCK-SE001-" + seatId,
                            seatId,
                            "SE-001",
                            pName,
                            nowTime,
                            "1.250.000 VNĐ",
                            TicketStatus.CONFIRMED,
                            null
                    );
                    addTicket(updated);
                }
                case AVAILABLE -> {
                    if (existing != null) {
                        existing.setStatus(TicketStatus.CANCELLED);
                        existing.setReasonCode("HỦY QUA THAO TÁC MOCK");
                    } else {
                        TicketViewModel cancelled = new TicketViewModel(
                                "REQ-MOCK-" + seatId,
                                "TCK-SE001-" + seatId,
                                seatId,
                                "SE-001",
                                "",
                                nowTime,
                                "1.250.000 VNĐ",
                                TicketStatus.CANCELLED,
                                "HỦY QUA THAO TÁC MOCK"
                        );
                        addTicket(cancelled);
                    }
                }
                case UNKNOWN -> {
                    String pName = (existing != null && existing.getPassengerName() != null && !existing.getPassengerName().isBlank())
                            ? existing.getPassengerName()
                            : "";
                    String reqId = (existing != null && existing.getRequestId() != null && !existing.getRequestId().isBlank())
                            ? existing.getRequestId()
                            : "REQ-MOCK-" + seatId;
                    TicketViewModel updated = new TicketViewModel(
                            reqId,
                            "", // Chưa cấp mã vé vì kết quả chưa rõ
                            seatId,
                            "SE-001",
                            pName,
                            nowTime,
                            "1.250.000 VNĐ",
                            TicketStatus.UNKNOWN,
                            "TIMEOUT_MOCK_CONTROL"
                    );
                    addTicket(updated);
                }
                default -> {
                    // SELECTED: chỉ là trạng thái chọn tạm trên UI, không sinh vé
                }
            }
        }

        /**
         * Đồng bộ snapshot trạng thái nhiều ghế vào danh sách vé MOCK.
         */
        public static void syncSnapshot(java.util.Map<String, SeatViewModel.SeatStatus> snapshot) {
            if (snapshot == null) {
                return;
            }
            snapshot.forEach(MockTicketSource::syncSeatStatusChange);
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
                            "REQ-20261105-005", "", "G03", "SE-001",
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
