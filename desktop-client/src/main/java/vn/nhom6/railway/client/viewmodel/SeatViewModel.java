package vn.nhom6.railway.client.viewmodel;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;

/**
 * ViewModel đại diện cho trạng thái của một ghế ngồi và trạng thái màn hình (TASK D04).
 * <p>
 * Tính năng:
 * - Enum {@link SeatStatus} gồm 4 trạng thái: AVAILABLE, BOOKED, SELECTED, UNKNOWN.
 * - Mỗi ghế có `seatId` (G01–G10) và `ObjectProperty<SeatStatus>` để UI bind, đổi trạng thái là UI tự cập nhật.
 * - Hàm ánh xạ trạng thái sang style class: seat-available, seat-booked, seat-selected, seat-unknown.
 * - Enum {@link ScreenState} gồm 4 trạng thái màn hình: IDLE, LOADING, ERROR, DISCONNECTED kèm message để bind.
 */
public class SeatViewModel {

    /**
     * 4 trạng thái ghế theo đặc tả SPEC-06 và nhiệm vụ D04.
     */
    public enum SeatStatus {
        AVAILABLE("Trống", "seat-available"),
        BOOKED("Đã đặt", "seat-booked"),
        SELECTED("Đang chọn", "seat-selected"),
        UNKNOWN("Chưa rõ", "seat-unknown");

        private final String displayName;
        private final String styleClass;

        SeatStatus(String displayName, String styleClass) {
            this.displayName = displayName;
            this.styleClass = styleClass;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getStyleClass() {
            return styleClass;
        }

        /**
         * Ký hiệu phụ trợ trực quan đáp ứng tiêu chuẩn Accessibility (SPEC-06),
         * không dựa duy nhất vào màu sắc.
         */
        public String getBadgeSymbol() {
            return switch (this) {
                case AVAILABLE -> "✓";
                case BOOKED -> "✗";
                case SELECTED -> "★";
                case UNKNOWN -> "⚠";
            };
        }

        /**
         * Định dạng văn bản hiển thị trên nút ghế (Mã ghế + [Ký hiệu + Tên trạng thái]).
         */
        public String formatButtonText(String seatId) {
            return seatId + "\n[" + getBadgeSymbol() + " " + displayName + "]";
        }
    }

    /**
     * 4 trạng thái màn hình theo yêu cầu D04.
     */
    public enum ScreenState {
        IDLE("Hệ thống sẵn sàng ở chế độ MOCK (D04).", "status-idle"),
        LOADING("Đang tải dữ liệu từ hệ thống...", "status-loading"),
        ERROR("Đã xảy ra lỗi khi xử lý dữ liệu ghế.", "status-error"),
        DISCONNECTED("Mất kết nối với mạng / cụm máy chủ.", "status-disconnected");

        private final String defaultMessage;
        private final String styleClass;

        ScreenState(String defaultMessage, String styleClass) {
            this.defaultMessage = defaultMessage;
            this.styleClass = styleClass;
        }

        public String getDefaultMessage() {
            return defaultMessage;
        }

        public String getStyleClass() {
            return styleClass;
        }
    }

    /**
     * Lớp model quản lý trạng thái màn hình (Screen State) để bind với giao diện.
     */
    public static class ScreenStateModel {
        private final ObjectProperty<ScreenState> state = new SimpleObjectProperty<>(ScreenState.IDLE);
        private final StringProperty message = new SimpleStringProperty(ScreenState.IDLE.getDefaultMessage());

        public ScreenState getState() {
            return state.get();
        }

        public void setState(ScreenState newState) {
            this.state.set(newState);
            this.message.set(newState.getDefaultMessage());
        }

        public void setState(ScreenState newState, String customMessage) {
            this.state.set(newState);
            this.message.set(customMessage != null ? customMessage : newState.getDefaultMessage());
        }

        public ObjectProperty<ScreenState> stateProperty() {
            return state;
        }

        public String getMessage() {
            return message.get();
        }

        public void setMessage(String msg) {
            this.message.set(msg);
        }

        public StringProperty messageProperty() {
            return message;
        }

        public String getStyleClass() {
            return state.get().getStyleClass();
        }
    }

    private final String seatId;
    private final ObjectProperty<SeatStatus> status = new SimpleObjectProperty<>(SeatStatus.AVAILABLE);

    public SeatViewModel(String seatId) {
        this(seatId, SeatStatus.AVAILABLE);
    }

    public SeatViewModel(String seatId, SeatStatus initialStatus) {
        this.seatId = seatId;
        this.status.set(initialStatus);
    }

    public String getSeatId() {
        return seatId;
    }

    public SeatStatus getStatus() {
        return status.get();
    }

    public void setStatus(SeatStatus newStatus) {
        this.status.set(newStatus);
    }

    public ObjectProperty<SeatStatus> statusProperty() {
        return status;
    }

    /**
     * Ánh xạ trạng thái ghế sang tên lớp CSS (style class).
     *
     * @param status trạng thái ghế
     * @return tên style class CSS tương ứng
     */
    public static String mapStatusToStyleClass(SeatStatus status) {
        if (status == null) {
            return SeatStatus.AVAILABLE.getStyleClass();
        }
        return status.getStyleClass();
    }

    public String getStyleClass() {
        return mapStatusToStyleClass(getStatus());
    }

    public String getFormattedText() {
        return getStatus().formatButtonText(seatId);
    }

    @Override
    public String toString() {
        return "SeatViewModel{" +
                "seatId='" + seatId + '\'' +
                ", status=" + getStatus() +
                '}';
    }
}
