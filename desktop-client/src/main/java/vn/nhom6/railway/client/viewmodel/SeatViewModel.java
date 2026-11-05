package vn.nhom6.railway.client.viewmodel;

import javafx.beans.Observable;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.util.Set;

/**
 * ViewModel đại diện cho trạng thái của một ghế ngồi và trạng thái màn hình (TASK D04 & D05).
 * <p>
 * Tính năng nâng cấp D05:
 * - Enum {@link SeatStatus} gồm 4 trạng thái: AVAILABLE, BOOKED, SELECTED, UNKNOWN.
 * - Mỗi ghế có `seatId` (read-only), `ObjectProperty<SeatStatus> status` đúng chuẩn JavaFX bean.
 * - Bổ sung các JavaFX Properties phục vụ UI data binding:
 *   + `displayTextProperty`: Nhãn hiển thị kèm ký hiệu accessibility tự động đồng bộ theo status.
 *   + `bookedProperty`: BooleanProperty đại diện trạng thái đã đặt (dùng bind disable button).
 *   + `selectedProperty`: BooleanProperty đại diện trạng thái đang chọn.
 *   + `availableProperty`, `unknownProperty`: BooleanProperty hỗ trợ logic kiểm tra.
 *   + `styleClassProperty`: Tên lớp CSS tương ứng để bind style class.
 * - Nguồn dữ liệu MOCK duy nhất thông qua {@link MockSeatSource} sinh danh sách
 *   `ObservableList<SeatViewModel>` có extractor theo dõi `statusProperty`.
 * - Enum {@link ScreenState} và {@link ScreenStateModel} quản lý trạng thái màn hình.
 */
public class SeatViewModel {

    /**
     * 4 trạng thái ghế theo đặc tả SPEC-06 và nhiệm vụ D04/D05.
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
        IDLE("Hệ thống sẵn sàng ở chế độ MOCK (D05).", "status-idle"),
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
     * Nguồn dữ liệu ghế MOCK duy nhất (TASK D05).
     * Dữ liệu ban đầu có nhãn MOCK, không rải số liệu giả ở nhiều nơi.
     */
    public static class MockSeatSource {
        public static final String MOCK_LABEL = "MOCK DATA — D05";
        public static final int TOTAL_SEATS = 10;
        public static final Set<String> INITIAL_BOOKED_SEATS = Set.of("G02", "G04", "G08");

        /**
         * Sinh danh sách ObservableList gồm 10 ghế (G01–G10) có extractor theo dõi statusProperty.
         */
        public static ObservableList<SeatViewModel> createMockSeats() {
            ObservableList<SeatViewModel> list = FXCollections.observableArrayList(
                    seat -> new Observable[]{seat.statusProperty()}
            );
            for (int i = 1; i <= TOTAL_SEATS; i++) {
                String seatId = String.format("G%02d", i);
                SeatStatus initialStatus = INITIAL_BOOKED_SEATS.contains(seatId)
                        ? SeatStatus.BOOKED
                        : SeatStatus.AVAILABLE;
                list.add(new SeatViewModel(seatId, initialStatus));
            }
            return list;
        }

        /**
         * Sinh snapshot trạng thái ghế mặc định ban đầu: G02, G04, G08 là BOOKED, còn lại AVAILABLE.
         */
        public static java.util.Map<String, SeatStatus> createInitialSnapshot() {
            java.util.Map<String, SeatStatus> map = new java.util.LinkedHashMap<>();
            for (int i = 1; i <= TOTAL_SEATS; i++) {
                String seatId = String.format("G%02d", i);
                map.put(seatId, INITIAL_BOOKED_SEATS.contains(seatId) ? SeatStatus.BOOKED : SeatStatus.AVAILABLE);
            }
            return map;
        }

        /**
         * Sinh snapshot mô phỏng dữ liệu cập nhật mới (refresh):
         * Ví dụ: G01, G05 được đặt (BOOKED), G04 được mở lại (AVAILABLE), G09 CHƯA RÕ (UNKNOWN).
         */
        public static java.util.Map<String, SeatStatus> createRefreshedMockSnapshot() {
            java.util.Map<String, SeatStatus> map = new java.util.LinkedHashMap<>(createInitialSnapshot());
            map.put("G01", SeatStatus.BOOKED);
            map.put("G04", SeatStatus.AVAILABLE);
            map.put("G05", SeatStatus.BOOKED);
            map.put("G09", SeatStatus.UNKNOWN);
            return map;
        }

        /**
         * Áp dụng snapshot trạng thái ghế mới vào danh sách ghế hiện có mà KHÔNG tạo lại đối tượng SeatViewModel.
         *
         * @param seats danh sách ghế hiện tại
         * @param snapshot bản đồ trạng thái mới (seatId -> SeatStatus)
         */
        public static void applySnapshot(ObservableList<SeatViewModel> seats, java.util.Map<String, SeatStatus> snapshot) {
            if (seats == null || snapshot == null) {
                return;
            }
            for (SeatViewModel seat : seats) {
                String id = seat.getSeatId();
                if (snapshot.containsKey(id)) {
                    SeatStatus newStatus = snapshot.get(id);
                    if (newStatus != null && seat.getStatus() != newStatus) {
                        seat.setStatus(newStatus);
                    }
                }
            }
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
    private final ObjectProperty<SeatStatus> status = new SimpleObjectProperty<>();
    private final ReadOnlyStringWrapper displayText = new ReadOnlyStringWrapper();
    private final ReadOnlyBooleanWrapper booked = new ReadOnlyBooleanWrapper();
    private final ReadOnlyBooleanWrapper selected = new ReadOnlyBooleanWrapper();
    private final ReadOnlyBooleanWrapper available = new ReadOnlyBooleanWrapper();
    private final ReadOnlyBooleanWrapper unknown = new ReadOnlyBooleanWrapper();
    private final ReadOnlyStringWrapper styleClass = new ReadOnlyStringWrapper();

    public SeatViewModel(String seatId) {
        this(seatId, SeatStatus.AVAILABLE);
    }

    public SeatViewModel(String seatId, SeatStatus initialStatus) {
        this.seatId = seatId;
        this.status.addListener((obs, oldVal, newVal) -> syncProperties(newVal));
        this.status.set(initialStatus != null ? initialStatus : SeatStatus.AVAILABLE);
        syncProperties(this.status.get());
    }

    private void syncProperties(SeatStatus s) {
        if (s == null) {
            s = SeatStatus.AVAILABLE;
        }
        displayText.set(s.formatButtonText(seatId));
        booked.set(s == SeatStatus.BOOKED);
        selected.set(s == SeatStatus.SELECTED);
        available.set(s == SeatStatus.AVAILABLE);
        unknown.set(s == SeatStatus.UNKNOWN);
        styleClass.set(s.getStyleClass());
    }

    public String getSeatId() {
        return seatId;
    }

    public SeatStatus getStatus() {
        return status.get();
    }

    public void setStatus(SeatStatus newStatus) {
        this.status.set(newStatus != null ? newStatus : SeatStatus.AVAILABLE);
    }

    public ObjectProperty<SeatStatus> statusProperty() {
        return status;
    }

    public String getDisplayText() {
        return displayText.get();
    }

    public ReadOnlyStringProperty displayTextProperty() {
        return displayText.getReadOnlyProperty();
    }

    public boolean isBooked() {
        return getStatus() == SeatStatus.BOOKED;
    }

    public ReadOnlyBooleanProperty bookedProperty() {
        return booked.getReadOnlyProperty();
    }

    public boolean isSelected() {
        return getStatus() == SeatStatus.SELECTED;
    }

    public ReadOnlyBooleanProperty selectedProperty() {
        return selected.getReadOnlyProperty();
    }

    public boolean isAvailable() {
        return getStatus() == SeatStatus.AVAILABLE;
    }

    public ReadOnlyBooleanProperty availableProperty() {
        return available.getReadOnlyProperty();
    }

    public boolean isUnknown() {
        return getStatus() == SeatStatus.UNKNOWN;
    }

    public ReadOnlyBooleanProperty unknownProperty() {
        return unknown.getReadOnlyProperty();
    }

    public String getStyleClass() {
        return styleClass.get();
    }

    public ReadOnlyStringProperty styleClassProperty() {
        return styleClass.getReadOnlyProperty();
    }

    /**
     * Tạo danh sách ghế MOCK từ nguồn MOCK duy nhất.
     */
    public static ObservableList<SeatViewModel> createMockSeats() {
        return MockSeatSource.createMockSeats();
    }

    /**
     * Áp dụng snapshot trạng thái ghế mới vào danh sách ghế hiện có mà KHÔNG tạo lại đối tượng (TASK D05).
     *
     * @param seats danh sách ghế cần cập nhật
     * @param snapshot bản đồ trạng thái mới (seatId -> SeatStatus)
     */
    public static void applySnapshot(ObservableList<SeatViewModel> seats, java.util.Map<String, SeatStatus> snapshot) {
        MockSeatSource.applySnapshot(seats, snapshot);
    }

    /**
     * Ánh xạ trạng thái ghế sang tên lớp CSS (style class).
     */
    public static String mapStatusToStyleClass(SeatStatus status) {
        if (status == null) {
            return SeatStatus.AVAILABLE.getStyleClass();
        }
        return status.getStyleClass();
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
