package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.util.HashMap;
import java.util.Map;

/**
 * Controller điều khiển giao diện chính của ứng dụng bán vé Desktop (JavaFX).
 * <p>
 * Phạm vi D01:
 * - Hiển thị nhãn MOCK nổi bật.
 * - Dựng sẵn vùng điều hướng bên trái (placeholder).
 * - Sinh lưới 10 ghế (G01 - G10) từ dữ liệu mock cứng trong code.
 * - Cho phép chọn ghế cục bộ (SELECTED_LOCAL) mà không kết nối Backend hay gọi API.
 */
public class MainController {

    // === Tiêu đề & Nhãn MOCK ===
    @FXML
    private Label tripTitleLabel;

    @FXML
    private Label mockBadgeLabel;

    // === Vùng điều hướng (Navigation Region) ===
    @FXML
    private VBox navRegion;

    @FXML
    private Button navTripBtn;

    @FXML
    private Button navSeatBtn;

    @FXML
    private Button navPaymentBtn;

    @FXML
    private Button navMyTicketsBtn;

    // === Container sơ đồ ghế & Form ===
    @FXML
    private GridPane seatGridContainer;

    @FXML
    private Label selectedSeatLabel;

    @FXML
    private TextField passengerNameField;

    @FXML
    private Button bookSeatBtn;

    @FXML
    private Button refreshBtn;

    @FXML
    private Button lookupBtn;

    // === Thanh trạng thái bên dưới ===
    @FXML
    private Label statusMessageLabel;

    /**
     * Định nghĩa các trạng thái ghế phục vụ chế độ MOCK (theo SPEC-06).
     */
    public enum SeatMockState {
        AVAILABLE("Trống", "-fx-background-color: #ecfdf5; -fx-text-fill: #065f46; -fx-border-color: #10b981; -fx-border-width: 1.5; -fx-border-radius: 8; -fx-background-radius: 8; -fx-cursor: hand;"),
        BOOKED("Đã đặt", "-fx-background-color: #fef2f2; -fx-text-fill: #991b1b; -fx-border-color: #ef4444; -fx-border-width: 1.5; -fx-border-radius: 8; -fx-background-radius: 8; -fx-cursor: default;"),
        SELECTED_LOCAL("Đang chọn", "-fx-background-color: #fffbeb; -fx-text-fill: #92400e; -fx-border-color: #f59e0b; -fx-border-width: 2.5; -fx-border-radius: 8; -fx-background-radius: 8; -fx-cursor: hand;");

        private final String displayText;
        private final String cssStyle;

        SeatMockState(String displayText, String cssStyle) {
            this.displayText = displayText;
            this.cssStyle = cssStyle;
        }

        public String getDisplayText() {
            return displayText;
        }

        public String getCssStyle() {
            return cssStyle;
        }
    }

    // Bộ nhớ trạng thái 10 ghế (MOCK cứng)
    private final Map<String, SeatMockState> seatStateMap = new HashMap<>();
    private final Map<String, Button> seatButtonMap = new HashMap<>();

    // Ghế đang chọn cục bộ trên Client
    private String selectedSeatId = null;

    @FXML
    public void initialize() {
        // 1. Đảm bảo nhãn MOCK nổi bật
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText("MOCK DATA");
        }

        // 2. Khởi tạo dữ liệu mock cứng cho 10 ghế G01 – G10
        initMockData();

        // 3. Dựng lưới 10 ghế (GridPane 2 hàng x 5 cột)
        buildSeatGrid();

        // 4. Gắn sự kiện cho các nút điều hướng và form thao tác (placeholder)
        bindEventHandlers();
    }

    /**
     * Sinh dữ liệu trạng thái ban đầu cho 10 ghế (G01 đến G10).
     * Một số ghế là BOOKED để minh họa, còn lại là AVAILABLE.
     */
    private void initMockData() {
        seatStateMap.clear();
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            // Ghế G02, G04, G08 giả lập là đã được đặt trước đó
            if (i == 2 || i == 4 || i == 8) {
                seatStateMap.put(seatId, SeatMockState.BOOKED);
            } else {
                seatStateMap.put(seatId, SeatMockState.AVAILABLE);
            }
        }
        selectedSeatId = null;
    }

    /**
     * Dựng 10 nút bấm đại diện cho 10 ghế và đặt vào GridPane (2 hàng x 5 cột).
     * Hàng 0: G01 - G05
     * Hàng 1: G06 - G10
     */
    private void buildSeatGrid() {
        if (seatGridContainer == null) {
            return;
        }

        seatGridContainer.getChildren().clear();
        seatButtonMap.clear();

        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            SeatMockState state = seatStateMap.get(seatId);

            Button seatButton = new Button();
            seatButton.setPrefWidth(120.0);
            seatButton.setPrefHeight(75.0);
            seatButton.setAlignment(Pos.CENTER);

            // Cập nhật nhãn và style theo trạng thái
            updateSeatButtonStyle(seatButton, seatId, state);

            // Gắn sự kiện click ghế
            seatButton.setOnAction(e -> handleSeatClick(seatId));

            // Lưu tham chiếu
            seatButtonMap.put(seatId, seatButton);

            // Tọa độ lưới: 5 cột / hàng
            int col = (i - 1) % 5;
            int row = (i - 1) / 5;
            seatGridContainer.add(seatButton, col, row);
        }

        updateSelectionDisplay();
    }

    /**
     * Cập nhật văn bản hiển thị và định dạng CSS cho nút ghế.
     */
    private void updateSeatButtonStyle(Button button, String seatId, SeatMockState state) {
        // Hiển thị cả mã ghế và nhãn trạng thái (hỗ trợ accessibility)
        button.setText(seatId + "\n[" + state.getDisplayText() + "]");
        button.setStyle(state.getCssStyle() + " -fx-font-size: 13px; -fx-font-weight: bold; -fx-text-alignment: center;");
    }

    /**
     * Xử lý khi người dùng click vào một ghế.
     */
    private void handleSeatClick(String seatId) {
        SeatMockState currentState = seatStateMap.get(seatId);

        if (currentState == SeatMockState.BOOKED) {
            updateStatusMessage("Ghế " + seatId + " đã được đặt trước (BOOKED). Vui lòng chọn ghế trống khác!");
            return;
        }

        // Nếu click lại chính ghế đang chọn -> hủy chọn
        if (seatId.equals(selectedSeatId)) {
            seatStateMap.put(seatId, SeatMockState.AVAILABLE);
            updateSeatButtonStyle(seatButtonMap.get(seatId), seatId, SeatMockState.AVAILABLE);
            selectedSeatId = null;
            updateStatusMessage("Đã bỏ chọn ghế " + seatId + ".");
        } else {
            // Hủy chọn ghế cũ (nếu có)
            if (selectedSeatId != null) {
                seatStateMap.put(selectedSeatId, SeatMockState.AVAILABLE);
                updateSeatButtonStyle(seatButtonMap.get(selectedSeatId), selectedSeatId, SeatMockState.AVAILABLE);
            }

            // Chọn ghế mới
            selectedSeatId = seatId;
            seatStateMap.put(seatId, SeatMockState.SELECTED_LOCAL);
            updateSeatButtonStyle(seatButtonMap.get(seatId), seatId, SeatMockState.SELECTED_LOCAL);
            updateStatusMessage("Đã chọn ghế " + seatId + " (MOCK). Nhập tên khách để đặt vé.");
        }

        updateSelectionDisplay();
    }

    /**
     * Cập nhật nhãn hiển thị ghế đang chọn.
     */
    private void updateSelectionDisplay() {
        if (selectedSeatLabel != null) {
            if (selectedSeatId != null) {
                selectedSeatLabel.setText("Ghế đang chọn: " + selectedSeatId);
            } else {
                selectedSeatLabel.setText("Ghế đang chọn: (Chưa chọn)");
            }
        }
    }

    /**
     * Gắn xử lý cho các nút hành động (Mock placeholders).
     */
    private void bindEventHandlers() {
        // Nút Đặt vé (MOCK)
        if (bookSeatBtn != null) {
            bookSeatBtn.setOnAction(e -> {
                if (selectedSeatId == null) {
                    updateStatusMessage("Vui lòng click chọn một ghế trống trước khi nhấn Đặt vé!");
                    return;
                }

                String passenger = (passengerNameField != null) ? passengerNameField.getText().trim() : "";
                if (passenger.isEmpty()) {
                    updateStatusMessage("Vui lòng nhập họ tên hành khách để tiếp tục!");
                    return;
                }

                // Giả lập đặt vé thành công ở tầng giao diện MOCK
                String bookedSeat = selectedSeatId;
                seatStateMap.put(bookedSeat, SeatMockState.BOOKED);
                updateSeatButtonStyle(seatButtonMap.get(bookedSeat), bookedSeat, SeatMockState.BOOKED);
                selectedSeatId = null;
                updateSelectionDisplay();

                updateStatusMessage("MOCK SUCCESS: Đã đặt ghế " + bookedSeat + " cho khách [" + passenger + "] (Mock D01, chưa gọi Backend).");
            });
        }

        // Nút Làm mới
        if (refreshBtn != null) {
            refreshBtn.setOnAction(e -> {
                initMockData();
                buildSeatGrid();
                if (passengerNameField != null) {
                    passengerNameField.clear();
                }
                updateStatusMessage("Đã làm mới dữ liệu sơ đồ ghế (Reset về trạng thái MOCK ban đầu).");
            });
        }

        // Nút Tra cứu
        if (lookupBtn != null) {
            lookupBtn.setOnAction(e -> {
                updateStatusMessage("MOCK INFO: Tính năng tra cứu requestId sẽ khả dụng khi kết nối Backend ở các sprint tiếp theo.");
            });
        }

        // Các nút điều hướng (Placeholder)
        setupNavButton(navTripBtn, "Chọn chuyến");
        setupNavButton(navSeatBtn, "Chọn ghế (10 ghế)");
        setupNavButton(navPaymentBtn, "Thanh toán");
        setupNavButton(navMyTicketsBtn, "Vé của tôi");
    }

    private void setupNavButton(Button btn, String navName) {
        if (btn != null) {
            btn.setOnAction(e -> updateStatusMessage("Điều hướng: " + navName + " (Placeholder D01)"));
        }
    }

    private void updateStatusMessage(String message) {
        if (statusMessageLabel != null) {
            statusMessageLabel.setText(message);
        }
    }
}

