package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.GridPane;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Controller quản lý sơ đồ chọn ghế tương tác (TASK D02).
 * <p>
 * Tính năng:
 * - Dữ liệu MOCK 10 ghế (G01–G10), một vài ghế ở trạng thái BOOKED.
 * - Cho phép chọn và bỏ chọn từng ghế trống (SELECTED_LOCAL).
 * - Cập nhật nhãn ghế đang chọn và tự động bật/tắt nút "Tiếp tục".
 * - Cung cấp {@link #getSelectedSeats()} cho module D03 lấy dữ liệu ghế đã chọn.
 * - Hỗ trợ callback điều hướng quay lại màn hình chính hoặc tiếp tục sang form đặt vé.
 */
public class SeatGridController {

    @FXML
    private Label mockBadgeLabel;

    @FXML
    private Button backBtn;

    @FXML
    private GridPane seatGridPane;

    @FXML
    private Label selectedSeatsLabel;

    @FXML
    private Label seatStatusLabel;

    @FXML
    private Button resetSelectionBtn;

    @FXML
    private Button continueBtn;

    /**
     * Định nghĩa các trạng thái ghế phục vụ chế độ MOCK (theo SPEC-06).
     */
    public enum SeatMockState {
        AVAILABLE("Trống", "-fx-background-color: #ecfdf5; -fx-text-fill: #065f46; -fx-border-color: #10b981; -fx-border-width: 1.5; -fx-border-radius: 8; -fx-background-radius: 8; -fx-cursor: hand;"),
        BOOKED("Đã đặt", "-fx-background-color: #fef2f2; -fx-text-fill: #991b1b; -fx-border-color: #ef4444; -fx-border-width: 1.5; -fx-border-radius: 8; -fx-background-radius: 8; -fx-cursor: not-allowed;"),
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

    // Dữ liệu trạng thái ban đầu của 10 ghế (MOCK cứng)
    private final Map<String, SeatMockState> initialSeatStateMap = new HashMap<>();

    // Bản đồ lưu các nút ghế trên giao diện
    private final Map<String, Button> seatButtons = new HashMap<>();

    // Danh sách các ghế người dùng đang chọn cục bộ (dùng TreeSet để tự sắp xếp theo mã ghế)
    private final Set<String> selectedSeats = new TreeSet<>();

    // Callback điều hướng
    private Runnable onBackAction;
    private Consumer<List<String>> onContinueAction;

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText("MOCK DATA — D02");
        }

        // 1. Khởi tạo dữ liệu mock cứng
        initMockData();

        // 2. Dựng lưới 10 ghế (GridPane 2 hàng x 5 cột)
        buildSeatGrid();

        // 3. Khởi tạo trạng thái danh sách chọn và nút Tiếp tục
        updateSelectionUI();
    }

    /**
     * Dữ liệu ghế MOCK: 10 ghế G01 đến G10.
     * Các ghế G02, G04, G08 ở trạng thái BOOKED (không cho chọn).
     */
    private void initMockData() {
        initialSeatStateMap.clear();
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            if (i == 2 || i == 4 || i == 8) {
                initialSeatStateMap.put(seatId, SeatMockState.BOOKED);
            } else {
                initialSeatStateMap.put(seatId, SeatMockState.AVAILABLE);
            }
        }
    }

    /**
     * Dựng các nút bấm đại diện cho 10 ghế trong GridPane (2 hàng x 5 cột).
     * Hàng 0: G01 - G05
     * Hàng 1: G06 - G10
     */
    private void buildSeatGrid() {
        if (seatGridPane == null) {
            return;
        }

        seatGridPane.getChildren().clear();
        seatButtons.clear();

        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            SeatMockState state = initialSeatStateMap.get(seatId);

            Button button = new Button();
            button.setPrefWidth(120.0);
            button.setPrefHeight(75.0);
            button.setAlignment(Pos.CENTER);

            updateSeatButtonStyle(button, seatId, state);

            button.setOnAction(e -> handleSeatClick(seatId));

            seatButtons.put(seatId, button);

            int col = (i - 1) % 5;
            int row = (i - 1) / 5;
            seatGridPane.add(button, col, row);
        }
    }

    /**
     * Cập nhật văn bản hiển thị và định dạng CSS cho nút ghế.
     * Kèm nhãn trạng thái theo SPEC-06 để đảm bảo accessibility.
     */
    private void updateSeatButtonStyle(Button button, String seatId, SeatMockState state) {
        if (button == null) return;
        button.setText(seatId + "\n[" + state.getDisplayText() + "]");
        button.setStyle(state.getCssStyle() + " -fx-font-size: 13px; -fx-font-weight: bold; -fx-text-alignment: center;");
    }

    /**
     * Xử lý khi người dùng click vào một ghế.
     * - Ghế BOOKED: thông báo không thể chọn.
     * - Ghế AVAILABLE: chuyển sang SELECTED_LOCAL và thêm vào danh sách.
     * - Ghế SELECTED_LOCAL: chuyển về AVAILABLE và xóa khỏi danh sách.
     */
    public void handleSeatClick(String seatId) {
        SeatMockState baseState = initialSeatStateMap.get(seatId);

        if (baseState == SeatMockState.BOOKED) {
            updateStatus("Ghế " + seatId + " đã được đặt trước (BOOKED). Không thể chọn ghế này!");
            return;
        }

        Button button = seatButtons.get(seatId);

        if (selectedSeats.contains(seatId)) {
            // Hủy chọn ghế
            selectedSeats.remove(seatId);
            updateSeatButtonStyle(button, seatId, SeatMockState.AVAILABLE);
            updateStatus("Đã bỏ chọn ghế " + seatId + ".");
        } else {
            // Chọn ghế
            selectedSeats.add(seatId);
            updateSeatButtonStyle(button, seatId, SeatMockState.SELECTED_LOCAL);
            updateStatus("Đã chọn ghế " + seatId + ".");
        }

        updateSelectionUI();
    }

    /**
     * Cập nhật nhãn hiển thị danh sách ghế đang chọn và bật/tắt nút "Tiếp tục".
     */
    private void updateSelectionUI() {
        if (selectedSeats.isEmpty()) {
            if (selectedSeatsLabel != null) {
                selectedSeatsLabel.setText("Ghế đang chọn: (Chưa chọn ghế nào)");
            }
            if (continueBtn != null) {
                continueBtn.setDisable(true);
            }
        } else {
            String seatListStr = String.join(", ", selectedSeats);
            if (selectedSeatsLabel != null) {
                selectedSeatsLabel.setText("Ghế đang chọn: " + seatListStr + " (" + selectedSeats.size() + " ghế)");
            }
            if (continueBtn != null) {
                continueBtn.setDisable(false);
            }
        }
    }

    /**
     * Cung cấp getter để module D03 lấy được danh sách ghế đã chọn.
     *
     * @return danh sách các mã ghế đã chọn (ví dụ: ["G01", "G03"])
     */
    public List<String> getSelectedSeats() {
        return new ArrayList<>(selectedSeats);
    }

    /**
     * Cho phép nạp danh sách ghế đã chọn từ bên ngoài (phục vụ điều hướng khứ hồi).
     */
    public void setSelectedSeats(Collection<String> seats) {
        clearSelection();
        if (seats != null) {
            for (String seatId : seats) {
                if (initialSeatStateMap.get(seatId) == SeatMockState.AVAILABLE) {
                    selectedSeats.add(seatId);
                    updateSeatButtonStyle(seatButtons.get(seatId), seatId, SeatMockState.SELECTED_LOCAL);
                }
            }
        }
        updateSelectionUI();
    }

    /**
     * Xóa toàn bộ lựa chọn ghế.
     */
    public void clearSelection() {
        for (String seatId : selectedSeats) {
            Button btn = seatButtons.get(seatId);
            if (btn != null) {
                updateSeatButtonStyle(btn, seatId, SeatMockState.AVAILABLE);
            }
        }
        selectedSeats.clear();
        updateSelectionUI();
    }

    /**
     * Lấy tham chiếu nút ghế (phục vụ kiểm thử / automation).
     */
    public Button getSeatButton(String seatId) {
        return seatButtons.get(seatId);
    }

    // === Callback điều hướng ===

    public void setOnBackAction(Runnable onBackAction) {
        this.onBackAction = onBackAction;
    }

    public void setOnContinueAction(Consumer<List<String>> onContinueAction) {
        this.onContinueAction = onContinueAction;
    }

    @FXML
    private void onBackClicked() {
        if (onBackAction != null) {
            onBackAction.run();
        }
    }

    @FXML
    private void onContinueClicked() {
        if (!selectedSeats.isEmpty() && onContinueAction != null) {
            onContinueAction.accept(getSelectedSeats());
        }
    }

    @FXML
    private void onResetSelectionClicked() {
        clearSelection();
        updateStatus("Đã bỏ chọn tất cả ghế.");
    }

    private void updateStatus(String msg) {
        if (seatStatusLabel != null) {
            seatStatusLabel.setText(msg);
        }
    }
}

