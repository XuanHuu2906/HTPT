package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;

import java.io.IOException;

/**
 * Controller điều khiển giao diện chính của ứng dụng bán vé Desktop (JavaFX).
 * <p>
 * Phạm vi TASK D01 - D03:
 * - Hiển thị màn hình chính (Tổng quan chuyến tàu SE-001, thông tin hành trình).
 * - Điều hướng linh hoạt sang sơ đồ chọn ghế tương tác (seat-grid.fxml) bằng cơ chế đổi vùng center.
 * - Có đường quay lại màn hình chính ("Quay lại màn hình chính" / "Chọn chuyến").
 */
public class MainController {

    // === Container gốc BorderPane để hoán đổi nội dung vùng center ===
    @FXML
    private BorderPane rootBorderPane;

    @FXML
    private VBox mainCenterContainer;

    @FXML
    private Button openSeatGridBtn;

    // === Tiêu đề & Nhãn MOCK ===
    @FXML
    private Label tripTitleLabel;

    @FXML
    private Label mockBadgeLabel;

    // === Vùng điều hướng bên trái (Navigation Region) ===
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

    // === Thanh trạng thái bên dưới ===
    @FXML
    private Label statusMessageLabel;

    // Cache giao diện sơ đồ chọn ghế (D02) để bảo lưu trạng thái ghế đang chọn khi quay lại
    private Parent cachedSeatGridView;
    private SeatGridController cachedSeatGridController;

    // Cache giao diện tra cứu & quản lý vé (D06)
    private Parent cachedTicketStatusView;
    private TicketStatusController cachedTicketStatusController;

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText("MOCK DATA");
        }

        bindNavigationHandlers();
    }

    // =========================================================================
    // ĐIỀU HƯỚNG MÀN HÌNH (NAVIGATION)
    // =========================================================================

    /**
     * Chuyển vùng center của BorderPane sang Sơ đồ chọn ghế tương tác (seat-grid.fxml).
     */
    public void showSeatGridView() {
        try {
            if (cachedSeatGridView == null) {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/seat-grid.fxml"));
                cachedSeatGridView = loader.load();
                cachedSeatGridController = loader.getController();

                // Gắn callback quay lại màn hình chính
                cachedSeatGridController.setOnBackAction(this::showMainView);
            }

            if (rootBorderPane != null) {
                rootBorderPane.setCenter(cachedSeatGridView);
                highlightNavButton(navSeatBtn);
                updateStatusMessage("Đang ở Sơ đồ chọn ghế tương tác. Click chọn các ghế mong muốn rồi nhấn 'Đặt vé'.");
            }
        } catch (IOException e) {
            updateStatusMessage("Lỗi khi nạp giao diện seat-grid.fxml: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Quay lại màn hình chính ban đầu (mainCenterContainer).
     */
    public void showMainView() {
        if (rootBorderPane != null && mainCenterContainer != null) {
            rootBorderPane.setCenter(mainCenterContainer);
            highlightNavButton(navTripBtn);
            updateStatusMessage("Đã quay về màn hình chính (Chuyến tàu SE-001).");
        }
    }

    /**
     * Chuyển vùng center của BorderPane sang Màn hình tra cứu & quản lý vé (ticket-status.fxml - D06).
     */
    public void showTicketStatusView() {
        try {
            if (cachedTicketStatusView == null) {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/ticket-status.fxml"));
                cachedTicketStatusView = loader.load();
                cachedTicketStatusController = loader.getController();

                // Gắn callback quay lại màn hình chính
                cachedTicketStatusController.setOnBackAction(this::showMainView);
            }

            if (cachedTicketStatusController != null) {
                cachedTicketStatusController.refreshTicketList();
            }

            if (rootBorderPane != null) {
                rootBorderPane.setCenter(cachedTicketStatusView);
                highlightNavButton(navMyTicketsBtn);
                updateStatusMessage("Đang ở màn hình Tra cứu & Quản lý vé MOCK (D06).");
            }
        } catch (IOException e) {
            updateStatusMessage("Lỗi khi nạp giao diện ticket-status.fxml: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public SeatGridController getCachedSeatGridController() {
        return cachedSeatGridController;
    }

    public TicketStatusController getCachedTicketStatusController() {
        return cachedTicketStatusController;
    }

    // =========================================================================
    // EVENT HANDLERS
    // =========================================================================

    private void bindNavigationHandlers() {
        // Nút điều hướng chính trên màn hình chính
        if (openSeatGridBtn != null) {
            openSeatGridBtn.setOnAction(e -> showSeatGridView());
        }

        // Nút trên menu bên trái
        if (navSeatBtn != null) {
            navSeatBtn.setOnAction(e -> showSeatGridView());
        }

        if (navTripBtn != null) {
            navTripBtn.setOnAction(e -> showMainView());
        }

        if (navMyTicketsBtn != null) {
            navMyTicketsBtn.setOnAction(e -> showTicketStatusView());
        }

        setupNavButton(navPaymentBtn, "Thanh toán");
    }

    private void highlightNavButton(Button activeBtn) {
        String defaultStyle = "-fx-background-color: #1e293b; -fx-text-fill: #cbd5e1; -fx-font-size: 13px; -fx-padding: 10 12; -fx-background-radius: 6; -fx-cursor: hand;";
        String activeStyle = "-fx-background-color: #3b82f6; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 13px; -fx-padding: 10 12; -fx-background-radius: 6; -fx-cursor: hand;";

        if (navTripBtn != null) navTripBtn.setStyle(navTripBtn == activeBtn ? activeStyle : defaultStyle);
        if (navSeatBtn != null) navSeatBtn.setStyle(navSeatBtn == activeBtn ? activeStyle : defaultStyle);
        if (navPaymentBtn != null) navPaymentBtn.setStyle(navPaymentBtn == activeBtn ? activeStyle : defaultStyle);
        if (navMyTicketsBtn != null) navMyTicketsBtn.setStyle(navMyTicketsBtn == activeBtn ? activeStyle : defaultStyle);
    }

    private void setupNavButton(Button btn, String navName) {
        if (btn != null) {
            btn.setOnAction(e -> updateStatusMessage("Điều hướng: " + navName + " (Placeholder)"));
        }
    }

    public void updateStatusMessage(String message) {
        if (statusMessageLabel != null) {
            statusMessageLabel.setText(message);
        }
    }
}
