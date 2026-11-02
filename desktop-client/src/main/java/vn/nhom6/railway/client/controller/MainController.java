package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.io.IOException;
import java.util.List;

/**
 * Controller điều khiển giao diện chính của ứng dụng bán vé Desktop (JavaFX).
 * <p>
 * Phạm vi TASK D02:
 * - Hiển thị màn hình chính (Tổng quan chuyến tàu SE-001, thông tin hành trình).
 * - Điều hướng linh hoạt sang sơ đồ chọn ghế tương tác (seat-grid.fxml) bằng cơ chế đổi vùng center.
 * - Có đường quay lại màn hình chính ("Quay lại màn hình chính" / "Chọn chuyến").
 * - Chuyển tiếp sang khu vực đặt vé placeholder (chuẩn bị dữ liệu ghế đã chọn cho TASK D03).
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

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText("MOCK DATA");
        }

        bindNavigationHandlers();
    }

    // =========================================================================
    // ĐIỀU HƯỚNG MÀN HÌNH (NAVIGATION - TASK D02)
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

                // Gắn callback tiếp tục sang vùng đặt vé (Placeholder D03)
                cachedSeatGridController.setOnContinueAction(this::showBookingPlaceholder);
            }

            if (rootBorderPane != null) {
                rootBorderPane.setCenter(cachedSeatGridView);
                highlightNavButton(navSeatBtn);
                updateStatusMessage("Đang ở Sơ đồ chọn ghế tương tác (TASK D02). Click chọn các ghế mong muốn.");
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
     * Hiển thị khu vực đặt vé placeholder (Giai đoạn D03).
     *
     * @param selectedSeats danh sách ghế đã chọn từ SeatGridController
     */
    public void showBookingPlaceholder(List<String> selectedSeats) {
        if (rootBorderPane == null) return;

        VBox placeholderBox = new VBox(16.0);
        placeholderBox.setPadding(new Insets(20));
        placeholderBox.setStyle("-fx-background-color: #f8fafc;");

        // 1. Card tiêu đề & điều hướng quay lại
        HBox topCard = new HBox(15.0);
        topCard.setAlignment(Pos.CENTER_LEFT);
        topCard.setStyle("-fx-background-color: #ffffff; -fx-padding: 14 18; -fx-background-radius: 8; -fx-border-color: #e2e8f0; -fx-border-radius: 8;");

        Button backToSeatBtn = new Button("⬅ Quay lại chọn ghế");
        backToSeatBtn.setStyle("-fx-background-color: #1e293b; -fx-text-fill: #ffffff; -fx-font-size: 12px; -fx-font-weight: bold; -fx-padding: 8 14; -fx-background-radius: 6; -fx-cursor: hand;");
        backToSeatBtn.setOnAction(e -> showSeatGridView());

        Button backToHomeBtn = new Button("🏠 Màn hình chính");
        backToHomeBtn.setStyle("-fx-background-color: #f1f5f9; -fx-text-fill: #475569; -fx-font-size: 12px; -fx-padding: 8 14; -fx-background-radius: 6; -fx-border-color: #cbd5e1; -fx-border-radius: 6; -fx-cursor: hand;");
        backToHomeBtn.setOnAction(e -> showMainView());

        VBox titleBox = new VBox(2.0);
        Label mainTitle = new Label("KHU VỰC ĐẶT VÉ CHI TIẾT");
        mainTitle.setStyle("-fx-font-size: 15px; -fx-font-weight: bold; -fx-text-fill: #0f172a;");
        Label subTitle = new Label("Chuyến tàu SE-001 | Toa 01 - Khoang 10 ghế VIP");
        subTitle.setStyle("-fx-font-size: 12px; -fx-text-fill: #64748b;");
        titleBox.getChildren().addAll(mainTitle, subTitle);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        Label d03Badge = new Label("PLACEHOLDER — TASK D03");
        d03Badge.setStyle("-fx-background-color: #e0e7ff; -fx-text-fill: #3730a3; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 5 12; -fx-background-radius: 6; -fx-border-color: #c7d2fe; -fx-border-radius: 6;");

        topCard.getChildren().addAll(backToSeatBtn, backToHomeBtn, new Separator(), titleBox, spacer, d03Badge);

        // 2. Card thông tin ghế đã chọn
        VBox contentCard = new VBox(16.0);
        contentCard.setPadding(new Insets(20));
        contentCard.setStyle("-fx-background-color: #ffffff; -fx-background-radius: 8; -fx-border-color: #e2e8f0; -fx-border-radius: 8;");
        VBox.setVgrow(contentCard, Priority.ALWAYS);

        Label infoHeader = new Label("DANH SÁCH GHẾ ĐÃ CHỌN TỪ SƠ ĐỒ:");
        infoHeader.setStyle("-fx-font-size: 14px; -fx-font-weight: bold; -fx-text-fill: #0f172a;" );

        HBox seatsDisplayBox = new HBox(10.0);
        seatsDisplayBox.setAlignment(Pos.CENTER_LEFT);
        for (String seatId : selectedSeats) {
            Label seatPill = new Label(seatId);
            seatPill.setStyle("-fx-background-color: #ecfdf5; -fx-text-fill: #065f46; -fx-border-color: #10b981; -fx-border-width: 1.5; -fx-border-radius: 6; -fx-background-radius: 6; -fx-font-weight: bold; -fx-font-size: 14px; -fx-padding: 6 16;");
            seatsDisplayBox.getChildren().add(seatPill);
        }

        Label totalLabel = new Label("Tổng số ghế: " + selectedSeats.size() + " ghế");
        totalLabel.setStyle("-fx-font-size: 13px; -fx-text-fill: #334155; -fx-font-weight: bold;");

        // Hộp thông báo placeholder D03
        VBox noteBox = new VBox(6.0);
        noteBox.setPadding(new Insets(14));
        noteBox.setStyle("-fx-background-color: #eff6ff; -fx-background-radius: 8; -fx-border-color: #bfdbfe; -fx-border-radius: 8;");
        Label noteTitle = new Label("ℹ Ghi chú triển khai (TASK D03 Placeholder):");
        noteTitle.setStyle("-fx-font-weight: bold; -fx-text-fill: #1e40af; -fx-font-size: 13px;");
        Label noteContent = new Label("Form chi tiết thông tin hành khách (Họ tên, CCCD, Đối tượng ưu tiên), tổng tiền vé và gửi lệnh đặt vé xác thực sẽ được hoàn thiện trong TASK D03 theo đặc tả SPEC-06.");
        noteContent.setStyle("-fx-text-fill: #1e3a8a; -fx-font-size: 12px;");
        noteBox.getChildren().addAll(noteTitle, noteContent);

        contentCard.getChildren().addAll(infoHeader, seatsDisplayBox, totalLabel, new Separator(), noteBox);

        placeholderBox.getChildren().addAll(topCard, contentCard);

        rootBorderPane.setCenter(placeholderBox);
        updateStatusMessage("Đã chuyển sang khu vực đặt vé (Placeholder D03). Ghế: " + String.join(", ", selectedSeats));
    }

    public SeatGridController getCachedSeatGridController() {
        return cachedSeatGridController;
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

        setupNavButton(navPaymentBtn, "Thanh toán");
        setupNavButton(navMyTicketsBtn, "Vé của tôi");
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
