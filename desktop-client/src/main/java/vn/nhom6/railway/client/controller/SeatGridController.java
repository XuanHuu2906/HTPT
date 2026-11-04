package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import vn.nhom6.railway.client.BookingOutcome;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenStateModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Controller quản lý sơ đồ chọn ghế tương tác (TASK D02, D03 & D04).
 * <p>
 * Tính năng nâng cấp D04:
 * - Sử dụng {@link SeatViewModel} và {@link ScreenStateModel} thay vì hardcode màu trong Java.
 * - Phân biệt trực quan 4 trạng thái ghế: AVAILABLE, BOOKED, SELECTED, UNKNOWN (viền nét đứt).
 * - Legend cập nhật số lượng và trạng thái trực tiếp theo dữ liệu mock.
 * - Quản lý trạng thái màn hình: IDLE, LOADING (kèm ProgressIndicator), ERROR, DISCONNECTED.
 * - Xử lý đầy đủ kết quả đặt vé: CONFIRMED (đổi BOOKED), REJECTED (đổi BOOKED), UNKNOWN (đổi UNKNOWN).
 * - Bảng điều khiển MOCK kiểm thử nhanh để xác minh màu và legend tự động cập nhật.
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

    // === Vùng Thông Báo Màn Hình (Screen State Banner) ===
    @FXML
    private HBox statusBannerBox;

    @FXML
    private ProgressIndicator statusProgressIndicator;

    @FXML
    private Label screenStatusLabel;

    // === Legend Chú Giải Trực Quan (4 trạng thái) ===
    @FXML
    private Label legendAvailableLabel;

    @FXML
    private Label legendSelectedLabel;

    @FXML
    private Label legendBookedLabel;

    @FXML
    private Label legendUnknownLabel;

    // === Nút Điều Khiển MOCK (Fast Mock Controls) ===
    @FXML
    private Button mockLoadingBtn;

    @FXML
    private Button mockErrorBtn;

    @FXML
    private Button mockDisconnectedBtn;

    @FXML
    private Button mockIdleBtn;

    @FXML
    private Button mockToggleG04UnknownBtn;

    @FXML
    private Button mockToggleG05BookedBtn;

    @FXML
    private Button mockAll4StatesBtn;

    @FXML
    private Button mockResetDefaultBtn;

    // ViewModel lưu trữ dữ liệu 10 ghế (G01–G10)
    private final Map<String, SeatViewModel> seatViewModels = new LinkedHashMap<>();

    // Bản đồ lưu các nút ghế trên giao diện
    private final Map<String, Button> seatButtons = new HashMap<>();

    // Danh sách các ghế người dùng đang chọn cục bộ (dùng TreeSet để sắp xếp mã ghế tăng dần)
    private final Set<String> selectedSeats = new TreeSet<>();

    // Model quản lý trạng thái màn hình
    private final ScreenStateModel screenStateModel = new ScreenStateModel();

    // Callback điều hướng
    private Runnable onBackAction;
    private Consumer<List<String>> onContinueAction;

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText("MOCK DATA — D04");
        }

        // 1. Khởi tạo dữ liệu ghế bằng SeatViewModel
        initMockData();

        // 2. Dựng lưới 10 ghế trong GridPane (2 hàng x 5 cột)
        buildSeatGrid();

        // 3. Khởi tạo liên kết trạng thái màn hình (ScreenStateModel)
        setupScreenStateBinding();

        // 4. Cập nhật nhãn chọn và chú giải số lượng (Legend)
        updateSelectionUI();
        updateLegendCounts();
    }

    /**
     * Khởi tạo dữ liệu 10 ghế MOCK: G01 đến G10.
     * Mặc định ban đầu: G02, G04, G08 ở trạng thái BOOKED, các ghế còn lại là AVAILABLE.
     */
    private void initMockData() {
        seatViewModels.clear();
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            SeatStatus initialStatus;
            if (i == 2 || i == 4 || i == 8) {
                initialStatus = SeatStatus.BOOKED;
            } else {
                initialStatus = SeatStatus.AVAILABLE;
            }
            seatViewModels.put(seatId, new SeatViewModel(seatId, initialStatus));
        }
    }

    /**
     * Dựng các nút bấm đại diện cho 10 ghế trong GridPane.
     */
    private void buildSeatGrid() {
        if (seatGridPane == null) {
            return;
        }

        seatGridPane.getChildren().clear();
        seatButtons.clear();

        int index = 0;
        for (Map.Entry<String, SeatViewModel> entry : seatViewModels.entrySet()) {
            String seatId = entry.getKey();
            SeatViewModel vm = entry.getValue();

            Button button = new Button();
            button.getStyleClass().add("seat-button");

            // Cập nhật giao diện ban đầu
            updateSeatButtonVisuals(button, seatId, vm.getStatus());

            // Lắng nghe thay đổi trạng thái từ ViewModel để UI tự động cập nhật
            vm.statusProperty().addListener((obs, oldStatus, newStatus) -> {
                updateSeatButtonVisuals(button, seatId, newStatus);
                updateLegendCounts();
            });

            // Gắn sự kiện click ghế
            button.setOnAction(e -> handleSeatClick(seatId));

            seatButtons.put(seatId, button);

            int col = index % 5;
            int row = index / 5;
            seatGridPane.add(button, col, row);
            index++;
        }
    }

    /**
     * Cập nhật style class CSS và nhãn hiển thị cho nút ghế mà KHÔNG hardcode màu trong Java code.
     */
    private void updateSeatButtonVisuals(Button button, String seatId, SeatStatus status) {
        if (button == null || status == null) {
            return;
        }

        button.getStyleClass().removeAll("seat-available", "seat-booked", "seat-selected", "seat-unknown");
        button.getStyleClass().add(status.getStyleClass());
        button.setText(status.formatButtonText(seatId));
    }

    /**
     * Thiết lập lắng nghe thay đổi của ScreenStateModel để cập nhật banner thông báo.
     */
    private void setupScreenStateBinding() {
        // Lắng nghe thay đổi trạng thái
        screenStateModel.stateProperty().addListener((obs, oldState, newState) -> updateScreenStateUI(newState));

        // Lắng nghe thay đổi thông điệp
        screenStateModel.messageProperty().addListener((obs, oldMsg, newMsg) -> {
            if (screenStatusLabel != null) {
                screenStatusLabel.setText(newMsg);
            }
        });

        // Khởi tạo trạng thái ban đầu
        updateScreenStateUI(screenStateModel.getState());
    }

    /**
     * Đồng bộ giao diện thanh trạng thái theo ScreenState.
     */
    private void updateScreenStateUI(ScreenState state) {
        if (statusBannerBox != null) {
            statusBannerBox.getStyleClass().removeAll(
                    "status-idle", "status-loading", "status-error", "status-disconnected"
            );
            statusBannerBox.getStyleClass().add(state.getStyleClass());
        }

        if (statusProgressIndicator != null) {
            boolean isLoading = (state == ScreenState.LOADING);
            statusProgressIndicator.setVisible(isLoading);
            statusProgressIndicator.setManaged(isLoading);
        }

        if (screenStatusLabel != null) {
            screenStatusLabel.setText(screenStateModel.getMessage());
        }
    }

    /**
     * Cập nhật số lượng đếm trên chú giải (Legend) dựa trên dữ liệu ghế thực tế.
     * Minh chứng khi dữ liệu mock thay đổi thì legend tự động cập nhật đúng.
     */
    public void updateLegendCounts() {
        int availableCount = 0;
        int selectedCount = 0;
        int bookedCount = 0;
        int unknownCount = 0;

        for (SeatViewModel vm : seatViewModels.values()) {
            switch (vm.getStatus()) {
                case AVAILABLE -> availableCount++;
                case SELECTED -> selectedCount++;
                case BOOKED -> bookedCount++;
                case UNKNOWN -> unknownCount++;
            }
        }

        if (legendAvailableLabel != null) {
            legendAvailableLabel.setText("Trống (" + availableCount + ")");
        }
        if (legendSelectedLabel != null) {
            legendSelectedLabel.setText("Đang chọn (" + selectedCount + ")");
        }
        if (legendBookedLabel != null) {
            legendBookedLabel.setText("Đã đặt (" + bookedCount + ")");
        }
        if (legendUnknownLabel != null) {
            legendUnknownLabel.setText("Chưa rõ (" + unknownCount + ")");
        }
    }

    /**
     * Xử lý tương tác khi người dùng nhấp vào một ghế.
     */
    public void handleSeatClick(String seatId) {
        SeatViewModel vm = seatViewModels.get(seatId);
        if (vm == null) {
            return;
        }

        SeatStatus currentStatus = vm.getStatus();

        if (currentStatus == SeatStatus.BOOKED) {
            updateStatus("Ghế " + seatId + " đã được đặt trước (BOOKED). Không thể chọn ghế này!");
            return;
        }

        if (currentStatus == SeatStatus.UNKNOWN) {
            updateStatus("Ghế " + seatId + " đang ở trạng thái CHƯA RÕ (UNKNOWN). Vui lòng đợi xác nhận trước khi thao tác!");
            return;
        }

        if (selectedSeats.contains(seatId)) {
            // Hủy chọn
            selectedSeats.remove(seatId);
            vm.setStatus(SeatStatus.AVAILABLE);
            updateStatus("Đã bỏ chọn ghế " + seatId + ".");
        } else {
            // Chọn ghế
            selectedSeats.add(seatId);
            vm.setStatus(SeatStatus.SELECTED);
            updateStatus("Đã chọn ghế " + seatId + ".");
        }

        updateSelectionUI();
    }

    /**
     * Cập nhật nhãn ghế đang chọn và bật/tắt nút "Đặt vé".
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
     * Lấy danh sách các mã ghế người dùng đang chọn.
     */
    public List<String> getSelectedSeats() {
        return new ArrayList<>(selectedSeats);
    }

    /**
     * Thiết lập danh sách ghế đã chọn từ bên ngoài.
     */
    public void setSelectedSeats(Collection<String> seats) {
        clearSelection();
        if (seats != null) {
            for (String seatId : seats) {
                SeatViewModel vm = seatViewModels.get(seatId);
                if (vm != null && vm.getStatus() == SeatStatus.AVAILABLE) {
                    selectedSeats.add(seatId);
                    vm.setStatus(SeatStatus.SELECTED);
                }
            }
        }
        updateSelectionUI();
    }

    /**
     * Bỏ chọn toàn bộ ghế đang chọn.
     */
    public void clearSelection() {
        for (String seatId : new ArrayList<>(selectedSeats)) {
            SeatViewModel vm = seatViewModels.get(seatId);
            if (vm != null && vm.getStatus() == SeatStatus.SELECTED) {
                vm.setStatus(SeatStatus.AVAILABLE);
            }
        }
        selectedSeats.clear();
        updateSelectionUI();
    }

    public Button getSeatButton(String seatId) {
        return seatButtons.get(seatId);
    }

    public SeatViewModel getSeatViewModel(String seatId) {
        return seatViewModels.get(seatId);
    }

    public ScreenStateModel getScreenStateModel() {
        return screenStateModel;
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
        if (selectedSeats.isEmpty()) {
            return;
        }

        openBookingDialog();

        if (onContinueAction != null) {
            onContinueAction.accept(getSelectedSeats());
        }
    }

    /**
     * Mở hộp thoại modal Đặt vé (TASK D03/D04).
     */
    public void openBookingDialog() {
        URL fxmlLocation = getClass().getResource("/fxml/booking-dialog.fxml");
        if (fxmlLocation == null) {
            String errorMsg = "LỖI: Không tìm thấy tệp giao diện /fxml/booking-dialog.fxml!";
            updateStatus(errorMsg);
            System.err.println(errorMsg);
            return;
        }

        try {
            FXMLLoader loader = new FXMLLoader(fxmlLocation);
            Parent dialogRoot = loader.load();
            BookingDialogController dialogController = loader.getController();

            dialogController.setSelectedSeats(getSelectedSeats());

            Stage dialogStage = new Stage();
            dialogStage.setTitle("Xác nhận đặt vé");
            dialogStage.initModality(Modality.APPLICATION_MODAL);

            Window owner = null;
            if (seatGridPane != null && seatGridPane.getScene() != null) {
                owner = seatGridPane.getScene().getWindow();
            }
            if (owner != null) {
                dialogStage.initOwner(owner);
            }

            dialogController.setDialogStage(dialogStage);
            dialogStage.setScene(new Scene(dialogRoot));
            dialogStage.setResizable(false);
            dialogStage.showAndWait();

            // Nhận kết quả sau khi đóng hộp thoại
            Optional<BookingOutcome> outcomeOpt = dialogController.getOutcome();
            if (outcomeOpt.isPresent()) {
                handleBookingOutcome(outcomeOpt.get());
            } else {
                updateStatus("Đã hủy thao tác đặt vé. Trạng thái ghế giữ nguyên.");
            }
        } catch (IOException e) {
            String errorMsg = "Lỗi khi mở form đặt vé: " + e.getMessage();
            updateStatus(errorMsg);
            e.printStackTrace();
        }
    }

    /**
     * Xử lý tác động kết quả đặt vé lên trạng thái ghế theo đặc tả SPEC-06 và D04.
     * <p>
     * Quy tắc nghiệm thu:
     * - CONFIRMED: Ghế chuyển sang BOOKED.
     * - REJECTED (SEAT_ALREADY_BOOKED): Ghế chuyển sang BOOKED, báo lỗi rõ lý do.
     * - UNKNOWN / timeout: Ghế chuyển sang UNKNOWN (viền nét đứt), KHÔNG coi là REJECTED,
     *   KHÔNG tự ý chuyển BOOKED hay AVAILABLE.
     */
    public void handleBookingOutcome(BookingOutcome outcome) {
        if (outcome == null) return;

        List<String> affectedSeats = new ArrayList<>(selectedSeats);

        switch (outcome.getStatus()) {
            case CONFIRMED -> {
                String refCode = outcome.getReferenceCode();
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = seatViewModels.get(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                }
                selectedSeats.clear();
                updateSelectionUI();
                String msg = "Đặt vé thành công! Mã tham chiếu: " + refCode + " (Ghế: " + String.join(", ", affectedSeats) + ")";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.IDLE, msg);
            }
            case REJECTED -> {
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = seatViewModels.get(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                }
                selectedSeats.clear();
                updateSelectionUI();
                String msg = "Ghế đã có người đặt trước (REJECTED: " + outcome.getReasonCode() + "), vui lòng chọn ghế khác.";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.ERROR, msg);
            }
            case UNKNOWN -> {
                // UNKNOWN / timeout: Chuyển ghế sang UNKNOWN, phân biệt rõ với REJECTED
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = seatViewModels.get(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.UNKNOWN);
                    }
                }
                selectedSeats.clear();
                updateSelectionUI();
                String msg = "Chưa xác định kết quả (TIMEOUT/MẤT KẾT NỐI). Ghế đã chuyển sang trạng thái CHƯA RÕ (UNKNOWN)!";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.DISCONNECTED, msg);
            }
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

    // =========================================================================
    // VÙNG ĐIỀU KHIỂN MOCK TEST (FAST MOCK CONTROLS — TASK D04)
    // =========================================================================

    @FXML
    public void onMockLoadingClicked() {
        screenStateModel.setState(ScreenState.LOADING, "Đang đồng bộ dữ liệu ghế từ cụm máy chủ...");
        updateStatus("MOCK: Đã chuyển màn hình sang trạng thái LOADING (đang tải).");
    }

    @FXML
    public void onMockErrorClicked() {
        screenStateModel.setState(ScreenState.ERROR, "Lỗi kết nối cụm Raft Node-02 (500 Internal Error)!");
        updateStatus("MOCK: Đã chuyển màn hình sang trạng thái ERROR (lỗi xử lý).");
    }

    @FXML
    public void onMockDisconnectedClicked() {
        screenStateModel.setState(ScreenState.DISCONNECTED, "Mất kết nối mạng tới cụm Node (Socket Timeout)!");
        updateStatus("MOCK: Đã chuyển màn hình sang trạng thái DISCONNECTED (mất kết nối).");
    }

    @FXML
    public void onMockIdleClicked() {
        screenStateModel.setState(ScreenState.IDLE, "Hệ thống sẵn sàng ở chế độ MOCK (D04).");
        updateStatus("MOCK: Đã chuyển màn hình về trạng thái IDLE (sẵn sàng).");
    }

    @FXML
    public void onMockToggleG04Unknown() {
        SeatViewModel vm = seatViewModels.get("G04");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.UNKNOWN) ? SeatStatus.AVAILABLE : SeatStatus.UNKNOWN;
            vm.setStatus(newStatus);
            selectedSeats.remove("G04");
            updateSelectionUI();
            updateStatus("MOCK: Đã đổi ghế G04 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockToggleG05Booked() {
        SeatViewModel vm = seatViewModels.get("G05");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.BOOKED) ? SeatStatus.AVAILABLE : SeatStatus.BOOKED;
            vm.setStatus(newStatus);
            selectedSeats.remove("G05");
            updateSelectionUI();
            updateStatus("MOCK: Đã đổi ghế G05 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockAll4States() {
        // Thiết lập đồng thời cả 4 trạng thái để kiểm thử và chụp ảnh minh chứng
        clearSelection();

        setSeatStatusHelper("G01", SeatStatus.AVAILABLE);
        setSeatStatusHelper("G02", SeatStatus.BOOKED);
        setSeatStatusHelper("G03", SeatStatus.AVAILABLE);
        setSeatStatusHelper("G04", SeatStatus.UNKNOWN);
        setSeatStatusHelper("G05", SeatStatus.SELECTED);
        setSeatStatusHelper("G06", SeatStatus.AVAILABLE);
        setSeatStatusHelper("G07", SeatStatus.AVAILABLE);
        setSeatStatusHelper("G08", SeatStatus.BOOKED);
        setSeatStatusHelper("G09", SeatStatus.AVAILABLE);
        setSeatStatusHelper("G10", SeatStatus.AVAILABLE);

        selectedSeats.add("G05");
        updateSelectionUI();
        screenStateModel.setState(ScreenState.IDLE, "Đang hiển thị đủ 4 trạng thái ghế: AVAILABLE, BOOKED, SELECTED, UNKNOWN.");
        updateStatus("MOCK: Đã thiết lập đủ 4 trạng thái ghế (G01 AVAILABLE, G02/G08 BOOKED, G05 SELECTED, G04 UNKNOWN).");
    }

    @FXML
    public void onMockResetDefault() {
        clearSelection();
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            SeatViewModel vm = seatViewModels.get(seatId);
            if (vm != null) {
                if (i == 2 || i == 4 || i == 8) {
                    vm.setStatus(SeatStatus.BOOKED);
                } else {
                    vm.setStatus(SeatStatus.AVAILABLE);
                }
            }
        }
        updateSelectionUI();
        screenStateModel.setState(ScreenState.IDLE);
        updateStatus("MOCK: Đã khôi phục dữ liệu ghế MOCK ban đầu.");
    }

    private void setSeatStatusHelper(String seatId, SeatStatus status) {
        SeatViewModel vm = seatViewModels.get(seatId);
        if (vm != null) {
            vm.setStatus(status);
        }
    }
}
