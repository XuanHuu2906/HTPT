package vn.nhom6.railway.client.controller;

import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.binding.StringBinding;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import vn.nhom6.railway.client.BookingOutcome;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.MockSeatSource;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenStateModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Controller quản lý sơ đồ chọn ghế tương tác (TASK D02, D03, D04 & D05).
 * <p>
 * Tính năng nâng cấp D05:
 * - Sinh các ô ghế tự động bằng vòng lặp từ {@link ObservableList} của {@link SeatViewModel},
 *   không khai báo hoặc hardcode từng button G01..G10.
 * - Danh sách ghế đang chọn được suy ra tự động bằng {@link FilteredList} từ ViewModel,
 *   không duy trì bản sao thủ công dễ lệch dữ liệu.
 * - Nút "Tiếp tục" (Đặt vé) tự động bind disableProperty theo `Bindings.isEmpty(selectedSeatsList)`.
 * - Nhãn danh sách ghế chọn và 4 nhãn Legend (AVAILABLE, BOOKED, SELECTED, UNKNOWN)
 *   tự động bind vào ViewModel và tự re-evaluate khi dữ liệu thay đổi.
 * - Mỗi nút ghế bind trực tiếp vào ViewModel:
 *   + textProperty bind vào displayTextProperty.
 *   + disableProperty bind vào bookedProperty (disabled khi BOOKED).
 *   + Style class CSS cập nhật qua listener trên statusProperty.
 * - Click ghế chỉ gọi vào ViewModel (setStatus), UI tự động cập nhật nhờ binding.
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

    // === Nút & ComboBox Điều Khiển MOCK (Fast Mock Controls — TASK D05) ===
    @FXML
    private Button mockLoadingBtn;

    @FXML
    private Button mockErrorBtn;

    @FXML
    private Button mockDisconnectedBtn;

    @FXML
    private Button mockIdleBtn;

    @FXML
    private ComboBox<String> mockSeatComboBox;

    @FXML
    private ComboBox<SeatStatus> mockStatusComboBox;

    @FXML
    private Button mockApplySeatStatusBtn;

    @FXML
    private Button mockToggleG03BookedBtn;

    @FXML
    private Button mockRefreshSnapshotBtn;

    @FXML
    private Button mockAll4StatesBtn;

    @FXML
    private Button mockResetDefaultBtn;

    // Nguồn dữ liệu ObservableList 10 ghế (G01–G10) sinh từ MockSeatSource duy nhất
    private final ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();

    // Danh sách ghế đang chọn được suy ra trực tiếp từ seats qua FilteredList (không giữ bản sao thủ công)
    private final FilteredList<SeatViewModel> selectedSeatsList = new FilteredList<>(seats, SeatViewModel::isSelected);

    // Bản đồ lưu tham chiếu các nút ghế trên giao diện
    private final Map<String, Button> seatButtons = new HashMap<>();

    // Model quản lý trạng thái màn hình
    private final ScreenStateModel screenStateModel = new ScreenStateModel();

    // Reactive Properties lưu trữ số lượng ghế cho Legend binding (TASK D05)
    private final IntegerProperty availableCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty selectedCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty bookedCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty unknownCountProperty = new SimpleIntegerProperty(0);

    // Lưu trữ tham chiếu Binding để tránh bị garbage collection
    private StringBinding selectedSeatsSummaryBinding;
    private BooleanBinding continueBtnDisableBinding;

    // Callback điều hướng
    private Runnable onBackAction;
    private Consumer<List<String>> onContinueAction;

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText(MockSeatSource.MOCK_LABEL);
        }

        // 1. Lắng nghe thay đổi danh sách ObservableList để refresh lưới nếu cấu trúc thay đổi
        setupSeatsListListener();

        // 2. Dựng lưới ghế tự động bằng vòng lặp và thiết lập data binding
        buildSeatGrid();

        // 3. Khởi tạo liên kết trạng thái màn hình (ScreenStateModel)
        setupScreenStateBinding();

        // 4. Thiết lập Data Binding cho Legend, Nút Đặt vé và Danh sách ghế chọn
        setupReactiveBindings();

        // 5. Khởi tạo danh sách lựa chọn cho vùng điều khiển MOCK
        initMockControls();
    }

    /**
     * Khởi tạo dữ liệu cho ComboBox điều khiển ghế MOCK.
     */
    private void initMockControls() {
        if (mockSeatComboBox != null) {
            mockSeatComboBox.getItems().setAll(
                    seats.stream().map(SeatViewModel::getSeatId).toList()
            );
            mockSeatComboBox.setValue("G03");
        }

        if (mockStatusComboBox != null) {
            mockStatusComboBox.getItems().setAll(SeatStatus.values());
            mockStatusComboBox.setValue(SeatStatus.BOOKED);
        }
    }

    /**
     * Thiết lập các reactive data bindings cho Legend, Nhãn tóm tắt và Nút Đặt vé.
     */
    private void setupReactiveBindings() {
        // 1. Bind nhãn danh sách ghế đang chọn
        selectedSeatsSummaryBinding = Bindings.createStringBinding(() -> {
            if (selectedSeatsList.isEmpty()) {
                return "Ghế đang chọn: (Chưa chọn ghế nào)";
            }
            List<String> sortedIds = selectedSeatsList.stream()
                    .map(SeatViewModel::getSeatId)
                    .sorted()
                    .toList();
            return "Ghế đang chọn: " + String.join(", ", sortedIds) + " (" + sortedIds.size() + " ghế)";
        }, selectedSeatsList);

        if (selectedSeatsLabel != null) {
            selectedSeatsLabel.textProperty().bind(selectedSeatsSummaryBinding);
        }

        // 2. Bind disableProperty của nút Tiếp tục (Đặt vé)
        continueBtnDisableBinding = Bindings.isEmpty(selectedSeatsList);
        if (continueBtn != null) {
            continueBtn.disableProperty().bind(continueBtnDisableBinding);
        }

        // 3. Bind 4 nhãn Legend trực tiếp vào các reactive IntegerProperties
        if (legendAvailableLabel != null) {
            legendAvailableLabel.textProperty().bind(Bindings.concat("Trống (", availableCountProperty.asString(), ")"));
        }
        if (legendSelectedLabel != null) {
            legendSelectedLabel.textProperty().bind(Bindings.concat("Đang chọn (", selectedCountProperty.asString(), ")"));
        }
        if (legendBookedLabel != null) {
            legendBookedLabel.textProperty().bind(Bindings.concat("Đã đặt (", bookedCountProperty.asString(), ")"));
        }
        if (legendUnknownLabel != null) {
            legendUnknownLabel.textProperty().bind(Bindings.concat("Chưa rõ (", unknownCountProperty.asString(), ")"));
        }

        updateLegendCounts();
    }

    /**
     * Lắng nghe thay đổi của ObservableList để refresh lưới ghế tự động khi danh sách thêm/bớt.
     */
    private void setupSeatsListListener() {
        seats.addListener((ListChangeListener<SeatViewModel>) change -> {
            boolean needsRebuild = false;
            while (change.next()) {
                if (change.wasAdded() || change.wasRemoved() || change.wasPermutated()) {
                    needsRebuild = true;
                }
            }
            if (needsRebuild) {
                buildSeatGrid();
            }
            updateLegendCounts();
        });
    }

    /**
     * Dựng các ô ghế tự động bằng vòng lặp từ ObservableList<SeatViewModel>.
     * KHÔNG khai báo hoặc hardcode từng button G01..G10.
     */
    private void buildSeatGrid() {
        if (seatGridPane == null) {
            return;
        }

        seatGridPane.getChildren().clear();
        seatButtons.clear();

        for (int index = 0; index < seats.size(); index++) {
            SeatViewModel vm = seats.get(index);
            String seatId = vm.getSeatId();

            Button button = new Button();
            button.getStyleClass().add("seat-button");

            // 1. Bind văn bản hiển thị trên nút tự động theo displayTextProperty của ViewModel
            button.textProperty().bind(vm.displayTextProperty());

            // 2. Bind disable trạng thái: disabled tự động khi ghế đã BOOKED
            button.disableProperty().bind(vm.bookedProperty());

            // 3. Khởi tạo và lắng nghe thay đổi style class CSS qua statusProperty
            updateSeatButtonStyleClass(button, vm.getStatus());
            vm.statusProperty().addListener((obs, oldStatus, newStatus) -> {
                updateSeatButtonStyleClass(button, newStatus);
                updateLegendCounts();
            });

            // 4. Click ghế: Controller chỉ gọi ViewModel để đổi status (AVAILABLE ⇄ SELECTED)
            button.setOnAction(e -> handleSeatClick(vm));

            seatButtons.put(seatId, button);

            int col = index % 5;
            int row = index / 5;
            seatGridPane.add(button, col, row);
        }
        updateLegendCounts();
    }

    /**
     * Cập nhật style class CSS cho nút ghế từ CSS railway.css (không hardcode màu trong Java).
     */
    private void updateSeatButtonStyleClass(Button button, SeatStatus status) {
        if (button == null || status == null) {
            return;
        }

        button.getStyleClass().removeAll("seat-available", "seat-booked", "seat-selected", "seat-unknown");
        button.getStyleClass().add(status.getStyleClass());
    }

    /**
     * Cập nhật các IntegerProperty đếm số lượng ghế theo 4 trạng thái để Legend tự động phản hồi (TASK D05).
     */
    public void updateLegendCounts() {
        int avail = 0;
        int sel = 0;
        int book = 0;
        int unk = 0;

        for (SeatViewModel vm : seats) {
            switch (vm.getStatus()) {
                case AVAILABLE -> avail++;
                case SELECTED -> sel++;
                case BOOKED -> book++;
                case UNKNOWN -> unk++;
            }
        }

        availableCountProperty.set(avail);
        selectedCountProperty.set(sel);
        bookedCountProperty.set(book);
        unknownCountProperty.set(unk);
    }

    /**
     * Thiết lập lắng nghe thay đổi của ScreenStateModel để cập nhật banner thông báo.
     */
    private void setupScreenStateBinding() {
        screenStateModel.stateProperty().addListener((obs, oldState, newState) -> updateScreenStateUI(newState));

        screenStateModel.messageProperty().addListener((obs, oldMsg, newMsg) -> {
            if (screenStatusLabel != null) {
                screenStatusLabel.setText(newMsg);
            }
        });

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
     * Xử lý tương tác khi người dùng nhấp vào một ghế.
     * Controller chỉ gọi vào ViewModel để đổi trạng thái, UI tự động cập nhật qua bindings.
     */
    public void handleSeatClick(SeatViewModel vm) {
        if (vm == null) {
            return;
        }

        SeatStatus currentStatus = vm.getStatus();

        if (currentStatus == SeatStatus.BOOKED) {
            updateStatus("Ghế " + vm.getSeatId() + " đã được đặt trước (BOOKED). Không thể chọn ghế này!");
            return;
        }

        if (currentStatus == SeatStatus.UNKNOWN) {
            updateStatus("Ghế " + vm.getSeatId() + " đang ở trạng thái CHƯA RÕ (UNKNOWN). Vui lòng đợi xác nhận trước khi thao tác!");
            return;
        }

        if (currentStatus == SeatStatus.SELECTED) {
            // Hủy chọn: chuyển về AVAILABLE trong model, UI và FilteredList tự cập nhật
            vm.setStatus(SeatStatus.AVAILABLE);
            updateStatus("Đã bỏ chọn ghế " + vm.getSeatId() + ".");
        } else if (currentStatus == SeatStatus.AVAILABLE) {
            // Chọn ghế: chuyển sang SELECTED trong model, UI và FilteredList tự cập nhật
            vm.setStatus(SeatStatus.SELECTED);
            updateStatus("Đã chọn ghế " + vm.getSeatId() + ".");
        }
    }

    public void handleSeatClick(String seatId) {
        handleSeatClick(getSeatViewModel(seatId));
    }

    /**
     * Lấy danh sách các mã ghế người dùng đang chọn suy ra từ FilteredList.
     */
    public List<String> getSelectedSeats() {
        return selectedSeatsList.stream()
                .map(SeatViewModel::getSeatId)
                .sorted()
                .toList();
    }

    /**
     * Thiết lập danh sách ghế đã chọn từ bên ngoài bằng cách cập nhật model.
     */
    public void setSelectedSeats(Collection<String> seatIds) {
        clearSelection();
        if (seatIds != null) {
            for (String seatId : seatIds) {
                SeatViewModel vm = getSeatViewModel(seatId);
                if (vm != null && vm.isAvailable()) {
                    vm.setStatus(SeatStatus.SELECTED);
                }
            }
        }
    }

    /**
     * Bỏ chọn toàn bộ ghế đang chọn bằng cách chuyển status về AVAILABLE trong model.
     */
    public void clearSelection() {
        for (SeatViewModel vm : new ArrayList<>(selectedSeatsList)) {
            vm.setStatus(SeatStatus.AVAILABLE);
        }
    }

    public Button getSeatButton(String seatId) {
        return seatButtons.get(seatId);
    }

    public SeatViewModel getSeatViewModel(String seatId) {
        if (seatId == null) return null;
        for (SeatViewModel vm : seats) {
            if (seatId.equals(vm.getSeatId())) {
                return vm;
            }
        }
        return null;
    }

    public ObservableList<SeatViewModel> getSeats() {
        return seats;
    }

    public FilteredList<SeatViewModel> getSelectedSeatsList() {
        return selectedSeatsList;
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
        if (selectedSeatsList.isEmpty()) {
            return;
        }

        openBookingDialog();

        if (onContinueAction != null) {
            onContinueAction.accept(getSelectedSeats());
        }
    }

    /**
     * Mở hộp thoại modal Đặt vé (TASK D03/D04/D05).
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
     * Xử lý tác động kết quả đặt vé lên trạng thái ghế theo đặc tả SPEC-06 và D05.
     * Cập nhật trạng thái trực tiếp qua ViewModel, UI và binding tự động đồng bộ.
     */
    public void handleBookingOutcome(BookingOutcome outcome) {
        if (outcome == null) return;

        List<String> affectedSeats = getSelectedSeats();

        switch (outcome.getStatus()) {
            case CONFIRMED -> {
                String refCode = outcome.getReferenceCode();
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                }
                String msg = "Đặt vé thành công! Mã tham chiếu: " + refCode + " (Ghế: " + String.join(", ", affectedSeats) + ")";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.IDLE, msg);
            }
            case REJECTED -> {
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                }
                String msg = "Ghế đã có người đặt trước (REJECTED: " + outcome.getReasonCode() + "), vui lòng chọn ghế khác.";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.ERROR, msg);
            }
            case UNKNOWN -> {
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.UNKNOWN);
                    }
                }
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
    // VÙNG ĐIỀU KHIỂN MOCK TEST (FAST MOCK CONTROLS — TASK D04/D05)
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
        screenStateModel.setState(ScreenState.IDLE, "Hệ thống sẵn sàng ở chế độ MOCK (D05).");
        updateStatus("MOCK: Đã chuyển màn hình về trạng thái IDLE (sẵn sàng).");
    }

    @FXML
    public void onMockApplySeatStatus() {
        if (mockSeatComboBox == null || mockStatusComboBox == null) {
            return;
        }
        String seatId = mockSeatComboBox.getValue();
        SeatStatus targetStatus = mockStatusComboBox.getValue();
        if (seatId != null && targetStatus != null) {
            SeatViewModel vm = getSeatViewModel(seatId);
            if (vm != null) {
                vm.setStatus(targetStatus);
                updateStatus("MOCK: Đã đổi trạng thái ghế " + seatId + " sang " + targetStatus + " trong model.");
            }
        }
    }

    @FXML
    public void onMockToggleG03Booked() {
        SeatViewModel vm = getSeatViewModel("G03");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.BOOKED) ? SeatStatus.AVAILABLE : SeatStatus.BOOKED;
            vm.setStatus(newStatus);
            updateStatus("MOCK: Đã đổi ghế G03 sang trạng thái " + newStatus.name() + " trong model.");
        }
    }

    @FXML
    public void onMockRefreshSnapshotClicked() {
        clearSelection();
        SeatViewModel.applySnapshot(seats, MockSeatSource.createRefreshedMockSnapshot());
        screenStateModel.setState(ScreenState.IDLE, "MOCK: Đã refresh dữ liệu từ MockSeatSource (G01/G05 BOOKED, G04 AVAILABLE, G09 UNKNOWN).");
        updateStatus("MOCK: Đã gọi applySnapshot() refresh dữ liệu MOCK thành công.");
    }

    @FXML
    public void onMockToggleG04Unknown() {
        SeatViewModel vm = getSeatViewModel("G04");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.UNKNOWN) ? SeatStatus.AVAILABLE : SeatStatus.UNKNOWN;
            vm.setStatus(newStatus);
            updateStatus("MOCK: Đã đổi ghế G04 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockToggleG05Booked() {
        SeatViewModel vm = getSeatViewModel("G05");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.BOOKED) ? SeatStatus.AVAILABLE : SeatStatus.BOOKED;
            vm.setStatus(newStatus);
            updateStatus("MOCK: Đã đổi ghế G05 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockAll4States() {
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

        screenStateModel.setState(ScreenState.IDLE, "Đang hiển thị đủ 4 trạng thái ghế: AVAILABLE, BOOKED, SELECTED, UNKNOWN.");
        updateStatus("MOCK: Đã thiết lập đủ 4 trạng thái ghế (G01 AVAILABLE, G02/G08 BOOKED, G05 SELECTED, G04 UNKNOWN).");
    }

    @FXML
    public void onMockResetDefault() {
        clearSelection();
        SeatViewModel.applySnapshot(seats, MockSeatSource.createInitialSnapshot());
        screenStateModel.setState(ScreenState.IDLE);
        updateStatus("MOCK: Đã khôi phục dữ liệu ghế MOCK ban đầu.");
    }

    private void setSeatStatusHelper(String seatId, SeatStatus status) {
        SeatViewModel vm = getSeatViewModel(seatId);
        if (vm != null) {
            vm.setStatus(status);
        }
    }
}
