package vn.nhom6.railway.client.controller;

import javafx.beans.binding.Bindings;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import vn.nhom6.railway.client.BookingOutcome;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.SeatsRefreshService;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.DevSupportBridge;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenStateModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Controller quản lý sơ đồ chọn ghế tương tác 10 ghế G01–G10 (TASK D02..D28).
 * <p>
 * Bất biến chế độ RELEASE / SOCKET:
 * <ul>
 *   <li>Không chứa bất kỳ tham chiếu biên dịch nào tới các lớp giả lập phát triển.</li>
 *   <li>Khởi tạo 10 ghế ở trạng thái {@code UNKNOWN} (chưa xác minh) trước khi nhận {@code GET_SEATS}.</li>
 *   <li>Loại bỏ hoàn toàn các phần tử giao diện kiểm thử nội bộ khỏi cây Scene Graph khi ở chế độ Socket/Release.</li>
 * </ul>
 */
public class SeatGridController {

    private static final System.Logger LOGGER = System.getLogger(SeatGridController.class.getName());

    @FXML
    private VBox seatGridRoot;

    @FXML
    private VBox mockControlPanel;

    @FXML
    private Label modeBadgeLabel;

    @FXML
    private Label mockBadgeLabel;

    @FXML
    private Label verificationBadgeLabel;

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

    @FXML
    private HBox statusBannerBox;

    @FXML
    private ProgressIndicator statusProgressIndicator;

    @FXML
    private Label screenStatusLabel;

    @FXML
    private Label legendAvailableLabel;

    @FXML
    private Label legendSelectedLabel;

    @FXML
    private Label legendBookedLabel;

    @FXML
    private Label legendUnknownLabel;

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

    private final ObservableList<SeatViewModel> seats = createInitialSeatsByConfig();

    private static ObservableList<SeatViewModel> createInitialSeatsByConfig() {
        if (ClientConfig.isReleaseModeActive()) {
            return SeatViewModel.createInitialUnverifiedSeats();
        }
        try {
            ClientConfig cfg = ClientConfig.loadDefault();
            if (cfg.isReleaseMode() || cfg.isSocketMode()) {
                return SeatViewModel.createInitialUnverifiedSeats();
            }
        } catch (Exception ignored) {
            return SeatViewModel.createInitialUnverifiedSeats();
        }
        return DevSupportBridge.get().createDevMockSeats();
    }

    private final FilteredList<SeatViewModel> selectedSeatsList = new FilteredList<>(seats, SeatViewModel::isSelected);
    private final Map<String, Button> seatButtons = new HashMap<>();
    private final ScreenStateModel screenStateModel = new ScreenStateModel();

    private final IntegerProperty availableCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty selectedCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty bookedCountProperty = new SimpleIntegerProperty(0);
    private final IntegerProperty unknownCountProperty = new SimpleIntegerProperty(0);

    private final StringProperty selectedSeatsSummaryProperty = new SimpleStringProperty(
            "Ghế đang chọn: (Chưa chọn ghế nào)");
    private final BooleanProperty hasSelectedSeatsProperty = new SimpleBooleanProperty(false);

    private SeatsRefreshService seatsRefreshService;

    private Runnable onBackAction;
    private Consumer<List<String>> onContinueAction;

    @FXML
    public void initialize() {
        ClientConfig cfg = ClientConfig.loadDefault();
        boolean realFlow = cfg.isReleaseMode() || cfg.isSocketMode();

        if (realFlow) {
            removeNodeFromParent(mockBadgeLabel);
            removeNodeFromParent(mockControlPanel);
            if (modeBadgeLabel != null) {
                modeBadgeLabel.setStyle(null);
                modeBadgeLabel.getStyleClass().removeAll("badge-mock", "badge-cluster-mode", "badge-release-mode");
                if (cfg.isReleaseMode()) {
                    modeBadgeLabel.setText("RELEASE (" + cfg.getProfile().toUpperCase() + ")");
                    modeBadgeLabel.getStyleClass().add("badge-release-mode");
                } else {
                    modeBadgeLabel.setText("CỤM 5 NODE (" + cfg.getProfile().toUpperCase() + ")");
                    modeBadgeLabel.getStyleClass().add("badge-cluster-mode");
                }
            }
            if (screenStatusLabel != null) {
                screenStatusLabel.setText("Hệ thống kết nối Cụm 5 Node (" + cfg.getProfile().toUpperCase() + ") qua TCP Socket.");
            }
        } else {
            removeNodeFromParent(modeBadgeLabel);
            if (mockBadgeLabel != null) {
                mockBadgeLabel.setStyle(null);
                mockBadgeLabel.getStyleClass().removeAll("badge-cluster-mode", "badge-release-mode");
                if (!mockBadgeLabel.getStyleClass().contains("badge-mock")) {
                    mockBadgeLabel.getStyleClass().add("badge-mock");
                }
                mockBadgeLabel.setText(DevSupportBridge.get().getMockSeatBadgeLabel());
            }
        }

        setupSeatsListListener();
        buildSeatGrid();
        setupScreenStateBinding();
        setupReactiveBindings();
        initBookingAndDevControls(realFlow);

        initSeatsRefreshService();
        if (realFlow && seatsRefreshService != null) {
            seatsRefreshService.refreshNow();
        }
    }

    private static void removeNodeFromParent(Node node) {
        if (node != null && node.getParent() instanceof Pane parentPane) {
            parentPane.getChildren().remove(node);
        }
    }

    private void initBookingAndDevControls(boolean realFlow) {
        if (!realFlow) {
            if (mockSeatComboBox != null) {
                mockSeatComboBox.getItems().setAll(seats.stream().map(SeatViewModel::getSeatId).toList());
                mockSeatComboBox.setValue("G03");
            }
            if (mockStatusComboBox != null) {
                mockStatusComboBox.getItems().setAll(SeatStatus.values());
                mockStatusComboBox.setValue(SeatStatus.BOOKED);
            }
            if (mockControlPanel != null) {
                DevSupportBridge.get().attachDevScenarioRow(mockControlPanel, this::updateStatus);
            }
        }

        BookingViewModel bookingVm = ClientFactory.getBookingViewModel();
        bookingVm.uiStateProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal == null) {
                return;
            }
            switch (newVal) {
                case LOADING, SUBMITTING -> screenStateModel.setState(ScreenState.LOADING, bookingVm.getMessage());
                case ERROR, REJECTED -> screenStateModel.setState(ScreenState.ERROR, bookingVm.getMessage());
                case DISCONNECTED, UNKNOWN ->
                    screenStateModel.setState(ScreenState.DISCONNECTED, bookingVm.getMessage());
                case SUCCESS, CONFIRMED, IDLE -> screenStateModel.setState(ScreenState.IDLE, bookingVm.getMessage());
            }
        });
    }

    private void initSeatsRefreshService() {
        if (seatsRefreshService == null) {
            seatsRefreshService = ClientFactory.getSeatsRefreshService(seats);
        }
        bindRefreshService();
        BookingViewModel bvm = ClientFactory.getBookingViewModel();
        if (bvm != null) {
            bvm.setSeatsList(seats);
            bvm.setSeatsRefreshService(seatsRefreshService);
        }
    }

    public SeatsRefreshService getSeatsRefreshService() {
        if (seatsRefreshService == null) {
            seatsRefreshService = ClientFactory.getSeatsRefreshService(seats);
        }
        return seatsRefreshService;
    }

    public void setSeatsRefreshService(SeatsRefreshService service) {
        this.seatsRefreshService = service;
        bindRefreshService();
    }

    private void bindRefreshService() {
        if (seatsRefreshService == null) {
            return;
        }

        if (verificationBadgeLabel != null) {
            verificationBadgeLabel.textProperty().bind(
                    Bindings.createStringBinding(
                            () -> "[" + seatsRefreshService.getVerification().getDisplayName() + "]",
                            seatsRefreshService.verificationProperty()));
            seatsRefreshService.verificationProperty().addListener((obs, oldVal, newVal) -> {
                if (newVal != null) {
                    verificationBadgeLabel.getStyleClass().removeAll("badge-unverified", "badge-verified");
                    verificationBadgeLabel.getStyleClass().add(newVal.getStyleClass());
                }
            });
            verificationBadgeLabel.getStyleClass().removeAll("badge-unverified", "badge-verified");
            verificationBadgeLabel.getStyleClass().add(seatsRefreshService.getVerification().getStyleClass());
        }

        seatsRefreshService.hasConflictProperty().addListener((obs, oldVal, hasConflict) -> {
            if (Boolean.TRUE.equals(hasConflict)) {
                String warn = seatsRefreshService.getConflictWarning();
                screenStateModel.setState(ScreenState.ERROR, "⚠ XUNG ĐỘT: " + warn);
                updateSelectedSeatsUI();
                updateLegendCounts();
            }
        });

        seatsRefreshService.refreshingProperty().addListener((obs, oldVal, refreshing) -> {
            if (Boolean.TRUE.equals(refreshing)) {
                screenStateModel.setState(ScreenState.LOADING, seatsRefreshService.getStatusMessage());
            } else {
                var snap = seatsRefreshService.getLatestSnapshot();
                if (snap != null && snap.isUnavailable()) {
                    screenStateModel.setState(ScreenState.DISCONNECTED, seatsRefreshService.getStatusMessage());
                } else if (snap != null && !snap.isSuccess()) {
                    screenStateModel.setState(ScreenState.ERROR, seatsRefreshService.getStatusMessage());
                } else {
                    screenStateModel.setState(ScreenState.IDLE, seatsRefreshService.getStatusMessage());
                }
            }
            updateLegendCounts();
            updateSelectedSeatsUI();
        });
    }

    private void setupReactiveBindings() {
        if (selectedSeatsLabel != null) {
            selectedSeatsLabel.textProperty().bind(selectedSeatsSummaryProperty);
        }

        BookingViewModel bvm = ClientFactory.getBookingViewModel();
        if (continueBtn != null) {
            if (bvm != null) {
                continueBtn.disableProperty().bind(hasSelectedSeatsProperty.not().or(bvm.busyProperty()));
            } else {
                continueBtn.disableProperty().bind(hasSelectedSeatsProperty.not());
            }
        }
        if (seatGridPane != null && bvm != null) {
            seatGridPane.disableProperty().bind(bvm.busyProperty());
        }
        if (backBtn != null && bvm != null) {
            backBtn.disableProperty().bind(bvm.busyProperty());
        }

        if (legendAvailableLabel != null) {
            legendAvailableLabel.textProperty()
                    .bind(Bindings.concat("Trống (", availableCountProperty.asString(), ")"));
        }
        if (legendSelectedLabel != null) {
            legendSelectedLabel.textProperty()
                    .bind(Bindings.concat("Đang chọn (", selectedCountProperty.asString(), ")"));
        }
        if (legendBookedLabel != null) {
            legendBookedLabel.textProperty().bind(Bindings.concat("Đã đặt (", bookedCountProperty.asString(), ")"));
        }
        if (legendUnknownLabel != null) {
            legendUnknownLabel.textProperty().bind(Bindings.concat("Chưa rõ (", unknownCountProperty.asString(), ")"));
        }

        updateLegendCounts();
        updateSelectedSeatsUI();
    }

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
            updateSelectedSeatsUI();
        });
    }

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
            button.textProperty().bind(vm.displayTextProperty());
            button.disableProperty().bind(vm.bookedProperty());

            updateSeatButtonStyleClass(button, vm.getStatus());
            vm.statusProperty().addListener((obs, oldStatus, newStatus) -> {
                updateSeatButtonStyleClass(button, newStatus);
                updateLegendCounts();
                updateSelectedSeatsUI();
            });

            button.setOnAction(e -> handleSeatClick(vm));

            seatButtons.put(seatId, button);

            int col = index % 5;
            int row = index / 5;
            seatGridPane.add(button, col, row);
        }
        updateLegendCounts();
        updateSelectedSeatsUI();
    }

    private void updateSeatButtonStyleClass(Button button, SeatStatus status) {
        if (button == null || status == null) {
            return;
        }
        button.getStyleClass().removeAll("seat-available", "seat-booked", "seat-selected", "seat-unknown");
        button.getStyleClass().add(status.getStyleClass());
    }

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

    public void updateSelectedSeatsUI() {
        List<String> sortedIds = seats.stream()
                .filter(SeatViewModel::isSelected)
                .map(SeatViewModel::getSeatId)
                .sorted()
                .toList();

        if (sortedIds.isEmpty()) {
            selectedSeatsSummaryProperty.set("Ghế đang chọn: (Chưa chọn ghế nào)");
            hasSelectedSeatsProperty.set(false);
        } else {
            selectedSeatsSummaryProperty
                    .set("Ghế đang chọn: " + String.join(", ", sortedIds) + " (" + sortedIds.size() + " ghế)");
            hasSelectedSeatsProperty.set(true);
        }
    }

    private void setupScreenStateBinding() {
        screenStateModel.stateProperty().addListener((obs, oldState, newState) -> updateScreenStateUI(newState));

        screenStateModel.messageProperty().addListener((obs, oldMsg, newMsg) -> {
            if (screenStatusLabel != null) {
                screenStatusLabel.setText(newMsg);
            }
        });

        ReconnectController rc = ClientFactory.getReconnectController();
        if (rc != null) {
            rc.bannerMessageProperty().addListener((obs, oldVal, newVal) -> {
                if (rc.isBannerVisible() && newVal != null && !newVal.isBlank()) {
                    screenStateModel.setMessage(newVal);
                }
            });
            rc.stateProperty().addListener((obs, oldState, newState) -> {
                if (newState == ReconnectController.ReconnectState.SEARCHING_LEADER) {
                    screenStateModel.setState(ScreenState.LOADING, rc.getBannerMessage());
                } else if (newState == ReconnectController.ReconnectState.NO_LEADER) {
                    screenStateModel.setState(ScreenState.DISCONNECTED, rc.getBannerMessage());
                } else if (newState == ReconnectController.ReconnectState.CONNECTED
                        && oldState != ReconnectController.ReconnectState.CONNECTED) {
                    screenStateModel.setState(ScreenState.IDLE, rc.getBannerMessage());
                }
            });
        }

        updateScreenStateUI(screenStateModel.getState());
    }

    private void updateScreenStateUI(ScreenState state) {
        if (statusBannerBox != null) {
            statusBannerBox.getStyleClass().removeAll(
                    "status-idle", "status-loading", "status-error", "status-disconnected");
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
            updateStatus("Ghế " + vm.getSeatId()
                    + " đang ở trạng thái CHƯA RÕ (UNKNOWN). Vui lòng đợi xác nhận trước khi thao tác!");
            return;
        }

        if (currentStatus == SeatStatus.SELECTED) {
            vm.setStatus(SeatStatus.AVAILABLE);
            updateStatus("Đã bỏ chọn ghế " + vm.getSeatId() + ".");
        } else if (currentStatus == SeatStatus.AVAILABLE) {
            vm.setStatus(SeatStatus.SELECTED);
            updateStatus("Đã chọn ghế " + vm.getSeatId() + ".");
        }
    }

    public void handleSeatClick(String seatId) {
        handleSeatClick(getSeatViewModel(seatId));
    }

    public List<String> getSelectedSeats() {
        return seats.stream()
                .filter(SeatViewModel::isSelected)
                .map(SeatViewModel::getSeatId)
                .sorted()
                .toList();
    }

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
        updateLegendCounts();
        updateSelectedSeatsUI();
    }

    public void clearSelection() {
        for (SeatViewModel vm : seats) {
            if (vm.isSelected()) {
                vm.setStatus(SeatStatus.AVAILABLE);
            }
        }
        updateLegendCounts();
        updateSelectedSeatsUI();
    }

    public Button getSeatButton(String seatId) {
        return seatButtons.get(seatId);
    }

    public SeatViewModel getSeatViewModel(String seatId) {
        if (seatId == null) {
            return null;
        }
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

    public void openBookingDialog() {
        var fxmlLocation = getClass().getResource("/fxml/booking-dialog.fxml");
        if (fxmlLocation == null) {
            String errorMsg = "LỖI: Không tìm thấy tệp giao diện /fxml/booking-dialog.fxml!";
            updateStatus(errorMsg);
            return;
        }

        try {
            FXMLLoader loader = new FXMLLoader(fxmlLocation);
            Parent dialogRoot = loader.load();
            BookingDialogController dialogController = loader.getController();
            dialogController.setBookingViewModel(ClientFactory.getBookingViewModel());
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

            Optional<BookingOutcome> outcomeOpt = dialogController.getOutcome();
            if (outcomeOpt.isPresent()) {
                String passengerName = dialogController.getPassengerName();
                handleBookingOutcome(outcomeOpt.get(), passengerName);
            } else {
                updateStatus("Đã hủy thao tác đặt vé. Trạng thái ghế giữ nguyên.");
            }
        } catch (IOException e) {
            String errorMsg = "Không thể mở hộp thoại đặt vé.";
            updateStatus(errorMsg);
            LOGGER.log(System.Logger.Level.WARNING, "Lỗi mở booking-dialog.fxml", e);
        }
    }

    public void handleBookingOutcome(BookingOutcome outcome) {
        handleBookingOutcome(outcome, "Hành khách");
    }

    public void handleBookingOutcome(BookingOutcome outcome, String passengerName) {
        if (outcome == null) {
            return;
        }

        boolean socketOrRelease = ClientConfig.isReleaseModeActive();
        if (!socketOrRelease) {
            try {
                ClientConfig cfg = ClientConfig.loadDefault();
                socketOrRelease = cfg.isReleaseMode() || cfg.isSocketMode();
            } catch (Exception ignored) {
                socketOrRelease = true;
            }
        }

        List<String> affectedSeats = getSelectedSeats();
        String finalName = (passengerName != null && !passengerName.isBlank()) ? passengerName.trim() : "Hành khách";
        String nowTime = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

        switch (outcome.getStatus()) {
            case CONFIRMED -> {
                String refCode = outcome.getReferenceCode();
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                    if (!socketOrRelease) {
                        TicketViewModel newTicket = new TicketViewModel(
                                refCode, "TCK-SE001-" + seatId, seatId, "SE-001",
                                finalName, nowTime, "1.250.000 VNĐ", TicketStatus.CONFIRMED, null);
                        DevSupportBridge.get().addDevTicket(newTicket);
                    }
                }
                String msg = "Đặt vé thành công! Mã vé: " + refCode + " (Ghế: " + String.join(", ", affectedSeats)
                        + "). Vé đã được ghi nhận vào mục 'Vé của tôi'.";
                updateStatus(msg);
                screenStateModel.setState(ScreenState.IDLE, msg);
            }
            case REJECTED -> {
                ErrorPresentation p = ErrorPresentationMapper.mapCode(
                        outcome.getReasonCode() != null ? outcome.getReasonCode()
                                : ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                        outcome.getMessage(),
                        !socketOrRelease);
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null && p.needsSeatRefresh()) {
                        vm.setStatus(SeatStatus.BOOKED);
                    }
                    if (!socketOrRelease) {
                        TicketViewModel rejectedTicket = new TicketViewModel(
                                "REQ-" + (System.currentTimeMillis() % 1000000), "", seatId, "SE-001",
                                finalName, nowTime, "1.250.000 VNĐ", TicketStatus.REJECTED, p.rawCode());
                        DevSupportBridge.get().addDevTicket(rejectedTicket);
                    }
                }
                String msg = p.message();
                updateStatus(msg);
                screenStateModel.setState(ScreenState.ERROR, msg);
            }
            case UNKNOWN -> {
                ErrorPresentation p = ErrorPresentationMapper.mapCode(
                        ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN,
                        outcome.getMessage(),
                        !socketOrRelease);
                for (String seatId : affectedSeats) {
                    SeatViewModel vm = getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.UNKNOWN);
                    }
                    if (!socketOrRelease) {
                        TicketViewModel unknownTicket = new TicketViewModel(
                                "REQ-" + (System.currentTimeMillis() % 1000000), "", seatId, "SE-001",
                                finalName, nowTime, "1.250.000 VNĐ", TicketStatus.UNKNOWN, p.rawCode());
                        DevSupportBridge.get().addDevTicket(unknownTicket);
                    }
                }
                String msg = p.message();
                updateStatus(msg);
                screenStateModel.setState(ScreenState.DISCONNECTED, msg);
            }
        }
        updateLegendCounts();
        updateSelectedSeatsUI();
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

    @FXML
    public void onRefreshSeatsClicked() {
        SeatsRefreshService svc = getSeatsRefreshService();
        if (svc != null) {
            svc.refreshNow().whenComplete((snapshot, ex) -> {
                try {
                    javafx.application.Platform.runLater(() -> {
                        updateLegendCounts();
                        updateSelectedSeatsUI();
                    });
                } catch (IllegalStateException e) {
                    updateLegendCounts();
                    updateSelectedSeatsUI();
                }
            });
        } else if (!ClientConfig.isReleaseModeActive()) {
            clearSelection();
            Map<String, SeatStatus> snapshot = DevSupportBridge.get().createRefreshedMockSnapshot();
            SeatViewModel.applySnapshot(seats, snapshot);
            DevSupportBridge.get().syncDevSnapshot(snapshot);
            screenStateModel.setState(ScreenState.IDLE, "Đã làm mới dữ liệu sơ đồ ghế cục bộ.");
            updateLegendCounts();
            updateSelectedSeatsUI();
        }
    }

    // =========================================================================
    // CÁC HÀM SỰ KIỆN CHỈ HOẠT ĐỘNG Ở CHẾ ĐỘ PHÁT TRIỂN (VÔ HIỆU KHI RELEASE)
    // =========================================================================

    @FXML
    public void onMockLoadingClicked() {
        if (ClientConfig.isReleaseModeActive()) return;
        screenStateModel.setState(ScreenState.LOADING, "Đang đồng bộ dữ liệu ghế từ cụm máy chủ...");
        updateStatus("Đã chuyển màn hình sang trạng thái LOADING.");
    }

    @FXML
    public void onMockErrorClicked() {
        if (ClientConfig.isReleaseModeActive()) return;
        screenStateModel.setState(ScreenState.ERROR, "Cảnh báo lỗi kết nối cục bộ.");
        updateStatus("Đã chuyển màn hình sang trạng thái ERROR.");
    }

    @FXML
    public void onMockDisconnectedClicked() {
        if (ClientConfig.isReleaseModeActive()) return;
        screenStateModel.setState(ScreenState.DISCONNECTED, "Mất kết nối mạng tới cụm Node!");
        updateStatus("Đã chuyển màn hình sang trạng thái DISCONNECTED.");
    }

    @FXML
    public void onMockIdleClicked() {
        if (ClientConfig.isReleaseModeActive()) return;
        screenStateModel.setState(ScreenState.IDLE, "Hệ thống sẵn sàng.");
        updateStatus("Đã chuyển màn hình về trạng thái IDLE.");
    }

    @FXML
    public void onMockApplySeatStatus() {
        if (ClientConfig.isReleaseModeActive() || mockSeatComboBox == null || mockStatusComboBox == null) {
            return;
        }
        String seatId = mockSeatComboBox.getValue();
        SeatStatus targetStatus = mockStatusComboBox.getValue();
        if (seatId != null && targetStatus != null) {
            SeatViewModel vm = getSeatViewModel(seatId);
            if (vm != null) {
                vm.setStatus(targetStatus);
                DevSupportBridge.get().syncDevSeatStatusChange(seatId, targetStatus);
                updateLegendCounts();
                updateSelectedSeatsUI();
                updateStatus("Đã đổi trạng thái ghế " + seatId + " sang " + targetStatus + ".");
            }
        }
    }

    @FXML
    public void onMockToggleG03Booked() {
        if (ClientConfig.isReleaseModeActive()) return;
        SeatViewModel vm = getSeatViewModel("G03");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.BOOKED) ? SeatStatus.AVAILABLE : SeatStatus.BOOKED;
            vm.setStatus(newStatus);
            DevSupportBridge.get().syncDevSeatStatusChange("G03", newStatus);
            updateLegendCounts();
            updateSelectedSeatsUI();
            updateStatus("Đã đổi ghế G03 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockRefreshSnapshotClicked() {
        onRefreshSeatsClicked();
    }

    @FXML
    public void onMockToggleG04Unknown() {
        if (ClientConfig.isReleaseModeActive()) return;
        SeatViewModel vm = getSeatViewModel("G04");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.UNKNOWN) ? SeatStatus.AVAILABLE : SeatStatus.UNKNOWN;
            vm.setStatus(newStatus);
            DevSupportBridge.get().syncDevSeatStatusChange("G04", newStatus);
            updateLegendCounts();
            updateSelectedSeatsUI();
            updateStatus("Đã đổi ghế G04 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockToggleG05Booked() {
        if (ClientConfig.isReleaseModeActive()) return;
        SeatViewModel vm = getSeatViewModel("G05");
        if (vm != null) {
            SeatStatus newStatus = (vm.getStatus() == SeatStatus.BOOKED) ? SeatStatus.AVAILABLE : SeatStatus.BOOKED;
            vm.setStatus(newStatus);
            DevSupportBridge.get().syncDevSeatStatusChange("G05", newStatus);
            updateLegendCounts();
            updateSelectedSeatsUI();
            updateStatus("Đã đổi ghế G05 sang trạng thái " + newStatus.name() + ".");
        }
    }

    @FXML
    public void onMockAll4States() {
        if (ClientConfig.isReleaseModeActive()) return;
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

        var dev = DevSupportBridge.get();
        dev.syncDevSeatStatusChange("G01", SeatStatus.AVAILABLE);
        dev.syncDevSeatStatusChange("G02", SeatStatus.BOOKED);
        dev.syncDevSeatStatusChange("G03", SeatStatus.AVAILABLE);
        dev.syncDevSeatStatusChange("G04", SeatStatus.UNKNOWN);
        dev.syncDevSeatStatusChange("G06", SeatStatus.AVAILABLE);
        dev.syncDevSeatStatusChange("G07", SeatStatus.AVAILABLE);
        dev.syncDevSeatStatusChange("G08", SeatStatus.BOOKED);
        dev.syncDevSeatStatusChange("G09", SeatStatus.AVAILABLE);
        dev.syncDevSeatStatusChange("G10", SeatStatus.AVAILABLE);

        updateLegendCounts();
        updateSelectedSeatsUI();

        screenStateModel.setState(ScreenState.IDLE,
                "Đang hiển thị đủ 4 trạng thái ghế: AVAILABLE, BOOKED, SELECTED, UNKNOWN.");
        updateStatus("Đã thiết lập đủ 4 trạng thái ghế.");
    }

    @FXML
    public void onMockResetDefault() {
        if (ClientConfig.isReleaseModeActive()) return;
        clearSelection();
        var dev = DevSupportBridge.get();
        SeatViewModel.applySnapshot(seats, dev.createInitialMockSnapshot());
        dev.resetDevTicketsToDefault();
        screenStateModel.setState(ScreenState.IDLE);
        updateLegendCounts();
        updateSelectedSeatsUI();
        updateStatus("Đã khôi phục dữ liệu ghế cục bộ ban đầu.");
    }

    private void setSeatStatusHelper(String seatId, SeatStatus status) {
        SeatViewModel vm = getSeatViewModel(seatId);
        if (vm != null) {
            vm.setStatus(status);
        }
    }
}
