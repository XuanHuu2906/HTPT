package vn.nhom6.railway.client.controller;

import javafx.animation.PauseTransition;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import javafx.util.Duration;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.DevSupportBridge;
import vn.nhom6.railway.client.viewmodel.TicketLookupViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import java.io.IOException;
import java.util.List;

/**
 * Controller quản lý màn hình danh sách và tra cứu vé (TASK D06, tích hợp TV1-D24 / TV1-D28).
 */
public class TicketStatusController {

    @FXML
    private Button backBtn;

    @FXML
    private Label modeBadgeLabel;

    @FXML
    private Label mockBadgeLabel;

    @FXML
    private HBox bannerBox;

    @FXML
    private Label bannerTitleLabel;

    @FXML
    private Label bannerTextLabel;

    @FXML
    private HBox historyBlockedBox;

    @FXML
    private Label historyBlockedLabel;

    @FXML
    private ComboBox<String> searchTypeCombo;

    @FXML
    private TextField searchField;

    @FXML
    private Button searchBtn;

    @FXML
    private Button clearSearchBtn;

    @FXML
    private ProgressIndicator searchProgressIndicator;

    @FXML
    private Label searchResultLabel;

    @FXML
    private TableView<TicketViewModel> ticketTableView;

    @FXML
    private TableColumn<TicketViewModel, String> colRequestId;

    @FXML
    private TableColumn<TicketViewModel, String> colTicketId;

    @FXML
    private TableColumn<TicketViewModel, String> colSeat;

    @FXML
    private TableColumn<TicketViewModel, String> colTrip;

    @FXML
    private TableColumn<TicketViewModel, String> colPassenger;

    @FXML
    private TableColumn<TicketViewModel, String> colTime;

    @FXML
    private TableColumn<TicketViewModel, TicketStatus> colStatus;

    @FXML
    private TableColumn<TicketViewModel, String> colMock;

    @FXML
    private Label selectedTicketSummaryLabel;

    @FXML
    private Button cancelTicketBtn;

    private final ObservableList<TicketViewModel> allTickets = FXCollections.observableArrayList();
    private final ObservableList<TicketViewModel> displayedTickets = FXCollections.observableArrayList();

    private TicketLookupViewModel lookupViewModel;
    private Runnable onBackAction;

    @FXML
    public void initialize() {
        if (lookupViewModel == null) {
            try {
                lookupViewModel = ClientFactory.getTicketLookupViewModel();
            } catch (Exception ignored) {
            }
        }

        initSearchControls();
        initTableColumns();

        ticketTableView.setItems(displayedTickets);

        bindLookupViewModel();
        setupTableSelectionListener();
        updateSelectedTicketUI(null);
    }

    private static void removeNodeFromParent(Node node) {
        if (node != null && node.getParent() instanceof Pane parentPane) {
            parentPane.getChildren().remove(node);
        }
    }

    public void setLookupViewModel(TicketLookupViewModel lookupViewModel) {
        this.lookupViewModel = lookupViewModel;
        if (ticketTableView != null) {
            bindLookupViewModel();
        }
    }

    public TicketLookupViewModel getLookupViewModel() {
        return lookupViewModel;
    }

    private void bindLookupViewModel() {
        boolean releaseActive = ClientConfig.isReleaseModeActive();
        if (lookupViewModel != null) {
            displayedTickets.setAll(lookupViewModel.getDisplayedTickets());
            lookupViewModel.getDisplayedTickets().addListener((javafx.collections.ListChangeListener<TicketViewModel>) c -> {
                displayedTickets.setAll(lookupViewModel.getDisplayedTickets());
                if (ticketTableView != null) {
                    ticketTableView.refresh();
                }
                if (!displayedTickets.isEmpty() && ticketTableView != null) {
                    ticketTableView.getSelectionModel().select(0);
                }
            });

            if (searchProgressIndicator != null) {
                searchProgressIndicator.visibleProperty().bind(lookupViewModel.busyProperty());
                searchProgressIndicator.managedProperty().bind(lookupViewModel.busyProperty());
            }
            if (searchBtn != null) {
                searchBtn.disableProperty().bind(lookupViewModel.busyProperty());
            }
            if (searchResultLabel != null) {
                searchResultLabel.textProperty().bind(lookupViewModel.statusMessageProperty());
                lookupViewModel.statusStyleClassProperty().addListener((obs, oldV, newV) -> setSearchResultStyle(newV));
                setSearchResultStyle(lookupViewModel.getStatusStyleClass());
            }

            if (lookupViewModel.isMockMode() && !releaseActive) {
                removeNodeFromParent(modeBadgeLabel);
                if (mockBadgeLabel != null) {
                    mockBadgeLabel.setText(DevSupportBridge.get().getMockTicketNoticeLabel());
                    mockBadgeLabel.getStyleClass().removeAll("badge-ticket-confirmed", "badge-release-mode");
                    if (!mockBadgeLabel.getStyleClass().contains("badge-mock")) {
                        mockBadgeLabel.getStyleClass().add("badge-mock");
                    }
                }
                if (bannerTitleLabel != null) bannerTitleLabel.setText("ℹ THÔNG BÁO MÔI TRƯỜNG PHÁT TRIỂN:");
                if (bannerTextLabel != null) bannerTextLabel.setText("Dữ liệu bên dưới phục vụ kiểm thử giao diện cục bộ, không ghi DB hay gửi qua mạng.");
                if (historyBlockedLabel != null) historyBlockedLabel.setText("Chế độ phát triển cục bộ.");
            } else {
                removeNodeFromParent(mockBadgeLabel);
                if (modeBadgeLabel != null) {
                    modeBadgeLabel.getStyleClass().removeAll("badge-mock", "badge-ticket-confirmed", "badge-release-mode");
                    if (releaseActive) {
                        modeBadgeLabel.setText("REAL / RELEASE");
                        modeBadgeLabel.getStyleClass().add("badge-release-mode");
                    } else {
                        modeBadgeLabel.setText("REAL / SOCKET");
                        modeBadgeLabel.getStyleClass().add("badge-ticket-confirmed");
                    }
                }
                if (bannerTitleLabel != null) bannerTitleLabel.setText("ℹ CHẾ ĐỘ KẾT NỐI THẬT:");
                if (bannerTextLabel != null) bannerTextLabel.setText("Bảng hiển thị các vé đã xác nhận (CONFIRMED) trong phiên hoặc lưu bền từ Journal máy trạm này.");
                if (historyBlockedLabel != null) historyBlockedLabel.setText(lookupViewModel.getHistoryBlockedMessage());
            }
        } else if (!releaseActive) {
            boolean isSocket = ClientConfig.loadDefault().isSocketMode();
            if (isSocket) {
                removeNodeFromParent(mockBadgeLabel);
            } else {
                removeNodeFromParent(modeBadgeLabel);
                var devTickets = DevSupportBridge.get().getSharedDevTickets();
                allTickets.setAll(devTickets);
                if (mockBadgeLabel != null) {
                    mockBadgeLabel.setText(DevSupportBridge.get().getMockTicketNoticeLabel());
                }
                displayedTickets.setAll(allTickets);
            }
        } else {
            removeNodeFromParent(mockBadgeLabel);
        }
    }

    public void refreshTicketList() {
        if (searchField != null) {
            searchField.clear();
        }
        if (lookupViewModel != null) {
            lookupViewModel.loadConfirmedTicketsFromJournal();
            lookupViewModel.resetFilter();
        } else {
            displayedTickets.setAll(allTickets);
            setSearchResultMessage("Hiển thị tất cả " + allTickets.size() + " vé trong danh sách.", "search-msg-muted");
        }
        if (!displayedTickets.isEmpty() && ticketTableView != null) {
            ticketTableView.getSelectionModel().select(0);
        } else {
            updateSelectedTicketUI(null);
        }
        if (ticketTableView != null) {
            ticketTableView.refresh();
        }
    }

    private void initSearchControls() {
        if (searchTypeCombo != null) {
            searchTypeCombo.getItems().setAll(
                    "Tất cả mã",
                    "Mã yêu cầu (Request ID)",
                    "Mã vé (Ticket ID)"
            );
            searchTypeCombo.setValue("Tất cả mã");
        }

        if (searchField != null) {
            searchField.setOnAction(e -> onSearchClicked());
        }
    }

    private void initTableColumns() {
        colRequestId.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getRequestId()));
        colTicketId.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getTicketId()));
        colSeat.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getSeatId()));
        colTrip.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getTripCode()));
        colPassenger.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getPassengerName()));
        colTime.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getCreatedAt()));

        colStatus.setCellValueFactory(data -> data.getValue().statusProperty());
        colStatus.setCellFactory(column -> new TableCell<>() {
            private final Label badgeLabel = new Label();

            {
                badgeLabel.getStyleClass().add("badge-ticket");
            }

            @Override
            protected void updateItem(TicketStatus status, boolean empty) {
                super.updateItem(status, empty);
                if (empty || status == null) {
                    setGraphic(null);
                    setText(null);
                } else {
                    badgeLabel.setText(status.getFormattedText());
                    badgeLabel.getStyleClass().removeAll(
                            "badge-ticket-confirmed", "badge-ticket-cancelled",
                            "badge-ticket-rejected", "badge-ticket-unknown"
                    );
                    badgeLabel.getStyleClass().add(status.getStyleClass());
                    setGraphic(badgeLabel);
                    setText(null);
                }
            }
        });

        colMock.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getMockBadge()));
        colMock.setCellFactory(column -> new TableCell<>() {
            private final Label badgeLabel = new Label();

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setGraphic(null);
                    setText(null);
                } else {
                    badgeLabel.setText(item);
                    badgeLabel.getStyleClass().removeAll("badge-mock", "badge-ticket-confirmed");
                    if ("REAL".equalsIgnoreCase(item) || ClientConfig.isReleaseModeActive()) {
                        badgeLabel.getStyleClass().add("badge-ticket-confirmed");
                    } else {
                        badgeLabel.getStyleClass().add("badge-mock");
                    }
                    setGraphic(badgeLabel);
                    setText(null);
                }
            }
        });
    }

    private void setupTableSelectionListener() {
        ticketTableView.getSelectionModel().selectedItemProperty().addListener((obs, oldTicket, newTicket) ->
                updateSelectedTicketUI(newTicket));
    }

    private void updateSelectedTicketUI(TicketViewModel ticket) {
        if (ticket == null) {
            selectedTicketSummaryLabel.setText("(Chưa chọn vé nào — Vui lòng nhấp vào một dòng trong bảng)");
            setSummaryStyle(selectedTicketSummaryLabel, "ticket-summary-default");
            cancelTicketBtn.setDisable(true);
            cancelTicketBtn.setTooltip(new Tooltip("Vui lòng chọn một vé hợp lệ trong bảng để hủy."));
            return;
        }

        String summary = String.format("Mã vé: %s | Yêu cầu: %s | Ghế: %s (%s) | Khách: %s | Giá: %s | Trạng thái: %s",
                ticket.getTicketId(), ticket.getRequestId(), ticket.getSeatId(), ticket.getTripCode(),
                ticket.getPassengerName(), ticket.getPriceFormatted(), ticket.getStatus().getDisplayName());
        selectedTicketSummaryLabel.setText(summary);

        if (ticket.isCancellable()) {
            setSummaryStyle(selectedTicketSummaryLabel, "ticket-summary-cancellable");
            cancelTicketBtn.setDisable(false);
            cancelTicketBtn.setTooltip(new Tooltip("Nhấn để mở hộp thoại xác nhận hủy vé cho vé " + ticket.getTicketId() + "."));
        } else {
            setSummaryStyle(selectedTicketSummaryLabel, "ticket-summary-uncancellable");
            cancelTicketBtn.setDisable(true);

            String reason;
            switch (ticket.getStatus()) {
                case CANCELLED -> reason = "Vé này đã bị hủy trước đó. Không thể thực hiện hủy lại.";
                case REJECTED -> {
                    ErrorPresentation p = ErrorPresentationMapper.mapCode(
                            ticket.getReasonCode() != null ? ticket.getReasonCode() : ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                            false);
                    reason = "Yêu cầu đặt vé này đã bị từ chối (" + p.rawCode() + "): " + p.message() + " Không có vé hợp lệ để hủy.";
                }
                case UNKNOWN -> reason = "Vé đang ở trạng thái CHƯA RÕ (Timeout/Chờ xác nhận). Vui lòng tra cứu kết quả đặt trước khi hủy.";
                default -> reason = "Vé không ở trạng thái hợp lệ để hủy.";
            }
            cancelTicketBtn.setTooltip(new Tooltip(reason));
        }
    }

    private void setSummaryStyle(Label label, String styleClass) {
        if (label != null) {
            label.setStyle(null);
            label.getStyleClass().removeAll("ticket-summary-default", "ticket-summary-cancellable", "ticket-summary-uncancellable");
            label.getStyleClass().add(styleClass);
        }
    }

    @FXML
    public void onSearchClicked() {
        String query = (searchField != null && searchField.getText() != null)
                ? searchField.getText().trim()
                : "";

        if (query.isEmpty()) {
            if (lookupViewModel != null) {
                lookupViewModel.lookup("");
            } else {
                setSearchResultMessage("⚠️ Vui lòng nhập mã Request ID hoặc Ticket ID để tra cứu!", "search-msg-error");
            }
            return;
        }

        String searchType = (searchTypeCombo != null && searchTypeCombo.getValue() != null)
                ? searchTypeCombo.getValue()
                : "Tất cả mã";

        if (lookupViewModel != null) {
            if ("Mã yêu cầu (Request ID)".equals(searchType)) {
                lookupViewModel.lookup(query);
            } else if ("Mã vé (Ticket ID)".equals(searchType)) {
                lookupViewModel.searchByTicketId(query);
            } else {
                if (lookupViewModel.isValidRequestId(query)) {
                    lookupViewModel.lookup(query);
                } else {
                    lookupViewModel.filterAll(query);
                }
            }
        } else {
            setSearchLoading(true);
            PauseTransition delay = new PauseTransition(Duration.millis(350));
            delay.setOnFinished(event -> {
                setSearchLoading(false);
                performSearch(query);
            });
            delay.play();
        }
    }

    private void performSearch(String query) {
        String searchType = (searchTypeCombo != null && searchTypeCombo.getValue() != null)
                ? searchTypeCombo.getValue()
                : "Tất cả mã";

        String lowerQuery = query.toLowerCase();

        List<TicketViewModel> foundList = allTickets.stream()
                .filter(t -> {
                    boolean matchReq = t.getRequestId().toLowerCase().contains(lowerQuery);
                    boolean matchTicket = t.getTicketId().toLowerCase().contains(lowerQuery);

                    if ("Mã yêu cầu (Request ID)".equals(searchType)) {
                        return matchReq;
                    } else if ("Mã vé (Ticket ID)".equals(searchType)) {
                        return matchTicket;
                    } else {
                        return matchReq || matchTicket;
                    }
                })
                .toList();

        if (foundList.isEmpty()) {
            displayedTickets.clear();
            setSearchResultMessage("✗ Không tìm thấy vé nào khớp với mã '" + query + "'.", "search-msg-error");
            updateSelectedTicketUI(null);
        } else {
            displayedTickets.setAll(foundList);
            ticketTableView.getSelectionModel().select(0);
            setSearchResultMessage("✓ Đã tìm thấy " + foundList.size() + " vé khớp với từ khóa '" + query + "'.", "search-msg-success");
        }
    }

    @FXML
    public void onClearSearchClicked() {
        if (searchField != null) {
            searchField.clear();
        }
        if (lookupViewModel != null) {
            lookupViewModel.resetFilter();
        } else {
            displayedTickets.setAll(allTickets);
            setSearchResultMessage("Hiển thị tất cả " + allTickets.size() + " vé.", "search-msg-muted");
        }
        if (!displayedTickets.isEmpty() && ticketTableView != null) {
            ticketTableView.getSelectionModel().select(0);
        } else {
            updateSelectedTicketUI(null);
        }
    }

    private void setSearchResultStyle(String styleClass) {
        if (searchResultLabel != null) {
            searchResultLabel.setStyle(null);
            searchResultLabel.getStyleClass().removeAll("search-msg-info", "search-msg-muted", "search-msg-success", "search-msg-error");
            if (styleClass != null && !styleClass.isBlank()) {
                searchResultLabel.getStyleClass().add(styleClass);
            }
        }
    }

    private void setSearchLoading(boolean isLoading) {
        if (searchProgressIndicator != null) {
            searchProgressIndicator.setVisible(isLoading);
            searchProgressIndicator.setManaged(isLoading);
        }
        if (searchBtn != null) {
            searchBtn.setDisable(isLoading);
        }
        if (isLoading) {
            setSearchResultMessage("Đang tra cứu dữ liệu vé...", "search-msg-info");
        }
    }

    private void setSearchResultMessage(String msg, String styleClass) {
        if (searchResultLabel != null) {
            searchResultLabel.setText(msg);
            searchResultLabel.setStyle(null);
            searchResultLabel.getStyleClass().removeAll("search-msg-info", "search-msg-muted", "search-msg-success", "search-msg-error");
            searchResultLabel.getStyleClass().add(styleClass);
        }
    }

    @FXML
    public void onCancelTicketClicked() {
        TicketViewModel selectedTicket = ticketTableView.getSelectionModel().getSelectedItem();
        if (selectedTicket == null || !selectedTicket.isCancellable()) {
            return;
        }
        openCancelTicketDialog(selectedTicket);
    }

    public void openCancelTicketDialog(TicketViewModel ticket) {
        var fxmlLocation = getClass().getResource("/fxml/cancel-ticket-dialog.fxml");
        if (fxmlLocation == null) {
            String errorMsg = "LỖI: Không tìm thấy tệp giao diện /fxml/cancel-ticket-dialog.fxml!";
            setSearchResultMessage(errorMsg, "search-msg-error");
            return;
        }

        try {
            FXMLLoader loader = new FXMLLoader(fxmlLocation);
            Parent dialogRoot = loader.load();
            CancelTicketController dialogController = loader.getController();
            dialogController.setTicket(ticket);

            Stage dialogStage = new Stage();
            dialogStage.setTitle("Xác nhận hủy vé — SE-001");
            dialogStage.initModality(Modality.APPLICATION_MODAL);

            Window owner = null;
            if (ticketTableView != null && ticketTableView.getScene() != null) {
                owner = ticketTableView.getScene().getWindow();
            }
            if (owner != null) {
                dialogStage.initOwner(owner);
            }

            dialogController.setDialogStage(dialogStage);
            dialogStage.setScene(new Scene(dialogRoot));
            dialogStage.setMinWidth(640);
            dialogStage.setMinHeight(560);
            dialogStage.setResizable(true);
            dialogStage.sizeToScene();
            dialogStage.showAndWait();

            ticketTableView.refresh();
            updateSelectedTicketUI(ticket);
        } catch (IOException e) {
            String errorMsg = "Không thể mở hộp thoại hủy vé.";
            setSearchResultMessage(errorMsg, "search-msg-error");
            System.getLogger(TicketStatusController.class.getName())
                    .log(System.Logger.Level.WARNING, "Lỗi mở cancel-ticket-dialog.fxml", e);
        }
    }

    public void setOnBackAction(Runnable onBackAction) {
        this.onBackAction = onBackAction;
    }

    @FXML
    private void onBackClicked() {
        if (onBackAction != null) {
            onBackAction.run();
        }
    }

    public ObservableList<TicketViewModel> getAllTickets() {
        return allTickets;
    }

    public ObservableList<TicketViewModel> getDisplayedTickets() {
        return displayedTickets;
    }

    public TableView<TicketViewModel> getTicketTableView() {
        return ticketTableView;
    }
}
