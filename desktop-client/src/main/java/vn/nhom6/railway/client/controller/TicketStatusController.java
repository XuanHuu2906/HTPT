package vn.nhom6.railway.client.controller;

import javafx.animation.PauseTransition;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
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
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import javafx.util.Duration;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.MockTicketSource;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import java.io.IOException;
import java.util.List;

/**
 * Controller quản lý màn hình danh sách và tra cứu vé MOCK (TASK D06).
 * <p>
 * Tính năng chính:
 * - Hiển thị TableView danh sách vé bằng ObservableList mô phỏng (không ghi DB, không gọi mạng).
 * - Tra cứu theo Request ID hoặc Ticket ID kèm trạng thái loading giả (PauseTransition).
 * - Highlight kết quả tìm kiếm và báo lỗi rõ ràng nếu không tìm thấy.
 * - Điều khiển nút "Hủy vé" chỉ kích hoạt khi vé ở trạng thái CONFIRMED kèm Tooltip giải thích.
 * - Hỗ trợ mở hộp thoại xác nhận hủy vé (TASK D06 Part 2).
 */
public class TicketStatusController {

    @FXML
    private Button backBtn;

    @FXML
    private Label mockBadgeLabel;

    // === Vùng Tìm kiếm / Tra cứu ===
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

    // === Bảng TableView danh sách vé ===
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

    // === Khu vực tóm tắt & Hủy vé ===
    @FXML
    private Label selectedTicketSummaryLabel;

    @FXML
    private Button cancelTicketBtn;

    // Nguồn dữ liệu MOCK dùng chung
    private final ObservableList<TicketViewModel> allTickets = MockTicketSource.getSharedTickets();
    private final ObservableList<TicketViewModel> displayedTickets = FXCollections.observableArrayList();

    // Callback quay lại màn hình chính
    private Runnable onBackAction;

    @FXML
    public void initialize() {
        if (mockBadgeLabel != null) {
            mockBadgeLabel.setText(MockTicketSource.MOCK_NOTICE);
        }

        initSearchControls();
        initTableColumns();

        displayedTickets.setAll(allTickets);
        ticketTableView.setItems(displayedTickets);

        // Lắng nghe vé mới được thêm vào hoặc cập nhật để tự động làm mới
        allTickets.addListener((javafx.collections.ListChangeListener<TicketViewModel>) c -> {
            displayedTickets.setAll(allTickets);
            if (ticketTableView != null) {
                ticketTableView.refresh();
            }
            if (!displayedTickets.isEmpty() && ticketTableView != null) {
                ticketTableView.getSelectionModel().select(0);
            }
        });

        setupTableSelectionListener();
        updateSelectedTicketUI(null);
    }

    /**
     * Làm mới lại danh sách vé hiển thị, đảm bảo các vé vừa đặt hoặc thao tác mock xuất hiện ngay lập tức.
     */
    public void refreshTicketList() {
        if (searchField != null) {
            searchField.clear();
        }
        displayedTickets.setAll(allTickets);
        setSearchResultMessage("Hiển thị tất cả " + allTickets.size() + " vé trong danh sách MOCK.", "search-msg-muted");
        if (!displayedTickets.isEmpty() && ticketTableView != null) {
            ticketTableView.getSelectionModel().select(0);
        } else {
            updateSelectedTicketUI(null);
        }
        if (ticketTableView != null) {
            ticketTableView.refresh();
        }
    }

    /**
     * Khởi tạo ComboBox kiểu tìm kiếm và sự kiện nhấn Enter trong ô nhập.
     */
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

    /**
     * Cấu hình CellValueFactory và CellFactory cho từng cột của TableView.
     */
    private void initTableColumns() {
        colRequestId.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getRequestId()));
        colTicketId.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getTicketId()));
        colSeat.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getSeatId()));
        colTrip.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getTripCode()));
        colPassenger.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getPassengerName()));
        colTime.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getCreatedAt()));

        // Cột Trạng thái: Render badge trực quan theo màu sắc CSS của D04/D06
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

        // Cột Ghi chú: Hiển thị nhãn DEMO / MOCK
        colMock.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().getMockBadge()));
        colMock.setCellFactory(column -> new TableCell<>() {
            private final Label mockLabel = new Label("DEMO / MOCK");

            {
                mockLabel.getStyleClass().add("badge-mock");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setGraphic(null);
                } else {
                    setGraphic(mockLabel);
                }
            }
        });
    }

    /**
     * Lắng nghe việc người dùng chọn dòng trên bảng để cập nhật tóm tắt và nút "Hủy vé".
     */
    private void setupTableSelectionListener() {
        ticketTableView.getSelectionModel().selectedItemProperty().addListener((obs, oldTicket, newTicket) -> {
            updateSelectedTicketUI(newTicket);
        });
    }

    /**
     * Cập nhật thông tin chi tiết vé được chọn và kích hoạt/vô hiệu nút "Hủy vé".
     */
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
                case REJECTED -> reason = "Yêu cầu đặt vé này đã bị từ chối (" + ticket.getReasonCode() + "), không có vé hợp lệ để hủy.";
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

    // =========================================================================
    // XỬ LÝ TRA CỨU & LÀM MỚI (SEARCH / LOOKUP)
    // =========================================================================

    @FXML
    public void onSearchClicked() {
        String query = (searchField != null && searchField.getText() != null)
                ? searchField.getText().trim()
                : "";

        if (query.isEmpty()) {
            setSearchResultMessage("⚠️ Vui lòng nhập mã Request ID hoặc Ticket ID để tra cứu!", "search-msg-error");
            return;
        }

        // Bật trạng thái loading giả (giả lập độ trễ tra cứu mạng)
        setSearchLoading(true);

        PauseTransition delay = new PauseTransition(Duration.millis(350));
        delay.setOnFinished(event -> {
            setSearchLoading(false);
            performSearch(query);
        });
        delay.play();
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
            setSearchResultMessage("✗ Không tìm thấy vé nào khớp với mã '" + query + "' (Dữ liệu MOCK).", "search-msg-error");
            updateSelectedTicketUI(null);
        } else {
            displayedTickets.setAll(foundList);
            ticketTableView.getSelectionModel().select(0);
            setSearchResultMessage("✓ Đã tìm thấy " + foundList.size() + " vé khớp với từ khóa '" + query + "' (MOCK).", "search-msg-success");
        }
    }

    @FXML
    public void onClearSearchClicked() {
        if (searchField != null) {
            searchField.clear();
        }
        displayedTickets.setAll(allTickets);
        setSearchResultMessage("Hiển thị tất cả " + allTickets.size() + " vé trong danh sách MOCK.", "search-msg-muted");
        if (!displayedTickets.isEmpty()) {
            ticketTableView.getSelectionModel().select(0);
        } else {
            updateSelectedTicketUI(null);
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
            setSearchResultMessage("Đang tra cứu dữ liệu vé từ bộ nhớ mô phỏng...", "search-msg-info");
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

    // =========================================================================
    // HỘP THOẠI HỦY VÉ (CANCEL TICKET DIALOG)
    // =========================================================================

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
            System.err.println(errorMsg);
            return;
        }

        try {
            FXMLLoader loader = new FXMLLoader(fxmlLocation);
            Parent dialogRoot = loader.load();
            CancelTicketController dialogController = loader.getController();

            dialogController.setTicket(ticket);

            Stage dialogStage = new Stage();
            dialogStage.setTitle("Xác nhận hủy vé — MOCK D08");
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

            // Refresh lại lựa chọn trên bảng sau khi đóng dialog
            ticketTableView.refresh();
            updateSelectedTicketUI(ticket);
        } catch (IOException e) {
            String errorMsg = "Lỗi khi mở form hủy vé: " + e.getMessage();
            setSearchResultMessage(errorMsg, "search-msg-error");
            e.printStackTrace();
        }
    }

    // =========================================================================
    // ĐIỀU HƯỚNG
    // =========================================================================

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
