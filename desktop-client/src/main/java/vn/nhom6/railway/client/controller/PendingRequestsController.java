package vn.nhom6.railway.client.controller;

import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel.PendingItem;

/**
 * Controller điều khiển panel hiển thị danh sách yêu cầu chờ xử lý (TASK TV1-D14).
 * <p>
 * Bất biến:
 * 1. Không import java.net hay java.sql.
 * 2. Phản ánh đúng trạng thái từ ViewModel, không tự gán kết quả thành công.
 * 3. Hỗ trợ thao tác Kiểm tra kết quả (GET_REQUEST_RESULT) và Gửi lại (retry cùng requestId).
 */
public class PendingRequestsController {

    @FXML
    private Label pendingCountBadge;

    @FXML
    private Button refreshBtn;

    @FXML
    private TableView<PendingItem> pendingTableView;

    @FXML
    private TableColumn<PendingItem, String> requestIdCol;

    @FXML
    private TableColumn<PendingItem, String> summaryCol;

    @FXML
    private TableColumn<PendingItem, String> statusCol;

    @FXML
    private TableColumn<PendingItem, Number> attemptsCol;

    @FXML
    private TableColumn<PendingItem, String> lastAttemptCol;

    @FXML
    private TableColumn<PendingItem, String> sourceCol;

    @FXML
    private TableColumn<PendingItem, Void> actionsCol;

    @FXML
    private Label statusLabel;

    @FXML
    private ProgressIndicator busyIndicator;

    private PendingRequestsViewModel viewModel;

    @FXML
    public void initialize() {
        this.viewModel = ClientFactory.getPendingRequestsViewModel();

        setupTableColumns();
        bindViewModel();
    }

    private void setupTableColumns() {
        requestIdCol.setCellValueFactory(cellData -> new SimpleStringProperty(cellData.getValue().getRequestId()));
        requestIdCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setTooltip(null);
                } else {
                    String shortId = item.length() > 8 ? item.substring(0, 8) + "..." : item;
                    setText(shortId);
                    setTooltip(new Tooltip(item));
                }
            }
        });

        summaryCol.setCellValueFactory(cellData -> new SimpleStringProperty(cellData.getValue().getSummary()));

        statusCol.setCellValueFactory(cellData -> cellData.getValue().displayStatusProperty());
        statusCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setStyle(null);
                } else {
                    setText(item);
                    PendingItem pendingItem = getTableRow() != null ? getTableRow().getItem() : null;
                    if (pendingItem != null) {
                        String styleClass = pendingItem.getStyleClass();
                        if ("booking-status-confirmed".equals(styleClass)) {
                            setStyle("-fx-text-fill: #059669; -fx-font-weight: bold;");
                        } else if ("booking-status-rejected".equals(styleClass)) {
                            setStyle("-fx-text-fill: #dc2626; -fx-font-weight: bold;");
                        } else if ("status-error".equals(styleClass)) {
                            setStyle("-fx-text-fill: #b91c1c; -fx-font-weight: bold;");
                        } else {
                            // booking-status-unknown (CHƯA XÁC NHẬN)
                            setStyle("-fx-text-fill: #ea580c; -fx-font-weight: bold;");
                        }
                    }
                }
            }
        });

        attemptsCol.setCellValueFactory(cellData -> new SimpleIntegerProperty(cellData.getValue().getAttemptCount()));

        lastAttemptCol.setCellValueFactory(cellData -> cellData.getValue().lastAttemptProperty());
        lastAttemptCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                } else {
                    // Cắt bớt phần timestamp ISO để dễ đọc trên UI
                    String display = item.replace("T", " ");
                    int dotIdx = display.indexOf('.');
                    if (dotIdx > 0) {
                        display = display.substring(0, dotIdx);
                    }
                    setText(display);
                }
            }
        });

        sourceCol.setCellValueFactory(cellData -> new SimpleStringProperty(cellData.getValue().getSourceLabel()));
        sourceCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setStyle(null);
                } else {
                    setText(item);
                    if ("REAL".equals(item)) {
                        setStyle("-fx-text-fill: #0284c7; -fx-font-weight: bold;");
                    } else if ("FAKE-TEST".equals(item)) {
                        setStyle("-fx-text-fill: #7c3aed; -fx-font-weight: bold;");
                    } else {
                        setStyle("-fx-text-fill: #d97706; -fx-font-weight: bold;");
                    }
                }
            }
        });

        actionsCol.setCellFactory(col -> new TableCell<>() {
            private final Button checkBtn = new Button("🔍 Tra cứu");
            private final Button retryBtn = new Button("🔁 Gửi lại");
            private final HBox container = new HBox(6, checkBtn, retryBtn);

            {
                container.setAlignment(Pos.CENTER);
                checkBtn.setStyle("-fx-background-color: #e0f2fe; -fx-text-fill: #0369a1; -fx-font-size: 11px; -fx-padding: 3 6; -fx-cursor: hand;");
                retryBtn.setStyle("-fx-background-color: #fef3c7; -fx-text-fill: #92400e; -fx-font-size: 11px; -fx-padding: 3 6; -fx-cursor: hand;");

                checkBtn.setOnAction(e -> {
                    PendingItem item = getTableRow().getItem();
                    if (item != null && viewModel != null) {
                        viewModel.checkResult(item);
                    }
                });

                retryBtn.setOnAction(e -> {
                    PendingItem item = getTableRow().getItem();
                    if (item != null && viewModel != null) {
                        viewModel.retry(item);
                    }
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || getTableRow() == null || getTableRow().getItem() == null) {
                    setGraphic(null);
                } else {
                    PendingItem pendingItem = getTableRow().getItem();
                    checkBtn.disableProperty().bind(pendingItem.checkingProperty().or(viewModel.busyProperty()));
                    retryBtn.disableProperty().bind(pendingItem.canRetryProperty().not().or(pendingItem.checkingProperty()).or(viewModel.busyProperty()));
                    setGraphic(container);
                }
            }
        });
    }

    private void bindViewModel() {
        if (viewModel == null) return;

        pendingTableView.setItems(viewModel.getPendingItems());
        statusLabel.textProperty().bind(viewModel.statusMessageProperty());
        busyIndicator.visibleProperty().bind(viewModel.busyProperty());

        pendingCountBadge.textProperty().bind(Bindings.createStringBinding(
                () -> viewModel.getPendingItems().size() + " yêu cầu",
                viewModel.getPendingItems()
        ));
    }

    @FXML
    public void handleRefresh() {
        if (viewModel != null) {
            viewModel.reloadFromJournal();
        }
    }
}
