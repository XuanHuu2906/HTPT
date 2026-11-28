package vn.nhom6.railway.client.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;

import java.io.IOException;

/**
 * Controller điều khiển giao diện chính của ứng dụng bán vé Desktop (JavaFX).
 */
public class MainController {

    private static final System.Logger LOGGER = System.getLogger(MainController.class.getName());

    @FXML
    private BorderPane rootBorderPane;

    @FXML
    private VBox mainCenterContainer;

    @FXML
    private Button openSeatGridBtn;

    @FXML
    private Label tripTitleLabel;

    @FXML
    private Label modeBadgeLabel;

    @FXML
    private Label mockBadgeLabel;

    @FXML
    private Label tripConnectionBadgeLabel;

    @FXML
    private Label sidebarModeLabel;

    @FXML
    private Label sidebarBackendLabel;

    @FXML
    private Label releaseInfoLabel;

    @FXML
    private ClusterStatusBarController clusterStatusBarController;

    @FXML
    private PendingRequestsController pendingRequestsPanelController;

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

    @FXML
    private Label statusMessageLabel;

    private Parent cachedSeatGridView;
    private SeatGridController cachedSeatGridController;

    private Parent cachedTicketStatusView;
    private TicketStatusController cachedTicketStatusController;

    @FXML
    public void initialize() {
        ClientConfig cfg = ClientConfig.loadDefault();
        boolean realFlow = cfg.isReleaseMode() || cfg.isSocketMode();

        if (realFlow) {
            removeNodeFromParent(mockBadgeLabel);
            if (modeBadgeLabel != null) {
                modeBadgeLabel.setStyle(null);
                modeBadgeLabel.getStyleClass().removeAll("badge-mock", "badge-cluster-mode", "badge-release-mode");
                if (cfg.isReleaseMode()) {
                    modeBadgeLabel.setText("RELEASE v" + cfg.getReleaseVersion() + " (" + cfg.getProfile().toUpperCase() + ")");
                    modeBadgeLabel.getStyleClass().add("badge-release-mode");
                } else {
                    modeBadgeLabel.setText("CỤM 5 NODE (" + cfg.getProfile().toUpperCase() + ")");
                    modeBadgeLabel.getStyleClass().add("badge-cluster-mode");
                }
            }
            if (tripConnectionBadgeLabel != null) {
                tripConnectionBadgeLabel.setText("Hồ sơ kết nối: " + cfg.getProfile().toUpperCase()
                        + (cfg.isReleaseMode() ? " · RELEASE" : ""));
            }
            if (statusMessageLabel != null) {
                statusMessageLabel.setText("Hệ thống kết nối Cụm 5 Node (" + cfg.getProfile().toUpperCase()
                        + "). Nhấn 'Đặt vé' để vào sơ đồ ghế.");
            }
        } else {
            removeNodeFromParent(modeBadgeLabel);
            if (mockBadgeLabel != null) {
                mockBadgeLabel.setStyle(null);
                mockBadgeLabel.getStyleClass().removeAll("badge-cluster-mode", "badge-release-mode");
                if (!mockBadgeLabel.getStyleClass().contains("badge-mock")) {
                    mockBadgeLabel.getStyleClass().add("badge-mock");
                }
                mockBadgeLabel.setText("DEV PREVIEW");
            }
            if (tripConnectionBadgeLabel != null) {
                tripConnectionBadgeLabel.setText("Chế độ: DEV OFFLINE");
            }
            if (statusMessageLabel != null) {
                statusMessageLabel.setText("Hệ thống đang chạy ở chế độ phát triển cục bộ.");
            }
        }

        bindReleaseMetadataLabel(cfg);
        bindNavigationHandlers();
    }

    private void bindReleaseMetadataLabel(ClientConfig cfg) {
        if (releaseInfoLabel == null) {
            return;
        }
        ClusterConnectionViewModel connVm = ClientFactory.getClusterConnectionViewModel();
        updateReleaseMetadataText(cfg, connVm != null ? connVm.getConnectedNodeId() : "-");
        if (connVm != null) {
            connVm.connectedNodeIdProperty().addListener((obs, oldNode, newNode) ->
                    updateReleaseMetadataText(cfg, newNode));
        }
    }

    private void updateReleaseMetadataText(ClientConfig cfg, String nodeId) {
        if (releaseInfoLabel == null || cfg == null) {
            return;
        }
        String effectiveNode = (nodeId != null && !nodeId.isBlank()) ? nodeId : "-";
        String modeName = cfg.isReleaseMode() ? "RELEASE" : (cfg.isSocketMode() ? "SOCKET" : "DEV");
        releaseInfoLabel.setText("release.version=" + cfg.getReleaseVersion()
                + " | mode=" + modeName
                + " | profile=" + cfg.getProfile()
                + " | node=" + effectiveNode);
    }

    private static void removeNodeFromParent(Node node) {
        if (node != null && node.getParent() instanceof Pane parentPane) {
            parentPane.getChildren().remove(node);
        }
    }

    public void showSeatGridView() {
        try {
            if (cachedSeatGridView == null) {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/seat-grid.fxml"));
                cachedSeatGridView = loader.load();
                cachedSeatGridController = loader.getController();
                cachedSeatGridController.setOnBackAction(this::showMainView);
            }

            if (rootBorderPane != null) {
                rootBorderPane.setCenter(cachedSeatGridView);
                highlightNavButton(navSeatBtn);
                updateStatusMessage("Đang ở Sơ đồ chọn ghế tương tác. Click chọn các ghế mong muốn rồi nhấn 'Đặt vé'.");
            }
        } catch (IOException e) {
            updateStatusMessage("Không thể nạp giao diện chọn ghế.");
            LOGGER.log(System.Logger.Level.WARNING, "Lỗi nạp seat-grid.fxml", e);
        }
    }

    public void showMainView() {
        if (rootBorderPane != null && mainCenterContainer != null) {
            rootBorderPane.setCenter(mainCenterContainer);
            highlightNavButton(navTripBtn);
            updateStatusMessage("Đã quay về màn hình chính (Chuyến tàu SE-001).");
        }
    }

    public void showTicketStatusView() {
        try {
            if (cachedTicketStatusView == null) {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/ticket-status.fxml"));
                cachedTicketStatusView = loader.load();
                cachedTicketStatusController = loader.getController();
                cachedTicketStatusController.setOnBackAction(this::showMainView);
            }

            if (cachedTicketStatusController != null) {
                cachedTicketStatusController.refreshTicketList();
            }

            if (rootBorderPane != null) {
                rootBorderPane.setCenter(cachedTicketStatusView);
                highlightNavButton(navMyTicketsBtn);
                ClientConfig cfg = ClientConfig.loadDefault();
                updateStatusMessage((cfg.isReleaseMode() || cfg.isSocketMode())
                        ? "Đang ở màn hình Tra cứu & Quản lý vé (Cụm 5 Node)."
                        : "Đang ở màn hình Tra cứu & Quản lý vé cục bộ.");
            }
        } catch (IOException e) {
            updateStatusMessage("Không thể nạp giao diện tra cứu vé.");
            LOGGER.log(System.Logger.Level.WARNING, "Lỗi nạp ticket-status.fxml", e);
        }
    }

    public SeatGridController getCachedSeatGridController() {
        return cachedSeatGridController;
    }

    public TicketStatusController getCachedTicketStatusController() {
        return cachedTicketStatusController;
    }

    private void bindNavigationHandlers() {
        if (openSeatGridBtn != null) {
            openSeatGridBtn.setOnAction(e -> showSeatGridView());
        }
        if (navSeatBtn != null) {
            navSeatBtn.setOnAction(e -> showSeatGridView());
        }
        if (navTripBtn != null) {
            navTripBtn.setOnAction(e -> showMainView());
        }
        if (navMyTicketsBtn != null) {
            navMyTicketsBtn.setOnAction(e -> showTicketStatusView());
        }
        setupNavButton(navPaymentBtn);
    }

    private void highlightNavButton(Button activeBtn) {
        Button[] navButtons = {navTripBtn, navSeatBtn, navPaymentBtn, navMyTicketsBtn};
        for (Button btn : navButtons) {
            if (btn != null) {
                btn.setStyle(null);
                btn.getStyleClass().removeAll("nav-btn-default", "nav-btn-active");
                btn.getStyleClass().add(btn == activeBtn ? "nav-btn-active" : "nav-btn-default");
            }
        }
    }

    private void setupNavButton(Button btn) {
        if (btn != null) {
            btn.setOnAction(e -> updateStatusMessage("Chức năng thanh toán trực tuyến nằm ngoài phạm vi phiên bản v1 (SPEC-01 §7)."));
        }
    }

    public void updateStatusMessage(String message) {
        if (statusMessageLabel != null) {
            statusMessageLabel.setText(message);
        }
    }

    public ClusterStatusBarController getClusterStatusBarController() {
        return clusterStatusBarController;
    }

    public Label getReleaseInfoLabel() {
        return releaseInfoLabel;
    }
}
