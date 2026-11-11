package vn.nhom6.railway.client.controller;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.Region;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel.ConnectionState;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel.NodeRole;

/**
 * Controller cho thanh hiển thị trạng thái kết nối cụm, Node và Leader (TASK TV1-D11).
 *
 * <p>Nguyên tắc hiển thị (SPEC-06 & TV1-D11):
 * <ul>
 *   <li>Trạng thái ban đầu: {@code UNKNOWN}, Role {@code UNKNOWN}, DataSource {@code NONE}. Không hardcode Node 1.</li>
 *   <li>Chấm tròn màu biểu thị kết nối: Xanh (Connected), Vàng (Connecting), Đỏ (Disconnected), Xám (Unknown).</li>
 *   <li>Huy hiệu (badge) vai trò Node: LEADER (xanh lá), FOLLOWER (xanh dương), CANDIDATE (tím), UNKNOWN (xám).</li>
 *   <li>Huy hiệu nguồn dữ liệu minh bạch: REAL (kết nối mạng TCP thật), MOCK (giả lập), NONE (chưa có).</li>
 *   <li>Nút "Làm mới" cho phép người dùng chủ động thăm dò Node bất đồng bộ.</li>
 * </ul>
 */
public class ClusterStatusBarController {

    @FXML
    private Region statusDot;

    @FXML
    private Label connectionStatusLabel;

    @FXML
    private Label nodeIdLabel;

    @FXML
    private Label roleBadgeLabel;

    @FXML
    private Label leaderIdLabel;

    @FXML
    private Label termLabel;

    @FXML
    private Label lastUpdatedLabel;

    @FXML
    private Label dataSourceBadgeLabel;

    @FXML
    private Button refreshBtn;

    private ClusterConnectionViewModel viewModel;

    @FXML
    public void initialize() {
        this.viewModel = ClientFactory.getClusterConnectionViewModel();
        bindViewModel(this.viewModel);
    }

    /**
     * Cho phép tiêm hoặc đổi ViewModel (dùng trong kiểm thử).
     */
    public void setViewModel(ClusterConnectionViewModel viewModel) {
        if (viewModel != null) {
            this.viewModel = viewModel;
            bindViewModel(viewModel);
        }
    }

    public ClusterConnectionViewModel getViewModel() {
        return viewModel;
    }

    private void bindViewModel(ClusterConnectionViewModel vm) {
        if (vm == null) {
            return;
        }

        // 1. Trạng thái kết nối & Chấm tròn màu
        vm.connectionStateProperty().addListener((obs, oldVal, newVal) -> updateConnectionStateUi(newVal));
        updateConnectionStateUi(vm.getConnectionState());

        // 2. Node ID đang kết nối
        vm.connectedNodeIdProperty().addListener((obs, oldVal, newVal) -> updateNodeIdUi(newVal));
        updateNodeIdUi(vm.getConnectedNodeId());

        // 3. Vai trò của Node (Role badge)
        vm.roleProperty().addListener((obs, oldVal, newVal) -> updateRoleUi(newVal));
        updateRoleUi(vm.getRole());

        // 4. Leader hiện tại (hoặc gợi ý leaderHint)
        vm.leaderHintProperty().addListener((obs, oldVal, newVal) -> updateLeaderUi(newVal));
        updateLeaderUi(vm.getLeaderHint());

        // 5. Term
        vm.termTextProperty().addListener((obs, oldVal, newVal) -> updateTermUi(newVal));
        updateTermUi(vm.getTermText());

        // 6. Thời gian cập nhật cuối
        vm.lastUpdatedTextProperty().addListener((obs, oldVal, newVal) -> updateLastUpdatedUi(newVal));
        updateLastUpdatedUi(vm.getLastUpdatedText());

        // 7. Huy hiệu Nguồn dữ liệu (REAL / MOCK / NONE)
        vm.dataSourceProperty().addListener((obs, oldVal, newVal) -> updateDataSourceUi(newVal));
        updateDataSourceUi(vm.getDataSource());

        // 8. Nút làm mới và trạng thái bận (busy)
        if (refreshBtn != null) {
            refreshBtn.disableProperty().bind(vm.busyProperty());
            refreshBtn.setOnAction(e -> vm.pollNow());
            refreshBtn.setTooltip(new Tooltip("Thăm dò lại trạng thái Node ngay lập tức"));
        }

        // 9. Tooltip thông báo chi tiết
        vm.messageProperty().addListener((obs, oldVal, newVal) -> {
            if (connectionStatusLabel != null && newVal != null) {
                connectionStatusLabel.setTooltip(new Tooltip(newVal));
            }
        });
    }

    private void updateConnectionStateUi(ConnectionState state) {
        if (connectionStatusLabel != null) {
            connectionStatusLabel.setText(state != null ? state.getDisplay() : ConnectionState.UNKNOWN.getDisplay());
        }

        if (statusDot != null) {
            statusDot.getStyleClass().removeAll(
                    "status-dot-connected",
                    "status-dot-connecting",
                    "status-dot-disconnected",
                    "status-dot-unknown"
            );
            String dotClass = (state != null) ? state.getDotStyleClass() : ConnectionState.UNKNOWN.getDotStyleClass();
            if (!statusDot.getStyleClass().contains(dotClass)) {
                statusDot.getStyleClass().add(dotClass);
            }
        }
    }

    private void updateNodeIdUi(String node) {
        if (nodeIdLabel != null) {
            nodeIdLabel.setText((node != null && !node.isBlank()) ? node : "-");
        }
    }

    private void updateRoleUi(NodeRole r) {
        if (roleBadgeLabel != null) {
            NodeRole role = (r != null) ? r : NodeRole.UNKNOWN;
            roleBadgeLabel.setText(role.getDisplay());
            roleBadgeLabel.getStyleClass().removeAll(
                    "role-leader",
                    "role-follower",
                    "role-candidate",
                    "role-unknown"
            );
            if (!roleBadgeLabel.getStyleClass().contains(role.getBadgeStyleClass())) {
                roleBadgeLabel.getStyleClass().add(role.getBadgeStyleClass());
            }
        }
    }

    private void updateLeaderUi(String leader) {
        if (leaderIdLabel != null) {
            leaderIdLabel.setText((leader != null && !leader.isBlank()) ? leader : "-");
        }
    }

    private void updateTermUi(String term) {
        if (termLabel != null) {
            termLabel.setText((term != null && !term.isBlank()) ? term : "-");
        }
    }

    private void updateLastUpdatedUi(String timeText) {
        if (lastUpdatedLabel != null) {
            lastUpdatedLabel.setText((timeText != null && !timeText.isBlank()) ? timeText : "-");
        }
    }

    private void updateDataSourceUi(DataSource ds) {
        if (dataSourceBadgeLabel != null) {
            DataSource source = (ds != null) ? ds : DataSource.NONE;
            dataSourceBadgeLabel.setText(source.getLabel());
            dataSourceBadgeLabel.getStyleClass().removeAll(
                    "badge-real",
                    "badge-mock",
                    "badge-none"
            );
            if (!dataSourceBadgeLabel.getStyleClass().contains(source.getStyleClass())) {
                dataSourceBadgeLabel.getStyleClass().add(source.getStyleClass());
            }
        }
    }
}
