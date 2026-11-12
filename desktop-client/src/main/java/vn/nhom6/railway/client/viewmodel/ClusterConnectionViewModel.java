package vn.nhom6.railway.client.viewmodel;

import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.NodeStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * ViewModel quản lý hiển thị tình trạng kết nối mạng, Node và Leader của cụm phân tán (TASK TV1-D11).
 *
 * <p>Nguyên tắc thiết kế (AGENTS.md & TV1-D11):
 * <ul>
 *   <li>Trạng thái ban đầu bắt buộc: {@code UNKNOWN}, Role {@code UNKNOWN}, DataSource {@code NONE},
 *       Node/Leader/Term để trống/gạch nối. TUYỆT ĐỐI KHÔNG khởi tạo mặc định Node 1 hay LEADER.</li>
 *   <li>Phân biệt rạch ròi {@link DataSource}: REAL (TCP socket thật) vs MOCK (giả lập) vs NONE.</li>
 *   <li>Hỗ trợ 2 kênh cập nhật:
 *       1) Chủ động qua polling định kỳ hoặc nút làm mới (refresh) gọi {@code NODE_STATUS}.
 *       2) Bị động khi nhận lỗi {@code NOT_LEADER} kèm {@code leaderHint} từ luồng nghiệp vụ.</li>
 *   <li>Phát hiện dữ liệu quá hạn (stale > 10s): tự động chuyển trạng thái về {@code UNKNOWN}
 *       nhưng vẫn giữ lại thời điểm {@code lastUpdated} lần cuối ghi nhận.</li>
 *   <li>Chạy an toàn trên JavaFX Application Thread thông qua {@link Platform#runLater(Runnable)}.</li>
 * </ul>
 */
public class ClusterConnectionViewModel implements AutoCloseable {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    public static final long DEFAULT_POLL_INTERVAL_SECONDS = 3;
    public static final long STALE_THRESHOLD_SECONDS = 10;

    /**
     * Trạng thái kết nối tới Node.
     */
    public enum ConnectionState {
        CONNECTED("Đã kết nối", "status-dot-connected"),
        CONNECTING("Đang kết nối...", "status-dot-connecting"),
        DISCONNECTED("Mất kết nối", "status-dot-disconnected"),
        UNKNOWN("Chưa rõ kết nối", "status-dot-unknown");

        private final String display;
        private final String dotStyleClass;

        ConnectionState(String display, String dotStyleClass) {
            this.display = display;
            this.dotStyleClass = dotStyleClass;
        }

        public String getDisplay() {
            return display;
        }

        public String getDotStyleClass() {
            return dotStyleClass;
        }
    }

    /**
     * Vai trò Raft của Node quan sát được.
     */
    public enum NodeRole {
        LEADER("LEADER", "role-leader"),
        FOLLOWER("FOLLOWER", "role-follower"),
        CANDIDATE("CANDIDATE", "role-candidate"),
        UNKNOWN("UNKNOWN", "role-unknown");

        private final String display;
        private final String badgeStyleClass;

        NodeRole(String display, String badgeStyleClass) {
            this.display = display;
            this.badgeStyleClass = badgeStyleClass;
        }

        public String getDisplay() {
            return display;
        }

        public String getBadgeStyleClass() {
            return badgeStyleClass;
        }

        public static NodeRole fromString(String roleStr) {
            if (roleStr == null || roleStr.isBlank()) {
                return UNKNOWN;
            }
            try {
                return NodeRole.valueOf(roleStr.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return UNKNOWN;
            }
        }
    }

    private final ClusterStatusClient statusClient;
    private volatile boolean syncMode = false;

    // === JavaFX Properties quan sát bởi UI ===
    private final ObjectProperty<ConnectionState> connectionState = new SimpleObjectProperty<>(ConnectionState.UNKNOWN);
    private final StringProperty connectedNodeId = new SimpleStringProperty("-");
    private final ObjectProperty<NodeRole> role = new SimpleObjectProperty<>(NodeRole.UNKNOWN);
    private final StringProperty leaderHint = new SimpleStringProperty("-");
    private final StringProperty termText = new SimpleStringProperty("-");
    private final ObjectProperty<Long> term = new SimpleObjectProperty<>(null);
    private final StringProperty lastUpdatedText = new SimpleStringProperty("-");
    private final ObjectProperty<Instant> lastUpdated = new SimpleObjectProperty<>(null);
    private final ObjectProperty<DataSource> dataSource = new SimpleObjectProperty<>(DataSource.NONE);
    private final StringProperty message = new SimpleStringProperty("Chưa kết nối cụm / Trạng thái UNKNOWN");
    private final StringProperty endpoint = new SimpleStringProperty("-");
    private final BooleanProperty busy = new SimpleBooleanProperty(false);
    private final BooleanProperty stale = new SimpleBooleanProperty(false);

    // === Polling Scheduler ===
    private final ScheduledExecutorService pollScheduler;
    private ScheduledFuture<?> pollingTask;

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "cluster-status-poller");
        t.setDaemon(true);
        return t;
    };

    public ClusterConnectionViewModel(ClusterStatusClient statusClient) {
        this.statusClient = Objects.requireNonNull(statusClient, "ClusterStatusClient không được null");
        this.pollScheduler = Executors.newSingleThreadScheduledExecutor(DAEMON_THREAD_FACTORY);
        this.endpoint.set(statusClient.getEndpoint() != null ? statusClient.getEndpoint() : "-");
    }

    public void setSyncMode(boolean syncMode) {
        this.syncMode = syncMode;
    }

    private void runOnFxThread(Runnable action) {
        if (syncMode || Platform.isFxApplicationThread()) {
            action.run();
        } else {
            try {
                Platform.runLater(action);
            } catch (IllegalStateException e) {
                action.run();
            }
        }
    }

    // =========================================================================
    // CÁC THAO TÁC CẬP NHẬT TRẠNG THÁI (STATE MUTATIONS)
    // =========================================================================

    /**
     * Cập nhật trạng thái từ bản tin phản hồi NODE_STATUS thành công.
     */
    public void applyStatus(NodeStatus status, DataSource source) {
        if (status == null) {
            markUnknown();
            return;
        }

        runOnFxThread(() -> {
            this.connectionState.set(ConnectionState.CONNECTED);
            this.stale.set(false);
            this.dataSource.set(source != null ? source : statusClient.getDataSource());

            String node = status.nodeId();
            this.connectedNodeId.set((node != null && !node.isBlank()) ? node : "-");

            NodeRole r = NodeRole.fromString(status.role());
            this.role.set(r);

            Long t = status.term();
            this.term.set(t);
            this.termText.set(t != null ? String.valueOf(t) : "-");

            String leader = status.leaderId();
            this.leaderHint.set((leader != null && !leader.isBlank()) ? leader : "-");

            Instant now = Instant.now();
            this.lastUpdated.set(now);
            this.lastUpdatedText.set(TIME_FORMATTER.format(now));

            this.endpoint.set(statusClient.getEndpoint() != null ? statusClient.getEndpoint() : "-");

            String sourceTag = "[" + this.dataSource.get().getLabel() + "]";
            this.message.set("Kết nối tốt tới " + this.connectedNodeId.get() + " (" + r.getDisplay() + ", Term " + this.termText.get() + ") " + sourceTag);
        });
    }

    /**
     * Kênh bị động: Nhận gợi ý Leader từ phản hồi nghiệp vụ NOT_LEADER (SPEC-03 §7).
     */
    public void applyLeaderHint(String hint, DataSource source) {
        runOnFxThread(() -> {
            if (hint != null && !hint.isBlank()) {
                this.leaderHint.set(hint.trim());
                if (source != null) {
                    this.dataSource.set(source);
                }
                String sourceTag = "[" + this.dataSource.get().getLabel() + "]";
                this.message.set("Nhận gợi ý Leader: " + hint.trim() + " (từ phản hồi NOT_LEADER) " + sourceTag);
            }
        });
    }

    /**
     * Đánh dấu mất kết nối khi gọi mạng lỗi hoặc timeout.
     * Vẫn bảo lưu timestamp {@code lastUpdated} lần cuối nếu có.
     */
    public void markOffline(String reason, DataSource source) {
        runOnFxThread(() -> {
            this.connectionState.set(ConnectionState.DISCONNECTED);
            if (source != null) {
                this.dataSource.set(source);
            }
            String sourceTag = "[" + this.dataSource.get().getLabel() + "]";
            this.message.set("Mất kết nối tới Node: " + (reason != null ? reason : "Offline") + " " + sourceTag);
        });
    }

    /**
     * Đánh dấu dữ liệu quá hạn (stale > 10s):
     * Chuyển trạng thái về UNKNOWN và gán cờ stale=true, nhưng giữ nguyên lastUpdated.
     */
    public void markStale() {
        runOnFxThread(() -> {
            this.stale.set(true);
            this.connectionState.set(ConnectionState.UNKNOWN);
            this.role.set(NodeRole.UNKNOWN);
            this.message.set("Dữ liệu quá hạn (> 10s không có phản hồi mới từ Node)");
        });
    }

    /**
     * Thiết lập lại toàn bộ về trạng thái UNKNOWN ban đầu.
     */
    public void markUnknown() {
        runOnFxThread(() -> {
            this.connectionState.set(ConnectionState.UNKNOWN);
            this.connectedNodeId.set("-");
            this.role.set(NodeRole.UNKNOWN);
            this.leaderHint.set("-");
            this.term.set(null);
            this.termText.set("-");
            this.lastUpdated.set(null);
            this.lastUpdatedText.set("-");
            this.dataSource.set(DataSource.NONE);
            this.stale.set(false);
            this.message.set("Chưa kết nối cụm / Trạng thái UNKNOWN");
        });
    }

    // =========================================================================
    // POLLING VÀ THĂM DÒ CHỦ ĐỘNG
    // =========================================================================

    /**
     * Kích hoạt polling định kỳ qua ScheduledExecutorService daemon.
     */
    public synchronized void startPolling(long intervalSeconds) {
        if (pollingTask != null && !pollingTask.isCancelled()) {
            return;
        }

        long interval = (intervalSeconds > 0) ? intervalSeconds : DEFAULT_POLL_INTERVAL_SECONDS;
        pollingTask = pollScheduler.scheduleWithFixedDelay(
                this::pollTick,
                0,
                interval,
                TimeUnit.SECONDS
        );
    }

    /**
     * Thực hiện một nhịp kiểm tra: thăm dò Node và kiểm tra stale.
     */
    private void pollTick() {
        // 1. Kiểm tra stale trước
        checkStaleCondition();

        // 2. Thăm dò NodeStatus
        pollNow();
    }

    /**
     * Kiểm tra điều kiện dữ liệu quá hạn (> 10s).
     */
    public void checkStaleCondition() {
        Instant last = lastUpdated.get();
        if (last != null) {
            Duration duration = Duration.between(last, Instant.now());
            if (duration.getSeconds() >= STALE_THRESHOLD_SECONDS && connectionState.get() == ConnectionState.CONNECTED) {
                markStale();
            }
        }
    }

    /**
     * Thăm dò trạng thái Node ngay lập tức bất đồng bộ (dùng cho cả nút "Làm mới").
     */
    public CompletableFuture<NodeStatus> pollNow() {
        if (busy.get()) {
            CompletableFuture<NodeStatus> skipped = new CompletableFuture<>();
            skipped.complete(null);
            return skipped;
        }

        runOnFxThread(() -> {
            busy.set(true);
            if (connectionState.get() == ConnectionState.UNKNOWN) {
                connectionState.set(ConnectionState.CONNECTING);
            }
        });

        CompletableFuture<NodeStatus> future = new CompletableFuture<>();

        statusClient.getNodeStatus().whenComplete((status, throwable) -> {
            runOnFxThread(() -> busy.set(false));
            if (throwable != null) {
                String errorMsg = throwable.getMessage();
                if (throwable.getCause() != null) {
                    errorMsg = throwable.getCause().getMessage();
                }
                markOffline(errorMsg, statusClient.getDataSource());
                future.completeExceptionally(throwable);
            } else {
                applyStatus(status, statusClient.getDataSource());
                future.complete(status);
            }
        });

        return future;
    }

    public synchronized void stopPolling() {
        if (pollingTask != null) {
            pollingTask.cancel(true);
            pollingTask = null;
        }
    }

    @Override
    public void close() {
        stopPolling();
        pollScheduler.shutdownNow();
        try {
            statusClient.close();
        } catch (Exception ignored) {
        }
    }

    // =========================================================================
    // GETTERS & READ-ONLY PROPERTIES
    // =========================================================================

    public ReadOnlyObjectProperty<ConnectionState> connectionStateProperty() {
        return connectionState;
    }

    public ConnectionState getConnectionState() {
        return connectionState.get();
    }

    public ReadOnlyStringProperty connectedNodeIdProperty() {
        return connectedNodeId;
    }

    public String getConnectedNodeId() {
        return connectedNodeId.get();
    }

    public ReadOnlyObjectProperty<NodeRole> roleProperty() {
        return role;
    }

    public NodeRole getRole() {
        return role.get();
    }

    public ReadOnlyStringProperty leaderHintProperty() {
        return leaderHint;
    }

    public String getLeaderHint() {
        return leaderHint.get();
    }

    public ReadOnlyObjectProperty<Long> termProperty() {
        return term;
    }

    public Long getTerm() {
        return term.get();
    }

    public ReadOnlyStringProperty termTextProperty() {
        return termText;
    }

    public String getTermText() {
        return termText.get();
    }

    public ReadOnlyObjectProperty<Instant> lastUpdatedProperty() {
        return lastUpdated;
    }

    public Instant getLastUpdated() {
        return lastUpdated.get();
    }

    public ReadOnlyStringProperty lastUpdatedTextProperty() {
        return lastUpdatedText;
    }

    public String getLastUpdatedText() {
        return lastUpdatedText.get();
    }

    public ReadOnlyObjectProperty<DataSource> dataSourceProperty() {
        return dataSource;
    }

    public DataSource getDataSource() {
        return dataSource.get();
    }

    public ReadOnlyStringProperty messageProperty() {
        return message;
    }

    public String getMessage() {
        return message.get();
    }

    public ReadOnlyStringProperty endpointProperty() {
        return endpoint;
    }

    public String getEndpoint() {
        return endpoint.get();
    }

    public ReadOnlyBooleanProperty busyProperty() {
        return busy;
    }

    public boolean isBusy() {
        return busy.get();
    }

    public ReadOnlyBooleanProperty staleProperty() {
        return stale;
    }

    public boolean isStale() {
        return stale.get();
    }

    public ClusterStatusClient getStatusClient() {
        return statusClient;
    }
}
