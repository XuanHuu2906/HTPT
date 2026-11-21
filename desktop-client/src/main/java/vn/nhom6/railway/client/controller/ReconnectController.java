package vn.nhom6.railway.client.controller;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.LeaderStateListener;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiDispatcher;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;

import java.util.concurrent.CompletableFuture;

/**
 * Controller quản lý hiển thị trạng thái Reconnect khi Leader lỗi và bảo toàn Pending Request (TASK TV1-D21).
 * <p>
 * Bất biến nghiệp vụ & Kiến trúc Clean Architecture (AGENTS.md):
 * <ul>
 *   <li>Không import {@code java.net} hay {@code java.sql} trong controller.</li>
 *   <li>Cập nhật thuộc tính UI thông qua {@link UiDispatcher} (không chặn JavaFX thread, không dùng {@code .get()} hay {@code Thread.sleep}).</li>
 *   <li>Chặn tạo yêu cầu đặt vé MỚI khi đang tìm Leader; tuyệt đối không chặn tra cứu hay gửi lại các pending request hiện có.</li>
 *   <li>Pending journal giữ nguyên trạng thái {@code SENT_UNRESOLVED}/{@code FAILED_NOT_SENT}; không tự ý sửa hay xóa journal.</li>
 * </ul>
 */
public class ReconnectController implements LeaderStateListener {

    public enum ReconnectState {
        CONNECTED("Đã kết nối Leader", "reconnect-connected"),
        SEARCHING_LEADER("Đang tìm Leader trong cụm...", "reconnect-searching"),
        NO_LEADER("Không tìm thấy Leader khả dụng", "reconnect-no-leader");

        private final String defaultTitle;
        private final String styleClass;

        ReconnectState(String defaultTitle, String styleClass) {
            this.defaultTitle = defaultTitle;
            this.styleClass = styleClass;
        }

        public String getDefaultTitle() {
            return defaultTitle;
        }

        public String getStyleClass() {
            return styleClass;
        }
    }

    private final LeaderAwareBookingClient leaderClient;
    private final BookingViewModel bookingViewModel;
    private final ClusterConnectionViewModel connectionViewModel;
    private final PersistentPendingJournal journal;
    private UiDispatcher uiDispatcher;

    // === JavaFX Properties quan sát bởi UI ===
    private final ObjectProperty<ReconnectState> state = new SimpleObjectProperty<>(ReconnectState.CONNECTED);
    private final StringProperty bannerMessage = new SimpleStringProperty("");
    private final BooleanProperty bannerVisible = new SimpleBooleanProperty(false);
    private final BooleanProperty canBookNew = new SimpleBooleanProperty(true);
    private final BooleanProperty canRetryOrLookup = new SimpleBooleanProperty(true);
    private final StringProperty currentLeaderId = new SimpleStringProperty("-");
    private final ObjectProperty<DataSource> dataSource = new SimpleObjectProperty<>(DataSource.REAL);

    public ReconnectController(
            LeaderAwareBookingClient leaderClient,
            BookingViewModel bookingViewModel,
            ClusterConnectionViewModel connectionViewModel,
            PersistentPendingJournal journal) {
        this(leaderClient, bookingViewModel, connectionViewModel, journal, null);
    }

    public ReconnectController(
            LeaderAwareBookingClient leaderClient,
            BookingViewModel bookingViewModel,
            ClusterConnectionViewModel connectionViewModel,
            PersistentPendingJournal journal,
            UiDispatcher uiDispatcher) {
        this.leaderClient = leaderClient;
        this.bookingViewModel = bookingViewModel;
        this.connectionViewModel = connectionViewModel;
        this.journal = journal;
        this.uiDispatcher = (uiDispatcher != null) ? uiDispatcher : Runnable::run;

        if (this.leaderClient != null) {
            this.leaderClient.addLeaderStateListener(this);
            String current = this.leaderClient.getKnownLeaderId();
            if (current != null && !current.isBlank()) {
                this.currentLeaderId.set(current);
            }
        }

        if (this.bookingViewModel != null) {
            this.bookingViewModel.setRequestAllowedPredicate(this::isRequestAllowed);
        }
    }

    public void setUiDispatcher(UiDispatcher uiDispatcher) {
        this.uiDispatcher = (uiDispatcher != null) ? uiDispatcher : Runnable::run;
    }

    public void setDataSource(DataSource source) {
        if (source != null) {
            runOnUi(() -> this.dataSource.set(source));
        }
    }

    private void runOnUi(Runnable action) {
        if (uiDispatcher != null) {
            uiDispatcher.dispatch(action);
        } else {
            action.run();
        }
    }

    // =========================================================================
    // QUY TẮC THAO TÁC (CHẶN ĐẶT MỚI KHI TÌM LEADER, KHÔNG CHẶN PENDING)
    // =========================================================================

    /**
     * Kiểm tra xem một requestId có được phép thực thi không.
     * <p>
     * Quyết định đã chọn: Chặn cứng đặt vé mới (hard block) khi cụm đang tìm Leader hoặc không có Leader,
     * vì cụm có thể đang bầu cử, tránh sinh requestId rác và gây tải thêm cho mạng.
     * Ngược lại, KHÔNG chặn các lệnh retry/tra cứu cho các requestId đã tồn tại trong journal.
     *
     * @param requestId mã yêu cầu muốn gửi
     * @return {@code true} nếu được phép tiếp tục
     */
    public boolean isRequestAllowed(String requestId) {
        if (state.get() == ReconnectState.CONNECTED) {
            return true;
        }
        // Khi đang tìm Leader hoặc không có Leader:
        // Cho phép nếu requestId này đã có trong Journal (lệnh pending cũ)
        if (journal != null && requestId != null && !requestId.isBlank()) {
            return journal.get(requestId).isPresent();
        }
        return false;
    }

    /**
     * Kích hoạt dò tìm Leader chủ động qua phương thức discoverLeader của client.
     *
     * @return CompletableFuture chứa mã Leader mới tìm thấy (hoặc null)
     */
    public CompletableFuture<String> triggerReconnect() {
        if (leaderClient != null) {
            return leaderClient.discoverLeader();
        }
        return CompletableFuture.completedFuture(null);
    }

    // =========================================================================
    // LEADER STATE LISTENER CALLBACKS
    // =========================================================================

    @Override
    public void onLeaderLost(String oldLeaderId, String reason) {
        runOnUi(() -> {
            this.state.set(ReconnectState.SEARCHING_LEADER);
            this.canBookNew.set(false);
            this.canRetryOrLookup.set(true);
            this.bannerVisible.set(true);

            String srcTag = "[" + dataSource.get().getLabel() + "]";
            String oldNode = (oldLeaderId != null && !oldLeaderId.isBlank()) ? oldLeaderId : "?";
            String reasonMsg = (reason != null && !reason.isBlank()) ? " (" + reason + ")" : "";
            this.bannerMessage.set("Mất kết nối với Leader [" + oldNode + "]" + reasonMsg + ". Đang tìm Leader... " + srcTag);

            if (connectionViewModel != null) {
                connectionViewModel.markOffline("Mất Leader " + oldNode + reasonMsg, dataSource.get());
            }
        });
    }

    @Override
    public void onSearchingLeader() {
        runOnUi(() -> {
            this.state.set(ReconnectState.SEARCHING_LEADER);
            this.canBookNew.set(false);
            this.canRetryOrLookup.set(true);
            this.bannerVisible.set(true);

            String srcTag = "[" + dataSource.get().getLabel() + "]";
            this.bannerMessage.set("Đang tìm Leader trong cụm (Reconnecting)... " + srcTag);
        });
    }

    @Override
    public void onLeaderDiscovered(String newLeaderId, DataSource source) {
        runOnUi(() -> {
            if (source != null) {
                this.dataSource.set(source);
            }
            this.state.set(ReconnectState.CONNECTED);
            this.currentLeaderId.set(newLeaderId != null ? newLeaderId : "-");
            this.canBookNew.set(true);
            this.canRetryOrLookup.set(true);
            this.bannerVisible.set(false);

            String srcTag = "[" + this.dataSource.get().getLabel() + "]";
            this.bannerMessage.set("Đã kết nối Leader [" + this.currentLeaderId.get() + "] " + srcTag);

            if (connectionViewModel != null && newLeaderId != null) {
                connectionViewModel.applyLeaderHint(newLeaderId, this.dataSource.get());
            }
        });
    }

    @Override
    public void onNoLeaderAvailable() {
        runOnUi(() -> {
            this.state.set(ReconnectState.NO_LEADER);
            this.canBookNew.set(false);
            this.canRetryOrLookup.set(true);
            this.bannerVisible.set(true);

            String srcTag = "[" + dataSource.get().getLabel() + "]";
            this.bannerMessage.set("Không tìm thấy Leader khả dụng trong cụm. Vui lòng bấm 'Thử tìm lại' hoặc đợi cụm khôi phục! " + srcTag);
        });
    }

    // =========================================================================
    // GETTERS & READ-ONLY PROPERTIES
    // =========================================================================

    public ReconnectState getState() {
        return state.get();
    }

    public ReadOnlyObjectProperty<ReconnectState> stateProperty() {
        return state;
    }

    public String getBannerMessage() {
        return bannerMessage.get();
    }

    public ReadOnlyStringProperty bannerMessageProperty() {
        return bannerMessage;
    }

    public boolean isBannerVisible() {
        return bannerVisible.get();
    }

    public ReadOnlyBooleanProperty bannerVisibleProperty() {
        return bannerVisible;
    }

    public boolean getCanBookNew() {
        return canBookNew.get();
    }

    public ReadOnlyBooleanProperty canBookNewProperty() {
        return canBookNew;
    }

    public boolean getCanRetryOrLookup() {
        return canRetryOrLookup.get();
    }

    public ReadOnlyBooleanProperty canRetryOrLookupProperty() {
        return canRetryOrLookup;
    }

    public String getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public ReadOnlyStringProperty currentLeaderIdProperty() {
        return currentLeaderId;
    }

    public DataSource getDataSource() {
        return dataSource.get();
    }

    public ReadOnlyObjectProperty<DataSource> dataSourceProperty() {
        return dataSource;
    }
}
