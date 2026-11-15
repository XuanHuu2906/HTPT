package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.LongProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.ObservableList;
import vn.nhom6.railway.client.network.DataSource;
import vn.nhom6.railway.client.network.ReadAdapter;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.network.VerificationLevel;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service làm mới sơ đồ ghế an toàn cho UI Desktop (TASK TV1-D15).
 * <p>
 * <b>Các cam kết và invariant quan trọng:</b>
 * <ul>
 *   <li><b>Read-safe UI:</b> Mọi dữ liệu đọc từ Node trước khi TV4-D19 (Raft Read Barrier)
 *   được nghiệm thu đều mang mức {@link VerificationLevel#UNVERIFIED}. UI luôn hiển thị nhãn "Chưa xác minh".</li>
 *   <li><b>Single-flight execution:</b> Nếu đang có một yêu cầu đọc in-flight, yêu cầu gọi tiếp theo
 *   sẽ tái sử dụng CompletableFuture hiện tại, tuyệt đối không gửi hai truy vấn đọc chồng chéo lên nhau.</li>
 *   <li><b>Monotonic Generation (Chống Stale/Out-of-Order):</b> Mỗi lần bắt đầu đọc sẽ tăng generation.
 *   Nếu phản hồi mạng đến muộn có generation nhỏ hơn generation mới nhất đã áp dụng, snapshot đó sẽ bị BỎ QUA (drop).</li>
 *   <li><b>Xử lý xung đột lựa chọn (Selection Conflict):</b> Ghế người dùng đang chọn (SELECTED)
 *   nếu server thông báo đã BOOKED sẽ bị hủy chọn và cảnh báo; nếu server báo UNKNOWN sẽ giữ nguyên kèm cảnh báo.</li>
 *   <li><b>Bảo vệ Pending Request:</b> Ghế đang có lệnh BOOK_TICKET chưa ngã ngũ trong Journal sẽ không bao giờ
 *   bị hiển thị thành AVAILABLE sạch.</li>
 *   <li><b>An toàn UI Thread:</b> Mọi cập nhật lên JavaFX ObservableList và Properties đều thông qua
 *   {@link Platform#runLater(Runnable)} hoặc chạy trực tiếp nếu Toolkit chưa khởi tạo (môi trường kiểm thử).</li>
 * </ul>
 */
public class SeatsRefreshService implements AutoCloseable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ReadAdapter readAdapter;
    private final ObservableList<SeatViewModel> seatsList;
    private final PersistentPendingJournal journal;
    private final String tripId;

    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private ScheduledFuture<?> periodicTask;

    // Quản lý Single-Flight
    private final Object flightLock = new Object();
    private volatile CompletableFuture<SeatsSnapshot> currentFlightFuture = null;

    // Quản lý Monotonic Generation chống stale
    private final AtomicLong generationSequence = new AtomicLong(0);
    private final AtomicLong latestAppliedGeneration = new AtomicLong(0);
    private final AtomicInteger droppedStaleCount = new AtomicInteger(0);

    // JavaFX Observable Properties cho UI data-binding
    private final ObjectProperty<VerificationLevel> verificationProperty = new SimpleObjectProperty<>(VerificationLevel.UNVERIFIED);
    private final ObjectProperty<DataSource> dataSourceProperty = new SimpleObjectProperty<>(DataSource.NONE);
    private final ObjectProperty<SeatsSnapshot> latestSnapshotProperty = new SimpleObjectProperty<>(null);
    private final BooleanProperty refreshingProperty = new SimpleBooleanProperty(false);
    private final StringProperty statusMessageProperty = new SimpleStringProperty("Khởi tạo dịch vụ làm mới ghế");
    private final StringProperty lastRefreshTimeProperty = new SimpleStringProperty("");
    private final LongProperty latestGenerationProperty = new SimpleLongProperty(0);
    private final BooleanProperty hasConflictProperty = new SimpleBooleanProperty(false);
    private final StringProperty conflictWarningProperty = new SimpleStringProperty("");

    public SeatsRefreshService(ReadAdapter readAdapter, ObservableList<SeatViewModel> seatsList) {
        this(readAdapter, seatsList, null, SeatsSnapshot.DEFAULT_TRIP_ID, null);
    }

    public SeatsRefreshService(ReadAdapter readAdapter, ObservableList<SeatViewModel> seatsList, PersistentPendingJournal journal) {
        this(readAdapter, seatsList, journal, SeatsSnapshot.DEFAULT_TRIP_ID, null);
    }

    public SeatsRefreshService(
            ReadAdapter readAdapter,
            ObservableList<SeatViewModel> seatsList,
            PersistentPendingJournal journal,
            String tripId,
            ScheduledExecutorService customScheduler) {
        this.readAdapter = Objects.requireNonNull(readAdapter, "readAdapter không được null");
        this.seatsList = Objects.requireNonNull(seatsList, "seatsList không được null");
        this.journal = journal;
        this.tripId = (tripId != null && !tripId.isBlank()) ? tripId : SeatsSnapshot.DEFAULT_TRIP_ID;

        if (customScheduler != null) {
            this.scheduler = customScheduler;
            this.ownsScheduler = false;
        } else {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "seats-refresh-service-scheduler");
                t.setDaemon(true);
                return t;
            });
            this.ownsScheduler = true;
        }

        this.dataSourceProperty.set(readAdapter.getDataSource());
        this.verificationProperty.set(VerificationLevel.UNVERIFIED);
    }

    /**
     * Kích hoạt làm mới sơ đồ ghế ngay lập tức.
     * Áp dụng Single-Flight: nếu đang có truy vấn in-flight thì trả về future đó.
     */
    public CompletableFuture<SeatsSnapshot> refreshNow() {
        synchronized (flightLock) {
            if (currentFlightFuture != null && !currentFlightFuture.isDone()) {
                return currentFlightFuture;
            }

            final long thisGeneration = generationSequence.incrementAndGet();
            runOnFxThread(() -> {
                refreshingProperty.set(true);
                statusMessageProperty.set("Đang làm mới sơ đồ ghế (generation #" + thisGeneration + ")...");
            });

            CompletableFuture<SeatsSnapshot> flightFuture = new CompletableFuture<>();
            currentFlightFuture = flightFuture;

            readAdapter.readSeats(tripId).whenComplete((snapshot, throwable) -> {
                try {
                    if (throwable != null) {
                        handleRefreshFailure(thisGeneration, throwable);
                        flightFuture.completeExceptionally(throwable);
                    } else if (snapshot != null) {
                        handleRefreshSuccess(thisGeneration, snapshot);
                        flightFuture.complete(snapshot);
                    } else {
                        SeatsSnapshot emptySnapshot = SeatsSnapshot.readUnavailable(
                                tripId, readAdapter.getDataSource(), null, "Snapshot rỗng từ adapter"
                        );
                        handleRefreshSuccess(thisGeneration, emptySnapshot);
                        flightFuture.complete(emptySnapshot);
                    }
                } finally {
                    synchronized (flightLock) {
                        if (currentFlightFuture == flightFuture) {
                            currentFlightFuture = null;
                        }
                    }
                    runOnFxThread(() -> refreshingProperty.set(false));
                }
            });

            return flightFuture;
        }
    }

    private void handleRefreshSuccess(long generation, SeatsSnapshot snapshot) {
        long lastGen = latestAppliedGeneration.get();
        if (generation < lastGen) {
            // OUT-OF-ORDER DETECTED: Snapshot này đến sau nhưng thuộc thế hệ cũ hơn -> DROP!
            droppedStaleCount.incrementAndGet();
            runOnFxThread(() -> statusMessageProperty.set(
                    "Bỏ qua dữ liệu cũ (Gen #" + generation + " < hiện tại #" + lastGen + ")"
            ));
            return;
        }

        latestAppliedGeneration.set(generation);
        applySnapshotSafe(snapshot, generation);
    }

    private void handleRefreshFailure(long generation, Throwable throwable) {
        long lastGen = latestAppliedGeneration.get();
        if (generation < lastGen) {
            droppedStaleCount.incrementAndGet();
            return;
        }

        latestAppliedGeneration.set(generation);

        Throwable root = throwable;
        while (root.getCause() != null && root != root.getCause()) {
            root = root.getCause();
        }

        String msg = root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
        final String errorDesc = msg;

        runOnFxThread(() -> {
            latestGenerationProperty.set(generation);
            verificationProperty.set(VerificationLevel.UNVERIFIED);
            statusMessageProperty.set("⚡ Lỗi khi làm mới ghế (Gen #" + generation + "): " + errorDesc);

            // Khi lỗi mạng/mất kết nối, chuyển toàn bộ ghế sang UNKNOWN để tránh bán nhầm
            for (SeatViewModel seat : seatsList) {
                if (seat.getStatus() != SeatStatus.SELECTED) {
                    seat.setStatus(SeatStatus.UNKNOWN);
                }
                seat.setVerification(VerificationLevel.UNVERIFIED);
            }
        });
    }

    /**
     * Áp dụng dữ liệu snapshot an toàn vào danh sách ghế và các thuộc tính quan sát được.
     */
    private void applySnapshotSafe(SeatsSnapshot snapshot, long generation) {
        final Set<String> pendingSeatIds = extractPendingSeatIds();

        runOnFxThread(() -> {
            latestGenerationProperty.set(generation);
            latestSnapshotProperty.set(snapshot);
            verificationProperty.set(snapshot.getVerification());
            dataSourceProperty.set(snapshot.getSource());
            lastRefreshTimeProperty.set(snapshot.getAsOf().toString());

            boolean conflict = false;
            String conflictMsg = "";

            for (SeatViewModel seat : seatsList) {
                String seatId = seat.getSeatId();
                SeatStatus serverStatus = snapshot.getSeatStatus(seatId);

                // Cập nhật mức xác minh trên từng ghế
                seat.setVerification(snapshot.getVerification());

                if (seat.getStatus() == SeatStatus.SELECTED) {
                    // Xử lý xung đột lựa chọn của người dùng
                    if (serverStatus == SeatStatus.BOOKED) {
                        // Ghế vừa có người khác đặt thành công -> HỦY CHỌN, chuyển BOOKED
                        seat.setStatus(SeatStatus.BOOKED);
                        conflict = true;
                        conflictMsg = "Ghế " + seatId + " vừa có người khác đặt!";
                    } else if (serverStatus == SeatStatus.UNKNOWN || snapshot.isUnavailable()) {
                        // Không thể chứng minh trạng thái ghế (ví dụ READ_UNAVAILABLE)
                        // Giữ nguyên SELECTED trên UI nhưng bật cảnh báo
                        conflict = true;
                        conflictMsg = "Không thể xác minh trạng thái ghế " + seatId + " (READ_UNAVAILABLE)";
                    } else {
                        // Server báo AVAILABLE: Người dùng tiếp tục giữ trạng thái SELECTED
                    }
                } else {
                    // Ghế không được người dùng click chọn
                    if (pendingSeatIds.contains(seatId)) {
                        // Ghế đang có yêu cầu đặt chưa ngã ngũ trong Journal -> Bảo vệ, không hiển thị AVAILABLE
                        if (serverStatus == SeatStatus.BOOKED) {
                            seat.setStatus(SeatStatus.BOOKED);
                        } else {
                            seat.setStatus(SeatStatus.UNKNOWN);
                        }
                    } else {
                        seat.setStatus(serverStatus);
                    }
                }
            }

            hasConflictProperty.set(conflict);
            conflictWarningProperty.set(conflictMsg);

            if (snapshot.isUnavailable()) {
                statusMessageProperty.set(
                        "⚠ READ_UNAVAILABLE: Không chứng minh được trạng thái hiện hành. Sơ đồ ghế chuyển Chưa rõ (UNKNOWN)."
                );
            } else if (!snapshot.isSuccess()) {
                statusMessageProperty.set(
                        "⚡ Lỗi đọc ghế: " + snapshot.getErrorCode() + " - " + snapshot.getErrorMessage()
                );
            } else {
                statusMessageProperty.set(
                        "✓ Cập nhật sơ đồ 10 ghế thành công [" + snapshot.getVerification().getDisplayName() +
                                " | " + snapshot.getSource().getLabel() + "]"
                );
            }
        });
    }

    /**
     * Trích xuất danh sách ghế đang có lệnh BOOK_TICKET pending trong journal.
     */
    private Set<String> extractPendingSeatIds() {
        if (journal == null) {
            return Collections.emptySet();
        }
        try {
            List<PendingEntry> pendingEntries = journal.loadPending();
            Set<String> set = new HashSet<>();
            for (PendingEntry entry : pendingEntries) {
                if ("BOOK_TICKET".equals(entry.getCommandType()) &&
                        (entry.getState() == PendingState.PREPARED || entry.getState() == PendingState.SENT_UNRESOLVED)) {
                    String payload = entry.getPayload();
                    if (payload != null && !payload.isBlank()) {
                        try {
                            JsonNode node = OBJECT_MAPPER.readTree(payload);
                            if (node.hasNonNull("seatId")) {
                                set.add(node.get("seatId").asText());
                            }
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
            return set;
        } catch (Exception e) {
            return Collections.emptySet();
        }
    }

    /**
     * Bật tính năng định kỳ làm mới sơ đồ ghế.
     *
     * @param intervalMs chu kỳ làm mới (mili-giây), tối thiểu 500ms
     */
    public synchronized void startPeriodicRefresh(long intervalMs) {
        stopPeriodicRefresh();
        long interval = Math.max(500, intervalMs);
        periodicTask = scheduler.scheduleWithFixedDelay(
                this::refreshNow,
                interval,
                interval,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Tắt làm mới định kỳ.
     */
    public synchronized void stopPeriodicRefresh() {
        if (periodicTask != null && !periodicTask.isCancelled()) {
            periodicTask.cancel(true);
            periodicTask = null;
        }
    }

    /**
     * Thực thi an toàn trên JavaFX UI thread (hỗ trợ môi trường không có UI Toolkit).
     */
    private void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
        } else {
            try {
                Platform.runLater(action);
            } catch (IllegalStateException e) {
                // JavaFX Toolkit chưa khởi tạo (ví dụ chạy headless Unit Test) -> chạy trực tiếp
                action.run();
            }
        }
    }

    public ReadAdapter getReadAdapter() {
        return readAdapter;
    }

    public ObservableList<SeatViewModel> getSeatsList() {
        return seatsList;
    }

    public PersistentPendingJournal getJournal() {
        return journal;
    }

    public String getTripId() {
        return tripId;
    }

    public long getLatestAppliedGeneration() {
        return latestAppliedGeneration.get();
    }

    public int getDroppedStaleCount() {
        return droppedStaleCount.get();
    }

    public ObjectProperty<VerificationLevel> verificationProperty() {
        return verificationProperty;
    }

    public VerificationLevel getVerification() {
        return verificationProperty.get();
    }

    public ObjectProperty<DataSource> dataSourceProperty() {
        return dataSourceProperty;
    }

    public DataSource getDataSource() {
        return dataSourceProperty.get();
    }

    public ObjectProperty<SeatsSnapshot> latestSnapshotProperty() {
        return latestSnapshotProperty;
    }

    public SeatsSnapshot getLatestSnapshot() {
        return latestSnapshotProperty.get();
    }

    public BooleanProperty refreshingProperty() {
        return refreshingProperty;
    }

    public boolean isRefreshing() {
        return refreshingProperty.get();
    }

    public StringProperty statusMessageProperty() {
        return statusMessageProperty;
    }

    public String getStatusMessage() {
        return statusMessageProperty.get();
    }

    public StringProperty lastRefreshTimeProperty() {
        return lastRefreshTimeProperty;
    }

    public String getLastRefreshTime() {
        return lastRefreshTimeProperty.get();
    }

    public LongProperty latestGenerationProperty() {
        return latestGenerationProperty;
    }

    public BooleanProperty hasConflictProperty() {
        return hasConflictProperty;
    }

    public boolean hasConflict() {
        return hasConflictProperty.get();
    }

    public StringProperty conflictWarningProperty() {
        return conflictWarningProperty;
    }

    public String getConflictWarning() {
        return conflictWarningProperty.get();
    }

    @Override
    public void close() {
        stopPeriodicRefresh();
        if (ownsScheduler) {
            scheduler.shutdownNow();
        }
        try {
            readAdapter.close();
        } catch (Exception ignored) {
        }
    }
}
