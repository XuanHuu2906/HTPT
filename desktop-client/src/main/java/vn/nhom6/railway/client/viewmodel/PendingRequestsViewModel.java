package vn.nhom6.railway.client.viewmodel;

import javafx.application.Platform;
import javafx.beans.property.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * ViewModel quản lý danh sách yêu cầu đang chờ xử lý và khôi phục sau restart (TASK TV1-D14).
 * <p>
 * Bất biến:
 * 1. Khởi động: nạp từ journal và hiển thị các mục PREPARED / SENT_UNRESOLVED là "CHƯA XÁC NHẬN" (UNKNOWN).
 * 2. Tuyệt đối không tự ý hiển thị thành công khi chưa nhận được phản hồi kết quả cuối.
 * 3. Chống bấm trùng khi đang busy.
 * 4. Không có import java.net hay java.sql (Clean Architecture).
 */
public class PendingRequestsViewModel {

    private final JournalingBookingClient journalingClient;
    private final ObservableList<PendingItem> pendingItems = FXCollections.observableArrayList();
    private final BooleanProperty busy = new SimpleBooleanProperty(false);
    private final StringProperty statusMessage = new SimpleStringProperty("Sẵn sàng theo dõi yêu cầu pending.");

    public PendingRequestsViewModel(JournalingBookingClient journalingClient) {
        this.journalingClient = journalingClient;
        reloadFromJournal();
    }

    private static void runOnFxThread(Runnable action) {
        try {
            if (Platform.isFxApplicationThread()) {
                action.run();
            } else {
                Platform.runLater(action);
            }
        } catch (IllegalStateException e) {
            // Hỗ trợ unit test không có JavaFX Application Toolkit
            action.run();
        }
    }

    /**
     * Nạp lại toàn bộ danh sách pending từ journal đĩa cục bộ.
     */
    public synchronized void reloadFromJournal() {
        if (journalingClient == null || journalingClient.getJournal() == null) {
            return;
        }

        List<PendingEntry> entries = journalingClient.getJournal().loadPending();
        runOnFxThread(() -> {
            pendingItems.clear();
            for (PendingEntry entry : entries) {
                pendingItems.add(new PendingItem(entry, determineSourceLabel()));
            }
            if (entries.isEmpty()) {
                statusMessage.set("Không có yêu cầu nào đang chờ xử lý.");
            } else {
                statusMessage.set(String.format("Đã khôi phục %d yêu cầu chưa xác nhận từ journal.", entries.size()));
            }
        });
    }

    private String determineSourceLabel() {
        if (journalingClient == null) return "MOCK";
        String delegateName = journalingClient.getDelegate().getClass().getSimpleName();
        if (delegateName.contains("Mock")) return "MOCK";
        if (delegateName.contains("Fake")) return "FAKE-TEST";
        return "REAL";
    }

    /**
     * Kiểm tra kết quả (GET_REQUEST_RESULT) cho một mục pending cụ thể.
     */
    public CompletableFuture<GetRequestResultResponseDto> checkResult(PendingItem item) {
        Objects.requireNonNull(item, "item không được null");
        if (item.isChecking() || busy.get()) {
            return CompletableFuture.completedFuture(null);
        }

        item.setChecking(true);
        busy.set(true);
        statusMessage.set("Đang kiểm tra kết quả yêu cầu " + item.getRequestId() + "...");

        return journalingClient.getRequestResult(item.getRequestId()).whenComplete((response, throwable) -> {
            runOnFxThread(() -> {
                item.setChecking(false);
                busy.set(false);

                if (throwable != null) {
                    statusMessage.set("Kiểm tra thất bại hoặc timeout: " + throwable.getMessage());
                    return;
                }

                if (response != null) {
                    String state = response.getState();
                    if ("APPLIED_SUCCESS".equals(state)) {
                        item.updateResolved("CONFIRMED");
                        statusMessage.set("Yêu cầu " + item.getRequestId() + " đã được xác nhận THÀNH CÔNG (CONFIRMED)!");
                    } else if ("APPLIED_REJECTED".equals(state)) {
                        item.updateResolved("REJECTED");
                        statusMessage.set("Yêu cầu " + item.getRequestId() + " đã bị TỪ CHỐI (REJECTED): " + response.getReasonCode());
                    } else {
                        statusMessage.set("Yêu cầu " + item.getRequestId() + " chưa có kết quả (" + state + "), giữ CHƯA XÁC NHẬN.");
                    }
                }
            });
        });
    }

    /**
     * Gửi lại (retry) yêu cầu với cùng requestId và cùng payload nguyên văn.
     */
    public CompletableFuture<BookTicketResponseDto> retry(PendingItem item) {
        Objects.requireNonNull(item, "item không được null");
        if (!item.isCanRetry() || busy.get()) {
            return CompletableFuture.completedFuture(null);
        }

        item.setChecking(true);
        busy.set(true);
        statusMessage.set("Đang gửi lại yêu cầu " + item.getRequestId() + "...");

        return journalingClient.retry(item.getRequestId()).whenComplete((response, throwable) -> {
            runOnFxThread(() -> {
                item.setChecking(false);
                busy.set(false);

                if (throwable != null) {
                    statusMessage.set("Gửi lại thất bại hoặc timeout: " + throwable.getMessage());
                    // Cập nhật số lần thử từ journal
                    journalingClient.getJournal().get(item.getRequestId()).ifPresent(item::syncFromEntry);
                    return;
                }

                if (response != null) {
                    String outcome = response.getOutcome();
                    if ("CONFIRMED".equals(outcome)) {
                        item.updateResolved("CONFIRMED");
                        statusMessage.set("Gửi lại thành công: Vé đã được XÁC NHẬN (CONFIRMED)!");
                    } else if ("REJECTED".equals(outcome)) {
                        item.updateResolved("REJECTED");
                        statusMessage.set("Gửi lại có kết quả: Yêu cầu bị TỪ CHỐI (REJECTED): " + response.getReasonCode());
                    } else {
                        statusMessage.set("Gửi lại chưa nhận được kết quả cuối (" + outcome + "), tiếp tục giữ CHƯA XÁC NHẬN.");
                        journalingClient.getJournal().get(item.getRequestId()).ifPresent(item::syncFromEntry);
                    }
                }
            });
        });
    }

    public ObservableList<PendingItem> getPendingItems() {
        return pendingItems;
    }

    public BooleanProperty busyProperty() {
        return busy;
    }

    public boolean isBusy() {
        return busy.get();
    }

    public StringProperty statusMessageProperty() {
        return statusMessage;
    }

    public String getStatusMessage() {
        return statusMessage.get();
    }

    // =========================================================================
    // MODEL DỮ LIỆU HIỂN THỊ TRÊN UI (PENDING ITEM)
    // =========================================================================

    public static class PendingItem {
        private final String requestId;
        private final String commandType;
        private final String summary;
        private final String sourceLabel;

        private final ObjectProperty<PendingState> state = new SimpleObjectProperty<>();
        private final StringProperty displayStatus = new SimpleStringProperty();
        private final StringProperty styleClass = new SimpleStringProperty();
        private final IntegerProperty attemptCount = new SimpleIntegerProperty();
        private final StringProperty lastAttempt = new SimpleStringProperty();
        private final BooleanProperty checking = new SimpleBooleanProperty(false);
        private final BooleanProperty canRetry = new SimpleBooleanProperty(false);

        public PendingItem(PendingEntry entry, String sourceLabel) {
            this.requestId = entry.getRequestId();
            this.commandType = entry.getCommandType();
            this.summary = entry.getSummary();
            this.sourceLabel = sourceLabel;
            syncFromEntry(entry);
        }

        public void syncFromEntry(PendingEntry entry) {
            this.state.set(entry.getState());
            this.attemptCount.set(entry.getAttemptCount());
            this.lastAttempt.set(entry.getLastAttemptAtString() != null ? entry.getLastAttemptAtString() : entry.getCreatedAtString());

            PendingState s = entry.getState();
            if (s == PendingState.PREPARED || s == PendingState.SENT_UNRESOLVED) {
                this.displayStatus.set("CHƯA XÁC NHẬN");
                this.styleClass.set("booking-status-unknown");
                this.canRetry.set(true);
            } else if (s == PendingState.RESOLVED) {
                String outcome = entry.getFinalOutcome();
                if ("CONFIRMED".equals(outcome)) {
                    this.displayStatus.set("ĐÃ XÁC NHẬN");
                    this.styleClass.set("booking-status-confirmed");
                } else {
                    this.displayStatus.set("BỊ TỪ CHỐI");
                    this.styleClass.set("booking-status-rejected");
                }
                this.canRetry.set(false);
            } else if (s == PendingState.CORRUPT) {
                this.displayStatus.set("DỮ LIỆU HƯ HỎNG");
                this.styleClass.set("status-error");
                this.canRetry.set(false); // Khóa nút gửi lại khi corrupt
            } else {
                this.displayStatus.set("CHƯA GỬI");
                this.styleClass.set("status-error");
                this.canRetry.set(true);
            }
        }

        public void updateResolved(String outcome) {
            this.state.set(PendingState.RESOLVED);
            if ("CONFIRMED".equals(outcome)) {
                this.displayStatus.set("ĐÃ XÁC NHẬN");
                this.styleClass.set("booking-status-confirmed");
            } else {
                this.displayStatus.set("BỊ TỪ CHỐI");
                this.styleClass.set("booking-status-rejected");
            }
            this.canRetry.set(false);
        }

        public String getRequestId() {
            return requestId;
        }

        public String getCommandType() {
            return commandType;
        }

        public String getSummary() {
            return summary;
        }

        public String getSourceLabel() {
            return sourceLabel;
        }

        public PendingState getState() {
            return state.get();
        }

        public ObjectProperty<PendingState> stateProperty() {
            return state;
        }

        public String getDisplayStatus() {
            return displayStatus.get();
        }

        public StringProperty displayStatusProperty() {
            return displayStatus;
        }

        public String getStyleClass() {
            return styleClass.get();
        }

        public StringProperty styleClassProperty() {
            return styleClass;
        }

        public int getAttemptCount() {
            return attemptCount.get();
        }

        public IntegerProperty attemptCountProperty() {
            return attemptCount;
        }

        public String getLastAttempt() {
            return lastAttempt.get();
        }

        public StringProperty lastAttemptProperty() {
            return lastAttempt;
        }

        public boolean isChecking() {
            return checking.get();
        }

        public BooleanProperty checkingProperty() {
            return checking;
        }

        public void setChecking(boolean checking) {
            this.checking.set(checking);
        }

        public boolean isCanRetry() {
            return canRetry.get();
        }

        public BooleanProperty canRetryProperty() {
            return canRetry;
        }
    }
}
