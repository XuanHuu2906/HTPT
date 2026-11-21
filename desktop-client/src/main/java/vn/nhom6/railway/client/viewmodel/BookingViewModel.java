package vn.nhom6.railway.client.viewmodel;

import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyIntegerProperty;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyStringProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.ObservableList;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.SeatsRefreshService;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ViewModel quản lý máy trạng thái giao dịch đặt vé (TASK TV1-D07 / TV1-D17).
 * <p>
 * Máy trạng thái theo đặc tả SPEC-03 / SPEC-05 / TV1-D17:
 * <pre>
 *   IDLE ➔ SUBMITTING ➔ (CONFIRMED | REJECTED | ERROR | UNKNOWN)
 * </pre>
 * <p>
 * Tính năng chính:
 * <ul>
 *   <li>Chống double-click ở tầng ViewModel bằng cờ nguyên tử {@link AtomicBoolean#compareAndSet(boolean, boolean)}.</li>
 *   <li>Giữ trạng thái {@code SUBMITTING} cho đến khi nhận phản hồi cuối cùng (Raft commit + DB apply).</li>
 *   <li>Timeout UI (cấu hình {@code booking.submitTimeoutMs}, mặc định 5000ms) chuyển sang {@code UNKNOWN} và lưu pending journal.</li>
 *   <li>Refresh ghế tự động qua {@link SeatsRefreshService#refreshNow()} khi nhận CONFIRMED hoặc REJECTED(SEAT_ALREADY_BOOKED).</li>
 *   <li>Xử lý phản hồi đến muộn (late-arriving response) khớp requestId mà không đè giao dịch mới.</li>
 *   <li>Mọi cập nhật thuộc tính giao diện chạy qua {@link UiDispatcher} tiêm được (không chặn JavaFX Application Thread).</li>
 *   <li>Tuyệt đối KHÔNG phụ thuộc socket mạng hay JDBC trong ViewModel.</li>
 * </ul>
 */
public class BookingViewModel {

    private static final Logger LOGGER = Logger.getLogger(BookingViewModel.class.getName());
    public static final int DEFAULT_SUBMIT_TIMEOUT_MS = 5000;

    /**
     * Functional interface điều phối luồng cập nhật UI (hỗ trợ kiểm thử đồng bộ).
     */
    @FunctionalInterface
    public interface UiDispatcher {
        void dispatch(Runnable action);
    }

    /**
     * Các trạng thái giao diện UI theo đặc tả SPEC-06 và nhiệm vụ TV1-D17.
     */
    public enum UiState {
        IDLE("Sẵn sàng", "status-idle"),
        LOADING("Đang xử lý...", "status-loading"),
        SUBMITTING("Đang chờ Node xác nhận...", "booking-status-submitting"),
        CONFIRMED("Xác nhận thành công", "booking-status-confirmed"),
        SUCCESS("Thao tác thành công", "status-idle"), // Giữ tương thích D07/D08
        REJECTED("Bị từ chối nghiệp vụ", "booking-status-rejected"),
        ERROR("Lỗi hạ tầng / Cụm máy chủ", "status-error"),
        UNKNOWN("Chưa xác định kết quả (TIMEOUT)", "booking-status-unknown"),
        DISCONNECTED("Mất kết nối mạng", "status-disconnected");

        private final String defaultTitle;
        private final String styleClass;

        UiState(String defaultTitle, String styleClass) {
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

    private final BookingClient bookingClient;

    // === Cờ nguyên tử và kiểm soát đa luồng ===
    private final AtomicBoolean isSubmittingAtomic = new AtomicBoolean(false);
    private volatile BookTicketRequestDto activeRequest = null;
    private volatile long currentGeneration = 0;

    // === Cấu hình và dịch vụ tiêm phụ thuộc ===
    private volatile int submitTimeoutMs;
    private volatile UiDispatcher uiDispatcher;
    private ScheduledExecutorService scheduler;
    private boolean ownScheduler = false;
    private ScheduledFuture<?> timeoutTask = null;
    private ScheduledFuture<?> tickerTask = null;

    private PersistentPendingJournal journal;
    private SeatsRefreshService seatsRefreshService;
    private ObservableList<SeatViewModel> seatsList;

    // === JavaFX Properties ===
    private final ObjectProperty<UiState> uiState = new SimpleObjectProperty<>(UiState.IDLE);
    private final StringProperty message = new SimpleStringProperty("Hệ thống sẵn sàng ở chế độ MOCK (D08).");
    private final BooleanProperty busy = new SimpleBooleanProperty(false);
    private final BooleanProperty mock = new SimpleBooleanProperty(true);
    private final StringProperty errorCode = new SimpleStringProperty("");
    private final StringProperty leaderHint = new SimpleStringProperty("");
    private final StringProperty currentRequestId = new SimpleStringProperty("");
    private final IntegerProperty waitingDurationSeconds = new SimpleIntegerProperty(0);
    private final StringProperty lateArrivalNote = new SimpleStringProperty("");
    private final IntegerProperty checkCount = new SimpleIntegerProperty(0);
    private final StringProperty sentTimestamp = new SimpleStringProperty("");

    public BookingViewModel(BookingClient bookingClient) {
        this(bookingClient, null, null, null, resolveDefaultTimeoutMs());
    }

    public BookingViewModel(BookingClient bookingClient,
                            PersistentPendingJournal journal,
                            SeatsRefreshService seatsRefreshService,
                            ScheduledExecutorService scheduler,
                            int submitTimeoutMs) {
        this.bookingClient = Objects.requireNonNull(bookingClient, "BookingClient không được null");
        this.journal = journal;
        this.seatsRefreshService = seatsRefreshService;
        this.scheduler = scheduler;
        this.submitTimeoutMs = (submitTimeoutMs > 0) ? submitTimeoutMs : DEFAULT_SUBMIT_TIMEOUT_MS;

        // Mặc định UiDispatcher chuyển về JavaFX Application Thread
        this.uiDispatcher = action -> {
            if (Platform.isFxApplicationThread()) {
                action.run();
            } else {
                try {
                    Platform.runLater(action);
                } catch (IllegalStateException e) {
                    action.run(); // Môi trường kiểm thử non-JavaFX
                }
            }
        };
    }

    private static int resolveDefaultTimeoutMs() {
        try {
            String prop = System.getProperty("booking.submitTimeoutMs");
            if (prop != null && !prop.isBlank()) {
                return Integer.parseInt(prop.trim());
            }
        } catch (Exception ignored) {
        }
        return DEFAULT_SUBMIT_TIMEOUT_MS;
    }

    private synchronized ScheduledExecutorService getOrCreateScheduler() {
        if (scheduler == null || scheduler.isShutdown()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "booking-vm-timer");
                t.setDaemon(true);
                return t;
            });
            ownScheduler = true;
        }
        return scheduler;
    }

    public void setSyncMode(boolean syncMode) {
        if (syncMode) {
            this.uiDispatcher = Runnable::run;
        }
    }

    public void setUiDispatcher(UiDispatcher uiDispatcher) {
        this.uiDispatcher = Objects.requireNonNull(uiDispatcher, "UiDispatcher không được null");
    }

    public UiDispatcher getUiDispatcher() {
        return uiDispatcher;
    }

    public void setScheduler(ScheduledExecutorService scheduler) {
        synchronized (this) {
            if (ownScheduler && this.scheduler != null && !this.scheduler.isShutdown()) {
                this.scheduler.shutdownNow();
            }
            this.scheduler = scheduler;
            this.ownScheduler = false;
        }
    }

    public void setSubmitTimeoutMs(int submitTimeoutMs) {
        this.submitTimeoutMs = (submitTimeoutMs > 0) ? submitTimeoutMs : DEFAULT_SUBMIT_TIMEOUT_MS;
    }

    public int getSubmitTimeoutMs() {
        return submitTimeoutMs;
    }

    private volatile java.util.function.Predicate<String> requestAllowedPredicate;

    public void setRequestAllowedPredicate(java.util.function.Predicate<String> predicate) {
        this.requestAllowedPredicate = predicate;
    }

    public java.util.function.Predicate<String> getRequestAllowedPredicate() {
        return requestAllowedPredicate;
    }

    public void setJournal(PersistentPendingJournal journal) {
        this.journal = journal;
    }

    public PersistentPendingJournal getJournal() {
        return (journal != null) ? journal : ClientFactory.getJournal();
    }

    public void setSeatsRefreshService(SeatsRefreshService seatsRefreshService) {
        this.seatsRefreshService = seatsRefreshService;
    }

    public SeatsRefreshService getSeatsRefreshService() {
        return (seatsRefreshService != null) ? seatsRefreshService : ClientFactory.getSeatsRefreshService();
    }

    public void setSeatsList(ObservableList<SeatViewModel> seatsList) {
        this.seatsList = seatsList;
    }

    public ObservableList<SeatViewModel> getSeatsList() {
        return seatsList;
    }

    private void runOnUi(Runnable action) {
        if (uiDispatcher != null) {
            uiDispatcher.dispatch(action);
        } else {
            action.run();
        }
    }

    // =========================================================================
    // MÁY TRẠNG THÁI GIAO DỊCH ĐẶT VÉ (TASK TV1-D17)
    // =========================================================================

    /**
     * Gửi yêu cầu đặt vé bất đồng bộ theo hợp đồng SPEC-03 / SPEC-05.
     * <p>
     * Chống double-click: sử dụng AtomicBoolean compare-and-set. Nếu đang SUBMITTING,
     * các lệnh gọi tiếp theo bị bỏ qua hoàn toàn, không tạo requestId mới và không gọi client lần nữa.
     */
    public CompletableFuture<BookTicketResponseDto> submit(BookTicketRequestDto request) {
        Objects.requireNonNull(request, "BookTicketRequestDto không được null");

        // Kiểm tra ReconnectController / Failover Filter (TASK TV1-D21)
        if (requestAllowedPredicate != null && !requestAllowedPredicate.test(request.getRequestId())) {
            CompletableFuture<BookTicketResponseDto> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(new IllegalStateException("Không thể tạo yêu cầu đặt vé mới: Cụm đang tìm Leader / đang trong quá trình bầu cử!"));
            return rejected;
        }

        // Kiểm tra nguyên tử chống double-click
        if (!isSubmittingAtomic.compareAndSet(false, true)) {
            // Đang SUBMITTING: từ chối các lệnh gọi tiếp theo, không tạo requestId mới, không gọi client
            CompletableFuture<BookTicketResponseDto> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(new IllegalStateException("Hệ thống đang chờ Node xác nhận yêu cầu trước đó!"));
            return rejected;
        }

        final long gen = ++currentGeneration;
        final String reqId = (request.getRequestId() != null && !request.getRequestId().isBlank())
                ? request.getRequestId()
                : UUID.randomUUID().toString();

        final BookTicketRequestDto normalizedRequest = new BookTicketRequestDto(
                reqId,
                request.getTripId() != null ? request.getTripId() : "SE-001",
                request.getSeatId(),
                request.getPassengerName()
        );
        this.activeRequest = normalizedRequest;

        // Chuyển sang SUBMITTING và cập nhật giao diện
        runOnUi(() -> {
            busy.set(true);
            uiState.set(UiState.SUBMITTING);
            currentRequestId.set(reqId);
            waitingDurationSeconds.set(0);
            lateArrivalNote.set("");
            errorCode.set("");
            leaderHint.set("");
            checkCount.set(0);
            sentTimestamp.set(Instant.now().toString());
            message.set("Đang chờ Node xác nhận ghế " + normalizedRequest.getSeatId() + "... (requestId: " + reqId + ")");
        });

        // Thiết lập bộ đếm thời gian chờ và hẹn giờ timeout UI
        cancelTimers();
        startTimers(reqId, gen);

        CompletableFuture<BookTicketResponseDto> completionFuture = new CompletableFuture<>();

        // Gọi BookingClient bất đồng bộ (D14 JournalingBookingClient đã ghi journal trước khi gửi)
        bookingClient.book(normalizedRequest).whenComplete((result, throwable) -> {
            cancelTimers();
            runOnUi(() -> {
                // Kiểm tra generation để phản hồi cũ không đè trạng thái của lần đặt mới
                if (gen != currentGeneration) {
                    handleLateOrStaleResponse(result, throwable, reqId);
                    if (throwable != null) {
                        completionFuture.completeExceptionally(throwable);
                    } else {
                        completionFuture.complete(result);
                    }
                    return;
                }

                // Nếu giao diện đã chuyển sang UNKNOWN do timeout trước đó
                if (uiState.get() == UiState.UNKNOWN) {
                    handleLateOrStaleResponse(result, throwable, reqId);
                    if (throwable != null) {
                        completionFuture.completeExceptionally(throwable);
                    } else {
                        completionFuture.complete(result);
                    }
                    return;
                }

                isSubmittingAtomic.set(false);
                busy.set(false);

                if (throwable != null) {
                    try {
                        handleException(throwable, reqId);
                    } finally {
                        completionFuture.completeExceptionally(throwable);
                    }
                    return;
                }

                try {
                    handleBookingResult(result, reqId, false);
                } finally {
                    completionFuture.complete(result);
                }
            });
        });

        return completionFuture;
    }

    /**
     * Alias giữ tương thích cho {@link #submit(BookTicketRequestDto)}.
     */
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        return submit(request);
    }

    /**
     * Gửi yêu cầu đặt vé cho 1 ghế với họ tên hành khách.
     */
    public CompletableFuture<BookTicketResponseDto> book(String seatId, String passengerName) {
        return book("SE-001", seatId, passengerName);
    }

    /**
     * Gửi yêu cầu đặt vé với đầy đủ tripId, seatId và passengerName.
     */
    public CompletableFuture<BookTicketResponseDto> book(String tripId, String seatId, String passengerName) {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, tripId != null ? tripId : "SE-001", seatId, passengerName);
        return submit(request);
    }

    // =========================================================================
    // XỬ LÝ PHẢN HỒI VÀ TIMEOUT
    // =========================================================================

    private void handleBookingResult(BookTicketResponseDto result, String reqId, boolean isLateArrival) {
        mock.set(result.isMock());
        ErrorPresentation presentation = ErrorPresentationMapper.mapBookingResponse(result);
        message.set(presentation.message());
        errorCode.set(presentation.rawCode() != null ? presentation.rawCode() : "");

        switch (presentation.kind()) {
            case CONFIRMED -> {
                uiState.set(UiState.CONFIRMED);
                leaderHint.set(result.getLeaderHint() != null ? result.getLeaderHint() : "");
                updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.BOOKED);
                triggerSeatsRefresh();
                markJournalResolved(reqId, "CONFIRMED");
            }
            case REJECTED -> {
                uiState.set(UiState.REJECTED);
                if (ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED.equals(presentation.rawCode())) {
                    updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.BOOKED);
                    triggerSeatsRefresh();
                }
                markJournalResolved(reqId, "REJECTED");
            }
            case UNKNOWN -> {
                uiState.set(UiState.UNKNOWN);
                updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.UNKNOWN);
                ensureJournalPending(reqId);
            }
            case ERROR -> {
                uiState.set(UiState.ERROR);
                if (ErrorPresentationMapper.CODE_NOT_LEADER.equals(presentation.rawCode())) {
                    String hint = (result.getLeaderHint() != null && !result.getLeaderHint().isBlank())
                            ? result.getLeaderHint().trim()
                            : "";
                    leaderHint.set(hint);
                    try {
                        ClusterConnectionViewModel clusterVm = ClientFactory.getClusterConnectionViewModel();
                        if (clusterVm != null && !hint.isEmpty()) {
                            clusterVm.applyLeaderHint(hint, result.isMock()
                                    ? vn.nhom6.railway.client.network.ClusterStatusClient.DataSource.MOCK
                                    : vn.nhom6.railway.client.network.ClusterStatusClient.DataSource.REAL);
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
            case DISCONNECTED -> {
                uiState.set(UiState.DISCONNECTED);
                if (presentation.clientErrorKind() != ClientErrorKind.FAILED_NOT_SENT
                        && !ErrorPresentationMapper.CODE_FAILED_NOT_SENT.equals(presentation.rawCode())) {
                    updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.UNKNOWN);
                }
                ensureJournalPending(reqId);
            }
            case CLIENT_ERROR -> {
                uiState.set(UiState.ERROR);
            }
        }
    }

    /**
     * Xử lý kết quả đến muộn (late-arriving response) sau khi UI đã timeout sang UNKNOWN
     * hoặc phản hồi của request cũ khi request mới đã bắt đầu.
     */
    private void handleLateOrStaleResponse(BookTicketResponseDto result, Throwable throwable, String reqId) {
        if (result != null) {
            String outcome = result.getOutcome();
            if ("CONFIRMED".equals(outcome) || "REJECTED".equals(outcome)) {
                markJournalResolved(reqId, outcome);
            }

            // Chỉ cập nhật UI nếu requestId này vẫn là requestId hiện tại của màn hình
            if (reqId.equals(currentRequestId.get())) {
                if ("CONFIRMED".equals(outcome)) {
                    uiState.set(UiState.CONFIRMED);
                    lateArrivalNote.set("Kết quả đến muộn: Yêu cầu đặt vé đã được xác nhận (CONFIRMED)!");
                    message.set("Kết quả đến muộn: Đặt vé thành công ghế " + result.getSeatId() +
                            " (Vé: " + result.getTicketId() + ")");
                    updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.BOOKED);
                    triggerSeatsRefresh();
                } else if ("REJECTED".equals(outcome)) {
                    uiState.set(UiState.REJECTED);
                    lateArrivalNote.set("Kết quả đến muộn: Yêu cầu bị từ chối (" + result.getReasonCode() + ")");
                    message.set("Kết quả đến muộn: " + result.getMessage());
                    if ("SEAT_ALREADY_BOOKED".equals(result.getReasonCode())) {
                        updateSeatStatus(result.getSeatId(), SeatViewModel.SeatStatus.BOOKED);
                        triggerSeatsRefresh();
                    }
                }
            }
        }
    }

    private void handleException(Throwable throwable, String reqId) {
        ErrorPresentation presentation = ErrorPresentationMapper.mapThrowable(throwable, mock.get());
        message.set(presentation.message() + (reqId != null ? " (requestId: " + reqId + ")" : ""));
        errorCode.set(presentation.rawCode() != null ? presentation.rawCode() : "");

        if (presentation.clientErrorKind() == ClientErrorKind.FAILED_NOT_SENT
                || presentation.clientErrorKind() == ClientErrorKind.CONNECTION_FAILED
                || ErrorPresentationMapper.CODE_FAILED_NOT_SENT.equals(presentation.rawCode())) {
            uiState.set(UiState.DISCONNECTED);
            // Theo SPEC-03 / TV1-D19: Lỗi kết nối trước khi gửi (FAILED_NOT_SENT) -> GHẾ KHÔNG ĐỔI!
            // Tuyệt đối không đổi trạng thái ghế sang UNKNOWN
            ensureJournalPending(reqId);
        } else if (presentation.kind() == PresentationKind.DISCONNECTED) {
            uiState.set(UiState.DISCONNECTED);
            String targetSeat = (activeRequest != null) ? activeRequest.getSeatId() : null;
            updateSeatStatus(targetSeat, SeatViewModel.SeatStatus.UNKNOWN);
            ensureJournalPending(reqId);
        } else if (presentation.kind() == PresentationKind.UNKNOWN) {
            uiState.set(UiState.UNKNOWN);
            String targetSeat = (activeRequest != null) ? activeRequest.getSeatId() : null;
            updateSeatStatus(targetSeat, SeatViewModel.SeatStatus.UNKNOWN);
            ensureJournalPending(reqId);
        } else {
            uiState.set(UiState.ERROR);
        }
    }

    private void startTimers(String reqId, long gen) {
        ScheduledExecutorService ses = getOrCreateScheduler();

        // Timer đếm giây đang chờ
        tickerTask = ses.scheduleAtFixedRate(() -> runOnUi(() -> {
            if (gen == currentGeneration && uiState.get() == UiState.SUBMITTING) {
                waitingDurationSeconds.set(waitingDurationSeconds.get() + 1);
            }
        }), 1, 1, TimeUnit.SECONDS);

        // Timer timeout UI
        timeoutTask = ses.schedule(() -> triggerTimeoutInternal(reqId, gen), submitTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void cancelTimers() {
        if (timeoutTask != null) {
            timeoutTask.cancel(true);
            timeoutTask = null;
        }
        if (tickerTask != null) {
            tickerTask.cancel(true);
            tickerTask = null;
        }
    }

    private void triggerTimeoutInternal(String reqId, long gen) {
        runOnUi(() -> {
            if (gen != currentGeneration) {
                return;
            }
            if (uiState.get() != UiState.SUBMITTING) {
                return;
            }

            // Giải phóng trạng thái bận và chuyển sang UNKNOWN
            isSubmittingAtomic.set(false);
            busy.set(false);
            uiState.set(UiState.UNKNOWN);
            errorCode.set(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN);

            ErrorPresentation p = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, null, mock.get());
            message.set(p.message() + " (Mã yêu cầu: " + reqId + ")");

            String targetSeat = (activeRequest != null) ? activeRequest.getSeatId() : null;
            updateSeatStatus(targetSeat, SeatViewModel.SeatStatus.UNKNOWN);
            ensureJournalPending(reqId);
        });
    }

    /**
     * Kích hoạt timeout thủ công (phục vụ Unit Test kiểm tra trạng thái UNKNOWN ảo).
     */
    public void triggerTimeoutForTesting() {
        triggerTimeoutInternal(currentRequestId.get(), currentGeneration);
    }

    // =========================================================================
    // HỖ TRỢ REFRESH GHẾ VÀ JOURNAL
    // =========================================================================

    private void updateSeatStatus(String seatId, SeatViewModel.SeatStatus status) {
        if (seatId == null) {
            return;
        }
        ObservableList<SeatViewModel> list = (seatsList != null) ? seatsList : null;
        if (list == null) {
            SeatsRefreshService srs = getSeatsRefreshService();
            if (srs != null) {
                list = srs.getSeatsList();
            }
        }
        if (list != null) {
            for (SeatViewModel vm : list) {
                if (seatId.equalsIgnoreCase(vm.getSeatId())) {
                    vm.setStatus(status);
                    break;
                }
            }
        }
    }

    private void triggerSeatsRefresh() {
        SeatsRefreshService srs = getSeatsRefreshService();
        if (srs != null) {
            srs.refreshNow();
        }
    }

    private void markJournalResolved(String reqId, String outcome) {
        PersistentPendingJournal j = getJournal();
        if (j != null && reqId != null && !reqId.isBlank()) {
            try {
                if (j.get(reqId).isPresent()) {
                    j.markResolved(reqId, outcome);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Lỗi cập nhật journal markResolved cho requestId=" + reqId, e);
            }
        }
        try {
            PendingRequestsViewModel pvm = ClientFactory.getPendingRequestsViewModel();
            if (pvm != null) {
                pvm.reloadFromJournal();
            }
        } catch (Exception ignored) {
        }
    }

    private void ensureJournalPending(String reqId) {
        PersistentPendingJournal j = getJournal();
        if (j != null && reqId != null && !reqId.isBlank()) {
            try {
                Optional<PendingEntry> opt = j.get(reqId);
                if (opt.isEmpty() && activeRequest != null) {
                    String payload = String.format("{\"requestId\":\"%s\",\"tripId\":\"%s\",\"seatId\":\"%s\",\"passengerName\":\"%s\"}",
                            activeRequest.getRequestId(), activeRequest.getTripId(), activeRequest.getSeatId(), activeRequest.getPassengerName());
                    PendingEntry entry = new PendingEntry(
                            reqId,
                            "BOOK_TICKET",
                            payload,
                            PendingEntry.computePayloadHash(payload),
                            PendingState.SENT_UNRESOLVED,
                            Instant.now(),
                            null,
                            1,
                            null,
                            null,
                            1
                    );
                    j.recordPrepared(entry);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Lỗi kiểm tra pending journal cho requestId=" + reqId, e);
            }
        }
        try {
            PendingRequestsViewModel pvm = ClientFactory.getPendingRequestsViewModel();
            if (pvm != null) {
                pvm.reloadFromJournal();
            }
        } catch (Exception ignored) {
        }
    }

    // =========================================================================
    // HÀNH ĐỘNG RETRY VÀ KIỂM TRA KẾT QUẢ TỪ UNKNOWN
    // =========================================================================

    /**
     * Gửi lại yêu cầu từ trạng thái UNKNOWN với CÙNG requestId và CÙNG payload.
     */
    public CompletableFuture<BookTicketResponseDto> retry(String reqId) {
        Objects.requireNonNull(reqId, "requestId không được null");

        if (bookingClient instanceof JournalingBookingClient jbc) {
            runOnUi(() -> {
                busy.set(true);
                uiState.set(UiState.SUBMITTING);
                message.set("Đang gửi lại yêu cầu " + reqId + " với cùng requestId...");
            });
            return jbc.retry(reqId).whenComplete((res, ex) -> runOnUi(() -> {
                busy.set(false);
                if (ex != null) {
                    handleException(ex, reqId);
                } else if (res != null) {
                    handleBookingResult(res, reqId, false);
                }
            }));
        }

        if (activeRequest != null && reqId.equals(activeRequest.getRequestId())) {
            return submit(activeRequest);
        }

        CompletableFuture<BookTicketResponseDto> failed = new CompletableFuture<>();
        failed.completeExceptionally(new NoSuchElementException("Không tìm thấy payload gốc để retry cho requestId=" + reqId));
        return failed;
    }

    /**
     * Tra cứu kết quả yêu cầu theo requestId (SPEC-03 §4.4).
     */
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        Objects.requireNonNull(requestId, "requestId không được null");
        return bookingClient.getRequestResult(requestId).whenComplete((res, ex) -> runOnUi(() -> {
            checkCount.set(checkCount.get() + 1);
            if (ex != null) {
                Throwable cause = (ex instanceof java.util.concurrent.CompletionException && ex.getCause() != null)
                        ? ex.getCause() : ex;
                message.set("Lỗi khi tra cứu kết quả cho requestId=" + requestId + ": " + cause.getMessage()
                        + " (Lần kiểm tra: " + checkCount.get() + ")");
                return;
            }
            if (res != null) {
                String state = res.getState();
                if ("APPLIED_SUCCESS".equals(state)) {
                    uiState.set(UiState.CONFIRMED);
                    message.set("Tra cứu thành công: Vé đã được xác nhận (Mã vé: " +
                            (res.getTicketId() != null ? res.getTicketId() : "N/A") + ")");
                    String seat = resolveSeatId(requestId);
                    updateSeatStatus(seat, SeatViewModel.SeatStatus.BOOKED);
                    triggerSeatsRefresh();
                    markJournalResolved(requestId, "CONFIRMED");
                } else if ("APPLIED_REJECTED".equals(state)) {
                    uiState.set(UiState.REJECTED);
                    message.set("Tra cứu thành công: Yêu cầu bị từ chối (" + res.getReasonCode() + ")");
                    if ("SEAT_ALREADY_BOOKED".equals(res.getReasonCode())) {
                        String seat = resolveSeatId(requestId);
                        updateSeatStatus(seat, SeatViewModel.SeatStatus.BOOKED);
                        triggerSeatsRefresh();
                    }
                    markJournalResolved(requestId, "REJECTED");
                } else {
                    // PENDING hoặc UNKNOWN từ Node (chưa ghi nhận hoặc đang xử lý)
                    // Theo SPEC-03: giữ nguyên UiState.UNKNOWN, cho phép gửi lại hoặc tra cứu tiếp
                    uiState.set(UiState.UNKNOWN);
                    message.set("Node chưa ghi nhận kết quả cuối cho requestId=" + requestId + " (trạng thái: " + state
                            + "). Bạn có thể tiếp tục tra cứu hoặc bấm 'Gửi lại'. (Lần kiểm tra: " + checkCount.get() + ")");
                }
            }
        }));
    }

    public CompletableFuture<GetRequestResultResponseDto> checkCurrentResult() {
        String reqId = currentRequestId.get();
        if (reqId == null || reqId.isBlank()) {
            CompletableFuture<GetRequestResultResponseDto> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Không có requestId hiện tại"));
            return f;
        }
        return getRequestResult(reqId);
    }

    public CompletableFuture<BookTicketResponseDto> retryCurrent() {
        String reqId = currentRequestId.get();
        if (reqId == null || reqId.isBlank()) {
            CompletableFuture<BookTicketResponseDto> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Không có requestId hiện tại để retry"));
            return f;
        }
        return retry(reqId);
    }

    private String resolveSeatId(String reqId) {
        if (activeRequest != null && activeRequest.getSeatId() != null) {
            return activeRequest.getSeatId();
        }
        PersistentPendingJournal j = getJournal();
        if (j != null && reqId != null) {
            try {
                Optional<PendingEntry> opt = j.get(reqId);
                if (opt.isPresent() && opt.get().getPayload() != null) {
                    JsonNode node = MessageCodec.getMapper().readTree(opt.get().getPayload());
                    if (node.has("seatId")) {
                        return node.get("seatId").asText();
                    }
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    // =========================================================================
    // CÁC THAO TÁC KHÁC (HỦY VÉ, TẢI GHẾ)
    // =========================================================================

    public CompletableFuture<CancelTicketResponseDto> cancel(String ticketId) {
        String reqId = UUID.randomUUID().toString();
        CancelTicketRequestDto request = new CancelTicketRequestDto(reqId, ticketId);
        return cancel(request);
    }

    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        if (busy.get()) {
            CompletableFuture<CancelTicketResponseDto> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(new IllegalStateException("Hệ thống đang bận!"));
            return rejected;
        }

        runOnUi(() -> {
            busy.set(true);
            uiState.set(UiState.LOADING);
            message.set("Đang gửi yêu cầu hủy vé " + request.getTicketId() + "... [MOCK]");
        });

        CompletableFuture<CancelTicketResponseDto> completionFuture = new CompletableFuture<>();

        bookingClient.cancel(request).whenComplete((result, throwable) -> runOnUi(() -> {
            busy.set(false);
            if (throwable != null) {
                handleException(throwable, request.getRequestId());
                completionFuture.completeExceptionally(throwable);
                return;
            }

            mock.set(result.isMock());
            ErrorPresentation presentation = ErrorPresentationMapper.mapCancelResponse(result);
            message.set(presentation.message());
            errorCode.set(presentation.rawCode() != null ? presentation.rawCode() : "");

            switch (presentation.kind()) {
                case CONFIRMED -> uiState.set(UiState.SUCCESS);
                case REJECTED -> uiState.set(UiState.REJECTED);
                case UNKNOWN -> uiState.set(UiState.UNKNOWN);
                case ERROR -> uiState.set(UiState.ERROR);
                case DISCONNECTED -> uiState.set(UiState.DISCONNECTED);
                case CLIENT_ERROR -> uiState.set(UiState.ERROR);
            }
            completionFuture.complete(result);
        }));

        return completionFuture;
    }

    public CompletableFuture<GetSeatsResponseDto> refreshSeats() {
        return refreshSeats("SE-001");
    }

    public CompletableFuture<GetSeatsResponseDto> refreshSeats(String tripId) {
        runOnUi(() -> {
            busy.set(true);
            uiState.set(UiState.LOADING);
            message.set("Đang tải dữ liệu sơ đồ ghế chuyến " + tripId + "... [MOCK]");
        });

        CompletableFuture<GetSeatsResponseDto> completionFuture = new CompletableFuture<>();

        bookingClient.getSeats(new GetSeatsRequestDto(tripId)).whenComplete((result, throwable) -> runOnUi(() -> {
            busy.set(false);
            if (throwable != null) {
                handleException(throwable, null);
                completionFuture.completeExceptionally(throwable);
                return;
            }
            mock.set(result.isMock());
            uiState.set(UiState.IDLE);
            message.set("Đã cập nhật danh sách " + result.getSeats().size() + " ghế (Leader: " +
                    (result.getLeaderId() != null ? result.getLeaderId() : "N/A") + ") [MOCK]");
            completionFuture.complete(result);
        }));

        return completionFuture;
    }

    // =========================================================================
    // JAVAFX PROPERTIES GETTERS
    // =========================================================================

    public UiState getUiState() {
        return uiState.get();
    }

    public ReadOnlyObjectProperty<UiState> uiStateProperty() {
        return uiState;
    }

    public String getMessage() {
        return message.get();
    }

    public ReadOnlyStringProperty messageProperty() {
        return message;
    }

    public boolean isBusy() {
        return busy.get();
    }

    public ReadOnlyBooleanProperty busyProperty() {
        return busy;
    }

    public boolean isMock() {
        return mock.get();
    }

    public ReadOnlyBooleanProperty mockProperty() {
        return mock;
    }

    public String getErrorCode() {
        return errorCode.get();
    }

    public ReadOnlyStringProperty errorCodeProperty() {
        return errorCode;
    }

    public String getLeaderHint() {
        return leaderHint.get();
    }

    public ReadOnlyStringProperty leaderHintProperty() {
        return leaderHint;
    }

    public String getCurrentRequestId() {
        return currentRequestId.get();
    }

    public ReadOnlyStringProperty currentRequestIdProperty() {
        return currentRequestId;
    }

    public int getWaitingDurationSeconds() {
        return waitingDurationSeconds.get();
    }

    public ReadOnlyIntegerProperty waitingDurationSecondsProperty() {
        return waitingDurationSeconds;
    }

    public String getLateArrivalNote() {
        return lateArrivalNote.get();
    }

    public ReadOnlyStringProperty lateArrivalNoteProperty() {
        return lateArrivalNote;
    }

    public int getCheckCount() {
        return checkCount.get();
    }

    public ReadOnlyIntegerProperty checkCountProperty() {
        return checkCount;
    }

    public String getSentTimestamp() {
        return sentTimestamp.get();
    }

    public ReadOnlyStringProperty sentTimestampProperty() {
        return sentTimestamp;
    }

    public BookingClient getBookingClient() {
        return bookingClient;
    }
}
