package vn.nhom6.railway.client.viewmodel;

import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * ViewModel quản lý logic tra cứu kết quả yêu cầu (GET_REQUEST_RESULT)
 * và danh sách vé đã xác nhận theo đặc tả SPEC-03 §4.4 và SPEC-06 (TASK TV1-D24).
 *
 * <p>Nguyên tắc thiết kế:
 * <ul>
 *   <li>Tách biệt hoàn toàn khỏi Controller và FXML.</li>
 *   <li>Chạy bất đồng bộ, cập nhật thuộc tính giao diện qua {@link BookingViewModel.UiDispatcher}.</li>
 *   <li>Single-flight & Cooldown: Không gửi trùng lặp nhiều request tra cứu cùng lúc.</li>
 *   <li>Kiểm tra tính hợp lệ của requestId (rỗng/sai format -> chặn ngay tại Client).</li>
 *   <li>Khớp requestId phản hồi: Bỏ qua các kết quả không khớp requestId đã gửi.</li>
 *   <li>Chế độ socket không bao giờ sử dụng dữ liệu giả từ MockTicketSource.</li>
 *   <li>Vô hiệu hóa tính năng Lịch sử vé nâng cao khi chưa có contract được duyệt (BLOCKED/P1).</li>
 * </ul>
 */
public class TicketLookupViewModel {

    private static final Logger LOGGER = Logger.getLogger(TicketLookupViewModel.class.getName());
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[A-Za-z0-9_-]{1,128}$");

    public enum SearchType {
        ALL("Tất cả mã"),
        REQUEST_ID("Mã yêu cầu (Request ID)"),
        TICKET_ID("Mã vé (Ticket ID)");

        private final String displayName;

        SearchType(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }

        public static SearchType fromDisplayName(String name) {
            for (SearchType type : values()) {
                if (type.displayName.equalsIgnoreCase(name)) {
                    return type;
                }
            }
            return ALL;
        }
    }

    private final BookingClient bookingClient;
    private final PersistentPendingJournal journal;
    private final BookingViewModel.UiDispatcher uiDispatcher;
    private final Runnable seatsRefreshCallback;
    private final boolean mockMode;

    // Quản lý đơn chuyến (Single-flight) và Cooldown
    private final AtomicBoolean isLookingUp = new AtomicBoolean(false);
    private volatile long lastLookupTimestamp = 0L;
    private static final long COOLDOWN_MS = 250L;

    // JavaFX Properties
    private final ObjectProperty<SearchType> searchType = new SimpleObjectProperty<>(SearchType.ALL);
    private final StringProperty queryInput = new SimpleStringProperty("");
    private final BooleanProperty busy = new SimpleBooleanProperty(false);
    private final StringProperty statusMessage = new SimpleStringProperty("");
    private final StringProperty statusStyleClass = new SimpleStringProperty("search-msg-muted");
    private final BooleanProperty historyEnabled = new SimpleBooleanProperty(false);
    private final StringProperty historyBlockedMessage = new SimpleStringProperty(
            "Lịch sử vé nâng cao chưa có contract được duyệt (BLOCKED/P1)");

    private final ObjectProperty<GetRequestResultResponseDto> lastLookupResponse = new SimpleObjectProperty<>(null);

    // Danh sách vé
    private final ObservableList<TicketViewModel> allConfirmedTickets = FXCollections.observableArrayList();
    private final ObservableList<TicketViewModel> displayedTickets = FXCollections.observableArrayList();

    public TicketLookupViewModel(BookingClient bookingClient) {
        this(bookingClient, null, null, null);
    }

    public TicketLookupViewModel(BookingClient bookingClient,
                                 PersistentPendingJournal journal,
                                 BookingViewModel.UiDispatcher uiDispatcher,
                                 Runnable seatsRefreshCallback) {
        this.bookingClient = Objects.requireNonNull(bookingClient, "BookingClient không được null");
        this.journal = journal;
        this.seatsRefreshCallback = seatsRefreshCallback;
        this.uiDispatcher = (uiDispatcher != null) ? uiDispatcher : action -> {
            if (Platform.isFxApplicationThread()) {
                action.run();
            } else {
                try {
                    Platform.runLater(action);
                } catch (IllegalStateException e) {
                    // JavaFX Toolkit chưa khởi tạo (môi trường unit test không giao diện)
                    action.run();
                }
            }
        };

        BookingClient unwrapped = (bookingClient instanceof vn.nhom6.railway.client.network.JournalingBookingClient jbc)
                ? jbc.getDelegate()
                : bookingClient;
        boolean isRealTransport = (unwrapped instanceof vn.nhom6.railway.client.network.SocketBookingClient)
                || (unwrapped instanceof vn.nhom6.railway.client.network.LeaderAwareBookingClient);
        this.mockMode = !isRealTransport && (
                (unwrapped instanceof MockBookingClient)
                        || "mock".equalsIgnoreCase(System.getProperty("client.mode", ""))
                        || "mock".equalsIgnoreCase(System.getProperty("booking.client", ""))
        );

        // Lịch sử nâng cao GET_TICKET_HISTORY: BLOCKED/P1 khi ở chế độ socket
        this.historyEnabled.set(this.mockMode);

        initializeData();
    }

    private void runOnUi(Runnable action) {
        uiDispatcher.dispatch(action);
    }

    private void initializeData() {
        if (mockMode) {
            allConfirmedTickets.setAll(TicketViewModel.MockTicketSource.getSharedTickets());
            statusMessage.set("Chế độ MOCK: Hiển thị " + allConfirmedTickets.size() + " vé mô phỏng (DEMO).");
            statusStyleClass.set("search-msg-muted");
        } else {
            // Chế độ SOCKET / REAL: Tuyệt đối KHÔNG dùng MockTicketSource!
            loadConfirmedTicketsFromJournal();
            statusMessage.set("Chế độ SOCKET: Hiển thị " + allConfirmedTickets.size() + " vé đã xác nhận trong ứng dụng này.");
            statusStyleClass.set("search-msg-muted");
        }
        displayedTickets.setAll(allConfirmedTickets);
    }

    /**
     * Nạp các vé đã xác nhận từ Journal lưu bền (nếu có).
     */
    public void loadConfirmedTicketsFromJournal() {
        if (journal == null) {
            return;
        }
        try {
            List<PendingEntry> entries = journal.getAll();
            for (PendingEntry entry : entries) {
                if (entry.getState() == PendingState.RESOLVED
                        && "BOOK_TICKET".equals(entry.getCommandType())
                        && (entry.getFinalOutcome() == null || "CONFIRMED".equalsIgnoreCase(entry.getFinalOutcome()))) {
                    String reqId = entry.getRequestId();
                    boolean exists = allConfirmedTickets.stream().anyMatch(t -> reqId.equals(t.getRequestId()));
                    if (!exists) {
                        String parsedSeat = "—";
                        String parsedTrip = "SE-001";
                        if (entry.getPayload() != null && !entry.getPayload().isBlank()) {
                            try {
                                com.fasterxml.jackson.databind.JsonNode node =
                                        vn.nhom6.railway.protocol.codec.MessageCodec.getMapper().readTree(entry.getPayload());
                                if (node.hasNonNull("seatId")) {
                                    parsedSeat = node.get("seatId").asText();
                                }
                                if (node.hasNonNull("tripId")) {
                                    parsedTrip = node.get("tripId").asText();
                                }
                            } catch (Exception ignored) {
                            }
                        }
                        TicketViewModel tvm = new TicketViewModel(
                                reqId,
                                "TCK-" + reqId.substring(0, Math.min(8, reqId.length())),
                                parsedSeat,
                                parsedTrip,
                                "—",
                                entry.getCreatedAt().toString(),
                                "1.250.000 VNĐ",
                                TicketViewModel.TicketStatus.CONFIRMED,
                                "chưa xác minh",
                                "REAL"
                        );
                        allConfirmedTickets.add(tvm);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Không thể đọc journal để nạp vé đã xác nhận: " + e.getMessage(), e);
        }
    }

    /**
     * Kiểm tra định dạng requestId hợp lệ (không rỗng, đúng độ dài và ký tự).
     */
    public boolean isValidRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return false;
        }
        return REQUEST_ID_PATTERN.matcher(requestId.trim()).matches();
    }

    /**
     * Thực hiện tra cứu theo requestId theo hợp đồng SPEC-03 Section 4.4.
     */
    public CompletableFuture<GetRequestResultResponseDto> lookup(String rawRequestId) {
        if (rawRequestId == null || rawRequestId.isBlank()) {
            runOnUi(() -> {
                statusMessage.set("⚠️ Vui lòng nhập mã Request ID để tra cứu!");
                statusStyleClass.set("search-msg-error");
            });
            return CompletableFuture.failedFuture(new IllegalArgumentException("Mã Request ID không được để trống!"));
        }

        String requestId = rawRequestId.trim();

        if (!isValidRequestId(requestId)) {
            runOnUi(() -> {
                statusMessage.set("⚠️ Mã Request ID không hợp lệ (phải chứa ký tự chữ cái, số, gạch ngang hoặc gạch dưới)!");
                statusStyleClass.set("search-msg-error");
            });
            return CompletableFuture.failedFuture(new IllegalArgumentException("Mã Request ID không hợp lệ: " + requestId));
        }

        // Kiểm tra Single-Flight và Cooldown
        long now = System.currentTimeMillis();
        if (now - lastLookupTimestamp < COOLDOWN_MS) {
            runOnUi(() -> {
                statusMessage.set("⏳ Thao tác quá nhanh. Vui lòng chờ trong giây lát trước khi tra cứu lại.");
                statusStyleClass.set("search-msg-muted");
            });
            return CompletableFuture.failedFuture(new IllegalStateException("Đang trong thời gian cooldown tra cứu!"));
        }

        if (!isLookingUp.compareAndSet(false, true)) {
            runOnUi(() -> {
                statusMessage.set("⏳ Đang xử lý một yêu cầu tra cứu khác, vui lòng đợi...");
                statusStyleClass.set("search-msg-muted");
            });
            return CompletableFuture.failedFuture(new IllegalStateException("Đang có một yêu cầu tra cứu đang thực hiện!"));
        }

        lastLookupTimestamp = now;

        runOnUi(() -> {
            busy.set(true);
            statusMessage.set("🔍 Đang gửi yêu cầu tra cứu requestId=" + requestId + " tới máy chủ...");
            statusStyleClass.set("search-msg-info");
        });

        CompletableFuture<GetRequestResultResponseDto> future = new CompletableFuture<>();

        bookingClient.getRequestResult(requestId).whenComplete((response, throwable) -> runOnUi(() -> {
            isLookingUp.set(false);
            busy.set(false);

            if (throwable != null) {
                var pres = vn.nhom6.railway.client.controller.ErrorPresentationMapper.mapThrowable(throwable, mockMode);
                String errMsg = "Chưa thể hoàn tất tra cứu (" + pres.rawCode()
                        + "). Trạng thái yêu cầu giữ nguyên CHƯA RÕ (UNKNOWN).";
                statusMessage.set(errMsg);
                statusStyleClass.set("search-msg-error");
                future.completeExceptionally(throwable);
                return;
            }

            if (response == null) {
                statusMessage.set("Nhận phản hồi null từ máy chủ. Giữ nguyên trạng thái chưa rõ.");
                statusStyleClass.set("search-msg-error");
                future.complete(null);
                return;
            }

            // Bất biến: Kiểm tra requestId trong phản hồi phải khớp requestId đã gửi
            if (!Objects.equals(requestId, response.getRequestId())) {
                LOGGER.warning(String.format("[LOOKUP-MISMATCH] Bỏ qua kết quả tra cứu: kỳ vọng requestId=%s nhưng nhận được %s",
                        requestId, response.getRequestId()));
                statusMessage.set("⚠️ Cảnh báo: Mã requestId trong phản hồi không khớp với mã yêu cầu tra cứu. Bỏ qua kết quả này.");
                statusStyleClass.set("search-msg-error");
                future.complete(response);
                return;
            }

            lastLookupResponse.set(response);
            handleLookupResponse(requestId, response);
            future.complete(response);
        }));

        return future;
    }

    private void handleLookupResponse(String requestId, GetRequestResultResponseDto response) {
        String state = response.getState();

        switch (state) {
            case GetRequestResultResponseDto.STATE_APPLIED_SUCCESS -> {
                String cmdType = response.getCommandType() != null ? response.getCommandType() : "BOOK_TICKET";
                String ticketId = response.getTicketId() != null ? response.getTicketId() : ("TCK-" + requestId.substring(0, Math.min(8, requestId.length())));
                Long logIndex = response.getAppliedLogIndex();

                if ("BOOK_TICKET".equalsIgnoreCase(cmdType)) {
                    // Thêm hoặc cập nhật vé vào danh sách đã xác nhận
                    TicketViewModel existing = findTicketByRequestId(requestId);
                    if (existing == null) {
                        TicketViewModel confirmedTvm = new TicketViewModel(
                                requestId,
                                ticketId,
                                "—",
                                "SE-001",
                                "—",
                                LocalDateTime.now().format(DATE_TIME_FORMATTER),
                                "1.250.000 VNĐ",
                                TicketViewModel.TicketStatus.CONFIRMED,
                                "chưa xác minh",
                                mockMode ? "DEMO / MOCK" : "REAL"
                        );
                        allConfirmedTickets.add(0, confirmedTvm);
                    } else {
                        existing.setStatus(TicketViewModel.TicketStatus.CONFIRMED);
                        existing.setReasonCode("chưa xác minh");
                    }
                } else if ("CANCEL_TICKET".equalsIgnoreCase(cmdType)) {
                    TicketViewModel existing = findTicketByTicketId(ticketId);
                    if (existing != null) {
                        existing.setStatus(TicketViewModel.TicketStatus.CANCELLED);
                        existing.setReasonCode("HỦY ĐÃ XÁC NHẬN");
                    }
                }

                // Cập nhật journal sang RESOLVED
                if (journal != null) {
                    try {
                        journal.markResolved(requestId, "CONFIRMED");
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Không thể cập nhật journal sang RESOLVED: " + e.getMessage(), e);
                    }
                }

                if (seatsRefreshCallback != null) {
                    seatsRefreshCallback.run();
                }

                displayedTickets.setAll(allConfirmedTickets);
                statusMessage.set("✓ Tra cứu thành công (APPLIED_SUCCESS): Yêu cầu đã được xác nhận (LogIndex: "
                        + (logIndex != null ? logIndex : "—") + ") [Chưa xác minh]");
                statusStyleClass.set("search-msg-success");
            }
            case GetRequestResultResponseDto.STATE_APPLIED_REJECTED -> {
                String reasonCode = response.getReasonCode() != null ? response.getReasonCode() : "YÊU CẦU BỊ TỪ CHỐI";
                if (journal != null) {
                    try {
                        journal.markResolved(requestId, "REJECTED");
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Không thể cập nhật journal sang RESOLVED: " + e.getMessage(), e);
                    }
                }
                statusMessage.set("⛔ Tra cứu hoàn tất (APPLIED_REJECTED): Yêu cầu bị từ chối với lý do: " + reasonCode);
                statusStyleClass.set("search-msg-error");
            }
            case GetRequestResultResponseDto.STATE_PENDING -> {
                statusMessage.set("⏳ Node đã nhận yêu cầu và đang chờ xử lý (PENDING). Vui lòng thử lại sau giây lát.");
                statusStyleClass.set("search-msg-info");
            }
            case GetRequestResultResponseDto.STATE_UNKNOWN -> {
                String reason = response.getReasonCode();
                String details = (reason != null && !reason.isBlank()) ? " (" + reason + ")" : "";
                statusMessage.set("⚠ Chưa rõ kết quả xử lý tại Node (UNKNOWN)" + details + ". Yêu cầu tiếp tục được theo dõi trong Journal.");
                statusStyleClass.set("search-msg-muted");
            }
            default -> {
                statusMessage.set("Trạng thái phản hồi không xác định: " + state);
                statusStyleClass.set("search-msg-muted");
            }
        }
    }

    /**
     * Tra cứu theo mã vé (Ticket ID) trong danh sách vé đã xác nhận cục bộ của ứng dụng.
     * Tuyệt đối không gửi bản tin socket tự chế.
     */
    public void searchByTicketId(String rawTicketId) {
        if (rawTicketId == null || rawTicketId.isBlank()) {
            displayedTickets.setAll(allConfirmedTickets);
            statusMessage.set("Hiển thị tất cả " + allConfirmedTickets.size() + " vé đã xác nhận.");
            statusStyleClass.set("search-msg-muted");
            return;
        }

        String ticketId = rawTicketId.trim().toLowerCase();
        List<TicketViewModel> matched = allConfirmedTickets.stream()
                .filter(t -> t.getTicketId().toLowerCase().contains(ticketId))
                .toList();

        displayedTickets.setAll(matched);
        if (matched.isEmpty()) {
            statusMessage.set("✗ Tra cứu trong vé đã xác nhận của ứng dụng này: Không tìm thấy vé khớp với mã '" + rawTicketId + "'.");
            statusStyleClass.set("search-msg-error");
        } else {
            statusMessage.set("✓ Tra cứu trong vé đã xác nhận của ứng dụng này: Tìm thấy " + matched.size() + " vé khớp với mã '" + rawTicketId + "'.");
            statusStyleClass.set("search-msg-success");
        }
    }

    public void filterAll(String query) {
        if (query == null || query.isBlank()) {
            displayedTickets.setAll(allConfirmedTickets);
            statusMessage.set("Hiển thị tất cả " + allConfirmedTickets.size() + " vé.");
            statusStyleClass.set("search-msg-muted");
            return;
        }

        String q = query.trim().toLowerCase();
        List<TicketViewModel> matched = allConfirmedTickets.stream()
                .filter(t -> t.getRequestId().toLowerCase().contains(q) || t.getTicketId().toLowerCase().contains(q))
                .toList();

        displayedTickets.setAll(matched);
        if (matched.isEmpty()) {
            statusMessage.set("✗ Không tìm thấy vé nào khớp với từ khóa '" + query + "'.");
            statusStyleClass.set("search-msg-error");
        } else {
            statusMessage.set("✓ Tìm thấy " + matched.size() + " vé khớp với từ khóa '" + query + "'.");
            statusStyleClass.set("search-msg-success");
        }
    }

    public void resetFilter() {
        displayedTickets.setAll(allConfirmedTickets);
        statusMessage.set("Hiển thị tất cả " + allConfirmedTickets.size() + " vé.");
        statusStyleClass.set("search-msg-muted");
    }

    private TicketViewModel findTicketByRequestId(String requestId) {
        return allConfirmedTickets.stream()
                .filter(t -> Objects.equals(requestId, t.getRequestId()))
                .findFirst()
                .orElse(null);
    }

    private TicketViewModel findTicketByTicketId(String ticketId) {
        return allConfirmedTickets.stream()
                .filter(t -> Objects.equals(ticketId, t.getTicketId()))
                .findFirst()
                .orElse(null);
    }

    // =========================================================================
    // GETTERS & PROPERTY ACCESSORS
    // =========================================================================

    public SearchType getSearchType() {
        return searchType.get();
    }

    public ObjectProperty<SearchType> searchTypeProperty() {
        return searchType;
    }

    public void setSearchType(SearchType searchType) {
        this.searchType.set(searchType);
    }

    public String getQueryInput() {
        return queryInput.get();
    }

    public StringProperty queryInputProperty() {
        return queryInput;
    }

    public void setQueryInput(String queryInput) {
        this.queryInput.set(queryInput);
    }

    public boolean isBusy() {
        return busy.get();
    }

    public BooleanProperty busyProperty() {
        return busy;
    }

    public String getStatusMessage() {
        return statusMessage.get();
    }

    public StringProperty statusMessageProperty() {
        return statusMessage;
    }

    public String getStatusStyleClass() {
        return statusStyleClass.get();
    }

    public StringProperty statusStyleClassProperty() {
        return statusStyleClass;
    }

    public boolean isHistoryEnabled() {
        return historyEnabled.get();
    }

    public BooleanProperty historyEnabledProperty() {
        return historyEnabled;
    }

    public String getHistoryBlockedMessage() {
        return historyBlockedMessage.get();
    }

    public StringProperty historyBlockedMessageProperty() {
        return historyBlockedMessage;
    }

    public GetRequestResultResponseDto getLastLookupResponse() {
        return lastLookupResponse.get();
    }

    public ObjectProperty<GetRequestResultResponseDto> lastLookupResponseProperty() {
        return lastLookupResponse;
    }

    public ObservableList<TicketViewModel> getAllConfirmedTickets() {
        return allConfirmedTickets;
    }

    public ObservableList<TicketViewModel> getDisplayedTickets() {
        return displayedTickets;
    }

    public boolean isMockMode() {
        return mockMode;
    }
}
