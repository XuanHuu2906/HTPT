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
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * ViewModel quản lý trạng thái thao tác bán vé bất đồng bộ (TASK D07).
 * <p>
 * Tính năng chính:
 * <ul>
 *   <li>Giữ trạng thái màn hình bằng JavaFX Properties chuẩn (uiState, message, busy, mock, errorCode, leaderHint).</li>
 *   <li>Nhận {@link BookingClient} qua dependency injection, không phụ thuộc triển khai Mock hay TCP cụ thể.</li>
 *   <li>Cập nhật mọi thuộc tính trên JavaFX Application Thread thông qua {@link Platform#runLater(Runnable)}.</li>
 *   <li>Chống thao tác trùng: kiểm soát cờ {@code busy} để UI khóa nút gửi trong khi đang xử lý.</li>
 *   <li>Phân biệt rõ ràng các trạng thái: IDLE, LOADING, SUCCESS, REJECTED, ERROR, UNKNOWN, DISCONNECTED.</li>
 *   <li>Tuyệt đối KHÔNG phụ thuộc JDBC và KHÔNG phụ thuộc socket mạng trong UI layer.</li>
 * </ul>
 */
public class BookingViewModel {

    /**
     * Các trạng thái giao diện UI theo đặc tả SPEC-06 và nhiệm vụ D07.
     */
    public enum UiState {
        IDLE("Sẵn sàng", "status-idle"),
        LOADING("Đang xử lý...", "status-loading"),
        SUCCESS("Thao tác thành công", "status-idle"),
        REJECTED("Bị từ chối nghiệp vụ", "status-error"),
        ERROR("Lỗi hạ tầng / Cụm máy chủ", "status-error"),
        UNKNOWN("Chưa xác định kết quả (TIMEOUT)", "status-disconnected"),
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
    private volatile boolean syncMode = false;

    private final ObjectProperty<UiState> uiState = new SimpleObjectProperty<>(UiState.IDLE);
    private final StringProperty message = new SimpleStringProperty("Hệ thống sẵn sàng ở chế độ MOCK (D08).");
    private final BooleanProperty busy = new SimpleBooleanProperty(false);
    private final BooleanProperty mock = new SimpleBooleanProperty(true);
    private final StringProperty errorCode = new SimpleStringProperty("");
    private final StringProperty leaderHint = new SimpleStringProperty("");

    public BookingViewModel(BookingClient bookingClient) {
        this.bookingClient = Objects.requireNonNull(bookingClient, "BookingClient không được null");
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
                // Hỗ trợ unit test môi trường non-JavaFX
                action.run();
            }
        }
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
        return book(request);
    }

    /**
     * Gửi yêu cầu đặt vé bất đồng bộ theo hợp đồng SPEC-03.
     */
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        if (busy.get()) {
            CompletableFuture<BookTicketResponseDto> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(new IllegalStateException("Hệ thống đang bận xử lý yêu cầu trước đó!"));
            return rejected;
        }

        runOnFxThread(() -> {
            busy.set(true);
            uiState.set(UiState.LOADING);
            errorCode.set("");
            leaderHint.set("");
            message.set("Đang gửi yêu cầu đặt vé ghế " + request.getSeatId() + " (Độ trễ giả lập 2s)... [MOCK]");
        });

        CompletableFuture<BookTicketResponseDto> completionFuture = new CompletableFuture<>();

        bookingClient.book(request).whenComplete((result, throwable) -> runOnFxThread(() -> {
            busy.set(false);
            if (throwable != null) {
                handleException(throwable);
                completionFuture.completeExceptionally(throwable);
                return;
            }

            mock.set(result.isMock());
            ErrorPresentation presentation = ErrorPresentationMapper.mapBookingResponse(result);
            message.set(presentation.message());
            errorCode.set(presentation.rawCode() != null ? presentation.rawCode() : "");

            switch (presentation.kind()) {
                case CONFIRMED -> {
                    uiState.set(UiState.SUCCESS);
                    leaderHint.set(result.getLeaderHint() != null ? result.getLeaderHint() : "");
                }
                case REJECTED -> {
                    uiState.set(UiState.REJECTED);
                }
                case UNKNOWN -> {
                    uiState.set(UiState.UNKNOWN);
                }
                case ERROR -> {
                    uiState.set(UiState.ERROR);
                    if (ErrorPresentationMapper.CODE_NOT_LEADER.equals(presentation.rawCode())) {
                        String hint = (result.getLeaderHint() != null && !result.getLeaderHint().isBlank()) ? result.getLeaderHint().trim() : "";
                        leaderHint.set(hint);
                        try {
                            ClusterConnectionViewModel clusterVm = vn.nhom6.railway.client.ClientFactory.getClusterConnectionViewModel();
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
                }
                case CLIENT_ERROR -> {
                    uiState.set(UiState.ERROR);
                }
            }
            completionFuture.complete(result);
        }));

        return completionFuture;
    }

    /**
     * Gửi yêu cầu hủy vé bất đồng bộ theo hợp đồng SPEC-03.
     */
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

        runOnFxThread(() -> {
            busy.set(true);
            uiState.set(UiState.LOADING);
            message.set("Đang gửi yêu cầu hủy vé " + request.getTicketId() + "... [MOCK]");
        });

        CompletableFuture<CancelTicketResponseDto> completionFuture = new CompletableFuture<>();

        bookingClient.cancel(request).whenComplete((result, throwable) -> runOnFxThread(() -> {
            busy.set(false);
            if (throwable != null) {
                handleException(throwable);
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

    /**
     * Làm mới sơ đồ ghế bất đồng bộ.
     */
    public CompletableFuture<GetSeatsResponseDto> refreshSeats() {
        return refreshSeats("SE-001");
    }

    public CompletableFuture<GetSeatsResponseDto> refreshSeats(String tripId) {
        runOnFxThread(() -> {
            busy.set(true);
            uiState.set(UiState.LOADING);
            message.set("Đang tải dữ liệu sơ đồ ghế chuyến " + tripId + "... [MOCK]");
        });

        CompletableFuture<GetSeatsResponseDto> completionFuture = new CompletableFuture<>();

        bookingClient.getSeats(new GetSeatsRequestDto(tripId)).whenComplete((result, throwable) -> runOnFxThread(() -> {
            busy.set(false);
            if (throwable != null) {
                handleException(throwable);
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

    /**
     * Tra cứu trạng thái yêu cầu theo requestId.
     */
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        return bookingClient.getRequestResult(requestId);
    }

    private void handleException(Throwable throwable) {
        ErrorPresentation presentation = ErrorPresentationMapper.mapThrowable(throwable, mock.get());
        message.set(presentation.message());
        errorCode.set(presentation.rawCode() != null ? presentation.rawCode() : "");
        if (presentation.kind() == PresentationKind.DISCONNECTED) {
            uiState.set(UiState.DISCONNECTED);
        } else {
            uiState.set(UiState.ERROR);
        }
    }

    // === JavaFX Properties Getters ===

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

    public BookingClient getBookingClient() {
        return bookingClient;
    }
}
