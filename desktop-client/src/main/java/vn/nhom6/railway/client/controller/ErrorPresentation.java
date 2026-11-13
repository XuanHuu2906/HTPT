package vn.nhom6.railway.client.controller;

import java.util.Objects;

/**
 * Đối tượng trình diễn trạng thái và lỗi chuẩn hóa trên giao diện người dùng (SPEC-03 và TASK D13).
 * <p>
 * Thuần Java POJO, không phụ thuộc JavaFX hay I/O để phục vụ kiểm thử đơn vị độc lập.
 */
public class ErrorPresentation {

    private final PresentationKind kind;
    private final String title;
    private final String message;
    private final String styleClass;
    private final boolean retryable;
    private final boolean needsSeatRefresh;
    private final boolean showRequestId;
    private final String suggestedAction;
    private final String rawCode;
    private final String serverMessage;
    private final boolean mock;
    private final ClientErrorKind clientErrorKind;

    public ErrorPresentation(
            PresentationKind kind,
            String title,
            String message,
            String styleClass,
            boolean retryable,
            boolean needsSeatRefresh,
            boolean showRequestId,
            String suggestedAction,
            String rawCode,
            String serverMessage,
            boolean mock,
            ClientErrorKind clientErrorKind) {
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
        this.title = title != null ? title : "";
        this.message = message != null ? message : "";
        this.styleClass = styleClass != null ? styleClass : "status-error";
        this.retryable = retryable;
        this.needsSeatRefresh = needsSeatRefresh;
        this.showRequestId = showRequestId;
        this.suggestedAction = suggestedAction != null ? suggestedAction : "";
        this.rawCode = rawCode;
        this.serverMessage = serverMessage;
        this.mock = mock;
        this.clientErrorKind = clientErrorKind;
    }

    // === Record-style getters ===

    public PresentationKind kind() {
        return kind;
    }

    public String title() {
        return title;
    }

    public String message() {
        return message;
    }

    public String styleClass() {
        return styleClass;
    }

    public boolean retryable() {
        return retryable;
    }

    public boolean needsSeatRefresh() {
        return needsSeatRefresh;
    }

    public boolean showRequestId() {
        return showRequestId;
    }

    public String suggestedAction() {
        return suggestedAction;
    }

    public String rawCode() {
        return rawCode;
    }

    public String serverMessage() {
        return serverMessage;
    }

    public boolean mock() {
        return mock;
    }

    public ClientErrorKind clientErrorKind() {
        return clientErrorKind;
    }

    // === JavaBean-style getters ===

    public PresentationKind getKind() {
        return kind;
    }

    public String getTitle() {
        return title;
    }

    public String getMessage() {
        return message;
    }

    public String getStyleClass() {
        return styleClass;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public boolean isNeedsSeatRefresh() {
        return needsSeatRefresh;
    }

    public boolean isShowRequestId() {
        return showRequestId;
    }

    public String getSuggestedAction() {
        return suggestedAction;
    }

    public String getRawCode() {
        return rawCode;
    }

    public String getServerMessage() {
        return serverMessage;
    }

    public boolean isMock() {
        return mock;
    }

    public ClientErrorKind getClientErrorKind() {
        return clientErrorKind;
    }

    // === Convenience helpers ===

    public boolean isConfirmed() {
        return kind == PresentationKind.CONFIRMED;
    }

    public boolean isRejected() {
        return kind == PresentationKind.REJECTED;
    }

    public boolean isUnknown() {
        return kind == PresentationKind.UNKNOWN;
    }

    public boolean isError() {
        return kind == PresentationKind.ERROR || kind == PresentationKind.CLIENT_ERROR;
    }

    public boolean isDisconnected() {
        return kind == PresentationKind.DISCONNECTED;
    }

    /**
     * Trả về lớp CSS thích hợp cho nhãn trạng thái đặt vé trong dialog (D04/D07).
     */
    public String getBookingStatusClass() {
        return switch (kind) {
            case CONFIRMED -> "booking-status-confirmed";
            case REJECTED -> "booking-status-rejected";
            case UNKNOWN -> "booking-status-unknown";
            case ERROR, CLIENT_ERROR -> "booking-status-rejected";
            case DISCONNECTED -> "booking-status-unknown";
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorPresentation that = (ErrorPresentation) o;
        return retryable == that.retryable &&
                needsSeatRefresh == that.needsSeatRefresh &&
                showRequestId == that.showRequestId &&
                mock == that.mock &&
                kind == that.kind &&
                Objects.equals(title, that.title) &&
                Objects.equals(message, that.message) &&
                Objects.equals(styleClass, that.styleClass) &&
                Objects.equals(suggestedAction, that.suggestedAction) &&
                Objects.equals(rawCode, that.rawCode) &&
                Objects.equals(serverMessage, that.serverMessage) &&
                clientErrorKind == that.clientErrorKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, title, message, styleClass, retryable,
                needsSeatRefresh, showRequestId, suggestedAction, rawCode, serverMessage, mock, clientErrorKind);
    }

    @Override
    public String toString() {
        return "ErrorPresentation{" +
                "kind=" + kind +
                ", title='" + title + '\'' +
                ", message='" + message + '\'' +
                ", styleClass='" + styleClass + '\'' +
                ", retryable=" + retryable +
                ", needsSeatRefresh=" + needsSeatRefresh +
                ", showRequestId=" + showRequestId +
                ", suggestedAction='" + suggestedAction + '\'' +
                ", rawCode='" + rawCode + '\'' +
                ", serverMessage='" + serverMessage + '\'' +
                ", mock=" + mock +
                ", clientErrorKind=" + clientErrorKind +
                '}';
    }
}

