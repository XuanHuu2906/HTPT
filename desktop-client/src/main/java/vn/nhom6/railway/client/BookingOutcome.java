package vn.nhom6.railway.client;

/**
 * Kết quả trả về sau khi thực hiện đặt vé (MOCK / BACKEND).
 */
public final class BookingOutcome {

    /**
     * Mã lý do khi ghế đã bị đặt trước bởi giao dịch khác (theo SPEC-03 / SPEC-06).
     */
    public static final String SEAT_ALREADY_BOOKED = "SEAT_ALREADY_BOOKED";

    /**
     * Trạng thái kết quả đặt vé.
     */
    public enum BookingStatus {
        CONFIRMED,
        REJECTED,
        UNKNOWN
    }

    private final BookingStatus status;
    private final String reasonCode;
    private final String referenceCode;
    private final String message;

    public BookingOutcome(BookingStatus status, String reasonCode, String referenceCode, String message) {
        this.status = status;
        this.reasonCode = reasonCode;
        this.referenceCode = referenceCode;
        this.message = message;
    }

    public static BookingOutcome confirmed(String referenceCode, String message) {
        return new BookingOutcome(BookingStatus.CONFIRMED, null, referenceCode, message);
    }

    public static BookingOutcome rejected(String reasonCode, String message) {
        return new BookingOutcome(BookingStatus.REJECTED, reasonCode, null, message);
    }

    public static BookingOutcome unknown(String message) {
        return new BookingOutcome(BookingStatus.UNKNOWN, null, null, message);
    }

    public BookingStatus getStatus() {
        return status;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReferenceCode() {
        return referenceCode;
    }

    public String getMessage() {
        return message;
    }

    public boolean isConfirmed() {
        return status == BookingStatus.CONFIRMED;
    }

    public boolean isRejected() {
        return status == BookingStatus.REJECTED;
    }

    public boolean isUnknown() {
        return status == BookingStatus.UNKNOWN;
    }

    @Override
    public String toString() {
        return "BookingOutcome{" +
                "status=" + status +
                ", reasonCode='" + reasonCode + '\'' +
                ", referenceCode='" + referenceCode + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}

