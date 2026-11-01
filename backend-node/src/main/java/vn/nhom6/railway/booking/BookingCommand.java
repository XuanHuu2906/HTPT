package vn.nhom6.railway.booking;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * SPEC-05 §2 / SPEC-03 §6.1 — BookingCommand
 *
 * <p>Sealed interface đại diện cho một lệnh nghiệp vụ đặt/hủy vé được Leader bổ sung vào Raft log.
 * Tất cả field do Leader cấp (ticketId, issuedAt, cancelledAt) phải nằm trong command trước khi
 * append; không sinh UUID hay thời gian bên trong class này.
 *
 * <p>Độc lập với wire DTO của module shared (TV3-D01) — không import gì từ shared.
 *
 * <p>Ràng buộc validate được thực thi ngay trong compact constructor để đảm bảo mọi instance đều
 * hợp lệ.
 */
public sealed interface BookingCommand permits
        BookingCommand.BookTicket,
        BookingCommand.CancelTicket,
        BookingCommand.NoOp {

    // -----------------------------------------------------------------------
    // Enum type
    // -----------------------------------------------------------------------

    /** Loại lệnh khớp với commandType trong Raft log entry (SPEC-03 §6.1). */
    enum Type {
        BOOK_TICKET,
        CANCEL_TICKET,
        NO_OP
    }

    /** Trả về loại lệnh; dùng để phân biệt khi serialise/apply state machine. */
    Type type();

    // -----------------------------------------------------------------------
    // Hằng số validate dùng chung
    // -----------------------------------------------------------------------

    /** Pattern UUID chữ thường dạng 8-4-4-4-12 hex theo SPEC-03 §3.0. */
    Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    /** tripId duy nhất được phép trong SPEC-00 §3. */
    String VALID_TRIP_ID = "SE-001";

    /**
     * Pattern seatId: G01–G10 theo DDL constraint SPEC-05 §4:
     * {@code CHECK (seat_id ~ '^G(0[1-9]|10)$')}.
     */
    Pattern SEAT_PATTERN = Pattern.compile("^G(0[1-9]|10)$");

    /**
     * Pattern ticketId: ký tự ASCII {@code [A-Za-z0-9-]}, độ dài 1–64 theo SPEC-03 §3.0.
     */
    Pattern TICKET_ID_PATTERN = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

    /** Độ dài tối đa tên hành khách tính bằng Unicode code point (SPEC-05 §2). */
    int MAX_PASSENGER_NAME_CP = 120;

    // -----------------------------------------------------------------------
    // Utility methods (dùng trong compact constructors)
    // -----------------------------------------------------------------------

    /** Kiểm tra requestId hợp lệ; ném {@link IllegalArgumentException} nếu sai. */
    static void requireValidRequestId(String requestId) {
        if (requestId == null || !UUID_PATTERN.matcher(requestId).matches()) {
            throw new IllegalArgumentException(
                    "requestId phải là UUID chữ thường dạng 8-4-4-4-12; nhận: " + requestId);
        }
    }

    /** Kiểm tra tripId hợp lệ. */
    static void requireValidTripId(String tripId) {
        if (!VALID_TRIP_ID.equals(tripId)) {
            throw new IllegalArgumentException(
                    "tripId phải là \"SE-001\"; nhận: " + tripId);
        }
    }

    /** Kiểm tra seatId hợp lệ. */
    static void requireValidSeatId(String seatId) {
        if (seatId == null || !SEAT_PATTERN.matcher(seatId).matches()) {
            throw new IllegalArgumentException(
                    "seatId phải khớp ^G(0[1-9]|10)$; nhận: " + seatId);
        }
    }

    /**
     * Kiểm tra và trả về tên hành khách đã strip().
     *
     * <ul>
     *   <li>Gọi {@link String#strip()} trước.
     *   <li>Đếm bằng {@link String#codePointCount(int, int)} để đúng với Unicode đa mặt phẳng.
     *   <li>Không chứa U+0000, không đổi hoa/thường, không normalize Unicode.
     *   <li>1–120 code point sau strip.
     * </ul>
     */
    static String requireValidPassengerName(String passengerName) {
        if (passengerName == null) {
            throw new IllegalArgumentException("passengerName không được null");
        }
        String stripped = passengerName.strip();
        if (stripped.indexOf('\u0000') >= 0) {
            throw new IllegalArgumentException("passengerName chứa U+0000");
        }
        int cp = stripped.codePointCount(0, stripped.length());
        if (cp < 1 || cp > MAX_PASSENGER_NAME_CP) {
            throw new IllegalArgumentException(
                    "passengerName sau strip phải có 1–120 code point; có " + cp);
        }
        return stripped;
    }

    /** Kiểm tra ticketId hợp lệ. */
    static void requireValidTicketId(String ticketId) {
        if (ticketId == null || !TICKET_ID_PATTERN.matcher(ticketId).matches()) {
            throw new IllegalArgumentException(
                    "ticketId phải khớp [A-Za-z0-9-]{1,64}; nhận: " + ticketId);
        }
    }

    /**
     * Kiểm tra Instant chính xác đến millisecond.
     * Từ chối nếu có phần nhỏ hơn ms (nano % 1_000_000 != 0).
     */
    static void requireMillisInstant(Instant instant, String fieldName) {
        if (instant == null) {
            throw new IllegalArgumentException(fieldName + " không được null");
        }
        if (instant.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException(
                    fieldName + " phải chính xác đến millisecond (nano=" + instant.getNano() + ")");
        }
    }

    // -----------------------------------------------------------------------
    // Records
    // -----------------------------------------------------------------------

    /**
     * Lệnh đặt vé.
     *
     * <p>Tất cả field do Leader sinh ({@code ticketId}, {@code issuedAt}) phải được truyền vào
     * trước append; không sinh lại khi apply.
     *
     * @param requestId     UUID chữ thường 8-4-4-4-12 (SPEC-03 §3.0)
     * @param tripId        phải là "SE-001"
     * @param seatId        khớp ^G(0[1-9]|10)$
     * @param passengerName 1–120 Unicode code point sau strip, không chứa U+0000
     * @param ticketId      [A-Za-z0-9-]{1,64}, do Leader cấp
     * @param issuedAt      Instant chính xác millisecond, do Leader cấp
     */
    record BookTicket(
            String requestId,
            String tripId,
            String seatId,
            String passengerName,
            String ticketId,
            Instant issuedAt
    ) implements BookingCommand {

        /** Compact constructor — validate và chuẩn hóa tên. */
        public BookTicket {
            requireValidRequestId(requestId);
            requireValidTripId(tripId);
            requireValidSeatId(seatId);
            passengerName = requireValidPassengerName(passengerName); // lưu tên đã strip
            requireValidTicketId(ticketId);
            requireMillisInstant(issuedAt, "issuedAt");
        }

        @Override
        public Type type() {
            return Type.BOOK_TICKET;
        }
    }

    /**
     * Lệnh hủy vé (P1).
     *
     * @param requestId   UUID chữ thường 8-4-4-4-12
     * @param ticketId    vé đích, [A-Za-z0-9-]{1,64}
     * @param cancelledAt Instant chính xác millisecond, do Leader cấp
     */
    record CancelTicket(
            String requestId,
            String ticketId,
            Instant cancelledAt
    ) implements BookingCommand {

        /** Compact constructor — validate. */
        public CancelTicket {
            requireValidRequestId(requestId);
            requireValidTicketId(ticketId);
            requireMillisInstant(cancelledAt, "cancelledAt");
        }

        @Override
        public Type type() {
            return Type.CANCEL_TICKET;
        }
    }

    /**
     * Entry nội bộ phục vụ read barrier trong Raft (SPEC-03 §6.1 / SPEC-04).
     *
     * <p>Không có field nghiệp vụ; không có requestId/ticket/audit.
     * Command JSON tương ứng trong log là object rỗng {@code {}}.
     */
    record NoOp() implements BookingCommand {

        @Override
        public Type type() {
            return Type.NO_OP;
        }
    }
}
