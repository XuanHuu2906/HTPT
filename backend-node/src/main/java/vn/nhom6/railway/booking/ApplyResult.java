package vn.nhom6.railway.booking;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * SPEC-03 §4.2 / §4.3 — ApplyResult
 *
 * <p>Kết quả sau khi state machine apply một lệnh đã được Raft commit. Chỉ phản ánh kết quả
 * nghiệp vụ (CONFIRMED, REJECTED, UNKNOWN) — không phải lỗi pre-append như INVALID_REQUEST,
 * NOT_LEADER, NO_QUORUM (những lỗi đó là ERROR_RESPONSE, không thuộc ApplyResult).
 *
 * <p>Ràng buộc tổ hợp field được kiểm tra chặt trong constructor theo bảng SPEC-03 §4.2/§4.3:
 * <ul>
 *   <li>CONFIRMED: ticketId != null, reasonCode == null, appliedLogIndex >= 1
 *   <li>REJECTED (BOOK): ticketId == null, reasonCode ∈ {SEAT_ALREADY_BOOKED, INVALID_REQUEST},
 *       appliedLogIndex >= 1
 *   <li>REJECTED (CANCEL): ticketId là vé đích (non-null), reasonCode = TICKET_NOT_ACTIVE,
 *       appliedLogIndex >= 1
 *   <li>UNKNOWN: reasonCode = TIMEOUT_UNKNOWN, appliedLogIndex == null
 * </ul>
 *
 * <p>UNKNOWN không được lưu vào bảng {@code request_results} — xem {@link #isPersistable()}.
 */
public record ApplyResult(
        String requestId,
        Outcome outcome,
        String ticketId,
        String reasonCode,
        Long appliedLogIndex
) {

    // -----------------------------------------------------------------------
    // Enums
    // -----------------------------------------------------------------------

    /** Kết quả nghiệp vụ sau khi apply lệnh đã commit (SPEC-03 §4.2/§4.3). */
    public enum Outcome {
        /** Đã commit + apply thành công. */
        CONFIRMED,
        /** Đã commit + apply nhưng bị từ chối nghiệp vụ (deterministic). */
        REJECTED,
        /**
         * Server hết thời hạn chờ kết quả apply; Client chưa biết kết quả cuối cùng.
         * Không được lưu vào request_results.
         */
        UNKNOWN
    }

    // -----------------------------------------------------------------------
    // ReasonCode constants — khớp với SPEC-03 §7 errorCode dùng làm reasonCode trong response
    // -----------------------------------------------------------------------

    /** Ghế đã có vé ACTIVE — reasonCode khi BOOK bị REJECTED. */
    public static final String SEAT_ALREADY_BOOKED = "SEAT_ALREADY_BOOKED";

    /** Validation tất định khi apply — reasonCode khi BOOK bị REJECTED do input sai (SPEC-05 §3). */
    public static final String INVALID_REQUEST = "INVALID_REQUEST";

    /** Vé không còn hiệu lực — reasonCode khi CANCEL bị REJECTED. */
    public static final String TICKET_NOT_ACTIVE = "TICKET_NOT_ACTIVE";

    /** Server timeout, chưa biết kết quả — reasonCode của UNKNOWN. */
    public static final String TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN";

    /** Tập reasonCode hợp lệ cho BOOK REJECTED. */
    private static final Set<String> BOOK_REJECTED_REASON_CODES =
            Set.of(SEAT_ALREADY_BOOKED, INVALID_REQUEST);

    /** Pattern UUID chữ thường (dùng để validate requestId). */
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    // -----------------------------------------------------------------------
    // Compact constructor — validate tổ hợp field theo bảng SPEC
    // -----------------------------------------------------------------------

    /**
     * Compact constructor kiểm tra tất cả ràng buộc tổ hợp.
     *
     * <p>Constructor này KHÔNG biết lệnh BOOK hay CANCEL; nó kiểm tra điều kiện chung:
     * <ul>
     *   <li>requestId hợp lệ
     *   <li>outcome không null
     *   <li>CONFIRMED: ticketId != null, reasonCode == null, appliedLogIndex >= 1
     *   <li>REJECTED: appliedLogIndex >= 1; reasonCode phải là một trong các giá trị hợp lệ;
     *       xem lưu ý về ticketId bên dưới
     *   <li>UNKNOWN: reasonCode == TIMEOUT_UNKNOWN, appliedLogIndex == null
     * </ul>
     *
     * <p>Lưu ý về REJECTED ticketId: theo SPEC-03 §4.2 BOOK REJECTED ticketId == null; theo
     * §4.3 CANCEL REJECTED ticketId là vé đích (non-null). Thay vì bắt constructor biết loại
     * lệnh, các factory method {@link #bookRejected} và {@link #cancelRejected} kiểm tra
     * thêm điều kiện đó. Constructor chung chỉ enforce: reasonCode thuộc tập cho phép.
     */
    public ApplyResult {
        Objects.requireNonNull(outcome, "outcome không được null");
        if (requestId == null || !UUID_PATTERN.matcher(requestId).matches()) {
            throw new IllegalArgumentException(
                    "requestId phải là UUID chữ thường dạng 8-4-4-4-12; nhận: " + requestId);
        }

        switch (outcome) {
            case CONFIRMED -> {
                if (ticketId == null) {
                    throw new IllegalArgumentException(
                            "CONFIRMED yêu cầu ticketId != null");
                }
                if (reasonCode != null) {
                    throw new IllegalArgumentException(
                            "CONFIRMED yêu cầu reasonCode == null; nhận: " + reasonCode);
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException(
                            "CONFIRMED yêu cầu appliedLogIndex >= 1; nhận: " + appliedLogIndex);
                }
            }
            case REJECTED -> {
                // reasonCode phải thuộc tập hợp chung của cả BOOK và CANCEL
                Set<String> allRejectedReasonCodes =
                        Set.of(SEAT_ALREADY_BOOKED, INVALID_REQUEST, TICKET_NOT_ACTIVE);
                if (reasonCode == null || !allRejectedReasonCodes.contains(reasonCode)) {
                    throw new IllegalArgumentException(
                            "REJECTED yêu cầu reasonCode ∈ "
                            + allRejectedReasonCodes + "; nhận: " + reasonCode);
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException(
                            "REJECTED yêu cầu appliedLogIndex >= 1; nhận: " + appliedLogIndex);
                }
            }
            case UNKNOWN -> {
                if (!TIMEOUT_UNKNOWN.equals(reasonCode)) {
                    throw new IllegalArgumentException(
                            "UNKNOWN yêu cầu reasonCode == TIMEOUT_UNKNOWN; nhận: " + reasonCode);
                }
                if (appliedLogIndex != null) {
                    throw new IllegalArgumentException(
                            "UNKNOWN yêu cầu appliedLogIndex == null; nhận: " + appliedLogIndex);
                }
                // ticketId nên null cho UNKNOWN nhưng SPEC không cấm tường minh trong constructor
                // chung; để an toàn ta enforce null
                if (ticketId != null) {
                    throw new IllegalArgumentException(
                            "UNKNOWN yêu cầu ticketId == null; nhận: " + ticketId);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // isPersistable
    // -----------------------------------------------------------------------

    /**
     * Trả về {@code false} cho UNKNOWN vì UNKNOWN không được lưu vào {@code request_results}
     * (SPEC-03 §4.2: "UNKNOWN không được lưu vào request_results").
     */
    public boolean isPersistable() {
        return outcome != Outcome.UNKNOWN;
    }

    // -----------------------------------------------------------------------
    // Static factory methods
    // -----------------------------------------------------------------------

    /**
     * BOOK CONFIRMED — vé được cấp sau commit+apply thành công.
     *
     * @param requestId       UUID chữ thường 8-4-4-4-12
     * @param ticketId        vé được cấp (non-null)
     * @param appliedLogIndex index >= 1 của entry đã apply
     */
    public static ApplyResult bookConfirmed(
            String requestId,
            String ticketId,
            long appliedLogIndex) {
        return new ApplyResult(requestId, Outcome.CONFIRMED, ticketId, null, appliedLogIndex);
    }

    /**
     * BOOK REJECTED — bị từ chối nghiệp vụ tất định khi apply.
     * ticketId phải null (SPEC-03 §4.2).
     *
     * @param requestId       UUID chữ thường 8-4-4-4-12
     * @param reasonCode      {@link #SEAT_ALREADY_BOOKED} hoặc {@link #INVALID_REQUEST}
     * @param appliedLogIndex index >= 1
     */
    public static ApplyResult bookRejected(
            String requestId,
            String reasonCode,
            long appliedLogIndex) {
        if (!BOOK_REJECTED_REASON_CODES.contains(reasonCode)) {
            throw new IllegalArgumentException(
                    "bookRejected yêu cầu reasonCode ∈ "
                    + BOOK_REJECTED_REASON_CODES + "; nhận: " + reasonCode);
        }
        // ticketId phải null cho BOOK REJECTED
        return new ApplyResult(requestId, Outcome.REJECTED, null, reasonCode, appliedLogIndex);
    }

    /**
     * CANCEL CONFIRMED — vé được hủy thành công.
     *
     * @param requestId       UUID chữ thường 8-4-4-4-12
     * @param ticketId        vé đích đã hủy (non-null)
     * @param appliedLogIndex index >= 1
     */
    public static ApplyResult cancelConfirmed(
            String requestId,
            String ticketId,
            long appliedLogIndex) {
        return new ApplyResult(requestId, Outcome.CONFIRMED, ticketId, null, appliedLogIndex);
    }

    /**
     * CANCEL REJECTED — vé không còn ACTIVE, không thể hủy.
     * ticketId là vé đích (non-null) theo SPEC-03 §4.3.
     *
     * @param requestId       UUID chữ thường 8-4-4-4-12
     * @param ticketId        vé đích (non-null, đây là sự khác biệt với BOOK REJECTED)
     * @param appliedLogIndex index >= 1
     */
    public static ApplyResult cancelRejected(
            String requestId,
            String ticketId,
            long appliedLogIndex) {
        if (ticketId == null) {
            throw new IllegalArgumentException(
                    "cancelRejected yêu cầu ticketId != null (vé đích)");
        }
        // Phải build trực tiếp vì constructor chung accept REJECTED+non-null ticketId
        return new ApplyResult(requestId, Outcome.REJECTED, ticketId, TICKET_NOT_ACTIVE, appliedLogIndex);
    }

    /**
     * BOOK UNKNOWN — server timeout, Client chưa biết kết quả.
     *
     * @param requestId UUID chữ thường 8-4-4-4-12
     */
    public static ApplyResult bookUnknown(String requestId) {
        return new ApplyResult(requestId, Outcome.UNKNOWN, null, TIMEOUT_UNKNOWN, null);
    }

    /**
     * CANCEL UNKNOWN — server timeout, Client chưa biết kết quả hủy.
     *
     * @param requestId UUID chữ thường 8-4-4-4-12
     */
    public static ApplyResult cancelUnknown(String requestId) {
        return new ApplyResult(requestId, Outcome.UNKNOWN, null, TIMEOUT_UNKNOWN, null);
    }
}
