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

    /**
     * Pattern ticketId: {@code [A-Za-z0-9-]{1,64}} theo SPEC-03 §3.0.
     * Dùng để validate ticketId của CANCEL UNKNOWN trong constructor.
     */
    private static final Pattern TICKET_ID_PATTERN = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

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
     *   <li>REJECTED: appliedLogIndex >= 1; reasonCode ∈ tập hợp lệ
     *   <li>UNKNOWN: reasonCode == TIMEOUT_UNKNOWN, appliedLogIndex == null;
     *       ticketId có thể null (BOOK UNKNOWN) hoặc non-null hợp lệ (CANCEL UNKNOWN —
     *       SPEC-03 §4.3: "ticketId luôn là vé đích trong request")
     * </ul>
     *
     * <p>Lưu ý REJECTED ticketId: BOOK REJECTED → null; CANCEL REJECTED → vé đích (non-null).
     * Factory method {@link #bookRejected} và {@link #cancelRejected} enforce thêm điều kiện
     * đó. Constructor chung chỉ enforce reasonCode thuộc tập cho phép và appliedLogIndex.
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
                // BOOK UNKNOWN: ticketId == null
                // CANCEL UNKNOWN: ticketId là vé đích (non-null) — SPEC-03 §4.3
                // Constructor cho phép cả hai; factory method cancelUnknown() enforce non-null.
                // Nếu non-null thì phải đúng format để tránh garbage vào response.
                if (ticketId != null && !TICKET_ID_PATTERN.matcher(ticketId).matches()) {
                    throw new IllegalArgumentException(
                            "UNKNOWN ticketId (nếu có) phải khớp [A-Za-z0-9-]{1,64}; nhận: "
                            + ticketId);
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
     * <p>Theo SPEC-03 §4.3: "ticketId luôn là vé đích trong request" — áp dụng cho mọi
     * outcome của CANCEL, kể cả UNKNOWN. ticketId không được null.
     *
     * @param requestId UUID chữ thường 8-4-4-4-12
     * @param ticketId  vé đích của yêu cầu hủy (non-null, {@code [A-Za-z0-9-]{1,64}})
     */
    public static ApplyResult cancelUnknown(String requestId, String ticketId) {
        if (ticketId == null) {
            throw new IllegalArgumentException(
                    "cancelUnknown yêu cầu ticketId != null (SPEC-03 §4.3: vé đích)");
        }
        return new ApplyResult(requestId, Outcome.UNKNOWN, ticketId, TIMEOUT_UNKNOWN, null);
    }
}
