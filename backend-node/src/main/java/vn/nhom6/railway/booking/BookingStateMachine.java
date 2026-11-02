package vn.nhom6.railway.booking;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * SPEC-05 §3, §3.1 / SPEC-04 §6 — BookingStateMachine (in-memory, không có DB, không có Raft).
 *
 * <p>Lớp này giả định rằng mọi entry truyền vào {@link #apply} đã được Raft commit theo thứ tự.
 * Nhiệm vụ của lớp là áp dụng các lệnh một cách tất định lên state giả lập trong bộ nhớ.
 * Lớp này không tự quyết định commit, không giao tiếp với RaftNode, không truy cập DB/file/mạng.
 *
 * <p>State giả lập:
 * <ul>
 *   <li>10 ghế G01–G10 của SE-001, ban đầu AVAILABLE.
 *   <li>Bảng vé (ticketId → Ticket).
 *   <li>Bảng kết quả theo requestId (requestId → StoredResult).
 *   <li>{@code lastApplied} — ban đầu 0, tăng 1 mỗi entry hợp lệ.
 * </ul>
 *
 * <p>Kiểu trả về của {@link #apply}: {@link ApplyOutcome} sealed interface có 3 loại:
 * {@link ApplyOutcome.BusinessResult} (BOOK/CANCEL có kết quả nghiệp vụ),
 * {@link ApplyOutcome.NoOpApplied} (NO_OP, không có kết quả nghiệp vụ),
 * {@link ApplyOutcome.RequestIdConflict} (cùng requestId nhưng khác ý định).
 *
 * <p>Fail closed: mọi vi phạm thứ tự index hoặc toàn vẹn dữ liệu đều ném {@link IllegalStateException}.
 * Không bao giờ âm thầm bỏ qua hay nhảy cóc entry.
 *
 * <p>Tất định: tuyệt đối không dùng {@code Instant.now()}, {@code UUID.randomUUID()},
 * {@code System.currentTimeMillis()}, {@code Random}, hay đọc nguồn ngoại sinh trong {@link #apply}.
 */
public final class BookingStateMachine {

    // -----------------------------------------------------------------------
    // Hằng số domain
    // -----------------------------------------------------------------------

    private static final String TRIP_ID = "SE-001";

    /** Số ghế cố định theo SPEC-05 §1. */
    private static final int SEAT_COUNT = 10;

    // -----------------------------------------------------------------------
    // Sealed interface kết quả apply (public để test đọc được)
    // -----------------------------------------------------------------------

    /**
     * Kết quả của một lần gọi {@link #apply(long, BookingCommand)}.
     *
     * <p>Ba loại phân biệt:
     * <ul>
     *   <li>{@link BusinessResult}: BOOK/CANCEL có kết quả nghiệp vụ (CONFIRMED hoặc REJECTED).
     *   <li>{@link NoOpApplied}: entry NO_OP, không có kết quả nghiệp vụ.
     *   <li>{@link RequestIdConflict}: cùng requestId nhưng khác ý định — kết quả gốc không đổi.
     * </ul>
     */
    public sealed interface ApplyOutcome
            permits ApplyOutcome.BusinessResult, ApplyOutcome.NoOpApplied, ApplyOutcome.RequestIdConflict {

        /**
         * Kết quả nghiệp vụ BOOK/CANCEL — CONFIRMED hoặc REJECTED.
         * {@code isReplay=true} khi đây là kết quả trả lại từ lần apply đầu tiên (replay requestId
         * cùng ý định).
         */
        record BusinessResult(ApplyResult applyResult, boolean isReplay) implements ApplyOutcome {}

        /**
         * Entry NO_OP đã được apply: lastApplied tăng, không có kết quả nghiệp vụ.
         */
        record NoOpApplied(long newLastApplied) implements ApplyOutcome {}

        /**
         * Cùng requestId nhưng khác ý định hoặc khác loại lệnh.
         * Kết quả gốc không bị ghi đè; entry này vẫn tăng lastApplied.
         */
        record RequestIdConflict(String requestId, long logIndex) implements ApplyOutcome {}
    }

    // -----------------------------------------------------------------------
    // Các nested class lưu state nội bộ (private)
    // -----------------------------------------------------------------------

    /** Trạng thái của một ghế. */
    private enum SeatStatus { AVAILABLE, BOOKED }

    /** Một ghế trong state giả lập. */
    private static final class Seat {
        final String seatId;
        SeatStatus status;
        String currentTicketId; // null khi AVAILABLE

        Seat(String seatId) {
            this.seatId = seatId;
            this.status = SeatStatus.AVAILABLE;
            this.currentTicketId = null;
        }
    }

    /** Trạng thái của một vé. */
    private enum TicketStatus { ACTIVE, CANCELLED }

    /** Một vé trong state giả lập. */
    private static final class Ticket {
        final String ticketId;
        final String tripId;
        final String seatId;
        final String passengerName;
        TicketStatus status;
        final Instant issuedAt;
        Instant cancelledAt; // null khi ACTIVE

        Ticket(String ticketId, String tripId, String seatId,
               String passengerName, Instant issuedAt) {
            this.ticketId = ticketId;
            this.tripId = tripId;
            this.seatId = seatId;
            this.passengerName = passengerName;
            this.status = TicketStatus.ACTIVE;
            this.issuedAt = issuedAt;
            this.cancelledAt = null;
        }
    }

    /**
     * Kết quả đã lưu theo requestId — dùng để dedup/replay.
     * Bao gồm loại lệnh, fingerprint ý định và ApplyResult gốc.
     */
    private static final class StoredResult {
        final BookingCommand.Type commandType;
        final String intentFingerprint; // SHA-256 hex 64 ký tự
        final ApplyResult originalResult;

        StoredResult(BookingCommand.Type commandType,
                     String intentFingerprint,
                     ApplyResult originalResult) {
            this.commandType = commandType;
            this.intentFingerprint = intentFingerprint;
            this.originalResult = originalResult;
        }
    }

    // -----------------------------------------------------------------------
    // State chính
    // -----------------------------------------------------------------------

    /** Bảng ghế: seatId → Seat. Dùng LinkedHashMap để giữ thứ tự G01→G10. */
    private final Map<String, Seat> seats;

    /** Bảng vé: ticketId → Ticket. */
    private final Map<String, Ticket> tickets;

    /** Bảng kết quả theo requestId. */
    private final Map<String, StoredResult> requestResults;

    /** Checkpoint đã apply; tăng 1 mỗi entry (gồm NO_OP, conflict). Ban đầu 0. */
    private long lastApplied;

    // -----------------------------------------------------------------------
    // Khởi tạo
    // -----------------------------------------------------------------------

    /**
     * Tạo state machine mới với 10 ghế AVAILABLE của SE-001, lastApplied=0.
     * Không có vé, không có kết quả nào được lưu.
     */
    public BookingStateMachine() {
        seats = new LinkedHashMap<>();
        for (int i = 1; i <= SEAT_COUNT; i++) {
            String seatId = "G" + String.format("%02d", i);
            seats.put(seatId, new Seat(seatId));
        }
        tickets = new LinkedHashMap<>();
        requestResults = new HashMap<>();
        lastApplied = 0L;
    }

    // -----------------------------------------------------------------------
    // apply — điểm vào chính
    // -----------------------------------------------------------------------

    /**
     * Apply một lệnh đã được Raft commit tại {@code logIndex}.
     *
     * <p><b>Ràng buộc thứ tự:</b> {@code logIndex} phải bằng {@code lastApplied + 1}. Bất kỳ vi
     * phạm nào (nhảy cóc, lặp lại) đều ném {@link IllegalStateException} — fail closed theo
     * SPEC-04 §6.
     *
     * <p>Kết quả trả về:
     * <ul>
     *   <li>{@link ApplyOutcome.BusinessResult}: BOOK/CANCEL có kết quả.
     *   <li>{@link ApplyOutcome.NoOpApplied}: NO_OP, chỉ tăng lastApplied.
     *   <li>{@link ApplyOutcome.RequestIdConflict}: cùng requestId nhưng khác ý định.
     * </ul>
     *
     * <p>Entry giả định đã commit; đây không phải kiểm thử Raft.
     *
     * @param logIndex chỉ số log, phải bằng lastApplied + 1
     * @param cmd      lệnh nghiệp vụ (BookTicket, CancelTicket, hoặc NoOp)
     * @return kết quả apply
     * @throws IllegalStateException nếu {@code logIndex} sai thứ tự
     */
    public ApplyOutcome apply(long logIndex, BookingCommand cmd) {
        // --- Kiểm tra thứ tự (fail closed) ---
        if (logIndex != lastApplied + 1) {
            throw new IllegalStateException(
                    "apply thứ tự sai: mong đợi logIndex=" + (lastApplied + 1)
                    + " nhưng nhận logIndex=" + logIndex
                    + " (lastApplied=" + lastApplied + ")");
        }

        // --- Dispatch theo loại lệnh ---
        ApplyOutcome outcome = switch (cmd) {
            case BookingCommand.NoOp noOp -> applyNoOp();
            case BookingCommand.BookTicket book -> applyBook(logIndex, book);
            case BookingCommand.CancelTicket cancel -> applyCancel(logIndex, cancel);
        };

        // lastApplied tăng sau mọi entry hợp lệ (kể cả NO_OP và conflict)
        // Lưu ý: applyBook/applyCancel/applyNoOp KHÔNG tự tăng lastApplied.
        // Tăng tại đây để đảm bảo mọi path đều tăng đúng 1 lần.
        lastApplied = logIndex;

        return outcome;
    }

    // -----------------------------------------------------------------------
    // applyNoOp
    // -----------------------------------------------------------------------

    /** NO_OP: chỉ tăng lastApplied (sau khi return, caller sẽ gán lastApplied=logIndex). */
    private ApplyOutcome applyNoOp() {
        // Không tạo vé, không ghi request_results, không tạo sự kiện nghiệp vụ.
        // lastApplied sẽ được tăng bởi apply() sau khi return.
        // Trả NoOpApplied với giá trị sẽ là sau khi tăng — được tính bởi caller.
        // Dùng sentinel -1; caller sẽ thay bằng logIndex thật.
        return new ApplyOutcome.NoOpApplied(lastApplied + 1);
    }

    // -----------------------------------------------------------------------
    // applyBook
    // -----------------------------------------------------------------------

    private ApplyOutcome applyBook(long logIndex, BookingCommand.BookTicket book) {
        String requestId = book.requestId();
        String fingerprint = computeBookFingerprint(book.tripId(), book.seatId(), book.passengerName());

        // --- Kiểm tra requestId đã xử lý chưa ---
        StoredResult stored = requestResults.get(requestId);
        if (stored != null) {
            // Cùng ý định: trả kết quả gốc (replay)
            if (stored.commandType == BookingCommand.Type.BOOK_TICKET
                    && fingerprint.equals(stored.intentFingerprint)) {
                // lastApplied vẫn tăng (trong caller); kết quả gốc không đổi
                return new ApplyOutcome.BusinessResult(stored.originalResult, true);
            }
            // Khác ý định hoặc khác loại lệnh: REQUEST_ID_CONFLICT
            // Không ghi đè kết quả gốc, không đổi ghế/vé
            return new ApplyOutcome.RequestIdConflict(requestId, logIndex);
        }

        // --- Xử lý lần đầu ---
        Seat seat = seats.get(book.seatId());
        // seat sẽ không null vì BookingCommand đã validate seatId hợp lệ
        // và state machine chỉ giữ tripId=SE-001 với 10 ghế cố định.
        if (seat == null) {
            // Không xảy ra với command đã validate, nhưng fail closed
            throw new IllegalStateException(
                    "Ghế không tồn tại trong state: " + book.seatId());
        }

        ApplyResult result;
        if (seat.status == SeatStatus.AVAILABLE) {
            // Ghế trống: tạo vé ACTIVE
            Ticket ticket = new Ticket(
                    book.ticketId(),
                    book.tripId(),
                    book.seatId(),
                    book.passengerName(),
                    book.issuedAt()
            );
            tickets.put(book.ticketId(), ticket);

            // Cập nhật ghế
            seat.status = SeatStatus.BOOKED;
            seat.currentTicketId = book.ticketId();

            result = ApplyResult.bookConfirmed(requestId, book.ticketId(), logIndex);
        } else {
            // Ghế đã BOOKED
            result = ApplyResult.bookRejected(requestId, ApplyResult.SEAT_ALREADY_BOOKED, logIndex);
        }

        // Lưu kết quả (kể cả REJECTED, theo SPEC-05 §3.1 §8)
        requestResults.put(requestId, new StoredResult(
                BookingCommand.Type.BOOK_TICKET, fingerprint, result));

        return new ApplyOutcome.BusinessResult(result, false);
    }

    // -----------------------------------------------------------------------
    // applyCancel
    // -----------------------------------------------------------------------

    private ApplyOutcome applyCancel(long logIndex, BookingCommand.CancelTicket cancel) {
        String requestId = cancel.requestId();
        String fingerprint = computeCancelFingerprint(cancel.ticketId());

        // --- Kiểm tra requestId đã xử lý chưa ---
        StoredResult stored = requestResults.get(requestId);
        if (stored != null) {
            if (stored.commandType == BookingCommand.Type.CANCEL_TICKET
                    && fingerprint.equals(stored.intentFingerprint)) {
                return new ApplyOutcome.BusinessResult(stored.originalResult, true);
            }
            return new ApplyOutcome.RequestIdConflict(requestId, logIndex);
        }

        // --- Xử lý lần đầu ---
        Ticket ticket = tickets.get(cancel.ticketId());
        ApplyResult result;

        if (ticket == null || ticket.status != TicketStatus.ACTIVE) {
            // Vé không tồn tại hoặc không ACTIVE
            result = ApplyResult.cancelRejected(requestId, cancel.ticketId(), logIndex);
        } else {
            // Vé ACTIVE — kiểm tra toàn vẹn: ghế phải khớp currentTicketId
            Seat seat = seats.get(ticket.seatId);
            if (seat == null) {
                throw new IllegalStateException(
                        "Lỗi toàn vẹn: vé ACTIVE nhưng ghế " + ticket.seatId + " không tồn tại");
            }
            if (!cancel.ticketId().equals(seat.currentTicketId)) {
                // Vé ACTIVE nhưng ghế liên kết không khớp — lỗi toàn vẹn dữ liệu
                throw new IllegalStateException(
                        "Lỗi toàn vẹn: vé " + cancel.ticketId()
                        + " ACTIVE nhưng seat.currentTicketId=" + seat.currentTicketId
                        + " không khớp (seatId=" + ticket.seatId + ")");
            }

            // Hủy vé và trả ghế AVAILABLE
            ticket.status = TicketStatus.CANCELLED;
            ticket.cancelledAt = cancel.cancelledAt();
            seat.status = SeatStatus.AVAILABLE;
            seat.currentTicketId = null;

            result = ApplyResult.cancelConfirmed(requestId, cancel.ticketId(), logIndex);
        }

        requestResults.put(requestId, new StoredResult(
                BookingCommand.Type.CANCEL_TICKET, fingerprint, result));

        return new ApplyOutcome.BusinessResult(result, false);
    }

    // -----------------------------------------------------------------------
    // Fingerprint (private, tất định, SHA-256)
    // -----------------------------------------------------------------------

    /**
     * Tính fingerprint cho lệnh BOOK.
     *
     * <p>Tuple: {@code (BOOK_TICKET, tripId, seatId, passengerName)}.
     * Encode mỗi field: uint32 big-endian độ dài byte || UTF-8 bytes, nối theo đúng thứ tự,
     * SHA-256, trả hex chữ thường 64 ký tự.
     *
     * <p>KHÔNG đưa vào: requestId, ticketId (do Leader sinh), issuedAt (do Leader sinh).
     * Mục đích: cho phép nhận diện cùng ý định qua Leader khác, theo SPEC-05 §3.1.
     */
    private static String computeBookFingerprint(String tripId, String seatId, String passengerName) {
        return computeFingerprint(
                BookingCommand.Type.BOOK_TICKET.name(),
                tripId,
                seatId,
                passengerName
        );
    }

    /**
     * Tính fingerprint cho lệnh CANCEL.
     *
     * <p>Tuple: {@code (CANCEL_TICKET, ticketId)}.
     * ticketId là input Client, được đưa vào fingerprint (khác với BOOK).
     */
    private static String computeCancelFingerprint(String ticketId) {
        return computeFingerprint(
                BookingCommand.Type.CANCEL_TICKET.name(),
                ticketId
        );
    }

    /**
     * Encode các field thành: uint32 big-endian byteLength || UTF-8 bytes mỗi field,
     * nối theo thứ tự, tính SHA-256, trả hex 64 ký tự chữ thường.
     */
    private static String computeFingerprint(String... fields) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] utf8 = field.getBytes(StandardCharsets.UTF_8);
                // uint32 big-endian độ dài byte
                byte[] lenBytes = ByteBuffer.allocate(4).putInt(utf8.length).array();
                digest.update(lenBytes);
                digest.update(utf8);
            }
            byte[] hash = digest.digest();
            // Chuyển thành hex 64 ký tự chữ thường
            StringBuilder sb = new StringBuilder(64);
            for (byte b : hash) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 luôn có trong JDK chuẩn — không xảy ra
            throw new IllegalStateException("SHA-256 không khả dụng", e);
        }
    }

    // -----------------------------------------------------------------------
    // Read-only accessors (dùng cho test)
    // -----------------------------------------------------------------------

    /**
     * Trả về {@code lastApplied} hiện tại.
     */
    public long lastApplied() {
        return lastApplied;
    }

    /**
     * Trả về trạng thái ghế. {@code Optional.empty()} nếu seatId không tồn tại.
     * Kết quả là snapshot tại thời điểm gọi — chỉ dùng trong test.
     */
    public Optional<String> seatStatus(String seatId) {
        Seat seat = seats.get(seatId);
        return seat == null ? Optional.empty() : Optional.of(seat.status.name());
    }

    /**
     * Trả về {@code currentTicketId} của ghế; {@code Optional.empty()} nếu ghế không tồn tại.
     */
    public Optional<String> seatCurrentTicketId(String seatId) {
        Seat seat = seats.get(seatId);
        if (seat == null) return Optional.empty();
        return Optional.ofNullable(seat.currentTicketId);
    }

    /**
     * Trả về trạng thái vé. {@code Optional.empty()} nếu ticketId không tồn tại.
     */
    public Optional<String> ticketStatus(String ticketId) {
        Ticket t = tickets.get(ticketId);
        return t == null ? Optional.empty() : Optional.of(t.status.name());
    }

    /**
     * Trả về số vé ACTIVE hiện tại.
     */
    public long activeTicketCount() {
        return tickets.values().stream()
                .filter(t -> t.status == TicketStatus.ACTIVE)
                .count();
    }

    /**
     * Trả về tổng số vé (cả ACTIVE và CANCELLED).
     */
    public int totalTicketCount() {
        return tickets.size();
    }

    /**
     * Trả về kết quả đã lưu theo requestId; {@code Optional.empty()} nếu chưa có.
     */
    public Optional<ApplyResult> storedResult(String requestId) {
        StoredResult sr = requestResults.get(requestId);
        return sr == null ? Optional.empty() : Optional.of(sr.originalResult);
    }

    /**
     * Trả về map ghế không thể sửa (seatId → status string) — dùng cho test tất định.
     */
    public Map<String, String> allSeatStatuses() {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Seat> e : seats.entrySet()) {
            result.put(e.getKey(), e.getValue().status.name());
        }
        return Collections.unmodifiableMap(result);
    }
}
