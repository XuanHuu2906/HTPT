package vn.nhom6.railway.booking;

import java.time.Instant;
import java.util.Objects;

/**
 * SPEC-03 §4.3 / SPEC-05 §2 — CancelBookingCommand
 *
 * <p>Lệnh hủy vé theo chuẩn giao thức TCP và state machine của dự án.
 * Lệnh này chạy qua Raft log (được Leader gán thời điểm {@code cancelledAt}) và được apply
 * nguyên tử vào database/state machine tại mỗi node.
 *
 * <p>Bao gồm 3 trường bắt buộc:
 * <ul>
 *   <li>{@code requestId}: UUID chữ thường 8-4-4-4-12 sinh một lần từ phía Client.
 *   <li>{@code ticketId}: Mã định danh vé đích cần hủy, định dạng [A-Za-z0-9-]{1,64}.
 *   <li>{@code cancelledAt}: Thời điểm hủy do Leader gán trước khi replicate qua Raft log,
 *       chính xác đến millisecond.
 * </ul>
 *
 * <p>Tương thích và chuyển đổi trực tiếp sang {@link BookingCommand.CancelTicket} dùng trong
 * đường apply Raft của D23 (BookingStateMachine, JdbcBookingRepository, BookingRaftBridge,
 * RaftApplyAdapter, JdbcCommittedCommandApplier).
 */
public record CancelBookingCommand(
        String requestId,
        String ticketId,
        Instant cancelledAt
) {
    /**
     * Compact constructor thực hiện validate nghiêm ngặt theo SPEC-03 §3.0 / §4.3 và SPEC-05 §2.
     * Ném {@link IllegalArgumentException} nếu bất kỳ trường nào không hợp lệ.
     */
    public CancelBookingCommand {
        BookingCommand.requireValidRequestId(requestId);
        BookingCommand.requireValidTicketId(ticketId);
        BookingCommand.requireMillisInstant(cancelledAt, "cancelledAt");
    }

    /**
     * Loại lệnh nghiệp vụ tương ứng trong Raft log (SPEC-03 §6.1).
     */
    public BookingCommand.Type type() {
        return BookingCommand.Type.CANCEL_TICKET;
    }

    /**
     * Chuyển đổi sang {@link BookingCommand.CancelTicket} để apply qua Raft log và state machine.
     */
    public BookingCommand.CancelTicket toBookingCommand() {
        return new BookingCommand.CancelTicket(requestId, ticketId, cancelledAt);
    }

    /**
     * Tạo {@link CancelBookingCommand} từ {@link BookingCommand.CancelTicket}.
     */
    public static CancelBookingCommand from(BookingCommand.CancelTicket command) {
        Objects.requireNonNull(command, "command không được null");
        return new CancelBookingCommand(command.requestId(), command.ticketId(), command.cancelledAt());
    }
}

