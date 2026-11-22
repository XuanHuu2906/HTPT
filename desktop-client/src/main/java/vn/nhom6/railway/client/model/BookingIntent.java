package vn.nhom6.railway.client.model;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import java.util.Objects;

/**
 * Mô hình đại diện cho "ý định nghiệp vụ" (Business Intent) theo SPEC-05 §3.1 (TASK TV1-D22).
 * <p>
 * Bất biến an toàn phân tán & Idempotency:
 * <ul>
 *   <li>Tuple BOOK: {@code (commandType="BOOK_TICKET", tripId, seatId, passengerName)}.</li>
 *   <li>Tuple CANCEL: {@code (commandType="CANCEL_TICKET", ticketId)}.</li>
 *   <li>Các trường như {@code requestId}, {@code messageId}, {@code timestamp}, {@code ticketId} (đối với BOOK)
 *       KHÔNG thuộc ý định đặt vé vì do Leader hoặc giao thức sinh.</li>
 *   <li>Cùng ý định: tái sử dụng {@code requestId} và payload nguyên văn từ Journal.</li>
 *   <li>Đổi ý định (khác ghế, khác tên, khác chuyến): sinh {@code requestId} mới.</li>
 * </ul>
 */
public record BookingIntent(
        String commandType,
        String tripId,
        String seatId,
        String passengerName,
        String ticketId
) {
    public static final String CMD_BOOK = "BOOK_TICKET";
    public static final String CMD_CANCEL = "CANCEL_TICKET";

    public BookingIntent {
        Objects.requireNonNull(commandType, "commandType không được null");
        commandType = commandType.trim().toUpperCase();
        tripId = (tripId != null) ? tripId.trim() : "SE-001";
        seatId = (seatId != null) ? seatId.trim() : "";
        passengerName = (passengerName != null) ? passengerName.trim() : "";
        ticketId = (ticketId != null) ? ticketId.trim() : "";
    }

    public static BookingIntent forBook(String tripId, String seatId, String passengerName) {
        return new BookingIntent(CMD_BOOK, tripId, seatId, passengerName, null);
    }

    public static BookingIntent forCancel(String ticketId) {
        return new BookingIntent(CMD_CANCEL, null, null, null, ticketId);
    }

    public static BookingIntent fromBookRequest(BookTicketRequestDto request) {
        Objects.requireNonNull(request, "request không được null");
        return forBook(request.getTripId(), request.getSeatId(), request.getPassengerName());
    }

    public static BookingIntent fromJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode node = MessageCodec.getMapper().readTree(json);
            String cmd = node.hasNonNull("commandType") ? node.get("commandType").asText() : CMD_BOOK;
            String trip = node.hasNonNull("tripId") ? node.get("tripId").asText() : "SE-001";
            String seat = node.hasNonNull("seatId") ? node.get("seatId").asText() : "";
            String name = node.hasNonNull("passengerName") ? node.get("passengerName").asText() : "";
            String ticket = node.hasNonNull("ticketId") ? node.get("ticketId").asText() : "";
            return new BookingIntent(cmd, trip, seat, name, ticket);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * So sánh xem hai ý định có hoàn toàn trùng khớp không theo cấu trúc nghiệp vụ (SPEC-05 §3.1).
     */
    public boolean isSameIntent(BookingIntent other) {
        if (other == null) {
            return false;
        }
        if (!Objects.equals(this.commandType, other.commandType)) {
            return false;
        }

        if (CMD_BOOK.equals(commandType)) {
            return Objects.equals(this.tripId, other.tripId)
                    && Objects.equals(this.seatId, other.seatId)
                    && Objects.equals(this.passengerName, other.passengerName);
        } else if (CMD_CANCEL.equals(commandType)) {
            return Objects.equals(this.ticketId, other.ticketId);
        }
        return false;
    }

    /**
     * Kiểm tra xem có cùng ghế trong cùng chuyến tàu không (dù khác tên hành khách).
     */
    public boolean isSameSeat(BookingIntent other) {
        if (other == null) {
            return false;
        }
        return CMD_BOOK.equals(commandType)
                && CMD_BOOK.equals(other.commandType)
                && Objects.equals(this.tripId, other.tripId)
                && Objects.equals(this.seatId, other.seatId);
    }

    public String toIntentString() {
        if (CMD_BOOK.equals(commandType)) {
            return String.format("BOOK[chuyến=%s, ghế=%s, khách=%s]", tripId, seatId, passengerName);
        } else {
            return String.format("CANCEL[vé=%s]", ticketId);
        }
    }
}
