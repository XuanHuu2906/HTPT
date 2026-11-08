package vn.nhom6.railway.client.network;

import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.util.concurrent.CompletableFuture;

/**
 * Interface giao tiếp với hệ thống bán vé theo đặc tả SPEC-03.
 * <p>
 * Mọi phương thức trả về {@link CompletableFuture} bất đồng bộ, không chặn UI thread,
 * không ném checked exception ra ngoài.
 * Interface hoàn toàn tách biệt khỏi JavaFX và JDBC.
 */
public interface BookingClient {

    /**
     * Lấy sơ đồ trạng thái ghế an toàn từ cụm máy chủ (SPEC-03 §4.1 - GET_SEATS).
     *
     * @param request yêu cầu chứa tripId (mặc định "SE-001")
     * @return {@link CompletableFuture} chứa danh sách 10 ghế và trạng thái (AVAILABLE / BOOKED)
     */
    CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request);

    /**
     * Gửi yêu cầu đặt vé tàu (SPEC-03 §4.2 - BOOK_TICKET).
     *
     * @param request thông tin đặt vé (requestId, tripId, seatId, passengerName)
     * @return {@link CompletableFuture} chứa kết quả đặt vé (CONFIRMED, REJECTED, UNKNOWN...)
     */
    CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request);

    /**
     * Gửi yêu cầu hủy vé đã đặt (SPEC-03 §4.3 - CANCEL_TICKET).
     *
     * @param request thông tin hủy vé (requestId, ticketId)
     * @return {@link CompletableFuture} chứa kết quả hủy vé (CONFIRMED, REJECTED, UNKNOWN...)
     */
    CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request);

    /**
     * Tra cứu kết quả của một yêu cầu trước đó khi gặp timeout/UNKNOWN (SPEC-03 §4.4 - GET_REQUEST_RESULT).
     *
     * @param requestId mã định danh duy nhất của yêu cầu nghiệp vụ
     * @return {@link CompletableFuture} chứa trạng thái cuối cùng (APPLIED_SUCCESS, APPLIED_REJECTED, PENDING, UNKNOWN)
     */
    CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId);
}
