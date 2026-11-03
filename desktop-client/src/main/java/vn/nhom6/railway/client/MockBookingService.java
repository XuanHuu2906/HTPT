package vn.nhom6.railway.client;

import java.util.List;

/**
 * Giao diện dịch vụ đặt vé (Service Layer).
 * <p>
 * Định nghĩa hợp đồng trừu tượng để tách rời tầng Controller khỏi việc triển khai cụ thể
 * (Mock ở giai đoạn D03, sau này thay bằng Client TCP/Backend mà không cần sửa Controller).
 */
public interface MockBookingService {

    /**
     * Gửi yêu cầu đặt vé với họ tên hành khách đã được chuẩn hóa và danh sách ghế.
     *
     * @param normalizedPassengerName họ tên hành khách đã chuẩn hóa
     * @param seatIds                 danh sách mã ghế cần đặt
     * @return kết quả đặt vé {@link BookingOutcome}
     */
    BookingOutcome submit(String normalizedPassengerName, List<String> seatIds);
}

