package vn.nhom6.railway.client;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Cài đặt giả lập dịch vụ đặt vé (MOCK) cho giai đoạn D03.
 * <p>
 * Tính năng:
 * - Giả lập độ trễ mạng từ 500ms - 800ms.
 * - Quy tắc demo có thể kiểm soát:
 *   + Tên chứa "reject" (không phân biệt hoa thường) -> REJECTED (reasonCode = SEAT_ALREADY_BOOKED).
 *   + Tên chứa "unknown" (không phân biệt hoa thường) -> UNKNOWN.
 *   + Các trường hợp còn lại -> CONFIRMED kèm mã tham chiếu mock dạng MOCK-xxxxxx.
 * - Cờ cấu hình ngẫu nhiên qua system property "railway.mock.random" (mặc định tắt).
 */
public class MockBookingServiceImpl implements MockBookingService {

    /**
     * Tên system property để kích hoạt chế độ kết quả ngẫu nhiên (mặc định: false).
     */
    public static final String RANDOM_MODE_PROPERTY = "railway.mock.random";

    private final boolean simulatedDelayEnabled;
    private final Random random = new Random();

    public MockBookingServiceImpl() {
        this(true);
    }

    public MockBookingServiceImpl(boolean simulatedDelayEnabled) {
        this.simulatedDelayEnabled = simulatedDelayEnabled;
    }

    @Override
    public BookingOutcome submit(String normalizedPassengerName, List<String> seatIds) {
        if (simulatedDelayEnabled) {
            // Giả lập trễ mạng từ 500ms đến 800ms
            int delayMs = 500 + random.nextInt(301);
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return BookingOutcome.unknown("Yêu cầu bị gián đoạn: " + e.getMessage());
            }
        }

        // Kiểm tra chế độ ngẫu nhiên nếu được cấu hình bật
        boolean isRandomMode = Boolean.getBoolean(RANDOM_MODE_PROPERTY);
        if (isRandomMode) {
            int dice = random.nextInt(100);
            if (dice < 65) {
                return createConfirmedOutcome();
            } else if (dice < 85) {
                return createRejectedOutcome();
            } else {
                return createUnknownOutcome();
            }
        }

        // Quy tắc kiểm soát qua tên hành khách (Demo)
        String lowerName = (normalizedPassengerName == null) ? "" : normalizedPassengerName.toLowerCase();

        if (lowerName.contains("reject")) {
            return createRejectedOutcome();
        } else if (lowerName.contains("unknown")) {
            return createUnknownOutcome();
        } else {
            return createConfirmedOutcome();
        }
    }

    private BookingOutcome createConfirmedOutcome() {
        String mockRef = "MOCK-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        return BookingOutcome.confirmed(mockRef, "Đặt vé thành công");
    }

    private BookingOutcome createRejectedOutcome() {
        return BookingOutcome.rejected(
                BookingOutcome.SEAT_ALREADY_BOOKED,
                "Ghế đã có người đặt, vui lòng chọn ghế khác"
        );
    }

    private BookingOutcome createUnknownOutcome() {
        return BookingOutcome.unknown(
                "Chưa xác định được kết quả, vui lòng kiểm tra lại trước khi đặt lại"
        );
    }
}

