package vn.nhom6.railway.client.network;

import java.io.IOException;

/**
 * Ngoại lệ ném ra khi kết nối tới máy chủ thất bại TRƯỚC KHI gửi bất kỳ byte nào
 * theo SPEC-03 Section 7 và TASK TV1-D19.
 * <p>
 * Đảm bảo phân biệt rạch ròi giữa:
 * <ul>
 *   <li>FAILED_NOT_SENT: Lệnh chưa rời máy khách ➔ ghế không đổi, cho thử lại cùng requestId.</li>
 *   <li>UNKNOWN: Lệnh đã bắt đầu gửi hoặc đã gửi xong rồi mất kết nối/timeout ➔ ghế sang UNKNOWN, giữ pending.</li>
 * </ul>
 */
public class FailedNotSentException extends IOException {

    public FailedNotSentException(String message) {
        super(message);
    }

    public FailedNotSentException(String message, Throwable cause) {
        super(message, cause);
    }
}
