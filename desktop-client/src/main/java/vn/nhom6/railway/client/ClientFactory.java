package vn.nhom6.railway.client;

import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;

/**
 * Nơi tập trung cấu hình và tiêm phụ thuộc (Dependency Injection) cho Desktop Client (TASK D07/D09).
 * <p>
 * Cho phép đổi nguồn dữ liệu từ {@link MockBookingClient} sang {@link SocketBookingClient} TCP thật
 * hoặc Stub kiểm thử mà không phải sửa FXML hay các Controller.
 * <p>
 * Hỗ trợ chọn implementation theo tham số khởi chạy {@code -Dbooking.client=socket|mock} (mặc định là {@code mock}).
 */
public final class ClientFactory {

    private static volatile BookingClient bookingClient = createDefaultClient();
    private static volatile BookingViewModel bookingViewModel = new BookingViewModel(bookingClient);

    private ClientFactory() {}

    /**
     * Khởi tạo BookingClient mặc định theo cấu hình hệ thống:
     * -Dbooking.client=socket -> SocketBookingClient
     * Mặc định hoặc -Dbooking.client=mock -> MockBookingClient.getInstance()
     */
    public static BookingClient createDefaultClient() {
        String clientMode = System.getProperty("booking.client", "mock").trim().toLowerCase();
        if ("socket".equals(clientMode) || "tcp".equals(clientMode)) {
            return new SocketBookingClient();
        }
        return MockBookingClient.getInstance();
    }

    /**
     * Lấy BookingClient hiện tại đang hoạt động.
     */
    public static BookingClient getBookingClient() {
        return bookingClient;
    }

    /**
     * Đổi implementation của BookingClient (chỉ cần gọi ở nơi khởi tạo hoặc test runner).
     * Tuyệt đối không cần sửa FXML hay Controller.
     */
    public static synchronized void setBookingClient(BookingClient client) {
        bookingClient = (client != null) ? client : MockBookingClient.getInstance();
        bookingViewModel = new BookingViewModel(bookingClient);
    }

    /**
     * Thiết lập lại client theo cấu hình hệ thống hiện tại.
     */
    public static synchronized void resetToConfiguredClient() {
        setBookingClient(createDefaultClient());
    }

    /**
     * Lấy BookingViewModel dùng chung kết nối với BookingClient hiện hành.
     */
    public static BookingViewModel getBookingViewModel() {
        return bookingViewModel;
    }
}
