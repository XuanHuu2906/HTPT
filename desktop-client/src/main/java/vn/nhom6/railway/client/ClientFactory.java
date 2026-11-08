package vn.nhom6.railway.client;

import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;

/**
 * Nơi tập trung cấu hình và tiêm phụ thuộc (Dependency Injection) cho Desktop Client (TASK D07).
 * <p>
 * Cho phép đổi nguồn dữ liệu từ {@link MockBookingClient} sang TCP Client hoặc Stub kiểm thử
 * mà không phải sửa FXML hay các Controller.
 */
public final class ClientFactory {

    private static volatile BookingClient bookingClient = MockBookingClient.getInstance();
    private static volatile BookingViewModel bookingViewModel = new BookingViewModel(bookingClient);

    private ClientFactory() {}

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
     * Lấy BookingViewModel dùng chung kết nối với BookingClient hiện hành.
     */
    public static BookingViewModel getBookingViewModel() {
        return bookingViewModel;
    }
}
