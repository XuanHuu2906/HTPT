package vn.nhom6.railway.client;

import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;

/**
 * Nơi tập trung cấu hình và tiêm phụ thuộc (Dependency Injection) cho Desktop Client (TASK D07/D09/D11/D12).
 * <p>
 * Cho phép đổi nguồn dữ liệu từ Mock sang TCP Socket thật (hoặc Fake Server kiểm thử)
 * mà không phải sửa FXML hay các Controller.
 * <p>
 * Hỗ trợ chọn implementation theo tham số khởi chạy:
 * <ul>
 *   <li>{@code -Dbooking.client=socket|mock} (mặc định là {@code mock}).</li>
 *   <li>{@code -Dcluster.client=socket|mock} (nếu không đặt, kế thừa từ {@code booking.client}).</li>
 * </ul>
 */
public final class ClientFactory {

    private static volatile ClusterStatusClient clusterStatusClient = createDefaultClusterStatusClient();
    private static volatile ClusterConnectionViewModel clusterConnectionViewModel = new ClusterConnectionViewModel(clusterStatusClient);

    private static volatile BookingClient bookingClient = createDefaultBookingClient();
    private static volatile BookingViewModel bookingViewModel = new BookingViewModel(bookingClient);

    private ClientFactory() {}

    /**
     * Khởi tạo BookingClient mặc định theo cấu hình hệ thống:
     * -Dbooking.client=socket -> LeaderAwareBookingClient bọc SocketBookingClient
     * Mặc định hoặc -Dbooking.client=mock -> MockBookingClient.getInstance()
     */
    public static BookingClient createDefaultClient() {
        return createDefaultBookingClient();
    }

    public static BookingClient createDefaultBookingClient() {
        String clientMode = System.getProperty("booking.client", "mock").trim().toLowerCase();
        if ("socket".equals(clientMode) || "tcp".equals(clientMode) || "real".equals(clientMode)) {
            ClusterEndpoints endpoints = ClusterEndpoints.loadDefault();
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            return new LeaderAwareBookingClient(endpoints, null, policy, clusterConnectionViewModel, null);
        }
        return MockBookingClient.getInstance();
    }

    public static ClusterStatusClient createDefaultClusterStatusClient() {
        String mode = System.getProperty("cluster.client");
        if (mode == null || mode.isBlank()) {
            mode = System.getProperty("booking.client", "mock");
        }
        mode = mode.trim().toLowerCase();
        if ("socket".equals(mode) || "tcp".equals(mode) || "real".equals(mode)) {
            return new SocketClusterStatusClient();
        }
        return MockClusterStatusClient.getInstance();
    }

    /**
     * Lấy BookingClient hiện tại đang hoạt động.
     */
    public static BookingClient getBookingClient() {
        return bookingClient;
    }

    /**
     * Đổi implementation của BookingClient (chỉ cần gọi ở nơi khởi tạo hoặc test runner).
     */
    public static synchronized void setBookingClient(BookingClient client) {
        bookingClient = (client != null) ? client : MockBookingClient.getInstance();
        bookingViewModel = new BookingViewModel(bookingClient);
    }

    /**
     * Lấy ClusterStatusClient hiện tại đang hoạt động.
     */
    public static ClusterStatusClient getClusterStatusClient() {
        return clusterStatusClient;
    }

    /**
     * Đổi implementation của ClusterStatusClient (dùng trong test hoặc cấu hình lại).
     */
    public static synchronized void setClusterStatusClient(ClusterStatusClient client) {
        if (clusterConnectionViewModel != null) {
            clusterConnectionViewModel.stopPolling();
        }
        clusterStatusClient = (client != null) ? client : MockClusterStatusClient.getInstance();
        clusterConnectionViewModel = new ClusterConnectionViewModel(clusterStatusClient);
    }

    /**
     * Lấy ClusterConnectionViewModel dùng chung cho toàn bộ ứng dụng.
     */
    public static ClusterConnectionViewModel getClusterConnectionViewModel() {
        return clusterConnectionViewModel;
    }

    /**
     * Thiết lập lại toàn bộ client và viewModel theo cấu hình hệ thống hiện tại.
     */
    public static synchronized void resetToConfiguredClient() {
        setBookingClient(createDefaultBookingClient());
        setClusterStatusClient(createDefaultClusterStatusClient());
    }

    /**
     * Lấy BookingViewModel dùng chung kết nối với BookingClient hiện hành.
     */
    public static BookingViewModel getBookingViewModel() {
        return bookingViewModel;
    }
}
