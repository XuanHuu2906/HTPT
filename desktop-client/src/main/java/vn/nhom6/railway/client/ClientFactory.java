package vn.nhom6.railway.client;

import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;

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

    private static volatile PersistentPendingJournal journal = createDefaultJournal();
    private static volatile ClusterStatusClient clusterStatusClient = createDefaultClusterStatusClient();
    private static volatile ClusterConnectionViewModel clusterConnectionViewModel = new ClusterConnectionViewModel(clusterStatusClient);

    private static volatile BookingClient bookingClient = createDefaultBookingClient();
    private static volatile BookingViewModel bookingViewModel = new BookingViewModel(bookingClient);
    private static volatile PendingRequestsViewModel pendingRequestsViewModel = new PendingRequestsViewModel(getJournalingBookingClient());

    private ClientFactory() {}

    /**
     * Lấy PersistentPendingJournal dùng chung cho ứng dụng.
     */
    public static synchronized PersistentPendingJournal getJournal() {
        if (journal == null || journal.isClosed()) {
            journal = createDefaultJournal();
        }
        return journal;
    }

    /**
     * Đặt Journal tùy biến (ví dụ trong unit test dùng thư mục tạm).
     */
    public static synchronized void setJournal(PersistentPendingJournal customJournal) {
        if (journal != null && !journal.isClosed()) {
            try {
                journal.close();
            } catch (Exception ignored) {}
        }
        journal = customJournal;
        setBookingClient(bookingClient);
    }

    private static PersistentPendingJournal createDefaultJournal() {
        try {
            return PersistentPendingJournal.createDefault();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Khởi tạo BookingClient mặc định theo cấu hình hệ thống:
     * -Dbooking.client=socket -> LeaderAwareBookingClient bọc SocketBookingClient
     * Mặc định hoặc -Dbooking.client=mock -> MockBookingClient.getInstance()
     * Tự động được bọc bằng JournalingBookingClient (TASK D14).
     */
    public static BookingClient createDefaultClient() {
        return createDefaultBookingClient();
    }

    public static BookingClient createDefaultBookingClient() {
        String clientMode = System.getProperty("booking.client", "mock").trim().toLowerCase();
        BookingClient rawClient;
        if ("socket".equals(clientMode) || "tcp".equals(clientMode) || "real".equals(clientMode)) {
            ClusterEndpoints endpoints = ClusterEndpoints.loadDefault();
            RedirectPolicy policy = RedirectPolicy.defaultPolicy();
            rawClient = new LeaderAwareBookingClient(endpoints, null, policy, clusterConnectionViewModel, null);
        } else {
            rawClient = MockBookingClient.getInstance();
        }

        PersistentPendingJournal currentJournal = getJournal();
        if (currentJournal != null) {
            return new JournalingBookingClient(rawClient, currentJournal);
        }
        return rawClient;
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
     * Lấy JournalingBookingClient nếu đang sử dụng decorator.
     */
    public static JournalingBookingClient getJournalingBookingClient() {
        if (bookingClient instanceof JournalingBookingClient jbc) {
            return jbc;
        }
        PersistentPendingJournal j = getJournal();
        if (j != null) {
            return new JournalingBookingClient(bookingClient, j);
        }
        return null;
    }

    /**
     * Đổi implementation của BookingClient (chỉ cần gọi ở nơi khởi tạo hoặc test runner).
     * Tự động bọc bằng JournalingBookingClient nếu chưa có.
     */
    public static synchronized void setBookingClient(BookingClient client) {
        BookingClient raw = (client != null) ? client : MockBookingClient.getInstance();
        PersistentPendingJournal currentJournal = getJournal();
        if (currentJournal != null && !(raw instanceof JournalingBookingClient)) {
            bookingClient = new JournalingBookingClient(raw, currentJournal);
        } else {
            bookingClient = raw;
        }
        bookingViewModel = new BookingViewModel(bookingClient);
        pendingRequestsViewModel = new PendingRequestsViewModel(getJournalingBookingClient());
    }

    /**
     * Lấy PendingRequestsViewModel dùng chung cho ứng dụng (TASK D14).
     */
    public static synchronized PendingRequestsViewModel getPendingRequestsViewModel() {
        if (pendingRequestsViewModel == null) {
            pendingRequestsViewModel = new PendingRequestsViewModel(getJournalingBookingClient());
        }
        return pendingRequestsViewModel;
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
