package vn.nhom6.railway.client;

import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterNodeSelector;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.ReadAdapter;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import javafx.collections.ObservableList;

/**
 * Nơi tập trung cấu hình và tiêm phụ thuộc (Dependency Injection) cho Desktop
 * Client (TASK D07/D09/D11/D12).
 * <p>
 * Cho phép đổi nguồn dữ liệu từ Mock sang TCP Socket thật (hoặc Fake Server
 * kiểm thử)
 * mà không phải sửa FXML hay các Controller.
 * <p>
 * Hỗ trợ chọn implementation theo tham số khởi chạy:
 * <ul>
 * <li>{@code -Dbooking.client=socket|mock} (mặc định là {@code mock}).</li>
 * <li>{@code -Dcluster.client=socket|mock} (nếu không đặt, kế thừa từ
 * {@code booking.client}).</li>
 * </ul>
 */
public final class ClientFactory {

    private static volatile PersistentPendingJournal journal = createDefaultJournal();
    private static volatile ClusterNodeSelector clusterNodeSelector = createDefaultClusterNodeSelector();
    private static volatile ClusterStatusClient clusterStatusClient = createDefaultClusterStatusClient();
    private static volatile ClusterConnectionViewModel clusterConnectionViewModel = createInitialClusterConnectionViewModel();

    private static ClusterConnectionViewModel createInitialClusterConnectionViewModel() {
        ClusterConnectionViewModel vm = new ClusterConnectionViewModel(clusterStatusClient);
        try {
            ClientConfig cfg = ClientConfig.loadDefault();
            if (cfg.isSocketMode() && clusterNodeSelector != null) {
                NodeEndpoint ep = clusterNodeSelector.selectNode(true);
                vm.updateConnectedNode(ep.nodeId(), ep.toAddressString());
            }
        } catch (Exception ignored) {}
        return vm;
    }

    private static volatile BookingClient rawBookingClient = createDefaultBookingClient();
    private static volatile JournalingBookingClient journalingBookingClient = wrapWithJournal(rawBookingClient);
    private static volatile BookingViewModel bookingViewModel = new BookingViewModel(journalingBookingClient);
    private static volatile PendingRequestsViewModel pendingRequestsViewModel = new PendingRequestsViewModel(
            journalingBookingClient);
    private static volatile ReadAdapter readAdapter = createDefaultReadAdapter();
    private static volatile SeatsRefreshService seatsRefreshService = null;

    private ClientFactory() {
    }

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
            } catch (Exception ignored) {
            }
        }
        journal = customJournal;
        journalingBookingClient = wrapWithJournal(rawBookingClient);
        bookingViewModel = new BookingViewModel(journalingBookingClient);
        pendingRequestsViewModel = new PendingRequestsViewModel(journalingBookingClient);
    }

    private static PersistentPendingJournal createDefaultJournal() {
        try {
            return PersistentPendingJournal.createDefault();
        } catch (Exception e) {
            System.err.println("[ClientFactory] Không thể lấy khóa journal mặc định: " + e.getMessage());
            try {
                java.nio.file.Path fallbackDir = java.nio.file.Files.createTempDirectory("railway-journal-fallback-");
                System.err.println("[ClientFactory] Sử dụng thư mục journal tạm thời: " + fallbackDir.toAbsolutePath());
                return new PersistentPendingJournal(fallbackDir);
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static JournalingBookingClient wrapWithJournal(BookingClient client) {
        if (client instanceof JournalingBookingClient jbc) {
            return jbc;
        }
        PersistentPendingJournal j = getJournal();
        if (j != null) {
            return new JournalingBookingClient(client != null ? client : MockBookingClient.getInstance(), j);
        }
        PersistentPendingJournal fallback = createDefaultJournal();
        return new JournalingBookingClient(client != null ? client : MockBookingClient.getInstance(), fallback);
    }

    public static synchronized ClusterNodeSelector getClusterNodeSelector() {
        if (clusterNodeSelector == null) {
            clusterNodeSelector = createDefaultClusterNodeSelector();
        }
        return clusterNodeSelector;
    }

    public static synchronized void setClusterNodeSelector(ClusterNodeSelector selector) {
        clusterNodeSelector = selector;
    }

    private static ClusterNodeSelector createDefaultClusterNodeSelector() {
        try {
            ClientConfig config = ClientConfig.loadDefault();
            return new ClusterNodeSelector(config);
        } catch (Exception e) {
            System.err.println("[ClientFactory] Không thể nạp ClientConfig mặc định: " + e.getMessage());
            return null;
        }
    }

    /**
     * Khởi tạo BookingClient mặc định theo cấu hình hệ thống:
     * -Dbooking.client=socket -> LeaderAwareBookingClient bọc SocketBookingClient
     * Mặc định hoặc -Dbooking.client=mock -> MockBookingClient.getInstance()
     */
    public static BookingClient createDefaultClient() {
        return createDefaultBookingClient();
    }

    public static BookingClient createDefaultBookingClient() {
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        String clientMode = config.getClientMode();
        if ("socket".equals(clientMode) || "tcp".equals(clientMode) || "real".equals(clientMode)) {
            if (selector == null) {
                selector = new ClusterNodeSelector(config);
                setClusterNodeSelector(selector);
            }
            ClusterEndpoints endpoints = ClusterEndpoints.fromSelector(selector);
            RedirectPolicy policy = new RedirectPolicy(
                    config.getMaxRedirects(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs()
            );

            // Cập nhật ngay Node ban đầu vào ClusterConnectionViewModel
            try {
                NodeEndpoint initialEp = selector.selectNode(true);
                if (clusterConnectionViewModel != null) {
                    clusterConnectionViewModel.updateConnectedNode(initialEp.nodeId(), initialEp.toAddressString());
                }
            } catch (Exception ignored) {}

            return new LeaderAwareBookingClient(
                    endpoints,
                    null,
                    policy,
                    clusterConnectionViewModel,
                    config.getPreferredNode(),
                    selector
            );
        }
        return MockBookingClient.getInstance();
    }

    public static ClusterStatusClient createDefaultClusterStatusClient() {
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        String mode = System.getProperty("cluster.client");
        if (mode == null || mode.isBlank()) {
            mode = config.getClientMode();
        }
        mode = mode.trim().toLowerCase();
        if ("socket".equals(mode) || "tcp".equals(mode) || "real".equals(mode)) {
            NodeEndpoint ep = null;
            if (selector != null) {
                try {
                    ep = selector.selectNode(true);
                } catch (Exception ignored) {}
            }
            if (ep == null) {
                ep = config.getEndpoints().isEmpty()
                        ? new NodeEndpoint("node-1", "127.0.0.1", 9101)
                        : config.getEndpoints().get(0);
            }
            return new SocketClusterStatusClient(
                    ep.host(),
                    ep.port(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs(),
                    "client-status-monitor"
            );
        }
        return MockClusterStatusClient.getInstance();
    }

    /**
     * Lấy BookingClient hiện tại đang hoạt động (raw client: LeaderAware hoặc
     * Mock).
     */
    public static BookingClient getBookingClient() {
        return rawBookingClient;
    }

    /**
     * Lấy JournalingBookingClient (decorator bọc journal bền vững - TASK D14).
     */
    public static JournalingBookingClient getJournalingBookingClient() {
        if (journalingBookingClient == null) {
            journalingBookingClient = wrapWithJournal(rawBookingClient);
        }
        return journalingBookingClient;
    }

    /**
     * Đổi implementation của BookingClient (chỉ cần gọi ở nơi khởi tạo hoặc test
     * runner).
     * Tự động bọc vào JournalingBookingClient để tiêm vào BookingViewModel và
     * PendingRequestsViewModel.
     */
    public static synchronized void setBookingClient(BookingClient client) {
        rawBookingClient = (client != null) ? client : MockBookingClient.getInstance();
        journalingBookingClient = wrapWithJournal(rawBookingClient);
        bookingViewModel = new BookingViewModel(journalingBookingClient);
        pendingRequestsViewModel = new PendingRequestsViewModel(journalingBookingClient);
    }

    /**
     * Lấy ClusterStatusClient hiện tại đang hoạt động.
     */
    public static ClusterStatusClient getClusterStatusClient() {
        return clusterStatusClient;
    }

    /**
     * Đổi implementation của ClusterStatusClient (dùng trong test hoặc cấu hình
     * lại).
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
        clusterNodeSelector = createDefaultClusterNodeSelector();
        setBookingClient(createDefaultBookingClient());
        setClusterStatusClient(createDefaultClusterStatusClient());
    }

    /**
     * Lấy BookingViewModel dùng chung kết nối với BookingClient hiện hành (được bảo
     * vệ bởi journal).
     */
    public static BookingViewModel getBookingViewModel() {
        return bookingViewModel;
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
     * Tạo ReadAdapter mặc định theo cấu hình hệ thống (TASK D15).
     * <p>
     * -Dread.adapter=fake -> FakeReadAdapter
     * -Dbooking.client=socket -> BookingClientReadAdapter
     * Mặc định: FakeReadAdapter phục vụ kiểm thử an toàn
     */
    public static ReadAdapter createDefaultReadAdapter() {
        String readMode = System.getProperty("read.adapter", "").trim().toLowerCase();
        if ("fake".equals(readMode)) {
            return new FakeReadAdapter();
        }
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        String clientMode = config.getClientMode();
        if ("socket".equals(clientMode) || "tcp".equals(clientMode) || "real".equals(clientMode)) {
            return new BookingClientReadAdapter(getBookingClient());
        }
        return new FakeReadAdapter();
    }

    /**
     * Lấy ReadAdapter hiện tại đang dùng.
     */
    public static synchronized ReadAdapter getReadAdapter() {
        if (readAdapter == null) {
            readAdapter = createDefaultReadAdapter();
        }
        return readAdapter;
    }

    /**
     * Đổi ReadAdapter (dùng trong kiểm thử hoặc khi đổi cấu hình).
     */
    public static synchronized void setReadAdapter(ReadAdapter adapter) {
        if (readAdapter != null && readAdapter != adapter) {
            try {
                readAdapter.close();
            } catch (Exception ignored) {
            }
        }
        readAdapter = (adapter != null) ? adapter : createDefaultReadAdapter();
        if (seatsRefreshService != null) {
            seatsRefreshService.close();
            seatsRefreshService = null;
        }
    }

    /**
     * Lấy SeatsRefreshService gắn với danh sách ghế ObservableList (TASK D15).
     */
    public static synchronized SeatsRefreshService getSeatsRefreshService(ObservableList<SeatViewModel> seatsList) {
        if (seatsRefreshService == null || seatsRefreshService.getSeatsList() != seatsList) {
            if (seatsRefreshService != null) {
                seatsRefreshService.close();
            }
            seatsRefreshService = new SeatsRefreshService(getReadAdapter(), seatsList, getJournal());
        }
        return seatsRefreshService;
    }

    /**
     * Lấy SeatsRefreshService hiện tại (nếu đã khởi tạo).
     */
    public static synchronized SeatsRefreshService getSeatsRefreshService() {
        return seatsRefreshService;
    }

    /**
     * Đặt SeatsRefreshService tùy biến (phục vụ Unit Test).
     */
    public static synchronized void setSeatsRefreshService(SeatsRefreshService service) {
        if (seatsRefreshService != null && seatsRefreshService != service) {
            seatsRefreshService.close();
        }
        seatsRefreshService = service;
    }
}
