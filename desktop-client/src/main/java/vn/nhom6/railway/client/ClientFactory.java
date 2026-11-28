package vn.nhom6.railway.client;

import javafx.collections.ObservableList;
import vn.nhom6.railway.client.controller.ReconnectController;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterNodeSelector;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.DevSupportBridge;
import vn.nhom6.railway.client.network.JournalingBookingClient;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.ReadAdapter;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.network.SocketClusterStatusClient;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.TicketLookupViewModel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

/**
 * Trung tâm khởi tạo và tiêm phụ thuộc (Dependency Injection) cho Desktop Client (TASK D07..D28).
 * <p>
 * Bất biến chế độ RELEASE ({@code release.mode=true} — TASK TV1-D28):
 * <ul>
 *   <li>Luồng release CHỈ khởi tạo {@link SocketBookingClient} bọc bởi
 *       {@link LeaderAwareBookingClient} và {@link JournalingBookingClient},
 *       cùng {@link BookingClientReadAdapter} và {@link SocketClusterStatusClient}.</li>
 *   <li>Mọi yêu cầu cấu hình hoặc tiêm đối tượng {@code Mock*}/{@code Fake*} trong chế độ release
 *       đều bị từ chối fail-fast bằng ngoại lệ rõ ràng.</li>
 *   <li>Lớp này hoàn toàn không chứa bất kỳ import hay tham chiếu tĩnh nào tới {@code Mock*}/{@code Fake*}.</li>
 * </ul>
 */
public final class ClientFactory {

    private static volatile ClientConfig activeConfig = null;
    private static volatile PersistentPendingJournal journal = null;
    private static volatile ClusterNodeSelector clusterNodeSelector = null;
    private static volatile ClusterStatusClient clusterStatusClient = null;
    private static volatile ClusterConnectionViewModel clusterConnectionViewModel = null;
    private static volatile BookingClient rawBookingClient = null;
    private static volatile JournalingBookingClient journalingBookingClient = null;
    private static volatile BookingViewModel bookingViewModel = null;
    private static volatile PendingRequestsViewModel pendingRequestsViewModel = null;
    private static volatile ReadAdapter readAdapter = null;
    private static volatile SeatsRefreshService seatsRefreshService = null;
    private static volatile ReconnectController reconnectController = null;
    private static volatile TicketLookupViewModel ticketLookupViewModel = null;

    private ClientFactory() {
    }

    /**
     * Khởi tạo tường minh và thẩm tra fail-fast theo cấu hình khi khởi chạy ứng dụng.
     */
    public static synchronized void initializeForConfig(ClientConfig config) {
        Objects.requireNonNull(config, "ClientConfig không được null");
        config.validateForStartup();
        closeQuietly();

        activeConfig = config;
        clusterNodeSelector = new ClusterNodeSelector(config);
        clusterStatusClient = createClusterStatusClientForConfig(config, clusterNodeSelector);
        clusterConnectionViewModel = new ClusterConnectionViewModel(clusterStatusClient);
        if (config.isSocketMode()) {
            try {
                NodeEndpoint ep = clusterNodeSelector.selectNode(true);
                clusterConnectionViewModel.updateConnectedNode(ep.nodeId(), ep.toAddressString());
            } catch (Exception ignored) {
            }
        }
        rawBookingClient = createBookingClientForConfig(config, clusterNodeSelector, clusterConnectionViewModel, null);
        journalingBookingClient = wrapWithJournal(rawBookingClient);
        bookingViewModel = new BookingViewModel(journalingBookingClient);
        pendingRequestsViewModel = new PendingRequestsViewModel(journalingBookingClient);
        readAdapter = createReadAdapterForConfig(config, rawBookingClient);
    }

    private static void closeQuietly() {
        if (seatsRefreshService != null) {
            try {
                seatsRefreshService.close();
            } catch (Exception ignored) {
            }
            seatsRefreshService = null;
        }
        if (clusterConnectionViewModel != null) {
            clusterConnectionViewModel.stopPolling();
        }
        reconnectController = null;
        ticketLookupViewModel = null;
    }

    public static synchronized ClientConfig getActiveConfig() {
        if (activeConfig == null) {
            activeConfig = ClientConfig.loadDefault();
        }
        return activeConfig;
    }

    public static synchronized PersistentPendingJournal getJournal() {
        if (journal == null || journal.isClosed()) {
            journal = createDefaultJournal();
        }
        return journal;
    }

    public static synchronized void setJournal(PersistentPendingJournal customJournal) {
        if (journal != null && journal != customJournal && !journal.isClosed()) {
            try {
                journal.close();
            } catch (Exception ignored) {
            }
        }
        journal = customJournal;
        if (rawBookingClient != null) {
            journalingBookingClient = wrapWithJournal(rawBookingClient);
            bookingViewModel = new BookingViewModel(journalingBookingClient);
            pendingRequestsViewModel = new PendingRequestsViewModel(journalingBookingClient);
            ticketLookupViewModel = null;
        }
    }

    private static PersistentPendingJournal createDefaultJournal() {
        try {
            return PersistentPendingJournal.createDefault();
        } catch (Exception e) {
            try {
                Path fallbackDir = Files.createTempDirectory("railway-journal-fallback-");
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
        BookingClient resolved = (client != null) ? client : createDefaultBookingClient();
        rejectForbiddenComponentInRelease(resolved, "BookingClient");
        PersistentPendingJournal j = getJournal();
        if (j == null) {
            j = createDefaultJournal();
        }
        return new JournalingBookingClient(resolved, j);
    }

    private static void rejectForbiddenComponentInRelease(Object component, String roleName) {
        if (component == null) {
            return;
        }
        boolean releaseActive = (activeConfig != null && activeConfig.isReleaseMode())
                || ClientConfig.isReleaseModeActive();
        if (!releaseActive) {
            return;
        }
        String className = component.getClass().getName();
        String simpleName = component.getClass().getSimpleName();
        if (simpleName.contains("Mock") || simpleName.contains("Fake") || className.contains(".dev.")) {
            throw new IllegalStateException(
                    "Chế độ RELEASE (release.mode=true) từ chối thành phần không phải thật cho "
                            + roleName + ": " + className);
        }
        if (component instanceof JournalingBookingClient jbc) {
            rejectForbiddenComponentInRelease(jbc.getDelegate(), roleName + ".delegate");
        }
    }

    public static synchronized ClusterNodeSelector getClusterNodeSelector() {
        if (clusterNodeSelector == null) {
            clusterNodeSelector = createDefaultClusterNodeSelector();
        }
        return clusterNodeSelector;
    }

    public static synchronized void setClusterNodeSelector(ClusterNodeSelector selector) {
        clusterNodeSelector = selector;
        if (selector != null) {
            activeConfig = selector.getConfig();
        }
    }

    private static ClusterNodeSelector createDefaultClusterNodeSelector() {
        ClientConfig config = ClientConfig.loadDefault();
        activeConfig = config;
        return new ClusterNodeSelector(config);
    }

    public static BookingClient createDefaultClient() {
        return createDefaultBookingClient();
    }

    public static synchronized BookingClient createDefaultBookingClient() {
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        return createBookingClientForConfig(config, selector, clusterConnectionViewModel, null);
    }

    /**
     * Khởi tạo {@link BookingClient} theo {@link ClientConfig} và hàm tạo transport tùy chọn (hỗ trợ spy kiểm thử).
     */
    public static BookingClient createBookingClientForConfig(
            ClientConfig config,
            Function<NodeEndpoint, BookingClient> customSocketFactory) {
        Objects.requireNonNull(config, "config không được null");
        config.validateForStartup();
        ClusterNodeSelector selector = new ClusterNodeSelector(config);
        return createBookingClientForConfig(config, selector, null, customSocketFactory);
    }

    private static BookingClient createBookingClientForConfig(
            ClientConfig config,
            ClusterNodeSelector selector,
            ClusterConnectionViewModel connVm,
            Function<NodeEndpoint, BookingClient> customSocketFactory) {
        Objects.requireNonNull(config, "config không được null");
        if (config.isReleaseMode()) {
            config.validateForRelease();
        }
        if (config.isReleaseMode() || config.isSocketMode()) {
            ClusterNodeSelector effectiveSelector = (selector != null) ? selector : new ClusterNodeSelector(config);
            ClusterEndpoints endpoints = ClusterEndpoints.fromSelector(effectiveSelector);
            RedirectPolicy policy = new RedirectPolicy(
                    config.getMaxRedirects(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs()
            );

            try {
                NodeEndpoint initialEp = effectiveSelector.selectNode(true);
                if (connVm != null) {
                    connVm.updateConnectedNode(initialEp.nodeId(), initialEp.toAddressString());
                }
            } catch (Exception ignored) {
            }

            Function<NodeEndpoint, BookingClient> socketFactory = (customSocketFactory != null)
                    ? customSocketFactory
                    : ep -> new SocketBookingClient(
                            ep.host(),
                            ep.port(),
                            config.getConnectTimeoutMs(),
                            config.getReadTimeoutMs(),
                            "desktop-client"
                    );

            return new LeaderAwareBookingClient(
                    endpoints,
                    socketFactory,
                    policy,
                    connVm,
                    config.getPreferredNode(),
                    effectiveSelector
            );
        }
        return DevSupportBridge.get().createDevBookingClient();
    }

    public static synchronized ClusterStatusClient createDefaultClusterStatusClient() {
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        return createClusterStatusClientForConfig(config, selector);
    }

    public static ClusterStatusClient createClusterStatusClientForConfig(
            ClientConfig config,
            ClusterNodeSelector selector) {
        Objects.requireNonNull(config, "config không được null");
        if (config.isReleaseMode()) {
            config.validateForRelease();
        }
        String mode = System.getProperty("cluster.client");
        if (mode == null || mode.isBlank()) {
            mode = config.getClientMode();
        }
        mode = mode.trim().toLowerCase();

        if (config.isReleaseMode()) {
            if (!"socket".equals(mode) && !"tcp".equals(mode) && !"real".equals(mode)) {
                throw new IllegalArgumentException(
                        "Chế độ RELEASE từ chối 'cluster.client=" + mode + "' — bắt buộc dùng SocketClusterStatusClient.");
            }
        }

        if (config.isReleaseMode() || "socket".equals(mode) || "tcp".equals(mode) || "real".equals(mode)) {
            NodeEndpoint ep = null;
            if (selector != null) {
                try {
                    ep = selector.selectNode(true);
                } catch (Exception ignored) {
                }
            }
            if (ep == null) {
                if (config.getEndpoints().isEmpty()) {
                    throw new IllegalStateException("Danh sách endpoint rỗng khi khởi tạo SocketClusterStatusClient");
                }
                ep = config.getEndpoints().get(0);
            }
            return new SocketClusterStatusClient(
                    ep.host(),
                    ep.port(),
                    config.getConnectTimeoutMs(),
                    config.getReadTimeoutMs(),
                    "client-status-monitor"
            );
        }
        return DevSupportBridge.get().createDevClusterStatusClient();
    }

    public static synchronized BookingClient getBookingClient() {
        if (rawBookingClient == null) {
            rawBookingClient = createDefaultBookingClient();
        }
        return rawBookingClient;
    }

    public static synchronized JournalingBookingClient getJournalingBookingClient() {
        if (journalingBookingClient == null) {
            journalingBookingClient = wrapWithJournal(getBookingClient());
        }
        return journalingBookingClient;
    }

    public static synchronized void setBookingClient(BookingClient client) {
        if (client != null) {
            rejectForbiddenComponentInRelease(client, "BookingClient");
            rawBookingClient = client;
        } else {
            rawBookingClient = createDefaultBookingClient();
        }
        journalingBookingClient = wrapWithJournal(rawBookingClient);
        bookingViewModel = new BookingViewModel(journalingBookingClient);
        pendingRequestsViewModel = new PendingRequestsViewModel(journalingBookingClient);
        ticketLookupViewModel = null;
    }

    public static synchronized ClusterStatusClient getClusterStatusClient() {
        if (clusterStatusClient == null) {
            clusterStatusClient = createDefaultClusterStatusClient();
        }
        return clusterStatusClient;
    }

    public static synchronized void setClusterStatusClient(ClusterStatusClient client) {
        if (client != null) {
            rejectForbiddenComponentInRelease(client, "ClusterStatusClient");
        }
        if (clusterConnectionViewModel != null) {
            clusterConnectionViewModel.stopPolling();
        }
        clusterStatusClient = (client != null) ? client : createDefaultClusterStatusClient();
        clusterConnectionViewModel = new ClusterConnectionViewModel(clusterStatusClient);
    }

    public static synchronized ClusterConnectionViewModel getClusterConnectionViewModel() {
        if (clusterConnectionViewModel == null) {
            clusterConnectionViewModel = new ClusterConnectionViewModel(getClusterStatusClient());
            try {
                ClientConfig cfg = getActiveConfig();
                if (cfg.isSocketMode()) {
                    NodeEndpoint ep = getClusterNodeSelector().selectNode(true);
                    clusterConnectionViewModel.updateConnectedNode(ep.nodeId(), ep.toAddressString());
                }
            } catch (Exception ignored) {
            }
        }
        return clusterConnectionViewModel;
    }

    public static synchronized void resetToConfiguredClient() {
        ClientConfig cfg = ClientConfig.loadDefault();
        initializeForConfig(cfg);
    }

    public static synchronized BookingViewModel getBookingViewModel() {
        if (bookingViewModel == null) {
            bookingViewModel = new BookingViewModel(getJournalingBookingClient());
        }
        return bookingViewModel;
    }

    public static synchronized PendingRequestsViewModel getPendingRequestsViewModel() {
        if (pendingRequestsViewModel == null) {
            pendingRequestsViewModel = new PendingRequestsViewModel(getJournalingBookingClient());
        }
        return pendingRequestsViewModel;
    }

    public static synchronized ReadAdapter createDefaultReadAdapter() {
        ClusterNodeSelector selector = getClusterNodeSelector();
        ClientConfig config = (selector != null) ? selector.getConfig() : ClientConfig.loadDefault();
        return createReadAdapterForConfig(config, getBookingClient());
    }

    public static ReadAdapter createReadAdapterForConfig(ClientConfig config, BookingClient bookingClient) {
        Objects.requireNonNull(config, "config không được null");
        String readMode = System.getProperty("read.adapter", "").trim().toLowerCase();
        if (config.isReleaseMode()) {
            config.validateForRelease();
            if ("fake".equals(readMode) || "mock".equals(readMode)) {
                throw new IllegalArgumentException(
                        "Chế độ RELEASE từ chối 'read.adapter=" + readMode + "' — chỉ cho phép BookingClientReadAdapter thật.");
            }
            return new BookingClientReadAdapter(Objects.requireNonNull(bookingClient, "bookingClient không được null"));
        }
        if ("fake".equals(readMode)) {
            return DevSupportBridge.get().createDevReadAdapter();
        }
        if (config.isSocketMode()) {
            return new BookingClientReadAdapter(Objects.requireNonNull(bookingClient, "bookingClient không được null"));
        }
        return DevSupportBridge.get().createDevReadAdapter();
    }

    public static synchronized ReadAdapter getReadAdapter() {
        if (readAdapter == null) {
            readAdapter = createDefaultReadAdapter();
        }
        return readAdapter;
    }

    public static synchronized void setReadAdapter(ReadAdapter adapter) {
        if (adapter != null) {
            rejectForbiddenComponentInRelease(adapter, "ReadAdapter");
        }
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

    public static synchronized SeatsRefreshService getSeatsRefreshService(ObservableList<SeatViewModel> seatsList) {
        if (seatsRefreshService == null || seatsRefreshService.getSeatsList() != seatsList) {
            if (seatsRefreshService != null) {
                seatsRefreshService.close();
            }
            seatsRefreshService = new SeatsRefreshService(getReadAdapter(), seatsList, getJournal());
        }
        return seatsRefreshService;
    }

    public static synchronized SeatsRefreshService getSeatsRefreshService() {
        return seatsRefreshService;
    }

    public static synchronized void setSeatsRefreshService(SeatsRefreshService service) {
        if (seatsRefreshService != null && seatsRefreshService != service) {
            seatsRefreshService.close();
        }
        seatsRefreshService = service;
    }

    public static synchronized ReconnectController getReconnectController() {
        if (reconnectController == null) {
            BookingClient raw = getBookingClient();
            LeaderAwareBookingClient leaderClient = (raw instanceof LeaderAwareBookingClient labc)
                    ? labc : null;
            reconnectController = new ReconnectController(
                    leaderClient,
                    getBookingViewModel(),
                    getClusterConnectionViewModel(),
                    getJournal()
            );
        }
        return reconnectController;
    }

    public static synchronized void setReconnectController(ReconnectController controller) {
        reconnectController = controller;
    }

    public static synchronized TicketLookupViewModel getTicketLookupViewModel() {
        if (ticketLookupViewModel == null) {
            ticketLookupViewModel = new TicketLookupViewModel(
                    getJournalingBookingClient(),
                    getJournal(),
                    null,
                    () -> {
                        if (seatsRefreshService != null) {
                            seatsRefreshService.refreshNow();
                        }
                    }
            );
        }
        return ticketLookupViewModel;
    }

    public static synchronized void setTicketLookupViewModel(TicketLookupViewModel vm) {
        ticketLookupViewModel = vm;
    }
}
