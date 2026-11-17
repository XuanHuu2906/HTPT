package vn.nhom6.railway.client.network;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Điều phối lựa chọn Node kết nối trong cụm và quản lý failover an toàn (TASK TV1-D16).
 * <p>
 * Tính năng chính:
 * <ul>
 *   <li>Áp dụng các chiến lược kết nối cấu hình được: {@link SelectionStrategy#RANDOM},
 *       {@link SelectionStrategy#ROUND_ROBIN}, {@link SelectionStrategy#PREFERRED_FIRST}.</li>
 *   <li>Failover khi kết nối thất bại: đánh dấu Node gặp lỗi tạm thời (cooldown); tự động bỏ qua Node
 *       đang trong cooldown để chọn Node tiếp theo.</li>
 *   <li>Không chọn lại Node đang cooldown trừ khi có yêu cầu hoặc tất cả các Node đều đã gặp lỗi.
 *       Khi hết Node khả dụng, báo lỗi kết nối rõ ràng (phía client, không giả mạo mã SPEC-03).</li>
 *   <li>Cung cấp ánh xạ {@link #endpointOf(String)} duy nhất cho {@link LeaderAwareBookingClient} chuyển hướng.</li>
 *   <li>Thread-safe, không mở socket hay chặn luồng JavaFX UI.</li>
 * </ul>
 */
public class ClusterNodeSelector {

    private static final System.Logger LOGGER = System.getLogger(ClusterNodeSelector.class.getName());

    private volatile ClientConfig config;
    private final Random random;
    private final LongSupplier timeSource;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final Map<String, Long> failureTimestamps = new ConcurrentHashMap<>();

    /**
     * Khởi tạo với cấu hình {@link ClientConfig}.
     */
    public ClusterNodeSelector(ClientConfig config) {
        this(config, new Random(), System::currentTimeMillis);
    }

    /**
     * Khởi tạo hỗ trợ tiêm {@link Random} và nguồn thời gian (phục vụ Unit Test kiểm thử xác định).
     */
    public ClusterNodeSelector(ClientConfig config, Random random, LongSupplier timeSource) {
        this.config = Objects.requireNonNull(config, "config không được null");
        this.random = (random != null) ? random : new Random();
        this.timeSource = (timeSource != null) ? timeSource : System::currentTimeMillis;
    }

    /**
     * Lấy cấu hình {@link ClientConfig} hiện tại.
     */
    public ClientConfig getConfig() {
        return config;
    }

    /**
     * Nạp lại (reload) cấu hình mới từ tệp hoặc nguồn mặc định.
     */
    public synchronized void reload() {
        this.config = ClientConfig.loadDefault();
        this.failureTimestamps.clear();
        LOGGER.log(System.Logger.Level.INFO, "[ClusterNodeSelector] Đã nạp lại cấu hình: {0} endpoints", config.getEndpoints().size());
    }

    /**
     * Cập nhật cấu hình mới trực tiếp.
     */
    public synchronized void reload(ClientConfig newConfig) {
        this.config = Objects.requireNonNull(newConfig, "newConfig không được null");
        this.failureTimestamps.clear();
    }

    /**
     * Lấy danh sách toàn bộ {@link NodeEndpoint} trong cấu hình hiện tại.
     */
    public List<NodeEndpoint> getAllEndpoints() {
        return config.getEndpoints();
    }

    /**
     * Lấy danh sách toàn bộ nodeId trong cấu hình hiện tại.
     */
    public List<String> getAllNodeIds() {
        return config.getNodeIds();
    }

    /**
     * Tìm {@link NodeEndpoint} theo nodeId chỉ định.
     * Chỉ trả về endpoint nếu nodeId nằm trong danh bạ cấu hình, ngược lại trả về {@code null}.
     */
    public NodeEndpoint endpointOf(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return null;
        }
        return config.getEndpoint(nodeId.trim());
    }

    /**
     * Kiểm tra một Node có đang khả dụng không (không bị đánh dấu lỗi trong thời gian cooldown).
     */
    public boolean isAvailable(String nodeId) {
        if (nodeId == null || !config.containsNode(nodeId)) {
            return false;
        }
        Long failedAt = failureTimestamps.get(nodeId);
        if (failedAt == null) {
            return true;
        }
        long elapsed = timeSource.getAsLong() - failedAt;
        if (elapsed >= config.getFailureCooldownMs()) {
            failureTimestamps.remove(nodeId);
            return true;
        }
        return false;
    }

    /**
     * Đánh dấu một Node gặp lỗi kết nối tạm thời.
     */
    public void markFailed(String nodeId) {
        if (nodeId != null && config.containsNode(nodeId)) {
            long now = timeSource.getAsLong();
            failureTimestamps.put(nodeId, now);
            LOGGER.log(System.Logger.Level.WARNING,
                    "[ClusterNodeSelector] Đã đánh dấu Node {0} gặp lỗi tạm thời (cooldown {1}ms)",
                    nodeId, config.getFailureCooldownMs());
        }
    }

    /**
     * Đánh dấu một Node kết nối thành công, xóa trạng thái cooldown.
     */
    public void markSuccess(String nodeId) {
        if (nodeId != null) {
            failureTimestamps.remove(nodeId);
        }
    }

    /**
     * Xóa sạch toàn bộ trạng thái lỗi cooldown của tất cả các Node.
     */
    public void resetCooldowns() {
        failureTimestamps.clear();
    }

    /**
     * Lấy danh sách các Node hiện đang khả dụng (không bị cooldown).
     */
    public List<NodeEndpoint> getAvailableEndpoints() {
        List<NodeEndpoint> all = config.getEndpoints();
        List<NodeEndpoint> available = new ArrayList<>();
        long now = timeSource.getAsLong();
        long cooldown = config.getFailureCooldownMs();

        for (NodeEndpoint ep : all) {
            Long failedAt = failureTimestamps.get(ep.nodeId());
            if (failedAt == null || (now - failedAt) >= cooldown) {
                available.add(ep);
            }
        }
        return available;
    }

    /**
     * Lựa chọn Node tiếp theo để kết nối theo chiến lược cấu hình và cơ chế failover.
     * <p>
     * Nếu còn Node khả dụng: chọn theo chiến lược (RANDOM, ROUND_ROBIN, PREFERRED_FIRST).
     * Nếu TẤT CẢ các Node đều bị cooldown: ném ngoại lệ {@link IllegalStateException} báo lỗi kết nối rõ ràng.
     *
     * @return {@link NodeEndpoint} được chọn
     * @throws IllegalStateException khi không còn Node nào khả dụng trong cụm
     */
    public NodeEndpoint selectNode() {
        return selectNode(false);
    }

    /**
     * Lựa chọn Node tiếp theo để kết nối.
     *
     * @param allowFallbackWhenAllFailed nếu true, khi tất cả Node đều bị cooldown sẽ chọn Node có thời gian lỗi lâu nhất;
     *                                   nếu false, ném ngoại lệ fail-fast.
     * @return {@link NodeEndpoint} được chọn
     * @throws IllegalStateException khi không còn Node khả dụng và allowFallbackWhenAllFailed = false
     */
    public NodeEndpoint selectNode(boolean allowFallbackWhenAllFailed) {
        List<NodeEndpoint> available = getAvailableEndpoints();

        if (available.isEmpty()) {
            if (!allowFallbackWhenAllFailed) {
                throw new IllegalStateException(String.format(
                        "Không còn Node nào khả dụng trong cụm (tất cả %d Node đều đang trong thời gian cooldown %dms)",
                        config.getEndpoints().size(), config.getFailureCooldownMs()));
            }
            // Fallback: chọn Node có thời gian lỗi xa nhất (đã cooldown lâu nhất)
            NodeEndpoint oldestFailed = findOldestFailedEndpoint();
            if (oldestFailed != null) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "[ClusterNodeSelector] Tất cả Node đều lỗi, chọn lại Node cooldown lâu nhất: {0}", oldestFailed.nodeId());
                return oldestFailed;
            }
            // Nếu danh sách endpoints rỗng
            throw new IllegalStateException("Cấu hình không có Node nào để lựa chọn");
        }

        SelectionStrategy strategy = config.getSelectionStrategy();
        String preferredNode = config.getPreferredNode();

        if (strategy == SelectionStrategy.PREFERRED_FIRST && preferredNode != null && !preferredNode.isBlank()) {
            for (NodeEndpoint ep : available) {
                if (ep.nodeId().equals(preferredNode)) {
                    return ep;
                }
            }
            // Nếu preferredNode không khả dụng thì fallback sang ROUND_ROBIN trong số các node khả dụng còn lại
        }

        if (strategy == SelectionStrategy.RANDOM) {
            int idx = random.nextInt(available.size());
            return available.get(idx);
        }

        // Mặc định hoặc ROUND_ROBIN
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % available.size());
        return available.get(idx);
    }

    private NodeEndpoint findOldestFailedEndpoint() {
        NodeEndpoint best = null;
        long oldestTime = Long.MAX_VALUE;
        for (NodeEndpoint ep : config.getEndpoints()) {
            Long failedAt = failureTimestamps.get(ep.nodeId());
            if (failedAt != null && failedAt < oldestTime) {
                oldestTime = failedAt;
                best = ep;
            }
        }
        if (best == null && !config.getEndpoints().isEmpty()) {
            best = config.getEndpoints().get(0);
        }
        return best;
    }
}
