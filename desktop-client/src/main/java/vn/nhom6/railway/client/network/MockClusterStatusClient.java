package vn.nhom6.railway.client.network;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Triển khai MOCK của {@link ClusterStatusClient} phục vụ kiểm thử giao diện và chạy offline (D11).
 *
 * <p>Đặc điểm:
 * <ul>
 *   <li>Luôn trả về nguồn dữ liệu là {@link DataSource#MOCK} để UI hiển thị nhãn "MOCK" rõ ràng.</li>
 *   <li>Cho phép cấu hình động trạng thái để kiểm thử các kịch bản: Leader, Follower có hint, Candidate, mất mạng.</li>
 *   <li>Không bao giờ giả mạo dữ liệu là REAL.</li>
 * </ul>
 */
public class MockClusterStatusClient implements ClusterStatusClient {

    private static final MockClusterStatusClient INSTANCE = new MockClusterStatusClient();

    public static MockClusterStatusClient getInstance() {
        return INSTANCE;
    }

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "mock-status-client-worker");
        t.setDaemon(true);
        return t;
    });

    private volatile NodeStatus currentStatus;
    private volatile boolean simulateOffline = false;
    private volatile long simulatedDelayMs = 50;

    public MockClusterStatusClient() {
        // Khởi tạo mặc định: MOCK ban đầu để trống hoặc trạng thái uninitialized
        // Chú ý: Không hardcode là Node 1 LEADER
        this.currentStatus = new NodeStatus(
                "node-1",
                "FOLLOWER",
                1L,
                "node-2",
                10L,
                10L,
                10L,
                true,
                Instant.now()
        );
    }

    @Override
    public CompletableFuture<NodeStatus> getNodeStatus() {
        CompletableFuture<NodeStatus> future = new CompletableFuture<>();

        Runnable task = () -> {
            if (simulateOffline) {
                future.completeExceptionally(new java.net.ConnectException("Mô phỏng mất kết nối tới Node (MOCK OFFLINE)"));
            } else {
                NodeStatus status = currentStatus;
                // Cập nhật timestamp thời điểm quan sát mới
                NodeStatus fresh = new NodeStatus(
                        status.nodeId(),
                        status.role(),
                        status.term(),
                        status.leaderId(),
                        status.lastLogIndex(),
                        status.commitIndex(),
                        status.lastApplied(),
                        status.storageHealthy(),
                        Instant.now()
                );
                future.complete(fresh);
            }
        };

        if (simulatedDelayMs > 0) {
            scheduler.schedule(task, simulatedDelayMs, TimeUnit.MILLISECONDS);
        } else {
            task.run();
        }

        return future;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.MOCK;
    }

    @Override
    public String getEndpoint() {
        return "mock://cluster-sim";
    }

    public synchronized void setStatus(NodeStatus status) {
        this.currentStatus = Objects.requireNonNull(status, "status không được null");
        this.simulateOffline = false;
    }

    public synchronized void setLeader(String nodeId, long term) {
        this.currentStatus = new NodeStatus(
                nodeId,
                "LEADER",
                term,
                nodeId,
                100L,
                100L,
                100L,
                true,
                Instant.now()
        );
        this.simulateOffline = false;
    }

    public synchronized void setFollower(String nodeId, long term, String leaderId) {
        this.currentStatus = new NodeStatus(
                nodeId,
                "FOLLOWER",
                term,
                leaderId,
                100L,
                100L,
                100L,
                true,
                Instant.now()
        );
        this.simulateOffline = false;
    }

    public synchronized void setSimulateOffline(boolean simulateOffline) {
        this.simulateOffline = simulateOffline;
    }

    public synchronized void setSimulatedDelayMs(long simulatedDelayMs) {
        this.simulatedDelayMs = Math.max(0, simulatedDelayMs);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
