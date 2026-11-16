package vn.nhom6.railway.client.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Công cụ kiểm tra kết nối mạng (Connectivity Check / PING / STATUS) tới từng Node trong cụm (TASK TV1-D16).
 * <p>
 * Tính năng chính:
 * <ul>
 *   <li>Gửi {@link MessageType#NODE_STATUS} chuẩn SPEC-03 tới từng Node trong cấu hình.</li>
 *   <li>Đo thời gian phản hồi (latency ms), trạng thái reachable/unreachable, role và nodeId do Node tự báo.</li>
 *   <li>So khớp nodeId cấu hình với reportedNodeId của Node (cảnh báo nếu sai lệch).</li>
 *   <li>Gắn nhãn phân định rõ ràng giữa {@link DataSource#REAL} và {@link DataSource#FAKE}.</li>
 *   <li>Tuyệt đối không suy diễn Leader: chỉ ghi nhận đúng vai trò Node trả về; không tự tuyên bố election PASS.</li>
 *   <li>Hỗ trợ thực thi song song bất đồng bộ, có phương thức {@link #main(String[])} chạy từ dòng lệnh.</li>
 * </ul>
 */
public class ClusterConnectivityCheck {

    private static final System.Logger LOGGER = System.getLogger(ClusterConnectivityCheck.class.getName());
    public static final String SENDER_ID = "client-connectivity-check";
    public static final int DEFAULT_TIMEOUT_MS = 2000;

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "connectivity-checker-worker");
        t.setDaemon(true);
        return t;
    };

    /**
     * Bản ghi chi tiết kết quả kiểm tra một Node.
     */
    public record NodeCheckResult(
            NodeEndpoint endpoint,
            boolean reachable,
            long latencyMs,
            String role,
            String reportedNodeId,
            Long term,
            boolean nodeIdMatched,
            String errorMessage,
            DataSource dataSource
    ) {
        public boolean isLeader() {
            return "LEADER".equalsIgnoreCase(role);
        }

        public String toDisplayString() {
            if (!reachable) {
                return String.format("[DOWN] %s (%s) - Không thể kết nối: %s",
                        endpoint.nodeId(), endpoint.toAddressString(), errorMessage);
            }
            String matchWarning = nodeIdMatched ? "" : String.format(" [CẢNH BÁO LỆCH NODE_ID: báo %s]", reportedNodeId);
            return String.format("[UP]   %s (%s) - Role: %s, Term: %s, Latency: %dms%s [%s]",
                    endpoint.nodeId(), endpoint.toAddressString(), role,
                    (term != null ? term : "N/A"), latencyMs, matchWarning, dataSource);
        }
    }

    /**
     * Bản tóm tắt kết quả kiểm tra toàn bộ cụm.
     */
    public record ClusterCheckSummary(
            List<NodeCheckResult> results,
            int totalNodes,
            int reachableNodes,
            int leaderCount,
            Instant checkedAt,
            DataSource dataSource
    ) {
        public String toFormattedSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("================================================================================\n");
            sb.append(String.format("KẾT QUẢ KIỂM TRA KẾT NỐI CỤM (Thời điểm: %s | Nguồn: %s)\n", checkedAt, dataSource));
            sb.append("================================================================================\n");
            for (NodeCheckResult r : results) {
                sb.append(r.toDisplayString()).append("\n");
            }
            sb.append("--------------------------------------------------------------------------------\n");
            sb.append(String.format("Tổng số Node cấu hình: %d | Khả dụng (UP): %d/%d | Số Leader ghi nhận: %d\n",
                    totalNodes, reachableNodes, totalNodes, leaderCount));
            sb.append("================================================================================\n");
            return sb.toString();
        }
    }

    /**
     * Kiểm tra đồng bộ một Node đơn lẻ.
     */
    public static NodeCheckResult checkNode(NodeEndpoint endpoint, int timeoutMs, DataSource dataSource) {
        Objects.requireNonNull(endpoint, "endpoint không được null");
        DataSource src = (dataSource != null) ? dataSource : DataSource.REAL;
        long startTime = System.currentTimeMillis();

        ObjectNode emptyPayload = MessageCodec.getMapper().createObjectNode();
        MessageEnvelope<ObjectNode> reqEnvelope = MessageEnvelope.createRequest(
                MessageType.NODE_STATUS,
                SENDER_ID,
                emptyPayload
        );
        String sentMessageId = reqEnvelope.getMessageId();

        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            if (timeoutMs > 0) {
                socket.setSoTimeout(timeoutMs);
            }
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), timeoutMs);

            try (OutputStream out = new BufferedOutputStream(socket.getOutputStream());
                 InputStream in = socket.getInputStream()) {

                FrameCodec.writeMessage(out, reqEnvelope);
                byte[] responseBytes = FrameCodec.readFrame(in);
                long latency = System.currentTimeMillis() - startTime;

                MessageEnvelope<JsonNode> resEnvelope = MessageCodec.decode(responseBytes);
                if (!Objects.equals(sentMessageId, resEnvelope.getCorrelationId())) {
                    throw new IllegalStateException("correlationId không khớp");
                }

                MessageType type = resEnvelope.getType();
                if (type == MessageType.NODE_STATUS_RESPONSE) {
                    JsonNode payload = resEnvelope.getPayload();
                    String reportedNodeId = (payload != null && payload.hasNonNull("nodeId")) ? payload.get("nodeId").asText() : null;
                    String role = (payload != null && payload.hasNonNull("role")) ? payload.get("role").asText() : "UNKNOWN";
                    Long term = (payload != null && payload.hasNonNull("term")) ? payload.get("term").asLong() : null;

                    boolean matched = reportedNodeId == null || reportedNodeId.equalsIgnoreCase(endpoint.nodeId());
                    if (!matched) {
                        String warnMsg = String.format(
                                "[ClusterConnectivityCheck CẢNH BÁO] Node tại %s trả về nodeId='%s' nhưng cấu hình lại là '%s'",
                                endpoint.toAddressString(), reportedNodeId, endpoint.nodeId()
                        );
                        LOGGER.log(System.Logger.Level.WARNING, warnMsg);
                        System.err.println(warnMsg);
                    }

                    return new NodeCheckResult(endpoint, true, latency, role, reportedNodeId, term, matched, null, src);
                } else {
                    return new NodeCheckResult(endpoint, true, latency, "UNKNOWN", null, null, true,
                            "Nhận phản hồi không phải NODE_STATUS_RESPONSE: " + type, src);
                }
            }
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            return new NodeCheckResult(endpoint, false, elapsed, "UNKNOWN", null, null, false,
                    e.getClass().getSimpleName() + ": " + e.getMessage(), src);
        }
    }

    /**
     * Kiểm tra toàn bộ danh sách Node (chạy tuần tự hoặc song song).
     */
    public static ClusterCheckSummary checkCluster(List<NodeEndpoint> endpoints, int timeoutMs, DataSource dataSource) {
        Objects.requireNonNull(endpoints, "endpoints không được null");
        DataSource src = (dataSource != null) ? dataSource : DataSource.REAL;
        List<NodeCheckResult> results = new ArrayList<>();
        int reachable = 0;
        int leaders = 0;

        for (NodeEndpoint ep : endpoints) {
            NodeCheckResult res = checkNode(ep, timeoutMs, src);
            results.add(res);
            if (res.reachable()) {
                reachable++;
                if (res.isLeader()) {
                    leaders++;
                }
            }
        }

        return new ClusterCheckSummary(
                Collections.unmodifiableList(results),
                endpoints.size(),
                reachable,
                leaders,
                Instant.now(),
                src
        );
    }

    /**
     * Kiểm tra bất đồng bộ toàn bộ Node qua Thread Pool.
     */
    public static CompletableFuture<ClusterCheckSummary> checkClusterAsync(
            List<NodeEndpoint> endpoints, int timeoutMs, DataSource dataSource) {
        ExecutorService pool = Executors.newFixedThreadPool(
                Math.max(1, Math.min(endpoints.size(), 10)), DAEMON_THREAD_FACTORY
        );
        return CompletableFuture.supplyAsync(() -> {
            try {
                return checkCluster(endpoints, timeoutMs, dataSource);
            } finally {
                pool.shutdown();
            }
        }, pool);
    }

    /**
     * Điểm nhập chạy kiểm tra trực tiếp từ dòng lệnh.
     */
    public static void main(String[] args) {
        System.out.println("Đang nạp cấu hình cụm qua ClientConfig...");
        ClientConfig config = ClientConfig.loadDefault();
        System.out.printf("Profile: %s | Endpoints: %d | Timeout: %dms\n",
                config.getProfile(), config.getEndpoints().size(), config.getConnectTimeoutMs());

        ClusterCheckSummary summary = checkCluster(config.getEndpoints(), config.getConnectTimeoutMs(), DataSource.REAL);
        System.out.println(summary.toFormattedSummary());
    }
}
