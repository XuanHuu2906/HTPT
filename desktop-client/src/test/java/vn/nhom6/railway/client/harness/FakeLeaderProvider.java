package vn.nhom6.railway.client.harness;

import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.smoke.FakeNodeServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FAKE LEADER PROVIDER — không phải Raft thật, không phải cụm thật (TASK TV1-D21).
 * <p>
 * Quản lý một cụm gồm 3–5 Node giả lập bằng {@link FakeNodeServer} để kiểm thử
 * các kịch bản failover, mất Leader, bầu cử và bảo toàn pending request trên Client.
 */
public class FakeLeaderProvider implements AutoCloseable {

    private final Map<String, FakeNodeServer> nodes = new ConcurrentHashMap<>();
    private final ClusterEndpoints clusterEndpoints;
    private volatile String currentLeaderId = "node-1";

    public FakeLeaderProvider() throws IOException {
        this(3);
    }

    public FakeLeaderProvider(int nodeCount) throws IOException {
        int count = Math.max(3, Math.min(5, nodeCount));
        Map<String, NodeEndpoint> endpointMap = new HashMap<>();

        for (int i = 1; i <= count; i++) {
            String nodeId = "node-" + i;
            FakeNodeServer server = new FakeNodeServer(nodeId, 0);
            nodes.put(nodeId, server);
            endpointMap.put(nodeId, new NodeEndpoint(nodeId, "127.0.0.1", server.getPort()));
        }

        this.clusterEndpoints = ClusterEndpoints.fromEndpoints(endpointMap.values(), "node-1");

        // Khởi tạo: node-1 là LEADER, các node còn lại là FOLLOWER trỏ về node-1
        electLeader("node-1");
    }

    public ClusterEndpoints getClusterEndpoints() {
        return clusterEndpoints;
    }

    public String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public FakeNodeServer getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public List<FakeNodeServer> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    /**
     * Kịch bản A: Node Leader bị sập đột ngột (đóng kết nối socket hoàn toàn).
     */
    public void crashNode(String nodeId) {
        FakeNodeServer node = nodes.get(nodeId);
        if (node != null) {
            node.close();
            if (nodeId.equals(currentLeaderId)) {
                currentLeaderId = null;
            }
        }
    }

    /**
     * Kịch bản B: Cửa sổ bầu cử (Election Window): tất cả các Node còn sống đều trả về NOT_LEADER với leaderHint = null.
     */
    public void setElectionWindow() {
        this.currentLeaderId = null;
        for (FakeNodeServer node : nodes.values()) {
            node.setNotLeader(null, 2L);
        }
    }

    /**
     * Kịch bản C: Một Node đắc cử Leader mới (ví dụ node-2 lên Leader).
     * Node mới trở thành LEADER, các Node còn lại chuyển thành FOLLOWER trỏ hint về Node mới.
     */
    public void electLeader(String newLeaderId) {
        this.currentLeaderId = newLeaderId;
        for (Map.Entry<String, FakeNodeServer> entry : nodes.entrySet()) {
            String id = entry.getKey();
            FakeNodeServer node = entry.getValue();
            if (id.equals(newLeaderId)) {
                node.setLeader();
            } else {
                node.setNotLeader(newLeaderId, 2L);
            }
        }
    }

    /**
     * Kịch bản D: Leader cũ đã lưu và áp dụng lệnh trước khi sập.
     * Dữ liệu được đồng bộ sang Leader mới để trả về kết quả lưu theo requestId (idempotency).
     */
    public void replicateBookingToNode(String targetNodeId, String requestId, String tripId, String seatId, String passengerName, vn.nhom6.railway.protocol.dto.BookTicketResponseDto response) {
        FakeNodeServer target = nodes.get(targetNodeId);
        if (target != null) {
            target.savePreExistingBooking(requestId, tripId, seatId, passengerName, response);
        }
    }

    @Override
    public void close() {
        for (FakeNodeServer node : nodes.values()) {
            try {
                node.close();
            } catch (Exception ignored) {
            }
        }
    }
}
