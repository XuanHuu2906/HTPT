package vn.nhom6.railway.client.network;

/**
 * Điểm cuối kết nối mạng (endpoint) của một Node trong cụm phân tán (TASK TV1-D12).
 * <p>
 * Lưu trữ định danh node (nodeId) cùng host và port TCP dành cho Client theo đặc tả SPEC-08 §3.
 *
 * @param nodeId định danh Node (ví dụ: "node-1", "node-2",...)
 * @param host   địa chỉ IP hoặc hostname (ví dụ: "127.0.0.1")
 * @param port   cổng TCP client (1 - 65535, ví dụ: 9101)
 */
public record NodeEndpoint(String nodeId, String host, int port) {

    public NodeEndpoint {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId không được null hoặc rỗng");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host không được null hoặc rỗng");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port phải nằm trong khoảng (0, 65535], nhận được: " + port);
        }
        nodeId = nodeId.trim();
        host = host.trim();
    }

    public String toAddressString() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return nodeId + "@" + host + ":" + port;
    }
}
