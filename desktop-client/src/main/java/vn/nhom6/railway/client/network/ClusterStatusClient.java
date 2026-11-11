package vn.nhom6.railway.client.network;

import java.util.concurrent.CompletableFuture;

/**
 * Giao diện máy khách đọc trạng thái cụm/Node theo hợp đồng SPEC-03 Section 5 (Diagnostic NODE_STATUS).
 *
 * <p>Bảo đảm:
 * <ul>
 *   <li>Tách biệt rạch ròi nguồn dữ liệu {@link DataSource}: REAL (TCP Socket thật), MOCK (giả lập), NONE (chưa có).</li>
 *   <li>Chạy bất đồng bộ qua {@link CompletableFuture}, không chặn JavaFX Application Thread.</li>
 *   <li>Tuyệt đối không dùng HTTP/REST hay Web libraries (tuân thủ AGENTS.md Section 0 & 5).</li>
 * </ul>
 */
public interface ClusterStatusClient extends AutoCloseable {

    /**
     * Nguồn gốc dữ liệu trạng thái để UI hiển thị nhãn minh bạch.
     */
    enum DataSource {
        REAL("REAL", "badge-real"),
        MOCK("MOCK", "badge-mock"),
        NONE("NONE", "badge-none");

        private final String label;
        private final String styleClass;

        DataSource(String label, String styleClass) {
            this.label = label;
            this.styleClass = styleClass;
        }

        public String getLabel() {
            return label;
        }

        public String getStyleClass() {
            return styleClass;
        }
    }

    /**
     * Gửi yêu cầu NODE_STATUS bất đồng bộ tới Node đích.
     *
     * @return Future chứa {@link NodeStatus} nhận được, hoặc hoàn thành ngoại lệ nếu lỗi kết nối/timeout.
     */
    CompletableFuture<NodeStatus> getNodeStatus();

    /**
     * Nguồn dữ liệu của client này (REAL hoặc MOCK).
     */
    DataSource getDataSource();

    /**
     * Địa chỉ endpoint đang kết nối (ví dụ "127.0.0.1:9101" hoặc "mock://in-memory").
     */
    String getEndpoint();

    @Override
    default void close() {}
}
