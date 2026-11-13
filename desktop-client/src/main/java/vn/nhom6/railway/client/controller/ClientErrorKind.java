package vn.nhom6.railway.client.controller;

/**
 * Phân loại các lỗi phát sinh thuần túy ở phía ứng dụng Client (kết nối mạng, timeout cục bộ, lỗi định dạng).
 * <p>
 * Lưu ý theo SPEC-03: Nhóm lỗi này hoàn toàn tách biệt với {@code errorCode} và {@code reasonCode}
 * của máy chủ server để tránh nhầm lẫn logic và trạng thái.
 */
public enum ClientErrorKind {
    CONNECTION_LOST("Mất kết nối tới cụm máy chủ", "status-disconnected", true),
    CONNECTION_FAILED("Không thể kết nối tới cụm máy chủ", "status-disconnected", true),
    READ_TIMEOUT("Hết thời gian chờ phản hồi từ máy chủ", "booking-status-unknown", true),
    NOT_LEADER_UNRESOLVED("Không tìm thấy Node Leader sau số lần thử lại tối đa", "status-error", true),
    CORRUPTED_FRAME("Khung dữ liệu phản hồi bị lỗi hoặc sai định dạng", "status-error", false),
    UNEXPECTED_RESPONSE_TYPE("Loại phản hồi không hợp lệ", "status-error", false),
    CLIENT_BUSY("Hệ thống đang bận xử lý yêu cầu trước đó", "status-error", true),
    INTERNAL_CLIENT_ERROR("Lỗi xử lý nội bộ phía ứng dụng", "status-error", false);

    private final String description;
    private final String styleClass;
    private final boolean retryable;

    ClientErrorKind(String description, String styleClass, boolean retryable) {
        this.description = description;
        this.styleClass = styleClass;
        this.retryable = retryable;
    }

    public String getDescription() {
        return description;
    }

    public String getStyleClass() {
        return styleClass;
    }

    public boolean isRetryable() {
        return retryable;
    }
}

