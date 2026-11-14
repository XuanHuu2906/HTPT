package vn.nhom6.railway.client.storage;

/**
 * Trạng thái của một yêu cầu trong journal lưu trữ bền vững (SPEC-03 / TASK TV1-D14).
 * <p>
 * Bất biến:
 * 1. Trạng thái chỉ chuyển biến do sự kiện thật hoặc phản hồi có cấu trúc từ Node (hoặc fake test gắn nhãn).
 * 2. Tuyệt đối KHÔNG có chuyển trạng thái tự động thành công khi chưa nhận được xác nhận cuối cùng.
 */
public enum PendingState {
    /**
     * Đã ghi bền vững xuống journal trên đĩa (flush + force), chưa bắt đầu gửi qua mạng.
     */
    PREPARED,

    /**
     * Đã gửi hoặc đang trong quá trình gửi, nhưng chưa nhận được kết quả cuối cùng
     * (bao gồm các tình huống TIMEOUT_UNKNOWN, mất kết nối, hoặc response UNKNOWN).
     */
    SENT_UNRESOLVED,

    /**
     * Đã nhận được phản hồi kết quả cuối cùng được áp dụng bền vững từ Node (CONFIRMED hoặc REJECTED).
     */
    RESOLVED,

    /**
     * Xác định chắc chắn chưa gửi được tới mạng (ví dụ: lỗi validation trước gửi hoặc ghi journal thất bại).
     */
    FAILED_NOT_SENT,

    /**
     * Dữ liệu payload bị hư hỏng (sai lệch hash SHA-256) hoặc bị cắt dở không thể phục hồi an toàn.
     */
    CORRUPT;

    /**
     * Kiểm tra xem yêu cầu có đang ở trạng thái chờ xử lý (cần theo dõi hoặc tra cứu) hay không.
     */
    public boolean isPending() {
        return this == PREPARED || this == SENT_UNRESOLVED;
    }
}
