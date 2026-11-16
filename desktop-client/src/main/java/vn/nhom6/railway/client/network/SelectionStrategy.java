package vn.nhom6.railway.client.network;

/**
 * Chiến lược lựa chọn Node ban đầu khi Desktop Client kết nối vào cụm phân tán (TASK TV1-D16).
 */
public enum SelectionStrategy {
    /**
     * Lựa chọn ngẫu nhiên trong danh sách các Node khả dụng.
     */
    RANDOM,

    /**
     * Lựa chọn xoay vòng (Round-Robin) lần lượt qua các Node khả dụng.
     */
    ROUND_ROBIN,

    /**
     * Ưu tiên kết nối tới Node chỉ định trước (preferredNode), nếu Node đó lỗi thì chuyển sang Node khác.
     */
    PREFERRED_FIRST;

    /**
     * Phân giải chuỗi sang {@link SelectionStrategy}, mặc định trả về {@link #ROUND_ROBIN} nếu chuỗi rỗng hoặc không khớp.
     */
    public static SelectionStrategy fromString(String val) {
        if (val == null || val.isBlank()) {
            return ROUND_ROBIN;
        }
        try {
            return SelectionStrategy.valueOf(val.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return ROUND_ROBIN;
        }
    }
}
