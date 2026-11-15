package vn.nhom6.railway.client.network;

/**
 * Mức độ xác minh tính nhất quán của dữ liệu đọc sơ đồ ghế (TASK TV1-D15).
 * <p>
 * Theo SPEC-03 và SPEC-04, dữ liệu đọc từ Node chỉ được coi là tuyến tính hóa
 * (linearizable) khi đã vượt qua Read Barrier tại Leader (TV4-D19).
 * Trước khi read barrier được nghiệm thu, mọi dữ liệu đọc từ bất kỳ Node nào
 * đều là {@link #UNVERIFIED} và UI bắt buộc phải hiển thị rõ ràng nhãn này.
 */
public enum VerificationLevel {

    /**
     * Dữ liệu chưa qua read barrier (mặc định cho toàn bộ giai đoạn trước TV4-D19).
     * Chỉ lệnh BOOK qua Raft mới quyết định kết quả giao dịch.
     */
    UNVERIFIED("Chưa xác minh", "badge-unverified", "⚠ Dữ liệu chưa qua read barrier (UNVERIFIED)"),

    /**
     * Dữ liệu đã được xác minh tuyến tính hóa thông qua Raft Read Barrier (TV4-D19).
     */
    VERIFIED("Đã xác minh", "badge-verified", "✓ Dữ liệu đã xác minh qua Raft Read Barrier");

    private final String displayName;
    private final String styleClass;
    private final String description;

    VerificationLevel(String displayName, String styleClass, String description) {
        this.displayName = displayName;
        this.styleClass = styleClass;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getStyleClass() {
        return styleClass;
    }

    public String getDescription() {
        return description;
    }

    public boolean isVerified() {
        return this == VERIFIED;
    }
}
