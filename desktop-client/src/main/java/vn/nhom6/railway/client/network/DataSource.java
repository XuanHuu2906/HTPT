package vn.nhom6.railway.client.network;

/**
 * Nguồn dữ liệu phục vụ hiển thị nhãn minh bạch trên UI (TASK TV1-D15).
 * <p>
 * Phân biệt rõ rệt giữa dữ liệu FAKE (kiểm thử adapter), MOCK (mô phỏng UI),
 * REAL (TCP Socket kết nối cụm Node thật), và NONE.
 */
public enum DataSource {
    FAKE("FAKE", "badge-fake"),
    MOCK("MOCK", "badge-mock"),
    REAL("REAL", "badge-real"),
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
