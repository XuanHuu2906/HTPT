package vn.nhom6.railway.client.network;

/**
 * Các kịch bản giả lập kiểm thử cho {@link MockBookingClient} (TASK D07).
 */
public enum MockScenario {
    SUCCESS("✓ SUCCESS (CONFIRMED)"),
    SEAT_ALREADY_BOOKED("⛔ SEAT_ALREADY_BOOKED (REJECTED)"),
    NOT_LEADER("⚡ NOT_LEADER (Gợi ý Leader: node-2)"),
    NO_QUORUM("⚡ NO_QUORUM (Cụm thiếu đa số)"),
    UNKNOWN("⚠ UNKNOWN / TIMEOUT (Chưa rõ kết quả)"),
    CONNECTION_LOST("❌ CONNECTION_LOST (Mất kết nối mạng)");

    private final String displayName;

    MockScenario(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
