package vn.nhom6.railway.client.network;

import java.io.IOException;

/**
 * Ngoại lệ khi Node từ chối xử lý vì không phải là Leader (SPEC-03 §7 và TASK TV1-D12).
 * <p>
 * Mang theo {@code leaderHint} (nếu có) để {@link LeaderAwareBookingClient} chuyển hướng yêu cầu.
 */
public class NotLeaderException extends IOException {
    private final String leaderHint;

    public NotLeaderException(String message, String leaderHint) {
        super(message != null ? message : "Node không phải là Leader (NOT_LEADER)");
        this.leaderHint = leaderHint;
    }

    public String getLeaderHint() {
        return leaderHint;
    }
}
