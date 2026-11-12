package vn.nhom6.railway.client.network;

import java.time.Instant;

/**
 * Bản ghi trạng thái Node quan sát được theo đặc tả SPEC-03 Section 5.
 *
 * <p>Các trường phản ánh trực tiếp payload của bản tin {@code NODE_STATUS_RESPONSE}:
 * <ul>
 *   <li>{@code nodeId}: ID của Node trả lời (ví dụ "node-1", "node-2", ... hoặc null khi chưa khởi tạo).</li>
 *   <li>{@code role}: Vai trò Raft ("LEADER", "FOLLOWER", "CANDIDATE", hoặc null/UNKNOWN).</li>
 *   <li>{@code term}: Raft term hiện tại (hoặc null nếu chưa khởi tạo Raft).</li>
 *   <li>{@code leaderId}: ID của Leader hiện tại theo góc nhìn của Node này (hoặc null).</li>
 *   <li>{@code lastLogIndex}: Chỉ số log cuối cùng.</li>
 *   <li>{@code commitIndex}: Chỉ số commit hiện tại.</li>
 *   <li>{@code lastApplied}: Chỉ số state machine đã apply.</li>
 *   <li>{@code storageHealthy}: Tình trạng lưu trữ bền vững (true/false/null).</li>
 *   <li>{@code observedAt}: Thời điểm ghi nhận quan sát.</li>
 * </ul>
 */
public record NodeStatus(
        String nodeId,
        String role,
        Long term,
        String leaderId,
        Long lastLogIndex,
        Long commitIndex,
        Long lastApplied,
        Boolean storageHealthy,
        Instant observedAt
) {

    public NodeStatus {
        if (observedAt == null) {
            observedAt = Instant.now();
        }
    }

    /**
     * Tạo trạng thái rỗng / UNKNOWN ban đầu.
     */
    public static NodeStatus emptyUnknown() {
        return new NodeStatus(null, "UNKNOWN", null, null, null, null, null, null, Instant.now());
    }

    public boolean isLeader() {
        return "LEADER".equalsIgnoreCase(role);
    }

    public boolean isFollower() {
        return "FOLLOWER".equalsIgnoreCase(role);
    }

    public boolean isCandidate() {
        return "CANDIDATE".equalsIgnoreCase(role);
    }
}
