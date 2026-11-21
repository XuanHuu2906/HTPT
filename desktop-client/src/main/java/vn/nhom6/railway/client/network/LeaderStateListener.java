package vn.nhom6.railway.client.network;

import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;

/**
 * Listener nhận thông báo về sự thay đổi trạng thái Leader và Failover trong cụm (TASK TV1-D21).
 * <p>
 * Bất biến phân tán (AGENTS.md & SPEC-03):
 * <ul>
 *   <li>Không tự gửi lại lệnh BOOK/CANCEL khi chuyển trạng thái.</li>
 *   <li>Không suy diễn kết quả thành công/thất bại từ việc thay đổi Leader.</li>
 *   <li>Dữ liệu nguồn (REAL/FAKE) được phân định rõ ràng.</li>
 * </ul>
 */
public interface LeaderStateListener {

    /**
     * Kích hoạt khi phát hiện Leader hiện tại bị sập, ngắt kết nối hoặc timeout.
     *
     * @param oldLeaderId mã Node Leader vừa mất
     * @param reason      lý do mất kết nối
     */
    void onLeaderLost(String oldLeaderId, String reason);

    /**
     * Kích hoạt khi bắt đầu quy trình dò tìm / reconnect Leader mới trong cụm.
     */
    void onSearchingLeader();

    /**
     * Kích hoạt khi phát hiện ra Leader mới hợp lệ trong cụm phân tán.
     *
     * @param newLeaderId mã Node Leader mới
     * @param source      nguồn xác thực (REAL từ socket, FAKE từ fixture)
     */
    void onLeaderDiscovered(String newLeaderId, DataSource source);

    /**
     * Kích hoạt khi đã thăm dò hết các node mà không tìm thấy Leader khả dụng (cụm mất quorum hoặc đang bầu cử).
     */
    void onNoLeaderAvailable();
}
