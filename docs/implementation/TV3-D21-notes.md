# TV3-D21: Mô Phỏng Mạng Chia Cụm (TestNetworkFilter) & Hook Lọc Bản Tin

## 1. Thông tin Task
- **Mã task:** `tv3-d21`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§5 TCP Socket), `04-RAFT.md`, `07-TESTING.md` (TC-RAFT-06, TC-READ-01)
- **Ngày hoàn thành:** 21/11/2026

## 2. Mục tiêu kỹ thuật & Tiêu chí Nghiệm thu
1. **Hook Lọc & Drop Bản tin Không Kill Process (AGENTS.md §0, §5 & SPEC-07):**
   - Trong sự cố mạng thực tế (cáp lỏng, lỗi switch, router/firewall drop packet), các tiến trình trên cả 5 Node vẫn đang chạy bình thường, socket vẫn mở, timer vẫn đếm.
   - **Tuyệt đối không giả lập chia cụm bằng cách kill process:** Nếu kill process thì hệ điều hành sẽ gửi cờ TCP FIN/RST ngay lập tức, làm mất đi tính chất phân tán thực tế (hết hạn timeout, split vote, step-down).
   - Bổ sung hook lọc/hủy bản tin `TestNetworkFilter` tại tầng mạng (`MessageDispatcher` và `RaftRpcClient`) để can thiệp drop hoặc allow bản tin giữa các cặp `(fromPeer, toPeer)` theo kịch bản cấu hình động.
2. **Đa dạng Kịch bản Mô phỏng Phân vùng Mạng:**
   - **Majority/Minority Partition:** Phân vùng 3 Node đa số `{node-1, node-2, node-3}` vs 2 Node thiểu số `{node-4, node-5}`. Giao tiếp nội bộ mỗi phân vùng hoạt động $100\%$; giao tiếp xuyên phân vùng bị drop silently (khiến sender timeout tự nhiên).
   - **Partition Healing:** Khôi phục mạng (`heal()`) xóa bỏ toàn bộ ranh giới phân vùng, nối lại liên lạc ngay lập tức mà không cần restart socket/node.
   - **Single Node Isolation:** Cô lập hoàn toàn 1 Node (`isolate("node-3")`), mọi traffic vào/ra Node đó đều bị drop; 4 Node còn lại liên lạc thông suốt.
   - **Asymmetric Link Drop:** Mô phỏng lỗi đứt 1 sợi quang (unidirectional drop: `node-1 -> node-2` bị drop, chiều `node-2 -> node-1` vẫn thành công).
   - **Selective Drop by Message Type:** Drop có chọn lọc theo loại bản tin (ví dụ: chỉ drop `REQUEST_VOTE`, vẫn cho phép `APPEND_ENTRIES` heartbeats đi qua).
   - **Process Lifecycle Invariant:** Toàn bộ 5 Node và 10 TCP ports duy trì trạng thái `isRunning() == true` và socket binding trong suốt thời gian phân vùng.
   - **Egress Drop Hook:** Hỗ trợ kiểm thử drop ngay tại card mạng gửi thông qua `RaftRpcClient.RpcFilterHook`.

## 3. Các thay đổi đã thực hiện

### 3.1. `backend-node` Module
- [`backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java):
  - Bổ sung interface hàm `MessageFilterHook` với phương thức `boolean shouldDeliver(String senderId, String recipientNodeId, MessageEnvelope<?> message)`.
  - Bổ sung trường `private volatile MessageFilterHook filterHook;` cùng các getters/setters `getFilterHook()` và `setFilterHook(MessageFilterHook)`.
  - Trong phương thức `dispatch()`, kiểm tra `filterHook` ngay sau kiểm tra cách ly kênh/cổng và trước tracking in-flight. Khi hook trả về `false`, ghi log cảnh báo và trả về `null` (TcpServer không gửi frame phản hồi, mô phỏng blackhole packet loss trong phân vùng mạng).
- [`backend-node/src/main/java/vn/nhom6/railway/networking/RaftRpcClient.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/RaftRpcClient.java):
  - Bổ sung interface hàm `RpcFilterHook` với phương thức `boolean shouldDeliver(String localNodeId, String peerId, MessageEnvelope<?> message)`.
  - Bổ sung trường `private volatile RpcFilterHook filterHook;` cùng getters/setters `getFilterHook()` và `setFilterHook(RpcFilterHook)`.
  - Trong phương thức `sendEnvelope()`, kiểm tra hook trước khi mã hóa frame. Khi hook từ chối, trả về failed future với `SocketTimeoutException` để mô phỏng sự cố không gửi được gói tin ra khỏi card mạng.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/TestNetworkFilter.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/TestNetworkFilter.java):
  - Bộ điều khiển lỗi mạng kiểm thử độc lập, cài đặt đồng thời cả `MessageDispatcher.MessageFilterHook` và `RaftRpcClient.RpcFilterHook`.
  - Cung cấp các API topology: `partition(Set, Set)`, `partition(List<Set>)`, `isolate(nodeId)`, `deisolate(nodeId)`, `blockLink(from, to)`, `blockBiLink(nodeA, nodeB)`, `allowLink(from, to)`, `allowBiLink(nodeA, nodeB)`, `heal()`.
  - Cung cấp các API lọc chọn lọc: `dropMessageType(type)`, `dropMessageTypeOnLink(from, to, type)`, `addDropPredicate(predicate)`.
  - Hỗ trợ gắn/gỡ tự động vào cụm: `attachTo(node)`, `attachToCluster(cluster)`, `detachFromCluster(cluster)`, `attachToClients(clients)`, `detachFromClients(clients)`.
  - Quản lý metrics luồng an toàn: `getDroppedCount()`, `getDeliveredCount()`, `getDroppedCount(from, to)`, `resetMetrics()`.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/TestNetworkFilterTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/TestNetworkFilterTest.java):
  - Bộ 7 ca kiểm thử toàn diện trên cụm TCP 5 Node:
    1. `testMajorityMinorityPartitionOverTcpCluster`: Phân vùng 3 vs 2, đa số và thiểu số liên lạc nội bộ $100\%$, liên lạc xuyên phân vùng bị drop và timeout, 5 node duy trì alive.
    2. `testPartitionHealingRestoresCommunication`: Khôi phục mạng nối lại liên lạc ngay tức thì.
    3. `testSingleNodeIsolation`: Cô lập 1 node và khôi phục khi de-isolate.
    4. `testAsymmetricUnidirectionalLinkDrop`: Kiểm thử rớt gói tin 1 chiều (half-duplex).
    5. `testSelectiveRpcTypeDrop`: Drop `REQUEST_VOTE` có chọn lọc nhưng cho phép `APPEND_ENTRIES` heartbeats.
    6. `testAllProcessesStayAliveDuringPartition`: Khẳng định cả 5 tiến trình và 10 cổng socket tiếp tục mở và lắng nghe kết nối bình thường trong thời gian phân vùng mạng.
    7. `testClientEgressDropWithRaftRpcClientHook`: Kiểm thử egress drop tại client với exception timeout tức thời.

## 4. Kết quả kiểm thử thực tế

- **Kiểm thử có chủ đích (`TestNetworkFilterTest`):**
  ```bash
  mvn -B -ntp test -Dtest=TestNetworkFilterTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 4.436 s` (PASS 100%).
  - Bằng chứng: [`artifacts/evidence/D21/TV3-D21-targeted.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D21/TV3-D21-targeted.log).

- **Kiểm thử kết hợp các bài kiểm thử mạng cụm (`TestNetworkFilterTest`, `TcpClusterSmokeTest`, `TcpLoadTest`):**
  ```bash
  mvn -B -ntp test -Dtest=TestNetworkFilterTest,TcpClusterSmokeTest,TcpLoadTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 18, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%).

- **Kiểm thử toàn bộ Reactor Suite (6 modules):**
  ```bash
  mvn -B -ntp test
  ```
  - Kết quả: `BUILD SUCCESS` (0 failures, 0 errors trên toàn bộ `railway-distributed-system`, `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`).
  - Bằng chứng: [`artifacts/evidence/D21/TV3-D21-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D21/TV3-D21-result.log).
