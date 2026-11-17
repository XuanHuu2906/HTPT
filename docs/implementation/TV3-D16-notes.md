# TV3-D16 Implementation Notes: 5-Node TCP Network Integration & Cluster Smoke Test

> **Mã task:** `tv3-d16`  
> **Người thực hiện:** TV3 (Networking & Protocol)  
> **Ngày hoàn thành:** 2026-11-16  
> **Trạng thái:** HOÀN THÀNH (100% Tests Pass)

---

## 1. Mục tiêu và bối cảnh

Trong hệ thống phân tán đường sắt của Nhóm 6, cụm 5 Node (`node-1` đến `node-5`) đóng vai trò duy trì sự đồng thuận dữ liệu vé và chuyến tàu thông qua thuật toán Raft (Quorum 3/5).
Theo đặc tả kiến trúc **SPEC-02 §5**, **SPEC-03**, **SPEC-08 §3** và chỉ thị **AGENTS.md**:
1. **Phân bổ cổng và danh bạ Peer:**
   - Mỗi Node vận hành 2 TCP Server riêng biệt:
     - **Client Server:** Lắng nghe kết nối từ Client JavaFX và Monitor (cổng 9100 container / 9101..9105 dev host).
     - **Raft Peer Server:** Lắng nghe RPC đồng thuận giữa các Node (cổng 9200 container / 9201..9205 dev host).
   - Toàn cụm 5 Node có đúng 10 cổng mạng. Quá trình khởi động phải đảm bảo không xảy ra xung đột cổng (`BindException`).
   - Mỗi Node phải phân giải được danh bạ gồm đúng 4 peer còn lại ($5 \times 4 = 20$ kênh giao tiếp có hướng).
2. **Khả năng chịu tải mạng đồng thời (20 RPC/Node):**
   - Trước khi tích hợp trọn vẹn state machine và quorum election của TV4/TV5, tầng mạng TCP phải chứng minh khả năng chịu tải đồng thời tối thiểu 20 RPC/Node ($5 \times 20 = 100$ RPCs đồng thời full-mesh) mà không bị drop socket, timeout, hay rò rỉ bộ đệm frame.
3. **Log correlation theo Node / Term:**
   - Bắt buộc gắn kết chặt chẽ `messageId` của request với `correlationId` của response theo từng node và từng term, loại trừ hoàn toàn rủi ro nhầm lẫn phản hồi giữa các luồng đồng thời.

---

## 2. Các thành phần đã triển khai

### `backend-node/src/test/java/vn/nhom6/railway/networking/TcpClusterSmokeTest.java`

Triển khai bộ kiểm thử tích hợp mạng 5 Node toàn diện:

1. **Khởi tạo và giải phóng cụm 5 Node (`@BeforeEach startCluster` / `@AfterEach stopCluster`):**
   - Khởi động 5 tiến trình `NodeMain` trên cổng động (`0`), thu thập cặp cổng `clientPort` và `raftPort` thực tế cho từng Node.
   - Đăng ký `RequestVoteRpcHandler` và `AppendEntriesRpcHandler` (backed by `FakeRaftRequestHandler(currentTerm)`) vào `raftRegistry` của từng Node.
   - Thiết lập danh bạ 4 peer cho từng Node và khởi tạo 5 thực thể `RaftRpcClient` với bộ đệm in-flight multiplexing qua `RpcCorrelationRegistry`.
   - `@AfterEach`: Đóng sạch toàn bộ 5 `RaftRpcClient` và 5 `NodeMain`, bảo đảm không để lại socket treo hay thread daemon rò rỉ.

2. **Xác minh cổng và danh bạ Peer (`testClusterPortAllocationAndPeerListVerification`):**
   - Xác minh 10 cổng mạng được cấp phát thành công, đôi một phân biệt và `> 0`.
   - Xác minh danh bạ peer của mỗi Node gồm đúng 4 peer khác, không chứa chính mình.
   - Tích hợp và kiểm chứng parser `ElectionRuntime.parsePeers` (`node-1@host:port,...`) chuẩn hóa từ biến môi trường `NODE_PEERS`.

3. **Thực thi tải đồng thời 20 RPC/Node (`testConcurrentFullMeshRpcWorkload`):**
   - Mỗi Node trong 5 Node đồng thời phát đi 20 RPC:
     - 10 `REQUEST_VOTE` RPCs tới 4 peer.
     - 10 `APPEND_ENTRIES` RPCs tới 4 peer.
   - Tổng cộng 100 RPCs chạy đồng thời trên toàn mạng full-mesh.
   - Toàn bộ 100 `CompletableFuture` hoàn thành thành công trong thời gian dưới 1 giây.
   - Khẳng định 100% response có `correlationId` khớp `messageId`, `senderId` khớp target peer, và payload thành công (`isVoteGranted() == true`, `isSuccess() == true`).

4. **Theo dõi và ghi log correlation theo Node / Term (`testLogCorrelationByNodeAndTerm`):**
   - Thử nghiệm qua các term khác nhau (Term 1, Term 2, Term 5).
   - Sử dụng `client.exchange(...)` để theo dõi chính xác `messageId`.
   - Ghi log correlation định dạng:
     ```text
     [CORRELATION] sender=node-1 target=node-2 type=REQUEST_VOTE_RESPONSE reqId=... corrId=... term=1 status=SUCCESS latency=2ms
     ```
   - Xác minh 60/60 bản tin request-response khớp tuyệt đối correlationId và term.

5. **Cô lập ranh giới kênh mạng (`testChannelBoundaryAcrossFiveNodes`):**
   - Gửi `REQUEST_VOTE` / `APPEND_ENTRIES` tới Client port -> nhận `INVALID_REQUEST`.
   - Gửi `GET_SEATS` tới Raft peer port -> nhận `INVALID_REQUEST`.
   - Gửi raw frame `PING` tới Client port -> nhận `PONG`.

6. **Đồng thời truy vấn chẩn đoán Monitor (`testConcurrentMonitorStatusBroadcast`):**
   - Giả lập Desktop Monitor phát đồng thời 5 request `NODE_STATUS` tới Client port của cả 5 Node.
   - Cả 5 Node phản hồi `NODE_STATUS_RESPONSE` hợp lệ, chứa `observedAt` và `peerConnectivity` với đủ 4 peer.

7. **Hỗ trợ kiểm thử trực tiếp container Docker Compose (`testDockerClusterConnectivityIfAvailable`):**
   - Tự động dò trạng thái cụm container trên các cổng `9101..9105` và `9201..9205`.
   - Khi chạy trên môi trường có Docker Compose (`compose.dev.yml`), test tự động xác nhận tính sẵn sàng của cả 5 container; khi chạy trong môi trường không có Docker daemon (như sandbox hoặc CI), test ghi nhận và bỏ qua an toàn mà không làm fail build.

---

## 3. Bằng chứng kiểm thử

### 3.1. Kiểm thử tích hợp mạng (Targeted Suite)
- **Lệnh:**
  ```bash
  mvn -B -ntp test -Dtest=TcpClusterSmokeTest,RaftRpcTcpIntegrationTest,RaftRpcServerHandlersTcpIntegrationTest,NodeStatusTcpIntegrationTest,NodeMainTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
- **Kết quả:**
  - `TcpClusterSmokeTest`: 6/6 tests PASS (100 RPCs full-mesh trao đổi thành công, 0 lỗi bind cổng).
  - `RaftRpcTcpIntegrationTest`: 8/8 tests PASS.
  - `RaftRpcServerHandlersTcpIntegrationTest`: 6/6 tests PASS.
  - `NodeStatusTcpIntegrationTest`: 2/2 tests PASS.
  - `NodeMainTest`: 4/4 tests PASS.
  - **Tổng cộng: 26/26 tests PASS** trong 5.5s.

### 3.2. Toàn bộ Reactor Test (6 modules)
- **Lệnh:**
  ```bash
  HEADLESS="true" JAVA_TOOL_OPTIONS="-Djava.awt.headless=true -Dtestfx.robot=glass -Dtestfx.headless=true -Dprism.order=sw" mvn -B -ntp test
  ```
- **Kết quả:**
  - `railway-distributed-system`: SUCCESS
  - `shared`: SUCCESS (33/33 tests PASS)
  - `backend-node`: SUCCESS (898/898 tests PASS)
  - `desktop-client`: SUCCESS (185/185 tests PASS)
  - `desktop-monitor`: SUCCESS
  - `integration-tests`: SUCCESS
  - **Tổng kết: BUILD SUCCESS** (0 failures, 0 errors qua toàn bộ 6 modules).

---

## 4. Giao diện phối hợp và bàn giao

- **TV4 (Replication) & TV5 (Election):**
  - Tầng mạng TCP đã chứng minh khả năng truyền tải và ghép nối correlation cho 100 RPC đồng thời trên cụm 5 Node.
  - Khi hoàn tất logic G4 (Election + Log Replication), TV4/TV5 chỉ cần cắm các processor thật vào `RequestVoteRpcHandler` và `AppendEntriesRpcHandler` mà không cần chỉnh sửa giao thức socket.
- **TV6 (DevOps / Docker):**
  - Cấu hình cổng mạng (9101..9105 client, 9201..9205 peer) và biến môi trường `NODE_PEERS` trong `docker/compose.dev.yml` và `configs/dev/cluster.properties` hoàn toàn tương thích và khớp với cơ chế phân giải của backend.
