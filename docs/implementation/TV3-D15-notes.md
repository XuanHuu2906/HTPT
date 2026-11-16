# TV3-D15 Implementation Notes: Node Status Handler & Monitor Diagnostics

> **Mã task:** `tv3-d15`  
> **Người thực hiện:** TV3 (Networking & Protocol)  
> **Ngày hoàn thành:** 2026-11-15  
> **Trạng thái:** HOÀN THÀNH (100% Tests Pass)

---

## 1. Mục tiêu và bối cảnh

Để phục vụ ứng dụng giám sát cụm (**Desktop Monitor — TV6**) và chẩn đoán trạng thái Node/Cluster thời gian thực, hệ thống cần cung cấp endpoint chẩn đoán `NODE_STATUS` theo quy chuẩn **SPEC-03 §5** và kiến trúc **SPEC-02 §5**.

### Các nguyên tắc và bất biến cốt lõi (SPEC-03 §5 & AGENTS.md §5):
1. **Quan sát cục bộ (Local Diagnostic Observation):**
   - Bản tin `NODE_STATUS` chỉ phản ánh quan sát tại chính Node nhận request tại thời điểm `observedAt`.
   - Được phục vụ trực tiếp trên Client port của mọi Node; không yêu cầu Node phải là Leader và không chạy qua Raft read barrier.
   - Dữ liệu có thể trễ (stale) so với tiến độ đồng thuận toàn cụm; Monitor không được suy ra log đã commit chỉ vì thấy `lastLogIndex` tăng.
2. **Chẩn đoán trung thực (Truthful Diagnostic):**
   - Khi Raft consensus hoặc tầng lưu trữ bền vững (durable storage) chưa sẵn sàng hoặc bị lỗi, `role` và `term` có thể là `null`.
   - Tuyệt đối không bịa đặt trạng thái giả (như giả định vô căn cứ `FOLLOWER` hay `term 0`) nhằm che giấu sự cố sập storage.
3. **Bản đồ kết nối ngang hàng (Peer Connectivity Map):**
   - Trong cụm 5 voter (`node-1` đến `node-5`), trường `peerConnectivity` luôn chứa đầy đủ đúng 4 peer (loại trừ chính Node trả lời).
   - Giá trị boolean biểu thị: `true` = lần RPC gần nhất thành công, `false` = thất bại hoặc timeout, `null` = chưa quan sát.
4. **Không phụ thuộc blocking TV4/TV5-D15:**
   - Sử dụng ranh giới trừu tượng `StatusProvider` cùng các adapter chuẩn (`fromRaftState`, `fromElectionComponent`) giúp handler hoạt động độc lập và tương thích ngay lập tức khi TV4/TV5 tích hợp.

---

## 2. Các thành phần đã triển khai

### 2.1. `shared/.../protocol/dto/NodeStatusDto.java`
- Khai báo DTO chuẩn hóa trao đổi qua giao thức TCP cho bản tin `NODE_STATUS_RESPONSE` (SPEC-03 §5).
- 10 trường dữ liệu chính xác theo hợp đồng:
  - `nodeId`: Định danh Node (`node-[a-zA-Z0-9_-]+`).
  - `role`: Vai trò Raft (`LEADER`, `FOLLOWER`, `CANDIDATE`, hoặc `null` nếu chưa khởi tạo).
  - `term`: Thuật ngữ Raft hiện tại (`>= 0` hoặc `null`).
  - `leaderId`: Định danh Leader được ghi nhận trong term hiện tại.
  - `lastLogIndex`: Chỉ số log cuối cùng đã ghi.
  - `commitIndex`: Chỉ số log đã đạt đồng thuận đa số.
  - `lastApplied`: Chỉ số log cao nhất đã apply vào State Machine / DB.
  - `storageHealthy`: Trạng thái kiểm tra sức khỏe phân vùng lưu trữ bền vững (boolean).
  - `peerConnectivity`: Map trạng thái kết nối tới 4 node ngang hàng.
  - `observedAt`: Dấu thời gian quan sát chuẩn ISO-8601 UTC truncated to millis.
- Hỗ trợ đầy đủ Jackson `@JsonProperty`, `@JsonCreator`, và hàm xác thực ràng buộc `validate()`.
- Bổ sung các phương thức tiện ích `@JsonIgnore`: `isLeader()`, `isFollower()`, `isCandidate()`, `isStorageHealthy()`.

### 2.2. `backend-node/.../networking/NodeStatusHandler.java`
- Implement cả `HandlerRegistry.RequestHandler` và `HandlerRegistry.StatusHandler` để tương thích hoàn toàn với cơ chế phân phối `MessageDispatcher` và `HandlerRegistry`.
- Cung cấp functional interface `StatusProvider` với:
  - `fromRaftState(...)`: Adapter kết nối trực tiếp với model `RaftState` (TV4-D01).
  - `fromElectionComponent(...)`: Adapter kết nối với `RaftElectionComponent` (TV5-D12).
  - `staticSnapshot(...)` & `uninitialized(...)`: Adapter phục vụ kiểm thử và trạng thái khởi động ban đầu.
- Tự động chuẩn hóa bản đồ `peerConnectivity` với đúng 4 peer, loại trừ self node.

### 2.3. Tích hợp vào `backend-node/.../networking/NodeMain.java`
- Thay thế đoạn lambda ad-hoc bằng instance chính thức `NodeStatusHandler` đăng ký vào `clientRegistry`:
  ```java
  NodeStatusHandler statusHandler = new NodeStatusHandler(config.nodeId(), () -> { ... });
  clientRegistry.registerStatusHandler(statusHandler);
  ```
- Kết nối trích xuất dữ liệu từ `startupState` và `RaftElectionComponent` khi có sẵn.

---

## 3. Bằng chứng kiểm thử

### 3.1. Unit Tests
- `shared/src/test/java/vn/nhom6/railway/protocol/dto/NodeStatusDtoTest.java`:
  - `testGoldenJsonSerialization`: Kiểm tra serialize/deserialize khớp 100% ví dụ mẫu SPEC-03 §5.
  - `testUninitializedConsensusState`: Kiểm tra chấp nhận role/term null khi chưa khởi tạo consensus.
  - `testValidationFailures`: Kiểm tra chặn các trường hợp sai lệch định dạng (vai trò lạ, term âm, nodeId rỗng).
  - **Kết quả:** 3/3 tests PASS.
- `backend-node/src/test/java/vn/nhom6/railway/networking/NodeStatusHandlerTest.java`:
  - `testLeaderStatusSnapshot`: Snapshot Leader kèm đầy đủ 4 peers.
  - `testFollowerStatusSnapshot`: Snapshot Follower kèm leader hint.
  - `testUninitializedConsensus`: Phản ánh trung thực trạng thái chưa khởi tạo, không sinh trạng thái giả.
  - `testCandidateRoleWithIncompletePeerMap`: Xử lý an toàn khi thiếu key peer.
  - `testStorageUnhealthy`: Báo cáo cờ lưu trữ gặp sự cố.
  - `testFromRaftStateAdapter`: Adapter tích hợp trực tiếp từ `RaftState`.
  - `testNullRequestReturnsNull`: An toàn đối với envelope null.
  - `testMessageCodecRoundTrip`: Mã hóa và giải mã trọn vẹn qua `MessageCodec`.
  - **Kết quả:** 8/8 tests PASS.

### 3.2. TCP Socket Integration Tests
- `backend-node/src/test/java/vn/nhom6/railway/networking/NodeStatusTcpIntegrationTest.java`:
  - `testNodeStatusOverTcp`: Khởi chạy NodeMain thật trên cổng động, kết nối `TcpClient`, gửi `NODE_STATUS` và nhận `NODE_STATUS_RESPONSE` giải mã thành công `NodeStatusDto`.
  - `testRepeatedMonitorPollingOverTcp`: Mô phỏng chu kỳ Monitor polling liên tục 5 lần trên cùng kết nối socket TCP.
  - **Kết quả:** 2/2 tests PASS.
- `NodeMainTest` (4/4 tests PASS, bao gồm subprocess startup test).
- `TcpSingleNodeProtocolTest` (12/12 tests PASS).

### 3.3. Toàn bộ Reactor Test
- Chạy lệnh kiểm thử toàn hệ thống:
  ```bash
  HEADLESS="true" JAVA_TOOL_OPTIONS="-Djava.awt.headless=true -Dtestfx.robot=glass -Dtestfx.headless=true -Dprism.order=sw" mvn -B -ntp test
  ```
- **Kết quả:**
  - `railway-distributed-system`: SUCCESS
  - `shared`: SUCCESS (33/33 tests PASS)
  - `backend-node`: SUCCESS (892/892 tests PASS)
  - `desktop-client`: SUCCESS (174/174 tests PASS)
  - `desktop-monitor`: SUCCESS
  - `integration-tests`: SUCCESS
  - **Tổng kết: BUILD SUCCESS** (0 failures, 0 errors).

---

## 4. Giao diện phối hợp và bàn giao

- **TV6 (Desktop Monitor):**
  - Có thể sử dụng trực tiếp class `NodeStatusDto` từ module `shared`.
  - Gửi bản tin `MessageType.NODE_STATUS` với payload rỗng `{}` tới Client port của bất kỳ Node nào trong 5 Node để lấy snapshot chẩn đoán.
- **TV4 / TV5 (Raft Core & Election):**
  - Đã có sẵn adapter `NodeStatusHandler.StatusProvider.fromRaftState(...)` và `fromElectionComponent(...)`.
  - Khi hoàn thiện các module consensus, chỉ cần truyền instance vào constructor của `NodeStatusHandler` hoặc callback trong `NodeMain`.
