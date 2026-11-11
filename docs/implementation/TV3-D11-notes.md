# Ghi chú triển khai: Task TV3-D11 — Viết Raft RPC Client

> **Tài liệu kỹ thuật nội bộ:** Nhóm 6 · Module `backend-node/networking` · **Tác giả:** TV3 (Protocol & TCP).  
> **Tham chiếu chính:** [SPEC-02 §2 (Kiến trúc)](../specs/02-ARCHITECTURE.md), [SPEC-03 §2, §6 (Contracts - Raft RPC)](../specs/03-CONTRACTS.md), [SPEC-04 (Raft)](../specs/04-RAFT.md), [SPEC-07 (Testing TC-TCP-04)](../specs/07-TESTING.md), [SPEC-08 §64 (Deployment Port Isolation)](../specs/08-DEPLOYMENT.md).

---

## 1. Bối cảnh & Mục tiêu kỹ thuật

Sau khi hoàn thiện gia cố điều phối và bộ kiểm thử giao thức TCP ở ngày D10, nhiệm vụ ngày D11 là xây dựng **Raft RPC Client bất đồng bộ** (`RaftRpcClient.java`) phục vụ giao tiếp giữa các node Raft trong cụm 5 node:
1. **Asynchronous Non-blocking API**: Các lời gọi `sendRequestVote` và `sendAppendEntries` trả về `CompletableFuture`, không làm nghẽn luồng xử lý Raft.
2. **Correlation ID Tracking**: Ghép nối chính xác từng cặp request-response qua `messageId` và `correlationId`, hỗ trợ ghép kênh (multiplexing) nhiều RPC đồng thời trên cùng một luồng TCP.
3. **Persistent Connection Reuse**: Duy trì kết nối TCP bền vững cho mỗi peer (`1 socket / peer`), tái sử dụng qua nhiều vòng bầu cử và nhịp tim heartbeat (mỗi 50–150ms), loại bỏ hoàn toàn nguy cơ cạn kiệt ephemeral ports (`TIME_WAIT`) trên OS.
4. **Timeout & Backpressure Defense**:
   - Tự động hủy và ném `TimeoutException` khi peer không phản hồi trong deadline định trước.
   - Giới hạn số lượng in-flight RPCs trên mỗi peer (`maxInFlightPerPeer`), từ chối nhanh (`RpcBackpressureException`) khi vượt ngưỡng để chống tích tụ hàng đợi (buffer bloat).
5. **Tiêu chuẩn DTO**: Đóng băng và bổ sung 2 Response DTOs theo SPEC-03 §6 vào `shared`:
   - `RequestVoteResponseDto`: `{"term": 8, "voteGranted": true}`.
   - `AppendEntriesResponseDto`: `{"term": 8, "success": true, "matchIndex": 43}`.

---

## 2. Kiến trúc & Thiết kế chi tiết

### 2.1. `RpcCorrelationRegistry.java`
Bộ điều phối correlation trung tâm chịu trách nhiệm:
- Lưu trữ các RPC đang chờ trong `ConcurrentHashMap<String, PendingRpc<T>>`.
- Định thời gian sống (deadline) bằng một daemon `ScheduledExecutorService` duy nhất; khi hết hạn gọi `onTimeout()` hủy future và giảm bộ đếm in-flight.
- Đếm số lượng in-flight bằng `AtomicInteger` trên từng peer qua thuật toán CAS.
- Xử lý chuyển đổi typed DTO an toàn khi nhận envelope: hỗ trợ cả `JsonNode` (từ socket raw) lẫn DTO typed, tự động phát hiện `ERROR_RESPONSE` để ném `IOException` tường minh.
- Hỗ trợ `failAll(peerId, error)` hủy đồng loạt các RPC đang chờ khi phát hiện đứt kết nối vật lý.

### 2.2. `RaftRpcClient.java`
- Quản lý bản đồ kết nối `ConcurrentMap<String, PeerConnection> connections`.
- Lớp nội bộ `PeerConnection`:
  - Socket cấu hình `TCP_NODELAY = true` và `SO_KEEPALIVE = true`.
  - Khóa tuần tự hóa `writeLock`: Mọi thao tác `FrameCodec.writeFrame` được bảo vệ bởi khóa này, đảm bảo không có 2 luồng nào ghi đè frame 4-byte BE lên nhau (SPEC-03 §2).
  - Luồng nền `PeerReaderThread`: Liên tục đọc frame từ `InputStream`, parse envelope và bàn giao ngay cho `RpcCorrelationRegistry.complete(correlationId, envelope)`.
  - Khi luồng đọc gặp `EOFException` hoặc lỗi mạng: đóng socket, gọi `failAll(peerId, error)` và giải phóng connection để lần gửi sau tự động kết nối lại (lazy reconnection).

---

## 3. Danh mục tệp đã thêm mới

| STT | Đường dẫn tệp | Trách nhiệm |
|---|---|---|
| 1 | [`shared/src/main/java/.../dto/RequestVoteResponseDto.java`](../../shared/src/main/java/vn/nhom6/railway/protocol/dto/RequestVoteResponseDto.java) | Wire DTO phản hồi phiếu bầu cử theo SPEC-03 §6. |
| 2 | [`shared/src/main/java/.../dto/AppendEntriesResponseDto.java`](../../shared/src/main/java/vn/nhom6/railway/protocol/dto/AppendEntriesResponseDto.java) | Wire DTO phản hồi sao chép log / heartbeat theo SPEC-03 §6. |
| 3 | [`backend-node/src/main/java/.../networking/RpcBackpressureException.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/RpcBackpressureException.java) | Ngoại lệ ném khi vượt ngưỡng in-flight RPCs trên peer. |
| 4 | [`backend-node/src/main/java/.../networking/RpcCorrelationRegistry.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/RpcCorrelationRegistry.java) | Quản lý vòng đời correlation, timeout và backpressure. |
| 5 | [`backend-node/src/main/java/.../networking/RaftRpcClient.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/RaftRpcClient.java) | Client bất đồng bộ gửi Raft RPC với persistent connection. |
| 6 | [`shared/src/test/java/.../protocol/RaftResponseDtoTest.java`](../../shared/src/test/java/vn/nhom6/railway/protocol/RaftResponseDtoTest.java) | Unit test kiểm thử serde JSON và validation của 2 DTOs mới. |
| 7 | [`backend-node/src/test/java/.../networking/RpcCorrelationRegistryTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/RpcCorrelationRegistryTest.java) | Unit test kiểm thử timeout, backpressure, failAll và cleanup. |
| 8 | [`backend-node/src/test/java/.../networking/ProcessRaftNode.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/ProcessRaftNode.java) | Standalone Node runner hỗ trợ test đa tiến trình JVM qua TCP. |
| 9 | [`backend-node/src/test/java/.../networking/RaftRpcTcpIntegrationTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/RaftRpcTcpIntegrationTest.java) | Bộ 8 integration test bao gồm trao đổi đa tiến trình Java thật. |

---

## 4. Nghiệm thu tiêu chí Pass

Tiêu chí giao việc: **"2 process Java trao đổi VOTE và APPEND qua TCP"**.
- Đã được chứng minh qua test `testTwoJavaProcessesExchangeVoteAndAppendOverTcp()` trong `RaftRpcTcpIntegrationTest`:
  - Tiến trình cha (Node 1) khởi động tiến trình con JVM (`ProcessRaftNode`) qua `ProcessBuilder`.
  - Tiến trình con mở socket TCP trên cổng ngẫu nhiên và thông báo `READY:<port>`.
  - Tiến trình cha dùng `RaftRpcClient` gửi `REQUEST_VOTE` -> tiến trình con xử lý và trả về `REQUEST_VOTE_RESPONSE` qua TCP.
  - Tiến trình cha gửi `APPEND_ENTRIES` (kèm log entry) -> tiến trình con xử lý và trả về `APPEND_ENTRIES_RESPONSE` qua TCP.
  - Cả 2 RPC đều thành công 100%, payload khớp hoàn toàn.

Minh chứng log chi tiết lưu tại: [`artifacts/evidence/D11/TV3-D11-result.log`](../../artifacts/evidence/D11/TV3-D11-result.log).
