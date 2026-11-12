# Ghi chú triển khai: Task TV3-D12 — Viết Raft RPC Server Handlers

> **Tài liệu kỹ thuật nội bộ:** Nhóm 6 · Module `backend-node/networking` · **Tác giả:** TV3 (Protocol & TCP).  
> **Tham chiếu chính:** [SPEC-02 §2 (Kiến trúc)](../specs/02-ARCHITECTURE.md), [SPEC-03 §2, §6, §7 (Contracts - Raft RPC)](../specs/03-CONTRACTS.md), [SPEC-04 (Raft)](../specs/04-RAFT.md), [SPEC-07 (Testing TC-TCP-03, TC-TCP-04)](../specs/07-TESTING.md), [SPEC-08 §64 (Deployment Port Isolation)](../specs/08-DEPLOYMENT.md).

---

## 1. Bối cảnh & Mục tiêu kỹ thuật

Sau khi hoàn thành Raft RPC Client ở ngày D11 (`RaftRpcClient`), nhiệm vụ ngày D12 là triển khai **Raft RPC Server Handlers** phía máy chủ tiếp nhận:
1. **`RequestVoteRpcHandler`**: Tiếp nhận và xử lý yêu cầu bỏ phiếu bầu cử `REQUEST_VOTE` từ Candidate, phản hồi `REQUEST_VOTE_RESPONSE` (SPEC-03 §6).
2. **`AppendEntriesRpcHandler`**: Tiếp nhận và xử lý nhịp tim heartbeat và sao chép log `APPEND_ENTRIES` từ Leader, phản hồi `APPEND_ENTRIES_RESPONSE` (SPEC-03 §6).
3. **Decoupled Boundary (`RaftRpcProcessor` & `FakeRaftRequestHandler`)**: Tách biệt ranh giới mạng transport (TV3) và consensus engine (TV4/TV5), cho phép kiểm thử inbound TCP độc lập mà không cần phụ thuộc vào tiến độ ngày D12 của TV4/TV5.

---

## 2. Thiết kế chi tiết & Ranh giới an toàn

### 2.1. `RaftRpcProcessor.java` & `FakeRaftRequestHandler.java`
- Định nghĩa interface `RaftRpcProcessor` gồm hai phương thức nghiệp vụ:
  ```java
  RequestVoteResponseDto handleRequestVote(RequestVoteRequestDto request, String senderId) throws Exception;
  AppendEntriesResponseDto handleAppendEntries(AppendEntriesRequestDto request, String senderId) throws Exception;
  ```
- `FakeRaftRequestHandler` đóng vai trò stub/mock độc lập:
  - Cho phép cấu hình term, cấp phiếu hoặc từ chối phiếu, chấp nhận hoặc từ chối append (log mismatch), và tiêm lỗi (fault injection).
  - Thu thập toàn bộ lịch sử gọi để phục vụ assertion trong các kịch bản kiểm thử.

### 2.2. Xử lý & Xác thực tại Server Handlers
- **Bảo đảm định danh (Sender Validation):**
  - Trong `RequestVoteRpcHandler`: bắt buộc `senderId == candidateId` và `senderId != localNodeId` (ngăn node tự vote cho mình qua RPC).
  - Trong `AppendEntriesRpcHandler`: bắt buộc `senderId == leaderId`.
- **Bảo đảm tương quan phản hồi (Correlation Guarantee):**
  - Mọi response envelope trả về bắt buộc gắn `correlationId == request.messageId`.
- **Fail-closed Error Mapping (SPEC-03 §7):**
  - Sai cấu trúc hoặc vi phạm trường -> `ERROR_RESPONSE(INVALID_REQUEST)`.
  - Lỗi nội bộ / storage failure -> `ERROR_RESPONSE(INTERNAL_ERROR)` có log chi tiết, không làm crash tiến trình Node.

---

## 3. Danh mục tệp đã triển khai

| STT | Đường dẫn tệp | Mô tả trách nhiệm |
|---|---|---|
| 1 | [`backend-node/src/main/java/.../networking/RaftRpcProcessor.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/RaftRpcProcessor.java) | Interface seam ranh giới giữa TCP transport và consensus engine. |
| 2 | [`backend-node/src/main/java/.../networking/FakeRaftRequestHandler.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/FakeRaftRequestHandler.java) | Cài đặt fake mock phục vụ kiểm thử độc lập cho TV3. |
| 3 | [`backend-node/src/main/java/.../networking/RequestVoteRpcHandler.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/RequestVoteRpcHandler.java) | Inbound handler cho REQUEST_VOTE trên PEER port. |
| 4 | [`backend-node/src/main/java/.../networking/AppendEntriesRpcHandler.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/AppendEntriesRpcHandler.java) | Inbound handler cho APPEND_ENTRIES trên PEER port. |
| 5 | [`backend-node/src/test/java/.../networking/RequestVoteRpcHandlerTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/RequestVoteRpcHandlerTest.java) | 7 unit tests kiểm tra toàn diện các kịch bản RequestVoteRpcHandler. |
| 6 | [`backend-node/src/test/java/.../networking/AppendEntriesRpcHandlerTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/AppendEntriesRpcHandlerTest.java) | 7 unit tests kiểm tra toàn diện các kịch bản AppendEntriesRpcHandler. |
| 7 | [`backend-node/src/test/java/.../networking/RaftRpcServerHandlersTcpIntegrationTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/RaftRpcServerHandlersTcpIntegrationTest.java) | 6 integration tests kiểm tra toàn bộ inbound TCP socket và ghép kênh 100 RPC. |

---

## 4. Nghiệm thu tiêu chí Pass

Tiêu chí giao việc: **"RPC server handlers + fake Raft processor unit/TCP tests PASS; tích hợp 5 Node thật chấm tại G4"**.
- 14/14 Unit tests PASS (7 cho RequestVoteRpcHandler, 7 cho AppendEntriesRpcHandler).
- 6/6 TCP Integration tests PASS trên socket TCP thật.
- Toàn bộ multi-module reactor `mvn -B -ntp clean test` PASS 100%.

Minh chứng chi tiết lưu tại: [`artifacts/evidence/D12/TV3-D12-result.log`](../../artifacts/evidence/D12/TV3-D12-result.log).
