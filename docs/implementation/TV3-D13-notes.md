# TV3-D13 Implementation Notes: Leader Redirection & Forwarding

> **Mã task:** `tv3-d13`  
> **Người thực hiện:** TV3 (Networking & Protocol)  
> **Ngày hoàn thành:** 2026-11-13  
> **Trạng thái:** HOÀN THÀNH (100% Tests Pass)

---

## 1. Mục tiêu và bối cảnh

Trong mô hình kiến trúc phân tán Raft 5 node của dự án (SPEC-02 §4, SPEC-03 §7):
- Chỉ Node giữ vai trò `LEADER` mới được quyền xử lý các lệnh ghi trạng thái (`BOOK_TICKET`, `CANCEL_TICKET`) và read barrier (`GET_SEATS`).
- Khi Desktop Client kết nối tới một Follower hoặc Candidate, Node nhận bản tin không được tự ý thực thi nghiệp vụ hoặc xác nhận lệnh.
- Thay vào đó, Node phải:
  1. Trả về mã lỗi `NOT_LEADER` kèm `leaderHint` (địa chỉ hoặc node ID của Leader) và `term` hiện tại.
  2. Cung cấp cơ chế fallback danh sách node nếu `leaderHint` chưa xác định (tránh bế tắc cho Client).
  3. Tuyệt đối không trỏ vào chính mình khi không phải Leader (tránh redirect loop tức thời).
  4. Giữ nguyên vẹn `requestId` (UUID) của Client qua toàn bộ chu kỳ chuyển hướng.
  5. Hỗ trợ cả 2 phương thức:
     - **Chuyển hướng (Client Redirect):** [APPROVED-v1 theo SPEC-02 §4] — Node trả `NOT_LEADER` để Client tự kết nối lại.
     - **Chuyển tiếp (Transparent Proxy/Forwarding):** Node Follower chuyển tiếp bản tin tới Leader đúng 1 lần (`forwarded request đúng 1 lần`), kiểm soát vòng lặp chuyển tiếp (loop prevention).

---

## 2. Các thành phần đã triển khai

### 2.1. `ErrorReplyFactory.java`
- Đặt tại `vn.nhom6.railway.networking.ErrorReplyFactory`.
- Factory tập trung tạo các bản tin `ERROR_RESPONSE` (SPEC-03 §7) chuẩn hóa với đầy đủ 5 trường:
  - `errorCode`: String mã lỗi (`NOT_LEADER`, `INVALID_REQUEST`, `NO_QUORUM`, `READ_UNAVAILABLE`, `INTERNAL_ERROR`).
  - `message`: Diễn giải cho người dùng/log.
  - `leaderHint`: Nullable node ID hint (ví dụ `"node-1"`).
  - `term`: Nullable Raft term.
  - `requestId`: Nullable client UUID, được giữ nguyên vẹn từ request.
- Tự động gọi `dto.validate()` để bảo đảm tính tương thích tuyệt đối với `shared` DTO contract.
- Cung cấp hàm trích xuất an toàn `extractRequestId(envelope)` từ cả `JsonNode` lẫn các DTO có kiểu (như `BookTicketRequestDto`).

### 2.2. `LeaderRouter.java`
- Đặt tại `vn.nhom6.railway.networking.LeaderRouter`.
- Điều hướng và bảo vệ các handler nghiệp vụ ghi:
  - **`RoleProvider`**: Functional interface cung cấp `RoleSnapshot(role, term, leaderHint)`, hỗ trợ chuyển đổi từ TV5 `RaftNode`, `RaftElectionComponent`, hoặc `BookingService`.
  - **`resolveLeaderHint()`**:
    - Trả về `leaderHint` hiện tại nếu hợp lệ và khác `localNodeId`.
    - Tự động fallback sang danh sách các node voter còn lại trong cụm (`fallbackNodes`), loại trừ `localNodeId` để loại bỏ hoàn toàn tự trỏ (self-loop).
  - **`route(request)`**:
    - Khi vai trò là `LEADER`: Trả về `RoutingOutcome.executeLocally()`.
    - Khi vai trò là `FOLLOWER`/`CANDIDATE`:
      - Ở chế độ `REDIRECT`: Tạo `ERROR_RESPONSE(NOT_LEADER)` với `leaderHint`, `term`, `requestId`.
      - Ở chế độ `FORWARD`: Chuyển tiếp tới Leader thông qua `RequestForwarder`.
  - **Phòng chống Loop trong Forwarding**:
    - Kiểm tra `senderId`: Nếu đến từ một node voter trong cụm (peer), router lập tức từ chối forward tiếp và trả `NOT_LEADER`.
    - Kiểm tra `requestId`: Bounded cache ghi nhận các `requestId` đã forward, ngăn chặn forward trùng lặp.
    - Đảm bảo giới hạn bước nhảy tối đa = 1 (`đúng 1 lần`).

### 2.3. Tích hợp Handlers
- `BookTicketHandler` và `GetSeatsHandler`:
  - Tiếp nhận constructor nhận `LeaderRouter` (hoặc khởi tạo default router từ `BookingService`).
  - Gọi `leaderRouter.route(request)` ngay tại đầu hàm `handle()`. Nếu outcome là `handled()`, trả về ngay phản hồi chuyển hướng.

---

## 3. Bằng chứng kiểm thử

1. **Unit Tests (18 tests PASS):**
   - `ErrorReplyFactoryTest`: 9/9 tests PASS.
   - `LeaderRouterTest`: 9/9 tests PASS.
2. **TCP Socket Integration Tests (4 tests PASS):**
   - `LeaderRedirectionTcpIntegrationTest`:
     - Test 1: Gửi `BOOK_TICKET` tới Follower nhận hint `NOT_LEADER` với `leaderHint`, `term`, `requestId` nguyên vẹn.
     - Test 2: Client nhận hint từ Follower, tự kết nối lại sang Leader và đặt vé thành công (`CONFIRMED`).
     - Test 3: Follower chạy Forwarding Mode, chuyển tiếp tới Leader đúng 1 lần và trả về `CONFIRMED`.
     - Test 4: Chặn đứng vòng lặp chuyển tiếp khi nhận bản tin từ peer voter.
3. **Toàn bộ Reactor:**
   - 6/6 module compile và pass toàn bộ test suite (`BUILD SUCCESS`).
