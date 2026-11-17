# TV3-D17: Điều hướng BOOK_TICKET đến Leader bằng NOT_LEADER Redirect

## 1. Thông tin Task
- **Mã task:** `tv3-d17`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§4 & §9 ADR-002), `03-CONTRACTS.md` (§4.4 & §7)
- **Ngày hoàn thành:** 17/11/2026

## 2. Các thay đổi đã thực hiện
### 2.1. `backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`
- Bổ sung trường `leaderRouter` (volatile) và constructor mở rộng `MessageDispatcher(HandlerRegistry, ChannelType, String nodeId, LeaderRouter leaderRouter)`.
- Duy trì 100% backward compatibility cho các constructor cũ (mặc định `leaderRouter = null`).
- Định nghĩa tập lệnh ghi yêu cầu quyền Leader: `LEADER_ONLY_MESSAGE_TYPES = Set.of(BOOK_TICKET, CANCEL_TICKET)`.
- Thêm bước chặn trước (Step 9) trong `dispatch(request)`:
  - Khi nhận yêu cầu ghi trên cổng Client (`BOOK_TICKET` hoặc `CANCEL_TICKET`), tham khảo `leaderRouter.route(request)`.
  - Nếu node là `FOLLOWER` hoặc `CANDIDATE`: trả ngay `ERROR_RESPONSE(NOT_LEADER)` kèm `leaderHint`, `term` và bảo toàn nguyên vẹn `requestId`.
  - **Không bao giờ gọi domain booking handler** trên Follower.
  - Đảm bảo **Follower không tự ghi DB hay trả CONFIRMED**.
- Bổ sung cơ chế xử lý ngoại lệ an toàn `BookingRequestHandler.NotLeaderException`: chuyển đổi thành `ERROR_RESPONSE(NOT_LEADER)` chuẩn thay vì trả về `INTERNAL_ERROR`.

### 2.2. `backend-node/src/main/java/vn/nhom6/railway/networking/LeaderRouter.java`
- Thêm getter `getRoleSnapshot()` và `getRoleProvider()` phục vụ truy xuất term và role hiện tại.

### 2.3. `backend-node/src/main/java/vn/nhom6/railway/networking/NodeMain.java`
- Khởi tạo `LeaderRouter` linh hoạt qua `resolveLeaderRouter`:
  - Khi có `RaftElectionComponent election != null`: khởi tạo `LeaderRouter` dựa trên trạng thái đồng thuận Raft thật.
  - Khi có `bookingService` tùy biến (e.g. `FakeBookingService` trong các bài test mạng): khởi tạo `LeaderRouter` dựa trên `bookingService.isLeader()`.
  - Khi chạy chế độ kiểm thử commit bền vững `persistentBooking != null && election == null`: để `leaderRouter = null` nhằm phân quyền xử lý trực tiếp cho `CommandSubmitter` của `BookingRequestHandler` (tránh chặn nhầm harness trong `JdbcBookingTcpIntegrationTest`).
- Thêm getter `getClientDispatcher()` và `getRaftDispatcher()`.

### 2.4. `backend-node/src/test/java/vn/nhom6/railway/networking/MessageDispatcherTest.java`
- Bổ sung 6 unit tests kiểm tra:
  1. `testFollowerRedirectsBookTicketWithLeaderHint`: Follower redirect với đầy đủ hint, term, requestId.
  2. `testFollowerNeverInvokesBookingHandler`: Khẳng định handler ghi DB có call count = 0 trên Follower.
  3. `testLeaderExecutesBookingHandlerNormally`: Leader thực thi booking bình thường và trả `CONFIRMED`.
  4. `testFollowerFallbackWhenLeaderHintNullOrSelf`: Khi `leaderHint` null hoặc trỏ về chính mình, router chọn candidate từ peer cluster nodes (không bao giờ tự gợi ý chính mình).
  5. `testNotLeaderExceptionHandledGracefully`: Bắt `NotLeaderException` và trả về `NOT_LEADER` response thay vì `INTERNAL_ERROR`.
  6. `testFollowerRedirectsCancelTicket`: Kiểm tra lệnh `CANCEL_TICKET` cũng được chuyển hướng an toàn.

### 2.5. `backend-node/src/test/java/vn/nhom6/railway/networking/LeaderRedirectionTcpIntegrationTest.java`
- Bổ sung 2 bài test TCP đầu-cuối:
  1. `testDispatcherLevelRedirectGuaranteesZeroDatabaseWrites`: Follower node qua `NodeMain` trả `NOT_LEADER`, booking service gọi 0 lần.
  2. `testFollowerFallbackCandidatesWhenHintNullOrStaleOverTcp`: Kiểm tra luân chuyển fallback node qua TCP khi hint null/stale.

## 3. Đối chiếu SPEC & Safety Invariants
- **SPEC-02 §4 & ADR-002 [APPROVED-v1]:** Redirect thay vì proxy server-side. Follower trả `NOT_LEADER` kèm `leaderHint` để Desktop Client tự kết nối lại.
- **SPEC-03 §7:** Payload `ERROR_RESPONSE` có đầy đủ `errorCode`, `message`, `leaderHint`, `term`, `requestId`.
- **RequestId Invariance:** `requestId` nghiệp vụ được giữ nguyên vẹn 100% qua các lần redirect và retry.
- **Không hardcode Leader:** Leader hint được lấy động từ trạng thái đồng thuận; khi không có hint thì xoay vòng fallback qua danh bạ peer, không bao giờ ép buộc Node 1.
