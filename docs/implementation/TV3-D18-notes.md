# TV3-D18: Giữ Correlation/RequestId Đúng Qua Redirect và Retry

## 1. Thông tin Task
- **Mã task:** `tv3-d18`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md` (FR-BOOK-01, FR-BOOK-02, AC01-AC04), `02-ARCHITECTURE.md` (§4 & ADR-002), `03-CONTRACTS.md` (§1, §3.0, §4.4 & §7)
- **Ngày hoàn thành:** 18/11/2026

## 2. Mục tiêu kỹ thuật & Safety Invariants
1. **Phân biệt rạch ròi 3 định danh:**
   - `messageId`: UUID của khung truyền tải TCP (transport level). Mỗi lần gửi hoặc retry phải tạo `messageId` mới.
   - `correlationId`: `null` trên frame request; trên frame response bắt buộc phải bằng chính xác `request.getMessageId()`.
   - `requestId`: UUID nghiệp vụ nằm trong payload (`BookTicketRequestDto.requestId`, `ErrorResponseDto.requestId`). Được tạo duy nhất 1 lần từ Client và được giữ nguyên vẹn 100% qua mọi lần redirect hay retry.
2. **Giới hạn In-Flight & Ngăn chặn trùng lặp Frame:**
   - Theo dõi các request đang được xử lý trong `MessageDispatcher` bằng `ConcurrentHashMap<String, InFlightContext>`.
   - Chặn request có `messageId` trùng đang xử lý -> trả `INVALID_REQUEST`.
   - Chặn quá tải in-flight (`activeInFlight.size() >= maxInFlightRequests`) -> trả `INTERNAL_ERROR`.
3. **Bảo vệ phản hồi muộn (Late Response Isolation):**
   - Đảm bảo `ensureStrictCorrelation` luôn gắn chặt `correlationId == request.messageId` trước khi trả ra kênh socket.
   - Phía Client (`SocketBookingClient`) kiểm tra chặt chẽ `expectedCorrelationId.equals(response.getCorrelationId())`; phản hồi muộn hoặc sai lệch định danh khung truyền bị hủy bỏ ngay lập tức, không bao giờ hoàn tất nhầm thao tác khác.
4. **Bảo toàn Idempotency & Zero Double-Booking:**
   - Khi client retry với cùng `requestId` nhưng `messageId` mới, Server xử lý qua `BookingStateMachine` / `JdbcBookingRepository` trả về đúng kết quả của `requestId` đó (cùng `ticketId`) và gắn đúng `correlationId` mới của frame retry.

## 3. Các thay đổi đã thực hiện
### 3.1. `backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`
- Khai báo hằng số `DEFAULT_MAX_IN_FLIGHT = 1000`.
- Khai báo record `InFlightContext(messageId, requestId, type, senderId, startTimeNanos)`.
- Bổ sung cấu trúc `ConcurrentMap<String, InFlightContext> activeInFlight` và cấu hình `maxInFlightRequests`.
- Cung cấp constructor mở rộng: `MessageDispatcher(HandlerRegistry, ChannelType, String nodeId, LeaderRouter, int maxInFlightRequests)`.
- Trong `dispatch(request)`:
  - Kiểm tra duplicate `messageId` đang in-flight -> trả `INVALID_REQUEST`.
  - Kiểm tra ngưỡng in-flight -> trả `INTERNAL_ERROR`.
  - Ghi nhận `InFlightContext` và bọc khối xử lý router / handler trong `try ... finally { activeInFlight.remove(messageId); }`.
  - Áp dụng phương thức `ensureStrictCorrelation` cho mọi response (cả từ `LeaderRouter` và từ `RequestHandler`) nhằm loại trừ tuyệt đối nguy cơ correlationId bị null hoặc lệch.
- Thêm các helper / inspection methods:
  - `getActiveInFlightCount()`
  - `isInFlight(String messageId)`
  - `getInFlightContext(String messageId)`
  - `getMaxInFlightRequests()`

### 3.2. `backend-node/src/test/java/vn/nhom6/railway/networking/MessageDispatcherTest.java`
- Thêm 5 unit tests toàn diện:
  1. `testSixIndependentRequestsNoInterference`: 6 request độc lập gửi đồng thời có `messageId` và `requestId` riêng biệt; tất cả 6 request hoàn thành với 6 ticket ID phân biệt và correlationId chính xác 1-1.
  2. `testRetryWithSameRequestIdPreservesIdempotencyAndNewCorrelation`: Lần 1 gửi (R1, M1) -> Confirmed Ticket 1 (corr M1); Lần 2 retry (R1, M2) -> Confirmed Ticket 1 (corr M2). Nghiệp vụ chỉ chạy 1 lần duy nhất, không double-booking.
  3. `testLateOrMismatchedCorrelationIdEnforcedByDispatcher`: Bắt buộc dispatcher chuẩn hóa lại `correlationId == request.messageId` ngay cả khi handler trả ra correlationId lệch.
  4. `testInFlightLimitEnforced`: Cấu hình max 1 request in-flight; request thứ 2 gửi đến khi request 1 đang chạy bị từ chối ngay với `INTERNAL_ERROR`.
  5. `testDuplicateInFlightMessageIdRejected`: Gửi cùng 1 `messageId` trong khi request đang in-flight bị từ chối ngay với `INVALID_REQUEST`.

### 3.3. `backend-node/src/test/java/vn/nhom6/railway/networking/LeaderRedirectionTcpIntegrationTest.java`
- Thêm 2 bài kiểm thử tích hợp mạng TCP thực thụ:
  1. `testSixConcurrentIndependentBookingRequestsOverTcp`: 6 luồng client TCP đồng thời kết nối vào Leader đặt ghế G01..G06; nhận về 6 vé hợp lệ với correlationId khớp khung truyền mạng tương ứng.
  2. `testFollowerRedirectThenLeaderRetryPreservesRequestIdAndIdempotencyOverTcp`:
     - Gửi đến Follower (reqId=R1, msgId=M1) -> nhận NOT_LEADER (corr=M1, hint=node-1).
     - Client chuyển hướng sang Leader với cùng `requestId=R1`, `messageId=M2` -> nhận CONFIRMED vé T1 (corr=M2).
     - Client retry lại Leader với cùng `requestId=R1`, `messageId=M3` -> nhận CONFIRMED cùng vé T1 (corr=M3).
     - Không xảy ra double-booking trên state machine.

## 4. Kết quả kiểm thử
- **Targeted Test Command:**
  ```bash
  mvn -B -ntp test -Dtest=MessageDispatcherTest,LeaderRouterTest,LeaderRedirectionTcpIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 40, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%).
  - Bằng chứng: `artifacts/evidence/D18/TV3-D18-targeted.log`.
- **Full Reactor Test Command:**
  ```bash
  mvn -B -ntp test
  ```
  - Kết quả: `BUILD SUCCESS` trên toàn bộ 6 modules (`railway-distributed-system`, `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`).
  - Bằng chứng: `artifacts/evidence/D18/TV3-D18-result.log`.
