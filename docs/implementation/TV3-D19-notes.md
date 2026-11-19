# TV3-D19: Chuẩn hóa Mapping Result và ERROR_RESPONSE theo SPEC-03

## 1. Thông tin Task
- **Mã task:** `tv3-d19`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§4 & ADR-002, ADR-003), `03-CONTRACTS.md` (§4.2, §4.4 & §7)
- **Ngày hoàn thành:** 19/11/2026

## 2. Mục tiêu kỹ thuật & Safety Invariants
1. **Phân tách rạch ròi BOOK outcome (SPEC-03 §4.2):**
   - `CONFIRMED`: Vé được cấp thành công sau khi Raft commit và apply vào DB (`ticketId != null`, `reasonCode == null`, `appliedLogIndex >= 1`).
   - `REJECTED`: Đã commit + apply và bị từ chối nghiệp vụ tất định (`ticketId == null`, `reasonCode ∈ {SEAT_ALREADY_BOOKED, INVALID_REQUEST}`, `appliedLogIndex >= 1`).
   - `UNKNOWN`: Server hết thời hạn chờ kết quả apply (`ticketId == null`, `reasonCode == TIMEOUT_UNKNOWN`, `appliedLogIndex == null`).
   - **Bất biến an toàn:** Lỗi routing, transport, storage, quorum **tuyệt đối không được chuyển thành kết quả từ chối nghiệp vụ `REJECTED`**!
2. **Chuẩn hóa trạng thái tra cứu `GET_REQUEST_RESULT` (SPEC-03 §4.4):**
   - `APPLIED_SUCCESS`: Lệnh đã commit+apply thành công (`commandType` non-null, `ticketId` non-null cho BOOK, `reasonCode == null`, `appliedLogIndex >= 1`).
   - `APPLIED_REJECTED`: Lệnh đã commit+apply và bị từ chối nghiệp vụ (`commandType` non-null, `ticketId == null` cho BOOK, `reasonCode` non-null, `appliedLogIndex >= 1`).
   - `PENDING`: Leader đang giữ thông tin lệnh đang xử lý nhưng chưa có kết quả apply (`commandType` non-null, các trường khác `null`).
   - `UNKNOWN`: Chưa có kết quả hoặc thông tin pending tại Leader sau read barrier (cả 4 trường `commandType`, `ticketId`, `reasonCode`, `appliedLogIndex` đều `null`).
   - Khi read barrier thất bại -> trả `ERROR_RESPONSE(READ_UNAVAILABLE)`.
   - Follower luôn trả `NOT_LEADER`.
3. **Phân loại lỗi đồng thuận và hạ tầng (SPEC-03 §7 `ERROR_RESPONSE`):**
   - `NOT_LEADER`: Node không phải Leader -> trả kèm `leaderHint`, `term`, `requestId`.
   - `NO_QUORUM`: Không thể cam kết ghi an toàn (quorum < 3/5) -> trả kèm `term`, `requestId`.
   - `READ_UNAVAILABLE`: Không chứng minh được tính hiện hành của dữ liệu đọc (read barrier thất bại/timeout) -> trả kèm `term`, `requestId`.
   - `REQUEST_ID_CONFLICT`: Cùng `requestId` nhưng payload/intent khác biệt -> trả kèm `requestId`.
   - `INVALID_REQUEST`: Vi phạm định dạng/schema request trước khi append.
   - `INTERNAL_ERROR`: Lỗi hệ thống nội bộ chưa phân loại.

## 3. Các thay đổi đã thực hiện
### 3.1. `shared` Module
- [`shared/src/main/java/vn/nhom6/railway/protocol/dto/GetRequestResultRequestDto.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/shared/src/main/java/vn/nhom6/railway/protocol/dto/GetRequestResultRequestDto.java):
  - Request DTO cho tra cứu trạng thái yêu cầu (`requestId` UUID hợp lệ).
- [`shared/src/main/java/vn/nhom6/railway/protocol/dto/GetRequestResultResponseDto.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/shared/src/main/java/vn/nhom6/railway/protocol/dto/GetRequestResultResponseDto.java):
  - Response DTO cho `GET_REQUEST_RESULT_RESPONSE` theo đúng bảng trạng thái SPEC-03 §4.4.
  - Hỗ trợ các factory methods: `appliedSuccess`, `appliedRejected`, `pending`, `unknown`.
- [`shared/src/test/java/vn/nhom6/railway/protocol/GetRequestResultDtoTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/shared/src/test/java/vn/nhom6/railway/protocol/GetRequestResultDtoTest.java):
  - 5 tests kiểm tra toàn diện round-trip JSON và validation các trạng thái.

### 3.2. `backend-node` Module
- [`backend-node/src/main/java/vn/nhom6/railway/networking/ResponseMapper.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/ResponseMapper.java):
  - Lớp chuyển đổi tập trung chuyển đổi `ApplyResult` sang `BookTicketResponseDto`, `ResultLookup.LookupResult` sang `GetRequestResultResponseDto`, và unrolling exception sang `ErrorResponseDto`.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/ErrorReplyFactory.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/ErrorReplyFactory.java):
  - Bổ sung `createRequestIdConflict(...)` và `createReadUnavailable(...)` có hỗ trợ `requestId`.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/GetRequestResultHandler.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/GetRequestResultHandler.java):
  - Handler xử lý `GET_REQUEST_RESULT`: redirect Follower với `NOT_LEADER`, tra cứu kết quả qua `ResultLookup`, map lỗi read barrier sang `READ_UNAVAILABLE`.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java):
  - Thêm `GET_REQUEST_RESULT` vào `LEADER_ONLY_MESSAGE_TYPES`.
  - Tích hợp `ResponseMapper.mapThrowable(...)` trong khối catch xử lý ngoại lệ: chuyển đổi chính xác `NoQuorumException` -> `NO_QUORUM`, `ReadUnavailableException` -> `READ_UNAVAILABLE`, `RequestIdConflictException` -> `REQUEST_ID_CONFLICT`.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/NodeMain.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/NodeMain.java):
  - Đăng ký `GetRequestResultHandler` vào `clientRegistry`.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/FakeBookingService.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/FakeBookingService.java):
  - Triển khai interface `ResultLookup` dựa trên `BookingStateMachine.storedResult(requestId)` phục vụ testing độc lập.

### 3.3. Unit & Integration Tests
- [`backend-node/src/test/java/vn/nhom6/railway/networking/ResponseMapperTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/ResponseMapperTest.java):
  - 15 unit tests kiểm thử 4 outcome của BOOK, 4 state của GET_REQUEST_RESULT, bất biến cô lập lỗi transport, exception unrolling.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/MessageDispatcherTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/MessageDispatcherTest.java):
  - Bổ sung 5 tests (nâng tổng số lên 28 tests) xác minh phân loại lỗi `NO_QUORUM`, `READ_UNAVAILABLE`, `REQUEST_ID_CONFLICT`, `NOT_LEADER` trên Follower cho `GET_REQUEST_RESULT`, và xử lý thành công trên Leader.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/LeaderRedirectionTcpIntegrationTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/LeaderRedirectionTcpIntegrationTest.java):
  - Bổ sung test TCP: Client đặt vé trên Leader -> tra cứu `GET_REQUEST_RESULT` trên Follower nhận `NOT_LEADER` -> tra cứu trên Leader nhận `APPLIED_SUCCESS` cùng `ticketId`.

## 4. Kết quả kiểm thử
- **Targeted Tests:**
  ```bash
  mvn -B -ntp test -Dtest=GetRequestResultDtoTest,ResponseMapperTest,MessageDispatcherTest,LeaderRouterTest,LeaderRedirectionTcpIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 61, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%).
  - Bằng chứng: [`artifacts/evidence/D19/TV3-D19-targeted.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D19/TV3-D19-targeted.log).
- **Full Reactor Tests:**
  ```bash
  mvn -B -ntp test
  ```
  - Kết quả: `BUILD SUCCESS` trên toàn bộ 6/6 module.
  - Bằng chứng: [`artifacts/evidence/D19/TV3-D19-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D19/TV3-D19-result.log).
