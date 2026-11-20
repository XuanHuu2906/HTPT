# TV3-D20: Load test TCP/Socket Lifecycle và Bền vững Kết nối

## 1. Thông tin Task
- **Mã task:** `tv3-d20`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§5 TCP Socket), `03-CONTRACTS.md` (§2 Framing & Transport), `07-TESTING.md`
- **Ngày hoàn thành:** 20/11/2026

## 2. Mục tiêu kỹ thuật & Tiêu chí Nghiệm thu
1. **Kiểm thử tải đồng thời (Concurrent Load):**
   - 6 client đồng thời bắn 120 requests hỗn hợp (`GET_SEATS`, `BOOK_TICKET`, `GET_REQUEST_RESULT`) vào `TcpServer` / `NodeMain`.
   - Bảo đảm không xảy ra deadlock, không có ngoại lệ rơi rớt, tính toàn vẹn `correlationId` đạt 100%.
   - Bảo toàn bất biến an toàn (Safety Invariant): SE-001 chỉ có 10 ghế (G01–G10), số vé `CONFIRMED` không bao giờ vượt quá 10, toàn bộ request còn lại nhận `REJECTED(SEAT_ALREADY_BOOKED)`.
2. **Kiểm tra vòng đời kết nối & Rò rỉ tài nguyên (Connection Lifecycle & Leak Detection):**
   - Thực thi 50 chu kỳ kết nối (25 tuần tự + 25 song song qua client thread pool).
   - Kiểm tra `server.getActiveConnectionCount()` trở về `0` sau khi client ngắt kết nối.
   - Kiểm tra số lượng luồng (thread count) được kiểm soát trong phạm vi worker pool định mức của `TcpServer` (`maxThreads = 8`), không sinh luồng rò rỉ theo từng kết nối.
3. **Cô lập Client Chậm (Slow Client Isolation):**
   - 1 client gửi từng byte với độ trễ cố ý (chậm ~400ms) không được chiếm dụng worker pool hoặc làm nghẽn 5 fast clients khác.
   - 5 fast clients hoàn tất 50 requests chỉ trong vài mili-giây với 100% thành công.
4. **Xử lý ngắt kết nối đột ngột & Frame dị dạng (Abrupt Disconnect & Malformed Frames):**
   - Xử lý an toàn: zero-byte EOF, truncated frame header (2/4 bytes), truncated payload body, hard TCP RST (`setSoLinger(true, 0)`), và frame vượt quá giới hạn 1 MiB.
   - `TcpServer` bắt được `EOFException` / `SocketException` và đóng kết nối êm đẹp, giải phóng tài nguyên về 0 active sockets.
   - Server duy trì trạng thái hoạt động bình thường cho các kết nối kế tiếp.
5. **Kiểm soát hàng đợi & Giới hạn In-Flight (Queue Overload & Backpressure):**
   - Kiểm thử cấu hình khắt khe: pool chỉ có 2 threads và `maxInFlightRequests = 5`.
   - 30 requests dồn dập cùng lúc được phản hồi có kiểm soát (thành công hoặc mã lỗi `INTERNAL_ERROR`), hoàn toàn không bị treo hoặc deadlock.

## 3. Các thay đổi đã thực hiện
### 3.1. `backend-node` Module
- [`backend-node/src/main/java/vn/nhom6/railway/networking/NodeMain.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/NodeMain.java):
  - Bổ sung 2 getters công khai `public TcpServer getClientServer()` và `public TcpServer getRaftServer()` hỗ trợ giám sát kết nối hoạt động từ bên ngoài.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/TcpLoadTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/TcpLoadTest.java):
  - Bộ kiểm thử mới với 5 kịch bản:
    1. `testSixClientsConcurrentContentionAndThroughput`: 6 clients, 120 requests, 100% correlation match, zero deadlock.
    2. `testSocketConnectionLifecycleAndLeakDetection`: 50 kết nối tuần tự/song song, active sockets = 0, thread count bounded.
    3. `testSlowClientDoesNotStarveFastClients`: 1 slow client + 5 fast clients, fast clients hoàn thành < 2.5s.
    4. `testAbruptClientDisconnectionAndEofHandling`: 5 trường hợp ngắt kết nối đột ngột/frame dị dạng, dọn dẹp sạch.
    5. `testQueueOverloadAndInFlightBackpressure`: 30 requests dồn dập trên pool 2 threads, phản hồi có kiểm soát.

## 4. Kết quả kiểm thử thực tế
- **Kiểm thử có chủ đích (`TcpLoadTest`):**
  ```bash
  mvn -B -ntp test -Dtest=TcpLoadTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.004 s` (PASS 100%).
  - Bằng chứng: [`artifacts/evidence/D20/TV3-D20-targeted.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D20/TV3-D20-targeted.log).
- **Kiểm thử toàn bộ Reactor (6 modules):**
  ```bash
  mvn -B -ntp test
  ```
  - Kết quả: `BUILD SUCCESS` (0 failures, 0 errors qua toàn bộ modules `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`).
  - Bằng chứng: [`artifacts/evidence/D20/TV3-D20-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D20/TV3-D20-result.log).
