# TV3-D27: Regression Protocol, Framing Fuzz & Concurrency + Kiểm Tra Log/Exception Từng Node

## 1. Thông tin Task
- **Mã task:** `tv3-d27`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md` (§5.10), `03-CONTRACTS.md` (§2 Framing `[1, 1 MiB]`, §3 Correlation/Error), `07-TESTING.md` (`TC-TCP-01..04`, `TC-AC03`, `TC-AC10`)
- **Phụ thuộc:** Network/codec tests từ cuối D26; fuzz và concurrency chạy trên port/test profile riêng (`port=0`, loopback `127.0.0.1`), không chờ TV6-D27.

## 2. Phát hiện & Cải tiến trong Code

### 2.1. Sửa lỗi nuốt cảnh báo Malformed Frame trong `TcpServer.java`
- **Nguyên nhân:** `FrameCodec.readFrame(in)` ném `IOException("Frame length out of bounds: ...")` khi gặp length âm (`-1`, `Integer.MIN_VALUE`), `0` hoặc quá lớn (`> 1_048_576`). Tuy nhiên `TcpServer.handleClient` trước đây chỉ bắt `IllegalArgumentException`, nên các malformed frame rơi xuống khối `catch (IOException)` ngoài cùng với `Level.FINE` mà không ghi nhận `WARNING`.
- **Khắc phục ([`TcpServer.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/TcpServer.java)):**
  - Phân tách rõ `EOFException` (tách riêng clean EOF vs `"Premature EOF"` truncated frame), `SocketTimeoutException`, và `IllegalArgumentException | IOException` trong vòng đọc frame, ghi `Level.WARNING` kèm `[port=...]`.
  - Bổ sung bộ đếm `malformedFrameCount` và `socketTimeoutCount` (`getMalformedFrameCount()`, `getSocketTimeoutCount()`).

### 2.2. Diagnostics & Thống nhất Log `[nodeId]` trong `MessageDispatcher.java`
- [`MessageDispatcher.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java):
  - Bổ sung record `DispatcherMetrics(completedCount, rejectedCount, exceptionCount, droppedCount, peakInFlightCount, currentInFlightCount)` qua `getMetrics()`.
  - Thống nhất helper `logAndCreateError(...)` gắn tiền tố `[nodeId]` và `channelType` trên toàn bộ nhánh reject; tăng `exceptionCount` + log `[nodeId]` khi handler hoặc `leaderRouter` ném ngoại lệ; ghi nhận `peakInFlightCount` qua CAS loop khi có request đồng thời.

## 3. Bộ Kiểm thử Regression D27 ([`ProtocolFuzzAndConcurrencyRegressionTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/ProtocolFuzzAndConcurrencyRegressionTest.java))
1. **`TC-D27-01` (Framing Fuzz trên cả 5 Node):** Tấn công đồng loạt cổng Client & Peer của `node-1..5` bằng length âm (`-1`, `-1024`, `Integer.MIN_VALUE`), `0`, quá lớn (`1_048_577`, `10_000_000`, `Integer.MAX_VALUE`), header/payload cắt cụt, byte UTF-8/JSON hỏng, key trùng và `protocolVersion=99`. Cả 5 Node không crash (`isRunning() == true`), `activeConnectionCount == 0`, ghi nhận đầy đủ `malformedFrameCount` & `WARNING` log, và phục vụ ngay `PING -> PONG` hợp lệ sau fuzz.
2. **`TC-D27-02` (12 Kết nối đồng thời + Fuzz xen kẽ):** Chạy 12 luồng song song vừa bắn fuzz length âm/lớn vừa gửi `GET_SEATS`/`BOOK_TICKET` hỗn hợp. Không deadlock, `peakInFlightCount >= 1`, `currentInFlightCount == 0`, và `100%` phản hồi khớp `correlationId` + `requestId`.
3. **`TC-D27-03` (Timeout, Response-Lost & Retry đồng thời):** 5 luồng đồng thời mất phản hồi đầu -> `UNKNOWN (TIMEOUT_UNKNOWN)` giữ nguyên `requestId`, truy vấn `GET_REQUEST_RESULT` khôi phục đúng `APPLIED_SUCCESS`, không đặt trùng vé.
4. **`TC-D27-04` (Kiểm toán Exception & Log theo từng Node):** Kích hoạt lỗi có kiểm soát riêng trên `node-2` (handler exception) và `node-4` (`TC-TCP-04` + unauthorized peer); xác nhận đúng `exceptionCount`/`rejectedCount` và log `[node-2]` / `[node-4]` cô lập theo từng Node.

## 4. Kết quả kiểm thử
- **Targeted Protocol/Fuzz/Concurrency Regression (`14` suites, `110/110 PASS`, `29.154 s`):** `shared` `28/28` (`0` skipped), `backend-node` `82/82` (`0` skipped).
- **Full `shared` + `backend-node` Regression (`BUILD SUCCESS`, `01:14 min`):** `shared` `38/38`, `backend-node` `1329` run / `0` failures / `0` errors / `146` DB-Docker-LAN skipped.
- **Bằng chứng:** [`artifacts/evidence/D27/TV3-D27-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D27/TV3-D27-result.log).
