# TV3-D14 Implementation Notes: Safe ConnectionManager & Timeout Reconnect

> **Mã task:** `tv3-d14`  
> **Người thực hiện:** TV3 (Networking & Protocol)  
> **Ngày hoàn thành:** 2026-11-14  
> **Trạng thái:** HOÀN THÀNH (100% Tests Pass)

---

## 1. Mục tiêu và bối cảnh

Trong hệ thống phân tán, các sự cố timeout mạng (kết nối chậm, đứt gói, read/write timeout) là các lỗi một phần (partial failure) không thể tránh khỏi.
Theo **SPEC-00 điều 10** và **AGENTS.md §5**:
- **Bất biến an toàn tối cao:** *"Timeout hoặc mất phản hồi là **chưa biết kết quả (UNKNOWN)**, không được đổi thành `FAILED` hay `REJECTED` bằng suy đoán."*
- Nếu một lệnh ghi `BOOK_TICKET` đã được ghi vào stream TCP nhưng phía gửi gặp `SocketTimeoutException`:
  - Phía nhận (Leader) có thể đã nhận, commit và apply thành công.
  - Việc phía gửi tự ý đổi thành `REJECTED` hay `BOOK_FAILED` sẽ dẫn tới sai lệch dữ liệu nghiêm trọng (người dùng tưởng thất bại và đặt lại, gây trùng vé).
  - Trạng thái trả về **bắt buộc là `UNKNOWN`** với mã `TIMEOUT_UNKNOWN`.
  - Toàn bộ chu kỳ retry / lookup phải giữ **nguyên vẹn 100% `requestId`** ban đầu.
- Khi socket gặp timeout giữa chừng, frame trên TCP stream có thể bị phân mảnh hoặc đọc dở dang (poisoned socket). Kết nối bắt buộc phải được đóng sạch (`close()`) và kết nối lại (`reconnect()`) trước khi gửi bản tin mới.

---

## 2. Các thành phần đã triển khai

### 2.1. `RetryPolicy.java`
- Định nghĩa các hằng số và tham số chính sách:
  - `maxAttempts`: Số lần thử tối đa khi gặp lỗi kết nối tạm thời (mặc định: 3 lần).
  - `initialBackoffMs`: Thời gian chờ ban đầu (mặc định: 100ms).
  - `maxBackoffMs`: Thời gian chờ tối đa (mặc định: 1000ms).
  - `backoffMultiplier`: Hệ số nhân exponential backoff (mặc định: 2.0).
  - `connectTimeoutMs`: Timeout thiết lập kết nối TCP (mặc định: 3000ms).
  - `readTimeoutMs`: Socket `SO_TIMEOUT` (mặc định: 5000ms).
- Phân biệt rõ:
  - `isTransientConnectionError(ex)`: Nhận diện lỗi kết nối trước khi gửi (`ConnectException`, `NoRouteToHostException`) có thể thử lại an toàn.
  - `isTimeout(ex)`: Nhận diện `SocketTimeoutException`.

### 2.2. `ResultLookup.java` & `FakeResultLookup.java`
- Định nghĩa interface `ResultLookup` (SPEC-03 §4.4) tra cứu kết quả theo `requestId` khi xảy ra timeout.
- Cung cấp `FakeResultLookup` phục vụ kiểm thử độc lập mà không cần chờ TV2 bàn giao D19.
- Hỗ trợ các trạng thái chuẩn: `APPLIED_SUCCESS`, `APPLIED_REJECTED`, `PENDING`, `UNKNOWN`.

### 2.3. `ConnectionManager.java`
- Quản lý vòng đời socket TCP:
  - `connect()`: Thiết lập socket với `connectTimeoutMs`, `readTimeoutMs` (`SO_TIMEOUT`), và `TCP_NODELAY = true`.
  - `close()` / `closeInternal()`: Đóng sạch và giải phóng tài nguyên mà không ném ngoại lệ.
  - `reconnect()`: Đóng socket cũ và mở socket mới an toàn.
  - `isConnected()`: Báo cáo trạng thái kết nối.
- Thực thi bản tin qua `sendAndReceive(...)`:
  - Thử lại kết nối theo `RetryPolicy` nếu gặp lỗi transient connection.
  - Bắt `SocketTimeoutException` khi gửi/nhận:
    1. Đóng kết nối ngay lập tức (`close()`) để loại bỏ socket bị phân mảnh frame.
    2. Nếu có `ResultLookup`, tra cứu trạng thái của `requestId`:
       - `APPLIED_SUCCESS` -> Trả về `CONFIRMED`.
       - `APPLIED_REJECTED` -> Trả về `REJECTED` từ DB.
       - `PENDING` / `UNKNOWN` / Tra cứu lỗi -> Trả về `UNKNOWN` (`TIMEOUT_UNKNOWN`).
    3. Giữ nguyên vẹn 100% `requestId` ban đầu.
    4. **Tuyệt đối không chuyển timeout thành `REJECTED` hay `BOOK_FAILED`.**

---

## 3. Bằng chứng kiểm thử

1. **Unit Tests (14 tests PASS):**
   - `RetryPolicyTest`: 7/7 tests PASS (backoff computation, max bounds, transient classification, timeout classification, validations).
   - `ConnectionManagerTest`: 7/7 tests PASS (connect, reconnect, safe close, timeout -> UNKNOWN, never REJECTED, lookup success, lookup rejected, lookup failure).
2. **TCP Socket Integration Tests (3 tests PASS):**
   - `ConnectionManagerTcpIntegrationTest`:
     - Test 1: Đặt vé bình thường qua `ConnectionManager` thành công.
     - Test 2: Server delay gây `SocketTimeoutException` -> trả về `UNKNOWN (TIMEOUT_UNKNOWN)`, đóng socket an toàn; lệnh tiếp theo tự động reconnect socket mới và thành công.
     - Test 3: Kết nối tới cổng đóng -> retry kết nối với backoff đúng số lần `maxAttempts`.
3. **Toàn bộ Reactor:**
   - 6/6 module compile và pass toàn bộ test suite (`BUILD SUCCESS`).
