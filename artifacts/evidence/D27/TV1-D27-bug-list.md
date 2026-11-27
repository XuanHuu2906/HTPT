# TV1-D27 — Danh sách lỗi & Phụ thuộc liên module (Desktop Client Regression)

- **Mã tác vụ:** TV1-D27 (`Chạy regression toàn bộ Desktop Client`)
- **Thành viên phụ trách:** TV1 (Khoa — `vn.nhom6.railway.client`)
- **Nhánh làm việc:** `feature/UI`
- **Snapshot đối chiếu đầu vào:** `c92e280d9012c11047bc442d1e55d2b9da2b181e`
- **Ngày ghi nhận:** `2026-11-27`

---

## 1. Bảng tổng hợp nhanh

| Mã ghi nhận | Phân loại | Mức độ | Phát hiện tại | Module / Phạm vi | Owner | Trạng thái | Commit xử lý |
|---|---|---|---|---|---|---|---|
| `BUG-D27-01` | Lỗi Desktop Client | **P0** | `REG-E01` (`FIXTURE`) | `desktop-client` (`JournalingBookingClient`) | TV1 | **FIXED** (`PASS`) | `7ea3664` |
| `BUG-D27-02` | Lỗi Desktop Client | **P1** | `REG-G01` (`FIXTURE`) | `desktop-client` (`JournalingBookingClient`, `BookingViewModel`) | TV1 | **FIXED** (`PASS`) | `7ea3664` |
| `BUG-D27-03` | Lỗi Desktop Client | **P1** | `REG-D02` (`FIXTURE`) | `desktop-client` (`BookingViewModel`) | TV1 | **FIXED** (`PASS`) | `c470eac` |
| `BUG-D27-04` | Lỗi Desktop Client | **P1** | `REG-M01` (`FIXTURE`) | `desktop-client` (`BookingViewModel`, `MainController`, `App`) | TV1 | **FIXED** (`PASS`) | `f504279` |
| `DEP-D27-01` | Phụ thuộc liên module | **P1** | Preflight & `REG-D01` (`REAL`) | `shared/**`, `backend-node/**/booking/**` | TV2, TV3 | **OPEN / BLOCKED_CANCEL** | Giao TV2/TV3 |
| `DEP-D27-02` | Tính năng P1 chưa khóa contract | **P1** | `REG-E02` (`FIXTURE` & `REAL`) | `shared/**`, `backend-node/**/booking/**` | TV2, TV3 | **OPEN / BLOCKED (P1)** | Giao TV2/TV3 |
| `DEP-D27-03` | Điều phối cụm test & ngân sách ghế | **Infra** | Preflight & Lớp `REAL` | Cụm 5 Node / Lịch test D27 | TV6 | **NOT_RUN (Guarded)** | Giao TV6 |

---

## 2. Chi tiết lỗi thuộc Desktop Client (TV1 đã sửa tối thiểu và kiểm chứng lại)

### `BUG-D27-01` (P0) — `getRequestResult` ném `NoSuchElementException` khi tra cứu `requestId` không nằm trong journal cục bộ
- **Mục kiểm thử phát hiện:** `REG-E01` (`ClientRegressionTest#regE01_Fixture_GetRequestResultFourStatesReadOnly`)
- **Tệp ảnh hưởng:** `desktop-client/src/main/java/vn/nhom6/railway/client/network/JournalingBookingClient.java`
- **Triệu chứng / Bằng chứng lỗi trước khi sửa:**
  - Khi người dùng mở màn hình Tra cứu vé (`TicketLookupViewModel`) và nhập một `requestId` hợp lệ đã được xử lý trên cụm (`APPLIED_SUCCESS` hoặc `APPLIED_REJECTED`) nhưng mã đó được tạo từ máy trạm khác hoặc phiên không lưu trong `PersistentPendingJournal` hiện tại, lời gọi `getRequestResult(requestId)` thất bại với ngoại lệ:
    `ExecutionException: java.util.NoSuchElementException: Không tìm thấy requestId trong journal: <uuid>`
- **Nguyên nhân gốc:**
  - `JournalingBookingClient.getRequestResult(...)` gọi thẳng `journal.markResolved(requestId, ...)` ngay khi nhận `APPLIED_SUCCESS` / `APPLIED_REJECTED` mà không kiểm tra `journal.get(requestId).isPresent()`.
- **Giải pháp sửa tối thiểu:**
  - Chỉ gọi `journal.markResolved(...)` khi `journal.get(requestId).isPresent()` trả về `true`; nếu `requestId` không thuộc journal cục bộ thì trả nguyên vẹn `GetRequestResultResponseDto` về cho tầng giao diện hiển thị kết quả tra cứu chỉ-đọc.
- **Commit sửa lỗi:** `7ea3664` (`fix(desktop-client): xử lý tra cứu requestId ngoài journal và ghi nhận FAILED_NOT_SENT [BUG-D27-01, BUG-D27-02] (D27)`)
- **Kết quả kiểm chứng sau khi sửa:** `REG-E01` đạt `PASS`; toàn bộ `TicketLookupFlowTest` và `PendingRequestRetryTest` đạt `PASS`.

---

### `BUG-D27-02` (P1) — Lỗi kết nối trước khi ghi byte lên socket (`FailedNotSentException`) vẫn bị giữ ở `SENT_UNRESOLVED` thay vì `FAILED_NOT_SENT`
- **Mục kiểm thử phát hiện:** `REG-G01` (`ClientRegressionTest#regG01_Fixture_TimeoutAfterSendVsFailBeforeSend`)
- **Tệp ảnh hưởng:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/network/JournalingBookingClient.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/BookingViewModel.java`
- **Triệu chứng / Bằng chứng lỗi trước khi sửa:**
  - Khi toàn bộ endpoint từ chối kết nối TCP trước khi bất kỳ byte khung tin nào được ghi lên luồng mạng (`FailedNotSentException`), bản ghi trong `PersistentPendingJournal` vẫn nằm ở trạng thái `SENT_UNRESOLVED` và `BookingViewModel` chuyển sang `UNKNOWN` thay vì ghi nhận `FAILED_NOT_SENT` và giữ ghế ở `AVAILABLE`:
    `AssertionFailedError: Lỗi trước khi gửi byte phải ghi nhận FAILED_NOT_SENT ==> expected: <FAILED_NOT_SENT> but was: <SENT_UNRESOLVED>`
- **Nguyên nhân gốc:**
  - `JournalingBookingClient.book(...)` và `cancel(...)` gọi `journal.recordSent(...)` trước khi ủy quyền xuống transport để đảm bảo crash-safety, nhưng nhánh `.whenComplete((resp, ex) -> ...)` chưa bóc nhân ngoại lệ (`CompletionException` -> `FailedNotSentException`) để chuyển trạng thái journal sang `FAILED_NOT_SENT`. Đồng thời `BookingViewModel.handleException(...)` chưa phân nhánh riêng cho `FailedNotSentException`.
- **Giải pháp sửa tối thiểu:**
  - Bổ sung hàm giải bọc nguyên nhân gốc `unwrap(Throwable)` trong `JournalingBookingClient`; nếu nguyên nhân là `FailedNotSentException` (chắc chắn 0 byte đã rời Client) thì gọi `journal.markFailedNotSent(requestId, ...)`.
  - Cập nhật `BookingViewModel.handleException(...)` để khi gặp `FailedNotSentException` thì chuyển `UiState` sang `ERROR` (thay vì `UNKNOWN`), không khóa ghế sang `PENDING`/`BOOKED`, cho phép đặt lại an toàn. Các lỗi sau khi đã bắt đầu ghi byte vẫn giữ nguyên bất biến `UNKNOWN` + `SENT_UNRESOLVED`.
- **Commit sửa lỗi:** `7ea3664` (`fix(desktop-client): xử lý tra cứu requestId ngoài journal và ghi nhận FAILED_NOT_SENT [BUG-D27-01, BUG-D27-02] (D27)`)
- **Kết quả kiểm chứng sau khi sửa:** `REG-G01` và `REG-G02` đạt `PASS`; không phá bất biến timeout-after-send của D17/D22.

---

### `BUG-D27-03` (P1) — Chưa chặn yêu cầu hủy vé khi lệnh `BOOK_TICKET` gốc của vé dẫn xuất vẫn đang `SENT_UNRESOLVED` (`UNKNOWN`)
- **Mục kiểm thử phát hiện:** `REG-D02` (`ClientRegressionTest#regD02_Fixture_BlockCancelWhenOriginalBookUnresolved`)
- **Tệp ảnh hưởng:** `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/BookingViewModel.java`
- **Triệu chứng / Bằng chứng lỗi trước khi sửa:**
  - Khi lệnh `BOOK_TICKET` với `requestId` bị rơi phản hồi (đang nằm ở `SENT_UNRESOLVED` trong journal) và người dùng cố gửi `CANCEL_TICKET` với mã vé dẫn xuất `TCK-<8-ký-tự-đầu-requestId>` hoặc chính `requestId`, `BookingViewModel.cancel(...)` không nhận diện được mối liên hệ và vẫn gửi khung tin `CANCEL_TICKET` qua mạng:
    `AssertionFailedError: expected: <true> but was: <false> (Kỳ vọng chặn CANCEL khi BOOK gốc còn UNKNOWN)`
- **Nguyên nhân gốc:**
  - Điều kiện kiểm tra cũ chỉ dò `e.getPayload().contains(request.getTicketId())`, trong khi payload JSON của lệnh `BOOK_TICKET` chỉ chứa `(requestId, tripId, seatId, passengerName)` chứ chưa hề chứa `ticketId` do Server cấp.
- **Giải pháp sửa tối thiểu:**
  - Mở rộng hàm đối sánh `matchesUnresolvedBookingTicket(PendingRequestEntry, String)` trong `BookingViewModel`: ngoài kiểm tra payload, kiểm tra khớp trực tiếp `entry.getRequestId()` và tiền tố chuẩn `TCK-` + 8 ký tự đầu của `entry.getRequestId()` (không phân biệt hoa thường). Khi phát hiện trùng khớp với mục `BOOK_TICKET` chưa ngã ngũ, từ chối ngay tại Client (0 khung tin gửi đi) và hướng dẫn tra cứu `GET_REQUEST_RESULT` trước.
- **Commit sửa lỗi:** `c470eac` (`fix(desktop-client): chặn hủy vé khi lệnh BOOK_TICKET gốc còn UNKNOWN [BUG-D27-03] (D27)`)
- **Kết quả kiểm chứng sau khi sửa:** `REG-D02` và `REG-D01` đạt `PASS` (0 khung tin `CANCEL_TICKET` lọt qua socket khi `BOOK_TICKET` còn `UNKNOWN`).

---

### `BUG-D27-04` (P1) — Nhãn nguồn dữ liệu hiển thị nhầm `[MOCK]` / `MOCK (D06)` ở chế độ Socket thật và ghi đè ảnh bằng chứng D01 khi khởi chạy `App`
- **Mục kiểm thử phát hiện:** `REG-M01` (`ClientRegressionTest#regM01_Fixture_StartupResourcesAndSourceLabelSeparation`)
- **Tệp ảnh hưởng:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/BookingViewModel.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/MainController.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/App.java`
- **Triệu chứng / Bằng chứng lỗi trước khi sửa:**
  1. `BookingViewModel` khởi tạo cứng `SimpleBooleanProperty(true)` cho thuộc tính `mock`, thông điệp mặc định luôn là `"Đang dùng dữ liệu MOCK (D05)"`, và hàm `refreshSeats(...)` luôn nối hậu tố `" [MOCK]"` ngay cả khi `resp.isMock() == false` (chế độ `SocketBookingClient` / `LeaderAwareBookingClient`):
     `AssertionFailedError: Khi gắn với Socket/LeaderAware client, cờ mock ban đầu phải là false ==> expected: <false> but was: <true>`
  2. `MainController.showTicketStatusView()` truyền cứng `"MOCK (D06)"` vào `setSourceMode(...)` bất kể `isSocketMode`.
  3. `App.start(...)` luôn ghi đè tệp `artifacts/evidence/D01/TV1-D01-javafx-running.png` mỗi lần mở ứng dụng, có nguy cơ làm bẩn bằng chứng lịch sử D01.
- **Nguyên nhân gốc:**
  - Mã khởi tạo từ giai đoạn prototype D05/D06 chưa được tách điều kiện theo loại `BookingClient` và cờ `resp.isMock()` khi nâng cấp lên TCP Socket.
- **Giải pháp sửa tối thiểu:**
  - Trong `BookingViewModel`: tự động suy diễn trạng thái `mock` khởi đầu dựa trên tên lớp client (`Mock*` / `Fake*` -> `true`; `Socket*` / `LeaderAware*` / `Journaling*` -> `false`), và trong `refreshSeats(...)` cập nhật `mock.set(resp.isMock())`, chỉ gắn nhãn `" [MOCK]"` khi `resp.isMock() == true`.
  - Trong `MainController`: truyền nhãn nguồn động theo `isSocketMode` (`"TCP Socket (" + activeProfile + ")"` so với `"MOCK (Offline)"`).
  - Trong `App`: chỉ chụp snapshot D01 khi bật tường minh `-Dd01.capture=true`.
- **Commit sửa lỗi:** `f504279` (`fix(desktop-client): tách bạch nhãn nguồn REAL và MOCK khi khởi tạo và làm mới ghế [BUG-D27-04] (D27)`)
- **Kết quả kiểm chứng sau khi sửa:** `REG-M01` đạt `PASS`; các bài test cũ dùng `FakeBookingClient` / `MockBookingClient` vẫn tương thích ngược 100%.

---

## 3. Lỗi / Thiếu hụt phụ thuộc ngoài phạm vi TV1 (Chỉ ghi nhận & bàn giao đúng Owner)

> Theo đúng Mục 1 & Mục 3 của `AGENTS.md`, TV1 **không chạm** vào `shared/**`, `backend-node/**`, `configs/**`, `docker/**`, `scripts/**`. Các mục dưới đây được ghi nhận trung thực để bàn giao cho đúng Owner.

### `DEP-D27-01` (P1) — Backend chưa đăng ký TCP handler cho `CANCEL_TICKET` và `shared` chưa có DTO hủy vé
- **Phát hiện tại:** Kiểm tra đối chiếu đầu vào & thiết kế mục `REG-D01 [REAL]`.
- **Bằng chứng trong kho mã hiện tại:**
  - `backend-node/src/main/java/vn/nhom6/railway/booking/service/BookingRequestHandler.java` (dòng 108–109) hiện chỉ đăng ký 2 message type nghiệp vụ ghế/vé trên kênh Client:
    - `dispatcher.register(ProtocolConstants.TYPE_BOOK_TICKET, this::handleBookTicket)`
    - `dispatcher.register(ProtocolConstants.TYPE_GET_SEATS, this::handleGetSeats)`
  - Chưa đăng ký `ProtocolConstants.TYPE_CANCEL_TICKET`; gói `vn.nhom6.railway.protocol.dto` trong `shared` chưa có `CancelTicketRequestDto` / `CancelTicketResponseDto`.
- **Phạm vi ảnh hưởng:**
  - Ở lớp `FIXTURE`: Desktop Client kiểm thử đầy đủ luồng wire `CANCEL_TICKET` qua `FakeNodeServer` (`REG-D01`, `REG-D02` đạt `PASS`).
  - Ở lớp `REAL`: Nếu chạy trên cụm thật hiện tại, bước dọn dẹp vé `REG-D01` sẽ nhận lỗi không hỗ trợ từ Server và tự động gắn nhãn `BLOCKED_CANCEL` (cảnh báo ghế đã tiêu thụ cần TV2/TV6 reset sau).
- **Owner nhận bàn giao:** **TV2** (`backend-node/**/booking/**`) & **TV3** (`shared/**`, `networking/**`).

---

### `DEP-D27-02` (P1) — Chưa có hợp đồng bản tin và handler Backend cho `GET_TICKET_HISTORY`
- **Phát hiện tại:** `REG-E02` (`FIXTURE` & `REAL`).
- **Bằng chứng trong kho mã hiện tại:**
  - Theo `docs/specs/01-REQUIREMENTS.md` (FR-06, ưu tiên P1) và `03-CONTRACTS.md`, tra cứu lịch sử danh sách vé chưa có DTO trong `shared` và chưa có handler trong `backend-node`.
- **Phạm vi ảnh hưởng:**
  - Desktop Client xử lý fail-closed đúng quy tắc: ở chế độ Socket thật, `TicketLookupViewModel.isHistoryEnabled() == false`, khóa nút tải lịch sử, hiển thị thông báo rõ `BLOCKED` / tính năng `P1`, và trả về `0` dòng dữ liệu (không trộn dữ liệu giả).
- **Owner nhận bàn giao:** **TV2** & **TV3** (nếu nhóm quyết định mở rộng P1 sau khi khóa P0).

---

### `DEP-D27-03` (Hạ tầng / Lịch kiểm thử) — Chưa mở cụm 5 Node dành riêng và chưa có xác nhận khóa lịch + ngân sách ghế từ TV6
- **Phát hiện tại:** Bước 0.3 Preflight & cổng bảo vệ `assumeRealClusterReadReady()` của lớp `REAL`.
- **Bằng chứng đo thực tế:**
  - Quét kết nối TCP tới `127.0.0.1:9101..9105`: cả 5 cổng đều `DOWN` (`reachableNodes = 0/5`).
  - Chưa có tệp `docs/implementation/TV6-D27-notes.md` và biến môi trường `TV6_SCHEDULE_CONFIRMED` chưa được thiết lập.
  - Biến `ALLOW_REAL_REGRESSION` và `REGRESSION_SEATS` chưa được cấp.
- **Phạm vi ảnh hưởng:**
  - Toàn bộ 11 kịch bản `@Tag("real")` trong `ClientRegressionTest` tự động dừng an toàn tại cổng `Assumptions`, xuất trạng thái `NOT_RUN` trong `TV1-D27-regression-results.csv`, tiêu thụ **0 ghế**, gửi **0 lệnh ghi**, tuyệt đối không ảnh hưởng tới cụm nghiệm thu của nhóm.
- **Hướng dẫn kích hoạt lại cho TV6 khi có cụm riêng:**
  ```powershell
  mvn -B -ntp -pl desktop-client test "-Dtest=ClientRegressionTest" "-Dgroups=real" `
    "-Dallow.real.regression=true" `
    "-Dcluster.profile=dev" `
    "-Dtv6.schedule.confirmed=TV6-DEDICATED-WINDOW" `
    "-Dregression.seats=G09,G10" `
    "-Dmaven.compiler.release=21"
  ```
  *(Lưu ý: cổng bảo vệ sẽ tự động từ chối nếu `regression.seats` chứa `G02` hoặc `G05` dành riêng cho bộ acceptance của TV6).*
- **Owner nhận bàn giao:** **TV6** (điều phối cụm nghiệm thu AC01–AC10 và lịch chạy cụm thật).
