# DANH SÁCH LỖI DESKTOP CLIENT ĐÃ SỬA VÀ CÁC MỤC GIAO LẠI TRONG D28 (TV1-D28)

> **Mã task:** TV1-D28 · **Phụ trách:** TV1 (Khoa — Desktop Client JavaFX, `vn.nhom6.railway.client`)  
> **Baseline đầu ca:** `47f209fa068a1f569abcf5f0edca99213bd1690d` (`origin/main`)  
> **Tham chiếu đầu vào:** `artifacts/evidence/D27/TV1-D27-bug-list.md`, `artifacts/evidence/D27/TV1-D27-regression-results.csv`  
> **Ghi chú:** Bộ `acceptance-results.csv` của TV6-D27 chưa có trên kho tại thời điểm đóng băng RC1.

---

## 1. BẢNG LỖI P0/P1 THUỘC DESKTOP ĐÃ SỬA TRONG D28

### 1.1. `BUG-D28-01` (liên đới mở rộng từ `BUG-D27-03` / `BUG-D27-04` ở tầng Controller & ViewModel)
- **Mức độ:** **P0 / P1** (An toàn hiển thị trạng thái ghế & cách ly dữ liệu giả lập khi chạy Socket/Release).
- **Mô tả lỗi:**
  1. `SeatGridController` trước đây khởi tạo danh sách ghế ban đầu qua `MockSeatSource.createMockSeats()` ngay cả khi `client.mode=socket`, khiến sơ đồ ghế gán sẵn `G02/G04/G08 = BOOKED` giả trước khi `GET_SEATS` đầu tiên hoàn tất (trái SPEC-06 §8: trước khi nhận dữ liệu từ máy chủ phải ở trạng thái `UNKNOWN` / chưa xác minh), đồng thời `handleBookingOutcome(...)` ghi chèn bản ghi giả vào `MockTicketSource`.
  2. `TicketLookupViewModel` khi nhận `JournalingBookingClient` chưa mở lớp bọc (`getDelegate()`) để nhận diện `SocketBookingClient`/`LeaderAwareBookingClient`, nên nếu tồn tại biến hệ thống cũ `-Dbooking.client=mock` mâu thuẫn với `-Dclient.mode=socket` thì ViewModel bị rơi nhầm về `mockMode=true`; ngoài ra `loadConfirmedTicketsFromJournal()` trước đó gán cứng mã ghế `"—"` thay vì đọc `seatId`/`tripId` từ JSON của `PendingEntry`.
  3. `ClientConfig.loadWithOverrides(...)` trước đó để cờ phụ kế thừa `booking.client` ghi đè ngược lên `client.mode` tường minh.
- **Bước tái hiện:**
  1. Đặt `-Dclient.mode=socket` và `-Dbooking.client=mock`.
  2. Đặt vé thành công trên `FakeNodeServer` qua `JournalingBookingClient(LeaderAwareBookingClient)` cho ghế `G07`, sau đó khởi tạo `TicketLookupViewModel` và `SeatGridController`.
  3. Quan sát `lookupVm.isMockMode()`, `seatId` khôi phục từ Journal, số ghế `BOOKED` ban đầu của `SeatGridController` trước khi gọi `GET_SEATS`, và kích thước `MockTicketSource.getSharedTickets()` sau `handleBookingOutcome`.
- **Nguyên nhân gốc (Root cause):**
  - Thiếu kiểm tra cấu hình `isSocketMode()` / `isReleaseMode()` tại điểm khởi tạo danh sách ghế và thiếu mở bọc `JournalingBookingClient.getDelegate()` trong `TicketLookupViewModel`.
- **File đã sửa:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/network/ClientConfig.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/SeatViewModel.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/TicketLookupViewModel.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/SeatGridController.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/TicketStatusController.java`
- **Test tái hiện đã thêm:**
  - `ClientRegressionTest#regM02_Fixture_SocketModeNoMockSeatOrTicketContamination` (`REG-M02`).
- **Kết quả Trước / Sau:**
  - **Trước khi sửa:** `FAIL` (`AssertionFailedError: JournalingBookingClient bọc Socket/LeaderAware không được rơi vào mockMode ==> expected: <false> but was: <true>` và `SeatGridController` có 3 ghế `BOOKED` giả ban đầu).
  - **Sau khi sửa:** `PASS` (`0` ghế `BOOKED` giả trước kết nối, `0` bản ghi chèn bẩn vào `MockTicketSource`, trích xuất đúng ghế `G07` từ Journal).
- **Commit:** `bf879ff` — `fix(desktop-client): sửa lỗi rò rỉ dữ liệu Mock và nuốt lỗi hủy vé ở tầng Controller/ViewModel [BUG-D28-01, BUG-D28-02] (D28)`.

---

### 1.2. `BUG-D28-02` (liên đới mở rộng từ Nhóm D — Hủy vé D23/D25 ở tầng `CancelTicketController`)
- **Mức độ:** **P1** (Tính trung thực thông báo tiền kiểm tra và cách ly `MockBookingClient` khi hủy vé).
- **Mô tả lỗi:**
  1. `CancelTicketController.onConfirmCancelClicked()` trước đây luôn gọi `MockBookingClient.getInstance().setNextScenario(...)` bất kể ứng dụng đang chạy ở chế độ `socket` thật hay `mock`.
  2. Khi `BookingViewModel.cancel(...)` chặn tiền kiểm tra ở phía Client trước khi gửi qua mạng (ví dụ ném `IllegalStateException` vì lệnh `BOOK_TICKET` gốc sinh ra vé đó vẫn đang `SENT_UNRESOLVED`, hoặc `FailedNotSentException`), nhánh `if (ex != null)` trong `CancelTicketController` gom mọi ngoại lệ vào `UNKNOWN_TIMEOUT`, làm hiển thị nhầm thông báo timeout mạng thay vì thông báo lỗi chặn trước khi gửi.
- **Bước tái hiện:**
  1. Chạy ở `-Dclient.mode=socket`, ghi một mục `BOOK_TICKET` ở trạng thái `SENT_UNRESOLVED` vào `PersistentPendingJournal`.
  2. Nạp `/fxml/cancel-ticket-dialog.fxml`, gắn vé dẫn xuất từ `requestId` đang `SENT_UNRESOLVED`, tích xác nhận và gọi `onConfirmCancelClicked()`.
- **Nguyên nhân gốc (Root cause):**
  - Thiếu phân nhánh giữa lỗi tiền kiểm tra trước khi gửi (`IllegalStateException` / `IllegalArgumentException` / `FailedNotSentException`) và lỗi bất định sau khi gửi trong callback `whenComplete` của `CancelTicketController`.
- **File đã sửa:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/CancelTicketController.java`
- **Test tái hiện đã thêm:**
  - `ClientRegressionTest#regD03_Fixture_CancelControllerNoMockLeakAndPreservesPreSendBlock` (`REG-D03`).
- **Kết quả Trước / Sau:**
  - **Trước khi sửa:** `FAIL` (`AssertionFailedError: Lỗi chặn hủy trước khi gửi (IllegalStateException) không được nuốt thành TIMEOUT_UNKNOWN`).
  - **Sau khi sửa:** `PASS` (hiển thị rõ `KHÔNG THỂ GỬI YÊU CẦU HỦY VÉ` kèm lý do tiền kiểm tra, vé giữ nguyên `CONFIRMED`, không chạm `MockBookingClient`, không phát sinh nhãn `MOCK`).
- **Commit:** `bf879ff` — `fix(desktop-client): sửa lỗi rò rỉ dữ liệu Mock và nuốt lỗi hủy vé ở tầng Controller/ViewModel [BUG-D28-01, BUG-D28-02] (D28)`.

---

### 1.3. Rà soát vệ sinh CSS / FXML / Thông báo / Sơ đồ ghế & cô lập JAR `-Prelease` (Bước 2 & Bước 3)
- **Các hạng mục hoàn thiện thêm trong D28:**
  1. Bổ sung đầy đủ các lớp CSS còn thiếu vào `railway.css` (`.badge-cluster-mode`, `.badge-release-mode`, `.version-status-label`, `.reconnect-*`, `.pending-status-*`, `.pending-source-*`, `.btn-pending-*`, `.config-error-*`), loại bỏ toàn bộ `setStyle` chứa mã màu `#hex` cứng và `printStackTrace()` trong Java UI (`Commit 0d3eb99`).
  2. Chuẩn hóa văn bản thông báo `UNKNOWN`/timeout trong `PendingRequestsViewModel`, `TicketLookupViewModel` (0 từ cấm) và loại bỏ hoàn toàn từ khóa `MOCK`/`DEMO`/`FAKE`/`Placeholder` khỏi cả 7 tệp `/fxml/*.fxml` (`Commit 0d3eb99`).
  3. Chống cắt chữ tiêu đề phụ trên `seat-grid.fxml` khi thu nhỏ về kích thước tối thiểu `1040x720` (`Commit ddf4d6b`).
  4. Triển khai chế độ `RELEASE` nghiêm ngặt (`release.mode=true`), màn hình lỗi cấu hình fail-fast trong `App`, gỡ vật lý các node `#mockBadgeLabel` / `#mockControlPanel` / `#cancelMockPanel` khỏi Scene Graph, cô lập mã giả lập sang `vn.nhom6.railway.client.dev.DevModeSupport` qua `DevSupportBridge` và loại bỏ sạch `Mock*`/`Fake*`/`dev/**` khỏi gói JAR `-Prelease` (`Commits 25d3bab, 4d591ac`).

### 1.4. `BUG-D28-03` — Treo tiến trình Surefire trên CI Headless không có `openjfx-monocle` (Phát hiện trên PR #148, đóng gói RC2)
- **Mức độ:** **P0** (Chặn đường ống CI GitHub Actions — treo >13 phút ở bước `Standardized Maven Clean Test`).
- **Mô tả lỗi:**
  - Trên GitHub Actions (`.github/workflows/ci.yml`), biến `JAVA_TOOL_OPTIONS` đặt `-Djava.awt.headless=true -Dglass.platform=Monocle -Dmonocle.platform=Headless`, nhưng dự án không khai báo thư viện `openjfx-monocle`.
  - Bài test mới `REG-D03` trong `ClientRegressionTest` gọi trực tiếp `Platform.startup(() -> {})` mà chưa kiểm tra `isJavaFxDisplayAvailable()`, khiến `QuantumToolkit.startup` bật `PlatformImpl.initialized = true` rồi nổ `NullPointerException` (`PlatformFactory.getPlatformFactory() is null`) trước khi giảm chốt `startupLatch`. Toàn bộ các lệnh `Platform.runLater(...)` ở các bài test kế tiếp bị khóa cứng vô thời hạn tại `startupLatch.await()`.
- **File đã sửa:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/CancelTicketController.java`
  - `desktop-client/src/test/java/vn/nhom6/railway/client/ClientRegressionTest.java`
  - `desktop-client/src/test/java/vn/nhom6/railway/client/ReleaseModeTest.java`
  - `desktop-client/src/test/java/vn/nhom6/railway/client/ReleaseUiHygieneTest.java`
- **Kết quả Trước / Sau (với đúng `JAVA_TOOL_OPTIONS` của `ci.yml`):**
  - **Trước khi sửa:** `regD03` ném `NullPointerException` tại `QuantumToolkit.startup`, sau đó tiến trình Maven treo cứng >13 phút tại `ReleaseModeTest`.
  - **Sau khi sửa:** `348` test chạy hoàn tất trong **~36 giây** (`0` Failures, `0` Errors, `REG-D03` vẫn chạy và PASS trên cả CI headless lẫn Windows có màn hình).

---

## 2. DANH SÁCH LỖI ĐÃ ĐÓNG TỪ D27 (KHÔNG TÁI PHÁT TRONG D28)

| Mã lỗi D27 | Mức | Owner | Trạng thái ở D28 | Kiểm chứng |
|---|---|---|---|---|
| `BUG-D27-01` | P1 | TV1 | **CLOSED** | `REG-I02` PASS (`0` frame xung đột rời Client). |
| `BUG-D27-02` | P1 | TV1 | **CLOSED** | `REG-H01` PASS (`0` frame `BOOK_TICKET`/`CANCEL_TICKET` khi dò Leader). |
| `BUG-D27-03` | P0 | TV1 | **CLOSED** | `REG-G01` PASS (`FAILED_NOT_SENT` giữ nguyên ghế `AVAILABLE`). |
| `BUG-D27-04` | P1 | TV1 | **CLOSED** | `REG-M01`, `REG-M02`, `ReleaseModeTest` PASS (`0` nhãn `MOCK` rò sang luồng Socket/Release). |

---

## 3. CÁC HẠNG MỤC ĐỂ LẠI / GIAO LẠI CHO OWNER KHÁC (NGOÀI PHẠM VI GHI CỦA TV1)

| Mã theo dõi | Mức | Owner phụ trách | Mô tả hiện trạng | Hành động phía Desktop Client (TV1) |
|---|---|---|---|---|
| `DEP-D27-01` | P1 (Backend) | **TV2** (`backend-node/**/booking/**`) | Handler `CANCEL_TICKET` chưa được đăng ký trên `BookingRequestHandler` của Backend (`BookingRequestHandlerTest` vẫn khẳng định `hasHandler(CANCEL_TICKET) == false`). | Giữ nguyên client contract theo SPEC-03 §4.2; trong lớp `REAL` phân loại minh bạch `BLOCKED_CANCEL` và ghi nhận ghế cần TV2/TV6 reset nếu phát sinh. |
| `DEP-D27-02` | P1 (Contract/Backend) | **TV2 + TV3** (`shared/**`, `backend-node/**`) | Truy vấn lịch sử vé nâng cao (`GET_TICKET_HISTORY`) chưa có wire DTO khóa trong `shared` (`DESIGN-v1 / CHƯA KHÓA`). | Khóa an toàn trên giao diện với thông báo `BLOCKED/P1`, chỉ hiển thị vé `CONFIRMED` lưu bền trong Journal cục bộ, `0` dữ liệu giả. |
| `DEP-D27-03` | Hạ tầng / Lịch chạy | **TV6 + TV3** (`configs/lan/**`, `docker/**`) | `configs/lan/node-*.properties` và `client-lan.properties` vẫn giữ IP mẫu `192.168.10.11..15` chờ chốt IP thật của 6 máy LAN; chưa có văn bản `TV3-D28-notes.md` / `TV6-D28-notes.md` xác nhận cấu hình release. | `ClientConfig.validateForRelease()` chặn fail-fast mọi IP placeholder và hiển thị màn hình lỗi cấu hình rõ khóa + `nodeId`, yêu cầu ghi đè bằng IP thật khi chạy LAN. |
