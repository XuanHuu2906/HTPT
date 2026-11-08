# DANH SÁCH LỖI VÀ XỬ LÝ (TV1-D08-BUG-LIST)
**Dự án:** Hệ thống Bán Vé Tàu Cao Tốc — Nhóm 6  
**Phân hệ:** Desktop Client (JavaFX) — TV1  
**Giai đoạn:** Nghiệm thu hoàn chỉnh giao diện ở chế độ MOCK (TASK D08)  
**Ngày kiểm tra:** 08/11/2026  

---

## Tóm tắt
- **Tổng số lỗi phát hiện:** 3
- **Số lỗi đã khắc phục (FIXED):** 3
- **Số lỗi tồn đọng:** 0

---

## Chi tiết các lỗi đã khắc phục

### BUG-01: Hard-code màu sắc trực tiếp trong mã Java thay vì dùng file `railway.css`
- **Mã lỗi:** `BUG-01`
- **Mục checklist liên quan:** `F4` (Không có màu hard-code trong Java ngoài `railway.css`)
- **Mô tả:** Trong các controller `TicketStatusController`, `CancelTicketController`, `MainController` và `SeatGridController`, nhiều đoạn code sử dụng trực tiếp chuỗi màu hex (`#dc2626`, `#059669`, `#64748b`, `#2563eb`, `#1e293b`, `#3b82f6`...) qua hàm `setStyle("-fx-text-fill: ...")`. Điều này vi phạm nguyên tắc tách biệt giao diện và chuẩn hóa CSS của SPEC-06.
- **Bước tái hiện:**
  1. Quét tìm chuỗi hex trong package controller:  
     `grep -rnE "#[0-9a-fA-F]{3,8}" desktop-client/src/main/java`
  2. Kết quả xuất hiện 28 vị trí chuỗi màu hex hard-code trong Java.
- **Tệp đã sửa:**
  - `desktop-client/src/main/resources/css/railway.css`: Bổ sung các style class chuẩn: `.nav-btn-default`, `.nav-btn-active`, `.mock-control-label`, `.search-msg-info`, `.search-msg-muted`, `.search-msg-success`, `.search-msg-error`, `.ticket-summary-default`, `.ticket-summary-cancellable`, `.ticket-summary-uncancellable`, `.cancel-box-*`, `.cancel-title-*`, `.cancel-detail-*`.
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/MainController.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/SeatGridController.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/CancelTicketController.java`
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/TicketStatusController.java`
- **Trạng thái:** `FIXED` (Quét lại bằng grep ra 0 dòng chuỗi màu hex).

---

### BUG-02: Phản hồi UNKNOWN bị ánh xạ nhầm thành màu đỏ ERROR trong SeatGridController
- **Mã lỗi:** `BUG-02`
- **Mục checklist liên quan:** `D3` (UNKNOWN/TIMEOUT hiển thị riêng, không bị coi là REJECTED, không tự đổi ghế sang BOOKED/AVAILABLE)
- **Mô tả:** Khi `BookingViewModel` trả về trạng thái `UiState.UNKNOWN` (do timeout hoặc mất kết nối ngầm), listener của `SeatGridController` gom chung `REJECTED, UNKNOWN` vào `ScreenState.ERROR` (banner màu đỏ). Điều này khiến người dùng dễ nhầm lẫn giữa lỗi từ chối nghiệp vụ (REJECTED) và lỗi chờ xác nhận phân tán (UNKNOWN).
- **Bước tái hiện:**
  1. Chọn ghế G05, chọn kịch bản `UNKNOWN (Timeout/Mất gói)` và bấm Đặt vé.
  2. Banner trên thanh trạng thái hiển thị màu đỏ của lỗi ERROR thay vì màu cam nét đứt đặc trưng của UNKNOWN.
- **Tệp đã sửa:**
  - `desktop-client/src/main/java/vn/nhom6/railway/client/controller/SeatGridController.java`: Tách riêng `UiState.UNKNOWN` và `UiState.DISCONNECTED` chuyển vào `ScreenState.DISCONNECTED` (banner màu cam cảnh báo nét đứt), tách biệt hoàn toàn với `UiState.REJECTED / ERROR` (màu đỏ).
- **Trạng thái:** `FIXED`.

---

### BUG-03: Nhãn thông tin hệ thống và nhãn badge còn hiển thị nhãn phiên bản cũ
- **Mã lỗi:** `BUG-03`
- **Mục checklist liên quan:** `A1` (Khởi động ứng dụng), `F1` (Mọi kết quả và màn hình có nhãn MOCK)
- **Mô tả:** Thông tin hệ thống ở thanh điều hướng trái ghi "Giai đoạn: D02 (MOCK)", `MockSeatSource` ghi "MOCK DATA — D05", `MockTicketSource` ghi "DEMO / MOCK — D06", chưa đồng bộ với giai đoạn nghiệm thu D08 hoàn chỉnh.
- **Bước tái hiện:**
  1. Mở màn hình chính, kiểm tra khung "Thông tin hệ thống" ở góc trái.
  2. Mở sơ đồ ghế và màn hình vé, kiểm tra nhãn badge ở góc phải.
- **Tệp đã sửa:**
  - `desktop-client/src/main/resources/fxml/main.fxml`: Cập nhật nhãn thành `• Giai đoạn: D08 (MOCK Nghiệm thu)`.
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/SeatViewModel.java`: Cập nhật `MOCK_LABEL = "MOCK DATA — D08"`.
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/TicketViewModel.java`: Cập nhật `MOCK_NOTICE = "DEMO / MOCK — D08"`.
  - `desktop-client/src/main/java/vn/nhom6/railway/client/viewmodel/BookingViewModel.java`: Cập nhật message khởi tạo `MOCK (D08)`.
- **Trạng thái:** `FIXED`.
