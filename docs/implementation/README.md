# Tài liệu hỗ trợ triển khai

**Thư mục này không thay thế SPEC-00…SPEC-08** trong `docs/specs/`. Chỉ thêm tài liệu được yêu cầu bởi một task cụ thể và có thông tin mới, không sao chép lý thuyết đã duyệt.

## Vị trí tài liệu D01
- **Khoa (TV1-D01) bắt đầu code trực tiếp**, không bắt buộc nộp `ui-flow.md` hoặc wireframe: `desktop-client/src/main/java/vn/nhom6/railway/client/App.java`, `desktop-client/src/main/java/vn/nhom6/railway/client/controller/MainController.java`, `desktop-client/src/main/resources/fxml/main.fxml`; chạy JavaFX có đủ 10 ghế G01–G10 và ảnh minh chứng `artifacts/evidence/D01/TV1-D01-javafx-running.png`.
- `booking-implementation-checklist.md` — checklist nghiệp vụ/SQL/transaction, Pham Minh Tuấn (TV2).
- `contract-implementation-map.md` — bảng DTO/message/consumer theo SPEC-03, Nguyễn Đức Thuận (TV3).
- `raft-replication-implementation-map.md` — kế hoạch interface, commit/apply/read barrier, Zun (TV4).
- `raft-election-test-matrix.md` — test matrix bầu cử/khôi phục, Nguyễn Xuân Hữu (TV5).
- `environment-checklist.md` — cấu hình và log chạy lệnh trên máy thật, Trần Hoàng Đạt (TV6).

**Các file tài liệu D01 được liệt kê cho TV2–TV6 chưa được tạo**: từng thành viên tạo khi thực hiện task của mình. JavaFX của Khoa là sản phẩm chạy được, không phải file `.md`. Không đánh dấu PASS nếu file, PR hoặc minh chứng chưa tồn tại.

## Quy tắc
- Mã nguồn: `*/src/main/java/`; UI: `desktop-client/src/main/resources/fxml|​css/`; test Java: `*/src/test/java/`.
- SQL: `database/migration/`, `database/seed/`; Docker: `docker/`; cấu hình LAN/dev: `configs/`; script: `scripts/`.
- Minh chứng ngắn phục vụ test: `artifacts/evidence/DXX/`, mỗi file kèm mã task, SHA commit và kết quả thực chạy; không commit bí mật, DB dump, video/binary lớn.
- Đường dẫn cụ thể của từng task xem **cột Đầu ra** trong Google Sheets MASTER. Đường dẫn là nơi *dự kiến tạo*, không phải minh chứng file đã tồn tại.
- 5 backend node dùng chung `backend-node/`; JavaFX luôn là Desktop qua TCP socket, không tạo REST/web service.
