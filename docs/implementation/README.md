# Tài liệu hỗ trợ triển khai

**Thư mục này không thay thế SPEC-00…SPEC-08** trong `docs/specs/`. Chỉ thêm tài liệu được yêu cầu bởi một task cụ thể và có thông tin mới, không sao chép lý thuyết đã duyệt.

## Ngày D01 — ưu tiên code và test, không bắt buộc viết .md

Theo kế hoạch mới, **cả 6 thành viên đều phải có đầu ra kiểm chứng được**:
- Khoa (TV1): khởi chạy JavaFX và hiện đủ G01–G10 bằng MOCK; xem `desktop-client/`.
- Tuấn (TV2): `BookingCommand.java`, `ApplyResult.java` và test theo SPEC-05.
- Thuận (TV3): `MessageEnvelope.java`, `MessageType.java` và test theo SPEC-03.
- Zun (TV4): `RaftState.java`, `RaftLogEntry.java` và test core theo SPEC-04.
- Hữu (TV5): `ElectionConfig.java`, `ElectionTimeoutPolicy.java` và test; TV4 là owner chung `raft/core`.
- Đạt (TV6): `scripts/check-environment.ps1` và log `mvn validate` thực.

Không cần nộp 5 file `booking-implementation-checklist.md`, `contract-implementation-map.md`, `raft-replication-implementation-map.md`, `raft-election-test-matrix.md`, `environment-checklist.md` như yêu cầu D01 cũ. Nội dung cần thống nhất đã có trong SPEC-03/04/05/08; ghi quyết định kỹ thuật mới trong PR review, và chỉ tạo tài liệu bổ sung nếu thật sự cần.

**D02–D03** tiếp tục viết code, kiểm thử và chốt hợp đồng qua interface và JUnit, không lập tài liệu trùng SPEC. **D03/G1** chỉ PASS khi đã chạy kiểm tra compile/test thật; khi chưa chạy phải ghi NOT_RUN/BLOCKED.

## Quy tắc
- Mã nguồn: `*/src/main/java/`; UI: `desktop-client/src/main/resources/fxml|​css/`; test Java: `*/src/test/java/`.
- SQL: `database/migration/`, `database/seed/`; Docker: `docker/`; cấu hình LAN/dev: `configs/`; script: `scripts/`.
- Minh chứng ngắn phục vụ test: `artifacts/evidence/DXX/`, mỗi file kèm mã task, SHA commit và kết quả thực chạy; không commit bí mật, DB dump, video/binary lớn.
- Đường dẫn cụ thể của từng task xem **cột Đầu ra** trong Google Sheets MASTER. Đường dẫn là nơi *dự kiến tạo*, không phải minh chứng file đã tồn tại.
- 5 backend node dùng chung `backend-node/`; JavaFX luôn là Desktop qua TCP socket, không tạo REST/web service.
