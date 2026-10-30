# Quy ước thư mục và nơi nộp đầu ra — Nhóm 6

## Dùng cấu trúc thực tế (không tạo Maven module mới)
| Phần việc | Đường dẫn chuẩn | Chủ trì |
|---|---|---|
| JavaFX Client | `desktop-client/src/main/java/vn/nhom6/railway/client/` và `desktop-client/src/main/resources/{fxml,css}/` | Khoa (TV1) |
| Booking nghiệp vụ | `backend-node/src/main/java/vn/nhom6/railway/booking/` | Pham Minh Tuấn (TV2) |
| PostgreSQL, Raft persistence | `backend-node/src/main/java/vn/nhom6/railway/persistence/` + `database/migration/` | TV2 + TV4 |
| DTO TCP, codec dùng chung | `shared/src/main/java/vn/nhom6/railway/protocol/{dto,codec}/` + `shared/src/main/java/vn/nhom6/railway/transport/` | Nguyễn Đức Thuận (TV3) |
| TCP server, RPC handler | `backend-node/src/main/java/vn/nhom6/railway/networking/` | TV3 |
| Raft log/replication | `backend-node/src/main/java/vn/nhom6/railway/raft/{core,replication}/` | Zun (TV4) |
| Raft election/recovery | `backend-node/src/main/java/vn/nhom6/railway/raft/{election,recovery}/` | Nguyễn Xuân Hữu (TV5) |
| Monitor JavaFX | `desktop-monitor/src/main/java/vn/nhom6/railway/monitor/` và `desktop-monitor/src/main/resources/` | Trần Hoàng Đạt (TV6) |
| Docker hai chế độ | `docker/`, `configs/{dev,lan}/`, `scripts/` | TV6 |
| Unit tests | `shared/src/test/java/`, `backend-node/src/test/java/`, `desktop-client/src/test/java/` | Chủ sở hữu phần code |
| Cluster integration tests | `integration-tests/src/test/java/vn/nhom6/railway/tests/` | Cả nhóm / TV6 điều phối |
| Chứng cứ kiểm thử | `artifacts/evidence/DXX/` (small text logs/reports) | Người thực hiện |
| Tài liệu lập trình không trùng SPEC | `docs/implementation/` | Theo task |

## Quy tắc quan trọng
1. **Giữ nguyên** mã `TV1-D01`…`TV6-D30` trong Google Sheets vì Apps Script dùng mã này để tìm và đồng bộ task.
2. Tên người hiển thị đã đổi sang tên thành viên. Không thay mã nội bộ thành tên.
3. `docs/specs/` (APPROVED-v1) là nguồn thiết kế chuẩn. Không tạo bản sao tài liệu chỉ để tick task.
4. Chỉ ghi PASS khi có PR/commit và lệnh chạy/test thật (trạng thái `NOT_RUN`/`BLOCKED` không phải `PASS`).
5. `.gitkeep` là file giữ chỗ để Git theo dõi thư mục, **không phải mã nguồn đã hoàn thành**.
6. Đường dẫn trong bảng là **đích dự kiến**, không phải file đang tồn tại. Được tạo theo task khi triển khai, không viết Java stub giả để lấp chỗ trống.

## D01 — tài liệu thực sự cần nộp
- `TV1-D01`: `docs/implementation/ui-flow.md`
- `TV2-D01`: `docs/implementation/booking-implementation-checklist.md`
- `TV3-D01`: `docs/implementation/contract-implementation-map.md`
- `TV4-D01`: `docs/implementation/raft-replication-implementation-map.md`
- `TV5-D01`: `docs/implementation/raft-election-test-matrix.md`
- `TV6-D01`: `docs/implementation/environment-checklist.md` + `artifacts/evidence/D01/TV6-D01-maven-validate.log`

Các ngày tiếp theo: đầu ra chính ở đúng module, tài liệu/biên bản ở `docs/implementation/` khi cần, log chứng cứ ở `artifacts/evidence/DXX/`.
