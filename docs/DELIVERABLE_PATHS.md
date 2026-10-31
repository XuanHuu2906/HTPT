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

## D01 — bắt đầu triển khai mã nguồn, không bắt buộc tạo file .md

| Task | Đầu ra tối thiểu | Trách nhiệm |
|---|---|---|
| `TV1-D01` | `desktop-client/src/main/java/vn/nhom6/railway/client/App.java`, `controller/MainController.java`, `desktop-client/src/main/resources/fxml/main.fxml`; ảnh JavaFX chạy 10 ghế | Khoa: JavaFX MOCK |
| `TV2-D01` | `backend-node/src/main/java/vn/nhom6/railway/booking/BookingCommand.java`, `ApplyResult.java`, `backend-node/src/test/java/vn/nhom6/railway/booking/BookingCommandTest.java` | Tuấn: mô hình Booking theo SPEC-05 |
| `TV3-D01` | `shared/src/main/java/vn/nhom6/railway/protocol/dto/MessageEnvelope.java`, `MessageType.java`, `shared/src/test/java/vn/nhom6/railway/protocol/MessageEnvelopeTest.java` | Thuận: envelope/type đúng SPEC-03 |
| `TV4-D01` | `backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftState.java`, `RaftLogEntry.java`, `backend-node/src/test/java/vn/nhom6/railway/raft/core/RaftCoreModelTest.java` | Zun: một nguồn Raft Core dùng chung |
| `TV5-D01` | `backend-node/src/main/java/vn/nhom6/railway/raft/election/ElectionConfig.java`, `ElectionTimeoutPolicy.java`, `backend-node/src/test/java/vn/nhom6/railway/raft/election/ElectionTimeoutPolicyTest.java` | Hữu: Election config; review RaftState của TV4 |
| `TV6-D01` | `scripts/check-environment.ps1`, `artifacts/evidence/D01/TV6-D01-maven-validate.log` | Đạt: kiểm tra JDK/Maven/Docker, ghi exit code |

**D02–D03:** TV2 triển khai BookingStateMachine rồi chốt interface Booking–Raft–DB; TV3 làm payload DTO và JSON MessageCodec; TV4 thiết kế interface RaftNode/RaftStorage rồi chạy contract test; TV5 làm VoteRules rồi ElectionService interface; TV6 chuẩn bị cấu hình dev và script kiểm tra Maven G1. D04 của TV3 là negative tests/strict validation cho MessageCodec để không viết lặp D03; D05 mới thực hiện TCP 4-byte framing.

**Giữ G1 D03:** kiểm tra mã Java/API ghép được giữa Booking, DTO, Raft Core và JavaFX; chỉ PASS khi lệnh build/test thực chạy. Tất cả dựa trên SPEC-03/04/05 APPROVED-v1. Không tạo mã Java giả hoặc dùng .md thay cho kết quả kỹ thuật.

Frontend TV1: D01 khởi chạy JavaFX 10 ghế; D02 làm sơ đồ ghế tương tác; D03–D07 hoàn thiện form, CSS, binding, tra cứu/hủy và MockBookingClient; D08 nghiệm thu toàn bộ giao diện MOCK. D09 trở đi mới tích hợp TCP/Backend thật.

Các ngày tiếp theo: đầu ra chính ở đúng module, tài liệu/biên bản ở `docs/implementation/` khi cần, log chứng cứ ở `artifacts/evidence/DXX/`.
