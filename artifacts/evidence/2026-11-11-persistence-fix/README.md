# Persistence fixes và Booking TCP/JDBC — 11/11/2026

## Candidate và môi trường

Base commit ban đầu: `adc4607b916cedcdbe654f6fdb6a5cdda229fa83`. Trong phiên, HEAD được cập nhật bên ngoài agent thành `32656979e3b3cde7f313c0cbc05caedbf6c6815c`; regression cuối dùng SHA mới và các sửa NOT_LEADER chưa commit. Agent không thực hiện lệnh git commit. Thay đổi chưa commit được định danh bằng `final.patch` và `source-sha256.txt` (bao gồm file Java mới chưa tracked). Không gán base SHA là commit đã chứa bản sửa. Không tạo Git commit.

Host Fedora Linux amd64, JDK 25.0.4.1, Maven 3.9.11; PostgreSQL Node 1 thật tại 127.0.0.1:5433/railway_node1, driver 42.7.7. Backend Docker build/runtime Java 21 theo Dockerfile hiện có. Credentials đọc từ container vào environment, không ghi vào log. Các fixture dùng schema `railway_test_*`, dọn sau test; không reset public hoặc ghi public booking trực tiếp.

## Nguyên nhân và sửa 3 lỗi gốc

1. **SQL test sai**, không phải lỗi transaction: PostgreSQL không có `jsonb_object_length(jsonb)`. Dùng scalar subquery `SELECT count(*) FROM jsonb_object_keys(result_json)` và tương tự cho event_json. Giữ nguyên yêu cầu số field payload (BOOK=7, CANCEL=5); không bỏ assertion.
2. **Fixture replay sai**: helper tạo passengerName từ ticketId. Đổi ticketId của Leader làm đổi cả business intent, nên repository trả REQUEST_ID_CONFLICT là đúng SPEC-05. Fixture retry nay giữ nguyên trip/seat/passengerName, chỉ đổi ticketId/issuedAt. Test mirror sequence cũng được sửa để thực sự chứa replay. Ca replay mở rộng thành 10 retry, mở repository mới, kiểm tra kết quả gốc, checkpoint và số vé/result/event không tăng.
3. **Assertion hủy sai theo SPEC-05**: 4 requestId mới (BOOK, CANCEL thành công, CANCEL vé không tồn tại, CANCEL vé đã hủy) phải tạo 4 kết quả. Chỉnh 3→4 cùng kiểm chứng rõ 2 APPLIED_SUCCESS, 2 APPLIED_REJECTED và lookup từng kết quả từ chối. Đây là sửa kỳ vọng theo contract, không sửa logic nghiệp vụ để chiều test.

Trong regression phát hiện thêm fixture cũ BookingRequestHandlerTest mong ticket-db-1 nhưng dùng supplier ticket-test-1. Đã inject supplier ticket-db-* riêng vào test JDBC, giữ nguyên assertions và không sửa production ticket generation. Lượt TCP test mới ban đầu lỗi serialize Instant trong fixture: chuyển timestamp sang ISO string trong command JSON theo SPEC-03, không thêm dependency.

Các lượt FAIL được giữ: `jdbc-tcp.log`, `jdbc-regression.log`, `docker-build.log` (DNS Maven Central). Không ghi đè lịch sử thành PASS.

## Kết quả

- `jdbc-final.log/json` và `jdbc-final-reports/`: **39 tests, failures=0, errors=0, skipped=0**, exit 0. Gồm repository 11, committed applier 9, Raft storage 10, request result store 9. Trong store suite có 5 test fingerprint thuần; không tuyên bố tất cả 39 đều thực hiện SQL.
- `jdbc-regression-fixed.log/json`: **698 tests** (shared 28 + backend 669 + integration 1), failures/errors/skips đều 0, exit 0. Sau lượt này còn chỉnh mã lỗi mặc định từ NO_QUORUM sang NOT_LEADER để đúng contract Follower; lượt xác nhận bản cuối là `jdbc-regression-authority.log/json`.
- `jdbc-tcp-fixed.log`: 2/2 test TCP/JDBC đạt. Trong test có kiểm tra chưa trả response trước controlled commit delivery, SQL chưa có vé trước apply, booking G02, retry giữ payload/index cũ, conflict giữ kết quả gốc, request khác cùng ghế bị từ chối, GET_SEATS đọc đúng ticketId BOOKED. Snapshot SQL in trong log; schema test đã được dọn.
- `jdbc-durability.log/json`: Java JDBC tạo fixture trong schema riêng → graceful restart PostgreSQL Node 1 → JVM mới kiểm tra còn nguyên một vé, request result và checkpoint=1. Từng subprocess exit 0. Không phải mất điện/crash và không phải Raft consensus.
- `docker-build-retry.log/json`: image build đạt sau lỗi DNS. Lượt build bản cuối: `docker-build-authority-retry.log/json`; `docker-build-authority.log/json` giữ lỗi DNS của lần trước. Các build Docker skip tests theo Dockerfile; kết quả JUnit nằm ở Maven host logs riêng.

`jdbc-durability.json` có annotation tách report inventory của Maven trước đó khỏi probe đã thực thi; không cộng inventory đó thành lượt JUnit mới.

## Điểm tích hợp TV2 ↔ TV3 và giới hạn

- NodeMain mặc định không tạo FakeBookingService nữa. Không có DB thì dùng service không có quyền Leader; có DB thì đăng ký BookingRequestHandler với JdbcBookingRepository và callback fail-closed.
- `NodeMain.withPersistentBooking(config, submitter, barrier, election)` là composition boundary dùng contract nội bộ hiện có. TV3 đưa TCP request vào handler; TV4/TV5 phải cung cấp submitter chỉ hoàn tất sau Raft commit **và** DB apply, barrier chỉ hoàn tất sau current-term NO_OP commit/apply. Factory không tự tuyên bố quorum, không ghi business khi append mới được accepted.
- JdbcBookingRepository cung cấp SeatSnapshotReader bằng một SELECT đọc cả status/current_ticket_id. Trước đây constructor handler fallback chỉ có status và mất ticketId của ghế BOOKED; điểm nối nay dùng snapshot đầy đủ. readSeatSnapshot cũ vẫn giữ chữ ký và ánh xạ status từ cùng nguồn đọc.
- Hai protocol suite cũ nay inject FakeBookingService tường minh; test fake không còn ngụy trang default runtime.
- JdbcBookingTcpIntegrationTest dùng TCP thật và PostgreSQL thật, **nguồn commit và read barrier là harness có kiểm soát**. Không phải bằng chứng quorum/replication. SQL snapshot không có nghĩa đã booking qua Raft thật.
- `RaftNode` hiện vẫn là interface; chưa có runtime Leader replication + current-term quorum commit hoàn chỉnh để composition root dùng. `RaftElectionComponent` cần CoreFactory và transport từ owner. Không viết cơ chế Raft thứ hai hoặc xác nhận chỉ vì local JDBC insert thành công.
- Runtime Docker cuối phải trả NOT_LEADER cho BOOK khi chưa có quyền Leader, READ_UNAVAILABLE cho GET_SEATS khi chưa có barrier; giữ requestId và không phát sinh public tickets/results/log. Bằng chứng chạy cuối ở `docker-booking-guard-authority.log/json`; log cũ `docker-booking-guard.log` là bản trước đổi mã lỗi.

**BLOCKED phần nghiệm thu hệ thống TV2-D10/TV3-D10**: chưa có commit runtime và chưa chạy JavaFX → TCP → Raft → DB. Chỉ điểm nối handler/JDBC và local transaction được kiểm chứng. Không đánh dấu AC02–AC10 toàn cụm PASS. Chưa có xác nhận review trực tiếp từ TV2/TV3/TV4/TV5; báo cáo này là bàn giao kỹ thuật, không phải approval giả định.

## D01–D10

[Ma trận 60 nhiệm vụ D01–D10](D01-D10-status.md) trích đúng workbook nguồn ở thư mục cha; `task-source.json` lưu tiêu chí gốc và SHA256 workbook. Có 10 file review mới cho TV2 trong các thư mục D01…D10, không sửa/xóa evidence lịch sử.

PASS_LOCAL chỉ áp dụng phạm vi kỹ thuật local của task, không thay review của owner. D03 còn thiếu xác nhận bàn giao; D10 chưa nghiệm thu hệ thống. Các task TV1/TV3/TV4/TV5/TV6 được đánh NOT_REASSESSED trong phiên này với link evidence lịch sử nếu có, không tự công nhận lại 60 task vì 39 JDBC test đạt. Những tên wire cũ trong workbook (BOOK_RESULT, SOLD) không thay SPEC-03.

## Tái chạy

Chạy từ root, Docker có postgres-1 healthy. Runner ghi argv, UTC, exit code, base SHA và patch hash vào JSON, stdout/stderr vào log. Với tên bắt đầu `jdbc`, credentials được đọc vào environment. Dùng thư mục phiên mới trước khi chạy lại để tránh ghi đè evidence.

```sh
python3 artifacts/evidence/2026-11-11-persistence-fix/run-command.py jdbc-final mvn -B -ntp -pl backend-node -am '-Dtest=JdbcBookingRepositoryTest,JdbcRequestResultStoreTest,JdbcRaftStorageTest,JdbcCommittedCommandApplierTest' -Dsurefire.failIfNoSpecifiedTests=false test
python3 artifacts/evidence/2026-11-11-persistence-fix/run-command.py jdbc-regression-authority mvn -B -ntp -pl integration-tests -am test
```

Lượt jdbc-final bắt buộc kiểm tra XML tổng=39 và skipped/failures/errors=0; exit Maven 0 không đủ nếu bài bắt buộc bị skip. Các test PostgreSQL cũ vẫn có assumption khi chạy ngoài runner; evidence phiên này xác nhận 0 skip. Test TCP/JDBC mới bắt buộc có environment DB, không tự fallback fake.

## Xác nhận cuối phiên

- `jdbc-regression-authority.json`: exit 0, 698 tests, 0 failures/errors/skips; trong đó đủ 39 persistence tests. `final-test-validation.json` kiểm tra riêng tổng 39 bắt buộc từ XML của lượt cuối.
- `docker-build-authority-retry.json`, `docker-start-authority.json`, `docker-booking-guard-authority.json`, `docker-status-authority.json`: tất cả exit 0. Cả 5 Backend trả NOT_LEADER cho BOOK và READ_UNAVAILABLE cho GET_SEATS; SQL ở từng DB xác nhận tickets/results/raft_log=0, lastApplied=0.
- 5 Backend và 5 PostgreSQL được để chạy, DB healthy. Vẫn chưa có nghiệm thu cụm Raft/JavaFX booking.
