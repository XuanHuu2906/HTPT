# TV2 — D13 startup / PostgreSQL / Backend kill-restart

- Ngày chạy: 2026-11-13, Asia/Ho_Chi_Minh.
- Base commit: `d9e02bb50827db9e92cae3fb75a8be3eafd0b22f` + working-tree changes; chưa commit.
- Môi trường: Linux, OpenJDK 25.0.4.1, Maven 3.9.11, PostgreSQL 17 (`postgres:17-alpine`) qua Docker Desktop; DB test riêng `railway_test`, host port 55439. Mỗi test tạo/drop schema UUID riêng; không sửa DB ứng dụng.
- Revision source được ghi trong `source-sha256.txt`; hash không bao gồm tài liệu minh chứng.

## Lệnh và kết quả thực chạy

Các biến `RAILWAY_TEST_JDBC_URL`, `RAILWAY_TEST_DB_USER`, `RAILWAY_TEST_DB_PASSWORD` phải trỏ đến PostgreSQL test có quyền tạo schema. Không dùng DB ứng dụng.

```bash
mvn -B -ntp -pl backend-node -am -Dtest=PersistenceRestartTest,NodeMainTest,BookingRequestHandlerTest -Dsurefire.failIfNoSpecifiedTests=false -Drailway.requirePostgres=true test
mvn -B -ntp -pl backend-node -am -Drailway.requirePostgres=true test
python3 scripts/check-d13-reports.py
```

| Lệnh | Exit | Actual | Minh chứng |
|---|---:|---|---|
| Targeted D13 + Node + Booking | 0 | 34 tests, 0 failures/errors/skips | `d13-postgresql.log` |
| Regression shared + backend | 0 | shared: 30 tests; backend: 762 tests; 0 failures/errors/skips | `backend-regression.log` |
| CI D13 report gate | 0 | 12 PostgreSQL tests, 0 failures/errors/skips | `ci-gate.log` |
| Lượt D13 đầu tiên | 1 | 18 tests, 1 assertion failure, 0 skipped; test mong cổng chưa bind là 0 nhưng TcpServer trả -1. Đã sửa assertion và chạy lại thành công | `d13-first-run-failed.log` |

## Phạm vi được chứng minh

- D13 Group A: 6 kiểm thử invariant không cần DB. Group B: 12 kiểm thử PostgreSQL thật; không skip.
- `NodeMain.start()` gọi DbStateLoader trước cả hai socket và timer; schema thiếu khiến startup thất bại. State startup được giữ để quan sát, commit index ban đầu bằng lastApplied.
- Process test chạy entry point thật `vn.nhom6.railway.node.NodeMain` trong JVM con, chờ READY, `destroyForcibly()` (không chạy shutdown hook), khởi động JVM mới trên cùng schema.
- Fixture chứa term=2, vote=node-3, log index=1..3, applied=2, một BOOK đã apply; index 3 chưa apply. Sau restart vẫn commit/applied=2, không tự apply suffix. So sánh toàn bộ JSON rows của raft_meta, raft_log, raft_apply_progress, seats, tickets, request_results, business_events; giữ nguyên ticket ACTIVE và kết quả request.
- Regression bao gồm JDBC apply (9), repository (11), request store (9), storage (12), TCP/JDBC (2), BookingRequestHandler (12), storage adapter (25), vote RPC adapter (5), heartbeat scheduler (8).

## Giới hạn / phần chưa hoàn thành

- Process test tạo dữ liệu bằng committed-entry fixture trước khi chạy Backend. Chưa chứng minh booking được commit bởi cụm 5 voter, chưa kill giữa transaction SQL thật; các lỗi SQL trước/sau statement được kiểm bằng fault injection riêng.
- `RaftNode` hiện chỉ có interface trong mã chính; không có implementation điều phối Leader replication, ACK, majority/current-term commit và dispatch apply. `NodeMain.main()` không lắp RaftElectionComponent; BOOK tiếp tục trả NOT_LEADER khi chưa có submitter thật.
- `JdbcBookingTcpIntegrationTest` dùng controlled commit harness, không phải đồng thuận thật. Vì vậy AC02/AC05–AC09 phân tán chưa được nghiệm thu trong lượt này.
- PostgreSQL CI service và gate đã cấu hình; GitHub Actions remote chưa chạy. Kết quả ở đây là chạy local.
