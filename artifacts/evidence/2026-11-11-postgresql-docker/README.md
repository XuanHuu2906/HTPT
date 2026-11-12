# PostgreSQL JDBC và Backend Docker — 11/11/2026

## Candidate, môi trường và phạm vi

- Base commit: `586410b290ec062ad498c8e1438a6948adab14e0`.
- JDBC chạy trên source tại commit trên; Docker build thành công dùng thêm thay đổi chưa commit trong `dockerfile.patch`. `source-sha256.txt` định danh source/config dùng kiểm tra. Không tự tạo Git commit.
- Host Fedora Linux amd64, OpenJDK 25.0.4.1, Maven 3.9.11; Docker Desktop 4.98.0, Engine 29.9.2. PostgreSQL 16.15; JDBC driver 42.7.7. Image Backend build/runtime Java 21 với override release=21 hiện có trong Dockerfile (không phải build default release=25).
- Owner thực hiện phạm vi: TV6 (Docker/integration/evidence); dependency/reviewer cần phối hợp: TV2 (JDBC/DB), TV3 (TCP), TV4/5 (Raft). Chưa có xác nhận review trực tiếp từ các thành viên; không suy diễn đã được TV2 approve.
- Không đổi shared DTO, framing TCP, schema hay logic business/Raft. Không gửi BOOK qua runtime fake và không ghi booking trực tiếp vào public để giả lập consensus.

## Kết quả thực chạy

| Kiểm tra | Kết quả | Exit code | Bằng chứng |
|---|---|---:|---|
| 4 suite JDBC persistence trên PG Node 1 | FAIL: 39 tests, 36 đạt, 2 failures, 1 error, 0 skip | 1 | `jdbc.log`, `jdbc.json`, `jdbc-reports/` |
| Java ConnectionFactory → schema, metadata và 10 ghế Node 1 | PASS: 1 test, 0 skip | 0 | `node1-jdbc.log`, `node1-jdbc.json` |
| 8 bảng × 5 DB trước/sau restart PostgreSQL | PASS: dữ liệu giống nhau, 10 ghế AVAILABLE mỗi DB | 0 | `databases-restart.log`, `postgres-*-before-restart.json`, `postgres-*-after-restart.json` |
| Dockerfile ban đầu | FAIL: copy-dependencies không resolve shared | 1 | `backend-build.log`, `backend-build.json` |
| Dockerfile sau package → install | PASS: build image | 0 | `backend-build-fixed.log`, `backend-build-fixed.json`, `dockerfile.patch` |
| Khởi động 5 Backend + 5 DB | PASS ở mức tiến trình | 0 | `backend-start.log`, `containers-final.log` |
| 5 client PING/PONG + 5 peer TCP connect + 5 correlated NODE_STATUS | PASS ở mức transport | 0 | `tcp.log`, `tcp.json`, `check-tcp.py` |
| Cụm Raft, quorum, booking phân tán | BLOCKED: runtime không inject election/storage adapters | — | `backend-logs.log`: Election disabled; `tcp.log` term/index=0 |

`environment.log` giữ cả lần Docker bị sandbox từ chối (exit 1); `docker-environment.log` là lượt được cấp quyền, exit 0. Không coi exit 1 đó là Docker daemon hỏng.

## JDBC: commit, rollback và các lỗi giao TV2 review

Các suite sử dụng PostgreSQL thật `127.0.0.1:5433/railway_node1`, tạo schema `railway_test_*`, migrate V1/V2 trong schema riêng rồi dọn schema. Public business state không bị reset. Credentials dùng qua environment; không lưu password vào evidence. 5 test fingerprint trong JdbcRequestResultStoreTest là kiểm tra thuần, các test DB còn lại dùng JDBC thật; không gọi tất cả 39 test là SQL test.

- JdbcCommittedCommandApplierTest: 9/9 đạt, gồm callback chỉ được thấy sau DB commit, lỗi DB rollback và retry cùng entry.
- JdbcRaftStorageTest: 10/10 đạt, gồm persist/reload term/vote, fault injection giữa batch rollback, persist-before-ACK, fail closed khi log/checkpoint lỗi.
- JdbcRequestResultStoreTest: 9/9 đạt, gồm payload persisted, duplicate/conflict và fingerprint.
- JdbcBookingRepositoryTest: 8/11 đạt; `injectedFailuresRollbackEveryWriteAndSameEntryCanRetry` đạt. Ba ca lỗi bên dưới giữ nguyên để reviewer đánh giá; không sửa production chỉ để biến báo cáo xanh.

1. `bookAvailableSeatPersistsOneAtomicBusinessResult`: SQLState 42883, test gọi `jsonb_object_length(jsonb)` không tồn tại trong PostgreSQL đang chạy. Test dừng trước các assertion phía sau; cần TV2 sửa truy vấn đếm key rồi chạy lại, không thể kết luận toàn bộ test đạt.
2. `replayReturnsOriginalResultAndIndexWhileCheckpointAdvances`: nhận REQUEST_ID_CONFLICT. Kiểm tra source cho thấy helper tạo `passengerName="Passenger " + ticketId`; đổi ticketId ở lần retry cũng đổi input passengerName, nên fingerprint khác theo SPEC-05. Đây là lỗi fixture có căn cứ, cần TV2 review và giữ nguyên passengerName khi muốn kiểm tra replay.
3. `cancelUpdatesTicketAndSeatAndRejectsMissingOrCancelledTickets`: test đòi 3 request_results nhưng đã apply 4 requestId khác nhau; DB trả 4. SPEC-05 yêu cầu lưu cả APPLIED_REJECTED. Cần TV2 review assertion và cả các truy vấn jsonb_object_length phía sau chưa được thực thi do assertion dừng sớm.

Callback nguồn commit trong JdbcCommittedCommandApplierTest là harness, không phải quorum Raft thật. Vì vậy suite này chỉ xác nhận JDBC apply ở phạm vi cục bộ.

## Độ bền dữ liệu và trạng thái 5 Node

`check-databases.py` truy vấn và lưu tất cả rows trong 8 public tables trên từng DB, chạy:

```sh
docker compose -f docker/compose.dev.yml restart postgres-1 postgres-2 postgres-3 postgres-4 postgres-5
```

Lệnh restart exit 0; đợi cả 5 healthy rồi đọc lại và assert trước == sau cho cả 5 DB. 5 volume độc lập `railway_pgdata1`…`railway_pgdata5` được ghi trong `mounts-image.log`. Đây là graceful restart, không phải mất điện/crash hoặc remove/recreate container. Chỉ có seed data, tickets/request_results/raft_log rỗng và lastApplied=0: chưa kiểm chứng độ bền vé CONFIRMED sau crash.

Cả 5 Backend cùng image ID ghi ở `mounts-image.log`; log từng node xác nhận JDBC tới PostgreSQL riêng. `storageHealthy=false` trong NODE_STATUS hiện là giá trị mặc định khi election adapter null, không phủ định kết nối JDBC đã thành công. Role FOLLOWER/term=0/peerConnectivity=false cũng là status mặc định, không phải bằng chứng Raft hoạt động. Không gửi RPC Raft giả để làm xuất hiện quorum.

FR01/AC01: seed DB đúng 10 ghế đã kiểm chứng; chưa chứng minh toàn bộ luồng UI. FR06: 5 storage riêng tồn tại, chưa chứng minh replication. AC02–AC10 ở cấp hệ thống: chưa nghiệm thu qua các kiểm tra này.

## Lệnh, exit code và chạy lại

Mỗi `<name>.json` ghi argv chính xác, start/end UTC, base commit và exit code; `<name>.log` chứa stdout/stderr. `databases-restart.log` còn ghi SQL và exit từng truy vấn. Các script chạy từ repository root bằng Python 3. Runner lưu tại `run-command.py` (bản đã chạy ban đầu ở `/tmp/railway-evidence-run.py`, nội dung cùng chức năng).

```sh
python3 artifacts/evidence/2026-11-11-postgresql-docker/run-command.py jdbc mvn -B -ntp -pl integration-tests -am '-Dtest=Node1DatabaseIntegrationTest,JdbcBookingRepositoryTest,JdbcRequestResultStoreTest,JdbcRaftStorageTest,JdbcCommittedCommandApplierTest' -Dsurefire.failIfNoSpecifiedTests=false test
python3 artifacts/evidence/2026-11-11-postgresql-docker/run-command.py node1-jdbc mvn -B -ntp -pl integration-tests -am -Dtest=Node1DatabaseIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Lượt đầu dừng ở backend do 3 ca lỗi, nên Node1DatabaseIntegrationTest chạy riêng ở lượt thứ hai. Không dùng `maven.test.failure.ignore`. Các tên run ghi đè evidence cùng tên khi chạy lại; nên dùng thư mục phiên mới trước khi tái kiểm tra. Runner `jdbc` đọc credentials Node 1 từ container, không in password; lượt `node1-jdbc` dùng NODE1_DB_* hoặc mặc định test hiện có.

`previous/` là bản sao nguyên vẹn của toàn bộ 6 file `/tmp/railway-pg-check-20261111/*.txt` từ lượt trước. Các file cũ không có SHA/timestamp/exit metadata được nhúng; không gán hồi tố metadata mới cho chúng. Lượt hiện tại bổ sung bằng chứng cả 5 DB với timestamp/SHA/exit code. Không xóa bản gốc trong /tmp.

## Việc còn lại

TV2 review/sửa 3 ca test, chạy lại JDBC suite. TV3/TV4/TV5 cùng TV2 tích hợp runtime Raft → commit → JDBC apply; sau đó mới test quorum, replication, failover và durability của vé thật. 10 container được để chạy cho người dùng tiếp tục kiểm tra; chưa deploy LAN.
