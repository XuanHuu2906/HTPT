# D13 — Persistent Raft core + TCP election runtime

Ngày: 2026-11-13 (Asia/Ho_Chi_Minh). Base: `a58e7aef6219ed6ef8f7655541d63ebb6374ae85` + thay đổi trong nhánh triển khai. Owner review cần TV3/TV4/TV5; tài liệu này không thay xác nhận của từng thành viên.

## Thay đổi / đối chiếu SPEC

- `PersistentRaftNode` dùng chính RaftState mà RaftElectionComponent restore, một PersistentElectionState dùng chung cho election/follower core, và PersistentLogAppender/AppendEntriesHandler hiện có. Term/vote/log lưu qua RaftStorage (runtime dùng JdbcRaftStorage).
- `ElectionRuntime` nối VoteRpcAdapter → RaftRpcClient.exchange → peer TCP thật. Exchange giữ messageId của adapter và chạy connect/write trên pool I/O có giới hạn; reply trở lại một Raft event loop.
- `NodeMain.main()` tạo runtime khi có NODE_PEERS. Runtime yêu cầu chính xác node-1..node-5 và PostgreSQL; D13 chạy trước khi mở socket/timer. Không có NODE_PEERS thì giữ chế độ diagnostic cũ, không tự bầu Leader.
- Leader bắt đầu heartbeat sau khi có 3 phiếu khác nhau. Higher-term vote/append/request/response lưu term mới và dừng lịch heartbeat cũ. Timeout vote không còn tính grant muộn, nhưng vẫn xử lý term cao hơn của phản hồi hợp lệ đến muộn.
- FR07; TC-RAFT-01/02/08 và phần election của AC07. Không đổi wire DTO/protocolVersion/framing, không đổi quorum 3/5.

## Lệnh đã chạy / bằng chứng

Môi trường local: OpenJDK 25.0.4.1, Maven 3.9.11, Docker Desktop, PostgreSQL 17 Alpine. Docker Backend dùng Dockerfile hiện có (Temurin JRE21, compiler release21).

| Kiểm tra | Exit / kết quả | Evidence |
|---|---|---|
| Targeted core/election/heartbeat tests | 0; 21 tests, 0 failures/errors/skips | `unit-tests.log` |
| Build Docker backend | 0 | `docker-build.log` |
| `python3 scripts/test-election-docker.py` | 0; PASS trên 5 Backend/5 PostgreSQL riêng | `docker-test.log`, `docker/` |
| `mvn -B -ntp -Drailway.requirePostgres=true clean test` với RAILWAY_TEST_* và HEADLESS/JAVA_TOOL_OPTIONS giống CI | 0; shared 30, backend 767 (không skip), desktop 151 (5 skip), integration 1 (1 skip) | `ci-postgresql-local.log` |
| `python3 scripts/check-d13-reports.py` | 0; 12 PostgreSQL D13 tests, 0 skip | `d13-gate-local.log` |
| GitHub Actions run 43408025529 / commit cc00635 | SUCCESS; D13 PostgreSQL 12 tests, 0 skip; backend 767 tests, 0 skip | `ci-remote.md`, `github-actions-postgresql.log` |

Lượt compile test đầu lỗi vì payload kiểu Object; đã sửa test chuyển về JsonNode (`first-test-compile-failure.log`). Lượt full reactor đầu gặp test multiplex 100 RPC cấu hình giới hạn 64 in-flight; đã sửa riêng fixture test cho phép 100, giữ giới hạn production và test backpressure (`first-regression-backpressure-failure.log`). Sau sửa đã chạy lại full reactor thành công.

Các skip ngoài D13 là kiểm thử desktop/Node1 DB phụ thuộc môi trường; không sửa chúng để gán PASS. D13 Group A=6, Group B=12, đều 0 skip. CI remote SUCCESS được ghi riêng trong [ci-remote.md](ci-remote.md); log local không được gọi là log GitHub Actions.

## Docker assertions thực chạy

- Cụm mới, 5 Node: một Leader ổn định, cùng term, storageHealthy=true.
- Dừng Leader và một follower thật: 3 Node còn lại bầu Leader mới ổn định.
- Dừng tiếp Leader mới: 2 Node còn lại tăng term qua nhiều campaign, không có Leader mới trong 20 mẫu/5 giây sau thời gian ổn định.
- Khởi động lại lần lượt để trở về 3/5 rồi 5/5: hội tụ một Leader/cùng term.
- Gửi RequestVote term cao hơn tới Leader qua peer port: response đúng correlation, Leader trở thành Follower, SQL `raft_meta` lưu term/vote mới. Log có `heartbeat=STOPPED`; unit tests còn kiểm higher-term AppendEntries, heartbeat response và persistence failure.
- Compose test tạo project ngẫu nhiên riêng, không dùng volume dev; script lưu status JSON/log từng Node/SQL rồi dọn container và volume test.

Chạy lại:

```bash
docker build -t railway-backend-node:d13-election -f docker/Dockerfile.backend .
python3 scripts/test-election-docker.py
```

## Phạm vi còn lại

Core này hoàn thiện phần election/follower durable và local append acceptance. Leader majority replication/commit và Booking → durable apply chưa được nối. Runtime chưa trả CONFIRMED, chưa cung cấp authoritative read barrier. Kiểm thử 2/5 ở đây chứng minh không bầu Leader mới; Leader cũ có thể vẫn tự nhận LEADER khi mất quorum theo Raft, nên không dùng role để xác nhận ghi. Chưa nghiệm thu AC05/06/08/09/10 hoặc booking phân tán bằng các kết quả này.
