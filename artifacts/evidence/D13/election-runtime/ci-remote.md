# GitHub Actions PostgreSQL — D13 không skip

- Run: [43408025529](https://github.com/XuanHuu2906/HTPT/actions/runs/43408025529).
- Job: [131827097648 — Maven tests with PostgreSQL (D13 required)](https://github.com/XuanHuu2906/HTPT/actions/runs/43408025529/job/131827097648).
- Event/branch: push / `feature/d13-election-runtime`.
- Commit thực sự được CI kiểm thử: `cc00635bd9bec337ed4921f81f615ed7d8113454`.
- Ngày chạy: 2026-11-13, UTC 10:17–10:18 (17:17–17:18 Asia/Ho_Chi_Minh).
- Môi trường: GitHub-hosted Ubuntu, Oracle JDK 25, Maven, PostgreSQL 17 Alpine service thật.
- Kết quả job: **SUCCESS**, bao gồm gate `Require real PostgreSQL D13 results (no skips)` và upload artifact.
- Log tải trực tiếp qua GitHub connected tool: [github-actions-postgresql.log](github-actions-postgresql.log).

Các dòng kết quả thực trong log:

```text
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0 -- PersistenceRestartTest$GroupB_RealPostgreSqlRestartTests
Tests run: 767, Failures: 0, Errors: 0, Skipped: 0 -- backend-node summary
BUILD SUCCESS
D13 PostgreSQL: 12 tests, 0 failures, 0 errors, 0 skipped
```

D13 Group A có 6 kiểm thử invariant không cần DB; Group B có 12 kiểm thử PostgreSQL thật. Backend tổng cộng 767 test, không skip. Full reactor có 5 desktop tests và 1 Node1 integration test phụ thuộc môi trường bị skip; **không** gọi toàn bộ repository là “0 skip”. Các skip này không nằm trong D13/PostgreSQL suite đã gate.

Các commit tiếp theo chỉ thêm log/tài liệu minh chứng, không thay source đã chạy; đối chiếu `source-sha256.txt`. Docker election là kết quả chạy local Docker thật trong `docker/`, không tuyên bố Docker election đã chạy trong job CI này. Majority commit/booking phân tán vẫn chưa nghiệm thu.
