# TV5-D25: Raft Election + Recovery — Chuẩn bị Failover LAN Test Script & Fixtures

## 1. Thông tin Task
- **Mã task:** `TV5-D25`
- **Tên nhiệm vụ:** Chuẩn bị failover LAN test script/fixtures (`RaftElectionLanTest` với recorded trace/fake endpoint; chuẩn bị stop-leader actions và log term; sẵn sàng chạy trên 5 laptop sau `TV6-D25`/Gate `G6`)
- **Người thực hiện:** TV5 (Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 4, 5, 7, 9, 11; Gate G5/G6)
  - `docs/specs/01-REQUIREMENTS.md` (`FR06`–`FR09`, `FR12`; `AC05`–`AC09`)
  - `docs/specs/02-ARCHITECTURE.md` (§2, §7, §8 cấu trúc log)
  - `docs/specs/03-CONTRACTS.md` (§5 `NODE_STATUS` / `NodeStatusDto`, §6 `REQUEST_VOTE` / `APPEND_ENTRIES`)
  - `docs/specs/04-RAFT.md` (§3 Election/Re-election, §8 Failure modes 4/5, 3/5, 2/5, partition 3–2, §10 Invariants)
  - `docs/specs/07-TESTING.md` (`TC-AC05A`, `TC-AC05B`, `TC-AC06`, `TC-AC07`, `TC-AC08`, `TC-AC09`, `TC-TCP-04`, `TC-DEPLOY-02`)
  - `docs/specs/08-DEPLOYMENT.md` (§2 topology 6 laptop, §3 cổng `9100`/`9200` & `NODE_PEERS`, §6 LAN runbook, §7 fault logging)
- **Ngày hoàn thành:** 25/11/2026
- **Phụ thuộc & Ranh giới trung thực:**
  - Kế thừa bộ test election/partition **TV5-D22** (`TestNetworkFilter`, `RaftElectionService`, `WriteAvailabilityPolicy`) và bộ cấu hình/inventory LAN **TV3-D23 / TV6-D24** (`configs/lan/node-1..5.properties`, `configs/lan/PORTS.md`).
  - **Không chờ TV6-D25 khởi tạo cụm LAN thật.**
  - **Fake TCP v1 endpoints + Recorded trace fixtures + Script chạy giả lập: PASS.**
  - **Failover trên 5 laptop vật lý thật: `NOT_RUN` — chỉ thực hiện tại Gate G6 sau khi TV6-D25 dựng xong cụm LAN.**

---

## 2. Thiết kế & Các thành phần đã triển khai

### 2.1. Mở rộng `RaftElectionService.java` (TV5-D25)
File: `backend-node/src/main/java/vn/nhom6/railway/raft/election/RaftElectionService.java`
- **`TermLogEntry` (record):** Ghi nhận có cấu trúc mọi biến chuyển term/role/leader/vote/commit theo `SPEC-02 §8` (`ELECTION_STARTED`, `VOTE_GRANTED`, `LEADER_ELECTED`, `HIGHER_TERM_STEP_DOWN`, `STOP_LEADER_PREPARED`, `STOP_LEADER_EXECUTED`, `NODE_RESTARTED`, `PARTITION_APPLIED`, `PARTITION_HEALED`, `RECOVERY_RESTORED`), kèm `toLogLine()` và `assertTermMonotonicity()`.
- **`StopLeaderAction` (record) & phát hiện Leader động:**
  - `prepareStopLeaderFromStatuses(statuses, clientEndpoints, reason)`: Đọc trực tiếp `NodeStatusDto` chuẩn `SPEC-03 §5`, tuyệt đối không hardcode `node-1`, **fail-closed** khi có `0` Leader (`NO_LEADER_FOUND`), `> 1` Leader (`MULTIPLE_LEADERS_DETECTED`), hoặc Leader có `storageHealthy=false` (`UNHEALTHY_LEADER`).
  - `prepareLocalStopLeaderAction(...)`, `simulateStopLeader(...)`, `simulateRestartAfterStop()`: Chuẩn bị và thực thi hành động dừng Leader mô phỏng mà **không xóa storage bền vững**, phục hồi `currentTerm`/`votedFor` không giảm term khi restart.
- **`TraceAuditReport` (record) & kiểm toán bất biến:**
  - `auditTermLog(entries)`: Kiểm tra tính đơn điệu của term theo từng node và Election Safety (tối đa 1 Leader duy nhất mỗi term).
  - `auditRecordedStatusTrace(traceSteps)`: Validate từng `NodeStatusDto`, kiểm tra term không giảm, `0 <= lastApplied <= commitIndex <= lastLogIndex`, không có 2 Leader cùng term, và bước thiểu số `< 3/5` không bao giờ bầu được Leader mới.

### 2.2. Fixtures Recorded Trace & LAN Inventory Placeholder
Thư mục: `backend-node/src/test/resources/fixtures/lan-failover/`
1. `lan-inventory-placeholder.json`: Ánh xạ 5 voter (`192.168.10.11..15`, client `9100`, peer `9200`, 5 DB riêng `railway_node1..5`) + Laptop 6 Monitor non-voter (`192.168.10.16`), gắn rõ `"mode": "PLACEHOLDER_TEMPLATE"`, `"physicalLanVerified": false`, `"deferredToGate": "G6"`.
2. `recorded-trace-leader-failover-4of5.json`: Trace 4 bước — Leader động `node-3` (term 5) dừng -> 4/5 bầu `node-1` (term 6) -> `node-3` rejoin và catch-up.
3. `recorded-trace-3of5-and-2of5-quorum-loss.json`: Trace 5 bước — 3/5 sống sót và bầu Leader mới -> mất thêm node còn 2/5 (term tăng 4 -> 5, 0 Leader, `commitIndex` đóng băng) -> khôi phục 3/5 bầu Leader term 6.
4. `recorded-trace-partition-3-2-heal.json`: Trace phân vùng 3–2 (`{node-1, node-2}` vs `{node-3, node-4, node-5}`) -> thiểu số không commit, đa số bầu `node-4` term 4 -> heal hội tụ 5/5.
5. `recorded-trace-split-brain-negative.json`: Trace âm tính cố tình tạo 2 Leader trong term 7 và tụt term `7 -> 6` để chứng minh bộ kiểm toán fail-closed.

### 2.3. Bộ test `RaftElectionLanTest.java` (15 kịch bản)
File: `backend-node/src/test/java/vn/nhom6/railway/raft/election/RaftElectionLanTest.java`
- Kết hợp **5 fake TCP v1 endpoints thật trên loopback** (`TcpServer` + `MessageDispatcher(CLIENT)` + `NodeStatusHandler` + `TcpClient`) với `RaftElectionService`, `PersistentRaftNode`, và `TestNetworkFilter`:
  1. Tải & đối chiếu `lan-inventory-placeholder.json` + `configs/lan/node-1..5.properties` + `PORTS.md` + `ElectionRuntime.parsePeers`.
  2. Probe `NODE_STATUS` qua TCP v1 và phát hiện động Leader không phải `node-1` (`node-3`).
  3. Thực thi `StopLeaderAction` -> endpoint Leader cũ OFFLINE -> 4/5 bầu Leader mới term cao hơn -> log term đạt chuẩn.
  4. Mất 2 node (còn 3/5) -> bầu Leader hợp lệ và `WriteAvailability` báo `AVAILABLE (3/5)`.
  5. Mất 3 node (còn 2/5) -> tối đa 2/5 phiếu, 0 Leader, `NO_QUORUM`, `prepareStopLeaderFromStatuses` fail-closed.
  6. Leader cũ restart sau stop -> đọc storage bền vững, giữ term/vote, nhận higher term step-down và rejoin 5/5.
  7. Channel isolation cổng Client 9100 (`TC-TCP-04`): từ chối `REQUEST_VOTE` bằng `ERROR_RESPONSE(INVALID_REQUEST)`.
  8. Phân vùng mạng LAN 3–2 qua `TestNetworkFilter` + heal.
  9. Tắt Laptop 6 (Monitor non-voter — `TC-DEPLOY-02`): quorum 3/5 và bầu cử không đổi.
  10–13. Replay & audit đủ 4 recorded trace fixtures (3 dương tính + 1 âm tính split-brain/term-regression).
  14. Fail-closed guards của `prepareStopLeaderFromStatuses` (0 Leader, >1 Leader, `storageHealthy=false`).
  15. Mô phỏng firewall drop bất đối xứng cô lập Leader -> 4/5 failover -> mở firewall step-down.

### 2.4. Script kiểm thử LAN Failover (`scripts/test-election-lan.py`)
- Python 3 stdlib, không cần thư viện ngoài, hỗ trợ 3 chế độ:
  - `--mode fake`: Khởi chạy 5 fake TCP v1 endpoints nội bộ, chạy toàn bộ chuỗi failover 5/5 -> 4/5 -> 3/5 -> 2/5 -> 5/5 và audit term log.
  - `--mode replay --trace <fixture.json>`: Replay và audit recorded trace JSON.
  - `--mode live --inventory <inventory.json> [--dry-run]`: Sẵn sàng cho **TV6-D25 / G6** trên 5 laptop vật lý thật; tự động **fail-closed (exit code 2)** nếu inventory vẫn đang dùng IP placeholder (`192.168.10.*`).

---

## 3. Kết quả kiểm thử đã thực thi
- **Targeted LAN Failover + D22 + D23 Preflight:**
  ```bash
  mvn -B -ntp -pl backend-node -am test \
    -Dtest="RaftElectionLanTest,LanConfigPreflightTest,RaftElectionServiceTest,NetworkPartitionElectionTest" \
    -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 50, Failures: 0, Errors: 0, Skipped: 0` (**PASS**, `BUILD SUCCESS` — trong đó `RaftElectionLanTest` 15/15 PASS).
- **Full Raft Election Regression:**
  ```bash
  mvn -B -ntp -pl backend-node -am test \
    -Dtest="vn.nhom6.railway.raft.election.*Test" \
    -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 335, Failures: 0, Errors: 0, Skipped: 5` (**PASS**, `BUILD SUCCESS`, 6.007 s; 5 test skip là JDBC PostgreSQL tùy chọn khi không đặt biến kết nối PG).
- **LAN Failover Script (`scripts/test-election-lan.py`):**
  - `--mode fake`: **PASS** (exit code `0`)
  - `--mode replay` (3 positive fixtures): **PASS** (exit code `0`)
  - `--mode live --inventory lan-inventory-placeholder.json --dry-run`: **BLOCKED fail-closed** đúng thiết kế (exit code `2`) do IP vẫn là placeholder.
- **Evidence:** `artifacts/evidence/D25/TV5-D25-result.log`.
- **Phạm vi còn lại cho Gate G6:** Chạy `scripts/test-election-lan.py --mode live` cùng `scripts/stop-node.ps1 -TargetLeader` trên 5 laptop vật lý thật + Laptop 6 Monitor sau khi TV6-D25 hoàn tất triển khai cụm LAN.
