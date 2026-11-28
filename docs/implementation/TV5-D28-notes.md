# TV5-D28: Đóng băng Election & Recovery (`RaftElectionService`)

## 1. Thông tin Task
- **Mã task:** `TV5-D28`
- **Mục tiêu:** Đóng băng phân hệ Raft Election & Recovery: loại mock election ở production mode, hoàn thiện event logs và config timeout, chạy lại bộ kiểm thử G4–G6 làm kiểm tra release candidate Election riêng trước khi G7 tag chung sau merge.
- **Owner:** TV5 (`backend-node/**/raft/election/**`)
- **Snapshot đầu vào:** Election regression D27 + Raft/protocol snapshot cuối D27 (`123a83934530229263085d5845d376bdd8654691`), không chờ `TV4-D28` hoặc `TV3-D28` freeze.
- **Đối chiếu SPEC:**
  - `docs/specs/02-ARCHITECTURE.md` (§8 Nhật ký và khả năng quan sát)
  - `docs/specs/04-RAFT.md` (§2 Trạng thái, §3 Election/Timers, §5 Quorum, §8 Fault/Recovery, §9 Durability/Fail-closed, §10 Invariants)
  - `docs/specs/07-TESTING.md` (`TC-RAFT-01..09`, `TC-AC03`, `TC-AC05..10`)
  - `docs/specs/08-DEPLOYMENT.md` (§3.1 Config & Timeout)
  - `AGENTS.md` (§0, §3, §5, §6)
- **Hợp đồng giữ nguyên:** Không sửa wire DTO (`shared/**`), không sửa API công khai của `raft/core/**`, `raft/replication/**`, hoặc `networking/**`.

---

## 2. Các thay đổi đóng băng trong `RaftElectionService.java`

File: [`RaftElectionService.java`](file:///home/nguyenhuu/Hoc%20Tap/HTPT/railway-distributed-system/backend-node/src/main/java/vn/nhom6/railway/raft/election/RaftElectionService.java)

### 2.1. Loại mock election ở Production Mode (`RuntimeMode`)
- Bổ sung `RuntimeMode` (`PRODUCTION`, `TEST_SIMULATION`), các factory `RaftElectionService.production(...)`, `RaftElectionService.fromEnvironment(...)`, `resolveRuntimeMode(...)` (mặc định `PRODUCTION` khi không khai báo test/simulation) và `isProductionModeConfigured()`.
- Ở `RuntimeMode.PRODUCTION`:
  - **Bắt buộc** `storage != null` (bền vững), `node instanceof PersistentRaftNode` (chặn mock `RaftNode`), `transport != null && transport != NO_OP_TRANSPORT` (chặn no-op/mock transport), và `TimeoutConfig.validateForProduction()`.
  - Khi node thắng cử lên `Role.LEADER` mà chưa gắn policy ngoài, tự động khởi tạo `WriteAvailabilityPolicy` thật gắn `ReplicationTracker` + `CommitAdvancer` và `quorumFreshnessWindow` cấu hình; chặn `activeVoterCountFallback(...)` ở `PRODUCTION`.
  - Khi biến môi trường/system property đặt chế độ `production` (`RAFT_MODE`, `RAILWAY_MODE`, `railway.raft.mode`), constructor 4 tham số không transport (dùng cho test double) ném `IllegalStateException`.
  - Chặn toàn bộ helper mô phỏng/mock bằng `requireSimulationMode(...)` (ném `UnsupportedOperationException` + ghi log `WARNING`): `isolatePeer()`, `healPeer()`, `partition()`, `healAll()`, `simulateStopLeader()`, `simulateRestartAfterStop()`, `setClock()`, `clearTermLog()`.

### 2.2. Hoàn thiện Event Logs (`SPEC-02 §8` & `SPEC-04 §10`)
- Bổ sung đầy đủ các mã sự kiện còn thiếu bên cạnh bộ D25:
  - `EVENT_NODE_STARTED`, `EVENT_TIMEOUT_CONFIGURED`, `EVENT_VOTE_DENIED`, `EVENT_STEP_DOWN`, `EVENT_RECOVERY_FAILED`, `EVENT_FAIL_CLOSED`.
- `recordTermEvent()` điền đúng `leaderId = localId` khi đang là `LEADER`, xuất log có cấu trúc qua `java.util.logging.Logger` (`[RAFT-ELECTION]`) theo mức `INFO` / `WARNING` / `SEVERE` kèm `mode`, `lastApplied`, `commitIndex`, `lastLogIndex`, `term`, `role`, `leader`, `votedFor`, và phát tới listener (`addEventListener`/`removeEventListener`).
- Ghi nhận `VOTE_GRANTED` / `VOTE_DENIED` cho cả phản hồi `VoteTransport` (chèn đúng thứ tự thời gian trước `LEADER_ELECTED` khi thắng cử đồng bộ), từ chối ở `handleRequestVote()`, `recordVoteResponse()` bị từ chối/bỏ qua, `STEP_DOWN` cùng term, `RECOVERY_FAILED`, và `FAIL_CLOSED` (bao gồm cả trường hợp tụt term trong storage ở `validateDurableState()`).
- Bổ sung `TermLogEntry.toDetailedLogLine(long lastApplied, RuntimeMode mode)` và `exportEventLogs()`.

### 2.3. Hoàn thiện Config Timeout (`TimeoutConfig`)
- Bổ sung record `TimeoutConfig(ElectionConfig electionConfig, Duration rpcTimeout, Duration quorumFreshnessWindow)` dùng chung `ElectionConfig` (không tạo mô hình timer song song):
  - `TimeoutConfig.defaults()`, `TimeoutConfig.lanProductionDefaults()`, `TimeoutConfig.fromEnvironment(env, props)` đọc `RAFT_HEARTBEAT_INTERVAL_MS`, `RAFT_ELECTION_TIMEOUT_MIN_MS`, `RAFT_ELECTION_TIMEOUT_MAX_MS`, `RAFT_RPC_TIMEOUT_MS` / `TCP_RESPONSE_DEADLINE_MS`, `RAFT_QUORUM_FRESHNESS_MS`.
  - `validateForProduction()` kiểm tra bất biến liveness/safety Raft:
    - `electionTimeoutMinMillis >= heartbeatIntervalMillis * 3`
    - `electionTimeoutMaxMillis - electionTimeoutMinMillis >= heartbeatIntervalMillis`
    - `rpcTimeout <= electionTimeoutMaxMillis`
    - `quorumFreshnessWindow >= heartbeatIntervalMillis * 3`
  - Truyền `quorumFreshnessWindow` vào `WriteAvailabilityPolicy` và gắn `ElectionTimeoutPolicy` (`nextElectionTimeoutMillis()`).

---

## 3. Bộ kiểm thử mới & chạy lại G4–G6

Test mới: [`ElectionFreezeTest.java`](file:///home/nguyenhuu/Hoc%20Tap/HTPT/railway-distributed-system/backend-node/src/test/java/vn/nhom6/railway/raft/election/ElectionFreezeTest.java) (4 ca kiểm tra toàn diện `PRODUCTION` mode, chặn mock/simulation, event logs đầy đủ + Logger, và `TimeoutConfig` cho cả `compose.dev.yml`, `dev.yml`, `lan.yml`, `compose.election-test.yml`).

| Hạng mục kiểm thử | Lệnh / Môi trường | Kết quả thực chạy |
|---|---|---|
| TV5-D28 Freeze + TV5-D27 Safety Review | `mvn -B -ntp -pl backend-node -am test -Dtest="ElectionFreezeTest,ElectionSafetyReviewTest"` | **PASS** (`14/14`, `0` skipped, `5.439 s`) |
| Full Raft Regression + PostgreSQL 16 thật | Container Docker `postgres:16` (`127.0.0.1:15433/railway_node1`), `-Drailway.requirePostgres=true`, `vn.nhom6.railway.raft.**.*Test` | **PASS** (`801/801`: Core `124`, Election `349`, Replication `328`, `0` skipped, `27.215 s`) |
| G4–G5 Live 5-Node Real-TCP & Networking | `FiveNodeTcpConsensusIntegrationTest`, `LanConfigPreflightTest`, `LanLivePreflightAndIsolationTest`, `ProtocolFuzzAndConcurrencyRegressionTest`, `TcpPacketLossTimeoutIntegrationTest` | **PASS** (`22/22`, `0` skipped, `20.380 s`) |
| G4–G6 Integration Tests + Node 1 PG 16 | `Node1DatabaseIntegrationTest` (PG16 thật), `PartitionHarness`, `AcceptanceSuiteTest`, `ClusterSmokeTest`, `ConcurrentBookingRunner`, `ReadBarrierAndRetryTest` | **PASS** (`47` run, `0` fail, `3` opt-in skipped: `9.867 s`) |
| Shared Protocol Contract | `mvn -B -ntp -pl shared test` | **PASS** (`38/38`, `0` skipped, `1.051 s`) |
| G6 LAN Failover Python Runner | `scripts/test-election-lan.py` (`--mode fake`, `--mode replay` x3, `--mode live` placeholder guard) | **PASS** (`fake` + 3 `replay` exit `0`; `live` placeholder exit `2` fail-closed) |
| PowerShell runners (`g5-acceptance.ps1`, `g6-lan-fault.ps1`), Docker `cluster.smoke`, LAN 6 laptop vật lý | Linux workstation không có `pwsh`, không có sẵn image `railway-backend-node:d13-election`, không có 6 laptop LAN | **NOT_RUN / BLOCKED_ENV** |

Bằng chứng chi tiết: [`artifacts/evidence/D28/TV5-D28-result.log`](file:///home/nguyenhuu/Hoc%20Tap/HTPT/railway-distributed-system/artifacts/evidence/D28/TV5-D28-result.log).
