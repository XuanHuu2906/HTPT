# TV5-D24: Raft Election + Recovery — Phục hồi term/vote sau restart

## 1. Thông tin Task
- **Mã task:** `TV5-D24`
- **Tên nhiệm vụ:** Phục hồi term/vote sau restart — Tắt và restart Candidate/Follower/Leader, đọc `term`/`votedFor`, reset election timers đúng, không vote lại hai lần trong một term, không xóa log cũ
- **Người thực hiện:** TV5 (Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 5, 6, 9)
  - `docs/specs/01-REQUIREMENTS.md` (`FR06`, `FR07`, `FR09`; `AC07`, `AC08`)
  - `docs/specs/03-CONTRACTS.md` (§6 `REQUEST_VOTE`, `APPEND_ENTRIES`)
  - `docs/specs/04-RAFT.md` (§2 Trạng thái Node; §3.1–§3.5 Election/Vote/Timer; §6 Restart checkpoint & replay; §8–§10 Durability & Invariants)
  - `docs/specs/07-TESTING.md` (`TC-RAFT-01`, `TC-RAFT-02`, `TC-RAFT-05`, `TC-RAFT-08`, `TC-RAFT-09`, `TC-AC07`, `TC-AC08`)
- **Ngày hoàn thành:** 24/11/2026
- **Phụ thuộc & Tích hợp:**
  - **TV2-D13 Persistent Loader (`DbStateLoader`):** Kiểm tra `GENESIS` vs `RESTART`, `currentTerm`, `votedFor`, `lastLogIndex`, `lastLogTerm`, `lastApplied`, `initialCommitIndex == lastApplied`, và fail-closed khi dữ liệu hỏng/lệch checkpoint (xác minh cả in-memory lẫn PostgreSQL 16 thật).
  - **TV4-D22 Startup Replay (`LogReplayCoordinator`):** Điều phối replay giữa `RaftState` và `BookingRepository` qua `RaftStateMachineApplier` + `CommittedCommandApplier` / `JdbcCommittedCommandApplier` (`NOTHING_TO_REPLAY`, `REPLAYED`, `FAILED_CLOSED`, `restoreAligned`), bảo vệ uncommitted tail.
  - **TV5 Election Runtime (`RaftElectionComponent`, `PersistentRaftNode`, `PersistentElectionState`, `CandidateElection`, `ElectionTimer`, `HigherTermHandler`, `LeaderHeartbeatScheduler`):** Kiểm chứng đầy đủ ở cả 3 lớp: deterministic unit/5-node harness, real PostgreSQL 16 JDBC, và cụm 5 Docker container + 5 PostgreSQL thật qua TCP v1.

---

## 2. Các bất biến & kịch bản được kiểm chứng

### 2.1. Tắt và restart cả 3 vai trò (Follower / Candidate / Leader)
- **Follower restart (Test 1, 6, 14, JDBC-1, Live Docker Step 2):**
  - Đọc đúng `currentTerm`, `votedFor` qua `DbStateLoader` + `RaftStorage.load()` + `RaftState.restore(...)`.
  - Khởi động lại ở `Role.FOLLOWER`, `leaderId = null`, `commitIndex = lastApplied` (không suy diễn từ `lastLogIndex`).
  - Không cấp phiếu lần 2 cho candidate khác cùng term; retry cùng candidate trả `voteGranted=true` idempotent; term cũ bị từ chối và `currentTerm` không giảm.
- **Candidate restart giữa chiến dịch (Test 2, JDBC-2):**
  - Candidate đã tăng term và persist `(term, selfId)` vào `raft_meta` rồi tắt trước khi gom đủ 3/5 phiếu.
  - Khi restart: trở về `Role.FOLLOWER`, giữ `votedFor = selfId` nên từ chối mọi candidate khác cùng term; khi election timer mới hết hạn sẽ tăng sang `term + 1` và persist xuống PostgreSQL.
- **Leader restart (Test 3, 11, 12, JDBC-3, Live Docker Step 3):**
  - Tắt Leader dừng toàn bộ `LeaderHeartbeatScheduler` (không phát thêm heartbeat sau `close()`).
  - Sau restart: về `Role.FOLLOWER`, xóa `nextIndex`/`matchIndex`, từ chối client write với `NotLeader`, arm lại `ElectionTimer` mới, giữ nguyên `votedFor = selfId` ở term cũ và bảo toàn cả committed prefix lẫn uncommitted tail.

### 2.2. Quy tắc Election Timer sau restart (SPEC-04 §3.1 — Test 4, 11, JDBC-3)
- `timer.start()` arm đúng một deadline sau restart; gọi lặp lại là idempotent.
- Stale `APPEND_ENTRIES` và `REQUEST_VOTE` bị từ chối (kể cả khi `request.term > currentTerm`) **không** reset timer.
- `REQUEST_VOTE` được cấp phiếu persist `(term, candidateId)` **trước** khi reset timer.
- `APPEND_ENTRIES` hợp lệ với term không cũ (kể cả khi `prevLog` mismatch) luôn reset timer.
- `RaftElectionComponent` thật (cả in-memory lẫn PostgreSQL `JdbcRaftStorage`) xác minh timer mới thực sự hết hạn và phát động election ở term cao hơn khi mất heartbeat.

### 2.3. Phối hợp TV2-D13 `DbStateLoader` & TV4-D22 `LogReplayCoordinator` và bảo toàn log cũ (Test 5, 7–10, JDBC-1..4)
- **Không xóa log cũ (`TC-RAFT-02`, `TC-RAFT-09`):** `replaceSuffixCalls == 0` khi restart; số dòng `raft_log` trong PostgreSQL giữ nguyên tuyệt đối; luật log up-to-date `(lastLogTerm, lastLogIndex)` sau restart vẫn chặn candidate có log cũ hơn.
- **Clean restart với uncommitted tail (Test 7, JDBC-1):** `initialCommitIndex == lastApplied`, `LogReplayCoordinator` trả `NOTHING_TO_REPLAY`, giữ nguyên tail chưa commit cho đến khi Leader xác nhận `leaderCommit`.
- **Crash giữa durable DB apply và Raft `markApplied` (Test 8, JDBC-4 — `TC-RAFT-05`):** `LogReplayCoordinator` redeliver idempotent không tạo vé/sự kiện trùng trong PostgreSQL (`tickets`, `business_events`), không xóa tail chưa commit, và giữ nguyên luật phiếu.
- **Raft checkpoint vượt durable DB (Test 9):** `LogReplayCoordinator` fail-closed và phục hồi chuẩn bằng `restoreAligned(persisted, durable)` giữ nguyên `currentTerm`/`votedFor`/log cũ.
- **Fail-closed & Ambiguous write crash (Test 10):** Từ chối khởi động khi `lastApplied > lastLogIndex`, log gap, entry term > `currentTerm`, hoặc `votedFor` non-voter; sau ambiguous write crash vẫn tôn trọng phiếu đã xuống disk.

### 2.4. Cụm 5 Node trên mạng tất định & cụm 5 Docker Container + 5 PostgreSQL thật
- **Deterministic 5-node (Test 12, 13):** Leader crash/restart + catch-up hội tụ 5/5; Follower restart giữa split vote ngăn 2 Leader cùng term.
- **Live 5-Node Docker + 5-PostgreSQL 16 (TCP v1 thật):**
  - Baseline 5/5 khỏe mạnh (`leader=node-5`, `term=41`, `logCount=15`, `lastApplied=15`).
  - `docker stop/start` Follower `railway-backend-node-1`: phục hồi `FOLLOWER`, `term=41`, `votedFor=node-5`, `logCount=15`; gửi trực tiếp TCP peer `REQUEST_VOTE(term=41, node-2)` nhận `voteGranted=False`, gửi lại `REQUEST_VOTE(term=41, node-5)` nhận `voteGranted=True`.
  - `docker stop` Leader `railway-backend-node-5`: 4 node còn lại timeout và bầu `node-4` ở `term=50`. `docker start railway-backend-node-5`: khởi động với `DbStateLoader bootstrapType=RESTART`, giữ nguyên toàn bộ 15 dòng `raft_log`, cụm hội tụ 5/5 ở `term=51`, đồng thời từ chối `REQUEST_VOTE` cùng term `51` từ `node-1` (`voteGranted=False`).

---

## 3. Thành phần bàn giao
1. `backend-node/src/test/java/vn/nhom6/railway/raft/election/ElectionRestartTest.java` (18 kịch bản: 14 in-memory/component + 4 real PostgreSQL JDBC)
2. `artifacts/evidence/D24/TV5-D24-result.log`
3. `docs/implementation/TV5-D24-notes.md`

---

## 4. Kết quả kiểm thử đã thực thi
- **Real PostgreSQL JDBC Restart & Replay (`ElectionRestartTest` + `PersistenceRestartTest` + `LogReplayCoordinatorJdbcTest`):**
  ```bash
  RAILWAY_TEST_JDBC_URL="jdbc:postgresql://127.0.0.1:5438/railway_node1" \
  RAILWAY_TEST_DB_USER="railway" \
  RAILWAY_TEST_DB_PASSWORD="railway_secret_local_only" \
  mvn -B -ntp -pl backend-node test -Drailway.requirePostgres=true \
    -Dtest="ElectionRestartTest,PersistenceRestartTest,LogReplayCoordinatorJdbcTest" \
    -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 41, Failures: 0, Errors: 0, Skipped: 0` (PASS, `10.814 s` — gồm `ElectionRestartTest` 18/18, `PersistenceRestartTest` 18/18 với 12/12 real PostgreSQL, `LogReplayCoordinatorJdbcTest` 5/5).

- **Full Election Package Regression (with real PostgreSQL enabled):**
  ```bash
  RAILWAY_TEST_JDBC_URL="jdbc:postgresql://127.0.0.1:5438/railway_node1" \
  RAILWAY_TEST_DB_USER="railway" \
  RAILWAY_TEST_DB_PASSWORD="railway_secret_local_only" \
  mvn -B -ntp -pl backend-node test -Drailway.requirePostgres=true \
    -Dtest="vn.nhom6.railway.raft.election.*Test" \
    -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 320, Failures: 0, Errors: 0, Skipped: 0` (PASS, `8.828 s`).

- **Live 5-Node Docker + 5-PostgreSQL Container Restart Verification:**
  `PASS` (exit code `0`) — Follower stop/restart + live TCP single-vote-per-term, Leader stop + 4/5 reelection + Old Leader restart (`DbStateLoader bootstrapType=RESTART`) + 5/5 PostgreSQL convergence với `logCount=15, lastApplied=15` bảo toàn trên cả 5 DB.
