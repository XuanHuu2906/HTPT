# TV5-D23: Raft Election + Recovery — Leader cũ quay lại (Old Leader Rejoin, Step-Down & Uncommitted Suffix Repair)

## 1. Thông tin Task
- **Mã task:** `TV5-D23`
- **Tên nhiệm vụ:** Leader cũ quay lại — Heal partition, node term cũ nhận heartbeat/AppendEntries term mới, step-down và phối hợp TV4 sửa suffix chưa commit
- **Người thực hiện:** TV5 (Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 5, 6, 7, 9; Gate G5/G6)
  - `docs/specs/01-REQUIREMENTS.md` (`FR06`, `FR07`, `FR09`; `AC08`, `AC09`)
  - `docs/specs/03-CONTRACTS.md` (§3 Envelope & `correlationId`, §6 `REQUEST_VOTE` / `APPEND_ENTRIES`)
  - `docs/specs/04-RAFT.md` (§3.1, §3.5 Step-down & timer reset; §4 AppendEntries & log matching; §5 Commit current-term rule; §6 Restart checkpoint; §8–§10 Recovery & invariants)
  - `docs/specs/07-TESTING.md` (`TC-AC08`, `TC-AC09`, `TC-RAFT-03`, `TC-RAFT-06`, `TC-RAFT-07`, `TC-RAFT-08`, `TC-RAFT-09`)
- **Ngày hoàn thành:** 23/11/2026
- **Phụ thuộc & Tích hợp:**
  - **TV5-D14 (`StepDownHandler` / `HigherTermHandler`):** Tích hợp trực tiếp `HigherTermHandler`, `PersistentElectionState`, `LeadershipGuard`, `PersistentRaftNode`, `RaftElectionComponent`, `ElectionTimer`, `LeaderHeartbeatScheduler`.
  - **TV5-D22 (`NetworkPartitionElectionTest` & `TestNetworkFilter`):** Tái sử dụng mô hình phân vùng 3–2 và hàn gắn mạng qua `TestNetworkFilter` (TV3-D21) trên `FakeNetwork` tất định.
  - **Phối hợp TV4 (không chờ TV4-D23):** Sử dụng trực tiếp các lớp đã bàn giao của TV4 (`AppendEntriesHandler`, `LogConflictResolver`, `PersistentLogAppender`, `LogConsistencyChecker`, `ReplicationTracker`, `CommitAdvancer`) ngay trong `RejoinTest` thay vì giả lập logic sửa log tay.
  - **Ranh giới kiểm thử:** `RejoinTest` trên `FakeNetwork` đạt **PASS 100% (14/14)**; kiểm thử rejoin & sửa log trên cụm 5 Node TCP + PostgreSQL thực tế được thực hiện tại **Gate G6**.

---

## 2. Kiến trúc kiểm thử & Các bất biến cốt lõi

### 2.1. Step-Down trên Leader cũ khi nhận Heartbeat / AppendEntries / Response term mới (TV5-D14)
- Khi mạng được hàn gắn (`network.heal()`), Leader cũ `node-1` ở term 1 có thể gặp hai đường kích hoạt step-down:
  1. **Nhận `APPEND_ENTRIES` / heartbeat term cao hơn từ Leader mới (`node-3`, term 2):**
     - `PersistentRaftNode.handleAppendEntries(...)` -> TV4 `AppendEntriesHandler` phát hiện `request.term() > currentTerm`.
     - `PersistentElectionState.observeHigherTerm(2)` lưu bền vững `(term=2, votedFor=null)` vào `RaftStorage` **trước** khi chạm log (SPEC-04 §9).
     - `RaftState.becomeFollower(2, "node-3")` xóa trạng thái volatile Leader (`nextIndex`, `matchIndex`), cập nhật `leaderId = "node-3"`.
     - `onStepDown` dừng heartbeat cũ; `ElectionTimer` được re-arm (từ trạng thái stopped khi là Leader).
  2. **Leader cũ gửi `APPEND_ENTRIES` term cũ trước và nhận `AppendEntriesResponse(term=2, success=false)`:**
     - Phía đa số từ chối term cũ mà **không** đổi log, **không** đổi state và **không** reset election timer (SPEC-04 §3.1).
     - `node-1` chuyển response vào `HigherTermHandler.observe(2)`: dừng heartbeat -> persist `(2, null)` -> chuyển `FOLLOWER` -> re-arm election timer.
- **Fencing callback cũ (`LeadershipGuard`):** Mọi response trễ term 1 (TC-RAFT-08) và mọi `pendingWrite` chưa commit từ term 1 bị chặn hoàn toàn, không bao giờ complete `CONFIRMED`.
- **Fail-closed khi lỗi storage:** Nếu `RaftStorage.saveTermAndVote` ném `RaftStorageException` trong lúc step-down, heartbeat vẫn bị dừng ngay, không re-arm timer, và core từ chối mọi lệnh ghi mới bằng `IllegalStateException`.

### 2.2. Phối hợp TV4 sửa suffix chưa commit (`AppendEntriesHandler` + `LogConflictResolver` + `PersistentLogAppender`)
- **TC-RAFT-07 (Heartbeat & batch ngắn):**
  - Heartbeat từ Leader mới có `prevLog` mismatch vẫn khiến Leader cũ step-down và reset election timer (SPEC-04 §3.1), nhưng không xóa suffix.
  - Heartbeat khớp prefix tại `k=1` với `leaderCommit=4` trả `matchIndex=1`, **không xóa suffix chưa khớp** `[2, 3]` và **không nâng `commitIndex` vượt quá `min(leaderCommit, matchIndex) = 1`** (không dùng local `lastLogIndex=3`).
- **Backtracking & Truncation bền vững (TC-RAFT-03):**
  - Leader mới dùng `ReplicationTracker.lowerNextIndex(...)` lùi `nextIndex` qua các điểm lệch term.
  - Khi khớp `(prevLogIndex, prevLogTerm)`, TV4 `LogConflictResolver` lập kế hoạch trên bản sao qua `LogConsistencyChecker`, kiểm tra guard `truncatedFromIndex > commitIndex`, ghi bền qua `PersistentLogAppender` -> `RaftStorage.replaceSuffix(...)`, kiểm chứng hậu ghi tiền tố đã commit `[1..commitIndex]` không đổi rồi mirror sang `RaftState`.
  - Xác minh cả 3 hình thái suffix: **1 entry mồ côi**, **suffix dài hơn log Leader mới (5 vs 3 — Raft Figure 7)**, và **phân kỳ đa term (term 1 vs term 2 -> term 3)**.
- **Bảo vệ tiền tố đã commit (SPEC-04 §4):** Xung đột cố ghi đè tại `index <= commitIndex` bị chặn fail-closed (`IllegalStateException`), tiền tố đã commit trong `RaftState` và `RaftStorage` còn nguyên vẹn.
- **Restart với suffix chưa commit (TC-RAFT-09):** `RaftState.restore(...)` khởi tạo `commitIndex = lastApplied` (không dùng `lastLogIndex`); sau heal, node chỉ apply các entry mới sau checkpoint, không double-apply.

### 2.3. Kiểm chứng vòng đời thật `RaftElectionComponent` + `PersistentRaftNode`
- Test 12 chạy `RaftElectionComponent` với `LeaderHeartbeatScheduler` thật và peer `HandlerRegistry`:
  - `node-1` thắng bầu cử làm Leader, ghi 2 entry chưa commit vào `PersistentRaftNode`.
  - Nhận wire `APPEND_ENTRIES` DTO term cao hơn từ `node-3` qua `HandlerRegistry`.
  - `LeaderHeartbeatScheduler` ngừng phát heartbeat cũ, suffix `[2, 3]` bị cắt và thay bằng `[2]` term mới bền vững trong storage, `appendCommand` tiếp theo trả `NotLeader("node-3")`.

---

## 3. Thành phần bàn giao

1. `backend-node/src/test/java/vn/nhom6/railway/raft/election/RejoinTest.java` (14 kịch bản):
   1. `rejoinAfterHeal_oldLeaderReceivesHigherTermHeartbeat_stepsDownPersistsAndArmsTimer`
   2. `rejoinAfterHeal_oldLeaderSendsStaleAppendToMajority_stepsDownViaHigherTermHandlerOnResponse`
   3. `rejoinAfterHeal_heartbeatAloneDoesNotTruncateUnmatchedSuffix_andDoesNotAdvanceCommitPastMatchedIndex`
   4. `rejoinAfterHeal_singleUncommittedGhostWrite_truncatedDurablyAndReplacedByNewLeader`
   5. `rejoinAfterHeal_longDivergentUncommittedSuffix_longerThanNewLeaderLog_truncatedAndRepaired`
   6. `rejoinAfterHeal_multiTermCascadingDivergence_oldLeaderWithMultipleStaleTermsBacktracksAndRepairs`
   7. `rejoinAfterHeal_bothMinorityNodesHaveDivergentSuffixes_allFiveNodesConvergeExactly`
   8. `rejoinAfterHeal_oldLeaderRestartsFromDurableStorageWithUncommittedSuffix_repairsWithoutDoubleApply`
   9. `rejoinAfterHeal_staleDelayedResponsesFromOldTerm_ignoredByLeadershipGuardAndNeverCommit`
   10. `rejoinAfterHeal_committedPrefixProtection_conflictAtCommittedIndexFailsClosed`
   11. `rejoinAfterHeal_storageFailureDuringHigherTermStepDown_stopsLeaderAndFailsClosed`
   12. `rejoinAfterHeal_realRaftElectionComponentLifecycle_stopsHeartbeatSchedulerAndRepairsSuffix`
   13. `rejoinAfterHeal_oldLeaderClientRedirectionAndIdempotentRetry_succeedsOnNewLeader`
   14. `rejoinAfterHeal_oldLeaderCanBeReelectedInLaterTermAfterCatchUp`
2. `artifacts/evidence/D23/TV5-D23-result.log`
3. `docs/implementation/TV5-D23-notes.md`

---

## 4. Kết quả kiểm thử đã thực thi

- **Targeted TV5-D23 (`RejoinTest`):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="RejoinTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 14, Failures: 0, Errors: 0, Skipped: 0` (PASS, 0.869 s).

- **Combined TV5-D14/D21/D22/D23 + TV4 Log Repair:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="RejoinTest,StepDownTest,NetworkPartitionElectionTest,LeaderFailoverTest,LogConflictResolverTest,AppendEntriesHandlerTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 130, Failures: 0, Errors: 0, Skipped: 0` (PASS, 2.743 s).

- **Full Election Package Regression:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="vn.nhom6.railway.raft.election.*Test" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 302, Failures: 0, Errors: 0, Skipped: 1` (PASS, BUILD SUCCESS, 5.513 s).
