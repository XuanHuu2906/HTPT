# TV5-D22: Raft Election + Recovery — Phân vùng mạng 3–2 (NetworkPartitionElection Test Suite)

## 1. Thông tin Task
- **Mã task:** `TV5-D22`
- **Tên nhiệm vụ:** Phân vùng mạng 3–2 (3–2 Network Partition Election & Safety Suite)
- **Người thực hiện:** Nguyễn Xuân Hữu (TV5 — Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (Gate G5 & G6: Kiểm thử chịu lỗi phân vùng, không chia não, phục hồi sau phân vùng mạng)
  - `docs/specs/01-REQUIREMENTS.md` (FR10, FR11, FR12: Bầu cử, chịu lỗi phân vùng 3-2, leader minority không commit, failover an toàn)
  - `docs/specs/03-CONTRACTS.md` (§4.4: mã lỗi `NOT_LEADER` + `leaderHint`, `NO_QUORUM`, `REQUEST_ID_CONFLICT`)
  - `docs/specs/04-RAFT.md` (§3 Election & Re-election, §4 AppendEntries, §5 Commit Invariants, §6 Thứ tự apply, §9 Persistence)
  - `docs/specs/05-DATABASE.md` (§3 Idempotency, Request Results, Seat Constraints)
- **Ngày hoàn thành:** 22/11/2026
- **Phụ thuộc & Tích hợp:**
  - **TV3-D21 TestNetworkFilter:** Tích hợp trực tiếp bộ lọc mạng `TestNetworkFilter` làm cơ chế định tuyến và chặn gói tin (bản tin RPC `REQUEST_VOTE`, `APPEND_ENTRIES`) cho `FakeNetwork`.
  - **TV5-D21 LeaderFailoverTest & FakeNetwork:** Tái sử dụng và hoàn thiện mô hình giả lập bầu cử, sao chép và commit qua giao diện đặt vé giả lập.
  - **Độc lập tiến độ:** Không chờ harness `TV6-D22` (`integration-tests/PartitionHarness.java` hoặc Docker fault runner); kiểm thử phân vùng mạng 3–2 trên `FakeNetwork` đạt **PASS 100% (16/16 test methods)**; kiểm thử phân vùng trên mạng vật lý 5 laptop diễn tập tại Gate G6.

---

## 2. Mục tiêu kỹ thuật & Các bất biến cốt lõi (Core Invariants)

### 2.1. Phân vùng mạng 3–2 không kill process
- Mô phỏng phân tách mạng giữa hai nhóm node:
  - **Phía thiểu số A (2/5):** `{node-1, node-2}` (chứa leader cũ `node-1`).
  - **Phía đa số B (3/5):** `{node-3, node-4, node-5}`.
- Không kill tiến trình JVM; các tiến trình tiếp tục chạy để phân biệt chính xác giữa **Partition** và **Node Crash**.
- `TestNetworkFilter.partition(partA, partB)` chặn 2 chiều toàn bộ 12 directed link giữa A và B:
  - Cho phép lưu thông nội bộ trong nhóm A (`node-1` <-> `node-2`).
  - Cho phép lưu thông nội bộ trong nhóm B (`node-3` <-> `node-4` <-> `node-5`).
  - Chặn và ghi nhận drop đối với mọi gói tin qua lại giữa A và B.

### 2.2. Leader phía thiểu số (2/5) không được commit write
- Khi mạng phân vùng, `node-1` có thể chưa biết mình đã mất kết nối với đa số và vẫn giữ `Role.LEADER` ở term 1.
- Client gửi lệnh `bookTicket` (ghế `G02`) tới `node-1`.
- `node-1` ghi log cục bộ tại index 2 term 1 và sao chép:
  - Bản tin tới `node-2` thành công (ack matchIndex=2).
  - Bản tin tới `node-3, node-4, node-5` bị `TestNetworkFilter` drop.
- Tổng số node ack matchIndex >= 2 chỉ là 2/5 (thiếu quorum 3/5).
- **Bất biến cam kết:**
  - `commitIndex` trên `node-1` không tiến triển (giữ nguyên giá trị 1 từ term trước).
  - Lệnh ghi bị treo hoặc không hoàn thành (`future.isDone() == false`).
  - **Tuyệt đối không sinh sự kiện `BOOK_CONFIRMED`** cho client.
  - StateMachine trên `node-1` giữ nguyên trạng thái ghế `G02` là `AVAILABLE`.

### 2.3. Candidate phía thiểu số không thể đắc cử
- Nếu node trong nhóm thiểu số (`node-2`) timeout và phát động bầu cử ở term 2:
  - Bản tin `RequestVote` gửi sang `{node-3, node-4, node-5}` bị `TestNetworkFilter` drop.
  - `node-2` chỉ nhận tối đa 2/5 phiếu (self-vote + `node-1`).
  - Quorum yêu cầu 3/5 -> `node-2` không thể đắc cử, giữ nguyên vai trò `Role.CANDIDATE`.
  - Phía thiểu số duy trì 0 Leader hoạt động.

### 2.4. Phía đa số (3/5) bầu Leader mới ở term cao hơn
- Các follower trong nhóm đa số `{node-3, node-4, node-5}` mất tín hiệu heartbeat từ `node-1`.
- `node-3` kích hoạt election timeout và chuyển sang `Role.CANDIDATE` ở term 2.
- `node-3` phát động bầu cử:
  - Yêu cầu gửi tới `{node-1, node-2}` bị `TestNetworkFilter` chặn.
  - Yêu cầu gửi tới `node-4` và `node-5` được chuyển giao thành công.
- `node-4` và `node-5` kiểm tra term 2 > 1 và log up-to-date -> Đồng ý cấp phiếu.
- `node-3` thu thập đủ 3/5 phiếu tuyệt đối -> Đắc cử `Role.LEADER` ở term 2.
- Bền vững lưu trữ: `node-3, node-4, node-5` ghi nhận `term = 2`, `votedFor = "node-3"` vào `RaftStorage`.

### 2.5. Phía đa số tiếp nhận và commit lệnh ghi bình thường
- Client gửi lệnh `bookTicket` cho ghế `G02` (khách "Dave") tới Leader mới `node-3`.
- `node-3` append entry tại index 2 term 2, sao chép tới `node-4` và `node-5`.
- Cả `node-4` và `node-5` ghi nhận và phản hồi matchIndex=2.
- `node-3` đạt commit quorum (3/5 matchIndex >= 2) -> `commitIndex` tiến lên 2.
- Lệnh được apply vào StateMachine -> Trả về `ApplyResult.Outcome.CONFIRMED` kèm `ticketId` hợp lệ.
- Lượt heartbeat tiếp theo chuyển giao `leaderCommit = 2` tới `node-4` và `node-5`, đồng bộ trạng thái `BOOKED` ghế `G02` trên toàn phía đa số.

### 2.6. Từ chối trên Follower và chuyển tiếp (Redirection)
- Client gửi yêu cầu vào Follower `node-4` ở phía đa số.
- `node-4` ném `BookingRequestHandler.NotLeaderException` mang theo `leaderHint = "node-3"`.
- Client chuyển tiếp theo `leaderHint` tới `node-3` và được xác nhận thành công.

### 2.7. Hàn gắn mạng (Network Heal) và Giáng chức Leader cũ
- Khi mạng phục hồi (`network.heal()`), `TestNetworkFilter` dỡ bỏ toàn bộ 12 blocked links.
- Leader mới `node-3` (term 2) gửi heartbeat/AppendEntries tới `node-1` và `node-2`.
- `node-1` nhận bản tin với `term = 2 > currentTerm 1`:
  - Ngay lập tức giáng chức xuống `Role.FOLLOWER`.
  - Cập nhật `currentTerm = 2` và `leaderId = "node-3"`.
  - `storage.saveTermAndVote(2L, null)` lưu trạng thái bền vững.
- Toàn cụm duy trì tối đa 1 Leader duy nhất (`node-3`).

### 2.8. Ghi đè Log mồ côi (Ghost Write Overwriting) & Triệt tiêu Split-Brain
- Trước khi hàn gắn:
  - `node-1` có entry tại index 2 mang term 1 (lệnh ghi mồ côi của "Minority Ghost").
  - `node-3` có entry tại index 2 mang term 2 (lệnh ghi hợp lệ của "Dave").
- Sau khi hàn gắn:
  - `node-1` nhận AppendEntries từ `node-3` với `prevLogIndex = 1, prevLogTerm = 1`.
  - Phát hiện xung đột tại index 2 (term 1 != term 2), `node-1` cắt bỏ suffix (`truncateSuffixFrom(2)`) và ghi đè entry term 2 của Dave.
  - `commitIndex` trên `node-1` tiến lên 2, StateMachine apply lệnh của Dave.
  - StateMachine trên `node-1` xác nhận ghế `G02` thuộc về Dave, thông tin "Minority Ghost" bị triệt tiêu hoàn toàn.

### 2.9. Hội tụ toàn diện 5 Node (Cluster Convergence)
- Sau khi hàn gắn và hoàn tất một chu kỳ sao chép:
  - Cả 5 node (`node-1`..`node-5`) đạt cùng trạng thái:
    - `currentTerm() == 2`
    - `commitIndex() == 2`
    - `lastApplied() == 2`
    - `lastLogIndex() == 2`
  - Nội dung log entries tại mọi index hoàn toàn giống nhau.
  - Bảng trạng thái ghế (Seat Snapshot) trên toàn bộ 5 node hoàn toàn đồng nhất.

### 2.10. Kiểm định trực tiếp TestNetworkFilter
- Xác nhận cơ chế lọc mạng:
  - Chặn đúng 12 directed links trong phân vùng 3–2.
  - Đo lường chính xác telemetry `totalDroppedCount`, `totalDeliveredCount` và `droppedByLink`.
  - Kiểm tra tính năng lọc có chọn lọc `dropMessageType(REQUEST_VOTE)`: AppendEntries/heartbeat vẫn qua, bầu cử bị chặn.
  - Kiểm tra phân vùng bất đối xứng: cô lập 1 node (`isolate`), 4 node còn lại tiếp tục hoạt động theo quorum 3/5; tái hòa nhập (`deisolate`) thành công.

---

## 3. Các thành phần đã bàn giao

1. **`backend-node/src/test/java/vn/nhom6/railway/raft/election/NetworkPartitionElectionTest.java`**
   - Bộ 16 kịch bản kiểm thử phân vùng mạng 3–2 toàn diện:
     1. `initialClusterElection_electsNode1AtTerm1_andBooksFirstTicket`: Bầu ban đầu, lưu term/vote và commit vé G01.
     2. `minorityPartition_isolatedLeaderCannotCommitWrites_producesNoConfirmedBookings`: Leader 2/5 không commit writes, không tạo BOOK_CONFIRMED.
     3. `minorityPartition_candidateCannotWinElection_withoutQuorum`: Candidate 2/5 không thể đắc cử leader.
     4. `majorityPartition_electsNewLeaderInHigherTerm_withThreeVotes`: Đa số 3/5 bầu leader mới ở term 2 với 3/5 phiếu.
     5. `majorityPartition_commitsWritesSuccessfully_whilePartitioned`: Leader 3/5 commit ghi thành công khi đang phân vùng.
     6. `majorityPartition_followerRejectsBooking_withLeaderHint`: Follower từ chối kèm leaderHint chính xác.
     7. `businessRuleValidation_majorityLeaderRejectsDoubleBooking`: Từ chối đặt trùng ghế đã có người đặt từ term 1.
     8. `networkHeal_oldLeaderReceivesNewLeaderHeartbeat_stepsDownImmediately`: Leader cũ giáng chức ngay khi nhận heartbeat term 2.
     9. `networkHeal_uncommittedGhostWritesOnOldLeader_safelyOverwritten`: Entry mồ côi term 1 bị ghi đè bởi entry term 2.
     10. `networkHeal_fullClusterConvergenceAcrossAllFiveNodes`: Toàn bộ 5 node hội tụ đồng nhất log và state machine.
     11. `followerRedirectionAcrossPartitionsAfterHeal`: Client chuyển tiếp chính xác sau hàn gắn mạng.
     12. `testNetworkFilter_metricsAndDropVerification`: Kiểm định 12 directed link và telemetry của TestNetworkFilter.
     13. `selectiveDropRule_withCustomPredicate_blocksOnlyRequestVote`: Bộ lọc selective drop REQUEST_VOTE.
     14. `asymmetricPartition_isolatedNode_fourNodeQuorumRemainsFunctional`: Phân vùng 1 vs 4, duy trì quorum hoạt động.
     15. `successivePartitions_alternatingMinorityAndMajority`: Phân vùng liên tiếp đổi bên qua 3 đời term an toàn.
     16. `durablePersistenceAcrossPartitions_termMonotonicityAndVoteSafety`: Kiểm tra tính đơn điệu của term và vote safety trên storage.

2. **`artifacts/evidence/D22/TV5-D22-result.log`**
   - Minh chứng kiểm thử Maven Surefire với kết quả thực thi chi tiết.

3. **`docs/implementation/TV5-D22-notes.md`**
   - Tài liệu kỹ thuật chi tiết này.

---

## 4. Kết quả Kiểm thử Tổng hợp

- **Targeted TV5-D22 Suite (`NetworkPartitionElectionTest`):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="NetworkPartitionElectionTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 16, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 0.380s).

- **Combined Fake Network & Quorum Suites (D18 - D22):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="NetworkPartitionElectionTest,LeaderFailoverTest,RaftElectionServiceTest,MinorityWriteSafetyTest,WriteAvailabilityPolicyTest,RaftElectionFivePeerFakeNetworkTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 66, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 1.504s).

- **Full Election Package Regression Suite:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="vn.nhom6.railway.raft.election.*Test" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 288, Failures: 0, Errors: 0, Skipped: 1` (PASS 100%, BUILD SUCCESS).
