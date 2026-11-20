# TV5-D20: Raft Election + Recovery — Quorum 3/5 & 2/5 Fake Network Tests (Pre-G5)

## 1. Thông tin Task
- **Mã task:** `TV5-D20`
- **Người thực hiện:** Nguyễn Xuân Hữu (TV5 — Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (Gate G5: Chịu lỗi & bàn giao: mất Leader, mất 1–2 Node, mất quorum, phân vùng)
  - `docs/specs/01-REQUIREMENTS.md` (FR10, FR11, FR12: Bầu cử, chịu lỗi phân vùng, không chia não)
  - `docs/specs/03-CONTRACTS.md` (§7: mã lỗi chuẩn `NO_QUORUM`, `TIMEOUT_UNKNOWN`, `NOT_LEADER`)
  - `docs/specs/04-RAFT.md` (§1, §3 Election, §5 Commit Invariants, §8 Bảng lỗi & phục hồi, §10 Assertions)
- **Ngày hoàn thành:** 20/11/2026
- **Phụ thuộc & Tích hợp:**
  - `WriteAvailabilityPolicy` (TV5-D19): Đánh giá khả năng cam kết quorum ghi và phân giải timeout an toàn.
  - `RaftCore` (D19): Quản lý `RaftState`, `PersistentRaftNode`, và lưu trữ bền vững term/vote.
  - `LeadershipGuard` (TV5-D16): Bảo vệ callback chống thực thi trên term cũ của leader đã bị phế truất.
  - `FakeClusterNetwork`: Mô phỏng liên kết mạng, phân vùng 3–2, ngắt kết nối và hàn gắn mạng mà không phụ thuộc socket thật.
  - **Không chờ TV4-D20** (Distributed Booking) hoặc **TV2-D20** (DB consistency check).
  - Test quorum trên `FakeNetwork` đạt **PASS 100%**; bài test cụm Docker đa container cho G5 chạy riêng ở cuối mốc D20.

---

## 2. Mục tiêu kỹ thuật & Invariants đã chứng minh

### 2.1. Quorum 3/5 vs 2/5 Minority No-Commit
- Trong cụm 5 voter (`node-1` .. `node-5`), đa số tuyệt đối là 3 (3/5).
- Khi phân vùng mạng xảy ra theo tỷ lệ 3–2 (ví dụ: thiểu số `{node-1, node-2}` và đa số `{node-3, node-4, node-5}`):
  - Leader cũ `node-1` phía thiểu số 2/5 có thể vẫn giữ `role == Role.LEADER` trong RAM (chưa nhận diện mất liên lạc).
  - Tuy nhiên, hệ thống **tuyệt đối không dùng role == LEADER để tự báo success**.
  - `WriteAvailabilityPolicy` phát hiện chỉ có 2/5 node active commit ACKs -> trả về trạng thái `NO_QUORUM`.
  - `commitIndex` trên `node-1` và `node-2` **giữ nguyên ở 0**, không một lệnh ghi nào được commit phía 2/5.
  - Timeout ghi phân giải về mã lỗi `NO_QUORUM` (outcome `NO_QUORUM`), không bao giờ báo `CONFIRMED`.

### 2.2. Split Vote Retry & Leader Re-election
- Khi nhiều candidate cùng phát động bầu cử ở cùng một term (ví dụ term 1), phiếu bầu bị phân tán (chia phiếu 2 vs 2 vs 1) và không candidate nào đạt ngưỡng đa số 3/5.
- Hệ thống phát hiện chia phiếu an toàn, không sinh ra nhiều Leader trong cùng 1 term.
- Cơ chế `retryElection()` kích hoạt bầu cử ở term mới (term 2).
- Candidate thu thập đủ 3/5 phiếu (self vote + 2 peer grants) đắc cử `Role.LEADER` và khởi động quyền Leader ở term mới.

### 2.3. Bầu Leader mới phía Đa số 3/5 trong Phân vùng
- Phía 3/5 (`{node-3, node-4, node-5}`) không nhận được heartbeat từ `node-1`, kích hoạt timeout bầu cử.
- `node-3` phát động chiến dịch ở term 2, xin phiếu từ `node-4` và `node-5`.
- `node-3` đạt 3/5 phiếu -> trở thành Leader mới của term 2.
- `node-3` có `checkWriteAvailability()` đạt `AVAILABLE`, tiếp nhận lệnh ghi và commit thành công khi được `node-4` và `node-5` xác nhận.

### 2.4. Hàn gắn mạng, Giáng chức Leader cũ & Triệt tiêu Split-Brain
- Khi mạng hàn gắn, Leader mới `node-3` gửi thông điệp mang term 2 tới `node-1`.
- `node-1` phát hiện term 2 > term 1 -> lập tức giáng chức thành `FOLLOWER`, xóa quyền Leader và cập nhật `currentTerm = 2`.
- `LeadershipGuard` trên `node-1` vô hiệu hóa hoàn toàn term 1 cũ (`runIfCurrent` trả về `false`), ngăn chặn mọi hành vi ghi hoặc callback muộn từ quyền lực cũ.
- Entry chưa commit của term 1 trên `node-1` bị ghi đè an toàn bởi log của Leader term 2 thông qua cơ chế consistency check.
- Toàn cụm hội tụ về 1 Leader duy nhất (`node-3`) với lịch sử log đồng nhất.

### 2.5. Phân giải Timeout ghi chuẩn xác & Không Rollback
- Khi lệnh ghi timeout:
  - Nếu đã xác minh mất quorum (`VERIFIED_NO_QUORUM`): trả về mã lỗi `NO_QUORUM`.
  - Nếu trạng thái sao chép không xác định (`INDETERMINATE`): trả về mã lỗi `TIMEOUT_UNKNOWN`, client retry với cùng `requestId`.
  - Nếu entry thực tế đã commit (`VERIFIED_COMMITTED`): trả về `COMMITTED`, không báo lỗi.
- **Raft Invariant:** Entry chưa commit trong log **không bao giờ bị rollback** khi timeout (`canRollbackUncommitted = false`).

### 2.6. Chuẩn bị Kịch bản Live Fault cho Gate G5
1. **Leader Failure:** Leader chết -> 4 node còn lại phát hiện mất heartbeat và bầu Leader mới đạt 3/5 quorum.
2. **2 Followers Down:** 2 Follower chết -> Cụm còn đúng 3/5 node sống -> Duy trì Write Quorum `AVAILABLE`, tiếp tục commit.
3. **3 Followers Down:** 3 Follower chết -> Cụm chỉ còn 2/5 node sống -> Mất Write Quorum (`NO_QUORUM`), tạm dừng commit an toàn.
4. **Follower Recovery:** 1 Follower hồi phục -> Cụm tăng từ 2/5 lên 3/5 -> Khôi phục Write Quorum (`AVAILABLE`), tiếp tục commit các lệnh ghi.
5. **Durable Recovery:** Node phục hồi sau crash tải lại term và vote từ storage, bảo đảm tính đơn điệu tăng của term và không vote 2 lần trong cùng 1 term.

---

## 3. Các thành phần đã chuyển giao

1. **`backend-node/src/main/java/vn/nhom6/railway/raft/election/RaftElectionService.java`**
   - Triển khai đầy đủ contract `ElectionService`.
   - Quản lý vòng đời candidate, đếm phiếu đa số, retry chia phiếu (`retryElection`).
   - Tích hợp sâu với `WriteAvailabilityPolicy` (TV5-D19) để kiểm tra quorum ghi (`canCommitWrites`, `checkWriteAvailability`, `activeVoterCount`).
   - Hỗ trợ giáng chức an toàn (`observeHigherTerm`, `stepDown`) và bảo vệ callback qua `LeadershipGuard`.
   - Phân giải timeout theo đúng chuẩn SPEC-03 §7 (`NO_QUORUM`, `TIMEOUT_UNKNOWN`, `COMMITTED`).
   - Cung cấp các hook mô phỏng lỗi mạng (`partition`, `isolatePeer`, `healPeer`, `healAll`) cho kiểm thử và diễn tập G5.

2. **`backend-node/src/test/java/vn/nhom6/railway/raft/election/RaftElectionServiceTest.java`**
   - Bộ 12 kịch bản kiểm thử toàn diện trên `FakeClusterNetwork`:
     1. `clusterTopology_andVoterMembership`: Quorum 3/5 và xác thực voter.
     2. `splitVote_retriesInNewTerm_andElectsLeader`: Chia phiếu và bầu lại thành công ở term mới.
     3. `minorityPartition2of5_cannotCommitWrites_writeAvailabilityReportsNoQuorum`: Phân vùng 2/5 không commit.
     4. `majorityPartition3of5_electsNewLeader_andCommitsWrites`: Phân vùng 3/5 bầu leader mới và commit lệnh.
     5. `networkHeal_stepsDownOldLeader_andOverwritesMinorityUncommittedEntry`: Hàn gắn mạng, giáng chức và xóa log mồ côi.
     6. `writeTimeoutResolution_verifiedNoQuorumVsUnknownVsCommitted`: Phân giải timeout chuẩn xác, không rollback entry.
     7. `pingOnly_doesNotEstablishCommitQuorum`: Ping đơn không tạo thành quorum ghi.
     8. `storageRecovery_restoresTermAndVote_ensuresMonotonicTerm`: Phục hồi bền vững sau sự cố.
     9. `g5FaultScenario1_leaderFailure_majorityReelectsNewLeader`: Kịch bản G5.1 - Leader chết.
     10. `g5FaultScenario2_twoFollowersDown_leaderRetainsQuorumAndCommits`: Kịch bản G5.2 - Mất 2 follower.
     11. `g5FaultScenario3_threeFollowersDown_losesQuorum_writesPause`: Kịch bản G5.3 - Mất 3 follower (dưới quorum).
     12. `g5FaultScenario4_recoveredFollower_restoresWriteQuorum`: Kịch bản G5.4 - Hồi phục follower khôi phục quorum.

3. **`artifacts/evidence/D20/TV5-D20-result.log`**
   - Biên bản nghiệm thu kết quả kiểm thử D20 với log chạy thực tế từ Maven.

---

## 4. Tổng hợp Kết quả Kiểm thử

- **Targeted D20 Test Suite (`RaftElectionServiceTest`):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="RaftElectionServiceTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 12, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 0.092s).

- **Combined Fake Network & Quorum Suites (D18 + D19 + D20):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="RaftElectionServiceTest,MinorityWriteSafetyTest,WriteAvailabilityPolicyTest,RaftElectionFivePeerFakeNetworkTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 38, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 1.209s).

- **Full Election Package Regression Suite:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="vn.nhom6.railway.raft.election.*Test" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 260, Failures: 0, Errors: 0, Skipped: 1` (PASS 100%, BUILD SUCCESS).

- **Full Reactor Test (All 6 Modules):**
  ```bash
  mvn -B -ntp test
  ```
  `BUILD SUCCESS` trên toàn bộ 6 modules (`railway-distributed-system`, `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`).
