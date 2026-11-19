# TV5-D19: Định nghĩa cách phát hiện NO_QUORUM (Write Availability & Quorum Commit Capability)

## 1. Thông tin Task
- **Mã task:** `TV5-D19`
- **Người thực hiện:** Nguyễn Xuân Hữu (TV5 — Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md`
  - `docs/specs/01-REQUIREMENTS.md` (FR10, FR11, FR12)
  - `docs/specs/03-CONTRACTS.md` (§7 mã lỗi chuẩn: `NO_QUORUM`, `TIMEOUT_UNKNOWN`, `NOT_LEADER`)
  - `docs/specs/04-RAFT.md` (§1, §3, §5 quy tắc commit, §8 bảng tình huống lỗi 3–2 / mất quorum)
- **Ngày hoàn thành:** 19/11/2026
- **Phụ thuộc & Tích hợp:**
  - `CommitAdvancer` (TV4-D16): Tính toán majority commit index tuân thủ current-term rule.
  - `LeadershipGuard` (TV5-D16): Bảo vệ callback và xác thực term hiện hành của Leader.
  - `FakeReplicationTracker`: Test double giả lập phản hồi ACK, cô lập peer và phân vùng mạng không cần socket thật.
  - Không chờ `TV4-D19 ReadBarrier` (ReadBarrier thuộc đường dẫn đọc `GET_SEATS`, độc lập với write availability).
  - Nghiệm thu cụm thật 3/5–2/5 thực hiện tại mốc G5.

---

## 2. Mục tiêu kỹ thuật & Safety Invariants

### 2.1. Theo dõi khả năng cam kết chứ không dựa vào ping đơn
- Trong cụm 5 voter (`node-1` .. `node-5`), đa số (quorum) là $5 / 2 + 1 = 3$.
- Một kết nối socket sống hoặc một phản hồi ping TCP đơn lẻ giữa Leader và 1 Follower chỉ chứng minh 2 node đang liên lạc (Leader + 1 peer = 2/5).
- 2/5 là **thiểu số** (minority), **tuyệt đối không thể commit writes**.
- `WriteAvailabilityPolicy` theo dõi khả năng cam kết (`CommitCapability`) dựa trên các xác nhận sao chép bền vững (`verified commit ACKs`) trong term hiện hành, không dựa vào ping đơn. Cụm chỉ đạt trạng thái `AVAILABLE` khi có ít nhất 3 voter (Leader + ít nhất 2 peers) gửi ACK tươi mới trong cửa sổ `freshnessWindow`.

### 2.2. Timeout ghi trả UNKNOWN/NO_QUORUM theo tình huống xác minh
Khi một lệnh ghi bị timeout chờ xác nhận quorum:
1. **Tình huống `VERIFIED_NO_QUORUM`:**
   - Đã xác minh cụm mất quorum (ví dụ: Leader bị cô lập trong phân vùng thiểu số 2/5, số peer phản hồi < quorum - 1, hoặc lệnh bị từ chối trước khi append do thiếu quorum).
   - Trả về mã lỗi `NO_QUORUM` (outcome `NO_QUORUM`), message: `"The command could not be committed by a quorum"`.
   - Client hiểu rằng hệ thống chưa thể cam kết an toàn, không báo thành công, có thể thử lại sau với cùng `requestId`.
2. **Tình huống `INDETERMINATE`:**
   - Lệnh đã được append cục bộ và gửi tới các peer, nhưng phản hồi bị mất, mạng trễ, hoặc timeout xảy ra khi chưa đủ bằng chứng khẳng định các peer đã hay chưa commit entry.
   - Trả về mã lỗi `TIMEOUT_UNKNOWN` (outcome `UNKNOWN`), message: `"Write timed out with indeterminate consensus outcome; query or retry with same requestId"`.
   - Client tra cứu hoặc retry với **chính xác cùng `requestId`**.
3. **Tình huống `VERIFIED_COMMITTED`:**
   - Trong quá trình xử lý timeout, entry thực tế đã đạt đa số và `commitIndex >= entryIndex`.
   - Trả về outcome `COMMITTED`, không báo lỗi, tiến hành apply nghiệp vụ bình thường.

### 2.3. Tránh kết luận rollback entry chưa commit
- **Raft Invariant:** Trên một Leader, một entry log chưa commit **tuyệt đối không được phép rollback, xóa bỏ hay truncate cục bộ** khi xảy ra timeout hoặc khi mất quorum!
- `canRollbackUncommitted(logIndex)` luôn trả về `false`.
- `canReportRejectedOnTimeout()` luôn trả về `false` (không bao giờ chuyển timeout thành `REJECTED` hay `SEAT_ALREADY_BOOKED`).
- `assertUncommittedEntryNotRolledBack(logIndex)` bảo đảm entry vẫn được duy trì nguyên vẹn trong log.
- **Lý do an toàn:**
  - Nếu mạng hàn gắn và Leader giữ được quyền, entry chưa commit này hoàn toàn có thể được sao chép tiếp và commit thành công.
  - Nếu cụm bầu ra Leader mới ở term cao hơn, Leader mới đó sẽ an toàn ghi đè entry chưa commit thông qua cơ chế AppendEntries consistency check.
  - Tự ý rollback cục bộ hoặc báo REJECTED sẽ vi phạm tính tuyến tính (linearizability) và gây rủi ro bán vé không nhất quán.

---

## 3. Các thành phần đã triển khai

### 3.1. `backend-node/src/main/java/vn/nhom6/railway/raft/election/WriteAvailabilityPolicy.java`
- Khai báo các enum và record chuẩn hóa:
  - `AvailabilityStatus`: `AVAILABLE`, `NO_QUORUM`, `NOT_LEADER`.
  - `TimeoutVerificationSituation`: `VERIFIED_NO_QUORUM`, `INDETERMINATE`, `VERIFIED_COMMITTED`.
  - `TimeoutOutcome`: `NO_QUORUM`, `UNKNOWN`, `COMMITTED`.
  - `AvailabilityResult` và `WriteTimeoutResolution`.
- Các hàm nòng cốt:
  - `clusterSize()`, `quorumSize()`: Tính toán quorum đa số ($N / 2 + 1$).
  - `recordPeerAck(peerId, matchIndex, timestamp)`: Ghi nhận ACK commit đã kiểm chứng.
  - `recordPeerPing(peerId, timestamp)`: Ghi nhận ping đơn (chỉ phục vụ đo đạc, không tạo quorum).
  - `activeVoterCount(now)`, `hasCommitQuorum(now)`: Đếm số voter active thỏa mãn freshness window.
  - `checkWriteAvailability(now)`: Kiểm tra toàn diện điều kiện tiếp nhận ghi.
  - `canCommitIndex(candidateIndex)`: Kiểm tra chỉ số cụ thể có đạt đa số và đúng currentTerm (Figure 8 safety).
  - `tryAdvanceCommit()`: Tích hợp với `CommitAdvancer.advance()`.
  - `resolveTimeout(logIndex, entryTerm, appendedLocally, situation)`: Phân loại timeout thành `NO_QUORUM`, `UNKNOWN`, hoặc `COMMITTED`.
  - `canRollbackUncommitted()`, `assertUncommittedEntryNotRolledBack()`: Bảo vệ bất biến không rollback entry chưa commit.

### 3.2. `backend-node/src/test/java/vn/nhom6/railway/raft/election/FakeReplicationTracker.java`
- Test double cho replication tracking:
  - Giả lập ACK từ peer: `acknowledge()`, `acknowledgeQuorum()`, `acknowledgeMinority()`.
  - Giả lập cô lập peer và phân vùng mạng: `isolate()`, `partition()`, `heal()`, `healAll()`.
  - Tích hợp sẵn `CommitAdvancer` và `WriteAvailabilityPolicy`.

### 3.3. `backend-node/src/test/java/vn/nhom6/railway/raft/election/WriteAvailabilityPolicyTest.java`
- Bộ kiểm thử toàn diện gồm 14 test methods:
  1. `clusterTopology_fiveVotersRequiresThreeForQuorum`: Quorum của cụm 5 voter là 3.
  2. `singlePingToPeer_doesNotEstablishWriteQuorum`: Ping đơn tới 1 peer chỉ có 2/5 node -> báo NO_QUORUM.
  3. `verifiedAcksFromTwoPeers_establishesWriteQuorum`: Đủ ACK từ 2 peer (tổng 3/5 node) -> báo AVAILABLE.
  4. `minorityPartition_cannotCommitWrites_majorityCommits`: Phân vùng 2/5 không commit; nâng lên 3/5 commit thành công.
  5. `timeoutWithVerifiedNoQuorum_returnsNoQuorumResolution`: Timeout xác minh mất quorum trả về mã `NO_QUORUM`.
  6. `timeoutIndeterminate_returnsUnknownResolution`: Timeout không xác định trả về `TIMEOUT_UNKNOWN`.
  7. `timeoutWhenEntryActuallyCommitted_returnsCommittedResolution`: Timeout khi entry đã commit trả về `COMMITTED`.
  8. `writeRejectedPreAppend_returnsNoQuorum`: Lệnh bị từ chối trước append trả về `NO_QUORUM`.
  9. `uncommittedEntriesMustNeverBeRolledBackOnTimeout`: Khẳng định entry chưa commit không bao giờ bị rollback khi timeout.
  10. `assertUncommittedEntryNotRolledBack_throwsOnIllegalTruncation`: Ném ngoại lệ an toàn nếu entry bị xóa trái phép.
  11. `nonLeaderRole_returnsNotLeaderWithHint`: Follower/Candidate trả về `NOT_LEADER` kèm `leaderHint`.
  12. `staleAcksBeyondFreshnessWindow_causeQuorumLoss`: ACK hết hạn freshness window làm mất quorum write availability.
  13. `oldTermEntry_cannotBeCommittedDirectlyEvenWithQuorum`: Figure 8 rule: Không commit trực tiếp entry term cũ.
  14. `afterTimeout_healingAllowsUncommittedEntryToCommitSafely`: Mạng hàn gắn cho phép entry chưa commit được commit an toàn khi client retry cùng requestId.

---

## 4. Kết quả kiểm thử

- **Targeted D19 Test:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="WriteAvailabilityPolicyTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 14, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 0.060s).

- **Targeted Combined Suite (D16 + D18 + D19):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="WriteAvailabilityPolicyTest,MinorityWriteSafetyTest,CommitSafetyTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 33, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 0.732s).

- **Full Election & Replication Regression Suite:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="vn.nhom6.railway.raft.election.*Test,vn.nhom6.railway.raft.replication.*Test" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 494, Failures: 0, Errors: 0, Skipped: 8` (PASS 100%, BUILD SUCCESS).

- **Full Reactor Test (6 Modules):**
  ```bash
  mvn -B -ntp test
  ```
  `BUILD SUCCESS` trên toàn bộ 6 modules (`railway-distributed-system`, `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`).
