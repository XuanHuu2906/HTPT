# TV5-D21: Raft Election + Recovery — Dừng Leader và Failover (FakeNetwork Tests)

## 1. Thông tin Task
- **Mã task:** `TV5-D21`
- **Người thực hiện:** Nguyễn Xuân Hữu (TV5 — Raft Election + Recovery)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (Gate G5 & G6: Chịu lỗi mất Leader, phục hồi sau sự cố, phân vùng mạng)
  - `docs/specs/01-REQUIREMENTS.md` (FR10, FR11, FR12: Bầu cử, chịu lỗi phân vùng, không chia não, failover tự động)
  - `docs/specs/03-CONTRACTS.md` (§4.4: mã lỗi `NOT_LEADER` + `leaderHint`, `NO_QUORUM`, `REQUEST_ID_CONFLICT`)
  - `docs/specs/04-RAFT.md` (§3 Election & Re-election, §4 AppendEntries, §5 Commit Invariants, §6 Thứ tự apply, §9 Persistence)
  - `docs/specs/05-DATABASE.md` (§3 Idempotency, Request Results, Seat Constraints)
- **Ngày hoàn thành:** 21/11/2026
- **Phụ thuộc & Tích hợp:**
  - Tận dụng election/heartbeat/term logic từ **TV5-D20** (`RaftElectionService`, `PersistentElectionState`, `CandidateElection`, `LeadershipGuard`, `WriteAvailabilityPolicy`).
  - Giao diện giả lập đặt vé tích hợp trực tiếp với `BookingStateMachine` (TV2) và `BookingService` (TV3).
  - Sử dụng `FakeNetwork` deterministic: mô phỏng dừng leader, rớt mạng, chia cắt phân vùng và hàn gắn mạng mà không phụ thuộc socket hay container thật.
  - **Không phụ thuộc fault runner TV6-D21**; kiểm thử live Docker failover toàn cụm dành riêng cho Gate G6.
  - Toàn bộ 12 kịch bản test trên `FakeNetwork` đạt **PASS 100%**.

---

## 2. Mục tiêu kỹ thuật & Invariants đã chứng minh

### 2.1. Bầu Leader ban đầu & Bền vững Term/Votes
- Cụm 5 voter (`node-1` .. `node-5`) bầu `node-1` làm Leader term 1 với đa số 3/5 phiếu.
- Mọi node tham gia bầu cử cập nhật trạng thái bền vững vào `RaftStorage`: `term = 1`, `votedFor = "node-1"`.
- Leader tiếp nhận lệnh đặt vé qua fake interface, ghi log (index 1, term 1), sao chép tới follower và commit tại commitIndex 1 khi đạt quorum đa số (3/5).
- StateMachine ghi nhận trạng thái ghế `G01` chuyển sang `BOOKED` với ticketId hợp lệ.

### 2.2. Dừng Leader mô phỏng (Kill Leader) & An toàn Fail-Closed
- Mô phỏng Leader chết bằng cách cô lập và dừng hoàn toàn hoạt động của `node-1` (`network.killNode("node-1")`).
- Node chết lập tức ngừng gửi heartbeat hay AppendEntries vào mạng.
- Mọi nỗ lực gọi lệnh `bookTicket` vào node chết đều bị từ chối `fail-closed` (`IllegalStateException`), bảo vệ hệ thống khỏi ghi dữ liệu mồ côi.

### 2.3. Bầu cử lại (Failover Re-election) ở Term Mới
- Follower (`node-2`) phát hiện mất tín hiệu heartbeat từ Leader (election timeout).
- `node-2` chuyển sang vai trò `Role.CANDIDATE` và tăng term lên 2.
- Ứng viên phát động chiến dịch xin phiếu từ các node còn sống (`node-3`, `node-4`, `node-5`).
- Thu thập đủ đa số tuyệt đối (3/5 phiếu: `node-2` self-vote + `node-3` + `node-4`) -> Đắc cử `Role.LEADER` term 2.
- Toàn cụm duy trì bất biến: **tại một thời điểm chỉ có tối đa 1 Leader hoạt động trong cụm**.

### 2.4. Tính Bền vững & Tính Đơn điệu của Term & Vote
- Khi tham gia bầu cử term 2, các node ghi nhận bền vững vào storage: `term = 2`, `votedFor = "node-2"`.
- **Term Monotonicity:** Term không bao giờ giảm. Storage từ chối ghi nhận term thoái lui (`RaftStorageException`).
- **Vote Safety:** Mỗi node chỉ bỏ tối đa 1 phiếu trong một term nhất định. Một ứng viên cạnh tranh khác (`node-5`) yêu cầu phiếu ở cùng term 2 đều bị từ chối tuyệt đối.

### 2.5. Xử lý Lệnh BOOK trên Leader Mới
- Client gửi lệnh `bookTicket` cho ghế `G02` tới Leader mới `node-2`.
- `node-2` tiếp nhận, append vào log cục bộ tại index 2 (term 2).
- Sao chép AppendEntries tới các follower còn sống qua `FakeNetwork`.
- Khi đạt commit quorum (3/5 active voter ack), `commitIndex` trên Leader tiến lên 2, lệnh được apply vào StateMachine -> Trả về `CONFIRMED` kèm ticketId.
- Các follower nhận commitIndex mới qua đợt sao chép tiếp theo và đồng bộ trạng thái StateMachine (cả `G01` và `G02` đều `BOOKED`).

### 2.6. Từ chối trên Follower & Chuyển tiếp (Redirection)
- Client gửi nhầm lệnh `bookTicket` tới Follower `node-3`.
- Follower phát hiện `isLeader() == false`, ném `BookingRequestHandler.NotLeaderException` mang theo `leaderHint = "node-2"`.
- Client căn cứ `leaderHint` chuyển tiếp yêu cầu tới `node-2` và hoàn tất đặt vé thành công.

### 2.7. Tính Idempotent xuyên suốt Failover
- Client retry lại đúng `requestId` đã được đặt ở term 1 (trước failover) lên Leader mới `node-2`.
- Leader mới tra cứu log/StateMachine phát hiện request đã được commit trước đó -> Trả về kết quả gốc (`CONFIRMED`, cùng ticketId) mà không append thêm log hay sinh vé trùng.
- Nếu client gửi cùng `requestId` nhưng thay đổi thông tin đặt vé (khác ghế/chuyến) -> Ném `REQUEST_ID_CONFLICT`.

### 2.8. Thực thi Luật Nghiệp vụ (Double Booking Prevention)
- Khách hàng cố gắng đặt ghế `G01` (đã có người đặt ở term 1) trên Leader mới `node-2`.
- Lệnh được append và commit tuần tự theo Raft, nhưng StateMachine xác định ghế đã có chủ:
  - Trả về `ApplyResult.Outcome.REJECTED`.
  - `reasonCode = "SEAT_ALREADY_BOOKED"`.
  - `ticketId == null`.
  - Quyền sở hữu ghế `G01` không bị thay đổi.

### 2.9. Phục hồi Leader Cũ & Đồng bộ Dữ liệu
- Node `node-1` phục hồi sau sự cố, nạp lại trạng thái từ storage (`recoverFromStorage`).
- Node kết nối lại mạng và nhận AppendEntries từ Leader mới `node-2` mang term 2.
- `node-1` phát hiện term 2 > term 1 -> Ngay lập tức giáng chức thành `Role.FOLLOWER`, cập nhật `currentTerm = 2` và `leaderId = "node-2"`.
- `LeadershipGuard` của `node-1` vô hiệu hóa term 1 cũ, triệt tiêu mọi callback tồn đọng.
- `node-1` nhận các entry mới từ Leader `node-2`, đồng bộ `commitIndex = 2` và áp dụng vào StateMachine cục bộ.

### 2.10. Phân vùng 3-2 & Ghi đè Entry Mồ côi (No Split-Brain)
- Phân vùng 3-2 xảy ra: `{node-1, node-2}` (chứa leader cũ) vs `{node-3, node-4, node-5}`.
- Leader cũ `node-1` nhận lệnh ghi mồ côi (ghost write) tại index 2 term 1 nhưng không thể commit do thiếu quorum 3/5.
- Phía 3/5 bầu `node-3` làm Leader term 2 và commit lệnh ghi hợp lệ tại index 2 term 2.
- Khi hàn gắn mạng, `node-3` gửi AppendEntries tới `node-1` và `node-2`:
  - Entry mồ côi term 1 tại index 2 bị ghi đè hoàn toàn bởi entry term 2 của Leader mới.
  - Toàn cụm hội tụ về 1 Leader duy nhất và 1 lịch sử log đồng nhất.

### 2.11. Cascading Failover & Mất Quorum An toàn
- Hỗ trợ failover liên tiếp qua nhiều thế hệ Leader (`node-1` term 1 -> `node-2` term 2 -> `node-3` term 3).
- Khi số node chết vượt quá bán (>= 3 node chết, còn <= 2 node sống):
  - Không candidate nào có thể thu thập đủ 3/5 phiếu để trở thành Leader.
  - Toàn bộ lệnh ghi bị tạm dừng an toàn (`NotLeaderException`), bảo vệ tính toàn vẹn dữ liệu.

---

## 3. Các thành phần đã chuyển giao

1. **`backend-node/src/test/java/vn/nhom6/railway/raft/election/LeaderFailoverTest.java`**
   - Bộ 12 kịch bản kiểm thử toàn diện trên `FakeNetwork`:
     1. `initialLeaderElected_persistsTermAndVotes_andBooksTicketSuccessfully`: Bầu Leader ban đầu, lưu term/vote và book vé đầu tiên.
     2. `killSimulatedLeader_rejectsBookOnKilledLeader_andStopsHeartbeats`: Dừng leader, từ chối lệnh ghi fail-closed và ngừng heartbeat.
     3. `failoverReelection_candidateGathersMajorityVotesInNewTerm_becomesNewLeader`: Bầu cử lại ở term mới với 3/5 phiếu.
     4. `termAndVotesPersistence_strictlyMonotonic_andRejectsDoubleVotingInSameTerm`: Bền vững term/vote, term đơn điệu tăng và chặn vote 2 lần.
     5. `newLeaderAcceptsAndCommitsBooking_replicatesToAliveFollowers`: BOOK qua fake interface trên Leader mới thành công.
     6. `followerRejectsBooking_returnsNotLeaderWithLeaderHint`: Follower từ chối kèm leaderHint chính xác.
     7. `idempotentBookingAcrossFailover_sameRequestIdReturnsCachedResult`: Tính idempotent xuyên suốt failover.
     8. `businessRuleValidation_rejectsDoubleBookingOnNewLeader`: Từ chối đặt ghế trùng (SEAT_ALREADY_BOOKED).
     9. `recoveredOldLeader_restoresStorage_stepsDownToFollower_andSyncsWithNewLeader`: Phục hồi node cũ, giáng chức và đồng bộ log.
     10. `minorityPartitionFailover_ghostWriteOverwrittenSafelyUponHealing`: Phân vùng 3-2, ghi đè lệnh mồ côi và triệt tiêu split-brain.
     11. `cascadingLeaderFailover_multipleConsecutiveLeaderKillsAndReelections`: Failover liên tiếp qua 3 đời Leader.
     12. `lossOfQuorum_whenMajorityDead_pausesWritesAndPreventsElection`: Mất quorum dừng ghi an toàn.

2. **`artifacts/evidence/D21/TV5-D21-result.log`**
   - Bằng chứng nghiệm thu kiểm thử với log chi tiết từ Maven Surefire.

3. **`docs/implementation/TV5-D21-notes.md`**
   - Tài liệu kỹ thuật chi tiết về kiến trúc failover, cơ chế phục hồi và các bất biến được bảo đảm.

---

## 4. Tổng hợp Kết quả Kiểm thử

- **Targeted D21 Suite (`LeaderFailoverTest`):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="LeaderFailoverTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 12, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 0.227s).

- **Combined Fake Network & Election Suites (D18 + D19 + D20 + D21):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="LeaderFailoverTest,RaftElectionServiceTest,MinorityWriteSafetyTest,WriteAvailabilityPolicyTest,RaftElectionFivePeerFakeNetworkTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 50, Failures: 0, Errors: 0, Skipped: 0` (PASS 100%, 1.536s).

- **Full Election Package Regression Suite:**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="vn.nhom6.railway.raft.election.*Test" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 272, Failures: 0, Errors: 0, Skipped: 1` (PASS 100%, BUILD SUCCESS).
