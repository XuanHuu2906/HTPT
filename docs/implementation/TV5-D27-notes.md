# TV5-D27: Raft Election + Recovery — Independent Election Safety Review & P0/P1 Fixes

## 1. Thông tin Task
- **Mã task:** `TV5-D27`
- **Mục tiêu:** Rà soát độc lập toàn bộ election safety trên snapshot cuối D26 (`vote once/term`, `stale log eligibility`, `higher term stepdown`, `timer races`, `quorum`, `restart`), sửa toàn bộ lỗi P0/P1 tìm được, bổ sung bộ test tự động có log tên rõ cho từng safety property; sẵn sàng cho **TV4 review cuối ngày** mà không chờ `TV4-D27`.
- **Owner:** TV5 (`backend-node/**/raft/election/**`)
- **Đối chiếu SPEC:**
  - `docs/specs/04-RAFT.md` (§2 State, §3 Election/RequestVote, §5 Commit/Quorum, §6 Restart/Replay, §8 Fault/Recovery, §9 Durability/Fail-closed, §10 Invariants)
  - `docs/specs/07-TESTING.md` (`TC-RAFT-01`, `TC-RAFT-02`, `TC-RAFT-06`, `TC-RAFT-08`, `TC-RAFT-09`)
  - `AGENTS.md` (§3 Ownership, §4 Contract stability, §5 Safety invariants)
- **Hợp đồng giữ nguyên:** Không thay đổi wire DTO trong `shared/**`, không sửa public API trong `raft/core/**` hoặc `raft/replication/**`.

---

## 2. Kết quả rà soát & các lỗi P0/P1 đã sửa

| Mã | Mức độ | Safety Property | File | Vấn đề phát hiện | Cách khắc phục |
|---|---|---|---|---|---|
| `P0-1` | **P0** | Restart / Vote Once Per Term | `RaftElectionService.java` | `simulateRestartAfterStop()` gọi `state.becomeFollower(restoredTerm)` làm mất `votedFor` bền vững khi `storage.term > prevTerm` (dẫn tới double-vote sau restart); `recoverFromStorage()` và `simulateRestartAfterStop()` không kiểm tra bất biến `RaftState.restore(...)` cho log/checkpoint hỏng. | Thêm `validateDurableState()` kiểm tra đơn điệu term, cấm vote ở term 0, kiểm tra voter hợp lệ và toàn vẹn `RaftState.restore(...)` (fail-closed `RaftStorageException`). `simulateRestartAfterStop()` khôi phục nguyên vẹn `votedFor` bền vững và fail-closed nếu memory/storage lệch nhau trong cùng term. |
| `P0-2` | **P0** | Vote Once Per Term | `RaftElectionService.java`, `CandidateElection.java` | `currentGrants` tách rời khỏi `CandidateElection.grants`, bị ghi trước khi xác thực `requestTerm == campaignTerm` / `activeCampaign()` / partition isolation; không xóa `currentGrants` khi step-down qua `handleRequestVote()` hoặc khi `startElection()` bị từ chối trên Leader. | Dùng `CandidateElection.grantedVotersSnapshot()` + `clearCampaignOnStepDown()` làm nguồn chân lý duy nhất; drop phản hồi từ peer bị partition; xóa grants/ACKs khi step-down hoặc tăng term qua `handleRequestVote()`; kiểm tra `Role.LEADER` trước khi thay đổi state trong `startElection()`. |
| `P0-3` | **P0** | Higher Term Stepdown | `CandidateElection.java`, `RaftElectionService.java` | Trong vòng lặp gửi `REQUEST_VOTE` của `CandidateElection.startElection()`, gọi `voteRequestFor(expectedTerm).orElseThrow()` gây `NoSuchElementException` và kích hoạt `failClosed()` nếu transport đồng bộ trả higher-term step-down hoặc thắng cử ngay 3/5 trước khi duyệt hết 4 peer. | Dừng vòng lặp sạch bằng `if (!activeCampaign()) break` và kiểm tra `Optional.isEmpty()` trước khi gọi transport; chèn `EVENT_ELECTION_STARTED` đúng thứ tự trước các sự kiện đồng bộ trong `termLog`. |
| `P0-4` | **P0** | Higher Term Stepdown | `RaftElectionService.java` | `observeHigherTerm()`, `startElection()`, `handleRequestVote()` và transport callback không bật `failed = true` (`failClosed()`) khi `RaftStorageException` xảy ra. | Bắt lỗi storage trên mọi nhánh durable transition của `RaftElectionService`, gọi `failClosed()` (xóa grants, campaign state và peer ACKs) và ném lại ngoại lệ. |
| `P0-5` | **P0** | Quorum Safety | `WriteAvailabilityPolicy.java`, `RaftElectionService.java` | Peer ACK không gắn `term` (ACK term 1 có thể cấp quorum giả 3/5 ở term 3 trong `freshnessWindow`), cho phép ghi nhận ACK khi đang là `FOLLOWER` hoặc timestamp tương lai; `RaftElectionService.partition()`/`isolatePeer()` không báo `markPeerUnresponsive()` cho `WriteAvailabilityPolicy`. | Gắn `TermedAck(term, timestamp)` trong cả `WriteAvailabilityPolicy` và `RaftElectionService`, chỉ tính ACK của `currentTerm` khi là `LEADER` và `ack <= now`, từ chối non-voter, xóa ACK khi đổi term/role, và lan truyền `partition()`/`isolatePeer()` sang `WriteAvailabilityPolicy`. |
| `P1-1` | **P1** | Timer Races | `ElectionRetryCoordinator.java` | Nếu `startElection()` kích hoạt đồng bộ `onValidAppendEntries`/`onVoteDecision` (đã arm `generation` mới), `expire()` vẫn gọi `arm()` lần hai làm hủy deadline vừa đặt. | Chỉ gọi `arm()` trong `expire()` khi `generation == null`. |
| `P1-2` | **P1** | Timer Races | `RaftElectionComponent.java` | Phản hồi vote đến trễ đồng thời với `close()` gọi trực tiếp `loop.execute(...)` làm ném `RejectedExecutionException` vào `onFailure`. | Kiểm tra `closed`/`failed` và bắt `RejectedExecutionException` khi `closed` nhất quán với `execute(Runnable)`. |
| `P1-3` | **P1** | Vote Once Per Term | `RaftElectionService.java` | `auditTermLog()` chưa kiểm tra bất biến mỗi `(nodeId, currentTerm)` có tối đa một `votedFor`. | Bổ sung kiểm tra `voteByNodeTerm` trong `auditTermLog()`. |

---

## 3. Bộ test tự động & kết quả thực chạy

Bộ test mới: `backend-node/src/test/java/vn/nhom6/railway/raft/election/ElectionSafetyReviewTest.java` (10 test tự động cho đủ 6 safety property, mỗi test in dòng log tên property dạng `[SAFETY-PROPERTY][PROP-XX][<PROPERTY_NAME>] PASS - ...`):

1. **Targeted Safety Review (`ElectionSafetyReviewTest`, clean build trên `c92e280`):**
   - `Tests run: 10, Failures: 0, Errors: 0, Skipped: 0` (**PASS**, `7.176 s`)
2. **Full Raft Election Regression (`vn.nhom6.railway.raft.election.*Test`):**
   - `Tests run: 345, Failures: 0, Errors: 0, Skipped: 5` (**PASS**, `6.550 s`; 5 test skip là JDBC PostgreSQL tùy chọn khi không set `RAILWAY_TEST_JDBC_URL`)
3. **Full Raft Regression (`vn.nhom6.railway.raft.**.*Test` — Core + Election + Replication, gồm cả `PartitionReplicationTest` TV4-D25):**
   - `Tests run: 797, Failures: 0, Errors: 0, Skipped: 25` (**PASS**, `23.899 s`; 25 test skip là JDBC PostgreSQL tùy chọn)

Bằng chứng chi tiết: `artifacts/evidence/D27/TV5-D27-result.log`.
