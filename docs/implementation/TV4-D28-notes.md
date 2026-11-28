# TV4-D28 — Raft replication freeze

## 1. Data flow

```text
Desktop Client
  -> Leader appendCommand
  -> persist local raft_log
  -> AppendEntries one in-flight/peer
  -> follower persist
  -> ACK with verified matchIndex
  -> ReplicationTracker
  -> CommitAdvancer (quorum=3/5 + current-term rule)
  -> RaftStateMachineApplier
  -> CommittedCommandApplier/JDBC atomic apply
  -> raft_apply_progress.last_applied
  -> ReadBarrier/Client response
```

## 2. Storage and crash order

- `raft_meta`: bền term/votedFor.
- `raft_log`: term/index/command; persist trước ACK success.
- `raft_apply_progress`: `lastApplied` cùng transaction với business tables và `request_results`.
- `commitIndex` là volatile; restart đặt ở `lastApplied`, sau đó follower học lại commit từ Leader.
- Crash/replay không áp dụng trước `lastApplied+1`; lỗ index hoặc checkpoint quá log => fail closed.

## 3. Invariants

| Invariant | Evidence |
|---|---|
| Term không giảm và reply term cũ bị bỏ qua | `RaftState.java:186`, `RaftReplicationService.java:196`, `RaftInvariantReviewTest.java` |
| Log matching không truncate committed suffix | `AppendEntriesHandler.java:185`, `RaftState.java:333`, `StaleLeaderLogTest` |
| Commit 3/5 và chỉ entry current term | `CommitAdvancer.java:42`, `CommitSafetyTest` |
| Apply liên tục sau sink thành công | `RaftStateMachineApplier.java:153`, `RaftCrashConsistencyTest` |
| Restart/replay không dự đoán log tail | `LogReplayCoordinator.java:177`, `LogReplayCoordinatorTest` |
| Read barrier chỉ trả sau NO_OP commit/apply | `RaftReadBarrier.java:99`, `RaftReadBarrierTest` |

## 4. Structured logs

Tags:

- `RAFT-REPL`: SEND/ACK/REJECT/BACKTRACK/FAIL và next/match index.
- `RAFT-COMMIT`: commitIndex advance.
- `RAFT-APPLY`: index đã áp dụng.
- `RAFT-REPLAY`: startup outcome/floor.

Mức INFO dùng cho state transition và FINE cho ACK chi tiết. Không log heartbeat rỗng ở INFO, mật khẩu, request payload hoặc dữ liệu hành khách.

## 5. Fixed-Leader audit and open issues

- Production consensus nhận Leader qua election; không thấy fixed Leader trong `ElectionRuntime`, `NodeMain`, `BookingRequestHandler`.
- `NodeConfig` fallback `node-1` chỉ xác định Node identity mặc định, không chọn Raft Leader.
- `LeaderRouter.RoleProvider.fixed(...)` chỉ có usages trong test; đề xuất TV3 chuyển sang test fixture/package test.
- Compose/config `node-1` là identity/deployment address, không có role Leader mặc định.
- Scripts `cluster-fault.ps1`/`g6-lan-fault.ps1` có fake/dev leader observations; không thuộc production.
- Mở: lost-disk follower re-seed vs replacement; NO_OP leader wiring; `awaitIndex` cancellation; TV5 callback serialization.

## 6. Freeze boundary

Sau D28, đóng băng replication TV4: chỉ nhận fix regression/P0. Frozen files:

- `backend-node/src/main/java/vn/nhom6/railway/raft/replication/RaftReplicationService.java`
- `backend-node/src/main/java/vn/nhom6/railway/raft/replication/CommitAdvancer.java`
- `backend-node/src/main/java/vn/nhom6/railway/raft/core/RaftStateMachineApplier.java`
- `backend-node/src/main/java/vn/nhom6/railway/raft/replication/LogReplayCoordinator.java`
