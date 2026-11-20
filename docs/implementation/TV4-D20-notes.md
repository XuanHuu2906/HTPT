# TV4-D20 — Replication/commit invariant notes

## Pham vi da chay

- 8 kich ban, moi kich ban chay 20 seed deterministically: `1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71`.
- Mock one JVM, five in-memory Raft nodes, FakeNetwork, fake/durable in-memory storage, manual scheduler.
- Khong co TCP, socket, thread/sleep thuc, Docker runtime, PostgreSQL, crash process thuc, election RPC thuc hoac G5 5 process.

## Bat bien da kiem tra

1. Commit chi tang khi co it nhat 3/5 va entry tai commit index thuoc current term cua Leader.
2. Log Matching va payload/prefix bang nhau giua cac node.
3. Commit khong giam; `lastApplied <= commitIndex <= lastLogIndex`.
4. State machine giu ordered prefix chung va seat ACTIVE khong ban trung.
5. Khong apply entry chua commit; NO_OP chi tang checkpoint.
6. Entry commit duoc giu sau leader change va replay trong harness.
7. Read barrier chi tra khi co NO_OP moi thuoc term hien tai va da apply; partition minority khong tra barrier.
8. Khong co business result truoc commit va apply.
9. `matchIndex` monotonic, `nextIndex >= matchIndex+1`, mot RPC/in-flight moi peer, ACK loi/timeout khong duoc tinh thanh ACK.

## Khong duoc chung minh bang suite nay

- Election thuc, TCP peer, crash process/voltage, PostgreSQL commit-apply end-to-end, latency duoi tai va 5 process thuc.
- Multi-threaded/network timing va G5 la viec cua TV5/TV6 va phai co minh chung rieng.

## Gap noi runtime

- `RaftReadBarrier` chua duoc noi vao `NodeMain`/handler production.
- Runtime chua co caller goi `CommitAdvancer.advance()` va `RaftStateMachineApplier.applyCommitted()` tren Leader.
- `CandidateElection`/`RaftState.becomeLeader` chua append NO_OP dau nhiem ky.
- `RaftApplyAdapter.decodeCommand` da ho tro NO_OP/BOOK/CANCEL, nhung wiring runtime chua duoc gan.
- GetSeats/get-request-result/history van can barrier truyen qua handler/NodeMain.

## Viec ban giao

- TV3: noi read barrier vao handler va TCP/error mapping.
- TV2: reviewer wiring apply adapter va atomic commit/apply.
- TV4/5: noi commit advance/apply va leadership guard vao runtime; owner heartbeat/no-op election can phoi hop.
- TV6: G5 5 node+5 PostgreSQL rieng, partition, election, crash and evidence thuc.
