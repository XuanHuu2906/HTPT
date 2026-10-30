> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 07 — Kế hoạch 6 tuần, TV1–TV6

| Tuần | TV1 | TV2 | TV3 | TV4 | TV5 | TV6 | Milestone |
|---|---|---|---|---|---|---|---|
| W1 | UI mockup | Data model/schema | Protocol, DTO | Raft log design | Election design | Repository, Monitor mockup | Freeze contracts |
| W2 | Kết nối JavaFX→Node | Booking 1 Node | TCP client/server | Durable log | Heartbeat timer | Basic monitor/tests | Single-node booking |
| W3 | Multi-client UI | requestId/dedup | Node→Node sockets | AppendEntries | RequestVote | Live 5-node status | 5-node election + replication |
| W4 | Trạng thái tranh chấp | Apply after commit | Leader redirect/retry | Quorum commit | Stable role/term | Conflict tests | No double-sell |
| W5 | Unknown result UI | Recovery of requests | Network failure injection | Catch-up/replay | Failover/partition | Simulated crashes | AC05–AC10 |
| W6 | UI polish | DB validation | TCP stress | Consensus regression | Failure regression | Integrate/report/demo | Delivery |

## Gate cuối tuần
- W1: PR cho contract `shared`, schema, sơ đồ Raft.
- W2: **Chạy thật** một Node và booking thành công khi đã được TV2 wire vào state machine MVP. Scaffold hiện chưa làm.
- W3: 5 node giao tiếp, election hợp lệ, log replication có kiểm thử.
- W4: booking được commit đa số, không double-sell.
- W5: failover, partition và catch-up đúng.
- W6: AC01–AC10 và bàn giao.

## Quy tắc báo cáo hằng tuần
Mỗi TV nộp: PR link, danh sách file thay đổi, testcase thực chạy, log PASS/FAIL, blocker và dependency. Cả nhóm không chấp nhận hoàn thành công việc chỉ vì code đã viết xong.
