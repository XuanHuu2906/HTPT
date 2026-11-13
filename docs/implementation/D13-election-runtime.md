# D13 — Runtime election 5 Node

Cập nhật 2026-11-13. Phối hợp TV3/TV4/TV5; kiểm thử Docker TV6. Chưa có xác nhận review riêng từ các owner.

| Ưu tiên | Việc | Trạng thái |
|---|---|---|
| P0 | RaftNode dùng chung RaftState/persistence/election | Đã triển khai PersistentRaftNode cho durable election/follower; local append chỉ là Accepted, chưa phải commit |
| P0 | VoteRpcAdapter → TCP Raft RPC thật | PASS trên Docker 5 Node, giữ correlation và callback vào một event loop |
| P0 | Khởi tạo RaftElectionComponent trong runtime | NodeMain.main dùng ElectionRuntime khi cấu hình NODE_PEERS đủ 5 voter và PostgreSQL |
| P1 | Term cao hơn: Follower, dừng heartbeat cũ | Unit PASS cho vote request, append request, heartbeat response, lỗi persist; Docker PASS cho vote term cao hơn và SQL term/vote |
| P1 | Docker election 5/5, 3/5, 2/5 | PASS: bầu một Leader với 5/5 và 3/5; 2/5 không bầu Leader mới; khôi phục Node và bầu lại |
| P2 | CI PostgreSQL D13 không skip | GitHub Actions PASS ở commit cc00635: D13 12 PostgreSQL tests, 0 skip; Backend 767 test, 0 skip. Xem ci-remote.md trong evidence |

Khởi chạy: cấu hình NODE_PEERS=`node-1@host1:9200,...,node-5@host5:9200`, DB_* riêng từng Node; timers lấy từ RAFT_HEARTBEAT_INTERVAL_MS và RAFT_ELECTION_TIMEOUT_MIN_MS/MAX_MS. Peer port tách client port. Runtime chặn khởi động khi thiếu DB/schema hoặc dữ liệu D13 sai. Khi chưa có NODE_PEERS, entry point vẫn là diagnostic mode không election.

[Evidence, lệnh và giới hạn](../../artifacts/evidence/D13/election-runtime/README.md). [Compose test riêng](../../docker/compose.election-test.yml). [Script kiểm thử](../../scripts/test-election-docker.py).

Cập nhật này thay phần “chưa có implementation RaftNode/election runtime” trong báo cáo TV2 D11–D13 trước đó. Phần còn thiếu của luồng Booking là **Leader majority/current-term commit + dispatch durable apply + read barrier**; không còn thiếu bước khởi tạo election/TCP runtime. Test election không thay nghiệm thu booking phân tán hoặc catch-up dữ liệu.
