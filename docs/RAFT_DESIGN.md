> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 05 — Raft: quy tắc an toàn phải giữ

> Đây là checklist thiết kế. `raft-election`, `raft-replication`, `raft-core` hiện còn TODO, **không có Raft hoạt động** trong skeleton.

## State chung cho cả TV4 và TV5
- Persistent: `currentTerm`, `votedFor`, `log[]`.
- Volatile: `commitIndex`, `lastApplied` (có thể bổ sung lưu bền vững/khởi tạo đúng để tránh apply lặp).
- Leader volatile: `nextIndex[peer]`, `matchIndex[peer]`.
- Roles: FOLLOWER, CANDIDATE, LEADER.
- Fixed membership = 5, quorum = 3.
- Đồng bộ state mutations qua một event loop hoặc khóa chính xác; không dùng nhiều timer cạnh tranh chỉnh `term`.

## Election (TV5)
1. Follower timeout: tăng currentTerm, vote cho mình, persist term/vote, chuyển Candidate.
2. Gửi RequestVote cho 4 peers, mang lastLogIndex,lastLogTerm.
3. Peer vote nếu term không cũ, chưa vote node khác trong cùng term, candidate log ít nhất cập nhật bằng local log.
4. Nhận đa số 3/5 trong cùng term thì thành Leader; phát heartbeat.
5. Bất kỳ RPC có term cao hơn: persist term cao hơn, trở thành Follower, xóa vote cũ hợp lệ.
6. Randomized election timeout để tránh split vote lâu dài.

## AppendEntries/Replication (TV4)
1. Từ chối term cũ; kiểm tra `(prevLogIndex, prevLogTerm)`.
2. Nếu có conflict thì loại bản ghi xung đột **chưa commit** và append phần mới, không loại entry đã commit.
3. Persist log trước khi phản hồi ACK thành công.
4. Leader cập nhật matchIndex/nextIndex từ response và retry khi cần.
5. CommitIndex chỉ được tiến lên khi majority match, có quy tắc commit entries current term theo Raft.
6. Follower apply dãy entries tuần tự tới min(leaderCommit, lastLogIndex).

## State machine (TV2 + TV4)
- Chỉ apply committed entries theo thứ tự tăng index.
- Persist `lastApplied` cùng transaction DB nghiệp vụ.
- Không có 2 vé ACTIVE cùng trip/seat.
- Duplicate requestId: trả lại kết quả trước đó nếu payload giống hệt, không tạo vé mới.
- Client nhận thành công **chỉ sau khi command đã commit và áp dụng** (không phải sau khi ghi vào DB Leader đơn lẻ).

## Crash/partition (TV5)
- Fail Leader => bầu Leader mới với log đủ mới, không mất các bản ghi committed.
- 3/5 live: ghi có thể tiếp tục sau khi election xong.
- 2/5 live: KHÔNG commit ghi mới.
- Split 3–2: phía 2 không được confirm booking.
- Node hồi phục phải catch-up, xử lý log conflict, rồi apply missing entries.
- Mất RPC response: Client UNKNOWN; server có thể đã commit.

## Đọc dữ liệu nhất quán
Trong skeleton, GET_SEATS chỉ lấy dữ liệu local, **không đủ bảo đảm đọc tuyến tính**. Khi triển khai thực, đọc từ Leader với cơ chế ReadIndex hoặc cơ chế tương đương được kiểm chứng; không tin Leader cũ chỉ vì biến role. Khuyến nghị cho demo: route mọi GET_SEATS qua Leader và chờ điều kiện đọc an toàn; hoặc log hóa yêu cầu đọc để đơn giản hóa chứng minh (có đánh đổi hiệu năng).

## Những cách triển khai SAI
- “Write vào ba PostgreSQL là Raft”. Sai: thiếu log matching, term/vote, leader completeness.
- “Dữ liệu mỗi Node bằng nhau sau vài giây là nhất quán đủ”. Sai đối với booking.
- “Đặt local thành công rồi gửi cho Follower”. Sai: Client có thể nhận confirm mà failover mất vé.
- “Node ping được là healthy leader”. Sai: không bảo đảm membership/quorum/log.
- “Leader đang tồn tại nên luôn đọc/ghi an toàn”. Sai khi partition.

## Test tối thiểu
- Current term persisted after restart.
- Không bỏ phiếu cho 2 candidates cùng term.
- Không chọn leader có log lỗi thời.
- AppendEntries phát hiện term/index conflict.
- 2/5 nodes never confirm new write.
- Đã commit không bị rollback sau failover.
- Crash giữa persist log / apply / reply vẫn có idempotency.
