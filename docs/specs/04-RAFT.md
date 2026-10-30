# SPEC-04 — Raft: Election, Replication, Commit & Recovery

> **Trạng thái:** APPROVED-v1, phác thảo cài đặt Raft chuẩn cho **5 voter cố định**. **Owner:** TV4 (log/replication/commit) + TV5 (election/failure/recovery); cùng sở hữu `raft/core` và review chéo.

## 1. Mục tiêu và giới hạn

Raft thống nhất **thứ tự lệnh đặt/hủy vé** giữa 5 Node Java với 5 PostgreSQL riêng. Cụm có **majority = 3/5**; một hoặc hai Node lỗi vẫn **có thể** phục vụ khi những Node còn lại bầu/duy trì Leader có log hợp lệ. Không hứa tiếp tục trong mọi partition, delay hoặc khi storage lỗi.

**P0 bắt buộc:** bầu chọn, persistent term/vote/log, AppendEntries, log matching, majority commit đúng Raft, apply theo thứ tự, bắt kịp khi Node quay lại, bảo toàn an toàn khi split 3–2. Snapshot/log compaction là **mở rộng chưa thuộc baseline v1**, không phải điều kiện hoàn thành FR10–FR12/P1; không xóa log khi chưa có contract snapshot được duyệt.

## 2. Trạng thái mỗi Node

| Trường | Phân loại | Quy tắc |
|---|---|---|
| `currentTerm` | Persistent | Term hiện tại, không giảm; ghi bền trước khi phản hồi RPC cần thiết |
| `votedFor` | Persistent | Tối đa một candidate được vote mỗi term; reset khi sang term mới |
| `log[]` | Persistent | Entry `(index, term, command)` bất biến cho phần đã commit |
| `commitIndex` | Volatile/reconstructed | Chỉ số log đã biết commit, không được vượt `lastLogIndex` |
| `lastApplied` | Persistent app checkpoint | Cùng transaction với state-machine apply |
| `role` | Volatile | `FOLLOWER|CANDIDATE|LEADER` |
| `leaderId` | Volatile | Hint, có thể null/đã cũ |
| `nextIndex[peer]` | Leader volatile | Chỉ số sẽ gửi tiếp cho Follower |
| `matchIndex[peer]` | Leader volatile | Chỉ số log đã biết peer lưu phù hợp |

Nguồn `currentTerm`, `votedFor`, `log[]` phải nằm trong storage bền vững tại **chính Node đó**. Ví dụ schema ở [05-BOOKING-DATABASE.md](05-BOOKING-DATABASE.md). Không coi `lastApplied` thay thế được `commitIndex` của Leader.

## 3. Election / RequestVote

1. Nhận AppendEntries đúng schema từ peer Leader có term không cũ thì reset election timer (kể cả khi cần sửa log mismatch); vote được grant cũng reset timer. RPC term cũ hoặc vote bị từ chối không reset timer.
2. Election timeout **ngẫu nhiên, khác biệt giữa Node**; ứng viên tăng term và **persist `currentTerm/votedFor=self`** trước khi phát `REQUEST_VOTE`.
3. `REQUEST_VOTE(term,candidateId,lastLogIndex,lastLogTerm)`; Node chỉ cấp vote nếu term không cũ, chưa vote cho ai khác trong term (hoặc cùng candidate), **và** nhật ký candidate ít nhất mới bằng nhật ký của Node (so sánh `lastLogTerm`, rồi `lastLogIndex`). Persist vote trước khi trả đồng ý.
4. Khi candidate nhận **ít nhất 3 phiếu từ 3 voter khác nhau**, gồm self, trong term hiện tại, chuyển Leader; khởi tạo `nextIndex=lastLogIndex+1`, `matchIndex=0` cho từng peer, gửi AppendEntries heartbeat. Không đếm response trùng thành nhiều phiếu.
5. Nhận request **hoặc response** hợp lệ từ peer với term lớn hơn thì persist term mới, reset votedFor, từ bỏ role cũ, trở thành Follower trước khi xử lý tiếp. Candidate nhận AppendEntries term bằng term hiện tại cũng trở thành Follower. Response term cũ không được cập nhật tiến độ election/replication.
6. Leader không hợp lệ/term cũ không thể tiếp tục ghi; Node không bỏ phiếu vì tên laptop cố định.

**APPROVED-v1 timers dùng cho cấu hình demo, không phải hằng số thuật toán:** heartbeat khoảng 100–200 ms, election timeout ngẫu nhiên 500–1000 ms ở LAN ổn định; hiệu chỉnh sau test để giảm split vote. Không dùng thời gian vật lý giữa máy làm thứ tự commit.

## 4. AppendEntries và log matching

- Leader gửi `APPEND_ENTRIES(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)` tới Follower.
- Follower từ chối term cũ hoặc không khớp entry tại `(prevLogIndex,prevLogTerm)`.
- Nếu khớp: xử lý xung đột tại **các entry chưa commit** theo quy tắc Raft, xóa suffix xung đột, append entry mới, lưu bền trước khi xác nhận thành công.
- **Không bao giờ truncate/ghi đè entry đã commit**. Bất kỳ tình huống đòi hỏi làm vậy là lỗi nghiêm trọng, phải dừng thao tác nguy hiểm, log và test chẩn đoán.
- Sau AppendEntries thành công, đặt `lastMatchedIndex = prevLogIndex + entries.size()`; `commitIndex = max(commitIndex, min(leaderCommit, lastMatchedIndex))` rồi apply theo thứ tự. Heartbeat dùng lastMatchedIndex=prevLogIndex. Không dùng lastLogIndex cục bộ vì Follower có thể còn suffix chưa khớp Leader; không giảm commitIndex khi RPC cũ đến muộn.
- Leader khi bị từ chối giảm `nextIndex[peer]`/dùng conflict hint được chứng minh, retry đến khi log follower khớp; tránh busy loop.
- Heartbeat vẫn phải kiểm tra prevLogIndex/prevLogTerm; chỉ truncate khi thấy entry cùng index **khác term**, không xóa suffix chỉ vì batch ngắn hoặc rỗng. Cùng index/term mà payload khác là vi phạm invariant: fail closed, không âm thầm ghi đè.
- Baseline dùng tối đa một AppendEntries đang chờ trên mỗi peer; sau timeout bỏ qua response muộn cho RPC đã hết hiệu lực (vẫn xử lý term lớn hơn). Response success chỉ nâng matchIndex đến index xác minh trong RPC, không giảm matchIndex; nextIndex không dưới matchIndex+1 hay dưới 1. Peer/storage thất bại không được tính ACK.

## 5. Quy tắc commit — điểm dễ bị code sai

- Leader chỉ tăng `commitIndex` đến chỉ số `N` nếu có **ít nhất 3 Node gồm Leader** có `matchIndex >= N` **và** entry ở `N` thuộc **currentTerm** của Leader.
- Các entry term trước được commit gián tiếp khi commit một entry current term. Không được lấy “bất kỳ entry đã có trên 3 Node” làm công thức commit bỏ qua term.
- Khi commit index tiến lên, Leader apply các entry chưa apply theo `index` tăng dần, transaction bền vững, sau đó mới phản hồi kết quả nghiệp vụ.
- Raft **log committed** và nghiệp vụ **apply thành công** là hai mốc khác nhau. Nếu DB apply thất bại: không trả `CONFIRMED`; giữ log để retry apply/recovery; báo lỗi vận hành.
- Retry và duplicate requestId có thể xuất hiện tại nhiều log entries do leadership churn; **state machine xử lý idempotent**, không tạo hiệu ứng nghiệp vụ lần hai.

## 6. Máy trạng thái và replay

- Chỉ apply `index <= commitIndex` và `index == lastApplied + 1`.
- Lệnh có cùng dữ liệu phải tạo **cùng kết quả** trên các Node. Không phát sinh `UUID`, giờ hiện tại, random hoặc đọc thông tin ngoại sinh bên trong `apply`.
- Các trường không quyết định bởi Raft như `ticketId`, `issuedAt`, `cancelledAt` nếu cần phải được cấp trong **command** hoặc dẫn xuất tất định từ command/index.
- Một transaction DB apply cần cập nhật **business tables + request_results + lastApplied** nguyên tử; rollback toàn bộ khi lỗi.
- Khi restart: đọc nhất quán `term/votedFor/log/lastApplied`, kiểm tra log đủ đến checkpoint. Khởi tạo `commitIndex=lastApplied` vì checkpoint bền vững chứng minh prefix đó đã commit; phần sau chỉ phục hồi commit knowledge theo Raft. Nếu checkpoint vượt log hoặc log có lỗ thì fail closed. Không suy ra commitIndex=lastLogIndex và không replay lại prefix đã apply.
- NO_OP cũng đi qua transaction tăng lastApplied, nhưng không thêm request_results/vé/sự kiện nghiệp vụ; không để checkpoint dừng trước NO_OP.

## 7. Read safety (`GET_SEATS`, tra cứu requestId, lịch sử vé)

**APPROVED-v1 — baseline đã phê duyệt:** mỗi request đọc authoritative (GET_SEATS, GET_REQUEST_RESULT, GET_TICKET_HISTORY) được Leader append một entry `NO_OP` của currentTerm **sau khi nhận request**, persist/replicate/commit đúng majority, lấy index entry đó làm `readIndex`, chờ `lastApplied >= readIndex`, rồi query trong một DB snapshot. NO_OP có DTO tại SPEC-03 §6.1. Cách này dùng log barrier đã được đề cập trong draft, không cần thêm RPC ReadIndex; chi phí là tăng log mỗi lần đọc. Không tái dùng một NO_OP cũ cho mọi lần đọc sau đó.

Nếu đổi term/role trong lúc chờ thì hủy phục vụ đọc và trả NOT_LEADER; nếu không commit/apply kịp deadline thì READ_UNAVAILABLE. UNKNOWN trong lookup chỉ là không tìm thấy kết quả sau barrier thành công. Tối ưu ReadIndex/quorum heartbeat theo từng request cần review thiết kế riêng, không tự thay bằng một heartbeat cũ hoặc lease dựa trên đồng hồ.

- Không đọc từ Follower và công bố dữ liệu như đã nhất quán mạnh nếu chưa có giao thức đọc bảo đảm tương ứng.
- Không coi `leaderId` có trong RAM là bằng chứng vẫn có quorum.
- Nếu không đủ điều kiện đọc: trả `READ_UNAVAILABLE` (UI lookup hiển thị UNKNOWN); không dùng cache ghế để xác nhận có thể bán.
- Implementer phải viết test chứng minh Leader bị partition không tiếp tục cung cấp **đọc authoritative** từ state cũ.

## 8. Lỗi và phục hồi

| Tình huống | Hành vi yêu cầu |
|---|---|
| Mất một Follower | Leader có thể tiếp tục nếu thỏa majority; Follower sau đó catch-up |
| Mất Leader | Các Follower timeout, bầu Leader mới với log hợp lệ; ghi chờ đến khi có Leader và quorum |
| Còn 3/5 | Có thể commit nếu tổ chức được majority Raft hợp lệ |
| Còn 2/5 | Không xác nhận lệnh ghi mới |
| Partition 3–2 | Nhóm 2 không commit; không có hai Leader cùng xác nhận các log xung đột |
| Mất phản hồi Client đúng lúc commit | Không kết luận failed; truy vấn/retry cùng requestId |
| Node restart với log cũ | So sánh term/index và bắt kịp từ Leader; không ghi đè commit |
| Crash lúc DB apply | Transaction rollback hoặc hoàn tất bền vững; `lastApplied` cho phép replay an toàn |

## 9. Quy tắc durability/thứ tự ghi

- Persist `currentTerm/votedFor` **trước** phản hồi vote/grant cần thiết.
- Persist log entry **trước** `APPEND_ENTRIES_RESPONSE.success=true`.
- Không advance commit chỉ từ ACK socket, phải nhận phản hồi khẳng định lưu log hợp lệ.
- Leader trả kết quả thành công nghiệp vụ sau khi local apply transaction commit.
- Nếu PostgreSQL Node mất kết nối, Node không tiếp tục hoạt động như voter lưu bền vững bình thường; fail closed, log sự cố và dừng xác nhận ghi dựa trên storage không bền.

## 10. Invariants kiểm thử bằng assertion

```text
0 <= lastApplied <= commitIndex <= lastLogIndex
currentTerm không giảm sau restart
Không vote hai candidate khác nhau trong cùng term
Cùng (index, term) đã commit => payload command giống nhau trên mọi Node đúng
Một (tripId, seatId) có tối đa một ticket ACTIVE
Chưa commit / chưa apply => không trả CONFIRMED
2/5 không commit lệnh mới
```

## 11. Giao điểm bắt buộc với phân hệ khác

- TV3 phụ trách serialize `REQUEST_VOTE`/`APPEND_ENTRIES` đúng [03-CONTRACTS.md](03-CONTRACTS.md), TV4/5 không tạo DTO riêng.
- TV2 tạo `StateMachine.apply(entry)` với hiệu ứng nguyên tử/idempotent theo [05-BOOKING-DATABASE.md](05-BOOKING-DATABASE.md).
- TV1 không tự suy ra thành công từ `APPEND_ENTRIES`/log, chỉ từ nghiệp vụ response.
- TV6 test chéo `AC03, AC05–AC10` bằng Node/process thực sự, không chỉ thay đèn monitor.

## 12. Điều kiện DONE Raft

Kiểm thử restart term/vote/log, split vote, up-to-date election, append conflict, commit current-term rule, atomic apply, failover 3/5, partition 3–2, retry mất phản hồi, catch-up từ Node lỗi. Mỗi test có lệnh chạy, môi trường, assertion, log minh chứng. Snapshot chưa triển khai ghi **NOT_IMPLEMENTED (mở rộng ngoài baseline v1)**.

Đối chiếu thuật toán: [Ongaro & Ousterhout, Raft extended paper](https://raft.github.io/raft.pdf), Figure 2 (AppendEntries, election, commit) và §8 (client retry/read safety). Các lựa chọn DTO, PostgreSQL và log barrier cụ thể vẫn là APPROVED-v1 của nhóm.
