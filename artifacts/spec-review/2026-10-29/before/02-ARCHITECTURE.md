# SPEC-02 — Kiến trúc tổng thể

> **Trạng thái:** Draft v1 · **Bộ nền:** Java / JavaFX / PostgreSQL / TCP Socket / Raft 5 Node, **không Web Service**.

## 1. Sơ đồ logic

```text
          Desktop Client JavaFX (1..6 phiên, không Raft vote)
                 | TCP + JSON length-prefixed
       +---------+---------+-----------------------------+
       |         |         |                             |
   Node 1     Node 2    Node 3                       Node 4    Node 5
   Java       Java      Java                         Java      Java
      \          \        |                           /        /
       +--------- TCP peer-to-peer Raft cluster -------------+
       |            |          |           |           |
      PG1          PG2        PG3         PG4         PG5
  (storage riêng từng Node: log + state machine + booking)

             Cluster Monitor JavaFX (non-voter)
                  | TCP trạng thái/diagnostics
                  +--------> Node 1..5
```

Hình mang tính minh họa, không biểu diễn topology Raft cố định. Mỗi Node giao tiếp với các peer theo **danh sách 5 Node cố định**; Leader được bầu động.

## 2. Module Maven đề xuất

```text
railway-distributed-system/
├── pom.xml
├── AGENTS.md
├── docs/specs/00-MASTER.md ... 08-DEPLOYMENT.md
├── shared/                   # TV3: DTO, message codec, type/error enums
├── backend-node/             # Một codebase chạy năm instance
│   └── src/main/java/vn/nhom6/railway/
│       ├── node/             # startup, config, lifecycle, wiring
│       ├── networking/       # TV3: TCP server/client, dispatcher
│       ├── booking/          # TV2: commands, state-machine apply
│       ├── persistence/      # TV2 + TV4: JDBC, local durable state
│       └── raft/
│           ├── core/         # TV4 + TV5: term, votedFor, role, log
│           ├── replication/  # TV4: AppendEntries, nextIndex, commit
│           ├── election/     # TV5: RequestVote, election timers
│           └── recovery/     # TV4 + TV5: catch-up, replay
├── desktop-client/           # TV1: JavaFX bán vé
├── desktop-monitor/          # TV6: JavaFX monitor/fault controls
├── integration-tests/        # Cả nhóm: end-to-end 1..5 Node
├── database/                 # Migration/seed theo Node
├── docker/                   # Dockerfile, compose.dev.yml, compose.lan.yml
├── configs/{dev,lan}/
└── scripts/                  # start/stop/demo hỗ trợ
```

Cấu trúc là **đề xuất kế thừa tài liệu phương án Docker**, không khẳng định repository hiện tại đã có các file này. Không tạo 5 bản sao source `backend-node` khác nhau.

## 3. Biên trách nhiệm

| Thành phần | Được làm | Không được làm |
|---|---|---|
| Desktop Client | Nhập liệu, hiển thị, gửi lệnh, retry cùng requestId, truy vấn kết quả | Ghi SQL, tự quyết định vé thành công khi timeout |
| Shared protocol | Định nghĩa DTO/type/codecs/validation framing | Chứa logic giao dịch hoặc Raft runtime |
| Networking Node | Nhận và định tuyến TCP, timeout, peer endpoint | Tự commit lệnh vì nhận được một TCP ACK |
| Raft | Election, replicate, commit, thứ tự state-machine apply | Quyết định rằng ghế còn trống bằng UI cache |
| Booking state machine | Xét trạng thái ghế, apply booking/cancel tuần tự, idempotency | Thực hiện side effect trước commit Raft |
| PostgreSQL tại Node | Durable term/vote/log, apply state & vé nội bộ | Tự nhân bản chéo với các PostgreSQL khác |
| Monitor | Quan sát, gửi lệnh quản trị được giới hạn trong môi trường demo | Raft vote, đổi Leader bằng thay UI, sửa DB tùy ý |

## 4. Luồng ghi chuẩn

1. Client sinh `requestId` **một lần cho một ý định đặt/hủy**, tạo bản tin `BOOK_TICKET`/`CANCEL_TICKET`.
2. Node nhận yêu cầu; nếu Follower: trả `NOT_LEADER` + `leaderHint`, để Client tự kết nối lại. **[DESIGN-v1] Chọn redirect thay vì proxy** để tránh vòng chuyển tiếp và dễ test; tài liệu demo cho phép cả hai cách.
3. Leader kiểm tra định dạng/auth demo; ghi command theo thứ tự vào Raft log, lưu bền vững rồi replicate.
4. Khi đủ điều kiện commit Raft, Leader tiến hành apply log đã commit lên PostgreSQL trong transaction cùng checkpoint apply; sau đó trả kết quả đã lưu.
5. Client chỉ hiển thị vé `CONFIRMED` nếu nhận kết quả business có bằng chứng apply thành công, và refresh trạng thái ghế.
6. Follower tiếp tục catch-up rồi apply các committed entries theo đúng thứ tự.

**Không có shortcut “gửi INSERT tới ba PostgreSQL và coi như commit”.**

## 5. Luồng đọc

- Dữ liệu ghế/vé hiển thị chính thức đọc từ Leader qua read barrier bảo đảm không phải Leader cũ hoặc state chưa apply; xem [04-RAFT.md](04-RAFT.md).
- Khi đọc chưa thể xác nhận tính mới, trả lỗi `READ_UNAVAILABLE`/`UNKNOWN`, không tự biến bản sao Follower thành trạng thái ghế authoritative.
- Đối với Monitor, `NODE_STATUS` là chẩn đoán cục bộ (có thể trễ), không phải căn cứ xác nhận vé.

## 6. Quan hệ module và hợp đồng

`shared` là single source of truth cho wire DTO. `backend-node` implement server/network và Raft dựa trên shared. `desktop-client`, `desktop-monitor` dùng chung client codec nhưng không import SQL/booking persistence. Cùng cấu trúc dữ liệu và type enum ở [03-CONTRACTS.md](03-CONTRACTS.md); không viết định nghĩa kiểu trùng lặp.

## 7. Đồng thời và lỗi từng phần

- Các kết nối socket và handler có thể chạy song song qua `ExecutorService`, nhưng mọi thay đổi Raft state phải tuần tự hóa theo cơ chế đồng bộ được quyết định trong implementation.
- **Một Node lỗi ≠ một giao dịch chắc chắn thất bại.** Ghi có thể đã commit trước khi phản hồi mất. Client phải tra `requestId`.
- **Phân vùng 3–2:** một phía có thể duy trì tiến trình đồng thuận nếu điều kiện Raft thỏa; phía 2 Node không có quyền xác nhận lệnh mới.
- **Mất Laptop 6:** mất giám sát, quorum vẫn là 5 Node (hoặc số Node dữ liệu còn online) không bị ảnh hưởng.
- **Một laptop mô phỏng 10 container:** kiểm chứng lỗi container/process, **không** chứng minh chịu lỗi mất máy vật lý.

## 8. Nhật ký và khả năng quan sát

Các sự kiện tối thiểu: `node.started`, `leader.elected`, `raft.append`, `raft.committed`, `booking.applied`, `booking.rejected`, `node.disconnected`, `node.recovered`, `client.request.uncertain`. Cấu trúc log nên có `nodeId`, `requestId` (nếu có), `term`, `logIndex`, `commitIndex`, `lastApplied`, `eventType`, `timestamp`. Không coi timestamp khác máy là thứ tự đồng thuận; **logIndex/term** quyết định thứ tự Raft.

## 9. Quyết định cần được khóa khi tích hợp

- `ADR-001` [DESIGN-v1]: TCP length-prefix JSON version 1.
- `ADR-002` [DESIGN-v1]: Follower redirect Client đến Leader.
- `ADR-003` [DESIGN-v1]: leader read barrier cho `GET_SEATS`/`GET_REQUEST_RESULT`.
- `ADR-004` [DESIGN-v1]: lưu Raft metadata/log và booking state trong PostgreSQL **riêng của từng Node**, apply + `lastApplied` cùng transaction.

Khi nhóm thống nhất, đổi trạng thái quyết định từ `DRAFT` thành `ACCEPTED` trong lịch sử PR/issue; cập nhật hợp đồng, test và AGENTS cùng lúc.
