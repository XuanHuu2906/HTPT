# Hệ Thống Bán Vé Tàu Cao Tốc Phân Tán (Railway Distributed System)

> **Môn học:** Hệ thống phân tán — **Nhóm 6** · Học kỳ 1 (2026–2027)  
> **Kiến trúc cốt lõi:** Java 21/25 · JavaFX Desktop · Java TCP Socket (Length-Prefixed Framing) · Raft Consensus (5 Nodes, Quorum 3/5) · 5 PostgreSQL Instances độc lập · Docker Compose.

---

## 1. Giới thiệu tổng quan

Hệ thống bán vé tàu cao tốc phân tán là đồ án môn học mô phỏng kịch bản đặt vé trực tuyến thời gian thực trên chuyến tàu mẫu **`SE-001`** gồm **10 ghế (`G01` – `G10`)**. Hệ thống được thiết kế để giải quyết bài toán cốt lõi trong hệ thống phân tán: **duy trì tính nhất quán mạnh (linearizability), bảo toàn tính toàn vẹn dữ liệu (zero double-booking), và đảm bảo khả năng chịu lỗi cao (fault-tolerance)** khi các node mạng bị sự cố, trễ mạng hoặc xảy ra phân vùng mạng (network partition).

### Đặc trưng kỹ thuật bắt buộc (System Invariants)
- **Ứng dụng Desktop thuần túy:** Giao diện nhân viên bán vé (Desktop Client) và bảng điều khiển cụm (Desktop Monitor) viết bằng **JavaFX**. Tuyệt đối **không dùng Web, REST/HTTP API, GraphQL, WebSocket hay SaaS**.
- **Giao thức mạng TCP Socket nhị phân:** Giao tiếp qua kênh TCP thuần với framing 4-byte big-endian length prefix; payload đóng gói theo chuẩn `MessageEnvelope<T>` JSON (`protocolVersion=1`).
- **CSDL phân tán độc lập:** Cụm 5 Node sở hữu **5 cơ sở dữ liệu PostgreSQL riêng biệt** (`railway-postgres-1` đến `railway-postgres-5`). Tuyệt đối không dùng DB chung để giả lập phân tán.
- **Đồng thuận Raft chuẩn:** Cụm gồm 5 voter với đa số **quorum = 3/5**:
  - Bầu cử Leader động, chuyển quyền và phát hiện `HigherTerm` an toàn.
  - Sao chép nhật ký (Log Replication) với quy tắc commit đa số và điều kiện **Current-Term** (Figure 8 safety).
  - Tự động điều hướng Client về Leader thông qua `leaderHint` khi gửi nhầm Follower (`NOT_LEADER`).
- **An toàn giao dịch & Bán vé:**
  - Đúng 10 ghế `G01`–`G10`; không bao giờ tồn tại >1 vé `ACTIVE` trên cùng một ghế.
  - Phân định rõ ràng giữa `requestId` (UUID ổn định của ý định nghiệp vụ, giữ nguyên khi retry) và `messageId` (UUID khung truyền TCP).
  - Không bao giờ trả `CONFIRMED` khi chưa đạt Raft commit và chưa DB apply bền vững.
  - Xử lý timeout an toàn: timeout hoặc mất phản hồi trả về `TIMEOUT_UNKNOWN` / `UNKNOWN`, tuyệt đối không tự ý rollback entry chưa commit hay báo `REJECTED`.

---

## 2. Kiến trúc hệ thống (System Architecture)

```
       +-----------------------+           +-----------------------+
       | JavaFX Desktop Client |           | JavaFX Cluster Monitor|
       |  (Nhân viên bán vé)   |           |  (Giám sát trạng thái)|
       +-----------+-----------+           +-----------+-----------+
                   |                                   |
                   | TCP (Length-Prefixed Framing)     | TCP Polling
                   v                                   v
+-------------------------------------------------------------------------+
|                              RAFT CLUSTER                               |
|                                                                         |
|  +--------------------+                   +--------------------+        |
|  |   Backend Node 1   | <--- Peer RPC --> |   Backend Node 2   |        |
|  |     (LEADER)       |    (AppendEntries |    (FOLLOWER)      |        |
|  +---------+----------+     RequestVote)  +---------+----------+        |
|            |                                        |                   |
|            v JDBC (Port 5432/5433)                  v JDBC (Port 5432/5434)
|  +--------------------+                   +--------------------+        |
|  | PostgreSQL Node 1  |                   | PostgreSQL Node 2  |        |
|  +--------------------+                   +--------------------+        |
|            ^                                        ^                   |
|            |                   ...                  |                   |
|            v                                        v                   |
|  +--------------------+                   +--------------------+        |
|  |   Backend Node 3..5| <--- Peer RPC --> |  PostgreSQL 3..5   |        |
|  |    (FOLLOWERS)     |                   |  (5 DBs độc lập)   |        |
|  +--------------------+                   +--------------------+        |
+-------------------------------------------------------------------------+
```

### Cấu trúc Multi-Module Maven

| Module | Chức năng chính | Thành viên phụ trách |
|---|---|---|
| [`shared/`](shared/) | DTO, JSON MessageEnvelope, Codec, Enums, Type framing dùng chung | **Nguyễn Đức Thuận (TV3)** |
| [`backend-node/`](backend-node/) | Server TCP, Raft Engine (Election, Replication, Commit), Booking State Machine, PostgreSQL JDBC Storage | **Tuấn (TV2), Thuận (TV3), Zun (TV4), Hữu (TV5)** |
| [`desktop-client/`](desktop-client/) | Giao diện JavaFX bán vé 10 ghế, Controller, Client Network, Retry/Journaling | **Khoa (TV1)** |
| [`desktop-monitor/`](desktop-monitor/) | Giao diện JavaFX giám sát cụm 5 Node (Role, Term, CommitIndex, Health) | **Trần Hoàng Đạt (TV6)** |
| [`integration-tests/`](integration-tests/) | Kịch bản kiểm thử tích hợp cụm, multi-client, chaos fault-tolerance | **Cả nhóm / Đạt (TV6)** |
| [`database/`](database/) | Script Flyway/SQL migration (`V1__schema.sql`, `V2__seed.sql`) | **TV2 + TV4** |
| [`docker/`](docker/) | Docker Compose dev/lan, Dockerfile backend, entrypoints mạng | **Trần Hoàng Đạt (TV6)** |
| [`scripts/`](scripts/) | Scripts PowerShell/Bash tự động hóa test, reset database, kiểm tra môi trường | **TV6** |
| [`docs/`](docs/) | 9 bộ tài liệu đặc tả chuẩn [APPROVED-v1](docs/specs/00-MASTER.md) & ghi chép kỹ thuật | **Cả nhóm** |

---

## 3. Phân công trách nhiệm thành viên (WBS & Ownership)

Dự án được phân bổ chặt chẽ theo 180 nhiệm vụ chi tiết (30 ngày làm việc) với ranh giới module rõ ràng:

- **Khoa (TV1) — Desktop Client UI:** Giao diện JavaFX 10 ghế trực quan, quản lý trạng thái form, `SocketBookingClient`, xử lý tự động chuyển hướng khi nhận `NOT_LEADER`, ghi nhận `PersistentPendingJournal` để tra cứu idempotent khi mất mạng.
- **Phạm Minh Tuấn (TV2) — Booking & Database Persistence:** Xây dựng `BookingStateMachine`, `BookingCommand`, bảng CSDL `tickets`, `booking_audit`, `request_results`, `raft_log`, kiểm soát giao dịch nguyên tử, chống bán trùng vé và đảm bảo idempotency.
- **Nguyễn Đức Thuận (TV3) — Protocol & Networking:** Thiết kế giao thức nhị phân length-prefixed framing 4-byte BE, `MessageCodec`, `MessageDispatcher`, router chuyển hướng Leader, bộ tạo mã lỗi chuẩn `ErrorReplyFactory`, phân lập in-flight requests.
- **Zun (TV4) — Raft Replication & Commit:** Triển khai `RaftState`, `PersistentLogAppender`, `ReplicationTracker`, `CommitAdvancer` (thuật toán commit đa số theo current-term rule), giải quyết xung đột nhật ký (`LogConflictResolver`).
- **Nguyễn Xuân Hữu (TV5) — Raft Election, Safety & Recovery:** Thuật toán bầu cử `CandidateElection`, bộ luật bỏ phiếu `VoteRules`, `LeadershipGuard` bảo vệ callback cũ, kiểm soát an toàn phân vùng 3–2 (`MinorityWriteSafety`), chính sách phát hiện mất đa số `WriteAvailabilityPolicy` (TV5-D19).
- **Trần Hoàng Đạt (TV6) — DevOps, Monitor & QA:** Cấu hình Docker Compose 5 DB độc lập, Dockerfile backend, xây dựng ứng dụng JavaFX `desktop-monitor`, kịch bản giả lập lỗi mạng (`cluster-fault.ps1`), bộ test tích hợp tổng thể.

---

## 4. Các tính năng cốt lõi đã hoàn thành

### Đồng thuận Raft & Khả năng chịu lỗi (Consensus & Fault-Tolerance)
- [x] **Leader Election:** Tự động phát hiện timeout ngẫu nhiên, tăng term, bầu cử và trở thành Leader khi đạt $\ge 3/5$ phiếu.
- [x] **LeadershipGuard (TV5-D16):** Đóng băng term, loại bỏ mọi callback trễ hoặc phản hồi cũ sau khi đã giáng chức thành Follower.
- [x] **CommitAdvancer (TV4-D16):** Cam kết nhật ký đa số nghiêm ngặt tuân thủ quy tắc Figure 8 (chỉ commit trực tiếp entry thuộc currentTerm của Leader).
- [x] **Minority Write Safety (TV5-D18):** Khi mạng bị phân vùng 3–2, Leader phía thiểu số (2/5) không bao giờ commit được lệnh ghi; phía đa số (3/5) bầu Leader mới an toàn; khi hàn gắn mạng, log thiểu số bị ghi đè an toàn mà không sinh vé trùng.
- [x] **Write Availability & NO_QUORUM Policy (TV5-D19):** Giám sát năng lực cam kết dựa trên phản hồi sao chép bền vững, không dựa vào ping đơn. Timeout phân định rõ ràng giữa `NO_QUORUM` và `TIMEOUT_UNKNOWN`. Tuyệt đối không rollback entry chưa commit trên Leader.

### Nghiệp vụ vé & Tính toàn vẹn (Booking & Idempotency)
- [x] **Sơ đồ 10 ghế cố định:** G01 đến G10 trên chuyến tàu `SE-001`.
- [x] **Idempotent Booking:** Gửi lại lệnh cùng `requestId` luôn trả về kết quả ban đầu (cùng mã vé `ticketId`), không sinh vé mới, không trừ tiền hay chiếm ghế lần hai.
- [x] **Trùng `requestId` khác payload:** Tự động phát hiện và từ chối với lỗi `REQUEST_ID_CONFLICT`.
- [x] **State Machine Determinism:** Mọi node khi replay log đều ra trạng thái CSDL giống hệt nhau, không phụ thuộc vào đồng hồ hệ thống.

### Giao thức mạng & Client
- [x] **TCP Framing:** Mã hóa 4-byte big-endian length prefix, chống phân mảnh khung truyền (TCP fragmentation) và dính gói.
- [x] **Auto-Redirect:** Client tự động kết nối lại theo `leaderHint` khi gửi nhầm vào node Follower.
- [x] **Fail-Closed Client:** Khi gặp timeout, Client chuyển trạng thái sang `UNKNOWN` và lưu nhật ký để tra cứu lại, không bao giờ tự báo thất bại hay thành công giả.

---

## 5. Yêu cầu môi trường & Cài đặt

### Yêu cầu tiên quyết
- **Hệ điều hành:** Linux, macOS hoặc Windows 10/11.
- **Java Development Kit:** OpenJDK 21 hoặc OpenJDK 25.
- **Apache Maven:** Phiên bản 3.9+ trở lên.
- **Docker & Docker Compose:** Docker Engine 24+ (cho môi trường chạy CSDL hoặc cụm backend).

Kiểm tra môi trường:
```bash
java -version
mvn -version
docker compose version
```

---

## 6. Hướng dẫn biên dịch & Chạy thử nghiệm

### 6.1. Biên dịch toàn bộ dự án & Chạy Unit/Integration Tests

Tại thư mục gốc `railway-distributed-system`:

```bash
# Biên dịch và chạy toàn bộ hơn 1.000 bài kiểm thử trên tất cả các module
mvn -B -ntp clean test
```

Chạy riêng các bài kiểm thử đồng thuận Raft quan trọng:
```bash
# Kiểm thử chính sách sẵn sàng ghi & phát hiện NO_QUORUM (TV5-D19)
mvn -B -ntp -pl backend-node -am test -Dtest="WriteAvailabilityPolicyTest"

# Kiểm thử an toàn phân vùng 3-2 & leader cũ bị cô lập (TV5-D18)
mvn -B -ntp -pl backend-node -am test -Dtest="MinorityWriteSafetyTest"

# Kiểm thử quy tắc majority commit của Raft (TV4-D16)
mvn -B -ntp -pl backend-node -am test -Dtest="CommitSafetyTest"
```

---

### 6.2. Khởi chạy cụm 5 CSDL PostgreSQL (Docker Compose)

Khởi động 5 instance PostgreSQL độc lập dành cho 5 Node:
```bash
# Khởi động 5 database trên các port máy chủ 5433 -> 5437
docker compose -f docker/compose.dev.yml up -d postgres-1 postgres-2 postgres-3 postgres-4 postgres-5

# Kiểm tra trạng thái sức khỏe (health check) của 5 database
docker compose -f docker/compose.dev.yml ps
```

Các database được ánh xạ port ra máy chủ (host):
- `postgres-1`: `127.0.0.1:5433` (Database: `railway_node1`)
- `postgres-2`: `127.0.0.1:5434` (Database: `railway_node2`)
- `postgres-3`: `127.0.0.1:5435` (Database: `railway_node3`)
- `postgres-4`: `127.0.0.1:5436` (Database: `railway_node4`)
- `postgres-5`: `127.0.0.1:5437` (Database: `railway_node5`)

---

### 6.3. Khởi chạy 5 Backend Nodes

Có 2 phương thức vận hành:

#### Cách 1: Chạy toàn bộ 5 Node qua Docker Compose
```bash
docker compose -f docker/compose.dev.yml --profile all up -d --build
```

#### Cách 2: Chạy trực tiếp từ dòng lệnh Maven / Java
Thiết lập các biến môi trường tương ứng cho từng Node (ví dụ Node 1):
```bash
export NODE_ID=node-1
export DB_HOST=localhost
export DB_PORT=5433
export DB_NAME=railway_node1
export DB_USER=railway
export DB_PASSWORD=railway_secret_local_only
export NODE_CLIENT_PORT=9101
export NODE_PEER_PORT=9201
export NODE_PEERS="node-1@127.0.0.1:9201,node-2@127.0.0.1:9202,node-3@127.0.0.1:9203,node-4@127.0.0.1:9204,node-5@127.0.0.1:9205"

java -cp backend-node/target/classes:shared/target/classes:... vn.nhom6.railway.node.NodeMain
```

---

### 6.4. Khởi chạy giao diện JavaFX Desktop Client

Chạy ứng dụng bán vé dành cho nhân viên:
```bash
mvn -pl desktop-client javafx:run
```
Giao diện cho phép:
- Xem sơ đồ 10 ghế (màu sắc trực quan: Trống, Đang xử lý, Đã bán).
- Đặt vé với tên hành khách.
- Tự động kết nối, tự chuyển hướng sang Leader khi phát hiện `NOT_LEADER`.
- Tra cứu vé đã đặt theo `requestId`.

---

### 6.5. Khởi chạy giao diện JavaFX Desktop Monitor

Chạy bảng điều khiển giám sát trạng thái phân tán:
```bash
mvn -pl desktop-monitor javafx:run
```
Bảng điều khiển hiển thị thời gian thực:
- Trạng thái sống/chết của từng Node.
- Vai trò hiện tại (`LEADER`, `FOLLOWER`, `CANDIDATE`).
- Term hiện tại và `commitIndex` của từng Node trong cụm.

---

## 7. Các mã lỗi giao thức chuẩn (SPEC-03 §7)

Mọi phản hồi lỗi từ Server được đóng gói trong envelope `ERROR_RESPONSE` với các mã chuẩn:

| `errorCode` | Ý nghĩa | Hành vi phía Client |
|---|---|---|
| `NOT_LEADER` | Node tiếp nhận là Follower | Tự động kết nối lại theo `leaderHint` |
| `NO_QUORUM` | Cụm mất đa số, không thể cam kết ghi an toàn | Báo lỗi hạ tầng, chờ cụm hồi phục và thử lại cùng `requestId` |
| `TIMEOUT_UNKNOWN` | Hết thời gian chờ mà chưa rõ trạng thái commit | Không kết luận thất bại; tra cứu lại với cùng `requestId` |
| `SEAT_ALREADY_BOOKED` | Ghế đã có vé `ACTIVE` hợp lệ | Báo từ chối nghiệp vụ, cập nhật lại trạng thái ghế |
| `REQUEST_ID_CONFLICT` | Trùng `requestId` nhưng thay đổi nội dung lệnh | Từ chối ngay lập tức; không retry |
| `INVALID_REQUEST` | Sai định dạng, thiếu trường, ID sai quy tắc | Sửa dữ liệu đầu vào |
| `INTERNAL_ERROR` | Lỗi nội bộ không xác định | Giữ nguyên trạng thái để kiểm tra log |

---

## 8. Tài liệu kỹ thuật chi tiết

Tất cả các quyết định thiết kế và biên bản nghiệm thu được lưu trữ trong thư mục `docs/`:

- [SPEC-00: MASTER SPEC](docs/specs/00-MASTER.md) — Tài liệu gốc định nghĩa mục tiêu và thứ tự đọc.
- [SPEC-01: Yêu cầu & Tiêu chí nghiệm thu (AC01–AC10)](docs/specs/01-REQUIREMENTS.md).
- [SPEC-02: Kiến trúc & Ranh giới Module](docs/specs/02-ARCHITECTURE.md).
- [SPEC-03: Giao thức mạng TCP, Envelope & Mã lỗi](docs/specs/03-CONTRACTS.md).
- [SPEC-04: Thuật toán đồng thuận Raft](docs/specs/04-RAFT.md).
- [SPEC-05: Nghiệp vụ Đặt vé & Mô hình CSDL PostgreSQL](docs/specs/05-BOOKING-DATABASE.md).
- [SPEC-06: Đặc tả giao diện JavaFX Client & Monitor](docs/specs/06-DESKTOP-UI.md).
- [SPEC-07: Chiến lược kiểm thử & Ma trận nghiệm thu](docs/specs/07-TESTING.md).
- [SPEC-08: Kế hoạch triển khai Docker & Mạng LAN](docs/specs/08-DEPLOYMENT.md).
- [AGENTS.md](AGENTS.md) — Quy tắc làm việc nghiêm ngặt dành cho AI Coding Agents.
- [Ghi chép kỹ thuật TV5-D19](docs/implementation/TV5-D19-notes.md) — Báo cáo triển khai chính sách `WriteAvailabilityPolicy` và `NO_QUORUM`.

---

## 9. Giấy phép & Thông tin bản quyền

Đồ án được phát triển phục vụ mục đích học tập và nghiên cứu môn Hệ thống phân tán.  
Bản quyền thuộc về **Nhóm 6 — 2026**.
