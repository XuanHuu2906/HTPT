# SPEC-03 — TCP Protocol & Shared Contracts (Nguồn chuẩn DTO)

> **Trạng thái:** **DESIGN-v1 / CHƯA KHÓA**. Tài liệu đầu vào chỉ gợi ý nhóm bản tin; hình dạng JSON, field, error code và thời hạn dưới đây do bộ SPEC đề xuất để nhóm phê duyệt.  
> **Owner:** TV3 · **Review bắt buộc:** TV1, TV2, TV4, TV5, TV6. **Không dùng HTTP/REST/SOAP/Web Service**.

## 1. Nguyên tắc

- Một kết nối `Socket`/`ServerSocket` thuần TCP; tất cả Client↔Node và Node↔Node dùng cùng **framing và envelope**, khác loại message/role.
- Mã hóa `UTF-8 JSON`, parse bằng Jackson; tên field `camelCase`; không truyền Java Object Serialization.
- Mỗi request có một `messageId` duy nhất cho lần gửi; `correlationId` của phản hồi là `messageId` request.
- `requestId` nằm trong **payload nghiệp vụ**, sinh ở Client **một lần** và **không đổi qua retry**.
- `nodeId` chỉ từ `node-1` đến `node-5` trong Raft; Desktop là `client-<id>` hoặc `monitor-<id>`, không trở thành voter.
- Không coi TCP ACK hoặc socket write thành công là Raft commit.

## 2. Byte framing và giới hạn

```text
4 bytes unsigned length, network byte order (big-endian)
N bytes UTF-8 encoded JSON envelope
```

- `N` là độ dài **byte**, không phải số ký tự. **DESIGN-v1:** `1 <= N <= 1_048_576` (1 MiB).
- `readFully(4)`, kiểm tra `N`, rồi `readFully(N)`; EOF giữa khung = mất kết nối/khung lỗi, không parse phần còn thiếu.
- Mỗi socket có một writer được tuần tự hóa; nhiều request trên cùng kết nối phải phân biệt bằng `messageId`.
- Socket connect/read timeout và reconnect cấu hình, không khóa JavaFX Application Thread.
- Bản tin sai version, sai type hoặc quá giới hạn phải bị từ chối có kiểm soát; không ghi vào log Raft.

## 3. Envelope (mọi message)

```json
{
  "protocolVersion": 1,
  "messageId": "b4c410df-2b27-45dd-81ac-b7f9c0ec21e4",
  "correlationId": null,
  "type": "BOOK_TICKET",
  "senderId": "client-7",
  "sentAt": "2026-10-29T10:00:00Z",
  "payload": {
    "requestId": "542e32fb-b657-4478-9cea-52286822d1d2",
    "tripId": "SE-001",
    "seatId": "G05",
    "passengerName": "Khach demo"
  }
}
```

`sentAt` chỉ phục vụ log, timeout và hiển thị; **không** dùng timestamp xuyên Node để quyết định thứ tự giao dịch. `correlationId=null` cho bản tin chủ động; phản hồi ghi lại ID bản tin yêu cầu. `senderId` lấy từ kênh/định danh được xác thực trong môi trường demo, **không** tin mù quáng vào field JSON để cấp quyền quản trị.

### 3.1. Bảng kiểu thông điệp

| Request | Response | Quyền gọi | Ghi chú |
|---|---|---|---|
| `GET_SEATS` | `GET_SEATS_RESPONSE` | Client | Read barrier trên Leader |
| `BOOK_TICKET` | `BOOK_TICKET_RESPONSE` | Client | Lệnh ghi có `requestId` |
| `CANCEL_TICKET` | `CANCEL_TICKET_RESPONSE` | Client | P1, lệnh ghi có `requestId` |
| `GET_REQUEST_RESULT` | `GET_REQUEST_RESULT_RESPONSE` | Client | Tra cứu retry/timeout |
| `GET_TICKET_HISTORY` | `GET_TICKET_HISTORY_RESPONSE` | Client | P1, query có giới hạn |
| `NODE_STATUS` | `NODE_STATUS_RESPONSE` | Monitor | Đọc trạng thái cục bộ, có thể stale |
| `REQUEST_VOTE` | `REQUEST_VOTE_RESPONSE` | Node | Raft RPC |
| `APPEND_ENTRIES` | `APPEND_ENTRIES_RESPONSE` | Node | Raft RPC, heartbeat cũng dùng type này |
| `ERROR_RESPONSE` | — | Mọi request | Lỗi protocol/transport/không đủ điều kiện |

Snapshot/InstallSnapshot chưa có contract v1 do là **mở rộng**; không tự thêm message này và gọi P0 đã xong.

## 4. DTO nghiệp vụ chuẩn

### 4.1. `GET_SEATS`

Request payload: `{ "tripId": "SE-001" }`.

Response payload thành công (chỉ khi được đọc an toàn):

```json
{
  "tripId": "SE-001",
  "seats": [
    { "seatId": "G01", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G02", "status": "BOOKED", "ticketId": "ticket-uuid" }
  ],
  "readIndex": 42,
  "leaderId": "node-3"
}
```

`seats` phải đủ đúng 10 ghế khi trả kết quả đầy đủ; `readIndex` là chỉ số commit/applied tối thiểu đã dùng để đọc, **không** phải phiên bản riêng từng ghế. Trạng thái chỉ `AVAILABLE`/`BOOKED`, **không có `SELECTED` từ server**.

### 4.2. `BOOK_TICKET`

Request payload:

```json
{
  "requestId": "542e32fb-b657-4478-9cea-52286822d1d2",
  "tripId": "SE-001",
  "seatId": "G05",
  "passengerName": "Khach demo"
}
```

Response payload **sau quyết định nghiệp vụ**:

```json
{
  "requestId": "542e32fb-b657-4478-9cea-52286822d1d2",
  "outcome": "CONFIRMED",
  "ticketId": "ticket-uuid",
  "tripId": "SE-001",
  "seatId": "G05",
  "reasonCode": null,
  "appliedLogIndex": 43
}
```

`outcome`: `CONFIRMED` (đã commit+apply), `REJECTED` (quy tắc nghiệp vụ hoặc validation từ chối), `UNKNOWN` (không xác định khi lỗi/timeout). **Không** đặt `CONFIRMED` khi mới append hoặc khi mới replicate trên 1–2 Node.

### 4.3. `CANCEL_TICKET` (P1)

Request payload: `{"requestId":"<uuid>","ticketId":"ticket-uuid"}`. Response: `requestId`, `outcome` (`CONFIRMED|REJECTED|UNKNOWN`), `ticketId`, `reasonCode`, `appliedLogIndex`. Hủy thành công làm vé thành `CANCELLED` và ghế `AVAILABLE` sau commit+apply.

### 4.4. `GET_REQUEST_RESULT` (P0)

Request payload: `{"requestId":"<uuid>"}`.

Response payload:

```json
{
  "requestId": "542e32fb-b657-4478-9cea-52286822d1d2",
  "state": "APPLIED_SUCCESS",
  "commandType": "BOOK_TICKET",
  "ticketId": "ticket-uuid",
  "reasonCode": null,
  "appliedLogIndex": 43
}
```

`state`:

- `APPLIED_SUCCESS`: lệnh được commit+apply và thành công nghiệp vụ.
- `APPLIED_REJECTED`: đã có kết quả từ chối nghiệp vụ được ghi bền vững.
- `PENDING`: Leader có thông tin yêu cầu đang xử lý, nhưng chưa có kết quả apply.
- `UNKNOWN`: chưa thể chứng minh kết quả do mất Leader/quorum, read barrier lỗi hoặc chưa có bản ghi kết quả; **không đồng nghĩa chưa từng commit**.

**Quy tắc quan trọng:** khi Client nhận `UNKNOWN`, chỉ được retry **nguyên `requestId` và payload** hoặc tra cứu lại. Việc tra cứu `UNKNOWN` **không** cho phép khởi tạo requestId mới cho cùng ý định.

### 4.5. `GET_TICKET_HISTORY` (P1)

Request: `{ "tripId":"SE-001", "limit":50, "cursor":null }`. Response: danh sách `{ticketId, seatId, passengerName, status, issuedAt, cancelledAt}` và `nextCursor`. Chỉ cho đọc trạng thái đã xác nhận; giới hạn/ẩn dữ liệu hành khách demo tùy vai trò.

## 5. Diagnostic `NODE_STATUS` (Monitor)

Response payload:

```json
{
  "nodeId": "node-3",
  "role": "LEADER",
  "term": 8,
  "leaderId": "node-3",
  "lastLogIndex": 45,
  "commitIndex": 43,
  "lastApplied": 43,
  "storageHealthy": true,
  "peerConnectivity": {"node-1": true, "node-2": false},
  "observedAt": "2026-10-29T10:00:00Z"
}
```

Dữ liệu status chỉ phản ánh **quan sát tại Node trả lời**, không phải một snapshot nhất quán toàn cụm. Monitor không được suy ra đã commit chỉ vì thấy `lastLogIndex` tăng.

## 6. Raft RPC (chỉ Node↔Node)

**`REQUEST_VOTE.payload`:**

```json
{
  "term": 8,
  "candidateId": "node-3",
  "lastLogIndex": 42,
  "lastLogTerm": 7
}
```

**`REQUEST_VOTE_RESPONSE.payload`:** `{ "term":8, "voteGranted":true }`.

**`APPEND_ENTRIES.payload`:**

```json
{
  "term": 8,
  "leaderId": "node-3",
  "prevLogIndex": 42,
  "prevLogTerm": 7,
  "entries": [
    {
      "index": 43,
      "term": 8,
      "commandType": "BOOK_TICKET",
      "command": {
        "requestId": "542e32fb-b657-4478-9cea-52286822d1d2",
        "tripId": "SE-001",
        "seatId": "G05",
        "passengerName": "Khach demo",
        "ticketId": "ticket-uuid"
      }
    }
  ],
  "leaderCommit": 43
}
```

**`APPEND_ENTRIES_RESPONSE.payload`:** `{ "term":8, "success":true, "matchIndex":43 }`.

- Empty `entries=[]` là heartbeat/commit-index update.
- Nếu bị từ chối do `prevLogIndex/prevLogTerm` không khớp, `success=false`, `matchIndex` chỉ mang giá trị hữu ích nếu đã xác minh; xử lý lùi `nextIndex` là trách nhiệm Raft.
- Payload `command` được persist nguyên vẹn, không tái tạo `ticketId`, timestamp hoặc random khi apply ở từng Node.
- Tương thích phiên bản phải được test hai chiều; thay field wire là **breaking change** cần owner review.

## 7. Mã lỗi chuẩn

| `errorCode` | Ý nghĩa | Hành vi Client |
|---|---|---|
| `INVALID_REQUEST` | Sai schema, thiếu field, ID không hợp lệ | Sửa đầu vào, không retry nguyên trạng vô hạn |
| `UNSUPPORTED_VERSION` | `protocolVersion` không hỗ trợ | Dừng và báo lỗi tương thích |
| `NOT_LEADER` | Node không phải Leader | Dùng `leaderHint`, kiểm tra term, kết nối lại |
| `NO_QUORUM` | Không thể cam kết ghi an toàn | Không báo thành công, có thể thử sau cùng requestId |
| `READ_UNAVAILABLE` | Không chứng minh được read hiện hành | Không hiển thị kết quả như dữ liệu mới nhất |
| `SEAT_ALREADY_BOOKED` | Ghế đã có vé hiệu lực | Hiển thị bị từ chối/refresh |
| `TICKET_NOT_ACTIVE` | Vé không còn hiệu lực | Hiển thị từ chối hủy |
| `REQUEST_ID_CONFLICT` | Cùng requestId nhưng payload khác | Dừng; không tự thay ID và chạy lại cùng ý định |
| `TIMEOUT_UNKNOWN` | Mất phản hồi/chưa biết kết quả | Tra cứu cùng requestId |
| `INTERNAL_ERROR` | Lỗi xử lý chưa được phân loại | Không tự coi là giao dịch thất bại |

`ERROR_RESPONSE` dùng payload `{"errorCode":"NOT_LEADER","message":"...","leaderHint":"node-3","term":8,"requestId":"<uuid-or-null>"}`. `message` chỉ diễn giải cho người dùng, **logic phải dựa `errorCode`**.

## 8. Validation, tương thích và cách khóa contract

- Input: `tripId` đúng mẫu, `seatId` thuộc `G01`–`G10`, `requestId` UUID, `passengerName` không rỗng và bị giới hạn độ dài trong [05-BOOKING-DATABASE.md](05-BOOKING-DATABASE.md).
- Mọi DTO có serialization/deserialization round-trip test, nullability test và golden JSON fixtures; test framing với Unicode tiếng Việt, 2 khung nối tiếp, khung phân mảnh, kết nối đóng giữa khung.
- `shared` module do TV3 sở hữu. TV1/2/4/5/6 không tự định nghĩa DTO đối thủ; thay đổi phải qua PR và cập nhật test.
- **Điểm cần chốt nhóm trước code song song:** chọn field name/enums, quy ước `NOT_LEADER`, có `ticketId` trong log, read barrier, các giá trị error. Khi approved, tạo tag contract v1 và giữ backward-compat hoặc nâng `protocolVersion` khi breaking change.
