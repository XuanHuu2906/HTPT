# SPEC-03 — TCP Protocol & Shared Contracts (Nguồn chuẩn DTO)

> **Trạng thái:** **DESIGN-v1 / CHƯA KHÓA**. Tài liệu đầu vào chỉ gợi ý nhóm bản tin; hình dạng JSON, field, error code và thời hạn dưới đây do bộ SPEC đề xuất để nhóm phê duyệt.  
> **Owner:** TV3 · **Review bắt buộc:** TV1, TV2, TV4, TV5, TV6. **Không dùng HTTP/REST/SOAP/Web Service**.

> **Rà soát SPEC-REVIEW-01 (29/10/2026):** các bổ sung vẫn là đề xuất. Code bootstrap hiện dùng envelope/type/seed khác tài liệu này; xem [hồ sơ review và điều kiện phê duyệt](../reviews/SPEC-REVIEW-01.md). Không triển khai riêng một phía hoặc coi cùng số version là tương thích.

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
- Length ngoài giới hạn, UTF-8/JSON lỗi hoặc EOF giữa khung: đóng kết nối, không cố đọc tiếp để tìm khung mới. Chỉ gửi `ERROR_RESPONSE` khi đã parse được envelope và `messageId` hợp lệ để correlation; không trả lỗi cho một response lỗi (tránh vòng lặp).
- Response có thể về khác thứ tự gửi; chỉ ghép với request đang chờ đúng `correlationId`, type và peer. Response đến muộn không được hoàn tất một thao tác mới.

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

`sentAt` chỉ phục vụ log và hiển thị; deadline/timeout đo bằng đồng hồ monotonic cục bộ, không lấy chênh lệch `sentAt` giữa máy. `correlationId=null` cho request; response có `messageId` mới và `correlationId` bằng ID request. `senderId` phải được kiểm tra theo kênh/peer cấu hình, **không** đủ để xác thực hay cấp quyền quản trị; mô hình trust demo cần nhóm chốt tại hồ sơ review.

### 3.0. Kiểu dữ liệu và nullability chung (DESIGN-v1)

- Mọi field được liệt kê đều bắt buộc xuất hiện; chỉ các field ghi rõ nullable mới được `null`. `payload` luôn là object, không phải chuỗi JSON. Từ chối key trùng, field không biết, enum lạ, sai kiểu hoặc tràn số bằng `INVALID_REQUEST`; không ép chuỗi/số/boolean lẫn nhau.
- `protocolVersion`: integer đúng `1`; `messageId` và business `requestId`: UUID dạng `8-4-4-4-12` hex chữ thường. `correlationId`: UUID hoặc null như trên. `senderId`, `type`: string theo §1/§3.1.
- Term/index: JSON integer trong `[0, 9223372036854775807]`, ánh xạ Java `long`; index entry và `appliedLogIndex` đã apply phải >= 1. Log rỗng dùng `(index=0, term=0)`, không lưu entry index 0; entry thực có term >= 1. Không wrap khi tràn.
- Timestamp: string UTC ISO-8601 dạng `YYYY-MM-DDTHH:mm:ss.SSSZ`, chính xác millisecond, dùng chung trong command/DB/response. Ví dụ rút gọn `...:00Z` dưới đây cũng hợp lệ (millisecond bằng 0). Không dùng timestamp làm thứ tự nghiệp vụ.
- `ticketId`: định danh opaque 1–64 ký tự ASCII `[A-Za-z0-9-]`; ví dụ `ticket-uuid` là giá trị minh họa. Client không tự sinh ticketId cho BOOK.
- `leaderId`/`leaderHint`: `node-1`..`node-5` hoặc null nếu chưa biết; hint không chứng minh thẩm quyền. Client ánh xạ node ID qua danh sách endpoint cấu hình trong SPEC-08, không dùng cổng peer cho business request.

### 3.1. Bảng kiểu thông điệp

| Request | Response | Quyền gọi | Ghi chú |
|---|---|---|---|
| `GET_SEATS` | `GET_SEATS_RESPONSE` | Client | Read barrier trên Leader |
| `BOOK_TICKET` | `BOOK_TICKET_RESPONSE` | Client | Lệnh ghi có `requestId` |
| `CANCEL_TICKET` | `CANCEL_TICKET_RESPONSE` | Client | P1, lệnh ghi có `requestId` |
| `GET_REQUEST_RESULT` | `GET_REQUEST_RESULT_RESPONSE` | Client | Tra cứu retry/timeout |
| `GET_TICKET_HISTORY` | `GET_TICKET_HISTORY_RESPONSE` | Client | P1, query có giới hạn và read barrier |
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
    { "seatId": "G02", "status": "BOOKED", "ticketId": "ticket-uuid" },
    { "seatId": "G03", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G04", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G05", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G06", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G07", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G08", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G09", "status": "AVAILABLE", "ticketId": null },
    { "seatId": "G10", "status": "AVAILABLE", "ticketId": null }
  ],
  "readIndex": 42,
  "leaderId": "node-3"
}
```

`seats` phải đủ đúng 10 ghế khi trả kết quả đầy đủ; `readIndex` là chỉ số commit/applied tối thiểu đã dùng để đọc, **không** phải phiên bản riêng từng ghế. Trạng thái chỉ `AVAILABLE`/`BOOKED`, **không có `SELECTED` từ server**.

Thứ tự mảng là G01→G10, không trùng; `ticketId=null` khi AVAILABLE và non-null khi BOOKED. Response thành công có `leaderId` non-null. Query chạy trong một DB snapshot nhất quán sau read barrier, không trộn ghế đọc trước/sau các transaction apply khác nhau.

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

`outcome`: `CONFIRMED` (đã commit+apply), `REJECTED` (đã commit+apply quyết định từ chối nghiệp vụ), `UNKNOWN` (server hết thời hạn chờ kết quả apply). Validation trước append dùng `ERROR_RESPONSE(INVALID_REQUEST)`, không tạo kết quả APPLIED. **Không** đặt `CONFIRMED` khi mới append hoặc khi mới replicate trên 1–2 Node.

| Outcome | `ticketId` BOOK | `reasonCode` | `appliedLogIndex` |
|---|---|---|---|
| `CONFIRMED` | Vé được cấp | null | Index của kết quả đầu tiên đã lưu |
| `REJECTED` | null | `SEAT_ALREADY_BOOKED` hoặc `INVALID_REQUEST` (validation tất định khi apply) | Index của kết quả đã lưu |
| `UNKNOWN` | null | `TIMEOUT_UNKNOWN` | null |

`requestId`, `tripId`, `seatId` luôn giữ input hợp lệ đã nhận. UNKNOWN không được lưu vào `request_results`. Lỗi routing/storage/quorum dùng `ERROR_RESPONSE` theo §7, không biến thành một kết quả từ chối nghiệp vụ bền vững.

### 4.3. `CANCEL_TICKET` (P1)

Request payload: `{"requestId":"<uuid>","ticketId":"ticket-uuid"}`. Response: `requestId`, `outcome` (`CONFIRMED|REJECTED|UNKNOWN`), `ticketId`, `reasonCode`, `appliedLogIndex`. Hủy thành công làm vé thành `CANCELLED` và ghế `AVAILABLE` sau commit+apply.

`ticketId` luôn là vé đích trong request. CONFIRMED có reason null/index >= 1; REJECTED có `TICKET_NOT_ACTIVE` (vé không tồn tại hoặc đã hủy)/index >= 1; UNKNOWN có `TIMEOUT_UNKNOWN`/index null. Retry cùng ID trả kết quả hủy ban đầu; hủy lần nữa bằng ID mới bị từ chối, không giải phóng ghế đang thuộc vé mới.

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
- `UNKNOWN`: sau read barrier chưa có kết quả hoặc thông tin pending tại Leader; **không đồng nghĩa chưa từng commit**. Nếu read barrier thất bại thì trả `ERROR_RESPONSE(READ_UNAVAILABLE)`, Client cũng hiển thị UNKNOWN.

APPLIED_SUCCESS/APPLIED_REJECTED chỉ lấy từ `request_results` sau read barrier; `commandType` non-null và `appliedLogIndex >= 1`, reason/ticket theo kết quả BOOK/CANCEL ở trên. PENDING có `commandType` của ý định đang xử lý, các field `ticketId`, `reasonCode`, `appliedLogIndex` null. UNKNOWN có cả bốn field đó null. Follower luôn trả NOT_LEADER. Lookup theo ID trả kết quả **ý định đầu tiên** đã apply, không phải payload xung đột gửi lại sau đó; Client gặp REQUEST_ID_CONFLICT không được gán kết quả lookup cho payload đã sửa.

**Quy tắc quan trọng:** khi Client nhận `UNKNOWN`, chỉ được retry **nguyên `requestId` và payload** hoặc tra cứu lại. Việc tra cứu `UNKNOWN` **không** cho phép khởi tạo requestId mới cho cùng ý định.

### 4.5. `GET_TICKET_HISTORY` (P1)

Request: `{ "tripId":"SE-001", "limit":50, "cursor":null }`; `limit` integer 1–100. Response object có đúng các field `tripId`, `tickets`, `nextCursor`, `readIndex`, `leaderId`. `tickets` là mảng (có thể rỗng) các object `{ticketId, seatId, passengerName, status, issuedAt, cancelledAt}`; status ACTIVE/CANCELLED, `cancelledAt=null` chỉ khi ACTIVE. Các field khác non-null, trừ `nextCursor` khi hết trang.

DESIGN-v1: sắp vé theo index BOOK thành công tăng dần, lấy từ kết quả BOOK đã lưu; không sắp theo đồng hồ máy. `cursor` là null hoặc chuỗi thập phân chuẩn của index BOOK cuối trang trước (không dấu/zero đầu, số >= 1 trong miền long). Trang sau lấy index lớn hơn cursor; cursor sai dạng trả INVALID_REQUEST. `nextCursor` là index vé cuối trang nếu còn vé, nếu hết là null. Mỗi trang qua read barrier và một DB snapshot; các trang không phải snapshot toàn bộ lịch sử cố định, trạng thái hủy có thể thay đổi giữa hai lần đọc. UI refresh từ cursor null để cập nhật vé cũ. Response phải nằm trong giới hạn frame; không cắt JSON giữa chừng.

Endpoint này trả lịch sử **vé**, không phải API stream sự kiện hay danh sách mọi request bị từ chối. Nhật ký giao dịch/sự kiện FR11 được lưu trong `request_results`/`business_events` và log vận hành; giao diện tra toàn bộ sự kiện cần contract riêng được review, không suy diễn từ endpoint này. Demo chỉ dùng tên hành khách giả, chưa có phân quyền theo vai trò người dùng.

## 5. Diagnostic `NODE_STATUS` (Monitor)

Request payload: `{}`; được gọi tại mọi Node, không yêu cầu Leader/read barrier.

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
  "peerConnectivity": {"node-1": true, "node-2": false, "node-4": null, "node-5": true},
  "observedAt": "2026-10-29T10:00:00Z"
}
```

Dữ liệu status chỉ phản ánh **quan sát tại Node trả lời**, không phải một snapshot nhất quán toàn cụm. Monitor không được suy ra đã commit chỉ vì thấy `lastLogIndex` tăng.

`role` là FOLLOWER/CANDIDATE/LEADER, hoặc null nếu chưa khởi tạo được Raft; không bịa FOLLOWER/term 0 để che lỗi storage. `storageHealthy` boolean theo lần kiểm tra cục bộ; term/index có thể null khi không đọc được giá trị đáng tin. `peerConnectivity` có đủ 4 peer (không gồm self), true = lần RPC gần nhất thành công, false = thất bại/timeout, null = chưa quan sát; các giá trị không phải chứng nhận quorum. `observedAt` là thời điểm tạo status; Monitor tự đo độ cũ từ lúc nhận bằng đồng hồ cục bộ. Node không phản hồi thì Monitor tự hiển thị OFFLINE/UNKNOWN, không tạo NODE_STATUS giả.

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
        "ticketId": "ticket-uuid",
        "issuedAt": "2026-10-29T10:00:00.000Z"
      }
    }
  ],
  "leaderCommit": 43
}
```

**`APPEND_ENTRIES_RESPONSE.payload`:** `{ "term":8, "success":true, "matchIndex":43 }`.

- Empty `entries=[]` là heartbeat/commit-index update.
- Khi success=true, `matchIndex = prevLogIndex + entries.size()` của chính RPC đã xử lý (heartbeat dùng prevLogIndex), không phải toàn bộ đuôi log Follower. Khi success=false, `matchIndex=0` và Leader bỏ qua field này; thuật toán lùi nextIndex theo SPEC-04. Response term luôn là currentTerm của bên nhận sau xử lý.
- Payload `command` được persist nguyên vẹn, không tái tạo `ticketId`, timestamp hoặc random khi apply ở từng Node.
- Tương thích phiên bản phải được test hai chiều; thay field wire là **breaking change** cần owner review.

### 6.1. Log command và kiểm tra RPC (DESIGN-v1)

| `commandType` trong entry | Các field bắt buộc của `command` |
|---|---|
| `BOOK_TICKET` | `requestId`, `tripId`, `seatId`, `passengerName`, `ticketId`, `issuedAt` |
| `CANCEL_TICKET` | `requestId`, `ticketId`, `cancelledAt` |
| `NO_OP` | Object rỗng `{}` |

NO_OP là entry nội bộ phục vụ read barrier trong SPEC-04, **không** là message Client mới và không có requestId/ticket/audit nghiệp vụ. Leader cấp ticketId và timestamp trước append; không sửa command đã persist. Quy tắc intent/normalization nằm tại SPEC-05 §3.1.

Chỉ nhận Raft RPC trên kênh peer; candidateId/leaderId phải trùng senderId và thuộc tập 5 voter cấu hình. Entry bắt đầu `prevLogIndex+1`, liên tiếp, term không giảm trong chuỗi log và không vượt term RPC; `(prevLogIndex=0)` yêu cầu prevLogTerm=0. Leader chia batch để cả envelope <= 1 MiB. Type/schema lỗi dùng INVALID_REQUEST; term cũ/log mismatch dùng Raft response false. Chỉ response được correlation với RPC hiện tại từ peer hợp lệ mới được tính vote/ACK; chi tiết term và phản hồi trễ ở SPEC-04.

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

Tất cả 5 field trên đều xuất hiện: errorCode/message non-null, leaderHint nullable, term nullable nếu chưa có Raft state đáng tin, requestId nullable nếu request không có business ID hợp lệ. `SEAT_ALREADY_BOOKED`/`TICKET_NOT_ACTIVE` là reasonCode của response nghiệp vụ đã apply, không trả như lỗi pre-check ghế. REQUEST_ID_CONFLICT dùng ERROR_RESPONSE và không ghi đè kết quả gốc; cách apply conflict xem SPEC-05. Không có error code `UNKNOWN`: đó là outcome/state/UI state.

NO_QUORUM, NOT_LEADER sau đổi vai trò, INTERNAL_ERROR hoặc TIMEOUT_UNKNOWN không chứng minh lệnh trước đó chưa commit. Client giữ nguyên ý định khi retry. INVALID_REQUEST chỉ cho biết lần gửi này không được append; không phủ định một lần gửi trước đã mất phản hồi. Request sai type/schema/field bị từ chối trước append, kể cả Client cố gửi field do Leader cấp.

## 8. Validation, tương thích và cách khóa contract

- Input: `tripId` đúng `SE-001`, `seatId` thuộc `G01`–`G10`, `requestId` UUID, `passengerName` không rỗng và bị giới hạn độ dài trong [05-BOOKING-DATABASE.md](05-BOOKING-DATABASE.md).
- Mọi DTO có serialization/deserialization round-trip test, nullability test và golden JSON fixtures; test framing với Unicode tiếng Việt, 2 khung nối tiếp, khung phân mảnh, kết nối đóng giữa khung.
- `shared` module do TV3 sở hữu. TV1/2/4/5/6 không tự định nghĩa DTO đối thủ; thay đổi phải qua PR và cập nhật test.
- **Điểm cần chốt nhóm trước code song song:** chọn field name/enums, quy ước `NOT_LEADER`, có `ticketId` trong log, read barrier, các giá trị error. Khi approved, tạo tag contract v1 và giữ backward-compat hoặc nâng `protocolVersion` khi breaking change.
- Quyết định phê duyệt, migration từ bootstrap và người review được ghi tại [SPEC-REVIEW-01](../reviews/SPEC-REVIEW-01.md); chỉ đổi nhãn thành APPROVED-v1 khi có xác nhận cho đúng revision. Approval thiết kế không đồng nghĩa code đã hỗ trợ hoặc AC đã PASS.
