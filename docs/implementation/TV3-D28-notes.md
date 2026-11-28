# TV3-D28 — Đóng băng Protocol Version v1 (Non-Breaking Freeze)

> **Task ID:** `tv3-d28`  
> **Owner:** TV3 (`shared/**`, `backend-node/**/networking/**`)  
> **Ngày hoàn thành:** 2026-11-28  
> **Trạng thái giao thức:** **FROZEN v1 (`protocolVersion = 1`)** — Không breaking change.

---

## 1. Phạm vi đóng băng & Bảo đảm tương thích ngược

- `MessageEnvelope.CURRENT_PROTOCOL_VERSION` và `MessageDispatcher.FROZEN_PROTOCOL_VERSION` được giữ cố định ở giá trị **`1`**.
- Wire framing giữ nguyên chuẩn `SPEC-03 §2`: `[4-byte Big-Endian unsigned length || UTF-8 JSON]`, với `1 <= length <= 1,048,576` bytes (1 MiB).
- Không đổi tên field, không xóa/đổi tên `MessageType`, không đổi `errorCode` chuẩn trong `SPEC-03 §7`, và không thêm DTO breaking vào `shared` để bảo toàn 100% tương thích với `desktop-client` và các module đã tích hợp của TV1/TV2/TV4/TV5/TV6.

---

## 2. Danh mục 17 `MessageType` đóng băng & Fixture `protocol-v1.json`

Tất cả 17 giá trị trong `vn.nhom6.railway.protocol.dto.MessageType` đã có ví dụ chuẩn v1 trong `shared/src/test/resources/fixtures/protocol-v1.json`:

| # | `MessageType` | Hướng | Kênh cho phép (`ChannelType`) | Leader-Only | Key trong `protocol-v1.json` |
|---|---|---|---|---|---|
| 1 | `GET_SEATS` | Request | `CLIENT`, `ALL` | Không (Read barrier / Leader) | `getSeatsRequest` |
| 2 | `GET_SEATS_RESPONSE` | Response | — | — | `getSeatsResponse` |
| 3 | `BOOK_TICKET` | Request | `CLIENT`, `ALL` | **Có** | `bookTicketRequest` |
| 4 | `BOOK_TICKET_RESPONSE` | Response | — | — | `bookTicketResponse` |
| 5 | `CANCEL_TICKET` | Request | `CLIENT`, `ALL` | **Có** | `cancelTicketRequest` |
| 6 | `CANCEL_TICKET_RESPONSE` | Response | — | — | `cancelTicketResponse` |
| 7 | `GET_REQUEST_RESULT` | Request | `CLIENT`, `ALL` | **Có** | `getRequestResultRequest` |
| 8 | `GET_REQUEST_RESULT_RESPONSE` | Response | — | — | `getRequestResultResponse` |
| 9 | `GET_TICKET_HISTORY` | Request | `CLIENT`, `ALL` | Không | `getTicketHistoryRequest` |
| 10 | `GET_TICKET_HISTORY_RESPONSE` | Response | — | — | `getTicketHistoryResponse` |
| 11 | `NODE_STATUS` | Request | `CLIENT`, `MONITOR`, `ALL` | Không | `nodeStatusRequest` |
| 12 | `NODE_STATUS_RESPONSE` | Response | — | — | `nodeStatusResponse` |
| 13 | `REQUEST_VOTE` | Request | `PEER`, `ALL` | Không | `requestVoteRequest` |
| 14 | `REQUEST_VOTE_RESPONSE` | Response | — | — | `requestVoteResponse` |
| 15 | `APPEND_ENTRIES` | Request | `PEER`, `ALL` | Không | `appendEntriesReplicationRequest`, `appendEntriesHeartbeatRequest` |
| 16 | `APPEND_ENTRIES_RESPONSE` | Response | — | — | `appendEntriesResponse` |
| 17 | `ERROR_RESPONSE` | Response | — | — | `errorResponse` |

---

## 3. Bảng Port hợp nhất DEV / LAN / PostgreSQL (`SPEC-08 §3`)

| Node / Vai trò | Chế độ DEV (Docker Compose trên 1 máy) | Chế độ LAN (6 Laptop thật) | PostgreSQL |
|---|---|---|---|
| `node-1` (Laptop 1) | Client: `127.0.0.1:9101` (`9100` nội bộ)<br>Peer: `127.0.0.1:9201` (`backend-node-1:9200` nội bộ) | Client: `<IP_L1>:9100`<br>Peer: `<IP_L1>:9200` | Internal `5432` (Dev host: `5433`) |
| `node-2` (Laptop 2) | Client: `127.0.0.1:9102` (`9100` nội bộ)<br>Peer: `127.0.0.1:9202` (`backend-node-2:9200` nội bộ) | Client: `<IP_L2>:9100`<br>Peer: `<IP_L2>:9200` | Internal `5432` (Dev host: `5434`) |
| `node-3` (Laptop 3) | Client: `127.0.0.1:9103` (`9100` nội bộ)<br>Peer: `127.0.0.1:9203` (`backend-node-3:9200` nội bộ) | Client: `<IP_L3>:9100`<br>Peer: `<IP_L3>:9200` | Internal `5432` (Dev host: `5435`) |
| `node-4` (Laptop 4) | Client: `127.0.0.1:9104` (`9100` nội bộ)<br>Peer: `127.0.0.1:9204` (`backend-node-4:9200` nội bộ) | Client: `<IP_L4>:9100`<br>Peer: `<IP_L4>:9200` | Internal `5432` (Dev host: `5436`) |
| `node-5` (Laptop 5) | Client: `127.0.0.1:9105` (`9100` nội bộ)<br>Peer: `127.0.0.1:9205` (`backend-node-5:9200` nội bộ) | Client: `<IP_L5>:9100`<br>Peer: `<IP_L5>:9200` | Internal `5432` (Dev host: `5437`) |
| Client & Monitor (Laptop 6) | Kết nối tới `127.0.0.1:9101..9105` (`configs/dev/cluster.properties`) | Kết nối tới `<IP_L1..L5>:9100` (`configs/lan/PORTS.md`) | **Cấm** kết nối SQL trực tiếp |

- Quy tắc cách ly kênh (`MessageDispatcher.resolveChannelForPort(port)`):
  - `9100` hoặc `9101..9105` → `ChannelType.CLIENT` (chặn `REQUEST_VOTE`, `APPEND_ENTRIES`).
  - `9200` hoặc `9201..9205` → `ChannelType.PEER` (chỉ nhận `REQUEST_VOTE`, `APPEND_ENTRIES`).

---

## 4. Retry Semantics chuẩn v1 (`SPEC-03 §7`, `RetryPolicy`, `SocketBookingClient`)

1. **Phân biệt `messageId` và `requestId`:**
   - `messageId`: UUID mới cho **mỗi lần gửi/thử lại trên TCP wire**; `correlationId` của response phải khớp chính xác `messageId` của request tương ứng.
   - `requestId`: UUID định danh ý định nghiệp vụ (`BOOK_TICKET`, `CANCEL_TICKET`, `GET_REQUEST_RESULT`); **giữ nguyên tuyệt đối** qua mọi lần retry cùng ý định, chỉ đổi khi người dùng đổi ý định.
2. **Thông số `RetryPolicy` mặc định (mạng LAN):**
   - `maxAttempts = 3`, `initialBackoffMs = 100ms`, `maxBackoffMs = 1000ms`, `backoffMultiplier = 2.0`, `connectTimeoutMs = 3000ms`, `readTimeoutMs = 5000ms` (Client config dev: connect `2000ms`, request `5000ms`, backoff `500ms`).
3. **Bảng xử lý theo lỗi / timeout:**

| Tình huống | Được retry tự động? | Giữ `requestId`? | Trạng thái / Hành động chuẩn |
|---|---|---|---|
| `FAILED_NOT_SENT` (`ConnectException`, `NoRouteToHostException`, chưa ghi lên socket) | **Có** (theo `RetryPolicy`) | **Có** | Thử node khác / backoff; `messageId` mới. |
| `SocketTimeoutException` / `TIMEOUT_UNKNOWN` (đã gửi, mất response hoặc nghẽn) | Không tự đoán `CONFIRMED`/`REJECTED` | **Có** | Chuyển UI sang `UNKNOWN`; tra cứu `GET_REQUEST_RESULT(requestId)` hoặc retry nguyên `requestId`. |
| `ERROR_RESPONSE: NOT_LEADER` | **Có** (chuyển hướng) | **Có** | Nếu có `leaderHint` hợp lệ → nối tới node gợi ý; nếu null/stale → dò cluster endpoints với backoff. |
| `ERROR_RESPONSE: NO_QUORUM` / `READ_UNAVAILABLE` | Tùy chọn có backoff | **Có** | Hiển thị `UNKNOWN`/tạm thời không đủ quorum (2/5 không xác nhận ghi). |
| `ERROR_RESPONSE: REQUEST_ID_CONFLICT` | **Không** | — | Lỗi xung đột payload cho cùng `requestId` (`REJECTED`). |
| `ERROR_RESPONSE: INVALID_REQUEST` (kể cả sai `protocolVersion != 1`) | **Không** | — | Sửa payload/phiên bản giao thức; không retry mù. |
| `BOOK_TICKET_RESPONSE` / `CANCEL_TICKET_RESPONSE` với `REJECTED` (ví dụ `SEAT_ALREADY_BOOKED`) | **Không** | — | Kết quả nghiệp vụ cuối cùng. |

---

## 5. Kiểm thử xác minh (`artifacts/evidence/D28/TV3-D28-result.log`)

- `shared`: `ProtocolContractTest` (kiểm tra đủ 17 `MessageType` trong `protocol-v1.json`, `FrameCodec` + `MessageCodec` hai pha, UTF-8, negative cases) cùng các test `shared` liên quan.
- `backend-node`: `ProtocolV1FreezeCompatibilityTest`, `MessageDispatcherTest`, `ProtocolFuzzAndConcurrencyRegressionTest`, `TcpPacketLossTimeoutIntegrationTest`, `RetryPolicyTest`.
