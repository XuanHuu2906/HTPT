> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 03 — TCP protocol contract v1

**Cấm sử dụng HTTP / REST / Web Service.** Mọi payload đi trực tiếp trên raw TCP `Socket` / `ServerSocket`.

## Frame
```
+--------------------------+-----------------------+
| LENGTH: signed int32 BE  | JSON UTF-8 bytes      |
| 4 bytes                  | N bytes               |
+--------------------------+-----------------------+
```
- 4-byte big-endian integer N > 0, N <= 1 MiB.
- TCP không bảo đảm `read()` trả về đúng một message; luôn đọc đủ 4 byte rồi đủ N byte.
- Một request/connection ở bộ khung hiện tại, chỉ mở rộng keep-alive và multiplexing nếu có test.
- TCP **chỉ bảo đảm truyền byte**, không bảo đảm lệnh nghiệp vụ chạy exactly-once.

## Envelope
```json
{
  "version": 1,
  "type": "BOOK_TICKET",
  "requestId": "a30ca751-0471-46ac-aa5d-b3562826dd5d",
  "senderId": "counter-1",
  "correlationId": null,
  "payload": {
    "tripId": "TRIP001",
    "seatId": "G05",
    "passengerName": "Khach Mo Phong"
  }
}
```
`response.requestId` phải trùng `request.requestId`. `correlationId` có thể bằng requestId. Mọi thông điệp có `version` để tránh lệch schema.

## Message types
| Message | Hướng | Ý nghĩa |
|---|---|---|
| `PING` → `PONG` | any→Node | Liveness đơn giản, **không** chứng minh Raft khỏe |
| `GET_NODE_STATUS` → `NODE_STATUS` | Monitor→Node | Read-only node status |
| `GET_SEATS` → `SEATS_RESPONSE` | Client→Node | Hiện tại chỉ đọc local DB; **có thể stale** |
| `BOOK_TICKET` → `BOOKING_RESULT` | Client→Leader | Chỉ thành công sau commit+apply |
| `CANCEL_TICKET` → `BOOKING_RESULT` | Client→Leader | Hủy theo thứ tự log |
| `GET_REQUEST_STATUS` | Client→Leader | Tìm lại kết quả của requestId |
| `LEADER_REDIRECT` | Node→Client | Gợi ý Leader; client vẫn phải kiểm tra term/connection |
| `REQUEST_VOTE` → `REQUEST_VOTE_RESPONSE` | Node↔Node | Raft election |
| `APPEND_ENTRIES` → `APPEND_ENTRIES_RESPONSE` | Node↔Node | Heartbeat + log replication |
| `ERROR`, `NOT_IMPLEMENTED` | any | Không báo SUCCESS khi handler chưa có |

## Lỗi phải phân biệt
- `SEAT_ALREADY_SOLD`: giao dịch được xử lý nhưng ghế đã bán.
- `NOT_LEADER`: chuyển Client tới Leader hợp lệ.
- `NO_QUORUM`: không xác nhận giao dịch.
- `REQUEST_PENDING`: chưa biết commit/response.
- `REQUEST_ID_CONFLICT`: cùng requestId nhưng khác payload.
- `DATABASE_UNAVAILABLE`: node local database không sẵn sàng.
- `RAFT_NOT_IMPLEMENTED`: bộ khung chưa triển khai chức năng; không phải lỗi nghiệp vụ.

## Retry an toàn
- Client tạo UUID **một lần cho mỗi ý định mua**; mất phản hồi phải giữ nguyên requestId cùng payload.
- Không tự tạo requestId khác khi retry trạng thái UNKNOWN.
- Backend lưu kết quả requestId cùng với `lastApplied` trong transaction và trả lại kết quả cũ nếu retry.
- `GET_REQUEST_STATUS` chưa được triển khai ở skeleton. Chỉ dùng sau khi có test ghi chậm/commit rồi mất ACK.

## Raft RPC (phải hoàn thiện)
- `RequestVoteDto(term,candidateId,lastLogIndex,lastLogTerm)`.
- `AppendEntriesDto(term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit)`.
- Cần thêm message trả lời có `term`, `success`, `matchIndex`, `voteGranted`; DTO v1 hiện chỉ dựng request classes.
- `entries` hiện là JSON node tổng quát; trước khi triển khai chính thức, TV3–TV5 phải chuẩn hóa định dạng của RaftLogEntry.
