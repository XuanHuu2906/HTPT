> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 04 — PostgreSQL và state machine

## Cấu trúc ban đầu
- `trips` — một chuyến mẫu TRIP001.
- `seats` — 10 ghế G01...G10, trạng thái AVAILABLE / SOLD.
- `tickets` — vé có lịch sử ACTIVE / CANCELLED.
- `processed_requests` — requestId, payload fingerprint, kết quả đã quyết định, log index.
- `raft_meta` — currentTerm, votedFor, commitIndex, lastApplied.
- `raft_log` — logIndex, term, commandJson.

Migration: `database/V1__init.sql` (chạy trên từng PostgreSQL). Có unique partial index đảm bảo không có hai vé ACTIVE cho cùng trip + seat tại một Node.

## Cách dựng 5 DB độc lập
- `node-1` dùng PostgreSQL instance ở localhost:5433, DB `railway_node_1`.
- `node-2` -> 5434, v.v. tới `node-5` -> 5437.
- Mỗi process chỉ kết nối vào một DB riêng.
- Cấu hình Docker Compose là optional cho phát triển local; không phải server Web.
- Nếu demo trên LAN cần mỗi Node+DB tương ứng trên máy tính riêng; một PC chỉ chứng minh fault tolerance cấp tiến trình, không phải cấp máy vật lý.

## Transaction apply committed (TV2 + TV4)
1. Raft xác định log entry đủ điều kiện commit.
2. Kiểm tra `lastApplied` dưới transaction DB.
3. Nếu log index đã apply, lấy kết quả lưu và không apply lại.
4. Nếu chưa, kiểm tra yêu cầu, chặn requestId trùng khác payload.
5. Update `tickets`, `seats`, `processed_requests`.
6. Advance `lastApplied` trong **cùng transaction**, sau đó COMMIT PostgreSQL.
7. Chỉ sau đó mới trả kết quả đã được áp dụng.

**Lưu ý:** Quy tắc áp dụng lệnh phải deterministic; không lấy `System.currentTimeMillis`, random UUID riêng từng Follower hoặc trạng thái client local bên trong apply. Dữ liệu như ticket ID phải được Leader tính/chứa trong command hoặc phát sinh từ requestId theo cách xác định giống nhau.

## Các ràng buộc quan trọng
- `tickets` active unique per `(trip_id,seat_id)`; CHECK status; FK seat.
- Lưu `votedFor` và tăng term bền vững trước khi gửi reply vote hợp lệ.
- Follower phải persist AppendEntries hợp lệ trước khi ACK.
- `lastApplied` và nghiệp vụ ở một transaction để không apply trùng khi process crash.
- Snapshot chỉ mở rộng sau khi log replay hoạt động đúng.

## Chưa triển khai trong skeleton
`JdbcSeatQueryService` đã hỗ trợ SELECT; `JdbcCommittedCommandApplier` chưa áp dụng lệnh. Đừng dùng câu INSERT trực tiếp từ Desktop để giả lập đặt vé.
