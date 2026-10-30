> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 06 — Kiểm thử AC01–AC10

Chỉ đánh PASS sau khi chạy thực tế với bằng chứng (log/screenshot, process IDs, DB query, term/index tại từng node).

| AC | Kịch bản | Kết quả mong đợi | Chủ kiểm chứng |
|---|---|---|---|
| AC01 | Khởi tạo TRIP001 | G01–G10 đúng 10 ghế | TV1+TV2 |
| AC02 | Client đặt ghế trống | 1 vé ACTIVE được commit và hiển thị | TV1+TV2+TV4 |
| AC03 | 2 Client cùng G05 | Tối đa 1 kết quả thành công | TV2+TV6 |
| AC04 | Bán 10 ghế rồi đặt tiếp | Không có vé hợp lệ thứ 11 | TV2+TV6 |
| AC05 | Tắt 1 hoặc 2 follower | Cụm đủ quorum vẫn nhận ghi | TV4+TV5+TV6 |
| AC06 | Chỉ có 2/5 kết nối | Không xác nhận ghi mới | TV4+TV5 |
| AC07 | Kill Leader | Bầu Leader mới và tiếp tục có quorum | TV5+TV6 |
| AC08 | Khởi động lại Node cũ | Log catch-up và DB khớp committed state | TV4+TV5 |
| AC09 | Phân vùng 3–2 | Nhóm 2 không confirm, không có 2 Leader cùng xác nhận ghi | TV3+TV5+TV6 |
| AC10 | Mất phản hồi khi đặt | Retry cùng requestId không tạo vé trùng | TV2+TV3+TV6 |

## Ví dụ bằng chứng cho AC03
- Start 5 backend processes, ghi lại Leader/currentTerm.
- Mở hai Desktop clients ở hai cửa sổ độc lập.
- Cùng gửi BOOK_TICKET G05 với requestId khác nhau.
- Kiểm tra kết quả trả về, journal Leader và query `tickets` trên tất cả Node sau commit/apply.
- `SELECT COUNT(*) FROM tickets WHERE trip_id='TRIP001' AND seat_id='G05' AND status='ACTIVE'` phải bằng 1 ở replica đã bắt kịp.
- Không được đánh PASS chỉ vì ảnh hai cửa sổ khác nhau; cần log / DB evidence.

## Trạng thái bộ khung
- Chỉ có unit test của *in-memory fixture* và TCP frame.
- AC01 mới có schema với 10 ghế; cần thực chạy các instance để xác nhận.
- AC02–AC10 chưa chạy, chưa được PASS.
