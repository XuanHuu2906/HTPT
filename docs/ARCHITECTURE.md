> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# Kiến trúc dự án: 5 Maven module, một Backend Node codebase

## Cấu trúc vận hành

- `desktop-client`: JavaFX, mở nhiều cửa sổ nhân viên bán vé.
- `desktop-monitor`: JavaFX, quan sát cụm, kịch bản lỗi.
- `shared`: Java DTO, Envelope, JSON codec, TCP frame codec, TcpRequestClient. KHÔNG chứa Raft, PostgreSQL, JavaFX.
- `backend-node`: **một** Maven module và **một codebase**. Trong nó chia package `booking/`, `persistence/`, `networking/`, `raft/core/`, `raft/replication/`, `raft/election/`, `raft/recovery/`.
- `integration-tests`: JUnit + kiểm thử toàn hệ thống.

```text
Client JavaFX ------- shared ------- backend-node  (node-1; PostgreSQL 1)
Monitor JavaFX ------ shared ------- backend-node  (node-2; PostgreSQL 2)
                                   backend-node  (node-3; PostgreSQL 3)
                                   backend-node  (node-4; PostgreSQL 4)
                                   backend-node  (node-5; PostgreSQL 5)
                                   (các Node trao đổi trực tiếp bằng TCP peer)
```

## Module dependency (build-time)

```text
shared
   ↑         ↑                 ↑
backend-node desktop-client   desktop-monitor
   ↑
 integration-tests (phụ thuộc thêm shared)
```

5 tiến trình Node được chạy từ **cùng một JAR Backend**, cấu hình `node.id`, 2 cổng TCP và PostgreSQL riêng.

## Luồng giao dịch mục tiêu (chưa triển khai)

1. TV1 gửi `BOOK_TICKET` kèm requestId qua TCP, sử dụng `shared`.
2. TV3 ở Backend giải mã/chuyển yêu cầu đến `NodeRequestRouter` và Raft Leader.
3. TV4 + TV5 tiếp nhận lệnh vào Raft log, nhân bản và kiểm tra quorum (≥3/5) theo quy tắc Raft.
4. TV2 áp dụng lệnh **đã commit** vào PostgreSQL tương ứng, theo thứ tự; bảo đảm idempotency.
5. Backend trả `BOOKING_RESULT` cho Client; nếu timeout, Client tra cứu trạng thái theo cùng requestId.
6. Node follower catch-up log đã commit khi quay lại (TV4 + TV5).

**Không có HTTP/REST/Web Service.** TCP Socket tự định nghĩa message framing. Mỗi Node chỉ nối tới DB của chính mình.

## Các bất biến an toàn

- Không có hai vé hợp lệ cho cùng ghế/trip.
- Không trả SUCCESS khi chưa commit và apply đúng.
- Mất majority: không nhận ghi mới. Client timeout: kết quả `UNKNOWN`, không suy diễn.
- Term/vote/log và lastApplied phải được lưu bền vững theo thuật toán Raft.
- Chỉ chạy 5 tiến trình trên cùng máy là mô phỏng lỗi tiến trình/mạng; không chứng minh chịu lỗi khi hỏng toàn máy.

**Trạng thái starter:** chỉ có skeleton, no real Raft. `BOOK_TICKET` trả `NOT_IMPLEMENTED` cố ý.
