> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# Thay đổi so với bộ ZIP cũ (12 module)

| Cũ | Mới |
|---|---|
| `booking-core/` | `backend-node/src/main/java/vn/nhom6/railway/booking/` |
| `persistence/` | `backend-node/.../persistence/` |
| `tcp-communication/` | `shared/.../transport/` (codec/TCP Client); `backend-node/.../networking/` (TCP Server/peer) |
| `shared-protocol/` | `shared/` (`protocol/` + client-usable `transport/`) |
| `raft-core/` | `backend-node/.../raft/core/` |
| `raft-replication/` | `backend-node/.../raft/replication/` |
| `raft-election/` | `backend-node/.../raft/election/` |
| `fault-simulator/` | `backend-node/.../networking/fault/` |

**Lý do còn `shared/`:** cả Desktop Client và Monitor cần DTO + TCP client, nhưng không nên phụ thuộc vào JAR của Backend (vì sẽ kéo theo JDBC/Raft và chạy sai ranh giới). Do đó 5 Maven module chính: `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`.

Không đổi giao thức mạng, DB config, nghiệp vụ in-memory hay chính sách an toàn. Đây chỉ là tổ chức lại code, không biến TODO Raft thành tính năng hoàn chỉnh.
