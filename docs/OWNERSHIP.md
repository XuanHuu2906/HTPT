> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# Phân công module và package cho 6 thành viên

**Repository duy nhất; Backend chỉ là một Maven module** `backend-node/`. Các phần bên trong là **Java package**, không phải tiến trình Java độc lập và không có `pom.xml` riêng.

| Người | Code chính | Phối hợp |
|---|---|---|
| TV1 | `desktop-client/` | TV3 shared TCP, TV2 booking |
| TV2 | `backend-node/.../booking/`, `backend-node/.../persistence/`, `database/` | TV4 commit/apply |
| TV3 | `shared/.../protocol/`, `shared/.../transport/`, `backend-node/.../networking/` | Tất cả thành viên |
| TV4 | `backend-node/.../raft/core/` (cùng TV5), `raft/replication/` | TV2 persistence, TV5 election |
| TV5 | `backend-node/.../raft/core/` (cùng TV4), `raft/election/`, `raft/recovery/` | TV4 log store, TV3 TCP |
| TV6 | `desktop-monitor/`, `backend-node/.../networking/fault/` (phối hợp TV3), `integration-tests/`, `docs/` | Cả nhóm |

**Lưu ý:** `shared/transport` chỉ giữ thành phần TCP có thể tái sử dụng ở cả JavaFX và Backend (`TcpFrameCodec`, `EnvelopeCodec`, `TcpRequestClient`). `backend-node/networking` giữ phần server, peer adapter, routing. Không nhân đôi codec và không để Frontend phụ thuộc trực tiếp `backend-node`.

## Phân công theo sở hữu

- TV1 không chạm SQL, không quyết định kết quả commit.
- TV2 không trực tiếp tự xác nhận giao dịch khi chưa có Raft commit. Chỉ apply lệnh đã commit với DB transaction nguyên tử, xử lý requestId.
- TV3 chốt schema Envelope, JSON framing và message DTO vào cuối tuần 1; thay đổi contract cần review.
- TV4/TV5 chung **một** role, term, state store; core chỉ dùng một nguồn trạng thái, không xây hai engine khác nhau.
- TV6 làm Monitor và test harness, nhưng mỗi TV chịu trách nhiệm test phần code mình làm.

## Quy tắc Git

- Mỗi người có feature branch và Pull Request riêng. Review chéo TV1↔TV3, TV2↔TV4, TV4↔TV5.
- Mỗi PR đính kèm mô tả, test chạy thực tế, thay đổi contract và ảnh/log (nếu có).
- Không báo hoàn thành chỉ vì biên dịch thành công; cần có testcase và minh chứng.
