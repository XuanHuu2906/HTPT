# TV2 — Tiến độ D11–D13 và các sửa chữa ưu tiên

Cập nhật 2026-11-13. **Bổ sung sau báo cáo này:** [D13 election runtime](D13-election-runtime.md) đã nối core/election/TCP; phần còn thiếu là Leader majority commit/apply/read barrier. Các mô tả thiếu implementation bên dưới là trạng thái ở lượt TV2 trước.

Bảng này ghi nhận kết quả trong repository, không thay bảng kế hoạch 180 nhiệm vụ/Google Sheets MASTER (chưa cập nhật bảng ngoài repository). Không gán xác nhận review của TV3/TV4/TV5/TV6 khi chưa có.

| Ưu tiên | Công việc | Owner / phối hợp | Trạng thái thực tế | Căn cứ / phần còn lại |
|---|---|---|---|---|
| P0 | Nối D13 vào startup | TV2 + TV3 | Đã triển khai, kiểm thử local PASS | NodeMain kiểm tra DB trước mở cổng/timer; lỗi DB/schema/state chặn startup; snapshot giữ term/checkpoint và commitIndex=lastApplied. Raft runtime hoàn chỉnh vẫn chưa có. |
| P0 | D13 PostgreSQL thật, không skip | TV2; TV6 CI | Local PASS; remote CI NOT_RUN | 12 PostgreSQL tests, 0 skip; CI có service PG và gate bắt buộc report D13 không skip. |
| P1 | Kill/restart Backend, bảo toàn DB | TV2 | Local PASS với durable fixture | JVM Backend thật bị kill, restart cùng schema, so sánh toàn bộ rows của 7 bảng; không mất ticket/request/checkpoint, không apply suffix chưa commit. Chưa có booking commit bởi cụm 5 Node hoặc kill giữa SQL transaction. |
| P1 | Booking → Raft commit → DB apply | TV2 + TV3 + TV4/TV5 | CHƯA HOÀN THÀNH — thiếu Raft core runtime | Đã có BookingRequestHandler, withPersistentBooking, JdbcCommittedCommandApplier; TCP/JDBC harness PASS. Mã chính chưa có implementation RaftNode để majority/current-term commit; không được dùng append/ACK giả để trả CONFIRMED. |
| P2 | Minh chứng D11–D13 / tiến độ | TV2 | Đã cập nhật tài liệu và evidence local | Xem liên kết dưới; chưa nghiệm thu AC phân tán hoặc cập nhật Google Sheets MASTER. |

## Đối chiếu D11–D13

- D11: regression JDBC repository/request store/apply và vote RPC adapter PASS ở cấp module. Evidence D11 sẵn có giữ nguyên trong `artifacts/evidence/D11/`; không suy ra booking phân tán PASS.
- D12: regression JDBC storage, RaftStorageAdapter, heartbeat scheduler và TCP handlers PASS. Evidence D12 sẵn có giữ nguyên trong `artifacts/evidence/D12/`; chưa chứng minh Leader majority commit.
- D13: 6 invariant tests và 12 PostgreSQL tests PASS, gồm 3 test mới cho Node startup/schema thiếu/kill-restart. `TC-RAFT-09` được kiểm chứng cục bộ: commit khởi tạo từ applied, giữ nguyên suffix chưa apply. `FR09/AC08` catch-up cả cụm vẫn NOT_RUN.
- Contracts SPEC-03, Java TCP length framing, Raft 5 voter/quorum 3 và transaction apply+lastApplied giữ nguyên. Không có REST/API mới.

## Phụ thuộc để đóng P1 tích hợp

1. TV4/TV5 cung cấp implementation RaftNode dùng chung một RaftState/event loop cho election, replication và apply; commit theo majority 3/5 và current-term rule, không chỉ đếm ACK.
2. TV3 lắp core, peer transport/AppendEntries, timer và submitter/read barrier thật ở entry point NodeMain. State phải được restore từ cùng DB đã qua D13 trước xử lý traffic.
3. TV2 nối committed entry theo index vào JdbcCommittedCommandApplier, gọi onApplied sau SQL commit; future booking chỉ hoàn tất sau durable apply; lỗi apply không bỏ entry.
4. TV2/TV6 chạy 5 Node/5 PostgreSQL thật: booking, mất quorum, Leader kill/restart, retry requestId và đối chiếu DB sau catch-up. Chỉ đóng AC02/AC05–AC10 khi có minh chứng phù hợp.

Lệnh, revision, môi trường, kết quả và giới hạn chi tiết: [TV2 D13 evidence](../../artifacts/evidence/D13/TV2-startup/README.md). Regression thực chạy: shared 30 tests + backend 762 tests, không lỗi/skip. Chưa chạy full-reactor desktop/LAN hoặc GitHub Actions remote trong lượt này.
