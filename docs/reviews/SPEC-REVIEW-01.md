> Cập nhật BASE-CLEANUP-01: các source, migration, config và script kiểm tra được dẫn dưới đây thuộc trạng thái lịch sử và đã được xóa theo yêu cầu người dùng. Các nhận xét code chỉ áp dụng tại thời điểm rà soát; xem [báo cáo dọn base](BASE-CLEANUP-01.md).

# SPEC-REVIEW-01 — Rà soát contract và toàn bộ 9 SPEC

Ngày: 29/10/2026. Phạm vi: SPEC-00 đến SPEC-08, trọng tâm SPEC-03. Trạng thái hiện hành: **APPROVED-v1**.

Đây là hồ sơ rà soát và phê duyệt thiết kế. Căn cứ chuyển trạng thái: chỉ thị trực tiếp của người dùng ngày 29/10/2026, “không cần đụng tới code chuyển trạng thái cho tôi”. Chỉ chuyển trạng thái tài liệu; không xác nhận code đã tương thích hoặc test runtime đã PASS, không suy diễn từng TV1–TV6 đã review riêng.

## 1. Phát hiện và xử lý trong tài liệu

| ID | Vấn đề có căn cứ trong bản trước | Sửa/đề xuất | FR/AC và reviewer |
|---|---|---|---|
| R01 | SPEC-02 §7 gọi quorum là 5 hoặc số Node online, trái majority cố định | Ghi membership 5, quorum 3 không đổi theo online | FR06–09, AC05–09; TV4/5/6 |
| R02 | SPEC-04 commit Follower theo lastLogIndex có thể gồm suffix chưa khớp | Giới hạn bằng index cuối được RPC xác minh, không giảm commitIndex; heartbeat vẫn log-match | FR07/09, AC08/09; TV4/5 |
| R03 | Vote/ACK trùng, phản hồi trễ, term cao trong response và Candidate cùng term chưa rõ | Phiếu theo voter duy nhất, correlation, bỏ response lỗi thời, step-down/persist, một AppendEntries đang chờ/peer | FR07/08, AC05–09; TV3/4/5 |
| R04 | SPEC-04 đề cập no-op/read barrier nhưng SPEC-03 không có NO_OP; lịch sử chưa ghi barrier | Bổ sung command NO_OP nội bộ, đề xuất log barrier từng query; NO_OP chỉ cập nhật checkpoint | FR01/02/11, AC01/10; TV2/3/4/5 |
| R05 | issuedAt/cancelledAt bắt buộc ở SPEC-05 nhưng command wire chưa đủ | Thêm schema BOOK/CANCEL log và timestamp; không sinh lại khi apply | FR03/10/11, AC02/08/10; TV2/3/4 |
| R06 | Nullability, miền số, correlation lỗi, ví dụ ghế chỉ 2 phần tử chưa rõ | Quy tắc field/type/envelope, 10 ghế mẫu, đóng frame lỗi, chuẩn result/error/lookup | FR01–05, AC01–04/10; TV1/2/3 |
| R07 | REJECTED lẫn validation trước append và từ chối đã apply; UNKNOWN lẫn errorCode | Tách lỗi trước append khỏi kết quả đã lưu; reason/index theo state, barrier lỗi dùng READ_UNAVAILABLE | FR03/05, AC02/10; TV1/2/3 |
| R08 | Fingerprint chưa định nghĩa byte; loại ticketId chung làm sai CANCEL; conflict khi apply chưa có đường tiến checkpoint | Tuple/hash cụ thể, phân biệt ticketId của BOOK/CANCEL; giữ kết quả gốc, conflict không chặn apply | FR03/05/10, AC03/10; TV2/3/4 |
| R09 | BIGSERIAL audit có thể khác sau rollback; NO_OP và restart checkpoint chưa rõ | Audit khóa log_index; NO_OP không tạo business row; khởi tạo commitIndex từ checkpoint bền | FR06/09/11, AC08/10; TV2/4/5 |
| R10 | History thiếu tên mảng, limit/cursor/order; NODE_STATUS thiếu request và ý nghĩa unknown | Định nghĩa tickets/cursor index và snapshot mỗi trang; status đủ 4 peer, nullable khi chưa biết | FR11/12; TV1/2/3/6 |
| R11 | UI chỉ giữ requestId trong trạng thái thao tác, chưa xử lý restart; retry BOOK cũ sau hủy dễ hiển thị sai | Đề xuất pending journal trước gửi, restore cùng ý định; refresh trạng thái hiện tại riêng với kết quả cũ | FR03/10, AC10; TV1/2/3 |
| R12 | leaderHint chỉ ID nhưng deployment chưa có map đủ 5 client endpoint; advertise dev lẫn Docker/Windows | Thêm map/routing fallback, tách client/peer port; CLUSTER_ID không phải wire isolation | FR07/12, AC07/09/10; TV3/6 |
| R13 | Snapshot được gọi P1 trong Raft nhưng ngoài baseline; integration 1 Node có thể bị hiểu được commit 5 voter | Thống nhất mở rộng ngoài baseline; 1 Node chỉ test fixture ghi rõ, không thay test quorum | FR06–09, AC05–09; cả nhóm |
| R14 | Test “tối đa 1” có thể đạt khi mọi booking thất bại; “không CONFIRMED mới” nhầm response trễ | Thêm liveness trong cụm khỏe, phân biệt commit mới/response trễ; bổ sung 15 ca biên NOT_RUN | AC01–10, FR10–12; cả nhóm |

Nguồn thuật toán đối chiếu: [Raft extended paper](https://raft.github.io/raft.pdf), Figure 2 và §8. Phần sửa dựa vào tài liệu này: commit Follower chỉ đến phần khớp, xử lý term/vote/ACK, yêu cầu read authority. Các lựa chọn encoding, SQL, UI và barrier cụ thể là đề xuất của dự án, không phải nội dung được nguồn bên ngoài phê duyệt.

## 2. Mâu thuẫn với code hiện có — không sửa runtime trong task này

| Bằng chứng đã đọc | Code bootstrap | SPEC đề xuất | Tác động |
|---|---|---|---|
| `../../shared/src/main/java/vn/nhom6/railway/protocol/Envelope.java` (đã xóa), `../../shared/src/main/java/vn/nhom6/railway/protocol/dto/BookingRequestDto.java` (đã xóa) | version, requestId tại envelope; không messageId/sentAt | protocolVersion, messageId/sentAt, business requestId trong payload | Breaking wire shape; cùng số 1 không tương thích |
| `../../shared/src/main/java/vn/nhom6/railway/protocol/MessageType.java` (đã xóa) | SEATS_RESPONSE, BOOKING_RESULT, GET_REQUEST_STATUS, GET_NODE_STATUS, LEADER_REDIRECT, ERROR | Các type ở SPEC-03 khác | Cần đổi đồng bộ shared/handler/Desktop/fixtures |
| `../../backend-node/src/main/java/vn/nhom6/railway/node/NodeRequestRouter.java` (đã xóa) | Trả NOT_IMPLEMENTED cho write/Raft; GET_SEATS đọc local TRIP001 | Commit/apply + authoritative read barrier, SE-001 | Không coi bootstrap là distributed runtime đã đạt SPEC |
| `../../shared/src/main/java/vn/nhom6/railway/protocol/dto/NodeStatusDto.java` (đã xóa) | readiness, thiếu lastLogIndex/storageHealthy/peerConnectivity/observedAt | Diagnostic schema SPEC-03 | Cần phối hợp Monitor + backend |
| `../../backend-node/src/main/java/vn/nhom6/railway/booking/BookingCommand.java` (đã xóa), `../../backend-node/src/main/java/vn/nhom6/railway/persistence/JdbcCommittedCommandApplier.java` (đã xóa) | Chưa timestamp; applier ném UnsupportedOperationException | Command đầy đủ, atomic apply/checkpoint | Còn công việc TV2/4/5, không tự báo PASS |
| `../../database/V1__init.sql` (đã xóa), `../../backend-node/src/main/java/vn/nhom6/railway/booking/InMemoryBookingStateMachine.java` (đã xóa) | TRIP001, SOLD, processed_requests, schema metadata khác; state machine là fixture | SE-001, BOOKED, request_results, DDL SPEC-05 | Cần migration/fixture update; không ghi đè migration cũ tùy tiện |
| `../../configs/client.properties` (đã xóa) | Chỉ một host/port; comment cho biết JavaFX dùng JVM properties | Map đủ 5 endpoint để redirect/discovery | Config mục tiêu chưa được triển khai |

`../../shared/src/main/java/vn/nhom6/railway/transport/TcpFrameCodec.java` (đã xóa) dùng int signed và kiểm tra 1..1 MiB; byte framing trong miền hợp lệ vẫn trùng SPEC. Không coi riêng signed/unsigned comment là breaking change khi mọi high-bit length đều bị từ chối.

Lựa chọn để nhóm quyết định: **A**, giữ code bootstrap như hiện tại đến một lượt migration phối hợp sang SPEC đã duyệt (bản review này đi theo hướng A); **B**, điều chỉnh SPEC về code bootstrap, cần đánh giá lại invariants và không được giữ local read/no-Raft như nghiệm thu cuối. Trong hướng A, TV3 phải chốt tăng version hay chuyển toàn bộ cluster/client cùng lúc trong môi trường demo; không triển khai trộn hai envelope cùng version và không tự reset dữ liệu.

## 3. Baseline phê duyệt và việc triển khai còn lại

| Quyết định | Đề xuất/câu hỏi còn mở | Chủ trì / review | Trạng thái |
|---|---|---|---|
| D01 — Wire baseline & migration | Schema SPEC-03 được duyệt; phương án version migration/chuyển đồng bộ còn phải chọn trong lượt triển khai | TV3 / tất cả | APPROVED-v1; migration chưa triển khai |
| D02 — Read barrier | Log NO_OP mới cho từng query; chấp nhận log tăng trong demo, chưa tối ưu ReadIndex/snapshot | TV4/5 / TV2/3/6 | APPROVED-v1 |
| D03 — Deterministic apply | Fingerprint tuple, timestamp từ command, conflict checkpoint, audit log_index, retention request_results | TV2 / TV3/4/5 | APPROVED-v1 |
| D04 — Client recovery & history | Pending journal, nullability/result mapping, cursor theo index và refresh giữa trang | TV1 / TV2/3 | APPROVED-v1 |
| D05 — Deployment trust & timers | Baseline map peer/client và firewall được duyệt; cơ chế identity cụ thể/giá trị timeout còn là việc triển khai. senderId/CLUSTER_ID tự khai không phải authentication | TV3/6 / TV4/5 | APPROVED-v1; chi tiết vận hành còn mở |
| D06 — Phạm vi lịch sử/monitor | Baseline ticket history và log vận hành được duyệt; stream/query events trên UI cần contract bổ sung nếu triển khai | TV1/6 / TV2/3 | APPROVED-v1; mở rộng cần review riêng |

Chỉ thị chuyển trạng thái phê duyệt baseline tài liệu hiện có; không bổ sung giao thức auth, admin hoặc event stream và không tự chọn các chi tiết triển khai còn mở. Ngưỡng timeout cụ thể là cấu hình cần test, không phải cam kết SLA mới.

## 4. Sổ xác nhận và thao tác khóa

**Phê duyệt hiện hành:** người dùng yêu cầu chuyển bộ SPEC sang APPROVED-v1 ngày 29/10/2026. Bảng dưới lưu tình trạng xác nhận cá nhân tại lần rà soát ban đầu, không phải điều kiện trì hoãn chỉ thị chuyển trạng thái này.

| Thành viên | Phần review | Quyết định / ngày / bằng chứng |
|---|---|---|
| TV1 | Client, recovery, history, DTO | CHƯA CÓ XÁC NHẬN |
| TV2 | Business, SQL, fingerprint, atomic apply | CHƯA CÓ XÁC NHẬN |
| TV3 | Wire/schema/routing/identity/version migration | CHƯA CÓ XÁC NHẬN |
| TV4 | Log, replication, commit, read barrier | CHƯA CÓ XÁC NHẬN |
| TV5 | Election, term/vote, restart/recovery | CHƯA CÓ XÁC NHẬN |
| TV6 | Monitor, deployment, evidence/test plan | CHƯA CÓ XÁC NHẬN |

Lịch sử trạng thái:

1. Sau rà soát: `Draft v1` / `DESIGN-v1 / CHƯA KHÓA`; chưa ghi nhận phê duyệt.
2. Theo chỉ thị tiếp theo của người dùng: chuyển cả 9 SPEC và các ADR/nhãn thiết kế hiện hành thành `APPROVED-v1`. Giữ nguyên bản lưu lịch sử và evidence lần rà soát trước.
3. Revision phê duyệt: [manifest SHA-256](../../artifacts/spec-review/2026-10-29/approval/approved-spec-sha256.json); [kiểm tra chuyển trạng thái](../../artifacts/spec-review/2026-10-29/approval/validation.txt). Không tạo tag khi chưa có Git repository. Approval tài liệu không xóa gap code ở §2 hoặc đổi NOT_RUN thành PASS.

## 5. Kiểm tra và bằng chứng

- Môi trường: Windows PowerShell, Python, thư mục `D:\HTPT\railway-distributed-system` (phiên bản thực tế được ghi lại trong evidence).
- `rtk git status --short`: exit 1, `fatal: not a git repository`; không có SHA/tag để ghi. Sử dụng baseline và manifest SHA-256, không tạo commit giả.
- Kiểm tra tài liệu: `../../artifacts/spec-review/2026-10-29/validate_specs.py` (đã xóa) và [kết quả](../../artifacts/spec-review/2026-10-29/validation.txt). Phạm vi: file/link tồn tại, JSON mẫu parse được, số ghế/envelope/command khớp các assertion, và file ngoài phạm vi không đổi.
- [Unified diff so với bản đầu](../../artifacts/spec-review/2026-10-29/spec-review.diff), [manifest revision để review](../../artifacts/spec-review/2026-10-29/reviewed-spec-sha256.json), [baseline workspace](../../artifacts/spec-review/2026-10-29/baseline-sha256.json).
- Unit/integration/E2E Java/PostgreSQL/Raft/JavaFX: **NOT_RUN** trong task sửa tài liệu. Ma trận SPEC-07 là kế hoạch, không phải bằng chứng runtime đã đạt FR/AC.
