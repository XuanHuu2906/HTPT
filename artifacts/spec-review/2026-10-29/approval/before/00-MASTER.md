# MASTER SPEC — Hệ thống bán vé tàu cao tốc phân tán

> **Mã:** SPEC-00 · **Phiên bản tài liệu:** Draft v1.0 · **Ngày:** 29/10/2026  
> **Phạm vi:** Nhóm 6, môn Hệ thống phân tán · **Tình trạng:** Đặc tả đề xuất để nhóm review, **không phải xác nhận đã code/test**.

## 1. Mục đích và cách sử dụng

Đây là **điểm đọc đầu tiên** của mọi thành viên và AI Coding Agent. File này định nghĩa mục tiêu, phạm vi, nguyên tắc và thứ tự tham chiếu; đặc tả chi tiết nằm ở các file chuyên trách. Không đưa bản sao các DTO hoặc quy tắc Raft vào file này.

### Danh mục tài liệu

| File | Chủ đề | Chủ sở hữu nội dung chính |
|---|---|---|
| [01-REQUIREMENTS.md](01-REQUIREMENTS.md) | FR/NFR, AC01–AC10, phạm vi | Cả nhóm/TV6 điều phối |
| [02-ARCHITECTURE.md](02-ARCHITECTURE.md) | Kiến trúc, ranh giới module, luồng xử lý | TV3 + TV4 + TV5 |
| [03-CONTRACTS.md](03-CONTRACTS.md) | Giao thức TCP, DTO, thông điệp, mã lỗi | TV3; các bên cùng review |
| [04-RAFT.md](04-RAFT.md) | Bầu cử, nhật ký, quorum, commit, phục hồi | TV4 + TV5 |
| [05-BOOKING-DATABASE.md](05-BOOKING-DATABASE.md) | Nghiệp vụ vé, schema, atomic apply, idempotency | TV2 |
| [06-DESKTOP-UI.md](06-DESKTOP-UI.md) | JavaFX bán vé, Cluster Monitor | TV1 + TV6 |
| [07-TESTING.md](07-TESTING.md) | Test case, ma trận nghiệm thu, bằng chứng | Cả nhóm/TV6 tổng hợp |
| [08-DEPLOYMENT.md](08-DEPLOYMENT.md) | Docker hai chế độ, LAN, runbook demo | TV6 + TV3 + TV2 |
| [../../AGENTS.md](../../AGENTS.md) | Quy tắc làm việc cho coding agent | Cả nhóm |

## 2. Nguồn yêu cầu và cấp độ quyết định

**[SOURCE] — Đã được mô tả trong hồ sơ nhóm:**

- Đề bài giảng viên: mô phỏng bán vé tàu cao tốc, **10 ghế**, xây dựng **bản sao dữ liệu hoặc cụm dữ liệu** nhằm tăng khả năng hoạt động khi sự cố.
- Định hướng nhóm: một chuyến mẫu `SE-001`, ghế `G01`–`G10`, **5 Node** lưu dữ liệu độc lập và **6 thành viên**; không phải con số Node bắt buộc trong đề gốc.
- Công nghệ: **Java + JavaFX Desktop + PostgreSQL + Java TCP Socket + Maven**. Raft là hướng đồng thuận nhóm đã đề xuất; quorum của 5 Node là **3/5**.
- Desktop Client/Monitor **không phải website**. **Cấm Web Service, REST, HTTP API, SOAP** trong giao tiếp ứng dụng.
- Ở nhà: 5 Backend + 5 PostgreSQL qua Docker Compose, JavaFX trực tiếp trên Windows. Demo: 5 laptop chứa Node/DB, laptop 6 giám sát; tất cả dùng LAN.
- Các yêu cầu `FR01`–`FR12`, tiêu chí `AC01`–`AC10` lấy từ tài liệu Quản lý phạm vi/WBS.

**[DESIGN-v1] — Chi tiết do bộ SPEC này bổ sung, cần nhóm chấp thuận trước khi khóa API:**

- Envelope TCP JSON `protocolVersion=1`, prefix độ dài 4 byte big-endian; tên trường `camelCase`, bộ mã lỗi, timeout mẫu.
- Phân loại trạng thái yêu cầu, cấu trúc bảng CSDL cụ thể, chiến lược read barrier, quy ước response và cấu hình mẫu.
- Những chi tiết đó là **thiết kế triển khai đề xuất** chứ không được trình bày như nội dung đã được duyệt từ tài liệu nguồn hoặc đã chạy thành công.

**Quản lý trạng thái sau rà soát:** [SPEC-REVIEW-01](../reviews/SPEC-REVIEW-01.md) ghi vấn đề, lựa chọn thiết kế, lệch code và reviewer. `Draft v1`, `DESIGN-v1 / CHƯA KHÓA` đều là chưa phê duyệt. Chỉ đổi thành **APPROVED-v1** cho revision được TV1–TV6 xác nhận; giữ nguyên lịch sử trạng thái trong hồ sơ. Approval thiết kế và mức triển khai/test là hai trạng thái độc lập; không dùng APPROVED-v1 để khẳng định đã PASS AC.

## 3. Các bất biến không được vi phạm

1. `SE-001` có đúng 10 ghế: `G01`–`G10`.
2. **Không có hai vé đang hiệu lực cho cùng một ghế của cùng chuyến**.
3. Client và Monitor **không ghi trực tiếp PostgreSQL**.
4. Mỗi Backend Node có PostgreSQL/storage riêng; không dùng một DB chung để giả làm 5 Node.
5. Chỉ **Leader hợp lệ** điều phối lệnh ghi; Follower không tự xác nhận vé.
6. Kết quả `CONFIRMED`/hủy thành công chỉ trả sau khi lệnh **commit đúng Raft và được apply bền vững** tại Leader.
7. Khi cụm **không đạt đa số** để commit hợp lệ, không được xác nhận giao dịch ghi mới.
8. Retry cùng `requestId` và cùng nội dung không tạo thêm hiệu ứng nghiệp vụ; trùng `requestId` khác nội dung bị từ chối.
9. Node phục hồi phải catch-up phần log đã cam kết; log cũ không được ghi đè trạng thái đã commit.
10. Timeout/mất phản hồi là **chưa biết kết quả**, không được đổi thành `FAILED` hay `SUCCESS` bằng suy đoán.
11. Không tự nhận đã PASS test khi chưa chạy và lưu minh chứng.

## 4. Phạm vi

**Cốt lõi/P0:** 10 ghế, hiển thị, đặt vé, nhiều Client, chống bán trùng, 5 Node, Raft election/log/commit, phục hồi, mất quorum an toàn, retry an toàn.  
**Mở rộng có trong đề xuất/P1:** hủy vé, lịch sử giao dịch, Cluster Monitor, mô phỏng lỗi, kịch bản demo.  
**Ngoài phạm vi:** website, Web Service, REST/HTTP API, thanh toán thật, chuyến tàu thực tế, nhiều tuyến, nhiều ngày, phân hạng ghế, giữ chỗ có thời hạn, thống kê doanh thu nâng cao, Kubernetes, hệ thống Internet công khai. Snapshot/log compaction là mở rộng chưa thuộc baseline v1; chỉ bổ sung sau khi có thay đổi SPEC/contract được review.

## 5. Thuật ngữ chuẩn

- **Client:** JavaFX Desktop nhân viên bán vé, có thể chạy trên cả 6 máy.
- **Node:** một tiến trình Backend Java với PostgreSQL/storage riêng; chỉ Node 1–5 là Raft voter.
- **Leader/Follower/Candidate:** vai trò Raft **động**, không hardcode theo laptop.
- **Commit:** bản ghi được cam kết theo các quy tắc Raft, không đơn thuần là ghi vào 3 database.
- **Apply:** cập nhật state machine nghiệp vụ từ bản ghi đã commit, transaction bền vững tại một Node.
- **`requestId`:** định danh ổn định của **ý định nghiệp vụ**, phải được giữ nguyên khi retry.
- **`messageId`:** định danh một bản tin truyền qua TCP; có thể thay đổi theo lần gửi lại.
- **Kết quả chưa xác định (`UNKNOWN`):** phía Client chưa biết giao dịch đã commit/apply hay chưa.

## 6. Thứ tự đọc và giải quyết xung đột

1. Yêu cầu gốc của giảng viên và các ràng buộc đã thống nhất trong hồ sơ nhóm.
2. `01-REQUIREMENTS.md` cho phạm vi và điều kiện nghiệm thu.
3. `03-CONTRACTS.md` cho tên bản tin/DTO/error; `04-RAFT.md` cho điều kiện đồng thuận; `05-BOOKING-DATABASE.md` cho quyết định nghiệp vụ.
4. Các SPEC kiến trúc/UI/testing/deployment viện dẫn các nguồn trên.
5. Nếu hai SPEC không khớp: **dừng phần code phụ thuộc**, ghi issue/ADR, yêu cầu chủ sở hữu và bên liên quan review; sửa **nguồn chuẩn** trước, rồi mới cập nhật tham chiếu. Không tự lựa chọn ngầm.

## 7. Luồng phát triển theo mốc

- **G1 — Contracts nền tảng:** Maven multi-module, DTO, shared protocol, DB migration sơ bộ, stub build được.
- **G2 — MVP 1 Node:** JavaFX → TCP → Booking → PostgreSQL, test một nút, chưa tự nhận là phân tán.
- **G3 — Cụm 5 Node:** bầu cử Leader và AppendEntries, log độc lập, chưa báo booking distributed PASS nếu chưa commit/apply.
- **G4 — Đúng đắn 5 Node:** majority commit, state machine, concurrent booking, idempotency.
- **G5 — Chịu lỗi & bàn giao:** mất Leader, mất 1–2 Node, mất quorum, phân vùng, catch-up, demo LAN.

Các mốc trên mô tả **trình tự phụ thuộc**; bảng kế hoạch 30 ngày (180 nhiệm vụ) vẫn là nguồn theo dõi công việc, không được thay thế bằng nội dung SPEC.

## 8. Chứng minh hoàn thành

`DONE` chỉ khi có: mã nguồn đúng phạm vi, build/test đã chạy (lệnh + kết quả), kịch bản AC liên quan, log có `requestId`/Node/term/index, và minh chứng thực tế. `NOT_RUN`, `BLOCKED`, `FAIL` là trạng thái hợp lệ; tuyệt đối không làm đẹp báo cáo bằng cách tự chuyển thành `PASS`.

## 9. Hồ sơ nền của bộ SPEC

Tài liệu đầu vào: `Yều-cầu-đồ-án.txt`; `Mo_ta_y_tuong_do_an_ban_ve_tau_phan_tan_nhom_6.docx`; `Tech_Stack_Do_An_Ban_Ve_Tau_Phan_Tan_5_Node_Nhom_6.docx`; `Quan_ly_pham_vi_Nhom_6_WBS_4_Level.docx`; `Kich_ban_demo_he_thong_ban_ve_tau_phan_tan_6_laptop_5_node.docx`; `Phuong_An_Trien_Khai_Docker_2_Che_Do_Nhom_6.docx`; `Canh_bao_xung_dot_phu_thuoc_30_ngay_Nhom_6.docx`; `Ke_hoach_30_ngay_chi_tiet_180_nhiem_vu_Nhom_6.xlsx`.
