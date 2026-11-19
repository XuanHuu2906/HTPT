# BẢNG MA TRẬN ĐỐI CHIẾU MÃ LỖI VÀ TRẠNG THÁI (TASK TV1-D19)

> **Dự án:** Railway Distributed System — Nhóm 6  
> **Thành viên:** TV1 (Khoa - Phụ trách JavaFX Desktop Client)  
> **Tài liệu căn cứ:** `docs/specs/03-CONTRACTS.md` (§4.2, §4.4, §7), `docs/specs/05-BOOKING-DATABASE.md` (§1, §3), `AGENTS.md`.  
> **Ngày lập:** 19/11/2026.

---

## 1. Ma trận đối chiếu tổng thể SPEC-03 ↔ Shared ↔ Client Presentation ↔ Raft Semantics

| Mã (Code) | Vị trí trường trong Protocol | Định nghĩa trong `shared` | Ánh xạ `ErrorPresentationMapper` | Trạng thái UI `BookingViewModel` | Ngữ nghĩa Raft & Lệnh đã commit chưa? | Hành vi Client khuyến nghị |
|---|---|---|---|---|---|---|
| **`SEAT_ALREADY_BOOKED`** | `reasonCode` trong `BOOK_TICKET_RESPONSE(outcome=REJECTED)` hoặc `GET_REQUEST_RESULT_RESPONSE` | Hằng số `BookTicketResponseDto.REASON_SEAT_ALREADY_BOOKED` | `kind: REJECTED`, `style: booking-status-rejected`, `retryable: false`, `needsSeatRefresh: true`, `showRequestId: false` | `UiState.REJECTED`, ghế chuyển `BOOKED`, pending journal chuyển `RESOLVED`, kích hoạt `refreshNow()` | **ĐÃ COMMIT + APPLY**. Quyết định từ chối nghiệp vụ dứt khoát do ghế đã có vé ACTIVE trước đó. Không bao giờ commit vé mới cho request này. | Hiển thị từ chối dứt khoát. KHÔNG retry cùng ghế. Giải phóng pending. Yêu cầu người dùng chọn ghế khác trên sơ đồ. |
| **`TIMEOUT_UNKNOWN`** | `reasonCode` trong `BOOK_TICKET_RESPONSE(outcome=UNKNOWN)` hoặc sinh từ Client khi read timeout / kết nối đứt sau khi ghi byte | Hằng số `BookTicketResponseDto.REASON_TIMEOUT_UNKNOWN` | `kind: UNKNOWN`, `style: booking-status-unknown`, `retryable: true`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.UNKNOWN`, ghế chuyển `UNKNOWN` (viền nét đứt), pending journal giữ `SENT_UNRESOLVED`, hiển thị `requestId` | **CHƯA RÕ (Lệnh CÓ THỂ đã hoặc đang được commit+apply tại Leader)**. Server hết thời hạn chờ kết quả apply hoặc client bị mất kết nối sau khi gửi lệnh đi. | **TUYỆT ĐỐI KHÔNG coi là thành công hay thất bại**. KHÔNG dùng từ cấm ("thành công", "thất bại", "bị từ chối", "đã đặt"). Giữ nguyên `requestId` và payload; hiển thị nút "Tra cứu kết quả" (`GET_REQUEST_RESULT`) và nút "Gửi lại" (cùng `requestId`). |
| **`NO_QUORUM`** | `errorCode` trong `ERROR_RESPONSE` (khi request là BOOK_TICKET hoặc CANCEL_TICKET) | Trường `errorCode` trong `ErrorResponseDto` | `kind: ERROR`, `style: status-error`, `retryable: true`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.ERROR` (hoặc cảnh báo cụm mất quorum), ghế chuyển `UNKNOWN`, pending journal giữ `SENT_UNRESOLVED`, giữ nguyên `currentRequestId` | **CHƯA RÕ (Leader không đạt đa số voter 3/5 nên chưa thể cam kết an toàn)**. Lệnh có thể sẽ được commit sau khi cụm phục hồi và bầu Leader mới, hoặc có thể bị hủy nếu term cũ bị đè. | Không báo thành công, không báo thất bại dứt khoát. Giữ nguyên `requestId`. Giữ backoff kết nối (không bắn lệnh dồn dập). Tra cứu hoặc thử lại với cùng `requestId` sau khi cụm có Leader ổn định. |
| **`NOT_LEADER`** | `errorCode` trong `ERROR_RESPONSE` (kèm `leaderHint`, `term`) | Trường `errorCode` trong `ErrorResponseDto` | `kind: ERROR`, `style: status-error`, `retryable: true`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.ERROR` (hoặc chuyển hướng tự động qua `LeaderAwareBookingClient`), cập nhật `leaderHint` lên `ClusterConnectionViewModel` | **CHẮC CHẮN CHƯA COMMIT trên node này** (nếu là Follower nhận nhầm). Nhưng nếu là Leader cũ bị giáng cấp sau split-brain, lệnh trước đó có thể đã gửi. | `LeaderAwareBookingClient` tự động chuyển hướng theo `leaderHint` (giữ nguyên `requestId`, tối đa `maxRedirects`). Nếu hết lượt chuyển hướng thì báo `NOT_LEADER_UNRESOLVED`, giữ pending để tra cứu lại. |
| **`READ_UNAVAILABLE`** | `errorCode` trong `ERROR_RESPONSE` (khi gọi `GET_SEATS`, `GET_REQUEST_RESULT` hoặc query không vượt qua read barrier) | Trường `errorCode` trong `ErrorResponseDto` | `kind: UNKNOWN`, `style: booking-status-unknown`, `retryable: true`, `needsSeatRefresh: true`, `showRequestId: false` | `UiState.UNKNOWN`, toàn bộ 10 ghế hiển thị `UNKNOWN` với nhãn `[Chưa xác minh]`, kích hoạt làm mới khi Leader sẵn sàng | **Không liên quan đến ghi**. Leader hoặc node không chứng minh được tính hiện hành của dữ liệu đọc (chưa hoàn thành lease/heartbeat quorum). | Không hiển thị kết quả như dữ liệu mới nhất. Đánh dấu giao diện chưa xác minh, chờ cụm ổn định rồi đọc lại. |
| **`REQUEST_ID_CONFLICT`** | `errorCode` trong `ERROR_RESPONSE` | Trường `errorCode` trong `ErrorResponseDto` | `kind: ERROR`, `style: status-error`, `retryable: false`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.ERROR`, thông báo xung đột `requestId`, không đổi trạng thái ghế | **Kết quả gốc ĐÃ COMMIT/ĐANG XỬ LÝ với payload ban đầu**. Lần gửi này bị từ chối trước khi append vì cùng `requestId` nhưng khác payload. | DỪNG LẠI. Không tự ý đổi `requestId` và chạy lại cùng ý định. Nếu người dùng muốn đổi thông tin vé (họ tên/ghế khác), phải tạo giao dịch mới với `requestId` mới. |
| **`INVALID_REQUEST`** | `errorCode` trong `ERROR_RESPONSE` (hoặc `reasonCode` trong `BOOK_TICKET_RESPONSE` nếu lỗi định dạng khi apply) | Hằng số `BookTicketResponseDto.REASON_INVALID_REQUEST`, `ErrorResponseDto.invalidRequest` | `kind: ERROR`, `style: status-error`, `retryable: false`, `needsSeatRefresh: false`, `showRequestId: false` | `UiState.ERROR`, thông báo lỗi schema/dữ liệu đầu vào, không đổi ghế | **CHẮC CHẮN CHƯA APPEND VÀO RAFT LOG** (đối với lần gửi này). Lỗi cú pháp, thiếu trường, sai định dạng ID trước khi append. | Yêu cầu người dùng sửa lại thông tin đầu vào hợp lệ. Không tự động retry nguyên trạng vô hạn. |
| **`INTERNAL_ERROR`** | `errorCode` trong `ERROR_RESPONSE` | Trường `errorCode` trong `ErrorResponseDto` | `kind: ERROR`, `style: status-error`, `retryable: true`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.ERROR`, thông báo lỗi máy chủ chưa phân loại | **CHƯA RÕ**. Lỗi nội bộ không chứng minh lệnh trước đó chưa commit. | Không tự coi là giao dịch thất bại. Tra cứu lại kết quả bằng `requestId` trước khi thực hiện hành động khác. |
| **`UNSUPPORTED_VERSION`** | `errorCode` trong `ERROR_RESPONSE` | Trường `errorCode` trong `ErrorResponseDto` | `kind: ERROR`, `style: status-error`, `retryable: false`, `needsSeatRefresh: false`, `showRequestId: false` | `UiState.ERROR`, thông báo không tương thích phiên bản protocol | **CHẮC CHẮN CHƯA XỬ LÝ**. Node từ chối ngay ở tầng transport handshake / framing. | Dừng gửi lệnh và thông báo người dùng nâng cấp ứng dụng. |
| **`TICKET_NOT_ACTIVE`** | `reasonCode` trong `CANCEL_TICKET_RESPONSE(outcome=REJECTED)` | Thuộc tính `reasonCode` trong Cancel response | `kind: REJECTED`, `style: booking-status-rejected`, `retryable: false`, `needsSeatRefresh: true`, `showRequestId: false` | `UiState.REJECTED`, thông báo từ chối hủy do vé không còn hiệu lực | **ĐÃ COMMIT + APPLY**. Quyết định từ chối hủy vé được ghi nhận bền vững. | Thông báo từ chối hủy vé. Làm mới danh sách vé. |
| **`FAILED_NOT_SENT`** | Phân loại lỗi mạng nội bộ Client (khi socket `connect()` thất bại trước khi gửi byte nào) | Client-side exception / `ClientErrorKind.FAILED_NOT_SENT` | `kind: DISCONNECTED`, `style: status-disconnected`, `retryable: true`, `needsSeatRefresh: false`, `showRequestId: true` | `UiState.DISCONNECTED`, **GHẾ KHÔNG ĐỔI** (không chuyển sang `UNKNOWN`), giữ pending journal, cho thử lại cùng `requestId` | **CHẮC CHẮN CHƯA RỜI KHỎI MÁY KHÁCH**. Không có byte nào được ghi lên socket stream. Cụm máy chủ chưa hề biết về request này. | Thông báo mất kết nối tới máy chủ. Cho phép người dùng thử lại với cùng `requestId`. Ghế đang chọn được giữ nguyên. |

---

## 2. Danh sách đối chiếu và các điểm lệch cần chuẩn hóa (Trước / Sau sửa)

1. **Chuỗi tự chế `SEAT_SOLD`:**
   - *Trước:* SPEC-03 và `BookTicketResponseDto` quy định rõ mã là `SEAT_ALREADY_BOOKED`. Mã `SEAT_SOLD` không tồn tại trong SPEC-03.
   - *Sau:* `ErrorPresentationMapper` tiếp tục tuân thủ nghiêm ngặt quy tắc: TUYỆT ĐỐI KHÔNG dùng `SEAT_SOLD`. Nếu nhận được chuỗi `SEAT_SOLD`, hệ thống rơi vào nhánh lỗi không nhận diện (`mapUnrecognizedCode`), không tự ý biến thành ghế đã đặt.
2. **Phân biệt mất mạng trước khi gửi (`FAILED_NOT_SENT`) vs sau khi gửi (`UNKNOWN`):**
   - *Trước:* Trong `BookingViewModel.handleException`, mọi ngoại lệ `IOException` (bao gồm `ConnectException` khi máy chủ offline) đều bị chuyển thành `UiState.DISCONNECTED` và gọi `updateSeatStatus(targetSeat, SeatViewModel.SeatStatus.UNKNOWN)` làm ghế chuyển sang trạng thái `UNKNOWN`.
   - *Sau:* 
     - Nếu lỗi kết nối xảy ra TRƯỚC khi ghi byte (`FAILED_NOT_SENT` / `ConnectException`): `UiState.DISCONNECTED`, ghế **GIỮ NGUYÊN** (không chuyển sang `UNKNOWN`), cho phép thử lại với cùng `requestId`.
     - Nếu lỗi kết nối / read timeout xảy ra SAU khi ghi byte (`SocketTimeoutException`, `EOFException`, connection reset): `SocketBookingClient` trả về `BookTicketResponseDto(UNKNOWN, TIMEOUT_UNKNOWN)`, UI chuyển sang `UiState.UNKNOWN`, ghế chuyển sang `SeatStatus.UNKNOWN` (viền nét đứt), pending journal giữ `SENT_UNRESOLVED`.
3. **Ngữ nghĩa `NO_QUORUM`:**
   - *Quy tắc:* `NO_QUORUM` trả về từ Node cho biết Leader không thể cam kết ghi an toàn (thiếu quorum 3/5). Lệnh có thể sẽ được commit sau khi cụm phục hồi, hoặc có thể bị hủy.
   - *Giao diện:* Tuyệt đối không coi là thành công, không coi là thất bại dứt khoát. Giữ nguyên `requestId` và payload trong journal, hiển thị `requestId` kèm nút Tra cứu và Gửi lại, áp dụng backoff không gửi dồn dập.
4. **Không parse nội dung chuỗi lỗi:**
   - Toàn bộ logic giao diện dựa 100% vào `outcome`, `errorCode`, `reasonCode`, `clientErrorKind`. Tuyệt đối không dùng `message.contains(...)` từ server để suy đoán logic.
5. **Cấm các từ ngữ gây hiểu nhầm trong trạng thái `UNKNOWN`:**
   - Trong mọi thông báo hiển thị cho `UNKNOWN` / `TIMEOUT_UNKNOWN`, tuyệt đối không xuất hiện các từ: `"thành công"`, `"thất bại"`, `"bị từ chối"`, `"đã đặt"`.

---

## 3. Danh sách mẫu phản hồi: Thật (REAL) vs Giả lập (FIXTURE / FAKE)

| Tình huống phản hồi | Phân loại | Nguồn gốc / Trạng thái hiện tại | Ghi chú |
|---|---|---|---|
| `BOOK_TICKET_RESPONSE(outcome=CONFIRMED)` | FIXTURE | `FakeNodeServer` / SPEC-03 §4.2 | Đã test trong harness D10, D17, D18 |
| `BOOK_TICKET_RESPONSE(outcome=REJECTED, reasonCode=SEAT_ALREADY_BOOKED)` | FIXTURE | `FakeNodeServer` / SPEC-03 §4.2 | Đã test trong harness D18 tranh ghế |
| `BOOK_TICKET_RESPONSE(outcome=UNKNOWN, reasonCode=TIMEOUT_UNKNOWN)` | FIXTURE | `FakeNodeServer` / SPEC-03 §4.2 | Đã test trong D17 fake delay |
| `ERROR_RESPONSE(NO_QUORUM)` | FIXTURE | `FakeNodeServer` / SPEC-03 §7 | Kiểm thử trong `BookingErrorFlowTest` |
| `ERROR_RESPONSE(NOT_LEADER)` | FIXTURE | `FakeNodeServer` / SPEC-03 §7 | Đã test redirect trong D12 |
| `ERROR_RESPONSE(READ_UNAVAILABLE)` | FIXTURE | `FakeNodeServer` / SPEC-03 §7 | Kiểm thử trong `BookingErrorFlowTest` |
| `ERROR_RESPONSE(REQUEST_ID_CONFLICT)` | FIXTURE | `FakeNodeServer` / SPEC-03 §7 | Kiểm thử trong `BookingErrorFlowTest` |
| `ERROR_RESPONSE(INVALID_REQUEST)` | FIXTURE | `FakeNodeServer` / SPEC-03 §7 | Kiểm thử trong `BookingErrorFlowTest` |
| Mẫu phản hồi thật từ cụm 5 Node đang chạy | **NOT_RUN / BLOCKED** | Chờ cụm 5 Node chạy thật từ TV2/TV3/TV4/TV5/TV6 | Cụm dev hiện đang DOWN (ports 9101-9105 không mở). Cần TV6 hỗ trợ khởi chạy và tạo fault injection bằng `scripts/cluster-fault.ps1`. |
