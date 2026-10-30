# SPEC-01 — Yêu cầu và nghiệm thu

> **Trạng thái:** APPROVED-v1 · **Nguồn:** Quản lý phạm vi/WBS Nhóm 6, ý tưởng dự án, yêu cầu giảng viên. Các mã FR/AC giữ nguyên tài liệu nhóm.

## 1. Mục tiêu

Mô phỏng bán vé cho **một chuyến `SE-001` với 10 ghế `G01`–`G10`**; cho phép nhiều quầy bán vé cùng thao tác, có dữ liệu được nhân bản trên **5 Node**, chịu lỗi có giới hạn mà không chấp nhận bán trùng.

## 2. Functional requirements (giữ nguyên mã nguồn)

| Mã | Yêu cầu | Ưu tiên | Kiểm chứng |
|---|---|---|---|
| FR01 | Hiển thị chuyến có đúng 10 ghế G01–G10 | P0 | AC01 |
| FR02 | Hiển thị trạng thái trống/đã bán | P0 | AC01, AC02 |
| FR03 | Chọn ghế, thông tin tối thiểu, xác nhận vé | P0 | AC02 |
| FR04 | Nhiều Desktop Client đồng thời | P0 | AC03 |
| FR05 | Không có hai vé hiệu lực cho cùng ghế | P0 | AC03, AC04 |
| FR06 | Bản sao dữ liệu trên 5 Node độc lập | P0 | AC05, AC08 |
| FR07 | Điều phối qua Leader và đồng thuận thống nhất | P0 | AC05, AC07 |
| FR08 | Tiếp tục ghi khi mất tối đa 2 Node và đủ đa số hợp lệ | P0 | AC05, AC06 |
| FR09 | Node hồi phục bắt kịp dữ liệu đã cam kết | P0 | AC08 |
| FR10 | Hủy vé hợp lệ, giải phóng ghế | P1 | TC-CANCEL-* |
| FR11 | Tra cứu lịch sử vé/giao dịch/sự kiện | P1 | TC-HISTORY-* |
| FR12 | Giám sát cụm 5 Node và tạo tình huống lỗi | P1 | AC07–AC09 |

**P0/P1** là nhãn ưu tiên triển khai trong bộ SPEC; không thay đổi việc FR10–FR12 được ghi là **bổ sung** trong hồ sơ phạm vi nhóm.

## 3. Use cases

### UC-01 Xem ghế

- **Tác nhân:** nhân viên tại JavaFX Client.
- **Đầu vào:** `tripId=SE-001`.
- **Thành công:** đúng 10 mã ghế, mỗi ghế một trạng thái server đã đọc; kèm thông tin mức độ mới/cập nhật.
- **Lỗi:** mất Leader, timeout, dữ liệu không xác thực được; hiển thị cảnh báo, **không mô tả dữ liệu cache là mới nhất**.

### UC-02 Đặt vé

- **Tiền điều kiện:** ghế thuộc G01–G10, tên hành khách hợp lệ theo quy tắc dữ liệu.
- **Luồng chính:** chọn ghế → nhập thông tin → gửi `BOOK_TICKET` có `requestId` → Leader đưa lệnh vào Raft → commit → apply → xác nhận kết quả → Client làm mới ghế.
- **Xung đột:** ghế đã bán tại thời điểm apply → kết quả từ chối có lý do; Client cập nhật lại.
- **Mất phản hồi:** giữ `requestId`, tra cứu `GET_REQUEST_RESULT` và retry **cùng ID/cùng payload** nếu cần; không suy luận giao dịch thất bại.
- **Hậu điều kiện:** tối đa một vé hiệu lực trên mỗi `(tripId,seatId)`.

### UC-03 Hủy vé (bổ sung)

Xác định vé đang hiệu lực → gửi `CANCEL_TICKET` với `requestId` → cam kết & apply → chuyển vé sang `CANCELLED`, trả ghế `AVAILABLE`, ghi lịch sử. Hủy một vé đã hủy phải trả kết quả nghiệp vụ rõ ràng; không xóa lịch sử.

### UC-04 Quan sát và mô phỏng lỗi

Monitor lấy trạng thái từng Node, Leader, term, log/commit/applied index; người vận hành dừng/khởi động lại Node thực sự hoặc áp dụng fault injection trong môi trường thử nghiệm; kiểm tra quorum và catch-up. **Monitor không phải thành viên bỏ phiếu thứ sáu.**

## 4. Non-functional requirements

| Nhóm | Cam kết / tiêu chí cần test |
|---|---|
| Safety | Không có 2 vé `ACTIVE` cho cùng một ghế; không báo xác nhận ghi khi mất majority |
| Consistency | Thứ tự apply nhất quán theo log đã commit; Node hồi phục không ghi đè commit |
| Durability | Sau khi trả thành công, khởi động lại tiến trình không mất vé đã xác nhận |
| Availability có điều kiện | Có thể tiếp tục khi còn **ít nhất 3/5 Node** phối hợp đúng luật Raft; không cam kết luôn sẵn sàng nếu mạng trễ/phân vùng |
| Idempotency | Cùng `requestId`/payload lặp lại không tạo hiệu ứng khác; payload khác cùng ID bị từ chối |
| Usability | Phân biệt `SUCCESS`, `REJECTED`, `PENDING/UNKNOWN`, `OFFLINE`, `STALE` |
| Observability | Log theo Node, requestId, term, index, commit, state-machine apply; dữ liệu quan sát không được ngụy tạo |
| Deployment | Chạy được localhost và LAN theo cùng codebase; JavaFX Desktop, không Web Service |

Không đưa ra SLA độ trễ/throughput hoặc mức khả dụng phần trăm vì tài liệu nguồn không chốt số liệu; khi có benchmark, nhóm phải định nghĩa môi trường và ngưỡng riêng.

`SUCCESS` trong ngôn ngữ UI/AC là kết quả nghiệp vụ thành công (`outcome=CONFIRMED` hoặc lookup `state=APPLIED_SUCCESS` theo SPEC-03), không phải wire enum mới. UNKNOWN/PENDING không phải thất bại đã xác nhận. Quorum luôn 3 trong tập 5 voter cấu hình, không tính lại theo số Node online.

## 5. Acceptance criteria (giữ mã AC01–AC10)

| Mã | Bối cảnh | Điều kiện PASS |
|---|---|---|
| AC01 | Khởi tạo chuyến | Chính xác 10 ghế G01–G10 |
| AC02 | Đặt ghế trống | Vé được xác nhận hợp lệ sau commit+apply, ghế chuyển trạng thái |
| AC03 | Hai Client đặt cùng ghế | Tối đa **một** yêu cầu thành công; không có hai vé ACTIVE |
| AC04 | Bán hết 10 ghế | Không thể cấp vé ACTIVE thứ 11 nếu chưa có hủy hợp lệ |
| AC05 | Mất 1–2 Node | Ghi mới vẫn có thể commit khi còn majority và Leader/log hợp lệ |
| AC06 | Chỉ 2/5 Node phối hợp | **Không xác nhận** ghi mới |
| AC07 | Leader ngừng hoạt động | Bầu Leader mới hợp lệ, tiếp tục khi còn quorum |
| AC08 | Node cũ quay lại | Catch-up bản ghi commit, dữ liệu khớp sau đồng bộ |
| AC09 | Mạng phân vùng 3–2 | Phía thiểu số không xác nhận ghi mới; không split-brain commit |
| AC10 | Client mất phản hồi | Có thể truy vấn và retry cùng requestId, không cấp vé trùng |

Ngoài AC gốc: `TC-CANCEL-*`, `TC-HISTORY-*`, `TC-UI-*` được định nghĩa ở [07-TESTING.md](07-TESTING.md), không đánh tráo chúng thành AC11 từ tài liệu nhóm.

AC03/AC04 có assertion safety “tối đa một/không vé thứ 11”; khi test trong cụm khỏe và ghế trống phải kiểm tra thêm có một booking thành công, tránh hệ thống từ chối mọi request vẫn được coi đạt chức năng. AC06/AC09 cấm **commit mới** khi chỉ còn 2/5; response trễ hoặc lookup lại một lệnh đã commit trước lúc mất quorum không phải bằng chứng commit mới. Phải đối chiếu term/index và thời điểm inject lỗi theo SPEC-07.

## 6. Luật quyết định nghiệp vụ

- Không đặt vượt ngoài G01–G10 hoặc chuyến khác `SE-001`.
- Trạng thái `SELECTED` chỉ là lựa chọn **cục bộ ở UI**, không phải khóa ghế toàn cụm.
- Hai Client có thể cùng chọn ghế trống; chỉ lệnh được apply theo thứ tự log mới quyết định bên thắng.
- Hủy và đặt xen kẽ phải tạo lịch sử nhất quán; vé đã hủy không còn hiệu lực.
- Khi Client không biết chắc kết quả, UI phải cho tra cứu/truy vấn lại, không tự tạo `requestId` mới cho cùng ý định.

## 7. Ngoài phạm vi và giả định

- Không xây website, API HTTP/REST/SOAP, không thanh toán thật, không vé điện tử công cộng, không mở nhiều chuyến/ngày.
- Danh sách thành viên Raft cố định 5 Node; không triển khai dynamic cluster membership.
- Dữ liệu hành khách chỉ phục vụ demo, không dùng dữ liệu nhạy cảm thật.
- Yêu cầu bảo mật production (TLS, PKI, phân quyền thật, audit luật định) không được coi là đã hoàn thiện từ bản demo LAN; dữ liệu thử phải được giới hạn.

## 8. Quy trình thay đổi yêu cầu

Mọi thay đổi tác động đến FR/AC hoặc invariants phải ghi lý do, thay đổi phạm vi, module chịu ảnh hưởng, owner, test hồi quy, và được review trước khi cập nhật SPEC. Bổ sung ngoài phạm vi không được ngầm thành P0.
