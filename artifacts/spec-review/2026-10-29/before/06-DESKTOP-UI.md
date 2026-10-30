# SPEC-06 — JavaFX Desktop Client và Cluster Monitor

> **Trạng thái:** Draft v1 · **Owner:** TV1 (Client), TV6 (Monitor). Không có trang web, WebView làm frontend hay HTTP API.

## 1. Hai ứng dụng Desktop

**Ứng dụng bán vé:** JavaFX + FXML + CSS, cho nhân viên thấy `SE-001`, trạng thái 10 ghế, form hành khách, nút đặt/hủy, tra cứu kết quả khi mất phản hồi. Có thể chạy 1–6 phiên trên các máy khác nhau.

**Cluster Monitor:** JavaFX Desktop hiển thị Node 1–5, Leader/term/log, kết nối, sự kiện và fault simulation trong môi trường demo. Monitor chạy trên Laptop 6 khi bảo vệ nhưng có thể chạy tại máy phát triển.

## 2. Layout đề xuất Client

```text
+--------------------------------------------------------------------+
| SE-001  | Kết nối: Connected/Recovering/Offline | Leader: node-X   |
+--------------------------------------------------------------------+
|  [G01] [G02] [G03] [G04] [G05]                                     |
|  [G06] [G07] [G08] [G09] [G10]                                     |
|  Legend: Available / Booked / Selected locally / Unknown/Refreshing |
+----------------------------------+---------------------------------+
| Ghế đang chọn: G05               | Họ tên khách: [................] |
| [Đặt vé] [Làm mới] [Tra requestId] | Kết quả: Success/Rejected/...   |
+----------------------------------+---------------------------------+
| Vé / lịch sử (P1) | log thao tác gần nhất                             |
+--------------------------------------------------------------------+
```

Màu sắc phải có nhãn/ký hiệu đi kèm, không dựa duy nhất vào màu. Ghế `SELECTED_LOCAL` chỉ chọn ở một Client, không được hứa ghế đã được khóa.

## 3. Trạng thái hiển thị

| UI state | Khi nào | Hành vi bắt buộc |
|---|---|---|
| `AVAILABLE` | Đọc an toàn cho biết ghế trống | Cho chọn; cần xác nhận lại khi BOOK |
| `BOOKED` | Có vé ACTIVE | Hiển thị đã bán; không chấp nhận đặt lại ngay |
| `SELECTED_LOCAL` | Người dùng click ghế trong cửa sổ | Không gửi lock và không ghi DB |
| `SUBMITTING` | Đã gửi request, chờ kết quả | Khóa nút double click cùng thao tác, giữ requestId |
| `CONFIRMED` | Response xác nhận đã apply | Hiện vé và cập nhật ghế |
| `REJECTED` | Business rejected hoặc validation rõ ràng | Hiện mã lỗi, gợi ý chọn ghế khác |
| `UNKNOWN` | TCP timeout/mất phản hồi/chưa có read barrier | Cảnh báo “Chưa xác định kết quả”; hỗ trợ tra cứu cùng requestId |
| `OFFLINE` | Không kết nối được Node nào | Không thông báo đã đặt; giữ yêu cầu đang dang dở |
| `STALE` | Dữ liệu trên màn đã cũ/chưa xác thực | Đánh dấu rõ; không trình bày như số ghế mới nhất |

## 4. Luồng người dùng đặt vé

1. Mở Client, tải ghế bằng `GET_SEATS` (khi chưa đọc được dữ liệu đáng tin cậy, hiển thị placeholder/status warning).
2. Nhân viên chọn ghế và nhập `passengerName` dùng cho demo.
3. Khi nhấn **Đặt vé**, sinh một `requestId` duy nhất, lưu vào trạng thái thao tác cho đến khi xác định kết quả; gửi `BOOK_TICKET`.
4. Nếu Node trả `NOT_LEADER`, kết nối `leaderHint` nếu có, retry **cùng requestId và payload**.
5. Nếu `CONFIRMED`, hiển thị `ticketId`, chuyển ghế sang `BOOKED` sau refresh; nếu `REJECTED`, hiển thị lỗi nghiệp vụ.
6. Nếu `TIMEOUT_UNKNOWN`/disconnect, hiển thị `UNKNOWN`, cho `GET_REQUEST_RESULT` hoặc retry cùng ID. **Không** tự sinh requestId mới bằng nút "Thử lại".
7. `Làm mới` dùng `GET_SEATS` và không được thay đổi kết quả của yêu cầu đang pending.

## 5. Hủy vé & lịch sử (P1)

- Tra cứu vé và trạng thái từ Leader; yêu cầu `CANCEL_TICKET` với `requestId` mới cho **ý định hủy**.
- Trước khi hủy hiển thị ticket id và xác nhận của người dùng; sau khi thành công, ghế thành `AVAILABLE`.
- Vé đã hủy vẫn xuất hiện lịch sử với trạng thái `CANCELLED`; không tự xóa khỏi Client chỉ vì ghế trống.
- Khi Client không chắc hủy thành công, tra cứu cùng requestId như luồng đặt.

## 6. Kiến trúc JavaFX

- `FXML` = layout, `Controller` = tương tác, `Service/Repository` = TCP Client adapter, `ViewModel` = properties/bindings.
- **Mọi I/O Socket, reconnect, read/write** phải nằm ngoài JavaFX Application Thread (`Task`, `Service`, `CompletableFuture` + executor).
- Cập nhật controls/properties trên JavaFX thread (`Platform.runLater` hoặc binding đúng ngữ cảnh).
- Controller không import `java.sql`, không tự xử lý Raft term/commit hoặc transaction.
- Model giao thức lấy từ module `shared`; không nhân bản DTO trong frontend.

## 7. Cluster Monitor

### 7.1. Vùng thông tin bắt buộc

| Thành phần | Dữ liệu | Lưu ý |
|---|---|---|
| Tổng quan | 5 thẻ Node ID/IP, online/offline | Trạng thái theo lần quan sát, có `observedAt` |
| Leader | Role, `leaderId`, `term` | Không cố định Node/Laptop nào |
| Tiến độ Raft | `lastLogIndex`, `commitIndex`, `lastApplied` | Không gộp ba khái niệm |
| Kết nối | Peer heartbeat/timeout | Có thể trễ so với thực tế |
| Nhật ký | RequestId, term, index, event | Cho lọc/đối chiếu khi demo |
| Cảnh báo | Thiếu quorum, Leader thay đổi, node lag | Không khẳng định commit khi chỉ thấy log append |

### 7.2. Fault controls (chỉ demo/testing)

- `STOP_NODE`, `START_NODE`, `INJECT_DELAY`, `BLOCK_PEER_LINK` là **ý định điều hành test**, không là các message Raft nghiệp vụ. Phương thức thực thi (Docker CLI/script/process controller) được đặt ở **hạ tầng demo**, không mở cổng điều khiển không xác thực trên LAN.
- Nếu chưa triển khai fault control thật, hiển thị trạng thái rõ `NOT_IMPLEMENTED` thay vì nút giả nhấp mà không dừng Node.
- Khi mất Laptop 6, Node 1–5 vẫn chạy; Monitor không bao giờ có phiếu bầu.

## 8. Empty/loading/error và accessibility

- Trước kết nối: không mặc định tất cả 10 ghế trống; hiển thị đang tải.
- Ghế bị đổi bởi Client khác: refresh sau thao tác bị từ chối hoặc làm mới theo chu kỳ.
- Không hiển thị “Đặt thành công” dựa trên thao tác click, animation, hay SQL ghi ở một Node.
- Form có nhãn, validate, keyboard navigation, đọc được ở độ phân giải laptop; cảnh báo lỗi rõ nguyên nhân.
- Các message lỗi dùng `errorCode` từ SPEC-03, bản tiếng Việt ở UI; không phân tích chuỗi `message` trả về để quyết định state.

## 9. Tiêu chí kiểm chứng

- `UI-01`: luôn hiển thị đúng 10 ghế khi đọc thành công.
- `UI-02`: hai JavaFX Client tranh cùng G05; màn của bên thắng được xác nhận, bên còn lại không bị hiển thị thành công giả.
- `UI-03`: mất phản hồi đặt vé, nút retry giữ nguyên requestId.
- `UI-04`: bấm đặt nhiều lần nhanh không tạo nhiều **ý định** trên cùng form.
- `UI-05`: Monitor thể hiện Leader/term thật; dừng Node thật thì trạng thái thay đổi từ dữ liệu mạng, không chỉ đổi màu thủ công.
- `UI-06`: tắt Monitor không ảnh hưởng quorum.
- `UI-07`: giao diện không đơ khi Node timeout/offline.
