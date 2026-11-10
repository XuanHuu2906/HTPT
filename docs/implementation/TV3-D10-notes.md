# Ghi chú triển khai: Task TV3-D10 — TCP Single-Node Protocol Tests & Hardening

> **Tài liệu kỹ thuật nội bộ:** Nhóm 6 · Module `backend-node/networking` · **Tác giả:** TV3 (Protocol & TCP).  
> **Tham chiếu chính:** [SPEC-02 §2 (Kiến trúc)](../specs/02-ARCHITECTURE.md), [SPEC-03 §2, §3, §4, §7 (Contracts)](../specs/03-CONTRACTS.md), [SPEC-07 (Testing TC-TCP-03, TC-TCP-04)](../specs/07-TESTING.md), [SPEC-08 §64 (Deployment Port Isolation)](../specs/08-DEPLOYMENT.md).

---

## 1. Bối cảnh & Mục tiêu kỹ thuật

Sau khi hoàn thành nối TCP với Booking 1 Node ở ngày D09 (`BookTicketHandler`, `GetSeatsHandler`), mục tiêu của ngày D10 là:
1. **Gia cố tầng điều phối (Hardening `MessageDispatcher.java`)**: Đảm bảo an toàn giao thức tuyệt đối trước các vi phạm về cấu trúc phong bì, sai version, nhầm kênh/cổng, hoặc phản hồi thiếu tương quan.
2. **Kiểm thử giao thức TCP độc lập (`TcpSingleNodeProtocolTest.java`)**: Xây dựng bộ kiểm thử tự động toàn diện gửi nhận frame TCP thật trên socket, bao phủ các tình huống khắt khe:
   - Smoke tests (`PING`, `NODE_STATUS`, `GET_SEATS`, `BOOK_TICKET`).
   - Fixture JSON với ký tự tiếng Việt Unicode đa mặt phẳng.
   - Khả năng chống chịu ngắt kết nối (Disconnect resilience): client ngắt đột ngột, vòng lặp kết nối nhanh, vi phạm độ dài frame byte, JSON hỏng.
   - Tính toàn vẹn tương quan (Correlation integrity) giữa `messageId` và `correlationId`.
   - Phân tách cổng mạng (Port/Channel Isolation) chặn truy cập trái phép.
3. **Độc lập tiến độ**: Bộ kiểm thử chạy độc lập hoàn toàn với tầng JavaFX UI của TV1 (TV1-D10).

---

## 2. Chi tiết các biện pháp gia cố (Hardening Measures)

Trong [`MessageDispatcher.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java), các kiểm tra sau được bổ sung theo đúng thứ tự logic ưu tiên:

| Thứ tự | Kiểm tra | Quy định SPEC | Hành vi xử lý |
|---|---|---|---|
| **0** | `isValidUuid(messageId)` | SPEC-03 §2 | Bỏ qua không phản hồi (`return null`) nếu `messageId` không hợp lệ, tránh gửi response uncorrelatable làm loạn client. |
| **1** | `protocolVersion == 1` | SPEC-03 §3.0 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` nếu version khác `1`. |
| **2** | `type == null` | SPEC-03 §3.0 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` với lý do thiếu `type`. |
| **3** | `type.isResponse()` | SPEC-03 §3.1 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` nếu client gửi nhầm response type lên server. |
| **4** | `correlationId == null` | SPEC-03 §3.0 | Bắt buộc mọi incoming request phải có `correlationId == null`. |
| **5** | `senderId` non-null/blank | SPEC-03 §3.0 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` nếu thiếu `senderId`. |
| **6** | `sentAt` non-null/blank | SPEC-03 §3.0 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` nếu thiếu `sentAt`. |
| **7** | `payload` non-null | SPEC-03 §3.0 | Trả về `ERROR_RESPONSE(INVALID_REQUEST)` nếu thiếu `payload`. |
| **8** | Channel/Port Isolation | SPEC-08 §64 | Cổng Client từ chối Raft RPCs (`REQUEST_VOTE`, `APPEND_ENTRIES`). Cổng Peer từ chối Booking requests. |
| **9** | Correlation Safety | SPEC-03 §2 | Nếu handler trả về response nhưng thiếu `correlationId`, dispatcher tự động gán `correlationId = request.getMessageId()`. |
| **10** | Fail-closed Exception | SPEC-03 §7 | Mọi lỗi không lường trước (`Throwable`) đều được bắt và chuyển thành `INTERNAL_ERROR`, không để crash Node. |

---

## 3. Khả năng chống chịu ngắt kết nối (Disconnect Resilience)

Khi triển khai trên socket TCP thực tế, client có thể gặp sự cố mạng hoặc gửi dữ liệu bất thường:

1. **Client đóng socket đột ngột (Abrupt disconnect / EOF):**
   - Vòng lặp `handleClient` trong [`TcpServer.java`](../../backend-node/src/main/java/vn/nhom6/railway/networking/TcpServer.java) bắt được `EOFException` hoặc `SocketException`.
   - Tiến trình giải phóng socket sạch sẽ, xóa khỏi `activeSockets`, không ném uncaught exception.
   - Các client khác và các kết nối mới tiếp tục hoạt động mà không bị ảnh hưởng.
2. **Vòng lặp kết nối nhanh (Rapid connect/disconnect):**
   - Đã kiểm chứng qua 15 kết nối mở và đóng tức thì mà không gửi frame.
   - Server không bị cạn kiệt tài nguyên file descriptor hay thread pool.
3. **Khung byte sai kích thước (Malformed framing length):**
   - Theo SPEC-03 §2, khung byte tối đa là 1 MiB ($1,048,576$ bytes) và độ dài phải $> 0$.
   - Khi client gửi length prefix $2,000,000$ hoặc $\le 0$, server lập tức ngắt kết nối mà không phản hồi.
4. **Dữ liệu JSON hỏng (Corrupt JSON payload):**
   - Khi khung byte chứa ký tự không thể parse thành JSON envelope hợp lệ, server đóng kết nối ngay lập tức, không gửi phản hồi rác làm tắc nghẽn giao thức.

---

## 4. Tính toàn vẹn tương quan thông điệp (Correlation Integrity)

Hệ thống phân biệt rõ ràng 3 định danh theo đúng SPEC-03:

```
[Request Envelope]
  ├── messageId     : UUID v4 (Định danh duy nhất của frame request này)
  ├── correlationId : null (Bắt buộc null trên request)
  └── payload
        └── requestId : UUID v4 (Định danh nghiệp vụ idempotent, giữ nguyên khi retry)

[Response Envelope]
  ├── messageId     : UUID v4 mới (Định danh duy nhất của frame response này)
  ├── correlationId : request.messageId (Liên kết 1-1 với request ban đầu)
  └── payload
        └── requestId : request.requestId (Bảo toàn requestId nghiệp vụ)
```

Kiểm thử `testCorrelationIdStrictMatching` trong [`TcpSingleNodeProtocolTest.java`](../../backend-node/src/test/java/vn/nhom6/railway/networking/TcpSingleNodeProtocolTest.java) đã chứng minh $100\%$ các phản hồi trả về qua TCP đều có `correlationId` khớp tuyệt đối với `messageId` của yêu cầu tương ứng.

---

## 5. Kết luận nghiệm thu Gate G3

Nhiệm vụ TV3-D10 hoàn thành xuất sắc các yêu cầu:
- Socket/protocol tests chạy độc lập qua JUnit/Maven với 100% ca kiểm thử PASS.
- Đầy đủ minh chứng nghiệm thu tại [`artifacts/evidence/D10/TV3-D10-result.log`](../../artifacts/evidence/D10/TV3-D10-result.log).
- Sẵn sàng bàn giao cho các thành viên khác tích hợp UI và Raft cluster.
