# BỘ QUY TẮC TỰ HÀNH & KỶ LUẬT THỰC THI CỦA AI AGENT (SELF-RULES)
> **Dự án:** Railway Distributed System — Nhóm 6  
> **Áp dụng:** Mọi phiên làm việc của AI Coding Agent trên repository `D:\HTPT`  
> **Căn cứ nguồn gốc:** `D:\HTPT\AGENTS.md` và hệ thống đặc tả `docs/specs/*`

---

## 1. Tôn chỉ cao nhất & Ranh giới công nghệ (Zero Deviation)
1. **Bản chất hệ sinh thái:** Đây là hệ thống phân tán chạy trên mạng LAN/localhost với Java Desktop UI.
2. **CẤM TUYỆT ĐỐI:**
   - Không sinh Web Frontend (HTML/JS web client, React, Vue, Angular, Vite, Next.js).
   - Không dùng REST API, Spring Boot Web / Spring MVC, SOAP, GraphQL, WebSocket để thay thế giao thức socket.
   - Không dùng bất kỳ dịch vụ Cloud/SaaS bên ngoài nào (Firebase, AWS, Supabase, Cloud DB, Redis Cloud...).
   - Không tự ý thêm message broker ngoài như Kafka, RabbitMQ, Redis.
3. **BẮT BUỘC SỬ DỤNG:**
   - Ngôn ngữ: Java (JDK 17/21).
   - Giao diện: JavaFX (Desktop Client & Desktop Monitor).
   - Mạng: Pure Java TCP Socket (Length-prefixed binary/JSON framing theo `03-CONTRACTS.md`).
   - Lưu trữ: 5 instance PostgreSQL độc lập riêng biệt cho 5 Backend Node.
   - Đồng thuận: Raft Consensus Engine 5 Node tự cài đặt (Quorum = 3/5).

---

## 2. Kỷ luật Trung thực & Bằng chứng thực nghiệm (No Fake Reporting)
1. **Không báo cáo kết quả giả mạo:** Tuyệt đối không bao giờ trả lời `PASS`, `DONE`, `works`, `tested` nếu chưa thực sự thực thi lệnh qua công cụ (`run_command`) và thu được exit code = 0 cùng log output thực tế.
2. **Xử lý thiếu môi trường:** Nếu môi trường thiếu Java, Docker, PostgreSQL hoặc test script chưa thể chạy được, bắt buộc báo cáo chính xác là `NOT_RUN` hoặc `BLOCKED`, kèm lý do cụ thể và yêu cầu mở chặn.
3. **Không bịa đặt tệp tin:** Không bao giờ tuyên bố một tệp đã tồn tại, một container đã chạy hay 5 node đã thông nhau khi chưa có bằng chứng kiểm tra file system / tiến trình.

---

## 3. Thứ tự tra cứu bắt buộc trước mọi thay đổi (Hierarchy of Truth)
Trước khi phân tích hay sinh code, Agent bắt buộc đọc tài liệu theo đúng thứ tự:
1. `docs/specs/00-MASTER.md`: Mục tiêu, phạm vi P0/P1/Out-of-scope.
2. `docs/specs/01-REQUIREMENTS.md`: Yêu cầu nghiệp vụ (FR) và tiêu chí chấp nhận (AC).
3. `docs/specs/02-ARCHITECTURE.md`: Kiến trúc phân tầng, phân vùng mạng, luồng dữ liệu.
4. `docs/specs/03-CONTRACTS.md`: Hợp đồng chuẩn cho Wire DTO, Error Code, Message Type, Framing.
5. SPEC chuyên môn:
   - `04-RAFT.md`: Thuật toán Raft, election, log replication, safety.
   - `05-BOOKING-DATABASE.md`: Nghiệp vụ vé tàu, 10 ghế G01–G10, schema DB, idempotency.
   - `06-DESKTOP-UI.md`: JavaFX Client & Monitor layout, user interactions.
   - `07-TESTING.md`: Chiến lược kiểm thử 5 cấp độ (L1 -> L5), kịch bản lỗi mạng.
   - `08-DEPLOYMENT.md`: Cấu hình Docker, scripts, topology 5 node.

*Lưu ý:* Mọi chi tiết ghi `DESIGN-v1/CHƯA KHÓA` chỉ là đề xuất. Nếu phát hiện code trong repo khác SPEC, phải báo cáo mâu thuẫn cho User kèm phương án lựa chọn, không tự ý phá vỡ contract đang chạy.

---

## 4. Kỷ luật Phạm vi & Ranh giới Module (Ownership Boundaries)
Tuân thủ nghiêm ngặt ranh giới sở hữu mã nguồn:
- **TV1 (`desktop-client/**`):** Giao diện JavaFX cho khách đặt vé. Đổi DTO phải phối hợp TV3.
- **TV2 (`backend-node/**/booking/**`, `persistence/**`, `database/**`):** Logic vé, PostgreSQL migration/schema, SQL constraints, callback apply từ Raft.
- **TV3 (`shared/**`, `backend-node/**/networking/**`):** DTO dùng chung, Codec/Framing, TCP Network Transport, router.
- **TV4 (`backend-node/**/raft/replication/**`):** Raft Durable Log, AppendEntries RPC, Commit Index, Quorum check.
- **TV5 (`backend-node/**/raft/election/**`, `raft/recovery/**`):** Raft Election, RequestVote RPC, State recovery, Leader heartbeat.
- **TV6 (`desktop-monitor/**`, `docker/**`, `configs/**`, `scripts/**`, `integration-tests/**`):** Dashboard giám sát cluster, kịch bản test tích hợp, Docker Compose, fault injection scripts.
- **Cả nhóm:** `pom.xml`, `AGENTS.md`, `docs/specs/**`, `backend-node/**/node/**`.

**Nguyên tắc trước khi sửa:** Agent phải trình bày: Task ID, FR/AC liên quan, file/module dự kiến sửa + owner, contract cần giữ nguyên, và phương án kiểm thử. Không tự mở rộng phạm vi ra ngoài task được giao.

---

## 5. Quy tắc Bất biến An toàn Hệ thống (Safety Invariants)
**An toàn dữ liệu quan trọng hơn tốc độ code.**
1. **Duy nhất 10 ghế (G01–G10):** Tuyệt đối không bao giờ có >1 vé ở trạng thái `ACTIVE` cho cùng 1 ghế trên 1 chuyến tàu.
2. **Điều kiện xác nhận `CONFIRMED`:** Chỉ được trả về `CONFIRMED` cho client khi và chỉ khi:
   - Entry đặt vé đã được đa số Quorum (ít nhất 3/5 node) chấp thuận và Raft đã commit.
   - Local State Machine (PostgreSQL của node) đã apply thành công trong database transaction.
3. **Raft Current-Term Safety:** Tuân thủ nghiêm ngặt quy tắc Raft Figure 8: Leader không bao giờ trực tiếp commit một log entry từ term trước bằng cách đếm số replica; việc commit term trước chỉ được thực hiện gián tiếp khi commit entry của term hiện tại.
4. **Bền vững hóa (Durability):** `currentTerm`, `votedFor`, và toàn bộ `log entries` phải được persist bền vững xuống disk trước khi gửi phản hồi RPC.
5. **Đồng bộ Apply & `lastApplied`:** Việc apply log entry vào state machine và cập nhật `lastApplied` phải nằm trong cùng một local database transaction để đảm bảo idempotent khi crash recovery.
6. **Tính tất định (Determinism):**
   - Tuyệt đối không dùng `System.currentTimeMillis()` trên từng node riêng lẻ để xác định thứ tự commit.
   - Không sinh dữ liệu ngẫu nhiên (UUID/random) bên trong State Machine apply. Mọi dữ liệu phải do Leader đóng gói vào log entry từ trước.
7. **Bảo toàn Idempotency:** Phân biệt rõ `messageId` (ID của mỗi gói tin TCP gửi đi) và `requestId` (ID định danh nghiệp vụ do client sinh ra). Khi retry cùng một tác vụ, client BẮT BUỘC giữ nguyên `requestId`.
8. **Follower Rejection:** Node Follower khi nhận request ghi từ Client không được tự ý xử lý; bắt buộc trả về mã lỗi `NOT_LEADER` kèm theo `leaderHint` (nếu biết).
9. **Desktop Monitor:** Chỉ đóng vai trò quan sát viên (read-only), không tham gia bỏ phiếu (non-voter), không sửa authoritative state.
10. **Fail Closed:** Khi có bất kỳ nghi ngờ nào về tính đúng đắn hoặc an toàn của dữ liệu, Agent phải dừng lại, báo cáo blocker, tuyệt đối không dùng shortcut/workaround tạm bợ.

---

## 6. Quy trình 5 bước thực hiện mỗi lượt (5-Step Execution Workflow)
Mỗi yêu cầu từ User phải được triển khai tuần tự qua 5 bước:
1. **INSPECT:** Kiểm tra code hiện có, cấu trúc thư mục, test case, tài liệu SPEC liên quan trước khi kết luận.
2. **PLAN:** Xác định phạm vi ảnh hưởng, đối chiếu owner, xác định rõ contract cần giữ nguyên.
3. **IMPLEMENT:** Viết code chuẩn mực, sạch sẽ, tuân thủ Java conventions, không tự ý thêm thư viện ngoài không duyệt trong `pom.xml`.
4. **TEST:** Chạy unit test / integration test thực tế. Ghi nhận chính xác command, output và exit code.
5. **REPORT:** Báo cáo kết quả theo đúng cấu trúc chuẩn.

---

## 7. Mẫu báo cáo kết thúc bắt buộc (Mandatory Output Format)
Cuối mỗi tác vụ hoàn thành, Agent bắt buộc trình bày câu trả lời theo đúng mẫu quy định trong `AGENTS.md`:

```markdown
### Đã thay đổi
- <file>: <thay đổi có thể kiểm chứng>

### Đối chiếu SPEC
- FR/AC: ...
- Contracts/API giữ nguyên hay thay đổi có phê duyệt: ...

### Kiểm thử đã thực sự chạy
- Command: ...
- Environment: ...
- Result: PASS/FAIL/BLOCKED/NOT_RUN
- Evidence: <đường dẫn thật>

### Chưa hoàn thành / rủi ro
- ...
```
