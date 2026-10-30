# SPEC-05 — Nghiệp vụ đặt/hủy vé & PostgreSQL

> **Trạng thái:** Draft v1. Yêu cầu nghiệp vụ lấy từ hồ sơ nhóm; **DDL và một số quy tắc dữ liệu chi tiết là DESIGN-v1**, cần TV2 và TV4/5 review.  
> **Owner:** TV2; phối hợp TV4/5 về commit/apply và TV3 về command DTO.

## 1. Domain và bất biến

- Một chuyến demo duy nhất `SE-001`; **đúng 10 ghế** `G01`–`G10`.
- Trạng thái **ghế** lưu server: `AVAILABLE` hoặc `BOOKED`. `SELECTED_LOCAL` chỉ có ở UI.
- Trạng thái **vé:** `ACTIVE` hoặc `CANCELLED`; giữ vé đã hủy để phục vụ lịch sử.
- Tại mọi trạng thái được commit: tối đa **một** vé `ACTIVE` cho cùng `(tripId, seatId)`.
- Mọi lệnh đặt/hủy phát sinh hiệu ứng nghiệp vụ **chỉ sau commit Raft** và theo `logIndex` tăng dần; không chấp nhận business write cục bộ trước commit.
- Retry cùng `requestId`/cùng ý định trả lại kết quả đã bền vững; khác ý định với cùng `requestId` => `REQUEST_ID_CONFLICT`.
- Một trạng thái được báo `CONFIRMED` khi Node xử lý đã **apply và commit DB transaction**, không chỉ ghi Raft log.

## 2. BookingCommand / CancelCommand

**BOOK:** `requestId`, `tripId`, `seatId`, `passengerName`; Leader bổ sung trong log `ticketId`, `issuedAt` theo quy tắc định danh/thời gian được khóa cùng contract. Các Node dùng **đúng giá trị đã nhân bản**, không tự sinh lại trong `apply`.

**CANCEL (P1):** `requestId`, `ticketId`; Leader bổ sung `cancelledAt` vào log. Hủy hợp lệ nếu vé tồn tại, thuộc trạng thái `ACTIVE`; hủy xong trả ghế về `AVAILABLE` và giữ lịch sử.

**Validation DESIGN-v1:** `requestId` là UUID theo SPEC-03; `passengerName` sau Java `String.strip()` phải có 1–120 Unicode code point (không đếm UTF-16 code unit), không chứa U+0000; `tripId=SE-001`, ghế trong tập G01–G10. Không tự đổi hoa/thường hay Unicode normalization của tên; lưu tên đã strip vào command. Tên người thật không cần thiết — dùng dữ liệu demo. Schema/type lỗi được từ chối trước append; không append command thiếu field do Leader cấp.

## 3. Logic quyết định trạng thái (deterministic state machine)

### BOOK

```text
on apply(COMMITTED_BOOK_ENTRY at index i):
  begin transaction
  verify i == lastApplied + 1
  if requestId already processed:
      if same business intent fingerprint: reuse persisted result
      else: return REQUEST_ID_CONFLICT for this entry without overwriting original result
      do not change seat/ticket; still advance lastApplied=i atomically
  else if trip/seat invalid:
      persist APPLIED_REJECTED(INVALID_REQUEST)
  else if seat.status != AVAILABLE:
      persist APPLIED_REJECTED(SEAT_ALREADY_BOOKED)
  else:
      create ticket ACTIVE using ticketId/issuedAt in log command
      update seat to BOOKED, currentTicketId=ticketId
      persist APPLIED_SUCCESS(ticketId)
  append business audit event if newly relevant
  update lastApplied=i
  COMMIT transaction
```

### CANCEL (P1)

```text
on apply(COMMITTED_CANCEL_ENTRY at index i):
  begin transaction; verify next index
  if requestId already processed:
      compare command type + normalized intent as for BOOK
      reuse original result only if intent matches; otherwise REQUEST_ID_CONFLICT
      do not change seat/ticket or overwrite original result
  else if ticket does not exist or is not ACTIVE:
      persist APPLIED_REJECTED(TICKET_NOT_ACTIVE)
  else:
      set ticket CANCELLED, set cancelledAt from command
      update seat AVAILABLE only if currentTicketId == ticketId
      persist APPLIED_SUCCESS(ticketId)
  update lastApplied=i; COMMIT transaction
```

`REQUEST_ID_CONFLICT`: phải so sánh **business intent fingerprint**, gồm loại lệnh và input do Client cung cấp, **không** gồm các field do Leader sinh (như `issuedAt`/`ticketId`) để retry qua Leader mới có thể được nhận diện đúng. Nếu kết quả đã bền vững với ID đó, mọi replay cùng ý định phải trả cùng kết quả.

`ticketId` **bị loại khỏi fingerprint BOOK** vì Leader sinh, nhưng **bắt buộc nằm trong fingerprint CANCEL** vì là input Client. Cùng ID nhưng đổi loại BOOK↔CANCEL cũng là conflict. Không ném lỗi khiến apply bị kẹt mãi khi conflict đã nằm trong committed log: entry conflict không thay business state/kết quả gốc, nhưng checkpoint vẫn tiến trong transaction; trả ERROR_RESPONSE(REQUEST_ID_CONFLICT) cho lần gửi xung đột. Lookup ID vẫn trả kết quả gốc. Không thêm hàng request_results thứ hai để lưu conflict vì khóa chính là requestId.

### 3.1. Fingerprint, replay và dữ liệu tất định (DESIGN-v1)

- Chuẩn hóa input trước append và trước so sánh retry; UUID theo dạng chuẩn SPEC-03, tên theo §2. Tuple BOOK: `(BOOK_TICKET, tripId, seatId, passengerName)`; tuple CANCEL: `(CANCEL_TICKET, ticketId)`.
- Encode từng field tuple thành `uint32 big-endian byteLength || UTF-8 bytes`, nối theo đúng thứ tự trên, SHA-256 rồi lưu hex chữ thường 64 ký tự vào intent_fingerprint. Không hash toàn bộ JSON, thứ tự key, envelope, requestId, hoặc field Leader sinh. Không có field null trong tuple hợp lệ. Golden fixtures phải chứng minh thứ tự key JSON không đổi fingerprint, nhưng đổi vé đích CANCEL thì có.
- Có thể gộp retry đang pending cùng ID/intent để giảm log; đó không thay thế dedup khi apply. Nếu nhiều entry cùng ID do đổi Leader, kết quả của lần apply đầu quyết định; duplicate cùng intent dùng lại **appliedLogIndex ban đầu**, dù checkpoint tiếp tục tăng theo entry duplicate.
- Retry BOOK đã xác nhận sau khi vé bị hủy vẫn trả kết quả BOOK gốc; đó là xác nhận thao tác lịch sử, không chứng minh vé hiện còn ACTIVE. UI refresh ghế/lịch sử để biết trạng thái hiện tại.
- Hủy vé chỉ được giải phóng ghế nếu currentTicketId đúng vé đích. Nếu ticket ACTIVE mà seat liên kết không khớp, đây là lỗi toàn vẹn dữ liệu: rollback/fail closed, không trả CONFIRMED hoặc giải phóng ghế của vé khác.
- Timestamp command chỉ phục vụ hiển thị; cancelledAt có thể nhỏ hơn issuedAt do lệch đồng hồ Leader. Thứ tự dùng log index, không thêm constraint thời gian làm hỏng replay.
- Không xóa request_results hoặc tái sử dụng requestId trong vòng đời cluster v1; chưa có contract hết hạn dedup. NO_OP chỉ tăng checkpoint theo SPEC-04, không cần fingerprint hoặc kết quả nghiệp vụ.

**Lưu ý:** state machine **được quyền từ chối nghiệp vụ một cách tất định** trên một command đã commit (ví dụ ghế đã bán); đó không phải lỗi Raft. Để tất cả Node có cùng kết quả và lịch sử, ghi cả `APPLIED_REJECTED` vào `request_results`.

## 4. Lược đồ PostgreSQL v1 đề xuất

Các bảng dưới đây là **baseline DDL đề xuất**, không phải migration đã tồn tại. TV2 cần đưa vào Flyway/JDBC và viết test migrate/rollback. Mỗi Node có bản schema này trong một **PostgreSQL instance/volume riêng**.

```sql
CREATE TABLE raft_meta (
  singleton SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
  current_term BIGINT NOT NULL DEFAULT 0 CHECK (current_term >= 0),
  voted_for VARCHAR(20)
);
INSERT INTO raft_meta(singleton, current_term, voted_for) VALUES (1, 0, NULL);

CREATE TABLE raft_log (
  log_index BIGINT PRIMARY KEY CHECK (log_index >= 1),
  term BIGINT NOT NULL CHECK (term >= 1),
  command_type VARCHAR(32) NOT NULL
    CHECK (command_type IN ('BOOK_TICKET', 'CANCEL_TICKET', 'NO_OP')),
  command_json JSONB NOT NULL
);

CREATE TABLE raft_apply_progress (
  singleton SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
  last_applied BIGINT NOT NULL DEFAULT 0 CHECK (last_applied >= 0)
);
INSERT INTO raft_apply_progress(singleton, last_applied) VALUES (1, 0);

CREATE TABLE trips (
  trip_id VARCHAR(30) PRIMARY KEY
);

CREATE TABLE seats (
  trip_id VARCHAR(30) NOT NULL REFERENCES trips(trip_id),
  seat_id VARCHAR(3) NOT NULL,
  status VARCHAR(12) NOT NULL CHECK (status IN ('AVAILABLE', 'BOOKED')),
  current_ticket_id VARCHAR(64),
  PRIMARY KEY (trip_id, seat_id),
  CHECK (seat_id ~ '^G(0[1-9]|10)$'),
  CHECK ((status = 'AVAILABLE' AND current_ticket_id IS NULL)
      OR (status = 'BOOKED' AND current_ticket_id IS NOT NULL))
);

CREATE TABLE tickets (
  ticket_id VARCHAR(64) PRIMARY KEY,
  trip_id VARCHAR(30) NOT NULL,
  seat_id VARCHAR(3) NOT NULL,
  passenger_name VARCHAR(120) NOT NULL,
  status VARCHAR(12) NOT NULL CHECK (status IN ('ACTIVE', 'CANCELLED')),
  issued_at TIMESTAMPTZ NOT NULL,
  cancelled_at TIMESTAMPTZ,
  FOREIGN KEY (trip_id, seat_id) REFERENCES seats(trip_id, seat_id)
);
CREATE UNIQUE INDEX ux_one_active_ticket_per_seat
  ON tickets(trip_id, seat_id) WHERE status = 'ACTIVE';

CREATE TABLE request_results (
  request_id UUID PRIMARY KEY,
  command_type VARCHAR(32) NOT NULL
    CHECK (command_type IN ('BOOK_TICKET', 'CANCEL_TICKET')),
  intent_fingerprint CHAR(64) NOT NULL,
  state VARCHAR(20) NOT NULL
     CHECK (state IN ('APPLIED_SUCCESS', 'APPLIED_REJECTED')),
  reason_code VARCHAR(40),
  ticket_id VARCHAR(64),
  applied_log_index BIGINT NOT NULL CHECK (applied_log_index >= 1),
  result_json JSONB NOT NULL
);

CREATE TABLE business_events (
  log_index BIGINT PRIMARY KEY CHECK (log_index >= 1),
  request_id UUID NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  event_json JSONB NOT NULL
);
```

Mối liên hệ `seats.current_ticket_id` ↔ `tickets.ticket_id` được bảo đảm bởi **transaction và kiểm thử đối chiếu**. Nếu muốn thêm FK trong migration, cần thiết kế thứ tự insert/deferrable constraint để không chặn luồng apply. DESIGN-v1: đúng một business_events row khi tạo kết quả APPLIED mới, event_type=`BOOK_CONFIRMED|BOOK_REJECTED|CANCEL_CONFIRMED|CANCEL_REJECTED`; event_json là kết quả đã lưu. Duplicate/conflict/NO_OP không tạo event nghiệp vụ mới. Dùng log_index làm khóa tất định; không dùng BIGSERIAL vì sequence có thể nhảy khác nhau sau rollback tại các Node. Log chẩn đoán conflict vẫn ghi được ngoài business_events, không tham gia đối chiếu state machine.

`result_json` lưu đúng **payload** BOOK_TICKET_RESPONSE/CANCEL_TICKET_RESPONSE cuối cùng theo SPEC-03, không lưu envelope/messageId/senderId/sentAt của một lần trả lời. Lookup ánh xạ outcome CONFIRMED→APPLIED_SUCCESS, REJECTED→APPLIED_REJECTED từ kết quả này và các cột; duplicate chỉ tạo envelope mới, không sửa payload đã lưu. Đối chiếu JSONB theo nội dung field, không theo thứ tự key hay cách escape chuỗi.

### 4.1. Seed dữ liệu

```sql
INSERT INTO trips(trip_id) VALUES ('SE-001');
INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
SELECT 'SE-001', 'G' || LPAD(n::text, 2, '0'), 'AVAILABLE', NULL
FROM generate_series(1,10) AS n;
```

Seed chỉ thực hiện trên **cluster mới đồng nhất**. Không reset riêng một Node đang tham gia consensus; quy trình reset toàn cụm ở [08-DEPLOYMENT.md](08-DEPLOYMENT.md).

## 5. Tính nguyên tử giữa apply và checkpoint

**Transaction chuẩn:**

1. Lock `raft_apply_progress` (ví dụ `SELECT ... FOR UPDATE`) và xác minh `i = last_applied + 1`.
2. Đọc trạng thái ghế/ticket cục bộ trong cùng transaction (lock row cần thiết).
3. Tính kết quả, update seat/ticket, insert `request_results` và audit.
4. `UPDATE raft_apply_progress SET last_applied=i WHERE singleton=1`.
5. `COMMIT` DB trước khi gửi kết quả apply thành công cho Raft/Client.

Nếu crash trước commit SQL, **mọi thay đổi phải rollback** và entry được replay. Nếu crash sau commit SQL nhưng trước phản hồi, `lastApplied=i` và `request_results` giúp trả lại kết quả bền vững. Lỗi apply (ví dụ DB unavailable) phải retry có kiểm soát, không bỏ qua entry để xử lý entry `i+1`.

## 6. Tính nhất quán giữa 5 PostgreSQL

- Tất cả Node apply cùng command sequence vào state machine giống nhau, nên dữ liệu **hội tụ** sau khi follower catch-up.
- Trong lúc nhân bản hoặc một Node offline, số dòng của các database **có thể khác nhau tạm thời**; không coi đó là sai khi follower đang lag. Chỉ so sánh sau khi `lastApplied` đã bắt kịp cùng commit index.
- PostgreSQL ACID **chỉ** bảo đảm transaction cục bộ; không phải cơ chế consensus toàn cụm.
- Không bật streaming replication PG giữa năm Node để thay thế Raft app-level của đồ án.

## 7. Chính sách dữ liệu và bảo mật demo

- Client không dùng JDBC, không biết DB password; tất cả SQL nằm trong Backend.
- Không đưa plaintext DB password vào Git; cấu hình qua `.env.example` và biến môi trường.
- Demo chỉ dùng thông tin hành khách giả, không thu CCCD, thẻ ngân hàng hay mật khẩu người thật.
- Cấu trúc DB không cho phép nhiều vé ACTIVE cùng ghế bằng **unique partial index** như tuyến phòng vệ thứ hai; phòng vệ chính vẫn là log commit+apply tuần tự.

## 8. Kiểm thử bắt buộc cho TV2

- Init 10 ghế, BOOK thành công, BOOK đã bán bị từ chối, BOOK khác ghế thành công.
- 20+ request đồng thời nhắm G05: **tối đa 1 ACTIVE**.
- Retry y nguyên requestId: không phát thêm vé; thay payload cùng requestId: `REQUEST_ID_CONFLICT`.
- Crash trước/sau SQL commit, restart và replay không double-apply.
- Cancellation P1: active→cancelled→available, đặt lại ghế, vé cũ giữ lịch sử.
- Check SQL sau 5 Node đồng bộ: cùng tập `ACTIVE` tickets, same seats and request_results theo commit index.
