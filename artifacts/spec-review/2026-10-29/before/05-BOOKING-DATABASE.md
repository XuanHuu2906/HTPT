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

**Validation DESIGN-v1:** `requestId` là UUID; `passengerName` trim không rỗng, từ 1–120 ký tự; `tripId=SE-001`, ghế trong tập G01–G10. Tên người thật không cần thiết — dùng dữ liệu demo.

## 3. Logic quyết định trạng thái (deterministic state machine)

### BOOK

```text
on apply(COMMITTED_BOOK_ENTRY at index i):
  begin transaction
  verify i == lastApplied + 1
  if requestId already processed:
      verify same business intent fingerprint
      reuse persisted result; do not change seat/ticket
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
  if same requestId was already processed: reuse prior result
  else if ticket does not exist or is not ACTIVE:
      persist APPLIED_REJECTED(TICKET_NOT_ACTIVE)
  else:
      set ticket CANCELLED, set cancelledAt from command
      update seat AVAILABLE only if currentTicketId == ticketId
      persist APPLIED_SUCCESS(ticketId)
  update lastApplied=i; COMMIT transaction
```

`REQUEST_ID_CONFLICT`: phải so sánh **business intent fingerprint**, gồm loại lệnh và input do Client cung cấp, **không** gồm các field do Leader sinh (như `issuedAt`/`ticketId`) để retry qua Leader mới có thể được nhận diện đúng. Nếu kết quả đã bền vững với ID đó, mọi replay cùng ý định phải trả cùng kết quả.

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
  term BIGINT NOT NULL CHECK (term >= 0),
  command_type VARCHAR(32) NOT NULL,
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
  command_type VARCHAR(32) NOT NULL,
  intent_fingerprint CHAR(64) NOT NULL,
  state VARCHAR(20) NOT NULL
     CHECK (state IN ('APPLIED_SUCCESS', 'APPLIED_REJECTED')),
  reason_code VARCHAR(40),
  ticket_id VARCHAR(64),
  applied_log_index BIGINT NOT NULL,
  result_json JSONB NOT NULL
);

CREATE TABLE business_events (
  event_id BIGSERIAL PRIMARY KEY,
  log_index BIGINT NOT NULL,
  request_id UUID,
  event_type VARCHAR(40) NOT NULL,
  event_json JSONB NOT NULL
);
```

Mối liên hệ `seats.current_ticket_id` ↔ `tickets.ticket_id` được bảo đảm bởi **transaction và kiểm thử đối chiếu**. Nếu muốn thêm FK trong migration, cần thiết kế thứ tự insert/deferrable constraint để không chặn luồng apply. `business_events` có thể lưu một event hoặc nhiều event cho cùng log index theo quy ước thống nhất.

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
