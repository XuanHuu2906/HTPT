# AGENTS.md — Quy tắc bắt buộc cho AI Coding Agents

> **Dự án:** Railway Distributed System — Nhóm 6 · **Phiên bản:** Draft v1.0 (29/10/2026).  
> **Mục tiêu:** Hướng dẫn AI/Codex/agent triển khai đúng SPEC, không làm lệch kiến trúc hoặc báo cáo tiến độ giả.

## 0. Chỉ thị cao nhất của repository

**Đây là Desktop App hệ thống phân tán trên LAN/localhost, không phải ứng dụng web.**

- **CẤM:** website frontend, React/Vue/Angular, REST/HTTP API, Spring MVC/Web, SOAP, GraphQL, WebSocket nhằm thay thế giao thức đã chốt, Web Service hoặc gọi SaaS/cloud ngoài làm lõi ứng dụng.
- **BẮT BUỘC:** Java/JavaFX Desktop, Backend Java TCP Socket, PostgreSQL riêng cho từng Node, Raft 5 Node, quorum 3/5.
- **Không được:** đổi công nghệ, thuật toán, tên contract hay phạm vi nghiệp vụ mà không được chấp thuận qua thay đổi SPEC.
- **Không được:** bỏ qua Raft bằng cách ghi trực tiếp 3 PostgreSQL, cho Client SQL trực tiếp, hardcode Laptop 1 làm Leader, tạo 5 backend codebase khác nhau.
- **Không được:** tuyên bố `PASS`, `DONE`, `works`, `tested` khi chưa thực sự chạy tương ứng và có bằng chứng.

## 1. Đọc các SPEC trước mọi thay đổi

**Bắt buộc đọc theo thứ tự:**

1. `docs/specs/00-MASTER.md`
2. `docs/specs/01-REQUIREMENTS.md`
3. `docs/specs/02-ARCHITECTURE.md`
4. `docs/specs/03-CONTRACTS.md`
5. SPEC chuyên trách tác vụ: `04-RAFT.md`, `05-BOOKING-DATABASE.md`, `06-DESKTOP-UI.md`, `07-TESTING.md` hoặc `08-DEPLOYMENT.md`.

Dù task nhỏ, agent phải hiểu **source of truth**: wire DTO/type/error → `03-CONTRACTS.md`; Raft safety/liveness có điều kiện → `04-RAFT.md`; business rules/SQL → `05-BOOKING-DATABASE.md`; FR/AC → `01-REQUIREMENTS.md`. **Không tự định nghĩa bản thứ hai**.

### Trạng thái tài liệu

- Nhiều chi tiết ghi `DESIGN-v1/CHƯA KHÓA`; đó là hợp đồng **đề xuất** cần owner review, **không phải** API đã tồn tại hay đã approve.
- Tài liệu mô tả kết quả kỳ vọng, **không chứng minh** repository đã cài đặt hay test pass.
- Nếu trong repo thật code/contract khác SPEC: báo rõ **mâu thuẫn hiện tại** trước khi sửa, cung cấp lựa chọn và tác động; không tự phá contract đang chạy.

## 2. Làm việc theo task, không tự mở rộng phạm vi

Trước khi sửa, agent phải trình bày ngắn gọn:

- Task ID / mục tiêu / FR-AC liên quan.
- File/module dự kiến sửa và owner có liên quan.
- Hợp đồng/phụ thuộc cần giữ nguyên.
- Cách kiểm tra (unit/integration/E2E, lệnh và assertion kỳ vọng).

Chỉ sửa trong phạm vi được giao. Không tự triển khai tính năng ngoài phạm vi (thanh toán thật, website, nhiều chuyến, Redis/Kafka, dynamic Raft membership, snapshot khi chưa cần).

## 3. Module ownership & ranh giới chỉnh sửa

| Owner | Đường dẫn/quyền sở hữu chính | Phối hợp |
|---|---|---|
| TV1 | `desktop-client/**` | Thay DTO cần TV3 |
| TV2 | `backend-node/**/booking/**`, `**/persistence/**`, `database/**` | Callback apply/DB Raft review TV4/5 |
| TV3 | `shared/**`, `backend-node/**/networking/**`, TCP routing | Mọi người dùng DTO chung |
| TV4 | `backend-node/**/raft/replication/**`, durable log + commit | TV5 cùng review `raft/core/**` |
| TV5 | `backend-node/**/raft/election/**`, `raft/recovery/**` | TV4 cùng review `raft/core/**` |
| TV6 | `desktop-monitor/**`, `docker/**`, `configs/**`, `scripts/**`, `integration-tests/**` tổng hợp | TV2 migration; TV3 ports; TV4/5 test Raft |
| Cả nhóm | `pom.xml`, `AGENTS.md`, `docs/specs/**`, `backend-node/**/node/**`, test xuyên module | Phải review khi đổi shared contracts |

`raft/recovery/**` có thể có thay đổi TV4/TV5 cùng nhau; owner chính của issue quyết định PR, nhưng **không được viết hai cơ chế Raft cạnh tranh**. Một file có một owner/reviewer rõ ở mỗi PR.

## 4. Quy tắc chỉnh sửa contract

1. Không đổi wire DTO, tên field, status/error code, length framing trong code riêng nếu `03-CONTRACTS.md` chưa cập nhật và được các bên review.
2. Thay đổi phải cập nhật cùng lượt: `shared` DTO/codec, message handler, Client, test fixtures, SPEC, changelog PR.
3. Giữ tương thích v1; nếu breaking change thực sự cần thiết, phải thống nhất tăng `protocolVersion` hoặc quy trình migration.
4. `messageId` khác với `requestId`: retry request business **giữ nguyên requestId**, có thể có messageId mới.
5. Follower trả `NOT_LEADER` và `leaderHint` theo SPEC-02/03; không tự xác nhận booking.

## 5. Safety invariants quan trọng hơn tốc độ code

- Đúng 10 ghế G01–G10; không bao giờ >1 vé `ACTIVE`/ghế/chuyến.
- Không có `CONFIRMED` nếu **chưa Raft-commit và chưa local DB-apply bền vững**.
- Majority của cluster 5 voter là 3; với 2/5 không xác nhận ghi.
- Raft commit phải tuân thủ current-term rule, log up-to-date election và log matching; không biến thuật toán thành “3 ACK là đủ”.
- `currentTerm/votedFor/log` phải persist đúng; apply và `lastApplied` cùng transaction để crash/replay an toàn.
- Không sử dụng `System.currentTimeMillis()` trên từng Node để xác lập thứ tự commit; không sinh random/UUID trong state-machine apply.
- Client timeout hoặc response lost => `UNKNOWN`, tra cứu hoặc retry **cùng requestId**.
- Monitor không phải voter, không sửa authoritative state.

Khi không chắc safety, agent phải **fail closed** và ghi blocker rõ ràng, không thêm shortcut “tạm thời” khiến demo đẹp nhưng bán trùng.

## 6. Quy trình phát triển bắt buộc

1. **Inspect:** đọc source code liên quan, test hiện có, configs, SPEC trước khi quyết định thay đổi.
2. **Plan:** chia thay đổi nhỏ, xác định dependency/owner và giao diện chung cần khóa.
3. **Implement:** giữ nguyên architecture, không tự thêm framework/web layer; dùng Maven module theo spec.
4. **Test:** chạy test nhỏ nhất liên quan, sau đó test tích hợp cần thiết; lưu command, exit code, output, môi trường, commit SHA nếu có.
5. **Report:** mô tả file đã sửa, kết quả test từng lệnh, testcase/AC nào đạt/chưa đạt, các rủi ro và bước tiếp theo. Trích dẫn evidence path **thực tồn tại**.

**Không tạo thông tin giả:** không nói có file chưa tạo, không nói Docker đang chạy nếu chỉ viết Compose, không báo 5 Node thông nhau nếu chỉ compile Java, không gán PASS cho script không chạy được.

## 7. Bộ lệnh kiểm tra tham khảo — phải kiểm tra repo có file trước

```bash
# Chỉ chạy khi repo đã có pom.xml/modules tương ứng
mvn -B -ntp clean verify

# Chỉ chạy khi Dockerfile + compose.dev.yml đã được tạo
docker compose -f docker/compose.dev.yml config
docker compose -f docker/compose.dev.yml up -d --build
docker compose -f docker/compose.dev.yml ps
```

Lệnh mẫu **không phải bằng chứng chạy**. Nếu môi trường hiện tại không có JDK/Docker/PostgreSQL, báo `NOT_RUN` hay `BLOCKED`, không suy diễn `PASS`.

## 8. Test matrix ưu tiên

- Nghiệp vụ: `AC01`, `AC02`, `AC03`, `AC04`, `AC10` trước (bao gồm requestId và SQL constraint).
- Cụm: `AC05`, `AC06`, `AC07`, `AC08`, `AC09` sau khi Election/AppendEntries/commit/apply đúng.
- P1: hủy, lịch sử, monitor/fault tools sau P0, nhưng phải gắn nhãn chính xác chưa triển khai nếu có thiếu.
- Tham chiếu `docs/specs/07-TESTING.md`; không thay kiểm thử 5 Node bằng fake/mock chỉ để đánh dấu hoàn thành.

## 9. Rủi ro phụ thuộc trong kế hoạch 30 ngày

Tài liệu cảnh báo nhóm chỉ ra các thời điểm D3 (khóa API), D6 (storage chưa kịp), D12 (RPC + lưu bền), D15 (election/replication), D17–18 (apply/commit), D21–23 và D25–26 (tích hợp, safety test) là **nguy cơ phụ thuộc**, không phải lỗi đã xảy ra.

- D1–D3: TV3 chốt shared DTO với TV2/4/5; các module dùng stub/fake khi dependency chưa có.
- Không sửa API độc lập chỉ vì module khác chưa bàn giao; dùng interface stubs ghi rõ fake vs real.
- Tích hợp `RAFT COMMIT -> Booking apply` cần xác nhận từ TV4 và TV2; không báo distributed-booking PASS bằng callback mock.
- Trước test system phải đóng băng build/candidate; log, screenshot và test report thuộc đúng commit đang test.

## 10. Cấu trúc câu trả lời cuối mỗi task cho người giao việc

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

**Kết luận:** an toàn dữ liệu và tính đúng đắn của hệ thống phân tán quan trọng hơn số lượng feature và báo cáo đẹp.
