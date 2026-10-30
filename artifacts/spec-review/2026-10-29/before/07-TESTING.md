# SPEC-07 — Chiến lược kiểm thử, AC01–AC10 và bằng chứng

> **Trạng thái:** Draft v1 · **Owner điều phối:** TV6 · **Thực thi:** Cả sáu thành viên.  
> Đây là **kịch bản và tiêu chí kỳ vọng**, không phải kết quả đã test. Trạng thái ban đầu của mọi testcase: **NOT_RUN**.

## 1. Nguyên tắc kiểm thử

- Test **đúng binary/config đang định bàn giao**; ghi commit SHA/tag và môi trường (dev Docker 1 laptop hoặc LAN 6 laptop).
- Phân biệt unit test với integration test 5 Node và demo mất **máy vật lý**. Unit/mock pass không thay được AC failover thực.
- Mỗi testcase ghi: `ID`, mục tiêu, precondition, action, expected, actual, status (`PASS|FAIL|BLOCKED|NOT_RUN`), lệnh chạy, bằng chứng.
- `PASS` chỉ khi **đã chạy**, so sánh thực tế với expected và lưu minh chứng; không điền PASS dựa trên việc code nhìn hợp lý.
- Sau lỗi/partition, so sánh dữ liệu **khi các Node đã bắt kịp cùng committed index**, tránh lấy lag hợp lệ làm lỗi đồng nhất.
- Các test ghi phải xác minh log Raft (term/index/commit) **và** trạng thái PostgreSQL thật, không chỉ kiểm tra UI.

## 2. Ma trận AC01–AC10

| Test ID | AC | Setup/hành động | Expected |
|---|---|---|---|
| `TC-AC01` | AC01 | Reset cụm mới, query `GET_SEATS` | Đúng `SE-001`, G01–G10, không trùng ghế |
| `TC-AC02` | AC02 | Đặt G02 với requestId mới | Trả `CONFIRMED` sau commit+apply, đúng 1 ACTIVE G02 |
| `TC-AC03` | AC03 | 6 Client hoặc harness đồng thời BOOK G05 bằng 6 requestId khác nhau | Tối đa 1 CONFIRMED, tối đa 1 ACTIVE G05 |
| `TC-AC04` | AC04 | Bán đủ 10 ghế rồi yêu cầu BOOK lại ghế đã bán | Không phát hành vé ACTIVE thứ 11 |
| `TC-AC05A` | AC05 | Dừng 1 Follower thật, BOOK ghế khác | Commit hợp lệ khi còn 4/5 và Leader đủ điều kiện |
| `TC-AC05B` | AC05 | Dừng 2 Node, BOOK ghế mới khi còn 3/5 | Có thể commit khi 3 Node còn lại thành majority hợp lệ |
| `TC-AC06` | AC06 | Dừng thêm Node để chỉ còn 2/5 | **Không có CONFIRMED mới** |
| `TC-AC07` | AC07 | Xác định Leader động, dừng Leader thật | Leader mới hợp lệ được bầu khi còn quorum, các lệnh tiếp tục an toàn |
| `TC-AC08` | AC08 | Node offline qua vài lệnh, bật lại | Catch-up, `lastApplied` tiến đến committed state, dữ liệu khớp |
| `TC-AC09` | AC09 | Chia mạng 3–2 bằng fault injector thật | Phía 2/5 không commit; không split-brain dẫn đến 2 vé ACTIVE |
| `TC-AC10` | AC10 | Gửi BOOK, cắt phản hồi hoặc kết nối trước khi Client nhận | Client giữ requestId, tra cứu/retry cùng payload, không sinh vé trùng |

`TC-AC07` phải xét trường hợp Leader lỗi **trong khi** có request chưa phản hồi; status cần được truy hồi, không đơn giản giả định failed. `TC-AC09` cần kiểm tra cả phía đa số và thiểu số. `TC-AC10` nên inject lỗi tại nhiều điểm: trước Leader nhận, sau append, sau majority commit, sau apply/trước response.

## 3. Bổ sung P1 và test kỹ thuật

| Test ID | Mục tiêu | Expected |
|---|---|---|
| `TC-CANCEL-01` | Hủy vé ACTIVE | Vé thành CANCELLED, ghế AVAILABLE, log/DB cập nhật |
| `TC-CANCEL-02` | BOOK lại ghế sau hủy | Vé mới ACTIVE, vé cũ vẫn CANCELLED |
| `TC-CANCEL-03` | Hủy trùng requestId | Không có hủy thứ hai gây mất dữ liệu |
| `TC-HISTORY-01` | Tra lịch sử qua nhiều BOOK/CANCEL | Thứ tự/log và vé trạng thái đúng, không mất lịch sử |
| `TC-RAFT-01` | Restart term/vote | Không bỏ phiếu hai candidate cùng term; term không giảm |
| `TC-RAFT-02` | Vote candidate log cũ | Node có log cập nhật hơn từ chối vote candidate cũ |
| `TC-RAFT-03` | AppendEntries không khớp prevLog | Từ chối, Leader bắt kịp đúng; không mất committed suffix |
| `TC-RAFT-04` | Leader có entry term cũ nhân bản majority | Không commit chỉ vì 3 bản sao khi entry chưa thỏa current-term rule |
| `TC-RAFT-05` | Crash trong lúc apply DB | Phục hồi transaction+lastApplied, không double apply |
| `TC-RAFT-06` | Old Leader bị partition | Không phục vụ ghi/đọc authoritative thiếu quorum |
| `TC-TCP-01` | Khung phân mảnh, Unicode, 2 message liên tiếp | Frame parse chính xác, không lệch độ dài byte |
| `TC-TCP-02` | Frame lỗi/oversize/version khác | Từ chối kiểm soát, không crash node hoặc commit |
| `TC-IDEMP-01` | Retry cùng requestId/intent | Trả lại cùng kết quả, không thêm vé |
| `TC-IDEMP-02` | Retry cùng requestId nhưng đổi seat | `REQUEST_ID_CONFLICT` |
| `TC-UI-01` | Mất kết nối khi đang đặt | UI UNKNOWN, không giả báo thành công, có đường tra kết quả |
| `TC-DEPLOY-01` | Mở nhiều Client trên LAN | Cùng một trip và cùng trạng thái commit |
| `TC-DEPLOY-02` | Dừng Laptop 6 | Cụm 5 Node không mất quorum vì Monitor không có vote |

## 4. Unit / integration / system

### Unit

- TV2: state machine, validation, requestId fingerprint, BOOK/CANCEL, rollback SQL (nếu có DB fixture).
- TV3: envelope codec, length prefix byte, validation, timeout/redirect.
- TV4/5: Raft log matching, term, vote, commit current-term rule, replay, timer behavior với fake transport/fake storage.
- TV1: ViewModel state transitions `SUBMITTING→CONFIRMED|REJECTED|UNKNOWN`.

### Integration 1 Node

- JavaFX → TCP → Java Node → PostgreSQL với một đường đặt vé.
- **Không được coi PASS “Raft cụm 5 Node” khi mới chỉ chạy một Node.**

### Integration 5 Node (Docker tại nhà)

- 5 Backend container, 5 PG volumes riêng, thật sự trao đổi TCP, bầu Leader, có log/commit/apply.
- Chạy các TC-AC02–10, mô phỏng lỗi container/mạng, so sánh state.
- Test partition cần fault injector **cắt liên lạc peer thực**, không chỉ tắt icon ở Monitor.

### System demo LAN (6 laptop)

- Laptop 1–5 mỗi máy có một Node+PG riêng; Laptop 6 chạy Monitor; tất cả có thể mở Client.
- Thử tắt máy/Node Leader và 2/5, khởi động lại, truy vấn đồng nhất; ghi bằng chứng nhiều máy.

## 5. SQL assertions ví dụ

Các câu dưới đây được thực thi **ở từng database Node đang sync đủ**, không phải một DB tập trung.

```sql
-- Ghế được seed đủ
SELECT COUNT(*) AS total_seats FROM seats WHERE trip_id = 'SE-001';
-- Phải bằng 10

-- Không có hai vé đang hiệu lực cùng ghế
SELECT trip_id, seat_id, COUNT(*) AS active_count
FROM tickets WHERE status = 'ACTIVE'
GROUP BY trip_id, seat_id HAVING COUNT(*) > 1;
-- Phải trả 0 hàng

-- Cùng một requestId chỉ có một kết quả đã apply
SELECT request_id, COUNT(*)
FROM request_results GROUP BY request_id HAVING COUNT(*) > 1;
-- Phải trả 0 hàng (PK cũng bảo vệ điều này)

-- Theo dõi tiến độ replay cục bộ
SELECT last_applied FROM raft_apply_progress WHERE singleton = 1;
```

Kết quả SQL chỉ là một lớp bằng chứng; phải đối chiếu đồng thời ticket/seat và Raft log/commit.

## 6. Artifact bằng chứng

Gợi ý thư mục **sẽ tạo khi kiểm thử thực tế**:

```text
artifacts/test-evidence/
└── 2026-xx-xx_<environment>_<commit-sha>/
    ├── TEST-RESULTS.md          # test ID, PASS/FAIL/BLOCKED/NOT_RUN
    ├── configs/                 # cấu hình ẩn secret
    ├── screenshots/             # màn hình JavaFX/Monitor
    ├── node-1.log ... node-5.log
    ├── queries/                 # SQL outputs/query text
    └── reproduction.txt         # lệnh chạy, seed/reset, fault actions
```

Log luôn ghi đúng `requestId`, `nodeId`, `term`, `index`, thời điểm và message type; không upload credential/token hoặc dữ liệu khách thật.

### Mẫu kết quả từng TC

```markdown
### TC-AC03 — Six Clients same seat
- Date/environment/commit: TBD
- Preconditions: 5 Node khỏe; seed sạch; Leader hợp lệ
- Steps: khởi chạy 6 Client đồng thời đặt G05, mỗi Client một requestId
- Expected: <= 1 CONFIRMED; <= 1 ACTIVE(G05)
- Actual: NOT_RUN
- Status: NOT_RUN
- Command/log/screenshot/SQL evidence: chưa có
- Owner/reviewer: TV2 + TV6
```

## 7. Cổng nghiệm thu cuối

Không công bố **MVP distributed DONE** khi chưa có tối thiểu: `AC01–AC10` chạy trên môi trường phù hợp (một số AC bắt buộc 5 Node), không có double booking, mô tả đúng điều kiện quorum, dữ liệu restart/recovery bảo toàn, source build được và đủ evidence. Nếu thiếu kịch bản P1, ghi rõ phạm vi chưa đạt thay vì báo tất cả hoàn thành.
