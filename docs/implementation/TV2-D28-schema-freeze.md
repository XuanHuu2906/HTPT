# TV2-D28 — Đóng băng Schema Version (`schema-v1`) & Quy trình Seed/Reset Cụm

> **Owner:** TV2 (Phạm Minh Tuấn) · **Mốc:** D28 (Schema Freeze) · **Mức bằng chứng đã kiểm chứng:** `LOCAL` (5 container PostgreSQL 16-alpine D25 + fixture cô lập). Việc áp dụng reset thật trên 5 laptop LAN và launch cụm là việc của TV6 (`BLOCKED_INTEGRATION`).

---

## 1. Manifest Đóng Băng (`schema-v1`)

- **Schema Version:** `schema-v1` (`V1__schema.sql` + `V2__seed.sql`)
- **Phạm vi đối chiếu:** Khớp 100% DDL APPROVED-v1 trong [05-BOOKING-DATABASE.md](file:///d:/ok/HTPT/HTPT/docs/specs/05-BOOKING-DATABASE.md) §4 & §4.1. Không có migration thứ ba, không có DDL nằm ngoài thư mục `database/migration/`.

| Thành phần | Đường dẫn | Cách tính SHA-256 (tái lập trên Windows/Linux) | SHA-256 (chữ thường) |
|---|---|---|---|
| Schema DDL | `database/migration/V1__schema.sql` | Đọc UTF-8, chuẩn hóa CRLF (`\r\n`) → LF (`\n`), băm SHA-256 | `2dbebd9934220aac1ae76e43f286a214eb8e1dfc501554c86ba550a1f0a937ae` |
| Seed data | `database/migration/V2__seed.sql` | Đọc UTF-8, chuẩn hóa CRLF (`\r\n`) → LF (`\n`), băm SHA-256 | `b1ba3ab90465082332d10dbeeb7671730107b13df9b1221ed9b3d2bdf0af28e2` |
| **Structure Hash** | `information_schema.columns` + `pg_constraint` + `pg_indexes` | `SET search_path TO <schema>;` sắp xếp tất định theo `(table_name, ordinal_position)` / `(relname, conname)` / `(tablename, indexname)`, loại bỏ prefix schema trong `indexdef`, băm SHA-256 (không phụ thuộc OID/thời điểm) | `b1d434fd832997059d5780fdf69729b21df0127d58b24c18cd953713c10b0632` |

Lệnh PowerShell tái lập SHA-256 chuẩn hóa LF trên Windows:
```powershell
$lf = ([IO.File]::ReadAllText("database/migration/V1__schema.sql", [Text.Encoding]::UTF8)) -replace "`r`n", "`n"
$sha = [Security.Cryptography.SHA256]::Create()
([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($lf))) -replace "-", "").ToLower()
```
Trên Linux/macOS: `tr -d '\r' < database/migration/V1__schema.sql | sha256sum`.

---

## 2. Seed Đóng Băng (`V2__seed.sql`)

Seed khởi tạo genesis **chỉ** gồm:
- `trips`: 1 chuyến duy nhất `SE-001`.
- `seats`: đúng 10 ghế `G01`–`G10` cho `SE-001`, trạng thái ban đầu `AVAILABLE`, `current_ticket_id = NULL`.
- Trạng thái khởi tạo từ `V1__schema.sql`: `raft_meta(singleton=1, current_term=0, voted_for=NULL)`, `raft_apply_progress(singleton=1, last_applied=0)`.
- **Không** chèn bất kỳ dữ liệu nghiệp vụ nào (`tickets`, `request_results`, `business_events`, `raft_log` đều rỗng).

---

## 3. Quy Trình Reset Toàn Cụm (5 Node Đồng Bộ)

> **CẢNH BÁO — THAO TÁC `DESTRUCTIVE`:**
> - Thao tác dưới đây **XÓA TOÀN BỘ VÉ, LOG RAFT, AUDIT EVENTS VÀ KẾT QUẢ `request_results` CỦA CẢ CỤM 5 NODE**.
> - **KHÔNG** chạy khi hệ thống đang demo/bảo vệ trừ khi có chủ ý reset toàn cụm.
> - **KHÔNG** chạy trên một Node riêng lẻ đang tham gia consensus (reset 1/5 node sẽ làm lệch `raft_log`/`last_applied` với các node còn lại).
> - **KHÔNG** chạy trên PostgreSQL dùng chung với ứng dụng khác.

### Các bước thực hiện (áp dụng đồng thời cho cả 5 Node `node-1`..`node-5`)
1. **Dừng toàn bộ 5 Backend Node** trước khi chạm vào database (tránh ghi/replay trong lúc reset).
2. **Xác nhận & Sao lưu:** Người vận hành xác nhận đúng mục đích reset và sao lưu log/evidence (`pg_dump` hoặc copy `artifacts/`) nếu cần lưu giữ lịch sử phiên trước.
3. **Thực thi SQL reset trên TỪNG PostgreSQL instance trong 5 Node** (truyền mật khẩu qua biến môi trường `PGPASSWORD`/`DB_PASSWORD`, không ghi vào script/repo):
   ```sql
   BEGIN;
   TRUNCATE TABLE raft_log, business_events, request_results, tickets, seats, trips;
   UPDATE raft_meta SET current_term = 0, voted_for = NULL WHERE singleton = 1;
   UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1;
   -- Chạy lại database/migration/V2__seed.sql:
   INSERT INTO trips(trip_id) VALUES ('SE-001');
   INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
   SELECT 'SE-001', 'G' || LPAD(n::text, 2, '0'), 'AVAILABLE', NULL
   FROM generate_series(1,10) AS n;
   COMMIT;
   ```
   *(Hoặc với Docker dev/LAN dùng volume riêng: dừng cả 5 node, `down -v` theo đúng quy trình reset toàn cụm ở [08-DEPLOYMENT.md](file:///d:/ok/HTPT/HTPT/docs/specs/08-DEPLOYMENT.md) §7 rồi chạy lại `V1` + `V2` đồng nhất trên cả 5 volume mới).*
4. **Kiểm tra sau reset bằng runner D26** ([compare-5db.ps1](file:///d:/ok/HTPT/HTPT/database/compare-5db.ps1)):
   ```powershell
   $env:DB_PASSWORD = "<MAT_KHAU_POSTGRES>"
   powershell -ExecutionPolicy Bypass -File database\compare-5db.ps1 -NodesFile <file-cau-hinh-5-node> -PollRetries 0
   ```
   - Yêu cầu bắt buộc: `CONCLUSION: CONSISTENT (exit=0)`, cả 5 node đều `LastApplied=0`, `Tickets=EMPTY`, `Results=EMPTY`, `state_hash=f3b37d79...`, mọi `CHECK` `PASS`.
5. **Khởi động lại 5 Backend Node** và đợi bầu Leader hợp lệ.

*(TV6 có thể đóng gói quy trình trên vào `scripts/demo-reset-all.ps1` khi triển khai cụm thật.)*

---

## 4. Chính Sách Thay Đổi Sau Đóng Băng

1. **Bất biến file đã đóng băng:** **KHÔNG** sửa `V1__schema.sql` và `V2__seed.sql` sau khi đóng băng `schema-v1`.
2. **Thay đổi schema mới:** Mọi thay đổi phải tạo file migration mới (`V3__...sql`, `V4__...sql`) theo quy trình:
   - Cập nhật và được duyệt thay đổi tại [05-BOOKING-DATABASE.md](file:///d:/ok/HTPT/HTPT/docs/specs/05-BOOKING-DATABASE.md).
   - **Thông báo bắt buộc tới các thành viên bị ảnh hưởng:**
     - **TV1** (`desktop-client`): khi thay đổi dữ liệu hiển thị ghế/vé/lịch sử.
     - **TV3** (`shared`, `networking`): khi thay đổi DTO/contract/mã lỗi phản ánh trạng thái DB.
     - **TV4 & TV5** (`raft`): khi thay đổi `raft_meta`, `raft_log`, `raft_apply_progress` hoặc hợp đồng apply/replay.
     - **TV6** (`desktop-monitor`, `docker`, `scripts`): khi thay đổi migration mount, biến môi trường DB, script reset/healthcheck hoặc `compare-5db.ps1`.
   - Chạy lại toàn bộ regression D27 (`mvn -B -ntp -pl backend-node -am test`) và cập nhật manifest đóng băng mới.
3. **Đề xuất lệnh Git sau khi owner duyệt** *(không tự chạy trong task D28)*:
   ```bash
   git add database/migration/ docs/implementation/TV2-D28-schema-freeze.md artifacts/evidence/D28/TV2-D28-result.log
   git commit -m "chore(db): freeze schema-v1 manifest and cluster reset procedure (TV2-D28)"
   git tag -a schema-v1 -m "Freeze schema-v1 (V1+V2)"
   git push origin <branch> --tags
   ```

