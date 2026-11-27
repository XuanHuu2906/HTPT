# TV2-D26 — Hướng dẫn dùng SQL Checker `database/compare-5db.ps1` (Hậu Recovery)

> **Owner:** TV2 (Phạm Minh Tuấn) · **Phạm vi:** Công cụ `READ ONLY` chạy độc lập trên Windows PowerShell 5.1 + `psql`, đối chiếu trạng thái PostgreSQL giữa 5 node sau recovery hoặc tại mốc nghiệm thu G6.

## 1. Khi nào chạy tại G6

- Chạy **sau khi** cụm ngừng nhận lệnh ghi mới hoặc các node vừa khởi động lại đã có đủ thời gian catch-up (`lastApplied` ổn định).
- Script chỉ thực thi giao dịch `BEGIN TRANSACTION READ ONLY` qua [compare-db.sql](file:///d:/ok/HTPT/HTPT/database/compare-db.sql) và một truy vấn đọc `pg_indexes`, **không sửa dữ liệu, không tạo/sửa schema**.

## 2. Cách chạy

Mật khẩu PostgreSQL truyền qua biến môi trường (mặc định `DB_PASSWORD`, hoặc `RAILWAY_TEST_DB_PASSWORD`, hoặc `PGPASSWORD`), **không** ghi vào file cấu hình hay command line:

```powershell
$env:DB_PASSWORD = "<MAT_KHAU_POSTGRES>"

# 1) Chạy trên cấu hình mặc định D25 (5 PG cục bộ 127.0.0.1:5433..5437)
powershell -ExecutionPolicy Bypass -File database\compare-5db.ps1

# 2) Chạy trên mạng LAN 6 laptop bằng file danh sách node ngoài (CSV / JSON / pipe-delimited)
powershell -ExecutionPolicy Bypass -File database\compare-5db.ps1 -NodesFile configs\lan\pg-nodes.csv -PollRetries 5 -PollIntervalSeconds 3

# 3) Truyền danh sách endpoint trực tiếp qua tham số
powershell -ExecutionPolicy Bypass -File database\compare-5db.ps1 -Nodes "node-1=192.168.10.11:5432/railway:railway","node-2=192.168.10.12:5432/railway:railway"
```

Định dạng `-NodesFile`:
- **CSV:** header `id,host,port,db,user`
- **JSON:** mảng đối tượng `[{"id":"node-1","host":"...","port":5432,"db":"...","user":"..."}]`
- **Text/conf:** mỗi dòng `id|host|port|db|user` (bỏ qua dòng trống và `#`)

## 3. Cách đọc kết quả & Mã thoát (Exit Codes)

| Kết luận | Mã thoát | Ý nghĩa & Hành động |
|---|---:|---|
| `CONSISTENT` | `0` | Mọi node reachable, mọi `CHECK` cục bộ `PASS`, index `ux_one_active_ticket_per_seat` khớp, cùng `last_applied` và cùng `state_hash`. |
| `DIVERGENCE` | `2` | Có ít nhất 2 node có **cùng `last_applied`** nhưng khác `state_hash` (lỗi phân kỳ dữ liệu nghiêm trọng). Xem mục `DIVERGENCE DETAILS` để biết node và bảng lệch (`seats`, `tickets`, `request_results`). |
| `LAG_UNRESOLVED` | `3` | Một hoặc nhiều node có `last_applied` thấp hơn sau khi đã chờ hết số lần `-PollRetries`. Không coi là phân kỳ; chờ node catch-up thêm rồi chạy lại. |
| `UNREACHABLE` | `4` | Có node không kết nối được hoặc quá hạn `-ConnectTimeoutSeconds`. Các node còn lại vẫn được đối chiếu với nhau, nhưng kết luận tổng không phải `CONSISTENT`. |
| `CHECK_FAILED` | `5` | Có node vi phạm bất biến cục bộ (`CHECK\|*\|FAIL`), thiếu index `ux_one_active_ticket_per_seat`, hoặc định nghĩa index khác nhau giữa các node. |
| `USAGE_ERROR` | `6` | Thiếu `psql.exe`, sai tham số đầu vào, hoặc thiếu biến môi trường mật khẩu. |

## 4. Giới hạn

- **Không thay thế kiểm chứng Raft log/quorum:** Tool chỉ đối chiếu state-machine database (`seats`, `tickets`, `request_results`, `raft_apply_progress`) và index cục bộ; không đọc `raft_log` hay `raft_meta`.
- **Trạng thái hiện tại:** Đã kiểm chứng ở mức **LOCAL** (5 container PostgreSQL D25 + fixture schema phụ). Việc chạy thực tế sau fault thật tại cụm 5 node (**G6**) đang ở trạng thái `BLOCKED_INTEGRATION` do chưa có cụm chạy thật của TV4/TV5/TV6.

