# TV6-D23: Docker + Monitor + Integration — Hiển thị recovery & lịch sử sự kiện trên Cluster Monitor

## 1. Thông tin Task
- **Mã task:** `TV6-D23`
- **Tên nhiệm vụ:** Hiển thị recovery trên Monitor (`MonitorEventLog.java` + tích hợp `MonitorController`, `StatusPoller`, `MonitorTcpClient`, `main.fxml`)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 9, 11)
  - `docs/specs/01-REQUIREMENTS.md` (`FR09`, `FR12`; `AC07`, `AC08`)
  - `docs/specs/03-CONTRACTS.md` (§5 `NODE_STATUS` / `NodeStatusDto` — TV3-D15)
  - `docs/specs/06-DESKTOP-UI.md` (§7.1 Vùng thông tin bắt buộc: không gộp `lastLogIndex`/`commitIndex`/`lastApplied`, nhật ký lọc được, cảnh báo lag/đổi Leader; §7.2 Monitor không bỏ phiếu)
- **Phụ thuộc & Tích hợp:**
  - Kế thừa Monitor **TV6-D13/D14** (`MonitorController`, `StatusPoller`, `MonitorTcpClient`, `main.fxml`) và dùng trực tiếp `NodeStatusDto` từ module `shared` (**TV3-D15**).
  - **Độc lập, không chờ TV5-D23:** Kiểm chứng bằng replay recorded/fake recovery sequences và `FakeNodeStatusAdapter`; trực quan hóa real leader rejoin 5 node thực tế thuộc **Gate G6**.

---

## 2. Thiết kế & Bất biến hiển thị

### 2.1. `MonitorEventLog` (`desktop-monitor/src/main/java/vn/nhom6/railway/monitor/MonitorEventLog.java`)
- **Phân loại sự kiện (`EventType`):**
  - `LEADER_CHANGE`: bầu Leader mới, chuyển Leader (`node-1 -> NONE -> node-3`), mất Leader, hoặc cảnh báo nhiều Leader đồng thời.
  - `NODE_OFFLINE`: node chuyển từ `ONLINE` sang `OFFLINE`/`TIMEOUT`/`STORAGE_ERROR`/`UNKNOWN`.
  - `NODE_ONLINE`: node quay lại `ONLINE` (ghi rõ đã khớp hay đang catch-up, không tự gắn nhãn `SYNCED`).
  - `CATCH_UP_PROGRESS`: node đang bắt kịp (`lastApplied < commitIndex`, hoặc `commitIndex < leaderCommitIndex`, hoặc `term < leaderTerm`), phát sự kiện mỗi lần `lastApplied`/`commitIndex` tiến triển mà chưa kịp.
  - `CATCH_UP_COMPLETE`: chỉ phát khi node trước đó đang `CATCHING_UP`/`OFFLINE` đã thực sự bắt kịp hoàn toàn (`SYNCED`).
- **Quy tắc nghiêm ngặt cho nhãn `SyncState` (không gắn nhãn `SYNCED` trước khi bắt kịp):**
  - `OFFLINE`: node không `ONLINE` (`OFFLINE`, `TIMEOUT`, `STORAGE_ERROR`, `UNKNOWN`).
  - `UNKNOWN_PROGRESS`: thiếu tham chiếu Leader duy nhất khỏe mạnh (0 Leader hoặc >1 Leader split-brain), hoặc dữ liệu bất thường (`lastApplied > commitIndex` hoặc `null`).
  - `CATCHING_UP`: `lastApplied < commitIndex` cục bộ, HOẶC `commitIndex < leader.commitIndex` (kể cả khi cục bộ `lastApplied == commitIndex`!), HOẶC `term < leader.term`.
  - `SYNCED`: **CHỈ KHI** `ONLINE` + có đúng 1 Leader khỏe mạnh + `term == leader.term` + `lastApplied == commitIndex >= leader.commitIndex`.
- **Hiển thị `lastApplied` so với `commitIndex` (`applyVsCommitDisplay`):**
  - Giữ riêng 2 cột `commitIndex` và `lastApplied` (SPEC-06 §7.1), đồng thời bổ sung cột so sánh tường minh `lastApplied / commitIndex` (ví dụ `41 / 43 (lag -2)`, `43 / 43 (lag -7 vs leader 50)`, `50 / 50 (SYNCED)`, `— / —`) và cột `syncState`.
- **Tích hợp UI (`MonitorController`, `StatusPoller`, `main.fxml`):**
  - `StatusPoller` đẩy snapshot vào `controller.applySnapshot(rows)` trên JavaFX Application Thread, tự động cập nhật `nodeTable`, `eventTable` và `recoverySummaryLabel`.
  - Giữ tương thích ngược với D13/D14 (`NodeRow` không đổi signature; các control mới null-safe).

---

## 3. Kết quả kiểm thử đã chạy

- **Command:**
  ```bash
  mvn -B -ntp -pl desktop-monitor -am test -Dtest="MonitorEventLogTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
- **Result:** **PASS** — `Tests run: 10, Failures: 0, Errors: 0, Skipped: 0` (`0.155 s`, `BUILD SUCCESS`).
- **Evidence:** `artifacts/evidence/D23/TV6-D23-result.log`.
- **Phạm vi còn lại:** Kiểm thử hiển thị real leader rejoin live trên JavaFX Desktop kết nối cụm 5 Node thật sẽ thực hiện tại **Gate G6**.
