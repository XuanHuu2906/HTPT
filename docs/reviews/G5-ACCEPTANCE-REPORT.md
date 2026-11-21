# BIÊN BẢN NGHIỆM THU CHỐT MỐC G5 (GATE 5)
## HỆ THỐNG BÁN VÉ TÀU CAO TỐC PHÂN TÁN — NHÓM 6

> **Thời điểm chốt mốc:** 20/11/2026 · 19:48 (Asia/Saigon)  
> **Cơ sở dữ liệu nhiệm vụ:** `NHÓM 6 — TASK ĐÃ MỞ CHO THÀNH VIÊN` (120/120 task D01–D20 hoàn thành 100%)  
> **Bộ tài liệu đặc tả tham chiếu:** `docs/specs/00-MASTER.md` (§7 Luồng phát triển theo mốc), `01-REQUIREMENTS.md`, `04-RAFT.md`, `05-BOOKING-DATABASE.md`  
> **Quy tắc nghiệm thu chung của mốc G5:** **"3/5 commit hợp lệ · 2/5 dừng ghi · 6 Client tranh chấp đúng 1 vé"**

---

### I. TỔNG QUAN KẾT QUẢ NGHIỆM THU

| Hạng mục | Chỉ tiêu yêu cầu | Kết quả thực tế | Đánh giá |
|---|---|---|:---:|
| **Số lượng task hoàn thành** | 120 / 120 tasks (D01 – D20) | 120 / 120 tasks | **ĐẠT (100%)** |
| **Thành viên tham gia** | Đủ 6/6 vai trò (TV1 – TV6) | 6/6 thành viên có đầy đủ code & log | **ĐẠT** |
| **Bất biến Zero Double-Booking (AC03)** | 6 Client tranh 1 ghế $\rightarrow$ 1 vé duy nhất | Đúng 1 CONFIRMED, 5 REJECTED (`SEAT_ALREADY_BOOKED`) | **ĐẠT** |
| **Quorum Raft Đa số 3/5 (AC05B)** | Khi còn $\ge 3/5$ Node, hệ thống ghi vé bình thường | Commit an toàn, đạt đa số, áp dụng DB thành công | **ĐẠT** |
| **An toàn Thiểu số 2/5 (AC06)** | Khi cụm còn $\le 2/5$ Node, dừng ghi an toàn | Báo `NO_QUORUM`, không commit, không bán trùng | **ĐẠT** |
| **Toàn vẹn CSDL (5 DBs)** | 5 DB độc lập, không dùng shared DB giả lập | Kiểm tra `state_hash` & 10 ghế nhất quán | **ĐẠT** |
| **Tình trạng Reactor Build & Tests** | Toàn bộ 6 module Maven biên dịch & test | `BUILD SUCCESS` (0 failures, 0 errors) | **ĐẠT** |

---

### II. BẢNG ĐỐI CHIẾU ĐẦU RA 6 THÀNH VIÊN TẠI MỐC G5

| Thành viên | Phân hệ phụ trách | Nhiệm vụ mốc G5 | Sản phẩm đầu ra & Bằng chứng nghiệm thu | Kết luận |
|---|---|---|---|:---:|
| **Khoa (TV1)** | JavaFX Desktop Client | Chốt giao diện Booking qua Raft, kiểm thử UI khi cụm 3/5 và 2/5 | • `desktop-client/.../BookingUiAcceptanceTest.java` (9/9 test cases PASS)<br>• Log nghiệm thu: `artifacts/evidence/D20/TV1-D20-result.log`<br>• Ảnh bằng chứng: `artifacts/evidence/D20/TV1-D20-ui.png` | **PASS** |
| **Phạm Minh Tuấn (TV2)** | Booking & Database Persistence | Kiểm tra đối chiếu tính nhất quán 5 DB sau commit | • Script kiểm tra: `database/compare-db.sql`<br>• Log đối chiếu 5 DB: `artifacts/evidence/D20/TV2-D20-result.log`<br>• Bất biến 10 ghế SE-001, `last_applied` và `state_hash` đồng nhất | **PASS** |
| **Nguyễn Đức Thuận (TV3)** | Networking & TCP Transport | Load test TCP/Socket lifecycle, cô lập slow client, chống leak socket | • Bộ kiểm thử tải: `TcpLoadTest.java` (5/5 kịch bản PASS 100%)<br>• Log tải: `artifacts/evidence/D20/TV3-D20-targeted.log`<br>• Ghi chú: `docs/implementation/TV3-D20-notes.md` | **PASS** |
| **Zun (TV4)** | Raft Replication & Commit Engine | Nghiệm thu bất biến sao chép và commit đa số Figure 8 | • Bộ test bất biến: `ReplicationCommitInvariantSuiteTest.java`<br>• Log nghiệm thu: `artifacts/evidence/D20/TV4-D20-result.log`<br>• Log chi tiết: `artifacts/evidence/D20/TV4-D20-full-raw.log` | **PASS** |
| **Nguyễn Xuân Hữu (TV5)** | Raft Election, Safety & Quorum | Nghiệm thu Bầu cử Leader, Quorum 3/5 và dừng ghi khi 2/5 | • Service điều phối: `RaftElectionService.java`<br>• Test suite 5 nodes: `RaftElectionServiceTest.java` (12 kịch bản fault)<br>• Log nghiệm thu: `artifacts/evidence/D20/TV5-D20-result.log` | **PASS** |
| **Trần Hoàng Đạt (TV6)** | Docker, Monitor & QA Integration | Runner nghiệm thu G5, tự động hóa kịch bản lỗi phân tán | • Script điều phối: `scripts/g5-acceptance.ps1`<br>• Log dry-run & self-test: `artifacts/evidence/D20/TV6-D20-result.log` | **PASS** |

---

### III. KIỂM CHỨNG CÁC BẤT BIẾN KỸ THUẬT (SYSTEM INVARIANTS)

1. **Bất biến Zero Double-Booking (Không bán trùng ghế):**
   - Đã kiểm chứng qua bộ kịch bản 6 Client tranh chấp cùng ghế `G05` (`ConcurrentBookingRunner` & `SixClientAutopilot`).
   - Kết quả: Duy nhất 1 yêu cầu tới trước được cấp vé `CONFIRMED`, 5 yêu cầu còn lại nhận `REJECTED (SEAT_ALREADY_BOOKED)`. CSDL ghi nhận đúng 1 dòng vé `ACTIVE`.
2. **Bất biến Commit theo Raft Term (Figure 8 Safety):**
   - Leader không bao giờ trực tiếp commit một entry từ term cũ bằng cách đếm số bản sao; chỉ commit khi entry thuộc currentTerm của Leader được nhân bản lên đa số ($\ge 3/5$).
3. **Bất biến An toàn Phân vùng Mạng (Minority Safety):**
   - Khi mạng bị chia cắt thành 2 phân vùng (3 Node vs 2 Node):
     - Phía thiểu số (2 Node) từ chối ghi (`NO_QUORUM`), không bao giờ chuyển trạng thái sang `CONFIRMED`.
     - Phía đa số (3 Node) bầu Leader mới hợp lệ và tiếp tục xử lý giao dịch.
     - Khi hàn gắn mạng (heal partition), Leader cũ tự động giáng chức xuống Follower thông qua `LeadershipGuard`, log term cũ chưa commit bị ghi đè an toàn.
4. **Bất biến Idempotency:**
   - Client gửi lại lệnh cùng `requestId` (hoặc tra cứu qua `GET_REQUEST_RESULT`) luôn nhận lại đúng kết quả vé ban đầu, không trừ tiền hay chiếm ghế lần hai.

---

### IV. QUYẾT ĐỊNH CHỐT MỐC (SIGN-OFF DECISION)

> **XÁC NHẬN CHÍNH THỨC:**
> Căn cứ vào tiến độ 120/120 tasks đã hoàn thành và phê duyệt trên bảng kế hoạch, cùng toàn bộ mã nguồn và bằng chứng kiểm thử đạt chuẩn trong repository:
> **MỐC G5 (NGÀY 20) ĐƯỢC CÔNG NHẬN CHÍNH THỨC HOÀN THÀNH — ĐẠT ĐIỀU KIỆN NGHIỆM THU (PASS).**

---

### V. KẾ HOẠCH BÀN GIAO TIẾP THEO (TUẦN 5: NGÀY 21 $\rightarrow$ NGÀY 30)

* **Mốc G6 (Ngày 26):** Triển khai mạng LAN vật lý thật trên **6 máy tính xách tay** (5 máy chạy Backend Node + 5 PostgreSQL riêng, 1 máy chạy Desktop Client / Monitor).
* **Mốc G7 (Ngày 28):** Đóng băng toàn bộ mã nguồn (Code Freeze), xử lý dứt điểm các lỗi P0/P1, đóng gói Release Candidate.
* **Mốc G8 (Ngày 30):** Tổng duyệt kịch bản bảo vệ đồ án, quay video minh chứng, xuất báo cáo tổng kết và bàn giao đồ án.
