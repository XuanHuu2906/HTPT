# TV3-D24: LAN Preflight Script Kiểm Tra Cổng, Tường Lửa & Endpoint (lan-connectivity-check.ps1)

## 1. Thông tin Task
- **Mã task:** `tv3-d24`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Phối hợp:** TV6 (Deployment & Scripting / TV6-D25)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§5 TCP Socket, §7 Failure recovery), `07-TESTING.md` (TC-TCP-04, Preflight), `08-DEPLOYMENT.md` (§1, §2, §3, §3.1, §6), `configs/lan/PORTS.md`
- **Ngày hoàn thành:** 24/11/2026

## 2. Mục tiêu kỹ thuật & Tiêu chí Nghiệm thu
1. **Xây Dựng Kịch Bản Tiền Kiểm LAN (`scripts/lan-connectivity-check.ps1`):**
   - Đọc và thẩm định cấu hình của cả 5 file `configs/lan/node-1..5.properties`:
     - Cổng Client cố định `9100` và cổng Raft Peer cố định `9200` theo **SPEC-08 §3**.
     - Danh bạ `node.peers` bắt buộc chứa đủ 5 voter (`node-1` .. `node-5`) trên cổng 9200, tương thích parser `ElectionRuntime.parsePeers()`.
     - Phân lập cơ sở dữ liệu độc lập cho từng node (`railway_node1` .. `railway_node5`).
   - Kiểm tra khả năng mở/bind của cổng TCP 9100 và 9200 trên máy trạm cục bộ.
   - Tiền kiểm quy tắc tường lửa (Windows Firewall Inbound TCP 9100/9200) và xuất câu lệnh khắc phục nếu thiếu rule.
2. **Tuân Thủ Ràng Buộc Báo Cáo An Toàn (AGENTS.md §0, §5, §6):**
   - Nhận diện dải địa chỉ IP placeholder (`192.168.10.11`..`15` / `192.168.1.101`..`105`).
   - Khẳng định rõ ràng: **Port/firewall endpoint preflight kiểm được local, không claim 5 laptop kết nối thành công**.
   - Việc kết nối và đo kiểm xuyên suốt 5 laptop vật lý thuộc phạm vi Gate G6 sau khi TV6-D25 deploy.
3. **Đa Dạng Chế Độ Thực Thi (`-Mode`):**
   - `-Mode Preflight` (Mặc định): Kiểm tra cú pháp, danh bạ 5 voter, port bind, và firewall.
   - `-Mode LocalCheck`: Kiểm tra kết nối TCP socket thực tế đến cổng 9100/9200 cục bộ.
   - `-Mode SelfTest`: Tự kiểm tra các hàm phân tích cấu hình, phát hiện lỗi thiếu voter hoặc sai port.
   - `-Mode LiveProbe`: Thử nghiệm socket probe cho ngày chạy LAN thực tế D25.

## 3. Các thay đổi đã thực hiện

### 3.1. Thư mục `scripts/`
- [`scripts/lan-connectivity-check.ps1`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/scripts/lan-connectivity-check.ps1):
  - Kịch bản PowerShell tiền kiểm toàn diện với đầy đủ các hàm:
    - `Get-PropertiesFileContent`: Đọc và phân giải cấu hình Java properties.
    - `Test-NodeConfigSyntax`: Thẩm định `node.id`, `cluster.id`, `client.port=9100`, `raft.port=9200`.
    - `Test-NodePeersFormat`: Xác minh danh bạ 5 voter không trùng lặp trên port 9200.
    - `Test-PortBindAvailability`: Kiểm tra tính khả dụng mở cổng socket 9100 và 9200.
    - `Test-FirewallPreflight`: Kiểm tra rules tường lửa Inbound trên Windows Firewall và hướng dẫn mở cổng.
    - `Test-SocketConnection`: Kiểm tra kết nối TCP socket timeout.
    - `Invoke-SelfTest`: Bộ kiểm thử nội bộ tự kiểm tra 5 ca testcase giả định.

### 3.2. Module `backend-node`
- [`backend-node/src/test/java/vn/nhom6/railway/networking/LanConnectivityCheckScriptTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/LanConnectivityCheckScriptTest.java):
  - Bộ 7 ca kiểm thử tự động kiểm tra kịch bản `scripts/lan-connectivity-check.ps1`:
    1. `testScriptFileExistsAndIsNotEmpty`: File script tồn tại và không rỗng.
    2. `testScriptHeaderAndParameterDefinitions`: Đầy đủ tham số (`-ConfigFile`, `-AllNodes`, `-Mode`, `-LogPath`, v.v.).
    3. `testScriptEnforcesStandardPorts9100And9200`: Kiểm tra ép chuẩn cổng 9100 và 9200.
    4. `testScriptEnforcesFiveVoterQuorumRule`: Kiểm tra ép chuẩn 5 voter `node-1..5`.
    5. `testScriptContainsFirewallCheckLogic`: Kiểm tra logic truy vấn Windows Firewall.
    6. `testScriptCompliesWithNoPhysicalClaimInvariant`: Kiểm tra ràng buộc AGENTS.md không claim kết nối 5 laptop vật lý.
    7. `testAllFiveLanPropertiesValidAgainstScriptRules`: Kiểm tra cả 5 file cấu hình thật đạt chuẩn script.

### 3.3. File bằng chứng nghiệm thu
- [`artifacts/evidence/D24/TV3-D24-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D24/TV3-D24-result.log):
  - Ghi vết toàn bộ quá trình chạy tiền kiểm trên cả 5 file cấu hình `node-1..5.properties`.
  - Khẳng định kết quả: Cú pháp PASS, Cổng 9100/9200 PASS, Danh bạ 5 voter PASS, Khả năng bind cổng PASS, Chế độ IP PLACEHOLDER, Kết nối 5 Laptop vật lý: NOT_RUN / DEFERRED_TO_D25.

## 4. Kết quả kiểm thử thực tế

- **Kiểm thử tự động Script (`LanConnectivityCheckScriptTest`):**
  ```bash
  mvn -B -ntp test -Dtest=LanConnectivityCheckScriptTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.112 s` (PASS 100%).

- **Kiểm thử tổ hợp các bài test cấu hình & mạng:**
  ```bash
  mvn -B -ntp test -Dtest=LanConnectivityCheckScriptTest,LanConfigPreflightTest,NodeMainTest,PeerConnectionManagerTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 24, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.367 s` (PASS 100%).

- **Chạy thực tế công cụ tiền kiểm Preflight:**
  - Kết quả ghi nhận tại: [`artifacts/evidence/D24/TV3-D24-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D24/TV3-D24-result.log).
  - Tóm tắt kết quả:
    - `Cú pháp cấu hình (configs/lan/node-1..5.properties) : [PASS]`
    - `Quy chuẩn cổng mạng (Client: 9100, Peer: 9200)       : [PASS]`
    - `Định dạng danh bạ cụm (Đủ 5 voter node-1..node-5)   : [PASS]`
    - `Chế độ địa chỉ IP                                   : [PLACEHOLDER_MODE]`
    - `Trạng thái kết nối 5 Laptop vật lý                   : [NOT_RUN / DEFERRED_TO_D25]`

## 5. Bàn giao cho TV6-D25
- Kịch bản `scripts/lan-connectivity-check.ps1` sẵn sàng cho TV6 sử dụng để tiền kiểm các laptop trước khi khởi động consensus Raft trong buổi demo D25:
  ```powershell
  # Kiểm tra toàn bộ 5 cấu hình và firewall trên máy:
  .\scripts\lan-connectivity-check.ps1 -AllNodes -CheckFirewall
  ```
