# TV3-D25: LAN Connectivity Preflight Trước Khi Launch Toàn Cụm & MessageDispatcher Peer Hardening

## 1. Thông tin Task
- **Mã task:** `tv3-d25`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Phối hợp:** TV6 (Inventory D24, LAN Deploy D25, Gate G6)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `02-ARCHITECTURE.md` (§5), `03-CONTRACTS.md`, `07-TESTING.md` (`TC-TCP-04`, `TC-DEPLOY-01..03`), `08-DEPLOYMENT.md` (§3, §3.1, §6, §9), `configs/lan/PORTS.md`
- **Ngày hoàn thành phần host sẵn có:** 25/11/2026

## 2. Mục tiêu kỹ thuật & Ranh giới Nghiệm thu
1. **Tiền kiểm TCP/Firewall trên host sẵn có (không chờ TV6-D25 deploy):**
   - Kiểm tra khả năng bind cổng `9100` (Client) và `9200` (Raft Peer) trên host hiện có.
   - Chạy kiểm thử tích hợp TCP thật qua `NodeMain` và `TcpClusterSmokeTest` (5 Node, 10 cổng tách biệt, `PING -> PONG`, `NODE_STATUS`, Raft RPC mesh, `TC-TCP-04`).
2. **Siết chặt ranh giới kênh & Peer Allowlist trong `MessageDispatcher.java` (SPEC-08 §3.1):**
   - Bổ sung kiểm tra `allowedPeerIds` tuỳ chọn trên kênh `PEER` (`9200`) để từ chối bản tin từ `senderId` lạ hoặc tự mạo danh `nodeId` bản thân bằng `ERROR_RESPONSE(INVALID_REQUEST)`.
   - Ghi rõ theo `SPEC-08 §3.1`: kiểm tra `senderId` là configuration sanity guard ở tầng dispatch, **không** quảng cáo là xác thực mật mã; bảo vệ mức host do firewall cổng `9200` đảm nhiệm.
   - Bổ sung helper chuẩn hoá `isStandardClientPort(int)` và `isStandardPeerPort(int)`.
3. **Hoàn thiện kịch bản `scripts/lan-connectivity-check.ps1` cho Gate G6:**
   - Thêm tham số `-InventoryFile` đọc danh bạ IP thật từ TV6-D24 (`NODE_PEERS` hoặc `lan.node.node-X.*`).
   - Hoàn thiện nhánh `-Mode LiveProbe`:
     - Fail-closed nếu phát hiện IP trong danh bạ vẫn là placeholder (`192.168.10.11..15`, `192.168.1.101..105`) — xuất `REAL_IP_INVENTORY=MISSING_PLACEHOLDER_DETECTED` và `PHYSICAL_5_LAPTOP_CLUSTER=NOT_RUN (DEFERRED_TO_G6)`.
     - Khi chạy với IP thật của 5 Laptop: kiểm tra raw frame `PING -> PONG` trên cổng `9100`, kết nối TCP socket trên cổng `9200`, kiểm tra Windows Firewall và tổng kết số node đạt quorum (`>= 3/5`, baseline `5/5`).
4. **Ranh giới trung thực (AGENTS.md §0, §5, §6):**
   - Tại thời điểm thực hiện `tv3-d25`, repo **chưa có file inventory IP thật từ TV6-D24** và **chưa có 5 laptop vật lý đang chạy** trên mạng LAN.
   - Do đó, phần preflight host + 5-node local TCP smoke đạt `PASS`, còn phần probe 5 laptop LAN vật lý giữ nguyên trạng thái **`NOT_RUN / DEFERRED_TO_G6`**.

## 3. Các thay đổi đã thực hiện
- [`backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java):
  - Bổ sung `isStandardClientPort(int port)` và `isStandardPeerPort(int port)`.
  - Bổ sung `allowedPeerIds` (`getAllowedPeerIds()`, `setAllowedPeerIds(Set<String>)`) và logic từ chối `senderId` không hợp lệ/tự gửi trên `ChannelType.PEER`.
- [`scripts/lan-connectivity-check.ps1`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/scripts/lan-connectivity-check.ps1):
  - Bổ sung `-InventoryFile`, `-AllowPlaceholderProbe`, `Get-InventoryEndpoints`, `Test-RawPingFrame`, và triển khai đầy đủ nhánh `-Mode LiveProbe`.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/LanLivePreflightAndIsolationTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/LanLivePreflightAndIsolationTest.java):
  - 4 ca kiểm thử JUnit: port helpers (`TC-D25-01`), peer allowlist trên kênh `PEER` (`TC-D25-02`), TCP thật `NodeMain` channel isolation `TC-TCP-04` + `PING/PONG` + `NODE_STATUS` (`TC-D25-03`), và kiểm tra interface + bind cổng `9100`/`9200` trên host (`TC-D25-04`).
- [`backend-node/src/test/java/vn/nhom6/railway/networking/LanConnectivityCheckScriptTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/LanConnectivityCheckScriptTest.java):
  - Cập nhật kiểm tra tham số `-InventoryFile`, nhánh `LiveProbe`, `Get-InventoryEndpoints`, `Test-RawPingFrame`, và ràng buộc fail-closed với IP placeholder.
- [`artifacts/evidence/D25/TV3-D25-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D25/TV3-D25-result.log):
  - Tổng hợp bằng chứng thực thi thực tế trên host sẵn có và ranh giới Gate G6.

## 4. Kết quả kiểm thử đã chạy
- **Targeted Networking & Preflight Suite (`28/28 PASS`):**
  ```bash
  mvn -B -ntp -o -Daether.syncContext.named.factory=noop test -pl backend-node -am \
    -Dtest="LanLivePreflightAndIsolationTest,TcpClusterSmokeTest,LanConfigPreflightTest,LanConnectivityCheckScriptTest,NodeMainTest" \
    -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - `LanConfigPreflightTest`: `7/7 PASS` (`0.185 s`)
  - `LanConnectivityCheckScriptTest`: `7/7 PASS` (`0.027 s`)
  - `LanLivePreflightAndIsolationTest`: `4/4 PASS` (`0.380 s`)
  - `NodeMainTest`: `4/4 PASS` (`0.674 s`)
  - `TcpClusterSmokeTest`: `6/6 PASS` (`0.302 s`)
- **Full 6-Module Reactor Regression (`BUILD SUCCESS`, `0` failures, `0` errors):**
  - `mvn -B -ntp -o -Daether.syncContext.named.factory=noop test`
  - `shared`: `38/38` (`0` skipped)
  - `backend-node`: `1304` run, `0` failed, `146` skipped (PostgreSQL/Docker-gated)
  - `desktop-client`: `273` run, `0` failed, `17` skipped
  - `desktop-monitor`: `12/12` (`0` skipped)
  - `integration-tests`: `24` run, `0` failed, `3` skipped (PostgreSQL/Docker-gated)

## 5. Hướng dẫn chạy tiếp tại Gate G6 (khi có IP thật từ TV6-D24 & deploy TV6-D25)
1. Lưu bảng IP thật của TV6-D24 vào file tạm cục bộ (không commit IP cá nhân nếu quy định bảo mật nhóm yêu cầu), ví dụ `inventory-live.env`:
   ```dotenv
   NODE_PEERS=node-1@<IP_1>:9200,node-2@<IP_2>:9200,node-3@<IP_3>:9200,node-4@<IP_4>:9200,node-5@<IP_5>:9200
   ```
2. Sau khi mở firewall và khởi động Node 1..5 trên 5 laptop, chạy từ Laptop 6 (Monitor/Client):
   ```powershell
   .\scripts\lan-connectivity-check.ps1 -Mode LiveProbe -InventoryFile inventory-live.env -CheckFirewall -LogPath artifacts/evidence/D25/TV3-D25-g6-live.log
   ```
3. Tiếp tục nghiệm thu bầu cử và đặt vé 5 Node LAN tại Gate G6.
