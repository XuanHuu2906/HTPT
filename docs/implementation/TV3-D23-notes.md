# TV3-D23: Soạn Config LAN Theo SPEC-08 Bằng IP Placeholder & Bảng Ánh Xạ PORTS.md

## 1. Thông tin Task
- **Mã task:** `tv3-d23`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md`, `02-ARCHITECTURE.md` (§5 TCP Socket, §7 Failure recovery), `03-CONTRACTS.md`, `07-TESTING.md` (TC-TCP-04), `08-DEPLOYMENT.md` (§1, §2, §3, §3.1, §6)
- **Ngày hoàn thành:** 23/11/2026

## 2. Mục tiêu kỹ thuật & Tiêu chí Nghiệm thu
1. **Soạn Cấu Hình Mẫu LAN 5 Node (`configs/lan/node-1..5.properties`):**
   - Đảm bảo mỗi Node có một file cấu hình độc lập cho việc triển khai trên 5 Laptop vật lý.
   - Cố định cổng giao tiếp theo **SPEC-08 §3**: Client Port = `9100`, Raft Peer Port = `9200` trên toàn bộ 5 Laptop.
   - Sử dụng địa chỉ IP **placeholder** (`192.168.10.11`..`15` theo SPEC-08 và `192.168.1.101`..`105` theo `client.properties`). Tuyệt đối **không hardcode IP thật** của các thành viên vào Git (TV6-D24 sẽ thu thập IP thực địa).
   - Bảo đảm nguyên tắc phân lập dữ liệu PostgreSQL (TV2 DB isolation): Mỗi node có tên DB riêng (`railway_node1` .. `railway_node5`). Duy trì `db.port=5433` cho `node-1.properties` nhằm đảm bảo tương thích ngược $100\%$ với bài test `NodeMainTest`.
2. **Xây Dựng Tài Liệu Đặc Tả Cổng Mạng & Topology (`configs/lan/PORTS.md`):**
   - Lập bảng ma trận ánh xạ 6 Laptop: Laptop 1–5 là 5 voter của Raft (Quorum $3/5$), Laptop 6 là non-voter (Client + Monitor).
   - Quy định ranh giới kênh và phân tách cổng (Channel / Port Isolation) theo **SPEC-08 §3.1** và **TC-TCP-04**: Cổng 9100 chỉ nhận yêu cầu Client, từ chối Raft RPC; Cổng 9200 chỉ nhận Raft RPC, từ chối yêu cầu Client.
   - Định dạng chuẩn chuỗi `NODE_PEERS` tương thích với `ElectionRuntime.parsePeers()`.
   - Hướng dẫn cấu hình Firewall Inbound/Outbound và quy trình bàn giao cho TV6-D24.
3. **Cập Nhật `MessageDispatcher.java` (Đầu ra code):**
   - Bổ sung hằng số `STANDARD_CLIENT_PORT = 9100`, `STANDARD_PEER_PORT = 9200`.
   - Bổ sung helper `resolveChannelForPort(int port)` hỗ trợ chuẩn hóa phân giải kênh `CLIENT` vs `PEER` cho cổng LAN và cổng dev published.
4. **Kiểm Thử Tiền Kiểm Preflight (`LanConfigPreflightTest.java`):**
   - Kiểm tra toàn bộ 5 file cấu hình, định dạng peer, tài liệu PORTS.md, và logic phân giải kênh.
   - Tuân thủ nghiêm ngặt **AGENTS.md §0, §5, §6**: Cấu hình mẫu dùng placeholder, preflight syntax PASS; ghi rõ chưa chạy kiểm thử TCP trên cụm 5 laptop vật lý (chờ TV6-D24 / G6).

## 3. Các thay đổi đã thực hiện

### 3.1. Thư mục `configs/lan/`
- [`configs/lan/node-1.properties`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/node-1.properties): Chuẩn hóa cấu hình Node 1 với `node.id=node-1`, `client.port=9100`, `raft.port=9200`, `db.port=5433`, các trường advertise và chuỗi danh bạ `node.peers` placeholder.
- [`configs/lan/node-2.properties`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/node-2.properties): Cấu hình Node 2 (`node.id=node-2`, `client.port=9100`, `raft.port=9200`, `db.name=railway_node2`, `db.port=5434`).
- [`configs/lan/node-3.properties`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/node-3.properties): Cấu hình Node 3 (`node.id=node-3`, `client.port=9100`, `raft.port=9200`, `db.name=railway_node3`, `db.port=5435`).
- [`configs/lan/node-4.properties`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/node-4.properties): Cấu hình Node 4 (`node.id=node-4`, `client.port=9100`, `raft.port=9200`, `db.name=railway_node4`, `db.port=5436`).
- [`configs/lan/node-5.properties`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/node-5.properties): Cấu hình Node 5 (`node.id=node-5`, `client.port=9100`, `raft.port=9200`, `db.name=railway_node5`, `db.port=5437`).
- [`configs/lan/PORTS.md`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/configs/lan/PORTS.md): Tài liệu hướng dẫn toàn diện: ma trận 6 Laptop, quy tắc channel isolation trên cổng 9100/9200, cú pháp `NODE_PEERS`, hướng dẫn firewall, so sánh dev vs LAN, và checklist bàn giao cho TV6-D24.

### 3.2. Module `backend-node`
- [`backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/MessageDispatcher.java):
  - Khai báo hằng số `STANDARD_CLIENT_PORT = 9100` và `STANDARD_PEER_PORT = 9200`.
  - Bổ sung phương thức `resolveChannelForPort(int port)` phân giải cổng thành `ChannelType.CLIENT` (9100, 9101..9105) hoặc `ChannelType.PEER` (9200, 9201..9205).
- [`backend-node/src/test/java/vn/nhom6/railway/networking/LanConfigPreflightTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/LanConfigPreflightTest.java):
  - Bộ 7 ca kiểm thử tiền kiểm LAN config:
    1. `testAllFiveLanPropertiesFilesExist`: Xác nhận cả 5 file properties tồn tại và đọc được.
    2. `testNodeConfigParsesAllFiveNodes`: Đảm bảo `NodeConfig.load()` parse thành công 5 node với client port 9100 và peer port 9200.
    3. `testSeparateDatabaseConfigurationPerNode`: Xác nhận 5 DB độc lập (`railway_node1` .. `railway_node5`).
    4. `testNodePeersFormatCompatibleWithElectionRuntime`: Kiểm tra chuỗi `node.peers` tương thích hoàn toàn với `ElectionRuntime.parsePeers()`, trả về đúng 5 voter.
    5. `testPortsDocumentationExistsAndCoversSpec`: Kiểm tra tài liệu `PORTS.md` bao phủ đủ các nội dung đặc tả.
    6. `testMessageDispatcherPortChannelResolution`: Kiểm tra ánh xạ cổng sang kênh `CLIENT` và `PEER`.
    7. `testExplicitLanPreflightAssertionNotClaimingPhysicalCluster`: Khẳng định rõ hệ thống đang chạy ở chế độ placeholder preflight, chưa báo PASS trên cụm 5 Laptop vật lý.

## 4. Kết quả kiểm thử thực tế

- **Kiểm thử có chủ đích (`LanConfigPreflightTest`):**
  ```bash
  mvn -B -ntp test -Dtest=LanConfigPreflightTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tee artifacts/evidence/D23/TV3-D23-targeted.log
  ```
  - Kết quả: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.159 s` (PASS 100%).
  - Bằng chứng: [`artifacts/evidence/D23/TV3-D23-targeted.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D23/TV3-D23-targeted.log).

- **Kiểm thử tổ hợp mạng & cấu hình:**
  ```bash
  mvn -B -ntp test -Dtest=LanConfigPreflightTest,NodeMainTest,PeerConnectionManagerTest,TestNetworkFilterTest -Dsurefire.failIfNoSpecifiedTests=false
  ```
  - Kết quả: `Tests run: 24, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.871 s` (PASS 100%).

- **Kiểm thử toàn bộ Reactor (6 Modules):**
  ```bash
  mvn -B -ntp test 2>&1 | tee artifacts/evidence/D23/TV3-D23-result.log
  ```
  - Bằng chứng: [`artifacts/evidence/D23/TV3-D23-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D23/TV3-D23-result.log).

## 5. Rủi ro & Bàn giao TV6-D24
- **Bàn giao:** File `configs/lan/PORTS.md` và 5 file `configs/lan/node-1..5.properties` đã sẵn sàng để TV6-D24 điền địa chỉ IP thật của các máy vào ngày ráp mạng LAN.
- **Rủi ro:** Khi cấu hình IP thật, cần đảm bảo subnet mask và firewall của các máy không chặn cổng Inbound 9100 và 9200.
