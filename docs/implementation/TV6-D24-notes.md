# TV6-D24 — LAN inventory 6 laptop và chuẩn bị triển khai

Ngày thực hiện: 26/11/2026 (Asia/Saigon). Owner: TV6; bàn giao cả nhóm.
Baseline đọc: `aeea4ba7cd40077c2d3c2fceda4090d4ecafe543`.

## Phạm vi và căn cứ

- Đọc `AGENTS.md`, `.agents/rules/agent_self_rules.md`, SPEC-00 → 01 → 02 → 03, SPEC-08 và SPEC-07.
- Đối chiếu TV3-D23: `configs/lan/node-1..5.properties`, `configs/lan/PORTS.md`; đọc bàn giao TV3-D24 để biết định dạng inventory cho lần ráp mạng sau. Collector D24 không phụ thuộc script TV3-D24 hoặc DB TV2-D25.
- SPEC-08 §2/3/3.1/6/8/9; chuẩn bị cho TC-DEPLOY-01/02/03, FR06/FR12. Không xác nhận AC01–AC10 hay LAN cluster.
- Đúng 3 đầu ra: `AcceptanceSuiteTest.java`, `TV6-D24-result.log`, tài liệu này. Giữ nguyên wire contract, cấu hình TV3 và các file khác.

## Inventory thực đo

Chỉ truy cập được máy cục bộ. Người dùng xác nhận mình là TV6; gán máy đang làm việc vào **Laptop 6 theo phân công Client/Monitor**, không phải một node/voter được tự động phát hiện. Hostname và địa chỉ mạng được đo trực tiếp. Dữ liệu dưới đây là snapshot mạng hiện tại, chưa phải IP triển khai tại phòng thực hành. Các IP thực đo được ghi trong minh chứng theo yêu cầu nhiệm vụ; không thay IP mẫu trong `configs/lan`.

| Máy | Vai trò dự kiến theo SPEC-08 | Hostname / IP / mask thực đo | Firewall / Docker | Trạng thái |
|---|---|---|---|---|
| Laptop 1 | node-1 + PostgreSQL riêng | NOT_RUN | NOT_RUN / NOT_RUN | NOT_RUN — chưa truy cập máy |
| Laptop 2 | node-2 + PostgreSQL riêng | NOT_RUN | NOT_RUN / NOT_RUN | NOT_RUN — chưa truy cập máy |
| Laptop 3 | node-3 + PostgreSQL riêng | NOT_RUN | NOT_RUN / NOT_RUN | NOT_RUN — chưa truy cập máy |
| Laptop 4 | node-4 + PostgreSQL riêng | NOT_RUN | NOT_RUN / NOT_RUN | NOT_RUN — chưa truy cập máy |
| Laptop 5 | node-5 + PostgreSQL riêng | NOT_RUN | NOT_RUN / NOT_RUN | NOT_RUN — chưa truy cập máy |
| Laptop 6 (TV6) | JavaFX Client/Monitor, non-voter | HoangDat (OS: HOANGDAT) / Wi-Fi 192.168.1.50 /24 = 255.255.255.0; gateway 192.168.1.1 | Profile đọc được; chi tiết port rules BLOCKED / CLI có, daemon BLOCKED | Thu được một phần; connectivity NOT_RUN |

Quan sát thêm:

- Wi-Fi là adapter vật lý `Up`. VMware VMnet1/2/8 là adapter ảo, không dùng làm IP LAN demo; Ethernet disconnected có APIPA `169.254.*`, không dùng làm endpoint.
- Các profile Windows Firewall đang bật. Đọc `Get-NetFirewallPortFilter -PolicyStore ActiveStore` bị `Access is denied`; không suy diễn thiếu rule hay port đã mở. Collector chỉ chụp rule local port 9100/9200 cụ thể; rule `Any`, range, chương trình, remote scope hoặc policy khác vẫn cần review. Profile/rule hiện diện không chứng minh kết nối xuyên máy.
- Docker CLI 29.4.3, Compose v5.1.4 có sẵn; không kết nối được named pipe `dockerDesktopLinuxEngine`. Images, backend image ID/architecture và export offline: BLOCKED. Không tự khởi động Docker, không pull/build/load image hoặc khởi động cluster trong lần đo này.
- Laptop 6 không cần backend/PostgreSQL/Docker để chạy Client/Monitor; Docker được đo để biết máy này có chuẩn bị bộ image offline được hay không. Không mở inbound peer/DB trên laptop 6.

## Đối chiếu cấu hình TV3-D23

`AcceptanceSuiteTest` dùng trực tiếp `NodeConfig.load()` và `ElectionRuntime.parsePeers()`, kiểm tra 5 ID, cùng một danh bạ peer, client 9100, peer 9200, advertise khớp peer map và 5 tên DB riêng. Đây là kiểm tra cú pháp mẫu, không kiểm tra 5 database đang chạy.

- Danh bạ hiện tại vẫn `192.168.10.11..15`, được TV3 đánh dấu placeholder; không chuyển thành IP thực đo hay tự probe các địa chỉ mẫu.
- NIC Wi-Fi local đang ở `192.168.1.0/24`, khác subnet mẫu `192.168.10.0/24`. Chưa có IP của 5 host khác nên chưa sinh `NODE_PEERS` live.
- Mẫu `db.host=localhost`, host ports `5433..5437` dành cho JVM chạy trên host. Backend trong container cần DB service DNS và port nội bộ 5432 theo cấu hình triển khai tương ứng; không đưa nguyên mẫu host vào container.
- `docker/compose.lan.yml` chưa có tại baseline. `compose.dev.yml` chứa 5 node trên một máy, không thay thế triển khai 1 node + 1 DB trên mỗi laptop.
- Rule tự hành ghi JDK 17/21 nhưng POM hiện có release 25. Kiểm thử dùng JDK 25 theo POM, không sửa build. Dockerfile hiện build/runtime Java 21; kiểm tra build image từ candidate là việc riêng, chưa xác nhận tương thích.

## Cách thu trên từng máy

JUnit mặc định chạy 6 kiểm tra cục bộ cho cấu hình, trạng thái chưa đo, chọn NIC, netmask, command failure/missing/timeout; collector thực bị tắt mặc định. Lệnh dưới dùng Maven/JDK đã chuẩn bị trên mỗi máy, không sửa firewall hoặc kết nối host khác:

```powershell
# JDK theo POM; thay đường dẫn phù hợp trên máy thực tế.
$env:JAVA_HOME = 'C:\path\to\jdk-25'
mvn -B -ntp -pl integration-tests -am '-Dtest=AcceptanceSuiteTest' '-Dsurefire.failIfNoSpecifiedTests=false' test

# Chỉ gán số sau khi người giữ máy xác nhận; ví dụ laptop 6:
mvn -B -ntp -pl integration-tests -am '-Dtest=AcceptanceSuiteTest' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dlan.inventory.local=true' '-Dlan.laptop=6' test
```

Không truyền `lan.laptop` thì host giữ `UNASSIGNED`. Collector in JSON `LOCAL_INVENTORY_BEGIN/END` ra stdout/Surefire, không tạo inventory live giả hoặc sửa file mẫu. Mỗi lệnh con có deadline 20 giây, exit code và lỗi; section lỗi được ghi BLOCKED độc lập. JUnit PASS của collector chứng minh việc thu/báo cáo, không có nghĩa Docker/firewall/LAN sẵn sàng. Khi máy chưa được đo, giữ NOT_RUN.

Sau khi nhận đủ 6 bản đo: xác nhận laptop↔hostname, IP/mask/gateway thực, loại IP trùng/APIPA/adapter ảo, thống nhất mạng demo; thay map trong bản cấu hình triển khai cục bộ, dùng cùng 5 voter trên mọi node. Chỉ khi có đủ địa chỉ mới bàn giao inventory live cho TV3-D24/D25; có thể dùng `NODE_PEERS` và `lan.node.node-X.client.host/port` theo reader hiện có. Không điền địa chỉ mẫu vào bản live.

Kiểm tra thực sau triển khai: peer 9200 chỉ giữa 5 host voter; client 9100 từ Client/Monitor; PostgreSQL riêng không mở cho Client/Monitor. Cần probe từ các host nguồn thật và đối chiếu profile/scope firewall, không chỉ dựa vào local rule. Phần này NOT_RUN trong D24.

## Chuẩn bị bộ offline — kế hoạch và điều kiện chặn

Đã xác định đầu vào, trình tự và tiêu chí kiểm tra; **chưa có tar offline** do Docker daemon không khả dụng. Các lệnh dưới là hướng dẫn CHƯA CHẠY, không phải minh chứng export thành công. Không chờ TV2-D25 hoặc script TV3-D24 để nộp phần chuẩn bị này.

| Thành phần | Nguồn/candidate | Việc cần làm | Kết quả D24 |
|---|---|---|---|
| Backend | Cùng `docker/Dockerfile.backend`, đóng băng SHA khi bàn giao | Build một image, ghi image ID/OS/architecture; dùng cùng image trên 5 node | BLOCKED — daemon unavailable |
| PostgreSQL | `postgres:16-alpine` trong compose.dev baseline | Chốt phiên bản cho LAN với TV2, ghi digest; không ngầm đổi sang 17 của election fixture | BLOCKED — chưa inspect/save được |
| Migration/config | Thư mục `database/migration`, mẫu `configs/lan` cùng candidate | Mang bản cấu hình local và migration đã review; DB/storage vẫn riêng từng node | Chuẩn bị nguồn; chạy DB NOT_RUN |
| JavaFX/JDK/Maven | POM release 25, JavaFX 25; JDK Windows đúng kiến trúc | Cache Maven dependencies, installer và artifact Client/Monitor; thử khi ngắt Internet | Offline desktop run NOT_RUN |

Trên máy chuẩn bị có Docker hoạt động, thực hiện lần lượt và dừng nếu lệnh lỗi:

```powershell
$candidate = (git rev-parse HEAD).Trim()
$backendImage = 'railway-backend-node:d24-' + $candidate.Substring(0, 12)
$offlineDir = Join-Path $env:TEMP ('railway-d24-offline-' + $candidate.Substring(0, 12))
New-Item -ItemType Directory -Force -Path $offlineDir
docker version
docker build -f docker/Dockerfile.backend -t $backendImage .
# Chỉ pull khi máy có mạng, trước buổi demo và sau khi TV2 chốt bản PG.
docker pull postgres:16-alpine
docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}' $backendImage postgres:16-alpine
docker image save -o "$offlineDir\railway-images.tar" $backendImage postgres:16-alpine
Get-FileHash -Algorithm SHA256 "$offlineDir\railway-images.tar"
# Ghi candidate, hai image ID/architecture và SHA256 cạnh tar; copy cả bộ sang 5 laptop.
```

Trên mỗi laptop 1–5: so SHA256 với manifest nguồn, `docker image load -i <tar>`, inspect lại **image ID và architecture**, rồi mới triển khai. Local-built image có thể không có RepoDigest; dùng content image ID cộng SHA256 của tar, không lấy tag làm bằng chứng đồng nhất. Verify bằng cách chạy thử khi không có Internet và đối chiếu phiên bản/dependencies; phần save/load/verify này NOT_RUN/BLOCKED, không được đánh dấu PASS bằng checklist.

Không đưa mật khẩu vào evidence/manifest. Không xuất volumes/DB cá nhân vào bộ image; không reset, xóa volumes hoặc mở firewall hàng loạt để xử lý lỗi.

## Kiểm thử và bàn giao

Lệnh thực tế, exit code, phiên bản công cụ, SHA source và JSON máy local nằm trong `artifacts/evidence/D24/TV6-D24-result.log`.

- Kiểm tra tự động D24 và collector local được chạy riêng; PASS của test phân biệt với inventory BLOCKED và physical LAN NOT_RUN.
- Chưa đo/ánh xạ đủ 6 laptop; chưa verify inbound/outbound giữa máy; chưa có image tar; chưa build/deploy Compose LAN hoặc query DB/Raft thật.
- Bàn giao: nhóm cung cấp 5 bản đo còn lại, chốt mạng demo/phiên bản PG và kiểm tra lại IP Laptop 6 khi ráp mạng; máy có Docker daemon chuẩn bị tar và xác nhận hash/image ID. LAN cluster PASS chỉ sau system test đúng SPEC-07/08 tại gate chung.
