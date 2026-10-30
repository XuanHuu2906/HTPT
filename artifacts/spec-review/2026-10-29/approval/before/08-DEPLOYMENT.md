# SPEC-08 — Deployment & vận hành: Docker 2 chế độ / 6 Laptop LAN

> **Trạng thái:** Draft v1 dựa trên phương án Docker Nhóm 6. **Owner:** TV6; review TV2 (DB), TV3 (TCP/ports), TV4–TV5 (Raft). Docker hỗ trợ **Backend và PostgreSQL**; **JavaFX Desktop chạy trực tiếp Windows**.

## 1. Một codebase, hai môi trường

| Thành phần | Phát triển tại nhà | Demo trên 6 Laptop |
|---|---|---|
| Backend | 5 containers trên 1 máy | 1 container trên Laptop 1–5 |
| PostgreSQL | 5 containers/5 volumes riêng | 1 PostgreSQL+volume trên Laptop 1–5 |
| Client JavaFX | 1 hoặc nhiều process trên Windows | Có thể chạy trên cả 6 Laptop |
| Monitor JavaFX | Một process Windows | Laptop 6 |
| Node peer transport | TCP trong Docker bridge | TCP qua IP LAN đã publish port |
| Quorum | 3/5 | 3/5 |
| Source/image | Cùng một backend codebase | Cùng codebase và artifact; chỉ đổi config |

**Không tạo website hoặc HTTP/REST/Web Service dưới bất kỳ chế độ nào.** Docker không đồng nghĩa triển khai cloud. Việc chạy 10 container trên 1 máy chỉ chứng minh mức process/container, không phải khả năng chịu lỗi cả máy vật lý.

## 2. Bố trí laptop

| Máy | Chạy gì | Vai trò |
|---|---|---|
| Laptop 1 | Node 1 + PostgreSQL 1, có thể mở Client | Raft voter |
| Laptop 2 | Node 2 + PostgreSQL 2, có thể mở Client | Raft voter |
| Laptop 3 | Node 3 + PostgreSQL 3, có thể mở Client | Raft voter |
| Laptop 4 | Node 4 + PostgreSQL 4, có thể mở Client | Raft voter |
| Laptop 5 | Node 5 + PostgreSQL 5, có thể mở Client | Raft voter |
| Laptop 6 | JavaFX Client + Cluster Monitor | **Không** phải voter |

Không giả định Laptop 1 luôn là Leader; bầu cử động theo Raft. Nếu Laptop 6 tắt, cụm 5 Node không đổi số phiếu.

## 3. Cổng mạng và biến môi trường (DESIGN-v1)

**Quy ước mẫu cần khóa để tránh lỗi LAN:**

| Chế độ | Client TCP | Peer Raft TCP | Ghi chú |
|---|---|---|---|
| Container nội bộ | `9100` | `9200` | Mỗi container có IP/hostname riêng |
| Dev host Windows | `localhost:9101..9105` | host port `9201..9205` nếu cần debug | Container↔container dùng tên service + internal `9200`, **không** dùng `localhost:920x` giữa containers |
| LAN Laptop 1–5 | `IP_LAPTOP:9100` | `IP_LAPTOP:9200` | Port host cố định được publish trên mỗi laptop khác nhau |

Mẫu `.env.example` (placeholder, không phải cấu hình đã có):

```dotenv
NODE_ID=node-1
CLUSTER_ID=railway-demo-5
NODE_CLIENT_BIND=0.0.0.0:9100
NODE_PEER_BIND=0.0.0.0:9200
NODE_ADVERTISE_CLIENT=192.168.10.11:9100
NODE_ADVERTISE_PEER=192.168.10.11:9200
NODE_PEERS=node-1@192.168.10.11:9200,node-2@192.168.10.12:9200,node-3@192.168.10.13:9200,node-4@192.168.10.14:9200,node-5@192.168.10.15:9200
DB_HOST=postgres
DB_PORT=5432
DB_NAME=railway
DB_USER=railway
DB_PASSWORD=CHANGE_ME_LOCAL_ONLY
```

Các IP ví dụ `192.168.10.11–15` từ kịch bản minh họa, **không** được giả định đúng trong lớp. Gán IP thật/địa chỉ tĩnh theo LAN hiện tại. Khi chạy dev, peer advertise/peers dùng hostname service nội bộ Docker; Client advertise phải là địa chỉ truy cập được từ Desktop Windows (`127.0.0.1:9101..9105` khi cùng máy), không dùng hostname Docker cho Host Client.

### 3.1. Discovery, ranh giới kênh và cấu hình thiếu (DESIGN-v1)

- Client/Monitor cần map đủ `node-1`..`node-5` → **client endpoint**: dev `127.0.0.1:9101..9105`; LAN `IP_LAPTOP_1..5:9100`. `leaderHint` chỉ là node ID, không mang host/port; map này là nguồn resolve. Hint null/stale thì thử các endpoint với backoff/deadline, không hardcode Leader.
- Backend peer map phải có đủ 5 ID duy nhất, chứa chính Node; quorum luôn 3 dù peer nào offline. Tách handler/cổng Client và peer: không nhận REQUEST_VOTE/APPEND_ENTRIES trên client port. Firewall giới hạn peer port cho 5 host cấu hình; kiểm tra senderId với cấu hình không được quảng cáo là xác thực mật mã. Cơ chế nhận diện peer cụ thể còn cần TV3/TV6 chốt trong hồ sơ review.
- `CLUSTER_ID` hiện chỉ là cấu hình vận hành, **chưa có field/handshake trên wire** để chặn RPC khác cluster. Không dựa vào biến này để tuyên bố cô lập giao thức. Baseline demo phải cô lập mạng/peer list giữa các cụm và reset đồng bộ; muốn kiểm tra cluster trên wire phải bổ sung contract được review.
- Cần cấu hình connect timeout, response deadline, retry backoff, heartbeat/election timeout; TV3/TV4/TV5/TV6 chốt giá trị từ test LAN. Deadline dùng đồng hồ monotonic cục bộ, không so với sentAt phía khác; hết hạn ghi chuyển UNKNOWN và giữ requestId.
- Cùng protocolVersion=1 không có nghĩa code bootstrap tương thích SPEC-03. Trước đổi DTO phải chốt migration/reset coordinated hoặc version mới tại [SPEC-REVIEW-01](../reviews/SPEC-REVIEW-01.md); không rolling deploy hai hình dạng envelope khác nhau dưới cùng version.

## 4. Cấu trúc file mục tiêu

```text
docker/
  Dockerfile.backend
  compose.dev.yml             # 5 backend + 5 postgres trên 1 laptop
  compose.lan.yml             # 1 backend + 1 postgres trên từng laptop
  .env.example
configs/
  dev/                        # address list dev
  lan/                        # map Laptop -> Node ID/IP
scripts/
  start-dev.ps1
  stop-dev.ps1
  start-lan.ps1
  demo-reset-all.ps1
```

**Đây là danh sách file cần triển khai**; không được chạy lệnh rồi tự tuyên bố PASS khi Compose/Dockerfile chưa tồn tại.

## 5. Development mode — runbook

1. Kiểm tra JDK/Maven/Docker Desktop theo bản nhóm thống nhất; tài liệu tech stack **đề xuất** Java 25/JavaFX 25/PostgreSQL 18, phải kiểm tra tính tương thích thực tế.
2. Build `shared`, `backend-node` và Desktop modules. Tạo image Backend từ **cùng một source**.
3. Khởi động 5 PostgreSQL có volumes riêng và 5 Backend containers. Healthcheck PostgreSQL bằng `pg_isready`, không dùng HTTP health endpoint.
4. Kiểm tra `NODE_ID`, peer DNS/config, port mapping, migration, `currentTerm`, Node log; chờ Leader hợp lệ.
5. Mở JavaFX Client/Monitor trên Windows; thử GET_SEATS và AC nhỏ trước, rồi AC03/05/06/07/08/09/10.
6. Dừng Compose bằng lệnh phù hợp; **không xóa volume** nếu cần giữ dữ liệu để kiểm chứng durability.

**Lệnh tham khảo chỉ có hiệu lực sau khi tạo Compose files:**

```powershell
docker compose -f docker/compose.dev.yml up -d --build
docker compose -f docker/compose.dev.yml ps
docker compose -f docker/compose.dev.yml logs -f backend-node-1
docker compose -f docker/compose.dev.yml down
```

`down -v` xóa volume: chỉ dùng trong quy trình reset test **toàn cụm**, có xác nhận trước; không tự dùng khi debug Node đơn lẻ đang tham gia cluster.

## 6. Demo LAN — runbook

1. Tất cả 6 laptop kết nối cùng LAN; ghi bảng IP thực, NodeID 1–5, firewall port 9100/9200.
2. Trên Laptop 1–5: từng người chạy Compose LAN với **một** Backend Node và **một** PostgreSQL volume; Node dùng cùng image, khác ID/config.
3. Đảm bảo `NODE_PEERS` tại mọi Laptop có cùng tập `node-1`..`node-5`, nhưng mỗi địa chỉ peer trỏ đúng IP host khác nhau; Node nhận biết chính mình.
4. Laptop 6 chạy JavaFX Client/Monitor, kết nối tới danh sách IP của Node (không vào DB trực tiếp).
5. Kiểm tra `NODE_STATUS` thực tế cho từng Node, đúng một Leader sau ổn định, cùng 10 ghế seed.
6. Demo tranh G05 từ nhiều Client; kiểm tra log commit và bảng ghế/vé tại các Node đang cập nhật.
7. Dừng Leader thực, chờ bầu mới; kiểm tra tiếp tục ghi với 4/5 hoặc 3/5; dừng thêm Node để còn 2/5, gửi requestId mới và xác nhận **không commit/xác nhận lệnh mới sau mất quorum**. Phân biệt response trễ của lệnh đã commit trước fault theo SPEC-07.
8. Khởi động các Node đã dừng; đối chiếu `lastApplied`, tickets, seats sau catch-up; cuối cùng khôi phục 5/5.

**Không** demo bằng cách chỉ bấm chuyển màu Node thành OFFLINE trên Monitor; phải dừng tiến trình/container hoặc chặn kết nối mạng thực theo kịch bản.

## 7. Fault injection và reset dữ liệu

### Fault injection

- Hỗ trợ script dừng/khởi động Node (Docker CLI trên máy chủ tương ứng); delay/partition chỉ khi công cụ thực sự tác động đến TCP liên Node.
- Thực hiện trong môi trường demo/kiểm thử có kiểm soát, không đưa chức năng điều khiển container từ xa không xác thực vào mạng ngoài.
- Mọi lần inject log thời điểm, Node ID, cách khôi phục. Không phá dữ liệu thật.

### Reset sạch cả cụm

1. Dừng **toàn bộ 5 Node**, sao lưu evidence nếu cần.
2. Xác định test cho phép mất dữ liệu; làm sạch **toàn bộ 5 volumes/storage cùng phiên** (hoặc tạo cluster/volumes mới), không làm sạch từng Node đơn lẻ đang chạy.
3. Migrate DB, seed `SE-001`/G01–G10 nhất quán; đưa `term/vote/log/apply` về trạng thái genesis của **cả năm**.
4. Khởi động toàn cụm, đợi bầu Leader, xác minh ghế trống và Node health rồi mới chạy test.

## 8. Failure modes và giới hạn

- Mất 1–2 máy dữ liệu: có thể tiếp tục nếu phần còn lại đủ majority Raft.
- Mất 3 máy dữ liệu / còn 2/5: không xác nhận ghi mới.
- Mất Laptop 6: chỉ mất Monitor và Client trên máy đó; 5 voter không đổi.
- Cả router/switch LAN chung hỏng: **ngoài** khả năng chịu lỗi 1–2 Node; không cam kết hệ thống hoạt động trong mọi tình huống vật lý.
- Docker host/máy yếu: test ở nhà có thể giảm container chạy cho unit/MVP, nhưng chỉ công nhận AC phân tán khi chạy đủ cấu hình yêu cầu.
- Không có Internet lúc demo: chuẩn bị image, Maven artifacts, tài liệu, installer và dependencies trước; runtime dùng LAN nội bộ.

## 9. Checklist trước giờ bảo vệ

- [ ] Cùng source version/image digest trên 5 máy.
- [ ] 5 instance PostgreSQL độc lập, port/volume riêng, có migration seed.
- [ ] Client/Monitor chỉ dùng TCP Socket; không website/REST/Web Service.
- [ ] Đúng Node-ID/IP, 1 Leader hợp lệ, `NODE_STATUS` thật.
- [ ] Test đặt G05 tranh chấp có bằng chứng không double-booking.
- [ ] Thử dừng Leader và mất quorum 2/5, ghi log cụ thể.
- [ ] Thử Node quay lại/catch-up và kiểm tra SQL.
- [ ] Không ghi nhãn PASS khi chưa có log/screenshot/SQL evidence.
