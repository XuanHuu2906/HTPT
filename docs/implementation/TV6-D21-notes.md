# TV6-D21: Docker + Monitor + Integration — Bộ mô phỏng dừng/khởi động lại Node & Chọn Leader động từ NODE_STATUS

## 1. Thông tin Task
- **Mã task:** `TV6-D21`
- **Tên nhiệm vụ:** Tạo bộ mô phỏng dừng Node (`stop-node.ps1`, `restart-node.ps1`, `select-leader.ps1`)
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 4, 9, 11)
  - `docs/specs/01-REQUIREMENTS.md` (`FR08`, `FR09`, `FR12`; `AC05`–`AC08`)
  - `docs/specs/03-CONTRACTS.md` (§2 Framing 4-byte big-endian, §3 Envelope & `correlationId`, §5 `NODE_STATUS` / `NODE_STATUS_RESPONSE`)
  - `docs/specs/07-TESTING.md` (`TC-AC05A`, `TC-AC05B`, `TC-AC06`, `TC-AC07`, `TC-AC08`)
  - `docs/specs/08-DEPLOYMENT.md` (§3.1 Discovery, §5–§7 Fault injection không xóa volume)
- **Phụ thuộc & Tích hợp:**
  - Kế thừa mô hình Docker Compose/container từ **TV6-D11** (`docker/compose.dev.yml`, `docker/compose.election-test.yml`).
  - Giao tiếp chuẩn **TV3-D15** `NODE_STATUS` (`NodeStatusHandler` / `NodeStatusDto` theo SPEC-03 §5).
  - **Độc lập, không chờ TV5-D21:** Kiểm thử thực tế trên test container + named volume độc lập và loopback `NODE_STATUS` fixture; cụm 5 Node thật + PostgreSQL dùng lại bộ script này tại **Gate G6**.

---

## 2. Thành phần bàn giao

### 2.1. `scripts/select-leader.ps1` — Chọn Leader hiện tại động từ `NODE_STATUS`
- Kết nối TCP thuần (không HTTP/REST) theo framing 4-byte big-endian + UTF-8 JSON (`protocolVersion=1`, `type=NODE_STATUS`, `correlationId` khớp `messageId`, `senderId` & `payload.nodeId` khớp node đích).
- Hỗ trợ 3 chế độ endpoint:
  1. `-Endpoints "node-1@host:port,..."` (phù hợp cả dev localhost lẫn 6 laptop LAN theo SPEC-08 §3.1).
  2. `-Project <compose-project> -ComposeFile <file> -ServicePrefix <backend-node|node>` (tự động discover host port từ `docker compose port <svc> 9100`).
  3. Mặc định dev host `127.0.0.1:9101..9105`.
- Hợp đồng exit code rõ ràng:
  - `0`: Quan sát đúng 1 `LEADER` khỏe (`storageHealthy=true`, `term != null`).
  - `1`: Lỗi tham số / runtime.
  - `2`: Không có Leader (`NO_LEADER_FOUND` — tuyệt đối không hardcode `node-1`).
  - `3`: Phát hiện >1 Leader (`MULTIPLE_LEADERS` — fail-closed, trừ khi bật `-SelectHighestTerm`).

### 2.2. `scripts/stop-node.ps1` — Dừng từng Node bằng Docker không xóa volume
- Chỉ gọi `docker stop --time <sec>` hoặc `docker compose stop --timeout <sec>` — **tuyệt đối không gọi `rm`, `down`, `down -v`, `volume rm`**.
- Hỗ trợ 3 cách chọn mục tiêu:
  - `-NodeId node-1..node-5` (kèm tùy chọn `-UseCompose` + `-ComposeFile` + `-Project` + `-ServicePrefix`)
  - `-TargetLeader` (gọi `scripts/select-leader.ps1` để chọn động Leader hiện tại từ `NODE_STATUS`)
  - `-ContainerName <name>` (phục vụ kiểm thử với test container độc lập)
- Ghi log timestamp UTC ISO-8601 (`yyyy-MM-ddTHH:mm:ss.fffZ`) cho mọi bước, lưu exit code của từng lệnh Docker, kiểm tra `docker inspect` trước/sau (trạng thái `exited`, danh sách mounts không đổi) và xác minh `docker volume inspect` nguyên vẹn.
- Hợp đồng exit code: `0` = dừng thành công & volume nguyên vẹn; `1` = lỗi tham số/Docker; `2` = `-TargetLeader` không tìm thấy Leader duy nhất.

### 2.3. `scripts/restart-node.ps1` — Khởi động lại từng Node bằng Docker không xóa volume
- Chỉ gọi `docker start` hoặc `docker compose start` (và `stop` trước nếu bật `-RestartIfRunning`).
- Ghi log timestamp UTC ISO-8601, exit code lệnh Docker, chờ container đạt trạng thái `running` ổn định (`-WaitRunningSeconds`), xác minh toàn bộ mounts & named volumes nguyên vẹn.
- Hỗ trợ tùy chọn `-WaitStatus` (dùng tại Gate G6): chờ node mở lại cổng `NODE_STATUS` và trả snapshot hợp lệ sau khi restart.

---

## 3. Lệnh sử dụng tham khảo cho Gate G6

```powershell
# 1. Chọn Leader hiện tại từ NODE_STATUS
.\scripts\select-leader.ps1
.\scripts\select-leader.ps1 -AsJson

# 2. Dừng đúng Leader hiện tại (tự chọn từ NODE_STATUS, không xóa volume)
.\scripts\stop-node.ps1 -TargetLeader -UseCompose -ComposeFile docker/compose.dev.yml -Project docker -LogPath artifacts/evidence/G6/stop-leader.log

# 3. Dừng một Node cụ thể (ví dụ node-2)
.\scripts\stop-node.ps1 -NodeId node-2 -UseCompose -ComposeFile docker/compose.dev.yml -Project docker

# 4. Khởi động lại Node đã dừng và chờ NODE_STATUS sẵn sàng
.\scripts\restart-node.ps1 -NodeId node-2 -UseCompose -ComposeFile docker/compose.dev.yml -Project docker -WaitStatus
```

---

## 4. Kết quả kiểm thử thực tế với Test Container

- Thực thi ngày 23/11/2026 trên Docker `29.9.2` + PowerShell 7 LTS (`mcr.microsoft.com/powershell:lts-alpine-3.20` + `docker-cli` + `docker-cli-compose`) với 11 test cases (test container `alpine:3.22` + named volume có file sentinel + loopback `NODE_STATUS` TCP fixture):
  - **11/11 PASS**, exit code đúng hợp đồng (`0`, `1`, `2`, `3`), dữ liệu sentinel trong volume giữ nguyên 100% qua stop/start/restart, không để lại container/volume tạm, 5 volume dev `railway_pgdata1..5` nguyên vẹn.
- Minh chứng chi tiết: `artifacts/evidence/D21/TV6-D21-result.log`.
- **Giới hạn phạm vi:** Kiểm thử end-to-end với cụm 5 Node + 5 PostgreSQL thật (failover, đặt vé, catch-up SQL) là **NOT_RUN** ở task này và sẽ dùng lại chính bộ script trên tại **Gate G6**.
