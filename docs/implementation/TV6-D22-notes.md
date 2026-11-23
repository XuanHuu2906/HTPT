# TV6-D22: Docker + Monitor + Integration — Bộ harness phân vùng mạng 3–2 (`PartitionHarness`)

## 1. Thông tin Task
- **Mã task:** `TV6-D22`
- **Tên nhiệm vụ:** Tạo test phân vùng 3–2 — cài fault test network filter theo peer ID trên cụm TCP 5 node, giữ toàn bộ tiến trình sống (phân biệt partition với crash), chia 3–2 rồi heal và thu được response từ cả 2 phía
- **Tài liệu tham chiếu:**
  - `docs/specs/00-MASTER.md` (§3 Bất biến 5–7, 11)
  - `docs/specs/01-REQUIREMENTS.md` (`FR06`, `FR08`, `FR12`; `AC09`)
  - `docs/specs/03-CONTRACTS.md` (§2–§6 Framing, `NODE_STATUS`, `BOOK_TICKET`, `GET_SEATS`, `REQUEST_VOTE`, `APPEND_ENTRIES`)
  - `docs/specs/07-TESTING.md` (`TC-AC09`, `TC-RAFT-06`)
  - `docs/specs/08-DEPLOYMENT.md` (§7 Fault injection có kiểm soát)
- **Phụ thuộc & Tích hợp:**
  - Tích hợp trực tiếp hook lọc mạng của **TV3-D21** (`MessageDispatcher.MessageFilterHook` và `RaftRpcClient.RpcFilterHook`) trên 5 `NodeMain` thật (dual-port TCP: client port + Raft peer port) và `NodeStatusHandler` (**TV3-D15**).
  - Kiểm chứng sẵn sàng bộ fault scripts **TV6-D21** (`scripts/stop-node.ps1`, `scripts/restart-node.ps1`, `scripts/select-leader.ps1`).
  - Xây dựng độc lập **không chờ TV5-D22**.

---

## 2. Thiết kế `PartitionHarness`
- File: `integration-tests/src/test/java/vn/nhom6/railway/tests/PartitionHarness.java`
- **Kiến trúc tách kênh (Channel Isolation):**
  - Mỗi node trong 5 node (`node-1`..`node-5`) mở 2 cổng TCP độc lập trên `127.0.0.1` (ephemeral ports):
    - **Client port** (`ChannelType.CLIENT`): nhận `PING`, `NODE_STATUS`, `GET_SEATS`, `BOOK_TICKET`.
    - **Raft peer port** (`ChannelType.PEER`): nhận `REQUEST_VOTE`, `APPEND_ENTRIES`.
  - `PeerPartitionFilter` (implement `MessageDispatcher.MessageFilterHook` và `RaftRpcClient.RpcFilterHook`) được gắn lên **Raft peer dispatcher** (và tùy chọn trên outbound `RaftRpcClient`), chặn chính xác **12 directed links** xuyên biên giới giữa 2 phía 3–2 trong khi **giữ sống 5/5 tiến trình `NodeMain`** (`isRunning() == true`) và giữ mở toàn bộ Client ports để thu response từ cả 2 phía!
- **Các kịch bản đã kiểm chứng (8/8 PASS):**
  1. `baselineFullMesh_allFiveNodesAlive_peerRpcsAndClientResponsesSucceed`: 5/5 node sống, 20/20 đường TCP peer thông, 5/5 `NODE_STATUS` thành công.
  2. `partition3vs2_blocksTwelveCrossLinks_whileIntraPartitionTrafficAndAllFiveProcessesStayAlive`: Chặn đúng 12 đường chéo, nội bộ 3/5 và 2/5 thông 100%, 5/5 process sống (`PING -> PONG`, `NODE_STATUS` khỏe), xuyên phân vùng bị drop & timeout sạch.
  3. `partition3vs2_collectsResponsesFromBothSidesSimultaneously_thenHealsAndRestoresFullMesh`: Trong lúc chia 3–2 thu đồng thời response thật qua TCP từ phía đa số (`CONFIRMED` trên `node-1`, `NOT_LEADER` trên `node-2`) và phía thiểu số (`UNKNOWN` trên `node-4`, `NOT_LEADER` trên `node-5`, `NODE_STATUS` trên cả hai phía); sau `heal()` không khởi động lại process (giữ nguyên port), RPC xuyên biên giới thông trở lại ngay và thu đủ 5/5 `NODE_STATUS`.
  4. `partition3vs2_selectiveOutboundAndInboundFilterHooks_dropCrossPartitionWithoutKillingSockets`: Xác minh cả outbound (`RaftRpcClient`) + inbound (`MessageDispatcher`) hooks và bộ đếm telemetry `droppedCount`/`deliveredCount`.
  5. `asymmetricPartition_unidirectionalLinkBlock_preservesReverseDirectionAndProcessLiveness`: Chặn 1 chiều `node-1 -> node-4`, chiều ngược `node-4 -> node-1` vẫn thông.
  6. `selectiveMessageTypeDrop_blocksRequestVoteAcrossBoundary_whileAppendEntriesAndStatusContinue`: Chặn chọn lọc riêng `REQUEST_VOTE` trên một đường liên kết.
  7. `successivePartitions_rotate3vs2Boundaries_andHealCleanlyEveryCycle`: Phân vùng 3–2 xoay vòng 2 chu kỳ đổi biên (`{1,2,3}|{4,5}` -> heal -> `{1,2}|{3,4,5}` -> heal) khôi phục đủ 20/20 đường TCP.
  8. `faultScriptsInventory_tv6D21ScriptsPresentAndExecutableContractVerified`: Kiểm tra sự tồn tại và an toàn volume của 3 script TV6-D21 cho Gate G6.

---

## 3. Kết quả kiểm thử đã chạy

- **PartitionHarness (`integration-tests`):**
  ```bash
  mvn -B -ntp -pl integration-tests -am test -Dtest="PartitionHarness" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 8, Failures: 0, Errors: 0, Skipped: 0` (PASS, `5.507 s`, `BUILD SUCCESS`).
- **Cross-check TV3-D21 (`TestNetworkFilterTest`):**
  ```bash
  mvn -B -ntp -pl backend-node -am test -Dtest="TestNetworkFilterTest" -Dsurefire.failIfNoSpecifiedTests=false
  ```
  `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0` (PASS, `4.023 s`, `BUILD SUCCESS`).
- **Minh chứng:** `artifacts/evidence/D22/TV6-D22-result.log`.
- **Giới hạn phạm vi:** Harness chứng minh chia 3–2, giữ tiến trình sống, thu response 2 phía và heal trên cụm 5 `NodeMain` loopback TCP thật (với `FakeBookingService`/`FakeRaftRequestHandler`); kiểm thử `TC-AC09` end-to-end với Raft consensus + 5 PostgreSQL thật thuộc **Gate G6**.
