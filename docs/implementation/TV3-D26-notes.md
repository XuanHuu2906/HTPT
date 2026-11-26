# TV3-D26: Thử TCP Packet Loss / Timeout Qua TestNetworkFilter & Response-Lost / Correlation / UNKNOWN Invariants

## 1. Thông tin Task
- **Mã task:** `tv3-d26`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md` (§5.10), `03-CONTRACTS.md` (§1–§4, §7), `07-TESTING.md` (`TC-AC10`, `TC-TCP-04`), `08-DEPLOYMENT.md` (§3, §3.1)
- **Phụ thuộc:** `TestNetworkFilter` (TV3-D21) & `MessageDispatcher` / peer config (TV3-D25) — không chờ TV4/TV5-D26.

## 2. Chi tiết kỹ thuật & Giải pháp

### 2.1. Hook mô phỏng mất phản hồi trong `MessageDispatcher`
- Trước D26, `MessageFilterHook` trong `MessageDispatcher` chỉ chặn trước khi vào handler (mất request trước xử lý).
- Để mô phỏng đúng kịch bản **response lost sau khi server đã commit/apply** (`SPEC-07 TC-AC10`), `MessageDispatcher.java` được bổ sung:
  - `@FunctionalInterface public interface ResponseFilterHook` (chỉ dùng cho test, mặc định `null` ở production).
  - `getResponseFilterHook()` / `setResponseFilterHook(ResponseFilterHook)`.
  - Helper dùng chung `finalizeResponse(...)` chạy `ensureStrictCorrelation(...)` trước rồi kiểm tra `responseFilterHook` cho cả nhánh `leaderRouter` và `handler.handle(...)`. Nếu hook trả `false`, dispatcher trả `null` để `TcpServer` không gửi frame phản hồi, trong khi khối `finally` vẫn giải phóng `activeInFlight.remove(messageId)` sạch sẽ.

### 2.2. Mở rộng `TestNetworkFilter` cho chiều phản hồi
- [`TestNetworkFilter.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/TestNetworkFilter.java) cài đặt thêm `MessageDispatcher.ResponseFilterHook`:
  - Hỗ trợ `dropResponseType(MessageType)`, `dropResponsesTo(String)`, `addResponseDropPredicate(...)`, thống kê `getResponseDroppedCount()`.
  - Các rule drop request một chiều (`blockLink`) không chặn nhầm socket response đã thiết lập của D21; chỉ `isolatedNodes` hoặc `responseDropPredicates` mới chặn chiều phản hồi.
  - `attachTo(...)`, `detachFromCluster(...)`, `heal()` và `resetMetrics()` đồng bộ cả hai chiều.

### 2.3. Kiểm chứng Packet Loss, Timeout, Correlation và UNKNOWN (`TcpPacketLossTimeoutIntegrationTest`)
1. **TC-D26-01 (Request lost):** Drop `BOOK_TICKET` ở ingress -> `ConnectionManager` timeout trả `UNKNOWN (TIMEOUT_UNKNOWN)`, không ghi vé trên server, đóng socket lỗi -> sau `heal()` retry với **cùng `requestId`** (`messageId` mới) đặt vé thành công.
2. **TC-D26-02 (Response lost sau apply):** Drop `BOOK_TICKET_RESPONSE` sau khi server đã apply ghế `G04`/`G05` -> client gặp `SocketTimeoutException` -> `GET_REQUEST_RESULT(requestId)` / `ResultLookup` khôi phục đúng `CONFIRMED` + `ticketId` gốc, không cấp trùng vé.
3. **TC-D26-03 (Correlation safety):** `MessageDispatcher` tự sửa `correlationId` lệch về `request.messageId`; `RpcCorrelationRegistry` giải phóng in-flight slot khi timeout và loại bỏ mọi phản hồi trễ/lạ sau deadline.
4. **TC-D26-04 (5-Node Loopback + D25 peer allowlist):** Drop có chọn lọc `APPEND_ENTRIES` (`node-1 -> node-3`) và drop response (`node-4 -> node-1`) -> đúng timeout trên 2 node bị drop trong khi `node-2,5` bình thường, `activeInFlight == 0` ở cả 5 node, sau `heal()` hồi phục ngay.
5. **TC-D26-05 (Repeated faults & TCP framing):** 2 lần liên tiếp mất response timeout vẫn tự reconnect sạch sẽ cho lần 3, không lệch byte frame.
6. **TC-D26-06 (Channel isolation dưới fault):** `TC-TCP-04` và D25 peer allowlist (`allowedPeerIds`) vẫn chặn `REQUEST_VOTE` trên cổng client và `senderId` lạ trên cổng peer khi `TestNetworkFilter` đang kích hoạt.

## 3. Kết quả kiểm thử & Ranh giới Gate G6
- **Targeted suite (`61/61 PASS`, `16.071 s`):** `TcpPacketLossTimeoutIntegrationTest` (`6/6`), `TestNetworkFilterTest` (`7/7`), `ConnectionManagerTcpIntegrationTest` (`3/3`), `PeerConnectionManagerTest` (`6/6`), `RpcCorrelationRegistryTest` (`7/7`), `MessageDispatcherTest` (`28/28`), `LanLivePreflightAndIsolationTest` (`4/4`).
- **Full `shared` + `backend-node` regression (`BUILD SUCCESS`, `56.718 s`):** `shared` `38/38` (`0` skipped), `backend-node` `1325` run / `0` failed / `146` DB-Docker skipped.
- **Physical 5-laptop LAN:** `NOT_RUN` — thực hiện riêng khi cụm 5 Laptop chạy tại **Gate G6**.
- **Bằng chứng:** [`artifacts/evidence/D26/TV3-D26-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D26/TV3-D26-result.log)
