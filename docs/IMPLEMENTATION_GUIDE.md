> Tài liệu bootstrap lịch sử: implementation và script/config demo đã được xóa theo BASE-CLEANUP-01. Không dùng các mô tả hoặc lệnh dưới đây như trạng thái hiện tại. Xem [MASTER SPEC](specs/00-MASTER.md) và [README](../README.md).

# 10 — Danh sách TODO theo thành viên / phương thức thực hiện

Đây là hướng dẫn **bước tiếp theo từ bộ khung**, không phải danh sách đã làm xong.

## TV1 — Desktop client

**Đã có:** JavaFX App, FXML 10 nút ghế, form nhập tên, đọc GET_SEATS qua TCP, báo lỗi nếu chưa hỗ trợ BOOK.

**Cần làm:**
1. `ClientController.book()` tạo `requestId` UUID, lưu vào pending-requests để retry **cùng ID** nếu phản hồi không đến.
2. Viết `cancel()` bằng `ticketId` đã commit, thêm lịch sử giao dịch và tra cứu request.
3. Xử lý `LEADER_REDIRECT`: đổi endpoint sang Leader mới, không retry mù.
4. Đọc danh sách ghế nhất quán từ Leader sau khi TV4/5 có cơ chế đọc an toàn.
5. Bổ sung UI CONNECTING/READY/NO_QUORUM/UNKNOWN, không coi màu ghế local là trạng thái authoritative.
6. Viết JavaFX smoke test và ghi lại ảnh màn hình thật.

## TV2 — Booking & Persistence

**Đã có:** `BookingCommand`, `BookingResult`, in-memory fixture, JDBC read service, SQL schema.

**Cần làm:**
1. Thêm `JdbcRaftStateStore` cho `RaftStateStore` trên PostgreSQL từng Node, phối hợp TV4/TV5.
2. Cài `JdbcCommittedCommandApplier.applyCommitted()` với transaction `tickets + seats + processed_requests + lastApplied` nguyên tử.
3. Đảm bảo payload fingerprint khớp khi retry cùng requestId, nếu khác => `REQUEST_ID_CONFLICT`.
4. Cài `GET_REQUEST_STATUS` theo trạng thái committed, đừng tự nhận PENDING thành thất bại.
5. Tạo test JUnit/JDBC với hai yêu cầu cùng ghế, restart, replay, cancel và idempotency.
6. Xác định ticketId deterministic và tránh dùng UUID random trong apply.

## TV3 — Protocol & TCP

**Đã có:** Envelope v1, MessageType, Jackson mapper, length-prefix, TCP client/server, basic PING test.

**Cần làm:**
1. Freeze response DTO cho Raft (`term`,`voteGranted`,`success`,`matchIndex`) và các lỗi typed, test serde.
2. Thêm framing negative cases: incomplete header, truncated JSON, connection close, oversized frame.
3. Leader redirect: Node không phải Leader trả hint hợp lệ; Client re-route.
4. Có thể tối ưu connection reuse, nhưng phải bảo đảm requestId/correlationId chính xác.
5. Hook `NetworkFaultPolicy` vào transport để mô phỏng packet drop, delay, block theo cặp Node (không đổi semantics TCP ngoài test).
6. Viết network integration test: 5 peer ports + concurrent clients + lost ACK/reconnect.

## TV4 — Replication & Commit

**Đã có trong `backend-node/raft`:** Raft interfaces, RaftLogEntry, RaftReplicationService TODO.

**Cần làm:**
1. Implement persistent term/log storage, `appendOrReplace`, log prefix matching, conflict resolution.
2. Implement `AppendEntries` đầy đủ: reject stale terms / incorrect `prevLogTerm`, persist before ACK.
3. Leader track `nextIndex/matchIndex`, retry and backtrack correctly.
4. Tính `commitIndex` theo majority và current-term safety rule.
5. Áp dụng command với TV2 sau commit, theo thứ tự tăng `lastApplied`.
6. Test 3/5 alive, 2/5 alive, stale follower, conflict logs, crash during commit.

## TV5 — Election, Failover, Recovery

**Đã có:** RaftRole, ElectionService, RaftElectionService TODO.

**Cần làm:**
1. One shared RaftNode state with TV4: không tạo term/vote riêng.
2. Election timer random, vote request, up-to-date log checks, persist votedFor before ACK.
3. Heartbeats AppendEntries (empty entries), term bump & follower demotion.
4. Khi Leader crash, đợi election hoàn thành, yêu cầu Client tìm Leader mới.
5. Mô phỏng partition 3–2 và bảo đảm không có minority commit.
6. Node restart: log catch-up/replay với TV4, cross-check PostgreSQL và committed log.

## TV6 — Monitor, Fault Injection, QA

**Đã có:** JavaFX Monitor chỉ đọc GET_NODE_STATUS; NetworkFaultPolicy và PartitionFaultPolicy; test module.

**Cần làm:**
1. Khi TV4/5 có Raft, `GET_NODE_STATUS` trả role/term/leader/commitIndex/lastApplied thật.
2. Hiển thị 5 Node, đồng hồ election, replication lag và event stream.
3. Dựng UI admin bật/tắt Node **an toàn, kiểm soát được** bằng process supervisor hoặc test harness (không arbitrary shell execution từ message tùy ý).
4. Phối hợp TV3 để điều khiển artificial delay/block; làm scenario 3–2.
5. Tạo test automation chứng minh AC01–AC10, lưu log với timestamp, nodeId, requestId.
6. Phối hợp mọi TV để tạo runbook, slide/demo cuối; không nhận trách nhiệm sửa bug module người khác.

## Nguyên tắc tích hợp sớm

**Cuối ngày thiết kế đầu tiên**, chốt `Envelope` + `BookingCommand` + `RaftStateStore`. Có thể code UI, DB schema, TCP và Raft stub song song mà không merge trùng file. Sau đó tích hợp *một đường đi* từ UI đến Node trước khi mở 5 Node.

**Không** sửa TODO bằng `return true`, không tạo Leader giả và không trả BOOKING_RESULT success khi chưa commit.
