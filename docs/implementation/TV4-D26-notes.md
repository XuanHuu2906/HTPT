# TV4-D26 — Catch-up replication snapshot

1. **Đã chạy**
   - Targeted: `FollowerCatchupTest`, `PartitionReplicationTest`, `RaftCrashConsistencyTest`, `StaleLeaderLogTest`.
   - Full backend reactor: `mvn -B -ntp -pl backend-node -am clean verify`.
   - Production replication không cần đổi; không thêm log mới trong D26.

2. **Kết quả fake**
   - `FollowerCatchupTest`: 13 tests, 0 failures, 0 errors.
   - `PartitionReplicationTest`: 9 tests, 0 failures, 0 errors.
   - `RaftCrashConsistencyTest`: 10 tests, 5 JDBC/PostgreSQL tests skipped khi thiếu PG; 5 in-memory tests pass.
   - `StaleLeaderLogTest`: 10 tests, 0 failures, 0 errors.
   - Full backend reactor: 1348 tests, 0 failures, 0 errors, 146 skipped, `BUILD SUCCESS`.

3. **Live snapshot**
   - Docker daemon không kết nối được qua pipe Docker Desktop Linux engine.
   - Không chạy compose, không dừng/restart container, không thu PG/raft log thật.
   - Trạng thái: `NOT_RUN`; không tuyên bố G6 live hoặc G6 PASS.

4. **Vấn đề mở cần thống nhất**
   - D21-12: follower mất đĩa có thể giữ `matchIndex=24`, follower log=0, nextIndex=25; nhóm cần policy re-seed hoặc thay node, không được ACK/SUCCESS giả.
   - Leader cần NO_OP `currentTerm` khi trở thành Leader nếu muốn read barrier nhanh; chỗ này cần TV5 xác nhận wiring.
   - `awaitIndex` timeout/cancellation và waiter lifecycle cần TV5/TV2 review.

5. **G6 checklist TV4**
   - So sánh `raft_log` và `raft_apply_progress.last_applied` trên 5 Node sau catch-up.
   - Xác nhận không commit mất, không split-brain commit, follower catch-up tự động đến cùng committed prefix.
