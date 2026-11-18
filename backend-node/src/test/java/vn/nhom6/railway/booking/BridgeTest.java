package vn.nhom6.railway.booking;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử cho {@link BookingRaftBridge}.
 *
 * <p>Tuân thủ SPEC-04 §4 (chỉ Leader propose), §5 (apply sau commit), §7, §9;
 * SPEC-03 §4.4 (trạng thái, TIMEOUT_UNKNOWN, NOT_LEADER + leaderHint), §6;
 * SPEC-05 (idempotency, request_results).
 *
 * <p>Bao gồm 13 bài test bắt buộc theo SPEC:
 * <ul>
 *   <li>Nhóm không DB (dùng BookingStateMachine giả lập) luôn chạy.</li>
 *   <li>Nhóm DB chạy trên PostgreSQL cô lập schema {@code railway_test_*}, dọn sạch schema trong finally.</li>
 * </ul>
 */
class BridgeTest {

    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_3 = "33333333-3333-3333-3333-333333333333";

    private static final Instant TIME_1 = Instant.parse("2026-11-15T08:00:00Z");
    private static final Instant TIME_2 = Instant.parse("2026-11-15T08:01:00Z");

    // =========================================================================
    // Fake Classes CHỈ DÙNG TRONG TEST (không phải Raft thật)
    // =========================================================================

    /**
     * FakeRaft: Lớp giả lập RaftPort cho unit test. KHÔNG PHẢI RAFT THẬT.
     */
    static final class FakeRaft implements BookingRaftBridge.RaftPort {
        Role role = Role.LEADER;
        String leaderHint = "node-1";
        long currentTerm = 1;
        long nextLogIndex = 1;
        final List<AppendedCommand> log = new ArrayList<>();
        RaftNode.AppendOutcome nextAppendOutcome = null;

        record AppendedCommand(long index, long term, String type, String payload) {}

        @Override
        public Role role() {
            return role;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderHint);
        }

        @Override
        public long currentTerm() {
            return currentTerm;
        }

        @Override
        public synchronized RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
            if (nextAppendOutcome != null) {
                RaftNode.AppendOutcome outcome = nextAppendOutcome;
                nextAppendOutcome = null;
                return outcome;
            }
            if (role != Role.LEADER) {
                return new RaftNode.NotLeader(leaderHint);
            }
            long index = nextLogIndex++;
            log.add(new AppendedCommand(index, currentTerm, commandType, commandPayload));
            return new RaftNode.Accepted(index, currentTerm);
        }
    }

    /**
     * FakeCommitSource: Giả lập nguồn commit từ Raft chuyển giao vào RaftApplyAdapter.
     */
    static final class FakeCommitSource {
        private final RaftApplyAdapter adapter;

        FakeCommitSource(RaftApplyAdapter adapter) {
            this.adapter = adapter;
        }

        void commitAndDeliver(long index, long term, String type, String payload) {
            adapter.advanceCommitIndex(index);
            RaftLogEntry entry = new RaftLogEntry(index, term, type, payload);
            adapter.onCommitted(entry);
        }

        void commitAndDeliver(FakeRaft.AppendedCommand cmd) {
            commitAndDeliver(cmd.index(), cmd.term(), cmd.type(), cmd.payload());
        }
    }

    private static BookingCommand.BookTicket bookCommand(
            String requestId, String seatId, String passengerName, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(requestId, "SE-001", seatId, passengerName, ticketId, issuedAt);
    }

    // =========================================================================
    // 13 BÀI TEST BẮT BUỘC THEO SPEC
    // =========================================================================

    /**
     * 1. Đường đầy đủ submit→append→commit→apply→result:
     * BOOK thành công trả CONFIRMED đúng ticketId; thứ tự gọi: append trước, apply sau commit.
     */
    @Test
    @DisplayName("1. Đường đầy đủ submit→append→commit→apply→result: CONFIRMED đúng ticketId")
    void test01_fullPathSubmitAppendCommitApplyResult() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        List<String> order = new ArrayList<>();

        RaftApplyAdapter adapter = new RaftApplyAdapter(
                (idx, cmd) -> {
                    order.add("APPLY_" + idx);
                    return Optional.of(sm.apply(idx, cmd));
                },
                null,
                null,
                null);

        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Alice", "tk-001", TIME_1);

            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

            // Kiểm tra đã append vào Raft log nhưng chưa apply
            assertEquals(1, raft.log.size());
            assertEquals(0, sm.lastApplied());
            assertFalse(future.isDone(), "Response không được hoàn tất trước khi commit");

            order.add("COMMIT_1");
            commitSource.commitAndDeliver(raft.log.get(0));

            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
            assertEquals("tk-001", result.ticketId());
            assertEquals(1L, result.appliedLogIndex());
            assertNull(result.reasonCode());

            // Thứ tự gọi: COMMIT_1 rồi mới đến APPLY_1
            assertEquals(List.of("COMMIT_1", "APPLY_1"), order);
            assertEquals(Optional.of("BOOKED"), sm.seatStatus("G01"));
        }
    }

    /**
     * 2. Chưa commit: response không hoàn tất, không CONFIRMED;
     * chỉ hoàn tất sau khi FakeCommitSource commit và apply xong.
     */
    @Test
    @DisplayName("2. Chưa commit: response không hoàn tất; chỉ hoàn tất sau commit+apply")
    void test02_responseNotCompleteUntilCommittedAndApplied() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Bob", "tk-002", TIME_1);
            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

            assertFalse(future.isDone());
            assertEquals(0L, adapter.lastApplied());

            // Chờ một khoảng thời gian nhỏ, future vẫn chưa hoàn tất
            assertThrows(TimeoutException.class, () -> future.get(50, TimeUnit.MILLISECONDS));

            // Commit entry
            commitSource.commitAndDeliver(raft.log.get(0));

            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertTrue(future.isDone());
            assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        }
    }

    /**
     * 3. Tại thời điểm response CONFIRMED, DB (connection khác) đã thấy vé COMMIT.
     * (Kiểm thử trên PostgreSQL thật).
     */
    @Test
    @DisplayName("3. Tại thời điểm response CONFIRMED, DB (connection khác) đã thấy vé COMMIT")
    void test03_dbConnectionSeesCommittedTicketWhenResponseConfirmed() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
            FakeRaft raft = new FakeRaft();
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofSeconds(2))) {
                BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Charlie", "tk-003", TIME_1);
                CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

                // Ghi raft_log vào DB để thỏa ràng buộc FK trước khi applier chạy
                db.insertCommittedLog(1, cmd);
                commitSource.commitAndDeliver(raft.log.get(0));

                ApplyResult result = future.get(1, TimeUnit.SECONDS);
                assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());

                // Connection ĐỘC LẬP khác query trực tiếp database
                int activeTickets = db.queryInt(
                        "SELECT count(*) FROM tickets WHERE ticket_id = 'tk-003' AND status = 'ACTIVE'");
                assertEquals(1, activeTickets, "DB phải thấy vé ACTIVE ngay khi CONFIRMED");

                String seatStatus = db.queryString(
                        "SELECT status FROM seats WHERE trip_id = 'SE-001' AND seat_id = 'G01'");
                assertEquals("BOOKED", seatStatus, "Ghế phải BOOKED trong DB");
            }
        }
    }

    /**
     * 4. Không phải Leader: NOT_LEADER + leaderHint (cả trường hợp hint null và có hint), không append.
     */
    @Test
    @DisplayName("4. Không phải Leader: NOT_LEADER + leaderHint, không append")
    void test04_notLeaderReturnsExceptionWithHintAndDoesNotAppend() {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Dave", "tk-004", TIME_1);

            // Trường hợp 4a: FOLLOWER có leaderHint "node-2"
            raft.role = Role.FOLLOWER;
            raft.leaderHint = "node-2";
            BookingRequestHandler.NotLeaderException ex1 =
                    assertThrows(BookingRequestHandler.NotLeaderException.class, () -> bridge.submitCommand(cmd));
            assertEquals("node-2", ex1.leaderHint());
            assertEquals(0, raft.log.size(), "Không được append khi không phải Leader");

            // Trường hợp 4b: CANDIDATE không có leaderHint (null)
            raft.role = Role.CANDIDATE;
            raft.leaderHint = null;
            BookingRequestHandler.NotLeaderException ex2 =
                    assertThrows(BookingRequestHandler.NotLeaderException.class, () -> bridge.submitCommand(cmd));
            assertNull(ex2.leaderHint());
            assertEquals(0, raft.log.size(), "Không được append khi không có Leader");
        }
    }

    /**
     * 5. Hết hạn chờ khi commit chưa tới: TIMEOUT_UNKNOWN, không CONFIRMED/REJECTED,
     * không có dòng nào lưu UNKNOWN trong request_results.
     */
    @Test
    @DisplayName("5. Hết hạn chờ khi commit chưa tới: TIMEOUT_UNKNOWN, không lưu UNKNOWN vào DB")
    void test05_timeoutReturnsUnknownWithoutPersistingToDatabase() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
            FakeRaft raft = new FakeRaft();

            // Timeout ngắn 100ms
            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofMillis(100))) {
                BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Eve", "tk-005", TIME_1);
                CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

                // Không commit
                ApplyResult result = future.get(2, TimeUnit.SECONDS);
                assertEquals(ApplyResult.Outcome.UNKNOWN, result.outcome());
                assertEquals(ApplyResult.TIMEOUT_UNKNOWN, result.reasonCode());
                assertNull(result.ticketId());
                assertNull(result.appliedLogIndex());

                // Xác minh request_results KHÔNG có dòng nào lưu UNKNOWN
                int unknownRows = db.queryInt(
                        "SELECT count(*) FROM request_results WHERE request_id = '" + REQUEST_1 + "'");
                assertEquals(0, unknownRows, "Tuyệt đối không lưu UNKNOWN vào request_results");
            }
        }
    }

    /**
     * 6. Step-down sau append: UNKNOWN; sau đó entry commit và apply muộn → tra cứu theo requestId trả kết quả gốc.
     */
    @Test
    @DisplayName("6. Step-down sau append: UNKNOWN; commit muộn tra cứu trả kết quả gốc")
    void test06_stepDownReturnsUnknownAndLateApplyEnablesLookup() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Frank", "tk-006", TIME_1);
            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

            assertEquals(1, raft.log.size());

            // Node bị step down sau khi đã append
            raft.role = Role.FOLLOWER;
            raft.leaderHint = "node-2";
            bridge.onStepDown();

            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.UNKNOWN, result.outcome());

            // Entry commit muộn qua Raft
            commitSource.commitAndDeliver(raft.log.get(0));

            // Tra cứu lại theo requestId phải ra kết quả gốc CONFIRMED
            Optional<ApplyResult> lookup = sm.storedResult(REQUEST_1);
            assertTrue(lookup.isPresent());
            assertEquals(ApplyResult.Outcome.CONFIRMED, lookup.get().outcome());
            assertEquals("tk-006", lookup.get().ticketId());
        }
    }

    /**
     * 7. Entry bị ghi đè (term khác tại index): UNKNOWN, không CONFIRMED.
     */
    @Test
    @DisplayName("7. Entry bị ghi đè: UNKNOWN, không CONFIRMED")
    void test07_overwrittenEntryReturnsUnknown() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Grace", "tk-007", TIME_1);
            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

            assertEquals(1, raft.log.size());

            // Báo cho bridge biết log tại index 1 bị ghi đè bởi term 2
            bridge.onEntryOverwritten(1, 2);

            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.UNKNOWN, result.outcome());
            assertNotEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        }
    }

    /**
     * 8. Gửi lại cùng requestId khi đang PENDING: KHÔNG append lần hai, trả chung một kết quả;
     * sau khi APPLIED gửi lại trả kết quả gốc.
     */
    @Test
    @DisplayName("8. Gửi lại cùng requestId khi đang PENDING: không append lần hai; sau APPLIED trả kết quả gốc")
    void test08_pendingAndAppliedIdempotencyReusesFutureAndDoesNotAppend() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Heidi", "tk-008", TIME_1);

            // Lần 1: Submit khi chưa commit
            CompletableFuture<ApplyResult> future1 = bridge.submitCommand(cmd);
            assertEquals(1, raft.log.size());

            // Lần 2: Submit lại cùng requestId và fingerprint khi đang PENDING
            CompletableFuture<ApplyResult> future2 = bridge.submitCommand(cmd);
            assertEquals(1, raft.log.size(), "KHÔNG được append lần hai khi đang PENDING");

            // Commit entry
            commitSource.commitAndDeliver(raft.log.get(0));

            ApplyResult res1 = future1.get(1, TimeUnit.SECONDS);
            ApplyResult res2 = future2.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.CONFIRMED, res1.outcome());
            assertEquals("tk-008", res1.ticketId());
            assertEquals(res1, res2);

            // Lần 3: Submit lại sau khi đã APPLIED thành công
            CompletableFuture<ApplyResult> future3 = bridge.submitCommand(cmd);
            assertEquals(1, raft.log.size(), "KHÔNG được append sau khi đã APPLIED");
            ApplyResult res3 = future3.get(1, TimeUnit.SECONDS);
            assertEquals(res1, res3, "Trả về đúng kết quả gốc đã lưu");
        }
    }

    /**
     * 9. Cùng requestId khác fingerprint: REQUEST_ID_CONFLICT, không append.
     */
    @Test
    @DisplayName("9. Cùng requestId khác fingerprint: REQUEST_ID_CONFLICT, không append")
    void test09_sameRequestIdDifferentPayloadThrowsConflictAndDoesNotAppend() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            BookingCommand.BookTicket cmd1 = bookCommand(REQUEST_1, "G01", "Ivan", "tk-009A", TIME_1);
            CompletableFuture<ApplyResult> future1 = bridge.submitCommand(cmd1);

            // Trường hợp 9a: Đang PENDING nhưng gửi lại khác payload (ghế G02)
            BookingCommand.BookTicket conflictPending = bookCommand(REQUEST_1, "G02", "Ivan", "tk-009B", TIME_1);
            assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                    () -> bridge.submitCommand(conflictPending));
            assertEquals(1, raft.log.size(), "Không được append lệnh conflict");

            // Commit lệnh 1
            commitSource.commitAndDeliver(raft.log.get(0));
            assertEquals(ApplyResult.Outcome.CONFIRMED, future1.get(1, TimeUnit.SECONDS).outcome());

            // Trường hợp 9b: Đã APPLIED nhưng gửi lại khác payload (đổi tên hành khách)
            BookingCommand.BookTicket conflictApplied = bookCommand(REQUEST_1, "G01", "Ivan Conflict", "tk-009C", TIME_1);
            assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                    () -> bridge.submitCommand(conflictApplied));
            assertEquals(1, raft.log.size(), "Không được append sau khi đã conflict");
        }
    }

    /**
     * 10. Business rejected (ghế đã bán): REJECTED đúng reasonCode, commit nhưng không tạo vé thứ hai.
     */
    @Test
    @DisplayName("10. Ghế đã bán: REJECTED SEAT_ALREADY_BOOKED, không tạo vé thứ hai")
    void test10_alreadyBookedSeatReturnsRejectedWithoutSecondTicket() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            // Lệnh 1: Đặt G01 thành công
            BookingCommand.BookTicket cmd1 = bookCommand(REQUEST_1, "G01", "Judy", "tk-010A", TIME_1);
            CompletableFuture<ApplyResult> future1 = bridge.submitCommand(cmd1);
            commitSource.commitAndDeliver(raft.log.get(0));
            assertEquals(ApplyResult.Outcome.CONFIRMED, future1.get(1, TimeUnit.SECONDS).outcome());

            // Lệnh 2: Đặt tiếp G01 với requestId khác
            BookingCommand.BookTicket cmd2 = bookCommand(REQUEST_2, "G01", "Mallory", "tk-010B", TIME_2);
            CompletableFuture<ApplyResult> future2 = bridge.submitCommand(cmd2);
            commitSource.commitAndDeliver(raft.log.get(1));

            ApplyResult result2 = future2.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.REJECTED, result2.outcome());
            assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, result2.reasonCode());
            assertNull(result2.ticketId());
            assertEquals(Optional.of("BOOKED"), sm.seatStatus("G01"));
        }
    }

    /**
     * 11. Lỗi append (NOT_LEADER giữa chừng): NOT_LEADER, không UNKNOWN, không để lại PENDING rác.
     */
    @Test
    @DisplayName("11. Lỗi append NOT_LEADER: ném NOT_LEADER, không để lại PENDING rác")
    void test11_appendNotLeaderThrowsNotLeaderAndCleansPending() {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            // raft.role ban đầu là LEADER, nhưng khi gọi appendCommand trả về NotLeader("node-3")
            raft.nextAppendOutcome = new RaftNode.NotLeader("node-3");

            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Oscar", "tk-011", TIME_1);
            BookingRequestHandler.NotLeaderException ex =
                    assertThrows(BookingRequestHandler.NotLeaderException.class, () -> bridge.submitCommand(cmd));

            assertEquals("node-3", ex.leaderHint());
            // Trạng thái trong adapter phải là NONE, không còn PENDING rác
            assertEquals(RaftApplyAdapter.RequestState.NONE, adapter.requestState(REQUEST_1));
        }
    }

    /**
     * 12. Hai BOOK cùng ghế đi qua bridge tuần tự: đúng một CONFIRMED, một REJECTED; DB đúng một vé ACTIVE.
     * (Kiểm thử trên PostgreSQL thật).
     */
    @Test
    @DisplayName("12. Hai BOOK cùng ghế tuần tự: 1 CONFIRMED, 1 REJECTED; DB đúng 1 vé ACTIVE")
    void test12_sequentialBooksOnSameSeatOneConfirmedOneRejectedDatabase() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
            FakeRaft raft = new FakeRaft();
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofSeconds(2))) {
                BookingCommand.BookTicket cmd1 = bookCommand(REQUEST_1, "G01", "Peggy", "tk-012A", TIME_1);
                CompletableFuture<ApplyResult> future1 = bridge.submitCommand(cmd1);
                db.insertCommittedLog(1, cmd1);
                commitSource.commitAndDeliver(raft.log.get(0));
                ApplyResult res1 = future1.get(1, TimeUnit.SECONDS);
                assertEquals(ApplyResult.Outcome.CONFIRMED, res1.outcome());

                BookingCommand.BookTicket cmd2 = bookCommand(REQUEST_2, "G01", "Rupert", "tk-012B", TIME_2);
                CompletableFuture<ApplyResult> future2 = bridge.submitCommand(cmd2);
                db.insertCommittedLog(2, cmd2);
                commitSource.commitAndDeliver(raft.log.get(1));
                ApplyResult res2 = future2.get(1, TimeUnit.SECONDS);
                assertEquals(ApplyResult.Outcome.REJECTED, res2.outcome());
                assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, res2.reasonCode());

                // Database: đúng 1 vé ACTIVE cho ghế G01
                int activeTickets = db.queryInt(
                        "SELECT count(*) FROM tickets WHERE trip_id = 'SE-001' AND seat_id = 'G01' AND status = 'ACTIVE'");
                assertEquals(1, activeTickets);
            }
        }
    }

    /**
     * 13. Không có đường nào phát CONFIRMED khi entry chưa commit/apply.
     */
    @Test
    @DisplayName("13. Bất biến safety: Không bao giờ có CONFIRMED khi entry chưa commit/apply")
    void test13_neverConfirmedBeforeCommitAndApply() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofMillis(200))) {
            BookingCommand.BookTicket cmd = bookCommand(REQUEST_1, "G01", "Sybil", "tk-013", TIME_1);
            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);

            // Khi chưa commit, future chưa done
            assertFalse(future.isDone());

            // Đợi timeout
            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertNotEquals(ApplyResult.Outcome.CONFIRMED, result.outcome(),
                    "Không được phát CONFIRMED khi chưa commit");
            assertEquals(ApplyResult.Outcome.UNKNOWN, result.outcome());
            assertEquals(0L, sm.lastApplied());
        }
    }

    // =========================================================================
    // PostgreSQL Test Infrastructure
    // =========================================================================

    private static void requireDatabase() {
        Assumptions.assumeTrue(isDatabaseConfigured(), "Bỏ qua test DB vì thiếu biến môi trường RAILWAY_TEST_*");
    }

    private static boolean isDatabaseConfigured() {
        return isSet(System.getenv("RAILWAY_TEST_JDBC_URL"))
                && isSet(System.getenv("RAILWAY_TEST_DB_USER"))
                && isSet(System.getenv("RAILWAY_TEST_DB_PASSWORD"));
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    private static TestDatabase openDatabase() throws Exception {
        return TestDatabase.open(
                System.getenv("RAILWAY_TEST_JDBC_URL"),
                System.getenv("RAILWAY_TEST_DB_USER"),
                System.getenv("RAILWAY_TEST_DB_PASSWORD"));
    }

    private static final class TestDatabase implements AutoCloseable {
        private final String baseUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final ConnectionFactory connectionFactory;

        private TestDatabase(String jdbcUrl, String username, String password) throws Exception {
            this.baseUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
            createSchema();
            try {
                URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
                String dbName = uri.getPath().substring(1) + "?currentSchema=" + schema;
                DbConfig config = new DbConfig(
                        uri.getHost(), uri.getPort() < 0 ? 5432 : uri.getPort(),
                        dbName, username, password);
                this.connectionFactory = new ConnectionFactory(config);
                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
            } catch (Exception exception) {
                try {
                    dropSchema();
                } catch (Exception ignored) {
                }
                throw exception;
            }
        }

        static TestDatabase open(String jdbcUrl, String username, String password) throws Exception {
            return new TestDatabase(jdbcUrl, username, password);
        }

        ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        void insertCommittedLog(long logIndex, BookingCommand command) throws SQLException {
            String commandType = command.type().name();
            String commandJson;
            if (command instanceof BookingCommand.BookTicket book) {
                commandJson = String.format(
                        "{\"requestId\":\"%s\",\"tripId\":\"%s\",\"seatId\":\"%s\",\"passengerName\":\"%s\",\"ticketId\":\"%s\",\"issuedAt\":\"%s\"}",
                        book.requestId(), book.tripId(), book.seatId(),
                        book.passengerName().replace("\"", "\\\""),
                        book.ticketId(), book.issuedAt());
            } else if (command instanceof BookingCommand.CancelTicket cancel) {
                commandJson = String.format(
                        "{\"requestId\":\"%s\",\"ticketId\":\"%s\",\"cancelledAt\":\"%s\"}",
                        cancel.requestId(), cancel.ticketId(), cancel.cancelledAt());
            } else if (command instanceof BookingCommand.NoOp) {
                commandJson = "{}";
            } else {
                throw new IllegalArgumentException("Unknown command type: " + command.getClass());
            }

            try (Connection conn = connectionFactory.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, 1, ?, ?::jsonb)")) {
                ps.setLong(1, logIndex);
                ps.setString(2, commandType);
                ps.setString(3, commandJson);
                ps.executeUpdate();
            }
        }

        int queryInt(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                throw new IllegalStateException("Empty result set for: " + sql);
            }
        }

        String queryString(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        }

        private void createSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                 Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA " + schema);
            }
        }

        private void dropSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                 Statement st = conn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }

        private void runMigration(String filename) throws Exception {
            String sql;
            try (InputStream in = getClass().getClassLoader().getResourceAsStream("migration/" + filename)) {
                if (in == null) {
                    try (InputStream fallback = getClass().getClassLoader().getResourceAsStream(filename)) {
                        if (fallback == null) {
                            java.nio.file.Path rootPath = java.nio.file.Paths.get("..", "database", "migration", filename);
                            if (!java.nio.file.Files.exists(rootPath)) {
                                rootPath = java.nio.file.Paths.get("database", "migration", filename);
                            }
                            sql = java.nio.file.Files.readString(rootPath, StandardCharsets.UTF_8);
                        } else {
                            sql = new String(fallback.readAllBytes(), StandardCharsets.UTF_8);
                        }
                    }
                } else {
                    sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }

            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }

        @Override
        public void close() throws Exception {
            dropSchema();
        }
    }
}
