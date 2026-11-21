package vn.nhom6.railway.booking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.raft.core.RaftLogEntry;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB Follower Catch-up Assertions bằng Fake Log (SPEC-04 §5, §6; SPEC-05 §3,
 * §5; SPEC-07 §1, §5).
 *
 * <p>
 * Mức bằng chứng: FAKE+LOCAL (dùng FakeCommitSource, chạy trên 2 schema
 * PostgreSQL thật trong cùng DB test).
 * Đánh dấu phụ thuộc:
 * <ul>
 * <li>Replay TV4-D18: BLOCKED_INTEGRATION (dùng FakeCommitSource kiểm thử lớp
 * DB/apply).</li>
 * <li>Catch-up thật TV4-D21 & fault runner TV6: BLOCKED_INTEGRATION (chờ mốc
 * tích hợp G5).</li>
 * </ul>
 *
 * <p>
 * Bao gồm 12 bài kiểm thử bắt buộc:
 * <ol>
 * <li>Follower chậm trước catch-up: lastApplied = K, dữ liệu đúng prefix K,
 * không có dữ liệu > K.</li>
 * <li>Catch-up đầy đủ: sau khi giao K+1..N, follower khớp 100% node tham
 * chiếu.</li>
 * <li>Catch-up theo lô nhỏ: lastApplied tăng đơn điệu, không nhảy cóc, kết quả
 * cuối khớp.</li>
 * <li>Ngắt giữa catch-up: dừng ở M, restart adapter trên cùng DB, phát lại từ
 * 1, tiếp tục tới N, khớp tham chiếu.</li>
 * <li>Entry trùng/giao lại (redelivery): idempotent, không đổi dữ liệu, không
 * tạo vé thứ hai.</li>
 * <li>Entry chưa commit (index > commitIndex): follower KHÔNG apply; chỉ apply
 * sau khi commitIndex tăng.</li>
 * <li>Lỗ index trong log: fail closed, không apply entry sau lỗ, lastApplied
 * không tăng, dữ liệu giữ nguyên.</li>
 * <li>Lỗi DB giữa catch-up: lastApplied không tăng cho entry lỗi, rollback
 * sạch; gỡ lỗi gọi lại thành công.</li>
 * <li>Kết quả REJECTED và NO_OP: replay chính xác, REJECTED lưu reasonCode,
 * NO_OP chỉ tăng lastApplied.</li>
 * <li>Bất biến sau catch-up: <= 1 vé ACTIVE/ghế, seats khớp tickets, không có
 * UNKNOWN, đúng 10 ghế.</li>
 * <li>Follower không tự ghi dữ liệu nghiệp vụ: chỉ apply entry nhận từ log đã
 * commit.</li>
 * <li>Lặp 20 vòng với seed khác nhau cho điểm K/M và kích thước lô: mọi vòng
 * đều khớp node tham chiếu.</li>
 * </ol>
 */
public class DbCatchupConsistencyTest {

    private static final String TRIP_ID = "SE-001";
    private static final Instant BASE_TIME = Instant.parse("2026-11-21T10:00:00Z");

    private String jdbcUrl;
    private String username;
    private String password;

    private TestDb referenceDb;
    private TestDb followerDb;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        username = System.getenv("RAILWAY_TEST_DB_USER");
        password = System.getenv("RAILWAY_TEST_DB_PASSWORD");

        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Bỏ qua vì thiếu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER hoặc RAILWAY_TEST_DB_PASSWORD");

        referenceDb = new TestDb(jdbcUrl, username, password, "ref");
        followerDb = new TestDb(jdbcUrl, username, password, "fol");
    }

    @AfterEach
    void tearDown() {
        if (referenceDb != null) {
            try {
                referenceDb.close();
            } catch (Exception ignored) {
            }
        }
        if (followerDb != null) {
            try {
                followerDb.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static boolean hasText(String str) {
        return str != null && !str.isBlank();
    }

    // =========================================================================
    // Fake Log Entries (Danh sách N = 12 entries hỗn hợp)
    // =========================================================================

    record LogItem(long index, long term, String type, String payload, BookingCommand command) {
    }

    private List<LogItem> createStandardLog() {
        List<LogItem> log = new ArrayList<>();
        // 1. BOOK G01 thành công
        log.add(makeBookItem(1, 1, "11111111-1111-1111-1111-111111111111", "G01", "Nguyen Van A", "TK-001", 0));
        // 2. BOOK G02 thành công
        log.add(makeBookItem(2, 1, "22222222-2222-2222-2222-222222222222", "G02", "Tran Thi B", "TK-002", 1));
        // 3. BOOK G01 trùng ghế -> REJECTED
        log.add(makeBookItem(3, 1, "33333333-3333-3333-3333-333333333333", "G01", "Le Van C", "TK-003", 2));
        // 4. NO_OP -> chỉ tăng lastApplied
        log.add(makeNoOpItem(4, 1));
        // 5. CANCEL G02 (TK-002) -> APPLIED_SUCCESS
        log.add(makeCancelItem(5, 1, "44444444-4444-4444-4444-444444444444", "TK-002", 3));
        // 6. BOOK lại G02 -> APPLIED_SUCCESS
        log.add(makeBookItem(6, 1, "55555555-5555-5555-5555-555555555555", "G02", "Pham Thi D", "TK-004", 4));
        // 7. BOOK G03 thành công
        log.add(makeBookItem(7, 1, "66666666-6666-6666-6666-666666666666", "G03", "Hoang Van E", "TK-005", 5));
        // 8. BOOK G04 thành công
        log.add(makeBookItem(8, 1, "77777777-7777-7777-7777-777777777777", "G04", "Vu Thi F", "TK-006", 6));
        // 9. BOOK G03 trùng ghế -> REJECTED
        log.add(makeBookItem(9, 1, "88888888-8888-8888-8888-888888888888", "G03", "Dang Van G", "TK-007", 7));
        // 10. NO_OP -> chỉ tăng lastApplied
        log.add(makeNoOpItem(10, 1));
        // 11. CANCEL G01 (TK-001) -> APPLIED_SUCCESS
        log.add(makeCancelItem(11, 1, "99999999-9999-9999-9999-999999999999", "TK-001", 8));
        // 12. BOOK lại G01 -> APPLIED_SUCCESS
        log.add(makeBookItem(12, 1, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "G01", "Bui Van H", "TK-008", 9));
        return Collections.unmodifiableList(log);
    }

    private LogItem makeBookItem(long index, long term, String reqId, String seatId, String name, String ticketId,
            int minuteOffset) {
        Instant time = BASE_TIME.plusSeconds(minuteOffset * 60L);
        BookingCommand.BookTicket cmd = new BookingCommand.BookTicket(reqId, TRIP_ID, seatId, name, ticketId, time);
        String payload = String.format(
                "{\"requestId\":\"%s\",\"tripId\":\"%s\",\"seatId\":\"%s\",\"passengerName\":\"%s\",\"ticketId\":\"%s\",\"issuedAt\":\"%s\"}",
                reqId, TRIP_ID, seatId, name, ticketId, time);
        return new LogItem(index, term, "BOOK_TICKET", payload, cmd);
    }

    private LogItem makeCancelItem(long index, long term, String reqId, String ticketId, int minuteOffset) {
        Instant time = BASE_TIME.plusSeconds(minuteOffset * 60L);
        BookingCommand.CancelTicket cmd = new BookingCommand.CancelTicket(reqId, ticketId, time);
        String payload = String.format("{\"requestId\":\"%s\",\"ticketId\":\"%s\",\"cancelledAt\":\"%s\"}", reqId,
                ticketId, time);
        return new LogItem(index, term, "CANCEL_TICKET", payload, cmd);
    }

    private LogItem makeNoOpItem(long index, long term) {
        return new LogItem(index, term, "NO_OP", "{}", new BookingCommand.NoOp());
    }

    // =========================================================================
    // FakeCommitSource (LỚP GIẢ CHỈ DÙNG TRONG TEST, KHÔNG PHẢI RAFT THẬT)
    // =========================================================================

    /**
     * FakeCommitSource đóng vai trò nguồn phát lại các log entry đã commit cho
     * RaftApplyAdapter.
     * Chú ý: Đây là lớp giả lập cục bộ dùng cho kiểm thử, không phải cơ chế
     * replication thật của Raft.
     */
    static final class FakeCommitSource {
        private final RaftApplyAdapter adapter;
        private final TestDb testDb;

        FakeCommitSource(RaftApplyAdapter adapter, TestDb testDb) {
            this.adapter = Objects.requireNonNull(adapter, "adapter");
            this.testDb = testDb;
        }

        void commitAndDeliver(LogItem item) throws SQLException {
            commitAndDeliver(item.index(), item.term(), item.type(), item.payload(), item.command());
        }

        void commitAndDeliver(long index, long term, String type, String payload, BookingCommand cmd)
                throws SQLException {
            if (testDb != null && cmd != null) {
                testDb.insertCommittedLog(index, term, type, payload);
            }
            adapter.advanceCommitIndex(index);
            RaftLogEntry entry = new RaftLogEntry(index, term, type, payload);
            adapter.onCommitted(entry);
        }

        void deliverWithoutAdvanceCommit(LogItem item) {
            RaftLogEntry entry = new RaftLogEntry(item.index(), item.term(), item.type(), item.payload());
            adapter.onCommitted(entry);
        }
    }

    // =========================================================================
    // 12 BÀI KIỂM THỬ BẮT BUỘC
    // =========================================================================

    @Test
    @DisplayName("1. Follower chậm trước catch-up: lastApplied = K, dữ liệu đúng prefix K, không có dữ liệu > K")
    void test01_followerBeforeCatchupHasPrefixDataOnly() throws Exception {
        List<LogItem> log = createStandardLog();
        int K = 5;

        // Node tham chiếu apply toàn bộ N=12
        applyLogRange(referenceDb, log, 0, log.size());
        assertEquals(12L, referenceDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

        // Follower chậm chỉ apply prefix K=5
        applyLogRange(followerDb, log, 0, K);
        assertEquals(5L, followerDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

        // Kiểm tra follower không chứa dữ liệu của entry > K
        assertEquals(0, followerDb.queryInt(
                "SELECT count(*) FROM tickets WHERE ticket_id IN ('TK-004', 'TK-005', 'TK-006', 'TK-007', 'TK-008')"));
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index > 5"));

        // Kiểm tra đúng dữ liệu của prefix K=5
        assertEquals(1,
                followerDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'TK-001' AND status = 'ACTIVE'"));
        assertEquals(1, followerDb
                .queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'TK-002' AND status = 'CANCELLED'"));
        assertEquals("AVAILABLE", followerDb.queryString("SELECT status FROM seats WHERE seat_id = 'G02'"));
    }

    @Test
    @DisplayName("2. Catch-up đầy đủ: sau khi giao K+1..N, follower khớp 100% node tham chiếu")
    void test02_fullCatchupMatchesReferenceNodeCompletely() throws Exception {
        List<LogItem> log = createStandardLog();
        int K = 5;

        applyLogRange(referenceDb, log, 0, log.size());

        // Follower áp dụng K=5 đầu tiên
        applyLogRange(followerDb, log, 0, K);

        // Sau đó catch-up phần còn lại: K..12
        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        for (int i = K; i < log.size(); i++) {
            folSource.commitAndDeliver(log.get(i));
        }

        // Đối chiếu 100% giữa referenceDb và followerDb
        assertSchemasConsistent(referenceDb, followerDb);
    }

    @Test
    @DisplayName("3. Catch-up theo lô nhỏ: lastApplied tăng đơn điệu, không nhảy cóc, kết quả cuối khớp")
    void test03_catchupInSmallBatchesMonotonicProgress() throws Exception {
        List<LogItem> log = createStandardLog();
        int K = 3;

        applyLogRange(referenceDb, log, 0, log.size());
        applyLogRange(followerDb, log, 0, K);

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Chia lô nhỏ: [3..4], [5..7], [8..8], [9..11] (index 0-based: 3 đến 11)
        int[][] batches = { { 3, 5 }, { 5, 8 }, { 8, 9 }, { 9, 12 } };
        long previousLastApplied = K;

        for (int[] batch : batches) {
            for (int i = batch[0]; i < batch[1]; i++) {
                folSource.commitAndDeliver(log.get(i));
                long currentLA = folAdapter.lastApplied();
                assertEquals(previousLastApplied + 1, currentLA, "lastApplied phải tăng đơn điệu từng bước +1");
                previousLastApplied = currentLA;
            }
        }

        assertEquals(12L, folAdapter.lastApplied());
        assertSchemasConsistent(referenceDb, followerDb);
    }

    @Test
    @DisplayName("4. Ngắt giữa catch-up: dừng ở M, restart adapter trên cùng DB, phát lại từ 1, tiếp tục tới N, khớp tham chiếu")
    void test04_interruptedCatchupRestartAndResume() throws Exception {
        List<LogItem> log = createStandardLog();
        int M = 7;

        applyLogRange(referenceDb, log, 0, log.size());

        // Giai đoạn 1: follower apply tới M=7 thì dừng/crash
        applyLogRange(followerDb, log, 0, M);
        assertEquals(7L, followerDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

        // Giai đoạn 2: Restart node/adapter trên cùng database
        RaftApplyAdapter restartedAdapter = createAdapter(followerDb);
        assertEquals(7L, restartedAdapter.lastApplied(), "lastApplied phục hồi từ DB phải đúng bằng 7");
        FakeCommitSource restartedSource = new FakeCommitSource(restartedAdapter, followerDb);

        // Phát lại toàn bộ log từ index 1..7 (replay prefix): hệ thống nhận diện index
        // <= lastApplied và bỏ qua an toàn
        for (int i = 0; i < M; i++) {
            restartedSource.commitAndDeliver(log.get(i));
        }
        assertEquals(7L, restartedAdapter.lastApplied(), "Replay prefix đã apply không được làm thay đổi lastApplied");

        // Tiếp tục catch-up từ M đến N (index 7 đến 11, tức log_index 8..12)
        for (int i = M; i < log.size(); i++) {
            restartedSource.commitAndDeliver(log.get(i));
        }

        assertEquals(12L, restartedAdapter.lastApplied());
        assertSchemasConsistent(referenceDb, followerDb);
    }

    @Test
    @DisplayName("5. Entry trùng/giao lại (redelivery): idempotent, không đổi dữ liệu, không tạo vé thứ hai")
    void test05_duplicateRedeliveryDuringCatchupDoesNotCorrupt() throws Exception {
        List<LogItem> log = createStandardLog();
        applyLogRange(referenceDb, log, 0, log.size());

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Giao các entries với sự trùng lặp có chủ đích
        for (int i = 0; i < log.size(); i++) {
            LogItem item = log.get(i);
            folSource.commitAndDeliver(item);
            // Giao lặp lại ngay lập tức entry vừa giao
            folSource.commitAndDeliver(item);
            if (i == 5) {
                // Giao lặp lại thêm 2 lần nữa cho entry 6
                folSource.commitAndDeliver(item);
                folSource.commitAndDeliver(item);
            }
        }

        assertEquals(12L, folAdapter.lastApplied());
        assertSchemasConsistent(referenceDb, followerDb);
    }

    @Test
    @DisplayName("6. Entry chưa commit (index > commitIndex): follower KHÔNG apply; chỉ apply sau khi commitIndex tăng")
    void test06_uncommittedEntriesNotAppliedUntilCommitIndexAdvances() throws Exception {
        List<LogItem> log = createStandardLog();

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Giao 2 entry đầu tiên
        folSource.commitAndDeliver(log.get(0));
        folSource.commitAndDeliver(log.get(1));
        assertEquals(2L, folAdapter.lastApplied());

        // Entry 3 chưa được commitIndex advance (commitIndex vẫn đang là 2)
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            folSource.deliverWithoutAdvanceCommit(log.get(2));
        });
        assertTrue(ex.getMessage().contains("has not been committed"), "Phải ném ngoại lệ khi entry chưa commit");

        // Dữ liệu không thay đổi
        assertEquals(2L, folAdapter.lastApplied());
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index = 3"));

        // Sau khi advance commitIndex, giao lại entry 3 -> thành công!
        folSource.commitAndDeliver(log.get(2));
        assertEquals(3L, folAdapter.lastApplied());
        assertEquals(1, followerDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index = 3"));
    }

    @Test
    @DisplayName("7. Lỗ index trong log: fail closed, không apply entry sau lỗ, lastApplied không tăng, dữ liệu giữ nguyên")
    void test07_gapInLogIndicesFailsClosed() throws Exception {
        List<LogItem> log = createStandardLog();

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Giao index 1
        folSource.commitAndDeliver(log.get(0));
        assertEquals(1L, folAdapter.lastApplied());

        // Bỏ qua index 2, cố tình giao index 3 (gặp lỗ hổng index)
        folAdapter.advanceCommitIndex(3);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            folSource.deliverWithoutAdvanceCommit(log.get(2));
        });
        assertTrue(
                ex.getMessage().contains("lỗ") || ex.getMessage().contains("thứ tự")
                        || ex.getMessage().toLowerCase().contains("gap"),
                "Phải ném ngoại lệ báo gap log index: " + ex.getMessage());

        // Fail-closed: lastApplied không tăng, dữ liệu entry 3 không được lưu
        assertEquals(1L, folAdapter.lastApplied());
        assertEquals(1L, followerDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index = 3"));
    }

    @Test
    @DisplayName("8. Lỗi DB giữa catch-up: lastApplied không tăng cho entry lỗi, rollback sạch; gỡ lỗi gọi lại thành công")
    void test08_dbErrorDuringCatchupRollsBackAndRetriesSafely() throws Exception {
        List<LogItem> log = createStandardLog();
        applyLogRange(referenceDb, log, 0, log.size());

        // Follower apply 1..5
        applyLogRange(followerDb, log, 0, 5);
        assertEquals(5L, followerDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Chèn lỗi DB tại follower bằng trigger tạm thời trên bảng tickets
        followerDb.executeSql(
                "CREATE OR REPLACE FUNCTION sim_err() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'SIMULATED_DB_ERROR'; END; $$ LANGUAGE plpgsql;");
        followerDb.executeSql(
                "CREATE TRIGGER trg_sim_err BEFORE INSERT ON tickets FOR EACH ROW EXECUTE FUNCTION sim_err();");

        // Cố gắng apply entry 6 (BOOK G02) -> phải gặp lỗi DB và rollback
        assertThrows(RuntimeException.class, () -> {
            folSource.commitAndDeliver(log.get(5));
        });

        // Kiểm tra tính nguyên tử: lastApplied không tăng, không có vé nửa chừng
        assertEquals(5L, folAdapter.lastApplied());
        assertEquals(5L, followerDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'TK-004'"));
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index = 6"));

        // Gỡ bỏ lỗi DB
        followerDb.executeSql("DROP TRIGGER trg_sim_err ON tickets;");
        followerDb.executeSql("DROP FUNCTION sim_err();");

        // Thử lại entry 6 -> thành công!
        folSource.commitAndDeliver(log.get(5));
        assertEquals(6L, folAdapter.lastApplied());
        assertEquals(1, followerDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'TK-004'"));

        // Hoàn tất catch-up các entries còn lại 7..12
        for (int i = 6; i < log.size(); i++) {
            folSource.commitAndDeliver(log.get(i));
        }

        assertSchemasConsistent(referenceDb, followerDb);
    }

    @Test
    @DisplayName("9. Kết quả REJECTED và NO_OP: replay chính xác, REJECTED lưu reasonCode, NO_OP chỉ tăng lastApplied")
    void test09_rejectedAndNoOpReplayedAccurately() throws Exception {
        List<LogItem> log = createStandardLog();

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Apply 1..4 (trong đó entry 3 là REJECTED, entry 4 là NO_OP)
        for (int i = 0; i < 4; i++) {
            folSource.commitAndDeliver(log.get(i));
        }

        // Kiểm tra entry 3: REJECTED được lưu bền vững với reasonCode chính xác
        assertEquals("APPLIED_REJECTED", followerDb.queryString(
                "SELECT state FROM request_results WHERE applied_log_index = 3"));
        assertEquals("SEAT_ALREADY_BOOKED", followerDb.queryString(
                "SELECT reason_code FROM request_results WHERE applied_log_index = 3"));

        // Kiểm tra entry 4: NO_OP chỉ tăng lastApplied, không ghi vào request_results
        // hay tickets
        assertEquals(4L, folAdapter.lastApplied());
        assertEquals(4L, followerDb.queryLong(
                "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, followerDb.queryInt(
                "SELECT count(*) FROM request_results WHERE applied_log_index = 4"));
    }

    @Test
    @DisplayName("10. Bất biến sau catch-up: <= 1 vé ACTIVE/ghế, seats khớp tickets, không có UNKNOWN, đúng 10 ghế")
    void test10_postCatchupInvariantsPreserved() throws Exception {
        List<LogItem> log = createStandardLog();
        applyLogRange(followerDb, log, 0, log.size());

        // Sử dụng connection độc lập để kiểm chứng 4 bất biến cốt lõi
        try (Connection conn = followerDb.openIndependentConnection();
                Statement st = conn.createStatement()) {

            // Invariant 1: Tối đa 1 vé ACTIVE mỗi ghế
            try (ResultSet rs = st.executeQuery(
                    "SELECT seat_id, count(*) FROM tickets WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id HAVING count(*) > 1")) {
                assertFalse(rs.next(), "Không được có ghế nào có nhiều hơn 1 vé ACTIVE");
            }

            // Invariant 2: Trạng thái seats khớp hoàn hảo với tickets ACTIVE
            try (ResultSet rs = st.executeQuery(
                    "SELECT s.seat_id FROM seats s LEFT JOIN tickets t ON s.trip_id = t.trip_id AND s.seat_id = t.seat_id AND t.status = 'ACTIVE' "
                            +
                            "WHERE (s.status = 'BOOKED' AND (t.ticket_id IS NULL OR s.current_ticket_id != t.ticket_id)) "
                            +
                            "   OR (s.status = 'AVAILABLE' AND (s.current_ticket_id IS NOT NULL OR t.ticket_id IS NOT NULL))")) {
                assertFalse(rs.next(), "Trạng thái bảng seats phải khớp hoàn toàn với bảng tickets ACTIVE");
            }

            // Invariant 3: Không có bất kỳ request_results nào ở trạng thái UNKNOWN
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM request_results WHERE state NOT IN ('APPLIED_SUCCESS', 'APPLIED_REJECTED')")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "request_results không được chứa trạng thái UNKNOWN");
            }

            // Invariant 4: Đúng 10 ghế G01-G10 cho SE-001
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM seats WHERE trip_id = 'SE-001' AND seat_id ~ '^G(0[1-9]|10)$'")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1), "Phải có đúng 10 ghế G01-G10");
            }
        }
    }

    @Test
    @DisplayName("11. Follower không tự ghi dữ liệu nghiệp vụ: chỉ apply entry nhận từ log đã commit")
    void test11_followerDoesNotMutateAuthoritativeStateWithoutCommittedEntry() throws Exception {
        List<LogItem> log = createStandardLog();

        RaftApplyAdapter folAdapter = createAdapter(followerDb);
        FakeCommitSource folSource = new FakeCommitSource(folAdapter, followerDb);

        // Trước khi giao bất kỳ entry nào, DB follower ở trạng thái ban đầu
        // (lastApplied = 0, 0 vé)
        assertEquals(0L, folAdapter.lastApplied());
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM tickets"));
        assertEquals(0, followerDb.queryInt("SELECT count(*) FROM request_results"));

        // Giao 1 entry duy nhất
        folSource.commitAndDeliver(log.get(0));

        // Kiểm tra authoritative state chỉ tăng đúng 1 hàng vé và 1 kết quả của entry
        // đó
        assertEquals(1L, folAdapter.lastApplied());
        assertEquals(1, followerDb.queryInt("SELECT count(*) FROM tickets"));
        assertEquals(1, followerDb.queryInt("SELECT count(*) FROM request_results"));
    }

    @Test
    @DisplayName("12. Lặp 20 vòng với seed khác nhau cho điểm K/M và kích thước lô: mọi vòng đều khớp node tham chiếu")
    void test12_twentyIterationsWithVaryingSeedsConsistent() throws Exception {
        List<LogItem> log = createStandardLog();

        for (int seed = 1; seed <= 20; seed++) {
            Random rng = new Random(seed);
            int K = 1 + rng.nextInt(6); // Điểm dừng ban đầu: 1 đến 6
            int batchSize = 1 + rng.nextInt(3); // Kích thước lô catch-up: 1 đến 3

            // Dựng mới 2 schema test cho mỗi vòng
            try (TestDb ref = new TestDb(jdbcUrl, username, password, "ref_s" + seed);
                    TestDb fol = new TestDb(jdbcUrl, username, password, "fol_s" + seed)) {

                // Node tham chiếu apply toàn bộ log 1..12
                applyLogRange(ref, log, 0, log.size());

                // Follower áp dụng prefix K
                applyLogRange(fol, log, 0, K);

                // Catch-up theo lô kích thước batchSize
                RaftApplyAdapter folAdapter = createAdapter(fol);
                FakeCommitSource folSource = new FakeCommitSource(folAdapter, fol);

                int cursor = K;
                while (cursor < log.size()) {
                    int end = Math.min(cursor + batchSize, log.size());
                    for (int i = cursor; i < end; i++) {
                        folSource.commitAndDeliver(log.get(i));
                    }
                    cursor = end;
                }

                assertEquals(12L, folAdapter.lastApplied(), "Vòng seed " + seed + " phải đạt lastApplied = 12");
                assertSchemasConsistent(ref, fol);
            }
        }
    }

    // =========================================================================
    // Helper Methods: Apply Log Range & Schema Consistency Check (Pure JDBC)
    // =========================================================================

    private RaftApplyAdapter createAdapter(TestDb db) {
        ConnectionFactory factory = db.connectionFactory();
        JdbcBookingRepository repo = new JdbcBookingRepository(factory);
        JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(factory, res -> {
        });
        return new RaftApplyAdapter(applier, repo);
    }

    private void applyLogRange(TestDb db, List<LogItem> log, int startInclusive, int endExclusive) throws SQLException {
        RaftApplyAdapter adapter = createAdapter(db);
        FakeCommitSource source = new FakeCommitSource(adapter, db);

        for (int i = startInclusive; i < endExclusive; i++) {
            source.commitAndDeliver(log.get(i));
        }
    }

    /**
     * So sánh tính nhất quán tuyệt đối giữa 2 schema bằng JDBC thuần.
     * Kiểm tra chi tiết từng bảng: raft_apply_progress, seats, tickets,
     * request_results.
     */
    private void assertSchemasConsistent(TestDb refDb, TestDb folDb) throws SQLException {
        // 1. So sánh last_applied
        long refLA = refDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
        long folLA = folDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
        assertEquals(refLA, folLA, "last_applied giữa reference và follower phải bằng nhau");

        // 2. So sánh danh sách seats
        List<String> refSeats = queryList(refDb,
                "SELECT trip_id || '|' || seat_id || '|' || status || '|' || COALESCE(current_ticket_id, 'NULL') " +
                        "FROM seats ORDER BY trip_id, seat_id");
        List<String> folSeats = queryList(folDb,
                "SELECT trip_id || '|' || seat_id || '|' || status || '|' || COALESCE(current_ticket_id, 'NULL') " +
                        "FROM seats ORDER BY trip_id, seat_id");
        assertEquals(refSeats, folSeats, "Bảng seats giữa reference và follower không khớp nhau!");

        // 3. So sánh danh sách tickets
        List<String> refTickets = queryList(refDb,
                "SELECT ticket_id || '|' || trip_id || '|' || seat_id || '|' || passenger_name || '|' || status || '|' || "
                        +
                        "replace(to_char(issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z|' || "
                        +
                        "COALESCE(replace(to_char(cancelled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z', 'NULL') "
                        +
                        "FROM tickets ORDER BY ticket_id");
        List<String> folTickets = queryList(folDb,
                "SELECT ticket_id || '|' || trip_id || '|' || seat_id || '|' || passenger_name || '|' || status || '|' || "
                        +
                        "replace(to_char(issued_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z|' || "
                        +
                        "COALESCE(replace(to_char(cancelled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS'), ' ', 'T') || 'Z', 'NULL') "
                        +
                        "FROM tickets ORDER BY ticket_id");
        assertEquals(refTickets, folTickets, "Bảng tickets giữa reference và follower không khớp nhau!");

        // 4. So sánh danh sách request_results
        List<String> refResults = queryList(refDb,
                "SELECT request_id::text || '|' || command_type || '|' || intent_fingerprint || '|' || state || '|' || "
                        +
                        "COALESCE(reason_code, 'NULL') || '|' || COALESCE(ticket_id, 'NULL') || '|' || applied_log_index "
                        +
                        "FROM request_results ORDER BY request_id");
        List<String> folResults = queryList(folDb,
                "SELECT request_id::text || '|' || command_type || '|' || intent_fingerprint || '|' || state || '|' || "
                        +
                        "COALESCE(reason_code, 'NULL') || '|' || COALESCE(ticket_id, 'NULL') || '|' || applied_log_index "
                        +
                        "FROM request_results ORDER BY request_id");
        assertEquals(refResults, folResults, "Bảng request_results giữa reference và follower không khớp nhau!");
    }

    private List<String> queryList(TestDb db, String sql) throws SQLException {
        List<String> list = new ArrayList<>();
        try (Connection conn = db.connectionFactory().getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        }
        return list;
    }

    // =========================================================================
    // TestDb: Quản lý vòng đời Schema cô lập trong PostgreSQL
    // =========================================================================

    static final class TestDb implements AutoCloseable {
        private final String baseUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final ConnectionFactory connectionFactory;

        TestDb(String jdbcUrl, String username, String password, String prefix) throws Exception {
            this.baseUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = "railway_test_" + prefix + "_"
                    + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

            createSchema();
            try {
                URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
                String rawPath = uri.getPath();
                String dbName = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
                int port = uri.getPort() > 0 ? uri.getPort() : 5432;

                DbConfig config = new DbConfig(uri.getHost(), port, dbName + "?currentSchema=" + schema, username,
                        password);
                this.connectionFactory = new ConnectionFactory(config);

                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
            } catch (Exception ex) {
                dropSchema();
                throw ex;
            }
        }

        ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        Connection openIndependentConnection() throws SQLException {
            return connectionFactory.getConnection();
        }

        void executeSql(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }

        void insertCommittedLog(long logIndex, long term, String type, String payload) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, ?, ?, ?::jsonb) "
                                    +
                                    "ON CONFLICT (log_index) DO NOTHING")) {
                ps.setLong(1, logIndex);
                ps.setLong(2, term);
                ps.setString(3, type);
                ps.setString(4, payload);
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
                throw new IllegalStateException("Empty result for: " + sql);
            }
        }

        long queryLong(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                throw new IllegalStateException("Empty result for: " + sql);
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
                st.execute("CREATE SCHEMA \"" + schema + "\"");
            }
        }

        private void dropSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                    Statement st = conn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }

        private void runMigration(String filename) throws Exception {
            String sql;
            try (InputStream in = getClass().getClassLoader().getResourceAsStream("migration/" + filename)) {
                if (in == null) {
                    try (InputStream fallback = getClass().getClassLoader().getResourceAsStream(filename)) {
                        if (fallback == null) {
                            java.nio.file.Path rootPath = java.nio.file.Paths.get("..", "database", "migration",
                                    filename);
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
