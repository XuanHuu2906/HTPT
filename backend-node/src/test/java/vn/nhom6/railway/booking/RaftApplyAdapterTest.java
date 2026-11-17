package vn.nhom6.railway.booking;

import org.junit.jupiter.api.Assumptions;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử cho {@link RaftApplyAdapter}.
 *
 * <p>
 * Nhóm in-memory (không cần DB) luôn chạy. Nhóm kiểm thử DB chỉ chạy khi có các
 * biến môi trường
 * {@code RAILWAY_TEST_JDBC_URL}, {@code RAILWAY_TEST_DB_USER},
 * {@code RAILWAY_TEST_DB_PASSWORD},
 * sử dụng schema cô lập {@code railway_test_*} và dọn dẹp sạch sẽ sau khi test.
 */
class RaftApplyAdapterTest {

    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_3 = "33333333-3333-3333-3333-333333333333";
    private static final Instant TIME_1 = Instant.parse("2026-11-15T08:00:00Z");
    private static final Instant TIME_2 = Instant.parse("2026-11-15T08:01:00Z");
    private static final Instant TIME_3 = Instant.parse("2026-11-15T08:02:00Z");

    // =========================================================================
    // NHÓM IN-MEMORY: Không cần DB
    // =========================================================================

    /**
     * 1. Entry đã commit, giao theo thứ tự: apply đúng, lastApplied tăng từng bước.
     */
    @Test
    void test01_committedEntriesAppliedInOrderAndAdvanceLastApplied() {
        BookingStateMachine stateMachine = new BookingStateMachine();
        List<Long> appliedCallbacks = new ArrayList<>();
        RaftApplyAdapter adapter = new RaftApplyAdapter(
                stateMachine,
                null,
                null,
                appliedCallbacks::add);

        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
        RaftLogEntry entry2 = bookEntry(2, 1, REQUEST_2, "G02", "Passenger 2", "tk-002", TIME_2);

        commitSource.commitAndDeliver(entry1);
        assertEquals(1L, adapter.lastApplied());
        assertEquals(List.of(1L), appliedCallbacks);

        commitSource.commitAndDeliver(entry2);
        assertEquals(2L, adapter.lastApplied());
        assertEquals(List.of(1L, 2L), appliedCallbacks);
        assertEquals(Optional.of("BOOKED"), stateMachine.seatStatus("G01"));
        assertEquals(Optional.of("BOOKED"), stateMachine.seatStatus("G02"));
    }

    /**
     * 2. Entry chưa commit (index > commitIndex): KHÔNG apply, applier không được
     * gọi, DB không đổi;
     * sau khi commitIndex tăng tới index đó thì mới apply.
     */
    @Test
    void test02_uncommittedEntriesRejectedUntilCommitIndexAdvances() {
        AtomicInteger callCount = new AtomicInteger();
        BookingStateMachine stateMachine = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(
                (idx, cmd) -> {
                    callCount.incrementAndGet();
                    return Optional.of(stateMachine.apply(idx, cmd));
                },
                null,
                null,
                null);

        FakeCommitSource commitSource = new FakeCommitSource(adapter);
        RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);

        // commitIndex đang là 0, entry index là 1 -> từ chối
        assertEquals(0L, adapter.commitIndex());
        assertThrows(IllegalStateException.class, () -> commitSource.deliverWithoutCommit(entry1));
        assertEquals(0, callCount.get(), "Applier không được gọi khi entry chưa commit");
        assertEquals(0L, adapter.lastApplied());
        assertEquals(Optional.of("AVAILABLE"), stateMachine.seatStatus("G01"));

        // Nâng commitIndex lên 1 -> apply thành công
        adapter.advanceCommitIndex(1);
        assertEquals(1L, adapter.commitIndex());
        commitSource.deliverWithoutCommit(entry1);

        assertEquals(1, callCount.get());
        assertEquals(1L, adapter.lastApplied());
        assertEquals(Optional.of("BOOKED"), stateMachine.seatStatus("G01"));
    }

    /**
     * 3. commitIndex không giảm khi nhận callback cũ muộn.
     */
    @Test
    void test03_commitIndexNeverDecreasesOnStaleCallback() {
        RaftApplyAdapter adapter = new RaftApplyAdapter((idx, cmd) -> Optional.empty(), null, null, null);

        adapter.advanceCommitIndex(5);
        assertEquals(5L, adapter.commitIndex());

        // Callback cũ muộn báo commitIndex = 3 hoặc 4 -> không giảm
        adapter.advanceCommitIndex(3);
        assertEquals(5L, adapter.commitIndex());
        adapter.advanceCommitIndex(4);
        assertEquals(5L, adapter.commitIndex());
        adapter.advanceCommitIndex(5);
        assertEquals(5L, adapter.commitIndex());

        // Chỉ tăng khi nhận giá trị lớn hơn
        adapter.advanceCommitIndex(6);
        assertEquals(6L, adapter.commitIndex());
    }

    /**
     * 7. Giải mã lỗi / commandType lạ: fail closed, không bỏ qua entry, lastApplied
     * không tăng.
     */
    @Test
    void test07_decodeFailureOrUnknownCommandTypeFailsClosed() {
        BookingStateMachine stateMachine = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(stateMachine, null);
        adapter.advanceCommitIndex(10);
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        // 7a: JSON hỏng
        RaftLogEntry badJson = new RaftLogEntry(1, 1, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{bad-json");
        assertThrows(RaftApplyAdapter.CommandDecodeException.class, () -> commitSource.deliverWithoutCommit(badJson));
        assertEquals(0L, adapter.lastApplied());

        // 7b: Thiếu trường bắt buộc
        RaftLogEntry missingFields = new RaftLogEntry(1, 1, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                "{\"requestId\":\"" + REQUEST_1 + "\"}");
        assertThrows(RaftApplyAdapter.CommandDecodeException.class,
                () -> commitSource.deliverWithoutCommit(missingFields));
        assertEquals(0L, adapter.lastApplied());

        // 7c: Timestamp sai định dạng ISO-8601
        String invalidTimePayload = "{"
                + "\"requestId\":\"" + REQUEST_1 + "\","
                + "\"tripId\":\"SE-001\","
                + "\"seatId\":\"G01\","
                + "\"passengerName\":\"Passenger 1\","
                + "\"ticketId\":\"tk-001\","
                + "\"issuedAt\":\"not-a-timestamp\""
                + "}";
        RaftLogEntry badTime = new RaftLogEntry(1, 1, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, invalidTimePayload);
        assertThrows(RaftApplyAdapter.CommandDecodeException.class, () -> commitSource.deliverWithoutCommit(badTime));
        assertEquals(0L, adapter.lastApplied());
    }

    /**
     * 10. Chờ theo index: future hoàn tất đúng khi lastApplied >= index, không sớm
     * hơn;
     * NO_OP làm tăng lastApplied và giải phóng chờ mà không tạo vé/kết quả nghiệp
     * vụ.
     */
    @Test
    void test10_awaitIndexCompletesWhenLastAppliedReachesTargetAndNoOpReleasesWaiters() throws Exception {
        BookingStateMachine stateMachine = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(stateMachine, null);
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        CompletableFuture<Void> waitIndex1 = adapter.awaitIndex(1);
        CompletableFuture<Void> waitIndex2 = adapter.awaitIndex(2);

        assertFalse(waitIndex1.isDone());
        assertFalse(waitIndex2.isDone());

        // Entry 1 là NO_OP
        commitSource.commitAndDeliver(noOpEntry(1, 1));
        assertEquals(1L, adapter.lastApplied());
        assertTrue(waitIndex1.isDone(), "Wait index 1 phải hoàn tất khi lastApplied đạt 1");
        assertFalse(waitIndex2.isDone(), "Wait index 2 chưa được hoàn tất");
        assertEquals(0, stateMachine.totalTicketCount(), "NO_OP không tạo vé");

        // Entry 2 là BOOK_TICKET
        commitSource.commitAndDeliver(bookEntry(2, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1));
        assertEquals(2L, adapter.lastApplied());
        assertTrue(waitIndex2.isDone(), "Wait index 2 phải hoàn tất khi lastApplied đạt 2");
        assertEquals(1, stateMachine.totalTicketCount());

        // Chờ index <= lastApplied đã qua -> hoàn tất ngay lập tức
        CompletableFuture<Void> alreadyPassed = adapter.awaitIndex(2);
        assertTrue(alreadyPassed.isDone());
    }

    /**
     * 12. Không bao giờ phát CONFIRMED khi entry chưa commit/apply.
     */
    @Test
    void test12_neverEmitsConfirmedWhenEntryUncommittedOrUnapplied() {
        BookingStateMachine stateMachine = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(stateMachine, null);

        CompletableFuture<ApplyResult> future = adapter.registerPending(REQUEST_1);
        assertEquals(RaftApplyAdapter.RequestState.PENDING, adapter.requestState(REQUEST_1));
        assertFalse(future.isDone(), "Future không được hoàn tất khi entry chưa được commit/apply");

        // Entry chưa commit
        RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
        assertThrows(IllegalStateException.class, () -> adapter.onCommitted(entry1));
        assertFalse(future.isDone());
        assertEquals(RaftApplyAdapter.RequestState.PENDING, adapter.requestState(REQUEST_1));
    }

    // =========================================================================
    // NHÓM CẦN DB: PostgreSQL thật (Chỉ chạy khi có RAILWAY_TEST_*)
    // =========================================================================

    /**
     * 4. PENDING vs APPLIED: đăng ký chờ → PENDING; apply xong → APPLIED và future
     * hoàn tất với
     * ApplyResult đúng; tại thời điểm future hoàn tất, DB (đọc bằng connection
     * khác) đã thấy dữ liệu đã COMMIT.
     */
    @Test
    void test04_pendingVsAppliedStatusAndFutureCompletesOnlyAfterDbCommit() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repository);
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            // 4a: Chưa biết -> NONE
            assertEquals(RaftApplyAdapter.RequestState.NONE, adapter.requestState(REQUEST_1));

            // 4b: Đăng ký chờ -> PENDING
            CompletableFuture<ApplyResult> future = adapter.registerPending(REQUEST_1);
            assertEquals(RaftApplyAdapter.RequestState.PENDING, adapter.requestState(REQUEST_1));
            assertFalse(future.isDone());

            // 4c: Chèn raft_log để thỏa verifyCommittedCommand của repository
            RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
            db.insertCommittedLog(1, RaftApplyAdapter.decodeCommand(entry1));

            // Đăng ký callback kiểm tra tại thời điểm future hoàn tất, connection khác
            // trong DB đã thấy dữ liệu COMMIT
            AtomicBoolean dbCommittedAtCallbackTime = new AtomicBoolean(false);
            future.thenAccept(result -> {
                try {
                    int activeTickets = db.queryInt(
                            "SELECT count(*) FROM tickets WHERE ticket_id = 'tk-001' AND status = 'ACTIVE'");
                    String seatStatus = db.queryString(
                            "SELECT status FROM seats WHERE seat_id = 'G01'");
                    if (activeTickets == 1 && "BOOKED".equals(seatStatus)) {
                        dbCommittedAtCallbackTime.set(true);
                    }
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });

            // Apply entry 1
            commitSource.commitAndDeliver(entry1);

            assertTrue(future.isDone());
            assertTrue(dbCommittedAtCallbackTime.get(),
                    "Tại thời điểm future hoàn tất, connection khác phải đọc được dữ liệu đã COMMIT");
            assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "tk-001", 1L), future.get());
            assertEquals(RaftApplyAdapter.RequestState.APPLIED, adapter.requestState(REQUEST_1));
            assertEquals(Optional.of(ApplyResult.bookConfirmed(REQUEST_1, "tk-001", 1L)),
                    adapter.findResult(REQUEST_1));
        }
    }

    /**
     * 5. Redelivery cùng index: không apply lại, future không hoàn tất hai lần, DB
     * không đổi.
     */
    @Test
    void test05_redeliverySameIndexDoesNotReapplyOrCompleteFutureTwice() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repository);
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
            db.insertCommittedLog(1, RaftApplyAdapter.decodeCommand(entry1));

            CompletableFuture<ApplyResult> future = adapter.registerPending(REQUEST_1);
            commitSource.commitAndDeliver(entry1);
            assertEquals(1L, adapter.lastApplied());
            assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "tk-001", 1L), future.get());

            // Redelivery lại cùng entry tại index 1
            commitSource.commitAndDeliver(entry1);
            assertEquals(1L, adapter.lastApplied());
            assertEquals(1, db.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'tk-001'"));
            assertEquals(1, db.queryInt("SELECT count(*) FROM business_events"));
            assertEquals(1, db.queryInt("SELECT count(*) FROM request_results"));
        }
    }

    /**
     * 6. Lỗ index: fail closed, không apply entry sau, DB không đổi.
     */
    @Test
    void test06_indexGapFailsClosedWithoutAdvancingLastApplied() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repository);
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            // Bỏ qua index 1, gửi thẳng index 2
            RaftLogEntry entry2 = bookEntry(2, 1, REQUEST_2, "G02", "Passenger 2", "tk-002", TIME_2);
            db.insertCommittedLog(2, RaftApplyAdapter.decodeCommand(entry2));

            adapter.advanceCommitIndex(2);
            assertThrows(IllegalStateException.class, () -> commitSource.deliverWithoutCommit(entry2));

            assertEquals(0L, adapter.lastApplied(), "lastApplied không được tăng khi có lỗ index");
            assertEquals(0, db.queryInt("SELECT count(*) FROM tickets"));
            assertEquals(0, db.queryInt("SELECT count(*) FROM business_events"));
        }
    }

    /**
     * 8. Lỗi DB giữa apply (chèn lỗi): lastApplied không tăng, future KHÔNG hoàn
     * tất với CONFIRMED;
     * gỡ lỗi rồi gọi lại cùng index thì thành công đúng một lần.
     */
    @Test
    void test08_databaseFailureDuringApplyRollsBackAndRetriesSuccessfully() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repository);
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            CompletableFuture<ApplyResult> future = adapter.registerPending(REQUEST_1);

            // Chèn trigger gây lỗi DB khi chèn vé mới (giả lập lỗi lưu trữ/disk trong lúc
            // apply)
            try (Connection conn = db.connectionFactory().getConnection();
                    Statement st = conn.createStatement()) {
                st.execute("CREATE OR REPLACE FUNCTION fail_trigger() RETURNS TRIGGER AS $$\n"
                        + "BEGIN\n"
                        + "    RAISE EXCEPTION 'Simulated database failure during apply';\n"
                        + "END;\n"
                        + "$$ LANGUAGE plpgsql;");
                st.execute("CREATE TRIGGER fail_apply_trigger BEFORE INSERT ON tickets "
                        + "FOR EACH ROW EXECUTE FUNCTION fail_trigger();");
            }

            RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
            db.insertCommittedLog(1, RaftApplyAdapter.decodeCommand(entry1));

            assertThrows(BookingRepository.StorageException.class, () -> commitSource.commitAndDeliver(entry1));

            // Kiểm tra: lastApplied không tăng, future chưa hoàn tất, DB rollback sạch sẽ
            assertEquals(0L, adapter.lastApplied());
            assertFalse(future.isDone(), "Future không được hoàn tất khi DB apply bị lỗi");
            assertEquals(0, db.queryInt("SELECT count(*) FROM tickets"));
            assertEquals(0, db.queryInt("SELECT count(*) FROM business_events"));

            // Gỡ trigger lỗi
            try (Connection conn = db.connectionFactory().getConnection();
                    Statement st = conn.createStatement()) {
                st.execute("DROP TRIGGER fail_apply_trigger ON tickets;");
            }

            // Gọi lại cùng index 1 -> thành công đúng 1 lần
            commitSource.commitAndDeliver(entry1);

            assertEquals(1L, adapter.lastApplied());
            assertTrue(future.isDone());
            assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "tk-001", 1L), future.get());
            assertEquals(1, db.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'tk-001'"));
        }
    }

    /**
     * 9. Step-down: hủy future đang chờ bằng ngoại lệ; sau đó entry đó được commit
     * và apply muộn
     * → tra cứu theo requestId trả APPLIED với kết quả gốc (không bị coi là thất
     * bại).
     */
    @Test
    void test09_stepDownCancelsPendingExceptionallyAndLateApplySucceeds() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repository);
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            CompletableFuture<ApplyResult> future = adapter.registerPending(REQUEST_1);
            assertEquals(RaftApplyAdapter.RequestState.PENDING, adapter.requestState(REQUEST_1));

            // Step-down xảy ra: Leader bị giáng chức / timeout
            BookingRequestHandler.NotLeaderException stepDownCause = new BookingRequestHandler.NotLeaderException(
                    "node-2");
            adapter.cancelPending(stepDownCause);

            assertTrue(future.isCompletedExceptionally());
            ExecutionException exception = assertThrows(ExecutionException.class, future::get);
            assertSame(stepDownCause, exception.getCause());
            assertEquals(0, adapter.pendingCount());

            // Sau đó, entry được commit bởi cụm và apply muộn trên node này
            RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
            db.insertCommittedLog(1, RaftApplyAdapter.decodeCommand(entry1));

            commitSource.commitAndDeliver(entry1);
            assertEquals(1L, adapter.lastApplied());

            // Tra cứu trạng thái: đã thành APPLIED và trả kết quả gốc (không bị coi là thất
            // bại)
            assertEquals(RaftApplyAdapter.RequestState.APPLIED, adapter.requestState(REQUEST_1));
            Optional<ApplyResult> persistedResult = adapter.findResult(REQUEST_1);
            assertTrue(persistedResult.isPresent());
            assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "tk-001", 1L), persistedResult.get());
        }
    }

    /**
     * 11. Restart: dựng lại adapter trên cùng DB, lastApplied lấy từ DB, PENDING
     * trong RAM không còn,
     * phát lại từ index 1 thì không apply lại prefix cũ.
     */
    @Test
    void test11_restartInitializesFromDatabaseAndIgnoresReplayedPrefix() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "Yêu cầu RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

        try (TestDatabase db = TestDatabase.open(jdbcUrl, username, password)) {
            // Khởi động lần 1: apply entry 1
            JdbcBookingRepository repo1 = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier1 = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter1 = new RaftApplyAdapter(applier1, repo1);
            FakeCommitSource commitSource1 = new FakeCommitSource(adapter1);

            RaftLogEntry entry1 = bookEntry(1, 1, REQUEST_1, "G01", "Passenger 1", "tk-001", TIME_1);
            db.insertCommittedLog(1, RaftApplyAdapter.decodeCommand(entry1));
            commitSource1.commitAndDeliver(entry1);
            assertEquals(1L, adapter1.lastApplied());

            // Đăng ký 1 pending chưa apply
            adapter1.registerPending(REQUEST_2);
            assertEquals(1, adapter1.pendingCount());

            // Giả lập Restart: Khởi tạo adapter2 trên cùng database
            JdbcBookingRepository repo2 = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier2 = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter2 = new RaftApplyAdapter(applier2, repo2);
            FakeCommitSource commitSource2 = new FakeCommitSource(adapter2);

            // lastApplied phải được nạp từ DB (= 1)
            assertEquals(1L, adapter2.lastApplied());
            assertEquals(1L, adapter2.commitIndex());
            // PENDING trong RAM không còn
            assertEquals(0, adapter2.pendingCount());
            assertEquals(RaftApplyAdapter.RequestState.NONE, adapter2.requestState(REQUEST_2));

            // Phát lại từ index 1: redelivery prefix cũ không apply lại
            commitSource2.commitAndDeliver(entry1);
            assertEquals(1L, adapter2.lastApplied());
            assertEquals(1, db.queryInt("SELECT count(*) FROM tickets"));

            // Apply tiếp entry 2 tại index 2 thành công
            RaftLogEntry entry2 = bookEntry(2, 1, REQUEST_2, "G02", "Passenger 2", "tk-002", TIME_2);
            db.insertCommittedLog(2, RaftApplyAdapter.decodeCommand(entry2));
            commitSource2.commitAndDeliver(entry2);

            assertEquals(2L, adapter2.lastApplied());
            assertEquals(2, db.queryInt("SELECT count(*) FROM tickets"));
        }
    }

    // =========================================================================
    // Helpers & Test Harness
    // =========================================================================

    /**
     * FakeCommitSource là lớp giả CHỈ DÙNG TRONG TEST (không phải Raft thật, không
     * mở socket).
     */
    private static final class FakeCommitSource {
        private final RaftApplyAdapter adapter;

        FakeCommitSource(RaftApplyAdapter adapter) {
            this.adapter = Objects.requireNonNull(adapter, "adapter");
        }

        void commitAndDeliver(RaftLogEntry entry) {
            adapter.advanceCommitIndex(entry.index());
            adapter.onCommitted(entry);
        }

        void deliverWithoutCommit(RaftLogEntry entry) {
            adapter.onCommitted(entry);
        }
    }

    private static RaftLogEntry bookEntry(
            long index, long term, String requestId, String seatId, String passengerName, String ticketId,
            Instant issuedAt) {
        String payload = "{"
                + "\"requestId\":\"" + requestId + "\","
                + "\"tripId\":\"SE-001\","
                + "\"seatId\":\"" + seatId + "\","
                + "\"passengerName\":\"" + passengerName + "\","
                + "\"ticketId\":\"" + ticketId + "\","
                + "\"issuedAt\":\"" + issuedAt.toString() + "\""
                + "}";
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static RaftLogEntry noOpEntry(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * TestDatabase cô lập schema PostgreSQL riêng cho từng bài test.
     */
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
