package vn.nhom6.railway.persistence;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.ConnectionFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPEC-05 §3, §3.1, §5 & SPEC-04 §5, §6, §9:
 * Atomic apply, concurrency lock, and checkpoint recovery edge cases.
 */
class AtomicApplyTest {
    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_CANCEL = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    private static final Instant TIME_1 = Instant.parse("2026-10-29T10:00:00Z");
    private static final Instant TIME_2 = Instant.parse("2026-10-29T10:01:00Z");

    private String jdbcUrl;
    private String username;
    private String password;
    private String schema;
    private ConnectionFactory connectionFactory;
    private final Map<Long, BookingCommand> committedEntries = new HashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        username = System.getenv("RAILWAY_TEST_DB_USER");
        password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
        committedEntries.clear();
        try (Connection connection = rawConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA \"" + schema + "\"");
        }
        try {
            runMigration(readMigration("V1__schema.sql"));
            runMigration(readMigration("V2__seed.sql"));
            connectionFactory = new ConnectionFactory(dbConfig());
        } catch (Exception exception) {
            dropSchema();
            throw exception;
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try {
            dropSchema();
        } finally {
            schema = null;
        }
    }

    /**
     * Mục 7b: Crash sau COMMIT SQL nhưng trước khi trả kết quả cho caller.
     * last_applied và request_results đã được lưu bền vững; khi replay lại cùng index qua FakeCommitSource,
     * hệ thống nhận diện redelivery (index <= lastApplied), trả đúng kết quả gốc đã lưu mà không ghi đúp dữ liệu.
     */
    @Test
    void crashAfterCommitBeforeClientResponseReplayReturnsPersistedResultWithoutReapplying() {
        JdbcCommittedCommandApplier firstApplier = applier();
        FakeCommitSource firstSource = source(firstApplier);
        BookingCommand.BookTicket bookCommand = book(REQUEST_1, "G01", "ticket-1", TIME_1);

        // Lần apply đầu tiên ghi thành công vào DB
        var initialOutcome = firstSource.commit(1, bookCommand).orElseThrow();
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, countWhere("tickets", "ticket_id = 'ticket-1' AND status = 'ACTIVE'"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));

        // Giả lập caller/node crash trước khi xử lý phản hồi, khởi tạo lại applier mới trên checkpoint DB hiện tại
        JdbcCommittedCommandApplier restartedApplier = applier();
        assertEquals(1L, restartedApplier.initialCommitIndex());
        assertEquals(1L, restartedApplier.lastApplied());
        FakeCommitSource restartedSource = source(restartedApplier);

        // Caller redeliver lại cùng logIndex = 1
        var replayOutcome = restartedSource.commit(1, bookCommand).orElseThrow();
        BookingStateMachine.ApplyOutcome.BusinessResult replayResult = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, replayOutcome);

        // Kết quả trả về phải khớp với kết quả gốc; isReplay là false vì đây là redelivery đúng logIndex gốc
        assertEquals(initialOutcome, replayOutcome);
        assertFalse(replayResult.isReplay());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-1", 1), replayResult.applyResult());

        // Không phát sinh thêm bản ghi vé, kết quả, hay sự kiện nghiệp vụ
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(1L, restartedApplier.lastApplied());

        // Entry tiếp theo (index 2) tiếp tục apply bình thường
        var nextOutcome = restartedSource.commit(2, book(REQUEST_2, "G02", "ticket-2", TIME_2)).orElseThrow();
        assertEquals(ApplyResult.bookConfirmed(REQUEST_2, "ticket-2", 2),
                assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, nextOutcome).applyResult());
        assertEquals(2L, restartedApplier.lastApplied());
        assertEquals(2, count("tickets"));
    }

    /**
     * Mục 8: Hai luồng đồng thời tranh chấp apply cùng logIndex.
     * Nhờ khóa dòng SELECT FOR UPDATE trên raft_apply_progress và điều kiện logIndex == last_applied + 1,
     * chỉ đúng một luồng thành công; luồng còn lại fail-closed, không bao giờ tạo 2 vé cho cùng ghế hay apply đúp.
     */
    @Test
    void concurrentApplyOnSameIndexOnlyAllowsOneThreadToSucceed() throws Exception {
        BookingCommand.BookTicket bookCmd1 = book(REQUEST_1, "G01", "ticket-1", TIME_1);
        BookingCommand.BookTicket bookCmd2 = book(REQUEST_2, "G01", "ticket-2", TIME_1);

        // Ghi trước raft_log cho index 1 (vì 2 luồng đều thử apply index 1)
        insertLog(1, bookCmd1);

        JdbcBookingRepository repo1 = repository();
        JdbcBookingRepository repo2 = repository();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readyLatch = new CountDownLatch(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

        Future<?> task1 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                repo1.applyAtomically(1, bookCmd1);
                successCount.incrementAndGet();
            } catch (Throwable t) {
                failureCount.incrementAndGet();
                exceptions.add(t);
            }
        });

        Future<?> task2 = executor.submit(() -> {
            readyLatch.countDown();
            try {
                startLatch.await();
                repo2.applyAtomically(1, bookCmd2);
                successCount.incrementAndGet();
            } catch (Throwable t) {
                failureCount.incrementAndGet();
                exceptions.add(t);
            }
        });

        assertTrue(readyLatch.await(5, TimeUnit.SECONDS));
        startLatch.countDown();

        task1.get(10, TimeUnit.SECONDS);
        task2.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Đúng 1 luồng thành công và 1 luồng thất bại
        assertEquals(1, successCount.get(), "Chỉ đúng 1 luồng được phép apply thành công");
        assertEquals(1, failureCount.get(), "Luồng còn lại phải bị từ chối");

        // Dữ liệu DB phải nhất quán: đúng 1 vé được tạo, last_applied = 1
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, count("tickets"));
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(1, countWhere("seats", "seat_id = 'G01' AND status = 'BOOKED'"));
        assertEquals(1, count("business_events"));
        assertEquals(1, count("request_results"));
    }

    /**
     * Mục 10: Fail closed khi dữ liệu mâu thuẫn (vé ACTIVE nhưng trạng thái ghế không khớp khi hủy).
     * Khi phát hiện trạng thái vé và ghế không nhất quán, hệ thống ném integrityFailure,
     * rollback toàn bộ giao dịch, last_applied không tăng, không phát CONFIRMED.
     * Khi dữ liệu được khắc phục, retry entry có thể apply thành công.
     */
    @Test
    void inconsistentSeatStateFailsClosedAndRollsBackWithoutAdvancingLastApplied() throws Exception {
        JdbcCommittedCommandApplier applier = applier();
        FakeCommitSource source = source(applier);

        // 1. Đặt vé G01 thành công
        source.commit(1, book(REQUEST_1, "G01", "ticket-1", TIME_1));
        assertEquals(1L, applier.lastApplied());
        assertEquals("ACTIVE", queryString("SELECT status FROM tickets WHERE ticket_id = 'ticket-1'"));

        // 2. Cố tình can thiệp làm hỏng dữ liệu: ghế G01 thành AVAILABLE trong khi vé vẫn ACTIVE
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE seats SET status = 'AVAILABLE', current_ticket_id = NULL WHERE seat_id = 'G01'");
        }

        // 3. Thực hiện hủy vé ticket-1 ở index 2
        BookingCommand.CancelTicket cancelCmd = new BookingCommand.CancelTicket(REQUEST_CANCEL, "ticket-1", TIME_2);
        BookingRepository.StorageException integrityException = assertThrows(
                BookingRepository.StorageException.class, () -> source.commit(2, cancelCmd));
        assertTrue(integrityException.getMessage().contains("not linked to its BOOKED seat"));

        // 4. Kiểm tra invariant: Fail closed & rollback hoàn toàn
        assertEquals(1L, applier.lastApplied(), "lastApplied không được tăng khi apply thất bại");
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals("ACTIVE", queryString("SELECT status FROM tickets WHERE ticket_id = 'ticket-1'"),
                "Vé vẫn giữ nguyên trạng thái ACTIVE do rollback");
        assertEquals(0, countWhere("request_results", "request_id = '" + REQUEST_CANCEL + "'"),
                "Không được lưu kết quả cho request bị lỗi toàn vẹn");
        assertEquals(1, count("business_events"), "Không được phát sinh business_event cho lệnh bị rollback");

        // 5. Khắc phục trạng thái ghế về đúng BOOKED cho ticket-1
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE seats SET status = 'BOOKED', current_ticket_id = 'ticket-1' WHERE seat_id = 'G01'");
        }

        // 6. Retry apply entry index 2: thành công!
        var retryOutcome = source.commit(2, cancelCmd).orElseThrow();
        BookingStateMachine.ApplyOutcome.BusinessResult cancelResult = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, retryOutcome);
        assertEquals(ApplyResult.cancelConfirmed(REQUEST_CANCEL, "ticket-1", 2), cancelResult.applyResult());

        assertEquals(2L, applier.lastApplied());
        assertEquals(2L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals("CANCELLED", queryString("SELECT status FROM tickets WHERE ticket_id = 'ticket-1'"));
        assertEquals("AVAILABLE", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
        assertEquals(2, count("request_results"));
        assertEquals(2, count("business_events"));
    }

    private JdbcCommittedCommandApplier applier() {
        return new JdbcCommittedCommandApplier(repository(), result -> { });
    }

    private JdbcBookingRepository repository() {
        return new JdbcBookingRepository(() -> {
            Connection connection = connectionFactory.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET search_path TO \"" + schema + "\"");
            } catch (SQLException exception) {
                try {
                    connection.close();
                } catch (SQLException closeException) {
                    exception.addSuppressed(closeException);
                }
                throw new IllegalStateException("Could not set test schema search_path", exception);
            }
            return connection;
        });
    }

    private FakeCommitSource source(JdbcCommittedCommandApplier applier) {
        return new FakeCommitSource(applier.commitListener());
    }

    private final class FakeCommitSource {
        private final CommittedCommandApplier.CommitListener listener;

        private FakeCommitSource(CommittedCommandApplier.CommitListener listener) {
            this.listener = listener;
        }

        private Optional<BookingStateMachine.ApplyOutcome> commit(
                long index, BookingCommand command) {
            BookingCommand previous = committedEntries.putIfAbsent(index, command);
            if (previous == null) {
                insertLog(index, command);
            } else {
                assertEquals(previous, command, "committed log entry cannot change on redelivery");
            }
            return listener.onCommitted(index, command);
        }
    }

    private static BookingCommand.BookTicket book(
            String requestId, String seatId, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, "Passenger " + ticketId, ticketId, issuedAt);
    }

    private void insertLog(long logIndex, BookingCommand command) {
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (?, 1, ?, ?::jsonb) "
                                + "ON CONFLICT (log_index) DO NOTHING")) {
            statement.setLong(1, logIndex);
            statement.setString(2, command.type().name());
            statement.setString(3, commandJson(command));
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not seed committed raft log row", exception);
        }
    }

    private static String commandJson(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return "{\"requestId\":\"" + book.requestId() + "\",\"tripId\":\""
                    + book.tripId() + "\",\"seatId\":\"" + book.seatId()
                    + "\",\"passengerName\":\"" + escape(book.passengerName())
                    + "\",\"ticketId\":\"" + book.ticketId() + "\",\"issuedAt\":\""
                    + book.issuedAt() + "\"}";
        }
        if (command instanceof BookingCommand.CancelTicket cancel) {
            return "{\"requestId\":\"" + cancel.requestId() + "\",\"ticketId\":\""
                    + cancel.ticketId() + "\",\"cancelledAt\":\""
                    + cancel.cancelledAt() + "\"}";
        }
        return "{}";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private int count(String table) {
        return queryInt("SELECT COUNT(*) FROM " + table);
    }

    private int countWhere(String table, String condition) {
        return queryInt("SELECT COUNT(*) FROM " + table + " WHERE " + condition);
    }

    private int queryInt(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test query failed: " + sql, exception);
        }
    }

    private long queryLong(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getLong(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test query failed: " + sql, exception);
        }
    }

    private String queryString(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getString(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test query failed: " + sql, exception);
        }
    }

    private Connection schemaConnection() throws SQLException {
        Connection connection = rawConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO \"" + schema + "\"");
            return connection;
        } catch (SQLException exception) {
            connection.close();
            throw exception;
        }
    }

    private Connection rawConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private void runMigration(String migration) throws SQLException {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement()) {
            for (String sql : migration.split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
    }

    private static String readMigration(String name) throws IOException {
        Path local = Path.of("database", "migration", name);
        Path parent = Path.of("..", "database", "migration", name);
        if (Files.isRegularFile(local)) {
            return Files.readString(local, StandardCharsets.UTF_8);
        }
        if (Files.isRegularFile(parent)) {
            return Files.readString(parent, StandardCharsets.UTF_8);
        }
        throw new IOException("Migration not found: " + local.toAbsolutePath());
    }

    private DbConfig dbConfig() {
        URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
        String dbName = uri.getPath().substring(1);
        int port = uri.getPort() < 0 ? 5432 : uri.getPort();
        return new DbConfig(uri.getHost(), port, dbName, username, password);
    }

    private void dropSchema() throws SQLException {
        if (schema == null || !hasText(jdbcUrl) || !hasText(username) || !hasText(password)) {
            return;
        }
        try (Connection connection = rawConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
