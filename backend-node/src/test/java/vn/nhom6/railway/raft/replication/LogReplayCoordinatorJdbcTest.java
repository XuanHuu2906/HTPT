package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D22 PostgreSQL check of {@link LogReplayCoordinator} over the real
 * {@link JdbcRaftStorage}, {@link JdbcBookingRepository} and
 * {@link JdbcCommittedCommandApplier}.
 *
 * <p>Gated exactly like {@code RaftStateMachineApplierJdbcTest}: it runs only when
 * {@code RAILWAY_TEST_JDBC_URL}, {@code RAILWAY_TEST_DB_USER} and
 * {@code RAILWAY_TEST_DB_PASSWORD} are set. Every test creates and drops its own temporary
 * schema; it never touches existing data. Without those variables JUnit reports the class as
 * SKIPPED, which is not a PASS.
 *
 * <p><strong>Honesty boundary.</strong> A "restart" here means building a new
 * {@link RaftState} from {@link JdbcRaftStorage#load()} in the same JVM, not killing a real
 * process. PostgreSQL is real and durable: the booking rows, {@code raft_apply_progress} and
 * {@code raft_log} survive the restart. No socket, thread, Docker container or five-node cluster is
 * involved. The production JSON decoder {@link RaftApplyAdapter#decodeCommand(RaftLogEntry)} is
 * used, not a test codec.
 */
class LogReplayCoordinatorJdbcTest {

    private static final String TIME = "2026-11-22T09:00:00Z";

    private String jdbcUrl;
    private String username;
    private String password;
    private String schema;
    private ConnectionFactory connectionFactory;
    private JdbcRaftStorage storage;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        username = System.getenv("RAILWAY_TEST_DB_USER");
        password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER and RAILWAY_TEST_DB_PASSWORD are required");

        schema = "railway_d22_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = rawConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA \"" + schema + "\"");
        }
        try {
            runMigration(readMigration("V1__schema.sql"));
            runMigration(readMigration("V2__seed.sql"));
            connectionFactory = new ConnectionFactory(dbConfig());
            storage = new JdbcRaftStorage(connectionFactory);
        } catch (Exception exception) {
            dropSchema();
            throw exception;
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try {
            if (schema != null && hasText(jdbcUrl)) {
                dropFailureTrigger();
            }
            dropSchema();
        } finally {
            schema = null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // 1. Clean restart: only the durable prefix is known; the tail stays untouched
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: restart giữ nguyên tail chưa commit và không replay gì khi checkpoint khớp")
    void cleanRestartLeavesTheUncommittedTailUntouched() throws Exception {
        seedLog(book(1, "G01", "ticket-1"),
                new BookingCommand.NoOp(),
                book(2, "G02", "ticket-2"),
                book(3, "G03", "ticket-3"));

        // The previous process applied 1..2 durably, then stopped before learning more.
        Node crashed = node(RaftState.restore(1L, null, loadLog(), 0L));
        crashed.state.advanceCommitIndex(2L);
        assertEquals(2, crashed.applier.applyCommitted(), "precondition: apply 1..2");
        assertEquals(2L, dbLastApplied());
        assertEquals(1, count("tickets"), "only BOOK 1 created a ticket; index 2 is a NO_OP");

        // Restart: Raft state is rebuilt from PostgreSQL, not from the in-memory objects.
        RaftState restarted = restoreFromStorage();
        Node node = node(restarted);
        LogReplayCoordinator coordinator = new LogReplayCoordinator(
                restarted, node.applier, repository()::lastApplied);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report.outcome(), report.reason());
        assertFalse(coordinator.failed());
        assertEquals(2L, report.commitFloor());
        assertEquals(4L, report.lastLogIndex());
        assertEquals(2L, report.uncommittedSuffixLength());
        assertEquals(2L, restarted.commitIndex(), "commitIndex is lastApplied, not lastLogIndex");
        assertEquals(2L, restarted.lastApplied());
        assertEquals(2L, dbLastApplied());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(0, countWhere("tickets", "seat_id = 'G03'"),
                "the uncommitted tail must never reach the state machine");
        assertEquals(4, storage.load().log().size(), "the tail stays in the durable log");
        assertTrue(node.results.isEmpty(), "a clean restart applies and reports nothing");
    }

    // -------------------------------------------------------------------------------------------
    // 2. Crash between the durable commit and markApplied: redelivery only, no second event
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: crash sau commit DB trước markApplied chỉ redeliver, không sinh event thứ hai")
    void crashAfterDatabaseCommitBeforeMarkAppliedRedeliversOnly() throws Exception {
        seedLog(book(1, "G01", "ticket-1"),
                book(2, "G02", "ticket-2"),
                book(3, "G03", "ticket-3"),
                new BookingCommand.NoOp());

        Node crashed = node(RaftState.restore(1L, null, loadLog(), 0L));
        crashed.state.advanceCommitIndex(2L);
        assertEquals(2, crashed.applier.applyCommitted());
        assertEquals(2, crashed.results.size(), "precondition: BOOK 1 and BOOK 2 reported results");
        assertEquals(2, count("business_events"));

        // The durable apply of index 2 finished, but Raft only recorded 1 before the crash.
        RaftStorage.PersistentState persisted = storage.load();
        RaftState restarted = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), 1L);
        Node node = node(restarted);
        LogReplayCoordinator coordinator = new LogReplayCoordinator(
                restarted, node.applier, repository()::lastApplied);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
        assertEquals(2L, report.commitFloor(), "the durable checkpoint is the commit floor");
        assertEquals(2L, report.replayedFrom());
        assertEquals(2L, report.replayedTo());
        assertEquals(1, report.redelivered());
        assertEquals(2L, restarted.lastApplied());
        assertEquals(2, count("tickets"), "redelivery must not create another ticket");
        assertEquals(2, count("request_results"));
        assertEquals(2, count("business_events"), "redelivery must not add a business event");
        assertTrue(node.results.isEmpty(),
                "JdbcCommittedCommandApplier returns the stored outcome without reporting it again");

        // A new committed entry after the restart is reported exactly once.
        restarted.advanceCommitIndex(3L);
        LogReplayCoordinator.ReplayReport third = coordinator.replayOnStartup();
        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, third.outcome(), third.reason());
        assertEquals(0, third.redelivered());
        assertEquals(1, node.results.size(), "only the new BOOK 3 result is published");
        assertEquals(3L, dbLastApplied());
        assertEquals(3, count("business_events"));
    }

    // -------------------------------------------------------------------------------------------
    // 3. Whole prefix re-delivered after a restart from a stale Raft checkpoint
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: replay toàn prefix sau restart không bán trùng, rồi restart lần hai là no-op")
    void fullRedeliveryAfterRestartDoesNotSellTwice() throws Exception {
        seedLog(book(1, "G01", "ticket-1"),
                book(2, "G02", "ticket-2"),
                book(3, "G03", "ticket-3"),
                new BookingCommand.NoOp());

        Node crashed = node(RaftState.restore(1L, null, loadLog(), 0L));
        crashed.state.advanceCommitIndex(4L);
        assertEquals(4, crashed.applier.applyCommitted());
        assertEquals(3, count("tickets"));
        assertEquals(3, count("business_events"));

        // Raft lost its in-memory checkpoint; the Leader later re-teaches commitIndex = 4.
        RaftState restarted = RaftState.restore(1L, null, loadLog(), 0L);
        restarted.advanceCommitIndex(4L);
        Node node = node(restarted);
        LogReplayCoordinator coordinator = new LogReplayCoordinator(
                restarted, node.applier, repository()::lastApplied);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
        assertEquals(4L, report.commitFloor());
        assertEquals(4, report.redelivered(), "the whole durable prefix is re-delivered");
        assertEquals(4L, restarted.lastApplied());
        assertEquals(4L, dbLastApplied());
        assertEquals(3, count("tickets"), "replay must not create duplicates");
        assertEquals(3, count("request_results"));
        assertEquals(3, count("business_events"));
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE' AND seat_id = 'G01'"));
        assertEquals(0, queryInt(
                "SELECT COUNT(*) FROM (SELECT trip_id, seat_id FROM tickets "
                        + "WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id "
                        + "HAVING COUNT(*) > 1) duplicates"));
        assertTrue(node.results.isEmpty(), "a full redelivery reports no new business result");

        // Second restart: both checkpoints agree, nothing is applied or re-delivered.
        RaftState second = restoreFromStorage();
        Node secondNode = node(second);
        LogReplayCoordinator secondCoordinator = new LogReplayCoordinator(
                second, secondNode.applier, repository()::lastApplied);
        LogReplayCoordinator.ReplayReport secondReport = secondCoordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY,
                secondReport.outcome(), secondReport.reason());
        assertEquals(0, secondReport.redelivered());
        assertEquals(4L, secondReport.commitFloor());
        assertTrue(secondNode.results.isEmpty());
    }

    // -------------------------------------------------------------------------------------------
    // 4. A real SQL failure at the tail: retryable, then applied exactly once
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: lỗi SQL khi replay tail là RETRYABLE, retry apply đúng một lần")
    void sqlFailureDuringReplayIsRetryable() throws Exception {
        seedLog(book(1, "G01", "ticket-1"),
                book(2, "G02", "ticket-2"),
                book(3, "G03", "ticket-3"));

        Node crashed = node(RaftState.restore(1L, null, loadLog(), 0L));
        crashed.state.advanceCommitIndex(2L);
        assertEquals(2, crashed.applier.applyCommitted());
        assertEquals(2L, dbLastApplied());

        RaftStorage.PersistentState persisted = storage.load();
        RaftState restarted = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        restarted.advanceCommitIndex(3L);
        Node node = node(restarted);
        LogReplayCoordinator coordinator = new LogReplayCoordinator(
                restarted, node.applier, repository()::lastApplied);

        createFailureTrigger();
        LogReplayCoordinator.ReplayReport failed = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.RETRYABLE_FAILURE,
                failed.outcome(), failed.reason());
        assertFalse(coordinator.failed(), "a StorageException must stay retryable");
        assertEquals(2L, restarted.lastApplied(), "index 3 must not be marked applied");
        assertEquals(2L, dbLastApplied(), "the apply transaction rolled back");
        assertEquals(2, count("tickets"));
        assertEquals(2, count("request_results"));
        assertEquals(2, count("business_events"));
        assertTrue(node.results.isEmpty());

        dropFailureTrigger();
        LogReplayCoordinator.ReplayReport retried = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, retried.outcome(), retried.reason());
        assertEquals(3L, restarted.lastApplied());
        assertEquals(3L, dbLastApplied());
        assertEquals(3, count("tickets"));
        assertEquals(3, count("request_results"));
        assertEquals(3, count("business_events"));
        assertEquals(1, node.results.size(), "the retry publishes the pending result once");
    }

    // -------------------------------------------------------------------------------------------
    // 5. Fail closed on an inconsistent durable checkpoint
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: checkpoint vượt lastLogIndex fail closed và restoreAligned từ chối")
    void durableCheckpointBeyondTheLogFailsClosed() throws Exception {
        seedLog(book(1, "G01", "ticket-1"), book(2, "G02", "ticket-2"));

        Node crashed = node(RaftState.restore(1L, null, loadLog(), 0L));
        crashed.state.advanceCommitIndex(2L);
        assertEquals(2, crashed.applier.applyCommitted());

        RaftStorage.PersistentState persisted = storage.load();
        RaftState restarted = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        Node node = node(restarted);
        // The impossible checkpoint is injected on purpose: JdbcRaftStorage.load() itself rejects
        // lastApplied > lastLogIndex, so the real durable row can never produce this value.
        LogReplayCoordinator coordinator = new LogReplayCoordinator(
                restarted, node.applier, () -> persisted.log().size() + 1L);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, report.outcome());
        assertTrue(coordinator.failed());
        assertTrue(report.reason().contains("beyond lastLogIndex"), report.reason());
        assertEquals(2L, restarted.lastApplied(), "a fail-closed replay applies nothing");
        assertEquals(2, count("tickets"));
        assertTrue(node.results.isEmpty());
        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED,
                coordinator.replayOnStartup().outcome(), "the coordinator stays latched");
        assertThrows(IllegalArgumentException.class,
                () -> LogReplayCoordinator.restoreAligned(persisted, persisted.log().size() + 1L));
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private record Node(RaftState state, RaftStateMachineApplier applier,
            JdbcCommittedCommandApplier committed, List<ApplyResult> results) {
    }

    private Node node(RaftState state) {
        List<ApplyResult> results = new ArrayList<>();
        JdbcCommittedCommandApplier committed =
                new JdbcCommittedCommandApplier(connectionFactory, results::add);
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                state, RaftApplyAdapter::decodeCommand, committed.commitListener());
        return new Node(state, applier, committed, results);
    }

    private BookingRepository repository() {
        return new JdbcBookingRepository(connectionFactory);
    }

    private RaftState restoreFromStorage() throws Exception {
        RaftStorage.PersistentState persisted = storage.load();
        return RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
    }

    private List<RaftLogEntry> loadLog() throws Exception {
        return storage.load().log();
    }

    private void seedLog(BookingCommand... commands) throws Exception {
        storage.saveTermAndVote(1L, null);
        List<RaftLogEntry> entries = new ArrayList<>();
        long index = 1L;
        for (BookingCommand command : commands) {
            if (command instanceof BookingCommand.NoOp) {
                entries.add(RaftLogEntry.noOp(index, 1L));
            } else {
                entries.add(new RaftLogEntry(index, 1L, command.type().name(), payload(command)));
            }
            index++;
        }
        storage.replaceSuffix(1L, entries);
    }

    private static BookingCommand book(long sequence, String seatId, String ticketId) {
        return new BookingCommand.BookTicket(requestId(sequence), "SE-001", seatId,
                "Passenger " + ticketId, ticketId, Instant.parse(TIME));
    }

    private static String payload(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return "{\"requestId\":\"" + book.requestId() + "\",\"tripId\":\"" + book.tripId()
                    + "\",\"seatId\":\"" + book.seatId() + "\",\"passengerName\":\""
                    + book.passengerName() + "\",\"ticketId\":\"" + book.ticketId()
                    + "\",\"issuedAt\":\"" + book.issuedAt() + "\"}";
        }
        return "{}";
    }

    private static String requestId(long sequence) {
        return String.format(Locale.ROOT, "%08x-0000-4000-8000-%012x", sequence, sequence);
    }

    private long dbLastApplied() {
        return queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
    }

    private void createFailureTrigger() {
        execute("""
                CREATE OR REPLACE FUNCTION railway_d22_fail_request_results() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'injected request_results failure (TV4-D22 test)';
                END
                $$
                """);
        execute("CREATE TRIGGER railway_d22_fail_request_results BEFORE INSERT ON request_results "
                + "FOR EACH ROW EXECUTE FUNCTION railway_d22_fail_request_results()");
    }

    private void dropFailureTrigger() {
        if (schema == null || !hasText(jdbcUrl)) {
            return;
        }
        execute("DROP TRIGGER IF EXISTS railway_d22_fail_request_results ON request_results");
        execute("DROP FUNCTION IF EXISTS railway_d22_fail_request_results()");
    }

    private void execute(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test fixture SQL failed", exception);
        }
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
            throw new IllegalStateException("Test query failed", exception);
        }
    }

    private long queryLong(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getLong(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test query failed", exception);
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
        return new DbConfig(uri.getHost(), port, dbName + "?currentSchema=" + schema,
                username, password);
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
