package vn.nhom6.railway.persistence;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

class JdbcCommittedCommandApplierTest {
    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_3 = "33333333-3333-3333-3333-333333333333";
    private static final String REQUEST_4 = "44444444-4444-4444-4444-444444444444";
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

    @Test
    void bookAvailableSeatNotifiesOnlyAfterAllDatabaseWritesCommit() {
        AtomicReference<ApplyResult> callbackResult = new AtomicReference<>();
        AtomicBoolean callbackObservedCommittedState = new AtomicBoolean();
        JdbcCommittedCommandApplier applier = applier(result -> {
            callbackResult.set(result);
            callbackObservedCommittedState.set(
                    countWhere("tickets", "ticket_id = 'ticket-1' AND status = 'ACTIVE'") == 1
                    && countWhere("seats", "seat_id = 'G01' AND status = 'BOOKED' "
                            + "AND current_ticket_id = 'ticket-1'") == 1
                    && countWhere("request_results", "request_id = '" + REQUEST_1 + "'") == 1
                    && queryLong("SELECT last_applied FROM raft_apply_progress "
                            + "WHERE singleton = 1") == 1L);
        });

        Optional<BookingStateMachine.ApplyOutcome> outcome =
                source(applier).commit(1, book(REQUEST_1, "G01", "ticket-1", TIME_1));

        BookingStateMachine.ApplyOutcome.BusinessResult business = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome.orElseThrow());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-1", 1), business.applyResult());
        assertEquals(business.applyResult(), callbackResult.get());
        assertTrue(callbackObservedCommittedState.get());
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(1, countWhere("request_results", "request_id = '" + REQUEST_1 + "'"));
        assertEquals(1L, queryLong(
                "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
    }

    @Test
    void bookingAnOccupiedSeatIsRejectedWithoutCreatingAnotherActiveTicket() {
        JdbcCommittedCommandApplier applier = applier();
        FakeCommitSource source = source(applier);
        source.commit(1, book(REQUEST_1, "G01", "ticket-1", TIME_1));

        var result = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                source.commit(2, book(REQUEST_2, "G01", "ticket-2", TIME_2)).orElseThrow());

        assertEquals(ApplyResult.bookRejected(
                REQUEST_2, ApplyResult.SEAT_ALREADY_BOOKED, 2), result.applyResult());
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(1, count("tickets"));
    }

    @Test
    void replayAndRequestIdConflictPreserveOriginalResultAndAllowLaterEntries() {
        JdbcCommittedCommandApplier applier = applier();
        FakeCommitSource source = source(applier);
        BookingCommand.BookTicket original = bookWithPassenger(
                REQUEST_1, "G01", "Same passenger", "ticket-original", TIME_1);
        var originalOutcome = source.commit(1, original).orElseThrow();

        var replay = source.commit(
                2, bookWithPassenger(
                        REQUEST_1, "G01", "Same passenger", "ticket-retry", TIME_2))
                .orElseThrow();
        var conflict = source.commit(
                3, bookWithPassenger(
                        REQUEST_1, "G02", "Same passenger", "ticket-conflict", TIME_2))
                .orElseThrow();
        var next = source.commit(
                4, book(REQUEST_2, "G03", "ticket-next", TIME_2)).orElseThrow();

        assertEquals(
                assertInstanceOf(
                        BookingStateMachine.ApplyOutcome.BusinessResult.class, originalOutcome)
                        .applyResult(),
                assertInstanceOf(
                        BookingStateMachine.ApplyOutcome.BusinessResult.class, replay)
                        .applyResult());
        assertTrue(assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, replay).isReplay());
        assertEquals(new BookingStateMachine.ApplyOutcome.RequestIdConflict(REQUEST_1, 3), conflict);
        assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-original", 1),
                assertInstanceOf(
                        BookingStateMachine.ApplyOutcome.BusinessResult.class, replay).applyResult());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_2, "ticket-next", 4),
                assertInstanceOf(
                        BookingStateMachine.ApplyOutcome.BusinessResult.class, next).applyResult());
        assertEquals(2, count("tickets"));
        assertEquals(2, count("request_results"));
        assertEquals("ticket-original", queryString(
                "SELECT ticket_id FROM tickets WHERE seat_id = 'G01' AND status = 'ACTIVE'"));
        assertEquals(4L, applier.lastApplied());
    }

    @Test
    void noOpAdvancesOnlyThePersistentCheckpoint() {
        JdbcCommittedCommandApplier applier = applier();
        Optional<BookingStateMachine.ApplyOutcome> outcome =
                source(applier).commit(1, new BookingCommand.NoOp());

        assertEquals(Optional.of(new BookingStateMachine.ApplyOutcome.NoOpApplied(1)), outcome);
        assertEquals(1L, queryLong(
                "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, count("tickets"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));
        assertEquals(0, countWhere("seats", "status = 'BOOKED'"));
    }

    @Test
    void restartUsesDurableCheckpointAndRedeliveryDoesNotApplyAgain() {
        FakeCommitSource firstSource = source(applier());
        var original = firstSource.commit(
                1, book(REQUEST_1, "G01", "ticket-1", TIME_1)).orElseThrow();

        JdbcCommittedCommandApplier restarted = applier();
        assertEquals(1L, restarted.initialCommitIndex());
        FakeCommitSource restartedSource = source(restarted);
        assertEquals(original, restartedSource.commit(
                1, book(REQUEST_1, "G01", "ticket-1", TIME_1)).orElseThrow());
        restartedSource.commit(2, book(REQUEST_2, "G02", "ticket-2", TIME_2));

        assertEquals(2L, restarted.lastApplied());
        assertEquals(2, count("tickets"));
        assertEquals(2, count("request_results"));
    }

    @Test
    void skippedIndexFailsClosedWithoutChangingDatabase() {
        JdbcCommittedCommandApplier applier = applier();

        assertThrows(IllegalStateException.class, () -> source(applier).commit(
                2, book(REQUEST_1, "G01", "ticket-1", TIME_1)));

        assertEquals(0L, applier.lastApplied());
        assertEquals(0, count("tickets"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));
        assertEquals(0, countWhere("seats", "status = 'BOOKED'"));
    }

    @Test
    void databaseFailureRollsBackAndRetryOfSameEntrySucceedsOnce() {
        AtomicReference<SqlFault> fault = new AtomicReference<>(
                new SqlFault("INSERT INTO request_results", false));
        JdbcBookingRepository repository = repository(fault);
        AtomicBoolean callbackCalled = new AtomicBoolean();
        JdbcCommittedCommandApplier applier =
                new JdbcCommittedCommandApplier(repository, result -> callbackCalled.set(true));
        FakeCommitSource source = source(applier);
        BookingCommand.BookTicket book = book(REQUEST_1, "G01", "ticket-1", TIME_1);

        assertThrows(BookingRepository.StorageException.class, () -> source.commit(1, book));
        assertTrue(fault.get().triggered().get());
        assertFalse(callbackCalled.get());
        assertEquals(0L, applier.lastApplied());
        assertEquals(0, count("tickets"));
        assertEquals(0, countWhere("seats", "status = 'BOOKED'"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));

        fault.set(null);
        var successful = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class,
                source.commit(1, book).orElseThrow());
        assertFalse(successful.isReplay());
        assertTrue(callbackCalled.get());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(1L, applier.lastApplied());
    }

    @Test
    void sellingTenSeatsRejectsTheEleventhAndNeverDuplicatesAnActiveSeat() {
        JdbcCommittedCommandApplier applier = applier();
        FakeCommitSource source = source(applier);

        for (int seatNumber = 1; seatNumber <= 10; seatNumber++) {
            String seatId = String.format(Locale.ROOT, "G%02d", seatNumber);
            var result = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                    source.commit(seatNumber, book(
                            requestId(seatNumber), seatId, "ticket-" + seatNumber, TIME_1))
                            .orElseThrow());
            assertEquals(ApplyResult.Outcome.CONFIRMED, result.applyResult().outcome());
            assertNoDuplicateActiveSeats();
        }

        var eleventh = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                source.commit(11, book(
                        requestId(11), "G01", "ticket-11", TIME_2)).orElseThrow());

        assertEquals(ApplyResult.bookRejected(
                requestId(11), ApplyResult.SEAT_ALREADY_BOOKED, 11), eleventh.applyResult());
        assertEquals(10, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(10, count("tickets"));
        assertNoDuplicateActiveSeats();
    }

    @Test
    void jdbcResultsAndStateMatchInMemoryMachineForTheSameCommittedSequence() {
        JdbcCommittedCommandApplier applier = applier();
        FakeCommitSource source = source(applier);
        BookingStateMachine inMemory = new BookingStateMachine();
        List<BookingCommand> commands = List.of(
                bookWithPassenger(REQUEST_1, "G01", "Passenger ticket-a", "ticket-a", TIME_1),
                book(REQUEST_2, "G01", "ticket-b", TIME_2),
                bookWithPassenger(
                        REQUEST_1, "G01", "Passenger ticket-a", "ticket-replay", TIME_2),
                bookWithPassenger(
                        REQUEST_1, "G02", "Passenger ticket-a", "ticket-conflict", TIME_2),
                new BookingCommand.NoOp(),
                book(REQUEST_3, "G02", "ticket-c", TIME_2),
                new BookingCommand.CancelTicket(REQUEST_4, "ticket-a", TIME_2));

        for (int offset = 0; offset < commands.size(); offset++) {
            long index = offset + 1L;
            BookingCommand command = commands.get(offset);
            assertEquals(inMemory.apply(index, command), source.commit(index, command).orElseThrow(),
                    "apply outcome differs at index " + index);
        }

        assertEquals(inMemory.allSeatStatuses(), repository(null).readSeatSnapshot());
        assertEquals(inMemory.lastApplied(), applier.lastApplied());
        assertEquals(inMemory.ticketStatus("ticket-a"), ticketStatus("ticket-a"));
        assertEquals(inMemory.ticketStatus("ticket-b"), ticketStatus("ticket-b"));
        assertEquals(inMemory.ticketStatus("ticket-replay"), ticketStatus("ticket-replay"));
        assertEquals(inMemory.ticketStatus("ticket-conflict"), ticketStatus("ticket-conflict"));
        assertEquals(inMemory.ticketStatus("ticket-c"), ticketStatus("ticket-c"));
    }

    private JdbcCommittedCommandApplier applier() {
        return applier(result -> { });
    }

    private JdbcCommittedCommandApplier applier(
            CommittedCommandApplier.ApplyResultListener listener) {
        return new JdbcCommittedCommandApplier(repository(null), listener);
    }

    private JdbcBookingRepository repository(AtomicReference<SqlFault> fault) {
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
            return wrapConnection(connection, fault);
        });
    }

    /**
     * Fake commit adapter used only by these SPEC-07 tests; it is not real Raft and is never
     * enabled in runtime code.
     */
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
        return bookWithPassenger(
                requestId, seatId, "Passenger " + ticketId, ticketId, issuedAt);
    }

    private static BookingCommand.BookTicket bookWithPassenger(
            String requestId, String seatId, String passengerName, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, passengerName, ticketId, issuedAt);
    }

    private static String requestId(int suffix) {
        return String.format(Locale.ROOT, "%08d-0000-0000-0000-%012d", suffix, suffix);
    }

    private void insertLog(long logIndex, BookingCommand command) {
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (?, 1, ?, ?::jsonb)")) {
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

    private static Connection wrapConnection(
            Connection connection, AtomicReference<SqlFault> fault) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> {
                    if ("prepareStatement".equals(method.getName())
                            && arguments != null && arguments.length > 0
                            && arguments[0] instanceof String sql) {
                        PreparedStatement statement =
                                (PreparedStatement) invoke(connection, method, arguments);
                        return wrapStatement(statement, sql, fault);
                    }
                    return invoke(connection, method, arguments);
                });
    }

    private static PreparedStatement wrapStatement(
            PreparedStatement statement, String sql, AtomicReference<SqlFault> fault) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, arguments) -> {
                    SqlFault activeFault = fault == null ? null : fault.get();
                    boolean execution = "executeUpdate".equals(method.getName())
                            || "executeLargeUpdate".equals(method.getName())
                            || "execute".equals(method.getName());
                    if (execution && activeFault != null && activeFault.matches(sql)
                            && !activeFault.afterExecution()) {
                        activeFault.triggered().set(true);
                        throw new SQLException("Injected test failure for "
                                + activeFault.sqlPattern());
                    }
                    Object result = invoke(statement, method, arguments);
                    if (execution && activeFault != null && activeFault.matches(sql)
                            && activeFault.afterExecution()) {
                        activeFault.triggered().set(true);
                        throw new SQLException("Injected test failure after "
                                + activeFault.sqlPattern());
                    }
                    return result;
                });
    }

    private static Object invoke(
            Object target, java.lang.reflect.Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private void assertNoDuplicateActiveSeats() {
        assertEquals(0, queryInt(
                "SELECT COUNT(*) FROM (SELECT trip_id, seat_id FROM tickets "
                        + "WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id "
                        + "HAVING COUNT(*) > 1) duplicates"));
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

    private String queryString(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getString(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("Test query failed", exception);
        }
    }

    private Optional<String> ticketStatus(String ticketId) {
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT status FROM tickets WHERE ticket_id = ?")) {
            statement.setString(1, ticketId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? Optional.of(resultSet.getString(1)) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Ticket status query failed", exception);
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

    private record SqlFault(
            String sqlPattern, boolean afterExecution, AtomicBoolean triggered) {
        private SqlFault(String sqlPattern, boolean afterExecution) {
            this(sqlPattern, afterExecution, new AtomicBoolean());
        }

        private boolean matches(String sql) {
            return sql.toUpperCase(Locale.ROOT)
                    .contains(sqlPattern.toUpperCase(Locale.ROOT));
        }
    }
}
