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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.ConnectionFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBookingRepositoryTest {
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

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        username = System.getenv("RAILWAY_TEST_DB_USER");
        password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = rawConnection(); Statement statement = connection.createStatement()) {
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
        dropSchema();
    }

    @Test
    void bookAvailableSeatPersistsOneAtomicBusinessResult() throws Exception {
        JdbcBookingRepository repository = repository(null);
        BookingCommand.BookTicket book = book(REQUEST_1, "G01", "ticket-1", TIME_1);

        BookingStateMachine.ApplyOutcome outcome = apply(repository, 1, book);

        var business = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
        assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-1", 1), business.applyResult());
        assertEquals(1, count("tickets"));
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(1, countWhere("seats", "seat_id = 'G01' AND status = 'BOOKED' "
                + "AND current_ticket_id = 'ticket-1'"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(7, queryInt(
                "SELECT (SELECT count(*) FROM jsonb_object_keys(result_json)) FROM request_results WHERE request_id = '"
                        + REQUEST_1 + "'"));
        assertEquals(7, queryInt(
                "SELECT (SELECT count(*) FROM jsonb_object_keys(event_json)) FROM business_events WHERE log_index = 1"));
        assertEquals("G01", queryString(
                "SELECT result_json ->> 'seatId' FROM request_results WHERE request_id = '"
                        + REQUEST_1 + "'"));
        assertEquals(1L, repository.lastApplied());
        assertEquals(1L, repository.findResultByRequestId(REQUEST_1).orElseThrow().appliedLogIndex());
        assertEquals("BOOK_CONFIRMED", queryString(
                "SELECT event_type FROM business_events WHERE log_index = 1"));
    }

    @Test
    void injectedFailuresRollbackEveryWriteAndSameEntryCanRetry() throws Exception {
        List<SqlFault> faults = List.of(
                new SqlFault("INSERT INTO tickets", true),
                new SqlFault("UPDATE seats SET status", false),
                new SqlFault("INSERT INTO request_results", false),
                new SqlFault("UPDATE raft_apply_progress SET last_applied", false));

        for (SqlFault fault : faults) {
            resetSchemaData();
            BookingCommand.BookTicket book = book(
                    REQUEST_1, "G01", "ticket-rollback-" + faults.indexOf(fault), TIME_1);
            insertLog(1, book);
            AtomicReference<SqlFault> injectedFault = new AtomicReference<>(fault);
            JdbcBookingRepository repository = repository(injectedFault);

            assertThrows(RuntimeException.class, () -> repository.applyAtomically(1, book),
                    "fault must escape for SQL pattern " + fault.sqlPattern());
            assertTrue(fault.triggered().get(), "fault was not injected at " + fault.sqlPattern());
            assertEquals(0, count("tickets"));
            assertEquals(0, countWhere("seats", "seat_id = 'G01' AND status = 'BOOKED'"));
            assertEquals(0, count("request_results"));
            assertEquals(0, count("business_events"));
            assertEquals(0L, repository.lastApplied());

            injectedFault.set(null);
            var successful = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class,
                    repository.applyAtomically(1, book));
            assertFalse(successful.isReplay());
            assertEquals(1, count("tickets"));
            assertEquals(1, count("request_results"));
            assertEquals(1, count("business_events"));
            assertEquals(1L, repository.lastApplied());
        }
    }

    @Test
    void duplicateBookingIsRejectedWithoutSecondActiveTicket() {
        JdbcBookingRepository repository = repository(null);
        apply(repository, 1, book(REQUEST_1, "G01", "ticket-1", TIME_1));

        var result = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                apply(repository, 2, book(REQUEST_2, "G01", "ticket-2", TIME_2)));

        assertEquals(ApplyResult.bookRejected(
                REQUEST_2, ApplyResult.SEAT_ALREADY_BOOKED, 2), result.applyResult());
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE' AND seat_id = 'G01'"));
        assertEquals(1, count("tickets"));
        assertEquals("BOOK_REJECTED", queryString(
                "SELECT event_type FROM business_events WHERE log_index = 2"));
    }

    @Test
    void replayReturnsOriginalResultAndIndexWhileCheckpointAdvances() {
        JdbcBookingRepository repository = repository(null);
        BookingCommand.BookTicket original = book(REQUEST_1, "G01", "ticket-original", TIME_1);
        apply(repository, 1, original);

        var replay = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                apply(repository, 2,
                        new BookingCommand.BookTicket(REQUEST_1, original.tripId(), original.seatId(),
                                original.passengerName(), "ticket-from-new-leader", TIME_2)));

        assertTrue(replay.isReplay());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-original", 1), replay.applyResult());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(2L, repository.lastApplied());
        assertEquals(replay, repository.findAppliedOutcome(2).orElseThrow());
        // Ten retries, including freshly constructed repository instances, reuse the durable result.
        for (long index = 3; index <= 11; index++) {
            JdbcBookingRepository reopened = repository(null);
            var retried = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                    apply(reopened, index, new BookingCommand.BookTicket(REQUEST_1,
                            original.tripId(), original.seatId(), original.passengerName(),
                            "ticket-retry-" + index, TIME_2)));
            assertTrue(retried.isReplay());
            assertEquals(replay.applyResult(), retried.applyResult());
            assertEquals(replay.applyResult(), reopened.findResultByRequestId(REQUEST_1).orElseThrow());
            assertEquals(index, reopened.lastApplied());
            assertEquals(1, count("tickets"));
            assertEquals(1, count("request_results"));
            assertEquals(1, count("business_events"));
        }
    }

    @Test
    void requestIdConflictPreservesOriginalAndNextEntryCanApply() {
        JdbcBookingRepository repository = repository(null);
        ApplyResult original = ((BookingStateMachine.ApplyOutcome.BusinessResult)
                apply(repository, 1, book(REQUEST_1, "G01", "ticket-1", TIME_1))).applyResult();

        var conflict = assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class,
                apply(repository, 2, book(REQUEST_1, "G02", "ticket-conflict", TIME_2)));
        assertEquals(new BookingStateMachine.ApplyOutcome.RequestIdConflict(REQUEST_1, 2), conflict);
        assertEquals(original, repository.findResultByRequestId(REQUEST_1).orElseThrow());
        assertEquals("AVAILABLE", repository.readSeatSnapshot().get("G02"));
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));

        apply(repository, 3, book(REQUEST_2, "G02", "ticket-next", TIME_2));
        assertEquals(3L, repository.lastApplied());
        assertEquals(2, count("tickets"));
    }

    @Test
    void noOpOnlyAdvancesApplyCheckpoint() {
        JdbcBookingRepository repository = repository(null);

        assertEquals(new BookingStateMachine.ApplyOutcome.NoOpApplied(1),
                apply(repository, 1, new BookingCommand.NoOp()));

        assertEquals(1L, repository.lastApplied());
        assertEquals(0, count("tickets"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));
    }

    @Test
    void cancelUpdatesTicketAndSeatAndRejectsMissingOrCancelledTickets() {
        JdbcBookingRepository repository = repository(null);
        apply(repository, 1, book(REQUEST_1, "G01", "ticket-1", TIME_1));

        var cancelled = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                apply(repository, 2, new BookingCommand.CancelTicket(REQUEST_2, "ticket-1", TIME_2)));
        assertEquals(ApplyResult.cancelConfirmed(REQUEST_2, "ticket-1", 2), cancelled.applyResult());
        assertEquals("CANCELLED", queryString(
                "SELECT status FROM tickets WHERE ticket_id = 'ticket-1'"));
        assertEquals(1, countWhere("seats", "seat_id = 'G01' AND status = 'AVAILABLE' "
                + "AND current_ticket_id IS NULL"));

        var missing = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                apply(repository, 3,
                        new BookingCommand.CancelTicket(REQUEST_3, "missing-ticket", TIME_2)));
        assertEquals(ApplyResult.cancelRejected(REQUEST_3, "missing-ticket", 3),
                missing.applyResult());

        var alreadyCancelled = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class,
                apply(repository, 4,
                        new BookingCommand.CancelTicket(REQUEST_4, "ticket-1", TIME_2)));
        assertEquals(ApplyResult.cancelRejected(REQUEST_4, "ticket-1", 4),
                alreadyCancelled.applyResult());
        assertEquals(4L, repository.lastApplied());
        // SPEC-05 persists every new intent, including both rejected cancellations.
        assertEquals(4, count("request_results"));
        assertEquals(2, countWhere("request_results", "state = 'APPLIED_SUCCESS'"));
        assertEquals(2, countWhere("request_results", "state = 'APPLIED_REJECTED'"));
        assertEquals(missing.applyResult(), repository.findResultByRequestId(REQUEST_3).orElseThrow());
        assertEquals(alreadyCancelled.applyResult(),
                repository.findResultByRequestId(REQUEST_4).orElseThrow());
        assertEquals(4, count("business_events"));
        assertEquals(5, queryInt(
                "SELECT (SELECT count(*) FROM jsonb_object_keys(result_json)) FROM request_results WHERE request_id = '"
                        + REQUEST_2 + "'"));
        assertEquals("CANCEL_REJECTED", queryString(
                "SELECT event_type FROM business_events WHERE log_index = 4"));
    }

    @Test
    void invalidApplyIndexesFailClosedWithoutChangingState() {
        JdbcBookingRepository repository = repository(null);
        BookingCommand.BookTicket skipped = book(REQUEST_1, "G01", "ticket-1", TIME_1);
        insertLog(2, skipped);
        assertThrows(IllegalStateException.class, () -> repository.applyAtomically(2, skipped));
        assertEquals(0L, repository.lastApplied());
        assertEquals(0, count("tickets"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));

        apply(repository, 1, skipped);
        assertThrows(IllegalStateException.class, () -> repository.applyAtomically(1, skipped));
        assertEquals(1L, repository.lastApplied());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
    }

    @Test
    void jdbcOutcomesAndBookingStateMatchInMemoryMachine() {
        JdbcBookingRepository repository = repository(null);
        BookingStateMachine stateMachine = new BookingStateMachine();
        List<BookingCommand> commands = List.of(
                book(REQUEST_1, "G01", "ticket-a", TIME_1),
                book(REQUEST_2, "G01", "ticket-b", TIME_2),
                new BookingCommand.BookTicket(REQUEST_1, "SE-001", "G01",
                        "Passenger ticket-a", "ticket-replay", TIME_2),
                book(REQUEST_1, "G02", "ticket-conflict", TIME_2),
                new BookingCommand.NoOp(),
                new BookingCommand.CancelTicket(REQUEST_3, "ticket-a", TIME_2));

        for (int index = 0; index < commands.size(); index++) {
            long logIndex = index + 1L;
            BookingCommand command = commands.get(index);
            insertLog(logIndex, command);
            assertEquals(stateMachine.apply(logIndex, command),
                    repository.applyAtomically(logIndex, command),
                    "outcome differs at log index " + logIndex);
        }

        assertEquals(stateMachine.allSeatStatuses(), repository.readSeatSnapshot());
        assertEquals(stateMachine.lastApplied(), repository.lastApplied());
        assertEquals(stateMachine.ticketStatus("ticket-a"), ticketStatus("ticket-a"));
        assertEquals(stateMachine.ticketStatus("ticket-b"), ticketStatus("ticket-b"));
        assertEquals(stateMachine.ticketStatus("ticket-replay"), ticketStatus("ticket-replay"));
        assertEquals(stateMachine.ticketStatus("ticket-conflict"), ticketStatus("ticket-conflict"));
    }

    @Test
    void concurrentApplyOfSameIndexHasExactlyOneWinner() throws Exception {
        BookingCommand.BookTicket book = book(REQUEST_1, "G01", "ticket-1", TIME_1);
        insertLog(1, book);
        JdbcBookingRepository repository = repository(null);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            List<Future<BookingStateMachine.ApplyOutcome>> futures = new ArrayList<>();
            for (int index = 0; index < 2; index++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return repository.applyAtomically(1, book);
                }));
            }
            ready.await();
            start.countDown();

            int successes = 0;
            int failures = 0;
            for (Future<BookingStateMachine.ApplyOutcome> future : futures) {
                try {
                    assertInstanceOf(
                            BookingStateMachine.ApplyOutcome.BusinessResult.class, future.get());
                    successes++;
                } catch (ExecutionException exception) {
                    assertInstanceOf(IllegalStateException.class, exception.getCause());
                    failures++;
                }
            }
            assertEquals(1, successes);
            assertEquals(1, failures);
        }
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(1L, repository.lastApplied());
    }

    @Test
    void seatSnapshotContainsExactlyTenSeatsInOrder() {
        Map<String, String> snapshot = repository(null).readSeatSnapshot();
        assertEquals(10, snapshot.size());
        for (int number = 1; number <= 10; number++) {
            assertEquals("G" + String.format("%02d", number),
                    snapshot.keySet().toArray()[number - 1]);
            assertEquals("AVAILABLE", snapshot.get("G" + String.format("%02d", number)));
        }
    }

    private BookingStateMachine.ApplyOutcome apply(
            JdbcBookingRepository repository, long logIndex, BookingCommand command) {
        insertLog(logIndex, command);
        return repository.applyAtomically(logIndex, command);
    }

    private static BookingCommand.BookTicket book(
            String requestId, String seatId, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, "Passenger " + ticketId, ticketId, issuedAt);
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
                        throw new SQLException("Injected test failure for " + activeFault.sqlPattern());
                    }
                    Object result = invoke(statement, method, arguments);
                    if (execution && activeFault != null && activeFault.matches(sql)
                            && activeFault.afterExecution()) {
                        activeFault.triggered().set(true);
                        throw new SQLException("Injected test failure after " + activeFault.sqlPattern());
                    }
                    return result;
                });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private void insertLog(long logIndex, BookingCommand command) {
        String type = command.type().name();
        String json = commandJson(command);
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (?, 1, ?, ?::jsonb)")) {
            statement.setLong(1, logIndex);
            statement.setString(2, type);
            statement.setString(3, json);
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

    private void resetSchemaData() throws SQLException {
        try (Connection connection = schemaConnection(); Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE business_events, request_results, tickets, raft_log CASCADE");
            statement.execute("UPDATE seats SET status = 'AVAILABLE', current_ticket_id = NULL");
            statement.execute("UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1");
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
        try (Connection connection = schemaConnection(); Statement statement = connection.createStatement()) {
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
        try (Connection connection = rawConnection(); Statement statement = connection.createStatement()) {
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
            return sql.toUpperCase(java.util.Locale.ROOT)
                    .contains(sqlPattern.toUpperCase(java.util.Locale.ROOT));
        }
    }
}
