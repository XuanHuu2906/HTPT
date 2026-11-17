package vn.nhom6.railway.raft.core;

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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D18 PostgreSQL check of {@link RaftStateMachineApplier} over the real
 * {@link JdbcCommittedCommandApplier} and {@code JdbcBookingRepository}.
 *
 * <p>Gated exactly like {@code JdbcCommittedCommandApplierTest}: it runs only when
 * {@code RAILWAY_TEST_JDBC_URL}, {@code RAILWAY_TEST_DB_USER} and
 * {@code RAILWAY_TEST_DB_PASSWORD} are set. Every test creates and drops its own temporary
 * schema; it never touches existing data. Without those variables JUnit reports the class as
 * SKIPPED, which is not a PASS.
 *
 * <p>What this proves when it really runs: the applier's order, checkpoint and replay behaviour
 * against a durable transaction, including a real SQL rollback in the middle of the prefix. What
 * it does not prove: Raft replication, a five-node cluster, or a production command codec.
 */
class RaftStateMachineApplierJdbcTest {

    private static final String SELF = "node-1";
    private static final String TIME = "2026-10-29T10:00:00Z";

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
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER and RAILWAY_TEST_DB_PASSWORD are required");

        schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
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
            if (schema != null && hasText(jdbcUrl)) {
                dropTrigger();
            }
            dropSchema();
        } finally {
            schema = null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // 1. Ordered apply through the real JDBC transaction, including NO_OP
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: apply 1..commitIndex qua transaction thật, NO_OP chỉ tăng checkpoint")
    void appliesCommittedPrefixAndNoOpAdvancesOnlyTheDurableCheckpoint() {
        seedLog(1, book(requestId(1), "G01", "ticket-1"));
        seedLog(2, new BookingCommand.NoOp());
        seedLog(3, book(requestId(2), "G01", "ticket-2"));

        NodeFixture node = node(3, entry(1, book(requestId(1), "G01", "ticket-1")),
                RaftLogEntry.noOp(2, 1L), entry(3, book(requestId(2), "G01", "ticket-2")));

        assertEquals(3, node.applier.applyCommitted(),
                "applier stopped early; failures=" + node.failures);
        assertFalse(node.applier.failed(), "unexpected fatal failure: " + node.failures);
        assertEquals(3L, node.state.lastApplied());
        assertEquals(3L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(2, count("request_results"),
                "the real repository also stores the REJECTED decision of BOOK 3");
        assertEquals(2, count("business_events"),
                "one BOOK_CONFIRMED and one BOOK_REJECTED; the NO_OP must not create an event");
        assertEquals(2, node.results.size(), "only BOOK 1 and BOOK 3 emit a business result");
        assertEquals(ApplyResult.Outcome.REJECTED,
                assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                        node.outcomes.get(2)).applyResult().outcome(),
                "the second G01 request is rejected deterministically by the real repository");
    }

    // -------------------------------------------------------------------------------------------
    // 2. Replay after a crash must not sell twice
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: replay sau crash, giữ DB, không bán trùng và không sinh event mới")
    void replayAfterCrashDoesNotSellTwice() {
        List<RaftLogEntry> entries = List.of(
                entry(1, book(requestId(1), "G01", "ticket-1")),
                RaftLogEntry.noOp(2, 1L),
                entry(3, book(requestId(2), "G02", "ticket-2")));
        for (RaftLogEntry entry : entries) {
            seedLog(entry.index(), commandOf(entry));
        }

        NodeFixture first = node(3, entries.toArray(RaftLogEntry[]::new));
        assertEquals(3, first.applier.applyCommitted(),
                "applier stopped early; failures=" + first.failures);
        int tickets = count("tickets");
        int results = count("request_results");
        int events = count("business_events");
        assertEquals(2, tickets);
        assertEquals(2, results);

        // Restart: durable repository survives, RaftState restarts with lastApplied = 0.
        RaftState restartedState = RaftState.restore(1L, null, entries, 0L);
        restartedState.advanceCommitIndex(3);
        NodeFixture restarted = nodeOver(restartedState);

        assertEquals(3, restarted.applier.applyCommitted(), "the whole prefix is re-delivered");
        assertFalse(restarted.applier.failed(), "unexpected failure: " + restarted.failures);
        assertEquals(3L, restartedState.lastApplied());
        assertEquals(3L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(tickets, count("tickets"), "replay must not create another ticket");
        assertEquals(results, count("request_results"));
        assertEquals(events, count("business_events"), "redelivery must not add a business event");
        assertEquals(0, restarted.results.size(),
                "JdbcCommittedCommandApplier replays from request_results without re-reporting");
        assertEquals(1, countWhere("tickets", "status = 'ACTIVE' AND seat_id = 'G01'"));
        assertEquals(0, queryInt(
                "SELECT COUNT(*) FROM (SELECT trip_id, seat_id FROM tickets "
                        + "WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id "
                        + "HAVING COUNT(*) > 1) duplicates"));
    }

    // -------------------------------------------------------------------------------------------
    // 3. A real SQL failure in the middle rolls back and is retried at the same index
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("JDBC: lỗi SQL giữa chừng rollback, không markApplied, retry đúng index")
    void sqlFailureMidwayRollsBackAndRetryAppliesTheSameIndexOnce() {
        seedLog(1, book(requestId(1), "G01", "ticket-1"));
        seedLog(2, book(requestId(2), "G02", "ticket-2"));

        NodeFixture node = node(1, entry(1, book(requestId(1), "G01", "ticket-1")),
                entry(2, book(requestId(2), "G02", "ticket-2")));
        assertEquals(1, node.applier.applyCommitted(),
                "the first committed entry must apply; failures=" + node.failures);
        assertEquals(1L, node.state.lastApplied());

        node.state.advanceCommitIndex(2);
        createFailingRequestResultsTrigger();
        assertEquals(0, node.applier.applyCommitted());
        assertFalse(node.applier.failed(), "a SQL/storage failure is retryable, not fatal");
        assertEquals(1L, node.state.lastApplied(), "index 2 must not be marked applied");
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"),
                "the apply transaction rolled back");
        assertEquals(1, count("tickets"), "the rejected transaction left no ticket");
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        assertEquals(1, node.failures.size());
        assertTrue(node.failures.getFirst().startsWith("false:"),
                "a SQL failure must be classified as retryable: " + node.failures);

        dropTrigger();
        assertEquals(1, node.applier.applyCommitted(), "the retry applies exactly index 2");
        assertEquals(2L, node.state.lastApplied());
        assertEquals(2L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(2, countWhere("tickets", "status = 'ACTIVE'"));
        assertEquals(2, count("request_results"));
        assertEquals(2, node.results.size());
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static final class NodeFixture {

        final RaftState state;
        final RaftStateMachineApplier applier;
        final List<ApplyResult> results;
        final List<BookingStateMachine.ApplyOutcome> outcomes;
        final List<String> failures;

        NodeFixture(RaftState state, RaftStateMachineApplier applier, List<ApplyResult> results,
                List<BookingStateMachine.ApplyOutcome> outcomes, List<String> failures) {
            this.state = state;
            this.applier = applier;
            this.results = results;
            this.outcomes = outcomes;
            this.failures = failures;
        }
    }

    private NodeFixture node(long commitIndex, RaftLogEntry... entries) {
        RaftState state = new RaftState();
        state.startElection(SELF);
        for (RaftLogEntry entry : entries) {
            state.append(entry);
        }
        state.advanceCommitIndex(commitIndex);
        return nodeOver(state);
    }

    private NodeFixture nodeOver(RaftState state) {
        List<ApplyResult> results = new ArrayList<>();
        List<BookingStateMachine.ApplyOutcome> outcomes = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        JdbcCommittedCommandApplier committed =
                new JdbcCommittedCommandApplier(connectionFactory, results::add);
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                state, new TestCommandCodec(), committed.commitListener(),
                new RaftStateMachineApplier.Hooks() {
                    @Override
                    public void onApplied(RaftLogEntry entry,
                            Optional<BookingStateMachine.ApplyOutcome> outcome) {
                        outcome.ifPresent(outcomes::add);
                    }

                    @Override
                    public void onApplyFailure(RaftLogEntry entry, RuntimeException failure,
                            boolean fatal) {
                        failures.add(fatal + ":" + failure);
                    }
                });
        return new NodeFixture(state, applier, results, outcomes, failures);
    }

    private static final class TestCommandCodec implements RaftStateMachineApplier.CommandDecoder {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public BookingCommand decode(RaftLogEntry entry) {
            try {
                JsonNode node = MAPPER.readTree(entry.commandPayload());
                return switch (entry.commandType()) {
                    case RaftLogEntry.COMMAND_TYPE_BOOK_TICKET -> new BookingCommand.BookTicket(
                            text(node, "requestId"),
                            text(node, "tripId"),
                            text(node, "seatId"),
                            text(node, "passengerName"),
                            text(node, "ticketId"),
                            Instant.parse(text(node, "issuedAt")));
                    case RaftLogEntry.COMMAND_TYPE_NO_OP -> new BookingCommand.NoOp();
                    default -> throw new IllegalArgumentException(
                            "fixture codec does not support '" + entry.commandType() + "'");
                };
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "fixture payload is not valid JSON for index " + entry.index(), failure);
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null || !value.isTextual()) {
                throw new IllegalArgumentException("fixture payload is missing text field '" + field + "'");
            }
            return value.asText();
        }
    }

    private static RaftLogEntry entry(long index, BookingCommand command) {
        return new RaftLogEntry(index, 1L, command.type().name(), payload(command));
    }

    private static BookingCommand book(String requestId, String seatId, String ticketId) {
        return new BookingCommand.BookTicket(requestId, "SE-001", seatId,
                "Passenger " + ticketId, ticketId, Instant.parse(TIME));
    }

    private static BookingCommand commandOf(RaftLogEntry entry) {
        return new TestCommandCodec().decode(entry);
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

    private static String requestId(long suffix) {
        return String.format(Locale.ROOT, "%08d-0000-0000-0000-%012d", suffix, suffix);
    }

    private void seedLog(long logIndex, BookingCommand command) {
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (?, 1, ?, ?::jsonb)")) {
            statement.setLong(1, logIndex);
            statement.setString(2, command.type().name());
            statement.setString(3, payload(command));
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not seed raft_log row " + logIndex, exception);
        }
    }

    private void createFailingRequestResultsTrigger() {
        execute("""
                CREATE OR REPLACE FUNCTION railway_d18_fail_request_results() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'injected request_results failure (TV4-D18 test)';
                END
                $$
                """);
        execute("CREATE TRIGGER railway_d18_fail_request_results BEFORE INSERT ON request_results "
                + "FOR EACH ROW EXECUTE FUNCTION railway_d18_fail_request_results()");
    }

    private void dropTrigger() {
        if (schema == null || !hasText(jdbcUrl)) {
            return;
        }
        execute("DROP TRIGGER IF EXISTS railway_d18_fail_request_results ON request_results");
        execute("DROP FUNCTION IF EXISTS railway_d18_fail_request_results()");
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
        // ConnectionFactory owns the URL, so the temporary schema is selected per connection.
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
