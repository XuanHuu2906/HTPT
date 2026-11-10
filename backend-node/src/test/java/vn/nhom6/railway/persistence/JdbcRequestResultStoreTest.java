package vn.nhom6.railway.persistence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.persistence.JdbcRequestResultStore.Classification;
import vn.nhom6.railway.persistence.JdbcRequestResultStore.StoredRequestResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcRequestResultStoreTest {
    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final Instant TIME_1 = Instant.parse("2026-10-29T10:00:00Z");
    private static final Instant TIME_2 = Instant.parse("2026-10-29T10:01:00Z");
    private static final String BOOK_GOLDEN =
            "d2ce4e7bc0b8f00bd68dd016f15dfd5e1d8e0999493d22a298ec18c6e160c83a";
    private static final String CANCEL_GOLDEN =
            "9b841213b45fcb426eec780e5c7aa935d6d9141a53a9c9f80844e57aaf1b1375";

    @Test
    void goldenFingerprintsMatchIndependentEncodingAndFixedDigests()
            throws NoSuchAlgorithmException {
        String book = BookingStateMachine.computeBookFingerprint(
                "SE-001", "G05", "Nguyễn Ái Quốc");
        String cancel = BookingStateMachine.computeCancelFingerprint("ticket-123");

        assertEquals(BOOK_GOLDEN, book);
        assertEquals(CANCEL_GOLDEN, cancel);
        assertEquals(referenceFingerprint("BOOK_TICKET", "SE-001", "G05", "Nguyễn Ái Quốc"), book);
        assertEquals(referenceFingerprint("CANCEL_TICKET", "ticket-123"), cancel);
        assertLowercaseSha256(book);
        assertLowercaseSha256(cancel);
    }

    @Test
    void bookIgnoresRequestIdTicketIdAndIssuedAtButTracksBusinessIntent() {
        BookingCommand.BookTicket original =
                book(REQUEST_1, "G05", "leader-ticket-1", TIME_1);
        BookingCommand.BookTicket retried =
                book(REQUEST_2, "G05", "leader-ticket-2", TIME_2);
        String fingerprint = fingerprint(original);

        assertEquals(fingerprint, fingerprint(retried));
        assertNotEquals(fingerprint,
                BookingStateMachine.computeBookFingerprint("SE-001", "G06", "Passenger"));
        assertNotEquals(fingerprint,
                BookingStateMachine.computeBookFingerprint("SE-002", "G05", "Passenger"));
        assertNotEquals(fingerprint,
                BookingStateMachine.computeBookFingerprint("SE-001", "G05", "Different"));
    }

    @Test
    void cancelTracksTicketIdButIgnoresRequestIdAndCancelledAt() {
        BookingCommand.CancelTicket original =
                new BookingCommand.CancelTicket(REQUEST_1, "ticket-123", TIME_1);
        BookingCommand.CancelTicket retried =
                new BookingCommand.CancelTicket(REQUEST_2, "ticket-123", TIME_2);

        assertEquals(
                BookingStateMachine.computeCancelFingerprint(original.ticketId()),
                BookingStateMachine.computeCancelFingerprint(retried.ticketId()));
        assertNotEquals(
                BookingStateMachine.computeCancelFingerprint(original.ticketId()),
                BookingStateMachine.computeCancelFingerprint("ticket-456"));
    }

    @Test
    void tupleFieldsAreLengthDelimitedByUtf8BytesWithoutNormalization()
            throws NoSuchAlgorithmException {
        String separated = BookingStateMachine.computeBookFingerprint("ab", "c", "x");
        String shifted = BookingStateMachine.computeBookFingerprint("a", "bc", "x");
        assertNotEquals(separated, shifted);
        assertEquals(referenceFingerprint("BOOK_TICKET", "ab", "c", "x"), separated);
        assertEquals(referenceFingerprint("BOOK_TICKET", "a", "bc", "x"), shifted);

        String vietnamese = BookingStateMachine.computeBookFingerprint(
                "SE-001", "G01", "Nguyễn");
        assertEquals(referenceFingerprint("BOOK_TICKET", "SE-001", "G01", "Nguyễn"), vietnamese);

        String longEmojiName = "🙂".repeat(80);
        assertTrue(longEmojiName.getBytes(StandardCharsets.UTF_8).length > 255);
        String longNameFingerprint = BookingStateMachine.computeBookFingerprint(
                "SE-001", "G01", longEmojiName);
        assertEquals(
                referenceFingerprint("BOOK_TICKET", "SE-001", "G01", longEmojiName),
                longNameFingerprint);

        String nfc = "\u00e9";
        String nfd = "e\u0301";
        assertNotEquals(
                BookingStateMachine.computeBookFingerprint("SE-001", "G01", nfc),
                BookingStateMachine.computeBookFingerprint("SE-001", "G01", nfd));
        assertLowercaseSha256(separated);
        assertLowercaseSha256(shifted);
        assertLowercaseSha256(vietnamese);
        assertLowercaseSha256(longNameFingerprint);
    }

    @Test
    void commandTypeIsPartOfBusinessIntentFingerprint() {
        String book = BookingStateMachine.computeBookFingerprint(
                "SE-001", "G05", "Nguyễn Ái Quốc");
        String cancel = BookingStateMachine.computeCancelFingerprint("Nguyễn Ái Quốc");
        assertNotEquals(book, cancel);
        assertLowercaseSha256(book);
        assertLowercaseSha256(cancel);
    }

    @Test
    void insertAndFindPreserveBookAndCancelPayloads() throws Exception {
        try (TestDatabase database = TestDatabase.open()) {
            JdbcRequestResultStore store = new JdbcRequestResultStore();
            String bookFingerprint = BookingStateMachine.computeBookFingerprint(
                    "SE-001", "G05", "Nguyễn Ái Quốc");
            ApplyResult bookResult = ApplyResult.bookConfirmed(REQUEST_1, "ticket-1", 17);
            try (Connection connection = database.connection()) {
                connection.setAutoCommit(false);
                String bookJson = store.insert(
                        connection, REQUEST_1, "BOOK_TICKET", bookFingerprint,
                        bookResult, "SE-001", "G05");
                assertFalse(connection.getAutoCommit());
                assertTrue(jsonbEquals(connection, REQUEST_1,
                        "{\"requestId\":\"" + REQUEST_1
                                + "\",\"outcome\":\"CONFIRMED\",\"ticketId\":\"ticket-1\""
                                + ",\"tripId\":\"SE-001\",\"seatId\":\"G05\","
                                + "\"reasonCode\":null,\"appliedLogIndex\":17}"));
                assertTrue(jsonbEquals(connection, REQUEST_1, bookJson));
                assertEquals(7, jsonObjectLength(connection, REQUEST_1));
                StoredRequestResult stored = store.find(connection, REQUEST_1).orElseThrow();
                assertEquals(REQUEST_1, stored.requestId());
                assertEquals("BOOK_TICKET", stored.commandType());
                assertEquals(bookFingerprint, stored.intentFingerprint());
                assertEquals("APPLIED_SUCCESS", stored.state());
                assertEquals("ticket-1", stored.ticketId());
                assertEquals(17, stored.appliedLogIndex());
                assertEquals(bookResult, stored.toApplyResult());
                connection.commit();
            }

            String cancelFingerprint =
                    BookingStateMachine.computeCancelFingerprint("ticket-2");
            ApplyResult cancelResult =
                    ApplyResult.cancelRejected(REQUEST_2, "ticket-2", 18);
            try (Connection connection = database.connection()) {
                String cancelJson = store.insert(
                        connection, REQUEST_2, "CANCEL_TICKET", cancelFingerprint,
                        cancelResult, null, null);
                StoredRequestResult stored = store.find(connection, REQUEST_2).orElseThrow();
                assertEquals("CANCEL_TICKET", stored.commandType());
                assertEquals(cancelFingerprint, stored.intentFingerprint());
                assertEquals("APPLIED_REJECTED", stored.state());
                assertEquals(ApplyResult.Outcome.REJECTED, stored.toApplyResult().outcome());
                assertEquals("TICKET_NOT_ACTIVE", stored.reasonCode());
                assertEquals("ticket-2", stored.ticketId());
                assertEquals(18, stored.appliedLogIndex());
                assertTrue(jsonbEquals(connection, REQUEST_2,
                        "{\"requestId\":\"" + REQUEST_2
                                + "\",\"outcome\":\"REJECTED\",\"ticketId\":\"ticket-2\""
                                + ",\"reasonCode\":\"TICKET_NOT_ACTIVE\","
                                + "\"appliedLogIndex\":18}"));
                assertTrue(jsonbEquals(connection, REQUEST_2, cancelJson));
                assertEquals(5, jsonObjectLength(connection, REQUEST_2));
            }
        }
    }

    @Test
    void duplicateInsertAndUnknownNeverReplaceTheOriginalResult() throws Exception {
        try (TestDatabase database = TestDatabase.open();
                Connection connection = database.connection()) {
            JdbcRequestResultStore store = new JdbcRequestResultStore();
            String fingerprint = BookingStateMachine.computeBookFingerprint(
                    "SE-001", "G01", "Passenger");
            ApplyResult first = ApplyResult.bookConfirmed(REQUEST_1, "ticket-original", 3);
            store.insert(connection, REQUEST_1, "BOOK_TICKET", fingerprint,
                    first, "SE-001", "G01");

            assertThrows(BookingRepository.StorageException.class,
                    () -> store.insert(connection, REQUEST_1, "BOOK_TICKET",
                            BookingStateMachine.computeBookFingerprint(
                                    "SE-001", "G02", "Changed"),
                            ApplyResult.bookConfirmed(REQUEST_1, "ticket-overwrite", 4),
                            "SE-001", "G02"));
            assertThrows(IllegalArgumentException.class,
                    () -> store.insert(connection, REQUEST_2, "BOOK_TICKET", fingerprint,
                            ApplyResult.bookUnknown(REQUEST_2), "SE-001", "G01"));

            StoredRequestResult stored = store.find(connection, REQUEST_1).orElseThrow();
            assertEquals(fingerprint, stored.intentFingerprint());
            assertEquals(first, stored.toApplyResult());
            assertEquals(1, count(connection, "request_results"));
        }
    }

    @Test
    void classifyDistinguishesNewReplayAndConflictingIntents() throws Exception {
        try (TestDatabase database = TestDatabase.open();
                Connection connection = database.connection()) {
            JdbcRequestResultStore store = new JdbcRequestResultStore();
            String originalFingerprint = BookingStateMachine.computeBookFingerprint(
                    "SE-001", "G01", "Passenger");
            assertEquals(Classification.NEW, JdbcRequestResultStore.classify(
                    store.find(connection, REQUEST_1), "BOOK_TICKET", originalFingerprint));

            store.insert(connection, REQUEST_1, "BOOK_TICKET", originalFingerprint,
                    ApplyResult.bookConfirmed(REQUEST_1, "ticket-original", 5),
                    "SE-001", "G01");
            Optional<StoredRequestResult> original = store.find(connection, REQUEST_1);
            assertEquals(Classification.REPLAY, JdbcRequestResultStore.classify(
                    original, "BOOK_TICKET", originalFingerprint));
            assertEquals(Classification.CONFLICT, JdbcRequestResultStore.classify(
                    original, "BOOK_TICKET",
                    BookingStateMachine.computeBookFingerprint(
                            "SE-001", "G02", "Passenger")));
            assertEquals(Classification.CONFLICT, JdbcRequestResultStore.classify(
                    original, "BOOK_TICKET",
                    BookingStateMachine.computeBookFingerprint(
                            "SE-001", "G01", "Changed")));
            assertEquals(Classification.CONFLICT, JdbcRequestResultStore.classify(
                    original, "CANCEL_TICKET",
                    BookingStateMachine.computeCancelFingerprint("ticket-original")));
            assertEquals(5, original.orElseThrow().appliedLogIndex());
            assertEquals("ticket-original",
                    store.find(connection, REQUEST_1).orElseThrow().ticketId());
        }
    }

    @Test
    void bookingRepositoryReturnsOriginalResultForRetryAndRejectsChangedPayload()
            throws Exception {
        try (TestDatabase database = TestDatabase.open()) {
            JdbcBookingRepository repository =
                    new JdbcBookingRepository(database::connectionUnchecked);
            BookingCommand.BookTicket original =
                    book(REQUEST_1, "G01", "ticket-original", TIME_1);
            insertLog(database, 1, original);
            BookingStateMachine.ApplyOutcome.BusinessResult first = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class,
                    repository.applyAtomically(1, original));

            BookingCommand.BookTicket retry =
                    book(REQUEST_1, "G01", "ticket-from-new-leader", TIME_2);
            insertLog(database, 2, retry);
            BookingStateMachine.ApplyOutcome.BusinessResult replay = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class,
                    repository.applyAtomically(2, retry));

            BookingCommand.BookTicket changed =
                    book(REQUEST_1, "G02", "ticket-conflict", TIME_2);
            insertLog(database, 3, changed);
            BookingStateMachine.ApplyOutcome conflict = repository.applyAtomically(3, changed);

            assertEquals(ApplyResult.bookConfirmed(REQUEST_1, "ticket-original", 1),
                    first.applyResult());
            assertTrue(replay.isReplay());
            assertEquals(first.applyResult(), replay.applyResult());
            assertEquals(1, replay.applyResult().appliedLogIndex());
            assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, conflict);
            assertEquals(1, count(database, "tickets"));
            assertEquals(1, count(database, "request_results"));
            assertEquals("ticket-original", queryString(
                    database,
                    "SELECT ticket_id FROM tickets WHERE status = 'ACTIVE'"));
            assertEquals(first.applyResult(),
                    repository.findResultByRequestId(REQUEST_1).orElseThrow());
            assertEquals(3, repository.lastApplied());
        }
    }

    private static BookingCommand.BookTicket book(
            String requestId, String seatId, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, "Passenger", ticketId, issuedAt);
    }

    private static String fingerprint(BookingCommand.BookTicket book) {
        return BookingStateMachine.computeBookFingerprint(
                book.tripId(), book.seatId(), book.passengerName());
    }

    private static String referenceFingerprint(String... fields)
            throws NoSuchAlgorithmException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        for (String field : fields) {
            byte[] utf8 = field.getBytes(StandardCharsets.UTF_8);
            encoded.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(utf8.length).array());
            encoded.writeBytes(utf8);
        }
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(encoded.toByteArray()));
    }

    private static void assertLowercaseSha256(String value) {
        assertTrue(value.matches("[0-9a-f]{64}"), "not lowercase SHA-256 hex: " + value);
    }

    private static boolean jsonbEquals(Connection connection, String requestId, String json)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT result_json = ?::jsonb FROM request_results WHERE request_id = ?")) {
            statement.setString(1, json);
            statement.setObject(2, UUID.fromString(requestId));
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getBoolean(1);
            }
        }
    }

    private static int jsonObjectLength(Connection connection, String requestId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM jsonb_object_keys("
                        + "(SELECT result_json FROM request_results WHERE request_id = ?))")) {
            statement.setObject(1, UUID.fromString(requestId));
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }
    }

    private static int count(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static int count(TestDatabase database, String table) throws SQLException {
        try (Connection connection = database.connection()) {
            return count(connection, table);
        }
    }

    private static String queryString(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getString(1);
        }
    }

    private static String queryString(TestDatabase database, String sql) throws SQLException {
        try (Connection connection = database.connection()) {
            return queryString(connection, sql);
        }
    }

    private static void insertLog(
            TestDatabase database, long logIndex, BookingCommand.BookTicket book)
            throws SQLException {
        String commandJson = "{\"requestId\":\"" + book.requestId()
                + "\",\"tripId\":\"" + book.tripId()
                + "\",\"seatId\":\"" + book.seatId()
                + "\",\"passengerName\":\"" + book.passengerName()
                + "\",\"ticketId\":\"" + book.ticketId()
                + "\",\"issuedAt\":\"" + book.issuedAt() + "\"}";
        try (Connection connection = database.connection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (?, 1, ?, ?::jsonb)")) {
            statement.setLong(1, logIndex);
            statement.setString(2, book.type().name());
            statement.setString(3, commandJson);
            statement.executeUpdate();
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value;
    }

    private static final class TestDatabase implements AutoCloseable {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final ConnectionFactory connectionFactory;

        private TestDatabase(
                String jdbcUrl,
                String username,
                String password,
                String schema,
                ConnectionFactory connectionFactory) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = schema;
            this.connectionFactory = connectionFactory;
        }

        private static TestDatabase open() throws Exception {
            String jdbcUrl = requiredEnvironment("RAILWAY_TEST_JDBC_URL");
            String username = requiredEnvironment("RAILWAY_TEST_DB_USER");
            String password = requiredEnvironment("RAILWAY_TEST_DB_PASSWORD");
            Assumptions.assumeTrue(
                    jdbcUrl != null && username != null && password != null,
                    "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and "
                            + "RAILWAY_TEST_DB_PASSWORD are required");

            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            String databaseName = uri.getPath().substring(1);
            int port = uri.getPort() < 0 ? 5432 : uri.getPort();
            DbConfig config = new DbConfig(
                    uri.getHost(), port, databaseName, username, password);
            ConnectionFactory factory = new ConnectionFactory(config);
            String schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");

            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                    Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + schema + "\"");
            }

            TestDatabase database =
                    new TestDatabase(jdbcUrl, username, password, schema, factory);
            try {
                database.runMigration(readMigration("V1__schema.sql"));
                database.runMigration(readMigration("V2__seed.sql"));
                return database;
            } catch (Exception exception) {
                try {
                    database.close();
                } catch (SQLException cleanupFailure) {
                    exception.addSuppressed(cleanupFailure);
                }
                throw exception;
            }
        }

        private Connection connection() throws SQLException {
            Connection connection = connectionFactory.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET search_path TO \"" + schema + "\"");
                return connection;
            } catch (SQLException exception) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
                throw exception;
            }
        }

        private Connection connectionUnchecked() {
            try {
                return connection();
            } catch (SQLException exception) {
                throw new IllegalStateException("Could not open test schema connection", exception);
            }
        }

        private void runMigration(String migration) throws SQLException {
            try (Connection connection = connection();
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

        @Override
        public void close() throws SQLException {
            try (Connection connection =
                            DriverManager.getConnection(jdbcUrl, username, password);
                    Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }
    }
}
