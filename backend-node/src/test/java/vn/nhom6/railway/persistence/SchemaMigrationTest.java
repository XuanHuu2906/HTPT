package vn.nhom6.railway.persistence;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaMigrationTest {
    private static final String V1_MIGRATION = "V1__schema.sql";
    private static final String V2_MIGRATION = "V2__seed.sql";

    @Test
    void migrationsContainRequiredSchemaAndSeed() throws IOException {
        String v1Sql = Files.readString(findMigration(V1_MIGRATION), StandardCharsets.UTF_8);
        String v2Sql = Files.readString(findMigration(V2_MIGRATION), StandardCharsets.UTF_8);
        String normalized = normalizeSql(v1Sql);
        String normalizedV2 = normalizeSql(v2Sql);

        Matcher tableMatcher = Pattern.compile("\\bCREATE\\s+TABLE\\s+([A-Z_]+)\\s*\\(")
                .matcher(normalized);
        Set<String> tables = new HashSet<>();
        while (tableMatcher.find()) {
            tables.add(tableMatcher.group(1));
        }
        assertEquals(Set.of(
                "RAFT_META", "RAFT_LOG", "RAFT_APPLY_PROGRESS", "TRIPS",
                "SEATS", "TICKETS", "REQUEST_RESULTS", "BUSINESS_EVENTS"
        ), tables);

        assertTrue(Pattern.compile(
                "\\bCREATE\\s+UNIQUE\\s+INDEX\\s+UX_ONE_ACTIVE_TICKET_PER_SEAT\\s+" +
                        "ON\\s+TICKETS\\s*\\(\\s*TRIP_ID\\s*,\\s*SEAT_ID\\s*\\)\\s*" +
                        "WHERE\\s+STATUS\\s*=\\s*'ACTIVE'(?:\\s|;|$)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bCHECK\\s*\\(\\s*SEAT_ID\\s*~\\s*'\\^G\\(0\\[1-9\\]\\|10\\)\\$'\\s*\\)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bSTATUS\\s+VARCHAR\\s*\\(\\s*12\\s*\\)\\s+NOT\\s+NULL\\s+" +
                        "CHECK\\s*\\(\\s*STATUS\\s+IN\\s*\\(\\s*'AVAILABLE'\\s*,\\s*'BOOKED'\\s*\\)\\s*\\)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bSTATUS\\s+VARCHAR\\s*\\(\\s*12\\s*\\)\\s+NOT\\s+NULL\\s+" +
                        "CHECK\\s*\\(\\s*STATUS\\s+IN\\s*\\(\\s*'ACTIVE'\\s*,\\s*'CANCELLED'\\s*\\)\\s*\\)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bCOMMAND_TYPE\\s+VARCHAR\\s*\\(\\s*32\\s*\\)\\s+NOT\\s+NULL\\s+" +
                        "CHECK\\s*\\(\\s*COMMAND_TYPE\\s+IN\\s*\\(\\s*" +
                        "'BOOK_TICKET'\\s*,\\s*'CANCEL_TICKET'\\s*,\\s*'NO_OP'\\s*\\)\\s*\\)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bSTATE\\s+VARCHAR\\s*\\(\\s*20\\s*\\)\\s+NOT\\s+NULL\\s+" +
                        "CHECK\\s*\\(\\s*STATE\\s+IN\\s*\\(\\s*" +
                        "'APPLIED_SUCCESS'\\s*,\\s*'APPLIED_REJECTED'\\s*\\)\\s*\\)")
                .matcher(normalized).find());
        assertTrue(Pattern.compile(
                "\\bREQUEST_ID\\s+UUID\\s+PRIMARY\\s+KEY")
                .matcher(normalized).find());
        assertFalse(Pattern.compile("\\bINSERT\\s+INTO\\s+(?:TRIPS|SEATS)\\b")
                .matcher(normalized).find(), "V1 must not seed trips or seats");
        assertTrue(Pattern.compile(
                "\\bINSERT\\s+INTO\\s+TRIPS\\s*\\(\\s*TRIP_ID\\s*\\)\\s*" +
                        "VALUES\\s*\\(\\s*'SE-001'\\s*\\)")
                .matcher(normalizedV2).find());
        assertTrue(Pattern.compile(
                "\\bINSERT\\s+INTO\\s+SEATS\\s*\\(\\s*TRIP_ID\\s*,\\s*SEAT_ID\\s*,\\s*" +
                        "STATUS\\s*,\\s*CURRENT_TICKET_ID\\s*\\)\\s*" +
                        "SELECT\\s+'SE-001'\\s*,\\s*'G'\\s*\\|\\|\\s*LPAD\\(N::TEXT\\s*,\\s*2\\s*,\\s*'0'\\)\\s*,\\s*" +
                        "'AVAILABLE'\\s*,\\s*NULL\\s+FROM\\s+GENERATE_SERIES\\(1\\s*,\\s*10\\)\\s+AS\\s+N")
                .matcher(normalizedV2).find());
        assertTrue(Pattern.compile(
                "(?im)^\\s*--[^\\r\\n]*không chạy/reset riêng một Node đang tham gia consensus")
                .matcher(v2Sql).find());
        assertFalse(Pattern.compile("\\b(?:BIGSERIAL|SERIAL)\\b")
                .matcher(normalizedV2).find());
        assertFalse(Pattern.compile("\\b(?:BIGSERIAL|SERIAL)\\b")
                .matcher(normalized).find());
    }

    @Test
    void migrationWorksAgainstIsolatedPostgresSchema() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        String v1Migration = Files.readString(findMigration(V1_MIGRATION), StandardCharsets.UTF_8);
        String v2Migration = Files.readString(findMigration(V2_MIGRATION), StandardCharsets.UTF_8);
        String schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            boolean schemaCreated = false;
            try {
                execute(connection, "CREATE SCHEMA \"" + schema + "\"");
                schemaCreated = true;
                execute(connection, "SET search_path TO \"" + schema + "\"");
                executeMigration(connection, v1Migration);
                assertEquals(8, queryInt(connection,
                        "SELECT COUNT(*) FROM information_schema.tables " +
                                "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"));
                assertEquals(0, queryInt(connection, "SELECT COUNT(*) FROM trips"));
                assertEquals(0, queryInt(connection, "SELECT COUNT(*) FROM seats"));
                assertEquals(1, queryInt(connection,
                        "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() " +
                                "AND tablename = 'tickets' AND indexname = 'ux_one_active_ticket_per_seat'"));
                assertEquals(1, queryInt(connection,
                        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'request_results'::regclass " +
                                "AND contype = 'p'"));
                assertEquals(0, queryLong(connection,
                        "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
                assertEquals(0, queryLong(connection,
                        "SELECT current_term FROM raft_meta WHERE singleton = 1"));
                assertEquals(1, queryInt(connection,
                        "SELECT COUNT(*) FROM raft_meta WHERE singleton = 1 AND voted_for IS NULL"));
                assertEquals(1, queryInt(connection,
                        "SELECT COUNT(*) FROM raft_apply_progress WHERE singleton = 1"));

                executeMigration(connection, v2Migration);
                assertEquals(1, queryInt(connection,
                        "SELECT COUNT(*) FROM trips WHERE trip_id = 'SE-001'"));
                assertEquals(10, queryInt(connection,
                        "SELECT COUNT(*) FROM seats WHERE trip_id = 'SE-001'"));
                assertEquals(10, queryInt(connection,
                        "SELECT COUNT(DISTINCT seat_id) FROM seats WHERE trip_id = 'SE-001'"));
                assertEquals(10, queryInt(connection,
                        "SELECT COUNT(*) FROM seats WHERE trip_id = 'SE-001' " +
                                "AND status = 'AVAILABLE' AND current_ticket_id IS NULL"));
                assertEquals(expectedSeatIds(), queryStringSet(connection,
                        "SELECT seat_id FROM seats WHERE trip_id = 'SE-001'"));
                assertThrows(SQLException.class, () -> executeMigration(connection, v2Migration),
                        "Running V2 a second time must fail rather than mask duplicate seed data");

                execute(connection, """
                        INSERT INTO tickets(ticket_id, trip_id, seat_id, passenger_name, status, issued_at)
                        VALUES ('ticket-active', 'SE-001', 'G01', 'Test Passenger', 'ACTIVE', CURRENT_TIMESTAMP)
                        """);
                assertRejected(connection, """
                        INSERT INTO tickets(ticket_id, trip_id, seat_id, passenger_name, status, issued_at)
                        VALUES ('ticket-active-duplicate', 'SE-001', 'G01', 'Test Passenger', 'ACTIVE', CURRENT_TIMESTAMP)
                        """);
                execute(connection, """
                        INSERT INTO tickets(ticket_id, trip_id, seat_id, passenger_name, status, issued_at)
                        VALUES ('ticket-cancelled', 'SE-001', 'G01', 'Test Passenger', 'CANCELLED', CURRENT_TIMESTAMP)
                        """);

                assertRejected(connection, """
                        INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
                        VALUES ('SE-001', 'G11', 'AVAILABLE', NULL)
                        """);
                assertRejected(connection, """
                        INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
                        VALUES ('SE-001', 'G00', 'AVAILABLE', NULL)
                        """);
                assertRejected(connection, """
                        INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
                        VALUES ('SE-001', 'G02', 'BOOKED', NULL)
                        """);
                assertRejected(connection, """
                        INSERT INTO seats(trip_id, seat_id, status, current_ticket_id)
                        VALUES ('SE-001', 'G03', 'AVAILABLE', 'ticket-active')
                        """);

                assertRejected(connection,
                        "INSERT INTO raft_meta(singleton, current_term, voted_for) VALUES (2, 0, NULL)");
                assertRejected(connection,
                        "INSERT INTO raft_meta(singleton, current_term, voted_for) VALUES (1, 0, NULL)");
                assertRejected(connection,
                        "INSERT INTO raft_apply_progress(singleton, last_applied) VALUES (2, 0)");
                assertRejected(connection,
                        "INSERT INTO raft_apply_progress(singleton, last_applied) VALUES (1, 0)");

                assertRejected(connection, """
                        INSERT INTO raft_log(log_index, term, command_type, command_json)
                        VALUES (1, 1, 'UNKNOWN', '{}'::jsonb)
                        """);
                assertRejected(connection, """
                        INSERT INTO raft_log(log_index, term, command_type, command_json)
                        VALUES (0, 1, 'NO_OP', '{}'::jsonb)
                        """);

                String requestId = UUID.randomUUID().toString();
                assertRejected(connection, requestResultInsert(requestId, "UNKNOWN", 1));
                assertRejected(connection, requestResultInsert(UUID.randomUUID().toString(),
                        "APPLIED_SUCCESS", 0));
                execute(connection, requestResultInsert(requestId, "APPLIED_SUCCESS", 1));
                assertRejected(connection, requestResultInsert(requestId, "APPLIED_SUCCESS", 1));

                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("""
                             SELECT trip_id, seat_id, COUNT(*) AS active_count
                             FROM tickets WHERE status = 'ACTIVE'
                             GROUP BY trip_id, seat_id HAVING COUNT(*) > 1
                             """)) {
                    assertFalse(resultSet.next(), "Found duplicate ACTIVE tickets for a seat");
                }
            } finally {
                if (schemaCreated) {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute("DROP SCHEMA \"" + schema + "\" CASCADE");
                    }
                }
            }
        }
    }

    private static Path findMigration(String migrationName) throws IOException {
        Path localPath = Path.of("database", "migration", migrationName);
        Path parentPath = Path.of("..", "database", "migration", migrationName);
        if (Files.isRegularFile(localPath)) {
            return localPath;
        }
        if (Files.isRegularFile(parentPath)) {
            return parentPath;
        }
        throw new IOException("Migration not found at " + localPath.toAbsolutePath()
                + " or " + parentPath.toAbsolutePath());
    }

    private static void executeMigration(Connection connection, String migration) throws SQLException {
        for (String command : migration.split(";")) {
            if (!command.isBlank()) {
                execute(connection, command);
            }
        }
    }

    private static String normalizeSql(String sql) {
        return sql.replaceAll("--[^\\r\\n]*", " ")
                .replaceAll("\\s+", " ")
                .trim()
                .toUpperCase(Locale.ROOT);
    }

    private static Set<String> expectedSeatIds() {
        Set<String> expectedSeats = new HashSet<>();
        for (int seatNumber = 1; seatNumber <= 10; seatNumber++) {
            expectedSeats.add(String.format(Locale.ROOT, "G%02d", seatNumber));
        }
        return expectedSeats;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void assertRejected(Connection connection, String sql) {
        assertThrows(SQLException.class, () -> execute(connection, sql), sql);
    }

    private static int queryInt(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Query returned no rows: " + sql);
            return resultSet.getInt(1);
        }
    }

    private static long queryLong(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Query returned no rows: " + sql);
            return resultSet.getLong(1);
        }
    }

    private static Set<String> queryStringSet(Connection connection, String sql) throws SQLException {
        Set<String> values = new HashSet<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                values.add(resultSet.getString(1));
            }
        }
        return values;
    }

    private static String requestResultInsert(String requestId, String state, long appliedLogIndex) {
        return """
                INSERT INTO request_results(
                  request_id, command_type, intent_fingerprint, state, applied_log_index, result_json
                )
                VALUES ('%s', 'BOOK_TICKET', '%s', '%s', %d, '{}'::jsonb)
                """.formatted(requestId, "0".repeat(64), state, appliedLogIndex);
    }
}
