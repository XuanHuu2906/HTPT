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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcRaftStorageTest {

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
    void test01_restartTermAndVotedFor_persistedAndReloadedAcrossStorageInstances() throws Exception {
        JdbcRaftStorage storage1 = storage(null);
        storage1.saveTermAndVote(1, "node-2");

        // Create a new instance representing node restart
        JdbcRaftStorage storage2 = storage(null);
        PersistentState state = storage2.load();

        assertEquals(1L, state.currentTerm());
        assertEquals("node-2", state.votedFor());
        assertEquals(0L, state.lastApplied());
        assertTrue(state.log().isEmpty());
    }

    @Test
    void test02_termMustNotDecrease_rejectedAndPreviousValuePreserved() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(2, "node-1");

        // Attempting to save smaller term must be rejected
        assertThrows(IllegalArgumentException.class, () -> storage.saveTermAndVote(1, "node-3"));

        PersistentState state = storage.load();
        assertEquals(2L, state.currentTerm());
        assertEquals("node-1", state.votedFor());
    }

    @Test
    void test03_newTermResetsVotedForAtomically() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(1, "node-1");

        // Step up to term 2 and clear vote atomically
        storage.saveTermAndVote(2, null);

        PersistentState state = storage.load();
        assertEquals(2L, state.currentTerm());
        assertNull(state.votedFor(), "votedFor must be null when moving to a new term");
    }

    @Test
    void test04_appendEntriesContiguously_readBackCorrectlyIncludingNoOp() throws Exception {
        JdbcRaftStorage storage = storage(null);
        RaftLogEntry entry1 = new RaftLogEntry(1, 1, "BOOK_TICKET",
                "{\"requestId\":\"11111111-1111-1111-1111-111111111111\",\"seatId\":\"G01\"}");
        RaftLogEntry entry2 = new RaftLogEntry(2, 1, "CANCEL_TICKET",
                "{\"requestId\":\"22222222-2222-2222-2222-222222222222\",\"ticketId\":\"t-1\"}");
        RaftLogEntry entry3 = RaftLogEntry.noOp(3, 2);

        storage.replaceSuffix(1, List.of(entry1, entry2, entry3));

        PersistentState state = storage.load();
        assertEquals(3, state.log().size());

        assertEquals(1L, state.log().get(0).index());
        assertEquals(1L, state.log().get(0).term());
        assertEquals("BOOK_TICKET", state.log().get(0).commandType());
        assertTrue(state.log().get(0).commandPayload().contains("11111111-1111-1111-1111-111111111111"));

        assertEquals(2L, state.log().get(1).index());
        assertEquals(1L, state.log().get(1).term());
        assertEquals("CANCEL_TICKET", state.log().get(1).commandType());

        assertEquals(3L, state.log().get(2).index());
        assertEquals(2L, state.log().get(2).term());
        assertEquals("NO_OP", state.log().get(2).commandType());
        assertEquals("{}", state.log().get(2).commandPayload());
    }

    @Test
    void test05_appendWithGapOrDecreasingTerm_rejectedAndDbUnchanged() throws Exception {
        JdbcRaftStorage storage = storage(null);

        // Gap: start at index 2 when log is empty
        RaftLogEntry gapEntry = new RaftLogEntry(2, 1, "NO_OP", "{}");
        assertThrows(IllegalArgumentException.class, () -> storage.replaceSuffix(2, List.of(gapEntry)));
        assertEquals(0, count("raft_log"));

        // Batch with internal gap: index 1 followed by index 3
        RaftLogEntry e1 = new RaftLogEntry(1, 1, "NO_OP", "{}");
        RaftLogEntry e3 = new RaftLogEntry(3, 1, "NO_OP", "{}");
        assertThrows(IllegalArgumentException.class, () -> storage.replaceSuffix(1, List.of(e1, e3)));
        assertEquals(0, count("raft_log"));

        // Batch with decreasing term: term 3 followed by term 2
        RaftLogEntry dec1 = new RaftLogEntry(1, 3, "NO_OP", "{}");
        RaftLogEntry dec2 = new RaftLogEntry(2, 2, "NO_OP", "{}");
        assertThrows(IllegalArgumentException.class, () -> storage.replaceSuffix(1, List.of(dec1, dec2)));
        assertEquals(0, count("raft_log"));
    }

    @Test
    void test06_faultInjectionMidBatch_atomicRollback_thenSuccessAfterFaultCleared() throws Exception {
        AtomicReference<SqlFault> faultRef = new AtomicReference<>(
                new SqlFault("INSERT INTO raft_log", false));
        JdbcRaftStorage faultStorage = storage(faultRef);

        RaftLogEntry e1 = new RaftLogEntry(1, 1, "NO_OP", "{}");
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "NO_OP", "{}");
        RaftLogEntry e3 = new RaftLogEntry(3, 1, "NO_OP", "{}");

        // Execution should fail and roll back completely
        assertThrows(RaftStorageException.class, () -> faultStorage.replaceSuffix(1, List.of(e1, e2, e3)));
        assertTrue(faultRef.get().triggered().get(), "Fault must have been triggered");
        assertEquals(0, count("raft_log"), "All batch entries must be rolled back on failure");

        // Clear fault and retry
        faultRef.set(null);
        faultStorage.replaceSuffix(1, List.of(e1, e2, e3));
        assertEquals(3, count("raft_log"));
    }

    @Test
    void test07_conflictTruncatesUncommittedSuffix_truncatingCommittedIndexFailsClosed() throws Exception {
        JdbcRaftStorage storage = storage(null);

        // Seed entries 1, 2, 3 with term 1
        RaftLogEntry e1 = new RaftLogEntry(1, 1, "BOOK_TICKET", "{\"seatId\":\"G01\"}");
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "BOOK_TICKET", "{\"seatId\":\"G02\"}");
        RaftLogEntry e3 = new RaftLogEntry(3, 1, "BOOK_TICKET", "{\"seatId\":\"G03\"}");
        storage.replaceSuffix(1, List.of(e1, e2, e3));

        // Leader sends replacement from index 2 with term 2
        RaftLogEntry e2New = new RaftLogEntry(2, 2, "BOOK_TICKET", "{\"seatId\":\"G02\"}");
        RaftLogEntry e3New = new RaftLogEntry(3, 2, "BOOK_TICKET", "{\"seatId\":\"G03\"}");
        RaftLogEntry e4New = new RaftLogEntry(4, 2, "BOOK_TICKET", "{\"seatId\":\"G04\"}");
        storage.replaceSuffix(2, List.of(e2New, e3New, e4New));

        PersistentState state = storage.load();
        assertEquals(4, state.log().size());
        assertEquals(1L, state.log().get(0).term());
        assertEquals(2L, state.log().get(1).term());
        assertEquals(2L, state.log().get(2).term());
        assertEquals(2L, state.log().get(3).term());

        // Advance last_applied to 2 in DB (simulating state-machine apply)
        try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE raft_apply_progress SET last_applied = 2 WHERE singleton = 1");
        }

        // Attempting to truncate or overwrite at or before last_applied (index <= 2) must fail closed
        RaftLogEntry conflictCommitted = new RaftLogEntry(2, 3, "NO_OP", "{}");
        assertThrows(RaftStorageException.class, () -> storage.replaceSuffix(2, List.of(conflictCommitted)));
        assertThrows(RaftStorageException.class, () -> storage.replaceSuffix(1, List.of(e1)));

        // Verify data was not modified
        PersistentState afterAttempt = storage.load();
        assertEquals(4, afterAttempt.log().size());
        assertEquals(2L, afterAttempt.lastApplied());
    }

    @Test
    void test08_sameIndexAndTermWithDifferentCommand_failsClosed() throws Exception {
        JdbcRaftStorage storage = storage(null);
        RaftLogEntry e1 = new RaftLogEntry(1, 1, "BOOK_TICKET", "{\"seatId\":\"G01\"}");
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "BOOK_TICKET", "{\"action\":\"A\"}");
        storage.replaceSuffix(1, List.of(e1, e2));

        // Same index 2 and term 1, but conflicting command payload
        RaftLogEntry e2Conflict = new RaftLogEntry(2, 1, "BOOK_TICKET", "{\"action\":\"B\"}");
        assertThrows(RaftStorageException.class, () -> storage.replaceSuffix(2, List.of(e2Conflict)));

        // Old entry remains unchanged
        PersistentState state = storage.load();
        assertEquals(2, state.log().size());
        assertTrue(state.log().get(1).commandPayload().contains("\"A\""));
        assertTrue(!state.log().get(1).commandPayload().contains("\"B\""));
    }

    @Test
    void test09_persistBeforeAck_visibleToOtherConnectionImmediately_errorDoesNotPersist() throws Exception {
        JdbcRaftStorage storage = storage(null);

        // 1. Term/vote write
        storage.saveTermAndVote(5, "node-3");

        // Immediately read from a fresh, independent JDBC connection
        try (Connection otherConn = schemaConnection();
             PreparedStatement stmt = otherConn.prepareStatement("SELECT current_term, voted_for FROM raft_meta WHERE singleton = 1");
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(5L, rs.getLong("current_term"));
            assertEquals("node-3", rs.getString("voted_for"));
        }

        // 2. Log entry write
        RaftLogEntry e1 = new RaftLogEntry(1, 1, "NO_OP", "{}");
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "NO_OP", "{}");
        storage.replaceSuffix(1, List.of(e1, e2));

        try (Connection otherConn = schemaConnection();
             PreparedStatement stmt = otherConn.prepareStatement("SELECT COUNT(*) FROM raft_log");
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }

        // 3. Inject fault during saveTermAndVote: method throws, no new state visible
        AtomicReference<SqlFault> fault = new AtomicReference<>(new SqlFault("UPDATE raft_meta", false));
        JdbcRaftStorage faultyStorage = storage(fault);
        assertThrows(RaftStorageException.class, () -> faultyStorage.saveTermAndVote(6, "node-4"));

        try (Connection otherConn = schemaConnection();
             PreparedStatement stmt = otherConn.prepareStatement("SELECT current_term, voted_for FROM raft_meta WHERE singleton = 1");
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(5L, rs.getLong("current_term"), "Term must still be 5 on rollback");
            assertEquals("node-3", rs.getString("voted_for"));
        }
    }

    @Test
    void test10_loadFailsClosed_whenLastAppliedExceedsLogOrLogHasGap() throws Exception {
        JdbcRaftStorage storage = storage(null);
        RaftLogEntry e1 = new RaftLogEntry(1, 1, "NO_OP", "{}");
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "NO_OP", "{}");
        storage.replaceSuffix(1, List.of(e1, e2));

        // Case A: last_applied > lastLogIndex
        try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE raft_apply_progress SET last_applied = 5 WHERE singleton = 1");
        }
        assertThrows(RaftStorageException.class, storage::load);

        // Reset last_applied to 0
        try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1");
        }

        // Case B: Gap in raft_log (add index 4 while max was 2, missing 3)
        try (Connection conn = schemaConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (4, 1, 'NO_OP', '{}'::jsonb)")) {
            stmt.executeUpdate();
        }
        assertThrows(RaftStorageException.class, storage::load);
    }

    // ---------------------------------------------------------------------------------------
    // Test helpers & fixtures
    // ---------------------------------------------------------------------------------------

    private JdbcRaftStorage storage(AtomicReference<SqlFault> fault) {
        return new JdbcRaftStorage(() -> {
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

    private static Connection wrapConnection(Connection connection, AtomicReference<SqlFault> fault) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> {
                    if ("prepareStatement".equals(method.getName())
                            && arguments != null && arguments.length > 0
                            && arguments[0] instanceof String sql) {
                        PreparedStatement statement = (PreparedStatement) invoke(connection, method, arguments);
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
                            || "execute".equals(method.getName())
                            || "executeBatch".equals(method.getName());
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

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private int count(String table) {
        try (Connection connection = schemaConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
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

    private record SqlFault(String sqlPattern, boolean afterExecution, AtomicBoolean triggered) {
        private SqlFault(String sqlPattern, boolean afterExecution) {
            this(sqlPattern, afterExecution, new AtomicBoolean());
        }

        private boolean matches(String sql) {
            return sql.toUpperCase(java.util.Locale.ROOT)
                    .contains(sqlPattern.toUpperCase(java.util.Locale.ROOT));
        }
    }
}

