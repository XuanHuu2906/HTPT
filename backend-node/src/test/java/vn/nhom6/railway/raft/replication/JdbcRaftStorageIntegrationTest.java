package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcRaftStorageIntegrationTest {

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
            connectionFactory = new ConnectionFactory(dbConfig());
            runMigration(readMigration("V1__schema.sql"));
            runMigration(readMigration("V2__seed.sql"));
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
    void appendsThreeEntriesAndRecoversThemThroughFreshAdapterAndStorageInstances() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(2L, "node-1");
        RaftLogRepositoryAdapter firstAdapter = new RaftLogRepositoryAdapter(storage);
        firstAdapter.recover();
        List<RaftLogEntry> expected = List.of(
                RaftLogEntry.noOp(1L, 1L), RaftLogEntry.noOp(2L, 1L), RaftLogEntry.noOp(3L, 2L));

        for (RaftLogEntry entry : expected) {
            firstAdapter.appendLocal(entry);
        }
        RaftLogRepositoryAdapter restartedAdapter = new RaftLogRepositoryAdapter(storage(null));
        PersistentState recovered = restartedAdapter.recover();

        assertEquals(expected, recovered.log());
        assertEquals(expected, restartedAdapter.entries());
    }

    @Test
    void reloadsPersistedTermAndVote() throws Exception {
        storage(null).saveTermAndVote(4L, "node-3");

        PersistentState recovered = storage(null).load();

        assertEquals(4L, recovered.currentTerm());
        assertEquals("node-3", recovered.votedFor());
    }

    @Test
    void storageFailureDoesNotReturnAcceptedOrAdvanceVisibleOrDurableLog() throws Exception {
        AtomicBoolean failConnection = new AtomicBoolean();
        JdbcRaftStorage storage = storage(failConnection);
        storage.saveTermAndVote(1L, "node-1");
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        failConnection.set(true);

        RaftNode.AppendOutcome outcome = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_NO_OP, "{}", 1L);

        assertInstanceOf(RaftNode.StorageFailure.class, outcome);
        assertEquals(0L, appender.lastIndex());
        failConnection.set(false);
        assertTrue(storage(null).load().log().isEmpty());
    }

    @Test
    void truncatesOnlyUncommittedSuffixAndPersistsReplacement() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(2L, "node-1");
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);
        adapter.recover();
        RaftLogEntry first = RaftLogEntry.noOp(1L, 1L);
        adapter.appendLocal(first);
        adapter.appendLocal(RaftLogEntry.noOp(2L, 1L));
        adapter.appendLocal(RaftLogEntry.noOp(3L, 1L));
        List<RaftLogEntry> replacement = List.of(
                RaftLogEntry.noOp(2L, 2L), RaftLogEntry.noOp(3L, 2L));

        LogConsistencyChecker.ReconcileResult result =
                adapter.appendEntries(1L, 1L, replacement, 0L);
        PersistentState recovered = storage(null).load();

        assertTrue(result.accepted());
        assertEquals(List.of(first, replacement.get(0), replacement.get(1)), recovered.log());
    }

    @Test
    void refusesToTruncatePersistedAppliedPrefixAndLeavesDatabaseUnchanged() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(2L, "node-1");
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);
        adapter.recover();
        List<RaftLogEntry> original = List.of(RaftLogEntry.noOp(1L, 1L), RaftLogEntry.noOp(2L, 1L));
        original.forEach(entry -> append(adapter, entry));
        try (Connection connection = schemaConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE raft_apply_progress SET last_applied = 1 WHERE singleton = 1");
        }

        assertThrows(RaftStorageException.class, () -> adapter.appendEntries(
                0L, 0L, List.of(RaftLogEntry.noOp(1L, 2L)), 0L));
        PersistentState recovered = storage(null).load();

        assertEquals(original, recovered.log());
        assertEquals(1L, recovered.lastApplied());
    }

    @Test
    void recoveryFailsClosedForCheckpointBeyondLogAndForLogGap() throws Exception {
        JdbcRaftStorage storage = storage(null);
        storage.saveTermAndVote(2L, "node-1");
        storage.replaceSuffix(1L, List.of(RaftLogEntry.noOp(1L, 1L)));
        try (Connection connection = schemaConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE raft_apply_progress SET last_applied = 2 WHERE singleton = 1");
        }

        assertThrows(RaftStorageException.class, () -> new RaftLogRepositoryAdapter(storage(null)).recover());
        try (Connection connection = schemaConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1");
        }
        try (Connection connection = schemaConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                + "VALUES (3, 2, 'NO_OP', '{}'::jsonb)")) {
            statement.executeUpdate();
        }

        assertThrows(RaftStorageException.class, () -> new RaftLogRepositoryAdapter(storage(null)).recover());
    }

    @Test
    void followerSuccessResponseIsSentOnlyAfterJdbcEntryCanBeReloaded() throws Exception {
        JdbcRaftStorage storage = storage(null);
        RaftState state = new RaftState();
        PersistentElectionState electionState = new PersistentElectionState(state, storage);
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        AppendEntriesHandler handler = new AppendEntriesHandler(
                electionState, appender, new LogConflictResolver(appender), new AppendEntriesHandler.TimerHook() {
                    @Override
                    public void onValidAppendEntries(long requestTerm) {
                    }

                    @Override
                    public void start() {
                    }

                    @Override
                    public void close() {
                    }
                });
        RaftLogEntry entry = RaftLogEntry.noOp(1L, 1L);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, "node-2", 0L, 0L, List.of(entry), 0L);

        RaftNode.AppendEntriesResponse response = handler.handle(request, "node-2");
        PersistentState recovered = storage(null).load();

        assertTrue(response.success());
        assertEquals(1L, response.matchIndex());
        assertEquals(List.of(entry), recovered.log());
        assertEquals(1L, recovered.currentTerm());
    }

    private static void append(RaftLogRepositoryAdapter adapter, RaftLogEntry entry) {
        try {
            adapter.appendLocal(entry);
        } catch (RaftStorageException failure) {
            throw new IllegalStateException("test setup could not persist Raft entry", failure);
        }
    }

    private JdbcRaftStorage storage(AtomicBoolean failConnection) {
        return new JdbcRaftStorage(() -> {
            if (failConnection != null && failConnection.get()) {
                throw new SQLException("Injected connection failure");
            }
            return schemaConnection();
        });
    }

    private Connection schemaConnection() throws SQLException {
        Connection connection = connectionFactory.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO \"" + schema + "\"");
            return connection;
        } catch (SQLException failure) {
            connection.close();
            throw failure;
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
}
