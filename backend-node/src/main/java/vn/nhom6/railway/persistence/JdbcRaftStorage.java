package vn.nhom6.railway.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;

/**
 * PostgreSQL JDBC implementation of {@link RaftStorage} for a single node (SPEC-04 & SPEC-05 §4).
 *
 * <p><strong>Key Principles:</strong>
 * <ul>
 *   <li><strong>Persist before ACK:</strong> All state mutations (term, vote, log suffix) are
 *       committed to the database before the respective method returns. If persistence fails,
 *       a {@link RaftStorageException} is thrown, and the caller must not acknowledge the RPC.</li>
 *   <li><strong>Atomic term and vote:</strong> {@code current_term} and {@code voted_for} in
 *       {@code raft_meta} are updated in a single atomic transaction. The term must never decrease.
 *       Moving to a new term resets {@code voted_for} atomically in the same operation.</li>
 *   <li><strong>Contiguous log:</strong> {@code raft_log} entries start at index 1 and must be
 *       strictly contiguous without gaps. Entry terms must be &gt;= 1 and non-decreasing.
 *       The command type and JSON payload are preserved verbatim.</li>
 *   <li><strong>Conflict & truncate safety:</strong> Only uncommitted entries (indices &gt;
 *       {@code last_applied}) may be truncated. Attempting to overwrite or truncate an entry at or
 *       before {@code last_applied} fails closed immediately with an exception. Existing entries
 *       sharing the same (index, term) with conflicting command payloads violate the Raft log
 *       matching invariant and fail closed.</li>
 *   <li><strong>Fail-closed recovery:</strong> On startup, {@link #load()} verifies consistency:
 *       if {@code last_applied} exceeds the highest log index, or if the log contains gaps or
 *       decreasing terms, {@link RaftStorageException} is thrown without modifying state.</li>
 * </ul>
 */
public final class JdbcRaftStorage implements RaftStorage {

    private static final Logger LOGGER = Logger.getLogger(JdbcRaftStorage.class.getName());
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();

    @FunctionalInterface
    public interface ConnectionProvider {
        Connection getConnection() throws SQLException;
    }

    private final ConnectionProvider connectionProvider;

    public JdbcRaftStorage(ConnectionFactory connectionFactory) {
        this(Objects.requireNonNull(connectionFactory, "connectionFactory")::getConnection);
    }

    public JdbcRaftStorage(ConnectionProvider connectionProvider) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
    }

    @Override
    public PersistentState load() throws RaftStorageException {
        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 1. Read raft_meta
                final long currentTerm;
                final String votedFor;
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT current_term, voted_for FROM raft_meta WHERE singleton = 1")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new RaftStorageException("Missing raft_meta singleton row");
                        }
                        currentTerm = rs.getLong("current_term");
                        votedFor = rs.getString("voted_for");
                    }
                }

                // 2. Read raft_apply_progress
                final long lastApplied;
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new RaftStorageException("Missing raft_apply_progress singleton row");
                        }
                        lastApplied = rs.getLong("last_applied");
                    }
                }

                if (lastApplied < 0) {
                    throw new RaftStorageException("Inconsistent state: last_applied must be >= 0, was: " + lastApplied);
                }

                // 3. Read raft_log entries in ascending order
                List<RaftLogEntry> entries = new ArrayList<>();
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT log_index, term, command_type, command_json::text FROM raft_log ORDER BY log_index ASC")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        long expectedIndex = 1L;
                        long prevTerm = 1L;
                        while (rs.next()) {
                            long index = rs.getLong(1);
                            long term = rs.getLong(2);
                            String commandType = rs.getString(3);
                            String commandJson = rs.getString(4);

                            if (index != expectedIndex) {
                                throw new RaftStorageException("Raft log has a gap: expected index "
                                        + expectedIndex + " but found " + index);
                            }
                            if (term < 1L) {
                                throw new RaftStorageException("Entry term must be >= 1 at index " + index);
                            }
                            if (term < prevTerm) {
                                throw new RaftStorageException("Entry term decreased at index " + index + ": "
                                        + prevTerm + " -> " + term);
                            }
                            prevTerm = term;
                            expectedIndex = Math.addExact(expectedIndex, 1L);
                            entries.add(new RaftLogEntry(index, term, commandType, commandJson));
                        }
                    }
                }

                // 4. Validate lastApplied <= lastLogIndex
                long lastLogIndex = entries.isEmpty() ? 0L : entries.get(entries.size() - 1).index();
                if (lastApplied > lastLogIndex) {
                    throw new RaftStorageException("Inconsistent checkpoint: last_applied (" + lastApplied
                            + ") exceeds lastLogIndex (" + lastLogIndex + ")");
                }

                connection.commit();
                return new PersistentState(currentTerm, votedFor, entries, lastApplied);
            } catch (SQLException | RaftStorageException ex) {
                rollbackQuietly(connection);
                if (ex instanceof RaftStorageException rse) {
                    throw rse;
                }
                throw logAndWrap("load", (SQLException) ex);
            }
        } catch (SQLException e) {
            throw logAndWrap("getConnection for load", e);
        }
    }

    @Override
    public void saveTermAndVote(long currentTerm, String votedFor) throws RaftStorageException {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must be >= 0, was: " + currentTerm);
        }
        if (votedFor != null && votedFor.length() > 20) {
            throw new IllegalArgumentException("votedFor exceeds maximum length 20: " + votedFor);
        }

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            try {
                // Lock and check current term
                try (PreparedStatement selectStmt = connection.prepareStatement(
                        "SELECT current_term FROM raft_meta WHERE singleton = 1 FOR UPDATE")) {
                    try (ResultSet rs = selectStmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new RaftStorageException("Missing raft_meta singleton row");
                        }
                        long storedTerm = rs.getLong(1);
                        if (currentTerm < storedTerm) {
                            throw new IllegalArgumentException("currentTerm must not decrease: current="
                                    + storedTerm + ", requested=" + currentTerm);
                        }
                    }
                }

                // Update current_term and voted_for atomically
                try (PreparedStatement updateStmt = connection.prepareStatement(
                        "UPDATE raft_meta SET current_term = ?, voted_for = ? WHERE singleton = 1")) {
                    updateStmt.setLong(1, currentTerm);
                    updateStmt.setString(2, votedFor);
                    int updated = updateStmt.executeUpdate();
                    if (updated != 1) {
                        throw new RaftStorageException("Failed to update raft_meta row");
                    }
                }

                connection.commit();
            } catch (SQLException | RuntimeException | RaftStorageException ex) {
                rollbackQuietly(connection);
                if (ex instanceof IllegalArgumentException iae) {
                    throw iae;
                }
                if (ex instanceof RaftStorageException rse) {
                    throw rse;
                }
                throw logAndWrap("saveTermAndVote", (SQLException) ex);
            }
        } catch (SQLException e) {
            throw logAndWrap("getConnection for saveTermAndVote", e);
        }
    }

    @Override
    public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException {
        Objects.requireNonNull(entries, "entries");
        if (fromIndex < RaftLogEntry.FIRST_INDEX) {
            throw new IllegalArgumentException("fromIndex must be >= 1, was: " + fromIndex);
        }

        // Validate entries continuity and term ordering
        long expectedIndex = fromIndex;
        long prevBatchTerm = -1;
        for (RaftLogEntry entry : entries) {
            Objects.requireNonNull(entry, "entries must not contain null elements");
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException("replacement entries must be contiguous starting from "
                        + fromIndex + "; expected index " + expectedIndex + " but was " + entry.index());
            }
            if (entry.term() < RaftLogEntry.FIRST_TERM) {
                throw new IllegalArgumentException("entry term must be >= 1, was: " + entry.term());
            }
            if (prevBatchTerm != -1 && entry.term() < prevBatchTerm) {
                throw new IllegalArgumentException("entry terms must be non-decreasing: "
                        + prevBatchTerm + " -> " + entry.term());
            }
            prevBatchTerm = entry.term();
            expectedIndex = Math.addExact(expectedIndex, 1L);
        }

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 1. Lock and read last_applied
                final long lastApplied;
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1 FOR UPDATE")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new RaftStorageException("Missing raft_apply_progress singleton row");
                        }
                        lastApplied = rs.getLong(1);
                    }
                }

                // Invariant: NEVER truncate or overwrite index <= last_applied
                if (fromIndex <= lastApplied) {
                    LOGGER.log(Level.SEVERE, "Attempted to truncate or overwrite committed log index: fromIndex={0} <= lastApplied={1}",
                            new Object[]{fromIndex, lastApplied});
                    throw new RaftStorageException("Cannot truncate or overwrite committed entry: fromIndex="
                            + fromIndex + " <= lastApplied=" + lastApplied);
                }

                // 2. Check stored lastLogIndex
                long storedLastIndex = 0L;
                try (PreparedStatement stmt = connection.prepareStatement(
                        "SELECT COALESCE(MAX(log_index), 0) FROM raft_log")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            storedLastIndex = rs.getLong(1);
                        }
                    }
                }

                // fromIndex cannot create a gap beyond storedLastIndex + 1
                if (fromIndex > storedLastIndex + 1L) {
                    throw new IllegalArgumentException("fromIndex " + fromIndex
                            + " creates a gap beyond storedLastIndex " + storedLastIndex);
                }

                // 3. Inspect existing entries from fromIndex for log invariant check:
                // "Cùng (index, term) mà command khác là vi phạm invariant: fail closed."
                if (!entries.isEmpty() && fromIndex <= storedLastIndex) {
                    try (PreparedStatement stmt = connection.prepareStatement(
                            "SELECT log_index, term, command_type, command_json::text FROM raft_log WHERE log_index >= ? ORDER BY log_index ASC")) {
                        stmt.setLong(1, fromIndex);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                long existIndex = rs.getLong(1);
                                long existTerm = rs.getLong(2);
                                String existType = rs.getString(3);
                                String existJson = rs.getString(4);

                                int offset = (int) (existIndex - fromIndex);
                                if (offset >= 0 && offset < entries.size()) {
                                    RaftLogEntry newEntry = entries.get(offset);
                                    if (newEntry.term() == existTerm) {
                                        if (!sameCommand(existType, existJson, newEntry.commandType(), newEntry.commandPayload())) {
                                            LOGGER.log(Level.SEVERE,
                                                    "Log matching invariant violation: index {0} has same term {1} but different command!",
                                                    new Object[]{existIndex, existTerm});
                                            throw new RaftStorageException("Log matching invariant violation at index "
                                                    + existIndex + " in term " + existTerm + ": conflicting commands");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // 4. Truncate suffix from fromIndex
                try (PreparedStatement deleteStmt = connection.prepareStatement(
                        "DELETE FROM raft_log WHERE log_index >= ?")) {
                    deleteStmt.setLong(1, fromIndex);
                    deleteStmt.executeUpdate();
                }

                // 5. Insert replacement entries
                if (!entries.isEmpty()) {
                    try (PreparedStatement insertStmt = connection.prepareStatement(
                            "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, ?, ?, ?::jsonb)")) {
                        for (RaftLogEntry entry : entries) {
                            insertStmt.setLong(1, entry.index());
                            insertStmt.setLong(2, entry.term());
                            insertStmt.setString(3, entry.commandType());
                            insertStmt.setString(4, entry.commandPayload());
                            insertStmt.addBatch();
                        }
                        insertStmt.executeBatch();
                    }
                }

                // 6. Commit before returning (Persist before ACK)
                connection.commit();
            } catch (SQLException | RuntimeException | RaftStorageException ex) {
                rollbackQuietly(connection);
                if (ex instanceof IllegalArgumentException iae) {
                    throw iae;
                }
                if (ex instanceof RaftStorageException rse) {
                    throw rse;
                }
                throw logAndWrap("replaceSuffix", (SQLException) ex);
            }
        } catch (SQLException e) {
            throw logAndWrap("getConnection for replaceSuffix", e);
        }
    }

    @Override
    public long loadLastApplied() throws RaftStorageException {
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement stmt = connection.prepareStatement(
                     "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1")) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new RaftStorageException("Missing raft_apply_progress singleton row");
                }
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw logAndWrap("loadLastApplied", e);
        }
    }

    private static boolean sameCommand(String typeA, String jsonA, String typeB, String jsonB) {
        if (!Objects.equals(typeA, typeB)) {
            return false;
        }
        if (Objects.equals(jsonA, jsonB)) {
            return true;
        }
        try {
            JsonNode treeA = MAPPER.readTree(jsonA);
            JsonNode treeB = MAPPER.readTree(jsonB);
            return Objects.equals(treeA, treeB);
        } catch (JsonProcessingException e) {
            return Objects.equals(jsonA, jsonB);
        }
    }

    private static void rollbackQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to rollback connection", e);
            }
        }
    }

    private static RaftStorageException logAndWrap(String operation, SQLException cause) {
        String safeMessage = cause.getMessage() == null ? "unknown"
                : cause.getMessage().replaceAll("password=[^&;\\s]*", "password=***");
        LOGGER.log(Level.SEVERE, "PostgreSQL {0} failed: SQLState={1}, errorCode={2}, message={3}",
                new Object[]{operation, cause.getSQLState(), cause.getErrorCode(), safeMessage});
        return new RaftStorageException("PostgreSQL " + operation + " failed (SQLState="
                + cause.getSQLState() + ", errorCode=" + cause.getErrorCode() + ")", cause);
    }
}

