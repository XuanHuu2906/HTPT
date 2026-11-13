package vn.nhom6.railway.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * Loads and verifies durable database state at Node startup prior to Raft execution (SPEC-04 §6,
 * SPEC-05 §4.1, SPEC-08 §7).
 *
 * <p><strong>Key Invariants & Responsibilities:</strong>
 * <ul>
 *   <li><strong>Read-only:</strong> Never executes migrations, seed inserts, or resets. State is
 *       classified into {@link BootstrapType#GENESIS} or {@link BootstrapType#RESTART}. Missing
 *       schema triggers a fail-closed exception ("SCHEMA_MISSING").</li>
 *   <li><strong>Initial Commit Index:</strong> {@code commitIndex = lastApplied} (SPEC-04 §6).
 *       It is never inferred from {@code lastLogIndex}.</li>
 *   <li><strong>Fail-closed verification:</strong> Throws {@link BookingRepository.StorageException}
 *       if any Raft log continuity, term ordering, checkpoint, cluster voter, or business data
 *       inconsistency is detected.</li>
 * </ul>
 */
public final class DbStateLoader {

    private static final Logger LOGGER = Logger.getLogger(DbStateLoader.class.getName());

    public static final Set<String> VALID_VOTERS =
            Set.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private static final List<String> REQUIRED_TABLES = List.of(
            "raft_meta", "raft_log", "raft_apply_progress",
            "trips", "seats", "tickets", "request_results", "business_events");

    public static final String EXPECTED_TRIP_ID = "SE-001";
    public static final int EXPECTED_SEAT_COUNT = 10;

    public enum BootstrapType {
        GENESIS,
        RESTART
    }

    public record NodeStartupState(
            long currentTerm,
            String votedFor,
            long lastLogIndex,
            long lastLogTerm,
            long lastApplied,
            long initialCommitIndex,
            BootstrapType bootstrapType
    ) {
        public NodeStartupState {
            if (currentTerm < 0) {
                throw new IllegalArgumentException("currentTerm must be >= 0");
            }
            if (lastLogIndex < 0) {
                throw new IllegalArgumentException("lastLogIndex must be >= 0");
            }
            if (lastLogTerm < 0) {
                throw new IllegalArgumentException("lastLogTerm must be >= 0");
            }
            if (lastApplied < 0) {
                throw new IllegalArgumentException("lastApplied must be >= 0");
            }
            if (initialCommitIndex != lastApplied) {
                throw new IllegalArgumentException("initialCommitIndex must equal lastApplied");
            }
            Objects.requireNonNull(bootstrapType, "bootstrapType");
        }
    }

    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection getConnection() throws SQLException;
    }

    private final RaftStorage raftStorage;
    private final BookingRepository bookingRepository;
    private final ConnectionSupplier connectionSupplier;

    public DbStateLoader(RaftStorage raftStorage, BookingRepository bookingRepository, ConnectionFactory connectionFactory) {
        this(raftStorage, bookingRepository, connectionFactory == null ? null : connectionFactory::getConnection);
    }

    public DbStateLoader(RaftStorage raftStorage, BookingRepository bookingRepository, ConnectionSupplier connectionSupplier) {
        this.raftStorage = Objects.requireNonNull(raftStorage, "raftStorage");
        this.bookingRepository = bookingRepository;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * Executes the read-only bootstrap inspection and returns the initial startup state.
     *
     * @return the immutable startup state for Raft and Node initialization
     * @throws BookingRepository.StorageException if the database is uninitialized, inconsistent,
     *                                            or cannot be accessed
     */
    public NodeStartupState load() throws BookingRepository.StorageException {
        // 1. Verify required database schema presence if a connection supplier is provided
        if (connectionSupplier != null) {
            verifySchemaTables();
        }

        // 2. Read durable Raft state via RaftStorage contract
        PersistentState raftState;
        try {
            raftState = raftStorage.load();
        } catch (RaftStorageException | RuntimeException ex) {
            throw logAndWrap("load durable Raft state", ex);
        }

        long currentTerm = raftState.currentTerm();
        String votedFor = raftState.votedFor();
        List<RaftLogEntry> log = raftState.log();
        long lastApplied = raftState.lastApplied();

        // 3. Verify Raft state invariants
        if (currentTerm < 0) {
            throw new BookingRepository.StorageException("currentTerm must be >= 0, was: " + currentTerm);
        }
        if (votedFor != null && !VALID_VOTERS.contains(votedFor)) {
            throw new BookingRepository.StorageException(
                    "Invalid cluster voter in raft_meta: '" + votedFor + "'; expected one of " + VALID_VOTERS);
        }

        long expectedIndex = 1L;
        long prevTerm = 1L;
        for (RaftLogEntry entry : log) {
            if (entry == null) {
                throw new BookingRepository.StorageException("Raft log contains null entry");
            }
            if (entry.index() != expectedIndex) {
                throw new BookingRepository.StorageException(
                        "Raft log has a gap: expected index " + expectedIndex + " but found " + entry.index());
            }
            if (entry.term() < 1L) {
                throw new BookingRepository.StorageException(
                        "Raft log entry at index " + entry.index() + " has invalid term " + entry.term() + "; must be >= 1");
            }
            if (entry.term() < prevTerm) {
                throw new BookingRepository.StorageException(
                        "Raft log entry terms must be non-decreasing: index " + entry.index()
                                + " has term " + entry.term() + " < previous term " + prevTerm);
            }
            if (entry.term() > currentTerm) {
                throw new BookingRepository.StorageException(
                        "Raft log entry term (" + entry.term() + ") at index " + entry.index()
                                + " exceeds currentTerm (" + currentTerm + ")");
            }
            prevTerm = entry.term();
            expectedIndex = Math.addExact(expectedIndex, 1L);
        }

        long lastLogIndex = log.isEmpty() ? 0L : log.get(log.size() - 1).index();
        long lastLogTerm = log.isEmpty() ? 0L : log.get(log.size() - 1).term();

        if (currentTerm < lastLogTerm) {
            throw new BookingRepository.StorageException(
                    "currentTerm (" + currentTerm + ") is less than lastLogTerm (" + lastLogTerm + ")");
        }

        if (lastApplied > lastLogIndex) {
            throw new BookingRepository.StorageException(
                    "Inconsistent checkpoint: lastApplied (" + lastApplied + ") exceeds lastLogIndex (" + lastLogIndex + ")");
        }

        if (bookingRepository != null) {
            long repoLastApplied = bookingRepository.lastApplied();
            if (repoLastApplied != lastApplied) {
                throw new BookingRepository.StorageException(
                        "Checkpoint mismatch: RaftStorage reported lastApplied=" + lastApplied
                                + " while BookingRepository reported " + repoLastApplied);
            }
        }

        // SPEC-04 §6: initialCommitIndex is restored strictly from lastApplied
        long initialCommitIndex = lastApplied;

        // 4. Classify BootstrapType and verify business invariants
        BootstrapType bootstrapType;
        if (connectionSupplier != null) {
            bootstrapType = verifyBusinessDataAndClassify(currentTerm, votedFor, lastLogIndex, lastApplied);
        } else {
            boolean isGenesis = (currentTerm == 0L && votedFor == null && lastLogIndex == 0L && lastApplied == 0L);
            bootstrapType = isGenesis ? BootstrapType.GENESIS : BootstrapType.RESTART;
        }

        LOGGER.log(Level.INFO,
                "DbStateLoader successfully loaded startup state: currentTerm={0}, votedFor={1}, "
                        + "lastLogIndex={2}, lastLogTerm={3}, lastApplied={4}, initialCommitIndex={5}, bootstrapType={6}",
                new Object[]{currentTerm, votedFor, lastLogIndex, lastLogTerm, lastApplied, initialCommitIndex, bootstrapType});

        return new NodeStartupState(
                currentTerm,
                votedFor,
                lastLogIndex,
                lastLogTerm,
                lastApplied,
                initialCommitIndex,
                bootstrapType
        );
    }

    private void verifySchemaTables() {
        try (Connection conn = connectionSupplier.getConnection()) {
            for (String table : REQUIRED_TABLES) {
                try (Statement stmt = conn.createStatement()) {
                    // Verifies table existence and read access without loading data
                    stmt.execute("SELECT 1 FROM " + table + " LIMIT 0");
                } catch (SQLException ex) {
                    throw new BookingRepository.StorageException(
                            "SCHEMA_MISSING: Required database table '" + table
                                    + "' is not present or inaccessible. Run migration prior to startup.", ex);
                }
            }
        } catch (SQLException ex) {
            throw logAndWrap("verifySchemaTables connection", ex);
        }
    }

    private BootstrapType verifyBusinessDataAndClassify(
            long currentTerm, String votedFor, long lastLogIndex, long lastApplied) {
        try (Connection conn = connectionSupplier.getConnection()) {
            conn.setReadOnly(true);

            // Verify Trip presence
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COUNT(*) FROM trips WHERE trip_id = ?")) {
                stmt.setString(1, EXPECTED_TRIP_ID);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next() || rs.getInt(1) != 1) {
                        throw new BookingRepository.StorageException(
                                "Missing required seed trip '" + EXPECTED_TRIP_ID + "' in trips table");
                    }
                }
            }

            // Verify Seat count (exact 10 seats G01-G10)
            int seatCount;
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COUNT(*) FROM seats WHERE trip_id = ?")) {
                stmt.setString(1, EXPECTED_TRIP_ID);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    seatCount = rs.getInt(1);
                }
            }
            if (seatCount != EXPECTED_SEAT_COUNT) {
                throw new BookingRepository.StorageException(
                        "Invalid seat count for trip '" + EXPECTED_TRIP_ID + "': expected "
                                + EXPECTED_SEAT_COUNT + ", found " + seatCount);
            }

            boolean isGenesisRaft = (currentTerm == 0L && votedFor == null && lastLogIndex == 0L && lastApplied == 0L);

            if (isGenesisRaft) {
                // In GENESIS: All 10 seats must be AVAILABLE with no ticket assigned
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT COUNT(*) FROM seats WHERE trip_id = ? AND status = 'AVAILABLE' AND current_ticket_id IS NULL")) {
                    stmt.setString(1, EXPECTED_TRIP_ID);
                    try (ResultSet rs = stmt.executeQuery()) {
                        rs.next();
                        int availableCount = rs.getInt(1);
                        if (availableCount != EXPECTED_SEAT_COUNT) {
                            throw new BookingRepository.StorageException(
                                    "Genesis seat seed invalid: expected all " + EXPECTED_SEAT_COUNT
                                            + " seats to be AVAILABLE and unassigned, found " + availableCount);
                        }
                    }
                }

                // In GENESIS: tickets, request_results, business_events must be completely empty
                verifyTableEmpty(conn, "tickets", "Genesis state invalid: tickets table must be empty");
                verifyTableEmpty(conn, "request_results", "Genesis state invalid: request_results table must be empty");
                verifyTableEmpty(conn, "business_events", "Genesis state invalid: business_events table must be empty");

                return BootstrapType.GENESIS;
            } else {
                // In RESTART: Business data must be strictly consistent
                // Invariant 1: Every ACTIVE ticket must point to a BOOKED seat with matching current_ticket_id
                String ticketMismatchSql =
                        "SELECT COUNT(*) FROM tickets t "
                                + "LEFT JOIN seats s ON t.trip_id = s.trip_id AND t.seat_id = s.seat_id "
                                + "WHERE t.status = 'ACTIVE' AND (s.status != 'BOOKED' OR s.current_ticket_id IS DISTINCT FROM t.ticket_id)";
                int ticketMismatches = queryCount(conn, ticketMismatchSql);
                if (ticketMismatches > 0) {
                    throw new BookingRepository.StorageException(
                            "Business state inconsistency: found " + ticketMismatches
                                    + " ACTIVE ticket(s) whose corresponding seat is not BOOKED or has mismatched ticket ID");
                }

                // Invariant 2: Every BOOKED seat must correspond to an ACTIVE ticket
                String seatMismatchSql =
                        "SELECT COUNT(*) FROM seats s "
                                + "LEFT JOIN tickets t ON s.trip_id = t.trip_id AND s.seat_id = t.seat_id "
                                + "AND s.current_ticket_id = t.ticket_id AND t.status = 'ACTIVE' "
                                + "WHERE s.status = 'BOOKED' AND t.ticket_id IS NULL";
                int seatMismatches = queryCount(conn, seatMismatchSql);
                if (seatMismatches > 0) {
                    throw new BookingRepository.StorageException(
                            "Business state inconsistency: found " + seatMismatches
                                    + " BOOKED seat(s) without a corresponding ACTIVE ticket");
                }

                // Invariant 3: No two ACTIVE tickets may share the same trip and seat
                String duplicateActiveTicketsSql =
                        "SELECT COUNT(*) FROM ("
                                + "SELECT trip_id, seat_id FROM tickets WHERE status = 'ACTIVE' "
                                + "GROUP BY trip_id, seat_id HAVING COUNT(*) > 1"
                                + ") dup";
                int duplicates = queryCount(conn, duplicateActiveTicketsSql);
                if (duplicates > 0) {
                    throw new BookingRepository.StorageException(
                            "Business state inconsistency: duplicate ACTIVE tickets found for the same seat");
                }

                return BootstrapType.RESTART;
            }
        } catch (SQLException ex) {
            throw logAndWrap("verifyBusinessDataAndClassify", ex);
        }
    }

    private static void verifyTableEmpty(Connection conn, String table, String errorMessage) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            if (rs.getInt(1) > 0) {
                throw new BookingRepository.StorageException(errorMessage);
            }
        }
    }

    private static int queryCount(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static BookingRepository.StorageException logAndWrap(String action, Exception cause) {
        String message = cause.getMessage() == null ? "unknown"
                : cause.getMessage().replaceAll("password=[^&;\\s]*", "password=***");
        LOGGER.log(Level.SEVERE, "DbStateLoader error during {0}: {1}", new Object[]{action, message});
        return new BookingRepository.StorageException("DbStateLoader failure during " + action + ": " + message, cause);
    }
}

