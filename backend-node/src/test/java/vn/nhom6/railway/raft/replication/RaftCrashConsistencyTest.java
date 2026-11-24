package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D24 crash-consistency fault-point tests.
 *
 * <p>The in-memory tier proves ordering and retry with FakeRaftStorage; the JDBC tier
 * runs real PostgreSQL only when RAILWAY_TEST_* is configured. Neither tier starts
 * processes, containers, sockets or a five-node cluster.
 */
class RaftCrashConsistencyTest {

    private static final String TIME = "2026-11-24T09:00:00Z";
    private String jdbcUrl;
    private String username;
    private String password;
    private String schema;
    private ConnectionFactory connectionFactory;
    private JdbcRaftStorage jdbcStorage;

    @BeforeEach
    void setUpPostgres() throws Exception {
        jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        username = System.getenv("RAILWAY_TEST_DB_USER");
        password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        if (!hasText(jdbcUrl) || !hasText(username) || !hasText(password)) {
            return;
        }

        schema = "railway_d24_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = rawConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA \"" + schema + "\"");
        }
        try {
            runMigration(readMigration("V1__schema.sql"));
            runMigration(readMigration("V2__seed.sql"));
            connectionFactory = new ConnectionFactory(dbConfig());
            jdbcStorage = new JdbcRaftStorage(connectionFactory);
        } catch (Exception exception) {
            dropSchema();
            throw exception;
        }
    }

    @AfterEach
    void tearDownPostgres() throws SQLException {
        if (hasText(jdbcUrl) && hasText(schema)) {
            dropSchema();
        }
    }

    // -----------------------------------------------------------------------
    // Tier A: in-memory fault points
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("D24-A1 before append persist: no fake ACK and retry converges")
    void inMemoryBeforePersistAppendNoAckAndRetry() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, List.of(), 0L);
        RaftState state = RaftState.restore(1L, null, List.of(), 0L);
        PersistentRaftNode follower = follower(state, storage);
        RaftNode.AppendEntriesRequest request = appendRequest(
                1L, 0L, 0L, 0L, RaftLogEntry.noOp(1L, 1L));

        storage.failNext(new RaftStorage.RaftStorageException("D24-A1 injected before persist"));
        assertThrows(RaftStorage.RaftStorageException.class,
                () -> follower.handleAppendEntries(request));

        assertTrue(storage.storedLog().isEmpty());
        assertEquals(0L, follower.commitIndex());
        assertEquals(0L, follower.lastApplied());
        assertEquals(0L, follower.lastLogIndex());
        System.out.println("D24-A1 beforePersist matched=0 storedLog=0 retryReady=true");

        RaftState restartedState = RaftState.restore(1L, null, storage.storedLog(), 0L);
        PersistentRaftNode restarted = follower(restartedState, storage);
        RaftNode.AppendEntriesResponse retry =
                restarted.handleAppendEntries(request);
        assertTrue(retry.success());
        assertEquals(1L, retry.matchIndex());
        assertEquals(List.of(RaftLogEntry.noOp(1L, 1L)), storage.storedLog());
        System.out.println("D24-A1-retry success=true matchIndex=1 storedLog=1");
    }

    @Test
    @DisplayName("D24-A2 after persist before ACK: entry survives and retry is idempotent")
    void inMemoryPersistBeforeAckAmbiguousRetry() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, List.of(), 0L);
        RaftState state = RaftState.restore(1L, null, List.of(), 0L);
        PersistentRaftNode follower = follower(state, storage);
        RaftNode.AppendEntriesRequest request = appendRequest(
                1L, 0L, 0L, 0L, RaftLogEntry.noOp(1L, 1L));

        storage.persistThenFailNext();
        assertThrows(RaftStorage.RaftStorageException.class,
                () -> follower.handleAppendEntries(request));

        List<RaftLogEntry> persisted = List.copyOf(storage.storedLog());
        assertEquals(List.of(RaftLogEntry.noOp(1L, 1L)), persisted);
        assertEquals(1, storage.replaceSuffixCalls());
        System.out.println("D24-A2 ambiguous success=false persisted=1 replaceCalls=1");

        FakeRaftStorage restartedStorage = new FakeRaftStorage();
        restartedStorage.seedState(1L, null, persisted, 0L);
        RaftState restartedState = RaftState.restore(
                1L, null, restartedStorage.storedLog(), restartedStorage.loadLastApplied());
        PersistentRaftNode restarted = follower(restartedState, restartedStorage);
        RaftNode.AppendEntriesResponse retry = restarted.handleAppendEntries(request);

        assertTrue(retry.success());
        assertEquals(1L, retry.matchIndex());
        assertEquals(0, restartedStorage.replaceSuffixCalls(),
                "idempotent retry must not replace the persisted entry");
        assertEquals(persisted, restartedStorage.storedLog());
        System.out.println("D24-A2-restart success=true matchIndex=1 duplicates=0");
    }

    @Test
    @DisplayName("D24-A3 before apply: replay committed prefix, never uncommitted tail")
    void inMemoryBeforeApplyReplayAndTail() throws Exception {
        List<RaftLogEntry> log = List.of(
                RaftLogEntry.noOp(1L, 1L),
                RaftLogEntry.noOp(2L, 1L));
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, log, 0L);
        RaftState crashed = RaftState.restore(1L, null, log, 0L);
        assertEquals(0L, crashed.lastApplied());
        assertEquals(0L, crashed.commitIndex());
        crashed.advanceCommitIndex(1L);

        AtomicInteger sinkCalls = new AtomicInteger();
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                crashed, RaftApplyAdapter::decodeCommand,
                (index, command) -> {
                    sinkCalls.incrementAndGet();
                    assertTrue(index <= crashed.commitIndex());
                    return java.util.Optional.empty();
                });

        assertEquals(1, applier.applyCommitted());
        assertEquals(1L, crashed.lastApplied());
        assertEquals(0L, storage.loadLastApplied(), "FakeRaftStorage does not persist markApplied");

        crashed.advanceCommitIndex(2L);
        assertEquals(1, applier.applyCommitted());
        assertEquals(2L, crashed.lastApplied());
        assertEquals(2, sinkCalls.get());
        System.out.println("D24-A3 committed=2 applied=2 tailApplied=0");
    }

    @Test
    @DisplayName("D24-A4 sink success then hook failure: no index skip and no redelivery")
    void inMemoryAfterApplyHookFailureNoSkip() {
        List<RaftLogEntry> log = List.of(RaftLogEntry.noOp(1L, 1L));
        RaftState state = RaftState.restore(1L, null, log, 0L);
        state.advanceCommitIndex(1L);
        AtomicInteger sinkCalls = new AtomicInteger();
        AtomicBoolean hookFailed = new AtomicBoolean();
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                state, RaftApplyAdapter::decodeCommand,
                (index, command) -> {
                    sinkCalls.incrementAndGet();
                    return java.util.Optional.empty();
                },
                new RaftStateMachineApplier.Hooks() {
                    @Override
                    public void onApplied(RaftLogEntry entry,
                            java.util.Optional<BookingStateMachine.ApplyOutcome> outcome) {
                        hookFailed.set(true);
                        throw new IllegalStateException("D24-A4 caller crash after sink success");
                    }
                });

        assertEquals(1, applier.applyCommitted());
        assertTrue(hookFailed.get());
        assertEquals(1L, state.lastApplied());
        assertEquals(1, sinkCalls.get());
        assertEquals(0, applier.applyCommitted());
        assertEquals(1, sinkCalls.get());
        System.out.println("D24-A4 sinkCalls=1 onAppliedHook=1 lastApplied=1 replayDuplicates=0");
    }

    @Test
    @DisplayName("D24-A5 seed chain: repeated faults heal and converge")
    void inMemoryRepeatedFaultsConvergeAcrossSeeds() throws Exception {
        List<Long> seeds = List.of(
                1L, 2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L,
                29L, 31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L);
        for (long seed : seeds) {
            Random random = new Random(seed);
            FakeRaftStorage storage = new FakeRaftStorage();
            storage.seedState(1L, null, List.of(), 0L);
            RaftState state = RaftState.restore(1L, null, List.of(), 0L);
            PersistentRaftNode follower = follower(state, storage);
            RaftNode.AppendEntriesRequest request = appendRequest(
                    1L, 0L, 0L, 0L, RaftLogEntry.noOp(1L, 1L));

            int faultPoint = random.nextInt(4);
            if (faultPoint == 0) {
                storage.failNext(new RaftStorage.RaftStorageException("seed before persist"));
            } else if (faultPoint == 1) {
                storage.persistThenFailNext();
            } else if (faultPoint == 2) {
                storage.setFailing(false);
            }
            try {
                follower.handleAppendEntries(request);
            } catch (RaftStorage.RaftStorageException ignored) {
                // Ambiguous or pre-write fault: retry below.
            }
            storage.setFailing(false);
            RaftNode.AppendEntriesResponse healed =
                    follower.handleAppendEntries(request);
            assertTrue(healed.success(), "seed=" + seed);
            assertEquals(1L, healed.matchIndex(), "seed=" + seed);
            assertEquals(1, storage.replaceSuffixCalls(), "seed=" + seed);
            assertEquals(List.of(RaftLogEntry.noOp(1L, 1L)), storage.storedLog(), "seed=" + seed);
        }
        System.out.println("D24-A5 seeds=20 healed=20 duplicateEntries=0");
    }

    // -----------------------------------------------------------------------
    // Tier B: PostgreSQL fault points
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("D24-B1 JDBC before persist: no raft_log and retry appends once")
    void jdbcBeforePersistAppendNoWrite() throws Exception {
        requirePostgres();
        AtomicBoolean triggered = new AtomicBoolean();
        AtomicBoolean afterCommit = new AtomicBoolean();
        JdbcRaftStorage faulty = jdbcStorage((sql, executeUpdate) -> {
            if (executeUpdate && sql.contains("INSERT INTO raft_log")) {
                triggered.set(true);
                throw new SQLException("D24-B1 injected before raft_log insert");
            }
        });
        RaftLogEntry entry = entry(1L, book("11111111-1111-1111-1111-111111111111",
                "G01", "ticket-d24-b1"));
        assertThrows(RaftStorage.RaftStorageException.class,
                () -> faulty.replaceSuffix(1L, List.of(entry)));

        assertTrue(triggered.get());
        assertEquals(0, queryInt("SELECT COUNT(*) FROM raft_log"));
        jdbcStorage.saveTermAndVote(1L, null);
        jdbcStorage.replaceSuffix(1L, List.of(entry));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM raft_log"));
        RaftLogEntry loaded = jdbcStorage.load().log().getFirst();
        assertEquals(entry.index(), loaded.index());
        assertEquals(entry.term(), loaded.term());
        assertEquals(entry.commandType(), loaded.commandType());
        assertTrue(loaded.commandPayload().contains("G01"));
        System.out.println("D24-B1 fault=1 raftLog=1 retryEntries=1");
    }

    @Test
    @DisplayName("D24-B2 JDBC persist then commit reports error: log survives and retry deduplicates")
    void jdbcPersistThenCommitErrorLogSurvives() throws Exception {
        requirePostgres();
        AtomicBoolean triggered = new AtomicBoolean();
        AtomicBoolean commitFault = new AtomicBoolean(true);
        JdbcRaftStorage faulty = jdbcStorage((sql, executeUpdate) -> { });
        RaftLogEntry entry = entry(1L, book("22222222-2222-2222-2222-222222222222",
                "G02", "ticket-d24-b2"));
        jdbcStorage.saveTermAndVote(1L, null);

        JdbcRaftStorage commitFaultStorage = jdbcStorageCommitFault(commitFault, triggered);
        assertThrows(RaftStorage.RaftStorageException.class,
                () -> commitFaultStorage.replaceSuffix(1L, List.of(entry)));

        assertTrue(triggered.get());
        assertEquals(1, queryInt("SELECT COUNT(*) FROM raft_log"));
        RaftLogEntry loaded = jdbcStorage.load().log().getFirst();
        assertEquals(entry.index(), loaded.index());
        assertEquals(entry.term(), loaded.term());
        assertEquals(entry.commandType(), loaded.commandType());
        assertTrue(loaded.commandPayload().contains("\"seatId\":\"G02\"")
                || loaded.commandPayload().contains("\"seatId\": \"G02\""),
                "persisted payload must contain G02");
        commitFault.set(false);
        jdbcStorage.replaceSuffix(1L, List.of(entry));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM raft_log"));
        System.out.println("D24-B2 fault=1 raftLog=1 retryDuplicates=0");
    }

    @Test
    @DisplayName("D24-B3 JDBC preapply: committed entry replayed once, tail untouched")
    void jdbcBeforeApplyReplayOnceAndTailUntouched() throws Exception {
        requirePostgres();
        jdbcStorage.saveTermAndVote(1L, null);
        RaftLogEntry committed = entry(1L, book("33333333-3333-3333-3333-333333333333",
                "G03", "ticket-d24-b3"));
        RaftLogEntry tail = entry(2L, book("44444444-4444-4444-4444-444444444444",
                "G04", "ticket-d24-b3-tail"));
        jdbcStorage.replaceSuffix(1L, List.of(committed, tail));

        RaftStorage.PersistentState persisted = jdbcStorage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), 0L);
        assertEquals(0L, state.commitIndex());
        assertEquals(0L, state.lastApplied());
        state.advanceCommitIndex(1L);

        JdbcCommittedCommandApplier committedApplier =
                new JdbcCommittedCommandApplier(connectionFactory, result -> { });
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                state, RaftApplyAdapter::decodeCommand, committedApplier.commitListener());

        assertEquals(1, applier.applyCommitted());
        assertEquals(1L, state.lastApplied());
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, count("tickets"));
        assertEquals(0, countWhere("tickets", "seat_id = 'G04'"));
        assertEquals(1, count("business_events"));
        System.out.println("D24-B3 committed=1 applied=1 tailTickets=0 businessEvents=1");

        RaftState restarted = restoreFromStorage();
        assertEquals(1L, restarted.lastApplied());
        restarted.advanceCommitIndex(1L);
        JdbcCommittedCommandApplier restartedApplier =
                new JdbcCommittedCommandApplier(connectionFactory, result -> { });
        RaftStateMachineApplier restartedRunner = new RaftStateMachineApplier(
                restarted, RaftApplyAdapter::decodeCommand, restartedApplier.commitListener());
        assertEquals(0, restartedRunner.applyCommitted());
        assertEquals(1, count("tickets"));
    }

    @Test
    @DisplayName("D24-B4a JDBC commit rollback: no apply state and retry applies once")
    void jdbcApplyCommitRollbackRetryOnce() throws Exception {
        requirePostgres();
        BookingCommand bookCommand = book("55555555-5555-5555-5555-555555555555",
                "G05", "ticket-d24-b4");
        jdbcStorage.saveTermAndVote(1L, null);
        jdbcStorage.replaceSuffix(1L, List.of(entry(1L, bookCommand)));
        JdbcBookingRepository normalRepository = new JdbcBookingRepository(connectionFactory);
        JdbcCommittedCommandApplier normalApplier =
                new JdbcCommittedCommandApplier(connectionFactory, result -> { });
        RaftState state = RaftState.restore(1L, null,
                List.of(entry(1L, bookCommand)), 0L);
        state.advanceCommitIndex(1L);

        AtomicBoolean faultBeforeCommit = new AtomicBoolean(true);
        AtomicBoolean triggered = new AtomicBoolean();
        JdbcBookingRepository faultyRepository =
                faultyBookingRepository(faultBeforeCommit, triggered, false);
        JdbcCommittedCommandApplier faultyApplier =
                faultyCommittedApplier(faultyRepository);
        RaftStateMachineApplier faultyRunner = new RaftStateMachineApplier(
                state, RaftApplyAdapter::decodeCommand, faultyApplier.commitListener());

        assertEquals(0, faultyRunner.applyCommitted());
        assertTrue(triggered.get());
        assertFalse(faultyRunner.failed());
        assertEquals(0L, state.lastApplied());
        assertEquals(0L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, count("tickets"));
        assertEquals(0, count("request_results"));
        assertEquals(0, count("business_events"));

        faultBeforeCommit.set(false);
        triggered.set(false);
        RaftState retryState = state;
        RaftStateMachineApplier retryRunner = new RaftStateMachineApplier(
                retryState, RaftApplyAdapter::decodeCommand, normalApplier.commitListener());
        assertEquals(1, retryRunner.applyCommitted());
        assertEquals(1L, retryState.lastApplied());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        System.out.println("D24-B4a fault=1 rollback=complete retryApplied=1 duplicates=0");
    }

    @Test
    @DisplayName("D24-B4b JDBC commit success then caller error: restart does not reapply")
    void jdbcCommitSuccessThenCallerErrorReplayDurableOnce() throws Exception {
        requirePostgres();
        seedOneBook("66666666-6666-6666-6666-666666666666", "G06", "ticket-d24-b4b");
        AtomicBoolean triggered = new AtomicBoolean();
        AtomicBoolean afterCommit = new AtomicBoolean(true);

        JdbcBookingRepository faultyRepository =
                faultyBookingRepository(afterCommit, triggered, true);
        JdbcCommittedCommandApplier faultyApplier =
                faultyCommittedApplier(faultyRepository);
        RaftState state = RaftState.restore(1L, null,
                List.of(entry(1L, book("66666666-6666-6666-6666-666666666666",
                        "G06", "ticket-d24-b4b"))), 0L);
        state.advanceCommitIndex(1L);
        RaftStateMachineApplier faultyRunner = new RaftStateMachineApplier(
                state, RaftApplyAdapter::decodeCommand, faultyApplier.commitListener());

        assertEquals(0, faultyRunner.applyCommitted());
        assertTrue(triggered.get());
        assertTrue(faultyRunner.failed());
        assertEquals(0L, state.lastApplied());
        assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));

        RaftState restarted = restoreFromStorage();
        JdbcCommittedCommandApplier restartedApplier =
                new JdbcCommittedCommandApplier(connectionFactory, result -> { });
        RaftStateMachineApplier restartedRunner = new RaftStateMachineApplier(
                restarted, RaftApplyAdapter::decodeCommand, restartedApplier.commitListener());
        assertEquals(0, restartedRunner.applyCommitted());
        assertEquals(1, count("tickets"));
        assertEquals(1, count("request_results"));
        assertEquals(1, count("business_events"));
        System.out.println("D24-B4b commit=1 callerError=1 restartApplied=0 duplicates=0");
    }

    // -----------------------------------------------------------------------
    // Fault and database fixtures
    // -----------------------------------------------------------------------

    private PersistentRaftNode follower(RaftState state, FakeRaftStorage storage)
            throws Exception {
        PersistentRaftNode node = new PersistentRaftNode("node-2", state, storage);
        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
        ElectionTimer timer = new ElectionTimer(
                noOpElection(node, state), executor, ElectionConfig.defaults(),
                failure -> {
                    throw new IllegalStateException("D24 election timer failure", failure);
                });
        node.bindLifecycle(timer, () -> { });
        return node;
    }

    private static ElectionService noOpElection(PersistentRaftNode node, RaftState state) {
        return new ElectionService() {
            @Override public String localId() { return "node-2"; }
            @Override public RaftNode node() { return node; }
            @Override public RaftState state() { return state; }
            @Override public List<String> voters() {
                return List.of("node-1", "node-2", "node-3", "node-4", "node-5");
            }
            @Override public boolean isVoter(String nodeId) { return voters().contains(nodeId); }
            @Override public int requiredVotes() { return 3; }
            @Override public long startElection() { return state.currentTerm(); }
            @Override public java.util.Optional<RequestVoteRequestDto> voteRequestFor(long term) {
                return java.util.Optional.empty();
            }
            @Override public RaftNode.RequestVoteResponse handleRequestVote(
                    RequestVoteRequestDto request) { return new RaftNode.RequestVoteResponse(state.currentTerm(), false); }
            @Override public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
                return new IgnoredVote("D24 no-op");
            }
            @Override public void promoteToLeader(java.util.Set<String> peers) { }
        };
    }

    private static RaftNode.AppendEntriesRequest appendRequest(
            long term, long prevIndex, long prevTerm, long leaderCommit,
            RaftLogEntry... entries) {
        return new RaftNode.AppendEntriesRequest(
                term, "node-1", prevIndex, prevTerm, List.of(entries), leaderCommit);
    }

    private static RaftLogEntry entry(long index, BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return new RaftLogEntry(index, 1L, "BOOK_TICKET",
                    "{\"requestId\":\"" + book.requestId() + "\",\"tripId\":\"" + book.tripId()
                            + "\",\"seatId\":\"" + book.seatId() + "\",\"passengerName\":\""
                            + book.passengerName() + "\",\"ticketId\":\"" + book.ticketId()
                            + "\",\"issuedAt\":\"" + book.issuedAt() + "\"}");
        }
        return RaftLogEntry.noOp(index, 1L);
    }

    private static BookingCommand book(String requestId, String seatId, String ticketId) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, "Passenger", ticketId,
                Instant.parse(TIME));
    }

    private JdbcRaftStorage jdbcStorage(StorageFault fault) {
        return new JdbcRaftStorage(() -> {
            Connection connection = schemaConnection();
            return wrapStorageConnection(connection, fault);
        });
    }

    private JdbcRaftStorage jdbcStorageCommitFault(
            AtomicBoolean commitFault, AtomicBoolean triggered) {
        return new JdbcRaftStorage(() -> {
            Connection connection = schemaConnection();
            return wrapStorageCommitConnection(connection, commitFault, triggered);
        });
    }

    private JdbcBookingRepository faultyBookingRepository(
            AtomicBoolean fault, AtomicBoolean triggered, boolean afterCommit) {
        try {
            Class<?> providerClass =
                    Class.forName("vn.nhom6.railway.persistence.JdbcBookingRepository$ConnectionProvider");
            var constructor = JdbcBookingRepository.class.getDeclaredConstructor(providerClass);
            constructor.setAccessible(true);
            Object provider = Proxy.newProxyInstance(
                    providerClass.getClassLoader(),
                    new Class<?>[] {providerClass},
                    (proxy, method, args) -> wrapApplyConnection(
                            schemaConnection(), fault, triggered, afterCommit));
            return (JdbcBookingRepository) constructor.newInstance(provider);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Could not create faulty JDBC repository", exception);
        }
    }

    private JdbcCommittedCommandApplier faultyCommittedApplier(
            JdbcBookingRepository repository) {
        try {
            var constructor = JdbcCommittedCommandApplier.class.getDeclaredConstructor(
                    JdbcBookingRepository.class,
                    vn.nhom6.railway.booking.CommittedCommandApplier.ApplyResultListener.class);
            constructor.setAccessible(true);
            return (JdbcCommittedCommandApplier) constructor.newInstance(
                    repository, (vn.nhom6.railway.booking.CommittedCommandApplier.ApplyResultListener)
                            result -> { });
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Could not create faulty JDBC applier", exception);
        }
    }

    @FunctionalInterface
    private interface StorageFault {
        void check(String sql, boolean executeUpdate) throws SQLException;
    }

    private static Connection wrapStorageConnection(
            Connection connection, StorageFault fault) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName()) && args != null && args.length > 0
                            && args[0] instanceof String sql) {
                        PreparedStatement statement =
                                (PreparedStatement) invoke(connection, method, args);
                        return (PreparedStatement) Proxy.newProxyInstance(
                                PreparedStatement.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (stmtProxy, stmtMethod, stmtArgs) -> {
                                    boolean execution = "executeUpdate".equals(stmtMethod.getName())
                                            || "execute".equals(stmtMethod.getName())
                                            || "executeBatch".equals(stmtMethod.getName());
                                    if (execution) {
                                        fault.check(sql, true);
                                    }
                                    return invoke(statement, stmtMethod, stmtArgs);
                                });
                    }
                    return invoke(connection, method, args);
                });
    }

    private static Connection wrapStorageCommitConnection(
            Connection connection, AtomicBoolean fault, AtomicBoolean triggered) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("commit".equals(method.getName()) && fault.get()) {
                        triggered.set(true);
                        invoke(connection, method, args);
                        throw new SQLException("D24-B2 injected after persist before ACK");
                    }
                    return invoke(connection, method, args);
                });
    }

    private static Connection wrapApplyConnection(
            Connection connection, AtomicBoolean fault, AtomicBoolean triggered,
            boolean afterCommit) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("commit".equals(method.getName())) {
                        if (fault.get() && !afterCommit) {
                            triggered.set(true);
                            throw new SQLException("D24-B4a injected before SQL commit");
                        }
                        Object result = invoke(connection, method, args);
                        if (fault.get() && afterCommit) {
                            triggered.set(true);
                            throw new IllegalStateException(
                                    "D24-B4b commit succeeded, caller received error");
                        }
                        return result;
                    }
                    return invoke(connection, method, args);
                });
    }

    private static Object invoke(
            Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private void seedOneBook(String requestId, String seatId, String ticketId) throws Exception {
        jdbcStorage.saveTermAndVote(1L, null);
        RaftLogEntry entry = entry(1L, book(requestId, seatId, ticketId));
        jdbcStorage.replaceSuffix(1L, List.of(entry));
    }

    private RaftState restoreFromStorage() throws Exception {
        RaftStorage.PersistentState persisted = jdbcStorage.load();
        return RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
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
            throw new IllegalStateException("D24 query failed", exception);
        }
    }

    private long queryLong(String sql) {
        try (Connection connection = schemaConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getLong(1);
        } catch (SQLException exception) {
            throw new IllegalStateException("D24 query failed", exception);
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
        return new DbConfig(
                uri.getHost(), port, dbName + "?currentSchema=" + schema,
                username, password);
    }

    private void dropSchema() throws SQLException {
        try (Connection connection = rawConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        } finally {
            schema = null;
        }
    }

    private void requirePostgres() {
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER and RAILWAY_TEST_DB_PASSWORD are required");
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
