package vn.nhom6.railway.booking;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.persistence.DbStateLoader.BootstrapType;
import vn.nhom6.railway.persistence.DbStateLoader.NodeStartupState;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification of database state recovery on Node restart (SPEC-04 §6, SPEC-05 §4.1, SPEC-08 §7).
 */
class PersistenceRestartTest {

    // =======================================================================================
    // Nhóm A: Kiểm tra các luật nhất quán của DbStateLoader bằng fake storage (không cần DB)
    // =======================================================================================
    @Nested
    @DisplayName("Nhóm A: In-memory consistency rules (no DB required)")
    class GroupA_IsolatedConsistencyTests {

        @Test
        @DisplayName("lastApplied vượt quá lastLogIndex -> fail closed")
        void lastAppliedExceedsLastLogIndex_failsClosed() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    1L, "node-1",
                    List.of(new RaftLogEntry(1L, 1L, "NO_OP", "{}")),
                    2L // lastApplied=2 > lastLogIndex=1
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("exceeds lastLogIndex"));
        }

        @Test
        @DisplayName("Log có lỗ (gap) -> fail closed")
        void logWithGap_failsClosed() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    1L, "node-1",
                    List.of(
                            new RaftLogEntry(1L, 1L, "NO_OP", "{}"),
                            new RaftLogEntry(3L, 1L, "NO_OP", "{}") // thiếu index 2
                    ),
                    0L
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("Raft log has a gap"));
        }

        @Test
        @DisplayName("Term của entry giảm trong log -> fail closed")
        void entryTermDecreasing_failsClosed() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    3L, "node-1",
                    List.of(
                            new RaftLogEntry(1L, 2L, "NO_OP", "{}"),
                            new RaftLogEntry(2L, 1L, "NO_OP", "{}") // term 1 < term 2
                    ),
                    0L
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("non-decreasing"));
        }

        @Test
        @DisplayName("Term của entry vượt quá currentTerm -> fail closed")
        void entryTermExceedsCurrentTerm_failsClosed() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    1L, // currentTerm = 1
                    "node-1",
                    List.of(new RaftLogEntry(1L, 2L, "NO_OP", "{}")), // entry term = 2
                    0L
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("exceeds currentTerm"));
        }

        @Test
        @DisplayName("votedFor không thuộc cụm 5 voter hợp lệ -> fail closed")
        void unrecognizedVoter_failsClosed() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    1L, "node-9", // node-9 không hợp lệ
                    List.of(new RaftLogEntry(1L, 1L, "NO_OP", "{}")),
                    1L
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("Invalid cluster voter"));
        }

        @Test
        @DisplayName("SPEC-04 §6: initialCommitIndex phải bằng lastApplied, không lấy theo lastLogIndex")
        void initialCommitIndexEqualsLastApplied_notLastLogIndex() {
            FakeRaftStorage fakeStorage = new FakeRaftStorage(
                    2L, "node-2",
                    List.of(
                            new RaftLogEntry(1L, 1L, "NO_OP", "{}"),
                            new RaftLogEntry(2L, 2L, "NO_OP", "{}"),
                            new RaftLogEntry(3L, 2L, "NO_OP", "{}")
                    ),
                    1L // lastApplied = 1 trong khi lastLogIndex = 3
            );
            DbStateLoader loader = new DbStateLoader(fakeStorage, null, (DbStateLoader.ConnectionSupplier) null);
            NodeStartupState state = loader.load();

            assertEquals(1L, state.lastApplied());
            assertEquals(1L, state.initialCommitIndex(), "commitIndex phải khởi tạo bằng lastApplied theo SPEC-04 §6");
            assertEquals(3L, state.lastLogIndex());
            assertEquals(2L, state.lastLogTerm());
            assertEquals(2L, state.currentTerm());
            assertEquals("node-2", state.votedFor());
            assertEquals(BootstrapType.RESTART, state.bootstrapType());
        }
    }

    // =======================================================================================
    // Nhóm B: Kiểm thử toàn diện trên PostgreSQL thật (schema cô lập ngẫu nhiên)
    // =======================================================================================
    @Nested
    @DisplayName("Nhóm B: Real PostgreSQL restart tests")
    class GroupB_RealPostgreSqlRestartTests {

        private String rawJdbcUrl;
        private String dbUser;
        private String dbPassword;
        private String schema;
        private String host;
        private int port;
        private String dbName;

        @BeforeEach
        void setUp() throws Exception {
            rawJdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
            dbUser = System.getenv("RAILWAY_TEST_DB_USER");
            dbPassword = System.getenv("RAILWAY_TEST_DB_PASSWORD");

            if (Boolean.getBoolean("railway.requirePostgres")) {
                assertTrue(hasText(rawJdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                        "PostgreSQL is mandatory for this run; configure RAILWAY_TEST_* variables");
            }
            Assumptions.assumeTrue(
                    hasText(rawJdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                    "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD required");

            URI uri = URI.create(rawJdbcUrl.substring("jdbc:".length()));
            host = uri.getHost();
            port = uri.getPort() < 0 ? 5432 : uri.getPort();
            dbName = uri.getPath().substring(1);

            schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
            try (Connection conn = rawConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA \"" + schema + "\"");
            }
        }

        @AfterEach
        void tearDown() throws SQLException {
            if (schema != null && hasText(rawJdbcUrl) && hasText(dbUser) && hasText(dbPassword)) {
                try (Connection conn = rawConnection(); Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
                }
            }
        }

        private Connection rawConnection() throws SQLException {
            return DriverManager.getConnection(rawJdbcUrl, dbUser, dbPassword);
        }

        private Connection schemaConnection() throws SQLException {
            Connection conn = rawConnection();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET search_path TO \"" + schema + "\"");
            }
            return conn;
        }

        private ConnectionFactory connectionFactory() {
            // Embed currentSchema in database name parameter so all connections use the isolated schema
            String schemaDbName = dbName + "?currentSchema=" + schema;
            return new ConnectionFactory(new DbConfig(host, port, schemaDbName, dbUser, dbPassword));
        }

        private void runMigrations(boolean includeSeed) throws Exception {
            String v1 = readMigration("V1__schema.sql");
            try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
                for (String sql : v1.split(";")) {
                    if (!sql.isBlank()) {
                        stmt.execute(sql);
                    }
                }
                if (includeSeed) {
                    String v2 = readMigration("V2__seed.sql");
                    for (String sql : v2.split(";")) {
                        if (!sql.isBlank()) {
                            stmt.execute(sql);
                        }
                    }
                }
            }
        }

        @Test
        @DisplayName("Test 1: DB mới (genesis) -> loader trả term 0, votedFor null, log rỗng, lastApplied 0, commitIndex 0, GENESIS")
        void test01_genesisDatabase_returnsGenesisStartupState() throws Exception {
            runMigrations(true);

            ConnectionFactory factory = connectionFactory();
            JdbcRaftStorage storage = new JdbcRaftStorage(factory);
            JdbcBookingRepository repo = new JdbcBookingRepository(factory);
            DbStateLoader loader = new DbStateLoader(storage, repo, factory);

            NodeStartupState state = loader.load();

            assertEquals(0L, state.currentTerm());
            assertNull(state.votedFor());
            assertEquals(0L, state.lastLogIndex());
            assertEquals(0L, state.lastLogTerm());
            assertEquals(0L, state.lastApplied());
            assertEquals(0L, state.initialCommitIndex());
            assertEquals(BootstrapType.GENESIS, state.bootstrapType());
        }

        @Test
        @DisplayName("Test 2: Dựng trạng thái, apply lệnh -> restart toàn bộ đối tượng -> loader trả đúng trạng thái RESTART")
        void test02_statefulRestart_returnsRestartStartupState() throws Exception {
            runMigrations(true);

            // Phase 1: Tạo trạng thái
            ConnectionFactory factory1 = connectionFactory();
            JdbcRaftStorage storage1 = new JdbcRaftStorage(factory1);
            JdbcBookingRepository repo1 = new JdbcBookingRepository(factory1);
            JdbcCommittedCommandApplier applier1 = new JdbcCommittedCommandApplier(factory1);

            storage1.saveTermAndVote(2L, "node-3");

            Instant now = Instant.parse("2026-11-13T10:00:00Z");
            String req1 = "11111111-1111-1111-1111-111111111111";
            RaftLogEntry e1 = new RaftLogEntry(1L, 2L, "BOOK_TICKET",
                    "{\"requestId\":\"" + req1 + "\",\"tripId\":\"SE-001\",\"seatId\":\"G01\",\"passengerName\":\"Linh\",\"ticketId\":\"t-1\",\"issuedAt\":\"" + now + "\"}");
            RaftLogEntry e2 = new RaftLogEntry(2L, 2L, "NO_OP", "{}");
            RaftLogEntry e3 = new RaftLogEntry(3L, 2L, "NO_OP", "{}");
            storage1.replaceSuffix(1L, List.of(e1, e2, e3));

            // Apply index 1 và 2
            applier1.applyCommitted(1L, new BookingCommand.BookTicket(req1, "SE-001", "G01", "Linh", "t-1", now));
            applier1.applyCommitted(2L, new BookingCommand.NoOp());

            assertEquals(2L, repo1.lastApplied());

            // Phase 2: RESTART TOÀN BỘ ĐỐI TƯỢNG TRÊN CÙNG SCHEMA
            ConnectionFactory factory2 = connectionFactory();
            JdbcRaftStorage storage2 = new JdbcRaftStorage(factory2);
            JdbcBookingRepository repo2 = new JdbcBookingRepository(factory2);
            DbStateLoader loader2 = new DbStateLoader(storage2, repo2, factory2);

            NodeStartupState restarted = loader2.load();

            assertEquals(2L, restarted.currentTerm());
            assertEquals("node-3", restarted.votedFor());
            assertEquals(3L, restarted.lastLogIndex());
            assertEquals(2L, restarted.lastLogTerm());
            assertEquals(2L, restarted.lastApplied());
            assertEquals(2L, restarted.initialCommitIndex(), "commitIndex phải = lastApplied theo SPEC-04 §6");
            assertEquals(BootstrapType.RESTART, restarted.bootstrapType());
        }

        @Test
        @DisplayName("Test 3: Loader KHÔNG sửa DB: chụp snapshot trước/sau gọi loader nhiều lần phải y hệt 100%")
        void test03_loaderIsStrictlyReadOnly_preservesExactDbSnapshot() throws Exception {
            runMigrations(true);

            ConnectionFactory factory = connectionFactory();
            JdbcRaftStorage storage = new JdbcRaftStorage(factory);
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(factory);

            storage.saveTermAndVote(1L, "node-1");
            Instant now = Instant.parse("2026-11-13T10:00:00Z");
            String req = "11111111-1111-1111-1111-111111111111";
            storage.replaceSuffix(1L, List.of(new RaftLogEntry(1L, 1L, "BOOK_TICKET",
                    "{\"requestId\":\"" + req + "\",\"tripId\":\"SE-001\",\"seatId\":\"G01\",\"passengerName\":\"Linh\",\"ticketId\":\"t-1\",\"issuedAt\":\"" + now + "\"}")));
            applier.applyCommitted(1L, new BookingCommand.BookTicket(req, "SE-001", "G01", "Linh", "t-1", now));

            // Chụp snapshot trước khi gọi loader
            DatabaseSnapshot before = captureSnapshot();

            // Gọi loader 3 lần liên tiếp
            DbStateLoader loader = new DbStateLoader(storage, new JdbcBookingRepository(factory), factory);
            loader.load();
            loader.load();
            loader.load();

            // Chụp snapshot sau khi gọi loader
            DatabaseSnapshot after = captureSnapshot();

            assertEquals(before, after, "Loader không được phép sửa bất kỳ dòng dữ liệu nào");
            assertEquals(1, after.bookedSeatsCount, "Ghế BOOKED trước restart vẫn phải nguyên vẹn");
            assertEquals(1, after.activeTicketsCount, "Vé ACTIVE trước restart vẫn phải nguyên vẹn");
        }

        @Test
        @DisplayName("Test 4: Sau restart, phát lại entry cũ là idempotent, entry mới tiếp tục apply bình thường")
        void test04_afterRestart_replayingAppliedEntryIsIdempotent_newEntriesApplySuccessfully() throws Exception {
            runMigrations(true);

            ConnectionFactory factory1 = connectionFactory();
            JdbcRaftStorage storage1 = new JdbcRaftStorage(factory1);
            JdbcCommittedCommandApplier applier1 = new JdbcCommittedCommandApplier(factory1);

            storage1.saveTermAndVote(1L, "node-1");
            Instant now = Instant.parse("2026-11-13T10:00:00Z");
            String req1 = "11111111-1111-1111-1111-111111111111";
            BookingCommand.BookTicket bookG01 = new BookingCommand.BookTicket(req1, "SE-001", "G01", "Linh", "t-1", now);
            storage1.replaceSuffix(1L, List.of(new RaftLogEntry(1L, 1L, "BOOK_TICKET", commandJson(bookG01))));
            applier1.applyCommitted(1L, bookG01);

            // Restart: tạo instance mới
            ConnectionFactory factory2 = connectionFactory();
            JdbcBookingRepository repo2 = new JdbcBookingRepository(factory2);
            JdbcCommittedCommandApplier applier2 = new JdbcCommittedCommandApplier(factory2);
            JdbcRaftStorage storage2 = new JdbcRaftStorage(factory2);

            // Replay index 1 (lệnh cũ) -> idempotent, không tạo vé thứ hai, trả lại kết quả đã lưu
            var replayResult = applier2.applyCommitted(1L, bookG01).orElseThrow();
            assertTrue(replayResult instanceof BookingStateMachine.ApplyOutcome.BusinessResult);
            BookingStateMachine.ApplyOutcome.BusinessResult br1 = (BookingStateMachine.ApplyOutcome.BusinessResult) replayResult;
            assertEquals(ApplyResult.Outcome.CONFIRMED, br1.applyResult().outcome());
            assertEquals("t-1", br1.applyResult().ticketId());
            assertEquals(1L, br1.applyResult().appliedLogIndex());
            assertEquals(1L, repo2.lastApplied());
            assertEquals(1, countTable("tickets WHERE status = 'ACTIVE' AND seat_id = 'G01'"));

            // Apply index 2 (lệnh mới) -> thành công bình thường
            String req2 = "22222222-2222-2222-2222-222222222222";
            BookingCommand.BookTicket bookG02 = new BookingCommand.BookTicket(req2, "SE-001", "G02", "Nam", "t-2", now);
            storage2.replaceSuffix(2L, List.of(new RaftLogEntry(2L, 1L, "BOOK_TICKET", commandJson(bookG02))));
            var newResult = applier2.applyCommitted(2L, bookG02).orElseThrow();
            assertTrue(newResult instanceof BookingStateMachine.ApplyOutcome.BusinessResult);
            BookingStateMachine.ApplyOutcome.BusinessResult br2 = (BookingStateMachine.ApplyOutcome.BusinessResult) newResult;
            assertEquals(ApplyResult.Outcome.CONFIRMED, br2.applyResult().outcome());
            assertEquals("t-2", br2.applyResult().ticketId());
            assertEquals(2L, br2.applyResult().appliedLogIndex());
            assertEquals(2L, repo2.lastApplied());
            assertEquals(2, countTable("tickets WHERE status = 'ACTIVE'"));
        }

        @Test
        @DisplayName("Test 5: Chèn lỗi giữa apply -> rollback sạch sẽ, restart nhất quán, retry thành công đúng một lần")
        void test05_injectedFailureDuringApplyRollsBackCleanly() throws Exception {
            runMigrations(true);

            ConnectionFactory setupFactory = connectionFactory();
            JdbcRaftStorage setupStorage = new JdbcRaftStorage(setupFactory);
            setupStorage.saveTermAndVote(1L, "node-1");

            Instant now = Instant.parse("2026-11-13T10:00:00Z");
            String req = "11111111-1111-1111-1111-111111111111";
            BookingCommand.BookTicket cmd = new BookingCommand.BookTicket(req, "SE-001", "G01", "Linh", "t-1", now);
            setupStorage.replaceSuffix(1L, List.of(new RaftLogEntry(1L, 1L, "BOOK_TICKET", commandJson(cmd))));

            AtomicReference<SqlFault> fault = new AtomicReference<>(new SqlFault("INSERT INTO tickets", false));
            BookingRepository faultyRepo = createFaultyRepository(fault);

            // Apply gặp lỗi ném exception
            assertThrows(BookingRepository.StorageException.class, () -> faultyRepo.applyAtomically(1L, cmd));
            assertTrue(fault.get().triggered().get());

            // Restart: dựng lại các đối tượng sạch
            ConnectionFactory cleanFactory = connectionFactory();
            JdbcRaftStorage cleanStorage = new JdbcRaftStorage(cleanFactory);
            JdbcBookingRepository cleanRepo = new JdbcBookingRepository(cleanFactory);
            DbStateLoader loader = new DbStateLoader(cleanStorage, cleanRepo, cleanFactory);

            // DB không có hàng nửa chừng, lastApplied vẫn là 0
            NodeStartupState state = loader.load();
            assertEquals(0L, state.lastApplied());
            assertEquals(0, countTable("tickets"));
            assertEquals(0, countTable("request_results"));
            assertEquals(0, countTable("business_events"));

            // Retry lần 2 thành công trọn vẹn đúng 1 lần
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(cleanFactory);
            applier.applyCommitted(1L, cmd);

            assertEquals(1L, cleanRepo.lastApplied());
            assertEquals(1, countTable("tickets WHERE status = 'ACTIVE'"));
            assertEquals(1, countTable("request_results WHERE request_id = '" + req + "'"));
        }

        @Test
        @DisplayName("Test 6: Hỏng dữ liệu cố ý bằng SQL -> loader fail closed từng trường hợp, DB không bị sửa")
        void test06_corruptedData_failsClosedWithoutModifyingDatabase() throws Exception {
            runMigrations(true);

            ConnectionFactory factory = connectionFactory();
            JdbcRaftStorage storage = new JdbcRaftStorage(factory);
            storage.saveTermAndVote(1L, "node-1");
            storage.replaceSuffix(1L, List.of(new RaftLogEntry(1L, 1L, "NO_OP", "{}")));

            DbStateLoader loader = new DbStateLoader(storage, new JdbcBookingRepository(factory), factory);

            // Case A: last_applied > lastLogIndex
            executeSql("UPDATE raft_apply_progress SET last_applied = 5 WHERE singleton = 1");
            assertThrows(BookingRepository.StorageException.class, loader::load);
            executeSql("UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1");

            // Case B: Xóa entry giữa log tạo lỗ hổng (thêm index 3 khi có 1, thiếu 2)
            executeSql("INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (3, 1, 'NO_OP', '{}'::jsonb)");
            assertThrows(BookingRepository.StorageException.class, loader::load);
            executeSql("DELETE FROM raft_log WHERE log_index = 3");

            // Case C: voted_for = 'node-9'
            executeSql("UPDATE raft_meta SET voted_for = 'node-9' WHERE singleton = 1");
            assertThrows(BookingRepository.StorageException.class, loader::load);
            executeSql("UPDATE raft_meta SET voted_for = 'node-1' WHERE singleton = 1");

            // Case D: Ghế seats chỉ còn 9 hàng (bị xóa 1 ghế)
            executeSql("DELETE FROM seats WHERE seat_id = 'G10'");
            assertThrows(BookingRepository.StorageException.class, loader::load);
        }

        @Test
        @DisplayName("Test 7: Schema chưa có (schema rỗng) -> loader báo SCHEMA_MISSING, không tự tạo bảng")
        void test07_emptySchema_failsClosedWithSchemaMissing() {
            // Không chạy migrations V1, schema trống hoàn toàn
            ConnectionFactory factory = connectionFactory();
            JdbcRaftStorage storage = new JdbcRaftStorage(factory);
            DbStateLoader loader = new DbStateLoader(storage, null, factory);

            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("SCHEMA_MISSING"), "Thông báo lỗi phải nêu rõ SCHEMA_MISSING");

            // Kiểm tra không có bảng nào bị tự động tạo
            int tableCount = countTablesInCurrentSchema();
            assertEquals(0, tableCount, "Loader tuyệt đối không tự ý chạy migration hay tạo bảng");
        }

        @Test
        @DisplayName("Test 8: Genesis nhưng thiếu seed -> fail closed, không tự seed bù")
        void test08_genesisMissingSeed_failsClosedWithoutAutoSeeding() throws Exception {
            // Chạy V1__schema.sql nhưng KHÔNG chạy V2__seed.sql
            runMigrations(false);

            ConnectionFactory factory = connectionFactory();
            JdbcRaftStorage storage = new JdbcRaftStorage(factory);
            DbStateLoader loader = new DbStateLoader(storage, null, factory);

            BookingRepository.StorageException ex = assertThrows(
                    BookingRepository.StorageException.class, loader::load);
            assertTrue(ex.getMessage().contains("Missing required seed trip") || ex.getMessage().contains("Invalid seat count"));

            // Đảm bảo không có hàng nào được tự seed bù vào trips hoặc seats
            assertEquals(0, countTable("trips"));
            assertEquals(0, countTable("seats"));
        }

        @Test
        @DisplayName("Test 9: Term không tụt: sau restart, currentTerm >= term đã lưu và >= term entry cuối log")
        void test09_termDoesNotDecreaseAcrossRestarts() throws Exception {
            runMigrations(true);

            ConnectionFactory factory1 = connectionFactory();
            JdbcRaftStorage storage1 = new JdbcRaftStorage(factory1);
            storage1.saveTermAndVote(3L, "node-2");
            storage1.replaceSuffix(1L, List.of(
                    new RaftLogEntry(1L, 1L, "NO_OP", "{}"),
                    new RaftLogEntry(2L, 2L, "NO_OP", "{}"),
                    new RaftLogEntry(3L, 3L, "NO_OP", "{}")
            ));

            // Restart
            ConnectionFactory factory2 = connectionFactory();
            JdbcRaftStorage storage2 = new JdbcRaftStorage(factory2);
            DbStateLoader loader = new DbStateLoader(storage2, new JdbcBookingRepository(factory2), factory2);

            NodeStartupState state = loader.load();
            assertEquals(3L, state.currentTerm());
            assertEquals(3L, state.lastLogTerm());
            assertTrue(state.currentTerm() >= 3L);
            assertTrue(state.currentTerm() >= state.lastLogTerm());
        }

        @Test
        void nodeStartupLoadsD13BeforeOpeningSockets() throws Exception {
            runMigrations(true);
            var config = nodeConfig();
            try (var node = new vn.nhom6.railway.networking.NodeMain(config)) {
                node.start();
                assertTrue(node.isRunning());
                assertEquals(BootstrapType.GENESIS, node.getStartupState().bootstrapType());
                assertEquals(0L, node.getStartupState().initialCommitIndex());
            }
        }

        @Test
        void missingSchemaPreventsNodeStartupAndSocketBinding() throws Exception {
            var config = nodeConfig();
            try (var node = new vn.nhom6.railway.networking.NodeMain(config)) {
                assertThrows(IOException.class, node::start);
                org.junit.jupiter.api.Assertions.assertFalse(node.isRunning());
                assertEquals(-1, node.getClientPort());
                assertEquals(-1, node.getRaftPort());
            }
        }

        @Test
        void killAndRestartBackendPreservesAppliedDataAndUncommittedSuffix() throws Exception {
            // Durable fixture only: this does not claim a five-voter Raft commit.
            test02_statefulRestart_returnsRestartStartupState();
            DatabaseSnapshot before = captureSnapshot();
            String durableRows = durableRows();
            Process first = startBackendProcess();
            try {
                first.destroyForcibly(); // SIGKILL: bypass Java shutdown hooks
                assertTrue(first.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(before, captureSnapshot());
                Process restarted = startBackendProcess();
                try {
                    assertEquals(before, captureSnapshot());
                    assertEquals(durableRows, durableRows());
                    var state = new DbStateLoader(new JdbcRaftStorage(connectionFactory()),
                            new JdbcBookingRepository(connectionFactory()), connectionFactory()).load();
                    assertEquals(2L, state.initialCommitIndex());
                    assertEquals(3L, state.lastLogIndex());
                    assertEquals(1, countTable("tickets WHERE ticket_id = 't-1' AND status = 'ACTIVE'"));
                    assertEquals(1, countTable("request_results WHERE request_id = '11111111-1111-1111-1111-111111111111'"));
                } finally {
                    restarted.destroyForcibly();
                    assertTrue(restarted.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
                }
            } finally {
                first.destroyForcibly();
                first.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }

        private String durableRows() throws SQLException {
            StringBuilder rows = new StringBuilder();
            try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
                for (String table : List.of("raft_meta", "raft_log", "raft_apply_progress", "seats",
                        "tickets", "request_results", "business_events")) {
                    try (ResultSet rs = stmt.executeQuery("SELECT COALESCE(jsonb_agg(to_jsonb(t) "
                            + "ORDER BY to_jsonb(t)::text), '[]'::jsonb)::text FROM " + table + " t")) {
                        rs.next();
                        rows.append(table).append(':').append(rs.getString(1)).append('\n');
                    }
                }
            }
            return rows.toString();
        }

        private vn.nhom6.railway.networking.NodeConfig nodeConfig() {
            return new vn.nhom6.railway.networking.NodeConfig("node-1", "railway-demo-5",
                    "127.0.0.1", 0, "127.0.0.1", 0,
                    new DbConfig(host, port, dbName + "?currentSchema=" + schema, dbUser, dbPassword),
                    4, 1000);
        }

        private Process startBackendProcess() throws Exception {
            Path output = Files.createTempFile("railway-d13-backend-", ".log");
            output.toFile().deleteOnExit();
            var builder = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"),
                    "vn.nhom6.railway.node.NodeMain");
            var env = builder.environment();
            env.remove("NODE_CLIENT_BIND");
            env.remove("NODE_PEER_BIND");
            env.put("NODE_ID", "node-1");
            env.put("CLIENT_HOST", "127.0.0.1");
            env.put("RAFT_HOST", "127.0.0.1");
            env.put("CLIENT_PORT", "0");
            env.put("RAFT_PORT", "0");
            env.put("DB_HOST", host);
            env.put("DB_PORT", Integer.toString(port));
            env.put("DB_NAME", dbName + "?currentSchema=" + schema);
            env.put("DB_USER", dbUser);
            env.put("DB_PASSWORD", dbPassword);
            Process process = builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
            while (process.isAlive() && System.nanoTime() < deadline) {
                if (Files.readString(output).contains("[READY] node-1")) return process;
                Thread.sleep(25);
            }
            process.destroyForcibly();
            process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
            throw new AssertionError("Backend did not become ready: " + Files.readString(output));
        }

        // -----------------------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------------------
        private BookingRepository createFaultyRepository(AtomicReference<SqlFault> fault) {
            try {
                Class<?> providerClass = Class.forName("vn.nhom6.railway.persistence.JdbcBookingRepository$ConnectionProvider");
                var ctor = JdbcBookingRepository.class.getDeclaredConstructor(providerClass);
                ctor.setAccessible(true);
                Object provider = Proxy.newProxyInstance(
                        providerClass.getClassLoader(),
                        new Class<?>[]{providerClass},
                        (proxy, method, args) -> {
                            Connection conn = schemaConnection();
                            return wrapConnection(conn, fault);
                        });
                return (BookingRepository) ctor.newInstance(provider);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create faulty repository", e);
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

        private void executeSql(String sql) throws SQLException {
            try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }

        private int countTable(String tableNameOrCondition) {
            try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableNameOrCondition)) {
                rs.next();
                return rs.getInt(1);
            } catch (SQLException e) {
                throw new IllegalStateException("Count failed for " + tableNameOrCondition, e);
            }
        }

        private int countTablesInCurrentSchema() {
            try (Connection conn = schemaConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?")) {
                stmt.setString(1, schema);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Table query failed", e);
            }
        }

        private DatabaseSnapshot captureSnapshot() throws SQLException {
            try (Connection conn = schemaConnection()) {
                int seats = countTable("seats");
                int bookedSeats = countTable("seats WHERE status = 'BOOKED'");
                int tickets = countTable("tickets");
                int activeTickets = countTable("tickets WHERE status = 'ACTIVE'");
                int requestResults = countTable("request_results");
                int businessEvents = countTable("business_events");
                int raftLog = countTable("raft_log");

                long term = 0;
                String vote = null;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT current_term, voted_for FROM raft_meta WHERE singleton = 1")) {
                    if (rs.next()) {
                        term = rs.getLong(1);
                        vote = rs.getString(2);
                    }
                }

                long lastApplied = 0;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1")) {
                    if (rs.next()) {
                        lastApplied = rs.getLong(1);
                    }
                }

                return new DatabaseSnapshot(seats, bookedSeats, tickets, activeTickets,
                        requestResults, businessEvents, raftLog, term, vote, lastApplied);
            }
        }
    }

    private record DatabaseSnapshot(
            int seatsCount,
            int bookedSeatsCount,
            int ticketsCount,
            int activeTicketsCount,
            int requestResultsCount,
            int businessEventsCount,
            int raftLogCount,
            long currentTerm,
            String votedFor,
            long lastApplied
    ) { }

    // ---------------------------------------------------------------------------------------
    // In-memory test fake for Group A
    // ---------------------------------------------------------------------------------------
    private static final class FakeRaftStorage implements RaftStorage {
        private final long currentTerm;
        private final String votedFor;
        private final List<RaftLogEntry> log;
        private final long lastApplied;

        FakeRaftStorage(long currentTerm, String votedFor, List<RaftLogEntry> log, long lastApplied) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.log = log;
            this.lastApplied = lastApplied;
        }

        @Override
        public PersistentState load() throws RaftStorageException {
            return new PersistentState(currentTerm, votedFor, log, lastApplied);
        }

        @Override
        public void saveTermAndVote(long currentTerm, String votedFor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long loadLastApplied() {
            return lastApplied;
        }
    }

    // ---------------------------------------------------------------------------------------
    // Fault injection proxies
    // ---------------------------------------------------------------------------------------
    private record SqlFault(String sqlPattern, boolean afterExecution, AtomicBoolean triggered) {
        private SqlFault(String sqlPattern, boolean afterExecution) {
            this(sqlPattern, afterExecution, new AtomicBoolean());
        }

        private boolean matches(String sql) {
            return sql.toUpperCase(java.util.Locale.ROOT)
                    .contains(sqlPattern.toUpperCase(java.util.Locale.ROOT));
        }
    }

    private static Connection wrapConnection(Connection connection, AtomicReference<SqlFault> fault) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
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
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    SqlFault activeFault = fault == null ? null : fault.get();
                    boolean execution = "executeUpdate".equals(method.getName())
                            || "executeLargeUpdate".equals(method.getName())
                            || "execute".equals(method.getName())
                            || "executeBatch".equals(method.getName());
                    if (execution && activeFault != null && activeFault.matches(sql)
                            && !activeFault.afterExecution()) {
                        activeFault.triggered().set(true);
                        throw new SQLException("Injected fault for " + activeFault.sqlPattern());
                    }
                    Object result = invoke(statement, method, arguments);
                    if (execution && activeFault != null && activeFault.matches(sql)
                            && activeFault.afterExecution()) {
                        activeFault.triggered().set(true);
                        throw new SQLException("Injected fault after " + activeFault.sqlPattern());
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

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}

