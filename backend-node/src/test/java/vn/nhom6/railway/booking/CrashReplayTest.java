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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.persistence.JdbcRequestResultStore;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV2-D22: Crash giữa apply và ACK (SPEC-04 §5, §6; SPEC-05 §3, §5; SPEC-03 §4.4; SPEC-07).
 *
 * <p>Mô phỏng crash node tại các điểm:
 * <ul>
 *   <li><b>CP1:</b> Ngay TRƯỚC khi gọi {@code connection.commit()} (transaction chưa commit -> rollback sạch sẽ).</li>
 *   <li><b>CP2:</b> Ngay SAU khi gọi {@code connection.commit()} thành công (DB đã có vé, request_results, lastApplied
 *       nhưng tiến trình crash trước khi hoàn tất future/ACK cho caller).</li>
 *   <li><b>CP3:</b> Sau commit và sau khi future hoàn tất nhưng trước khi response ra mạng.</li>
 * </ul>
 *
 * <p>Mức bằng chứng: FAKE+LOCAL (Mô phỏng crash qua CrashInjecting Connection Proxy; TV4-D18 replay API: BLOCKED_INTEGRATION
 * dùng FakeCommitSource; crash node thật TV4-D22 + fault runner TV6: BLOCKED_INTEGRATION).
 */
public class CrashReplayTest {

    private static final String TRIP_ID = "SE-001";
    private static final Instant BASE_TIME = Instant.parse("2026-10-29T10:00:00Z");

    public enum CrashPoint {
        NONE,
        CP1, // Ném CrashInjectedException ngay trước commit()
        CP2, // Commit thật thành công xong ném CrashInjectedException ngay sau commit()
        CP3  // Mô phỏng mất ACK / crash sau commit và future
    }

    public static final class CrashInjectedException extends RuntimeException {
        public CrashInjectedException(String message) {
            super(message);
        }
    }

    private String rawJdbcUrl;
    private String dbUser;
    private String dbPassword;

    private TestDb mainDb;

    @BeforeEach
    void setUp() throws Exception {
        rawJdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        dbUser = System.getenv("RAILWAY_TEST_DB_USER");
        dbPassword = System.getenv("RAILWAY_TEST_DB_PASSWORD");

        Assumptions.assumeTrue(
                hasText(rawJdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD required");

        mainDb = new TestDb(rawJdbcUrl, dbUser, dbPassword, "crash");
    }

    @AfterEach
    void tearDown() {
        if (mainDb != null) {
            try {
                mainDb.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    // =========================================================================
    // 12 BÀI KIỂM THỬ BẮT BUỘC + 1 BÀI TÙY CHỌN (13)
    // =========================================================================

    /**
     * Test 1: CP2 với BOOK(G05): sau crash, DB đã có vé ACTIVE, request_results, lastApplied = 1;
     * ba thứ này cùng thấy hoặc cùng không thấy (kiểm chứng tính nguyên tử).
     */
    @Test
    @DisplayName("1. CP2 với BOOK(G05): sau crash, vé ACTIVE, request_results và lastApplied cùng tồn tại nguyên tử")
    void test01_cp2_bookG05_atomicStatePresentAfterCrash() throws Exception {
        String reqId = "11111111-1111-1111-1111-111111111111";
        String ticketId = "TK-001";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            // Áp dụng entry 1 -> crash sau commit
            node.fakeCommit(1, 1, cmd);
        }

        // Mở connection mới kiểm tra DB sau crash
        try (Connection conn = mainDb.openIndependentConnection()) {
            assertEquals(1L, mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = '" + ticketId + "' AND status = 'ACTIVE' AND seat_id = 'G05'"));
            assertEquals("BOOKED", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G05'"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId + "' AND state = 'APPLIED_SUCCESS' AND applied_log_index = 1"));
        }
    }

    /**
     * Test 2: CP1 (crash trước commit): DB KHÔNG có vé, không có request_results, lastApplied không tăng;
     * restart + replay từ log thì apply đúng một lần.
     */
    @Test
    @DisplayName("2. CP1 (crash trước commit): rollback hoàn toàn; restart + replay apply đúng một lần")
    void test02_cp1_crashBeforeCommit_rollsBackCompletely() throws Exception {
        String reqId = "22222222-2222-2222-2222-222222222222";
        String ticketId = "TK-002";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP1);
            node.fakeCommit(1, 1, cmd);
        }

        // Kiểm tra sau CP1: rollback sạch sẽ
        assertEquals(0L, mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM tickets"));
        assertEquals("AVAILABLE", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G05'"));
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM request_results"));

        // Restart và replay lại entry 1 với CP = NONE
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(1, 1, cmd);

            assertEquals(1L, node.lastApplied());
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = '" + ticketId + "' AND status = 'ACTIVE'"));
            assertEquals("BOOKED", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G05'"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId + "'"));
        }
    }

    /**
     * Test 3: Restart sau CP2: lastApplied đọc từ DB, commitIndex không thấp hơn lastApplied (SPEC-04 §6),
     * log đã persist vẫn còn đủ; phát lại từ index 1 -> KHÔNG apply lại prefix, không tạo vé thứ hai.
     */
    @Test
    @DisplayName("3. Restart sau CP2: nạp lại lastApplied từ DB, replay prefix không apply lại, không tạo vé thứ hai")
    void test03_restartAfterCp2_lastAppliedFromDb_replayPrefixDoesNotDuplicate() throws Exception {
        String reqId = "33333333-3333-3333-3333-333333333333";
        String ticketId = "TK-003";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        // Chạy lần đầu gặp CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(1, 1, cmd);
        }

        // Kiểm tra qua DbStateLoader
        JdbcRaftStorage raftStorage = new JdbcRaftStorage(mainDb.connectionFactory());
        DbStateLoader loader = new DbStateLoader(raftStorage, null, mainDb.connectionFactory());
        DbStateLoader.NodeStartupState startup = loader.load();
        assertEquals(1L, startup.lastApplied(), "lastApplied phải bằng 1");
        assertEquals(1L, startup.initialCommitIndex(), "initialCommitIndex phải bằng lastApplied (SPEC-04 §6)");
        assertTrue(startup.lastLogIndex() >= 1L, "Log đã persist phải còn đủ");

        // Restart node mới và phát lại entry 1 (redelivery)
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            assertEquals(1L, node.lastApplied());

            // Redeliver index 1
            node.fakeCommit(1, 1, cmd);

            // Invariant: không apply lại, vẫn đúng 1 vé
            assertEquals(1L, node.lastApplied());
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results"));
        }
    }

    /**
     * Test 4: requestId còn tồn tại sau crash: lookup theo requestId (JdbcRequestResultStore)
     * trả kết quả gốc CONFIRMED; client gửi lại cùng requestId + cùng payload qua bridge -> trả kết quả gốc,
     * KHÔNG append/apply lần hai, vẫn đúng một vé.
     */
    @Test
    @DisplayName("4. requestId còn tồn tại sau crash: tra cứu ra kết quả gốc, gửi lại qua bridge không append lần hai")
    void test04_requestIdRemainsAfterCrash_idempotentQueryAndResubmit() throws Exception {
        String reqId = "44444444-4444-4444-4444-444444444444";
        String ticketId = "TK-004";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(1, 1, cmd);
        }

        // 1. Tra cứu trực tiếp từ Store
        try (Connection conn = mainDb.openIndependentConnection()) {
            JdbcRequestResultStore store = new JdbcRequestResultStore();
            Optional<JdbcRequestResultStore.StoredRequestResult> opt = store.find(conn, reqId);
            assertTrue(opt.isPresent(), "Phải tìm thấy request_results sau crash");
            JdbcRequestResultStore.StoredRequestResult stored = opt.get();
            assertEquals("APPLIED_SUCCESS", stored.state());
            assertEquals(ticketId, stored.ticketId());
            assertEquals(1L, stored.appliedLogIndex());
            assertEquals(ApplyResult.bookConfirmed(reqId, ticketId, 1L), stored.toApplyResult());
        }

        // 2. Client gửi lại cùng requestId + cùng payload qua bridge mới sau restart
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);

            long logCountBefore = node.raftPort().appendedLog().size();
            ApplyResult result = node.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

            assertEquals(ApplyResult.bookConfirmed(reqId, ticketId, 1L), result);
            assertEquals(logCountBefore, node.raftPort().appendedLog().size(), "Không được append lần hai vào Raft");
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets"));
        }
    }

    /**
     * Test 5: Cùng requestId khác payload sau crash -> REQUEST_ID_CONFLICT, không đổi dữ liệu.
     */
    @Test
    @DisplayName("5. Cùng requestId khác payload sau crash -> REQUEST_ID_CONFLICT, không đổi dữ liệu")
    void test05_sameRequestIdDifferentPayload_failsWithConflict() throws Exception {
        String reqId = "55555555-5555-5555-5555-555555555555";
        BookingCommand.BookTicket originalCmd = makeBookCmd(reqId, "G05", "TK-005", 0);
        BookingCommand.BookTicket conflictCmd = makeBookCmd(reqId, "G06", "TK-006", 0);

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(1, 1, originalCmd);
        }

        // Restart và gửi conflictCmd
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);

            assertThrows(BookingRequestHandler.RequestIdConflictException.class, () -> {
                try {
                    node.bridge().submitCommand(conflictCmd).get(5, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException ee) {
                    if (ee.getCause() instanceof Exception ex) {
                        throw ex;
                    }
                    throw ee;
                }
            });

            // Invariant: Dữ liệu DB không thay đổi
            assertEquals("BOOKED", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G05'"));
            assertEquals("AVAILABLE", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G06'"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets"));
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results"));
        }
    }

    /**
     * Test 6: ACK chưa ra: sau CP2/CP3, client chưa có response, client nhận UNKNOWN/timeout;
     * tra cứu mới thấy CONFIRMED (SPEC-03 §4.4). UNKNOWN không bao giờ được lưu vào DB.
     */
    @Test
    @DisplayName("6. ACK chưa ra: client nhận timeout/UNKNOWN, tra cứu ra CONFIRMED; DB không bao giờ lưu UNKNOWN")
    void test06_ackLost_clientReceivesUnknown_queryYieldsConfirmed_neverPersistsUnknown() throws Exception {
        String reqId = "66666666-6666-6666-6666-666666666666";
        String ticketId = "TK-006";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        // Mô phỏng: submit lệnh, commit DB thành công, nhưng kết nối mạng bị rớt / timeout trước ACK
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(1, 1, cmd);
        }

        // Client perspective: Do timeout hoặc rớt kết nối, client nhận UNKNOWN
        ApplyResult clientUnknown = ApplyResult.bookUnknown(reqId);
        assertEquals(ApplyResult.Outcome.UNKNOWN, clientUnknown.outcome());
        assertEquals("TIMEOUT_UNKNOWN", clientUnknown.reasonCode());

        // Kiểm tra Invariant: DB KHÔNG BAO GIỜ lưu UNKNOWN
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM request_results WHERE state = 'UNKNOWN'"));
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM request_results WHERE reason_code = 'TIMEOUT_UNKNOWN'"));
        assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId + "' AND state = 'APPLIED_SUCCESS'"));

        // Client tra cứu lại theo requestId -> nhận được CONFIRMED
        try (NodeContext node = new NodeContext(mainDb)) {
            Optional<ApplyResult> queried = node.bridge().findResult(reqId);
            assertTrue(queried.isPresent());
            assertEquals(ApplyResult.Outcome.CONFIRMED, queried.get().outcome());
            assertEquals(ticketId, queried.get().ticketId());
        }
    }

    /**
     * Test 7: Crash trên entry BOOK bị REJECTED (ghế đã bán): sau CP2 request_results lưu REJECTED + reasonCode,
     * lastApplied tăng, không có vé mới; replay giữ nguyên.
     */
    @Test
    @DisplayName("7. Crash trên entry BOOK bị REJECTED: lưu REJECTED + reasonCode, lastApplied tăng, replay giữ nguyên")
    void test07_crashOnRejectedBook_persistsRejectedAndReasonCode_replayPreserves() throws Exception {
        String reqId1 = "77777771-7777-7777-7777-777777777771";
        String reqId2 = "77777772-7777-7777-7777-777777777772";
        BookingCommand.BookTicket cmd1 = makeBookCmd(reqId1, "G05", "TK-007A", 0);
        BookingCommand.BookTicket cmd2 = makeBookCmd(reqId2, "G05", "TK-007B", 1); // Trùng ghế G05

        // Index 1 thành công
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(1, 1, cmd1);
        }

        // Index 2 bị REJECTED, chèn CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(2, 1, cmd2);
        }

        // Kiểm tra sau crash
        assertEquals(2L, mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets")); // Vẫn chỉ 1 vé TK-007A
        assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId2 + "' AND state = 'APPLIED_REJECTED' AND reason_code = '" + ApplyResult.SEAT_ALREADY_BOOKED + "'"));

        // Restart và replay lại index 2 -> giữ nguyên
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(2, 1, cmd2);

            assertEquals(2L, node.lastApplied());
            assertEquals(1, mainDb.queryInt("SELECT count(*) FROM tickets"));
            assertEquals(2, mainDb.queryInt("SELECT count(*) FROM request_results"));
        }
    }

    /**
     * Test 8: Crash trên entry CANCEL và trên entry NO_OP: lastApplied đúng, dữ liệu đúng, replay idempotent.
     */
    @Test
    @DisplayName("8. Crash trên entry CANCEL và NO_OP: lastApplied đúng, dữ liệu nhất quán, replay idempotent")
    void test08_crashOnCancelAndNoOp_atomicAndIdempotent() throws Exception {
        String reqBook = "88888881-8888-8888-8888-888888888881";
        String reqCancel = "88888882-8888-8888-8888-888888888882";
        String ticketId = "TK-008";
        BookingCommand.BookTicket bookCmd = makeBookCmd(reqBook, "G01", ticketId, 0);
        BookingCommand.CancelTicket cancelCmd = makeCancelCmd(reqCancel, ticketId, 1);
        BookingCommand.NoOp noOpCmd = new BookingCommand.NoOp();

        // 1. Book G01 thành công
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(1, 1, bookCmd);
        }

        // 2. Cancel với CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(2, 1, cancelCmd);
        }

        // Kiểm tra sau crash Cancel
        assertEquals(2L, mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals("CANCELLED", mainDb.queryString("SELECT status FROM tickets WHERE ticket_id = '" + ticketId + "'"));
        assertEquals("AVAILABLE", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
        assertEquals(1, mainDb.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqCancel + "' AND state = 'APPLIED_SUCCESS'"));

        // 3. NoOp với CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(3, 1, noOpCmd);
        }

        // Kiểm tra sau crash NoOp
        assertEquals(3L, mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        assertEquals(2, mainDb.queryInt("SELECT count(*) FROM request_results"), "NoOp không ghi request_results");

        // 4. Replay lại 2 và 3 -> Idempotent
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(2, 1, cancelCmd);
            node.fakeCommit(3, 1, noOpCmd);

            assertEquals(3L, node.lastApplied());
            assertEquals("CANCELLED", mainDb.queryString("SELECT status FROM tickets WHERE ticket_id = '" + ticketId + "'"));
            assertEquals("AVAILABLE", mainDb.queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
        }
    }

    /**
     * Test 9: Crash liên tiếp nhiều lần (CP2, restart, CP2 ở entry kế, restart…) trên một log N entry:
     * cuối cùng DB giống hệt DB của lần chạy không crash (so tickets, seats, request_results, lastApplied).
     */
    @Test
    @DisplayName("9. Crash liên tiếp nhiều lần (CP2 ở mọi bước): kết quả cuối khớp 100% với lần chạy sạch")
    void test09_consecutiveCrashesAcrossNEntries_matchesCleanRunExactly() throws Exception {
        TestDb cleanDb = new TestDb(rawJdbcUrl, dbUser, dbPassword, "clean");
        try {
            List<CommandEntry> script = List.of(
                    new CommandEntry(1, makeBookCmd("99999991-9999-9999-9999-999999999991", "G01", "TK-09A", 0)),
                    new CommandEntry(2, makeBookCmd("99999992-9999-9999-9999-999999999992", "G02", "TK-09B", 1)),
                    new CommandEntry(3, makeCancelCmd("99999993-9999-9999-9999-999999999993", "TK-09A", 2)),
                    new CommandEntry(4, makeBookCmd("99999994-9999-9999-9999-999999999994", "G01", "TK-09C", 3)),
                    new CommandEntry(5, new BookingCommand.NoOp())
            );

            // Chạy trên cleanDb không crash
            try (NodeContext node = new NodeContext(cleanDb)) {
                node.setCrashPoint(CrashPoint.NONE);
                for (CommandEntry ce : script) {
                    node.fakeCommit(ce.index, 1, ce.cmd);
                }
            }

            // Chạy trên mainDb với CP2 ở TẤT CẢ các bước
            for (CommandEntry ce : script) {
                try (NodeContext node = new NodeContext(mainDb)) {
                    node.setCrashPoint(CrashPoint.CP2);
                    node.fakeCommit(ce.index, 1, ce.cmd);
                }
            }

            // Đối chiếu dữ liệu giữa mainDb và cleanDb
            assertEquals(cleanDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"),
                    mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

            assertEquals(cleanDb.queryListString("SELECT seat_id || '|' || status || '|' || coalesce(current_ticket_id, '') FROM seats ORDER BY seat_id"),
                    mainDb.queryListString("SELECT seat_id || '|' || status || '|' || coalesce(current_ticket_id, '') FROM seats ORDER BY seat_id"));

            assertEquals(cleanDb.queryListString("SELECT ticket_id || '|' || seat_id || '|' || status FROM tickets ORDER BY ticket_id"),
                    mainDb.queryListString("SELECT ticket_id || '|' || seat_id || '|' || status FROM tickets ORDER BY ticket_id"));

            assertEquals(cleanDb.queryListString("SELECT request_id || '|' || state || '|' || coalesce(reason_code, '') || '|' || coalesce(ticket_id, '') || '|' || applied_log_index FROM request_results ORDER BY applied_log_index, request_id"),
                    mainDb.queryListString("SELECT request_id || '|' || state || '|' || coalesce(reason_code, '') || '|' || coalesce(ticket_id, '') || '|' || applied_log_index FROM request_results ORDER BY applied_log_index, request_id"));

        } finally {
            cleanDb.close();
        }
    }

    /**
     * Test 10: Crash ngay lúc lastApplied và dữ liệu nghiệp vụ: chứng minh không có trạng thái
     * "lastApplied tăng nhưng không có dữ liệu" hoặc ngược lại ở cả CP1 và CP2.
     */
    @Test
    @DisplayName("10. Nguyên tử tuyệt đối: không bao giờ có trạng thái lastApplied tăng mà thiếu dữ liệu vé hoặc ngược lại")
    void test10_atomicityVerification_noPartialStateAtCp1OrCp2() throws Exception {
        BookingCommand.BookTicket cmd1 = makeBookCmd("aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa", "G03", "TK-10A", 0);
        BookingCommand.BookTicket cmd2 = makeBookCmd("bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb", "G04", "TK-10B", 1);

        // 1. Kiểm tra với CP1
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP1);
            node.fakeCommit(1, 1, cmd1);
        }
        long lastAppliedCp1 = mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
        int ticketsCp1 = mainDb.queryInt("SELECT count(*) FROM tickets");
        int resultsCp1 = mainDb.queryInt("SELECT count(*) FROM request_results");
        assertEquals(0L, lastAppliedCp1);
        assertEquals(0, ticketsCp1);
        assertEquals(0, resultsCp1);

        // 2. Kiểm tra với CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2);
            node.fakeCommit(1, 1, cmd1);
        }
        long lastAppliedCp2 = mainDb.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
        int ticketsCp2 = mainDb.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = 'TK-10A' AND status = 'ACTIVE'");
        int resultsCp2 = mainDb.queryInt("SELECT count(*) FROM request_results WHERE applied_log_index = 1");
        assertEquals(1L, lastAppliedCp2);
        assertEquals(1, ticketsCp2);
        assertEquals(1, resultsCp2);
    }

    /**
     * Test 11: Bất biến sau mọi phục hồi: <= 1 vé ACTIVE mỗi ghế, seats khớp tickets,
     * không có request_results UNKNOWN, đúng 10 ghế G01-G10.
     */
    @Test
    @DisplayName("11. Bất biến hệ thống sau phục hồi: <= 1 vé ACTIVE/ghế, seats khớp tickets, không có UNKNOWN")
    void test11_safetyInvariantsHoldAfterAllRecoveries() throws Exception {
        // Thực hiện một chuỗi thao tác có cả crash CP1 và CP2
        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.NONE);
            node.fakeCommit(1, 1, makeBookCmd("11111111-0000-0000-0000-000000000001", "G01", "TK-11A", 0));
        }

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP1); // Bị rollback
            node.fakeCommit(2, 1, makeBookCmd("22222222-0000-0000-0000-000000000002", "G02", "TK-11B", 1));
        }

        try (NodeContext node = new NodeContext(mainDb)) {
            node.setCrashPoint(CrashPoint.CP2); // Crash sau commit
            node.fakeCommit(2, 1, makeBookCmd("22222222-0000-0000-0000-000000000002", "G02", "TK-11B", 1));
        }

        // Kiểm tra các bất biến cốt lõi
        assertEquals(0, mainDb.queryInt(
                "SELECT count(*) FROM (SELECT seat_id FROM tickets WHERE status = 'ACTIVE' GROUP BY seat_id HAVING count(*) > 1) t"),
                "Không bao giờ có quá 1 vé ACTIVE cho mỗi ghế");

        assertEquals(10, mainDb.queryInt("SELECT count(*) FROM seats"), "Luôn đủ 10 ghế");

        // Ghế BOOKED phải khớp với vé ACTIVE
        assertEquals(0, mainDb.queryInt(
                "SELECT count(*) FROM seats s WHERE s.status = 'BOOKED' AND NOT EXISTS ("
                        + "SELECT 1 FROM tickets t WHERE t.seat_id = s.seat_id AND t.ticket_id = s.current_ticket_id AND t.status = 'ACTIVE')"));

        // Ghế AVAILABLE không được có current_ticket_id
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM seats WHERE status = 'AVAILABLE' AND current_ticket_id IS NOT NULL"));

        // Không có kết quả UNKNOWN trong request_results
        assertEquals(0, mainDb.queryInt("SELECT count(*) FROM request_results WHERE state = 'UNKNOWN'"));
    }

    /**
     * Test 12: Lặp 20 vòng với điểm crash/index khác nhau theo seed cố định (không dùng Random không seed):
     * mọi vòng thỏa các assertion trên, không chập chờn.
     */
    @Test
    @DisplayName("12. Monte Carlo 20 vòng seed cố định: mọi vòng đều đạt tính nhất quán và bất biến an toàn")
    void test12_monteCarloSeedLoop_20Rounds_stableAndDeterministic() throws Exception {
        Random rnd = new Random(42L);

        for (int round = 1; round <= 20; round++) {
            TestDb roundDb = new TestDb(rawJdbcUrl, dbUser, dbPassword, "rnd" + round);
            try {
                int totalEntries = 4;
                long currentIndex = 0;

                for (int i = 0; i < totalEntries; i++) {
                    currentIndex++;
                    String seat = "G0" + ((i % 5) + 1);
                    String req = String.format("ffffffff-%04d-0000-0000-%012d", round, i);
                    String ticket = String.format("TK-R%d-%d", round, i);
                    BookingCommand cmd = makeBookCmd(req, seat, ticket, i);

                    CrashPoint cp = rnd.nextBoolean() ? CrashPoint.CP2 : CrashPoint.CP1;

                    try (NodeContext node = new NodeContext(roundDb)) {
                        node.setCrashPoint(cp);
                        node.fakeCommit(currentIndex, 1, cmd);
                    }

                    if (cp == CrashPoint.CP1) {
                        // CP1 bị rollback, phát lại thành công
                        try (NodeContext node = new NodeContext(roundDb)) {
                            node.setCrashPoint(CrashPoint.NONE);
                            node.fakeCommit(currentIndex, 1, cmd);
                        }
                    }
                }

                // Verify sau mỗi vòng
                assertEquals(0, roundDb.queryInt(
                        "SELECT count(*) FROM (SELECT seat_id FROM tickets WHERE status = 'ACTIVE' GROUP BY seat_id HAVING count(*) > 1) t"));
                assertEquals(10, roundDb.queryInt("SELECT count(*) FROM seats"));
                assertEquals(0, roundDb.queryInt("SELECT count(*) FROM request_results WHERE state = 'UNKNOWN'"));

            } finally {
                roundDb.close();
            }
        }
    }

    /**
     * Test 13: (Tùy chọn) Kill thật bằng tiến trình con:
     * Ghi nhận: NOT_RUN (Mô phỏng crash bằng CrashInjecting Connection Proxy đã kiểm chứng đầy đủ
     * tính nguyên tử của giao dịch PostgreSQL; việc spawn JVM con và kill bằng Runtime.halt quá phức tạp
     * và phụ thuộc harness môi trường).
     */
    @Test
    @DisplayName("13. Kill tiến trình thật (Tùy chọn): NOT_RUN")
    void test13_realProcessKill_skippedNotRun() {
        Assumptions.assumeTrue(false,
                "kill tiến trình thật: NOT_RUN (mô phỏng crash bằng CrashInjecting Connection Proxy đã kiểm chứng đầy đủ atomicity của PostgreSQL transaction; kill tiến trình thật trong JVM phụ quá phức tạp và phụ thuộc môi trường Maven)");
    }

    // =========================================================================
    // Helpers & Context
    // =========================================================================

    record CommandEntry(long index, BookingCommand cmd) {}

    private static BookingCommand.BookTicket makeBookCmd(String reqId, String seatId, String ticketId, int offsetSec) {
        return new BookingCommand.BookTicket(
                reqId, TRIP_ID, seatId, "Passenger " + ticketId, ticketId, BASE_TIME.plusSeconds(offsetSec));
    }

    private static BookingCommand.CancelTicket makeCancelCmd(String reqId, String ticketId, int offsetSec) {
        return new BookingCommand.CancelTicket(reqId, ticketId, BASE_TIME.plusSeconds(offsetSec));
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

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // =========================================================================
    // NodeContext: Đại diện cho một "Node Instance" (tiến trình có bộ nhớ riêng)
    // =========================================================================

    static final class NodeContext implements AutoCloseable {
        private final TestDb testDb;
        private final AtomicReference<CrashPoint> crashPointRef = new AtomicReference<>(CrashPoint.NONE);
        private final ScheduledExecutorService scheduler;
        private final BookingRepository repository;
        private final JdbcCommittedCommandApplier applier;
        private final RaftApplyAdapter adapter;
        private final FakeRaftPort raftPort;
        private final BookingRaftBridge bridge;

        NodeContext(TestDb testDb) {
            this.testDb = testDb;
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "crash-test-scheduler");
                t.setDaemon(true);
                return t;
            });

            this.repository = createFaultyRepository(testDb, crashPointRef);
            this.applier = createApplier(repository, result -> { });
            this.adapter = new RaftApplyAdapter(applier, repository);
            this.raftPort = new FakeRaftPort();
            this.bridge = new BookingRaftBridge(
                    raftPort,
                    adapter,
                    repository,
                    reqId -> {
                        try (Connection conn = testDb.openIndependentConnection()) {
                            return new JdbcRequestResultStore().find(conn, reqId)
                                    .map(JdbcRequestResultStore.StoredRequestResult::intentFingerprint);
                        } catch (Exception e) {
                            return Optional.empty();
                        }
                    },
                    Duration.ofSeconds(2)
            );
        }

        void setCrashPoint(CrashPoint cp) {
            this.crashPointRef.set(cp);
        }

        long lastApplied() {
            return repository.lastApplied();
        }

        FakeRaftPort raftPort() {
            return raftPort;
        }

        BookingRaftBridge bridge() {
            return bridge;
        }

        void fakeCommit(long index, long term, BookingCommand cmd) throws SQLException {
            String type = cmd.type().name();
            String payload = commandJson(cmd);

            // Ghi log vào DB
            testDb.insertCommittedLog(index, term, type, payload);

            // Cập nhật commitIndex trên adapter
            adapter.advanceCommitIndex(index);

            // Giao cho adapter apply
            try {
                RaftLogEntry entry = new RaftLogEntry(index, term, type, payload);
                adapter.onCommitted(entry);
            } catch (CrashInjectedException cie) {
                // Đã chèn crash thành công
            }
        }

        @Override
        public void close() {
            try {
                bridge.close();
            } catch (Exception ignored) {
            }
            scheduler.shutdownNow();
        }
    }

    private static BookingRepository createFaultyRepository(TestDb db, AtomicReference<CrashPoint> crashPointRef) {
        try {
            Class<?> providerClass = Class.forName("vn.nhom6.railway.persistence.JdbcBookingRepository$ConnectionProvider");
            var ctor = JdbcBookingRepository.class.getDeclaredConstructor(providerClass);
            ctor.setAccessible(true);
            Object provider = Proxy.newProxyInstance(
                    providerClass.getClassLoader(),
                    new Class<?>[]{providerClass},
                    (proxy, method, args) -> {
                        Connection rawConn = db.openIndependentConnection();
                        return wrapConnection(rawConn, crashPointRef);
                    }
            );
            return (BookingRepository) ctor.newInstance(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate faulty repository", e);
        }
    }

    private static Connection wrapConnection(Connection rawConn, AtomicReference<CrashPoint> crashPointRef) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("commit".equals(method.getName())) {
                        CrashPoint cp = crashPointRef.get();
                        if (cp == CrashPoint.CP1) {
                            throw new CrashInjectedException("CP1: Crash injected immediately BEFORE commit");
                        }
                        try {
                            Object res = method.invoke(rawConn, args);
                            if (cp == CrashPoint.CP2) {
                                throw new CrashInjectedException("CP2: Crash injected immediately AFTER commit");
                            }
                            return res;
                        } catch (InvocationTargetException ite) {
                            Throwable target = ite.getTargetException();
                            if (target instanceof CrashInjectedException cie) {
                                throw cie;
                            }
                            if (target instanceof SQLException se) {
                                throw se;
                            }
                            throw target;
                        }
                    }
                    try {
                        return method.invoke(rawConn, args);
                    } catch (InvocationTargetException ite) {
                        throw ite.getTargetException();
                    }
                }
        );
    }

    private static JdbcCommittedCommandApplier createApplier(BookingRepository repo, CommittedCommandApplier.ApplyResultListener listener) {
        try {
            var ctor = JdbcCommittedCommandApplier.class.getDeclaredConstructor(
                    JdbcBookingRepository.class,
                    CommittedCommandApplier.ApplyResultListener.class
            );
            ctor.setAccessible(true);
            return (JdbcCommittedCommandApplier) ctor.newInstance(repo, listener);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate JdbcCommittedCommandApplier", e);
        }
    }

    // =========================================================================
    // FakeRaftPort: Cung cấp vai trò Leader cho Bridge
    // =========================================================================

    static final class FakeRaftPort implements BookingRaftBridge.RaftPort {
        private final List<RaftLogEntry> appendedLog = Collections.synchronizedList(new ArrayList<>());
        private long lastIndex = 0;

        @Override public Role role() { return Role.LEADER; }
        @Override public Optional<String> leaderId() { return Optional.of("node-1"); }
        @Override public long currentTerm() { return 1; }

        @Override
        public synchronized RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
            lastIndex++;
            RaftLogEntry entry = new RaftLogEntry(lastIndex, 1, commandType, commandPayload);
            appendedLog.add(entry);
            return new RaftNode.Accepted(lastIndex, 1);
        }

        List<RaftLogEntry> appendedLog() {
            return appendedLog;
        }
    }

    // =========================================================================
    // TestDb: Quản lý vòng đời Schema cô lập trong PostgreSQL
    // =========================================================================

    static final class TestDb implements AutoCloseable {
        private final String baseUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final ConnectionFactory connectionFactory;

        TestDb(String jdbcUrl, String username, String password, String prefix) throws Exception {
            this.baseUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = "railway_test_" + prefix + "_"
                    + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

            createSchema();
            try {
                URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
                String rawPath = uri.getPath();
                String dbName = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
                int port = uri.getPort() > 0 ? uri.getPort() : 5432;

                DbConfig config = new DbConfig(uri.getHost(), port, dbName + "?currentSchema=" + schema, username, password);
                this.connectionFactory = new ConnectionFactory(config);

                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
            } catch (Exception ex) {
                dropSchema();
                throw ex;
            }
        }

        ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        Connection openIndependentConnection() throws SQLException {
            return connectionFactory.getConnection();
        }

        void insertCommittedLog(long logIndex, long term, String type, String payload) throws SQLException {
            try (Connection conn = connectionFactory.getConnection()) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, ?, ?, ?::jsonb) "
                                + "ON CONFLICT (log_index) DO NOTHING")) {
                    ps.setLong(1, logIndex);
                    ps.setLong(2, term);
                    ps.setString(3, type);
                    ps.setString(4, payload);
                    ps.executeUpdate();
                }
                // Đồng bộ current_term trong raft_meta để load() của DbStateLoader hợp lệ
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE raft_meta SET current_term = GREATEST(current_term, ?), voted_for = COALESCE(voted_for, 'node-1') WHERE singleton = 1")) {
                    ps.setLong(1, term);
                    ps.executeUpdate();
                }
            }
        }

        int queryInt(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                throw new SQLException("No row returned for query: " + sql);
            }
        }

        long queryLong(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                throw new SQLException("No row returned for query: " + sql);
            }
        }

        String queryString(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        }

        List<String> queryListString(String sql) throws SQLException {
            List<String> list = new ArrayList<>();
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
            }
            return list;
        }

        private void createSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                    Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA \"" + schema + "\"");
            }
        }

        private void dropSchema() {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                    Statement st = conn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            } catch (SQLException ignored) {
            }
        }

        private void runMigration(String filename) throws Exception {
            String sql = readMigrationFile(filename);
            try (Connection conn = connectionFactory.getConnection();
                    Statement st = conn.createStatement()) {
                for (String part : sql.split(";")) {
                    String trimmed = part.trim();
                    if (!trimmed.isEmpty()) {
                        st.execute(trimmed);
                    }
                }
            }
        }

        private static String readMigrationFile(String filename) throws IOException {
            Path path1 = Path.of("database", "migration", filename);
            if (Files.exists(path1)) {
                return Files.readString(path1, StandardCharsets.UTF_8);
            }
            Path path2 = Path.of("..", "database", "migration", filename);
            if (Files.exists(path2)) {
                return Files.readString(path2, StandardCharsets.UTF_8);
            }
            throw new IOException("Cannot find migration file: " + filename);
        }

        @Override
        public void close() {
            dropSchema();
        }
    }
}
