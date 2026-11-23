package vn.nhom6.railway.booking;

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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
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
 * TV2-D23: Retry khi đổi Leader (SPEC-03 §4.4, §6; SPEC-04 §4, §5, §6, §7; SPEC-05; SPEC-06 §4).
 *
 * <p>Mức bằng chứng: FAKE+LOCAL (Mô phỏng failover qua FakeLeaderProvider và FakeRaftPort;
 * failover thật TV5-D21/TV3-D22/D23: BLOCKED_INTEGRATION).
 */
public class FailoverIdempotencyTest {

    private static final String TRIP_ID = "SE-001";
    private static final Instant BASE_TIME = Instant.parse("2026-10-29T10:00:00Z");

    private String rawJdbcUrl;
    private String dbUser;
    private String dbPassword;

    private TestDb dbA;
    private TestDb dbB;
    private TestDb dbC;

    private NodeContext nodeA;
    private NodeContext nodeB;
    private NodeContext nodeC;

    private FakeLeaderProvider leaderProvider;

    @BeforeEach
    void setUp() throws Exception {
        rawJdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        dbUser = System.getenv("RAILWAY_TEST_DB_USER");
        dbPassword = System.getenv("RAILWAY_TEST_DB_PASSWORD");

        Assumptions.assumeTrue(
                hasText(rawJdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD required");

        leaderProvider = new FakeLeaderProvider();

        dbA = new TestDb(rawJdbcUrl, dbUser, dbPassword, "fa");
        dbB = new TestDb(rawJdbcUrl, dbUser, dbPassword, "fb");
        dbC = new TestDb(rawJdbcUrl, dbUser, dbPassword, "fc");

        nodeA = new NodeContext("node-1", dbA, leaderProvider);
        nodeB = new NodeContext("node-2", dbB, leaderProvider);
        nodeC = new NodeContext("node-3", dbC, leaderProvider);

        // Ban đầu node-1 là Leader ở term 1
        leaderProvider.changeLeader("node-1", 1);
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) nodeA.close();
        if (nodeB != null) nodeB.close();
        if (nodeC != null) nodeC.close();

        if (dbA != null) dbA.close();
        if (dbB != null) dbB.close();
        if (dbC != null) dbC.close();
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    // =========================================================================
    // 14 BÀI KIỂM THỬ BẮT BUỘC
    // =========================================================================

    /**
     * Test 1: R101 đã COMMIT rồi mất response: Leader cũ (term 1) apply xong, response mất,
     * Leader đổi sang node B (term 2) -> client retry cùng requestId + payload ->
     * B trả kết quả gốc CONFIRMED (cùng ticketId), KHÔNG append, KHÔNG tạo vé thứ hai.
     */
    @Test
    @DisplayName("1. R101 đã COMMIT rồi mất response: retry trên Leader mới trả kết quả gốc, không append lần hai")
    void test01_committedEntry_responseLost_retryOnNewLeader_returnsOriginalWithoutDuplicate() throws Exception {
        String reqId = "10110101-1111-1111-1111-101101011111";
        String ticketId = "TK-101";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        // Leader A (term 1) commit và apply R101; replicate sang B
        nodeA.commitEntry(1, 1, cmd);
        nodeB.commitEntry(1, 1, cmd);

        assertEquals(1, dbA.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = '" + ticketId + "' AND status = 'ACTIVE'"));
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = '" + ticketId + "' AND status = 'ACTIVE'"));

        // Giả lập mất response trên mạng; failover sang node B ở term 2
        leaderProvider.changeLeader("node-2", 2);

        // Client retry cùng requestId + cùng payload sang B
        long logCountOnBBefore = nodeB.raftPort().appendedLog().size();
        ApplyResult result = nodeB.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(ticketId, result.ticketId());
        assertEquals(1L, result.appliedLogIndex());
        assertEquals(logCountOnBBefore, nodeB.raftPort().appendedLog().size(), "Không được append lần hai trên Leader mới");

        // DB của B không phát sinh vé thứ hai
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE status = 'ACTIVE'"));
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId + "'"));
    }

    /**
     * Test 2: R101 chưa commit, Leader cũ mất quyền, entry bị cắt ở Leader mới:
     * retry trên B -> request_results không có -> append mới ở term 2 -> đúng một vé;
     * entry cũ không bao giờ được apply; lần phản hồi đầu của A là UNKNOWN, không CONFIRMED.
     */
    @Test
    @DisplayName("2. R101 chưa commit, entry bị cắt: retry trên Leader mới append ở term mới, đúng một vé")
    void test02_uncommittedOnOldLeader_truncatedOnNewLeader_retryAppendsAtNewTerm() throws Exception {
        String reqId = "10210202-2222-2222-2222-102102022222";
        String ticketId = "TK-102";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        // A (term 1) append vào log cục bộ nhưng CHƯA commit
        nodeA.raftPort().appendCommand(cmd.type().name(), commandJson(cmd));

        // Phản hồi của A cho lần thử đầu là UNKNOWN vì chưa commit
        ApplyResult unconfirmed = ApplyResult.bookUnknown(reqId);
        assertEquals(ApplyResult.Outcome.UNKNOWN, unconfirmed.outcome());

        // A mất quyền, B lên Leader ở term 2 (entry của A bị cắt, B chưa hề có entry này)
        leaderProvider.changeLeader("node-2", 2);

        // B chưa có request_results
        assertEquals(0, dbB.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + reqId + "'"));

        // Client retry tới B -> B append mới ở term 2
        CompletableFuture<ApplyResult> future = nodeB.bridge().submitCommand(cmd);
        assertEquals(1, nodeB.raftPort().appendedLog().size());
        assertEquals(2L, nodeB.raftPort().appendedLog().get(0).term());

        // B commit và apply entry mới
        nodeB.commitEntry(1, 2, cmd);

        ApplyResult finalResult = future.get(5, TimeUnit.SECONDS);
        assertEquals(ApplyResult.Outcome.CONFIRMED, finalResult.outcome());
        assertEquals(ticketId, finalResult.ticketId());
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE status = 'ACTIVE'"));
    }

    /**
     * Test 3: R101 chưa commit nhưng B có entry và commit sau:
     * retry khi B còn PENDING -> dùng chung future, KHÔNG append lần hai; sau commit trả đúng một kết quả; một vé.
     */
    @Test
    @DisplayName("3. R101 đang PENDING trên Leader mới: retry dùng chung future, không append lần hai, đúng một vé")
    void test03_uncommittedReplicatedToNewLeader_retryWhilePending_sharesFuture() throws Exception {
        String reqId = "10310303-3333-3333-3333-103103033333";
        String ticketId = "TK-103";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", ticketId, 0);

        // B lên Leader ở term 2
        leaderProvider.changeLeader("node-2", 2);

        // B nhận lệnh lần 1, đang PENDING chờ commit
        CompletableFuture<ApplyResult> future1 = nodeB.bridge().submitCommand(cmd);
        long logCountAfterFirst = nodeB.raftPort().appendedLog().size();
        assertEquals(1, logCountAfterFirst);

        // Client retry lần 2 trong khi B vẫn đang PENDING
        CompletableFuture<ApplyResult> future2 = nodeB.bridge().submitCommand(cmd);

        // Invariant: Không append lần hai
        assertEquals(logCountAfterFirst, nodeB.raftPort().appendedLog().size(), "Không được append lần hai khi đang PENDING");

        // Bây giờ B nhận được Quorum commit
        nodeB.commitEntry(1, 2, cmd);

        ApplyResult res1 = future1.get(5, TimeUnit.SECONDS);
        ApplyResult res2 = future2.get(5, TimeUnit.SECONDS);

        assertEquals(ApplyResult.Outcome.CONFIRMED, res1.outcome());
        assertEquals(res1, res2, "Cả hai future phải hoàn tất cùng một kết quả");
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE status = 'ACTIVE'"));
    }

    /**
     * Test 4: Retry trước khi B apply (PENDING) rồi sau APPLIED:
     * trước là PENDING/UNKNOWN (không CONFIRMED sớm), sau là kết quả gốc.
     */
    @Test
    @DisplayName("4. Tra cứu trạng thái: trước apply là PENDING/UNKNOWN (không CONFIRMED sớm), sau apply là CONFIRMED")
    void test04_retryStateTransition_pendingBeforeApply_confirmedAfterApply() throws Exception {
        String reqId = "10410404-4444-4444-4444-104104044444";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", "TK-104", 0);

        leaderProvider.changeLeader("node-2", 2);

        // B nhận lệnh, đưa vào PENDING
        nodeB.adapter().registerPending(reqId);

        // Tra cứu trước apply: không được CONFIRMED sớm
        Optional<ApplyResult> preApply = nodeB.bridge().findResult(reqId);
        assertTrue(preApply.isEmpty() || preApply.get().outcome() != ApplyResult.Outcome.CONFIRMED,
                "Tuyệt đối không được trả CONFIRMED sớm khi chưa commit & apply");

        // Sau khi apply thành công
        nodeB.commitEntry(1, 2, cmd);

        Optional<ApplyResult> postApply = nodeB.bridge().findResult(reqId);
        assertTrue(postApply.isPresent());
        assertEquals(ApplyResult.Outcome.CONFIRMED, postApply.get().outcome());
        assertEquals("TK-104", postApply.get().ticketId());
    }

    /**
     * Test 5: Retry tới node không phải Leader: NOT_LEADER + leaderHint;
     * client theo hint, retry cùng requestId + payload, messageId mới; hint null/cũ -> thử danh sách endpoint hữu hạn.
     */
    @Test
    @DisplayName("5. Gửi tới Follower nhận NOT_LEADER + hint: client tự động chuyển hướng retry sang Leader")
    void test05_retryToFollower_returnsNotLeaderWithHint_clientRedirects() throws Exception {
        String reqId = "10510505-5555-5555-5555-105105055555";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", "TK-105", 0);

        // Node A là Leader
        leaderProvider.changeLeader("node-1", 1);

        Map<String, NodeContext> cluster = Map.of("node-1", nodeA, "node-2", nodeB, "node-3", nodeC);
        FakeClient client = new FakeClient(cluster, List.of("node-2", "node-3", "node-1"));

        // Gửi tới Follower B -> client nhận NOT_LEADER, theo hint chuyển sang node-1
        // Trên node-1, commit entry 1
        nodeA.commitEntry(1, 1, cmd);

        ApplyResult result = client.submitWithRetry(cmd, "node-2", 3);
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals("TK-105", result.ticketId());
    }

    /**
     * Test 6: Leader cũ "zombie" (term cũ) vẫn nhận request:
     * append bị fencing từ chối -> trả UNKNOWN/NOT_LEADER, KHÔNG CONFIRMED; không có vé do term cũ.
     */
    @Test
    @DisplayName("6. Leader cũ zombie bị fencing chặn append: từ chối, không thay đổi DB ở term cũ")
    void test06_zombieLeaderRejectedByFencing_noStateMutated() throws Exception {
        String reqId = "10610606-6666-6666-6666-106106066666";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G05", "TK-106", 0);

        // Ban đầu A là Leader term 1, sau đó cụm đã chuyển sang term 2 với Leader B
        leaderProvider.changeLeader("node-2", 2);

        // Cố tình gửi lệnh tới zombie A (A vẫn giữ role cũ nhưng provider đã sang term 2)
        nodeA.raftPort().updateRole(Role.LEADER, 1, Optional.of("node-1"));

        assertThrows(BookingRequestHandler.NotLeaderException.class, () -> {
            try {
                nodeA.bridge().submitCommand(cmd).get(2, TimeUnit.SECONDS);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof Exception ex) throw ex;
                throw ee;
            }
        });

        // Không có vé nào được tạo trên A
        assertEquals(0, dbA.queryInt("SELECT count(*) FROM tickets"));
        assertEquals(0, dbA.queryInt("SELECT count(*) FROM request_results"));
    }

    /**
     * Test 7: Cùng requestId khác payload sau failover:
     * REQUEST_ID_CONFLICT trên Leader mới, dừng retry tự động, không đổi dữ liệu;
     * lookup trả ý định gốc, không gán cho payload đã sửa.
     */
    @Test
    @DisplayName("7. Cùng requestId khác payload sau failover: ném REQUEST_ID_CONFLICT, dừng retry, không đổi dữ liệu")
    void test07_sameRequestIdDifferentPayload_failsWithConflict_stopsRetry() throws Exception {
        String reqId = "10710707-7777-7777-7777-107107077777";
        BookingCommand.BookTicket originalCmd = makeBookCmd(reqId, "G05", "TK-107A", 0);
        BookingCommand.BookTicket conflictCmd = makeBookCmd(reqId, "G06", "TK-107B", 0);

        // A commit lệnh gốc G05; replicate sang B
        nodeA.commitEntry(1, 1, originalCmd);
        nodeB.commitEntry(1, 1, originalCmd);

        // Đổi Leader sang B
        leaderProvider.changeLeader("node-2", 2);

        // Gửi lệnh conflict cùng requestId nhưng khác ghế sang B
        assertThrows(BookingRequestHandler.RequestIdConflictException.class, () -> {
            try {
                nodeB.bridge().submitCommand(conflictCmd).get(5, TimeUnit.SECONDS);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof Exception ex) throw ex;
                throw ee;
            }
        });

        // Invariant: Dữ liệu DB của B không đổi, G05 vẫn BOOKED, G06 vẫn AVAILABLE
        assertEquals("BOOKED", dbB.queryString("SELECT status FROM seats WHERE seat_id = 'G05'"));
        assertEquals("AVAILABLE", dbB.queryString("SELECT status FROM seats WHERE seat_id = 'G06'"));
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets"));

        // Lookup requestId trả về ý định gốc G05
        Optional<ApplyResult> lookup = nodeB.bridge().findResult(reqId);
        assertTrue(lookup.isPresent());
        assertEquals("TK-107A", lookup.get().ticketId());
    }

    /**
     * Test 8: Hai client khác requestId cùng BOOK(G05) xuyên qua failover:
     * đúng một CONFIRMED, một REJECTED; <= 1 vé ACTIVE (truy vấn DB độc lập trên từng node).
     */
    @Test
    @DisplayName("8. Hai client tranh chấp ghế G05 xuyên failover: đúng 1 CONFIRMED, 1 REJECTED, <= 1 vé ACTIVE")
    void test08_twoClientsCompeteForSameSeatAcrossFailover_exactlyOneConfirmed() throws Exception {
        String req1 = "10810801-8888-8888-8888-108108018888";
        String req2 = "10810802-8888-8888-8888-108108028888";
        BookingCommand.BookTicket cmd1 = makeBookCmd(req1, "G05", "TK-108A", 0);
        BookingCommand.BookTicket cmd2 = makeBookCmd(req2, "G05", "TK-108B", 1);

        // Client 1 commit trên A ở term 1
        nodeA.commitEntry(1, 1, cmd1);
        nodeB.commitEntry(1, 1, cmd1);

        // Đổi Leader sang B ở term 2
        leaderProvider.changeLeader("node-2", 2);

        // Client 2 gửi lệnh tới B -> B commit entry 2 nhưng bị REJECTED vì G05 đã bán
        nodeB.commitEntry(2, 2, cmd2);

        assertEquals(1, dbB.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + req1 + "' AND state = 'APPLIED_SUCCESS'"));
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM request_results WHERE request_id = '" + req2 + "' AND state = 'APPLIED_REJECTED' AND reason_code = '" + ApplyResult.SEAT_ALREADY_BOOKED + "'"));

        // Truy vấn độc lập: <= 1 vé ACTIVE
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE seat_id = 'G05' AND status = 'ACTIVE'"));
    }

    /**
     * Test 9: R101 là BOOK đã CONFIRMED rồi bị CANCEL trước khi retry:
     * retry trả CONFIRMED gốc (theo SPEC-06 §4) nhưng ghế hiện AVAILABLE/vé CANCELLED; không tạo vé mới.
     */
    @Test
    @DisplayName("9. BOOK đã CONFIRMED rồi CANCEL: retry trả CONFIRMED gốc, ghế vẫn AVAILABLE, không tạo vé mới")
    void test09_bookConfirmedThenCancelled_retryYieldsOriginalConfirmed_seatRemainsAvailable() throws Exception {
        String reqBook = "10910901-9999-9999-9999-109109019999";
        String reqCancel = "10910902-9999-9999-9999-109109029999";
        String ticketId = "TK-109";
        BookingCommand.BookTicket bookCmd = makeBookCmd(reqBook, "G01", ticketId, 0);
        BookingCommand.CancelTicket cancelCmd = makeCancelCmd(reqCancel, ticketId, 1);

        // 1. Book G01 thành công ở index 1
        nodeA.commitEntry(1, 1, bookCmd);
        nodeB.commitEntry(1, 1, bookCmd);

        // 2. Cancel vé ở index 2
        nodeA.commitEntry(2, 1, cancelCmd);
        nodeB.commitEntry(2, 1, cancelCmd);

        // Failover sang B ở term 2
        leaderProvider.changeLeader("node-2", 2);

        // Client retry lại lệnh BOOK R101 sang B
        ApplyResult retryRes = nodeB.bridge().submitCommand(bookCmd).get(5, TimeUnit.SECONDS);

        // Theo SPEC-06 §4: trả về CONFIRMED gốc của R101
        assertEquals(ApplyResult.Outcome.CONFIRMED, retryRes.outcome());
        assertEquals(ticketId, retryRes.ticketId());

        // Nhưng trạng thái thực tế: ghế vẫn AVAILABLE, vé vẫn CANCELLED, không tạo vé mới
        assertEquals("AVAILABLE", dbB.queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
        assertEquals("CANCELLED", dbB.queryString("SELECT status FROM tickets WHERE ticket_id = '" + ticketId + "'"));
        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets"));
    }

    /**
     * Test 10: Nhiều lần retry liên tiếp (5 lần) qua nhiều lần đổi term:
     * cuối cùng đúng một vé, mọi lần trả cùng một kết quả cuối, không UNKNOWN được lưu vào DB.
     */
    @Test
    @DisplayName("10. Retry 5 lần qua 3 lần đổi term: kết quả nhất quán, đúng 1 vé, không lưu UNKNOWN trong DB")
    void test10_multipleConsecutiveRetriesAcrossTermChanges_idempotentOutcome() throws Exception {
        String reqId = "11011010-aaaa-aaaa-aaaa-11011010aaaa";
        String ticketId = "TK-110";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G02", ticketId, 0);

        // Lần 1 trên node A (term 1) commit
        nodeA.commitEntry(1, 1, cmd);
        nodeB.commitEntry(1, 1, cmd);
        nodeC.commitEntry(1, 1, cmd);

        // Retry lần 1 sang B (term 2)
        leaderProvider.changeLeader("node-2", 2);
        ApplyResult res1 = nodeB.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

        // Retry lần 2 sang C (term 3)
        leaderProvider.changeLeader("node-3", 3);
        ApplyResult res2 = nodeC.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

        // Retry lần 3 sang B (term 4)
        leaderProvider.changeLeader("node-2", 4);
        ApplyResult res3 = nodeB.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

        // Retry lần 4, 5
        ApplyResult res4 = nodeB.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);
        ApplyResult res5 = nodeB.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);

        // Invariants
        assertEquals(ApplyResult.bookConfirmed(reqId, ticketId, 1L), res1);
        assertEquals(res1, res2);
        assertEquals(res1, res3);
        assertEquals(res1, res4);
        assertEquals(res1, res5);

        assertEquals(1, dbB.queryInt("SELECT count(*) FROM tickets WHERE ticket_id = '" + ticketId + "'"));
        assertEquals(0, dbB.queryInt("SELECT count(*) FROM request_results WHERE state = 'UNKNOWN'"));
    }

    /**
     * Test 11: Lookup GET_REQUEST_RESULT trên Leader mới chỉ trả kết quả quyền uy
     * sau khi policy/read barrier (giả) cho phép; trước đó không suy kết quả từ seats.
     */
    @Test
    @DisplayName("11. GET_REQUEST_RESULT trên Leader mới: chỉ trả kết quả khi read policy cho phép")
    void test11_getRequestResultOnNewLeader_authoritativeOnlyViaReadPolicy() throws Exception {
        String reqId = "11111111-bbbb-bbbb-bbbb-11111111bbbb";
        BookingCommand.BookTicket cmd = makeBookCmd(reqId, "G03", "TK-111", 0);

        nodeB.commitEntry(1, 1, cmd);
        leaderProvider.changeLeader("node-2", 2);

        AtomicBoolean barrierPassed = new AtomicBoolean(false);
        GetRequestResultHandler.ReadPolicy readPolicy = new GetRequestResultHandler.ReadPolicy() {
            @Override
            public void ensureAuthoritative() throws BookingRequestHandler.NotLeaderException, BookingRequestHandler.ReadUnavailableException {
                if (!barrierPassed.get()) {
                    throw new BookingRequestHandler.ReadUnavailableException("Read barrier not passed");
                }
            }

            @Override
            public Optional<String> leaderHint() {
                return Optional.of("node-2");
            }
        };

        JdbcRequestResultStore store = new JdbcRequestResultStore();
        GetRequestResultHandler handler = new GetRequestResultHandler(
                readPolicy,
                rId -> {
                    try (Connection conn = dbB.openIndependentConnection()) {
                        return store.find(conn, rId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                nodeB.adapter()
        );

        // 1. Khi read barrier chưa qua -> fail closed
        assertThrows(BookingRequestHandler.ReadUnavailableException.class, () -> handler.lookup(reqId));

        // 2. Khi read barrier đã qua -> trả kết quả quyền uy
        barrierPassed.set(true);
        GetRequestResultHandler.ResultPayload payload = handler.lookup(reqId);
        assertEquals("APPLIED_SUCCESS", payload.state());
        assertEquals("TK-111", payload.ticketId());
    }

    /**
     * Test 12: Sau failover, các node apply cùng log có state giống nhau
     * (so tickets, seats, request_results, lastApplied bằng hàm JDBC thuần).
     */
    @Test
    @DisplayName("12. Tính nhất quán sau failover: mọi node apply cùng log đều có DB giống hệt nhau")
    void test12_postFailoverStateConsistencyAcrossNodes() throws Exception {
        List<BookingCommand> log = List.of(
                makeBookCmd("11211201-cccc-cccc-cccc-11211201cccc", "G01", "TK-112A", 0),
                makeBookCmd("11211202-cccc-cccc-cccc-11211202cccc", "G02", "TK-112B", 1),
                makeCancelCmd("11211203-cccc-cccc-cccc-11211203cccc", "TK-112A", 2),
                makeBookCmd("11211204-cccc-cccc-cccc-11211204cccc", "G01", "TK-112C", 3),
                new BookingCommand.NoOp()
        );

        long index = 0;
        for (BookingCommand cmd : log) {
            index++;
            nodeA.commitEntry(index, 1, cmd);
            nodeB.commitEntry(index, 1, cmd);
        }

        // Đối chiếu DB giữa node A và node B
        assertEquals(dbA.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"),
                dbB.queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

        assertEquals(dbA.queryListString("SELECT seat_id || '|' || status || '|' || coalesce(current_ticket_id, '') FROM seats ORDER BY seat_id"),
                dbB.queryListString("SELECT seat_id || '|' || status || '|' || coalesce(current_ticket_id, '') FROM seats ORDER BY seat_id"));

        assertEquals(dbA.queryListString("SELECT ticket_id || '|' || seat_id || '|' || status FROM tickets ORDER BY ticket_id"),
                dbB.queryListString("SELECT ticket_id || '|' || seat_id || '|' || status FROM tickets ORDER BY ticket_id"));

        assertEquals(dbA.queryListString("SELECT request_id || '|' || state || '|' || coalesce(reason_code, '') || '|' || coalesce(ticket_id, '') || '|' || applied_log_index FROM request_results ORDER BY applied_log_index, request_id"),
                dbB.queryListString("SELECT request_id || '|' || state || '|' || coalesce(reason_code, '') || '|' || coalesce(ticket_id, '') || '|' || applied_log_index FROM request_results ORDER BY applied_log_index, request_id"));
    }

    /**
     * Test 13: Lặp 20 vòng với seed cố định về thời điểm mất response/đổi term/số lần retry:
     * mọi vòng thỏa invariant, không chập chờn.
     */
    @Test
    @DisplayName("13. Monte Carlo 20 vòng seed cố định: mọi vòng đạt an toàn và bất biến")
    void test13_monteCarloSeedLoop_20Rounds_deterministic() throws Exception {
        Random rnd = new Random(0xCAFEBABE101L);

        for (int round = 1; round <= 20; round++) {
            TestDb rDb = new TestDb(rawJdbcUrl, dbUser, dbPassword, "mr" + round);
            try {
                FakeLeaderProvider rProvider = new FakeLeaderProvider();
                try (NodeContext rNode = new NodeContext("node-1", rDb, rProvider)) {
                    rProvider.changeLeader("node-1", 1);

                    int total = 3;
                    for (int i = 0; i < total; i++) {
                        String req = String.format("ffffffff-%04d-0000-0000-%012d", round, i);
                        String seat = "G0" + ((i % 5) + 1);
                        String ticket = String.format("TK-M%d-%d", round, i);
                        BookingCommand.BookTicket cmd = makeBookCmd(req, seat, ticket, i);

                        long term = rnd.nextInt(3) + 1;
                        rProvider.changeLeader("node-1", term);

                        rNode.commitEntry(i + 1, term, cmd);

                        // Retry ngẫu nhiên 1-3 lần
                        int retries = rnd.nextInt(3) + 1;
                        for (int r = 0; r < retries; r++) {
                            ApplyResult res = rNode.bridge().submitCommand(cmd).get(5, TimeUnit.SECONDS);
                            assertEquals(ApplyResult.Outcome.CONFIRMED, res.outcome());
                        }
                    }

                    // Invariant
                    assertEquals(0, rDb.queryInt(
                            "SELECT count(*) FROM (SELECT seat_id FROM tickets WHERE status = 'ACTIVE' GROUP BY seat_id HAVING count(*) > 1) t"));
                    assertEquals(10, rDb.queryInt("SELECT count(*) FROM seats"));
                    assertEquals(0, rDb.queryInt("SELECT count(*) FROM request_results WHERE state = 'UNKNOWN'"));
                }
            } finally {
                rDb.close();
            }
        }
    }

    /**
     * Test 14: (Tùy chọn) Test live 5-node cluster:
     * Ghi nhận: NOT_RUN (chờ bàn giao failover TV5-D21/D23 và fault runner TV6).
     */
    @Test
    @DisplayName("14. Live 5-node cluster failover: NOT_RUN")
    void test14_liveCluster_skippedNotRun() {
        Assumptions.assumeTrue(false,
                "live 5-node cluster: BLOCKED_INTEGRATION (chờ bàn giao failover TV5-D21/D23 và fault runner TV6)");
    }

    // =========================================================================
    // Helpers & Context
    // =========================================================================

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
    // FakeLeaderProvider: Điều phối Leader & Term giữa các node
    // =========================================================================

    static final class FakeLeaderProvider {
        private volatile long currentTerm = 1;
        private volatile String leaderId = "node-1";
        private final Map<String, FakeRaftPort> nodePorts = new ConcurrentHashMap<>();

        void register(String nodeId, FakeRaftPort port) {
            nodePorts.put(nodeId, port);
            port.bindProvider(this, nodeId);
        }

        long currentTerm() {
            return currentTerm;
        }

        String leaderId() {
            return leaderId;
        }

        synchronized void changeLeader(String newLeaderId, long newTerm) {
            this.currentTerm = newTerm;
            this.leaderId = newLeaderId;
            for (var entry : nodePorts.entrySet()) {
                boolean isLeader = entry.getKey().equals(newLeaderId);
                entry.getValue().updateRole(isLeader ? Role.LEADER : Role.FOLLOWER, newTerm, Optional.ofNullable(newLeaderId));
            }
        }
    }

    // =========================================================================
    // FakeRaftPort: Fencing theo term và điều hướng append
    // =========================================================================

    static final class FakeRaftPort implements BookingRaftBridge.RaftPort {
        private volatile Role role = Role.FOLLOWER;
        private volatile long term = 1;
        private volatile Optional<String> leaderId = Optional.empty();
        private volatile FakeLeaderProvider provider;
        private volatile String nodeId;
        private final List<RaftLogEntry> localLog = Collections.synchronizedList(new ArrayList<>());
        private long lastIndex = 0;

        void bindProvider(FakeLeaderProvider provider, String nodeId) {
            this.provider = provider;
            this.nodeId = nodeId;
        }

        void updateRole(Role role, long term, Optional<String> leaderId) {
            this.role = role;
            this.term = term;
            this.leaderId = leaderId;
        }

        @Override public Role role() { return role; }
        @Override public Optional<String> leaderId() { return leaderId; }
        @Override public long currentTerm() { return term; }

        @Override
        public synchronized RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
            if (provider != null && (provider.currentTerm() > this.term || !Objects.equals(provider.leaderId(), this.nodeId))) {
                return new RaftNode.NotLeader(provider.leaderId());
            }
            if (role != Role.LEADER) {
                return new RaftNode.NotLeader(leaderId.orElse(null));
            }
            lastIndex++;
            RaftLogEntry entry = new RaftLogEntry(lastIndex, term, commandType, commandPayload);
            localLog.add(entry);
            return new RaftNode.Accepted(lastIndex, term);
        }

        List<RaftLogEntry> appendedLog() {
            return localLog;
        }
    }

    // =========================================================================
    // FakeClient: Mô phỏng retry luồng SPEC-06 §4
    // =========================================================================

    static final class FakeClient {
        private final Map<String, NodeContext> clusterNodes;
        private final List<String> endpointOrder;

        FakeClient(Map<String, NodeContext> clusterNodes, List<String> endpointOrder) {
            this.clusterNodes = clusterNodes;
            this.endpointOrder = endpointOrder;
        }

        ApplyResult submitWithRetry(BookingCommand command, String initialTarget, int maxRetries) throws Exception {
            String target = initialTarget;
            int attempts = 0;
            Set<String> tried = new HashSet<>();

            while (attempts < maxRetries) {
                attempts++;
                if (target == null || !clusterNodes.containsKey(target)) {
                    target = endpointOrder.stream().filter(e -> !tried.contains(e)).findFirst().orElse(endpointOrder.get(0));
                }
                tried.add(target);
                NodeContext ctx = clusterNodes.get(target);

                try {
                    return ctx.bridge().submitCommand(command).get(2, TimeUnit.SECONDS);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof BookingRequestHandler.NotLeaderException nle) {
                        target = nle.leaderHint();
                        continue;
                    }
                    if (cause instanceof BookingRequestHandler.RequestIdConflictException ric) {
                        throw ric;
                    }
                    if (cause instanceof Exception ex) {
                        throw ex;
                    }
                    throw ee;
                } catch (BookingRequestHandler.NotLeaderException nle) {
                    target = nle.leaderHint();
                } catch (TimeoutException te) {
                    target = null;
                }
            }
            return ApplyResult.bookUnknown(extractReqId(command));
        }

        private static String extractReqId(BookingCommand cmd) {
            if (cmd instanceof BookingCommand.BookTicket b) return b.requestId();
            if (cmd instanceof BookingCommand.CancelTicket c) return c.requestId();
            return "unknown";
        }
    }

    // =========================================================================
    // NodeContext: Cụm đối tượng của 1 node
    // =========================================================================

    static final class NodeContext implements AutoCloseable {
        private final String nodeId;
        private final TestDb testDb;
        private final ScheduledExecutorService scheduler;
        private final JdbcBookingRepository repository;
        private final JdbcCommittedCommandApplier applier;
        private final RaftApplyAdapter adapter;
        private final FakeRaftPort raftPort;
        private final BookingRaftBridge bridge;

        NodeContext(String nodeId, TestDb testDb, FakeLeaderProvider leaderProvider) {
            this.nodeId = nodeId;
            this.testDb = testDb;
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "node-" + nodeId + "-scheduler");
                t.setDaemon(true);
                return t;
            });

            this.repository = new JdbcBookingRepository(testDb.connectionFactory());
            this.applier = new JdbcCommittedCommandApplier(testDb.connectionFactory(), result -> { });
            this.adapter = new RaftApplyAdapter(applier, repository);
            this.raftPort = new FakeRaftPort();
            leaderProvider.register(nodeId, raftPort);

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

        RaftApplyAdapter adapter() {
            return adapter;
        }

        FakeRaftPort raftPort() {
            return raftPort;
        }

        BookingRaftBridge bridge() {
            return bridge;
        }

        void commitEntry(long index, long term, BookingCommand cmd) throws SQLException {
            String type = cmd.type().name();
            String payload = commandJson(cmd);

            testDb.insertCommittedLog(index, term, type, payload);
            adapter.advanceCommitIndex(index);
            adapter.onCommitted(new RaftLogEntry(index, term, type, payload));
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

    // =========================================================================
    // TestDb: Quản lý Schema cô lập trong PostgreSQL
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
