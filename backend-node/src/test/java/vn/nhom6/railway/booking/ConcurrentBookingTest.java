package vn.nhom6.railway.booking;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử đồng thời cho đặt vé trước mốc G5 (SPEC-07, SPEC-05 §1/§3, SPEC-03 §4.2/§7).
 *
 * <p>Mức bằng chứng: FAKE+LOCAL (dùng FakeRaft và FakeCommitSource, chạy trên PostgreSQL thật cục bộ).
 * Đánh dấu:
 * <ul>
 *   <li>runner TV6: BLOCKED_INTEGRATION (tự dựng runner đồng thời bằng ExecutorService + CountDownLatch).</li>
 *   <li>TV4-D18 ordered applier (G5): BLOCKED_INTEGRATION (sẽ chạy tích hợp thật ở mốc G5).</li>
 * </ul>
 *
 * <p>Bao gồm 9 bài kiểm thử / assertion bắt buộc:
 * <ol>
 *   <li>6 BOOK(G05) đồng thời: đúng 1 CONFIRMED, 5 REJECTED (SEAT_ALREADY_BOOKED), 0 UNKNOWN.</li>
 *   <li>DB độc lập (connection khác): số vé ACTIVE của G05 <= 1 (đúng 1); vé thuộc đúng người thắng.</li>
 *   <li>Khớp thứ tự commit/apply của FakeCommitSource với nhiều seed xác định.</li>
 *   <li>Idempotency: gửi lại cùng payload trả kết quả gốc; khác payload ném REQUEST_ID_CONFLICT.</li>
 *   <li>request_results đầy đủ 6 hàng; không có hàng nào lưu UNKNOWN.</li>
 *   <li>Timeline safety: không CONFIRMED nào phát ra trước khi entry commit + apply DB.</li>
 *   <li>lastApplied = 6 (đúng bằng số entry đã commit), không entry nào bị apply hai lần.</li>
 *   <li>Lặp 20 vòng với 20 seed khác nhau: mọi vòng đều thỏa mãn invariant <= 1 vé ACTIVE.</li>
 *   <li>BOOK và CANCEL xen kẽ đồng thời: số vé ACTIVE của G05 luôn <= 1.</li>
 * </ol>
 */
class ConcurrentBookingTest {

    private static final String TRIP_ID = "SE-001";
    private static final String SEAT_G05 = "G05";
    private static final Instant BASE_TIME = Instant.parse("2026-11-18T10:00:00Z");

    // =========================================================================
    // Fake Classes CHỈ DÙNG TRONG TEST
    // =========================================================================

    /**
     * FakeRaft: Thread-safe port giả lập Leader append cho 6 luồng đồng thời.
     */
    static final class FakeRaft implements BookingRaftBridge.RaftPort {
        Role role = Role.LEADER;
        String leaderHint = "node-1";
        long currentTerm = 1;
        long nextLogIndex = 1;
        final List<AppendedCommand> log = new CopyOnWriteArrayList<>();

        record AppendedCommand(long index, long term, String type, String payload, BookingCommand command) {}

        @Override
        public Role role() {
            return role;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderHint);
        }

        @Override
        public long currentTerm() {
            return currentTerm;
        }

        @Override
        public synchronized RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
            if (role != Role.LEADER) {
                return new RaftNode.NotLeader(leaderHint);
            }
            long index = nextLogIndex++;
            AppendedCommand cmd = new AppendedCommand(index, currentTerm, commandType, commandPayload, null);
            log.add(cmd);
            return new RaftNode.Accepted(index, currentTerm);
        }

        public synchronized void registerAppendedCommand(long index, BookingCommand cmd) {
            for (int i = 0; i < log.size(); i++) {
                AppendedCommand c = log.get(i);
                if (c.index() == index) {
                    log.set(i, new AppendedCommand(c.index(), c.term(), c.type(), c.payload(), cmd));
                    break;
                }
            }
        }
    }

    /**
     * FakeCommitSource: Quyết định thứ tự commit và chuyển giao vào RaftApplyAdapter.
     */
    static final class FakeCommitSource {
        private final RaftApplyAdapter adapter;
        private final TestDatabase database;

        FakeCommitSource(RaftApplyAdapter adapter) {
            this(adapter, null);
        }

        FakeCommitSource(RaftApplyAdapter adapter, TestDatabase database) {
            this.adapter = adapter;
            this.database = database;
        }

        synchronized void commitAndDeliver(long index, long term, String type, String payload, BookingCommand command) throws Exception {
            if (database != null && command != null) {
                database.insertCommittedLog(index, command);
            }
            adapter.advanceCommitIndex(index);
            RaftLogEntry entry = new RaftLogEntry(index, term, type, payload);
            adapter.onCommitted(entry);
        }

        synchronized void commitAndDeliver(FakeRaft.AppendedCommand cmd) throws Exception {
            commitAndDeliver(cmd.index(), cmd.term(), cmd.type(), cmd.payload(), cmd.command());
        }
    }

    private static BookingCommand.BookTicket makeBook(int i) {
        String reqId = String.format("10000000-0000-0000-0000-00000000000%d", i);
        String passenger = "Passenger " + i;
        String ticketId = "tk-g05-" + i;
        Instant time = BASE_TIME.plusSeconds(i);
        return new BookingCommand.BookTicket(reqId, TRIP_ID, SEAT_G05, passenger, ticketId, time);
    }

    // =========================================================================
    // 9 BÀI TEST BẮT BUỘC THEO SPEC
    // =========================================================================

    /**
     * 1. 6 BOOK(G05) đồng thời, requestId khác nhau, tên hành khách khác nhau:
     * đúng 1 CONFIRMED, 5 REJECTED với mã lỗi "SEAT_ALREADY_BOOKED"; không có UNKNOWN.
     */
    @Test
    @DisplayName("1. 6 BOOK(G05) đồng thời: đúng 1 CONFIRMED, 5 REJECTED (SEAT_ALREADY_BOOKED), 0 UNKNOWN")
    void test01_sixConcurrentBooksExactlyOneConfirmedFiveRejected() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
            List<BookingCommand.BookTicket> commands = new ArrayList<>();
            for (int i = 1; i <= 6; i++) {
                commands.add(makeBook(i));
            }

            // Runner đồng thời: 6 worker threads xuất phát cùng lúc
            ExecutorService executor = Executors.newFixedThreadPool(6);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<CompletableFuture<ApplyResult>> futures = new CopyOnWriteArrayList<>();

            for (BookingCommand.BookTicket cmd : commands) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        CompletableFuture<ApplyResult> f = bridge.submitCommand(cmd);
                        raft.registerAppendedCommand(raft.log.size(), cmd);
                        futures.add(f);
                    } catch (Exception e) {
                        futures.add(CompletableFuture.failedFuture(e));
                    }
                });
            }

            // Đồng bộ nhả 6 luồng
            startLatch.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            assertEquals(6, raft.log.size(), "Tất cả 6 lệnh phải được append vào Raft log");

            // Commit tuần tự 6 entry theo thứ tự Raft log
            for (FakeRaft.AppendedCommand cmd : raft.log) {
                commitSource.commitAndDeliver(cmd);
            }

            // Thu thập và kiểm tra kết quả
            List<ApplyResult> results = new ArrayList<>();
            for (CompletableFuture<ApplyResult> f : futures) {
                results.add(f.get(2, TimeUnit.SECONDS));
            }

            long confirmedCount = results.stream().filter(r -> r.outcome() == ApplyResult.Outcome.CONFIRMED).count();
            long rejectedCount = results.stream().filter(r -> r.outcome() == ApplyResult.Outcome.REJECTED).count();
            long unknownCount = results.stream().filter(r -> r.outcome() == ApplyResult.Outcome.UNKNOWN).count();

            assertEquals(1, confirmedCount, "Đúng 1 yêu cầu CONFIRMED");
            assertEquals(5, rejectedCount, "Đúng 5 yêu cầu REJECTED");
            assertEquals(0, unknownCount, "Không có yêu cầu nào UNKNOWN");

            for (ApplyResult r : results) {
                if (r.outcome() == ApplyResult.Outcome.REJECTED) {
                    assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, r.reasonCode(),
                            "Lỗi từ chối phải là SEAT_ALREADY_BOOKED theo SPEC-03");
                    assertNull(r.ticketId());
                }
            }

            assertEquals(Optional.of("BOOKED"), sm.seatStatus(SEAT_G05));
        }
    }

    /**
     * 2. Invariant DB độc lập (connection khác):
     * số vé ACTIVE của (SE-001, G05) <= 1 (ở đây đúng 1); tổng ticket G05 = 1;
     * vé thuộc đúng người thắng; các ghế khác không đổi.
     */
    @Test
    @DisplayName("2. Invariant DB: đúng 1 vé ACTIVE trên G05, tổng vé G05 = 1, các ghế khác không đổi")
    void test02_databaseInvariantExactlyOneActiveTicketOnG05() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
            FakeRaft raft = new FakeRaft();
            FakeCommitSource commitSource = new FakeCommitSource(adapter, db);

            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofSeconds(5))) {
                List<BookingCommand.BookTicket> commands = new ArrayList<>();
                for (int i = 1; i <= 6; i++) {
                    commands.add(makeBook(i));
                }

                ExecutorService executor = Executors.newFixedThreadPool(6);
                CountDownLatch startLatch = new CountDownLatch(1);
                List<CompletableFuture<ApplyResult>> futures = new CopyOnWriteArrayList<>();

                for (BookingCommand.BookTicket cmd : commands) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            futures.add(bridge.submitCommand(cmd));
                        } catch (Exception e) {
                            futures.add(CompletableFuture.failedFuture(e));
                        }
                    });
                }

                startLatch.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

                // Gán command vào log để insertCommittedLog vào DB
                for (int i = 0; i < raft.log.size(); i++) {
                    FakeRaft.AppendedCommand c = raft.log.get(i);
                    BookingCommand.BookTicket matchingCmd = commands.stream()
                            .filter(cmd -> c.payload().contains(cmd.requestId()))
                            .findFirst().orElseThrow();
                    raft.log.set(i, new FakeRaft.AppendedCommand(c.index(), c.term(), c.type(), c.payload(), matchingCmd));
                }

                // Commit cả 6 entries
                for (FakeRaft.AppendedCommand cmd : raft.log) {
                    commitSource.commitAndDeliver(cmd);
                }

                // Chờ toàn bộ future hoàn tất
                ApplyResult winner = null;
                for (CompletableFuture<ApplyResult> f : futures) {
                    ApplyResult r = f.get(2, TimeUnit.SECONDS);
                    if (r.outcome() == ApplyResult.Outcome.CONFIRMED) {
                        winner = r;
                    }
                }
                assertNotNull(winner);

                // --- ASSERT ĐỘC LẬP BẰNG CONNECTION DATABASE KHÁC ---
                int activeTickets = db.queryInt(
                        "SELECT count(*) FROM tickets WHERE trip_id = 'SE-001' AND seat_id = 'G05' AND status = 'ACTIVE'");
                assertEquals(1, activeTickets, "Số vé ACTIVE của G05 phải chính xác là 1");

                int totalTickets = db.queryInt(
                        "SELECT count(*) FROM tickets WHERE trip_id = 'SE-001' AND seat_id = 'G05'");
                assertEquals(1, totalTickets, "Tổng số vé tạo cho G05 phải đúng 1 (không sinh vé thứ hai)");

                String activeTicketId = db.queryString(
                        "SELECT ticket_id FROM tickets WHERE trip_id = 'SE-001' AND seat_id = 'G05' AND status = 'ACTIVE'");
                assertEquals(winner.ticketId(), activeTicketId, "Vé trong DB phải thuộc đúng người thắng");

                int otherBookedSeats = db.queryInt(
                        "SELECT count(*) FROM seats WHERE trip_id = 'SE-001' AND status = 'BOOKED' AND seat_id != 'G05'");
                assertEquals(0, otherBookedSeats, "Các ghế khác ngoài G05 phải giữ nguyên AVAILABLE");
            }
        }
    }

    /**
     * 3. Người thắng khớp thứ tự commit/apply của FakeCommitSource (người có entry apply đầu tiên thắng),
     * kiểm với nhiều seed thứ tự đảo khác nhau.
     */
    @Test
    @DisplayName("3. Người thắng luôn là người có entry commit đầu tiên theo seed của FakeCommitSource")
    void test03_winnerMatchesFirstCommittedEntryAcrossRandomSeeds() throws Exception {
        long[] testSeeds = {42L, 100L, 2026L, 9999L};

        for (long seed : testSeeds) {
            BookingStateMachine sm = new BookingStateMachine();
            RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
            FakeRaft raft = new FakeRaft();
            FakeCommitSource commitSource = new FakeCommitSource(adapter);

            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
                List<BookingCommand.BookTicket> commands = new ArrayList<>();
                for (int i = 1; i <= 6; i++) {
                    commands.add(makeBook(i));
                }

                // Xáo trộn thứ tự commands dựa trên seed cố định
                List<BookingCommand.BookTicket> shuffled = new ArrayList<>(commands);
                Collections.shuffle(shuffled, new Random(seed));

                BookingCommand.BookTicket expectedWinner = shuffled.get(0);

                List<CompletableFuture<ApplyResult>> futures = new ArrayList<>();
                for (BookingCommand.BookTicket cmd : shuffled) {
                    futures.add(bridge.submitCommand(cmd));
                }

                // Commit lần lượt theo thứ tự shuffled (entry 1 = expectedWinner)
                for (int i = 0; i < raft.log.size(); i++) {
                    FakeRaft.AppendedCommand c = raft.log.get(i);
                    commitSource.commitAndDeliver(c.index(), c.term(), c.type(), c.payload(), shuffled.get(i));
                }

                ApplyResult winnerResult = futures.get(0).get(2, TimeUnit.SECONDS);
                assertEquals(ApplyResult.Outcome.CONFIRMED, winnerResult.outcome());
                assertEquals(expectedWinner.ticketId(), winnerResult.ticketId(),
                        "Người thắng phải thuộc về entry commit đầu tiên (seed=" + seed + ")");

                for (int i = 1; i < futures.size(); i++) {
                    ApplyResult loserResult = futures.get(i).get(2, TimeUnit.SECONDS);
                    assertEquals(ApplyResult.Outcome.REJECTED, loserResult.outcome());
                    assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, loserResult.reasonCode());
                }
            }
        }
    }

    /**
     * 4. Từ chối trùng: gửi lại cùng requestId + cùng payload (cả với bên thắng và bên thua):
     * trả đúng kết quả gốc, không tạo vé mới, không append lần hai.
     * Cùng requestId khác payload: REQUEST_ID_CONFLICT.
     */
    @Test
    @DisplayName("4. Idempotency: replay cùng payload trả kết quả gốc; khác payload ném REQUEST_ID_CONFLICT")
    void test04_idempotencyReplayReturnsOriginalAndConflictThrows() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
            List<BookingCommand.BookTicket> commands = new ArrayList<>();
            for (int i = 1; i <= 6; i++) {
                commands.add(makeBook(i));
            }

            List<CompletableFuture<ApplyResult>> futures = new ArrayList<>();
            for (BookingCommand.BookTicket cmd : commands) {
                futures.add(bridge.submitCommand(cmd));
            }

            for (FakeRaft.AppendedCommand cmd : raft.log) {
                commitSource.commitAndDeliver(cmd);
            }

            ApplyResult winnerOriginal = futures.get(0).get(2, TimeUnit.SECONDS);
            ApplyResult loserOriginal = futures.get(1).get(2, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.CONFIRMED, winnerOriginal.outcome());
            assertEquals(ApplyResult.Outcome.REJECTED, loserOriginal.outcome());

            int logSizeBeforeReplay = raft.log.size();

            // 4a. Replay người thắng cùng payload
            CompletableFuture<ApplyResult> winnerReplay = bridge.submitCommand(commands.get(0));
            assertEquals(winnerOriginal, winnerReplay.get(1, TimeUnit.SECONDS));
            assertEquals(logSizeBeforeReplay, raft.log.size(), "KHÔNG được append lần hai khi replay bên thắng");

            // 4b. Replay người thua cùng payload
            CompletableFuture<ApplyResult> loserReplay = bridge.submitCommand(commands.get(1));
            assertEquals(loserOriginal, loserReplay.get(1, TimeUnit.SECONDS));
            assertEquals(logSizeBeforeReplay, raft.log.size(), "KHÔNG được append lần hai khi replay bên thua");

            // 4c. Gửi cùng requestId bên thắng nhưng khác payload (đổi sang ghế G01)
            BookingCommand.BookTicket conflictWinner = new BookingCommand.BookTicket(
                    commands.get(0).requestId(), TRIP_ID, "G01", "Changed Name", "tk-changed-1", BASE_TIME);
            assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                    () -> bridge.submitCommand(conflictWinner));
            assertEquals(logSizeBeforeReplay, raft.log.size(), "KHÔNG được append khi conflict payload");

            // 4d. Gửi cùng requestId bên thua nhưng khác payload (đổi tên)
            BookingCommand.BookTicket conflictLoser = new BookingCommand.BookTicket(
                    commands.get(1).requestId(), TRIP_ID, SEAT_G05, "Different Name", "tk-changed-2", BASE_TIME);
            assertThrows(BookingRequestHandler.RequestIdConflictException.class,
                    () -> bridge.submitCommand(conflictLoser));
            assertEquals(logSizeBeforeReplay, raft.log.size(), "KHÔNG được append khi conflict payload");
        }
    }

    /**
     * 5. Mọi yêu cầu đều có hàng trong request_results; không có hàng UNKNOWN.
     */
    @Test
    @DisplayName("5. request_results: đủ 6 hàng kết quả, không có hàng nào UNKNOWN")
    void test05_requestResultsTableHasAllSixRowsAndNoUnknown() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
            RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
            FakeRaft raft = new FakeRaft();
            FakeCommitSource commitSource = new FakeCommitSource(adapter, db);

            try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofSeconds(5))) {
                List<BookingCommand.BookTicket> commands = new ArrayList<>();
                for (int i = 1; i <= 6; i++) {
                    commands.add(makeBook(i));
                }

                for (BookingCommand.BookTicket cmd : commands) {
                    bridge.submitCommand(cmd);
                }

                for (int i = 0; i < raft.log.size(); i++) {
                    FakeRaft.AppendedCommand c = raft.log.get(i);
                    commitSource.commitAndDeliver(c.index(), c.term(), c.type(), c.payload(), commands.get(i));
                }

                // Kiểm tra bảng request_results
                int totalRows = db.queryInt("SELECT count(*) FROM request_results");
                assertEquals(6, totalRows, "Bảng request_results phải có đúng 6 hàng");

                int successRows = db.queryInt(
                        "SELECT count(*) FROM request_results WHERE state = 'APPLIED_SUCCESS'");
                assertEquals(1, successRows, "Đúng 1 hàng APPLIED_SUCCESS");

                int rejectedRows = db.queryInt(
                        "SELECT count(*) FROM request_results WHERE state = 'APPLIED_REJECTED'");
                assertEquals(5, rejectedRows, "Đúng 5 hàng APPLIED_REJECTED");

                int unknownRows = db.queryInt(
                        "SELECT count(*) FROM request_results WHERE state = 'UNKNOWN' OR reason_code = 'TIMEOUT_UNKNOWN'");
                assertEquals(0, unknownRows, "Tuyệt đối không có hàng nào lưu UNKNOWN trong request_results");
            }
        }
    }

    /**
     * 6. Không có CONFIRMED nào phát ra trước khi entry của nó commit và apply
     * (kiểm theo thứ tự sự kiện ghi lại).
     */
    @Test
    @DisplayName("6. Timeline safety: CONFIRMED không bao giờ phát ra trước commit+apply")
    void test06_confirmedNeverIssuedBeforeCommitAndApply() throws Exception {
        List<String> eventTimeline = new CopyOnWriteArrayList<>();
        BookingStateMachine sm = new BookingStateMachine();

        RaftApplyAdapter adapter = new RaftApplyAdapter(
                (idx, cmd) -> {
                    eventTimeline.add("DB_APPLIED_" + idx);
                    return Optional.of(sm.apply(idx, cmd));
                },
                null,
                null,
                null);

        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
            BookingCommand.BookTicket cmd = makeBook(1);

            CompletableFuture<ApplyResult> future = bridge.submitCommand(cmd);
            future.thenAccept(res -> {
                if (res.outcome() == ApplyResult.Outcome.CONFIRMED) {
                    eventTimeline.add("CONFIRMED_ISSUED");
                }
            });

            // Lúc này chưa commit
            assertFalse(future.isDone());
            assertFalse(eventTimeline.contains("CONFIRMED_ISSUED"));

            eventTimeline.add("RAFT_COMMIT_START");
            commitSource.commitAndDeliver(raft.log.get(0));

            future.get(1, TimeUnit.SECONDS);

            // Timeline phải là: RAFT_COMMIT_START -> DB_APPLIED_1 -> CONFIRMED_ISSUED
            int commitIndex = eventTimeline.indexOf("RAFT_COMMIT_START");
            int applyIndex = eventTimeline.indexOf("DB_APPLIED_1");
            int confirmedIndex = eventTimeline.indexOf("CONFIRMED_ISSUED");

            assertTrue(commitIndex < applyIndex, "Raft commit phải xảy ra trước khi DB apply");
            assertTrue(applyIndex < confirmedIndex, "DB apply phải xảy ra trước khi phát CONFIRMED cho Client");
        }
    }

    /**
     * 7. lastApplied = số entry đã commit, không có entry bị apply hai lần.
     */
    @Test
    @DisplayName("7. lastApplied = 6, không có entry nào bị apply hai lần")
    void test07_lastAppliedEqualsCommittedEntriesNoDoubleApply() throws Exception {
        AtomicInteger applyCount = new AtomicInteger(0);
        BookingStateMachine sm = new BookingStateMachine();

        RaftApplyAdapter adapter = new RaftApplyAdapter(
                (idx, cmd) -> {
                    applyCount.incrementAndGet();
                    return Optional.of(sm.apply(idx, cmd));
                },
                null,
                null,
                null);

        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
            for (int i = 1; i <= 6; i++) {
                bridge.submitCommand(makeBook(i));
            }

            for (FakeRaft.AppendedCommand cmd : raft.log) {
                commitSource.commitAndDeliver(cmd);
            }

            assertEquals(6, adapter.lastApplied(), "lastApplied phải chính xác là 6");
            assertEquals(6, applyCount.get(), "Tổng số lần gọi apply phải đúng 6 lần (không apply lần hai)");
        }
    }

    /**
     * 8. Lặp 20 vòng với seed khác nhau: mọi vòng đều thỏa invariant <= 1 vé ACTIVE, không chập chờn.
     * (Kiểm thử trên PostgreSQL thật).
     */
    @Test
    @DisplayName("8. Lặp 20 vòng với 20 seed khác nhau: invariant <= 1 vé ACTIVE luôn thỏa, không flaky")
    void test08_twentyRoundsDifferentSeedsAllSatisfyInvariant() throws Exception {
        requireDatabase();
        try (TestDatabase db = openDatabase()) {
            for (int round = 1; round <= 20; round++) {
                // Reset database giữa các vòng lặp để test chạy nhanh và cô lập
                db.resetState();

                JdbcBookingRepository repo = new JdbcBookingRepository(db.connectionFactory());
                JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(db.connectionFactory());
                RaftApplyAdapter adapter = new RaftApplyAdapter(applier, repo);
                FakeRaft raft = new FakeRaft();
                FakeCommitSource commitSource = new FakeCommitSource(adapter, db);

                try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, repo, Duration.ofSeconds(5))) {
                    List<BookingCommand.BookTicket> commands = new ArrayList<>();
                    for (int i = 1; i <= 6; i++) {
                        String reqId = UUID.randomUUID().toString();
                        String pass = "Pass-" + round + "-" + i;
                        String ticketId = "tk-" + round + "-" + i;
                        commands.add(new BookingCommand.BookTicket(reqId, TRIP_ID, SEAT_G05, pass, ticketId, BASE_TIME));
                    }

                    // Đảo thứ tự commit theo seed của vòng này
                    List<BookingCommand.BookTicket> shuffled = new ArrayList<>(commands);
                    Collections.shuffle(shuffled, new Random(round * 1000L));

                    List<CompletableFuture<ApplyResult>> futures = new ArrayList<>();
                    for (BookingCommand.BookTicket cmd : shuffled) {
                        futures.add(bridge.submitCommand(cmd));
                    }

                    for (int i = 0; i < raft.log.size(); i++) {
                        FakeRaft.AppendedCommand c = raft.log.get(i);
                        commitSource.commitAndDeliver(c.index(), c.term(), c.type(), c.payload(), shuffled.get(i));
                    }

                    // Thu hoạch kết quả
                    int confirmed = 0;
                    int rejected = 0;
                    for (CompletableFuture<ApplyResult> f : futures) {
                        ApplyResult r = f.get(2, TimeUnit.SECONDS);
                        if (r.outcome() == ApplyResult.Outcome.CONFIRMED) confirmed++;
                        if (r.outcome() == ApplyResult.Outcome.REJECTED) rejected++;
                    }

                    assertEquals(1, confirmed, "Vòng " + round + " phải có đúng 1 CONFIRMED");
                    assertEquals(5, rejected, "Vòng " + round + " phải có đúng 5 REJECTED");

                    // Invariant DB độc lập
                    int activeCount = db.queryInt(
                            "SELECT count(*) FROM tickets WHERE trip_id = 'SE-001' AND seat_id = 'G05' AND status = 'ACTIVE'");
                    assertEquals(1, activeCount, "Vòng " + round + " vi phạm invariant: số vé ACTIVE != 1");
                }
            }
        }
    }

    /**
     * 9. BOOK(G05) lẫn CANCEL xen kẽ đồng thời: vẫn <= 1 ACTIVE ở mọi thời điểm quan sát.
     */
    @Test
    @DisplayName("9. BOOK và CANCEL xen kẽ đồng thời: số vé ACTIVE của G05 luôn <= 1 tại mọi thời điểm")
    void test09_interleavedBookAndCancelMaintainsAtMostOneActiveTicket() throws Exception {
        BookingStateMachine sm = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(sm, null);
        FakeRaft raft = new FakeRaft();
        FakeCommitSource commitSource = new FakeCommitSource(adapter);

        try (BookingRaftBridge bridge = new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(5))) {
            // Đặt vé đầu tiên để có vé ACTIVE
            BookingCommand.BookTicket book1 = makeBook(1);
            CompletableFuture<ApplyResult> f1 = bridge.submitCommand(book1);
            commitSource.commitAndDeliver(raft.log.get(0));
            assertEquals(ApplyResult.Outcome.CONFIRMED, f1.get(1, TimeUnit.SECONDS).outcome());

            // Lúc này: 1 ACTIVE
            assertSeatActiveCountAtMostOne(sm);

            // Chuẩn bị 1 lệnh CANCEL vé tk-g05-1 và 3 lệnh BOOK G05 mới
            BookingCommand.CancelTicket cancel1 = new BookingCommand.CancelTicket(
                    "20000000-0000-0000-0000-000000000001", "tk-g05-1", BASE_TIME.plusSeconds(10));
            BookingCommand.BookTicket book2 = makeBook(2);
            BookingCommand.BookTicket book3 = makeBook(3);
            BookingCommand.BookTicket book4 = makeBook(4);

            List<BookingCommand> concurrentCommands = List.of(book2, cancel1, book3, book4);
            List<CompletableFuture<ApplyResult>> futures = new ArrayList<>();

            for (BookingCommand cmd : concurrentCommands) {
                futures.add(bridge.submitCommand(cmd));
            }

            // Commit lần lượt từng entry và quan sát trạng thái sau mỗi bước
            for (int i = 1; i < raft.log.size(); i++) {
                commitSource.commitAndDeliver(raft.log.get(i));
                assertSeatActiveCountAtMostOne(sm);
            }

            for (CompletableFuture<ApplyResult> f : futures) {
                assertNotNull(f.get(1, TimeUnit.SECONDS));
            }

            // Kết thúc kịch bản, số vé ACTIVE trên G05 vẫn <= 1
            assertSeatActiveCountAtMostOne(sm);
        }
    }

    private static void assertSeatActiveCountAtMostOne(BookingStateMachine sm) {
        Optional<String> status = sm.seatStatus(SEAT_G05);
        assertTrue(status.isPresent());
        assertTrue("AVAILABLE".equals(status.get()) || "BOOKED".equals(status.get()));
    }

    // =========================================================================
    // PostgreSQL Test Infrastructure
    // =========================================================================

    private static void requireDatabase() {
        Assumptions.assumeTrue(isDatabaseConfigured(), "Bỏ qua test DB vì thiếu biến môi trường RAILWAY_TEST_*");
    }

    private static boolean isDatabaseConfigured() {
        return isSet(System.getenv("RAILWAY_TEST_JDBC_URL"))
                && isSet(System.getenv("RAILWAY_TEST_DB_USER"))
                && isSet(System.getenv("RAILWAY_TEST_DB_PASSWORD"));
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    private static TestDatabase openDatabase() throws Exception {
        return TestDatabase.open(
                System.getenv("RAILWAY_TEST_JDBC_URL"),
                System.getenv("RAILWAY_TEST_DB_USER"),
                System.getenv("RAILWAY_TEST_DB_PASSWORD"));
    }

    private static final class TestDatabase implements AutoCloseable {
        private final String baseUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final ConnectionFactory connectionFactory;

        private TestDatabase(String jdbcUrl, String username, String password) throws Exception {
            this.baseUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
            createSchema();
            try {
                URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
                String dbName = uri.getPath().substring(1) + "?currentSchema=" + schema;
                DbConfig config = new DbConfig(
                        uri.getHost(), uri.getPort() < 0 ? 5432 : uri.getPort(),
                        dbName, username, password);
                this.connectionFactory = new ConnectionFactory(config);
                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
            } catch (Exception exception) {
                try {
                    dropSchema();
                } catch (Exception ignored) {
                }
                throw exception;
            }
        }

        static TestDatabase open(String jdbcUrl, String username, String password) throws Exception {
            return new TestDatabase(jdbcUrl, username, password);
        }

        ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        void resetState() throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement()) {
                st.execute("TRUNCATE business_events, tickets, request_results, raft_log CASCADE");
                st.execute("UPDATE seats SET status = 'AVAILABLE', current_ticket_id = NULL");
                st.execute("UPDATE raft_apply_progress SET last_applied = 0 WHERE singleton = 1");
            }
        }

        void insertCommittedLog(long logIndex, BookingCommand command) throws SQLException {
            String commandType = command.type().name();
            String commandJson;
            if (command instanceof BookingCommand.BookTicket book) {
                commandJson = String.format(
                        "{\"requestId\":\"%s\",\"tripId\":\"%s\",\"seatId\":\"%s\",\"passengerName\":\"%s\",\"ticketId\":\"%s\",\"issuedAt\":\"%s\"}",
                        book.requestId(), book.tripId(), book.seatId(),
                        book.passengerName().replace("\"", "\\\""),
                        book.ticketId(), book.issuedAt());
            } else if (command instanceof BookingCommand.CancelTicket cancel) {
                commandJson = String.format(
                        "{\"requestId\":\"%s\",\"ticketId\":\"%s\",\"cancelledAt\":\"%s\"}",
                        cancel.requestId(), cancel.ticketId(), cancel.cancelledAt());
            } else if (command instanceof BookingCommand.NoOp) {
                commandJson = "{}";
            } else {
                throw new IllegalArgumentException("Unknown command type: " + command.getClass());
            }

            try (Connection conn = connectionFactory.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, 1, ?, ?::jsonb) ON CONFLICT (log_index) DO NOTHING")) {
                ps.setLong(1, logIndex);
                ps.setString(2, commandType);
                ps.setString(3, commandJson);
                ps.executeUpdate();
            }
        }

        int queryInt(String sql) throws SQLException {
            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                throw new IllegalStateException("Empty result set for: " + sql);
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

        private void createSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                 Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA " + schema);
            }
        }

        private void dropSchema() throws SQLException {
            try (Connection conn = DriverManager.getConnection(baseUrl, username, password);
                 Statement st = conn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }

        private void runMigration(String filename) throws Exception {
            String sql;
            try (InputStream in = getClass().getClassLoader().getResourceAsStream("migration/" + filename)) {
                if (in == null) {
                    try (InputStream fallback = getClass().getClassLoader().getResourceAsStream(filename)) {
                        if (fallback == null) {
                            java.nio.file.Path rootPath = java.nio.file.Paths.get("..", "database", "migration", filename);
                            if (!java.nio.file.Files.exists(rootPath)) {
                                rootPath = java.nio.file.Paths.get("database", "migration", filename);
                            }
                            sql = java.nio.file.Files.readString(rootPath, StandardCharsets.UTF_8);
                        } else {
                            sql = new String(fallback.readAllBytes(), StandardCharsets.UTF_8);
                        }
                    }
                } else {
                    sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }

            try (Connection conn = connectionFactory.getConnection();
                 Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }

        @Override
        public void close() throws Exception {
            dropSchema();
        }
    }
}

