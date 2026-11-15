// Theo SPEC-03 §6.1 và SPEC-05 §2: ticketId và issuedAt do Leader cấp và nằm TRONG command đã nhân bản;
// apply chỉ dùng đúng giá trị đó, KHÔNG sinh lại, KHÔNG dẫn xuất từ requestId/giờ/random trong apply.
// Test phải chứng minh điều này, không đặt quy tắc dẫn xuất mới.

package vn.nhom6.railway.booking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPEC-03 §4.2, §6.1, §7; SPEC-05 §3, §3.1; SPEC-04 §6:
 * Deterministic booking results, response standardization, and strict log-order execution.
 */
class DeterministicBookingTests {

    private static final ObjectMapper MAPPER = MessageCodec.getMapper();
    private static final String TRIP_ID = "SE-001";
    private static final String REQUEST_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String REQUEST_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    private static final String REQUEST_C = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    private static final String REQUEST_ZERO = "00000000-0000-0000-0000-000000000000";
    private static final String REQUEST_NINE = "99999999-9999-9999-9999-999999999999";
    private static final String TICKET_A = "tk-A";
    private static final String TICKET_B = "tk-B";
    private static final String TICKET_C = "tk-C";

    private static final Instant TIME_1 = Instant.parse("2026-10-29T10:00:00Z");
    private static final Instant TIME_2 = Instant.parse("2026-10-29T10:01:00Z");
    private static final Instant TIME_EARLIER = Instant.parse("2026-10-29T09:00:00Z");

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    // =========================================================================
    // NHÓM A: Không cần DB, chạy trên BookingStateMachine in-memory
    // =========================================================================

    /**
     * 1. ticketId lấy nguyên từ command: BOOK với ticketId "tk-A" cho vé "tk-A" đúng trong kết quả
     * và trong vé; apply hai lần trên hai state machine độc lập cho cùng ticketId, không có ticketId
     * nào được sinh thêm.
     */
    @Test
    void test01_ticketIdTakenVerbatimFromCommandWithoutRegeneration() {
        BookingStateMachine sm1 = new BookingStateMachine();
        BookingStateMachine sm2 = new BookingStateMachine();

        BookingCommand.BookTicket command = book(REQUEST_A, "G01", "Passenger A", TICKET_A, TIME_1);

        BookingStateMachine.ApplyOutcome outcome1 = sm1.apply(1, command);
        BookingStateMachine.ApplyOutcome outcome2 = sm2.apply(1, command);

        BookingStateMachine.ApplyOutcome.BusinessResult result1 = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome1);
        BookingStateMachine.ApplyOutcome.BusinessResult result2 = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome2);

        // Kết quả trả về đúng TICKET_A
        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), result1.applyResult());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), result2.applyResult());
        assertFalse(result1.isReplay());
        assertFalse(result2.isReplay());

        // Vé lưu trong cả hai state machine đúng là TICKET_A với trạng thái ACTIVE
        assertEquals(Optional.of("ACTIVE"), sm1.ticketStatus(TICKET_A));
        assertEquals(Optional.of("ACTIVE"), sm2.ticketStatus(TICKET_A));

        // Không có ticketId nào khác được sinh thêm
        assertEquals(1, sm1.totalTicketCount());
        assertEquals(1, sm2.totalTicketCount());
        assertEquals(1L, sm1.activeTicketCount());
        assertEquals(1L, sm2.activeTicketCount());
    }

    /**
     * 2. BOOK A rồi BOOK B cùng ghế G05 (requestId khác): A CONFIRMED (appliedLogIndex = index của A),
     * B REJECTED(SEAT_ALREADY_BOOKED) (appliedLogIndex = index của B); chỉ 1 vé ACTIVE.
     */
    @Test
    void test02_bookAThenBookBOnSameSeatFirstWinsSecondRejected() {
        BookingStateMachine sm = new BookingStateMachine();

        BookingCommand.BookTicket commandA = book(REQUEST_A, "G05", "Passenger A", TICKET_A, TIME_1);
        BookingCommand.BookTicket commandB = book(REQUEST_B, "G05", "Passenger B", TICKET_B, TIME_2);

        BookingStateMachine.ApplyOutcome outcomeA = sm.apply(1, commandA);
        BookingStateMachine.ApplyOutcome outcomeB = sm.apply(2, commandB);

        BookingStateMachine.ApplyOutcome.BusinessResult resultA = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeA);
        BookingStateMachine.ApplyOutcome.BusinessResult resultB = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeB);

        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), resultA.applyResult());
        assertEquals(ApplyResult.bookRejected(REQUEST_B, ApplyResult.SEAT_ALREADY_BOOKED, 2), resultB.applyResult());

        // Chỉ đúng 1 vé ACTIVE trên ghế G05 (vé của A)
        assertEquals(Optional.of("ACTIVE"), sm.ticketStatus(TICKET_A));
        assertEquals(Optional.empty(), sm.ticketStatus(TICKET_B));
        assertEquals(1, sm.totalTicketCount());
        assertEquals(1L, sm.activeTicketCount());
        assertEquals(Optional.of("BOOKED"), sm.seatStatus("G05"));
    }

    /**
     * 3. Đảo thứ tự (BOOK B rồi BOOK A, cùng nội dung): B thắng, A bị từ chối.
     * Thứ tự do LOG INDEX quyết định, không do giờ phát lệnh (đặt issuedAt của lệnh sau NHỎ HƠN
     * lệnh trước, vẫn không đổi kết quả) và không do thứ tự chữ cái của requestId.
     */
    @Test
    void test03_orderDeterminedStrictlyByLogIndexIndependentOfIssuedAtOrRequestIdOrdering() {
        // 3a: Đảo thứ tự lệnh, issuedAt của lệnh sau (A) nhỏ hơn lệnh trước (B)
        BookingStateMachine sm1 = new BookingStateMachine();
        BookingCommand.BookTicket commandB_early = book(REQUEST_B, "G05", "Passenger B", TICKET_B, TIME_2);
        BookingCommand.BookTicket commandA_earlierTime = book(REQUEST_A, "G05", "Passenger A", TICKET_A, TIME_EARLIER);

        assertTrue(commandA_earlierTime.issuedAt().isBefore(commandB_early.issuedAt()),
                "commandA phải có issuedAt sớm hơn commandB");

        // B apply tại index 1, A apply tại index 2 -> B phải thắng dù issuedAt muộn hơn
        var outcomeB1 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm1.apply(1, commandB_early));
        var outcomeA1 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm1.apply(2, commandA_earlierTime));

        assertEquals(ApplyResult.bookConfirmed(REQUEST_B, TICKET_B, 1), outcomeB1.applyResult());
        assertEquals(ApplyResult.bookRejected(REQUEST_A, ApplyResult.SEAT_ALREADY_BOOKED, 2), outcomeA1.applyResult());
        assertEquals(Optional.of("ACTIVE"), sm1.ticketStatus(TICKET_B));
        assertEquals(Optional.empty(), sm1.ticketStatus(TICKET_A));

        // 3b: Thứ tự chữ cái của requestId:
        // Case 1: requestId người thắng ("9999...") lớn hơn người thua ("0000...") theo alphabet
        BookingStateMachine sm2 = new BookingStateMachine();
        BookingCommand.BookTicket cmdWinnerBigId = book(REQUEST_NINE, "G05", "Winner", "tk-9", TIME_1);
        BookingCommand.BookTicket cmdLoserSmallId = book(REQUEST_ZERO, "G05", "Loser", "tk-0", TIME_2);
        assertTrue(REQUEST_NINE.compareTo(REQUEST_ZERO) > 0);

        var outWinnerBig = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm2.apply(1, cmdWinnerBigId));
        var outLoserSmall = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm2.apply(2, cmdLoserSmallId));

        assertEquals(ApplyResult.bookConfirmed(REQUEST_NINE, "tk-9", 1), outWinnerBig.applyResult());
        assertEquals(ApplyResult.bookRejected(REQUEST_ZERO, ApplyResult.SEAT_ALREADY_BOOKED, 2), outLoserSmall.applyResult());

        // Case 2: requestId người thắng ("0000...") nhỏ hơn người thua ("9999...") theo alphabet
        BookingStateMachine sm3 = new BookingStateMachine();
        BookingCommand.BookTicket cmdWinnerSmallId = book(REQUEST_ZERO, "G05", "Winner", "tk-0", TIME_1);
        BookingCommand.BookTicket cmdLoserBigId = book(REQUEST_NINE, "G05", "Loser", "tk-9", TIME_2);

        var outWinnerSmall = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm3.apply(1, cmdWinnerSmallId));
        var outLoserBig = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm3.apply(2, cmdLoserBigId));

        assertEquals(ApplyResult.bookConfirmed(REQUEST_ZERO, "tk-0", 1), outWinnerSmall.applyResult());
        assertEquals(ApplyResult.bookRejected(REQUEST_NINE, ApplyResult.SEAT_ALREADY_BOOKED, 2), outLoserBig.applyResult());
    }

    /**
     * 4. BOOK A và BOOK B khác ghế: cả hai CONFIRMED với đúng ticketId của từng lệnh,
     * appliedLogIndex tăng đúng thứ tự.
     */
    @Test
    void test04_bookAAndBookBOnDifferentSeatsBothConfirmedInLogOrder() {
        BookingStateMachine sm = new BookingStateMachine();

        BookingCommand.BookTicket commandA = book(REQUEST_A, "G01", "Passenger A", TICKET_A, TIME_1);
        BookingCommand.BookTicket commandB = book(REQUEST_B, "G02", "Passenger B", TICKET_B, TIME_2);

        var outcomeA = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, sm.apply(1, commandA));
        var outcomeB = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, sm.apply(2, commandB));

        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), outcomeA.applyResult());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_B, TICKET_B, 2), outcomeB.applyResult());

        assertEquals(Optional.of("ACTIVE"), sm.ticketStatus(TICKET_A));
        assertEquals(Optional.of("ACTIVE"), sm.ticketStatus(TICKET_B));
        assertEquals(2, sm.totalTicketCount());
        assertEquals(2L, sm.activeTicketCount());
        assertEquals(2L, sm.lastApplied());
        assertEquals(Optional.of("BOOKED"), sm.seatStatus("G01"));
        assertEquals(Optional.of("BOOKED"), sm.seatStatus("G02"));
    }

    /**
     * 5. Replay BOOK A cùng requestId (ticketId/issuedAt khác do Leader mới): trả kết quả gốc,
     * appliedLogIndex gốc, không thêm vé; đổi payload cùng requestId: REQUEST_ID_CONFLICT,
     * kết quả gốc không đổi.
     */
    @Test
    void test05_replaySameRequestIdReturnsOriginalResultPayloadConflictReturnsConflict() {
        BookingStateMachine sm = new BookingStateMachine();

        BookingCommand.BookTicket original = book(REQUEST_A, "G01", "Passenger A", TICKET_A, TIME_1);
        var initialOutcome = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, sm.apply(1, original));
        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), initialOutcome.applyResult());

        // Replay cùng requestId, cùng ý định (G01, Passenger A) nhưng ticketId/issuedAt khác do Leader mới
        BookingCommand.BookTicket retrySameIntent = book(REQUEST_A, "G01", "Passenger A", "tk-A-leader2", TIME_2);
        var replayOutcome = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, sm.apply(2, retrySameIntent));

        assertTrue(replayOutcome.isReplay());
        assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1), replayOutcome.applyResult());
        assertEquals(1, sm.totalTicketCount(), "Không được tạo thêm vé cho lệnh replay");
        assertEquals(Optional.empty(), sm.ticketStatus("tk-A-leader2"));

        // Thay đổi payload cùng requestId (sang ghế G02): REQUEST_ID_CONFLICT
        BookingCommand.BookTicket conflictPayload = book(REQUEST_A, "G02", "Passenger A", "tk-conflict", TIME_2);
        BookingStateMachine.ApplyOutcome conflictOutcome = sm.apply(3, conflictPayload);

        BookingStateMachine.ApplyOutcome.RequestIdConflict conflict = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.RequestIdConflict.class, conflictOutcome);
        assertEquals(REQUEST_A, conflict.requestId());
        assertEquals(3L, conflict.logIndex());

        // Trạng thái gốc không đổi: G01 vẫn BOOKED với TICKET_A, G02 vẫn AVAILABLE, không có vé tk-conflict
        assertEquals(Optional.of("BOOKED"), sm.seatStatus("G01"));
        assertEquals(Optional.of("AVAILABLE"), sm.seatStatus("G02"));
        assertEquals(Optional.empty(), sm.ticketStatus("tk-conflict"));
        assertEquals(3L, sm.lastApplied());
    }

    /**
     * 6. Hai state machine độc lập (mô phỏng hai Node) apply cùng chuỗi entry
     * [BOOK A, BOOK B trùng ghế, replay A, conflict, NO_OP, BOOK C khác ghế] cho trạng thái
     * ghế/vé, kết quả từng entry và lastApplied GIỐNG HỆT nhau.
     */
    @Test
    void test06_independentStateMachinesReplicatingSameSequenceYieldIdenticalDeterministicState() {
        BookingStateMachine sm1 = new BookingStateMachine();
        BookingStateMachine sm2 = new BookingStateMachine();

        List<BookingCommand> commands = List.of(
                book(REQUEST_A, "G01", "Passenger A", TICKET_A, TIME_1),
                book(REQUEST_B, "G01", "Passenger B", TICKET_B, TIME_2),
                book(REQUEST_A, "G01", "Passenger A", "tk-A-retry", TIME_2),
                book(REQUEST_A, "G02", "Passenger A", "tk-A-conflict", TIME_2),
                new BookingCommand.NoOp(),
                book(REQUEST_C, "G03", "Passenger C", TICKET_C, TIME_2)
        );

        for (int i = 0; i < commands.size(); i++) {
            long index = i + 1L;
            BookingCommand cmd = commands.get(i);
            BookingStateMachine.ApplyOutcome out1 = sm1.apply(index, cmd);
            BookingStateMachine.ApplyOutcome out2 = sm2.apply(index, cmd);
            assertEquals(out1, out2, "Outcome differs at index " + index);
        }

        assertEquals(sm1.lastApplied(), sm2.lastApplied());
        assertEquals(6L, sm1.lastApplied());
        assertEquals(sm1.allSeatStatuses(), sm2.allSeatStatuses());
        assertEquals(sm1.totalTicketCount(), sm2.totalTicketCount());
        assertEquals(sm1.activeTicketCount(), sm2.activeTicketCount());
    }

    /**
     * 7. Chuẩn hóa phản hồi: invalid seat (G00, G11, chữ thường), tripId sai, requestId không phải UUID:
     * không thể dựng BookingCommand hợp lệ (bị chặn trước append); duplicate seat là REJECTED với reason
     * SEAT_ALREADY_BOOKED (không phải lỗi pre-check); payload conflict là REQUEST_ID_CONFLICT
     * (không phải REJECTED, không ghi đè kết quả gốc).
     */
    @Test
    void test07_responseStandardizationPrecheckVsBusinessRejectionVsConflict() {
        // 7a: Pre-check validation trong constructor BookingCommand
        assertThrows(IllegalArgumentException.class, () -> book(REQUEST_A, "G00", "A", TICKET_A, TIME_1));
        assertThrows(IllegalArgumentException.class, () -> book(REQUEST_A, "G11", "A", TICKET_A, TIME_1));
        assertThrows(IllegalArgumentException.class, () -> book(REQUEST_A, "g01", "A", TICKET_A, TIME_1));
        assertThrows(IllegalArgumentException.class, () -> new BookingCommand.BookTicket(
                REQUEST_A, "SE-002", "G01", "A", TICKET_A, TIME_1));
        assertThrows(IllegalArgumentException.class, () -> new BookingCommand.BookTicket(
                "invalid-uuid", TRIP_ID, "G01", "A", TICKET_A, TIME_1));

        // 7b: Duplicate seat -> Business rejection (chứ không phải pre-check error)
        BookingStateMachine sm = new BookingStateMachine();
        sm.apply(1, book(REQUEST_A, "G01", "A", TICKET_A, TIME_1));

        var rejectedOutcome = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class,
                sm.apply(2, book(REQUEST_B, "G01", "B", TICKET_B, TIME_2)));
        assertEquals(ApplyResult.Outcome.REJECTED, rejectedOutcome.applyResult().outcome());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, rejectedOutcome.applyResult().reasonCode());

        // 7c: Payload conflict -> RequestIdConflict (chứ không phải REJECTED)
        var conflictOutcome = sm.apply(3, book(REQUEST_A, "G02", "A", "tk-conflict", TIME_2));
        BookingStateMachine.ApplyOutcome.RequestIdConflict conflict = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.RequestIdConflict.class, conflictOutcome);
        assertEquals(REQUEST_A, conflict.requestId());
        assertEquals(3L, conflict.logIndex());
        assertEquals(Optional.of("BOOKED"), sm.seatStatus("G01"));
        assertEquals(Optional.of("AVAILABLE"), sm.seatStatus("G02"));
    }

    // =========================================================================
    // NHÓM B: Trên PostgreSQL thật (chỉ chạy khi có RAILWAY_TEST_*)
    // =========================================================================

    /**
     * 8. Cùng chuỗi entry của test 6 chạy qua JdbcCommittedCommandApplier trên HAI schema riêng
     * (mô phỏng hai Node/hai DB): kết quả từng entry, bảng seats/tickets/request_results/business_events,
     * last_applied giống hệt nhau giữa hai schema VÀ giống kết quả của BookingStateMachine in-memory.
     */
    @Test
    void test08_twoIndependentPostgresDatabasesReplicatingSameSequenceYieldIdenticalState() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        try (TestDatabase db1 = TestDatabase.open(jdbcUrl, username, password);
             TestDatabase db2 = TestDatabase.open(jdbcUrl, username, password)) {

            JdbcCommittedCommandApplier applier1 = new JdbcCommittedCommandApplier(db1.connectionFactory());
            JdbcCommittedCommandApplier applier2 = new JdbcCommittedCommandApplier(db2.connectionFactory());
            BookingStateMachine inMemory = new BookingStateMachine();

            List<BookingCommand> commands = List.of(
                    book(REQUEST_A, "G01", "Passenger A", TICKET_A, TIME_1),
                    book(REQUEST_B, "G01", "Passenger B", TICKET_B, TIME_2),
                    book(REQUEST_A, "G01", "Passenger A", "tk-A-retry", TIME_2),
                    book(REQUEST_A, "G02", "Passenger A", "tk-A-conflict", TIME_2),
                    new BookingCommand.NoOp(),
                    book(REQUEST_C, "G03", "Passenger C", TICKET_C, TIME_2)
            );

            for (int i = 0; i < commands.size(); i++) {
                long index = i + 1L;
                BookingCommand cmd = commands.get(i);

                db1.insertCommittedLog(index, cmd);
                db2.insertCommittedLog(index, cmd);

                var outDb1 = applier1.applyCommitted(index, cmd);
                var outDb2 = applier2.applyCommitted(index, cmd);
                var outMem = inMemory.apply(index, cmd);

                assertEquals(outDb1, outDb2, "DB1 and DB2 outcome differ at index " + index);
                assertEquals(Optional.of(outMem), outDb1, "DB1 differs from inMemory at index " + index);
            }

            assertEquals(6L, applier1.lastApplied());
            assertEquals(6L, applier2.lastApplied());
            assertEquals(inMemory.lastApplied(), applier1.lastApplied());

            // So sánh trạng thái ghế từ DB với in-memory
            assertEquals(inMemory.allSeatStatuses(), db1.readSeatStatusMap());
            assertEquals(inMemory.allSeatStatuses(), db2.readSeatStatusMap());

            // So sánh số lượng vé và bản ghi giữa 2 database
            assertEquals(db1.queryInt("SELECT COUNT(*) FROM tickets"), db2.queryInt("SELECT COUNT(*) FROM tickets"));
            assertEquals(db1.queryInt("SELECT COUNT(*) FROM request_results"), db2.queryInt("SELECT COUNT(*) FROM request_results"));
            assertEquals(db1.queryInt("SELECT COUNT(*) FROM business_events"), db2.queryInt("SELECT COUNT(*) FROM business_events"));

            db1.printAndAssertSnapshot();
            db2.printAndAssertSnapshot();
        }
    }

    /**
     * 9. BOOK A rồi BOOK B trùng ghế qua JDBC: A CONFIRMED, B REJECTED; request_results của B lưu
     * REJECTED với reason SEAT_ALREADY_BOOKED; đảo thứ tự thì kết quả đảo theo log index.
     */
    @Test
    void test09_bookAThenBookBOnSameSeatViaJdbcReversesStrictlyWithLogOrder() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        // 9a: BOOK A (index 1), BOOK B (index 2) cùng ghế G05
        try (TestDatabase db1 = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcCommittedCommandApplier applier1 = new JdbcCommittedCommandApplier(db1.connectionFactory());

            BookingCommand.BookTicket cmdA = book(REQUEST_A, "G05", "Passenger A", TICKET_A, TIME_1);
            BookingCommand.BookTicket cmdB = book(REQUEST_B, "G05", "Passenger B", TICKET_B, TIME_2);

            db1.insertCommittedLog(1, cmdA);
            var outcomeA = applier1.applyCommitted(1, cmdA).orElseThrow();
            assertEquals(ApplyResult.bookConfirmed(REQUEST_A, TICKET_A, 1),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeA).applyResult());

            db1.insertCommittedLog(2, cmdB);
            var outcomeB = applier1.applyCommitted(2, cmdB).orElseThrow();
            assertEquals(ApplyResult.bookRejected(REQUEST_B, ApplyResult.SEAT_ALREADY_BOOKED, 2),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeB).applyResult());

            // Kiểm tra request_results của B lưu đúng REJECTED với reason SEAT_ALREADY_BOOKED
            assertEquals("APPLIED_REJECTED", db1.queryString(
                    "SELECT state FROM request_results WHERE request_id = '" + REQUEST_B + "'"));
            assertEquals("SEAT_ALREADY_BOOKED", db1.queryString(
                    "SELECT reason_code FROM request_results WHERE request_id = '" + REQUEST_B + "'"));
            assertEquals(1, db1.queryInt("SELECT COUNT(*) FROM tickets WHERE seat_id = 'G05' AND status = 'ACTIVE'"));
            assertEquals(TICKET_A, db1.queryString("SELECT ticket_id FROM tickets WHERE seat_id = 'G05'"));
        }

        // 9b: Đảo thứ tự: BOOK B (index 1), BOOK A (index 2) cùng ghế G05
        try (TestDatabase db2 = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcCommittedCommandApplier applier2 = new JdbcCommittedCommandApplier(db2.connectionFactory());

            BookingCommand.BookTicket cmdB = book(REQUEST_B, "G05", "Passenger B", TICKET_B, TIME_1);
            BookingCommand.BookTicket cmdA = book(REQUEST_A, "G05", "Passenger A", TICKET_A, TIME_2);

            db2.insertCommittedLog(1, cmdB);
            var outcomeB = applier2.applyCommitted(1, cmdB).orElseThrow();
            assertEquals(ApplyResult.bookConfirmed(REQUEST_B, TICKET_B, 1),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeB).applyResult());

            db2.insertCommittedLog(2, cmdA);
            var outcomeA = applier2.applyCommitted(2, cmdA).orElseThrow();
            assertEquals(ApplyResult.bookRejected(REQUEST_A, ApplyResult.SEAT_ALREADY_BOOKED, 2),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomeA).applyResult());

            // Kết quả đảo ngược hoàn toàn theo log index: B thắng, A thua
            assertEquals("SEAT_ALREADY_BOOKED", db2.queryString(
                    "SELECT reason_code FROM request_results WHERE request_id = '" + REQUEST_A + "'"));
            assertEquals(TICKET_B, db2.queryString("SELECT ticket_id FROM tickets WHERE seat_id = 'G05'"));
        }
    }

    /**
     * 10. Qua BookingRequestHandler (transport giả, như BookingRequestHandlerTest):
     * invalid seat → ERROR_RESPONSE(INVALID_REQUEST) và đường commit KHÔNG được gọi;
     * duplicate seat → BOOK_TICKET_RESPONSE outcome REJECTED, reasonCode SEAT_ALREADY_BOOKED;
     * cùng requestId khác payload → ERROR_RESPONSE(REQUEST_ID_CONFLICT).
     */
    @Test
    void test10_bookingRequestHandlerStandardizesResponsesAcrossInvalidDuplicateAndConflict() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        try (TestDatabase database = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(database.connectionFactory());
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(database.connectionFactory());
            DatabaseCommitter committer = new DatabaseCommitter(database, repository, applier);

            AtomicInteger ticketSequence = new AtomicInteger();
            BookingRequestHandler handler = new BookingRequestHandler(
                    committer,
                    timeout -> CompletableFuture.completedFuture(
                            new BookingRequestHandler.ReadBarrierResult(
                                    Math.max(1, repository.lastApplied()), "node-1")),
                    () -> "ticket-db-" + ticketSequence.incrementAndGet(),
                    Clock.fixed(TIME_1, ZoneOffset.UTC),
                    repository,
                    database::readSeatSnapshot,
                    "node-1",
                    TEST_TIMEOUT);

            // 10a: Invalid seat -> ERROR_RESPONSE(INVALID_REQUEST), đường commit không được gọi
            MessageEnvelope<?> responseInvalid1 = handler.handleBooking(bookRequest(REQUEST_A, "G11", "Passenger A"));
            assertError("INVALID_REQUEST", responseInvalid1);

            MessageEnvelope<?> responseInvalid2 = handler.handleBooking(bookRequest(REQUEST_A, "g01", "Passenger A"));
            assertError("INVALID_REQUEST", responseInvalid2);

            assertEquals(0, committer.submitCount(), "Commit path must not be called for invalid requests");
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM raft_log"));
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM tickets"));

            // 10b: Đặt ghế hợp lệ lần 1: CONFIRMED
            MessageEnvelope<?> confirmed = handler.handleBooking(bookRequest(REQUEST_A, "G01", "Passenger A"));
            assertBookResponse(confirmed, "CONFIRMED", "ticket-db-1", null, 1L);
            assertEquals(1, committer.submitCount());

            // Đặt trùng ghế lần 2: duplicate seat -> BOOK_TICKET_RESPONSE outcome REJECTED, reasonCode SEAT_ALREADY_BOOKED
            MessageEnvelope<?> rejected = handler.handleBooking(bookRequest(REQUEST_B, "G01", "Passenger B"));
            assertBookResponse(rejected, "REJECTED", null, ApplyResult.SEAT_ALREADY_BOOKED, 2L);
            assertEquals(2, committer.submitCount());

            // 10c: Cùng requestId khác payload -> ERROR_RESPONSE(REQUEST_ID_CONFLICT)
            MessageEnvelope<?> conflict = handler.handleBooking(bookRequest(REQUEST_A, "G02", "Passenger A"));
            assertError("REQUEST_ID_CONFLICT", conflict);

            // Xác nhận database snapshot vẫn bảo đảm đúng 1 vé ACTIVE trên G01
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM tickets WHERE status = 'ACTIVE'"));
            database.printAndAssertSnapshot();
        }
    }

    // =========================================================================
    // Helper methods & Fixtures
    // =========================================================================

    private static BookingCommand.BookTicket book(
            String requestId, String seatId, String passengerName, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(requestId, TRIP_ID, seatId, passengerName, ticketId, issuedAt);
    }

    private static MessageEnvelope<JsonNode> bookRequest(
            String requestId, String seatId, String passengerName) {
        return request(MessageType.BOOK_TICKET,
                "{\"requestId\":\"" + requestId + "\",\"tripId\":\"" + TRIP_ID + "\","
                        + "\"seatId\":\"" + seatId + "\",\"passengerName\":\""
                        + passengerName + "\"}");
    }

    private static MessageEnvelope<JsonNode> request(MessageType type, String payload) {
        try {
            return MessageEnvelope.createRequest(type, "client-test", MAPPER.readTree(payload));
        } catch (IOException exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }

    private static void assertBookResponse(
            MessageEnvelope<?> response,
            String outcome,
            String ticketId,
            String reasonCode,
            Long appliedLogIndex) {
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, response.getType());
        assertTrue(MessageEnvelope.isValidUuid(response.getMessageId()));
        assertTrue(MessageEnvelope.isValidUuid(response.getCorrelationId()));
        assertNotEquals(response.getCorrelationId(), response.getMessageId());
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = MAPPER.convertValue(response.getPayload(), Map.class);
        assertEquals(outcome, payload.get("outcome"));
        assertEquals(ticketId, payload.get("ticketId"));
        assertEquals(reasonCode, payload.get("reasonCode"));
        assertEquals(appliedLogIndex, payload.get("appliedLogIndex"));
    }

    private static void assertError(String code, MessageEnvelope<?> response) {
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(code, errorPayload(response).getErrorCode());
    }

    private static ErrorResponseDto errorPayload(MessageEnvelope<?> response) {
        return MAPPER.convertValue(response.getPayload(), ErrorResponseDto.class);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * LOCAL database committer driver for test purposes only.
     */
    private static final class DatabaseCommitter implements BookingRequestHandler.CommandSubmitter {
        private final TestDatabase database;
        private final JdbcBookingRepository repository;
        private final JdbcCommittedCommandApplier applier;
        private final AtomicInteger submitCount = new AtomicInteger();

        private DatabaseCommitter(
                TestDatabase database,
                JdbcBookingRepository repository,
                JdbcCommittedCommandApplier applier) {
            this.database = database;
            this.repository = repository;
            this.applier = applier;
        }

        @Override
        public CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) throws Exception {
            submitCount.incrementAndGet();
            long index = repository.lastApplied() + 1;
            database.insertCommittedLog(index, command);
            BookingStateMachine.ApplyOutcome outcome =
                    applier.applyCommitted(index, command).orElseThrow();
            if (outcome instanceof BookingStateMachine.ApplyOutcome.RequestIdConflict) {
                throw new BookingRequestHandler.RequestIdConflictException();
            }
            return CompletableFuture.completedFuture(
                    ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome).applyResult());
        }

        private int submitCount() {
            return submitCount.get();
        }
    }

    /**
     * Isolated PostgreSQL schema test harness for DeterministicBookingTests.
     */
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
                } catch (SQLException cleanupException) {
                    exception.addSuppressed(cleanupException);
                }
                throw exception;
            }
        }

        private static TestDatabase open(String jdbcUrl, String username, String password) throws Exception {
            return new TestDatabase(jdbcUrl, username, password);
        }

        private ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        private void insertCommittedLog(long index, BookingCommand command) throws Exception {
            Map<String, Object> json = new LinkedHashMap<>();
            if (command instanceof BookingCommand.BookTicket book) {
                json.put("requestId", book.requestId());
                json.put("tripId", book.tripId());
                json.put("seatId", book.seatId());
                json.put("passengerName", book.passengerName());
                json.put("ticketId", book.ticketId());
                json.put("issuedAt", book.issuedAt().toString());
            } else if (command instanceof BookingCommand.CancelTicket cancel) {
                json.put("requestId", cancel.requestId());
                json.put("ticketId", cancel.ticketId());
                json.put("cancelledAt", cancel.cancelledAt().toString());
            }
            try (Connection connection = connectionFactory.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                 + "VALUES (?, 1, ?, ?::jsonb)")) {
                statement.setLong(1, index);
                statement.setString(2, command.type().name());
                statement.setString(3, MAPPER.writeValueAsString(json));
                statement.executeUpdate();
            }
        }

        private Map<String, String> readSeatStatusMap() {
            Map<String, String> map = new LinkedHashMap<>();
            try (Connection connection = connectionFactory.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT seat_id, status FROM seats WHERE trip_id = 'SE-001' ORDER BY seat_id");
                 ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    map.put(rs.getString(1), rs.getString(2));
                }
            } catch (SQLException exception) {
                throw new BookingRepository.StorageException("seat status query failed", exception);
            }
            return map;
        }

        private Map<String, BookingRequestHandler.SeatState> readSeatSnapshot() {
            Map<String, BookingRequestHandler.SeatState> result = new LinkedHashMap<>();
            try (Connection connection = connectionFactory.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT seat_id, status, current_ticket_id FROM seats "
                                 + "WHERE trip_id = 'SE-001' ORDER BY seat_id");
                 ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.put(rows.getString(1),
                            new BookingRequestHandler.SeatState(rows.getString(2), rows.getString(3)));
                }
            } catch (SQLException exception) {
                throw new BookingRepository.StorageException("snapshot query failed", exception);
            }
            return result;
        }

        private int queryInt(String sql) throws SQLException {
            try (Connection connection = connectionFactory.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }

        private String queryString(String sql) throws SQLException {
            try (Connection connection = connectionFactory.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getString(1);
            }
        }

        private long queryLong(String sql) throws SQLException {
            try (Connection connection = connectionFactory.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getLong(1);
            }
        }

        private void printAndAssertSnapshot() throws SQLException {
            int totalSeats = queryInt("SELECT COUNT(*) FROM seats WHERE trip_id = 'SE-001'");
            int duplicateActiveSeats = queryInt("SELECT COUNT(*) FROM (SELECT trip_id, seat_id "
                    + "FROM tickets WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id "
                    + "HAVING COUNT(*) > 1) duplicate_active");
            int duplicateRequestIds = queryInt("SELECT COUNT(*) FROM (SELECT request_id "
                    + "FROM request_results GROUP BY request_id HAVING COUNT(*) > 1) duplicate_requests");
            long lastApplied = queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");

            assertEquals(10, totalSeats);
            assertEquals(0, duplicateActiveSeats);
            assertEquals(0, duplicateRequestIds);
            assertTrue(lastApplied >= 1);
        }

        private void createSchema() throws SQLException {
            try (Connection connection = DriverManager.getConnection(baseUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + schema + "\"");
            }
        }

        private void runMigration(String migrationName) throws IOException, SQLException {
            Path migrationPath = Path.of("database", "migration", migrationName);
            if (!Files.isRegularFile(migrationPath)) {
                migrationPath = Path.of("..", "database", "migration", migrationName);
            }
            String migration = Files.readString(migrationPath, StandardCharsets.UTF_8);
            try (Connection connection = connectionFactory.getConnection();
                 Statement statement = connection.createStatement()) {
                for (String sql : migration.split(";")) {
                    if (!sql.isBlank()) {
                        statement.execute(sql);
                    }
                }
            }
        }

        private void dropSchema() throws SQLException {
            try (Connection connection = DriverManager.getConnection(baseUrl, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }

        @Override
        public void close() throws SQLException {
            dropSchema();
        }
    }
}
