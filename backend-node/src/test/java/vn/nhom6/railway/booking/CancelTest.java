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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPEC-03 §4.3, SPEC-05 §1, §2, §3, §3.1, §4, §5 & D23:
 * Kiểm thử toàn diện tính năng Hủy vé (CANCEL_TICKET) qua Raft log và Transactional Apply.
 *
 * <p>Bao gồm:
 * <ul>
 *   <li>1. CancelBookingCommand: Validation DTO và chuyển đổi với BookingCommand.CancelTicket.
 *   <li>2. State Machine In-Memory: Logic tất định, idempotent, replay, conflict, giải phóng ghế.
 *   <li>3. Database & Raft Transaction: Lưu bền vững PostgreSQL, atomicity, rollback khi lỗi giữa chừng,
 *       không xóa record vé, audit business_events và request_results, replay log từ đầu.
 * </ul>
 */
@DisplayName("CancelTest — Kiểm thử Hủy vé qua Raft + Transaction")
public class CancelTest {

    private static final String TRIP_ID = "SE-001";
    private static final String REQ_BOOK_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQ_BOOK_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQ_BOOK_3 = "33333333-3333-3333-3333-333333333333";
    private static final String REQ_CANCEL_1 = "cccccccc-1111-1111-1111-111111111111";
    private static final String REQ_CANCEL_2 = "cccccccc-2222-2222-2222-222222222222";
    private static final String REQ_CANCEL_3 = "cccccccc-3333-3333-3333-333333333333";
    private static final String REQ_CANCEL_NEW = "cccccccc-9999-9999-9999-999999999999";

    private static final String TKT_1 = "ticket-001";
    private static final String TKT_2 = "ticket-002";
    private static final String TKT_3 = "ticket-003";

    private static final Instant T1 = Instant.parse("2026-10-29T10:00:00.000Z");
    private static final Instant T2 = Instant.parse("2026-10-29T10:01:00.000Z");
    private static final Instant T3 = Instant.parse("2026-10-29T10:02:00.000Z");
    private static final Instant T4 = Instant.parse("2026-10-29T10:03:00.000Z");

    // =======================================================================
    // 1. UNIT TESTS: CancelBookingCommand
    // =======================================================================

    @Nested
    @DisplayName("1. CancelBookingCommand Validation & Mapping")
    class CancelBookingCommandValidationTests {

        @Test
        @DisplayName("Tạo CancelBookingCommand hợp lệ và chuyển đổi toBookingCommand / from")
        void validCommandCreationAndConversion() {
            CancelBookingCommand cmd = new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2);
            assertEquals(REQ_CANCEL_1, cmd.requestId());
            assertEquals(TKT_1, cmd.ticketId());
            assertEquals(T2, cmd.cancelledAt());
            assertEquals(BookingCommand.Type.CANCEL_TICKET, cmd.type());

            BookingCommand.CancelTicket ticketCmd = cmd.toBookingCommand();
            assertNotNull(ticketCmd);
            assertEquals(REQ_CANCEL_1, ticketCmd.requestId());
            assertEquals(TKT_1, ticketCmd.ticketId());
            assertEquals(T2, ticketCmd.cancelledAt());
            assertEquals(BookingCommand.Type.CANCEL_TICKET, ticketCmd.type());

            CancelBookingCommand roundTrip = CancelBookingCommand.from(ticketCmd);
            assertEquals(cmd, roundTrip);
        }

        @Test
        @DisplayName("Từ chối requestId không phải UUID chữ thường chuẩn")
        void rejectInvalidRequestId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand("not-a-uuid", TKT_1, T2));
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(null, TKT_1, T2));
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand("11111111-1111-1111-1111-11111111111G", TKT_1, T2));
        }

        @Test
        @DisplayName("Từ chối ticketId rỗng hoặc chứa ký tự đặc biệt ngoài ASCII [A-Za-z0-9-]")
        void rejectInvalidTicketId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, "", T2));
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, null, T2));
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, "ticket#invalid", T2));
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, "a".repeat(65), T2));
        }

        @Test
        @DisplayName("Từ chối cancelledAt bị null hoặc có độ phân giải nhỏ hơn millisecond")
        void rejectInvalidCancelledAt() {
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, TKT_1, null));
            Instant subMillis = Instant.parse("2026-10-29T10:00:00.123456Z");
            assertThrows(IllegalArgumentException.class, () ->
                    new CancelBookingCommand(REQ_CANCEL_1, TKT_1, subMillis));
        }
    }

    // =======================================================================
    // 2. IN-MEMORY STATE MACHINE TESTS
    // =======================================================================

    @Nested
    @DisplayName("2. In-Memory State Machine Tests (Tất định & Logic chuyển trạng thái D02)")
    class StateMachineTests {

        private BookingStateMachine sm;

        @BeforeEach
        void setUp() {
            sm = new BookingStateMachine();
        }

        @Test
        @DisplayName("Hủy vé hợp lệ: status vé thành CANCELLED, ghế thành AVAILABLE, record vé vẫn còn")
        void cancelValidTicket() {
            // Index 1: Đặt vé TKT_1 cho ghế G01
            BookingCommand.BookTicket book = new BookingCommand.BookTicket(
                    REQ_BOOK_1, TRIP_ID, "G01", "Nguyen Van A", TKT_1, T1);
            sm.apply(1L, book);
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals("ACTIVE", sm.ticketStatus(TKT_1).orElseThrow());
            assertEquals(1, sm.activeTicketCount());
            assertEquals(1, sm.totalTicketCount());

            // Index 2: Hủy vé TKT_1 qua CancelBookingCommand
            CancelBookingCommand cancelCmd = new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2);
            var outcome = sm.apply(2L, cancelCmd.toBookingCommand());

            BookingStateMachine.ApplyOutcome.BusinessResult result = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertFalse(result.isReplay());
            ApplyResult applyResult = result.applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, applyResult.outcome());
            assertEquals(REQ_CANCEL_1, applyResult.requestId());
            assertEquals(TKT_1, applyResult.ticketId());
            assertEquals(2L, applyResult.appliedLogIndex());

            // Kiểm tra trạng thái sau hủy
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertTrue(sm.seatCurrentTicketId("G01").isEmpty());
            assertEquals("CANCELLED", sm.ticketStatus(TKT_1).orElseThrow());
            assertEquals(0, sm.activeTicketCount(), "Số vé ACTIVE phải về 0");
            assertEquals(1, sm.totalTicketCount(), "Bản ghi vé vẫn phải còn trong state machine để lưu lịch sử");
            assertEquals(2L, sm.lastApplied());
        }

        @Test
        @DisplayName("Hủy vé không tồn tại: mã lỗi đúng TICKET_NOT_ACTIVE, outcome REJECTED, dữ liệu không đổi")
        void cancelNonExistentTicket() {
            CancelBookingCommand cancelCmd = new CancelBookingCommand(REQ_CANCEL_1, "missing-ticket", T1);
            var outcome = sm.apply(1L, cancelCmd.toBookingCommand());

            BookingStateMachine.ApplyOutcome.BusinessResult result = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertFalse(result.isReplay());
            ApplyResult applyResult = result.applyResult();
            assertEquals(ApplyResult.Outcome.REJECTED, applyResult.outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, applyResult.reasonCode());
            assertEquals("missing-ticket", applyResult.ticketId());
            assertEquals(1L, applyResult.appliedLogIndex());

            assertEquals(0, sm.totalTicketCount());
            assertEquals(1L, sm.lastApplied());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
        }

        @Test
        @DisplayName("Hủy vé ở trạng thái không cho phép: vé đã CANCELLED hủy bằng requestId mới bị REJECTED")
        void cancelAlreadyCancelledTicketWithNewRequestId() {
            // Đặt rồi hủy vé TKT_1
            sm.apply(1L, new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "Passenger 1", TKT_1, T1));
            sm.apply(2L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand());
            assertEquals("CANCELLED", sm.ticketStatus(TKT_1).orElseThrow());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());

            // Gửi request mới REQ_CANCEL_NEW cố tình hủy lại vé TKT_1 đã CANCELLED
            var outcome = sm.apply(3L, new CancelBookingCommand(REQ_CANCEL_NEW, TKT_1, T3).toBookingCommand());
            BookingStateMachine.ApplyOutcome.BusinessResult result = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertFalse(result.isReplay());
            ApplyResult applyResult = result.applyResult();
            assertEquals(ApplyResult.Outcome.REJECTED, applyResult.outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, applyResult.reasonCode());
            assertEquals(TKT_1, applyResult.ticketId());
            assertEquals(3L, applyResult.appliedLogIndex());

            assertEquals("CANCELLED", sm.ticketStatus(TKT_1).orElseThrow());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertEquals(1, sm.totalTicketCount());
            assertEquals(0, sm.activeTicketCount());
        }

        @Test
        @DisplayName("Hủy hai lần (idempotent): lần hai cùng requestId trả kết quả gốc (isReplay=true), không đổi state")
        void cancelIdempotentRetrySameRequestId() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "Passenger 1", TKT_1, T1));

            // Lần 1 hủy thành công
            var firstOutcome = sm.apply(2L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand());
            BookingStateMachine.ApplyOutcome.BusinessResult firstResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, firstOutcome);
            assertFalse(firstResult.isReplay());
            assertEquals(ApplyResult.Outcome.CONFIRMED, firstResult.applyResult().outcome());

            // Lần 2 retry y nguyên requestId REQ_CANCEL_1 và ticketId TKT_1
            var secondOutcome = sm.apply(3L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T3).toBookingCommand());
            BookingStateMachine.ApplyOutcome.BusinessResult secondResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, secondOutcome);
            assertTrue(secondResult.isReplay(), "Retry cùng intent phải được đánh dấu isReplay = true");
            assertEquals(firstResult.applyResult(), secondResult.applyResult(), "Kết quả trả về phải khớp kết quả gốc");
            assertEquals(2L, secondResult.applyResult().appliedLogIndex(), "appliedLogIndex giữ nguyên của lần apply đầu");

            assertEquals(3L, sm.lastApplied(), "lastApplied vẫn phải tăng theo logIndex");
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertEquals("CANCELLED", sm.ticketStatus(TKT_1).orElseThrow());
        }

        @Test
        @DisplayName("Cùng requestId nhưng khác ticketId dẫn đến RequestIdConflict, không sửa đổi kết quả gốc")
        void requestIdConflictOnDifferentTicketId() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "Passenger 1", TKT_1, T1));
            sm.apply(2L, new BookingCommand.BookTicket(REQ_BOOK_2, TRIP_ID, "G02", "Passenger 2", TKT_2, T1));

            // Request REQ_CANCEL_1 hủy TKT_1
            sm.apply(3L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand());

            // Dùng lại REQ_CANCEL_1 nhưng nhắm vào TKT_2 -> Khác intent fingerprint!
            var conflictOutcome = sm.apply(4L, new CancelBookingCommand(REQ_CANCEL_1, TKT_2, T3).toBookingCommand());
            BookingStateMachine.ApplyOutcome.RequestIdConflict conflict = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.RequestIdConflict.class, conflictOutcome);
            assertEquals(REQ_CANCEL_1, conflict.requestId());
            assertEquals(4L, conflict.logIndex());

            // Kết quả gốc của REQ_CANCEL_1 trong state machine không bị ghi đè
            ApplyResult stored = sm.storedResult(REQ_CANCEL_1).orElseThrow();
            assertEquals(TKT_1, stored.ticketId());
            assertEquals(ApplyResult.Outcome.CONFIRMED, stored.outcome());
            // TKT_2 vẫn giữ nguyên trạng thái ACTIVE trên ghế G02
            assertEquals("ACTIVE", sm.ticketStatus(TKT_2).orElseThrow());
            assertEquals("BOOKED", sm.seatStatus("G02").orElseThrow());
        }

        @Test
        @DisplayName("Vé hủy xong, ghế đặt lại được bởi vé khác; hủy lại vé cũ không làm hỏng vé mới")
        void seatCanBeRebookedAfterCancellationAndOldTicketCancelDoesNotAffectNewTicket() {
            // 1. Đặt TKT_1 ghế G01
            sm.apply(1L, new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "Passenger A", TKT_1, T1));
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals(TKT_1, sm.seatCurrentTicketId("G01").orElseThrow());

            // 2. Hủy TKT_1 ghế G01 -> Ghế thành AVAILABLE
            sm.apply(2L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertTrue(sm.seatCurrentTicketId("G01").isEmpty());

            // 3. Khách hàng B đặt vé mới TKT_2 trên đúng ghế G01 vừa giải phóng
            var rebookOutcome = sm.apply(3L, new BookingCommand.BookTicket(
                    REQ_BOOK_2, TRIP_ID, "G01", "Passenger B", TKT_2, T3));
            ApplyResult rebookResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, rebookOutcome).applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, rebookResult.outcome());
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals(TKT_2, sm.seatCurrentTicketId("G01").orElseThrow());
            assertEquals("ACTIVE", sm.ticketStatus(TKT_2).orElseThrow());

            // 4a. Client cũ retry hủy TKT_1 với requestId cũ REQ_CANCEL_1
            var retryOldCancel = sm.apply(4L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T4).toBookingCommand());
            BookingStateMachine.ApplyOutcome.BusinessResult retryResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, retryOldCancel);
            assertTrue(retryResult.isReplay());
            assertEquals(TKT_1, retryResult.applyResult().ticketId());
            // Ghế G01 TUYỆT ĐỐI KHÔNG bị giải phóng lại, vẫn phải là của TKT_2!
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals(TKT_2, sm.seatCurrentTicketId("G01").orElseThrow());
            assertEquals("ACTIVE", sm.ticketStatus(TKT_2).orElseThrow());

            // 4b. Client cũ gửi request mới REQ_CANCEL_NEW để hủy TKT_1 lần nữa
            var newReqOldCancel = sm.apply(5L, new CancelBookingCommand(REQ_CANCEL_NEW, TKT_1, T4).toBookingCommand());
            BookingStateMachine.ApplyOutcome.BusinessResult newReqResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, newReqOldCancel);
            assertFalse(newReqResult.isReplay());
            assertEquals(ApplyResult.Outcome.REJECTED, newReqResult.applyResult().outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, newReqResult.applyResult().reasonCode());

            // Ghế G01 và vé TKT_2 vẫn hoàn toàn an toàn và ACTIVE
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals(TKT_2, sm.seatCurrentTicketId("G01").orElseThrow());
            assertEquals("ACTIVE", sm.ticketStatus(TKT_2).orElseThrow());
            assertEquals(1, sm.activeTicketCount());
            assertEquals(2, sm.totalTicketCount());
        }

        @Test
        @DisplayName("Vé nhiều ghế: giải phóng đủ tất cả ghế theo thứ tự tất định")
        void cancelMultipleTicketsReleasesAllSeats() {
            // Đặt 3 vé riêng biệt cho 3 ghế G01, G02, G03
            sm.apply(1L, new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "User 1", TKT_1, T1));
            sm.apply(2L, new BookingCommand.BookTicket(REQ_BOOK_2, TRIP_ID, "G02", "User 2", TKT_2, T1));
            sm.apply(3L, new BookingCommand.BookTicket(REQ_BOOK_3, TRIP_ID, "G03", "User 3", TKT_3, T1));

            assertEquals(3, sm.activeTicketCount());
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals("BOOKED", sm.seatStatus("G02").orElseThrow());
            assertEquals("BOOKED", sm.seatStatus("G03").orElseThrow());

            // Hủy tuần tự theo thứ tự seatId tăng dần (G01, G02, G03)
            sm.apply(4L, new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand());
            sm.apply(5L, new CancelBookingCommand(REQ_CANCEL_2, TKT_2, T2).toBookingCommand());
            sm.apply(6L, new CancelBookingCommand(REQ_CANCEL_3, TKT_3, T2).toBookingCommand());

            // Tất cả các ghế đều đã được giải phóng đầy đủ
            assertEquals(0, sm.activeTicketCount());
            assertEquals(3, sm.totalTicketCount());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertEquals("AVAILABLE", sm.seatStatus("G02").orElseThrow());
            assertEquals("AVAILABLE", sm.seatStatus("G03").orElseThrow());
            assertTrue(sm.seatCurrentTicketId("G01").isEmpty());
            assertTrue(sm.seatCurrentTicketId("G02").isEmpty());
            assertTrue(sm.seatCurrentTicketId("G03").isEmpty());
        }

        @Test
        @DisplayName("Deterministic: apply cùng chuỗi command lên 2 state machine độc lập cho cùng state cuối")
        void deterministicExecutionAcrossTwoIndependentStateMachines() {
            BookingStateMachine sm1 = new BookingStateMachine();
            BookingStateMachine sm2 = new BookingStateMachine();

            List<BookingCommand> commands = List.of(
                    new BookingCommand.BookTicket(REQ_BOOK_1, TRIP_ID, "G01", "Passenger 1", TKT_1, T1),
                    new BookingCommand.BookTicket(REQ_BOOK_2, TRIP_ID, "G02", "Passenger 2", TKT_2, T1),
                    new BookingCommand.NoOp(),
                    new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2).toBookingCommand(),
                    new BookingCommand.BookTicket(REQ_BOOK_3, TRIP_ID, "G01", "Passenger 3", TKT_3, T3),
                    new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T3).toBookingCommand(), // Replay
                    new CancelBookingCommand(REQ_CANCEL_2, TKT_2, T3).toBookingCommand(),
                    new CancelBookingCommand(REQ_CANCEL_NEW, "missing-tkt", T4).toBookingCommand() // Rejected
            );

            long index = 1;
            for (BookingCommand cmd : commands) {
                var out1 = sm1.apply(index, cmd);
                var out2 = sm2.apply(index, cmd);
                assertEquals(out1, out2, "Outcome tại logIndex " + index + " phải hoàn toàn giống nhau");
                index++;
            }

            // Đối chiếu toàn bộ state cuối cùng
            assertEquals(sm1.lastApplied(), sm2.lastApplied());
            assertEquals(sm1.activeTicketCount(), sm2.activeTicketCount());
            assertEquals(sm1.totalTicketCount(), sm2.totalTicketCount());
            assertEquals(sm1.allSeatStatuses(), sm2.allSeatStatuses());

            for (int i = 1; i <= 10; i++) {
                String seatId = String.format("G%02d", i);
                assertEquals(sm1.seatStatus(seatId), sm2.seatStatus(seatId));
                assertEquals(sm1.seatCurrentTicketId(seatId), sm2.seatCurrentTicketId(seatId));
            }
        }
    }

    // =======================================================================
    // 3. DATABASE & RAFT TRANSACTION INTEGRATION TESTS
    // =======================================================================

    @Nested
    @DisplayName("3. Database Transaction & PostgreSQL Durability Tests")
    class DatabaseTransactionalTests {

        private String jdbcUrl;
        private String username;
        private String password;
        private String host;
        private int port;
        private String dbName;
        private String schema;
        private ConnectionFactory connectionFactory;

        @BeforeEach
        void setUp() throws Exception {
            jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
            username = System.getenv("RAILWAY_TEST_DB_USER");
            password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
            Assumptions.assumeTrue(
                    hasText(jdbcUrl) && hasText(username) && hasText(password),
                    "Yêu cầu PostgreSQL: RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, RAILWAY_TEST_DB_PASSWORD");

            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            host = uri.getHost();
            port = uri.getPort() < 0 ? 5432 : uri.getPort();
            dbName = uri.getPath().substring(1);

            schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
            try (Connection connection = rawConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + schema + "\"");
            }
            try {
                runMigration(readMigration("V1__schema.sql"));
                runMigration(readMigration("V2__seed.sql"));
                // Gán currentSchema để mọi kết nối từ ConnectionFactory đều trỏ vào schema cô lập này
                String schemaDbName = dbName + "?currentSchema=" + schema;
                connectionFactory = new ConnectionFactory(new DbConfig(host, port, schemaDbName, username, password));
            } catch (Exception exception) {
                dropSchema();
                throw exception;
            }
        }

        @AfterEach
        void tearDown() throws SQLException {
            try {
                dropSchema();
            } finally {
                schema = null;
            }
        }

        @Test
        @DisplayName("Hủy vé bền vững trong PostgreSQL: vé thành CANCELLED, ghế AVAILABLE, record vé vẫn còn")
        void cancelPersistedInPostgres() {
            JdbcBookingRepository repo = new JdbcBookingRepository(connectionFactory);
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(connectionFactory);

            // Index 1: Book ticket-1
            BookingCommand.BookTicket book = new BookingCommand.BookTicket(
                    REQ_BOOK_1, TRIP_ID, "G01", "Tran Thi B", TKT_1, T1);
            seedRaftLog(1L, 1L, book);
            applier.applyCommitted(1L, book);

            assertEquals(1L, repo.lastApplied());
            assertEquals("ACTIVE", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertEquals("BOOKED", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));

            // Index 2: Cancel ticket-1
            CancelBookingCommand cancelCmd = new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2);
            seedRaftLog(2L, 1L, cancelCmd.toBookingCommand());
            var outcome = applier.applyCommitted(2L, cancelCmd.toBookingCommand()).orElseThrow();

            BookingStateMachine.ApplyOutcome.BusinessResult result = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertFalse(result.isReplay());
            ApplyResult applyResult = result.applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, applyResult.outcome());
            assertEquals(2L, applyResult.appliedLogIndex());

            // 1. Kiểm tra bảng tickets: status='CANCELLED', cancelled_at được cập nhật, record KHÔNG bị xóa
            assertEquals(1, countWhere("tickets", "ticket_id = '" + TKT_1 + "'"), "Record vé không được bị xóa");
            assertEquals("CANCELLED", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertNotNull(queryString("SELECT cancelled_at::text FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));

            // 2. Kiểm tra bảng seats: status='AVAILABLE', current_ticket_id IS NULL
            assertEquals("AVAILABLE", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
            assertTrue(queryString("SELECT current_ticket_id FROM seats WHERE seat_id = 'G01'") == null);

            // 3. Kiểm tra request_results: state='APPLIED_SUCCESS', ticket_id khớp
            assertEquals(2, count("request_results"));
            assertEquals("APPLIED_SUCCESS", queryString(
                    "SELECT state FROM request_results WHERE request_id = '" + REQ_CANCEL_1 + "'"));
            assertEquals(TKT_1, queryString(
                    "SELECT ticket_id FROM request_results WHERE request_id = '" + REQ_CANCEL_1 + "'"));

            // 4. Kiểm tra business_events: ghi nhận CANCEL_CONFIRMED
            assertEquals(2, count("business_events"));
            assertEquals("CANCEL_CONFIRMED", queryString(
                    "SELECT event_type FROM business_events WHERE log_index = 2"));

            // 5. Checkpoint last_applied tăng đúng 2
            assertEquals(2L, repo.lastApplied());
            assertEquals(2L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
        }

        @Test
        @DisplayName("Hủy vé không tồn tại trong DB: lưu APPLIED_REJECTED với TICKET_NOT_ACTIVE, ghế không đổi")
        void cancelNonExistentTicketPersistedAsRejected() {
            JdbcBookingRepository repo = new JdbcBookingRepository(connectionFactory);
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(connectionFactory);

            CancelBookingCommand cancelCmd = new CancelBookingCommand(REQ_CANCEL_1, "ticket-ghost", T1);
            seedRaftLog(1L, 1L, cancelCmd.toBookingCommand());
            var outcome = applier.applyCommitted(1L, cancelCmd.toBookingCommand()).orElseThrow();

            BookingStateMachine.ApplyOutcome.BusinessResult result = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertEquals(ApplyResult.Outcome.REJECTED, result.applyResult().outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, result.applyResult().reasonCode());

            assertEquals(1, count("request_results"));
            assertEquals("APPLIED_REJECTED", queryString(
                    "SELECT state FROM request_results WHERE request_id = '" + REQ_CANCEL_1 + "'"));
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, queryString(
                    "SELECT reason_code FROM request_results WHERE request_id = '" + REQ_CANCEL_1 + "'"));
            assertEquals(1, count("business_events"));
            assertEquals("CANCEL_REJECTED", queryString(
                    "SELECT event_type FROM business_events WHERE log_index = 1"));
            assertEquals(0, count("tickets"));
            assertEquals(10, countWhere("seats", "status = 'AVAILABLE'"));
        }

        @Test
        @DisplayName("Rollback: giả lập lỗi toàn vẹn giữa chừng, status vé và ghế đều không đổi, rollback 100%")
        void rollbackWhenIntegrityCheckFailsMidApply() throws SQLException {
            JdbcBookingRepository repo = new JdbcBookingRepository(connectionFactory);
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(connectionFactory);

            // Index 1: Book ticket-1 thành công
            BookingCommand.BookTicket book = new BookingCommand.BookTicket(
                    REQ_BOOK_1, TRIP_ID, "G01", "Passenger A", TKT_1, T1);
            seedRaftLog(1L, 1L, book);
            applier.applyCommitted(1L, book);
            assertEquals(1L, repo.lastApplied());
            assertEquals("ACTIVE", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertEquals("BOOKED", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));

            // Cố tình can thiệp làm sai lệch dữ liệu: ghế G01 đổi sang AVAILABLE trong khi vé vẫn ACTIVE
            try (Connection connection = schemaConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("UPDATE seats SET status = 'AVAILABLE', current_ticket_id = NULL WHERE seat_id = 'G01'");
            }

            // Index 2: Áp dụng lệnh CANCEL trên ticket-1
            CancelBookingCommand cancelCmd = new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2);
            seedRaftLog(2L, 1L, cancelCmd.toBookingCommand());

            // Phải ném StorageException do vi phạm kiểm tra toàn vẹn liên kết vé - ghế
            BookingRepository.StorageException exception = assertThrows(
                    BookingRepository.StorageException.class,
                    () -> applier.applyCommitted(2L, cancelCmd.toBookingCommand()));
            assertTrue(exception.getMessage().contains("not linked to its BOOKED seat")
                    || exception.getMessage().contains("Database integrity violation"));

            // XÁC MINH ROLLBACK HOÀN TOÀN:
            // 1. lastApplied KHÔNG được tăng
            assertEquals(1L, repo.lastApplied());
            assertEquals(1L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));

            // 2. Vé TKT_1 vẫn giữ nguyên ACTIVE (không bị UPDATE sang CANCELLED)
            assertEquals("ACTIVE", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertTrue(queryString("SELECT cancelled_at FROM tickets WHERE ticket_id = '" + TKT_1 + "'") == null);

            // 3. Không có kết quả nào được ghi vào request_results cho REQ_CANCEL_1
            assertEquals(0, countWhere("request_results", "request_id = '" + REQ_CANCEL_1 + "'"));

            // 4. Không phát sinh business_event cho lệnh bị rollback
            assertEquals(1, count("business_events"), "business_events chỉ có 1 event của lệnh Book trước đó");

            // Khắc phục lại dữ liệu ghế về trạng thái đúng
            try (Connection connection = schemaConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("UPDATE seats SET status = 'BOOKED', current_ticket_id = '" + TKT_1 + "' WHERE seat_id = 'G01'");
            }

            // Retry apply index 2: Phải thành công mỹ mãn!
            var retryOutcome = applier.applyCommitted(2L, cancelCmd.toBookingCommand()).orElseThrow();
            BookingStateMachine.ApplyOutcome.BusinessResult retryResult = assertInstanceOf(
                    BookingStateMachine.ApplyOutcome.BusinessResult.class, retryOutcome);
            assertEquals(ApplyResult.Outcome.CONFIRMED, retryResult.applyResult().outcome());
            assertEquals(2L, repo.lastApplied());
            assertEquals("CANCELLED", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertEquals("AVAILABLE", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));
            assertEquals(2, count("request_results"));
            assertEquals(2, count("business_events"));
        }

        @Test
        @DisplayName("Replay log từ đầu ra cùng state bền vững (Raft redelivery & restart)")
        void replayLogFromBeginningYieldsConsistentState() {
            JdbcBookingRepository repo = new JdbcBookingRepository(connectionFactory);
            JdbcCommittedCommandApplier applier = new JdbcCommittedCommandApplier(connectionFactory);

            BookingCommand.BookTicket book = new BookingCommand.BookTicket(
                    REQ_BOOK_1, TRIP_ID, "G01", "Passenger 1", TKT_1, T1);
            CancelBookingCommand cancel = new CancelBookingCommand(REQ_CANCEL_1, TKT_1, T2);

            seedRaftLog(1L, 1L, book);
            seedRaftLog(2L, 1L, cancel.toBookingCommand());

            applier.applyCommitted(1L, book);
            applier.applyCommitted(2L, cancel.toBookingCommand());

            assertEquals(2L, repo.lastApplied());
            assertEquals("CANCELLED", queryString("SELECT status FROM tickets WHERE ticket_id = '" + TKT_1 + "'"));
            assertEquals("AVAILABLE", queryString("SELECT status FROM seats WHERE seat_id = 'G01'"));

            // Giả lập restart Node: tạo applier mới đọc từ checkpoint DB
            JdbcBookingRepository restartedRepo = new JdbcBookingRepository(connectionFactory);
            JdbcCommittedCommandApplier restartedApplier = new JdbcCommittedCommandApplier(connectionFactory);
            assertEquals(2L, restartedApplier.initialCommitIndex());
            assertEquals(2L, restartedApplier.lastApplied());

            // Redeliver lại index 1 và index 2 (Raft replay các entry đã commit)
            var redeliverBook = restartedApplier.applyCommitted(1L, book).orElseThrow();
            var redeliverCancel = restartedApplier.applyCommitted(2L, cancel.toBookingCommand()).orElseThrow();

            // Trả về đúng kết quả gốc đã lưu mà không ghi đúp dữ liệu
            assertEquals(ApplyResult.bookConfirmed(REQ_BOOK_1, TKT_1, 1L),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, redeliverBook).applyResult());
            assertEquals(ApplyResult.cancelConfirmed(REQ_CANCEL_1, TKT_1, 2L),
                    assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, redeliverCancel).applyResult());

            assertEquals(1, count("tickets"));
            assertEquals(2, count("request_results"));
            assertEquals(2, count("business_events"));
            assertEquals(2L, restartedApplier.lastApplied());
        }

        // --- Helper Methods ---

        private void seedRaftLog(long logIndex, long term, BookingCommand command) {
            String type = command.type().name();
            String json = commandJson(command);
            try (Connection connection = schemaConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "INSERT INTO raft_log(log_index, term, command_type, command_json) VALUES (?, ?, ?, ?::jsonb)")) {
                statement.setLong(1, logIndex);
                statement.setLong(2, term);
                statement.setString(3, type);
                statement.setString(4, json);
                statement.executeUpdate();
            } catch (SQLException exception) {
                throw new IllegalStateException("Could not seed raft_log entry", exception);
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
                throw new IllegalStateException("Test query failed: " + sql, exception);
            }
        }

        private long queryLong(String sql) {
            try (Connection connection = schemaConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getLong(1);
            } catch (SQLException exception) {
                throw new IllegalStateException("Test query failed: " + sql, exception);
            }
        }

        private String queryString(String sql) {
            try (Connection connection = schemaConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getString(1);
            } catch (SQLException exception) {
                throw new IllegalStateException("Test query failed: " + sql, exception);
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

        private void dropSchema() throws SQLException {
            if (schema == null || !hasText(jdbcUrl) || !hasText(username) || !hasText(password)) {
                return;
            }
            try (Connection connection = rawConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }

        private static boolean hasText(String value) {
            return value != null && !value.isBlank();
        }
    }
}
