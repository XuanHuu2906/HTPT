package vn.nhom6.railway.booking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test cho {@link BookingStateMachine}.
 *
 * <p>Giả định bắt buộc: mọi entry truyền vào {@code apply()} đã được Raft commit theo thứ tự.
 * Đây KHÔNG phải kiểm thử Raft — không có Leader giả, không có RaftNode, không có commit logic.
 * Lớp test chỉ kiểm tra hành vi tất định của state machine trên chuỗi entry đã commit.
 *
 * <p>Tham chiếu: SPEC-05 §1, §2, §3, §3.1, §8; SPEC-04 §6.
 */
@DisplayName("BookingStateMachine — Unit Tests (entry giả định đã commit, không kiểm thử Raft)")
class BookingStateMachineTest {

    // -----------------------------------------------------------------------
    // Hằng số dùng chung trong test
    // -----------------------------------------------------------------------

    /** requestId hợp lệ dạng UUID chữ thường (SPEC-03 §3.0). */
    private static final String REQ_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQ_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQ_3 = "33333333-3333-3333-3333-333333333333";
    private static final String REQ_4 = "44444444-4444-4444-4444-444444444444";
    private static final String REQ_5 = "55555555-5555-5555-5555-555555555555";

    /** ticketId mẫu — do "Leader" cấp trong test. */
    private static final String TKT_A = "ticket-aaaaaa";
    private static final String TKT_B = "ticket-bbbbbb";
    private static final String TKT_C = "ticket-cccccc";

    /** issuedAt mẫu — cố định, chính xác millisecond (SPEC-05 §2). */
    private static final Instant T1 = Instant.ofEpochMilli(1_700_000_000_000L);
    private static final Instant T2 = Instant.ofEpochMilli(1_700_000_001_000L);
    private static final Instant T3 = Instant.ofEpochMilli(1_700_000_002_000L);

    private BookingStateMachine sm;

    @BeforeEach
    void setUp() {
        sm = new BookingStateMachine();
    }

    // -----------------------------------------------------------------------
    // §1 Khởi tạo
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("1. Khởi tạo")
    class Init {

        @Test
        @DisplayName("lastApplied ban đầu là 0")
        void initialLastApplied() {
            assertEquals(0L, sm.lastApplied());
        }

        @Test
        @DisplayName("Đúng 10 ghế G01–G10 đều AVAILABLE")
        void tenSeatsAllAvailable() {
            for (int i = 1; i <= 10; i++) {
                String seatId = "G" + String.format("%02d", i);
                assertEquals(
                        "AVAILABLE",
                        sm.seatStatus(seatId).orElseThrow(),
                        "Ghế " + seatId + " phải AVAILABLE khi khởi tạo"
                );
            }
            // Không có ghế nào ngoài G01–G10
            assertTrue(sm.seatStatus("G11").isEmpty(), "G11 không tồn tại");
            assertTrue(sm.seatStatus("G00").isEmpty(), "G00 không tồn tại");
        }

        @Test
        @DisplayName("Không có vé nào khi khởi tạo")
        void noTicketsInitially() {
            assertEquals(0, sm.totalTicketCount());
            assertEquals(0L, sm.activeTicketCount());
        }
    }

    // -----------------------------------------------------------------------
    // §2 BOOK ghế trống
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("2. BOOK ghế trống")
    class BookAvailable {

        @Test
        @DisplayName("BOOK G01 thành công: CONFIRMED, vé ACTIVE, ghế BOOKED, appliedLogIndex đúng")
        void bookAvailableSeat() {
            // Entry giả định đã commit; không kiểm thử Raft
            BookingCommand.BookTicket cmd = new BookingCommand.BookTicket(
                    REQ_1, "SE-001", "G01", "Nguyen Van A", TKT_A, T1);

            var outcome = sm.apply(1L, cmd);

            // Kiểu kết quả
            assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            var result = ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome).applyResult();

            // CONFIRMED
            assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
            assertEquals(TKT_A, result.ticketId());
            assertNull(result.reasonCode());
            assertEquals(1L, result.appliedLogIndex());
            assertEquals(REQ_1, result.requestId());

            // Ghế BOOKED
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals(TKT_A, sm.seatCurrentTicketId("G01").orElseThrow());

            // Vé ACTIVE
            assertEquals("ACTIVE", sm.ticketStatus(TKT_A).orElseThrow());
            assertEquals(1L, sm.activeTicketCount());
            assertEquals(1, sm.totalTicketCount());

            // lastApplied tăng
            assertEquals(1L, sm.lastApplied());
        }

        @Test
        @DisplayName("BOOK nhiều ghế khác nhau thành công")
        void bookMultipleSeats() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G03", "A", TKT_A, T1));
            sm.apply(2L, new BookingCommand.BookTicket(REQ_2, "SE-001", "G07", "B", TKT_B, T2));

            assertEquals("BOOKED", sm.seatStatus("G03").orElseThrow());
            assertEquals("BOOKED", sm.seatStatus("G07").orElseThrow());
            assertEquals("AVAILABLE", sm.seatStatus("G01").orElseThrow());
            assertEquals(2L, sm.activeTicketCount());
            assertEquals(2L, sm.lastApplied());
        }
    }

    // -----------------------------------------------------------------------
    // §3 BOOK trùng ghế
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("3. BOOK trùng ghế (requestId khác)")
    class BookDuplicate {

        @Test
        @DisplayName("BOOK lại ghế đã BOOKED bằng requestId khác: REJECTED(SEAT_ALREADY_BOOKED)")
        void bookAlreadyBookedSeat() {
            // Entry 1: đặt G05 thành công (giả định đã commit)
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1));

            // Entry 2: đặt lại G05 với requestId khác (giả định đã commit)
            var outcome2 = sm.apply(2L,
                    new BookingCommand.BookTicket(REQ_2, "SE-001", "G05", "Bob", TKT_B, T2));

            assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome2);
            var result2 = ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome2).applyResult();

            assertEquals(ApplyResult.Outcome.REJECTED, result2.outcome());
            assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, result2.reasonCode());
            assertNull(result2.ticketId()); // BOOK REJECTED: ticketId phải null
            assertEquals(2L, result2.appliedLogIndex());

            // Vẫn chỉ có đúng 1 vé ACTIVE cho G05
            assertEquals(1L, sm.activeTicketCount());
            assertEquals(1, sm.totalTicketCount()); // TKT_B không được tạo
            assertEquals("ACTIVE", sm.ticketStatus(TKT_A).orElseThrow());
            assertTrue(sm.ticketStatus(TKT_B).isEmpty(), "TKT_B không được tạo");

            assertEquals(2L, sm.lastApplied());
        }

        @Test
        @DisplayName("BOOK ghế khác sau khi G05 đã bán vẫn CONFIRMED")
        void bookOtherSeatAfterConflict() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1));
            sm.apply(2L, new BookingCommand.BookTicket(REQ_2, "SE-001", "G05", "Bob", TKT_B, T2));

            // G06 vẫn AVAILABLE, đặt phải thành công
            var outcome3 = sm.apply(3L,
                    new BookingCommand.BookTicket(REQ_3, "SE-001", "G06", "Carol", TKT_C, T3));

            assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome3);
            var r3 = ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome3).applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, r3.outcome());
            assertEquals(TKT_C, r3.ticketId());
            assertEquals(3L, r3.appliedLogIndex());

            assertEquals(3L, sm.lastApplied());
            assertEquals(2L, sm.activeTicketCount()); // TKT_A (G05) + TKT_C (G06)
        }
    }

    // -----------------------------------------------------------------------
    // §4 Replay requestId — cùng ý định
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("4. Replay requestId — cùng ý định")
    class ReplayRequestIdSameIntent {

        @Test
        @DisplayName("Replay cùng requestId, cùng ý định (kể cả ticketId/issuedAt khác do Leader khác): kết quả gốc")
        void replaySameIntent() {
            // Entry 1: lần đầu apply (giả định đã commit)
            BookingCommand.BookTicket cmd1 = new BookingCommand.BookTicket(
                    REQ_1, "SE-001", "G02", "Nguyen Thi B", TKT_A, T1);
            var first = sm.apply(1L, cmd1);
            var firstResult = ((BookingStateMachine.ApplyOutcome.BusinessResult) first).applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, firstResult.outcome());
            assertEquals(1L, firstResult.appliedLogIndex());

            // Entry 2: replay cùng requestId, cùng tripId/seatId/passengerName nhưng ticketId/issuedAt khác
            // (mô phỏng entry bị duplicate do đổi Leader trong Raft log)
            // Entry giả định đã commit; đây không phải kiểm thử Raft.
            BookingCommand.BookTicket cmd2 = new BookingCommand.BookTicket(
                    REQ_1, "SE-001", "G02", "Nguyen Thi B",
                    "ticket-other-leader", // ticketId khác
                    T2);                   // issuedAt khác
            var second = sm.apply(2L, cmd2);

            // Phải là BusinessResult với isReplay=true
            assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, second);
            var secondBR = (BookingStateMachine.ApplyOutcome.BusinessResult) second;
            assertTrue(secondBR.isReplay(), "Phải là replay");

            // Kết quả phải GIỐNG HỆT kết quả gốc (kể cả appliedLogIndex gốc = 1, không phải 2)
            ApplyResult replayResult = secondBR.applyResult();
            assertEquals(ApplyResult.Outcome.CONFIRMED, replayResult.outcome());
            assertEquals(TKT_A, replayResult.ticketId(), "ticketId phải là của lần apply đầu tiên");
            assertEquals(1L, replayResult.appliedLogIndex(), "appliedLogIndex phải là 1 (gốc), không phải 2");

            // Không thêm vé mới
            assertEquals(1, sm.totalTicketCount());
            assertEquals(1L, sm.activeTicketCount());
            assertTrue(sm.ticketStatus("ticket-other-leader").isEmpty(),
                    "Vé của Leader mới không được tạo");

            // lastApplied vẫn tăng đến 2
            assertEquals(2L, sm.lastApplied());
        }

        @Test
        @DisplayName("appliedLogIndex gốc không đổi sau nhiều lần replay; lastApplied vẫn tăng")
        void multipleReplayKeepsOriginalIndex() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G04", "Test", TKT_A, T1));

            // Replay 3 lần (các entry duplicate trong Raft log)
            // Entry giả định đã commit; đây không phải kiểm thử Raft.
            for (long idx = 2; idx <= 4; idx++) {
                var out = sm.apply(idx,
                        new BookingCommand.BookTicket(REQ_1, "SE-001", "G04", "Test",
                                "other-" + idx, T2));
                var br = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, out);
                assertTrue(br.isReplay());
                assertEquals(1L, br.applyResult().appliedLogIndex());
                assertEquals(idx, sm.lastApplied());
            }

            assertEquals(4L, sm.lastApplied());
            assertEquals(1, sm.totalTicketCount()); // chỉ có 1 vé
        }
    }

    // -----------------------------------------------------------------------
    // §5 Replay requestId — khác ý định (REQUEST_ID_CONFLICT)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("5. Replay requestId — khác ý định (REQUEST_ID_CONFLICT)")
    class ReplayRequestIdDifferentIntent {

        @Test
        @DisplayName("Cùng requestId nhưng đổi seatId: REQUEST_ID_CONFLICT, kết quả gốc không đổi")
        void conflictDifferentSeat() {
            // Entry 1: đặt G01 (giả định đã commit)
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G01", "Alice", TKT_A, T1));

            // Entry 2: cùng requestId nhưng đổi seat → CONFLICT
            var outcome2 = sm.apply(2L,
                    new BookingCommand.BookTicket(REQ_1, "SE-001", "G02", "Alice", TKT_B, T2));

            assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, outcome2);
            var conflict = (BookingStateMachine.ApplyOutcome.RequestIdConflict) outcome2;
            assertEquals(REQ_1, conflict.requestId());
            assertEquals(2L, conflict.logIndex());

            // Kết quả gốc không đổi
            var stored = sm.storedResult(REQ_1).orElseThrow();
            assertEquals(ApplyResult.Outcome.CONFIRMED, stored.outcome());
            assertEquals(TKT_A, stored.ticketId());
            assertEquals(1L, stored.appliedLogIndex());

            // G02 không bị đặt
            assertEquals("AVAILABLE", sm.seatStatus("G02").orElseThrow());
            assertEquals(1, sm.totalTicketCount());

            // lastApplied vẫn tăng
            assertEquals(2L, sm.lastApplied());
        }

        @Test
        @DisplayName("Cùng requestId nhưng đổi tên hành khách: REQUEST_ID_CONFLICT")
        void conflictDifferentName() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G03", "Alice", TKT_A, T1));
            var outcome2 = sm.apply(2L,
                    new BookingCommand.BookTicket(REQ_1, "SE-001", "G03", "Bob", TKT_B, T2));

            assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, outcome2);
        }

        @Test
        @DisplayName("Cùng requestId nhưng đổi loại lệnh (BOOK→CANCEL): REQUEST_ID_CONFLICT")
        void conflictDifferentCommandType() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1));
            // Cùng requestId nhưng là CANCEL → conflict loại lệnh
            var outcome2 = sm.apply(2L,
                    new BookingCommand.CancelTicket(REQ_1, TKT_A, T2));

            assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, outcome2);
        }

        @Test
        @DisplayName("Entry sau khi CONFLICT vẫn apply được bình thường")
        void entryAfterConflictStillApplies() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G01", "Alice", TKT_A, T1));
            sm.apply(2L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G02", "Zara", TKT_B, T2));

            // Entry 3: request mới hoàn toàn
            var outcome3 = sm.apply(3L,
                    new BookingCommand.BookTicket(REQ_2, "SE-001", "G08", "Carol", TKT_C, T3));
            var r3 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome3);
            assertEquals(ApplyResult.Outcome.CONFIRMED, r3.applyResult().outcome());
            assertEquals(3L, sm.lastApplied());
        }
    }

    // -----------------------------------------------------------------------
    // §6 NO_OP
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("6. NO_OP")
    class NoOpTest {

        @Test
        @DisplayName("NO_OP tăng lastApplied, không tạo vé, không đổi ghế")
        void noOpIncreasesLastApplied() {
            var outcome = sm.apply(1L, new BookingCommand.NoOp());

            assertInstanceOf(BookingStateMachine.ApplyOutcome.NoOpApplied.class, outcome);
            var noa = (BookingStateMachine.ApplyOutcome.NoOpApplied) outcome;
            assertEquals(1L, noa.newLastApplied());

            assertEquals(1L, sm.lastApplied());
            assertEquals(0, sm.totalTicketCount());
            // Tất cả ghế vẫn AVAILABLE
            for (int i = 1; i <= 10; i++) {
                assertEquals("AVAILABLE",
                        sm.seatStatus("G" + String.format("%02d", i)).orElseThrow());
            }
        }

        @Test
        @DisplayName("NO_OP xen giữa hai BOOK vẫn đúng thứ tự")
        void noOpBetweenBooks() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G01", "A", TKT_A, T1));
            sm.apply(2L, new BookingCommand.NoOp());
            sm.apply(3L, new BookingCommand.BookTicket(REQ_2, "SE-001", "G02", "B", TKT_B, T2));

            assertEquals(3L, sm.lastApplied());
            assertEquals(2L, sm.activeTicketCount());
            assertEquals("BOOKED", sm.seatStatus("G01").orElseThrow());
            assertEquals("BOOKED", sm.seatStatus("G02").orElseThrow());
        }
    }

    // -----------------------------------------------------------------------
    // §7 Sai thứ tự
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("7. Sai thứ tự — fail closed")
    class OrderingViolation {

        @Test
        @DisplayName("Nhảy index: apply(1) rồi apply(3) ném IllegalStateException")
        void skipIndex() {
            sm.apply(1L, new BookingCommand.NoOp());
            assertThrows(IllegalStateException.class,
                    () -> sm.apply(3L, new BookingCommand.NoOp()),
                    "Nhảy từ 1 lên 3 phải ném lỗi");
        }

        @Test
        @DisplayName("Apply lại index cũ: apply(1) rồi apply(1) ném IllegalStateException")
        void duplicateIndex() {
            sm.apply(1L, new BookingCommand.NoOp());
            assertThrows(IllegalStateException.class,
                    () -> sm.apply(1L, new BookingCommand.NoOp()),
                    "Apply lại index 1 phải ném lỗi");
        }

        @Test
        @DisplayName("Apply index 0 ngay từ đầu ném IllegalStateException (chỉ index>=1 hợp lệ)")
        void indexZero() {
            assertThrows(IllegalStateException.class,
                    () -> sm.apply(0L, new BookingCommand.NoOp()),
                    "Index 0 phải ném lỗi (lastApplied+1 = 1)");
        }

        @Test
        @DisplayName("Apply index âm ném IllegalStateException")
        void negativeIndex() {
            assertThrows(IllegalStateException.class,
                    () -> sm.apply(-1L, new BookingCommand.NoOp()));
        }
    }

    // -----------------------------------------------------------------------
    // §8 Tất định: hai state machine cùng chuỗi → state giống hệt
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("8. Tất định — hai state machine cùng chuỗi entry cho kết quả giống nhau")
    class Determinism {

        @Test
        @DisplayName("Hai instance apply cùng chuỗi 4 entry cho allSeatStatuses() và lastApplied() giống nhau")
        void twoMachinesSameSequence() {
            // Chuỗi entry đại diện cho một kịch bản thực tế (giả định đã commit)
            // Đây không phải kiểm thử Raft; chỉ kiểm tra tính tất định của state machine.
            BookingStateMachine sm1 = new BookingStateMachine();
            BookingStateMachine sm2 = new BookingStateMachine();

            // Tạo sequence
            BookingCommand[] seq = {
                    new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1),
                    new BookingCommand.BookTicket(REQ_2, "SE-001", "G05", "Bob", TKT_B, T2), // sẽ bị REJECTED
                    new BookingCommand.NoOp(),
                    new BookingCommand.BookTicket(REQ_3, "SE-001", "G06", "Carol", TKT_C, T3),
            };

            for (int i = 0; i < seq.length; i++) {
                sm1.apply(i + 1L, seq[i]);
                sm2.apply(i + 1L, seq[i]);
            }

            // lastApplied giống nhau
            assertEquals(sm1.lastApplied(), sm2.lastApplied());

            // Tất cả ghế có status giống nhau
            Map<String, String> s1 = sm1.allSeatStatuses();
            Map<String, String> s2 = sm2.allSeatStatuses();
            assertEquals(s1, s2, "Trạng thái ghế phải giống nhau");

            // Số vé giống nhau
            assertEquals(sm1.activeTicketCount(), sm2.activeTicketCount());
            assertEquals(sm1.totalTicketCount(), sm2.totalTicketCount());

            // Kết quả theo requestId giống nhau
            for (String req : new String[]{REQ_1, REQ_2, REQ_3}) {
                ApplyResult r1 = sm1.storedResult(req).orElseThrow();
                ApplyResult r2 = sm2.storedResult(req).orElseThrow();
                assertEquals(r1.outcome(), r2.outcome());
                assertEquals(r1.ticketId(), r2.ticketId());
                assertEquals(r1.reasonCode(), r2.reasonCode());
                assertEquals(r1.appliedLogIndex(), r2.appliedLogIndex());
            }
        }

        @Test
        @DisplayName("Fingerprint tất định: cùng input cho cùng fingerprint; đổi field thì khác")
        void fingerprintDeterminism() {
            // Kiểm tra gián tiếp qua hành vi replay:
            // Cùng ý định với ticketId/issuedAt khác → replay (fingerprint giống)
            // Đổi seat → conflict (fingerprint khác)

            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G01", "A", TKT_A, T1));

            // Cùng tripId/seatId/passengerName, ticketId/issuedAt khác → fingerprint giống → replay
            var out2 = sm.apply(2L,
                    new BookingCommand.BookTicket(REQ_1, "SE-001", "G01", "A", "other", T2));
            var br2 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, out2);
            assertTrue(br2.isReplay(), "Cùng ý định (khác ticketId/issuedAt) phải là replay");

            // Cùng requestId nhưng đổi seatId → fingerprint khác → conflict
            sm.apply(3L, new BookingCommand.BookTicket(REQ_2, "SE-001", "G02", "A", TKT_B, T1));
            var out4 = sm.apply(4L,
                    new BookingCommand.BookTicket(REQ_2, "SE-001", "G03", "A", TKT_C, T2));
            assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, out4,
                    "Đổi seatId phải là REQUEST_ID_CONFLICT");
        }
    }

    // -----------------------------------------------------------------------
    // §9 CANCEL (cơ bản)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("9. CANCEL (cơ bản)")
    class CancelBasic {

        @Test
        @DisplayName("Hủy vé ACTIVE: vé CANCELLED, ghế AVAILABLE")
        void cancelActiveTicket() {
            // Entry 1: đặt G05 (giả định đã commit)
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1));
            assertEquals("BOOKED", sm.seatStatus("G05").orElseThrow());

            // Entry 2: hủy vé TKT_A (giả định đã commit)
            var outcome = sm.apply(2L, new BookingCommand.CancelTicket(REQ_2, TKT_A, T2));
            var br = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);

            assertEquals(ApplyResult.Outcome.CONFIRMED, br.applyResult().outcome());
            assertEquals(TKT_A, br.applyResult().ticketId());
            assertEquals(2L, br.applyResult().appliedLogIndex());

            // Ghế trả về AVAILABLE
            assertEquals("AVAILABLE", sm.seatStatus("G05").orElseThrow());

            // Vé CANCELLED (vẫn còn trong bảng vé, lịch sử)
            assertEquals("CANCELLED", sm.ticketStatus(TKT_A).orElseThrow());
            assertEquals(0L, sm.activeTicketCount());
            assertEquals(1, sm.totalTicketCount()); // vé lịch sử vẫn giữ

            assertEquals(2L, sm.lastApplied());
        }

        @Test
        @DisplayName("Hủy lại vé đã CANCELLED (requestId mới): REJECTED(TICKET_NOT_ACTIVE)")
        void cancelAlreadyCancelledTicket() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G05", "Alice", TKT_A, T1));
            sm.apply(2L, new BookingCommand.CancelTicket(REQ_2, TKT_A, T2));

            // Hủy lại bằng requestId mới
            var outcome = sm.apply(3L, new BookingCommand.CancelTicket(REQ_3, TKT_A, T3));
            var br = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);

            assertEquals(ApplyResult.Outcome.REJECTED, br.applyResult().outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, br.applyResult().reasonCode());
            assertEquals(TKT_A, br.applyResult().ticketId()); // CANCEL REJECTED giữ ticketId
            assertEquals(3L, br.applyResult().appliedLogIndex());

            // Ghế vẫn AVAILABLE (không bị ảnh hưởng)
            assertEquals("AVAILABLE", sm.seatStatus("G05").orElseThrow());

            assertEquals(3L, sm.lastApplied());
        }

        @Test
        @DisplayName("Hủy vé không tồn tại: REJECTED(TICKET_NOT_ACTIVE)")
        void cancelNonExistentTicket() {
            var outcome = sm.apply(1L,
                    new BookingCommand.CancelTicket(REQ_1, "ticket-notexist", T1));
            var br = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcome);
            assertEquals(ApplyResult.Outcome.REJECTED, br.applyResult().outcome());
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, br.applyResult().reasonCode());
        }

        @Test
        @DisplayName("Replay CANCEL cùng requestId + cùng ticketId: kết quả gốc, không đổi state")
        void replayCancelSameIntent() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G09", "Alice", TKT_A, T1));
            sm.apply(2L, new BookingCommand.CancelTicket(REQ_2, TKT_A, T2));

            // Replay CANCEL cùng requestId REQ_2, cùng ticketId TKT_A (đây là đúng ý định)
            var out3 = sm.apply(3L, new BookingCommand.CancelTicket(REQ_2, TKT_A, T3));
            var br3 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, out3);
            assertTrue(br3.isReplay(), "Phải là replay CANCEL");
            assertEquals(ApplyResult.Outcome.CONFIRMED, br3.applyResult().outcome());
            assertEquals(2L, br3.applyResult().appliedLogIndex(), "appliedLogIndex gốc là 2");

            // Ghế vẫn AVAILABLE (không bị đặt lại)
            assertEquals("AVAILABLE", sm.seatStatus("G09").orElseThrow());
            assertEquals(3L, sm.lastApplied());
        }

        @Test
        @DisplayName("CANCEL + BOOK ghế đã hủy thành công sau đó (ghế được giải phóng)")
        void bookSeatAfterCancel() {
            sm.apply(1L, new BookingCommand.BookTicket(REQ_1, "SE-001", "G10", "Alice", TKT_A, T1));
            sm.apply(2L, new BookingCommand.CancelTicket(REQ_2, TKT_A, T2));
            assertEquals("AVAILABLE", sm.seatStatus("G10").orElseThrow());

            // Đặt lại G10 với request mới
            var out3 = sm.apply(3L,
                    new BookingCommand.BookTicket(REQ_3, "SE-001", "G10", "Bob", TKT_B, T3));
            var br3 = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, out3);
            assertEquals(ApplyResult.Outcome.CONFIRMED, br3.applyResult().outcome());
            assertEquals("BOOKED", sm.seatStatus("G10").orElseThrow());
            assertEquals(1L, sm.activeTicketCount()); // TKT_A cancelled, TKT_B active
            assertEquals(2, sm.totalTicketCount());

            assertEquals(3L, sm.lastApplied());
        }
    }
}
