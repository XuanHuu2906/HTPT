package vn.nhom6.railway.booking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV2-D01 — Unit tests cho BookingCommand và ApplyResult.
 *
 * <p>Không giả lập commit/apply Raft. Chỉ kiểm tra tạo lệnh, validate và factory.
 */
@DisplayName("TV2-D01: BookingCommand & ApplyResult")
class BookingCommandTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static final String VALID_REQUEST_ID = "542e32fb-b657-4478-9cea-52286822d1d2";
    private static final String VALID_TICKET_ID   = "ticket-abc123";
    private static final String VALID_TRIP_ID     = "SE-001";
    private static final Instant VALID_INSTANT     = Instant.ofEpochMilli(1_730_200_000_000L);

    // -----------------------------------------------------------------------
    // 1. BookTicket — trường hợp hợp lệ
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("1. BookTicket hợp lệ")
    class BookTicketValid {

        @Test
        @DisplayName("Tạo BookTicket bình thường")
        void createBookTicket() {
            var cmd = new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G05",
                    "Nguyen Van A", VALID_TICKET_ID, VALID_INSTANT);

            assertEquals(BookingCommand.Type.BOOK_TICKET, cmd.type());
            assertEquals(VALID_REQUEST_ID, cmd.requestId());
            assertEquals("G05", cmd.seatId());
            assertEquals("Nguyen Van A", cmd.passengerName());
        }

        @Test
        @DisplayName("Tên được strip() — khoảng trắng đầu/cuối bị bỏ, lưu tên đã strip")
        void passengerNameIsStripped() {
            var cmd = new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                    "  Trần Thị Bình  ", VALID_TICKET_ID, VALID_INSTANT);
            // Tên lưu trong record là đã strip
            assertEquals("Trần Thị Bình", cmd.passengerName());
        }

        @Test
        @DisplayName("Tên tiếng Việt nhiều dấu — đếm codePoint đúng (không phải UTF-16 unit)")
        void passengerNameVietnameseCodePoint() {
            // "Nguyễn" có 6 code point (chữ ễ là 1 code point dù UTF-16 có thể 2 unit nếu surrogate,
            // nhưng trong BMP nên vẫn 1; test đảm bảo codePointCount được dùng)
            String name = "Nguyễn Thị Mỹ Linh";
            var cmd = new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G02",
                    name, VALID_TICKET_ID, VALID_INSTANT);
            assertEquals(name.strip(), cmd.passengerName());
        }

        @Test
        @DisplayName("Tên có emoji (U+1F600) — codePointCount đếm 1 code point, không phải 2 UTF-16 unit")
        void passengerNameWithEmoji() {
            // U+1F600 là surrogate pair trong Java, nhưng codePointCount phải đếm là 1
            String name = "Test\uD83D\uDE00Name"; // 9 code point tổng
            var cmd = new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G03",
                    name, VALID_TICKET_ID, VALID_INSTANT);
            // codePointCount(0, length) = 9 (không phải 10 charAt)
            assertEquals(9, name.codePointCount(0, name.length()));
            assertEquals(name, cmd.passengerName()); // không có khoảng trắng cần strip
        }

        @Test
        @DisplayName("Tên đúng 120 code point — biên trên hợp lệ")
        void passengerName120CodePoints() {
            // 120 ký tự Latin bình thường = 120 code point
            String name = "A".repeat(120);
            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G04",
                    name, VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên 1 code point — biên dưới hợp lệ")
        void passengerName1CodePoint() {
            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G05",
                    "A", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Đủ 10 ghế G01–G10 đều hợp lệ")
        void allTenSeatsValid() {
            for (int i = 1; i <= 10; i++) {
                String seatId = String.format("G%02d", i);
                final int fi = i;
                assertDoesNotThrow(
                        () -> new BookingCommand.BookTicket(
                                VALID_REQUEST_ID, VALID_TRIP_ID, seatId,
                                "Test", VALID_TICKET_ID, VALID_INSTANT),
                        "Ghế " + seatId + " phải hợp lệ"
                );
            }
        }

        @Test
        @DisplayName("ticketId dài 64 ký tự — biên trên hợp lệ")
        void ticketIdMax64() {
            String id = "A".repeat(64);
            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G06",
                    "Test", id, VALID_INSTANT));
        }

        @Test
        @DisplayName("ticketId 1 ký tự — biên dưới hợp lệ")
        void ticketIdMin1() {
            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G07",
                    "Test", "x", VALID_INSTANT));
        }

        @Test
        @DisplayName("issuedAt ms chính xác = 0 nano dư — hợp lệ")
        void issuedAtExactMillis() {
            Instant ms = Instant.ofEpochSecond(1_730_000L, 500_000_000L); // 500ms chính xác
            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G08",
                    "Test", VALID_TICKET_ID, ms));
        }
    }

    // -----------------------------------------------------------------------
    // 2. BookTicket — validate từ chối
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("2. BookTicket validate từ chối")
    class BookTicketInvalid {

        @Test
        @DisplayName("UUID in hoa bị từ chối")
        void rejectUpperCaseUUID() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            "542E32FB-B657-4478-9CEA-52286822D1D2",
                            VALID_TRIP_ID, "G01", "Test", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("UUID thiếu dấu gạch — sai định dạng")
        void rejectMalformedUUID() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            "542e32fbb6574478-9cea-52286822d1d2",
                            VALID_TRIP_ID, "G01", "Test", VALID_TICKET_ID, VALID_INSTANT));
        }

        @ParameterizedTest(name = "seatId=\"{0}\" bị từ chối")
        @ValueSource(strings = {"G00", "G11", "G1", "g01", "H01", "G010", ""})
        @DisplayName("seatId không hợp lệ bị từ chối")
        void rejectInvalidSeatId(String seatId) {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, seatId,
                            "Test", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("seatId chữ thường g01 bị từ chối")
        void rejectLowerCaseSeatId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "g01",
                            "Test", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("tripId sai bị từ chối")
        void rejectWrongTripId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, "SE-002", "G01",
                            "Test", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên rỗng sau strip bị từ chối")
        void rejectEmptyName() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "   ", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên quá 120 code point bị từ chối")
        void rejectNameOver120CodePoints() {
            String name = "A".repeat(121);
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            name, VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên chứa U+0000 bị từ chối")
        void rejectNameWithNullChar() {
            String name = "Test\u0000Name";
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            name, VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("ticketId rỗng bị từ chối")
        void rejectEmptyTicketId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "Test", "", VALID_INSTANT));
        }

        @Test
        @DisplayName("ticketId dài 65 ký tự bị từ chối")
        void rejectTicketIdTooLong() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "Test", "A".repeat(65), VALID_INSTANT));
        }

        @Test
        @DisplayName("ticketId chứa ký tự @ bị từ chối")
        void rejectTicketIdInvalidChar() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "Test", "ticket@invalid", VALID_INSTANT));
        }

        @Test
        @DisplayName("issuedAt có nanosecond dưới ms bị từ chối")
        void rejectIssuedAtWithSubMillis() {
            Instant subMillis = Instant.ofEpochSecond(1_730_000L, 1L); // 1 nanosecond
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "Test", VALID_TICKET_ID, subMillis));
        }

        @Test
        @DisplayName("issuedAt null bị từ chối")
        void rejectNullIssuedAt() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            "Test", VALID_TICKET_ID, null));
        }
    }

    // -----------------------------------------------------------------------
    // 3. CancelTicket — trường hợp hợp lệ và từ chối
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("3. CancelTicket")
    class CancelTicketTests {

        @Test
        @DisplayName("Tạo CancelTicket hợp lệ")
        void createCancelTicket() {
            var cmd = new BookingCommand.CancelTicket(
                    VALID_REQUEST_ID, VALID_TICKET_ID, VALID_INSTANT);

            assertEquals(BookingCommand.Type.CANCEL_TICKET, cmd.type());
            assertEquals(VALID_REQUEST_ID, cmd.requestId());
            assertEquals(VALID_TICKET_ID, cmd.ticketId());
            assertEquals(VALID_INSTANT, cmd.cancelledAt());
        }

        @Test
        @DisplayName("cancelledAt có microsecond dư bị từ chối")
        void rejectCancelledAtSubMillis() {
            Instant subMillis = Instant.ofEpochSecond(0L, 999_999L); // 999 µs
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.CancelTicket(VALID_REQUEST_ID, VALID_TICKET_ID, subMillis));
        }

        @Test
        @DisplayName("UUID sai dạng bị từ chối trong CancelTicket")
        void rejectBadRequestId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.CancelTicket("not-a-uuid", VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("cancelledAt null bị từ chối")
        void rejectNullCancelledAt() {
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.CancelTicket(VALID_REQUEST_ID, VALID_TICKET_ID, null));
        }
    }

    // -----------------------------------------------------------------------
    // 4. NoOp — không field nào
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("4. NoOp")
    class NoOpTests {

        @Test
        @DisplayName("Tạo NoOp, type() trả NO_OP")
        void createNoOp() {
            var cmd = new BookingCommand.NoOp();
            assertEquals(BookingCommand.Type.NO_OP, cmd.type());
        }

        @Test
        @DisplayName("NoOp là instance của BookingCommand")
        void noOpIsBookingCommand() {
            BookingCommand cmd = new BookingCommand.NoOp();
            assertInstanceOf(BookingCommand.NoOp.class, cmd);
        }
    }

    // -----------------------------------------------------------------------
    // 5. ApplyResult factory — BOOK
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("5. ApplyResult factory — BOOK")
    class ApplyResultBookFactory {

        @Test
        @DisplayName("bookConfirmed — tạo CONFIRMED với ticketId và index")
        void bookConfirmed() {
            var r = ApplyResult.bookConfirmed(VALID_REQUEST_ID, VALID_TICKET_ID, 43L);
            assertEquals(ApplyResult.Outcome.CONFIRMED, r.outcome());
            assertEquals(VALID_TICKET_ID, r.ticketId());
            assertNull(r.reasonCode());
            assertEquals(43L, r.appliedLogIndex());
            assertTrue(r.isPersistable());
        }

        @Test
        @DisplayName("bookRejected SEAT_ALREADY_BOOKED — ticketId null, reasonCode set, index >= 1")
        void bookRejectedSeatAlreadyBooked() {
            var r = ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.SEAT_ALREADY_BOOKED, 10L);
            assertEquals(ApplyResult.Outcome.REJECTED, r.outcome());
            assertNull(r.ticketId(), "BOOK REJECTED phải có ticketId = null");
            assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, r.reasonCode());
            assertEquals(10L, r.appliedLogIndex());
            assertTrue(r.isPersistable());
        }

        @Test
        @DisplayName("bookRejected INVALID_REQUEST — hợp lệ")
        void bookRejectedInvalidRequest() {
            var r = ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.INVALID_REQUEST, 5L);
            assertEquals(ApplyResult.Outcome.REJECTED, r.outcome());
            assertEquals(ApplyResult.INVALID_REQUEST, r.reasonCode());
            assertTrue(r.isPersistable());
        }

        @Test
        @DisplayName("bookUnknown — UNKNOWN, isPersistable() = false")
        void bookUnknown() {
            var r = ApplyResult.bookUnknown(VALID_REQUEST_ID);
            assertEquals(ApplyResult.Outcome.UNKNOWN, r.outcome());
            assertEquals(ApplyResult.TIMEOUT_UNKNOWN, r.reasonCode());
            assertNull(r.appliedLogIndex());
            assertNull(r.ticketId());
            assertFalse(r.isPersistable(), "UNKNOWN không được lưu vào request_results");
        }

        @Test
        @DisplayName("bookRejected với reasonCode TICKET_NOT_ACTIVE bị từ chối")
        void bookRejectedWithCancelReasonCode() {
            // TICKET_NOT_ACTIVE không hợp lệ cho BOOK
            assertThrows(IllegalArgumentException.class, () ->
                    ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.TICKET_NOT_ACTIVE, 5L));
        }
    }

    // -----------------------------------------------------------------------
    // 6. ApplyResult factory — CANCEL
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("6. ApplyResult factory — CANCEL")
    class ApplyResultCancelFactory {

        @Test
        @DisplayName("cancelConfirmed — CONFIRMED với ticketId vé đích")
        void cancelConfirmed() {
            var r = ApplyResult.cancelConfirmed(VALID_REQUEST_ID, VALID_TICKET_ID, 50L);
            assertEquals(ApplyResult.Outcome.CONFIRMED, r.outcome());
            assertEquals(VALID_TICKET_ID, r.ticketId());
            assertNull(r.reasonCode());
            assertEquals(50L, r.appliedLogIndex());
            assertTrue(r.isPersistable());
        }

        @Test
        @DisplayName("cancelRejected — REJECTED với TICKET_NOT_ACTIVE, ticketId non-null")
        void cancelRejected() {
            var r = ApplyResult.cancelRejected(VALID_REQUEST_ID, VALID_TICKET_ID, 20L);
            assertEquals(ApplyResult.Outcome.REJECTED, r.outcome());
            assertEquals(VALID_TICKET_ID, r.ticketId(),
                    "CANCEL REJECTED phải giữ ticketId là vé đích");
            assertEquals(ApplyResult.TICKET_NOT_ACTIVE, r.reasonCode());
            assertTrue(r.isPersistable());
        }

        @Test
        @DisplayName("cancelRejected null ticketId bị từ chối")
        void cancelRejectedNullTicketId() {
            assertThrows(IllegalArgumentException.class, () ->
                    ApplyResult.cancelRejected(VALID_REQUEST_ID, null, 5L));
        }

        @Test
        @DisplayName("cancelUnknown — UNKNOWN, isPersistable() = false")
        void cancelUnknown() {
            var r = ApplyResult.cancelUnknown(VALID_REQUEST_ID);
            assertEquals(ApplyResult.Outcome.UNKNOWN, r.outcome());
            assertFalse(r.isPersistable());
        }
    }

    // -----------------------------------------------------------------------
    // 7. REJECTED khác UNKNOWN
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("7. REJECTED ≠ UNKNOWN — phân biệt rõ")
    class RejectedVsUnknown {

        @Test
        @DisplayName("REJECTED có appliedLogIndex; UNKNOWN thì không")
        void rejectedHasIndexUnknownDoesNot() {
            var rejected = ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.SEAT_ALREADY_BOOKED, 7L);
            var unknown  = ApplyResult.bookUnknown(VALID_REQUEST_ID);

            assertNotNull(rejected.appliedLogIndex());
            assertNull(unknown.appliedLogIndex());
        }

        @Test
        @DisplayName("REJECTED isPersistable=true; UNKNOWN isPersistable=false")
        void persistability() {
            var rejected = ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.INVALID_REQUEST, 3L);
            var unknown  = ApplyResult.cancelUnknown(VALID_REQUEST_ID);

            assertTrue(rejected.isPersistable());
            assertFalse(unknown.isPersistable());
        }

        @Test
        @DisplayName("REJECTED reasonCode là nghiệp vụ; UNKNOWN là TIMEOUT_UNKNOWN")
        void reasonCodeDifferent() {
            var rejected = ApplyResult.bookRejected(VALID_REQUEST_ID, ApplyResult.SEAT_ALREADY_BOOKED, 1L);
            var unknown  = ApplyResult.bookUnknown(VALID_REQUEST_ID);

            assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, rejected.reasonCode());
            assertEquals(ApplyResult.TIMEOUT_UNKNOWN, unknown.reasonCode());
        }
    }

    // -----------------------------------------------------------------------
    // 8. Tổ hợp field sai bị từ chối trong constructor ApplyResult
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("8. ApplyResult — tổ hợp field sai bị từ chối")
    class ApplyResultInvalidCombinations {

        @Test
        @DisplayName("CONFIRMED với appliedLogIndex = 0 bị từ chối")
        void confirmedIndexZero() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.CONFIRMED,
                            VALID_TICKET_ID, null, 0L));
        }

        @Test
        @DisplayName("CONFIRMED với ticketId = null bị từ chối")
        void confirmedNullTicketId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.CONFIRMED,
                            null, null, 1L));
        }

        @Test
        @DisplayName("CONFIRMED với reasonCode != null bị từ chối")
        void confirmedWithReasonCode() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.CONFIRMED,
                            VALID_TICKET_ID, ApplyResult.SEAT_ALREADY_BOOKED, 1L));
        }

        @Test
        @DisplayName("REJECTED với appliedLogIndex = null bị từ chối")
        void rejectedNullIndex() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.REJECTED,
                            null, ApplyResult.SEAT_ALREADY_BOOKED, null));
        }

        @Test
        @DisplayName("REJECTED với reasonCode không hợp lệ bị từ chối")
        void rejectedBadReasonCode() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.REJECTED,
                            null, "MADE_UP_CODE", 1L));
        }

        @Test
        @DisplayName("UNKNOWN với appliedLogIndex != null bị từ chối")
        void unknownWithIndex() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.UNKNOWN,
                            null, ApplyResult.TIMEOUT_UNKNOWN, 5L));
        }

        @Test
        @DisplayName("UNKNOWN với reasonCode sai bị từ chối")
        void unknownWrongReasonCode() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.UNKNOWN,
                            null, ApplyResult.SEAT_ALREADY_BOOKED, null));
        }

        @Test
        @DisplayName("UNKNOWN với ticketId != null bị từ chối")
        void unknownWithTicketId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(VALID_REQUEST_ID, ApplyResult.Outcome.UNKNOWN,
                            VALID_TICKET_ID, ApplyResult.TIMEOUT_UNKNOWN, null));
        }

        @Test
        @DisplayName("requestId null bị từ chối")
        void nullRequestId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult(null, ApplyResult.Outcome.UNKNOWN,
                            null, ApplyResult.TIMEOUT_UNKNOWN, null));
        }

        @Test
        @DisplayName("requestId in hoa bị từ chối")
        void upperCaseRequestId() {
            assertThrows(IllegalArgumentException.class, () ->
                    new ApplyResult("542E32FB-B657-4478-9CEA-52286822D1D2",
                            ApplyResult.Outcome.CONFIRMED,
                            VALID_TICKET_ID, null, 1L));
        }
    }

    // -----------------------------------------------------------------------
    // 9. Tên sau strip nhưng 120 code point có emoji — kiểm tra biên phức hợp
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("9. Tên strip + code point biên phức hợp")
    class NameBoundaryWithUnicode {

        @Test
        @DisplayName("Tên gồm 120 emoji (mỗi cái 2 UTF-16 unit) — 120 code point hợp lệ")
        void name120EmojiCodePoints() {
            // U+1F600 mỗi cái là 1 code point nhưng 2 char Java
            String emoji = "\uD83D\uDE00"; // 1 code point
            String name  = emoji.repeat(120); // 120 code point, 240 char
            assertEquals(240, name.length());        // xác nhận 240 Java char
            assertEquals(120, name.codePointCount(0, name.length())); // nhưng 120 code point

            assertDoesNotThrow(() -> new BookingCommand.BookTicket(
                    VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                    name, VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên gồm 121 emoji (121 code point) — bị từ chối")
        void name121EmojiCodePoints() {
            String emoji = "\uD83D\uDE00";
            String name  = emoji.repeat(121); // 121 code point
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            name, VALID_TICKET_ID, VALID_INSTANT));
        }

        @Test
        @DisplayName("Tên chỉ khoảng trắng Unicode — sau strip rỗng, bị từ chối")
        void nameOnlyUnicodeSpaces() {
            // \u3000 là fullwidth space, bị strip()
            String name = "\u3000\u3000";
            assertThrows(IllegalArgumentException.class, () ->
                    new BookingCommand.BookTicket(
                            VALID_REQUEST_ID, VALID_TRIP_ID, "G01",
                            name, VALID_TICKET_ID, VALID_INSTANT));
        }
    }
}
