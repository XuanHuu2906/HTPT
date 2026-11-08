package vn.nhom6.railway.booking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** Kiểm thử hợp đồng Booking–Raft–Repository bằng fake; không kiểm thử Raft/PostgreSQL thật. */
@DisplayName("Booking–Raft contract (fake callback, không kiểm thử Raft thật)")
class BookingRaftContractTest {

    private static final String REQ_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQ_2 = "22222222-2222-2222-2222-222222222222";
    private static final Instant T1 = Instant.ofEpochMilli(1_700_000_000_000L);
    private static final Instant T2 = Instant.ofEpochMilli(1_700_000_001_000L);

    @Test
    @DisplayName("Entry commit tuần tự: BOOK, ghế trùng, NO_OP")
    void appliesCommittedEntriesInOrder() {
        FakeBookingRepository repository = new FakeBookingRepository();
        List<ApplyResult> notified = new ArrayList<>();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository, notified::add);
        FakeRaftCallback raft = new FakeRaftCallback(applier.commitListener());

        var first = raft.commit(book(REQ_1, "G01", "ticket-a", T1)).orElseThrow();
        assertEquals(ApplyResult.Outcome.CONFIRMED, businessResult(first).outcome());
        assertEquals(1L, repository.lastApplied());

        var second = raft.commit(book(REQ_2, "G01", "ticket-b", T2)).orElseThrow();
        assertEquals(ApplyResult.Outcome.REJECTED, businessResult(second).outcome());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, businessResult(second).reasonCode());
        assertEquals(2L, repository.lastApplied());

        var third = raft.commit(new BookingCommand.NoOp()).orElseThrow();
        assertInstanceOf(BookingStateMachine.ApplyOutcome.NoOpApplied.class, third);
        assertEquals(3L, repository.lastApplied());
        assertEquals(1, repository.stateMachine().totalTicketCount());
        assertEquals(1L, repository.stateMachine().activeTicketCount());
        assertEquals("ACTIVE", repository.stateMachine().ticketStatus("ticket-a").orElseThrow());
        assertTrue(repository.stateMachine().ticketStatus("ticket-b").isEmpty());
        assertEquals(2, notified.size());
    }

    @Test
    @DisplayName("Trước khi Raft callback đẩy entry, không phát CONFIRMED")
    void doesNotNotifyBeforeCommittedEntryIsDelivered() {
        FakeBookingRepository repository = new FakeBookingRepository();
        List<ApplyResult> notified = new ArrayList<>();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository, notified::add);
        FakeRaftCallback raft = new FakeRaftCallback(applier.commitListener());

        assertTrue(notified.isEmpty());
        assertEquals(0L, repository.lastApplied());
        raft.commit(book(REQ_1, "G01", "ticket-a", T1));
        assertEquals(1, notified.size());
        assertEquals(ApplyResult.Outcome.CONFIRMED, notified.getFirst().outcome());
    }

    @Test
    @DisplayName("Redelivery cùng index trả kết quả cũ và không apply lần hai")
    void redeliveryReturnsSavedResultWithoutApplyingAgain() {
        FakeBookingRepository repository = new FakeBookingRepository();
        List<ApplyResult> notified = new ArrayList<>();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository, notified::add);
        BookingCommand command = book(REQ_1, "G01", "ticket-a", T1);
        var original = applier.applyCommitted(1, command).orElseThrow();
        int ticketsBeforeRedelivery = repository.stateMachine().totalTicketCount();
        var replay = applier.applyCommitted(1, command).orElseThrow();

        assertEquals(original, replay);
        assertEquals(1L, repository.lastApplied());
        assertEquals(ticketsBeforeRedelivery, repository.stateMachine().totalTicketCount());
        assertEquals(1L, repository.stateMachine().activeTicketCount());
        assertEquals(1, notified.size());
    }

    @Test
    @DisplayName("Index có lỗ bị từ chối, state và lastApplied không đổi")
    void rejectsIndexGap() {
        FakeBookingRepository repository = new FakeBookingRepository();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository);
        Map<String, String> before = repository.readSeatSnapshot();

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> applier.applyCommitted(2, book(REQ_1, "G01", "ticket-a", T1)));
        assertTrue(error.getMessage().contains("lỗ"));
        assertEquals(0L, repository.lastApplied());
        assertEquals(before, repository.readSeatSnapshot());
        assertEquals(0, repository.stateMachine().totalTicketCount());
    }

    @Test
    @DisplayName("Lỗi lưu trữ rollback, chặn index sau và retry cùng entry thành công")
    void storageFailureRollsBackAndCanRetry() {
        FakeBookingRepository repository = new FakeBookingRepository();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository);
        FakeRaftCallback raft = new FakeRaftCallback(applier.commitListener());
        BookingCommand first = book(REQ_1, "G01", "ticket-a", T1);

        repository.failNextApply();
        assertThrows(BookingRepository.StorageException.class, () -> raft.commit(first));
        assertEquals(0L, repository.lastApplied());
        assertEquals(0, repository.stateMachine().totalTicketCount());

        assertThrows(IllegalStateException.class,
                () -> raft.deliverCommitted(2, book(REQ_2, "G02", "ticket-b", T2)));
        assertEquals(0L, repository.lastApplied());

        var retry = raft.commit(first).orElseThrow();
        assertEquals(ApplyResult.Outcome.CONFIRMED, businessResult(retry).outcome());
        assertEquals(1L, repository.lastApplied());
        assertEquals(1, repository.stateMachine().totalTicketCount());
        assertEquals(1L, repository.stateMachine().activeTicketCount());
    }

    @Test
    @DisplayName("Restart applier và phát lại từ index 1 không apply lại, entry mới tiếp tục")
    void restartAndReplayFromFirstIndex() {
        FakeBookingRepository repository = new FakeBookingRepository();
        CommittedCommandApplier firstApplier = new CommittedCommandApplier(repository);
        FakeRaftCallback firstRaft = new FakeRaftCallback(firstApplier.commitListener());
        BookingCommand first = book(REQ_1, "G01", "ticket-a", T1);
        BookingCommand second = new BookingCommand.NoOp();
        firstRaft.commit(first);
        firstRaft.commit(second);
        int ticketsBeforeRestart = repository.stateMachine().totalTicketCount();

        CommittedCommandApplier restarted = new CommittedCommandApplier(repository);
        FakeRaftCallback replayingRaft = new FakeRaftCallback(restarted.commitListener());
        assertTrue(replayingRaft.deliverCommitted(1, first).orElseThrow()
                instanceof BookingStateMachine.ApplyOutcome.BusinessResult);
        assertTrue(replayingRaft.deliverCommitted(2, second).isEmpty());
        assertEquals(2L, repository.lastApplied());
        assertEquals(ticketsBeforeRestart, repository.stateMachine().totalTicketCount());

        var newEntry = replayingRaft.commit(book(REQ_2, "G02", "ticket-b", T2)).orElseThrow();
        assertEquals(ApplyResult.Outcome.CONFIRMED, businessResult(newEntry).outcome());
        assertEquals(3L, repository.lastApplied());
        assertEquals(2, repository.stateMachine().totalTicketCount());
    }

    @Test
    @DisplayName("RequestId qua Leader khác trả kết quả gốc; khác ý định conflict không ghi đè")
    void deduplicatesRequestIdAcrossLeadersAndPreservesOriginalOnConflict() {
        FakeBookingRepository repository = new FakeBookingRepository();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository);
        FakeRaftCallback raft = new FakeRaftCallback(applier.commitListener());
        BookingCommand first = book(REQ_1, "G01", "ticket-original", T1);
        ApplyResult original = businessResult(raft.commit(first).orElseThrow());

        BookingCommand retryFromOtherLeader = book(REQ_1, "G01", "ticket-other", T2);
        var retryOutcome = raft.commit(retryFromOtherLeader).orElseThrow();
        assertTrue(((BookingStateMachine.ApplyOutcome.BusinessResult) retryOutcome).isReplay());
        assertEquals(original, businessResult(retryOutcome));
        assertEquals(1, repository.stateMachine().totalTicketCount());
        assertTrue(repository.stateMachine().ticketStatus("ticket-other").isEmpty());

        var conflict = raft.commit(book(REQ_1, "G02", "ticket-conflict", T2)).orElseThrow();
        assertInstanceOf(BookingStateMachine.ApplyOutcome.RequestIdConflict.class, conflict);
        assertEquals(original, repository.findResultByRequestId(REQ_1).orElseThrow());
        assertEquals(1, repository.stateMachine().totalTicketCount());
        assertEquals("AVAILABLE", repository.readSeatSnapshot().get("G02"));
    }

    @Test
    @DisplayName("Applier không trả UNKNOWN; UNKNOWN từ repository bị fail-closed")
    void neverReturnsUnknown() {
        FakeBookingRepository repository = new FakeBookingRepository();
        CommittedCommandApplier applier = new CommittedCommandApplier(repository);
        BookingCommand command = book(REQ_1, "G01", "ticket-a", T1);
        repository.returnUnknownNextApply();

        assertThrows(IllegalStateException.class, () -> applier.applyCommitted(1, command));
        var replay = applier.applyCommitted(1, command).orElseThrow();
        assertNotEquals(ApplyResult.Outcome.UNKNOWN, businessResult(replay).outcome());
    }

    private static BookingCommand.BookTicket book(
            String requestId, String seatId, String ticketId, Instant issuedAt) {
        return new BookingCommand.BookTicket(
                requestId, "SE-001", seatId, "Passenger", ticketId, issuedAt);
    }

    private static ApplyResult businessResult(BookingStateMachine.ApplyOutcome outcome) {
        return ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome).applyResult();
    }

    /** Fake in-memory transaction: stage a rebuilt state machine and publish it only on success. */
    private static final class FakeBookingRepository implements BookingRepository {
        private record AppliedEntry(long index, BookingCommand command) { }

        private BookingStateMachine stateMachine = new BookingStateMachine();
        private final List<AppliedEntry> appliedEntries = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();
        private boolean failNext;
        private boolean returnUnknownNext;

        @Override
        public long lastApplied() {
            return stateMachine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            BookingStateMachine staged = new BookingStateMachine();
            for (AppliedEntry entry : appliedEntries) {
                staged.apply(entry.index(), entry.command());
            }
            BookingStateMachine.ApplyOutcome stagedOutcome = staged.apply(logIndex, command);

            // Giả lập lỗi giữa transaction: staged state đã đổi, nhưng chưa publish nên rollback.
            if (failNext) {
                failNext = false;
                throw new StorageException("Lỗi lưu trữ giả lập trước commit transaction");
            }

            stateMachine = staged;
            appliedEntries.add(new AppliedEntry(logIndex, command));
            outcomes.put(logIndex, stagedOutcome);
            if (returnUnknownNext) {
                returnUnknownNext = false;
                String requestId = switch (command) {
                    case BookingCommand.BookTicket book -> book.requestId();
                    case BookingCommand.CancelTicket cancel -> cancel.requestId();
                    case BookingCommand.NoOp ignored ->
                        throw new IllegalStateException("NO_OP không có requestId");
                };
                return new BookingStateMachine.ApplyOutcome.BusinessResult(
                    ApplyResult.bookUnknown(requestId), false);
            }
            return stagedOutcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return stateMachine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomes.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return stateMachine.allSeatStatuses();
        }

        BookingStateMachine stateMachine() {
            return stateMachine;
        }

        void failNextApply() {
            failNext = true;
        }

        void returnUnknownNextApply() {
            returnUnknownNext = true;
        }
    }

    /** Fake callback mô phỏng Raft chỉ bằng cách gửi entry đã commit đến applier. */
    private static final class FakeRaftCallback {
        private final CommittedCommandApplier.CommitListener listener;
        private long nextIndex;

        FakeRaftCallback(CommittedCommandApplier.CommitListener listener) {
            this(listener, 1);
        }

        FakeRaftCallback(CommittedCommandApplier.CommitListener listener, long firstIndex) {
            this.listener = listener;
            this.nextIndex = firstIndex;
        }

        Optional<BookingStateMachine.ApplyOutcome> commit(BookingCommand command) {
            return deliverCommitted(nextIndex, command);
        }

        Optional<BookingStateMachine.ApplyOutcome> deliverCommitted(long index, BookingCommand command) {
            // Đây là entry giả định đã commit, không phải kiểm thử Raft thật.
            Optional<BookingStateMachine.ApplyOutcome> result = listener.onCommitted(index, command);
            if (index == nextIndex) {
                nextIndex++;
            }
            return result;
        }
    }
}