package vn.nhom6.railway.raft.replication;

import java.util.List;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.raft.core.RaftLogEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure in-memory tests for {@link LogConsistencyChecker} (SPEC-04 sections 4 and 10, SPEC-03
 * section 6).
 *
 * <p>Only {@link InMemoryRaftLog} and {@link RaftLogEntry} are used: no database, socket, RPC,
 * timer, thread, clock, random value or file takes part, so these tests prove the prevLog and
 * conflict rules of the in-memory log only. They never claim that replication, durability, commit
 * or apply works.
 */
class ConflictTests {

    private static final String PAYLOAD_A = "{\"requestId\":\"r-1\",\"tripId\":\"SE-001\",\"seatId\":\"G01\"}";
    private static final String PAYLOAD_B = "{\"requestId\":\"r-2\",\"tripId\":\"SE-001\",\"seatId\":\"G02\"}";

    private static RaftLogEntry entry(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static RaftLogEntry payloadEntry(long index, long term, String payload) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static InMemoryRaftLog logOf(RaftLogEntry... entries) {
        InMemoryRaftLog log = new InMemoryRaftLog();
        for (RaftLogEntry entry : entries) {
            log.append(entry);
        }
        return log;
    }

    private static LogConsistencyChecker checkerOf(InMemoryRaftLog log) {
        return new LogConsistencyChecker(log);
    }

    private static void assertLogUnchanged(InMemoryRaftLog log, List<RaftLogEntry> expected) {
        assertEquals(expected, log.entries());
        assertEquals(expected.size(), log.size());
        assertEquals(expected.isEmpty() ? 0L : expected.get(expected.size() - 1).index(), log.lastIndex());
        assertEquals(expected.isEmpty() ? 0L : expected.get(expected.size() - 1).term(), log.lastTerm());
    }

    private static void assertRejected(LogConsistencyChecker.ReconcileResult result) {
        assertFalse(result.accepted());
        assertEquals(0L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertTrue(result.appended().isEmpty());
    }

    // -------------------------------------------------------------------------------------------
    // validate: empty log and schema errors
    // -------------------------------------------------------------------------------------------

    @Test
    void emptyLogMatchesPrevLogZeroZero() {
        LogConsistencyChecker checker = checkerOf(new InMemoryRaftLog());

        assertEquals(LogConsistencyChecker.PrevLogCheck.MATCH, checker.validate(0L, 0L));
    }

    @Test
    void prevLogZeroWithNonZeroTermIsASchemaErrorNotAMismatch() {
        InMemoryRaftLog log = logOf(entry(1L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class, () -> checker.validate(0L, 1L));

        assertLogUnchanged(log, before);
    }

    @Test
    void prevLogIndexBeyondEmptyLogIsMissing() {
        LogConsistencyChecker checker = checkerOf(new InMemoryRaftLog());

        assertEquals(LogConsistencyChecker.PrevLogCheck.PREV_INDEX_MISSING, checker.validate(1L, 1L));
    }

    // -------------------------------------------------------------------------------------------
    // validate: populated log, read-only
    // -------------------------------------------------------------------------------------------

    @Test
    void matchingLastEntryAndBeyondMatchingIndex() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertEquals(LogConsistencyChecker.PrevLogCheck.MATCH, checker.validate(3L, 2L));
        assertEquals(LogConsistencyChecker.PrevLogCheck.MATCH, checker.validate(2L, 1L));
        assertEquals(LogConsistencyChecker.PrevLogCheck.PREV_INDEX_MISSING, checker.validate(4L, 2L));
        assertEquals(LogConsistencyChecker.PrevLogCheck.PREV_TERM_MISMATCH, checker.validate(2L, 2L));
        assertEquals(LogConsistencyChecker.PrevLogCheck.PREV_TERM_MISMATCH, checker.validate(1L, 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void negativePrevLogArgumentsAreRejectedWithoutChangingTheLog() {
        InMemoryRaftLog log = logOf(entry(1L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class, () -> checker.validate(-1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> checker.validate(1L, -1L));

        assertLogUnchanged(log, before);
    }

    // -------------------------------------------------------------------------------------------
    // PASS criterion 1: wrong prevLog is rejected and the log stays untouched
    // -------------------------------------------------------------------------------------------

    @Test
    void reconcileWithMissingPrevLogIsRejectedAndLeavesLogUntouched() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(4L, 2L, List.of(entry(5L, 2L)), 2L);

        assertRejected(result);
        assertLogUnchanged(log, before);
    }

    @Test
    void reconcileWithMismatchedPrevTermIsRejectedAndLeavesLogUntouched() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(3L, 4L, List.of(entry(4L, 4L)), 2L);

        assertRejected(result);
        assertLogUnchanged(log, before);
    }

    // -------------------------------------------------------------------------------------------
    // Malformed entry chains are schema errors and change nothing
    // -------------------------------------------------------------------------------------------

    @Test
    void chainNotStartingAtPrevLogIndexPlusOneIsRejected() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class,
                () -> checker.reconcile(2L, 1L, List.of(entry(4L, 2L)), 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void chainWithAGapIsRejected() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class,
                () -> checker.reconcile(2L, 1L, List.of(entry(3L, 2L), entry(5L, 2L)), 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void chainWithDecreasingTermsIsRejected() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class,
                () -> checker.reconcile(2L, 1L, List.of(entry(3L, 3L), entry(4L, 2L)), 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void firstEntryTermBelowPrevLogTermIsRejected() {
        InMemoryRaftLog log = logOf(entry(1L, 2L), entry(2L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class,
                () -> checker.reconcile(2L, 2L, List.of(entry(3L, 1L)), 2L));

        assertLogUnchanged(log, before);
    }

    // -------------------------------------------------------------------------------------------
    // Append, heartbeat, idempotency, shorter batch
    // -------------------------------------------------------------------------------------------

    @Test
    void appendingToAnEmptyLogFromPrevLogZeroZeroSucceeds() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> batch = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));

        LogConsistencyChecker.ReconcileResult result = checker.reconcile(0L, 0L, batch, 0L);

        assertTrue(result.accepted());
        assertEquals(3L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertEquals(batch, result.appended());
        assertEquals(batch, log.entries());
    }

    @Test
    void heartbeatChecksPrevLogWithoutTruncatingTheTail() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result = checker.reconcile(3L, 2L, List.of(), 2L);

        assertTrue(result.accepted());
        assertEquals(3L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertTrue(result.appended().isEmpty());
        assertLogUnchanged(log, before);
    }

    @Test
    void heartbeatAtAnEarlierPrevLogIndexDoesNotTruncateTheLongerTail() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result = checker.reconcile(1L, 1L, List.of(), 2L);

        assertTrue(result.accepted());
        assertEquals(1L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertLogUnchanged(log, before);
    }

    @Test
    void resendingEntriesThatAlreadyMatchIsIdempotent() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(0L, 0L, List.of(entry(1L, 1L), entry(2L, 1L)), 2L);

        assertTrue(result.accepted());
        assertEquals(2L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertTrue(result.appended().isEmpty());
        assertLogUnchanged(log, before);
    }

    @Test
    void shorterBatchThatMatchesThePrefixKeepsTheLongerTail() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(1L, 1L, List.of(entry(2L, 1L), entry(3L, 2L)), 2L);

        assertTrue(result.accepted());
        assertEquals(3L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertTrue(result.appended().isEmpty());
        assertLogUnchanged(log, before);
    }

    // -------------------------------------------------------------------------------------------
    // PASS criterion 2: remove exactly the conflicting uncommitted suffix
    // -------------------------------------------------------------------------------------------

    @Test
    void truncatesExactlyTheUncommittedConflictingSuffix() {
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        InMemoryRaftLog log = logOf(first, second, entry(3L, 2L), entry(4L, 2L), entry(5L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> batch = List.of(entry(3L, 3L), entry(4L, 3L));

        LogConsistencyChecker.ReconcileResult result = checker.reconcile(2L, 1L, batch, 2L);

        assertTrue(result.accepted());
        assertEquals(4L, result.matchIndex());
        assertEquals(3L, result.truncatedFromIndex());
        assertEquals(batch, result.appended());
        assertEquals(List.of(first, second, entry(3L, 3L), entry(4L, 3L)), log.entries());
        assertEquals(4L, log.lastIndex());
        assertEquals(3L, log.lastTerm());
        assertEquals(4, log.size());
        assertSame(first, log.get(1L).orElseThrow());
        assertSame(second, log.get(2L).orElseThrow());
    }

    @Test
    void divergenceInTheMiddleOfTheBatchKeepsTheEarlierMatchingEntries() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 3L), entry(5L, 3L));
        LogConsistencyChecker checker = checkerOf(log);

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(1L, 1L, List.of(entry(2L, 1L), entry(3L, 2L), entry(4L, 4L)), 2L);

        assertTrue(result.accepted());
        assertEquals(4L, result.matchIndex());
        assertEquals(4L, result.truncatedFromIndex());
        assertEquals(List.of(entry(4L, 4L)), result.appended());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 4L)), log.entries());
    }

    @Test
    void conflictAtCommitIndexPlusOneMayBeTruncated() {
        InMemoryRaftLog log = logOf(entry(1L, 2L), entry(2L, 2L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);

        LogConsistencyChecker.ReconcileResult result = checker.reconcile(2L, 2L, List.of(entry(3L, 3L)), 2L);

        assertTrue(result.accepted());
        assertEquals(3L, result.matchIndex());
        assertEquals(3L, result.truncatedFromIndex());
        assertEquals(List.of(entry(3L, 3L)), result.appended());
        assertEquals(List.of(entry(1L, 2L), entry(2L, 2L), entry(3L, 3L)), log.entries());
    }

    @Test
    void truncateThenAppendNeverLeavesAGap() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        LogConsistencyChecker checker = checkerOf(log);

        checker.reconcile(2L, 1L, List.of(entry(3L, 3L), entry(4L, 3L), entry(5L, 3L)), 2L);

        assertEquals(5L, log.lastIndex());
        assertEquals(3L, log.lastTerm());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                log.entries().stream().map(RaftLogEntry::index).toList());
        assertEquals(5, log.size());
    }

    // -------------------------------------------------------------------------------------------
    // Committed prefix is never touched
    // -------------------------------------------------------------------------------------------

    @Test
    void conflictAtACommittedIndexIsFatalAndLeavesTheLogUntouched() {
        InMemoryRaftLog log = logOf(entry(1L, 2L), entry(2L, 2L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalStateException.class,
                () -> checker.reconcile(0L, 0L, List.of(entry(1L, 2L), entry(2L, 3L)), 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void sameIndexAndTermWithDifferentCommandAtAnUncommittedIndexIsFatal() {
        RaftLogEntry committed = payloadEntry(1L, 1L, PAYLOAD_A);
        InMemoryRaftLog log = logOf(committed, payloadEntry(2L, 1L, PAYLOAD_A));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalStateException.class,
                () -> checker.reconcile(1L, 1L, List.of(payloadEntry(2L, 1L, PAYLOAD_B)), 1L));

        assertLogUnchanged(log, before);
        assertSame(committed, log.get(1L).orElseThrow());
    }

    @Test
    void sameIndexAndTermWithDifferentCommandAtACommittedIndexIsFatal() {
        InMemoryRaftLog log = logOf(payloadEntry(1L, 1L, PAYLOAD_A), payloadEntry(2L, 1L, PAYLOAD_A));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalStateException.class,
                () -> checker.reconcile(0L, 0L, List.of(payloadEntry(1L, 1L, PAYLOAD_A),
                        payloadEntry(2L, 1L, PAYLOAD_B)), 2L));

        assertLogUnchanged(log, before);
    }

    @Test
    void everyValidReconcileKeepsTheCommittedPrefixIdentical() {
        List<RaftLogEntry> committedPrefix = List.of(entry(1L, 1L), entry(2L, 1L));
        InMemoryRaftLog log = logOf(committedPrefix.get(0), committedPrefix.get(1), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);

        checker.reconcile(2L, 1L, List.of(entry(3L, 3L), entry(4L, 3L)), 2L);

        assertEquals(committedPrefix, log.entries().subList(0, 2));
        assertSame(committedPrefix.get(0), log.get(1L).orElseThrow());
        assertSame(committedPrefix.get(1), log.get(2L).orElseThrow());
    }

    @Test
    void phaseOneFailureAfterValidNewEntriesLeavesNoPartialChange() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class,
                () -> checker.reconcile(0L, 0L,
                        List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L), entry(5L, 1L)), 3L));

        assertLogUnchanged(log, before);
    }

    @Test
    void phaseOneCommittedConflictAfterMatchingEntriesLeavesNoPartialChange() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalStateException.class,
                () -> checker.reconcile(0L, 0L,
                        List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 3L)), 3L));

        assertLogUnchanged(log, before);
    }

    // -------------------------------------------------------------------------------------------
    // Argument validation
    // -------------------------------------------------------------------------------------------

    @Test
    void commitIndexOutsideTheLogIsRejected() {
        InMemoryRaftLog log = logOf(entry(1L, 1L), entry(2L, 1L));
        LogConsistencyChecker checker = checkerOf(log);
        List<RaftLogEntry> before = log.entries();

        assertThrows(IllegalArgumentException.class, () -> checker.reconcile(0L, 0L, List.of(), -1L));
        assertThrows(IllegalArgumentException.class, () -> checker.reconcile(2L, 1L, List.of(), 3L));

        assertLogUnchanged(log, before);
    }

    @Test
    void nullEntriesAndNullLogAreRejected() {
        LogConsistencyChecker checker = checkerOf(new InMemoryRaftLog());

        assertThrows(NullPointerException.class, () -> checker.reconcile(0L, 0L, null, 0L));
        assertThrows(NullPointerException.class, () -> new LogConsistencyChecker(null));
    }

    @Test
    void appendedSnapshotIsUnmodifiable() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        LogConsistencyChecker checker = checkerOf(log);

        LogConsistencyChecker.ReconcileResult result =
                checker.reconcile(0L, 0L, List.of(entry(1L, 1L)), 0L);

        assertThrows(UnsupportedOperationException.class, () -> result.appended().add(entry(2L, 1L)));
    }
}
