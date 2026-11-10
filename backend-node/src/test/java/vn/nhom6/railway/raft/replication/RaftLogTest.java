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
 * Pure in-memory tests for {@link InMemoryRaftLog} (SPEC-03 section 3.0, SPEC-04 sections 4, 6
 * and 10).
 *
 * <p>No database, socket, timer, thread, clock, random value or file is involved. {@link
 * RaftLogEntry} is a record whose components are final and whose only field type is
 * {@link String}, so an entry returned by {@link InMemoryRaftLog#get(long)} has no setter and can
 * never be modified through this log; {@link InMemoryRaftLog} itself exposes no mutation API for
 * an appended entry.
 *
 * <p>The {@code Math.addExact} overflow guard for {@code lastIndex() == Long.MAX_VALUE} is not
 * covered here: reaching it would require appending {@code Long.MAX_VALUE} entries and would
 * break the time/memory budget of a unit test, so the guard is kept as documented defensive code
 * rather than exercised.
 */
class RaftLogTest {

    private static RaftLogEntry entry(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    // -------------------------------------------------------------------------------------------
    // Empty log
    // -------------------------------------------------------------------------------------------

    @Test
    void emptyLogReportsZeroIndexZeroTermAndNoEntries() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        assertEquals(0L, log.lastIndex());
        assertEquals(0L, log.lastTerm());
        assertEquals(0, log.size());
        assertTrue(log.isEmpty());
        assertEquals(0L, log.termAt(0L), "the empty prefix is (index=0, term=0)");
        assertTrue(log.get(0L).isEmpty(), "index 0 is never a stored entry");
        assertTrue(log.get(1L).isEmpty(), "an empty log has no entry at index 1");
        assertEquals(List.of(), log.entries());
    }

    @Test
    void appendFirstEntryToEmptyLogStartsAtContiguousIndexOne() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        RaftLogEntry first = entry(1L, 1L);

        log.append(first);

        assertEquals(1L, log.lastIndex());
        assertEquals(1L, log.lastTerm());
        assertEquals(1, log.size());
        assertFalse(log.isEmpty());
        assertEquals(first, log.get(1L).orElseThrow());
        assertEquals(1L, log.termAt(1L));
        assertEquals(List.of(first), log.entries());
    }

    @Test
    void appendIndexTwoToEmptyLogIsRejectedAndLeavesLogEmpty() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        assertThrows(IllegalArgumentException.class, () -> log.append(entry(2L, 1L)));

        assertLogEmpty(log);
    }

    @Test
    void raftLogEntryStillRejectsIndexZeroForANewLog() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        assertThrows(IllegalArgumentException.class, () -> entry(0L, 1L));

        assertLogEmpty(log);
    }

    // -------------------------------------------------------------------------------------------
    // Contiguous increasing indices
    // -------------------------------------------------------------------------------------------

    @Test
    void consecutiveAppendsAdvanceIndexSizeTermAndContent() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        RaftLogEntry third = entry(3L, 2L);

        log.append(first);
        assertEquals(1L, log.lastIndex());
        assertEquals(1L, log.lastTerm());
        assertEquals(1, log.size());
        assertEquals(first, log.get(1L).orElseThrow());
        assertEquals(1L, log.termAt(1L));

        log.append(second);
        assertEquals(2L, log.lastIndex());
        assertEquals(1L, log.lastTerm());
        assertEquals(2, log.size());
        assertEquals(second, log.get(2L).orElseThrow());
        assertEquals(1L, log.termAt(2L));

        log.append(third);
        assertEquals(3L, log.lastIndex());
        assertEquals(2L, log.lastTerm());
        assertEquals(3, log.size());
        assertEquals(third, log.get(3L).orElseThrow());
        assertEquals(2L, log.termAt(3L));

        assertEquals(List.of(first, second, third), log.entries());
        assertEquals(List.of(1L, 2L, 3L), log.entries().stream().map(RaftLogEntry::index).toList());
    }

    @Test
    void duplicateIndexIsRejectedAndLogIsUnchanged() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));
        log.append(entry(2L, 1L));

        assertThrows(IllegalArgumentException.class, () -> log.append(entry(2L, 1L)));

        assertEquals(2L, log.lastIndex());
        assertEquals(2, log.size());
        assertEquals(List.of(1L, 2L), log.entries().stream().map(RaftLogEntry::index).toList());
    }

    @Test
    void skippedIndexIsRejectedAndLogIsUnchanged() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));
        log.append(entry(2L, 1L));

        assertThrows(IllegalArgumentException.class, () -> log.append(entry(4L, 1L)));

        assertEquals(2L, log.lastIndex());
        assertEquals(2, log.size());
        assertEquals(List.of(1L, 2L), log.entries().stream().map(RaftLogEntry::index).toList());
    }

    @Test
    void backwardsIndexIsRejectedAndLogIsUnchanged() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));
        log.append(entry(2L, 1L));
        log.append(entry(3L, 1L));

        assertThrows(IllegalArgumentException.class, () -> log.append(entry(2L, 1L)));

        assertEquals(3L, log.lastIndex());
        assertEquals(3, log.size());
        assertEquals(List.of(1L, 2L, 3L), log.entries().stream().map(RaftLogEntry::index).toList());
    }

    // -------------------------------------------------------------------------------------------
    // Terms
    // -------------------------------------------------------------------------------------------

    @Test
    void equalTermsAreAcceptedAndLastTermTracksLastEntry() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        log.append(entry(1L, 3L));
        assertEquals(3L, log.lastTerm());

        log.append(entry(2L, 3L));
        assertEquals(3L, log.lastTerm());
        assertEquals(3L, log.termAt(1L));
    }

    @Test
    void increasingTermIsAccepted() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        log.append(entry(1L, 2L));
        log.append(entry(2L, 5L));

        assertEquals(5L, log.lastTerm());
        assertEquals(2L, log.termAt(1L));
        assertEquals(5L, log.termAt(2L));
    }

    @Test
    void decreasingTermIsRejectedAndLogIsUnchanged() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 4L));
        log.append(entry(2L, 4L));

        assertThrows(IllegalArgumentException.class, () -> log.append(entry(3L, 3L)));

        assertEquals(2L, log.lastIndex());
        assertEquals(4L, log.lastTerm());
        assertEquals(2, log.size());
    }

    // -------------------------------------------------------------------------------------------
    // get / termAt queries
    // -------------------------------------------------------------------------------------------

    @Test
    void getRejectsNegativeIndexAndReturnsEmptyBeyondLastIndex() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));

        assertThrows(IllegalArgumentException.class, () -> log.get(-1L));
        assertTrue(log.get(2L).isEmpty());
        assertEquals(1L, log.lastIndex());
    }

    @Test
    void termAtReportsZeroForEmptyPrefixAndRejectsIndexBeyondLog() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));

        assertEquals(0L, log.termAt(0L));
        assertEquals(1L, log.termAt(1L));
        assertThrows(IndexOutOfBoundsException.class, () -> log.termAt(2L));
        assertThrows(IllegalArgumentException.class, () -> log.termAt(-1L));
    }

    // -------------------------------------------------------------------------------------------
    // Immutability
    // -------------------------------------------------------------------------------------------

    @Test
    void entriesSnapshotIsUnmodifiable() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));
        List<RaftLogEntry> snapshot = log.entries();

        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(entry(2L, 1L)));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.remove(0));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.set(0, entry(1L, 2L)));
        assertEquals(1, log.size());
    }

    @Test
    void earlierSnapshotIsNotAffectedByLaterAppends() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        log.append(entry(1L, 1L));
        List<RaftLogEntry> snapshot = log.entries();

        log.append(entry(2L, 1L));

        assertEquals(1, snapshot.size(), "the returned snapshot must be an independent copy");
        assertEquals(2, log.entries().size());
        assertEquals(2L, log.lastIndex());
    }

    @Test
    void getReturnsTheExactImmutableEntryThatWasAppended() {
        InMemoryRaftLog log = new InMemoryRaftLog();
        RaftLogEntry appended = new RaftLogEntry(
                1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"tripId\":\"T1\",\"seatId\":\"G01\"}");

        log.append(appended);

        RaftLogEntry returned = log.get(1L).orElseThrow();
        assertSame(appended, returned);
        assertEquals(appended, returned);
        assertEquals(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, returned.commandType());
        assertEquals("{\"tripId\":\"T1\",\"seatId\":\"G01\"}", returned.commandPayload());
    }

    @Test
    void appendNullEntryIsRejected() {
        InMemoryRaftLog log = new InMemoryRaftLog();

        assertThrows(NullPointerException.class, () -> log.append(null));

        assertLogEmpty(log);
    }

    private static void assertLogEmpty(InMemoryRaftLog log) {
        assertEquals(0L, log.lastIndex());
        assertEquals(0L, log.lastTerm());
        assertEquals(0, log.size());
        assertTrue(log.isEmpty());
        assertEquals(List.of(), log.entries());
    }
}
