package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.replication.LogConflictResolver.ConflictResolution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LogConflictResolver} using {@link FakeRaftStorage}
 * (SPEC-04 sections 4, 5, 6, 9 and 10, SPEC-03 section 6).
 *
 * <p>These are unit tests only. The fake storage is in memory, so this suite
 * <strong>does not</strong> prove durability on PostgreSQL, replication, Raft commitment, majority,
 * state-machine apply or real {@code APPEND_ENTRIES} RPC handling. {@code JdbcRaftStorage} belongs
 * to TV2-D12; the durable PostgreSQL check is performed after that hand-over.
 *
 * <p>The central properties are "only the uncommitted suffix may change" and "persist before ACK":
 * every successful resolution keeps the committed prefix {@code [1, commitIndex]} unchanged and
 * keeps the visible log equal to the stored log. No JDBC, socket, timer, thread, clock, random
 * value, UUID or file takes part.
 */
class LogConflictResolverTest {

    private static final String PAYLOAD_A = "{\"requestId\":\"r-1\",\"tripId\":\"SE-001\",\"seatId\":\"G01\"}";
    private static final String PAYLOAD_B = "{\"requestId\":\"r-2\",\"tripId\":\"SE-001\",\"seatId\":\"G02\"}";

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static RaftLogEntry entry(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static RaftLogEntry payloadEntry(long index, long term, String payload) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static AppendEntriesRequest append(
            long term, long prevLogIndex, long prevLogTerm, long leaderCommit, RaftLogEntry... entries) {
        return new AppendEntriesRequest(
                term, "node-1", prevLogIndex, prevLogTerm, List.of(entries), leaderCommit);
    }

    private static AppendEntriesRequest heartbeat(
            long term, long prevLogIndex, long prevLogTerm, long leaderCommit) {
        return append(term, prevLogIndex, prevLogTerm, leaderCommit);
    }

    private static FakeRaftStorage storageWith(long term, long applied, RaftLogEntry... entries) {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, List.of(entries), applied);
        return storage;
    }

    private static PersistentLogAppender recoveredAppender(FakeRaftStorage storage)
            throws RaftStorageException {
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        return appender;
    }

    private static List<RaftLogEntry> visibleLog(PersistentLogAppender appender) {
        List<RaftLogEntry> visible = new ArrayList<>();
        for (long index = RaftLogEntry.FIRST_INDEX; index <= appender.lastIndex(); index++) {
            visible.add(appender.read(index).orElseThrow());
        }
        return visible;
    }

    private static void assertInSync(PersistentLogAppender appender, FakeRaftStorage storage) {
        assertEquals(storage.storedLog(), visibleLog(appender), "visible log must equal stored log");
        List<RaftLogEntry> stored = storage.storedLog();
        long expectedLastIndex = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).index();
        long expectedLastTerm = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).term();
        assertEquals(expectedLastIndex, appender.lastIndex());
        assertEquals(expectedLastTerm, appender.lastTerm());
    }

    private static void assertCommittedPrefix(
            PersistentLogAppender appender, List<RaftLogEntry> expectedPrefix) {
        List<RaftLogEntry> visible = visibleLog(appender);
        assertEquals(expectedPrefix, visible.subList(0, expectedPrefix.size()),
                "the committed prefix must not change");
    }

    private static List<RaftLogEntry> prefix(PersistentLogAppender appender, long committedIndex) {
        return List.copyOf(visibleLog(appender).subList(0, (int) committedIndex));
    }

    // -------------------------------------------------------------------------------------------
    // Sheet PASS criterion: leader A and follower B with different terms
    // -------------------------------------------------------------------------------------------

    /**
     * Sheet criterion: B has {@code [1:t1, 2:t1, 3:t2, 4:t2]} with commitIndex 2. A sends term 3,
     * prev {@code (2, 1)} and {@code [3:t3, 4:t3, 5:t3]}. Only index 3 and 4 may be replaced.
     */
    @Test
    void leaderAFollowerBDifferentTerms() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L,
                entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        List<RaftLogEntry> committed = List.of(entry(1L, 1L), entry(2L, 1L));

        ConflictResolution resolution = resolver.resolve(
                append(3L, 2L, 1L, 2L, entry(3L, 3L), entry(4L, 3L), entry(5L, 3L)), 3L, 2L);

        assertTrue(resolution.response().success());
        assertEquals(3L, resolution.response().term());
        assertEquals(5L, resolution.response().matchIndex());
        assertEquals(3L, resolution.truncatedFromIndex());
        assertEquals(List.of(entry(3L, 2L), entry(4L, 2L)), resolution.removed());
        assertEquals(List.of(entry(3L, 3L), entry(4L, 3L), entry(5L, 3L)), resolution.appended());

        List<RaftLogEntry> leaderA = List.of(
                entry(1L, 1L), entry(2L, 1L), entry(3L, 3L), entry(4L, 3L), entry(5L, 3L));
        assertEquals(leaderA, visibleLog(appender));
        assertCommittedPrefix(appender, committed);

        assertEquals(1, storage.replaceSuffixCalls());
        assertEquals("replaceSuffix(3, 3)", storage.calls().get(storage.calls().size() - 1));
        assertInSync(appender, storage);
    }

    /**
     * The backward movement of {@code nextIndex} is modelled inside this test, not in production
     * code (that part belongs to TV5). B diverges from index 2 with commitIndex 1. The loop starts
     * at {@code lastIndex(A) + 1}, and every rejected request must leave storage and the log alone.
     */
    @Test
    void leaderBackoffConvergence() throws RaftStorageException {
        List<RaftLogEntry> leaderA = List.of(entry(1L, 1L), entry(2L, 2L), entry(3L, 2L));
        List<RaftLogEntry> followerB = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 1L));
        FakeRaftStorage storage = storageWith(2L, 1L,
                followerB.get(0), followerB.get(1), followerB.get(2));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        List<RaftLogEntry> committed = prefix(appender, 1L);

        long nextIndex = leaderA.size() + 1L;
        int attempts = 0;
        while (true) {
            attempts++;
            long prevLogIndex = nextIndex - 1L;
            long prevLogTerm = leaderA.get((int) prevLogIndex - 1).term();
            RaftLogEntry[] batch = leaderA
                    .subList((int) (nextIndex - 1L), leaderA.size())
                    .toArray(new RaftLogEntry[0]);

            ConflictResolution resolution = resolver.resolve(
                    append(2L, prevLogIndex, prevLogTerm, 1L, batch), 2L, 1L);

            if (resolution.response().success()) {
                break;
            }
            assertEquals(0L, resolution.truncatedFromIndex(), "a rejected attempt removes nothing");
            assertTrue(resolution.removed().isEmpty(), "a rejected attempt removes nothing");
            assertEquals(followerB, visibleLog(appender), "a rejected attempt must not change the log");
            assertEquals(0, storage.replaceSuffixCalls(), "a rejected attempt must not touch storage");
            nextIndex--;
        }

        assertEquals(3, attempts, "three attempts: heartbeat rejection, one entry rejection, one success");
        assertEquals(leaderA, visibleLog(appender));
        assertCommittedPrefix(appender, committed);
        assertEquals(1, storage.replaceSuffixCalls(), "only the successful attempt writes");
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Commit boundary
    // -------------------------------------------------------------------------------------------

    @Test
    void divergenceExactlyAtCommitIndexPlusOneIsReplaced() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 1L, entry(1L, 1L), entry(2L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        ConflictResolution resolution = resolver.resolve(
                append(2L, 1L, 1L, 1L, entry(2L, 2L)), 2L, 1L);

        assertTrue(resolution.response().success());
        assertEquals(2L, resolution.truncatedFromIndex());
        assertEquals(List.of(entry(2L, 1L)), resolution.removed());
        assertEquals(List.of(entry(2L, 2L)), resolution.appended());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 2L)), visibleLog(appender));
        assertEquals("replaceSuffix(2, 1)", storage.calls().get(storage.calls().size() - 1));
        assertInSync(appender, storage);
    }

    @Test
    void divergenceAtCommittedIndexFailsClosedWithoutStorageCall() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 1L));
        FakeRaftStorage storage = storageWith(2L, 2L, followerB.get(0), followerB.get(1), followerB.get(2));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(IllegalStateException.class, () -> resolver.resolve(
                append(2L, 1L, 1L, 2L, entry(2L, 2L), entry(3L, 2L)), 2L, 2L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(followerB, visibleLog(appender));
        assertEquals(followerB, storage.storedLog());
        assertInSync(appender, storage);
    }

    @Test
    void sameIndexAndTermDifferentPayloadFailsClosed() throws RaftStorageException {
        List<RaftLogEntry> committedLog = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 1L, PAYLOAD_A),
                payloadEntry(3L, 2L, PAYLOAD_A));
        FakeRaftStorage committedStorage = storageWith(2L, 2L,
                committedLog.get(0), committedLog.get(1), committedLog.get(2));
        PersistentLogAppender committedAppender = recoveredAppender(committedStorage);
        LogConflictResolver committedResolver = new LogConflictResolver(committedAppender);

        assertThrows(IllegalStateException.class, () -> committedResolver.resolve(
                append(2L, 1L, 1L, 2L, payloadEntry(2L, 1L, PAYLOAD_B)), 2L, 2L));
        assertEquals(0, committedStorage.replaceSuffixCalls());
        assertEquals(committedLog, visibleLog(committedAppender));
        assertInSync(committedAppender, committedStorage);

        List<RaftLogEntry> uncommittedLog = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 2L, PAYLOAD_A));
        FakeRaftStorage uncommittedStorage = storageWith(2L, 1L,
                uncommittedLog.get(0), uncommittedLog.get(1));
        PersistentLogAppender uncommittedAppender = recoveredAppender(uncommittedStorage);
        LogConflictResolver uncommittedResolver = new LogConflictResolver(uncommittedAppender);

        assertThrows(IllegalStateException.class, () -> uncommittedResolver.resolve(
                append(2L, 1L, 1L, 1L, payloadEntry(2L, 2L, PAYLOAD_B)), 2L, 1L));
        assertEquals(0, uncommittedStorage.replaceSuffixCalls());
        assertEquals(uncommittedLog, visibleLog(uncommittedAppender));
        assertInSync(uncommittedAppender, uncommittedStorage);
    }

    @Test
    void laterSamePayloadConflictAfterDivergenceFailsClosed() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 2L, PAYLOAD_A),
                payloadEntry(3L, 3L, PAYLOAD_A));
        FakeRaftStorage storage = storageWith(3L, 1L, followerB.get(0), followerB.get(1), followerB.get(2));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(IllegalStateException.class, () -> resolver.resolve(
                append(3L, 1L, 1L, 1L,
                        payloadEntry(2L, 3L, PAYLOAD_B), payloadEntry(3L, 3L, PAYLOAD_B)),
                3L, 1L));

        assertEquals(0, storage.replaceSuffixCalls(), "detection must abort before any storage call");
        assertEquals(followerB, visibleLog(appender));
        assertEquals(followerB, storage.storedLog());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // No-conflict paths
    // -------------------------------------------------------------------------------------------

    @Test
    void appendAtTail() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(1L, 1L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        ConflictResolution resolution = resolver.resolve(
                append(2L, 1L, 1L, 1L, entry(2L, 2L), entry(3L, 2L)), 2L, 1L);

        assertTrue(resolution.response().success());
        assertEquals(0L, resolution.truncatedFromIndex());
        assertTrue(resolution.removed().isEmpty());
        assertEquals(List.of(entry(2L, 2L), entry(3L, 2L)), resolution.appended());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 2L), entry(3L, 2L)), visibleLog(appender));
        assertEquals(1, storage.replaceSuffixCalls());
        assertEquals("replaceSuffix(2, 2)", storage.calls().get(storage.calls().size() - 1));
        assertInSync(appender, storage);
    }

    @Test
    void heartbeat() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L, entry(1L, 1L), entry(2L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver fastPathResolver = new LogConflictResolver(appender,
                (copy, prevLogIndex, prevLogTerm, entries, commitIndex) -> {
                    throw new AssertionError("a heartbeat must not take an O(n) snapshot");
                });
        List<RaftLogEntry> before = visibleLog(appender);

        ConflictResolution accepted = fastPathResolver.resolve(heartbeat(2L, 2L, 2L, 2L), 2L, 2L);

        assertTrue(accepted.response().success());
        assertEquals(2L, accepted.response().matchIndex(), "a heartbeat matches prevLogIndex");
        assertEquals(0L, accepted.truncatedFromIndex());
        assertTrue(accepted.removed().isEmpty());
        assertTrue(accepted.appended().isEmpty());
        assertEquals(before, visibleLog(appender));
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(List.of("load"), storage.calls());

        ConflictResolution rejected = fastPathResolver.resolve(heartbeat(2L, 5L, 2L, 2L), 2L, 2L);

        assertFalse(rejected.response().success());
        assertEquals(0L, rejected.response().matchIndex());
        assertEquals(before, visibleLog(appender));
        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void idempotentResend() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        AppendEntriesRequest request = append(2L, 0L, 0L, 0L, entry(1L, 2L), entry(2L, 2L));

        ConflictResolution first = resolver.resolve(request, 2L, 0L);
        ConflictResolution second = resolver.resolve(request, 2L, 2L);

        assertTrue(first.response().success());
        assertEquals(List.of(entry(1L, 2L), entry(2L, 2L)), first.appended());
        assertTrue(second.response().success());
        assertEquals(2L, second.response().matchIndex());
        assertTrue(second.appended().isEmpty(), "an idempotent re-send appends nothing");
        assertTrue(second.removed().isEmpty());
        assertEquals(0L, second.truncatedFromIndex());
        assertEquals(1, storage.replaceSuffixCalls(), "an idempotent re-send must not write again");
        assertInSync(appender, storage);
    }

    @Test
    void shorterMatchingBatchKeepsLongerTail() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 1L));
        FakeRaftStorage storage = storageWith(1L, 2L, followerB.get(0), followerB.get(1), followerB.get(2));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        ConflictResolution resolution = resolver.resolve(
                append(1L, 1L, 1L, 2L, entry(2L, 1L)), 1L, 2L);

        assertTrue(resolution.response().success());
        assertEquals(0L, resolution.truncatedFromIndex());
        assertTrue(resolution.removed().isEmpty());
        assertTrue(resolution.appended().isEmpty());
        assertEquals(followerB, visibleLog(appender), "a short matching batch must not delete the tail");
        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void divergenceRemovesEntireLocalTailEvenBeyondBatch() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(
                entry(1L, 1L), entry(2L, 1L), entry(3L, 1L), entry(4L, 1L));
        FakeRaftStorage storage = storageWith(2L, 1L,
                followerB.get(0), followerB.get(1), followerB.get(2), followerB.get(3));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        ConflictResolution resolution = resolver.resolve(
                append(2L, 1L, 1L, 1L, entry(2L, 2L)), 2L, 1L);

        assertTrue(resolution.response().success());
        assertEquals(2L, resolution.truncatedFromIndex());
        assertEquals(List.of(entry(2L, 1L), entry(3L, 1L), entry(4L, 1L)), resolution.removed(),
                "Raft truncates the whole diverging suffix, including entries beyond the batch");
        assertEquals(List.of(entry(2L, 2L)), resolution.appended());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 2L)), visibleLog(appender));
        assertEquals(visibleLog(appender), storage.storedLog());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Rejections that must not touch storage
    // -------------------------------------------------------------------------------------------

    @Test
    void prevLogMissingOrTermMismatchIsRejected() throws RaftStorageException {
        FakeRaftStorage missingStorage = storageWith(2L, 1L, entry(1L, 1L));
        PersistentLogAppender missingAppender = recoveredAppender(missingStorage);
        LogConflictResolver missingResolver = new LogConflictResolver(missingAppender);

        ConflictResolution missing = missingResolver.resolve(
                append(2L, 5L, 1L, 1L, entry(6L, 2L)), 2L, 1L);

        assertFalse(missing.response().success());
        assertEquals(0L, missing.response().matchIndex());
        assertTrue(missing.removed().isEmpty());
        assertEquals(List.of(entry(1L, 1L)), visibleLog(missingAppender));
        assertEquals(0, missingStorage.replaceSuffixCalls());
        assertInSync(missingAppender, missingStorage);

        FakeRaftStorage mismatchStorage = storageWith(2L, 1L, entry(1L, 1L), entry(2L, 1L));
        PersistentLogAppender mismatchAppender = recoveredAppender(mismatchStorage);
        LogConflictResolver mismatchResolver = new LogConflictResolver(mismatchAppender);

        ConflictResolution mismatch = mismatchResolver.resolve(
                append(2L, 2L, 2L, 1L, entry(3L, 2L)), 2L, 1L);

        assertFalse(mismatch.response().success());
        assertEquals(0L, mismatch.response().matchIndex());
        assertTrue(mismatch.removed().isEmpty());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L)), visibleLog(mismatchAppender));
        assertEquals(0, mismatchStorage.replaceSuffixCalls());
        assertInSync(mismatchAppender, mismatchStorage);
    }

    @Test
    void oldTermIsRejectedWithoutStorageCall() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(entry(1L, 1L), entry(2L, 1L));
        FakeRaftStorage storage = storageWith(3L, 2L, followerB.get(0), followerB.get(1));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        ConflictResolution resolution = resolver.resolve(
                append(1L, 2L, 1L, 2L, entry(3L, 1L)), 3L, 2L);

        assertFalse(resolution.response().success());
        assertEquals(3L, resolution.response().term(), "response term is always currentTerm");
        assertEquals(0L, resolution.response().matchIndex());
        assertEquals(0L, resolution.truncatedFromIndex());
        assertTrue(resolution.removed().isEmpty());
        assertTrue(resolution.appended().isEmpty());
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(List.of("load"), storage.calls());
        assertEquals(followerB, visibleLog(appender));
        assertInSync(appender, storage);
    }

    @Test
    void requestTermAheadOfCurrentTermIsRejected() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 1L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(
                append(3L, 1L, 1L, 1L, entry(2L, 3L)), 2L, 1L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(List.of(entry(1L, 1L)), visibleLog(appender));
        assertInSync(appender, storage);
    }

    @Test
    void entryTermAboveRequestTermIsRejected() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 1L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(
                append(2L, 1L, 1L, 1L, entry(2L, 3L)), 2L, 1L));

        assertEquals(0, storage.replaceSuffixCalls(), "the appender must reject before storage");
        assertEquals(List.of(entry(1L, 1L)), visibleLog(appender));
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Storage failures: no resolution may exist
    // -------------------------------------------------------------------------------------------

    @Test
    void storageFailureAtTruncateAndAppendCreatesNoResolution() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(
                entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        FakeRaftStorage storage = storageWith(2L, 2L,
                followerB.get(0), followerB.get(1), followerB.get(2), followerB.get(3));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        AppendEntriesRequest request = append(3L, 2L, 1L, 2L,
                entry(3L, 3L), entry(4L, 3L), entry(5L, 3L));
        RaftStorageException failure = new RaftStorageException("fake: write failed");
        ConflictResolution[] resolution = new ConflictResolution[1];
        storage.failNext(failure);

        RaftStorageException thrown = assertThrows(RaftStorageException.class,
                () -> resolution[0] = resolver.resolve(request, 3L, 2L));

        assertSame(failure, thrown);
        assertNull(resolution[0], "a failed write must create no ConflictResolution");
        assertEquals(followerB, visibleLog(appender), "the visible log must not advance before recovery");
        assertEquals(followerB, storage.storedLog(), "failNext leaves storage unchanged");
        assertEquals(1, storage.replaceSuffixCalls());

        IllegalStateException blocked = assertThrows(IllegalStateException.class,
                () -> resolver.resolve(request, 3L, 2L));
        assertTrue(blocked.getMessage().contains("recover"));
        assertEquals(1, storage.replaceSuffixCalls(), "the blocked retry must not reach storage");

        appender.recover();
        ConflictResolution retried = resolver.resolve(request, 3L, 2L);

        assertTrue(retried.response().success());
        assertEquals(3L, retried.truncatedFromIndex());
        assertEquals(List.of(entry(3L, 2L), entry(4L, 2L)), retried.removed());
        assertEquals(List.of(entry(3L, 3L), entry(4L, 3L), entry(5L, 3L)), retried.appended());
        assertEquals(2, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    /**
     * In the ambiguous case storage wrote the replacement and then reported an error. After
     * {@code recover()} the new log is durable, so the retry appends nothing. The {@code removed}
     * information of the failed attempt is lost; the caller must infer it from the recovered log.
     */
    @Test
    void ambiguousPersistThenFailureLosesRemovedInformation() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(
                entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        FakeRaftStorage storage = storageWith(2L, 2L,
                followerB.get(0), followerB.get(1), followerB.get(2), followerB.get(3));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        AppendEntriesRequest request = append(3L, 2L, 1L, 2L,
                entry(3L, 3L), entry(4L, 3L), entry(5L, 3L));
        ConflictResolution[] resolution = new ConflictResolution[1];
        storage.persistThenFailNext();

        assertThrows(RaftStorageException.class, () -> resolution[0] = resolver.resolve(request, 3L, 2L));

        assertNull(resolution[0], "an ambiguous write must create no ConflictResolution");
        assertEquals(followerB, visibleLog(appender),
                "an ambiguous write must not be published before recover()");
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L),
                entry(3L, 3L), entry(4L, 3L), entry(5L, 3L)), storage.storedLog(),
                "the fake persisted before reporting the failure");

        appender.recover();
        ConflictResolution retried = resolver.resolve(request, 3L, 2L);

        assertTrue(retried.response().success());
        assertEquals(5L, retried.response().matchIndex());
        assertTrue(retried.removed().isEmpty(), "removed information is lost after the ambiguous failure");
        assertTrue(retried.appended().isEmpty());
        assertEquals(0L, retried.truncatedFromIndex());
        assertEquals(1, storage.replaceSuffixCalls(), "the replacement is already durable and must not repeat");
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Independent commit guard, planner disagreement, post-write verification
    // -------------------------------------------------------------------------------------------

    @Test
    void plannerClaimsTruncationAtOrBelowCommitIndex() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        FakeRaftStorage storage = storageWith(2L, 2L, followerB.get(0), followerB.get(1), followerB.get(2));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender,
                (copy, prevLogIndex, prevLogTerm, entries, commitIndex) ->
                        new LogConsistencyChecker.ReconcileResult(
                                true, prevLogIndex + entries.size(), 2L, List.of(entry(2L, 3L))));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> resolver.resolve(append(3L, 2L, 1L, 2L, entry(3L, 3L)), 3L, 2L));

        assertTrue(thrown.getMessage().contains("commitIndex"));
        assertEquals(0, storage.replaceSuffixCalls(), "the commit guard must run before storage");
        assertEquals(followerB, visibleLog(appender));
        assertEquals(followerB, storage.storedLog());
        assertInSync(appender, storage);
    }

    /**
     * A planner that says "accepted" while the appender rejects (wrong prevLog) must fail closed
     * without storage. A planner that under-reports the appended entries makes the post-write check
     * fail after the durable write, so the caller still receives no resolution and must not ACK.
     */
    @Test
    void plannerDisagreesWithAppender() throws RaftStorageException {
        FakeRaftStorage rejectingStorage = storageWith(2L, 1L, entry(1L, 1L));
        PersistentLogAppender rejectingAppender = recoveredAppender(rejectingStorage);
        LogConflictResolver acceptingPlanner = new LogConflictResolver(rejectingAppender,
                (copy, prevLogIndex, prevLogTerm, entries, commitIndex) ->
                        new LogConsistencyChecker.ReconcileResult(
                                true, prevLogIndex + entries.size(), 0L, List.of(entry(6L, 2L))));

        IllegalStateException rejected = assertThrows(IllegalStateException.class,
                () -> acceptingPlanner.resolve(append(2L, 5L, 1L, 1L, entry(6L, 2L)), 2L, 1L));

        assertTrue(rejected.getMessage().contains("disagree"));
        assertEquals(0, rejectingStorage.replaceSuffixCalls());
        assertEquals(List.of(entry(1L, 1L)), visibleLog(rejectingAppender));

        FakeRaftStorage shortPlanningStorage = new FakeRaftStorage();
        PersistentLogAppender shortPlanningAppender = recoveredAppender(shortPlanningStorage);
        LogConflictResolver shortPlanner = new LogConflictResolver(shortPlanningAppender,
                (copy, prevLogIndex, prevLogTerm, entries, commitIndex) ->
                        new LogConsistencyChecker.ReconcileResult(
                                true, prevLogIndex + entries.size(), 0L, List.of(entry(1L, 2L))));

        ConflictResolution[] resolution = new ConflictResolution[1];
        assertThrows(IllegalStateException.class, () -> resolution[0] = shortPlanner.resolve(
                append(2L, 0L, 0L, 0L, entry(1L, 2L), entry(2L, 2L)), 2L, 0L));

        assertNull(resolution[0], "a failed post-write verification must create no resolution");
        assertEquals(1, shortPlanningStorage.replaceSuffixCalls(),
                "this case shows the write already happened, so the caller must not ACK");
        assertInSync(shortPlanningAppender, shortPlanningStorage);
    }

    @Test
    void postWriteVerificationFailsWhenCommittedPrefixDiffers() throws RaftStorageException {
        List<RaftLogEntry> followerB = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A), payloadEntry(2L, 2L, PAYLOAD_A));
        FakeRaftStorage storage = storageWith(2L, 1L, followerB.get(0), followerB.get(1));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender,
                (copy, prevLogIndex, prevLogTerm, entries, commitIndex) ->
                        new LogConsistencyChecker.ReconcileResult(
                                true, prevLogIndex + entries.size(), 0L,
                                List.of(payloadEntry(1L, 1L, PAYLOAD_B), entry(3L, 2L))));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> resolver.resolve(append(2L, 2L, 2L, 1L, entry(3L, 2L)), 2L, 1L));

        assertTrue(thrown.getMessage().contains("committed prefix"));
        assertEquals(1, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Longer invariant scenario
    // -------------------------------------------------------------------------------------------

    @Test
    void everySuccessfulOperationKeepsCommittedPrefixAndVisibleLogInSync() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(1L, 1L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        List<RaftLogEntry> committed = prefix(appender, 1L);
        assertInSync(appender, storage);

        assertTrue(resolver.resolve(append(2L, 1L, 1L, 1L, entry(2L, 2L)), 2L, 1L)
                .response().success());
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        assertTrue(resolver.resolve(heartbeat(2L, 2L, 2L, 2L), 2L, 2L).response().success());
        committed = prefix(appender, 2L);
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        AppendEntriesRequest divergence = append(3L, 2L, 2L, 2L, entry(3L, 3L), entry(4L, 3L));
        assertTrue(resolver.resolve(divergence, 3L, 2L).response().success());
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        assertTrue(resolver.resolve(divergence, 3L, 2L).response().success());
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        assertTrue(resolver.resolve(heartbeat(3L, 4L, 3L, 4L), 3L, 4L).response().success());
        committed = prefix(appender, 4L);
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        AppendEntriesRequest afterFailure = append(4L, 4L, 3L, 4L, entry(5L, 4L));
        storage.failNext(new RaftStorageException("fake: write failed"));
        assertThrows(RaftStorageException.class, () -> resolver.resolve(afterFailure, 4L, 4L));
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);

        appender.recover();
        assertTrue(resolver.resolve(afterFailure, 4L, 4L).response().success());
        assertCommittedPrefix(appender, committed);
        assertInSync(appender, storage);
        assertEquals(5L, appender.lastIndex());
    }

    // -------------------------------------------------------------------------------------------
    // ConflictResolution invariants
    // -------------------------------------------------------------------------------------------

    @Test
    void conflictResolutionIsImmutable() {
        ConflictResolution resolution = new ConflictResolution(
                new AppendEntriesResponse(1L, true, 2L),
                2L,
                List.of(entry(2L, 2L)),
                List.of(entry(2L, 3L)));

        assertThrows(UnsupportedOperationException.class, () -> resolution.removed().add(entry(3L, 2L)));
        assertThrows(UnsupportedOperationException.class, () -> resolution.appended().clear());
    }

    @Test
    void rejectedResolutionCannotReportRemoved() {
        assertThrows(IllegalArgumentException.class, () -> new ConflictResolution(
                new AppendEntriesResponse(1L, false, 0L),
                2L,
                List.of(entry(2L, 2L)),
                List.of()));
    }

    @Test
    void nonContiguousRemovedIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ConflictResolution(
                new AppendEntriesResponse(1L, true, 2L),
                2L,
                List.of(entry(2L, 2L), entry(4L, 2L)),
                List.of()));
    }

    // -------------------------------------------------------------------------------------------
    // Arguments
    // -------------------------------------------------------------------------------------------

    @Test
    void nullAppenderIsRejected() {
        assertThrows(NullPointerException.class, () -> new LogConflictResolver(null));
    }

    @Test
    void nullRequestIsRejected() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(NullPointerException.class, () -> resolver.resolve(null, 1L, 0L));
    }

    @Test
    void resolveBeforeRecoverIsRejected() {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);

        assertThrows(IllegalStateException.class,
                () -> resolver.resolve(append(1L, 0L, 0L, 0L, entry(1L, 1L)), 1L, 0L));

        assertEquals(List.of(), storage.calls(), "an unrecovered resolver must not touch storage");
    }
}
