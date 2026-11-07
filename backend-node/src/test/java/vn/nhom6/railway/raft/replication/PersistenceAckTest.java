package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftNode.AppendOutcome;
import vn.nhom6.railway.raft.core.RaftNode.Accepted;
import vn.nhom6.railway.raft.core.RaftNode.StorageFailure;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ACK-contract tests for {@link PersistentLogAppender} using {@link FakeRaftStorage}
 * (SPEC-03 section 6, SPEC-04 sections 3, 4, 9 and 10).
 *
 * <p>These are unit tests only. The fake storage is in memory, so this suite
 * <strong>does not</strong> prove durability on PostgreSQL, replication, commit, majority,
 * state-machine apply or real {@code APPEND_ENTRIES} RPC handling. {@code JdbcRaftStorage}
 * belongs to TV2-D12; the durable PostgreSQL check is performed after that hand-over.
 *
 * <p>The central property is "persist first, acknowledge afterwards": a {@code success=true}
 * response or an {@code Accepted} outcome exists only after the storage reported success, and
 * never when storage failed. No JDBC, socket, timer, thread, clock, random value, UUID or file
 * takes part.
 */
class PersistenceAckTest {

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
                term, "node-2", prevLogIndex, prevLogTerm, List.of(entries), leaderCommit);
    }

    private static AppendEntriesRequest heartbeat(
            long term, long prevLogIndex, long prevLogTerm, long leaderCommit) {
        return append(term, prevLogIndex, prevLogTerm, leaderCommit);
    }

    private static PersistentLogAppender recoveredAppender(FakeRaftStorage storage)
            throws RaftStorageException {
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        return appender;
    }

    private static List<RaftLogEntry> visibleLog(PersistentLogAppender appender) {
        List<RaftLogEntry> visible = new ArrayList<>();
        for (long index = 1L; index <= appender.lastIndex(); index++) {
            visible.add(appender.read(index).orElseThrow());
        }
        return visible;
    }

    private static void assertInSync(PersistentLogAppender appender, FakeRaftStorage storage) {
        assertInSync(appender, storage.storedLog());
    }

    private static void assertInSync(PersistentLogAppender appender, PersistThenThrowStorage storage) {
        assertInSync(appender, storage.storedLog());
    }

    private static void assertInSync(PersistentLogAppender appender, List<RaftLogEntry> stored) {
        assertEquals(stored, visibleLog(appender), "visible log must equal the stored log");
        long expectedLastIndex = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).index();
        long expectedLastTerm = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).term();
        assertEquals(expectedLastIndex, appender.lastIndex());
        assertEquals(expectedLastTerm, appender.lastTerm());
    }

    private static FakeRaftStorage storageWith(long term, long applied, RaftLogEntry... entries) {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, List.of(entries), applied);
        return storage;
    }

    // -------------------------------------------------------------------------------------------
    // Follower: ACK success paths
    // -------------------------------------------------------------------------------------------

    @Test
    void acknowledgePersistsBatchBeforeReportingSuccess() throws RaftStorageException {
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        FakeRaftStorage storage = storageWith(2L, 0L, first, second);
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftLogEntry third = entry(3L, 2L);
        RaftLogEntry fourth = entry(4L, 2L);

        AppendEntriesResponse response =
                appender.acknowledge(append(2L, 2L, 1L, 2L, third, fourth), 2L, 2L);

        assertEquals(2L, response.term());
        assertTrue(response.success());
        assertEquals(4L, response.matchIndex());
        assertEquals(1, storage.replaceSuffixCalls());
        assertEquals("replaceSuffix(3, 2)", storage.calls().get(storage.calls().size() - 1));
        assertEquals(List.of(first, second, third, fourth), storage.storedLog());
        assertEquals(List.of(first, second, third, fourth), visibleLog(appender));
        assertEquals(4L, appender.lastIndex());
        assertEquals(2L, appender.lastTerm());
        assertInSync(appender, storage);
    }

    @Test
    void successResponseExistsOnlyAfterStorageReportsSuccess() throws RaftStorageException {
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        FakeRaftStorage storage = storageWith(2L, 0L, first, second);
        PersistentLogAppender appender = recoveredAppender(storage);
        List<String> events = new ArrayList<>();
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];

        storage.setBeforeReplaceSuffixHook(() -> {
            events.add("storage-write");
            assertNull(response[0], "no ACK may exist while storage is still writing");
            assertEquals(2L, appender.lastIndex(), "the visible log must not publish before storage");
            assertEquals(Optional.empty(), appender.read(3L));
            assertEquals(List.of(first, second), storage.storedLog());
        });

        response[0] = appender.acknowledge(
                append(2L, 2L, 1L, 2L, entry(3L, 2L), entry(4L, 2L)), 2L, 2L);
        events.add("ack-returned");

        assertEquals(List.of("storage-write", "ack-returned"), events);
        assertTrue(response[0].success());
        assertEquals(4L, response[0].matchIndex());
        assertEquals(4L, appender.lastIndex());
        assertInSync(appender, storage);
    }

    @Test
    void heartbeatIsAcceptedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L, entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);

        AppendEntriesResponse response = appender.acknowledge(heartbeat(2L, 3L, 2L, 3L), 2L, 3L);

        assertTrue(response.success());
        assertEquals(2L, response.term());
        assertEquals(3L, response.matchIndex(), "a heartbeat reports prevLogIndex");
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(List.of("load"), storage.calls());
        assertInSync(appender, storage);
    }

    @Test
    void idempotentResendIsAcceptedWithoutSecondStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftLogEntry first = entry(1L, 2L);
        RaftLogEntry second = entry(2L, 2L);
        AppendEntriesRequest request = append(2L, 0L, 0L, 0L, first, second);

        AppendEntriesResponse firstResponse = appender.acknowledge(request, 2L, 0L);
        AppendEntriesResponse secondResponse = appender.acknowledge(request, 2L, 2L);

        assertTrue(firstResponse.success());
        assertTrue(secondResponse.success());
        assertEquals(2L, secondResponse.matchIndex());
        assertEquals(1, storage.replaceSuffixCalls(), "an idempotent re-send must not write again");
        assertInSync(appender, storage);
    }

    @Test
    void uncommittedConflictTruncatesAndAppendsWithOneStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L,
                entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftLogEntry third = entry(3L, 3L);
        RaftLogEntry fourth = entry(4L, 3L);

        AppendEntriesResponse response = appender.acknowledge(
                append(3L, 2L, 1L, 2L, third, fourth), 3L, 2L);

        assertTrue(response.success());
        assertEquals(4L, response.matchIndex());
        assertEquals(1, storage.replaceSuffixCalls());
        assertEquals("replaceSuffix(3, 2)", storage.calls().get(storage.calls().size() - 1));
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L), third, fourth), storage.storedLog());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Follower: rejection paths that must not touch storage
    // -------------------------------------------------------------------------------------------

    @Test
    void oldTermIsRejectedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(3L, 2L, entry(1L, 1L), entry(2L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);

        AppendEntriesResponse response = appender.acknowledge(heartbeat(1L, 2L, 1L, 2L), 3L, 2L);

        assertFalse(response.success());
        assertEquals(3L, response.term(), "response term is always the local currentTerm");
        assertEquals(0L, response.matchIndex());
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(List.of("load"), storage.calls());
        assertInSync(appender, storage);
    }

    @Test
    void requestTermAheadOfCurrentTermIsRejectedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 0L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalArgumentException.class,
                () -> appender.acknowledge(heartbeat(3L, 1L, 1L, 1L), 2L, 0L));

        assertEquals(List.of("load"), storage.calls());
        assertInSync(appender, storage);
    }

    @Test
    void missingPrevLogIsRejectedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 0L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);

        AppendEntriesResponse response = appender.acknowledge(heartbeat(2L, 5L, 1L, 1L), 2L, 1L);

        assertFalse(response.success());
        assertEquals(0L, response.matchIndex());
        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void prevLogTermMismatchIsRejectedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(3L, 2L,
                entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);

        AppendEntriesResponse response = appender.acknowledge(
                append(3L, 2L, 3L, 2L, entry(3L, 3L)), 3L, 2L);

        assertFalse(response.success());
        assertEquals(0L, response.matchIndex());
        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void entryTermAboveRequestTermIsRejectedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 0L, entry(1L, 1L));
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalArgumentException.class,
                () -> appender.acknowledge(append(2L, 1L, 1L, 1L, entry(2L, 3L)), 2L, 1L));

        assertEquals(List.of("load"), storage.calls());
        assertInSync(appender, storage);
    }

    @Test
    void conflictAtCommittedIndexFailsClosedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L,
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 1L, PAYLOAD_A),
                payloadEntry(3L, 2L, PAYLOAD_A));
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalStateException.class, () -> appender.acknowledge(
                append(3L, 1L, 1L, 2L,
                        payloadEntry(2L, 3L, PAYLOAD_A), payloadEntry(3L, 3L, PAYLOAD_A)),
                3L, 2L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void sameIndexAndTermWithDifferentPayloadFailsClosedWithoutStorageWrite()
            throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 2L,
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 1L, PAYLOAD_A),
                payloadEntry(3L, 2L, PAYLOAD_A));
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalStateException.class, () -> appender.acknowledge(
                append(2L, 1L, 1L, 2L, payloadEntry(2L, 1L, PAYLOAD_B)), 2L, 2L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    @Test
    void laterPayloadConflictAfterDivergenceFailsClosedWithoutStorageWrite()
            throws RaftStorageException {
        FakeRaftStorage storage = storageWith(3L, 1L,
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 2L, PAYLOAD_A),
                payloadEntry(3L, 3L, PAYLOAD_A));
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalStateException.class, () -> appender.acknowledge(
                append(3L, 1L, 1L, 1L,
                        payloadEntry(2L, 3L, PAYLOAD_B), payloadEntry(3L, 3L, PAYLOAD_B)),
                3L, 1L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Follower: storage failures must never produce an ACK
    // -------------------------------------------------------------------------------------------

    @Test
    void storageFailureThrowsAndCreatesNoResponse() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftStorageException failure = new RaftStorageException("fake: write failed");
        storage.failNext(failure);
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];

        RaftStorageException thrown = assertThrows(RaftStorageException.class, () ->
                response[0] = appender.acknowledge(
                        append(1L, 0L, 0L, 0L, entry(1L, 1L)), 1L, 0L));

        assertSame(failure, thrown);
        assertNull(response[0], "a failed write must create no AppendEntriesResponse");
        assertEquals(0L, appender.lastIndex());
        assertEquals(List.of(), storage.storedLog());
        assertEquals(1, storage.replaceSuffixCalls());
    }

    @Test
    void afterStorageFailureNextWriteRequiresRecoverAndThenSucceeds() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        AppendEntriesRequest request = append(1L, 0L, 0L, 0L, first, second);
        storage.failNext(new RaftStorageException("fake: write failed"));
        assertThrows(RaftStorageException.class,
                () -> appender.acknowledge(request, 1L, 0L));

        IllegalStateException blocked = assertThrows(IllegalStateException.class,
                () -> appender.acknowledge(request, 1L, 0L));
        assertTrue(blocked.getMessage().contains("recover"));
        assertEquals(Optional.empty(), appender.read(1L), "reads stay available");
        assertEquals(0L, appender.lastIndex());
        assertEquals(1, storage.replaceSuffixCalls(), "the blocked retry must not reach storage");

        appender.recover();
        AppendEntriesResponse response = appender.acknowledge(request, 1L, 0L);

        assertTrue(response.success());
        assertEquals(2L, response.matchIndex());
        assertInSync(appender, storage);
    }

    @Test
    void uncheckedFailureAfterWritePropagatesAndLocksUntilRecover() throws RaftStorageException {
        PersistThenThrowStorage storage = new PersistThenThrowStorage();
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        RaftLogEntry first = entry(1L, 1L);
        AppendEntriesRequest request = append(1L, 0L, 0L, 0L, first);
        storage.throwAfterNextReplaceSuffix();
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> response[0] = appender.acknowledge(request, 1L, 0L));

        assertEquals("test storage: persisted entry but threw unchecked", thrown.getMessage());
        assertNull(response[0], "an unchecked failure must create no AppendEntriesResponse");
        assertEquals(0L, appender.lastIndex());
        assertEquals(List.of(first), storage.storedLog());

        assertThrows(IllegalStateException.class,
                () -> appender.acknowledge(request, 1L, 0L));
        assertEquals(1, storage.replaceSuffixCalls(), "the blocked retry must not reach storage");

        appender.recover();
        AppendEntriesResponse recovered = appender.acknowledge(request, 1L, 0L);
        assertTrue(recovered.success());
        assertEquals(1L, recovered.matchIndex());
        assertEquals(1, storage.replaceSuffixCalls(), "the persisted entry must not be written twice");
        assertInSync(appender, storage);
    }

    @Test
    void ambiguousPersistThenFailureIsSafeAfterRecover() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftLogEntry first = entry(1L, 2L);
        RaftLogEntry second = entry(2L, 2L);
        AppendEntriesRequest request = append(2L, 0L, 0L, 0L, first, second);
        storage.persistThenFailNext();
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];

        assertThrows(RaftStorageException.class,
                () -> response[0] = appender.acknowledge(request, 2L, 0L));

        assertNull(response[0]);
        assertEquals(0L, appender.lastIndex(),
                "an ambiguous write must not be published before recover()");
        assertEquals(List.of(first, second), storage.storedLog(),
                "the fake persisted before reporting failure");

        appender.recover();
        AppendEntriesResponse recovered = appender.acknowledge(request, 2L, 2L);

        assertTrue(recovered.success());
        assertEquals(2L, recovered.matchIndex());
        assertEquals(1, storage.replaceSuffixCalls(),
                "the entry is already durable; the retry must not write again");
        assertInSync(appender, storage);
    }

    @Test
    void prolongedStorageFailureAcknowledgesNothingUntilRecover() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        AppendEntriesRequest request = append(1L, 0L, 0L, 0L, entry(1L, 1L));
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];
        storage.setFailing(true);

        assertThrows(RaftStorageException.class,
                () -> response[0] = appender.acknowledge(request, 1L, 0L));
        assertNull(response[0]);
        assertThrows(IllegalStateException.class,
                () -> response[0] = appender.acknowledge(request, 1L, 0L));
        assertNull(response[0]);
        assertEquals(0L, appender.lastIndex());

        storage.setFailing(false);
        assertThrows(IllegalStateException.class,
                () -> response[0] = appender.acknowledge(request, 1L, 0L),
                "recover() is still required after a failed write");
        assertNull(response[0]);

        appender.recover();
        AppendEntriesResponse recovered = appender.acknowledge(request, 1L, 0L);
        assertTrue(recovered.success());
        assertEquals(2, storage.replaceSuffixCalls(),
                "one failed attempt, then one successful retry after recover()");
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Leader: appendLeaderCommand
    // -------------------------------------------------------------------------------------------

    @Test
    void acceptedOutcomeExistsOnlyAfterStorageReportsSuccess() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        List<String> events = new ArrayList<>();
        AppendOutcome[] outcome = new AppendOutcome[1];

        storage.setBeforeReplaceSuffixHook(() -> {
            events.add("storage-write");
            assertNull(outcome[0], "no Accepted may exist while storage is still writing");
            assertEquals(0L, appender.lastIndex());
            assertEquals(List.of(), storage.storedLog());
        });

        outcome[0] = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);
        events.add("accepted-returned");

        assertEquals(List.of("storage-write", "accepted-returned"), events);
        Accepted accepted = assertInstanceOf(Accepted.class, outcome[0]);
        assertEquals(1L, accepted.index());
        assertEquals(2L, accepted.term());
        assertEquals(1L, appender.lastIndex());
        assertEquals(List.of(payloadEntry(1L, 2L, PAYLOAD_A)), storage.storedLog());
        assertInSync(appender, storage);
    }

    @Test
    void threeConsecutiveLeaderAppendsUseIncreasingIndexes() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);

        for (long expectedIndex = 1L; expectedIndex <= 3L; expectedIndex++) {
            AppendOutcome outcome = appender.appendLeaderCommand(
                    RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);
            Accepted accepted = assertInstanceOf(Accepted.class, outcome);
            assertEquals(expectedIndex, accepted.index());
            assertEquals(2L, accepted.term());
            assertEquals(expectedIndex, appender.lastIndex());
            assertInSync(appender, storage);
        }

        assertEquals(3, storage.replaceSuffixCalls());
        assertEquals(2L, appender.lastTerm());
    }

    @Test
    void leaderStorageFailureReturnsStorageFailureAndKeepsLogUnchanged()
            throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        RaftStorageException failure = new RaftStorageException("fake: write failed");
        storage.failNext(failure);

        AppendOutcome outcome = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);

        StorageFailure storageFailure = assertInstanceOf(StorageFailure.class, outcome);
        assertSame(failure, storageFailure.cause(), "the original storage exception is reported");
        assertEquals(0L, appender.lastIndex());
        assertEquals(List.of(), storage.storedLog());
    }

    @Test
    void leaderFailureKeepsReturningStorageFailureUntilRecover() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        storage.failNext(new RaftStorageException("fake: write failed"));
        assertInstanceOf(StorageFailure.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L));

        AppendOutcome blocked = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);
        StorageFailure blockedFailure = assertInstanceOf(StorageFailure.class, blocked);
        assertTrue(blockedFailure.cause().getMessage().contains("recover"));
        assertEquals(0L, appender.lastIndex());
        assertEquals(1, storage.replaceSuffixCalls(),
                "the blocked append must not reach storage");

        appender.recover();
        Accepted accepted = assertInstanceOf(Accepted.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L));
        assertEquals(1L, accepted.index());
        assertInSync(appender, storage);
    }

    @Test
    void beforeRecoverLeaderAppendReturnsStorageFailureAndAcknowledgeThrows() {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = new PersistentLogAppender(storage);

        AppendOutcome outcome = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);
        StorageFailure failure = assertInstanceOf(StorageFailure.class, outcome);
        assertTrue(failure.cause().getMessage().contains("recover"));
        assertThrows(IllegalStateException.class,
                () -> appender.acknowledge(heartbeat(2L, 0L, 0L, 0L), 2L, 0L));
        assertEquals(List.of(), storage.calls(), "unrecovered appender must not touch storage");
    }

    @Test
    void invalidLeaderCommandIsRejectedWithoutCallingStorage() throws RaftStorageException {
        FakeRaftStorage storage = storageWith(2L, 0L, entry(1L, 2L));
        PersistentLogAppender appender = recoveredAppender(storage);
        List<String> callsAfterRecover = storage.calls();

        assertThrows(IllegalArgumentException.class, () -> appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 1L));

        assertThrows(IllegalArgumentException.class, () -> appender.appendLeaderCommand(
                "TRANSFER", PAYLOAD_A, 2L));

        assertThrows(IllegalArgumentException.class, () -> appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "   ", 2L));

        assertEquals(callsAfterRecover, storage.calls(),
                "an invalid entry must be rejected before any storage call");
        assertInSync(appender, storage);
    }

    @Test
    void uncheckedFailureDuringLeaderAppendReturnsStorageFailureAndLocksUntilRecover()
            throws RaftStorageException {
        PersistThenThrowStorage storage = new PersistThenThrowStorage();
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        storage.throwAfterNextReplaceSuffix();

        AppendOutcome outcome = appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L);

        StorageFailure failure = assertInstanceOf(StorageFailure.class, outcome);
        assertTrue(failure.cause().getMessage().contains("recover"));
        assertEquals(0L, appender.lastIndex());
        assertEquals(List.of(payloadEntry(1L, 2L, PAYLOAD_A)), storage.storedLog());

        assertInstanceOf(StorageFailure.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 2L));
        assertEquals(1, storage.replaceSuffixCalls(), "the blocked append must not reach storage");

        appender.recover();
        Accepted accepted = assertInstanceOf(Accepted.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_B, 2L));
        assertEquals(2L, accepted.index());
        assertInSync(appender, storage);
    }

    // -------------------------------------------------------------------------------------------
    // Arguments
    // -------------------------------------------------------------------------------------------

    @Test
    void nullStorageIsRejected() {
        assertThrows(NullPointerException.class, () -> new PersistentLogAppender(null));
    }

    @Test
    void nullAppendEntriesRequestIsRejected() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(NullPointerException.class, () -> appender.acknowledge(null, 1L, 0L));
    }

    @Test
    void negativeCurrentTermIsRejected() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);

        assertThrows(IllegalArgumentException.class,
                () -> appender.acknowledge(heartbeat(0L, 0L, 0L, 0L), -1L, 0L));
    }

    // -------------------------------------------------------------------------------------------
    // Invariant over a longer scenario
    // -------------------------------------------------------------------------------------------

    @Test
    void everySuccessfulOperationKeepsVisibleLogEqualToStoredLog() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        PersistentLogAppender appender = recoveredAppender(storage);
        assertInSync(appender, storage);

        Accepted first = assertInstanceOf(Accepted.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, PAYLOAD_A, 5L));
        assertEquals(1L, first.index());
        assertInSync(appender, storage);

        assertTrue(appender.acknowledge(
                append(5L, 1L, 5L, 1L, entry(2L, 5L)), 5L, 1L).success());
        assertInSync(appender, storage);

        Accepted second = assertInstanceOf(Accepted.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET, PAYLOAD_B, 6L));
        assertEquals(3L, second.index());
        assertInSync(appender, storage);

        assertTrue(appender.acknowledge(
                append(6L, 2L, 5L, 2L,
                        new RaftLogEntry(3L, 6L, RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET, PAYLOAD_B)),
                6L, 2L).success());
        assertInSync(appender, storage);

        AppendEntriesResponse conflict = appender.acknowledge(
                append(7L, 2L, 5L, 2L, entry(3L, 7L), entry(4L, 7L)), 7L, 2L);
        assertTrue(conflict.success());
        assertEquals(4L, conflict.matchIndex());
        assertInSync(appender, storage);

        Accepted third = assertInstanceOf(Accepted.class, appender.appendLeaderCommand(
                RaftLogEntry.COMMAND_TYPE_NO_OP, "{}", 7L));
        assertEquals(5L, third.index());
        assertInSync(appender, storage);

        storage.failNext(new RaftStorageException("fake: write failed"));
        assertThrows(RaftStorageException.class, () -> appender.acknowledge(
                append(7L, 5L, 7L, 5L, entry(6L, 7L)), 7L, 5L));
        assertEquals(5L, appender.lastIndex());
        assertInSync(appender, storage);

        appender.recover();
        AppendEntriesResponse retried = appender.acknowledge(
                append(7L, 5L, 7L, 5L, entry(6L, 7L)), 7L, 5L);
        assertTrue(retried.success());
        assertEquals(6L, retried.matchIndex());
        assertInSync(appender, storage);

        AppendEntriesResponse heartbeat = appender.acknowledge(heartbeat(7L, 6L, 7L, 6L), 7L, 6L);
        assertTrue(heartbeat.success());
        assertInSync(appender, storage);

        assertEquals(6L, appender.lastIndex());
        assertEquals(7L, appender.lastTerm());
    }

    /**
     * Test-only {@link RaftStorage} wrapper that delegates to {@link FakeRaftStorage} and then throws
     * an unchecked exception to simulate a write whose durable effect already happened but whose
     * result is ambiguous. In memory only; it proves no PostgreSQL durability.
     */
    private static final class PersistThenThrowStorage implements RaftStorage {

        private final FakeRaftStorage delegate = new FakeRaftStorage();
        private boolean throwAfterNextReplaceSuffix;
        private int replaceSuffixCalls;

        void throwAfterNextReplaceSuffix() {
            this.throwAfterNextReplaceSuffix = true;
        }

        int replaceSuffixCalls() {
            return replaceSuffixCalls;
        }

        List<RaftLogEntry> storedLog() {
            return delegate.storedLog();
        }

        @Override
        public PersistentState load() throws RaftStorageException {
            return delegate.load();
        }

        @Override
        public void saveTermAndVote(long currentTerm, String votedFor) throws RaftStorageException {
            delegate.saveTermAndVote(currentTerm, votedFor);
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException {
            replaceSuffixCalls++;
            delegate.replaceSuffix(fromIndex, entries);
            if (throwAfterNextReplaceSuffix) {
                throwAfterNextReplaceSuffix = false;
                throw new IllegalStateException("test storage: persisted entry but threw unchecked");
            }
        }

        @Override
        public long loadLastApplied() throws RaftStorageException {
            return delegate.loadLastApplied();
        }
    }
}
