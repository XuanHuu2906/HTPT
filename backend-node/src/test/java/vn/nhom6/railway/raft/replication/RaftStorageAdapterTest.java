package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RaftLogRepositoryAdapter} using {@link FakeRaftStorage} (SPEC-04 sections 4, 6
 * and 9).
 *
 * <p>These are unit tests of the adapter only. The fake storage is in memory, so this suite
 * <strong>does not</strong> prove durability on PostgreSQL, replication, commit, majority or
 * apply; {@code JdbcRaftStorage} belongs to TV2-D12. No JDBC, socket, timer, thread, clock, random
 * value, UUID or file takes part.
 *
 * <p>The central property under test is "persist first, acknowledge afterwards": when the storage
 * fails, no method returns an accepted result and the visible log is never ahead of durable state.
 */
class RaftStorageAdapterTest {

    private static final String PAYLOAD_A = "{\"requestId\":\"r-1\",\"tripId\":\"SE-001\",\"seatId\":\"G01\"}";
    private static final String PAYLOAD_B = "{\"requestId\":\"r-2\",\"tripId\":\"SE-001\",\"seatId\":\"G02\"}";

    private static RaftLogEntry entry(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static RaftLogEntry payloadEntry(long index, long term, String payload) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static RaftLogRepositoryAdapter recoveredAdapter(FakeRaftStorage storage)
            throws RaftStorageException {
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);
        adapter.recover();
        return adapter;
    }

    private static void assertInSync(RaftLogRepositoryAdapter adapter, FakeRaftStorage storage) {
        assertEquals(storage.storedLog(), adapter.entries(), "in-memory log must equal stored log");
        List<RaftLogEntry> stored = storage.storedLog();
        long expectedLastIndex = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).index();
        long expectedLastTerm = stored.isEmpty() ? 0L : stored.get(stored.size() - 1).term();
        assertEquals(expectedLastIndex, adapter.lastIndex());
        assertEquals(expectedLastTerm, adapter.lastTerm());
    }

    // -------------------------------------------------------------------------------------------
    // recover / read
    // -------------------------------------------------------------------------------------------

    @Test
    void recoverWithEmptyStorageStartsAtZeroAndReturnsEmptyState() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        RaftStorage.PersistentState state = adapter.recover();

        assertEquals(0L, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(List.of(), state.log());
        assertEquals(0L, state.lastApplied());
        assertEquals(0L, adapter.lastIndex());
        assertEquals(0L, adapter.lastTerm());
        assertEquals(Optional.empty(), adapter.read(1L));
        assertEquals(List.of(), adapter.entries());
        assertEquals(List.of("load"), storage.calls());
    }

    @Test
    void recoverRestoresPersistedLogAndExposesItForQueries() throws RaftStorageException {
        List<RaftLogEntry> persisted = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(4L, "node-1", persisted, 2L);
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        RaftStorage.PersistentState state = adapter.recover();

        assertEquals(4L, state.currentTerm());
        assertEquals("node-1", state.votedFor());
        assertEquals(persisted, state.log());
        assertEquals(2L, state.lastApplied());
        assertEquals(3L, adapter.lastIndex());
        assertEquals(2L, adapter.lastTerm());
        assertEquals(0L, adapter.termAt(0L));
        assertEquals(1L, adapter.termAt(1L));
        assertEquals(1L, adapter.termAt(2L));
        assertEquals(2L, adapter.termAt(3L));
        for (int position = 0; position < persisted.size(); position++) {
            assertEquals(Optional.of(persisted.get(position)), adapter.read(position + 1L));
        }
        assertEquals(persisted, adapter.entries());
    }

    @Test
    void queriesAndWritesBeforeRecoverFailWithIllegalState() {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        assertThrows(IllegalStateException.class, () -> adapter.read(1L));
        assertThrows(IllegalStateException.class, adapter::lastIndex);
        assertThrows(IllegalStateException.class, adapter::lastTerm);
        assertThrows(IllegalStateException.class, () -> adapter.termAt(1L));
        assertThrows(IllegalStateException.class, adapter::entries);
        assertThrows(IllegalStateException.class, () -> adapter.appendLocal(entry(1L, 1L)));
        assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(0L, 0L, List.of(entry(1L, 1L)), 0L));
        assertEquals(List.of(), storage.calls(), "the adapter must not touch storage before recover()");
    }

    @Test
    void recoverPropagatesLoadFailureAndStaysUnrecovered() {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.failNext(new RaftStorageException("fake: load failed"));
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        assertThrows(RaftStorageException.class, adapter::recover);

        assertThrows(IllegalStateException.class, adapter::lastIndex);
        assertEquals(List.of("load"), storage.calls());
    }

    @Test
    void recoverRejectsPersistedLogWithGap() {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, null, List.of(entry(1L, 1L), entry(3L, 1L)), 0L);
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        assertThrows(RaftStorageException.class, adapter::recover);

        assertThrows(IllegalStateException.class, adapter::lastIndex);
    }

    @Test
    void recoverRejectsPersistedLogWithDecreasingTerm() {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, null, List.of(entry(1L, 2L), entry(2L, 1L)), 0L);
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        assertThrows(RaftStorageException.class, adapter::recover);

        assertThrows(IllegalStateException.class, adapter::lastIndex);
    }

    @Test
    void recoverRejectsApplyCheckpointBeyondPersistedLog() {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, null, List.of(entry(1L, 1L)), 2L);
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);

        assertThrows(RaftStorageException.class, adapter::recover);

        assertThrows(IllegalStateException.class, adapter::lastIndex);
    }

    // -------------------------------------------------------------------------------------------
    // appendLocal: persist before publishing
    // -------------------------------------------------------------------------------------------

    @Test
    void appendLocalPersistsBeforePublishingEntryToInMemoryLog() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        List<Long> lastIndexSeenByStorage = new ArrayList<>();
        storage.setBeforeReplaceSuffixHook(() -> lastIndexSeenByStorage.add(adapter.lastIndex()));
        RaftLogEntry first = entry(1L, 1L);

        adapter.appendLocal(first);

        assertEquals(List.of(0L), lastIndexSeenByStorage,
                "the visible log must still be empty while storage is persisting the entry");
        assertEquals(1L, adapter.lastIndex());
        assertEquals(List.of(first), adapter.entries());
        assertEquals(List.of(first), storage.storedLog());
        assertEquals("replaceSuffix(1, 1)", storage.calls().get(storage.calls().size() - 1));
    }

    @Test
    void appendLocalRejectsGapOrDuplicateIndexWithoutTouchingStorage() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);

        assertThrows(IllegalArgumentException.class, () -> adapter.appendLocal(entry(2L, 1L)));
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(0L, adapter.lastIndex());

        adapter.appendLocal(entry(1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> adapter.appendLocal(entry(1L, 1L)));

        assertEquals(1, storage.replaceSuffixCalls(), "only the successful first append may be written");
        assertEquals(List.of(entry(1L, 1L)), adapter.entries());
    }

    // -------------------------------------------------------------------------------------------
    // appendEntries: success paths
    // -------------------------------------------------------------------------------------------

    @Test
    void appendEntriesContinuesFromPrevLogAndPersistsTheBatch() throws RaftStorageException {
        RaftLogEntry first = entry(1L, 1L);
        RaftLogEntry second = entry(2L, 1L);
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(first, second), 0L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        List<RaftLogEntry> batch = List.of(entry(3L, 2L), entry(4L, 2L));

        LogConsistencyChecker.ReconcileResult result = adapter.appendEntries(2L, 1L, batch, 2L);

        assertTrue(result.accepted());
        assertEquals(4L, result.matchIndex());
        assertEquals(0L, result.truncatedFromIndex());
        assertEquals(batch, result.appended());
        assertEquals("replaceSuffix(3, 2)", storage.calls().get(storage.calls().size() - 1));
        assertEquals(List.of(first, second, batch.get(0), batch.get(1)), adapter.entries());
        assertInSync(adapter, storage);
    }

    @Test
    void appendEntriesOverwritesOnlyUncommittedConflictingSuffix() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null,
                List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L), entry(4L, 2L)), 2L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        List<RaftLogEntry> batch = List.of(entry(3L, 3L), entry(4L, 3L));

        LogConsistencyChecker.ReconcileResult result = adapter.appendEntries(2L, 1L, batch, 2L);

        assertTrue(result.accepted());
        assertEquals(4L, result.matchIndex());
        assertEquals(3L, result.truncatedFromIndex());
        assertEquals(batch, result.appended());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L), batch.get(0), batch.get(1)),
                adapter.entries());
        assertEquals(1, storage.replaceSuffixCalls());
        assertEquals("replaceSuffix(3, 2)", storage.calls().get(storage.calls().size() - 1));
        assertInSync(adapter, storage);
    }

    @Test
    void heartbeatAndIdempotentResendAreAcceptedWithoutAnyStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        List<RaftLogEntry> persisted = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        storage.seedState(2L, null, persisted, 2L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);

        LogConsistencyChecker.ReconcileResult heartbeat = adapter.appendEntries(3L, 2L, List.of(), 3L);
        LogConsistencyChecker.ReconcileResult resend =
                adapter.appendEntries(1L, 1L, List.of(entry(2L, 1L), entry(3L, 2L)), 3L);

        assertTrue(heartbeat.accepted());
        assertEquals(3L, heartbeat.matchIndex());
        assertTrue(heartbeat.appended().isEmpty());
        assertTrue(resend.accepted());
        assertEquals(3L, resend.matchIndex());
        assertTrue(resend.appended().isEmpty());
        assertEquals(0, storage.replaceSuffixCalls(), "a heartbeat or matching resend must not write");
        assertEquals(persisted, adapter.entries());
    }

    // -------------------------------------------------------------------------------------------
    // appendEntries: rejection and fail-closed conflicts
    // -------------------------------------------------------------------------------------------

    @Test
    void prevLogMismatchIsRejectedWithoutStorageWriteOrLogChange() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        List<RaftLogEntry> persisted = List.of(entry(1L, 1L), entry(2L, 1L), entry(3L, 2L));
        storage.seedState(2L, null, persisted, 2L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);

        LogConsistencyChecker.ReconcileResult result =
                adapter.appendEntries(2L, 3L, List.of(entry(3L, 3L)), 2L);

        assertFalse(result.accepted());
        assertEquals(0L, result.matchIndex());
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(persisted, adapter.entries());
        assertInSync(adapter, storage);
    }

    @Test
    void sameIndexAndTermWithDifferentPayloadFailsClosedWithoutStorageWrite()
            throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        List<RaftLogEntry> persisted = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 1L, PAYLOAD_A),
                payloadEntry(3L, 2L, PAYLOAD_A));
        storage.seedState(2L, null, persisted, 2L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);

        assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(1L, 1L, List.of(payloadEntry(2L, 1L, PAYLOAD_B)), 2L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(persisted, adapter.entries());
        assertInSync(adapter, storage);
    }

    @Test
    void laterSameTermPayloadConflictAfterDivergenceFailsClosedWithoutStorageWrite()
            throws RaftStorageException {
        // Regression scenario fixed in D05: divergence at index 2, then a same-(index, term)
        // payload conflict at index 3. Nothing may be truncated or persisted.
        FakeRaftStorage storage = new FakeRaftStorage();
        List<RaftLogEntry> persisted = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 2L, PAYLOAD_A),
                payloadEntry(3L, 3L, PAYLOAD_A));
        storage.seedState(3L, null, persisted, 1L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        List<RaftLogEntry> batch = List.of(
                payloadEntry(2L, 3L, PAYLOAD_B),
                payloadEntry(3L, 3L, PAYLOAD_B));

        assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(1L, 1L, batch, 1L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(persisted, adapter.entries());
        assertInSync(adapter, storage);
    }

    @Test
    void conflictAtCommittedIndexFailsClosedWithoutStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        List<RaftLogEntry> persisted = List.of(
                payloadEntry(1L, 1L, PAYLOAD_A),
                payloadEntry(2L, 1L, PAYLOAD_A),
                payloadEntry(3L, 2L, PAYLOAD_A));
        storage.seedState(2L, null, persisted, 2L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);

        assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(1L, 1L,
                        List.of(payloadEntry(2L, 3L, PAYLOAD_A), payloadEntry(3L, 3L, PAYLOAD_A)), 2L));

        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(persisted, adapter.entries());
        assertInSync(adapter, storage);
    }

    // -------------------------------------------------------------------------------------------
    // save-fail: never acknowledge a failed persistence
    // -------------------------------------------------------------------------------------------

    @Test
    void appendEntriesStorageFailureThrowsAndLeavesLogAndStorageUnchanged()
            throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        storage.failNext(new RaftStorageException("fake: write failed"));

        assertThrows(RaftStorageException.class,
                () -> adapter.appendEntries(0L, 0L, List.of(entry(1L, 1L), entry(2L, 1L)), 0L));

        assertEquals(List.of(), adapter.entries());
        assertEquals(List.of(), storage.storedLog());
        assertEquals(1, storage.replaceSuffixCalls());
    }

    @Test
    void appendLocalStorageFailureThrowsAndKeepsPreviousLastIndex() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        storage.failNext(new RaftStorageException("fake: write failed"));

        assertThrows(RaftStorageException.class, () -> adapter.appendLocal(entry(1L, 1L)));

        assertEquals(0L, adapter.lastIndex());
        assertEquals(List.of(), adapter.entries());
        assertEquals(List.of(), storage.storedLog());
    }

    @Test
    void afterWriteFailureFurtherWritesRequireRecoverButReadsStillWork()
            throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        storage.failNext(new RaftStorageException("fake: write failed"));
        assertThrows(RaftStorageException.class, () -> adapter.appendLocal(entry(1L, 1L)));

        // Storage is healthy again, but the ambiguous write outcome still requires recover().
        assertThrows(IllegalStateException.class, () -> adapter.appendLocal(entry(1L, 1L)));
        assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(0L, 0L, List.of(entry(1L, 1L)), 0L));

        assertEquals(Optional.empty(), adapter.read(1L));
        assertEquals(0L, adapter.lastIndex());
        assertEquals(List.of(), adapter.entries());
    }

    @Test
    void recoverAfterFailureAllowsResendingTheSameBatch() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(entry(1L, 1L), entry(2L, 1L)), 0L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        List<RaftLogEntry> batch = List.of(entry(3L, 2L), entry(4L, 2L));
        storage.failNext(new RaftStorageException("fake: write failed"));
        assertThrows(RaftStorageException.class, () -> adapter.appendEntries(2L, 1L, batch, 2L));

        adapter.recover();
        LogConsistencyChecker.ReconcileResult result = adapter.appendEntries(2L, 1L, batch, 2L);

        assertTrue(result.accepted());
        assertEquals(List.of(entry(1L, 1L), entry(2L, 1L), batch.get(0), batch.get(1)),
                adapter.entries());
        assertInSync(adapter, storage);
    }

    @Test
    void ambiguousPersistThenFailureSynchronisesWithStorageOnRecover() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        RaftLogEntry first = entry(1L, 1L);
        storage.persistThenFailNext();

        assertThrows(RaftStorageException.class, () -> adapter.appendLocal(first));

        assertEquals(List.of(), adapter.entries(),
                "the visible log must not contain an entry whose persistence result is ambiguous");
        assertEquals(List.of(first), storage.storedLog(), "the fake did persist before reporting failure");

        adapter.recover();

        assertEquals(List.of(first), adapter.entries());
        assertEquals(1L, adapter.lastIndex());
        assertInSync(adapter, storage);
    }

    @Test
    void prolongedStorageFailureAcknowledgesNoWriteUntilRecover() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        storage.setFailing(true);

        assertThrows(RaftStorageException.class, () -> adapter.appendLocal(entry(1L, 1L)));
        assertThrows(IllegalStateException.class, () -> adapter.appendLocal(entry(1L, 1L)));
        assertEquals(0L, adapter.lastIndex());
        assertEquals(List.of(), storage.storedLog());

        storage.setFailing(false);
        assertThrows(IllegalStateException.class, () -> adapter.appendLocal(entry(1L, 1L)),
                "recover() is still required after a failed write");

        adapter.recover();
        adapter.appendLocal(entry(1L, 1L));

        assertEquals(1L, adapter.lastIndex());
        assertInSync(adapter, storage);
    }

    @Test
    void uncheckedFailureDuringAppendLocalLocksAdapterUntilRecover() throws RaftStorageException {
        PersistThenThrowStorage storage = new PersistThenThrowStorage();
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);
        adapter.recover();
        RaftLogEntry first = entry(1L, 1L);
        storage.throwAfterNextReplaceSuffix();

        IllegalStateException original =
                assertThrows(IllegalStateException.class, () -> adapter.appendLocal(first));

        assertEquals("test storage: persisted entry but threw unchecked", original.getMessage());
        assertEquals(List.of(), adapter.entries(),
                "the visible log must not publish an entry whose write result is ambiguous");
        assertEquals(List.of(first), storage.storedLog(), "the storage did persist before throwing");

        IllegalStateException blocked =
                assertThrows(IllegalStateException.class, () -> adapter.appendLocal(entry(1L, 1L)));
        assertTrue(blocked.getMessage().contains("recover"),
                "the adapter must ask for recover() after an unchecked storage failure");
        assertEquals(1, storage.replaceSuffixCalls(), "the second write must not reach storage");

        adapter.recover();
        assertEquals(storage.storedLog(), adapter.entries());
        adapter.appendLocal(entry(2L, 1L));
        assertEquals(storage.storedLog(), adapter.entries());
        assertEquals(2, storage.replaceSuffixCalls());
    }

    @Test
    void uncheckedFailureDuringAppendEntriesLocksAdapterUntilRecover() throws RaftStorageException {
        PersistThenThrowStorage storage = new PersistThenThrowStorage();
        RaftLogEntry first = entry(1L, 1L);
        storage.seedState(1L, null, List.of(first), 0L);
        RaftLogRepositoryAdapter adapter = new RaftLogRepositoryAdapter(storage);
        adapter.recover();
        List<RaftLogEntry> batch = List.of(entry(2L, 2L));
        storage.throwAfterNextReplaceSuffix();

        IllegalStateException original = assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(1L, 1L, batch, 1L));

        assertEquals("test storage: persisted entry but threw unchecked", original.getMessage());
        assertEquals(List.of(first), adapter.entries(),
                "the visible log must not publish a batch whose write result is ambiguous");
        assertEquals(List.of(first, entry(2L, 2L)), storage.storedLog(),
                "the storage did persist before throwing");

        IllegalStateException blocked = assertThrows(IllegalStateException.class,
                () -> adapter.appendEntries(1L, 1L, batch, 1L));
        assertTrue(blocked.getMessage().contains("recover"),
                "the adapter must ask for recover() after an unchecked storage failure");
        assertEquals(1, storage.replaceSuffixCalls(), "the second batch must not reach storage");

        adapter.recover();
        assertEquals(storage.storedLog(), adapter.entries());
        LogConsistencyChecker.ReconcileResult result =
                adapter.appendEntries(2L, 2L, List.of(entry(3L, 3L)), 2L);
        assertTrue(result.accepted());
        assertEquals(storage.storedLog(), adapter.entries());
        assertEquals(2, storage.replaceSuffixCalls());
    }

    // -------------------------------------------------------------------------------------------
    // Invariant over a longer successful scenario
    // -------------------------------------------------------------------------------------------

    @Test
    void everySuccessfulOperationKeepsInMemoryLogEqualToStoredLog() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, List.of(payloadEntry(1L, 1L, PAYLOAD_A)), 0L);
        RaftLogRepositoryAdapter adapter = recoveredAdapter(storage);
        assertInSync(adapter, storage);

        adapter.appendLocal(payloadEntry(2L, 1L, PAYLOAD_B));
        assertInSync(adapter, storage);

        adapter.appendEntries(2L, 1L,
                List.of(payloadEntry(3L, 2L, PAYLOAD_A), payloadEntry(4L, 2L, PAYLOAD_B)), 2L);
        assertInSync(adapter, storage);

        adapter.appendEntries(3L, 2L, List.of(payloadEntry(4L, 3L, PAYLOAD_A)), 3L);
        assertInSync(adapter, storage);

        adapter.appendEntries(4L, 3L, List.of(), 4L);
        assertInSync(adapter, storage);

        adapter.appendLocal(payloadEntry(5L, 3L, PAYLOAD_B));
        assertInSync(adapter, storage);

        assertEquals(5L, adapter.lastIndex());
        assertEquals(3L, adapter.lastTerm());
    }

    /**
     * Test-only {@link RaftStorage} wrapper that delegates to {@link FakeRaftStorage}, then throws
     * an unchecked exception to simulate an ambiguous write whose durable effect already happened.
     * It is in memory only and proves no PostgreSQL durability.
     */
    private static final class PersistThenThrowStorage implements RaftStorage {

        private final FakeRaftStorage delegate = new FakeRaftStorage();
        private boolean throwAfterNextReplaceSuffix;
        private int replaceSuffixCalls;

        void seedState(long term, String vote, List<RaftLogEntry> entries, long applied) {
            delegate.seedState(term, vote, entries, applied);
        }

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
