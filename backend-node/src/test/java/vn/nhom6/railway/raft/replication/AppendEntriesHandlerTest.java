package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionTimer;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link AppendEntriesHandler} using {@link FakeRaftStorage} and a recording
 * {@code TimerHook} (SPEC-03 sections 6 and 6.1, SPEC-04 sections 3.1, 3.5, 4, 6, 9 and 10).
 *
 * <p>These are unit tests only. The storage is in memory and the timer is a recording seam, so this
 * suite <strong>does not</strong> prove durability on PostgreSQL, replication, Raft commitment,
 * majority, state-machine apply or real {@code APPEND_ENTRIES} RPC handling. {@code JdbcRaftStorage}
 * belongs to TV2-D12 and the durable PostgreSQL check is performed after that hand-over. The
 * handler is not wired into {@code RaftNode} or TCP here.
 *
 * <p>The central properties are: schema/peer rules run before any change; a higher term is
 * persisted before the log is touched; {@code RaftState} is mirrored only after the durable write
 * succeeded; and {@code commitIndex} uses {@code min(leaderCommit, matchIndex)}, never the local
 * {@code lastLogIndex}. No thread, clock, random value, UUID or file takes part.
 */
class AppendEntriesHandlerTest {

    private static final String LEADER = "node-2";
    private static final String VOTE = "node-5";

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static RaftLogEntry noOp(long index, long term) {
        return RaftLogEntry.noOp(index, term);
    }

    private static AppendEntriesRequest append(
            long term, String leaderId, long prevLogIndex, long prevLogTerm, long leaderCommit,
            RaftLogEntry... entries) {
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, List.of(entries), leaderCommit);
    }

    private static AppendEntriesRequest heartbeat(
            long term, String leaderId, long prevLogIndex, long prevLogTerm, long leaderCommit) {
        return append(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit);
    }

    private static List<RaftLogEntry> visibleLog(PersistentLogAppender appender) {
        List<RaftLogEntry> visible = new ArrayList<>();
        for (long index = RaftLogEntry.FIRST_INDEX; index <= appender.lastIndex(); index++) {
            visible.add(appender.read(index).orElseThrow());
        }
        return visible;
    }

    private static Fixture fixture(long term, String votedFor, long lastApplied, RaftLogEntry... entries)
            throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, votedFor, List.of(entries), lastApplied);
        return fixture(storage);
    }

    private static Fixture fixture(FakeRaftStorage storage) throws RaftStorageException {
        return build(storage, storage::calls, storage::storedLog, LogConflictResolver::new);
    }

    private static Fixture fixture(ReplaceFailingStorage storage) throws RaftStorageException {
        return build(storage, storage::calls, storage::storedLog, LogConflictResolver::new);
    }

    private static Fixture fixtureWithResolver(
            FakeRaftStorage storage, Function<PersistentLogAppender, LogConflictResolver> resolverFactory)
            throws RaftStorageException {
        return build(storage, storage::calls, storage::storedLog, resolverFactory);
    }

    private static Fixture build(
            RaftStorage storage,
            Supplier<List<String>> calls,
            Supplier<List<RaftLogEntry>> storedLog,
            Function<PersistentLogAppender, LogConflictResolver> resolverFactory) throws RaftStorageException {
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        PersistentElectionState persistence = new PersistentElectionState(state, storage);
        RecordingTimer timer = new RecordingTimer();
        AppendEntriesHandler handler = new AppendEntriesHandler(
                persistence, appender, resolverFactory.apply(appender), timer);
        return new Fixture(calls, storedLog, state, appender, persistence, timer, handler);
    }

    private static void assertInSync(Fixture fixture) {
        List<RaftLogEntry> visible = visibleLog(fixture.appender);
        assertEquals(fixture.storedLog(), visible, "stored log must equal the appender's visible log");
        assertEquals(fixture.state.entries(), visible, "RaftState log must equal the appender's visible log");
        assertEquals(fixture.appender.lastIndex(), fixture.state.lastLogIndex());
        assertEquals(fixture.appender.lastTerm(), fixture.state.lastLogTerm());
        assertTrue(fixture.state.commitIndex() >= 0L);
        assertTrue(fixture.state.commitIndex() <= fixture.state.lastLogIndex(),
                "commitIndex must never exceed lastLogIndex");
    }

    private static void assertStateUnchanged(
            Fixture fixture, long term, String votedFor, Role role, String leaderId,
            List<RaftLogEntry> log, long commitIndex) {
        assertEquals(term, fixture.state.currentTerm());
        assertEquals(votedFor, fixture.state.votedFor());
        assertEquals(role, fixture.state.role());
        assertEquals(leaderId, fixture.state.leaderId());
        assertEquals(log, fixture.state.entries());
        assertEquals(commitIndex, fixture.state.commitIndex());
    }

    private static final class Fixture {
        final Supplier<List<String>> callsSupplier;
        final Supplier<List<RaftLogEntry>> storedLogSupplier;
        final RaftState state;
        final PersistentLogAppender appender;
        final PersistentElectionState persistence;
        final RecordingTimer timer;
        final AppendEntriesHandler handler;

        Fixture(
                Supplier<List<String>> callsSupplier,
                Supplier<List<RaftLogEntry>> storedLogSupplier,
                RaftState state,
                PersistentLogAppender appender,
                PersistentElectionState persistence,
                RecordingTimer timer,
                AppendEntriesHandler handler) {
            this.callsSupplier = callsSupplier;
            this.storedLogSupplier = storedLogSupplier;
            this.state = state;
            this.appender = appender;
            this.persistence = persistence;
            this.timer = timer;
            this.handler = handler;
        }

        List<String> calls() {
            return callsSupplier.get();
        }

        List<RaftLogEntry> storedLog() {
            return storedLogSupplier.get();
        }
    }

    /** Records timer calls without a real scheduler. */
    private static final class RecordingTimer implements AppendEntriesHandler.TimerHook {
        final List<String> events = new ArrayList<>();

        @Override
        public void onValidAppendEntries(long requestTerm) {
            events.add("onValidAppendEntries(" + requestTerm + ")");
        }

        @Override
        public void start() {
            events.add("start");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Term handling (depends on TV5-D08)
    // -------------------------------------------------------------------------------------------

    @Test
    void staleTermIsRejected() throws RaftStorageException {
        Fixture fixture = fixture(3L, VOTE, 2L, noOp(1L, 1L), noOp(2L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(2L, LEADER, 2L, 1L, 2L), LEADER);

        assertEquals(3L, response.term());
        assertFalse(response.success());
        assertEquals(0L, response.matchIndex());
        assertStateUnchanged(fixture, 3L, VOTE, Role.FOLLOWER, null, logBefore, 2L);
        assertEquals(before, fixture.calls(), "a stale term must not touch storage");
        assertTrue(fixture.timer.events.isEmpty(), "a stale term must not reset the timer");
        assertInSync(fixture);
    }

    @Test
    void higherTermPersistsTermBeforeTouchingTheLog() throws RaftStorageException {
        Fixture fixture = fixture(3L, VOTE, 1L, noOp(1L, 1L));
        List<String> before = fixture.calls();

        AppendEntriesResponse response = fixture.handler.handle(
                append(5L, LEADER, 1L, 1L, 1L, noOp(2L, 5L), noOp(3L, 5L)), LEADER);

        assertEquals(5L, response.term());
        assertTrue(response.success());
        assertEquals(3L, response.matchIndex());
        assertEquals(5L, fixture.state.currentTerm());
        assertNull(fixture.state.votedFor());
        assertEquals(LEADER, fixture.state.leaderId());

        List<String> after = fixture.calls();
        assertTrue(after.contains("saveTermAndVote(5, null)"));
        assertTrue(after.indexOf("saveTermAndVote(5, null)") < after.indexOf("replaceSuffix(2, 2)"),
                "the higher term must be durable before the log is written");
        assertEquals(before.size() + 2, after.size(), "exactly one term write and one log write");
        assertEquals(List.of("onValidAppendEntries(5)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void equalTermFollowerKeepsVoteAndSetsLeaderHint() throws RaftStorageException {
        Fixture fixture = fixture(4L, VOTE, 0L, noOp(1L, 1L), noOp(2L, 4L));
        List<String> before = fixture.calls();

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(4L, LEADER, 2L, 4L, 0L), LEADER);

        assertTrue(response.success());
        assertEquals(2L, response.matchIndex());
        assertEquals(4L, fixture.state.currentTerm());
        assertEquals(VOTE, fixture.state.votedFor(), "an equal term must keep the existing vote");
        assertEquals(LEADER, fixture.state.leaderId());
        assertEquals(before, fixture.calls(), "no term/vote or log write is needed");
        assertEquals(List.of("onValidAppendEntries(4)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void candidateAtEqualTermStepsDownKeepingItsVote() throws RaftStorageException {
        Fixture fixture = fixture(4L, VOTE, 0L, noOp(1L, 1L));
        fixture.state.startElection(VOTE); // term 5, candidate, votedFor = node-5
        List<String> before = fixture.calls();

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(5L, LEADER, 1L, 1L, 0L), LEADER);

        assertTrue(response.success());
        assertEquals(5L, response.term());
        assertEquals(Role.FOLLOWER, fixture.state.role());
        assertEquals(VOTE, fixture.state.votedFor(), "an equal term keeps the candidate's own vote");
        assertEquals(LEADER, fixture.state.leaderId());
        assertEquals(before, fixture.calls(), "an equal term must not persist term or vote again");
        assertEquals(List.of("onValidAppendEntries(5)"), fixture.timer.events);
    }

    @Test
    void candidateAtHigherTermStepsDownAndPersistsTerm() throws RaftStorageException {
        Fixture fixture = fixture(4L, VOTE, 0L, noOp(1L, 1L));
        fixture.state.startElection(VOTE); // term 5, candidate
        List<String> before = fixture.calls();

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(6L, LEADER, 1L, 1L, 0L), LEADER);

        assertTrue(response.success());
        assertEquals(6L, response.term());
        assertEquals(Role.FOLLOWER, fixture.state.role());
        assertNull(fixture.state.votedFor());
        assertEquals(LEADER, fixture.state.leaderId());
        List<String> after = fixture.calls();
        assertTrue(after.contains("saveTermAndVote(6, null)"));
        assertEquals(before.size() + 1, after.size());
        assertEquals(List.of("onValidAppendEntries(6)"), fixture.timer.events);
    }

    @Test
    void leaderAtHigherTermStepsDownAndRestartsTimer() throws RaftStorageException {
        Fixture fixture = leaderFixture();

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(6L, LEADER, 1L, 1L, 0L), LEADER);

        assertTrue(response.success());
        assertEquals(6L, response.term());
        assertEquals(Role.FOLLOWER, fixture.state.role());
        assertNull(fixture.state.votedFor());
        assertEquals(LEADER, fixture.state.leaderId());
        assertEquals(List.of("start", "onValidAppendEntries(6)"), fixture.timer.events,
                "a step-down Leader must re-arm its timer before the request is processed");
    }

    @Test
    void leaderAtEqualTermFromAnotherLeaderFailsClosedWithoutChange() throws RaftStorageException {
        Fixture fixture = leaderFixture();
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(heartbeat(5L, "node-3", 1L, 1L, 0L), "node-3"));

        assertTrue(thrown.getMessage().contains("two leaders"));
        assertStateUnchanged(fixture, 5L, "node-1", Role.LEADER, null, logBefore, 0L);
        assertEquals(before, fixture.calls());
        assertTrue(fixture.timer.events.isEmpty());
    }

    @Test
    void termPersistFailureFailsClosed() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, null, List.of(noOp(1L, 1L)), 0L);
        Fixture fixture = fixture(storage);
        RaftStorageException failure = new RaftStorageException("fake: term write failed");
        storage.failNext(failure);
        List<String> before = fixture.calls();

        RaftStorageException thrown = assertThrows(RaftStorageException.class,
                () -> fixture.handler.handle(heartbeat(5L, LEADER, 1L, 1L, 0L), LEADER));

        assertSame(failure, thrown);
        assertEquals(List.of("close"), fixture.timer.events);
        assertEquals(before.size() + 1, fixture.calls().size());
        assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(heartbeat(5L, LEADER, 1L, 1L, 0L), LEADER),
                "a handler that failed closed must refuse every later request");
    }

    private static Fixture leaderFixture() throws RaftStorageException {
        Fixture fixture = fixture(4L, "node-1", 0L, noOp(1L, 1L));
        fixture.state.startElection("node-1"); // term 5, candidate
        fixture.state.becomeLeader(Set.of("node-2", "node-3", "node-4", "node-5"));
        return fixture;
    }

    // -------------------------------------------------------------------------------------------
    // Sheet PASS criterion: heartbeats with entries = []
    // -------------------------------------------------------------------------------------------

    @Test
    void heartbeatWithMatchingPrevLog() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L, noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L));

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(2L, LEADER, 3L, 2L, 2L), LEADER);

        assertTrue(response.success());
        assertEquals(2L, response.term());
        assertEquals(3L, response.matchIndex(), "a heartbeat reports prevLogIndex");
        assertEquals(2L, fixture.state.commitIndex(), "commit = min(leaderCommit, prevLogIndex)");
        assertEquals(0, fixture.calls().stream().filter(call -> call.startsWith("replaceSuffix(")).count(),
                "a heartbeat must not write the log");
        assertEquals(List.of("onValidAppendEntries(2)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void heartbeatCommitIsCappedAtPrevLogIndexNotLocalLastIndex() throws RaftStorageException {
        Fixture fixture = fixture(1L, null, 0L,
                noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 1L), noOp(4L, 1L), noOp(5L, 1L));

        AppendEntriesResponse response = fixture.handler.handle(
                heartbeat(1L, LEADER, 3L, 1L, 9L), LEADER);

        assertTrue(response.success());
        assertEquals(3L, response.matchIndex());
        assertEquals(3L, fixture.state.commitIndex(),
                "commit must be capped at this RPC's match index, not at local lastLogIndex 5");
        assertInSync(fixture);
    }

    @Test
    void heartbeatNeverLowersCommitIndex() throws RaftStorageException {
        Fixture fixture = fixture(1L, null, 0L, noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 1L));

        assertTrue(fixture.handler.handle(heartbeat(1L, LEADER, 3L, 1L, 3L), LEADER).success());
        assertEquals(3L, fixture.state.commitIndex());

        AppendEntriesResponse late = fixture.handler.handle(heartbeat(1L, LEADER, 3L, 1L, 1L), LEADER);

        assertTrue(late.success());
        assertEquals(3L, fixture.state.commitIndex(), "a delayed lower leaderCommit must not lower commit");
        assertEquals(List.of("onValidAppendEntries(1)", "onValidAppendEntries(1)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void heartbeatWithMissingOrMismatchedPrevLog() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 1L, noOp(1L, 1L), noOp(2L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        AppendEntriesResponse missing = fixture.handler.handle(
                heartbeat(2L, LEADER, 5L, 1L, 2L), LEADER);

        assertFalse(missing.success());
        assertEquals(0L, missing.matchIndex());
        assertEquals(1L, fixture.state.commitIndex());
        assertEquals(logBefore, fixture.state.entries());

        AppendEntriesResponse mismatched = fixture.handler.handle(
                heartbeat(2L, LEADER, 2L, 9L, 2L), LEADER);

        assertFalse(mismatched.success());
        assertEquals(0L, mismatched.matchIndex());
        assertEquals(1L, fixture.state.commitIndex());
        assertEquals(logBefore, fixture.state.entries());
        assertEquals(before, fixture.calls(), "a rejected heartbeat must not write storage");
        assertEquals(List.of("onValidAppendEntries(2)", "onValidAppendEntries(2)"), fixture.timer.events,
                "SPEC-04 section 3.1: a valid request resets the timer even on log mismatch");
        assertInSync(fixture);
    }

    // -------------------------------------------------------------------------------------------
    // Sheet PASS criterion: appending entries without duplicates
    // -------------------------------------------------------------------------------------------

    @Test
    void appendToEmptyLog() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L);
        List<RaftLogEntry> batch = List.of(noOp(1L, 2L), noOp(2L, 2L), noOp(3L, 2L));

        AppendEntriesResponse response = fixture.handler.handle(
                append(2L, LEADER, 0L, 0L, 2L, batch.get(0), batch.get(1), batch.get(2)), LEADER);

        assertTrue(response.success());
        assertEquals(3L, response.matchIndex());
        assertEquals(batch, fixture.state.entries());
        assertEquals(batch, visibleLog(fixture.appender));
        assertEquals(batch, fixture.storedLog());
        assertEquals(2L, fixture.state.commitIndex(), "commit = min(leaderCommit, matchIndex)");
        assertEquals(1L, fixture.calls().stream().filter(call -> call.equals("replaceSuffix(1, 3)")).count());
        assertEquals(List.of("onValidAppendEntries(2)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void resendingTheSameBatchDoesNotDuplicateEntries() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L);
        AppendEntriesRequest request = append(2L, LEADER, 0L, 0L, 0L, noOp(1L, 2L), noOp(2L, 2L));

        assertTrue(fixture.handler.handle(request, LEADER).success());
        List<RaftLogEntry> afterFirst = fixture.state.entries();
        int writesAfterFirst = replaceSuffixCalls(fixture);

        AppendEntriesResponse second = fixture.handler.handle(request, LEADER);

        assertTrue(second.success());
        assertEquals(2L, second.matchIndex());
        assertEquals(afterFirst, fixture.state.entries());
        assertEquals(afterFirst, visibleLog(fixture.appender));
        assertEquals(afterFirst, fixture.storedLog());
        assertEquals(writesAfterFirst, replaceSuffixCalls(fixture),
                "an idempotent re-send must not write the log again");
        assertInSync(fixture);
    }

    @Test
    void overlappingBatchWritesOnlyTheNewEntries() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 0L);

        assertTrue(fixture.handler.handle(
                append(3L, LEADER, 0L, 0L, 0L, noOp(1L, 3L), noOp(2L, 3L)), LEADER).success());
        assertTrue(fixture.handler.handle(
                append(3L, LEADER, 2L, 3L, 0L, noOp(3L, 3L), noOp(4L, 3L)), LEADER).success());

        List<String> calls = fixture.calls();
        assertEquals("replaceSuffix(3, 2)", calls.get(calls.size() - 1),
                "only the entries that were missing may be written");
        assertEquals(4L, fixture.state.lastLogIndex());
        assertEquals(4L, fixture.storedLog().size());
        assertEquals(2, replaceSuffixCalls(fixture));
        assertInSync(fixture);
    }

    @Test
    void delayedOlderRpcDoesNotShrinkLogOrCommit() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L);
        assertTrue(fixture.handler.handle(
                append(2L, LEADER, 0L, 0L, 3L, noOp(1L, 2L), noOp(2L, 2L), noOp(3L, 2L)),
                LEADER).success());
        List<RaftLogEntry> fullLog = fixture.state.entries();
        assertEquals(3L, fixture.state.commitIndex());
        int writes = replaceSuffixCalls(fixture);

        AppendEntriesResponse delayed = fixture.handler.handle(
                append(2L, LEADER, 1L, 2L, 1L, noOp(2L, 2L)), LEADER);

        assertTrue(delayed.success());
        assertEquals(2L, delayed.matchIndex());
        assertEquals(fullLog, fixture.state.entries(), "a shorter matching batch must keep the tail");
        assertEquals(3L, fixture.state.commitIndex(), "a delayed RPC must not lower commitIndex");
        assertEquals(writes, replaceSuffixCalls(fixture), "a shorter matching batch must not write");
        assertInSync(fixture);
    }

    @Test
    void uncommittedConflictIsReplacedAndMirrored() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 2L,
                noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L), noOp(4L, 2L));

        AppendEntriesResponse response = fixture.handler.handle(
                append(3L, LEADER, 2L, 1L, 2L, noOp(3L, 3L), noOp(4L, 3L), noOp(5L, 3L)), LEADER);

        assertTrue(response.success());
        assertEquals(5L, response.matchIndex());
        List<RaftLogEntry> expected = List.of(
                noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 3L), noOp(4L, 3L), noOp(5L, 3L));
        assertEquals(expected, fixture.state.entries(), "RaftState must mirror truncation and append");
        assertEquals(expected, visibleLog(fixture.appender));
        assertEquals(expected, fixture.storedLog());
        assertEquals(2L, fixture.state.commitIndex());
        List<String> calls = fixture.calls();
        assertTrue(calls.contains("replaceSuffix(3, 3)"));
        assertInSync(fixture);
    }

    @Test
    void prevLogMismatchWithEntriesIsRejectedWithoutWrite() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 1L, noOp(1L, 1L), noOp(2L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        AppendEntriesResponse missing = fixture.handler.handle(
                append(3L, LEADER, 5L, 1L, 1L, noOp(6L, 3L)), LEADER);
        AppendEntriesResponse mismatched = fixture.handler.handle(
                append(3L, LEADER, 2L, 2L, 1L, noOp(3L, 3L)), LEADER);

        assertFalse(missing.success());
        assertEquals(0L, missing.matchIndex());
        assertFalse(mismatched.success());
        assertEquals(0L, mismatched.matchIndex());
        assertEquals(logBefore, fixture.state.entries());
        assertEquals(1L, fixture.state.commitIndex());
        assertEquals(before, fixture.calls(), "a log mismatch must not write storage");
        assertEquals(List.of("onValidAppendEntries(3)", "onValidAppendEntries(3)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void successResponseExistsOnlyAfterStorageWrite() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(), 0L);
        Fixture fixture = fixture(storage);
        List<String> events = new ArrayList<>();
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];

        storage.setBeforeReplaceSuffixHook(() -> {
            events.add("storage-write");
            assertNull(response[0], "no response may exist while storage is still writing");
            assertEquals(0L, fixture.state.lastLogIndex(), "RaftState must not mirror before storage");
            assertEquals(0L, fixture.appender.lastIndex(), "the appender must not publish before storage");
            assertEquals(List.of(), fixture.storedLog());
        });

        response[0] = fixture.handler.handle(
                append(2L, LEADER, 0L, 0L, 0L, noOp(1L, 2L), noOp(2L, 2L)), LEADER);
        events.add("handler-returned");

        assertEquals(List.of("storage-write", "handler-returned"), events);
        assertTrue(response[0].success());
        assertEquals(2L, response[0].matchIndex());
        assertInSync(fixture);
    }

    // -------------------------------------------------------------------------------------------
    // Schema and peer rejections must not change anything
    // -------------------------------------------------------------------------------------------

    @Test
    void invalidSenderIsRejectedBeforeAnyChange() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 1L, noOp(1L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        List<Executable> invalidRequests = List.of(
                () -> fixture.handler.handle(heartbeat(9L, "node-3", 1L, 1L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, "node-9", 1L, 1L, 1L), "node-9"),
                () -> fixture.handler.handle(heartbeat(9L, null, 1L, 1L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, "   ", 1L, 1L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, LEADER, 1L, 1L, 1L), "   "));

        for (Executable invalid : invalidRequests) {
            assertThrows(IllegalArgumentException.class, invalid);
        }

        assertStateUnchanged(fixture, 3L, null, Role.FOLLOWER, null, logBefore, 1L);
        assertEquals(before, fixture.calls(), "a schema/peer error must not touch storage");
        assertTrue(fixture.timer.events.isEmpty(), "a schema/peer error must not reset the timer");
    }

    @Test
    void malformedChainIsRejectedBeforeAnyChange() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 1L, noOp(1L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        List<Executable> malformed = List.of(
                () -> fixture.handler.handle(
                        append(9L, LEADER, 1L, 1L, 1L, noOp(3L, 9L)), LEADER),
                () -> fixture.handler.handle(
                        append(9L, LEADER, 0L, 0L, 1L, noOp(1L, 9L), noOp(3L, 9L)), LEADER),
                () -> fixture.handler.handle(
                        append(9L, LEADER, 0L, 0L, 1L, noOp(1L, 5L), noOp(2L, 4L)), LEADER),
                () -> fixture.handler.handle(
                        append(9L, LEADER, 0L, 0L, 1L, noOp(1L, 10L)), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, LEADER, 0L, 2L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(-1L, LEADER, 0L, 0L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, LEADER, -1L, 0L, 1L), LEADER),
                () -> fixture.handler.handle(heartbeat(9L, LEADER, 0L, 0L, -1L), LEADER));

        for (Executable invalid : malformed) {
            assertThrows(IllegalArgumentException.class, invalid);
        }

        assertStateUnchanged(fixture, 3L, null, Role.FOLLOWER, null, logBefore, 1L);
        assertEquals(before, fixture.calls());
        assertTrue(fixture.timer.events.isEmpty());

        assertTrue(fixture.handler.handle(heartbeat(3L, LEADER, 1L, 1L, 1L), LEADER).success(),
                "a schema rejection must not poison the handler");
    }

    // -------------------------------------------------------------------------------------------
    // Wire DTO mapping (depends on TV3-D03) and canonical command
    // -------------------------------------------------------------------------------------------

    @Test
    void dtoHeartbeatIsMappedToTheCoreContract() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 1L, noOp(1L, 1L));

        AppendEntriesResponse response = fixture.handler.handle(
                AppendEntriesRequestDto.createHeartbeat(2L, LEADER, 1L, 1L, 1L), LEADER);

        assertTrue(response.success());
        assertEquals(1L, response.matchIndex());
        assertEquals(LEADER, fixture.state.leaderId());
        assertEquals(1L, fixture.state.commitIndex());
        assertEquals(List.of("onValidAppendEntries(2)"), fixture.timer.events);
        assertInSync(fixture);
    }

    @Test
    void dtoWithEntriesMapsTheCanonicalCommandPayload() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L);
        Map<String, Object> command = new LinkedHashMap<>();
        command.put("seatId", "G05");
        command.put("requestId", "r-1");
        command.put("passengerName", "Khach demo");
        command.put("ticketId", "t-1");
        command.put("tripId", "SE-001");
        command.put("issuedAt", "2026-10-29T10:00:00.000Z");
        AppendEntriesRequestDto dto = new AppendEntriesRequestDto(
                2L, LEADER, 0L, 0L,
                List.of(new AppendEntriesRequestDto.LogEntryDto(1L, 2L, "BOOK_TICKET", command)),
                0L);

        AppendEntriesResponse response = fixture.handler.handle(dto, LEADER);

        assertTrue(response.success());
        RaftLogEntry mapped = fixture.state.entries().get(0);
        assertEquals(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, mapped.commandType());
        assertEquals("{\"issuedAt\":\"2026-10-29T10:00:00.000Z\","
                + "\"passengerName\":\"Khach demo\",\"requestId\":\"r-1\","
                + "\"seatId\":\"G05\",\"ticketId\":\"t-1\",\"tripId\":\"SE-001\"}",
                mapped.commandPayload(), "the command must be compact JSON with ordered keys");
        assertEquals(mapped, fixture.storedLog().get(0));
        assertInSync(fixture);
    }

    @Test
    void dtoValidationFailureIsRejectedBeforeAnyChange() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 1L, noOp(1L, 1L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        AppendEntriesRequestDto blankLeader = new AppendEntriesRequestDto(
                9L, "   ", 1L, 1L, List.of(), 1L);
        AppendEntriesRequestDto negativeTerm = new AppendEntriesRequestDto(
                -1L, LEADER, 1L, 1L, List.of(), 1L);

        assertThrows(IllegalArgumentException.class, () -> fixture.handler.handle(blankLeader, LEADER));
        assertThrows(IllegalArgumentException.class, () -> fixture.handler.handle(negativeTerm, LEADER));

        assertStateUnchanged(fixture, 3L, null, Role.FOLLOWER, null, logBefore, 1L);
        assertEquals(before, fixture.calls());
        assertTrue(fixture.timer.events.isEmpty());
    }

    @Test
    void commandKeyOrderDoesNotCreateAnIdempotencyConflict() throws RaftStorageException {
        Fixture fixture = fixture(2L, null, 0L);

        Map<String, Object> firstOrder = new LinkedHashMap<>();
        firstOrder.put("requestId", "r-1");
        firstOrder.put("seatId", "G01");
        firstOrder.put("ticketId", "t-1");
        Map<String, Object> secondOrder = new LinkedHashMap<>();
        secondOrder.put("ticketId", "t-1");
        secondOrder.put("seatId", "G01");
        secondOrder.put("requestId", "r-1");

        AppendEntriesRequestDto first = new AppendEntriesRequestDto(
                2L, LEADER, 0L, 0L,
                List.of(new AppendEntriesRequestDto.LogEntryDto(1L, 2L, "BOOK_TICKET", firstOrder)),
                0L);
        AppendEntriesRequestDto second = new AppendEntriesRequestDto(
                2L, LEADER, 0L, 0L,
                List.of(new AppendEntriesRequestDto.LogEntryDto(1L, 2L, "BOOK_TICKET", secondOrder)),
                0L);

        assertTrue(fixture.handler.handle(first, LEADER).success());
        int writes = replaceSuffixCalls(fixture);
        AppendEntriesResponse response = fixture.handler.handle(second, LEADER);

        assertTrue(response.success());
        assertEquals(1L, response.matchIndex());
        assertEquals(writes, replaceSuffixCalls(fixture),
                "the same command in another key order must be an idempotent re-send");
        assertEquals(1, fixture.state.entries().size());
        assertInSync(fixture);
    }

    // -------------------------------------------------------------------------------------------
    // Storage failures and fail closed
    // -------------------------------------------------------------------------------------------

    @Test
    void replaceSuffixFailureAfterTermPersistFailsClosed() throws RaftStorageException {
        ReplaceFailingStorage storage = new ReplaceFailingStorage();
        storage.seedState(2L, null, List.of(noOp(1L, 1L)), 0L);
        Fixture fixture = fixture(storage);
        List<String> before = storage.calls();
        RaftStorageException failure = new RaftStorageException("fake: replace failed");
        storage.failNextReplaceSuffix(failure);

        RaftStorageException thrown = assertThrows(RaftStorageException.class,
                () -> fixture.handler.handle(
                        append(3L, LEADER, 1L, 1L, 1L, noOp(2L, 3L), noOp(3L, 3L)), LEADER));

        assertSame(failure, thrown);
        List<String> after = storage.calls();
        assertEquals(before.size() + 1, after.size());
        assertEquals("saveTermAndVote(3, null)", after.get(after.size() - 1),
                "the higher term must be persisted before the failing log write");
        assertEquals(1, storage.replaceAttempts());
        assertEquals(3L, fixture.state.currentTerm());
        assertEquals(List.of(noOp(1L, 1L)), fixture.state.entries(),
                "RaftState must not mirror a failed durable write");
        assertEquals(List.of("close"), fixture.timer.events);
        assertInSync(fixture);
        assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(heartbeat(3L, LEADER, 1L, 1L, 0L), LEADER));

        assertEquals(1, storage.replaceAttempts(), "a failed handler must not reach storage again");
    }

    @Test
    void ambiguousPersistThenFailureIsSafeAfterRebuild() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(), 0L);
        Fixture fixture = fixture(storage);
        AppendEntriesRequest request = append(2L, LEADER, 0L, 0L, 0L, noOp(1L, 2L), noOp(2L, 2L));
        AppendEntriesResponse[] response = new AppendEntriesResponse[1];
        storage.persistThenFailNext();

        assertThrows(RaftStorageException.class,
                () -> response[0] = fixture.handler.handle(request, LEADER));

        assertNull(response[0], "an ambiguous write must create no response");
        assertEquals(List.of(), fixture.state.entries(), "RaftState must not mirror before recovery");
        assertEquals(List.of(), visibleLog(fixture.appender), "the appender must not publish before recovery");
        assertEquals(List.of(noOp(1L, 2L), noOp(2L, 2L)), fixture.storedLog(),
                "the fake persisted before reporting the failure");
        assertThrows(IllegalStateException.class, () -> fixture.handler.handle(request, LEADER));

        Fixture rebuilt = fixture(storage);
        int writesBeforeRetry = replaceSuffixCalls(rebuilt);
        AppendEntriesResponse retried = rebuilt.handler.handle(request, LEADER);

        assertTrue(retried.success());
        assertEquals(2L, retried.matchIndex());
        assertEquals(writesBeforeRetry, replaceSuffixCalls(rebuilt),
                "the replacement is already durable and must not be written twice");
        assertEquals(List.of(noOp(1L, 2L), noOp(2L, 2L)), rebuilt.state.entries());
        assertInSync(rebuilt);
    }

    @Test
    void committedConflictFailsClosed() throws RaftStorageException {
        Fixture fixture = fixture(3L, null, 2L, noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L));
        List<String> before = fixture.calls();
        List<RaftLogEntry> logBefore = fixture.state.entries();

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(
                        append(3L, LEADER, 1L, 1L, 1L, noOp(2L, 3L), noOp(3L, 3L)), LEADER));

        assertTrue(thrown.getMessage().contains("committed"));
        assertEquals(before, fixture.calls(), "a committed conflict must abort before storage");
        assertEquals(logBefore, fixture.state.entries());
        assertEquals(2L, fixture.state.commitIndex());
        assertEquals(List.of("close"), fixture.timer.events);
        assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(heartbeat(3L, LEADER, 3L, 2L, 2L), LEADER));
    }

    @Test
    void resolverInvariantFailurePoisonsHandler() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(), 0L);
        Fixture fixture = fixtureWithResolver(storage, appender -> new LogConflictResolver(
                appender, (copy, prevLogIndex, prevLogTerm, entries, commitIndex) -> {
                    throw new IllegalStateException("test: planner exploded");
                }));
        List<String> before = fixture.calls();

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(
                        append(2L, LEADER, 0L, 0L, 0L, noOp(1L, 2L)), LEADER));

        assertTrue(thrown.getMessage().contains("planner exploded"));
        assertEquals(before, fixture.calls(), "a planner failure must not write storage");
        assertEquals(List.of(), fixture.state.entries());
        assertEquals(List.of("close"), fixture.timer.events);
        assertThrows(IllegalStateException.class,
                () -> fixture.handler.handle(heartbeat(2L, LEADER, 0L, 0L, 0L), LEADER));
    }

    // -------------------------------------------------------------------------------------------
    // Construction and arguments
    // -------------------------------------------------------------------------------------------

    @Test
    void constructorRejectsDifferentLogs() throws RaftStorageException {
        FakeRaftStorage stateStorage = new FakeRaftStorage();
        stateStorage.seedState(2L, null, List.of(noOp(1L, 1L), noOp(2L, 1L)), 1L);
        FakeRaftStorage appenderStorage = new FakeRaftStorage();
        appenderStorage.seedState(2L, null, List.of(noOp(1L, 1L)), 0L);

        RaftStorage.PersistentState persisted = stateStorage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(appenderStorage);
        appender.recover();
        PersistentElectionState persistence = new PersistentElectionState(state, stateStorage);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new AppendEntriesHandler(
                        persistence, appender, new LogConflictResolver(appender), new RecordingTimer()));

        assertTrue(thrown.getMessage().contains("state and appender logs differ"));
    }

    @Test
    void constructorRejectsNullArguments() throws RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, List.of(), 0L);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        PersistentElectionState persistence = new PersistentElectionState(state, storage);
        LogConflictResolver resolver = new LogConflictResolver(appender);
        RecordingTimer timer = new RecordingTimer();

        assertThrows(NullPointerException.class,
                () -> new AppendEntriesHandler(null, appender, resolver, timer));
        assertThrows(NullPointerException.class,
                () -> new AppendEntriesHandler(persistence, null, resolver, timer));
        assertThrows(NullPointerException.class,
                () -> new AppendEntriesHandler(persistence, appender, null, timer));
        assertThrows(NullPointerException.class,
                () -> new AppendEntriesHandler(persistence, appender, resolver, null));
        assertThrows(NullPointerException.class,
                () -> new AppendEntriesHandler(persistence, appender, (ElectionTimer) null));
    }

    // -------------------------------------------------------------------------------------------
    // Longer invariant scenario
    // -------------------------------------------------------------------------------------------

    @Test
    void everySuccessfulOperationKeepsBothLogsInSyncAndCommitMonotonic() throws RaftStorageException {
        Fixture fixture = fixture(1L, null, 0L);
        long term = 1L;
        long commit = 0L;

        AppendEntriesResponse first = fixture.handler.handle(heartbeat(1L, LEADER, 0L, 0L, 0L), LEADER);
        assertTrue(first.success());
        assertEquals(0L, first.matchIndex());
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        AppendEntriesResponse append = fixture.handler.handle(
                append(2L, LEADER, 0L, 0L, 0L, noOp(1L, 2L), noOp(2L, 2L)), LEADER);
        assertTrue(append.success());
        assertEquals(2L, append.matchIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertTrue(fixture.state.commitIndex() >= commit);
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(heartbeat(2L, LEADER, 2L, 2L, 2L), LEADER).success());
        assertEquals(2L, fixture.state.commitIndex());
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(
                append(2L, LEADER, 0L, 0L, 2L, noOp(1L, 2L), noOp(2L, 2L)), LEADER).success());
        assertEquals(2L, fixture.state.entries().size());
        assertEquals(2L, fixture.state.commitIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(
                append(3L, LEADER, 2L, 2L, 3L, noOp(3L, 3L), noOp(4L, 3L)), LEADER).success());
        assertEquals(3L, fixture.state.commitIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(
                append(4L, LEADER, 3L, 3L, 3L, noOp(4L, 4L), noOp(5L, 4L)), LEADER).success());
        List<RaftLogEntry> afterConflict = List.of(
                noOp(1L, 2L), noOp(2L, 2L), noOp(3L, 3L), noOp(4L, 4L), noOp(5L, 4L));
        assertEquals(afterConflict, fixture.state.entries());
        assertEquals(3L, fixture.state.commitIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(heartbeat(4L, LEADER, 5L, 4L, 5L), LEADER).success());
        assertEquals(5L, fixture.state.commitIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        AppendEntriesResponse stale = fixture.handler.handle(
                heartbeat(3L, LEADER, 5L, 4L, 5L), LEADER);
        assertFalse(stale.success());
        assertEquals(4L, fixture.state.currentTerm(), "a stale request must not change the term");
        assertEquals(5L, fixture.state.commitIndex());
        assertInSync(fixture);
        term = fixture.state.currentTerm();
        commit = fixture.state.commitIndex();

        assertTrue(fixture.handler.handle(heartbeat(4L, LEADER, 5L, 4L, 4L), LEADER).success());
        assertEquals(commit, fixture.state.commitIndex(), "commitIndex must never decrease");
        assertEquals(5L, fixture.state.commitIndex());
        assertTrue(fixture.state.currentTerm() >= term);
        assertInSync(fixture);
    }

    private static int replaceSuffixCalls(Fixture fixture) {
        int count = 0;
        for (String call : fixture.calls()) {
            if (call.startsWith("replaceSuffix(")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Test-only storage wrapper that allows the next {@code replaceSuffix} to fail while still
     * letting a higher-term {@code saveTermAndVote} succeed first. It delegates every operation to
     * an in-memory {@link FakeRaftStorage}; it proves no PostgreSQL durability.
     */
    private static final class ReplaceFailingStorage implements RaftStorage {
        private final FakeRaftStorage delegate = new FakeRaftStorage();
        private RaftStorageException nextReplaceFailure;
        private int replaceAttempts;

        void seedState(long term, String votedFor, List<RaftLogEntry> log, long lastApplied) {
            delegate.seedState(term, votedFor, log, lastApplied);
        }

        void failNextReplaceSuffix(RaftStorageException failure) {
            this.nextReplaceFailure = failure;
        }

        int replaceAttempts() {
            return replaceAttempts;
        }

        List<String> calls() {
            return delegate.calls();
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
            replaceAttempts++;
            if (nextReplaceFailure != null) {
                RaftStorageException failure = nextReplaceFailure;
                nextReplaceFailure = null;
                throw failure;
            }
            delegate.replaceSuffix(fromIndex, entries);
        }

        @Override
        public long loadLastApplied() throws RaftStorageException {
            return delegate.loadLastApplied();
        }
    }

    // ===========================================================================================
    // TV4-D10: multi-step "pure logic" network scenarios
    // ===========================================================================================

    /**
     * Ten multi-step scenarios for the Follower side of {@code APPEND_ENTRIES}, driven by a
     * scripted Leader over an in-test mock network.
     *
     * <p><strong>Honesty boundary.</strong> This nested suite uses a mock network, a scripted
     * Leader, three or more real Follower components over {@link FakeRaftStorage} and a recording
     * timer. It does <strong>not</strong> use TCP, a real Leader, majority counting, cluster-wide
     * commit, {@code RaftNode}, PostgreSQL/JDBC or the state machine. {@code leaderCommit} is a
     * scenario input, never a value computed from {@code matchIndex}. It proves only the pure logic
     * of the D09 handler: a matching batch is accepted, a mismatching one is rejected with
     * {@code success=false}, and {@code commitIndex} never exceeds the last log entry. Durable
     * PostgreSQL behaviour belongs to TV2-D12; the scripted Leader is a test tool, not a second Raft
     * implementation.
     */
    @Nested
    class NetworkScenarioTests {

        // ---------------------------------------------------------------------------------------
        // S1: happy replication to three followers
        // ---------------------------------------------------------------------------------------

        @Test
        void happyReplicationConvergesAcrossThreeFollowers() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = List.of(
                    noOp(1L, 2L), bookTicket(2L, 2L, "r-1"), noOp(3L, 2L));
            ScriptedLeader leader = new ScriptedLeader("node-1", 2L, leaderLog);
            List<ScenarioNode> followers = List.of(
                    ScenarioNode.empty("node-2"),
                    ScenarioNode.empty("node-3"),
                    ScenarioNode.empty("node-4"));
            MockNetwork network = new MockNetwork();

            for (ScenarioNode follower : followers) {
                AppendEntriesResponse response = leader.sendHeartbeat(network, follower, 0L, 0L).orElseThrow();
                assertTrue(response.success());
                assertEquals(0L, response.matchIndex());
                assertEquals(leaderLog.subList(0, 0), follower.state.entries());
                assertNodeInvariants(follower, 0L);
            }

            for (ScenarioNode follower : followers) {
                AppendEntriesResponse response = leader.sendRange(network, follower, 1L, 3L, 0L).orElseThrow();
                assertTrue(response.success());
                assertEquals(3L, response.matchIndex());
                assertEquals(leaderLog, follower.state.entries());
                assertEquals(leaderLog, visibleLog(follower.appender));
                assertEquals(leaderLog, follower.storage.storedLog());
                assertNodeInvariants(follower, 0L);
            }

            for (ScenarioNode follower : followers) {
                AppendEntriesResponse response = leader.sendHeartbeat(network, follower, 3L, 3L).orElseThrow();
                assertTrue(response.success());
                assertEquals(3L, response.matchIndex());
                assertEquals(3L, follower.state.commitIndex());
                assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
                assertEquals(leaderLog, follower.state.entries());
                assertEquals(leaderLog, visibleLog(follower.appender));
                assertEquals(leaderLog, follower.storage.storedLog());
                assertEquals(3L, timerValidCalls(follower),
                        "every accepted heartbeat or batch must reset the timer exactly once");
                assertNodeInvariants(follower, 3L);
            }
        }

        // ---------------------------------------------------------------------------------------
        // S2: lagging follower backs nextIndex off until the logs match
        // ---------------------------------------------------------------------------------------

        @Test
        void laggingFollowerCatchesUpByBackingOffNextIndex() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = List.of(
                    noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L),
                    noOp(4L, 2L), noOp(5L, 3L), noOp(6L, 3L));
            ScriptedLeader leader = new ScriptedLeader("node-2", 3L, leaderLog);
            ScenarioNode follower = ScenarioNode.seeded("node-3", 1L, null,
                    List.of(noOp(1L, 1L), noOp(2L, 1L)), 0L);
            MockNetwork network = new MockNetwork();
            List<RaftLogEntry> initialLog = follower.state.entries();

            for (int attempt = 0; attempt < 4; attempt++) {
                int writesBefore = follower.storage.replaceSuffixCalls();
                AppendEntriesResponse response = leader.send(network, follower, 0L).orElseThrow();

                assertFalse(response.success());
                assertEquals(0L, response.matchIndex());
                assertEquals(initialLog, follower.state.entries(), "a rejected batch must not change the log");
                assertEquals(writesBefore, follower.storage.replaceSuffixCalls(),
                        "a rejected batch must not write storage");
                assertEquals(attempt + 1L, timerValidCalls(follower),
                        "a valid request must reset the timer even when the log does not match");
                assertNodeInvariants(follower, 0L);
            }

            AppendEntriesResponse accepted = leader.send(network, follower, 0L).orElseThrow();

            assertTrue(accepted.success());
            assertEquals(6L, accepted.matchIndex());
            assertEquals(List.of(7L, 6L, 5L, 4L, 3L), leader.attemptedNextIndex,
                    "the leader must try nextIndex 7, 6, 5, 4 then 3");
            assertEquals(leaderLog, follower.state.entries());
            assertEquals(5L, timerValidCalls(follower));
            assertNodeInvariants(follower, 0L);
        }

        // ---------------------------------------------------------------------------------------
        // S3: only the uncommitted divergent suffix is replaced
        // ---------------------------------------------------------------------------------------

        @Test
        void divergentFollowerUncommittedSuffixIsReplacedAndCommittedPrefixIsKept()
                throws RaftStorageException {
            List<RaftLogEntry> followerLog = List.of(
                    noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L), noOp(4L, 2L));
            List<RaftLogEntry> leaderLog = List.of(
                    noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 3L), noOp(4L, 3L), noOp(5L, 3L));
            ScenarioNode follower = ScenarioNode.seeded("node-4", 2L, null, followerLog, 2L);
            ScriptedLeader leader = new ScriptedLeader("node-1", 3L, leaderLog);
            MockNetwork network = new MockNetwork();
            List<RaftLogEntry> committedPrefix = List.of(followerLog.get(0), followerLog.get(1));
            int writesBefore = follower.storage.replaceSuffixCalls();

            AppendEntriesResponse response = null;
            for (int attempt = 0; attempt < 4; attempt++) {
                response = leader.send(network, follower, 3L).orElseThrow();
            }

            assertTrue(response.success());
            assertEquals(5L, response.matchIndex());
            assertEquals(leaderLog, follower.state.entries());
            assertEquals(committedPrefix, follower.state.entries().subList(0, 2),
                    "the committed prefix [1, 2] must stay unchanged");
            assertEquals(writesBefore + 1, follower.storage.replaceSuffixCalls(),
                    "exactly one replaceSuffix starting at index 3 is expected");
            assertEquals(1, countReplaceSuffixFrom(follower, 3L));
            assertEquals(3L, follower.state.commitIndex(), "commit = max(2, min(3, 5))");
            assertNodeInvariants(follower, 3L);
        }

        // ---------------------------------------------------------------------------------------
        // S4: duplicated RPCs must be idempotent
        // ---------------------------------------------------------------------------------------

        @Test
        void networkDuplicatesDoNotDuplicateEntries() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = List.of(noOp(1L, 2L), noOp(2L, 2L));
            ScriptedLeader leader = new ScriptedLeader("node-1", 2L, leaderLog);
            ScenarioNode follower = ScenarioNode.seeded("node-2", 2L, null, List.of(), 0L);
            MockNetwork network = new MockNetwork();

            AppendEntriesRequest heartbeat = leader.heartbeatRequest(0L, 0L);
            List<Optional<AppendEntriesResponse>> heartbeatResponses =
                    network.deliverTwice(follower, heartbeat);
            AppendEntriesResponse firstHeartbeat = heartbeatResponses.get(0).orElseThrow();
            AppendEntriesResponse secondHeartbeat = heartbeatResponses.get(1).orElseThrow();
            assertTrue(firstHeartbeat.success());
            assertTrue(secondHeartbeat.success());
            assertEquals(firstHeartbeat.matchIndex(), secondHeartbeat.matchIndex());

            AppendEntriesRequest batch = leader.requestRange(1L, 2L, 0L);
            int writesBefore = follower.storage.replaceSuffixCalls();
            List<Optional<AppendEntriesResponse>> batchResponses =
                    network.deliverTwice(follower, batch);
            AppendEntriesResponse firstBatch = batchResponses.get(0).orElseThrow();
            AppendEntriesResponse secondBatch = batchResponses.get(1).orElseThrow();

            assertEquals(2L, firstBatch.matchIndex());
            assertEquals(2L, secondBatch.matchIndex(), "a duplicate must report the same matchIndex");
            assertEquals(2, follower.state.entries().size());
            assertEquals(2, visibleLog(follower.appender).size());
            assertEquals(2, follower.storage.storedLog().size());
            assertEquals(writesBefore + 1, follower.storage.replaceSuffixCalls(),
                    "the duplicate batch must not write the same entries twice");
            assertNodeInvariants(follower, 0L);
        }

        // ---------------------------------------------------------------------------------------
        // S5: a reordered, stale RPC must not shrink the log or lower commitIndex
        // ---------------------------------------------------------------------------------------

        @Test
        void reorderedStaleRpcDoesNotShrinkLogOrLowerCommit() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = List.of(
                    noOp(1L, 2L), noOp(2L, 2L), noOp(3L, 2L), noOp(4L, 2L), noOp(5L, 2L));
            ScriptedLeader leader = new ScriptedLeader("node-1", 2L, leaderLog);
            ScenarioNode follower = ScenarioNode.seeded("node-2", 2L, null, List.of(), 0L);
            MockNetwork network = new MockNetwork();

            AppendEntriesRequest oldRpc = leader.requestRange(1L, 3L, 1L);
            AppendEntriesRequest newRpc = leader.requestRange(1L, 5L, 4L);
            network.deliverLater(follower, oldRpc);
            network.deliverLater(follower, newRpc);

            List<Optional<AppendEntriesResponse>> responses = network.flushLaterReversed();
            AppendEntriesResponse newer = responses.get(0).orElseThrow();
            assertTrue(newer.success());
            assertEquals(5L, newer.matchIndex());
            assertEquals(4L, follower.state.commitIndex());
            int writesAfterNewer = follower.storage.replaceSuffixCalls();

            AppendEntriesResponse stale = responses.get(1).orElseThrow();
            assertTrue(stale.success());
            assertEquals(3L, stale.matchIndex());
            assertEquals(5, follower.state.entries().size(), "a stale shorter batch must keep the tail");
            assertEquals(leaderLog, follower.state.entries());
            assertEquals(4L, follower.state.commitIndex(), "a stale RPC must not lower commitIndex");
            assertEquals(writesAfterNewer, follower.storage.replaceSuffixCalls(),
                    "a matching stale prefix must not write storage");
            assertNodeInvariants(follower, 1L);
        }

        // ---------------------------------------------------------------------------------------
        // S6: dropped request and dropped response are recovered by a retry
        // ---------------------------------------------------------------------------------------

        @Test
        void droppedRequestsAndResponsesAreRecoveredByRetry() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = List.of(
                    noOp(1L, 2L), noOp(2L, 2L), noOp(3L, 2L), noOp(4L, 2L));
            ScriptedLeader leader = new ScriptedLeader("node-1", 2L, leaderLog);
            ScenarioNode follower = ScenarioNode.seeded("node-2", 2L, null, List.of(), 0L);
            MockNetwork network = new MockNetwork();

            AppendEntriesRequest first = leader.requestRange(1L, 2L, 0L);
            NodeSnapshot before = follower.snapshot();
            network.dropRequest(follower, first);
            assertEquals(before, follower.snapshot(),
                    "a dropped request must not reach the handler or touch storage/timer");
            assertTrue(network.deliver(follower, first).orElseThrow().success());
            assertEquals(2L, follower.state.lastLogIndex());

            AppendEntriesRequest second = leader.requestRange(3L, 4L, 0L);
            int writesBefore = follower.storage.replaceSuffixCalls();
            network.dropResponse(follower, second);
            assertEquals(4L, follower.state.lastLogIndex(),
                    "the follower must have durably stored the batch before the response was lost");
            assertEquals(writesBefore + 1, follower.storage.replaceSuffixCalls());

            AppendEntriesResponse retried = network.deliver(follower, second).orElseThrow();
            assertTrue(retried.success());
            assertEquals(4L, retried.matchIndex());
            assertEquals(writesBefore + 1, follower.storage.replaceSuffixCalls(),
                    "the retry after a lost response must not write the batch twice");
            assertEquals(4, follower.state.entries().size());
            assertEquals(leaderLog, follower.state.entries());
            assertEquals(leaderLog, follower.storage.storedLog());
            assertNodeInvariants(follower, 0L);
        }

        // ---------------------------------------------------------------------------------------
        // S7: an old Leader is rejected after a new Leader term
        // ---------------------------------------------------------------------------------------

        @Test
        void staleLeaderIsRejectedAfterNewLeaderTerm() throws RaftStorageException {
            List<RaftLogEntry> oldLeaderLog = List.of(noOp(1L, 2L), noOp(2L, 2L));
            ScriptedLeader oldLeader = new ScriptedLeader("node-1", 2L, oldLeaderLog);
            ScriptedLeader newLeader = new ScriptedLeader("node-3", 3L, List.of(noOp(1L, 2L)));
            ScenarioNode follower = ScenarioNode.seeded("node-2", 0L, null, List.of(), 0L);
            MockNetwork network = new MockNetwork();

            assertTrue(oldLeader.sendRange(network, follower, 1L, 1L, 0L).orElseThrow().success());
            assertTrue(newLeader.sendHeartbeat(network, follower, 1L, 0L).orElseThrow().success());
            assertEquals(3L, follower.state.currentTerm());
            assertTrue(follower.storage.calls().contains("saveTermAndVote(3, null)"),
                    "the new term must be durable after the new Leader is accepted");

            List<RaftLogEntry> logBefore = follower.state.entries();
            int callsBefore = follower.storage.calls().size();
            int timerBefore = follower.timer.events.size();

            AppendEntriesResponse stale = oldLeader.send(network, follower, 0L).orElseThrow();

            assertFalse(stale.success());
            assertEquals(3L, stale.term());
            assertEquals(0L, stale.matchIndex());
            assertTrue(oldLeader.steppedDown, "the old Leader must step down after seeing a higher term");
            assertEquals(logBefore, follower.state.entries(), "a stale term must not change the log");
            assertEquals(callsBefore, follower.storage.calls().size(),
                    "a stale term must not touch storage");
            assertEquals(timerBefore, follower.timer.events.size(),
                    "a stale term must not reset the election timer");
            assertEquals(3L, follower.state.currentTerm());
            assertNodeInvariants(follower, 0L);
        }

        // ---------------------------------------------------------------------------------------
        // S8: commitIndex never exceeds the last log entry
        // ---------------------------------------------------------------------------------------

        @ParameterizedTest(name = "logLength={0}, prevLogIndex={1}, leaderCommit={2}, entries={3}")
        @CsvSource({
                "0, 0, 0, false",
                "0, 0, 1, false",
                "1, 0, 0, false",
                "1, 1, 1, false",
                "1, 1, 5, false",
                "3, 0, 0, false",
                "3, 2, 3, false",
                "3, 3, 3, false",
                "3, 3, 8, false",
                "3, 3, MAX, false",
                "0, 0, 1, true",
                "1, 1, 2, true",
                "3, 3, 4, true",
                "3, 3, MAX, true"
        })
        void commitIndexNeverExceedsLastLogEntry(
                int logLength, long prevLogIndex, String leaderCommitText, boolean withEntries)
                throws RaftStorageException {
            long leaderCommit = "MAX".equals(leaderCommitText)
                    ? Long.MAX_VALUE : Long.parseLong(leaderCommitText);
            List<RaftLogEntry> localLog = new ArrayList<>();
            for (long index = 1L; index <= logLength; index++) {
                localLog.add(noOp(index, 1L));
            }
            ScenarioNode follower = ScenarioNode.seeded("node-2", 1L, null, localLog, 0L);
            MockNetwork network = new MockNetwork();
            long commitBefore = follower.state.commitIndex();
            long prevLogTerm = prevLogIndex == 0L ? 0L : follower.state.termAt(prevLogIndex);

            List<RaftLogEntry> entries = new ArrayList<>();
            if (withEntries) {
                entries.add(noOp(prevLogIndex + 1L, 2L));
                entries.add(noOp(prevLogIndex + 2L, 2L));
            }
            AppendEntriesRequest request = new AppendEntriesRequest(
                    2L, "node-1", prevLogIndex, prevLogTerm, entries, leaderCommit);

            AppendEntriesResponse response = network.deliver(follower, request).orElseThrow();
            long matchIndex = Math.addExact(prevLogIndex, entries.size());
            long expectedCommit = Math.max(commitBefore, Math.min(leaderCommit, matchIndex));

            assertTrue(response.success());
            assertEquals(matchIndex, response.matchIndex());
            assertEquals(expectedCommit, follower.state.commitIndex());
            assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex(),
                    "commitIndex must never exceed lastLogIndex");
            assertTrue(follower.state.commitIndex() >= commitBefore, "commitIndex must never decrease");
            assertNodeInvariants(follower, leaderCommit);
        }

        @Test
        void commitIndexAfterConflictTruncationUsesThisRpcMatchIndex() throws RaftStorageException {
            List<RaftLogEntry> localLog = List.of(
                    noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L), noOp(4L, 2L));
            ScenarioNode follower = ScenarioNode.seeded("node-2", 2L, null, localLog, 2L);
            MockNetwork network = new MockNetwork();
            int writesBefore = follower.storage.replaceSuffixCalls();
            AppendEntriesRequest request = new AppendEntriesRequest(
                    3L, "node-1", 2L, 1L,
                    List.of(noOp(3L, 3L), noOp(4L, 3L), noOp(5L, 3L)), 4L);

            AppendEntriesResponse response = network.deliver(follower, request).orElseThrow();

            assertTrue(response.success());
            assertEquals(5L, response.matchIndex());
            assertEquals(writesBefore + 1, follower.storage.replaceSuffixCalls());
            assertEquals(1, countReplaceSuffixFrom(follower, 3L));
            assertEquals(4L, follower.state.commitIndex(), "commit = max(2, min(4, 5))");
            assertEquals(5L, follower.state.lastLogIndex());
            assertTrue(follower.state.commitIndex() <= follower.state.lastLogIndex());
            assertNodeInvariants(follower, 4L);
        }

        // ---------------------------------------------------------------------------------------
        // S9: an ACK implies durability; storage failures are never ACKed
        // ---------------------------------------------------------------------------------------

        @Test
        void ackImpliesDurableAndStorageFailuresAreNeverAcked() throws RaftStorageException {
            List<RaftLogEntry> leaderLog = new ArrayList<>();
            for (long index = 1L; index <= 12L; index++) {
                leaderLog.add(bookTicket(index, 2L, "r-" + index));
            }
            ScriptedLeader leader = new ScriptedLeader("node-1", 2L, leaderLog);
            ScenarioNode follower = ScenarioNode.seeded("node-2", 2L, null, List.of(), 0L);
            MockNetwork network = new MockNetwork();

            assertTrue(leader.sendRange(network, follower, 1L, 2L, 0L).orElseThrow().success());
            assertTrue(leader.sendRange(network, follower, 3L, 4L, 0L).orElseThrow().success());
            assertEquals(4L, follower.state.lastLogIndex());

            // RPC 3: the durable write fails before changing anything, so no response may exist.
            AppendEntriesRequest third = leader.requestRange(5L, 6L, 0L);
            follower.storage.failNext(new RaftStorageException("fake: RPC 3 write failed"));
            int errorsBefore = network.errors.size();
            int timerBefore = follower.timer.events.size();

            assertTrue(network.deliver(follower, third).isEmpty(),
                    "a failed durable write must not produce a response");
            assertEquals(errorsBefore + 1, network.errors.size());
            assertEquals(timerBefore + 1, follower.timer.events.size());
            assertEquals("close", follower.timer.events.get(follower.timer.events.size() - 1));
            assertEquals(4L, follower.state.lastLogIndex());
            assertEquals(4, follower.storage.storedLog().size());
            assertEquals(4, visibleLog(follower.appender).size());

            follower.restart();
            assertEquals(4L, follower.state.lastLogIndex());
            assertTrue(network.deliver(follower, third).orElseThrow().success());
            assertEquals(6L, follower.state.lastLogIndex());

            // RPC 4: a normal write after the first recovery.
            assertTrue(leader.sendRange(network, follower, 7L, 8L, 0L).orElseThrow().success());

            // RPC 5: storage persisted and then reported a failure, so the outcome is ambiguous.
            AppendEntriesRequest fifth = leader.requestRange(9L, 10L, 0L);
            follower.storage.persistThenFailNext();
            errorsBefore = network.errors.size();
            timerBefore = follower.timer.events.size();

            assertTrue(network.deliver(follower, fifth).isEmpty(),
                    "an ambiguous durable write must not produce a response");
            assertEquals(errorsBefore + 1, network.errors.size());
            assertEquals(timerBefore + 1, follower.timer.events.size());
            assertEquals("close", follower.timer.events.get(follower.timer.events.size() - 1));
            assertEquals(8, follower.state.entries().size(),
                    "the appender must not publish a write whose result was an error");
            assertEquals(10, follower.storage.storedLog().size(),
                    "the fake storage did persist the ambiguous write");

            int writesBeforeRetry = follower.storage.replaceSuffixCalls();
            follower.restart();
            assertTrue(network.deliver(follower, fifth).orElseThrow().success());
            assertEquals(writesBeforeRetry, follower.storage.replaceSuffixCalls(),
                    "a retry of an already durable replacement must not write it twice");
            assertEquals(10, follower.state.entries().size());
            assertEquals(10, follower.storage.storedLog().size());

            // RPC 6: a normal write; the whole prefix must converge to the Leader log.
            assertTrue(leader.sendRange(network, follower, 11L, 12L, 0L).orElseThrow().success());
            assertEquals(12, follower.state.entries().size());
            assertEquals(leaderLog, follower.state.entries());
            assertEquals(leaderLog, follower.storage.storedLog());
            assertNodeInvariants(follower, 0L);
        }

        // ---------------------------------------------------------------------------------------
        // S10: restart resumes from durable state without replay
        // ---------------------------------------------------------------------------------------

        @Test
        void restartResumesFromDurableStateWithoutReplay() throws RaftStorageException {
            List<RaftLogEntry> durableLog = List.of(
                    noOp(1L, 1L), noOp(2L, 1L), noOp(3L, 2L), noOp(4L, 2L));
            ScenarioNode follower = ScenarioNode.seeded("node-2", 3L, "node-2", durableLog, 2L);
            MockNetwork network = new MockNetwork();

            assertEquals(2L, follower.state.commitIndex());
            assertNodeInvariants(follower, 0L);

            follower.restart();

            assertEquals(2L, follower.state.commitIndex(),
                    "commitIndex must be reconstructed from lastApplied, not from lastLogIndex");
            assertEquals(3L, follower.state.currentTerm());
            assertEquals("node-2", follower.state.votedFor());
            assertEquals(4L, follower.state.lastLogIndex());
            assertNodeInvariants(follower, 0L);

            ScriptedLeader leader = new ScriptedLeader("node-2", 3L, durableLog);
            assertTrue(leader.sendHeartbeat(network, follower, 4L, 3L).orElseThrow().success());
            assertEquals(3L, follower.state.commitIndex());

            AppendEntriesRequest resend = leader.requestRange(1L, 4L, 3L);
            int writesBefore = follower.storage.replaceSuffixCalls();
            AppendEntriesResponse replayed = network.deliver(follower, resend).orElseThrow();
            assertTrue(replayed.success());
            assertEquals(4L, replayed.matchIndex());
            assertEquals(writesBefore, follower.storage.replaceSuffixCalls(),
                    "an idempotent batch after restart must not be written again");
            assertEquals(4, follower.state.entries().size());

            AppendEntriesRequest committedConflict = new AppendEntriesRequest(
                    3L, "node-2", 1L, 1L, List.of(noOp(2L, 3L)), 3L);
            List<String> callsBefore = follower.storage.calls();
            int errorsBefore = network.errors.size();
            int timerBefore = follower.timer.events.size();

            assertTrue(network.deliver(follower, committedConflict).isEmpty(),
                    "a conflict at a committed index must fail closed");
            assertEquals(errorsBefore + 1, network.errors.size());
            assertTrue(network.errors.get(network.errors.size() - 1) instanceof IllegalStateException);
            assertEquals(callsBefore, follower.storage.calls(),
                    "a committed conflict must abort before reaching storage");
            assertEquals(durableLog, follower.state.entries());
            assertEquals(3L, follower.state.commitIndex());
            assertEquals(timerBefore + 1, follower.timer.events.size());
            assertEquals("close", follower.timer.events.get(follower.timer.events.size() - 1));
            assertThrows(IllegalStateException.class,
                    () -> follower.handler.handle(committedConflict, "node-2"),
                    "a handler that failed closed must refuse every later request");
        }

        // ---------------------------------------------------------------------------------------
        // Test helpers: fixture entries, timer helpers and invariant checks
        // ---------------------------------------------------------------------------------------

        private static RaftLogEntry bookTicket(long index, long term, String requestId) {
            return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                    "{\"requestId\":\"" + requestId + "\",\"seatId\":\"G01\",\"ticketId\":\"t-"
                            + index + "\",\"tripId\":\"SE-001\"}");
        }

        private static long timerValidCalls(ScenarioNode node) {
            return node.timer.events.stream()
                    .filter(event -> event.startsWith("onValidAppendEntries("))
                    .count();
        }

        private static int countReplaceSuffixFrom(ScenarioNode node, long fromIndex) {
            String expected = "replaceSuffix(" + fromIndex + ", ";
            int count = 0;
            for (String call : node.storage.calls()) {
                if (call.startsWith(expected)) {
                    count++;
                }
            }
            return count;
        }

        /** Full cross-layer invariant; only valid when storage, appender and state are in sync. */
        private static void assertNodeInvariants(ScenarioNode node, long leaderCommitSeen) {
            List<RaftLogEntry> visible = visibleLog(node.appender);
            assertEquals(visible, node.state.entries(),
                    "RaftState log must equal the appender's visible log");
            assertEquals(node.storage.storedLog(), visible,
                    "storage must hold exactly the appender's visible log");
            assertEquals(node.appender.lastIndex(), node.state.lastLogIndex());
            assertEquals(node.appender.lastTerm(), node.state.lastLogTerm());
            assertStableInvariants(node, leaderCommitSeen);
        }

        /**
         * Invariants that hold even in the ambiguous window after a storage error, when storage may
         * be ahead of the unpublished appender log (S9).
         */
        private static void assertStableInvariants(ScenarioNode node, long leaderCommitSeen) {
            long commit = node.state.commitIndex();
            assertTrue(commit <= node.state.lastLogIndex(), "commitIndex must never exceed lastLogIndex");
            assertTrue(node.state.lastApplied() <= commit, "lastApplied must not exceed commitIndex");
            assertTrue(commit >= node.lastCheckedCommit, "commitIndex must never decrease");
            assertTrue(commit <= Math.max(node.lastCheckedCommit, leaderCommitSeen),
                    "commitIndex must never exceed max(previous commit, leaderCommit)");
            assertTrue(node.state.currentTerm() >= node.lastCheckedTerm, "term must never decrease");
            assertEquals(Role.FOLLOWER, node.state.role());
            node.lastCheckedCommit = commit;
            node.lastCheckedTerm = node.state.currentTerm();
        }

        // ---------------------------------------------------------------------------------------
        // One Follower: real state, real appender, real handler, fake storage and timer
        // ---------------------------------------------------------------------------------------

        private static final class ScenarioNode {
            final String id;
            final FakeRaftStorage storage;
            RaftState state;
            PersistentLogAppender appender;
            PersistentElectionState persistence;
            RecordingTimer timer;
            AppendEntriesHandler handler;
            long lastCheckedCommit;
            long lastCheckedTerm;

            private ScenarioNode(String id, FakeRaftStorage storage) throws RaftStorageException {
                this.id = id;
                this.storage = storage;
                open();
            }

            static ScenarioNode empty(String id) throws RaftStorageException {
                return seeded(id, 0L, null, List.of(), 0L);
            }

            static ScenarioNode seeded(
                    String id, long term, String votedFor, List<RaftLogEntry> log, long lastApplied)
                    throws RaftStorageException {
                FakeRaftStorage storage = new FakeRaftStorage();
                storage.seedState(term, votedFor, log, lastApplied);
                return new ScenarioNode(id, storage);
            }

            /** Rebuilds state, appender, election persistence, timer and handler from storage. */
            void restart() throws RaftStorageException {
                open();
            }

            private void open() throws RaftStorageException {
                RaftStorage.PersistentState persisted = storage.load();
                this.state = RaftState.restore(
                        persisted.currentTerm(), persisted.votedFor(),
                        persisted.log(), persisted.lastApplied());
                this.appender = new PersistentLogAppender(storage);
                this.appender.recover();
                this.persistence = new PersistentElectionState(state, storage);
                this.timer = new RecordingTimer();
                this.handler = new AppendEntriesHandler(
                        persistence, appender, new LogConflictResolver(appender), timer);
                this.lastCheckedCommit = state.commitIndex();
                this.lastCheckedTerm = state.currentTerm();
            }

            NodeSnapshot snapshot() {
                return new NodeSnapshot(
                        state.currentTerm(), state.votedFor(), state.role(), state.leaderId(),
                        state.entries(), state.commitIndex(), timer.events.size(),
                        storage.calls(), storage.replaceSuffixCalls());
            }
        }

        private record NodeSnapshot(
                long term,
                String votedFor,
                Role role,
                String leaderId,
                List<RaftLogEntry> log,
                long commitIndex,
                int timerCalls,
                List<String> storageCalls,
                int replaceSuffixCalls) {
        }

        // ---------------------------------------------------------------------------------------
        // Mock network: ordering, drops, duplication and the ACK durability audit
        // ---------------------------------------------------------------------------------------

        private static final class MockNetwork {
            final List<String> events = new ArrayList<>();
            final List<Exception> errors = new ArrayList<>();
            private final List<PendingDelivery> pending = new ArrayList<>();

            Optional<AppendEntriesResponse> deliver(ScenarioNode node, AppendEntriesRequest request) {
                events.add("deliver " + describe(request));
                return invoke(node, request);
            }

            List<Optional<AppendEntriesResponse>> deliverTwice(
                    ScenarioNode node, AppendEntriesRequest request) {
                List<Optional<AppendEntriesResponse>> responses = new ArrayList<>(2);
                responses.add(deliver(node, request));
                responses.add(deliver(node, request));
                return responses;
            }

            /** The handler never sees the request, like a packet lost before the Node. */
            void dropRequest(ScenarioNode node, AppendEntriesRequest request) {
                events.add("drop-request " + describe(request));
                assertNodeInvariants(node, request.leaderCommit());
            }

            /** The handler processes the request but the response is lost on the way back. */
            void dropResponse(ScenarioNode node, AppendEntriesRequest request) {
                events.add("drop-response " + describe(request));
                invoke(node, request);
            }

            void deliverLater(ScenarioNode node, AppendEntriesRequest request) {
                pending.add(new PendingDelivery(node, request));
                events.add("queued " + describe(request));
            }

            List<Optional<AppendEntriesResponse>> flushLaterReversed() {
                List<Optional<AppendEntriesResponse>> responses = new ArrayList<>(pending.size());
                for (int position = pending.size() - 1; position >= 0; position--) {
                    PendingDelivery delivery = pending.get(position);
                    responses.add(deliver(delivery.node(), delivery.request()));
                }
                pending.clear();
                return responses;
            }

            private Optional<AppendEntriesResponse> invoke(
                    ScenarioNode node, AppendEntriesRequest request) {
                AppendEntriesResponse response = null;
                try {
                    response = node.handler.handle(request, request.leaderId());
                } catch (RaftStorageException failure) {
                    // Fail closed: no response exists and the Node must recover from storage.
                    errors.add(failure);
                    events.add("storage-failure " + failure.getMessage());
                } catch (RuntimeException failure) {
                    errors.add(failure);
                    events.add("failure " + failure.getClass().getSimpleName()
                            + ": " + failure.getMessage());
                }

                if (response == null) {
                    assertStableInvariants(node, request.leaderCommit());
                    return Optional.empty();
                }
                auditDurability(node, response);
                assertNodeInvariants(node, request.leaderCommit());
                events.add("response " + describe(response));
                return Optional.of(response);
            }

            /** SPEC-04 section 9: a visible {@code success=true} implies the entry is durable. */
            private static void auditDurability(
                    ScenarioNode node, AppendEntriesResponse response) {
                if (!response.success()) {
                    return;
                }
                List<RaftLogEntry> stored = node.storage.storedLog();
                assertTrue(stored.size() >= response.matchIndex(),
                        "an ACK for matchIndex " + response.matchIndex()
                                + " requires storage to hold that index");
                if (response.matchIndex() > 0L) {
                    assertEquals(response.matchIndex(),
                            stored.get((int) Math.toIntExact(response.matchIndex() - 1L)).index(),
                            "the ACKed index must exist in durable storage");
                }
                assertTrue(node.appender.lastIndex() >= response.matchIndex(),
                        "the appender must not lag behind an ACKed matchIndex");
            }

            private static String describe(AppendEntriesRequest request) {
                return "term=" + request.term() + " leader=" + request.leaderId()
                        + " prev=(" + request.prevLogIndex() + "," + request.prevLogTerm() + ")"
                        + " entries=" + request.entries().size()
                        + " leaderCommit=" + request.leaderCommit();
            }

            private static String describe(AppendEntriesResponse response) {
                return "term=" + response.term() + " success=" + response.success()
                        + " match=" + response.matchIndex();
            }

            private record PendingDelivery(ScenarioNode node, AppendEntriesRequest request) {
            }
        }

        // ---------------------------------------------------------------------------------------
        // Scripted Leader: a test driver, not a Raft Leader implementation
        // ---------------------------------------------------------------------------------------

        private static final class ScriptedLeader {
            final String id;
            final long term;
            final List<RaftLogEntry> log;
            final Map<String, Long> nextIndexByPeer = new LinkedHashMap<>();
            final List<Long> attemptedNextIndex = new ArrayList<>();
            boolean steppedDown;

            ScriptedLeader(String id, long term, List<RaftLogEntry> log) {
                this.id = id;
                this.term = term;
                this.log = List.copyOf(log);
            }

            long nextIndexOf(String peer) {
                return nextIndexByPeer.getOrDefault(
                        peer, Math.addExact((long) log.size(), 1L));
            }

            long termAt(long index) {
                if (index == 0L) {
                    return 0L;
                }
                if (index < 0L || index > log.size()) {
                    throw new IllegalArgumentException(
                            "no term at index " + index + " in a log of " + log.size() + " entries");
                }
                return log.get((int) index - 1).term();
            }

            AppendEntriesRequest heartbeatRequest(long prevLogIndex, long leaderCommit) {
                return new AppendEntriesRequest(
                        term, id, prevLogIndex, termAt(prevLogIndex), List.of(), leaderCommit);
            }

            AppendEntriesRequest requestRange(long fromIndex, long toIndex, long leaderCommit) {
                if (fromIndex < 1L) {
                    throw new IllegalArgumentException("fromIndex must be >= 1");
                }
                if (toIndex < fromIndex - 1L) {
                    throw new IllegalArgumentException("toIndex must be >= fromIndex - 1");
                }
                if (toIndex > log.size()) {
                    throw new IllegalArgumentException(
                            "toIndex " + toIndex + " is beyond the leader log " + log.size());
                }
                long prevIndex = fromIndex - 1L;
                List<RaftLogEntry> entries = toIndex == prevIndex
                        ? List.of() : log.subList((int) prevIndex, (int) toIndex);
                return new AppendEntriesRequest(
                        term, id, prevIndex, termAt(prevIndex), entries, leaderCommit);
            }

            /** Sends starting at this peer's nextIndex and backs nextIndex off on a rejection. */
            Optional<AppendEntriesResponse> send(
                    MockNetwork network, ScenarioNode node, long leaderCommit) {
                long next = nextIndexOf(node.id);
                attemptedNextIndex.add(next);
                return sendRequest(network, node, requestRange(next, log.size(), leaderCommit));
            }

            Optional<AppendEntriesResponse> sendRange(
                    MockNetwork network, ScenarioNode node,
                    long fromIndex, long toIndex, long leaderCommit) {
                return sendRequest(network, node, requestRange(fromIndex, toIndex, leaderCommit));
            }

            Optional<AppendEntriesResponse> sendHeartbeat(
                    MockNetwork network, ScenarioNode node,
                    long prevLogIndex, long leaderCommit) {
                return sendRequest(network, node, heartbeatRequest(prevLogIndex, leaderCommit));
            }

            private Optional<AppendEntriesResponse> sendRequest(
                    MockNetwork network, ScenarioNode node, AppendEntriesRequest request) {
                Optional<AppendEntriesResponse> response = network.deliver(node, request);
                response.ifPresent(value -> applyResponse(node.id, request, value));
                return response;
            }

            private void applyResponse(
                    String peer, AppendEntriesRequest request, AppendEntriesResponse response) {
                if (response.term() > term) {
                    steppedDown = true;
                    return;
                }
                if (response.term() < term) {
                    return;
                }
                if (response.success()) {
                    long expectedMatch = Math.addExact(
                            request.prevLogIndex(), request.entries().size());
                    if (response.matchIndex() != expectedMatch) {
                        throw new AssertionError("success response matchIndex "
                                + response.matchIndex()
                                + " does not equal prevLogIndex + entries.size() = "
                                + expectedMatch);
                    }
                    long next = Math.addExact(response.matchIndex(), 1L);
                    if (next > nextIndexOf(peer)) {
                        nextIndexByPeer.put(peer, next);
                    }
                    return;
                }
                if (response.matchIndex() != 0L) {
                    throw new AssertionError("a rejected response must report matchIndex=0");
                }
                nextIndexByPeer.put(peer, Math.max(1L, nextIndexOf(peer) - 1L));
            }
        }
    }
}
