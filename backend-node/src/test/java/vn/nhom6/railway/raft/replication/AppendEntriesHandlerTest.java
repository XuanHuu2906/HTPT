package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
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
}
