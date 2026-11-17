package vn.nhom6.railway.raft.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D18 tests for {@link RaftStateMachineApplier}: ordered apply of the committed prefix.
 *
 * <p><strong>Honesty boundary.</strong> Everything here is in-memory and single-threaded: the Raft
 * state and log are the real {@link RaftState}/{@link RaftLogEntry}, the domain apply is the real
 * {@link CommittedCommandApplier} and {@link BookingStateMachine}, but the repository is a test
 * double with transactional semantics, and the log payload decoder is the test fixture
 * {@link TestCommandCodec} (no production codec exists yet). The follower integration test uses a
 * real {@link PersistentRaftNode} over {@link FakeRaftStorage}, not PostgreSQL, not TCP and not
 * five processes. No sockets, no threads, no sleeps, no Docker.
 *
 * <p>Consequently these tests prove the ordering/replay/fail-closed contract of the applier. They
 * do <strong>not</strong> prove durability on PostgreSQL (see
 * {@code RaftStateMachineApplierJdbcTest}), a real five-node cluster, or a Listener wired into a
 * running Node.
 */
class RaftStateMachineApplierTest {

    private static final String SELF = "node-1";
    private static final String LEADER = "node-1";
    private static final String FOLLOWER = "node-2";
    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ElectionConfig ELECTION_CONFIG = new ElectionConfig(1_000L, 2_000L, 4_000L);

    // -------------------------------------------------------------------------------------------
    // 1. Ordered prefix only
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Apply tuần tự lastApplied+1..commitIndex; entry sau commitIndex không bao giờ được apply")
    void appliesCommittedPrefixInOrderAndNeverBeyondCommitIndex() {
        Fixture node = Fixture.withEntries(
                book(1, 1, "G01"), noOp(2), book(3, 2, "G02"), book(4, 3, "G03"),
                book(5, 4, "G04"));
        node.state.advanceCommitIndex(3);

        assertEquals(3, node.applier.applyCommitted());
        assertEquals(3L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L, 3L), node.repository.appliedIndices());
        assertEquals(List.of(1L, 2L, 3L), node.hooks.appliedIndices(),
                "the hook must see the same entries in the same order");
        assertEquals(2, node.results.size(), "only two BOOK entries are inside the prefix");

        assertEquals(0, node.applier.applyCommitted(), "nothing new is committed");
        assertEquals(List.of(1L, 2L, 3L), node.repository.appliedIndices());

        node.state.advanceCommitIndex(4);
        assertEquals(1, node.applier.applyCommitted());
        assertEquals(4L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L, 3L, 4L), node.repository.appliedIndices(),
                "entry 5 is in the log but not committed and must stay unapplied");
        assertFalse(node.repository.appliedIndices().contains(5L));
    }

    // -------------------------------------------------------------------------------------------
    // 2. Incremental commit
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Commit tiến từng bước: apply từng phần đúng thứ tự, không trùng")
    void commitAdvanceIsAppliedIncrementallyWithoutDuplicates() {
        Fixture node = Fixture.withEntries(
                book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"), book(4, 4, "G04"));

        node.state.advanceCommitIndex(2);
        assertEquals(2, node.applier.applyCommitted());
        assertEquals(List.of(1L, 2L), node.repository.appliedIndices());

        node.state.advanceCommitIndex(4);
        assertEquals(2, node.applier.applyCommitted());
        assertEquals(List.of(1L, 2L, 3L, 4L), node.repository.appliedIndices());
        assertEquals(4, node.repository.applyCalls(), "each entry was applied exactly once");

        assertEquals(0, node.applier.applyCommitted());
        assertEquals(4, node.repository.applyCalls());
    }

    // -------------------------------------------------------------------------------------------
    // 3. NO_OP
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("NO_OP tăng checkpoint bền vững nhưng không sinh business event")
    void noOpAdvancesPersistentCheckpointWithoutBusinessEvent() {
        Fixture node = Fixture.withEntries(book(1, 1, "G01"), noOp(2), book(3, 2, "G02"));
        node.state.advanceCommitIndex(3);

        assertEquals(3, node.applier.applyCommitted());

        assertEquals(3L, node.state.lastApplied());
        assertEquals(3L, node.repository.lastApplied(), "durable checkpoint includes the NO_OP");
        assertEquals(List.of(1L, 2L, 3L), node.repository.appliedIndices());

        assertEquals(2, node.results.size(),
                "the ApplyResultListener must not be invoked for the NO_OP");
        for (ApplyResult result : node.results) {
            assertTrue(result.appliedLogIndex() == 1L || result.appliedLogIndex() == 3L,
                    "a NO_OP must not produce a business result, got " + result);
        }
        BookingStateMachine.ApplyOutcome noOpOutcome = node.hooks.outcomeAt(2);
        assertInstanceOf(BookingStateMachine.ApplyOutcome.NoOpApplied.class, noOpOutcome);
        assertFalse(noOpOutcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult);
        assertEquals(2, node.repository.machine().totalTicketCount());
        assertEquals("BOOKED", node.repository.machine().seatStatus("G01").orElseThrow());
        assertEquals("BOOKED", node.repository.machine().seatStatus("G02").orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // 4. Same seat twice
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Hai request cùng ghế: đúng một CONFIRMED và một REJECTED_SEAT_TAKEN theo thứ tự log")
    void sameSeatRequestsProduceOneConfirmedAndOneRejectedInLogOrder() {
        Fixture first = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G01"));
        Fixture second = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G01"));
        first.state.advanceCommitIndex(2);
        second.state.advanceCommitIndex(2);

        assertEquals(2, first.applier.applyCommitted());
        assertEquals(2, second.applier.applyCommitted());

        List<BookingStateMachine.ApplyOutcome> a = first.hooks.outcomes();
        List<BookingStateMachine.ApplyOutcome> b = second.hooks.outcomes();
        assertEquals(a, b, "two Nodes must derive the same outcome for the same log");

        var confirmed = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, a.get(0));
        assertEquals(ApplyResult.Outcome.CONFIRMED, confirmed.applyResult().outcome());
        assertEquals(requestId(1), confirmed.applyResult().requestId());

        var rejected = assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, a.get(1));
        assertEquals(ApplyResult.Outcome.REJECTED, rejected.applyResult().outcome());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, rejected.applyResult().reasonCode());
        assertEquals(requestId(2), rejected.applyResult().requestId());

        assertEquals(1, first.repository.machine().activeTicketCount());
        assertEquals(1, first.repository.machine().totalTicketCount());
        assertEquals("ticket-1", first.repository.machine().seatCurrentTicketId("G01").orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // 5. Crash / replay
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Replay sau crash từ lastApplied=0 không bán trùng và giữ nguyên vé")
    void restartReplayDoesNotSellTwice() {
        List<RaftLogEntry> entries = List.of(
                bookEntry(1, 1, "G01"), noOpEntry(2), bookEntry(3, 2, "G02"));
        Fixture first = Fixture.withEntries(entries);
        first.state.advanceCommitIndex(3);
        assertEquals(3, first.applier.applyCommitted());

        int ticketsBefore = first.repository.machine().totalTicketCount();
        Map<String, String> seatsBefore = first.repository.machine().allSeatStatuses();
        List<BookingStateMachine.ApplyOutcome> outcomesBefore = first.hooks.outcomes();
        int businessEventsBefore = first.results.size();

        // Simulate a process restart: durable repository survives, RaftState is rebuilt at 0.
        RaftState restartedState = RaftState.restore(1L, null, entries, 0L);
        restartedState.advanceCommitIndex(3);
        RecordingHooks hooks = new RecordingHooks();
        RaftStateMachineApplier restarted = new RaftStateMachineApplier(
                restartedState, new TestCommandCodec(), first.committed.commitListener(), hooks);

        assertEquals(3, restarted.applyCommitted(), "the whole prefix is re-delivered");
        assertFalse(restarted.failed());
        assertEquals(3L, restartedState.lastApplied());
        assertEquals(3L, first.repository.lastApplied(), "the durable checkpoint does not move back");
        assertEquals(ticketsBefore, first.repository.machine().totalTicketCount(),
                "replay must not create a second ticket");
        assertEquals(seatsBefore, first.repository.machine().allSeatStatuses());
        assertEquals(businessEventsBefore, first.results.size(),
                "redelivery must not emit a new business ApplyResult");
        // CommittedCommandApplier returns the stored business outcome on redelivery; for a NO_OP it
        // deliberately returns empty because a NO_OP has no business result to re-report.
        var replayBook1 = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, hooks.outcomes().get(0));
        var replayBook3 = assertInstanceOf(
                BookingStateMachine.ApplyOutcome.BusinessResult.class, hooks.outcomes().get(2));
        assertEquals(
                assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomesBefore.get(0))
                        .applyResult(),
                replayBook1.applyResult(), "the original result must not change on replay");
        assertEquals(
                assertInstanceOf(BookingStateMachine.ApplyOutcome.BusinessResult.class, outcomesBefore.get(2))
                        .applyResult(),
                replayBook3.applyResult());
        assertEquals(outcomesBefore.get(0), hooks.outcomes().get(0),
                "a redelivery returns the outcome stored by the repository, not a new one");
        assertEquals(outcomesBefore.get(2), hooks.outcomes().get(2));
        assertNull(hooks.outcomes().get(1),
                "a redelivered NO_OP reports no business outcome");
        assertEquals(3, hooks.appliedIndices().size(), "every replayed entry is reported to the hook");
        assertEquals(List.of(1L, 2L, 3L), first.repository.appliedIndices(),
                "the repository applied each index once");
    }

    @Test
    @DisplayName("Crash giữa sink thành công và markApplied không bán trùng")
    void crashBetweenSinkSuccessAndMarkAppliedDoesNotSellTwice() {
        Fixture node = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"));
        node.state.advanceCommitIndex(3);

        CommittedCommandApplier.CommitListener realSink = node.committed.commitListener();
        CommittedCommandApplier.CommitListener crashingSink = (index, command) -> {
            Optional<BookingStateMachine.ApplyOutcome> outcome = realSink.onCommitted(index, command);
            if (index == 2L) {
                throw new SimulatedCrash("crash after the durable apply of index 2");
            }
            return outcome;
        };
        RaftStateMachineApplier crashing = new RaftStateMachineApplier(
                node.state, new TestCommandCodec(), crashingSink, node.hooks);

        assertEquals(1, crashing.applyCommitted(), "index 1 applied, then the simulated crash");
        assertTrue(crashing.failed());
        assertEquals(1L, node.state.lastApplied(), "markApplied(2) never happened");
        assertEquals(2L, node.repository.lastApplied(), "the repository did commit index 2");
        assertEquals(2, node.repository.machine().totalTicketCount());

        // Restart the applier only; the durable repository and RaftState in memory are kept.
        RaftStateMachineApplier restarted = new RaftStateMachineApplier(
                node.state, new TestCommandCodec(), node.committed.commitListener(), node.hooks);
        assertFalse(restarted.failed());
        assertEquals(2, restarted.applyCommitted(), "redeliver 2, then apply 3");
        assertEquals(3L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L, 3L), node.repository.appliedIndices(),
                "index 2 is not applied a second time");
        assertEquals(3, node.repository.machine().activeTicketCount());
        assertEquals(3, node.repository.machine().totalTicketCount());
        assertEquals(3, node.results.size(), "only the three genuine applies emit a business result");
    }

    // -------------------------------------------------------------------------------------------
    // 6. Transient storage failure
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Lỗi lưu trữ tạm thời: không markApplied, không bỏ qua, retry đúng index đó")
    void transientStorageFailureRetriesSameIndexAndNeverSkipsLaterEntries() {
        Fixture node = Fixture.withEntries(
                book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"), book(4, 4, "G04"));
        node.state.advanceCommitIndex(1);
        assertEquals(1, node.applier.applyCommitted());

        node.state.advanceCommitIndex(4);
        node.repository.failNextApply();
        assertEquals(0, node.applier.applyCommitted());
        assertFalse(node.applier.failed(), "a storage failure is retryable, not fatal");
        assertEquals(1L, node.state.lastApplied(), "index 2 was not marked applied");
        assertEquals(1L, node.repository.lastApplied());
        assertEquals(List.of(1L), node.repository.appliedIndices(), "index 3/4 must not be skipped");
        assertEquals(1, node.hooks.failures().size());
        assertEquals("2:false", node.hooks.failures().getFirst(),
                "entry 2 failed with a non-fatal classification");

        assertEquals(3, node.applier.applyCommitted(), "the retry applies 2, 3 and 4 in order");
        assertEquals(4L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L, 3L, 4L), node.repository.appliedIndices());
        assertEquals(4, node.repository.machine().activeTicketCount());
        assertNull(node.hooks.lastFatalFailure(), "no fatal failure was latched");
    }

    // -------------------------------------------------------------------------------------------
    // 7. Hook failures are swallowed, not fatal
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Hook onApplied ném RuntimeException bị nuốt: failed() false, apply tiếp tục")
    void appliedHookFailureIsSwallowedAndDoesNotLatchFatalFailure() {
        Fixture node = Fixture.withEntries(
                book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"));
        node.state.advanceCommitIndex(3);

        var failingHooks = new RaftStateMachineApplier.Hooks() {
            @Override
            public void onApplied(RaftLogEntry entry, Optional<BookingStateMachine.ApplyOutcome> outcome) {
                if (entry.index() == 2L) {
                    throw new IllegalStateException("fixture: hook observer failed");
                }
            }
        };
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                node.state, new TestCommandCodec(), node.committed.commitListener(), failingHooks);

        assertDoesNotThrow(() -> applier.onCommitted(node.state.entryAt(3)));
        assertFalse(applier.failed(), "a hook failure must not become fatal");
        assertEquals(3L, node.state.lastApplied(), "the entry that triggered the hook still counts as applied");
        assertEquals(List.of(1L, 2L, 3L), node.repository.appliedIndices());
        assertEquals(3, node.results.size(), "all real business outcomes still emit a result");
    }

    // -------------------------------------------------------------------------------------------
    // 8. Fatal failures
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Decode hỏng là fatal: latch failed(), không apply tiếp, onCommitted không ném")
    void fatalDecodeFailureLatchesAndStopsWithoutThrowing() {
        Fixture node = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"));
        node.state.advanceCommitIndex(3);
        CommittedCommandApplier.CommitListener sink = node.committed.commitListener();
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                node.state,
                entry -> {
                    if (entry.index() == 2L) {
                        throw new IllegalArgumentException("fixture: undecodable entry");
                    }
                    return new TestCommandCodec().decode(entry);
                },
                sink,
                node.hooks);

        assertDoesNotThrow(() -> applier.onCommitted(node.state.entryAt(3)));
        assertTrue(applier.failed());
        assertEquals(1L, node.state.lastApplied());
        assertEquals(List.of(1L), node.repository.appliedIndices());
        assertEquals(1, node.hooks.failures().size());
        assertTrue(node.hooks.lastFailureWasFatal());

        assertEquals(0, applier.applyCommitted(), "a failed applier refuses to apply anything");
        assertEquals(1L, node.state.lastApplied());
        assertDoesNotThrow(() -> applier.onCommitted(node.state.entryAt(1)));
        assertEquals(1L, node.state.lastApplied(), "the lying callback is still ignored");
    }

    @Test
    @DisplayName("Lỗ index bền vững là fatal, không ném ra khỏi applier")
    void durableIndexHoleIsFatalAndFailsClosed() {
        RaftState state = new RaftState();
        state.startElection(SELF);
        state.append(bookEntry(1, 1, "G01"));
        state.advanceCommitIndex(1);
        RecordingHooks hooks = new RecordingHooks();
        RaftStateMachineApplier applier = new RaftStateMachineApplier(
                state, new TestCommandCodec(),
                new CommittedCommandApplier(new CorruptCheckpointRepository()).commitListener(), hooks);

        assertEquals(0, assertDoesNotThrow(applier::applyCommitted));
        assertTrue(applier.failed());
        assertEquals(0L, state.lastApplied(), "the index hole must not be silently skipped");
        assertEquals(1, hooks.failures().size());
        assertTrue(hooks.lastFailureWasFatal());
        assertInstanceOf(IllegalStateException.class, hooks.lastFailure());
    }

    // -------------------------------------------------------------------------------------------
    // 8. Lying callback
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("onCommitted với entry nói dối không apply vượt commitIndex")
    void lyingCallbackCannotApplyBeyondCommitIndex() {
        Fixture node = Fixture.withEntries(
                book(1, 1, "G01"), book(2, 2, "G02"), book(3, 3, "G03"),
                book(4, 4, "G04"), book(5, 5, "G05"));
        node.state.advanceCommitIndex(2);

        node.applier.onCommitted(node.state.entryAt(5));
        assertEquals(2L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L), node.repository.appliedIndices());

        node.state.advanceCommitIndex(5);
        node.applier.onCommitted(node.state.entryAt(1));
        assertEquals(5L, node.state.lastApplied());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), node.repository.appliedIndices());
        assertEquals(5L, node.repository.machine().activeTicketCount());
    }

    // -------------------------------------------------------------------------------------------
    // 9. Five Nodes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5 Node cùng log/commitIndex apply cùng thứ tự; Node commit thấp hơn là tiền tố")
    void fiveNodesApplyTheSameOrderAndLaggingNodesStayPrefixes() {
        List<RaftLogEntry> entries = List.of(
                bookEntry(1, 1, "G01"), bookEntry(2, 2, "G01"), noOpEntry(3),
                bookEntry(4, 3, "G02"), bookEntry(5, 4, "G03"));

        List<Fixture> nodes = new ArrayList<>();
        long[] commits = {5L, 5L, 5L, 4L, 3L};
        for (long commit : commits) {
            Fixture node = Fixture.withEntries(entries);
            node.state.advanceCommitIndex(commit);
            nodes.add(node);
        }
        for (Fixture node : nodes) {
            node.applier.applyCommitted();
        }

        List<BookingStateMachine.ApplyOutcome> full = nodes.getFirst().hooks.outcomes();
        assertEquals(5, full.size());
        for (int position = 1; position < 3; position++) {
            assertEquals(full, nodes.get(position).hooks.outcomes(),
                    "every fully caught-up Node must have the same outcome list");
            assertEquals(nodes.getFirst().repository.machine().allSeatStatuses(),
                    nodes.get(position).repository.machine().allSeatStatuses(),
                    "every fully caught-up Node must have the same seat state");
            assertEquals(5L, nodes.get(position).state.lastApplied());
        }
        for (int position = 3; position < 5; position++) {
            List<BookingStateMachine.ApplyOutcome> lagging = nodes.get(position).hooks.outcomes();
            assertEquals(full.subList(0, lagging.size()), lagging,
                    "a Node with a lower commitIndex applies only a prefix");
            assertEquals(commits[position], nodes.get(position).state.lastApplied());
            assertEquals(commits[position], nodes.get(position).repository.lastApplied());
            assertTrue(lagging.size() < full.size());
        }
        assertEquals(3, nodes.getFirst().repository.machine().activeTicketCount(),
                "G01, G02 and G03 are active; the second G01 request was rejected");
        assertEquals("ticket-1", nodes.getFirst().repository.machine()
                .seatCurrentTicketId("G01").orElseThrow(),
                "G01 must be sold exactly once on every Node");
    }

    // -------------------------------------------------------------------------------------------
    // 10. Idempotent calls and reentrancy
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Gọi lặp không đổi trạng thái; gọi lồng từ hook không đệ quy")
    void repeatedAndNestedCallsAreIgnored() {
        Fixture node = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G02"));
        node.state.advanceCommitIndex(2);

        assertEquals(2, node.applier.applyCommitted());
        assertEquals(0, node.applier.applyCommitted(), "nothing left to apply");
        assertEquals(2L, node.state.lastApplied());
        assertEquals(2, node.repository.applyCalls());

        Fixture nested = Fixture.withEntries(book(1, 1, "G01"), book(2, 2, "G02"));
        nested.state.advanceCommitIndex(2);
        nested.hooks.nestOnApplied = true;
        nested.hooks.nestedApplier = nested.applier;

        assertEquals(2, nested.applier.applyCommitted());
        assertEquals(List.of(0, 0), nested.hooks.nestedResults,
                "each nested call must be ignored instead of recursing");
        assertEquals(2L, nested.state.lastApplied());
        assertEquals(2, nested.repository.applyCalls());
    }

    // -------------------------------------------------------------------------------------------
    // 11. Real follower integration (D17): only entries <= leaderCommit are applied
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Follower PersistentRaftNode thật: chỉ entry <= leaderCommit được apply")
    void persistentFollowerAppliesOnlyEntriesAtOrBelowLeaderCommit() throws Exception {
        List<RaftLogEntry> entries = List.of(
                bookEntry(1, 1, "G01"), noOpEntry(2), bookEntry(3, 2, "G02"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, List.of(), 0L);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());

        PersistentRaftNode follower = new PersistentRaftNode(FOLLOWER, state, storage);
        ElectionTimer timer = new ElectionTimer(
                new StubElectionService(follower, state, FOLLOWER), new ManualScheduler(),
                ELECTION_CONFIG, failure -> {
                    throw new IllegalStateException("election timer failed in a D18 test", failure);
                });
        follower.bindLifecycle(timer, () -> { });
        try {
            Fixture fixture = Fixture.over(state);
            follower.addCommitListener(fixture.applier);

            var first = follower.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                    1L, LEADER, 0L, 0L, entries, 2L));
            assertTrue(first.success());
            assertEquals(3L, state.lastLogIndex(), "all three entries were stored durably");
            assertEquals(2L, state.commitIndex(),
                    "min(leaderCommit, matchIndex) caps the follower at 2 even though the log has 3");
            assertEquals(2L, state.lastApplied(), "only the committed prefix is applied");
            assertEquals(List.of(1L, 2L), fixture.repository.appliedIndices());

            var heartbeat = follower.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                    1L, LEADER, 3L, 1L, List.of(), 3L));
            assertTrue(heartbeat.success());
            assertEquals(3L, state.commitIndex());
            assertEquals(3L, state.lastApplied());
            assertEquals(List.of(1L, 2L, 3L), fixture.repository.appliedIndices());
            assertEquals(2, fixture.results.size(),
                    "only the two BOOK entries emit a business result; the NO_OP emits none");

            var duplicate = follower.handleAppendEntries(new RaftNode.AppendEntriesRequest(
                    1L, LEADER, 3L, 1L, List.of(), 3L));
            assertTrue(duplicate.success());
            assertEquals(3L, fixture.repository.applyCalls(), "a duplicate heartbeat applies nothing");
            assertFalse(fixture.applier.failed());
        } finally {
            timer.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fixture: one Node with a real applier, in-memory repository and recording hooks
    // -------------------------------------------------------------------------------------------

    private static final class Fixture {

        final RaftState state;
        final InMemoryBookingRepository repository;
        final List<ApplyResult> results = new ArrayList<>();
        final RecordingHooks hooks = new RecordingHooks();
        final CommittedCommandApplier committed;
        final RaftStateMachineApplier applier;

        private Fixture(RaftState state, InMemoryBookingRepository repository) {
            this.state = Objects.requireNonNull(state, "state");
            this.repository = Objects.requireNonNull(repository, "repository");
            this.committed = new CommittedCommandApplier(repository, results::add);
            this.applier = new RaftStateMachineApplier(
                    state, new TestCommandCodec(), committed.commitListener(), hooks);
        }

        static Fixture withEntries(List<RaftLogEntry> entries) {
            return withEntries(entries.toArray(RaftLogEntry[]::new));
        }

        static Fixture withEntries(RaftLogEntry... entries) {
            RaftState state = new RaftState();
            state.startElection(SELF); // term 1, candidate; the applier does not require a Leader
            for (RaftLogEntry entry : entries) {
                state.append(entry);
            }
            return over(state);
        }

        static Fixture over(RaftState state) {
            return new Fixture(state, new InMemoryBookingRepository());
        }
    }

    /**
     * Test-only repository with transactional semantics: changes are staged on a rebuilt state
     * machine and published only on success. It is not JDBC and not durable across processes.
     */
    private static final class InMemoryBookingRepository implements BookingRepository {

        private record AppliedEntry(long index, BookingCommand command) {
        }

        private BookingStateMachine machine = new BookingStateMachine();
        private final List<AppliedEntry> applied = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();
        private boolean failNextApply;
        private int applyCalls;

        @Override
        public long lastApplied() {
            return machine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            applyCalls++;
            if (failNextApply) {
                failNextApply = false;
                throw new StorageException("fixture: apply transaction failed before commit");
            }
            BookingStateMachine staged = new BookingStateMachine();
            for (AppliedEntry entry : applied) {
                staged.apply(entry.index(), entry.command());
            }
            BookingStateMachine.ApplyOutcome outcome = staged.apply(logIndex, command);
            machine = staged;
            applied.add(new AppliedEntry(logIndex, command));
            outcomes.put(logIndex, outcome);
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return machine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomes.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return machine.allSeatStatuses();
        }

        BookingStateMachine machine() {
            return machine;
        }

        int applyCalls() {
            return applyCalls;
        }

        List<Long> appliedIndices() {
            List<Long> indices = new ArrayList<>(applied.size());
            for (AppliedEntry entry : applied) {
                indices.add(entry.index());
            }
            return indices;
        }

        void failNextApply() {
            failNextApply = true;
        }
    }

    /** Reports a durable checkpoint ahead of the log with no stored outcome: an index hole. */
    private static final class CorruptCheckpointRepository implements BookingRepository {

        @Override
        public long lastApplied() {
            return 5L;
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            throw new AssertionError("a corrupted checkpoint must never reach the apply transaction");
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return Optional.empty();
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.empty();
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return Map.of();
        }
    }

    private static final class RecordingHooks implements RaftStateMachineApplier.Hooks {

        private final List<RaftLogEntry> applied = new ArrayList<>();
        private final List<Optional<BookingStateMachine.ApplyOutcome>> outcomes = new ArrayList<>();
        private final List<String> failures = new ArrayList<>();
        private final List<Boolean> fatalFlags = new ArrayList<>();
        private final List<RuntimeException> failureCauses = new ArrayList<>();
        private final List<Integer> nestedResults = new ArrayList<>();
        private boolean nestOnApplied;
        private RaftStateMachineApplier nestedApplier;

        @Override
        public void onApplied(RaftLogEntry entry, Optional<BookingStateMachine.ApplyOutcome> outcome) {
            applied.add(entry);
            outcomes.add(outcome);
            if (nestOnApplied && nestedApplier != null) {
                nestedResults.add(nestedApplier.applyCommitted());
            }
        }

        @Override
        public void onApplyFailure(RaftLogEntry entry, RuntimeException failure, boolean fatal) {
            failures.add((entry == null ? "null" : entry.index()) + ":" + fatal);
            fatalFlags.add(fatal);
            failureCauses.add(failure);
        }

        List<Long> appliedIndices() {
            List<Long> indices = new ArrayList<>(applied.size());
            for (RaftLogEntry entry : applied) {
                indices.add(entry.index());
            }
            return indices;
        }

        List<BookingStateMachine.ApplyOutcome> outcomes() {
            List<BookingStateMachine.ApplyOutcome> values = new ArrayList<>(outcomes.size());
            for (Optional<BookingStateMachine.ApplyOutcome> outcome : outcomes) {
                values.add(outcome.orElse(null));
            }
            return values;
        }

        BookingStateMachine.ApplyOutcome outcomeAt(long index) {
            for (int position = 0; position < applied.size(); position++) {
                if (applied.get(position).index() == index) {
                    return outcomes.get(position).orElse(null);
                }
            }
            throw new AssertionError("no applied outcome recorded for index " + index);
        }

        List<String> failures() {
            return List.copyOf(failures);
        }

        boolean lastFailureWasFatal() {
            return !fatalFlags.isEmpty() && fatalFlags.getLast();
        }

        RuntimeException lastFailure() {
            return failureCauses.isEmpty() ? null : failureCauses.getLast();
        }

        RuntimeException lastFatalFailure() {
            for (int position = fatalFlags.size() - 1; position >= 0; position--) {
                if (fatalFlags.get(position)) {
                    return failureCauses.get(position);
                }
            }
            return null;
        }
    }

    /** Small fixture-only decoder; the production codec is still unowned (TV2/TV3). */
    private static final class TestCommandCodec implements RaftStateMachineApplier.CommandDecoder {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public BookingCommand decode(RaftLogEntry entry) {
            try {
                JsonNode node = MAPPER.readTree(entry.commandPayload());
                return switch (entry.commandType()) {
                    case RaftLogEntry.COMMAND_TYPE_BOOK_TICKET -> new BookingCommand.BookTicket(
                            text(node, "requestId"),
                            text(node, "tripId"),
                            text(node, "seatId"),
                            text(node, "passengerName"),
                            text(node, "ticketId"),
                            Instant.parse(text(node, "issuedAt")));
                    case RaftLogEntry.COMMAND_TYPE_NO_OP -> new BookingCommand.NoOp();
                    default -> throw new IllegalArgumentException(
                            "fixture codec does not support '" + entry.commandType() + "'");
                };
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "fixture payload is not valid JSON for index " + entry.index(), failure);
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null || !value.isTextual()) {
                throw new IllegalArgumentException("fixture payload is missing text field '" + field + "'");
            }
            return value.asText();
        }
    }

    private static final class SimulatedCrash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SimulatedCrash(String message) {
            super(message);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Entry fixtures
    // -------------------------------------------------------------------------------------------

    private static RaftLogEntry bookEntry(long index, long suffix, String seatId) {
        return new RaftLogEntry(index, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                bookPayload(requestId(suffix), seatId, "Passenger " + suffix, "ticket-" + suffix));
    }

    private static RaftLogEntry noOpEntry(long index) {
        return RaftLogEntry.noOp(index, 1L);
    }

    private static RaftLogEntry book(long index, long suffix, String seatId) {
        return bookEntry(index, suffix, seatId);
    }

    private static RaftLogEntry noOp(long index) {
        return noOpEntry(index);
    }

    private static String requestId(long suffix) {
        return String.format(Locale.ROOT, "%08d-0000-0000-0000-%012d", suffix, suffix);
    }

    private static String bookPayload(String requestId, String seatId, String passengerName,
            String ticketId) {
        return "{\"requestId\":\"" + requestId
                + "\",\"tripId\":\"SE-001\""
                + ",\"seatId\":\"" + seatId
                + "\",\"passengerName\":\"" + passengerName
                + "\",\"ticketId\":\"" + ticketId
                + "\",\"issuedAt\":\"2026-10-29T10:00:00Z\"}";
    }

    // -------------------------------------------------------------------------------------------
    // PersistentRaftNode plumbing: election boundary and a scheduler that never fires
    // -------------------------------------------------------------------------------------------

    private static final class StubElectionService implements ElectionService {

        private final PersistentRaftNode node;
        private final RaftState state;
        private final String localId;

        StubElectionService(PersistentRaftNode node, RaftState state, String localId) {
            this.node = Objects.requireNonNull(node, "node");
            this.state = Objects.requireNonNull(state, "state");
            this.localId = Objects.requireNonNull(localId, "localId");
        }

        @Override
        public String localId() {
            return localId;
        }

        @Override
        public RaftNode node() {
            return node;
        }

        @Override
        public RaftState state() {
            return state;
        }

        @Override
        public List<String> voters() {
            return VOTERS;
        }

        @Override
        public boolean isVoter(String nodeId) {
            return VOTERS.contains(nodeId);
        }

        @Override
        public int requiredVotes() {
            return 3;
        }

        @Override
        public long startElection() {
            throw new UnsupportedOperationException("D18 follower never campaigns");
        }

        @Override
        public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
            return Optional.empty();
        }

        @Override
        public RaftNode.RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) {
            throw new UnsupportedOperationException("D18 follower never handles REQUEST_VOTE");
        }

        @Override
        public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
            throw new UnsupportedOperationException("D18 follower never counts votes");
        }

        @Override
        public void promoteToLeader(Set<String> peers) {
            throw new UnsupportedOperationException("D18 follower never promotes");
        }
    }

    /** Records deadlines but never runs them: the test explicitly drives append handling. */
    private static final class ManualScheduler extends AbstractExecutorService
            implements ScheduledExecutorService {

        private final List<FutureTask<Void>> scheduled = new ArrayList<>();
        private boolean shutdown;

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            FutureTask<Void> task = new FutureTask<>(Objects.requireNonNull(command), null);
            scheduled.add(task);
            return new NeverScheduledFuture(task);
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("D18 drives the Raft loop explicitly");
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("D18 schedules Runnables only");
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("election deadlines are one-shot");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("election deadlines are one-shot");
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }

    private static final class NeverScheduledFuture implements ScheduledFuture<Void> {

        private final FutureTask<Void> delegate;

        NeverScheduledFuture(FutureTask<Void> delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public Void get() throws InterruptedException,
                java.util.concurrent.ExecutionException {
            return delegate.get();
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException,
                java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
            return delegate.get(timeout, unit);
        }
    }
}
