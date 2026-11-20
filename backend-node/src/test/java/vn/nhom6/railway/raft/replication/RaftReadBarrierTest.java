package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRaftBridge;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.ElectionService;
import vn.nhom6.railway.raft.election.ElectionTimer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D19 tests for {@link RaftReadBarrier} (SPEC-04 section 7).
 *
 * <p><strong>Honesty boundary.</strong> The unit tests use an in-memory port, an in-memory
 * apply awaiter and a manual timeout scheduler: no real clock, no sleeps, no threads. The two
 * integration tests use the real {@link RaftReplicationService}, {@link ReplicationTracker},
 * {@link CommitAdvancer}, {@link RaftReplicationService} transport adapter and real
 * {@code PersistentRaftNode} followers over {@link FakeRaftStorage}; the network is a deterministic
 * fake queue. They do <strong>not</strong> use TCP, Docker, PostgreSQL, five real processes or a
 * real election. The Leader-side {@code advance()}/{@code applyCommitted()} wiring exists only in
 * this test harness because no production runtime wires it yet.
 */
class RaftReadBarrierTest {

    private static final String LEADER = "node-1";
    private static final List<String> PEERS = List.of("node-2", "node-3", "node-4", "node-5");
    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ElectionConfig HEARTBEAT_CONFIG = new ElectionConfig(1_000L, 2_000L, 4_000L);

    // -------------------------------------------------------------------------------------------
    // 1. Leader + 3/5 ACK + commit + apply => barrier with this NO_OP index
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Leader 3/5 ACK NO_OP, commit + apply => barrier readIndex = index, lastApplied >= index")
    void leaderBarrierCompletesAfterMajorityCommitAndApply() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(5L, 1L);
        port.commitIndex = 5L;
        FakeAwaiter awaiter = new FakeAwaiter();
        ManualBarrierScheduler scheduler = new ManualBarrierScheduler();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, scheduler);

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));

        assertFalse(stage.toCompletableFuture().isDone(), "apply has not happened yet");
        assertEquals(1, port.appendCalls, "exactly one fresh NO_OP");
        assertEquals(1, barrier.pendingCount());

        awaiter.completeApplied(5L);

        BookingRequestHandler.ReadBarrierResult result = join(stage);
        assertEquals(5L, result.readIndex());
        assertEquals("node-1", result.leaderId());
        assertTrue(awaiter.lastApplied >= result.readIndex(),
                "the awaiter only completes at or after readIndex");
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 2. Every read appends a new NO_OP
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Hai lần đọc liên tiếp sinh hai NO_OP index khác nhau, không dùng lại")
    void eachReadAppendsAFreshNoOp() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(5L, 1L);
        port.accept(6L, 1L);
        port.commitIndex = 6L;
        FakeAwaiter awaiter = new FakeAwaiter();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, new ManualBarrierScheduler());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> first =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));
        awaiter.completeApplied(5L);
        CompletionStage<BookingRequestHandler.ReadBarrierResult> second =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));
        awaiter.completeApplied(6L);

        assertEquals(5L, join(first).readIndex());
        assertEquals(6L, join(second).readIndex());
        assertNotEquals(join(first).readIndex(), join(second).readIndex());
        assertEquals(2, port.appendCalls);
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 3. Only 2/5 => never completes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Chỉ 2/5 (không đa số): barrier không hoàn tất, hết hạn TimeoutException, commit không đổi")
    void minorityNeverCompletesAndEndsWithTimeout() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(7L, 1L);
        FakeAwaiter awaiter = new FakeAwaiter();
        ManualBarrierScheduler scheduler = new ManualBarrierScheduler();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, scheduler);

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofMillis(500));

        assertFalse(stage.toCompletableFuture().isDone());
        assertEquals(0L, port.commitIndex, "2/5 must not move commitIndex");
        scheduler.fireAll();

        assertCompletionFailure(stage, TimeoutException.class);
        assertEquals(0L, port.commitIndex);
        assertEquals(0, barrier.pendingCount());

        // A late apply must not resurrect a failed barrier.
        awaiter.completeApplied(7L);
        port.commitIndex = 7L;
        assertTrue(stage.toCompletableFuture().isCompletedExceptionally());
    }

    // -------------------------------------------------------------------------------------------
    // 4. Majority committed but not applied => still pending
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Đã commit nhưng chưa apply: chưa hoàn tất; apply xong mới hoàn tất")
    void majorityCommittedButNotAppliedStaysPending() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(9L, 1L);
        port.commitIndex = 9L;
        FakeAwaiter awaiter = new FakeAwaiter();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, new ManualBarrierScheduler());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));

        assertFalse(stage.toCompletableFuture().isDone(), "commit alone is not enough");
        awaiter.completeApplied(9L);
        assertEquals(9L, join(stage).readIndex());
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 5. Not Leader
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Không phải Leader: NotLeaderException kèm leaderHint, không append")
    void nonLeaderFailsWithoutAppending() {
        FakePort port = FakePort.follower("node-2", "node-1", 1L);
        RaftReadBarrier barrier = new RaftReadBarrier(
                port, new FakeAwaiter(), new ManualBarrierScheduler());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));

        BookingRequestHandler.NotLeaderException failure =
                assertCompletionFailure(stage, BookingRequestHandler.NotLeaderException.class);
        assertEquals("node-1", failure.leaderHint());
        assertEquals(0, port.appendCalls);
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 6. Leadership lost mid-flight
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Mất Leader giữa chừng: NotLeaderException, không bao giờ trả ReadBarrierResult")
    void leadershipLostFailsEveryPendingBarrier() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(11L, 1L);
        port.commitIndex = 11L;
        FakeAwaiter awaiter = new FakeAwaiter();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, new ManualBarrierScheduler());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));
        assertFalse(stage.toCompletableFuture().isDone());

        port.leader = false;
        port.leaderHint = "node-3";
        barrier.onLeadershipLost();

        assertCompletionFailure(stage, BookingRequestHandler.NotLeaderException.class);
        assertEquals(0, barrier.pendingCount());

        awaiter.completeApplied(11L);
        assertTrue(stage.toCompletableFuture().isCompletedExceptionally());
    }

    // -------------------------------------------------------------------------------------------
    // 7. Entry overwritten by another term
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Entry tại readIndex bị ghi đè bởi term khác: không hoàn tất thành công")
    void overwrittenEntryNeverCompletesSuccessfully() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(13L, 1L);
        port.commitIndex = 13L;
        port.terms.put(13L, 2L); // the local log now holds another term at this index
        FakeAwaiter awaiter = new FakeAwaiter();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, new ManualBarrierScheduler());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofSeconds(1));
        awaiter.completeApplied(13L);

        assertCompletionFailure(stage, BookingRequestHandler.ReadUnavailableException.class);
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 8. Append NotLeader / StorageFailure
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Append trả NotLeader/StorageFailure: fail đúng loại, không là barrier")
    void appendFailuresAreNotBarriers() {
        FakePort notLeaderPort = FakePort.leader("node-1", 1L);
        notLeaderPort.outcomes.add(new RaftNode.NotLeader("node-4"));
        RaftReadBarrier notLeaderBarrier = new RaftReadBarrier(
                notLeaderPort, new FakeAwaiter(), new ManualBarrierScheduler());

        BookingRequestHandler.NotLeaderException notLeader =
                assertCompletionFailure(notLeaderBarrier.appendAndAwaitNoOp(Duration.ofSeconds(1)),
                        BookingRequestHandler.NotLeaderException.class);
        assertEquals("node-4", notLeader.leaderHint());
        assertEquals(0, notLeaderBarrier.pendingCount());

        FakePort storagePort = FakePort.leader("node-1", 1L);
        storagePort.outcomes.add(new RaftNode.StorageFailure(
                new RaftStorage.RaftStorageException("fake disk failure")));
        RaftReadBarrier storageBarrier = new RaftReadBarrier(
                storagePort, new FakeAwaiter(), new ManualBarrierScheduler());

        assertCompletionFailure(storageBarrier.appendAndAwaitNoOp(Duration.ofSeconds(1)),
                BookingRequestHandler.ReadUnavailableException.class);
        assertEquals(0, storageBarrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 9. Late completion after timeout is ignored
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Kết quả đến muộn sau timeout bị bỏ qua; pendingCount về 0")
    void lateCompletionAfterTimeoutIsIgnored() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(17L, 1L);
        port.commitIndex = 17L;
        FakeAwaiter awaiter = new FakeAwaiter();
        ManualBarrierScheduler scheduler = new ManualBarrierScheduler();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, scheduler);

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofMillis(1));
        scheduler.fireAll();
        assertCompletionFailure(stage, TimeoutException.class);
        assertEquals(0, barrier.pendingCount());

        awaiter.completeApplied(17L);
        assertTrue(stage.toCompletableFuture().isCompletedExceptionally(),
                "a late apply must not complete the timed-out future a second time");
        assertFalse(stage.toCompletableFuture().isCancelled());
    }

    // -------------------------------------------------------------------------------------------
    // 10. Multiple concurrent readers
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Nhiều reader đồng thời: mỗi reader một NO_OP riêng, kết quả không lẫn")
    void concurrentReadersGetTheirOwnNoOp() {
        FakePort port = FakePort.leader("node-1", 1L);
        port.accept(21L, 1L);
        port.accept(22L, 1L);
        port.accept(23L, 1L);
        port.accept(24L, 1L);
        port.commitIndex = 24L;
        FakeAwaiter awaiter = new FakeAwaiter();
        RaftReadBarrier barrier = new RaftReadBarrier(port, awaiter, new ManualBarrierScheduler());

        List<CompletionStage<BookingRequestHandler.ReadBarrierResult>> stages = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            stages.add(barrier.appendAndAwaitNoOp(Duration.ofSeconds(1)));
        }
        assertEquals(4, port.appendCalls);
        assertEquals(4, barrier.pendingCount());

        awaiter.completeApplied(24L);

        Set<Long> indices = new TreeSet<>();
        for (CompletionStage<BookingRequestHandler.ReadBarrierResult> stage : stages) {
            BookingRequestHandler.ReadBarrierResult result = join(stage);
            indices.add(result.readIndex());
            assertEquals("node-1", result.leaderId());
        }
        assertEquals(Set.of(21L, 22L, 23L, 24L), indices);
        assertEquals(0, barrier.pendingCount());
    }

    // -------------------------------------------------------------------------------------------
    // 11. Invalid timeout
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. timeout <= 0 bị từ chối bằng IllegalArgumentException")
    void nonPositiveTimeoutIsRejected() {
        FakePort port = FakePort.leader("node-1", 1L);
        RaftReadBarrier barrier = new RaftReadBarrier(
                port, new FakeAwaiter(), new ManualBarrierScheduler());

        assertThrows(IllegalArgumentException.class,
                () -> barrier.appendAndAwaitNoOp(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> barrier.appendAndAwaitNoOp(Duration.ofMillis(-1)));
        assertEquals(0, port.appendCalls);
    }

    // -------------------------------------------------------------------------------------------
    // 12. Handler-compatible error mapping
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Tương thích handler: NotLeaderException -> NOT_LEADER, TimeoutException -> READ_UNAVAILABLE")
    void handlerCompatibleErrorMapping() {
        FakePort follower = FakePort.follower("node-2", "node-1", 1L);
        RaftReadBarrier notLeaderBarrier = new RaftReadBarrier(
                follower, new FakeAwaiter(), new ManualBarrierScheduler());
        assertEquals("NOT_LEADER", consumeAsHandler(notLeaderBarrier));

        FakePort leader = FakePort.leader("node-1", 1L);
        leader.accept(31L, 1L);
        ManualBarrierScheduler scheduler = new ManualBarrierScheduler();
        RaftReadBarrier timeoutBarrier = new RaftReadBarrier(
                leader, new FakeAwaiter(), scheduler);
        CompletionStage<String> outcome = consumeAsHandlerAsync(timeoutBarrier);
        scheduler.fireAll();
        assertEquals("READ_UNAVAILABLE", await(outcome));
    }

    // -------------------------------------------------------------------------------------------
    // 13. CONFIRMED only after commit + apply (real BookingRaftBridge, TV2 path)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("13. Không CONFIRMED trước commit+apply (BookingRaftBridge + RaftApplyAdapter thật)")
    void confirmedOnlyAfterCommitAndApply() throws Exception {
        BookingStateMachine stateMachine = new BookingStateMachine();
        RaftApplyAdapter adapter = new RaftApplyAdapter(stateMachine, null);
        BridgeFakeRaft raft = new BridgeFakeRaft();
        String requestId = "11111111-1111-1111-1111-111111111111";
        BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                requestId, "SE-001", "G01", "Barrier Test", "tk-d19",
                Instant.parse("2026-11-20T08:00:00Z"));

        try (BookingRaftBridge bridge =
                new BookingRaftBridge(raft, adapter, null, Duration.ofSeconds(2))) {
            CompletableFuture<ApplyResult> future = bridge.submitCommand(command);

            assertFalse(future.isDone(), "append alone is not a CONFIRMED result");
            assertEquals(0L, stateMachine.lastApplied());

            BridgeFakeRaft.AppendedCommand appended = raft.log.getFirst();
            adapter.advanceCommitIndex(appended.index());
            adapter.onCommitted(new RaftLogEntry(
                    appended.index(), appended.term(), appended.type(), appended.payload()));

            ApplyResult result = future.get(1, TimeUnit.SECONDS);
            assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
            assertTrue(stateMachine.lastApplied() >= appended.index(),
                    "CONFIRMED only after the durable apply checkpoint reached the entry");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Integration: real Leader services, fake network, real follower nodes, D18 applier
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Integration: Leader thật commit + apply NO_OP rồi barrier mới hoàn tất")
    void leaderHarnessBarrierCompletesOnlyAfterCommitAndApply() throws Exception {
        HarnessCluster cluster = new HarnessCluster();
        try {
            CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                    cluster.barrier.appendAndAwaitNoOp(Duration.ofSeconds(5));
            assertFalse(stage.toCompletableFuture().isDone());
            assertEquals(1L, cluster.leader.lastLogIndex(), "the barrier appended exactly one NO_OP");
            assertEquals(RaftLogEntry.COMMAND_TYPE_NO_OP,
                    cluster.leader.entryAt(1L).commandType());

            cluster.service.replicateAll();
            cluster.settle(300);

            BookingRequestHandler.ReadBarrierResult result = join(stage);
            assertEquals(1L, result.readIndex());
            assertEquals(LEADER, result.leaderId());
            assertTrue(cluster.leaderRepository.lastApplied() >= result.readIndex(),
                    "the Leader applied the NO_OP through the D18 applier");
            assertEquals(0, cluster.barrier.pendingCount());
            assertTrue(cluster.leader.lastApplied() >= result.readIndex());
        } finally {
            cluster.close();
        }
    }

    @Test
    @DisplayName("Integration: Leader 2/5 không commit, barrier timeout, không đọc dữ liệu cũ")
    void leaderHarnessMinorityTimesOutWithoutBarrier() throws Exception {
        HarnessCluster cluster = new HarnessCluster();
        try {
            CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                    cluster.barrier.appendAndAwaitNoOp(Duration.ofMillis(500));

            cluster.service.replicate("node-2");
            cluster.settle(100);

            assertEquals(0L, cluster.leader.commitIndex(), "Leader + 1 follower is 2/5");
            assertFalse(stage.toCompletableFuture().isDone());
            cluster.barrierTimeouts.fireAll();

            assertCompletionFailure(stage, TimeoutException.class);
            assertEquals(0, cluster.barrier.pendingCount());
            assertEquals(0L, cluster.leader.commitIndex());
        } finally {
            cluster.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------------------------

    private static BookingRequestHandler.ReadBarrierResult join(
            CompletionStage<BookingRequestHandler.ReadBarrierResult> stage) {
        try {
            return stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new AssertionError("barrier did not complete successfully", failure);
        }
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new AssertionError("stage did not complete successfully", failure);
        }
    }

    private static <T extends Throwable> T assertCompletionFailure(
            CompletionStage<?> stage, Class<T> expected) {
        Throwable cause;
        try {
            stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
            cause = null;
        } catch (ExecutionException failure) {
            cause = failure.getCause();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interrupted);
        } catch (TimeoutException timeout) {
            cause = null;
        }
        assertNotNull(cause, "expected " + expected.getSimpleName() + " but the future completed normally");
        return assertInstanceOf(expected, cause);
    }

    private static String consumeAsHandler(RaftReadBarrier barrier) {
        try {
            barrier.appendAndAwaitNoOp(Duration.ofSeconds(1)).toCompletableFuture().join();
            return "OK";
        } catch (CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof BookingRequestHandler.NotLeaderException) {
                return "NOT_LEADER";
            }
            if (cause instanceof TimeoutException
                    || cause instanceof BookingRequestHandler.ReadUnavailableException) {
                return "READ_UNAVAILABLE";
            }
            throw failure;
        }
    }

    private static CompletionStage<String> consumeAsHandlerAsync(RaftReadBarrier barrier) {
        // Non-leader is reported as a failed future by appendAndAwaitNoOp, never thrown
        // synchronously; timeout <= 0 is the only synchronous failure and is unused here.
        return barrier.appendAndAwaitNoOp(Duration.ofSeconds(1))
                .handle((ignored, failure) -> {
                    if (failure instanceof BookingRequestHandler.NotLeaderException) {
                        return "NOT_LEADER";
                    }
                    if (failure instanceof TimeoutException
                            || failure instanceof BookingRequestHandler.ReadUnavailableException) {
                        return "READ_UNAVAILABLE";
                    }
                    if (failure != null) {
                        throw new AssertionError("unexpected barrier failure", failure);
                    }
                    return "OK";
                });
    }

    // -------------------------------------------------------------------------------------------
    // Fake port / awaiter / scheduler
    // -------------------------------------------------------------------------------------------

    private static final class FakePort implements RaftReadBarrier.Port {

        private final String localId;
        private boolean leader;
        private long term;
        private String leaderHint;
        private long commitIndex;
        private int appendCalls;
        private final Deque<RaftNode.AppendOutcome> outcomes = new ArrayDeque<>();
        private final Map<Long, Long> terms = new HashMap<>();

        private FakePort(String localId, boolean leader, String leaderHint, long term) {
            this.localId = localId;
            this.leader = leader;
            this.leaderHint = leaderHint;
            this.term = term;
        }

        static FakePort leader(String localId, long term) {
            return new FakePort(localId, true, null, term);
        }

        static FakePort follower(String localId, String leaderHint, long term) {
            return new FakePort(localId, false, leaderHint, term);
        }

        void accept(long index, long entryTerm) {
            outcomes.add(new RaftNode.Accepted(index, entryTerm));
            terms.put(index, entryTerm);
        }

        @Override
        public String localId() {
            return localId;
        }

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public long currentTerm() {
            return term;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderHint);
        }

        @Override
        public RaftNode.AppendOutcome appendNoOp() {
            appendCalls++;
            RaftNode.AppendOutcome outcome = outcomes.poll();
            return outcome == null
                    ? new RaftNode.Accepted(100L + appendCalls, term)
                    : outcome;
        }

        @Override
        public long commitIndex() {
            return commitIndex;
        }

        @Override
        public OptionalLong termAt(long index) {
            Long entryTerm = terms.get(index);
            return entryTerm == null ? OptionalLong.empty() : OptionalLong.of(entryTerm);
        }
    }

    private static final class FakeAwaiter implements RaftReadBarrier.AppliedAwaiter {

        private final Map<Long, CompletableFuture<Void>> waiters = new LinkedHashMap<>();
        private long lastApplied;

        @Override
        public synchronized CompletionStage<Void> awaitIndex(long index) {
            if (lastApplied >= index) {
                return CompletableFuture.completedFuture(null);
            }
            return waiters.computeIfAbsent(index, key -> new CompletableFuture<>());
        }

        synchronized void completeApplied(long applied) {
            lastApplied = Math.max(lastApplied, applied);
            List<Long> ready = new ArrayList<>();
            for (Long index : waiters.keySet()) {
                if (index <= lastApplied) {
                    ready.add(index);
                }
            }
            for (Long index : ready) {
                waiters.remove(index).complete(null);
            }
        }
    }

    private static final class ManualBarrierScheduler implements RaftReplicationService.Scheduler {

        private static final class Task implements Cancellable {

            private final Runnable runnable;
            private boolean cancelled;

            private Task(Runnable runnable) {
                this.runnable = runnable;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }

        private final List<Task> tasks = new ArrayList<>();

        @Override
        public Cancellable schedule(Runnable task, long delayMillis) {
            Task scheduled = new Task(Objects.requireNonNull(task));
            tasks.add(scheduled);
            return scheduled;
        }

        void fireAll() {
            List<Task> due = new ArrayList<>(tasks);
            tasks.clear();
            for (Task task : due) {
                if (!task.cancelled) {
                    task.runnable.run();
                }
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Real BookingRaftBridge fixture (test 13)
    // -------------------------------------------------------------------------------------------

    private static final class BridgeFakeRaft implements BookingRaftBridge.RaftPort {

        private record AppendedCommand(long index, long term, String type, String payload) {
        }

        private Role role = Role.LEADER;
        private String leaderHint = "node-1";
        private long term = 1L;
        private long nextIndex = 1L;
        private final List<AppendedCommand> log = new ArrayList<>();

        @Override
        public Role role() {
            return role;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.ofNullable(leaderHint);
        }

        @Override
        public long currentTerm() {
            return term;
        }

        @Override
        public RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
            long index = nextIndex++;
            log.add(new AppendedCommand(index, term, commandType, commandPayload));
            return new RaftNode.Accepted(index, term);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Integration harness: real Leader services + real follower PersistentRaftNodes
    // -------------------------------------------------------------------------------------------

    private static final class HarnessCluster implements AutoCloseable {

        final RaftState leader = leaderState();
        final FakeRaftStorage leaderStorage = leaderStorage(leader);
        final PersistentRaftNode leaderNode;
        final Map<String, HarnessFollower> followers = new LinkedHashMap<>();
        final ReplicationTracker tracker;
        final CommitAdvancer advancer;
        final HarnessHooks hooks;
        final FakeNetwork network;
        final RaftReplicationService service;
        final ManualRetryScheduler retries = new ManualRetryScheduler();
        final ManualBarrierScheduler barrierTimeouts = new ManualBarrierScheduler();
        final ManualEventLoop adapterDeadlines = new ManualEventLoop();
        final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
        final List<Throwable> loopFailures = new ArrayList<>();
        final InMemoryBookingRepository leaderRepository = new InMemoryBookingRepository();
        final RaftStateMachineApplier leaderApplier;
        final HarnessAwaiter applied = new HarnessAwaiter();
        final RaftReadBarrier barrier;

        HarnessCluster() throws Exception {
            leaderNode = new PersistentRaftNode(LEADER, leader, leaderStorage);
            leaderApplier = new RaftStateMachineApplier(
                    leader,
                    entry -> RaftApplyAdapter.decodeCommand(entry),
                    new CommittedCommandApplier(leaderRepository).commitListener());
            applied.lastApplied = leaderRepository.lastApplied();

            for (String peer : PEERS) {
                followers.put(peer, new HarnessFollower(peer));
            }
            Set<String> peers = new TreeSet<>(PEERS);
            tracker = new ReplicationTracker(leader, peers);
            advancer = new CommitAdvancer(leader, tracker);
            network = new FakeNetwork(followers);
            hooks = new HarnessHooks(this);
            RaftTransportAdapter transport = new RaftTransportAdapter(
                    LEADER, network, Duration.ofSeconds(30L), loop::add, adapterDeadlines,
                    loopFailures::add);
            service = new RaftReplicationService(LEADER, leader, transport, peers,
                    loop::add, retries, 64, new RaftReplicationService.Backoff(10L, 100L), hooks);
            new AppendRetryCoordinator(leader, tracker, service, loop::add, retries,
                    new RaftReplicationService.Backoff(10L, 100L), 10,
                    new AppendRetryCoordinator.Hooks() {
                        @Override
                        public void onHigherTerm(long higherTerm) {
                            leader.becomeFollower(higherTerm);
                        }

                        @Override
                        public void onGiveUp(String peerId, String reason) {
                        }
                    });

            barrier = new RaftReadBarrier(
                    new LeaderPort(leaderNode, leader), applied::awaitIndex, barrierTimeouts);
        }

        void drain() {
            Runnable task;
            while ((task = loop.poll()) != null) {
                task.run();
            }
        }

        void runRound() {
            network.deliverAll();
            drain();
            retries.fireAll();
            drain();
        }

        void settle(int maxRounds) {
            for (int round = 0; round < maxRounds; round++) {
                boolean progress = network.deliverAll();
                drain();
                progress |= retries.fireAll();
                drain();
                if (!progress && network.pendingCount() == 0 && retries.pending() == 0) {
                    return;
                }
            }
            throw new AssertionError("cluster did not settle: pending="
                    + network.pendingCount() + ", retries=" + retries.pending());
        }

        @Override
        public void close() {
            service.stop();
            for (HarnessFollower follower : followers.values()) {
                follower.close();
            }
        }

        private static RaftState leaderState() {
            RaftState state = new RaftState();
            state.startElection(LEADER);
            state.becomeLeader(new TreeSet<>(PEERS));
            return state;
        }

        private static FakeRaftStorage leaderStorage(RaftState state) {
            FakeRaftStorage storage = new FakeRaftStorage();
            storage.seedState(state.currentTerm(), state.votedFor(), state.entries(),
                    state.lastApplied());
            try {
                new PersistentLogAppender(storage).recover();
            } catch (RaftStorage.RaftStorageException failure) {
                throw new IllegalStateException("cannot seed leader storage", failure);
            }
            return storage;
        }
    }

    private static final class HarnessHooks implements RaftReplicationService.Hooks {

        private final HarnessCluster cluster;

        private HarnessHooks(HarnessCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void onHigherTerm(long term) {
            if (cluster.leader.role() == Role.LEADER) {
                cluster.leader.becomeFollower(term);
            }
            cluster.barrier.onLeadershipLost();
        }

        @Override
        public void onFailure(String peerId, Throwable failure) {
            cluster.loopFailures.add(failure);
        }

        @Override
        public void onMatchIndexAdvanced(String peerId, long matchIndex) {
            cluster.advancer.advance().ifPresent(commit -> {
                cluster.leaderApplier.applyCommitted();
                cluster.applied.notifyApplied(cluster.leaderRepository.lastApplied());
            });
        }
    }

    private static final class LeaderPort implements RaftReadBarrier.Port {

        private final RaftNode node;
        private final RaftState state;

        private LeaderPort(RaftNode node, RaftState state) {
            this.node = node;
            this.state = state;
        }

        @Override
        public String localId() {
            return LEADER;
        }

        @Override
        public boolean isLeader() {
            return node.role() == Role.LEADER;
        }

        @Override
        public long currentTerm() {
            return node.currentTerm();
        }

        @Override
        public Optional<String> leaderId() {
            return node.leaderId();
        }

        @Override
        public RaftNode.AppendOutcome appendNoOp() {
            return node.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        }

        @Override
        public long commitIndex() {
            return state.commitIndex();
        }

        @Override
        public OptionalLong termAt(long index) {
            if (index < RaftLogEntry.FIRST_INDEX || index > state.lastLogIndex()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(state.termAt(index));
        }
    }

    private static final class HarnessAwaiter {

        private final Map<Long, CompletableFuture<Void>> waiters = new LinkedHashMap<>();
        private long lastApplied;

        synchronized void notifyApplied(long applied) {
            lastApplied = Math.max(lastApplied, applied);
            List<Long> ready = new ArrayList<>();
            for (Long index : waiters.keySet()) {
                if (index <= lastApplied) {
                    ready.add(index);
                }
            }
            for (Long index : ready) {
                waiters.remove(index).complete(null);
            }
        }

        CompletionStage<Void> awaitIndex(long index) {
            synchronized (this) {
                if (lastApplied >= index) {
                    return CompletableFuture.completedFuture(null);
                }
                return waiters.computeIfAbsent(index, key -> new CompletableFuture<>());
            }
        }
    }

    private static final class HarnessFollower implements AutoCloseable {

        final String id;
        final FakeRaftStorage storage = new FakeRaftStorage();
        final RaftState state;
        final PersistentRaftNode node;
        final ElectionTimer timer;
        final InMemoryBookingRepository repository = new InMemoryBookingRepository();
        final RaftStateMachineApplier applier;

        HarnessFollower(String id) throws Exception {
            this.id = id;
            storage.seedState(1L, null, List.of(), 0L);
            RaftStorage.PersistentState persisted = storage.load();
            state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                    persisted.log(), persisted.lastApplied());
            node = new PersistentRaftNode(id, state, storage);
            ManualEventLoop electionClock = new ManualEventLoop();
            timer = new ElectionTimer(
                    new StubElectionService(node, state, id), electionClock, HEARTBEAT_CONFIG,
                    failure -> {
                        throw new IllegalStateException("election timer failed", failure);
                    });
            node.bindLifecycle(timer, () -> { });
            applier = new RaftStateMachineApplier(
                    state,
                    entry -> RaftApplyAdapter.decodeCommand(entry),
                    new CommittedCommandApplier(repository).commitListener());
            node.addCommitListener(applier);
        }

        @Override
        public void close() {
            timer.close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fake network and schedulers (no real clock, no threads, no sockets)
    // -------------------------------------------------------------------------------------------

    private static final class FakeNetwork implements RaftTransportAdapter.RaftRpcClient {

        private static final class Call {

            final String peerId;
            final MessageEnvelope<AppendEntriesRequestDto> request;
            final CompletableFuture<MessageEnvelope<JsonNode>> future = new CompletableFuture<>();

            private Call(String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
                this.peerId = peerId;
                this.request = request;
            }
        }

        private final Map<String, HarnessFollower> followers;
        private final Map<String, Deque<Call>> pending = new LinkedHashMap<>();
        private final List<Call> calls = new ArrayList<>();

        private FakeNetwork(Map<String, HarnessFollower> followers) {
            this.followers = Map.copyOf(followers);
            for (String peer : followers.keySet()) {
                pending.put(peer, new ArrayDeque<>());
            }
        }

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
            Call call = new Call(peerId, request);
            pending.computeIfAbsent(peerId, key -> new ArrayDeque<>()).add(call);
            calls.add(call);
            return call.future;
        }

        boolean deliverAll() {
            boolean delivered = false;
            for (String peerId : List.copyOf(pending.keySet())) {
                delivered |= deliverPeer(peerId);
            }
            return delivered;
        }

        boolean deliverPeer(String peerId) {
            Deque<Call> queue = pending.get(peerId);
            if (queue == null || queue.isEmpty()) {
                return false;
            }
            deliver(queue.poll());
            return true;
        }

        private void deliver(Call call) {
            try {
                RaftNode.AppendEntriesResponse response = followers.get(call.peerId).node
                        .handleAppendEntries(call.request.getPayload(), LEADER);
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("term", response.term());
                payload.put("success", response.success());
                payload.put("matchIndex", response.matchIndex());
                call.future.complete(MessageCodec.decode(MessageCodec.encode(
                        MessageEnvelope.createResponse(call.request.getMessageId(),
                                MessageType.APPEND_ENTRIES_RESPONSE, call.peerId, payload))));
            } catch (Exception failure) {
                call.future.completeExceptionally(failure);
            }
        }

        int pendingCount() {
            int count = 0;
            for (Deque<Call> queue : pending.values()) {
                count += queue.size();
            }
            return count;
        }
    }

    private static final class ManualRetryScheduler implements RaftReplicationService.Scheduler {

        private static final class Task implements Cancellable {

            private final Runnable runnable;
            private boolean cancelled;

            private Task(Runnable runnable) {
                this.runnable = runnable;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }

        private final List<Task> tasks = new ArrayList<>();

        @Override
        public Cancellable schedule(Runnable task, long delayMillis) {
            Task scheduled = new Task(Objects.requireNonNull(task));
            tasks.add(scheduled);
            return scheduled;
        }

        boolean fireAll() {
            List<Task> due = new ArrayList<>(tasks);
            tasks.clear();
            boolean fired = false;
            for (Task task : due) {
                if (!task.cancelled) {
                    fired = true;
                    task.runnable.run();
                }
            }
            return fired;
        }

        int pending() {
            int count = 0;
            for (Task task : tasks) {
                if (!task.cancelled) {
                    count++;
                }
            }
            return count;
        }
    }

    /** Virtual-time executor: records deadlines and never runs them implicitly. */
    private static final class ManualEventLoop extends AbstractExecutorService
            implements ScheduledExecutorService {

        private final List<FakeScheduledTask> scheduled = new ArrayList<>();
        private boolean shutdown;

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (shutdown) {
                throw new RejectedExecutionException("manual event loop is shut down");
            }
            FakeScheduledTask task = new FakeScheduledTask(command);
            scheduled.add(task);
            return task;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("the harness drains its own loop queue");
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are Runnable-only");
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are one-shot");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Raft deadlines are one-shot");
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

    private static final class FakeScheduledTask extends FutureTask<Void> implements ScheduledFuture<Void> {

        private FakeScheduledTask(Runnable command) {
            super(Objects.requireNonNull(command), null);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    // -------------------------------------------------------------------------------------------
    // Booking fixtures
    // -------------------------------------------------------------------------------------------

    private static final class StubElectionService implements ElectionService {

        private final PersistentRaftNode node;
        private final RaftState state;
        private final String localId;

        private StubElectionService(PersistentRaftNode node, RaftState state, String localId) {
            this.node = node;
            this.state = state;
            this.localId = localId;
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
            throw new UnsupportedOperationException("D19 follower never campaigns");
        }

        @Override
        public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
            return Optional.empty();
        }

        @Override
        public RaftNode.RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) {
            throw new UnsupportedOperationException("D19 follower never handles REQUEST_VOTE");
        }

        @Override
        public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
            throw new UnsupportedOperationException("D19 follower never counts votes");
        }

        @Override
        public void promoteToLeader(Set<String> peers) {
            throw new UnsupportedOperationException("D19 follower never promotes");
        }
    }

    /**
     * Transactional in-memory repository with the same replay contract as the JDBC repository:
     * an already applied index returns its stored outcome instead of applying twice.
     */
    private static final class InMemoryBookingRepository implements BookingRepository {

        private record AppliedEntry(long index, BookingCommand command) {
        }

        private BookingStateMachine machine = new BookingStateMachine();
        private final List<AppliedEntry> applied = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();

        @Override
        public long lastApplied() {
            return machine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
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
    }
}
