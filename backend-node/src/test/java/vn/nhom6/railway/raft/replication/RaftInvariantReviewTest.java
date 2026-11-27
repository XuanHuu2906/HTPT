package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftInvariantReviewTest {

    private static final String LOCAL = "node-1";
    private static final Set<String> PEERS = Set.of("node-2", "node-3", "node-4", "node-5");

    @Test
    void lateReplyFromOldLeaderTermCannotAdvanceMatchIndex() {
        RaftState state = leaderState(2L, 1);
        RaftReplicationService service = replicationService(state);
        RaftNode.AppendEntriesRequest oldRequest = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(state.entryAt(1L)), 0L);

        state.becomeFollower(2L);
        state.startElection(LOCAL);
        state.becomeLeader(PEERS);

        service.handleAppendResult("node-2", oldRequest,
                new RaftTransportAdapter.Replied("node-2", oldRequest,
                        new RaftNode.AppendEntriesResponse(1L, true, 1L)));

        assertEquals(Role.LEADER, state.role());
        assertEquals(3L, state.currentTerm());
        assertEquals(0L, state.matchIndexOf("node-2"));
    }

    @Test
    void stepDownAndCommitAdvanceAreSerializedByLifecycleOutcome() throws Exception {
        RaftState state = leaderState(2L, 3);
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS);
        for (String peer : List.of("node-2", "node-3")) {
            state.advanceMatchIndex(peer, 3L);
        }
        CommitAdvancer advancer = new CommitAdvancer(state, tracker);

        CountDownLatch steppedDown = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Void> stepDown = CompletableFuture.runAsync(() -> {
                state.becomeFollower(3L);
                steppedDown.countDown();
            }, executor);
            CompletableFuture<Boolean> advance = CompletableFuture.supplyAsync(() -> {
                try {
                    steppedDown.await(2L, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return advancer.advance().isPresent();
            }, executor);

            stepDown.get(2L, TimeUnit.SECONDS);
            assertEquals(false, advance.get(2L, TimeUnit.SECONDS));
            assertEquals(0L, state.commitIndex());
            assertEquals(Role.FOLLOWER, state.role());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void leadershipLostCancelsPendingReadBarrier() throws Exception {
        BarrierPort port = new BarrierPort(LOCAL, 4L);
        CompletableFuture<Void> awaiterFuture = new CompletableFuture<>();
        RaftReadBarrier barrier = new RaftReadBarrier(
                port,
                index -> awaiterFuture,
                (task, delayMillis) -> new NoopCancellable());

        CompletionStage<BookingRequestHandler.ReadBarrierResult> stage =
                barrier.appendAndAwaitNoOp(Duration.ofMillis(200L));
        assertEquals(1, barrier.pendingCount());

        barrier.onLeadershipLost();

        assertEquals(0, barrier.pendingCount());
        assertTrue(stage.toCompletableFuture().isCompletedExceptionally());
        Throwable failure = stage.toCompletableFuture().handle((ignored, error) -> error)
                .get(2L, TimeUnit.SECONDS);
        assertInstanceOf(BookingRequestHandler.NotLeaderException.class, failure);
    }

    @Test
    void leaderPeerIndicesInitializeFromLocalLastLogIndex() {
        RaftState state = leaderState(5L, 5);

        assertEquals(5L, state.lastLogIndex());
        for (String peer : PEERS) {
            assertEquals(6L, state.nextIndexOf(peer));
            assertEquals(0L, state.matchIndexOf(peer));
        }
    }

    @Test
    void restartAndApplyProgressRemainFailClosed() {
        RaftLogEntry first = RaftLogEntry.noOp(1L, 3L);
        RaftLogEntry second = RaftLogEntry.noOp(2L, 3L);
        RaftState state = RaftState.restore(3L, "node-1", List.of(first, second), 1L);

        assertEquals(1L, state.commitIndex());
        assertEquals(1L, state.lastApplied());
        assertEquals(2L, state.lastLogIndex());
        assertTrue(state.commitIndex() <= state.lastLogIndex());
    }

    private static RaftState leaderState(long term, int entries) {
        RaftState state = RaftState.restore(term - 1L, null, List.of(), 0L);
        state.startElection(LOCAL);
        for (int index = 1; index <= entries; index++) {
            state.append(RaftLogEntry.noOp(index, term));
        }
        state.becomeLeader(PEERS);
        return state;
    }

    private static RaftReplicationService replicationService(RaftState state) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        RaftTransportAdapter transport = new RaftTransportAdapter(
                LOCAL,
                (peerId, request) -> new CompletableFuture<>(),
                Duration.ofSeconds(1L),
                Runnable::run,
                scheduler,
                failure -> {
                });
        return new RaftReplicationService(
                LOCAL, state, transport, PEERS, Runnable::run,
                (task, delayMillis) -> new NoopCancellable(),
                64, new RaftReplicationService.Backoff(10L, 100L),
                new RaftReplicationService.Hooks() {
                });
    }

    private record NoopCancellable() implements RaftReplicationService.Scheduler.Cancellable {
        @Override
        public void cancel() {
        }
    }

    private static final class BarrierPort implements RaftReadBarrier.Port {
        private final String localId;
        private final long term;
        private long nextIndex;

        private BarrierPort(String localId, long term) {
            this.localId = localId;
            this.term = term;
            this.nextIndex = 1L;
        }

        @Override
        public String localId() {
            return localId;
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public long currentTerm() {
            return term;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.of(localId);
        }

        @Override
        public RaftNode.AppendOutcome appendNoOp() {
            long index = nextIndex++;
            return new RaftNode.Accepted(index, term);
        }

        @Override
        public long commitIndex() {
            return nextIndex - 1L;
        }

        @Override
        public OptionalLong termAt(long index) {
            return OptionalLong.of(term);
        }
    }
}
