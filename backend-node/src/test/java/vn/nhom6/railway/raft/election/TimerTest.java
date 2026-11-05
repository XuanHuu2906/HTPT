package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/** Real timer/candidate with virtual time and fake core/storage/transport; no cluster runtime. */
class TimerTest {
    private final FakeScheduler clock = new FakeScheduler();
    private final FakeNode node = new FakeNode();
    private final List<Exception> failures = new ArrayList<>();
    private final List<Sent> sent = new ArrayList<>();
    private Runnable onLeader = () -> { };
    private final CandidateElection election = new CandidateElection("node-1", node, node.state,
            (peer, request, reply) -> {
                assertEquals(node.savedTerm, request.getTerm(), "persist before sending votes");
                assertEquals("node-1", node.savedVote);
                sent.add(new Sent(peer, request, reply));
            }, term -> node.state.becomeFollower(term), () -> onLeader.run());

    private record Sent(String peer, RequestVoteRequestDto request, CandidateElection.VoteReply reply) { }

    private ElectionTimer timer(long... intervals) {
        int[] draw = {0};
        ElectionTimer timer = new ElectionTimer(election, clock,
                () -> intervals[Math.min(draw[0]++, intervals.length - 1)], failures::add);
        onLeader = timer::stop;
        return timer;
    }

    @Test
    void timeoutTriggersCandidateThroughCoreAndRetriesWithFreshDelay() {
        ElectionTimer timer = timer(500, 1_000);
        timer.start();
        clock.advance(499);
        assertEquals(Role.FOLLOWER, node.role());
        clock.advance(1);
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(1, node.currentTerm());
        assertEquals("node-1", node.state.votedFor());
        assertEquals(4, sent.size());
        assertTrue(sent.stream().allMatch(s -> s.request().getTerm() == 1));
        clock.advance(999);
        assertEquals(1, node.timeouts);
        clock.advance(1);
        assertEquals(2, node.timeouts);
        assertEquals(2, node.currentTerm());
        assertEquals(8, sent.size());
        assertTrue(sent.subList(4, 8).stream().allMatch(s -> s.request().getTerm() == 2));
        assertTrue(failures.isEmpty());
    }

    @Test
    void oneDeadlineStartsOneCampaignAndPromotionCancelsFurtherTimeouts() throws Exception {
        ElectionTimer timer = timer(500);
        timer.start();
        FakeTask expired = clock.tasks.peek();
        clock.advance(500);
        expired.command.run(); // An already delivered deadline must not run another campaign.

        assertEquals(1, node.timeouts);
        assertEquals(1, node.currentTerm());
        assertEquals(4, sent.size());
        assertEquals(Set.of("node-2", "node-3", "node-4", "node-5"),
                Set.copyOf(sent.stream().map(Sent::peer).toList()));
        assertTrue(sent.stream().allMatch(s -> s.request().getTerm() == 1));

        sent.get(0).reply().receive(new RaftNode.RequestVoteResponse(1, true));
        assertEquals(Role.CANDIDATE, node.role());
        sent.get(1).reply().receive(new RaftNode.RequestVoteResponse(1, true));
        assertEquals(Role.LEADER, node.role());
        clock.advance(2_000);
        assertEquals(1, node.timeouts);
        assertEquals(4, sent.size());
        assertTrue(failures.isEmpty());
    }

    @Test
    void validHeartbeatResetsDeadlineAndInvalidatesAlreadyQueuedCallback() {
        ElectionTimer timer = timer(500, 700);
        timer.start();
        FakeTask old = clock.tasks.peek();
        clock.advance(400);
        node.state.becomeFollower(1, "node-2");
        timer.onValidAppendEntries(1);
        assertTrue(old.isCancelled());
        old.command.run(); // Simulate a callback delivered despite cancel(false).
        clock.advance(699);
        assertEquals(0, node.timeouts);
        clock.advance(1);
        assertEquals(1, node.timeouts);
    }

    @Test
    void validLeaderContactResetsEvenWhenLogDoesNotMatch() throws Exception {
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(400);
        var request = new RaftNode.AppendEntriesRequest(2, "node-2", 7, 1, List.of(), 0);
        var response = node.handleAppendEntries(request);
        assertFalse(response.success());
        timer.onValidAppendEntries(request.term());
        clock.advance(499);
        assertEquals(0, node.timeouts);
        clock.advance(1);
        assertEquals(1, node.timeouts);
    }

    @Test
    void candidateStepsDownOnSameTermHeartbeatBeforeReset() {
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(500);
        clock.advance(400);
        node.state.becomeFollower(1, "node-2");
        timer.onValidAppendEntries(1);
        clock.advance(499);
        assertEquals(Role.FOLLOWER, node.role());
        clock.advance(1);
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(2, node.timeouts);
    }

    @Test
    void staleHeartbeatAndRejectedVoteDoNotPostponeElection() {
        node.state.becomeFollower(3);
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(400);
        timer.onValidAppendEntries(2);
        timer.onVoteDecision(3, false);
        timer.onVoteDecision(2, true); // Delayed grant notification from an older term.
        clock.advance(100);
        assertEquals(1, node.timeouts);
        assertEquals(4, node.currentTerm());
    }

    @Test
    void unprocessedHigherTermCannotResetTimer() {
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(400);
        timer.onValidAppendEntries(8);
        timer.onVoteDecision(8, true);
        clock.advance(100);
        assertEquals(1, node.timeouts);
    }

    @Test
    void higherTermRejectedVoteDoesNotResetEvenAfterStepDown() {
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(400);
        node.state.becomeFollower(9);
        timer.onVoteDecision(9, false);
        clock.advance(100);
        assertEquals(1, node.timeouts);
        assertEquals(10, node.currentTerm());
    }

    @Test
    void grantedVoteResetsDeadline() {
        ElectionTimer timer = timer(500, 800);
        timer.start();
        clock.advance(400);
        assertTrue(node.state.grantVote(1, "node-2", 0, 0));
        // Fake core has no DB: this hook represents a completed durable grant.
        timer.onVoteDecision(1, true);
        clock.advance(799);
        assertEquals(0, node.timeouts);
        clock.advance(1);
        assertEquals(1, node.timeouts);
    }

    @Test
    void repeatedStartDoesNotPostponeDeadline() {
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(400);
        timer.start();
        clock.advance(100);
        assertEquals(1, node.timeouts);
    }

    @Test
    void stopInvalidatesOldCallbacksAndCanRestartExplicitly() {
        ElectionTimer timer = timer(500);
        timer.start();
        FakeTask old = clock.tasks.peek();
        timer.stop();
        timer.stop();
        timer.onValidAppendEntries(0);
        timer.onVoteDecision(0, true);
        clock.advance(2_000);
        assertEquals(0, node.timeouts);
        timer.start();
        old.command.run();
        assertEquals(0, node.timeouts);
        clock.advance(500);
        assertEquals(1, node.timeouts);
    }

    @Test
    void closePreventsRestartAndLeavesSharedExecutorAlive() {
        ElectionTimer timer = timer(500);
        timer.start();
        FakeTask old = clock.tasks.peek();
        timer.close();
        timer.close();
        timer.onValidAppendEntries(0);
        timer.onVoteDecision(0, true);
        old.command.run();
        clock.advance(2_000);
        assertEquals(0, node.timeouts);
        assertThrows(IllegalStateException.class, timer::start);
        assertFalse(clock.isShutdown());
    }

    @Test
    void leaderDoesNotStartOrContinueElectionTimer() {
        ElectionTimer timer = timer(500);
        timer.start();
        node.state.startElection("node-1");
        node.state.becomeLeader(Set.of("node-2", "node-3", "node-4", "node-5"));
        timer.onValidAppendEntries(1);
        timer.onVoteDecision(1, true);
        clock.advance(500);
        assertEquals(0, node.timeouts);
        timer.start();
        assertTrue(clock.tasks.isEmpty());
        node.state.becomeFollower(2);
        timer.start();
        clock.advance(500);
        assertEquals(1, node.timeouts);
    }

    @Test
    void storageFailureStopsTimerAndReportsOriginalCause() {
        node.failure = new RaftStorageException("fake storage unavailable");
        ElectionTimer timer = timer(500);
        timer.start();
        clock.advance(2_000);
        assertEquals(1, node.timeouts);
        assertEquals(List.of(node.failure), failures);
        assertEquals(Role.FOLLOWER, node.role());
        assertTrue(sent.isEmpty(), "failed durable transition cannot send RequestVote");
        assertThrows(IllegalStateException.class, timer::start);
    }

    @Test
    void callbackCanStopTimerWithoutBeingRearmed() {
        ElectionTimer timer = timer(500);
        node.afterTimeout = timer::stop;
        timer.start();
        clock.advance(2_000);
        assertEquals(1, node.timeouts);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void invalidPolicyFailsClosed(long interval) {
        ElectionTimer timer = timer(interval);
        assertThrows(IllegalArgumentException.class, timer::start);
        assertThrows(IllegalStateException.class, timer::start);
        assertTrue(clock.tasks.isEmpty());
    }

    @Test
    void schedulerRejectionFailsClosed() {
        ElectionTimer timer = timer(500);
        clock.shutdown();
        assertThrows(RejectedExecutionException.class, timer::start);
        assertThrows(IllegalStateException.class, timer::start);
    }

    @Test
    void rearmFailureIsReportedInsteadOfSilentlyLosingScheduledTask() {
        ElectionTimer timer = timer(500, 0);
        timer.start();
        clock.advance(2_000);
        assertEquals(1, node.timeouts);
        assertEquals(1, failures.size());
        assertInstanceOf(IllegalArgumentException.class, failures.getFirst());
        assertThrows(IllegalStateException.class, timer::start);
    }

    @Test
    void productionConstructorUsesConfiguredRandomizedInterval() {
        ElectionTimer timer = new ElectionTimer(election, clock, ElectionConfig.defaults(), failures::add);
        timer.start();
        long deadline = clock.tasks.peek().deadline;
        assertTrue(deadline >= 500 && deadline <= 1_000);
        clock.advance(deadline - 1);
        assertEquals(0, node.timeouts);
        clock.advance(1);
        assertEquals(1, node.timeouts);
        timer.close();
    }

    /** Single-thread virtual event loop; no sleeps, wall clock, network or durable storage. */
    private static final class FakeScheduler extends AbstractExecutorService
            implements ScheduledExecutorService {
        private final PriorityQueue<FakeTask> tasks = new PriorityQueue<>();
        private long now;
        private boolean shutdown;

        void advance(long millis) {
            long target = now + millis;
            while (!tasks.isEmpty() && tasks.peek().deadline <= target) {
                FakeTask next = tasks.remove();
                now = next.deadline;
                next.run();
                if (!next.isCancelled()) {
                    // FutureTask captures exceptions; surface them to the test.
                    assertDoesNotThrow(() -> next.get());
                }
            }
            now = target;
        }

        @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (shutdown) throw new RejectedExecutionException("fake executor stopped");
            FakeTask task = new FakeTask(command, now + unit.toMillis(delay), this);
            tasks.add(task);
            return task;
        }
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && tasks.isEmpty(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return isTerminated(); }
        @Override public void execute(Runnable command) { throw new UnsupportedOperationException(); }
        @Override public <V> ScheduledFuture<V> schedule(Callable<V> task, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
        @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initial, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("election deadlines must be one-shot");
        }
        @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long initial, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("election deadlines must be one-shot");
        }
    }

    private static final class FakeTask extends FutureTask<Void> implements ScheduledFuture<Void> {
        private final Runnable command;
        private final long deadline;
        private final FakeScheduler clock;

        FakeTask(Runnable command, long deadline, FakeScheduler clock) {
            super(command, null);
            this.command = command;
            this.deadline = deadline;
            this.clock = clock;
        }
        @Override public long getDelay(TimeUnit unit) {
            return unit.convert(deadline - clock.now, TimeUnit.MILLISECONDS);
        }
        @Override public int compareTo(Delayed other) {
            return Long.compare(deadline, ((FakeTask) other).deadline);
        }
    }

    /** Only the core model is real. Persistence/transport and the runtime loop are not tested. */
    private static final class FakeNode implements RaftNode {
        private final RaftState state = new RaftState();
        private int timeouts;
        private long savedTerm;
        private String savedVote;
        private RaftStorageException failure;
        private Runnable afterTimeout = () -> { };

        @Override public void onElectionTimeout() throws RaftStorageException {
            timeouts++;
            if (failure != null) throw failure;
            state.startElection("node-1");
            // Test-only durable-write marker; there is no PostgreSQL in this fixture.
            savedTerm = state.currentTerm();
            savedVote = state.votedFor();
            afterTimeout.run();
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            state.becomeFollower(request.term(), request.leaderId());
            return new AppendEntriesResponse(state.currentTerm(), false, 0); // Fake log mismatch.
        }
        @Override public Role role() { return state.role(); }
        @Override public long currentTerm() { return state.currentTerm(); }
        @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
        @Override public long commitIndex() { return state.commitIndex(); }
        @Override public long lastApplied() { return state.lastApplied(); }
        @Override public long lastLogIndex() { return state.lastLogIndex(); }
        @Override public void onApplied(long index) { throw new UnsupportedOperationException(); }
        @Override public AppendOutcome appendCommand(String type, String payload) { throw new UnsupportedOperationException(); }
        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request) { throw new UnsupportedOperationException(); }
        @Override public void addCommitListener(CommitListener listener) { throw new UnsupportedOperationException(); }
    }
}
