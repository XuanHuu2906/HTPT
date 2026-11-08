package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftLogEntry;
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

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/** Five real election/handler/state instances; virtual scheduling and memory storage, no TCP/DB. */
class SplitVoteTest {
    @Test
    void twoCandidatesInSameTermCannotBothWinEvenWithDuplicateAndReorderedReplies() throws Exception {
        Cluster c = new Cluster();
        c.startTwoCandidates();
        assertEquals(1, c.node("node-1").core.currentTerm());
        assertEquals(1, c.node("node-2").core.currentTerm());
        c.deliver("node-1", "node-2", 1);
        c.deliver("node-2", "node-1", 1);
        Sent first = c.deliver("node-1", "node-3", 1);
        first.reply.receive(new RaftNode.RequestVoteResponse(1, true)); // Duplicate real grant.
        c.deliver("node-2", "node-4", 1);
        c.deliver("node-2", "node-3", 1); // Already voted for node-1.
        assertEquals(0, c.leaders());
        c.deliver("node-1", "node-5", 1);
        c.deliver("node-2", "node-5", 1); // Cannot vote twice in term 1.
        c.deliver("node-1", "node-4", 1);
        assertEquals(1, c.leaders());
        assertEquals(Role.LEADER, c.node("node-1").core.role());
        assertEquals(Role.CANDIDATE, c.node("node-2").core.role());
        assertEquals("node-1", c.node("node-5").core.storage.vote);
        c.close();
    }

    @Test
    void splitVoteRetriesAtFreshDeadlinesInNewTermAndDiscardsOldGrants() throws Exception {
        Cluster c = new Cluster();
        c.startTwoCandidates();
        c.deliver("node-1", "node-2", 1);
        c.deliver("node-2", "node-1", 1);
        c.deliver("node-1", "node-3", 1);
        c.deliver("node-2", "node-4", 1);
        // node-5 is unreachable: both have only two votes.
        assertEquals(0, c.leaders());
        c.clock.advance(499);
        assertEquals(1, c.node("node-1").core.timeouts);
        c.clock.advance(1); // node-1 redraws 500ms; node-2 redraws 900ms.
        assertEquals(2, c.node("node-1").core.currentTerm());
        assertEquals(1, c.node("node-2").core.currentTerm());
        c.deliver("node-1", "node-4", 1); // Late grant for abandoned campaign.
        c.deliver("node-1", "node-2", 2);
        assertEquals(0, c.leaders(), "old grant must not combine with term-2 votes");
        c.deliver("node-1", "node-3", 2);
        assertEquals(1, c.leaders());
        assertEquals(Role.LEADER, c.node("node-1").core.role());
        assertEquals(Role.FOLLOWER, c.node("node-2").core.role());
        assertEquals(2, c.node("node-2").core.storage.term);
        assertEquals("node-1", c.node("node-2").core.storage.vote);
        c.close();
        c.clock.advance(10_000);
        assertEquals(2, c.node("node-1").core.timeouts);
    }

    @Test
    void randomizedPolicyDrawsFreshIntervalForEveryRetry() {
        Cluster c = new Cluster();
        Member a = c.node("node-1");
        var config = ElectionConfig.defaults();
        var expected = ElectionTimeoutPolicy.randomized(config, new java.util.Random(7));
        var actual = ElectionTimeoutPolicy.randomized(config, new java.util.Random(7));
        ElectionRetryCoordinator retries = new ElectionRetryCoordinator(a.election, c.clock,
                actual, failure -> fail(failure));
        retries.start();
        for (int term = 1; term <= 5; term++) {
            long interval = expected.nextTimeoutMillis();
            assertTrue(interval >= 500 && interval <= 1_000);
            c.clock.advance(interval - 1);
            assertEquals(term - 1, a.core.timeouts);
            c.clock.advance(1);
            assertEquals(term, a.core.currentTerm());
            assertEquals(4 * term, c.sent.size());
        }
        retries.close();
        c.close();
    }

    @Test
    void missingQuorumWaitsForDeadlinesInsteadOfSpinningOrCountingOfflinePeers() throws Exception {
        Cluster c = new Cluster();
        Member a = c.node("node-1");
        a.timer.start();
        c.clock.advance(500);
        c.deliver("node-1", "node-2", 1); // Only 2/5 can communicate.
        assertEquals(0, c.leaders());
        c.clock.advance(499);
        assertEquals(1, a.core.timeouts);
        assertEquals(4, c.sent.size());
        c.clock.advance(1);
        assertEquals(2, a.core.timeouts);
        assertEquals(8, c.sent.size());
        c.clock.advance(10_000);
        assertEquals(22, a.core.timeouts, "exactly one campaign per 500ms deadline");
        assertEquals(88, c.sent.size(), "four bounded sends per campaign");
        assertEquals(Role.CANDIDATE, a.core.role());
        assertEquals(0, c.leaders());
        assertEquals(1, c.clock.tasks.stream().filter(t -> !t.isCancelled()).count());
        c.close();
    }

    private record Sent(String candidate, String peer, RequestVoteRequestDto request,
            CandidateElection.VoteReply reply) { }

    private static final class Cluster {
        final FakeScheduler clock = new FakeScheduler();
        final Map<String, Member> members = new LinkedHashMap<>();
        final List<Sent> sent = new ArrayList<>();

        Cluster() {
            for (int i = 1; i <= 5; i++) {
                String id = "node-" + i;
                members.put(id, new Member(this, id));
            }
        }
        Member node(String id) { return members.get(id); }
        long leaders() { return members.values().stream().filter(m -> m.core.role() == Role.LEADER).count(); }
        void startTwoCandidates() {
            node("node-1").timer.start();
            node("node-2").timer.start();
            clock.advance(500); // Both start term 1 before any queued RPC is delivered.
        }
        Sent deliver(String candidate, String peer, long term) throws Exception {
            Sent rpc = sent.stream().filter(s -> s.candidate.equals(candidate) && s.peer.equals(peer)
                    && s.request.getTerm() == term).findFirst().orElseThrow();
            var response = node(peer).election.handleRequestVote(rpc.request);
            assertEquals(node(peer).core.state.currentTerm(), node(peer).core.storage.term);
            assertEquals(node(peer).core.state.votedFor(), node(peer).core.storage.vote);
            rpc.reply.receive(response);
            assertTrue(leaders() <= 1, "no two leaders in these same-term scenarios");
            return rpc;
        }
        void close() { members.values().forEach(m -> m.timer.close()); }
    }

    private static final class Member {
        final FakeNode core;
        final CandidateElection election;
        final ElectionTimer timer;
        int draws;
        Member(Cluster cluster, String self) {
            core = new FakeNode(self);
            election = new CandidateElection(self, core, core.state, (peer, request, reply) -> {
                assertEquals(request.getTerm(), core.storage.term, "persist self-vote before send");
                assertEquals(self, core.storage.vote);
                cluster.sent.add(new Sent(self, peer, request, reply));
            }, term -> {
                core.state.becomeFollower(term);
                core.storage.saveTermAndVote(term, null);
                restartTimer();
            }, this::stopTimer);
            timer = new ElectionTimer(election, cluster.clock,
                    () -> ++draws == 1 || !self.equals("node-2") ? 500 : 900,
                    failure -> fail(failure));
            core.handler = new RequestVoteHandler(core.state, core.storage, timer);
        }
        void stopTimer() { timer.stop(); }
        void restartTimer() { timer.start(); }
    }

    private static final class MemoryStorage implements RaftStorage {
        long term;
        String vote;
        @Override public void saveTermAndVote(long term, String vote) {
            assertTrue(term >= this.term);
            if (term == this.term && this.vote != null) assertEquals(this.vote, vote);
            this.term = term;
            this.vote = vote;
        }
        @Override public PersistentState load() { return new PersistentState(term, vote, List.of(), 0); }
        @Override public long loadLastApplied() { return 0; }
        @Override public void replaceSuffix(long from, List<RaftLogEntry> entries) {
            throw new UnsupportedOperationException();
        }
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

    private static final class FakeNode implements RaftNode {
        private final String self;
        private RequestVoteHandler handler;
        private final MemoryStorage storage = new MemoryStorage();
        FakeNode(String self) { this.self = self; }
        private final RaftState state = new RaftState();
        private int timeouts;
        private long savedTerm;
        private String savedVote;
        private RaftStorageException failure;
        private Runnable afterTimeout = () -> { };

        @Override public void onElectionTimeout() throws RaftStorageException {
            timeouts++;
            if (failure != null) throw failure;
            state.startElection(self);
            storage.saveTermAndVote(state.currentTerm(), state.votedFor());
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
        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws RaftStorageException { return handler.handle(request); }
        @Override public void addCommitListener(CommitListener listener) { throw new UnsupportedOperationException(); }
    }
}
