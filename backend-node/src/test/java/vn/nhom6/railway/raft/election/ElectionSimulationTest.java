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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** D09 election simulation only: five shared core states, fake storage/network, no TCP/DB. */
class ElectionSimulationTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void majorityElectsWhileMinorityRetriesWithoutQuorum(int firstTimeout) {
        Network network = new Network();
        for (int i = 0; i < 5; i++) {
            // Rotate timeout ordering, never assign a leader role in the fixture.
            long interval = i < 3 ? 500 + ((i + firstTimeout) % 3) * 100 : 50;
            network.add("node-" + (i + 1), interval);
        }
        network.nodes.values().forEach(n -> n.timer.start());
        network.clock.advance(49);
        assertTrue(network.nodes.values().stream().allMatch(n -> n.core.role() == Role.FOLLOWER));
        // Stop at the first majority election, before heartbeat/replication is needed.
        for (int elapsed = 50; elapsed <= 500; elapsed++) {
            network.clock.advance(1);
            network.drain();
            network.assertMinorityHasNoLeader();
        }
        var leaders = network.nodes.values().stream().filter(n -> n.core.role() == Role.LEADER).toList();
        assertEquals(1, leaders.size());
        SimNode winner = leaders.getFirst();
        assertTrue(network.majority(winner.id));
        assertEquals(1, winner.core.currentTerm());
        assertEquals(3, network.nodes.values().stream()
                .filter(n -> network.majority(n.id) && winner.id.equals(n.core.state.votedFor())).count());
        assertEquals(4, winner.core.state.nextIndex().size());
        for (SimNode n : network.nodes.values()) {
            assertSame(n.core.state, n.election.state());
            assertSame(n.core, n.election.node());
            assertEquals(n.core.state.currentTerm(), n.core.savedTerm);
            assertEquals(n.core.state.votedFor(), n.core.savedVote);
            if (!network.majority(n.id)) {
                assertTrue(n.core.currentTerm() >= 3, "minority must actually retry election timeouts");
                assertEquals(Role.CANDIDATE, n.core.role());
            }
            n.timer.close();
        }
        assertTrue(network.dropped > 0, "partition must drop cross-group traffic");
        System.out.printf("D09 rotation=%d leader=%s term=%d grants=3/5 minorityTerms=%d,%d minorityLeaders=0 dropped=%d%n",
                firstTimeout, winner.id, winner.core.currentTerm(),
                network.nodes.get("node-4").core.currentTerm(), network.nodes.get("node-5").core.currentTerm(),
                network.dropped);
    }

    private static final class Network {
        final FakeScheduler clock = new FakeScheduler();
        final Map<String, SimNode> nodes = new LinkedHashMap<>();
        final ArrayDeque<Runnable> messages = new ArrayDeque<>();
        int dropped;

        boolean majority(String id) { return Integer.parseInt(id.substring(5)) <= 3; }

        void add(String id, long interval) { nodes.put(id, new SimNode(id, interval, this)); }

        void send(String from, String to, Runnable delivery) {
            if (majority(from) != majority(to)) { dropped++; return; }
            messages.add(delivery); // Never invoke transport callbacks inline.
        }

        void drain() {
            int budget = 1_000;
            while (!messages.isEmpty()) {
                assertTrue(budget-- > 0, "message loop must terminate");
                messages.remove().run();
            }
        }

        void assertMinorityHasNoLeader() {
            assertTrue(nodes.values().stream().filter(n -> !majority(n.id))
                    .noneMatch(n -> n.core.role() == Role.LEADER));
        }
    }

    private static final class SimNode {
        final String id;
        final FakeCore core;
        final CandidateElection election;
        final ElectionTimer timer;

        SimNode(String id, long interval, Network network) {
            this.id = id;
            core = new FakeCore(id, new RaftState(), new ArrayList<>());
            election = new CandidateElection(id, core, core.state, (peer, request, reply) -> {
                assertEquals(request.getTerm(), core.savedTerm);
                assertEquals(id, core.savedVote);
                network.send(id, peer, () -> assertDoesNotThrow(() -> {
                    SimNode recipient = network.nodes.get(peer);
                    var response = recipient.election.handleRequestVote(request);
                    recipient.timer.onVoteDecision(request.getTerm(), response.voteGranted());
                    network.send(peer, id, () -> assertDoesNotThrow(() -> reply.receive(response)));
                }));
            }, core::observeHigherTerm, this::onLeader);
            timer = new ElectionTimer(election, network.clock, () -> interval,
                    failure -> { throw new AssertionError("unexpected timer failure on " + id, failure); });
        }

        void onLeader() { timer.stop(); }
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

    /** Test-only core/storage adapter. Recorded writes are not PostgreSQL durability evidence. */
    private static final class FakeCore implements RaftNode {
        final String self;
        final RaftState state;
        final List<String> events;
        long savedTerm;
        String savedVote;
        boolean failSave;

        FakeCore(String self, RaftState state, List<String> events) {
            this.self = self;
            this.state = state;
            this.events = events;
            savedTerm = state.currentTerm();
            savedVote = state.votedFor();
        }

        private void persist() throws RaftStorageException {
            if (failSave) {
                // Deliberately fail after the in-memory mutation to test fail-closed handling.
                throw new RaftStorageException("injected storage failure");
            }
            savedTerm = state.currentTerm();
            savedVote = state.votedFor();
            events.add("persist:" + savedTerm + ":" + savedVote);
        }

        void observeHigherTerm(long term) throws RaftStorageException {
            state.becomeFollower(term);
            persist();
        }

        @Override public void onElectionTimeout() throws RaftStorageException {
            state.startElection(self);
            persist();
        }

        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
                throws RaftStorageException {
            long beforeTerm = state.currentTerm();
            String beforeVote = state.votedFor();
            boolean grant = VoteRules.grantVote(state, request.term(), request.candidateId(),
                    request.lastLogIndex(), request.lastLogTerm());
            if (beforeTerm != state.currentTerm() || !Objects.equals(beforeVote, state.votedFor())) {
                persist();
            }
            return new RequestVoteResponse(state.currentTerm(), grant);
        }

        @Override public Role role() { return state.role(); }
        @Override public long currentTerm() { return state.currentTerm(); }
        @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
        @Override public long commitIndex() { return state.commitIndex(); }
        @Override public long lastApplied() { return state.lastApplied(); }
        @Override public long lastLogIndex() { return state.lastLogIndex(); }
        @Override public void onApplied(long index) { throw new UnsupportedOperationException(); }
        @Override public AppendOutcome appendCommand(String type, String payload) {
            throw new UnsupportedOperationException();
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public void addCommitListener(CommitListener listener) {
            throw new UnsupportedOperationException();
        }
    }
}
