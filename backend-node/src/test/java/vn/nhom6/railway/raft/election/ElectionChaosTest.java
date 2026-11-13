package vn.nhom6.railway.raft.election;

import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.*;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import static org.junit.jupiter.api.Assertions.*;

/** D13: serialized event-loop interleavings over FakeNetwork, not TCP or G4.
 * JDBC smoke uses TV2 storage directly; no dependency on the TV4-D13 adapter.
 */
class ElectionChaosTest {
    @Test
    void fiveSimultaneousCandidatesSplitThenRetryWithDelayedAndDuplicateReplies() throws Exception {
        FakeNetwork network = new FakeNetwork();
        try {
            network.members.values().forEach(m -> m.timer.start());
            network.clock.advance(500); // All five self-vote before delivering any RPC.
            assertEquals(20, network.sent.size());
            var delayed = new ArrayList<>(network.sent);
            for (Sent rpc : delayed) {
                assertEquals(1, network.node(rpc.candidate).core.currentTerm());
                network.deliver(rpc.candidate, rpc.peer, 1);
            }
            assertEquals(0, network.leaders(), "five self-votes cannot form a majority");
            // Stop timers to choose a fair retry schedule deterministically.
            network.members.values().forEach(m -> m.timer.stop());
            network.node("node-3").election.startElection();
            for (Sent rpc : delayed) {
                network.deliver(rpc.candidate, rpc.peer, 1); // Stale requests/responses.
            }
            assertEquals(0, network.leaders());
            Sent grant = network.deliver("node-3", "node-1", 2);
            grant.reply.receive(new RaftNode.RequestVoteResponse(2, true));
            assertEquals(0, network.leaders(), "self + duplicated peer is only 2/5");
            network.deliver("node-3", "node-2", 2);
            assertEquals(1, network.leaders());
            assertEquals(Role.LEADER, network.node("node-3").core.role());
            for (String peer : List.of("node-4", "node-5")) network.deliver("node-3", peer, 2);
            assertEquals(1, network.leaders());
            System.out.println("D13 five simultaneous candidates: term=1 split; term=2 leader=node-3 quorum=3/5; stale/duplicate replies safe");
        } finally { network.close(); }
    }

    @Test
    void twoSimultaneousCandidatesRemainSafeAcrossSeededNetworkOrders() throws Exception {
        for (int seed = 0; seed < 100; seed++) {
            FakeNetwork network = new FakeNetwork();
            try {
                network.startTwoCandidates();
                List<Sent> order = new ArrayList<>(network.sent);
                Collections.shuffle(order, new Random(seed));
                for (Sent rpc : order) {
                    Sent delivered = network.deliver(rpc.candidate, rpc.peer, 1);
                    // Replay actual response by redelivering the request, including denials.
                    network.deliver(delivered.candidate, delivered.peer, 1);
                }
                assertEquals(1, network.leaders(), "all peers reachable; seed=" + seed);
                var winner = network.members.values().stream()
                        .filter(m -> m.core.role() == Role.LEADER).findFirst().orElseThrow();
                assertTrue(network.members.values().stream()
                        .filter(m -> Objects.equals(m.core.storage.vote, winner.core.self)).count() >= 3);
            } finally { network.close(); }
        }
        System.out.println("D13 FakeNetwork seeds=0..99: same-term unique leader with >=3 distinct memory-storage votes");
    }

    @Test
    void jdbcTermVoteSmokeReloadsAndRejectsSecondCandidateInSameTerm() throws Exception {
        String url = System.getenv("RAILWAY_TEST_JDBC_URL");
        String user = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        boolean configured = url != null && !url.isBlank() && user != null && !user.isBlank()
                && password != null && !password.isBlank();
        if (!configured) System.out.println("D13 JDBC BLOCKED: missing RAILWAY_TEST_JDBC_URL/RAILWAY_TEST_DB_USER/RAILWAY_TEST_DB_PASSWORD; smoke SKIPPED, not PASS");
        Assumptions.assumeTrue(configured, "JDBC smoke BLOCKED: missing test DB configuration");
        String schema = "election_d13_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection c = DriverManager.getConnection(url, user, password);
             Statement sql = c.createStatement()) {
            sql.execute("CREATE SCHEMA " + schema);
            try {
                sql.execute("SET search_path TO " + schema);
                Path migration = Path.of("database/migration/V1__schema.sql");
                if (!Files.exists(migration)) migration = Path.of("../database/migration/V1__schema.sql");
                for (String command : Files.readString(migration).split(";")) {
                    if (!command.isBlank()) sql.execute(command);
                }
                JdbcRaftStorage first = jdbcStorage(url, user, password, schema);
                first.saveTermAndVote(7, "node-2");
                JdbcRaftStorage reopened = jdbcStorage(url, user, password, schema);
                var loaded = reopened.load();
                assertEquals(7, loaded.currentTerm());
                assertEquals("node-2", loaded.votedFor());
                var state = RaftState.restore(loaded.currentTerm(), loaded.votedFor(), loaded.log(), loaded.lastApplied());
                var persistence = new PersistentElectionState(state, reopened);
                assertFalse(persistence.grantVote(new RaftNode.RequestVoteRequest(7, "node-3", 0, 0)));
                assertTrue(persistence.grantVote(new RaftNode.RequestVoteRequest(7, "node-2", 0, 0)));
                assertThrows(IllegalArgumentException.class, () -> reopened.saveTermAndVote(6, "node-1"));
                assertEquals("node-2", jdbcStorage(url, user, password, schema).load().votedFor());
                persistence.observeHigherTerm(8);
                var higher = jdbcStorage(url, user, password, schema).load();
                assertEquals(8, higher.currentTerm());
                assertNull(higher.votedFor());
                System.out.println("D13 JDBC PASS: fresh connection/instance reload term=7 vote=node-2; second candidate denied; lower term rejected; term=8 vote=null");
            } finally { sql.execute("DROP SCHEMA " + schema + " CASCADE"); }
        }
    }

    private static JdbcRaftStorage jdbcStorage(String url, String user, String password, String schema) {
        return new JdbcRaftStorage(() -> {
            Connection c = DriverManager.getConnection(url, user, password);
            try (Statement sql = c.createStatement()) {
                sql.execute("SET search_path TO " + schema);
                return c;
            } catch (SQLException failure) {
                c.close();
                throw failure;
            }
        });
    }

    private record Sent(String candidate, String peer, RequestVoteRequestDto request,
            CandidateElection.VoteReply reply) { }

    private static final class FakeNetwork {
        final FakeScheduler clock = new FakeScheduler();
        final Map<String, Member> members = new LinkedHashMap<>();
        final List<Sent> sent = new ArrayList<>();

        FakeNetwork() {
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
        Member(FakeNetwork cluster, String self) {
            core = new FakeNode(self);
            election = new CandidateElection(self, core, core.state, (peer, request, reply) -> {
                assertEquals(request.getTerm(), core.storage.term, "persist self-vote before send");
                assertEquals(self, core.storage.vote);
                cluster.sent.add(new Sent(self, peer, request, reply));
            }, term -> {
                core.persistence.observeHigherTerm(term);
                restartTimer();
            }, this::stopTimer);
            timer = new ElectionTimer(election, cluster.clock,
                    () -> ++draws == 1 || !self.equals("node-2") ? 500 : 900,
                    failure -> fail(failure));
            core.handler = new RequestVoteHandler(core.persistence, timer);
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
        private final long deadline;
        private final FakeScheduler clock;

        FakeTask(Runnable command, long deadline, FakeScheduler clock) {
            super(command, null);
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
        private final PersistentElectionState persistence = new PersistentElectionState(state, storage);

        @Override public void onElectionTimeout() throws RaftStorageException {
            persistence.startElection(self);
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            throw new UnsupportedOperationException("election-only fixture");
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
