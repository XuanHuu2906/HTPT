package vn.nhom6.railway.raft.election;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import vn.nhom6.railway.raft.core.*;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;

import static org.junit.jupiter.api.Assertions.*;

/** Real core and lifecycle with isolated storage/transport; not a five-node acceptance test. */
class StepDownTest {
    private static final Set<String> PEERS = Set.of("node-2", "node-3", "node-4", "node-5");

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void delayedHigherTermResponseStopsLeaderAndRejectsWrites(boolean appendResponse) throws Exception {
        var storage = new PersistentElectionRuntimeTest.Storage();
        var sends = new AtomicInteger();
        var core = new AtomicReference<PersistentRaftNode>();
        var voteReplies = new CopyOnWriteArrayList<CandidateElection.VoteReply>();
        var appendReply = new AtomicReference<Consumer<RaftNode.AppendEntriesResponse>>();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = new RaftElectionComponent("node-1", storage, (state, durable) -> {
            var node = new PersistentRaftNode("node-1", state, durable);
            core.set(node);
            return node;
        }, (peer, request, reply) -> {
            voteReplies.add(reply);
            try { reply.receive(new RaftNode.RequestVoteResponse(request.getTerm(), true)); }
            catch (RaftStorage.RaftStorageException failure) { throw new IllegalStateException(failure); }
        }, new ElectionConfig(10, 200, 400), failures::add, () -> {})) {
            component.configureHeartbeat((peer, request, reply) -> {
                sends.incrementAndGet();
                appendReply.set(reply);
            });
            new PersistentElectionRuntimeTest().awaitLeader(component);
            // A pending reply from the election can arrive after promotion to Leader.
            if (appendResponse) {
                component.execute(() -> appendReply.get().accept(new RaftNode.AppendEntriesResponse(30, true, 1)));
            } else {
                voteReplies.get(0).receive(new RaftNode.RequestVoteResponse(30, false));
            }
            assertEquals("FOLLOWER", component.status().role()); // event-loop barrier
            assertEquals(30, storage.term);
            assertNull(storage.vote);
            var write = new AtomicReference<RaftNode.AppendOutcome>();
            component.execute(() -> write.set(core.get().appendCommand("NO_OP", "{}")));
            component.status();
            assertInstanceOf(RaftNode.NotLeader.class, write.get());
            int stoppedAt = sends.get();
            Thread.sleep(70); // several heartbeat intervals, before the next election
            assertEquals(stoppedAt, sends.get());
            assertTrue(failures.isEmpty());
            var restored = storage.load();
            var restarted = RaftState.restore(restored.currentTerm(), restored.votedFor(),
                    restored.log(), restored.lastApplied());
            assertEquals(30, restarted.currentTerm());
            assertNull(restarted.votedFor());
            assertEquals(Role.FOLLOWER, restarted.role());
        }
    }

    @Test void equalAndLowerTermsPreserveLeadershipAndVote() throws Exception {
        var storage = new FakeRaftStorage();
        storage.seedState(4, "node-1", List.of(), 0);
        var state = RaftState.restore(4, "node-1", List.of(), 0);
        state.startElection("node-1");
        state.becomeLeader(PEERS);
        var stopped = new AtomicInteger();
        var restarted = new AtomicInteger();
        var handler = new HigherTermHandler(state,
                new PersistentElectionState(state, storage)::observeHigherTerm,
                stopped::incrementAndGet, restarted::incrementAndGet);
        handler.observe(4);
        handler.observe(5);
        assertEquals(Role.LEADER, state.role());
        assertEquals("node-1", state.votedFor());
        assertEquals(0, stopped.get());
        assertEquals(0, restarted.get());
        assertTrue(storage.calls().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void storageFailureStopsLeaderAndFailsClosed(boolean ambiguousWrite) throws Exception {
        var storage = new FakeRaftStorage();
        storage.seedState(4, "node-1", List.of(), 0);
        var state = RaftState.restore(4, "node-1", List.of(), 0);
        var core = new PersistentRaftNode("node-1", state, storage);
        state.startElection("node-1");
        state.becomeLeader(PEERS);
        var stopped = new AtomicInteger();
        var restarted = new AtomicInteger();
        var handler = new HigherTermHandler(state, core::observeHigherTerm,
                stopped::incrementAndGet, restarted::incrementAndGet);
        if (ambiguousWrite) storage.persistTermAndVoteThenFailNext();
        else storage.failNext(new RaftStorage.RaftStorageException("unavailable"));
        assertThrows(RaftStorage.RaftStorageException.class, () -> handler.observe(9));
        assertEquals(1, stopped.get());
        assertEquals(0, restarted.get());
        assertEquals(Role.FOLLOWER, state.role());
        assertNull(state.votedFor());
        assertThrows(IllegalStateException.class, () -> core.appendCommand("NO_OP", "{}"));
        assertThrows(IllegalStateException.class, () -> handler.observe(10));
    }
}
