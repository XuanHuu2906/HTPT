package vn.nhom6.railway.raft.election;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.networking.*;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;
import vn.nhom6.railway.raft.core.*;

import static org.junit.jupiter.api.Assertions.*;

/** Real core and scheduler; storage and transport are isolated fixtures, not a cluster claim. */
class PersistentElectionRuntimeTest {
    static class Storage implements RaftStorage {
        volatile long term = 1;
        volatile String vote;
        boolean broken;
        List<RaftLogEntry> log = List.of(new RaftLogEntry(1, 1, "NO_OP", "{}"));
        public PersistentState load() { return new PersistentState(term, vote, log, 0); }
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (broken) throw new RaftStorageException("injected unavailable storage");
            this.term = term; this.vote = vote;
        }
        public void replaceSuffix(long from, List<RaftLogEntry> entries) { throw new UnsupportedOperationException(); }
        public long loadLastApplied() { return 0; }
    }

    RaftElectionComponent component(Storage storage, AtomicInteger sends,
            AtomicReference<Consumer<RaftNode.AppendEntriesResponse>> reply,
            List<Exception> failures) throws Exception {
        var component = new RaftElectionComponent("node-1", storage,
                (state, durable) -> new PersistentRaftNode("node-1", state, durable),
                (peer, request, response) -> {
                    try { response.receive(new RaftNode.RequestVoteResponse(request.getTerm(), true)); }
                    catch (RaftStorage.RaftStorageException failure) { throw new IllegalStateException(failure); }
                },
                new ElectionConfig(10, 200, 400), failures::add, () -> {});
        component.configureHeartbeat((peer, request, response) -> {
            sends.incrementAndGet();
            reply.set(response);
        });
        return component;
    }

    void awaitLeader(RaftElectionComponent component) throws Exception {
        component.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!component.status().role().equals("LEADER") && System.nanoTime() < deadline) Thread.sleep(5);
        assertEquals("LEADER", component.status().role());
    }

    @Test void higherTermVoteDenialPersistsAndStopsOldHeartbeats() throws Exception {
        var storage = new Storage();
        var sends = new AtomicInteger();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, sends, new AtomicReference<>(), failures)) {
            awaitLeader(component);
            var registry = new HandlerRegistry();
            component.register(registry);
            var dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, "node-1");
            var request = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-2",
                    new RequestVoteRequestDto(20, "node-2", 0, 0)); // candidate log is stale
            var response = dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(request)));
            assertFalse(MessageCodec.getMapper().valueToTree(response.getPayload()).get("voteGranted").asBoolean());
            assertEquals(20, storage.term);
            assertNull(storage.vote);
            assertEquals("FOLLOWER", component.status().role());
            int stoppedAt = sends.get();
            Thread.sleep(70); // below the earliest new election deadline
            assertEquals(stoppedAt, sends.get());
            assertTrue(failures.isEmpty());
        }
    }

    @Test void higherTermHeartbeatResponsePersistsAndStopsOldSchedule() throws Exception {
        var storage = new Storage();
        var sends = new AtomicInteger();
        var reply = new AtomicReference<Consumer<RaftNode.AppendEntriesResponse>>();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, sends, reply, failures)) {
            awaitLeader(component);
            component.execute(() -> reply.get().accept(new RaftNode.AppendEntriesResponse(21, false, 0)));
            assertEquals("FOLLOWER", component.status().role());
            assertEquals(21, storage.term);
            assertNull(storage.vote);
            int stoppedAt = sends.get();
            Thread.sleep(70);
            assertEquals(stoppedAt, sends.get());
            assertTrue(failures.isEmpty());
        }
    }

    @Test void higherTermAppendStepsDownAndResetsFollowerDeadline() throws Exception {
        var storage = new Storage();
        var sends = new AtomicInteger();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, sends, new AtomicReference<>(), failures)) {
            awaitLeader(component);
            var registry = new HandlerRegistry();
            component.register(registry);
            var dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, "node-1");
            var request = MessageEnvelope.createRequest(MessageType.APPEND_ENTRIES, "node-2",
                    new AppendEntriesRequestDto(22, "node-2", 1, 1, List.of(), 0));
            var response = dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(request)));
            assertTrue(MessageCodec.getMapper().valueToTree(response.getPayload()).get("success").asBoolean());
            assertEquals(22, storage.term);
            assertNull(storage.vote);
            assertEquals("FOLLOWER", component.status().role());
            int stoppedAt = sends.get();
            Thread.sleep(70);
            assertEquals(stoppedAt, sends.get());
            assertTrue(failures.isEmpty());
        }
    }

    @Test void persistenceFailureStopsVotingAndHeartbeat() throws Exception {
        var storage = new Storage();
        var sends = new AtomicInteger();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, sends, new AtomicReference<>(), failures)) {
            awaitLeader(component);
            storage.broken = true;
            var registry = new HandlerRegistry();
            component.register(registry);
            var dispatcher = new MessageDispatcher(registry, MessageDispatcher.ChannelType.PEER, "node-1");
            var request = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-2",
                    new RequestVoteRequestDto(23, "node-2", 1, 1));
            assertEquals(MessageType.ERROR_RESPONSE,
                    dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(request))).getType());
            assertFalse(component.status().storageHealthy());
            int stoppedAt = sends.get();
            Thread.sleep(70);
            assertEquals(stoppedAt, sends.get());
            assertEquals(1, failures.size());
        }
    }
}
