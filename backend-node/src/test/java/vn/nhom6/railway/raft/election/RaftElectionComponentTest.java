package vn.nhom6.railway.raft.election;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import vn.nhom6.railway.networking.*;
import vn.nhom6.railway.protocol.dto.*;
import vn.nhom6.railway.raft.core.*;

import static org.junit.jupiter.api.Assertions.*;

/** Real scheduler and dispatcher, fake storage/core/peer replies; no JDBC durability claim. */
class RaftElectionComponentTest {
    static class Storage implements RaftStorage {
        volatile long term = 7;
        volatile String vote = "node-2";
        boolean broken;
        public PersistentState load() { return new PersistentState(term, vote, List.of(), 0); }
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (broken) throw new RaftStorageException("disk unavailable");
            this.term = term; this.vote = vote;
        }
        public void replaceSuffix(long index, List<RaftLogEntry> entries) { throw new UnsupportedOperationException(); }
        public long loadLastApplied() { return 0; }
    }

    private RaftElectionComponent component(Storage storage,
            CandidateElection.VoteTransport transport, List<Exception> failures) throws Exception {
        return new RaftElectionComponent("node-1", storage, (state, durable) -> {
            var persistence = new PersistentElectionState(state, durable);
            return (RaftNode) Proxy.newProxyInstance(RaftNode.class.getClassLoader(),
                    new Class<?>[]{RaftNode.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "role": return state.role();
                            case "currentTerm": return state.currentTerm();
                            case "onElectionTimeout": persistence.startElection("node-1"); return null;
                            case "handleRequestVote":
                                boolean granted = persistence.grantVote((RaftNode.RequestVoteRequest) args[0]);
                                return new RaftNode.RequestVoteResponse(state.currentTerm(), granted);
                            default: throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }, transport, new ElectionConfig(5, 40, 80), failures::add, () -> {});
    }

    @Test void restoresStateAndRequiresThreeDistinctVotes() throws Exception {
        var storage = new Storage();
        var replies = new CopyOnWriteArrayList<CandidateElection.VoteReply>();
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, (peer, request, reply) -> replies.add(reply), failures)) {
            assertEquals(7, component.status().currentTerm());
            assertEquals("FOLLOWER", component.status().role());
            component.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (replies.size() < 4 && System.nanoTime() < deadline) Thread.sleep(2);
            assertTrue(replies.size() >= 4);
            assertEquals("CANDIDATE", component.status().role());
            assertEquals("node-1", storage.vote);
            // No peer grants: repeated timeouts cannot produce a leader.
            Thread.sleep(150);
            assertEquals("CANDIDATE", component.status().role());
            assertTrue(failures.isEmpty());
        }
    }

    @Test void nodeMainRegistersVoteOnlyOnPeerDispatcher() throws Exception {
        var storage = new Storage();
        var failures = new CopyOnWriteArrayList<Exception>();
        var component = component(storage, (peer, request, reply) -> {}, failures);
        var config = new NodeConfig("node-1", "test", "127.0.0.1", 0,
                "127.0.0.1", 0, null, 8, 2000);
        try (var node = new NodeMain(config, new FakeBookingService(config.nodeId()), component)) {
            assertTrue(node.getRaftRegistry().hasHandler(MessageType.REQUEST_VOTE));
            assertFalse(node.getClientRegistry().hasHandler(MessageType.REQUEST_VOTE));
            var dispatcher = new MessageDispatcher(node.getRaftRegistry(), MessageDispatcher.ChannelType.PEER, "node-1");
            var request = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-2",
                    new RequestVoteRequestDto(100, "node-2", 0, 0));
            var raw = vn.nhom6.railway.protocol.codec.MessageCodec.decode(
                    vn.nhom6.railway.protocol.codec.MessageCodec.encode(request));
            var response = dispatcher.dispatch(raw);
            assertEquals(MessageType.REQUEST_VOTE_RESPONSE, response.getType());
            assertEquals(request.getMessageId(), response.getCorrelationId());
            assertEquals(100, storage.term);
            var spoof = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, "node-3",
                    new RequestVoteRequestDto(101, "node-2", 0, 0));
            var invalid = dispatcher.dispatch(vn.nhom6.railway.protocol.codec.MessageCodec.decode(
                    vn.nhom6.railway.protocol.codec.MessageCodec.encode(spoof)));
            assertEquals(MessageType.ERROR_RESPONSE, invalid.getType());
            assertEquals(100, storage.term);
            assertEquals("FOLLOWER", component.status().role());
        }
        component.close();
        assertThrows(IllegalStateException.class, component::start);
    }

    @Test void storageFailureStopsCampaigning() throws Exception {
        var storage = new Storage();
        storage.broken = true;
        var failures = new CopyOnWriteArrayList<Exception>();
        try (var component = component(storage, (peer, request, reply) -> fail("must persist first"), failures)) {
            component.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (failures.isEmpty() && System.nanoTime() < deadline) Thread.sleep(2);
            assertEquals(1, failures.size());
            assertFalse(component.status().storageHealthy());
            assertThrows(IllegalStateException.class, component::start);
        }
    }
}
