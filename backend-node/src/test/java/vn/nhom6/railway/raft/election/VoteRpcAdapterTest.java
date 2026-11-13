package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;
import vn.nhom6.railway.raft.core.*;
import static org.junit.jupiter.api.Assertions.*;

class VoteRpcAdapterTest {
    static class FakeRaftRpcClient implements VoteRpcAdapter.RaftRpcClient {
        final List<MessageEnvelope<RequestVoteRequestDto>> requests = new ArrayList<>();
        final List<CompletableFuture<MessageEnvelope<JsonNode>>> responses = new ArrayList<>();
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peer,
                MessageEnvelope<RequestVoteRequestDto> request) {
            requests.add(request);
            var future = new CompletableFuture<MessageEnvelope<JsonNode>>();
            responses.add(future);
            return future;
        }
        void respond(int index, String sender, String correlation, Object payload) throws Exception {
            responses.get(index).complete(MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(correlation, MessageType.REQUEST_VOTE_RESPONSE, sender, payload))));
        }
    }
    final FakeRaftRpcClient rpc = new FakeRaftRpcClient();
    final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
    final List<Exception> failures = new ArrayList<>();
    VoteRpcAdapter adapter(long millis) {
        return new VoteRpcAdapter("node-1", rpc, Duration.ofMillis(millis), loop::add, failures::add);
    }
    RequestVoteRequestDto request() { return new RequestVoteRequestDto(1, "node-1", 0, 0); }
    void drain() { Runnable task; while ((task = loop.poll()) != null) task.run(); }

    @Test void correlatesAndQueuesExactlyOneReplyWithDtoSnapshot() throws Exception {
        var votes = new ArrayList<RaftNode.RequestVoteResponse>();
        var dto = request();
        adapter(2000).requestVote("node-2", dto, votes::add);
        dto.setTerm(99);
        assertEquals(1, rpc.requests.get(0).getPayload().getTerm());
        assertEquals(MessageType.REQUEST_VOTE, rpc.requests.get(0).getType());
        rpc.respond(0, "node-2", rpc.requests.get(0).getMessageId(), Map.of("term", 1, "voteGranted", true));
        assertTrue(votes.isEmpty());
        drain();
        assertEquals(List.of(new RaftNode.RequestVoteResponse(1, true)), votes);
        assertFalse(rpc.responses.get(0).complete(null));
    }

    @Test void timeoutDisconnectAndLateReplyNeverCountAsVotes() throws Exception {
        var adapter = adapter(20);
        adapter.requestVote("node-2", request(), vote -> fail("expired RPC cannot vote"));
        adapter.requestVote("node-3", request(), vote -> fail("disconnected peer cannot vote"));
        rpc.responses.get(1).completeExceptionally(new java.io.IOException("disconnected"));
        // Let the adapter's actual deadline expire before completing the transport future.
        Thread.sleep(150);
        rpc.respond(0, "node-2", rpc.requests.get(0).getMessageId(), Map.of("term", 1, "voteGranted", true));
        drain();
        assertTrue(loop.isEmpty());
        assertTrue(failures.isEmpty());
    }

    @Test void lateHigherTermStillReachesLifecycleWithoutCountingOldGrant() throws Exception {
        var replies = new ArrayList<RaftNode.RequestVoteResponse>();
        adapter(20).requestVote("node-2", request(), replies::add);
        Thread.sleep(150);
        rpc.respond(0, "node-2", rpc.requests.get(0).getMessageId(), Map.of("term", 9, "voteGranted", false));
        drain();
        assertEquals(List.of(new RaftNode.RequestVoteResponse(9, false)), replies);
        assertTrue(failures.isEmpty());
    }

    @Test void rejectsSpoofedCorrelationAndMalformedPayloads() throws Exception {
        var adapter = adapter(2000);
        for (int i = 0; i < 5; i++) adapter.requestVote("node-2", request(), vote -> fail("invalid vote"));
        rpc.respond(0, "node-3", rpc.requests.get(0).getMessageId(), Map.of("term", 9, "voteGranted", true));
        rpc.respond(1, "node-2", UUID.randomUUID().toString(), Map.of("term", 9, "voteGranted", true));
        rpc.respond(2, "node-2", rpc.requests.get(2).getMessageId(), Map.of("term", 9));
        rpc.respond(3, "node-2", rpc.requests.get(3).getMessageId(), Map.of("term", "9", "voteGranted", true));
        rpc.respond(4, "node-2", rpc.requests.get(4).getMessageId(), Map.of("term", -1, "voteGranted", true));
        drain();
        assertTrue(failures.isEmpty());
    }

    @Test void higherTermReachesElectionAndPersistsStepDown() throws Exception {
        var state = new RaftState();
        var saved = new ArrayList<Long>();
        RaftNode core = (RaftNode) Proxy.newProxyInstance(RaftNode.class.getClassLoader(),
                new Class<?>[]{RaftNode.class}, (proxy, method, args) -> {
                    if (method.getName().equals("onElectionTimeout")) {
                        state.startElection("node-1"); saved.add(state.currentTerm()); return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var election = new CandidateElection("node-1", core, state, adapter(2000), term -> {
            saved.add(term); state.becomeFollower(term);
        }, () -> fail("no quorum"));
        election.startElection();
        rpc.respond(0, "node-2", rpc.requests.get(0).getMessageId(), Map.of("term", 8, "voteGranted", false));
        assertEquals(Role.CANDIDATE, state.role());
        drain();
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(8, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(List.of(1L, 8L), saved); // Fake persistence hook, no DB durability claim.
    }

    @Test void callbackStorageFailureReachesLifecycle() throws Exception {
        adapter(2000).requestVote("node-2", request(), vote -> {
            throw new RaftStorage.RaftStorageException("disk failed");
        });
        rpc.respond(0, "node-2", rpc.requests.get(0).getMessageId(), Map.of("term", 8, "voteGranted", false));
        drain();
        assertEquals(1, failures.size());
        assertInstanceOf(RaftStorage.RaftStorageException.class, failures.get(0));
    }
}
