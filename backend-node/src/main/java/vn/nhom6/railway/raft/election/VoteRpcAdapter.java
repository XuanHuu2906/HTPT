package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.dto.*;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;

/** D11 election adapter; real peer TCP transport is supplied by TV3 from D12. */
public final class VoteRpcAdapter implements CandidateElection.VoteTransport {
    /** Must send asynchronously over the configured peer channel, never the client port. */
    @FunctionalInterface
    public interface RaftRpcClient {
        CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peerId,
                MessageEnvelope<RequestVoteRequestDto> request);
    }

    private final String localId;
    private final RaftRpcClient client;
    private final Executor raftLoop;
    private final Consumer<Exception> onFailure;
    private final long timeoutMillis;

    /** raftLoop must enqueue work, never execute inline. Callback failures fail the node closed. */
    public VoteRpcAdapter(String localId, RaftRpcClient client, Duration timeout,
            Executor raftLoop, Consumer<Exception> onFailure) {
        requireVoter(localId);
        this.localId = localId;
        this.client = Objects.requireNonNull(client);
        this.raftLoop = Objects.requireNonNull(raftLoop);
        this.onFailure = Objects.requireNonNull(onFailure);
        timeoutMillis = Objects.requireNonNull(timeout).toMillis();
        if (timeoutMillis <= 0) throw new IllegalArgumentException("timeout must be positive milliseconds");
    }

    @Override public void requestVote(String peerId, RequestVoteRequestDto request,
            CandidateElection.VoteReply reply) {
        requireVoter(peerId);
        if (localId.equals(peerId)) throw new IllegalArgumentException("remote voter required");
        Objects.requireNonNull(request).validate();
        Objects.requireNonNull(reply);
        if (!localId.equals(request.getCandidateId())) throw new IllegalArgumentException("candidate must be local");
        // Snapshot the mutable DTO before handing it to another thread.
        var envelope = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, localId,
                new RequestVoteRequestDto(request.getTerm(), localId,
                        request.getLastLogIndex(), request.getLastLogTerm()));
        final long requestTerm = request.getTerm();
        CompletableFuture<MessageEnvelope<JsonNode>> pending;
        try { pending = Objects.requireNonNull(client.exchange(peerId, envelope)); }
        catch (RuntimeException unavailable) { return; }
        // Timeout invalidates the grant, but a late correlated higher term must still step down.
        var completed = new java.util.concurrent.atomic.AtomicBoolean();
        CompletableFuture.delayedExecutor(timeoutMillis, TimeUnit.MILLISECONDS)
                .execute(() -> completed.compareAndSet(false, true));
        pending.whenComplete((response, failure) -> {
            boolean onTime = completed.compareAndSet(false, true);
            if (failure != null) return;
            RequestVoteResponse vote;
            try {
                response.validate();
                if (response.getType() != MessageType.REQUEST_VOTE_RESPONSE
                        || !peerId.equals(response.getSenderId())
                        || !envelope.getMessageId().equals(response.getCorrelationId())) return;
                JsonNode payload = response.getPayload();
                if (!payload.isObject() || payload.size() != 2
                        || !payload.has("term") || !payload.get("term").isIntegralNumber()
                        || !payload.get("term").canConvertToLong() || payload.get("term").longValue() < 0
                        || !payload.has("voteGranted") || !payload.get("voteGranted").isBoolean()) return;
                vote = new RequestVoteResponse(payload.get("term").longValue(),
                        payload.get("voteGranted").booleanValue());
            } catch (RuntimeException invalid) { return; }
            if (!onTime && vote.term() <= requestTerm) return;
            // Keep lifecycle/storage errors separate from transport errors.
            try {
                raftLoop.execute(() -> {
                    try { reply.receive(vote); }
                    catch (Exception callbackFailure) { onFailure.accept(callbackFailure); }
                });
            } catch (RuntimeException rejected) { onFailure.accept(rejected); }
        });
    }

    private static void requireVoter(String id) {
        if (id == null || !id.matches("node-[1-5]")) throw new IllegalArgumentException("configured voter required");
    }
}
