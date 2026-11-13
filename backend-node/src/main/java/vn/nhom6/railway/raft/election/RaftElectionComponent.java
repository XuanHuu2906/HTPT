package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;

/** Per-node election lifecycle. Core and transport adapters remain owned by their modules. */
public final class RaftElectionComponent implements AutoCloseable {
    private final String localId;
    private final ScheduledThreadPoolExecutor loop;
    private final CandidateElection election;
    private final ElectionTimer timer;
    private final RaftState state;
    private final RaftNode core;
    private LeaderHeartbeatScheduler heartbeat;
    private boolean heartbeatActive;
    private final ElectionConfig config;
    private final Consumer<Exception> onFailure;
    private boolean failed;
    private volatile boolean closed;
    private volatile Thread raftThread;

    /** Factory must retain exactly the restored state passed here, without reloading storage. */
    @FunctionalInterface
    public interface CoreFactory {
        RaftNode create(RaftState state, RaftStorage storage) throws Exception;
    }

    public RaftElectionComponent(String localId, RaftStorage storage, CoreFactory coreFactory,
            CandidateElection.VoteTransport transport, ElectionConfig config,
            Consumer<Exception> onFailure, Runnable onLeader) throws Exception {
        this.localId = Objects.requireNonNull(localId);
        this.onFailure = Objects.requireNonNull(onFailure);
        Objects.requireNonNull(storage);
        var persisted = storage.load();
        state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        core = Objects.requireNonNull(coreFactory.create(state, storage));
        this.config = Objects.requireNonNull(config);
        var persistence = core instanceof PersistentRaftNode durable
                ? durable.persistence() : new PersistentElectionState(state, storage);
        loop = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "raft-election-" + localId);
            raftThread = thread;
            thread.setDaemon(true);
            return thread;
        });
        loop.setRemoveOnCancelPolicy(true);
        election = new CandidateElection(localId, core, state, (peer, request, reply) ->
                transport.requestVote(peer, request, response -> {
                    if (!closed) loop.execute(() -> {
                        if (failed) return;
                        try { reply.receive(response); }
                        catch (Exception failure) { fail(failure); }
                    });
                }), term -> {
                    stopHeartbeat();
                    if (core instanceof PersistentRaftNode durable) durable.observeHigherTerm(term);
                    else persistence.observeHigherTerm(term);
                    timerStart();
                }, () -> {
                    timerStop();
                    System.out.println("[RAFT] node=" + localId + " role=LEADER term=" + state.currentTerm());
                    if (heartbeat != null) { heartbeatActive = true; heartbeat.start(); }
                    onLeader.run();
                });
        timer = new ElectionTimer(election, loop, config, this::fail);
        if (core instanceof PersistentRaftNode durable) durable.bindLifecycle(timer, this::stopHeartbeat);
    }

    private void timerStart() { timer.start(); }
    private void timerStop() { timer.stop(); }

    /** Register only in NodeMain's peer registry, never its client registry. */
    public void register(HandlerRegistry peerRegistry) {
        peerRegistry.register(MessageType.REQUEST_VOTE, this::handleVote);
        if (core instanceof PersistentRaftNode) {
            peerRegistry.register(MessageType.APPEND_ENTRIES, this::handleAppend);
        }
    }

    /** Runtime adapters enqueue callbacks onto this same serialized loop. */
    public void execute(Runnable task) {
        if (closed) return;
        try {
            loop.execute(() -> {
                if (closed || failed) return;
                try { task.run(); }
                catch (RuntimeException failure) { fail(failure); }
            });
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            if (!closed) throw shutdown;
        }
    }

    public void configureHeartbeat(LeaderHeartbeatScheduler.HeartbeatTransport transport) throws Exception {
        call(() -> {
            if (heartbeat != null) throw new IllegalStateException("heartbeat already configured");
            heartbeat = new LeaderHeartbeatScheduler(localId, state, loop, config, transport, term -> {
                ((PersistentRaftNode) core).observeHigherTerm(term);
                timer.start();
            }, this::fail);
            return null;
        });
    }

    private void stopHeartbeat() {
        if (heartbeat != null) heartbeat.stop();
        if (heartbeatActive) {
            heartbeatActive = false;
            System.out.println("[RAFT] node=" + localId + " heartbeat=STOPPED term=" + state.currentTerm());
        }
    }

    private MessageEnvelope<?> handleAppend(MessageEnvelope<JsonNode> request) throws Exception {
        var dto = MessageCodec.decodePayload(request, AppendEntriesRequestDto.class);
        dto.validate();
        if (!election.isVoter(request.getSenderId()) || localId.equals(request.getSenderId())
                || !request.getSenderId().equals(dto.getLeaderId())) {
            throw new IllegalArgumentException("AppendEntries sender must be the remote leader voter");
        }
        return call(() -> {
            requireHealthy();
            try {
                var response = ((PersistentRaftNode) core).handleAppendEntries(dto, request.getSenderId());
                return MessageEnvelope.createResponse(request.getMessageId(),
                        MessageType.APPEND_ENTRIES_RESPONSE, localId, response);
            } catch (RaftStorage.RaftStorageException | IllegalStateException failure) {
                fail(failure);
                throw failure;
            }
        });
    }

    private MessageEnvelope<?> handleVote(MessageEnvelope<JsonNode> request) throws Exception {
        RequestVoteRequestDto dto;
        try { dto = MessageCodec.decodePayload(request, RequestVoteRequestDto.class); }
        catch (com.fasterxml.jackson.core.JsonProcessingException invalid) {
            throw new IllegalArgumentException("Invalid RequestVote payload", invalid);
        }
        dto.validate();
        if (!election.isVoter(request.getSenderId())
                || !request.getSenderId().equals(dto.getCandidateId())
                || localId.equals(request.getSenderId())) {
            throw new IllegalArgumentException("RequestVote sender must be the remote candidate voter");
        }
        return call(() -> {
            requireHealthy();
            try {
                var response = election.handleRequestVote(dto);
                if (state.role() != vn.nhom6.railway.raft.core.Role.LEADER) stopHeartbeat();
                timer.onVoteDecision(dto.getTerm(), response.voteGranted());
                // A leader's stopped timer must be armed even when a higher-term vote is denied.
                if (state.role() == vn.nhom6.railway.raft.core.Role.FOLLOWER) timer.start();
                return MessageEnvelope.createResponse(request.getMessageId(),
                        MessageType.REQUEST_VOTE_RESPONSE, localId, response);
            } catch (RaftStorage.RaftStorageException failure) {
                fail(failure);
                throw failure;
            }
        });
    }

    public void start() throws Exception {
        call(() -> { requireHealthy(); timer.start(); return null; });
    }

    /** Coherent snapshot for the monitor, read on the same Raft loop as mutations. */
    public Status status() throws Exception {
        return call(() -> new Status(state.role().name(), state.currentTerm(),
                state.commitIndex(), state.lastApplied(), !failed));
    }

    public record Status(String role, long currentTerm, long commitIndex,
            long lastApplied, boolean storageHealthy) { }

    private void fail(Exception failure) {
        if (failed) return;
        failed = true;
        timer.close();
        if (heartbeat != null) heartbeat.close();
        onFailure.accept(failure);
    }

    private void requireHealthy() {
        if (failed || closed) throw new IllegalStateException("Election unavailable; reload storage before reuse");
    }

    private <T> T call(Callable<T> task) throws Exception {
        if (closed) throw new IllegalStateException("Election component closed");
        try { return loop.submit(task).get(); }
        catch (ExecutionException failure) {
            if (failure.getCause() instanceof Exception cause) throw cause;
            throw new IllegalStateException(failure.getCause());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    @Override public synchronized void close() {
        if (closed) return;
        // Cancellation is safe from the event-loop failure callback as well as NodeMain.stop.
        try {
            if (Thread.currentThread() == raftThread) {
                timer.close();
                if (heartbeat != null) heartbeat.close();
            } else {
                call(() -> {
                    timer.close();
                    if (heartbeat != null) heartbeat.close();
                    return null;
                });
            }
        } catch (Exception shutdown) {
            onFailure.accept(shutdown);
        } finally {
            closed = true;
            loop.shutdownNow();
        }
    }
}
