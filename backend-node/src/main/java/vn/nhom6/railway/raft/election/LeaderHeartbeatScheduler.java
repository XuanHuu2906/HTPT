package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Heartbeat-only lifecycle boundary for TV4-D12; no TCP or log replication here.
 * All methods and replies must run on the node's serialized Raft event loop.
 * Start from CandidateElection's onLeader hook, after peer indices are initialized;
 * stop on every step-down and close before shutting down the shared executor.
 * Responses do not advance matchIndex or commitIndex: replication owns those rules.
 */
public final class LeaderHeartbeatScheduler implements AutoCloseable {
    private final String localId;
    private final RaftState state;
    private final ScheduledExecutorService loop;
    private final long intervalMillis;
    private final HeartbeatTransport transport;
    private final CandidateElection.HigherTermHandler higherTerm;
    private final Consumer<Exception> onFailure;
    private ScheduledFuture<?> pending;
    private Object generation;
    private long leaderTerm;
    private boolean closed;

    public LeaderHeartbeatScheduler(String localId, RaftState state,
            ScheduledExecutorService loop, ElectionConfig config, HeartbeatTransport transport,
            CandidateElection.HigherTermHandler higherTerm, Consumer<Exception> onFailure) {
        if (!java.util.Set.of("node-1", "node-2", "node-3", "node-4", "node-5")
                .contains(Objects.requireNonNull(localId))) {
            throw new IllegalArgumentException("localId must be a configured voter");
        }
        this.localId = localId;
        this.state = Objects.requireNonNull(state);
        this.loop = Objects.requireNonNull(loop);
        intervalMillis = Objects.requireNonNull(config).heartbeatIntervalMillis();
        this.transport = Objects.requireNonNull(transport);
        this.higherTerm = Objects.requireNonNull(higherTerm);
        this.onFailure = Objects.requireNonNull(onFailure);
    }

    /** Sends immediately; repeated starts in the same leadership term are idempotent. */
    public void start() {
        if (closed) throw new IllegalStateException("heartbeat scheduler closed");
        if (state.role() != Role.LEADER) {
            stop();
            return;
        }
        if (generation != null && leaderTerm == state.currentTerm()) return;
        stop();
        leaderTerm = state.currentTerm();
        Object token = new Object();
        generation = token;
        tick(token);
    }

    public void stop() {
        generation = null;
        if (pending != null) {
            pending.cancel(false);
            pending = null;
        }
    }

    @Override public void close() {
        closed = true;
        stop();
    }

    private boolean active(Object token) {
        return !closed && generation == token && state.role() == Role.LEADER
                && state.currentTerm() == leaderTerm;
    }

    private void tick(Object token) {
        if (generation != token) return;
        pending = null;
        if (!active(token)) { stop(); return; }
        try {
            for (String peer : state.nextIndex().keySet()) {
                if (!active(token)) return;
                long previous = state.nextIndexOf(peer) - 1;
                var request = AppendEntriesRequestDto.createHeartbeat(leaderTerm, localId,
                        previous, state.termAt(previous), state.commitIndex());
                transport.appendEntries(peer, request, response -> receive(response));
            }
            if (active(token)) {
                pending = loop.schedule(() -> tick(token), intervalMillis, TimeUnit.MILLISECONDS);
            }
        } catch (Exception failure) { fail(failure); }
    }

    private void receive(AppendEntriesResponse response) {
        if (closed) return;
        try {
            Objects.requireNonNull(response);
            // A delayed reply from a previous term still carries authoritative term information.
            if (response.term() > state.currentTerm()) {
                stop();
                higherTerm.observe(response.term());
                if (state.currentTerm() != response.term() || state.role() != Role.FOLLOWER
                        || state.votedFor() != null) {
                    throw new IllegalStateException("higher-term hook did not persist and step down");
                }
            }
        } catch (Exception failure) { fail(failure); }
    }

    private void fail(Exception failure) {
        close();
        onFailure.accept(failure);
    }

    /**
     * Nonblocking adapter: validate peer identity, schema and RPC correlation and enqueue
     * replies on the Raft loop. Timeout/disconnection yields no reply; it must not throw
     * as a normal network outcome. Local fatal errors are reported via onFailure.
     */
    @FunctionalInterface
    public interface HeartbeatTransport {
        void appendEntries(String peerId, AppendEntriesRequestDto request,
                Consumer<AppendEntriesResponse> reply);
    }
}
