package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;

/**
 * Leader-side sender for APPEND_ENTRIES RPCs. This service is intentionally small and focused on
 * the D12 scope: it prepares per-peer AppendEntries requests, sends them through a
 * {@link RaftTransportAdapter}, records in-flight requests, retries on failures with backoff, and
 * updates the leader's {@link RaftState#matchIndex()} only when the RPC proves the peer reached the
 * exact expected match index.
 *
 * <p>An attached {@link AppendRetryCoordinator} handles same-term log-mismatch rejections and
 * schedules further bounded batches after successful acknowledgements.
 *
 * <p>Same-term rejections are retried only when an {@link AppendRetryCoordinator} is attached;
 * both components are intended to be used together. An empty request (heartbeat) when
 * {@code nextIndex > lastLogIndex} may overlap TV5's {@code LeaderHeartbeatScheduler}; safety
 * relies on the one-in-flight-RPC-per-peer limit.
 *
 * <p>It deliberately does not compute majority/commit, does not apply state-machine entries and does
 * not touch JDBC or the durable log. The actual commit rule remains the responsibility of the later
 * replication/commit task described in SPEC-04 section 5.
 */
public final class RaftReplicationService {

    private static final Set<String> VALID_VOTERS = Set.of(
            "node-1", "node-2", "node-3", "node-4", "node-5");

    public interface Scheduler {
        Cancellable schedule(Runnable task, long delayMillis);

        interface Cancellable {
            void cancel();
        }
    }

    public record Backoff(long baseDelayMs, long maxDelayMs) {
        public Backoff {
            if (baseDelayMs <= 0L) {
                throw new IllegalArgumentException("baseDelayMs must be > 0");
            }
            if (maxDelayMs < baseDelayMs) {
                throw new IllegalArgumentException(
                        "maxDelayMs must be >= baseDelayMs but was base=" + baseDelayMs + ", max=" + maxDelayMs);
            }
        }

        public long delayFor(int consecutiveFailures) {
            if (consecutiveFailures <= 0) {
                return 0L;
            }
            long delay = baseDelayMs;
            for (int i = 1; i < consecutiveFailures; i++) {
                delay = Math.min(delay * 2L, maxDelayMs);
            }
            return Math.min(delay, maxDelayMs);
        }
    }

    public interface Hooks {
        default void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
        }

        default void onHigherTerm(long term) {
        }

        default void onFailure(String peerId, Throwable failure) {
        }
    }

    private final String localId;
    private final RaftState state;
    private final RaftTransportAdapter transport;
    private final Set<String> peers;
    private final Executor raftLoop;
    private final Scheduler scheduler;
    private final int batchMax;
    private final Backoff backoff;
    private final Hooks hooks;
    private final Map<String, Integer> consecutiveFailures = new HashMap<>();
    private final Map<String, Scheduler.Cancellable> scheduledRetries = new HashMap<>();
    private final Map<String, Boolean> inFlight = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private AppendRetryCoordinator retryCoordinator;

    public RaftReplicationService(String localId, RaftState state, RaftTransportAdapter transport,
            Collection<String> peers, Executor raftLoop, Scheduler scheduler, int batchMax,
            Backoff backoff, Hooks hooks) {
        this.localId = requireVoterId(localId, "localId");
        this.state = Objects.requireNonNull(state, "state");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.peers = normalizePeers(peers, localId);
        this.raftLoop = Objects.requireNonNull(raftLoop, "raftLoop");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        if (batchMax <= 0) {
            throw new IllegalArgumentException("batchMax must be > 0 but was " + batchMax);
        }
        this.batchMax = batchMax;
        this.backoff = Objects.requireNonNull(backoff, "backoff");
        this.hooks = Objects.requireNonNull(hooks, "hooks");
    }

    public void replicate(String peerId) {
        requirePeer(peerId);
        if (stopped.get() || state.role() != Role.LEADER) {
            return;
        }
        cancelRetry(peerId);
        if (inFlight.containsKey(peerId)) {
            return;
        }

        long nextIndex = state.nextIndexOf(peerId);
        long prevLogIndex = Math.max(0L, nextIndex - 1L);
        long prevLogTerm = state.termAt(prevLogIndex);
        List<RaftLogEntry> entries = entriesToSend(peerId, nextIndex);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                state.currentTerm(), localId, prevLogIndex, prevLogTerm, entries, state.commitIndex());

        inFlight.put(peerId, Boolean.TRUE);
        transport.sendAppendEntries(peerId, request, result -> {
            inFlight.remove(peerId);
            handleAppendResult(peerId, request, result);
        });
    }

    public void replicateAll() {
        for (String peer : new ArrayList<>(peers)) {
            replicate(peer);
        }
    }

    public void stop() {
        stopped.set(true);
        if (retryCoordinator != null) {
            retryCoordinator.stop();
        }
        for (Scheduler.Cancellable handle : scheduledRetries.values()) {
            if (handle != null) {
                handle.cancel();
            }
        }
        scheduledRetries.clear();
        inFlight.clear();
    }

    void attachRetryCoordinator(AppendRetryCoordinator coordinator) {
        Objects.requireNonNull(coordinator, "coordinator");
        if (retryCoordinator != null && retryCoordinator != coordinator) {
            throw new IllegalStateException("an AppendRetryCoordinator is already attached");
        }
        retryCoordinator = coordinator;
    }

    void handleAppendResult(String peerId, RaftNode.AppendEntriesRequest request,
            RaftTransportAdapter.PeerAppendResult result) {
        if (stopped.get()) {
            return;
        }
        try {
            if (result instanceof RaftTransportAdapter.Replied replied) {
                handleReplied(peerId, request, replied);
            } else if (result instanceof RaftTransportAdapter.Failed failed) {
                handleFailed(peerId, request, failed);
            } else if (result instanceof RaftTransportAdapter.LateHigherTerm lateHigherTerm) {
                hooks.onHigherTerm(lateHigherTerm.term());
            } else {
                throw new IllegalStateException("unsupported Raft transport result: " + result);
            }
        } catch (RuntimeException failure) {
            hooks.onFailure(peerId, failure);
        }
    }

    private void handleReplied(String peerId, RaftNode.AppendEntriesRequest request,
            RaftTransportAdapter.Replied replied) {
        long responseTerm = replied.response().term();
        if (responseTerm > state.currentTerm()) {
            hooks.onHigherTerm(responseTerm);
            return;
        }
        if (state.role() != Role.LEADER || state.currentTerm() != request.term()) {
            return;
        }
        if (responseTerm != request.term()) {
            return;
        }
        if (!replied.response().success()) {
            hooks.onRejected(peerId, request);
            if (retryCoordinator != null) {
                retryCoordinator.onRejected(peerId, request, replied.response());
            }
            consecutiveFailures.put(peerId, consecutiveFailures.getOrDefault(peerId, 0) + 1);
            return;
        }

        long expected = request.prevLogIndex() + request.entries().size();
        if (replied.response().matchIndex() != expected) {
            throw new IllegalStateException(
                    "peer " + peerId + " responded with matchIndex " + replied.response().matchIndex()
                            + " but expected " + expected + " for request " + request);
        }

        if (expected < state.matchIndexOf(peerId)) {
            return;
        }
        state.advanceMatchIndex(peerId, expected);
        consecutiveFailures.remove(peerId);
        resetRetry(peerId);
        if (retryCoordinator != null) {
            retryCoordinator.onAckSuccess(peerId);
        }
    }

    private void handleFailed(String peerId, RaftNode.AppendEntriesRequest request,
            RaftTransportAdapter.Failed failed) {
        if (state.role() != Role.LEADER || state.currentTerm() != request.term()) {
            return;
        }
        int failures = consecutiveFailures.getOrDefault(peerId, 0) + 1;
        consecutiveFailures.put(peerId, failures);
        long delayMs = backoff.delayFor(failures);
        scheduleRetry(peerId, delayMs);
    }

    private void scheduleRetry(String peerId, long delayMs) {
        if (stopped.get()) {
            return;
        }
        cancelRetry(peerId);
        Scheduler.Cancellable handle = scheduler.schedule(() -> {
            scheduledRetries.remove(peerId);
            if (!stopped.get()) {
                replicate(peerId);
            }
        }, delayMs);
        scheduledRetries.put(peerId, handle);
    }

    private void cancelRetry(String peerId) {
        Scheduler.Cancellable existing = scheduledRetries.remove(peerId);
        if (existing != null) {
            existing.cancel();
        }
    }

    private void resetRetry(String peerId) {
        cancelRetry(peerId);
        consecutiveFailures.remove(peerId);
    }

    private List<RaftLogEntry> entriesToSend(String peerId, long nextIndex) {
        long lastLogIndex = state.lastLogIndex();
        if (nextIndex > lastLogIndex) {
            return Collections.emptyList();
        }
        long endIndex = Math.min(lastLogIndex, nextIndex + batchMax - 1L);
        List<RaftLogEntry> result = new ArrayList<>();
        for (long index = nextIndex; index <= endIndex; index++) {
            result.add(state.entryAt(index));
        }
        return result;
    }

    private static String requireVoterId(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (!VALID_VOTERS.contains(value)) {
            throw new IllegalArgumentException(name + " must be one of " + VALID_VOTERS + " but was " + value);
        }
        return value;
    }

    private static Set<String> normalizePeers(Collection<String> peers, String localId) {
        Objects.requireNonNull(peers, "peers");
        Set<String> normalized = new TreeSet<>();
        for (String peer : peers) {
            if (peer == null || peer.isBlank()) {
                throw new IllegalArgumentException("peer ids must not be blank");
            }
            String sanitized = requireVoterId(peer, "peer");
            if (sanitized.equals(localId)) {
                throw new IllegalArgumentException("peer list must not contain the local node id: " + localId);
            }
            if (!normalized.add(sanitized)) {
                throw new IllegalArgumentException("peer list contains duplicate node id: " + sanitized);
            }
        }
        return normalized;
    }

    private void requirePeer(String peerId) {
        if (!peers.contains(peerId)) {
            throw new IllegalArgumentException("unknown peer " + peerId + "; allowed peers=" + peers);
        }
    }
}
