package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

/**
 * One-shot election deadlines on the Node's serialized Raft event loop (SPEC-04 section 3).
 *
 * <p>The supplied scheduler must be the same single-thread event loop used for all RaftNode
 * calls. Invoke every method, including stop/close, on that loop. This timer owns neither
 * the executor nor Raft state. At shutdown, enqueue close on the loop before shutting down
 * the executor. A ScheduledThreadPoolExecutor should enable remove-on-cancel to promptly
 * discard cancelled deadlines. Scheduling uses relative, local monotonic delays.
 *
 * <p>RPC handlers validate schema, peer membership and sender identity before notifying this
 * timer. They first persist any term/vote change and perform the required role transition
 * through RaftNode. Log mismatch does not invalidate leader contact. No general-purpose
 * reset method is exposed: stale RPCs, rejected votes and higher-term responses alone must
 * not postpone an election. Call stop on promotion to Leader and start after stepping down.
 */
public final class ElectionTimer implements AutoCloseable {
    private final ElectionService election;
    private final RaftNode node;
    private final ScheduledExecutorService eventLoop;
    private final ElectionTimeoutPolicy timeouts;
    private final Consumer<Exception> onFailure;
    private ScheduledFuture<?> pending;
    private Object generation;
    private boolean running;
    private boolean closed;

    /** Creates a timer with independently randomized intervals from the existing policy. */
    public ElectionTimer(ElectionService election, ScheduledExecutorService eventLoop,
            ElectionConfig config, Consumer<Exception> onFailure) {
        this(election, eventLoop, ElectionTimeoutPolicy.randomized(config), onFailure);
    }

    /** Injects deterministic intervals and a virtual-time scheduler for unit tests. */
    public ElectionTimer(ElectionService election, ScheduledExecutorService eventLoop,
            ElectionTimeoutPolicy timeouts, Consumer<Exception> onFailure) {
        this.election = Objects.requireNonNull(election, "election");
        this.node = Objects.requireNonNull(election.node(), "election.node");
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventLoop");
        this.timeouts = Objects.requireNonNull(timeouts, "timeouts");
        this.onFailure = Objects.requireNonNull(onFailure, "onFailure");
    }

    /** Arms once for a Follower/Candidate; repeated starts do not move the deadline. */
    public void start() {
        if (closed) {
            throw new IllegalStateException("election timer is closed");
        }
        if (!running && node.role() != Role.LEADER) {
            running = true;
            arm();
        }
    }

    /**
     * Called after a validated AppendEntries has updated term/role, even on log mismatch.
     * Stale terms and a notification made before the core accepted a higher term are ignored.
     * Invalid schema/peer/sender requests must never call this hook.
     */
    public void onValidAppendEntries(long requestTerm) {
        if (canReset(requestTerm)) {
            arm();
        }
    }

    /** Called after processing a valid vote request; true requires a durably stored grant. */
    public void onVoteDecision(long requestTerm, boolean voteGranted) {
        if (voteGranted && canReset(requestTerm)) {
            arm();
        }
    }

    private boolean canReset(long requestTerm) {
        return running && !closed && node.role() == Role.FOLLOWER
                && requestTerm >= 0 && requestTerm == node.currentTerm();
    }

    /** Cancels the pending deadline; start may explicitly arm a new election later. */
    public void stop() {
        running = false;
        cancelPending();
    }

    /** Permanently stops this timer without shutting down the shared Raft executor. */
    @Override
    public void close() {
        closed = true;
        stop();
    }

    private void cancelPending() {
        generation = null;
        if (pending != null) {
            pending.cancel(false);
            pending = null;
        }
    }

    private void arm() {
        cancelPending();
        try {
            long delay = timeouts.nextTimeoutMillis();
            if (delay <= 0) {
                throw new IllegalArgumentException("election timeout must be positive");
            }
            Object token = new Object();
            generation = token;
            pending = eventLoop.schedule(() -> expire(token), delay, TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) {
            close();
            throw failure;
        }
    }

    private void expire(Object token) {
        if (!running || closed || generation != token) {
            return;
        }
        pending = null;
        generation = null;
        try {
            if (node.role() == Role.LEADER) {
                stop();
                return;
            }
            // One entry point owns the campaign; it invokes the durable core exactly once.
            election.startElection();
            if (running && !closed) {
                if (node.role() == Role.LEADER) {
                    stop();
                } else {
                    arm(); // A split vote starts another campaign with a fresh interval.
                }
            }
        } catch (Exception failure) {
            // A scheduled task must not silently lose a storage/scheduling failure.
            close();
            onFailure.accept(failure);
        }
    }
}
