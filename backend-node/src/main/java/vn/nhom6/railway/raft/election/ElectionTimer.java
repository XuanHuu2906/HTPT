package vn.nhom6.railway.raft.election;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * Compatible RPC/lifecycle boundary for the single election retry coordinator.
 * All calls must run on the same serialized Raft event loop as CandidateElection.
 * Do not construct a second coordinator for this node: this timer owns its deadlines.
 */
public final class ElectionTimer implements AutoCloseable {
    private final ElectionRetryCoordinator retries;

    public ElectionTimer(ElectionService election, ScheduledExecutorService eventLoop,
            ElectionConfig config, Consumer<Exception> onFailure) {
        retries = new ElectionRetryCoordinator(election, eventLoop, config, onFailure);
    }

    public ElectionTimer(ElectionService election, ScheduledExecutorService eventLoop,
            ElectionTimeoutPolicy timeouts, Consumer<Exception> onFailure) {
        retries = new ElectionRetryCoordinator(election, eventLoop, timeouts, onFailure);
    }

    /** Arms one deadline; repeated starts never postpone it. */
    public void start() { retries.start(); }

    /** Notify only after the core accepts a validated leader request and updates role/term. */
    public void onValidAppendEntries(long requestTerm) { retries.onValidAppendEntries(requestTerm); }

    /** Notify after processing/persisting the vote; denials never reset the deadline. */
    public void onVoteDecision(long requestTerm, boolean voteGranted) {
        retries.onVoteDecision(requestTerm, voteGranted);
    }

    /** Cancel deadlines on promotion; a later step-down may explicitly start again. */
    public void stop() { retries.stop(); }

    /** Permanently cancel deadlines, leaving the shared executor alive. */
    @Override public void close() { retries.close(); }
}
