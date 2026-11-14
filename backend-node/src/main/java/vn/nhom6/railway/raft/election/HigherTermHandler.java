package vn.nhom6.railway.raft.election;

import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

/**
 * Shared higher-term response transition, invoked only on the serialized Raft loop.
 * The durable core hook owns term/vote persistence and write rejection. Transport must
 * validate peer identity and RPC correlation before passing a response here.
 */
public final class HigherTermHandler implements CandidateElection.HigherTermHandler {
    private final RaftState state;
    private final CandidateElection.HigherTermHandler durableTransition;
    private final Runnable stopHeartbeat;
    private final Runnable restartElectionTimer;
    private boolean failed;

    public HigherTermHandler(RaftState state,
            CandidateElection.HigherTermHandler durableTransition,
            Runnable stopHeartbeat, Runnable restartElectionTimer) {
        this.state = Objects.requireNonNull(state);
        this.durableTransition = Objects.requireNonNull(durableTransition);
        this.stopHeartbeat = Objects.requireNonNull(stopHeartbeat);
        this.restartElectionTimer = Objects.requireNonNull(restartElectionTimer);
    }

    /** Stop leader activity before persistence; rearm election only after durable success. */
    @Override public void observe(long term) throws RaftStorageException {
        if (failed) throw new IllegalStateException("higher-term transition failed; reload storage");
        if (term < 0) throw new IllegalArgumentException("term must be nonnegative");
        if (term <= state.currentTerm()) return;
        try {
            stopHeartbeat.run();
            durableTransition.observe(term);
            if (state.currentTerm() != term || state.role() != Role.FOLLOWER
                    || state.votedFor() != null) {
                throw new IllegalStateException("core did not complete durable higher-term step-down");
            }
            restartElectionTimer.run();
        } catch (RaftStorageException | RuntimeException failure) {
            failed = true;
            throw failure; // Owning component stops all Raft activity through its failure hook.
        }
    }
}
