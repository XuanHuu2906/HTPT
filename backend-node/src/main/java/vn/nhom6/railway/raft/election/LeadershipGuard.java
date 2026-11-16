package vn.nhom6.railway.raft.election;

import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Validates callbacks against the leadership term that created them.
 *
 * <p>Callbacks from an older RPC may still arrive after a node has stepped down or
 * been elected again. Such callbacks may be observed for diagnostics, but must not
 * mutate leader-only progress or complete a pending leader request.
 */
public final class LeadershipGuard {
    private final RaftState state;

    public LeadershipGuard(RaftState state) {
        this.state = Objects.requireNonNull(state, "state");
    }

    /** Captures the term of the current Leader for an outgoing operation. */
    public long captureTerm() {
        if (state.role() != Role.LEADER) {
            throw new IllegalStateException("leadership is not active; role=" + state.role());
        }
        return state.currentTerm();
    }

    /** Returns whether a callback created in {@code term} is still leader-authorized. */
    public boolean isCurrent(long term) {
        return term >= 0L && state.role() == Role.LEADER && state.currentTerm() == term;
    }

    /**
     * Runs a leader-only callback when its originating term is still current.
     *
     * @return {@code true} when the callback ran, {@code false} when it was stale
     */
    public boolean runIfCurrent(long term, Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        if (!isCurrent(term)) {
            return false;
        }
        callback.run();
        return true;
    }
}
