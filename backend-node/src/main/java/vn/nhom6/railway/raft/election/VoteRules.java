package vn.nhom6.railway.raft.election;

import java.util.Objects;
import vn.nhom6.railway.raft.core.RaftState;

/**
 * Election entry point for the in-memory RequestVote rules (SPEC-04 section 3).
 * Uses the existing core transition so there is only one implementation of term,
 * votedFor and log-freshness rules. No TCP, storage or timer is involved.
 */
public final class VoteRules {

    private VoteRules() {
    }

    /**
     * Grants and records a vote if the term is not stale, no other candidate has
     * this term's vote, and the candidate log is at least as new (term, then index).
     * A higher term clears the previous vote and steps down even if the log is too
     * old to grant a vote. A retry by the same candidate still needs a current log.
     *
     * <p>Mutates the supplied state; call only from its serialized Raft event loop.
     * The caller must validate peer membership/identity before calling, persist
     * changed term/vote before replying (including a higher-term rejection), and
     * reset the election timer only on a grant. The boolean is an in-memory
     * decision, not evidence of a durable vote or an RPC response. Read the response
     * term from {@link RaftState#currentTerm()} after this call.
     *
     * @throws NullPointerException if state or candidateId is null
     * @throws IllegalArgumentException if candidateId is blank, candidateTerm is
     *         below 1, or the candidate log metadata is invalid; state is unchanged
     */
    public static boolean grantVote(RaftState state, long candidateTerm, String candidateId,
            long candidateLastLogIndex, long candidateLastLogTerm) {
        Objects.requireNonNull(state, "state");
        return state.grantVote(candidateTerm, candidateId, candidateLastLogIndex, candidateLastLogTerm);
    }
}
