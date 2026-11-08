package vn.nhom6.railway.raft.election;

import java.util.Objects;

import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

/**
 * Core-owned delegate for {@link RaftNode#handleRequestVote(RequestVoteRequest)}.
 * Applies SPEC-04 section 3 using the existing VoteRules and persists before replying.
 *
 * <p>Construct once per Node with its shared state, storage and timer; invoke only on
 * that Node's serialized Raft event loop. ElectionService continues to call RaftNode,
 * which delegates here, so there is no second copy of term/vote or election rules.
 * State must already have been restored from this storage before use. The networking
 * adapter must validate the peer channel and senderId == candidateId before calling.
 * This handler checks voter membership but does not authenticate a sender.
 *
 * <p>A storage failure can leave memory ahead of storage, or an ambiguous successful
 * write. Never roll the vote back or retry using this instance: it closes the timer,
 * refuses subsequent requests and propagates the failure. The owning Node must stop
 * all other Raft activity and recover from storage before constructing fresh handlers.
 * Tests with fake storage do not establish PostgreSQL durability or runtime wiring.
 */
public final class RequestVoteHandler {
    private final RaftState state;
    private final PersistentElectionState persistence;
    private final ElectionTimer timer;
    private boolean failed;

    public RequestVoteHandler(RaftState state, RaftStorage storage, ElectionTimer timer) {
        this(new PersistentElectionState(state, storage), timer);
    }

    /** Shares the core's persistence boundary without duplicating term/vote ownership. */
    public RequestVoteHandler(PersistentElectionState persistence, ElectionTimer timer) {
        this.persistence = Objects.requireNonNull(persistence, "persistence");
        this.state = persistence.state();
        this.timer = Objects.requireNonNull(timer, "timer");
    }

    /**
     * Returns the receiver's term after processing, including on a rejected vote.
     * Changed term/vote is saved even when a higher-term candidate has an old log.
     * An identical retry may reuse the already stored vote, but still checks log freshness.
     *
     * @throws IllegalArgumentException for a non-voter or invalid term/log metadata,
     *                                  before any mutation, storage call or timer change
     * @throws RaftStorageException if required persistence fails; no response is returned
     * @throws IllegalStateException if this handler previously failed closed
     */
    public RequestVoteResponse handle(RequestVoteRequest request) throws RaftStorageException {
        if (failed) {
            throw new IllegalStateException("vote handler failed closed; recover the node before reuse");
        }
        Role previousRole = state.role();
        boolean granted;
        try {
            granted = persistence.grantVote(request);
        } catch (RaftStorageException failure) {
            failed = true;
            timer.close();
            throw failure;
        } catch (RuntimeException failure) {
            // Validation does not poison the node; a persistence failure does.
            try {
                persistence.requireHealthy();
            } catch (IllegalStateException unhealthy) {
                failed = true;
                timer.close();
            }
            throw failure;
        }

        try {
            if (previousRole == Role.LEADER && state.role() == Role.FOLLOWER) {
                // A Leader's timer was stopped. Arm on step-down, even on a denial;
                // this is lifecycle startup, not postponement of a running deadline.
                timer.start();
            } else {
                timer.onVoteDecision(request.term(), granted);
            }
            return new RequestVoteResponse(state.currentTerm(), granted);
        } catch (RuntimeException failure) {
            failed = true;
            timer.close();
            throw failure;
        }
    }
}
