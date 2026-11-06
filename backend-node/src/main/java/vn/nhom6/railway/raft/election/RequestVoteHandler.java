package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.Set;

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
    private static final Set<String> VOTERS =
            Set.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final RaftState state;
    private final RaftStorage storage;
    private final ElectionTimer timer;
    private boolean failed;

    public RequestVoteHandler(RaftState state, RaftStorage storage, ElectionTimer timer) {
        this.state = Objects.requireNonNull(state, "state");
        this.storage = Objects.requireNonNull(storage, "storage");
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
        Objects.requireNonNull(request, "request");
        if (request.candidateId() == null || !VOTERS.contains(request.candidateId())) {
            throw new IllegalArgumentException("candidate must be one of the five voters");
        }

        long previousTerm = state.currentTerm();
        String previousVote = state.votedFor();
        Role previousRole = state.role();
        // VoteRules validates all metadata before changing the shared state.
        boolean granted = VoteRules.grantVote(state, request.term(), request.candidateId(),
                request.lastLogIndex(), request.lastLogTerm());

        try {
            if (previousTerm != state.currentTerm()
                    || !Objects.equals(previousVote, state.votedFor())) {
                storage.saveTermAndVote(state.currentTerm(), state.votedFor());
            }
            if (previousRole == Role.LEADER && state.role() == Role.FOLLOWER) {
                // A Leader's timer was stopped. Arm on step-down, even on a denial;
                // this is lifecycle startup, not postponement of a running deadline.
                timer.start();
            } else {
                timer.onVoteDecision(request.term(), granted);
            }
            return new RequestVoteResponse(state.currentTerm(), granted);
        } catch (RaftStorageException | RuntimeException failure) {
            failed = true;
            timer.close();
            throw failure;
        }
    }
}
