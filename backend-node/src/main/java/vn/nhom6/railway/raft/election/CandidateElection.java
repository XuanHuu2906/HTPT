package vn.nhom6.railway.raft.election;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

/**
 * Candidate campaigns for the fixed five voters (SPEC-04 section 3).
 *
 * <p>All calls and transport replies run on the serialized Raft event loop. The supplied
 * state must be the very instance owned by node. Only campaign bookkeeping is local here;
 * durable transitions remain in the core. This class does not provide TCP or PostgreSQL.
 *
 * <p>D4's timer calls RaftNode.onElectionTimeout. Runtime wiring must arrange for that
 * event to invoke this coordinator once, whose node delegate performs the durable core
 * transition only; it must not call back into this coordinator recursively or send a
 * second set of vote requests. Runtime wiring is not provided by this D5 component.
 */
public final class CandidateElection implements ElectionService {
    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final String localId;
    private final RaftNode node;
    private final RaftState state;
    private final Set<String> peers;
    private final VoteTransport transport;
    private final HigherTermHandler higherTermHandler;
    private final Runnable onLeader;
    private final Set<String> grants = new HashSet<>();
    private long campaignTerm;
    private boolean campaigning;
    private boolean failed;

    /**
     * @param higherTermHandler core hook that durably saves (term, null), steps down and
     *                         restarts the election timer if it was stopped as Leader
     * @param onLeader event-loop hook that stops the election timer and starts TV4's
     *                 heartbeat/replication after the peer indices have been initialized
     */
    public CandidateElection(String localId, RaftNode node, RaftState state,
            VoteTransport transport, HigherTermHandler higherTermHandler, Runnable onLeader) {
        if (!VOTERS.contains(Objects.requireNonNull(localId, "localId"))) {
            throw new IllegalArgumentException("localId must be one of the five voters");
        }
        this.localId = localId;
        this.node = Objects.requireNonNull(node, "node");
        this.state = Objects.requireNonNull(state, "state");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.higherTermHandler = Objects.requireNonNull(higherTermHandler, "higherTermHandler");
        this.onLeader = Objects.requireNonNull(onLeader, "onLeader");
        Set<String> remote = new HashSet<>(VOTERS);
        remote.remove(localId);
        peers = Set.copyOf(remote);
    }

    @Override public String localId() { return localId; }
    @Override public RaftNode node() { return node; }
    @Override public RaftState state() { return state; }
    @Override public List<String> voters() { return VOTERS; }
    @Override public boolean isVoter(String id) { return id != null && VOTERS.contains(id); }
    @Override public int requiredVotes() { return 3; }

    /** Persists through core before sending one request to each of the four peers. */
    @Override
    public long startElection() throws RaftStorageException {
        requireHealthy();
        if (state.role() == Role.LEADER) {
            throw new IllegalStateException("a leader cannot start an election");
        }
        campaigning = false;
        grants.clear();
        long expectedTerm = Math.addExact(state.currentTerm(), 1L);
        try {
            node.onElectionTimeout();
            if (state.currentTerm() != expectedTerm || state.role() != Role.CANDIDATE
                    || !localId.equals(state.votedFor())) {
                throw new IllegalStateException("core did not start the expected campaign");
            }
            campaignTerm = expectedTerm;
            grants.add(localId);
            campaigning = true;
            for (String peer : VOTERS) {
                if (peers.contains(peer)) {
                    // Capture immutable correlation context, not the mutable wire DTO.
                    transport.requestVote(peer, voteRequestFor(expectedTerm).orElseThrow(),
                            response -> receiveReply(peer, expectedTerm, response));
                }
            }
            return expectedTerm;
        } catch (RaftStorageException | RuntimeException failure) {
            failClosed();
            throw failure;
        }
    }

    @Override
    public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
        if (!activeCampaign() || candidateTerm != campaignTerm) {
            return Optional.empty();
        }
        return Optional.of(new RequestVoteRequestDto(candidateTerm, localId,
                state.lastLogIndex(), state.lastLogTerm()));
    }

    @Override
    public RequestVoteResponse handleRequestVote(RequestVoteRequestDto request)
            throws RaftStorageException {
        requireHealthy();
        Objects.requireNonNull(request, "request").validate();
        if (!isVoter(request.getCandidateId())) {
            throw new IllegalArgumentException("candidate is not a voter");
        }
        try {
            return node.handleRequestVote(new RequestVoteRequest(request.getTerm(),
                    request.getCandidateId(), request.getLastLogIndex(), request.getLastLogTerm()));
        } catch (RaftStorageException failure) {
            failClosed();
            throw failure;
        }
    }

    /** Pure counting API; callers must already have validated peer and RPC correlation. */
    @Override
    public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
        if (voterId == null || voterId.isBlank() || term < 0) {
            throw new IllegalArgumentException("response requires a voter id and nonnegative term");
        }
        if (failed || !peers.contains(voterId)) {
            return new IgnoredVote("failed node or non-peer");
        }
        if (term > state.currentTerm()) {
            campaigning = false;
            grants.clear();
            return new SteppedDown(term);
        }
        if (!activeCampaign() || term != campaignTerm || !granted) {
            return new IgnoredVote("not a grant for the active campaign");
        }
        if (!grants.add(voterId)) {
            return new IgnoredVote("duplicate voter");
        }
        return grants.size() >= requiredVotes()
                ? new Won(grants.size()) : new VoteProgress(grants.size(), requiredVotes());
    }

    @Override
    public void promoteToLeader(Set<String> leaderPeers) {
        requireHealthy();
        if (!peers.equals(leaderPeers)) {
            throw new IllegalArgumentException("leader must initialize all four configured peers");
        }
        if (!activeCampaign() || grants.size() < requiredVotes()) {
            throw new IllegalStateException("no majority in the current campaign");
        }
        // The existing ElectionService boundary permits this one volatile core transition.
        state.becomeLeader(peers);
        campaigning = false;
        onLeader.run();
    }

    private void receiveReply(String peer, long requestTerm, RequestVoteResponse response)
            throws RaftStorageException {
        Objects.requireNonNull(response, "response");
        if (failed) {
            return;
        }
        // Even a delayed response to an old campaign can reveal a newer term.
        if (response.term() > state.currentTerm()) {
            recordVoteResponse(peer, response.term(), response.voteGranted());
            try {
                higherTermHandler.observe(response.term());
                if (state.currentTerm() != response.term() || state.role() != Role.FOLLOWER
                        || state.votedFor() != null) {
                    throw new IllegalStateException("core did not complete higher-term step-down");
                }
            } catch (RaftStorageException | RuntimeException failure) {
                failClosed();
                throw failure;
            }
            return;
        }
        if (requestTerm != campaignTerm) {
            return;
        }
        if (recordVoteResponse(peer, response.term(), response.voteGranted()) instanceof Won) {
            promoteToLeader(peers);
        }
    }

    private boolean activeCampaign() {
        return !failed && campaigning && state.role() == Role.CANDIDATE
                && state.currentTerm() == campaignTerm && localId.equals(state.votedFor());
    }

    private void requireHealthy() {
        if (failed) {
            throw new IllegalStateException("election failed closed; recover the node before reuse");
        }
    }

    private void failClosed() {
        failed = true;
        campaigning = false;
        grants.clear();
    }

    /**
     * Nonblocking send boundary, faked in D5 tests. The adapter must enqueue replies on
     * the Raft loop (never invoke inline), validate schema, peer channel, sender, type
     * and correlationId, and invoke only the callback belonging to that exact request.
     * Timeout/disconnection produces no vote. Propagate callback storage failures to
     * the node lifecycle so the whole node fails closed; never swallow them as timeouts.
     */
    @FunctionalInterface
    public interface VoteTransport {
        void requestVote(String peerId, RequestVoteRequestDto request, VoteReply reply);
    }

    @FunctionalInterface
    public interface VoteReply {
        void receive(RequestVoteResponse response) throws RaftStorageException;
    }

    /** Integration hook until RaftNode exposes processing of higher-term responses. */
    @FunctionalInterface
    public interface HigherTermHandler {
        void observe(long term) throws RaftStorageException;
    }
}
