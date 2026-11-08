package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.Set;

import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;

/**
 * Persists election transitions of the existing core state (SPEC-04 sections 3 and 9).
 * No separate term/vote cache, timer, transport, JDBC or vote rules live here.
 *
 * <p>Construct once per node from state already restored from this storage. The core may
 * use startElection/observeHigherTerm; RequestVoteHandler uses grantVote. All calls run
 * on the serialized Raft event loop. Do not also persist the same transition elsewhere.
 *
 * <p>A failed save may have succeeded on disk. Never roll back memory or retry on this
 * instance: all transitions then fail closed. The owning node must stop all Raft activity,
 * reload storage and create fresh state/adapters before participating again. The caller
 * must stop its timer and propagate failures to node lifecycle; this class owns neither.
 */
public final class PersistentElectionState {
    private static final Set<String> VOTERS =
            Set.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final RaftState state;
    private final RaftStorage storage;
    private boolean failed;

    public PersistentElectionState(RaftState state, RaftStorage storage) {
        this.state = Objects.requireNonNull(state, "state");
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    /** The very same model owned by the core; callers must not bypass persistence. */
    public RaftState state() { return state; }

    /** Persists the incremented term/self-vote before the caller may send RequestVote. */
    public long startElection(String selfId) throws RaftStorageException {
        requireHealthy();
        requireVoter(selfId);
        state.startElection(selfId);
        persist();
        return state.currentTerm();
    }

    /** Persist a higher observed term with a cleared vote before processing a peer response. */
    public void observeHigherTerm(long term) throws RaftStorageException {
        requireHealthy();
        if (term < 0) throw new IllegalArgumentException("term must be nonnegative");
        if (term > state.currentTerm()) {
            state.becomeFollower(term);
            persist();
        }
    }

    /**
     * Applies the core vote rules and persists any changed term/vote before returning.
     * A higher-term denial still persists (term, null). An unchanged retry needs no write.
     * Failure throws instead of returning any grant or denial; even a matching retry is
     * forbidden after failure because memory can be ahead of storage.
     */
    public boolean grantVote(RequestVoteRequest request) throws RaftStorageException {
        requireHealthy();
        Objects.requireNonNull(request, "request");
        requireVoter(request.candidateId());
        long previousTerm = state.currentTerm();
        String previousVote = state.votedFor();
        boolean granted = VoteRules.grantVote(state, request.term(), request.candidateId(),
                request.lastLogIndex(), request.lastLogTerm());
        if (previousTerm != state.currentTerm() || !Objects.equals(previousVote, state.votedFor())) {
            persist();
        }
        return granted;
    }

    void requireHealthy() {
        if (failed) {
            throw new IllegalStateException("election persistence failed closed; reload storage before reuse");
        }
    }

    private void persist() throws RaftStorageException {
        try {
            storage.saveTermAndVote(state.currentTerm(), state.votedFor());
        } catch (RaftStorageException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    private static void requireVoter(String id) {
        if (id == null || !VOTERS.contains(id)) {
            throw new IllegalArgumentException("id must be one of the five voters");
        }
    }
}
