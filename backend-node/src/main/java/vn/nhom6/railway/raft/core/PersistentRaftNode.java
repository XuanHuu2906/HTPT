package vn.nhom6.railway.raft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.election.ElectionTimer;
import vn.nhom6.railway.raft.election.PersistentElectionState;
import vn.nhom6.railway.raft.replication.AppendEntriesHandler;
import vn.nhom6.railway.raft.replication.PersistentLogAppender;

/**
 * Durable election/follower core. All calls belong to the owning component's Raft loop.
 * Shares its exact RaftState and one term/vote persistence boundary with election.
 * Leader append is local acceptance only; leader majority replication/commit is not provided here.
 */
public final class PersistentRaftNode implements RaftNode {
    private final String localId;
    private final RaftState state;
    private final PersistentElectionState persistence;
    private final PersistentLogAppender appender;
    private final List<CommitListener> listeners = new ArrayList<>();
    private AppendEntriesHandler appendHandler;
    private Runnable onStepDown = () -> {};
    private long delivered;
    private boolean failed;

    public PersistentRaftNode(String localId, RaftState state, RaftStorage storage)
            throws RaftStorage.RaftStorageException {
        if (localId == null || !localId.matches("node-[1-5]")) {
            throw new IllegalArgumentException("configured voter required");
        }
        this.localId = localId;
        this.state = Objects.requireNonNull(state);
        persistence = new PersistentElectionState(state, storage);
        appender = new PersistentLogAppender(storage);
        var durable = appender.recover();
        if (state.currentTerm() != durable.currentTerm()
                || !Objects.equals(state.votedFor(), durable.votedFor())
                || state.lastApplied() != durable.lastApplied()
                || !state.entries().equals(durable.log())) {
            throw new IllegalArgumentException("core must use the state restored from its own storage");
        }
        delivered = state.lastApplied();
    }

    public PersistentElectionState persistence() { return persistence; }

    /** Bind once, after the component constructs its sole election timer. */
    public void bindLifecycle(ElectionTimer timer, Runnable onStepDown) {
        if (appendHandler != null) throw new IllegalStateException("lifecycle already bound");
        this.onStepDown = Objects.requireNonNull(onStepDown);
        appendHandler = new AppendEntriesHandler(persistence, appender, timer);
    }

    @Override public Role role() { return state.role(); }
    @Override public long currentTerm() { return state.currentTerm(); }
    @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
    @Override public long commitIndex() { return state.commitIndex(); }
    @Override public long lastApplied() { return state.lastApplied(); }
    @Override public long lastLogIndex() { return state.lastLogIndex(); }
    @Override public void onApplied(long index) { requireHealthy(); state.markApplied(index); }

    @Override public void onElectionTimeout() throws RaftStorage.RaftStorageException {
        requireHealthy();
        if (role() == Role.LEADER) throw new IllegalStateException("leader cannot campaign");
        try { persistence.startElection(localId); }
        catch (RaftStorage.RaftStorageException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    public void observeHigherTerm(long term) throws RaftStorage.RaftStorageException {
        requireHealthy();
        try {
            if (term > currentTerm()) {
                onStepDown.run();
                persistence.observeHigherTerm(term);
            }
        } catch (RaftStorage.RaftStorageException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
            throws RaftStorage.RaftStorageException {
        requireHealthy();
        try {
            boolean granted = persistence.grantVote(request);
            if (role() != Role.LEADER) onStepDown.run();
            return new RequestVoteResponse(currentTerm(), granted);
        } catch (RaftStorage.RaftStorageException failure) {
            failed = true;
            onStepDown.run();
            throw failure;
        }
    }

    @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request)
            throws RaftStorage.RaftStorageException {
        requireHealthy();
        try {
            var response = Objects.requireNonNull(appendHandler, "lifecycle not bound")
                    .handle(request, request.leaderId());
            if (role() != Role.LEADER) onStepDown.run();
            deliverCommitted();
            return response;
        } catch (RaftStorage.RaftStorageException | IllegalStateException failure) {
            failed = true;
            onStepDown.run();
            throw failure;
        }
    }

    /** DTO conversion stays in the existing follower handler, including canonical JSON rules. */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequestDto request, String sender)
            throws RaftStorage.RaftStorageException {
        requireHealthy();
        try {
            var response = Objects.requireNonNull(appendHandler, "lifecycle not bound").handle(request, sender);
            if (role() != Role.LEADER) onStepDown.run();
            deliverCommitted();
            return response;
        } catch (RaftStorage.RaftStorageException | IllegalStateException failure) {
            failed = true;
            onStepDown.run();
            throw failure;
        }
    }

    @Override public AppendOutcome appendCommand(String type, String payload) {
        requireHealthy();
        if (role() != Role.LEADER) return new NotLeader(state.leaderId());
        var outcome = appender.appendLeaderCommand(type, payload, currentTerm());
        if (outcome instanceof Accepted accepted) {
            state.append(new RaftLogEntry(accepted.index(), accepted.term(), type, payload));
        } else if (outcome instanceof StorageFailure) {
            failed = true;
            onStepDown.run();
        }
        return outcome;
    }

    @Override public void addCommitListener(CommitListener listener) {
        requireHealthy();
        listeners.add(Objects.requireNonNull(listener));
    }

    private void deliverCommitted() {
        if (listeners.isEmpty()) return;
        while (delivered < commitIndex()) {
            var entry = state.entryAt(delivered + 1);
            try {
                for (var listener : listeners) listener.onCommitted(entry);
                delivered++;
            } catch (RuntimeException failure) {
                failed = true;
                onStepDown.run();
                return; // Do not escape a listener exception or deliver the next entry.
            }
        }
    }

    private void requireHealthy() {
        if (failed) throw new IllegalStateException("Raft core failed closed; reload storage");
    }
}
