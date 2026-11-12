package vn.nhom6.railway.networking;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone mock/fake implementation of {@link RaftRpcProcessor} for testing and bootstrapping.
 *
 * <p>Enables fully decoupled verification of RPC networking handlers (TV3-D12)
 * without requiring the live Raft consensus loop.
 */
public class FakeRaftRequestHandler implements RaftRpcProcessor {

    private final AtomicLong currentTerm = new AtomicLong(1L);
    private final AtomicBoolean voteGranted = new AtomicBoolean(true);
    private final AtomicBoolean appendSuccess = new AtomicBoolean(true);
    private final AtomicBoolean failWithException = new AtomicBoolean(false);
    private volatile String failureMessage = "Simulated Raft storage failure";

    private final List<VoteInvocation> voteInvocations = new CopyOnWriteArrayList<>();
    private final List<AppendInvocation> appendInvocations = new CopyOnWriteArrayList<>();

    public FakeRaftRequestHandler() {
        this(1L);
    }

    public FakeRaftRequestHandler(long initialTerm) {
        this.currentTerm.set(initialTerm);
    }

    @Override
    public RequestVoteResponseDto handleRequestVote(RequestVoteRequestDto request, String senderId) throws Exception {
        if (failWithException.get()) {
            throw new IOException(failureMessage);
        }

        voteInvocations.add(new VoteInvocation(request, senderId));

        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
        }

        boolean granted = voteGranted.get() && (request.getTerm() >= currentTerm.get());
        return new RequestVoteResponseDto(currentTerm.get(), granted);
    }

    @Override
    public AppendEntriesResponseDto handleAppendEntries(AppendEntriesRequestDto request, String senderId) throws Exception {
        if (failWithException.get()) {
            throw new IOException(failureMessage);
        }

        appendInvocations.add(new AppendInvocation(request, senderId));

        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
        }

        if (!appendSuccess.get() || request.getTerm() < currentTerm.get()) {
            return AppendEntriesResponseDto.failure(currentTerm.get());
        }

        long matchIndex = request.getPrevLogIndex() + request.getEntries().size();
        return AppendEntriesResponseDto.success(currentTerm.get(), matchIndex);
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public void setCurrentTerm(long term) {
        this.currentTerm.set(term);
    }

    public void setVoteGranted(boolean granted) {
        this.voteGranted.set(granted);
    }

    public void setAppendSuccess(boolean success) {
        this.appendSuccess.set(success);
    }

    public void setFailWithException(boolean fail, String message) {
        this.failWithException.set(fail);
        this.failureMessage = message;
    }

    public List<VoteInvocation> getVoteInvocations() {
        return Collections.unmodifiableList(new ArrayList<>(voteInvocations));
    }

    public List<AppendInvocation> getAppendInvocations() {
        return Collections.unmodifiableList(new ArrayList<>(appendInvocations));
    }

    public void clearInvocations() {
        voteInvocations.clear();
        appendInvocations.clear();
    }

    public record VoteInvocation(RequestVoteRequestDto request, String senderId) {}
    public record AppendInvocation(AppendEntriesRequestDto request, String senderId) {}
}
