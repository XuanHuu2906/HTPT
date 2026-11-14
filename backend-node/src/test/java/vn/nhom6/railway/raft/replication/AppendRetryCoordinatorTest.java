package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppendRetryCoordinatorTest {

    private static final String LEADER = "node-1";
    private static final String PEER = "node-2";
    private final Queue<Runnable> loop = new ArrayDeque<>();
    private final ManualScheduler retryScheduler = new ManualScheduler();
    private final ScheduledThreadPoolExecutor adapterScheduler = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "append-retry-adapter-timeout");
        thread.setDaemon(true);
        return thread;
    });
    private final List<String> giveUps = new ArrayList<>();
    private final List<Long> higherTerms = new ArrayList<>();
    private final List<Throwable> serviceFailures = new ArrayList<>();
    private Harness harness;

    @AfterEach
    void stopSender() {
        if (harness != null) {
            harness.service.stop();
        }
        adapterScheduler.shutdownNow();
    }

    @Test
    void rejectionLowersNextIndexByExactlyOne() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);

        reject(harness, PEER, request, 2L);

        assertEquals(4L, harness.tracker.nextIndexOf(PEER));
    }

    @Test
    void rejectionNeverLowersNextIndexBelowMatchPlusOne() {
        harness = harness(3, Set.of(PEER), 8, 3, null);
        harness.state.advanceMatchIndex(PEER, 2L);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);

        reject(harness, PEER, request, 2L);

        assertEquals(3L, harness.tracker.nextIndexOf(PEER));
        assertTrue(giveUps.isEmpty());
    }

    @Test
    void rejectionAtFirstEntryDoesNotLowerNextIndexBelowOne() {
        harness = harness(0, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);

        reject(harness, PEER, request, request.term());

        assertEquals(1L, harness.tracker.nextIndexOf(PEER));
        assertEquals(1, giveUps.size());
    }

    @Test
    void staleRejectionDoesNotChangeProgress() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest stale = new RaftNode.AppendEntriesRequest(
                harness.state.currentTerm(), LEADER, 2L, 1L, List.of(), 0L);

        reject(harness, PEER, stale, harness.state.currentTerm());

        assertEquals(5L, harness.tracker.nextIndexOf(PEER));
        assertTrue(retryScheduler.delays.isEmpty());
    }

    @Test
    void responseFromDifferentTermDoesNotBacktrack() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);

        reject(harness, PEER, request, request.term() - 1L);

        assertEquals(5L, harness.tracker.nextIndexOf(PEER));
    }

    @Test
    void higherTermResponseCallsStepDownHookWithoutBacktracking() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);
        harness.coordinator.onRejected(PEER, request,
                new RaftNode.AppendEntriesResponse(request.term() + 1L, false, 0L));

        assertEquals(List.of(request.term() + 1L), higherTerms);
        assertEquals(5L, harness.tracker.nextIndexOf(PEER));
    }

    @Test
    void givesUpOnceWhenBacktrackLimitIsExceeded() {
        harness = harness(5, Set.of(PEER), 1, 3, null);
        reject(harness, PEER, request(harness.state, PEER), 2L);
        retryScheduler.advance(50L);
        reject(harness, PEER, request(harness.state, PEER), 2L);
        reject(harness, PEER, request(harness.state, PEER), 2L);

        assertEquals(1, giveUps.size());
        assertEquals(5L, harness.tracker.nextIndexOf(PEER));
        assertEquals(0, retryScheduler.pending());
    }

    @Test
    void successfulAckResetsBacktrackCount() {
        harness = harness(4, Set.of(PEER), 8, 2, null);
        reject(harness, PEER, request(harness.state, PEER), 2L);
        RaftNode.AppendEntriesRequest success = new RaftNode.AppendEntriesRequest(
                harness.state.currentTerm(), LEADER, 0L, 0L,
                List.of(harness.state.entryAt(1L)), 0L);
        harness.service.handleAppendResult(PEER, success,
                new RaftTransportAdapter.Replied(PEER, success,
                        new RaftNode.AppendEntriesResponse(success.term(), true, 1L)));
        reject(harness, PEER, request(harness.state, PEER), 2L);

        assertEquals(List.of(50L, 50L), retryScheduler.delays);
    }

    @Test
    void senderLimitsEveryCatchUpBatchAndContinuesAsynchronously() {
        harness = harness(10, Set.of(PEER), 8, 3, null);
        harness.state.updateNextIndex(PEER, 1L);

        harness.service.replicate(PEER);
        drain();

        assertEquals(List.of(3, 3, 3, 1), harness.rpc.entryCounts);
        assertEquals(0L, harness.state.commitIndex());
        assertEquals(10L, harness.tracker.matchIndexOf(PEER));
    }

    @Test
    void fourPeerProgressAndRetryStateAreIndependent() {
        Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
        harness = harness(3, peers, 8, 2, null);
        for (String peer : peers) {
            reject(harness, peer, request(harness.state, peer), 2L);
        }
        RaftNode.AppendEntriesRequest success = new RaftNode.AppendEntriesRequest(
                harness.state.currentTerm(), LEADER, 2L, harness.state.termAt(2L),
                List.of(harness.state.entryAt(3L)), 0L);
        harness.service.handleAppendResult("node-2", success,
                new RaftTransportAdapter.Replied("node-2", success,
                        new RaftNode.AppendEntriesResponse(success.term(), true, 3L)));

        assertEquals(4L, harness.tracker.nextIndexOf("node-2"));
        assertEquals(3L, harness.tracker.nextIndexOf("node-3"));
        assertEquals(3L, harness.tracker.nextIndexOf("node-4"));
        assertEquals(3L, harness.tracker.nextIndexOf("node-5"));
    }

    @Test
    void nonLeaderIgnoresRejection() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);
        harness.state.becomeFollower(harness.state.currentTerm());

        reject(harness, PEER, request, request.term());

        assertEquals(Role.FOLLOWER, harness.state.role());
        assertTrue(giveUps.isEmpty());
        assertTrue(retryScheduler.delays.isEmpty());
    }

    @Test
    void rejectionAndRetryNeverAdvanceCommitIndex() {
        harness = harness(4, Set.of(PEER), 8, 3, null);
        harness.state.advanceCommitIndex(2L);
        RaftNode.AppendEntriesRequest request = request(harness.state, PEER);

        reject(harness, PEER, request, request.term());

        assertEquals(2L, harness.state.commitIndex());
    }

    @Test
    void followerWithConflictingUncommittedSuffixConvergesWithoutTruncatingPrefix() throws Exception {
        List<RaftLogEntry> followerEntries = new ArrayList<>();
        for (int index = 1; index <= 3; index++) {
            followerEntries.add(RaftLogEntry.noOp(index, 1L));
        }
        for (int index = 4; index <= 6; index++) {
            followerEntries.add(RaftLogEntry.noOp(index, 2L));
        }
        Follower follower = follower(followerEntries, 0L);
        harness = harness(10, Set.of(PEER), 16, 3, follower);

        harness.service.replicate(PEER);
        drain();
        driveUntilConverged(harness, follower);

        assertEquals(harness.state.entries(), follower.storage.storedLog());
        assertEquals(10L, harness.tracker.matchIndexOf(PEER));
        assertTrue(replacementIndexes(follower.storage).stream().allMatch(index -> index >= 4L));
        assertTrue(follower.storage.calls().stream().anyMatch(call -> call.startsWith("replaceSuffix(4,")));
    }

    @Test
    void followerMissingSuffixCatchesUpWithoutTouchingMatchingPrefix() throws Exception {
        List<RaftLogEntry> followerEntries = List.of(
                RaftLogEntry.noOp(1L, 1L), RaftLogEntry.noOp(2L, 1L), RaftLogEntry.noOp(3L, 1L));
        Follower follower = follower(followerEntries, 0L);
        harness = harness(10, Set.of(PEER), 16, 3, follower);

        harness.service.replicate(PEER);
        drain();
        driveUntilConverged(harness, follower);

        assertEquals(harness.state.entries(), follower.storage.storedLog());
        assertTrue(replacementIndexes(follower.storage).stream().allMatch(index -> index > 3L));
    }

    @Test
    void followerWithCommittedEntryInConflictFailsClosedWithoutChangingPrefix() throws Exception {
        List<RaftLogEntry> followerEntries = new ArrayList<>();
        for (int index = 1; index <= 3; index++) {
            followerEntries.add(RaftLogEntry.noOp(index, 1L));
        }
        for (int index = 4; index <= 6; index++) {
            followerEntries.add(RaftLogEntry.noOp(index, 2L));
        }
        Follower follower = follower(followerEntries, 4L);
        harness = harness(10, Set.of(PEER), 16, 3, follower);

        harness.service.replicate(PEER);
        drain();
        backtrackTo(harness, 4L);
        harness.service.replicate(PEER);
        drain();

        assertEquals(followerEntries, follower.storage.storedLog());
        assertEquals(0L, harness.tracker.matchIndexOf(PEER));
        assertTrue(retryScheduler.pending() > 0);
    }

    @Test
    void deepMismatchStopsAtConfiguredBacktrackLimit() throws Exception {
        Follower follower = follower(List.of(RaftLogEntry.noOp(1L, 1L)), 0L);
        harness = harness(10, Set.of(PEER), 2, 3, follower);

        harness.service.replicate(PEER);
        drain();
        retryScheduler.advance(50L);
        drain();
        retryScheduler.advance(100L);
        drain();
        retryScheduler.advance(200L);
        drain();

        assertEquals(1, giveUps.size());
        assertEquals(9L, harness.tracker.nextIndexOf(PEER));
    }

    @Test
    void lostReplyCanRetryAndConvergeUsingManualSchedulers() throws Exception {
        Follower follower = follower(List.of(
                RaftLogEntry.noOp(1L, 1L), RaftLogEntry.noOp(2L, 1L), RaftLogEntry.noOp(3L, 1L)), 0L);
        follower.rpc.dropReplies = 1;
        harness = harness(6, Set.of(PEER), 16, 2, follower);

        harness.service.replicate(PEER);
        drain();
        follower.rpc.failDroppedReply();
        drain();
        retryScheduler.advance(50L);
        driveUntilConverged(harness, follower);

        assertEquals(harness.state.entries(), follower.storage.storedLog());
        assertEquals(6L, harness.tracker.matchIndexOf(PEER));
    }

    private Harness harness(int entries, Set<String> peers, int maxAttempts, int batchMax, Follower follower) {
        RaftState state = leaderState(entries, peers);
        ManualRpc rpc = follower == null ? new ManualRpc() : follower.rpc;
        RaftTransportAdapter transport = new RaftTransportAdapter(LEADER, rpc,
                Duration.ofSeconds(30L), loop::add, adapterScheduler, serviceFailures::add);
        RaftReplicationService service = new RaftReplicationService(LEADER, state, transport,
                peers, loop::add, retryScheduler, batchMax,
                new RaftReplicationService.Backoff(50L, 200L), new RaftReplicationService.Hooks() {
                    @Override
                    public void onHigherTerm(long term) {
                        higherTerms.add(term);
                    }

                    @Override
                    public void onFailure(String peerId, Throwable failure) {
                        serviceFailures.add(failure);
                    }
                });
        ReplicationTracker tracker = new ReplicationTracker(state, peers);
        AppendRetryCoordinator coordinator = new AppendRetryCoordinator(
                state, tracker, service, loop::add, retryScheduler,
                new RaftReplicationService.Backoff(50L, 200L), maxAttempts,
                new AppendRetryCoordinator.Hooks() {
                    @Override
                    public void onHigherTerm(long term) {
                        higherTerms.add(term);
                    }

                    @Override
                    public void onGiveUp(String peerId, String reason) {
                        giveUps.add(peerId + ":" + reason);
                    }
                });
        return new Harness(state, tracker, service, coordinator, rpc);
    }

    private static RaftState leaderState(int entries, Set<String> peers) {
        RaftState state = new RaftState();
        state.startElection(LEADER);
        for (int index = 1; index <= entries; index++) {
            state.append(RaftLogEntry.noOp(index, 1L));
        }
        state.startElection(LEADER);
        state.becomeLeader(peers);
        return state;
    }

    private static Follower follower(List<RaftLogEntry> entries, long lastApplied) throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, entries, lastApplied);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        PersistentElectionState electionState = new PersistentElectionState(state, storage);
        AppendEntriesHandler handler = new AppendEntriesHandler(
                electionState, appender, new LogConflictResolver(appender), new AppendEntriesHandler.TimerHook() {
                    @Override
                    public void onValidAppendEntries(long requestTerm) {
                    }

                    @Override
                    public void start() {
                    }

                    @Override
                    public void close() {
                    }
                });
        ManualRpc rpc = new ManualRpc();
        rpc.followers.put(PEER, handler);
        rpc.storages.put(PEER, storage);
        return new Follower(storage, rpc);
    }

    private static RaftNode.AppendEntriesRequest request(RaftState state, String peer) {
        long next = state.nextIndexOf(peer);
        long previous = next - 1L;
        return new RaftNode.AppendEntriesRequest(
                state.currentTerm(), LEADER, previous, state.termAt(previous), List.of(), state.commitIndex());
    }

    private void reject(Harness harness, String peer, RaftNode.AppendEntriesRequest request, long responseTerm) {
        harness.service.handleAppendResult(peer, request,
                new RaftTransportAdapter.Replied(peer, request,
                        new RaftNode.AppendEntriesResponse(responseTerm, false, 0L)));
    }

    private void backtrackTo(Harness harness, long targetNext) {
        while (harness.tracker.nextIndexOf(PEER) > targetNext) {
            RaftNode.AppendEntriesRequest request = request(harness.state, PEER);
            reject(harness, PEER, request, request.term());
        }
    }

    private void driveUntilConverged(Harness harness, Follower follower) {
        for (int attempt = 0; attempt < 100; attempt++) {
            drain();
            if (harness.state.entries().equals(follower.storage.storedLog())
                    && harness.tracker.matchIndexOf(PEER) == harness.state.lastLogIndex()) {
                return;
            }
            retryScheduler.advance(250L);
        }
        throw new AssertionError("bounded manual retries did not converge follower log: nextIndex="
                + harness.tracker.nextIndexOf(PEER) + ", matchIndex=" + harness.tracker.matchIndexOf(PEER)
                + ", leaderLog=" + harness.state.entries() + ", followerLog=" + follower.storage.storedLog()
                + ", requests=" + follower.rpc.entryCounts + ", replacements=" + follower.storage.calls());
    }

    private static List<Long> replacementIndexes(FakeRaftStorage storage) {
        List<Long> indexes = new ArrayList<>();
        for (String call : storage.calls()) {
            if (call.startsWith("replaceSuffix(")) {
                int comma = call.indexOf(',');
                indexes.add(Long.parseLong(call.substring("replaceSuffix(".length(), comma)));
            }
        }
        return indexes;
    }

    private void drain() {
        Runnable task;
        while ((task = loop.poll()) != null) {
            task.run();
        }
    }

    private record Harness(
            RaftState state,
            ReplicationTracker tracker,
            RaftReplicationService service,
            AppendRetryCoordinator coordinator,
            ManualRpc rpc) {
    }

    private record Follower(FakeRaftStorage storage, ManualRpc rpc) {
    }

    private static final class ManualScheduler implements RaftReplicationService.Scheduler {
        private final List<Task> tasks = new ArrayList<>();
        private final List<Long> delays = new ArrayList<>();
        private long now;

        @Override
        public Cancellable schedule(Runnable action, long delayMillis) {
            delays.add(delayMillis);
            Task task = new Task(action, Math.addExact(now, delayMillis));
            tasks.add(task);
            return task;
        }

        void advance(long millis) {
            now = Math.addExact(now, millis);
            while (true) {
                Task due = null;
                for (Task task : tasks) {
                    if (!task.cancelled && task.dueAt <= now && (due == null || task.dueAt < due.dueAt)) {
                        due = task;
                    }
                }
                if (due == null) {
                    return;
                }
                tasks.remove(due);
                due.action.run();
            }
        }

        int pending() {
            return (int) tasks.stream().filter(task -> !task.cancelled).count();
        }

        private static final class Task implements Cancellable {
            private final Runnable action;
            private final long dueAt;
            private boolean cancelled;

            private Task(Runnable action, long dueAt) {
                this.action = action;
                this.dueAt = dueAt;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
    }

    private static final class ManualRpc implements RaftTransportAdapter.RaftRpcClient {
        private final Map<String, AppendEntriesHandler> followers = new HashMap<>();
        private final Map<String, FakeRaftStorage> storages = new HashMap<>();
        private final List<Integer> entryCounts = new ArrayList<>();
        private final List<CompletableFuture<MessageEnvelope<JsonNode>>> dropped = new ArrayList<>();
        private int dropReplies;

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
            entryCounts.add(request.getPayload().getEntries().size());
            CompletableFuture<MessageEnvelope<JsonNode>> response = new CompletableFuture<>();
            if (dropReplies > 0) {
                dropReplies--;
                dropped.add(response);
                return response;
            }

            try {
                RaftNode.AppendEntriesResponse payload;
                AppendEntriesHandler follower = followers.get(peerId);
                if (follower == null) {
                    long match = request.getPayload().getPrevLogIndex()
                            + request.getPayload().getEntries().size();
                    payload = new RaftNode.AppendEntriesResponse(request.getPayload().getTerm(), true, match);
                } else {
                    AppendEntriesRequestDto decoded = MessageCodec.decode(
                            MessageCodec.encode(request), AppendEntriesRequestDto.class).getPayload();
                    payload = follower.handle(decoded, LEADER);
                }
                Map<String, Object> responsePayload = Map.of(
                        "term", payload.term(),
                        "success", payload.success(),
                        "matchIndex", payload.matchIndex());
                response.complete(MessageCodec.decode(MessageCodec.encode(
                        MessageEnvelope.createResponse(request.getMessageId(),
                                MessageType.APPEND_ENTRIES_RESPONSE, peerId, responsePayload))));
            } catch (Exception failure) {
                response.completeExceptionally(failure);
            }
            return response;
        }

        private void failDroppedReply() {
            if (dropped.isEmpty()) {
                throw new AssertionError("no dropped reply is available to fail");
            }
            dropped.remove(0).completeExceptionally(new TimeoutException("simulated lost AppendEntries reply"));
        }
    }
}
