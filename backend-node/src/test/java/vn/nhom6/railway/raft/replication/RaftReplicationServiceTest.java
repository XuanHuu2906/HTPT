package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftReplicationServiceTest {

    private static final String LOCAL = "node-1";
    private static final String PEER = "node-2";
    private static final String OTHER = "node-3";

    private final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
    private final ScheduledThreadPoolExecutor adapterScheduler = new ScheduledThreadPoolExecutor(1);

    @Test
    void emitsHeartbeatForEmptyLeaderLog() throws Exception {
        RaftState state = new RaftState();
        state.startElection(LOCAL);
        state.becomeLeader(Set.of(PEER));
        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter transport = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);

        service.replicate(PEER);
        drain();

        assertEquals(1, rpc.requests.size());
        AppendEntriesRequestDto request = decode(rpc.requests.get(0));
        assertEquals(state.currentTerm(), request.getTerm());
        assertEquals(LOCAL, request.getLeaderId());
        assertEquals(0L, request.getPrevLogIndex());
        assertEquals(0L, request.getPrevLogTerm());
        assertTrue(request.getEntries().isEmpty());
        assertEquals(state.commitIndex(), request.getLeaderCommit());
    }

    @Test
    void usesNextIndexAndRespectsBatchMax() throws Exception {
        RaftState state = new RaftState();
        state.startElection(LOCAL);
        state.becomeLeader(Set.of(PEER));
        for (int i = 1; i <= 5; i++) {
            state.append(new RaftLogEntry(i, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                    "{\"requestId\":\"r-" + i + "\"}"));
        }
        state.updateNextIndex(PEER, 3L);
        state.advanceCommitIndex(4L);

        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter transport = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 2,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());

        service.replicate(PEER);
        drain();

        AppendEntriesRequestDto request = decode(rpc.requests.get(0));
        assertEquals(2L, request.getPrevLogIndex());
        assertEquals(1L, request.getPrevLogTerm());
        assertEquals(2, request.getEntries().size());
        assertEquals(3L, request.getEntries().get(0).getIndex());
        assertEquals(4L, request.getEntries().get(1).getIndex());
        assertEquals(4L, request.getLeaderCommit());
    }

    @Test
    void doesNothingWhenLeaderStateIsNotActive() throws Exception {
        RaftState state = new RaftState();
        state.becomeFollower(1L, OTHER);
        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter transport = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());

        service.replicate(PEER);
        drain();

        assertTrue(rpc.requests.isEmpty());
    }

    @Test
    void advancesMatchIndexOnSuccessfulRepliedResult() {
        RaftState state = leaderState();
        state.append(new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-1\"}"));
        state.append(new RaftLogEntry(2L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-2\"}"));
        state.append(new RaftLogEntry(3L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-3\"}"));
        state.updateNextIndex(PEER, 3L);
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);

        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                state.currentTerm(), LOCAL, 2L, 1L, List.of(state.entryAt(3L)), state.commitIndex());
        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, true, 3L)));

        assertEquals(3L, state.matchIndexOf(PEER));
        assertEquals(0, hooks.rejectedCount());
    }

    @Test
    void doesNotAdvanceMatchIndexWhenResponseMismatchOccurs() {
        RaftState state = leaderState();
        state.append(new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-1\"}"));
        state.append(new RaftLogEntry(2L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-2\"}"));
        state.append(new RaftLogEntry(3L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-3\"}"));
        state.updateNextIndex(PEER, 3L);
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);

        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                state.currentTerm(), LOCAL, 2L, 1L,
                List.of(state.entryAt(3L)), state.commitIndex());

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, true, 1L)));

        assertEquals(0L, state.matchIndexOf(PEER));
        assertEquals(1, hooks.failureCount());
        assertEquals(0, hooks.rejectedCount());
    }

    @Test
    void rejectsAndCallsHookWithoutChangingIndex() {
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, false, 0L)));

        assertEquals(0L, state.matchIndexOf(PEER));
        assertEquals(1, hooks.rejectedCount());
    }

    @Test
    void appliesExponentialBackoffAndCapsDelay() {
        ManualScheduler scheduler = new ManualScheduler();
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, scheduler, 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);

        for (int i = 0; i < 4; i++) {
            service.handleAppendResult(PEER, request,
                    new RaftTransportAdapter.Failed(PEER, request, RaftTransportAdapter.FailureKind.TIMEOUT));
        }

        assertEquals(List.of(50L, 100L, 200L, 200L), scheduler.delays);
        assertEquals(1, scheduler.pending());
        assertEquals(0L, state.matchIndexOf(PEER));
    }

    @Test
    void ignoresOlderSuccessfulReplyWithoutReducingMatchIndex() {
        RaftState state = leaderState();
        for (int i = 1; i <= 3; i++) {
            state.append(new RaftLogEntry(i, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                    "{\"requestId\":\"r-" + i + "\"}"));
        }
        state.advanceMatchIndex(PEER, 3L);
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, adapterWithNoRpc(),
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 1L, 1L, List.of(state.entryAt(2L)), 0L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, true, 2L)));

        assertEquals(3L, state.matchIndexOf(PEER));
    }

    @Test
    void callsHigherTermHookAndIgnoresStaleReply() {
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.LateHigherTerm(PEER, 2L));

        assertEquals(1, hooks.higherTermEvents.size());
        assertEquals(2L, hooks.higherTermEvents.get(0));
    }

    @Test
    void reportsHigherTermReplyEvenWhenOriginalRequestIsStale() {
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);
        state.becomeFollower(2L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(3L, false, 0L)));

        assertEquals(List.of(3L), hooks.higherTermEvents);
    }

    @Test
    void ignoresReplyAfterRoleAndTermChange() {
        RaftState state = leaderState();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, adapterWithNoRpc(),
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);
        state.becomeFollower(2L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, true, 0L)));

        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(2L, state.currentTerm());
        assertTrue(hooks.higherTermEvents.isEmpty());
    }

    @Test
    void doesNotSendSecondAppendWhilePeerRpcIsInFlight() throws Exception {
        RaftState state = leaderState();
        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter transport = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());

        service.replicate(PEER);
        service.replicate(PEER);
        drain();

        assertEquals(1, rpc.requests.size());
    }

    @Test
    void replicatesOtherPeersIndependently() throws Exception {
        Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
        RaftState state = new RaftState();
        state.startElection(LOCAL);
        state.becomeLeader(peers);
        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter transport = new RaftTransportAdapter(LOCAL, rpc,
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
        ManualScheduler scheduler = new ManualScheduler();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                peers, loop::add, scheduler, 64,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());

        service.replicateAll();
        drain();
        rpc.responses.get(0).completeExceptionally(new IOException("fake peer failure"));
        drain();

        assertEquals(peers, Set.copyOf(rpc.peers));
        assertEquals(4, rpc.requests.size());
        assertEquals(1, scheduler.pending());
    }

    @Test
    void commitIndexRemainsUnchangedAcrossReplicationResults() {
        RaftState state = leaderState();
        state.append(new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"r-1\"}"));
        state.advanceCommitIndex(1L);
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, adapterWithNoRpc(),
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(state.entryAt(1L)), 1L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, true, 1L)));
        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Replied(PEER, request,
                        new RaftNode.AppendEntriesResponse(1L, false, 0L)));
        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Failed(PEER, request, RaftTransportAdapter.FailureKind.TIMEOUT));

        assertEquals(1L, state.commitIndex());
    }

    @Test
    void rejectsInvalidPeerIds() {
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, new ManualScheduler(), 64,
                new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks());

        assertThrows(IllegalArgumentException.class, () -> service.replicate("node-10"));
        assertThrows(IllegalArgumentException.class, () ->
                new RaftReplicationService(LOCAL, state, transport, List.of(LOCAL), loop::add,
                        new ManualScheduler(), 64,
                        new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks()));
        assertThrows(IllegalArgumentException.class, () ->
                new RaftReplicationService(LOCAL, state, transport, Arrays.asList(PEER, PEER), loop::add,
                        new ManualScheduler(), 64,
                        new RaftReplicationService.Backoff(50L, 200L), new RecordingHooks()));
    }

    @Test
    void stopCancelsScheduledRetries() {
        ManualScheduler scheduler = new ManualScheduler();
        RaftState state = leaderState();
        RaftTransportAdapter transport = adapterWithNoRpc();
        RecordingHooks hooks = new RecordingHooks();
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, transport,
                Set.of(PEER), loop::add, scheduler, 64,
                new RaftReplicationService.Backoff(50L, 200L), hooks);
        RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                1L, LOCAL, 0L, 0L, List.of(), 0L);

        service.handleAppendResult(PEER, request,
                new RaftTransportAdapter.Failed(PEER, request, RaftTransportAdapter.FailureKind.TIMEOUT));
        assertEquals(1, scheduler.pending());

        service.stop();
        assertEquals(0, scheduler.pending());
    }

    private void drain() {
        Runnable task;
        while ((task = loop.poll()) != null) {
            task.run();
        }
    }

    private static AppendEntriesRequestDto decode(MessageEnvelope<AppendEntriesRequestDto> request)
            throws Exception {
        return MessageCodec.decode(MessageCodec.encode(request), AppendEntriesRequestDto.class).getPayload();
    }

    private static RaftState leaderState() {
        RaftState state = new RaftState();
        state.startElection(LOCAL);
        state.becomeLeader(Set.of(PEER));
        return state;
    }

    private RaftTransportAdapter adapterWithNoRpc() {
        return new RaftTransportAdapter(LOCAL, (peerId, request) -> new CompletableFuture<>(),
                Duration.ofSeconds(1L), loop::add, adapterScheduler, e -> {
                });
    }

    private static final class RecordingHooks implements RaftReplicationService.Hooks {
        private final List<Long> higherTermEvents = new ArrayList<>();
        private int rejectedCount;
        private int failureCount;

        @Override
        public void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
            rejectedCount++;
        }

        @Override
        public void onHigherTerm(long term) {
            higherTermEvents.add(term);
        }

        @Override
        public void onFailure(String peerId, Throwable failure) {
            failureCount++;
        }

        int rejectedCount() {
            return rejectedCount;
        }

        int failureCount() {
            return failureCount;
        }
    }

    private static final class ManualScheduler implements RaftReplicationService.Scheduler {
        private final List<Task> tasks = new ArrayList<>();
        private final List<Long> delays = new ArrayList<>();
        private long now;

        @Override
        public Cancellable schedule(Runnable task, long delayMillis) {
            delays.add(delayMillis);
            Task scheduled = new Task(task, now + delayMillis);
            tasks.add(scheduled);
            return scheduled;
        }

        void advance(long millis) {
            now += millis;
            while (true) {
                Task next = null;
                for (Task task : tasks) {
                    if (!task.cancelled && task.dueAt <= now && (next == null || task.dueAt < next.dueAt)) {
                        next = task;
                    }
                }
                if (next == null) {
                    return;
                }
                tasks.remove(next);
                next.task.run();
            }
        }

        int pending() {
            int count = 0;
            for (Task task : tasks) {
                if (!task.cancelled) {
                    count++;
                }
            }
            return count;
        }

        static final class Task implements Cancellable {
            private final Runnable task;
            private final long dueAt;
            private boolean cancelled;

            Task(Runnable task, long dueAt) {
                this.task = task;
                this.dueAt = dueAt;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
    }

    static final class FakeRpcClient implements RaftTransportAdapter.RaftRpcClient {
        final List<MessageEnvelope<AppendEntriesRequestDto>> requests = new ArrayList<>();
        final List<String> peers = new ArrayList<>();
        final List<CompletableFuture<MessageEnvelope<JsonNode>>> responses = new ArrayList<>();

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peerId,
                MessageEnvelope<AppendEntriesRequestDto> request) {
            peers.add(peerId);
            requests.add(request);
            CompletableFuture<MessageEnvelope<JsonNode>> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }
    }
}
