package vn.nhom6.railway.raft.replication;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationTrackerTest {

    private static final String LOCAL = "node-1";
    private static final String PEER = "node-2";
    private final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
    private final ScheduledThreadPoolExecutor transportScheduler = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "replication-tracker-test-timeout");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopScheduler() {
        transportScheduler.shutdownNow();
    }

    @Test
    void initializesProgressForEveryLeaderPeer() {
        Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
        RaftState state = leaderState(3, peers);

        ReplicationTracker tracker = new ReplicationTracker(state, peers);

        assertEquals(Map.of(
                "node-2", new ReplicationTracker.Progress(4L, 0L),
                "node-3", new ReplicationTracker.Progress(4L, 0L),
                "node-4", new ReplicationTracker.Progress(4L, 0L),
                "node-5", new ReplicationTracker.Progress(4L, 0L)), tracker.snapshot());
    }

    @Test
    void recordsVerifiedSuccessAndAdvancesNextIndex() {
        RaftState state = leaderState(3, Set.of(PEER));
        ReplicationTracker tracker = new ReplicationTracker(state, Set.of(PEER));

        assertTrue(tracker.recordSuccess(PEER, 2L));
        assertEquals(new ReplicationTracker.Progress(4L, 2L), tracker.snapshot().get(PEER));
    }

    @Test
    void ignoresOlderSuccessWithoutReducingMatchIndex() {
        RaftState state = leaderState(3, Set.of(PEER));
        ReplicationTracker tracker = new ReplicationTracker(state, Set.of(PEER));
        tracker.recordSuccess(PEER, 2L);

        assertFalse(tracker.recordSuccess(PEER, 1L));
        assertEquals(2L, tracker.matchIndexOf(PEER));
    }

    @Test
    void rejectsSuccessBeyondLocalLog() {
        ReplicationTracker tracker = new ReplicationTracker(leaderState(2, Set.of(PEER)), Set.of(PEER));

        assertThrows(IllegalArgumentException.class, () -> tracker.recordSuccess(PEER, 3L));
    }

    @Test
    void lowersNextIndexWhenExpectedValueMatches() {
        ReplicationTracker tracker = new ReplicationTracker(leaderState(3, Set.of(PEER)), Set.of(PEER));

        assertTrue(tracker.lowerNextIndex(PEER, 4L, 3L));
        assertEquals(3L, tracker.nextIndexOf(PEER));
    }

    @Test
    void staleExpectedNextIndexDoesNotOverwriteNewProgress() {
        RaftState state = leaderState(3, Set.of(PEER));
        ReplicationTracker tracker = new ReplicationTracker(state, Set.of(PEER));
        state.updateNextIndex(PEER, 3L);
        tracker.recordSuccess(PEER, 1L);

        assertFalse(tracker.lowerNextIndex(PEER, 4L, 2L));
        assertEquals(3L, tracker.nextIndexOf(PEER));
    }

    @Test
    void refusesToLowerNextIndexBelowMatchPlusOne() {
        RaftState state = leaderState(3, Set.of(PEER));
        ReplicationTracker tracker = new ReplicationTracker(state, Set.of(PEER));
        tracker.recordSuccess(PEER, 1L);

        assertThrows(IllegalArgumentException.class, () -> tracker.lowerNextIndex(PEER, 2L, 1L));
    }

    @Test
    void refusesNextIndexBelowOne() {
        ReplicationTracker tracker = new ReplicationTracker(leaderState(3, Set.of(PEER)), Set.of(PEER));

        assertThrows(IllegalArgumentException.class, () -> tracker.lowerNextIndex(PEER, 4L, 0L));
    }

    @Test
    void rejectsPeerNotInTracker() {
        ReplicationTracker tracker = new ReplicationTracker(leaderState(1, Set.of(PEER)), Set.of(PEER));

        assertThrows(IllegalArgumentException.class, () -> tracker.recordSuccess("node-5", 0L));
    }

    @Test
    void leaderWriteOnFollowerFailsClosed() {
        RaftState state = leaderState(1, Set.of(PEER));
        ReplicationTracker tracker = new ReplicationTracker(state, Set.of(PEER));
        state.becomeFollower(state.currentTerm());

        assertThrows(IllegalStateException.class, () -> tracker.recordSuccess(PEER, 0L));
    }

    @Test
    void progressForFourPeersIsIndependent() {
        Set<String> peers = Set.of("node-2", "node-3", "node-4", "node-5");
        ReplicationTracker tracker = new ReplicationTracker(leaderState(4, peers), peers);
        tracker.recordSuccess("node-3", 2L);
        tracker.lowerNextIndex("node-4", 5L, 4L);

        assertEquals(Map.of(
                "node-2", new ReplicationTracker.Progress(5L, 0L),
                "node-3", new ReplicationTracker.Progress(5L, 2L),
                "node-4", new ReplicationTracker.Progress(4L, 0L),
                "node-5", new ReplicationTracker.Progress(5L, 0L)), tracker.snapshot());
    }

    @Test
    void integratesWithFakeRpcSenderWithoutAdvancingCommitIndex() throws Exception {
        Set<String> peers = Set.of(PEER, "node-3");
        RaftState state = leaderState(2, peers);
        ReplicationTracker tracker = new ReplicationTracker(state, peers);
        FakeRpcClient rpc = new FakeRpcClient();
        RaftTransportAdapter adapter = new RaftTransportAdapter(LOCAL, rpc, Duration.ofSeconds(30L),
                loop::add, transportScheduler, failure -> {
                    throw new AssertionError("unexpected transport callback failure", failure);
                });
        RaftReplicationService service = new RaftReplicationService(LOCAL, state, adapter, peers,
                loop::add, (task, delay) -> {
                    throw new AssertionError("unexpected retry scheduling");
                }, 64, new RaftReplicationService.Backoff(50L, 200L), new RaftReplicationService.Hooks() {
                });

        service.replicateAll();
        drain();

        assertEquals(Map.of(
                PEER, new ReplicationTracker.Progress(3L, 2L),
                "node-3", new ReplicationTracker.Progress(3L, 2L)), tracker.snapshot());
        assertEquals(0L, state.commitIndex());
        assertEquals(2, rpc.requests.size());
    }

    @Test
    void trackerRequiresExactRaftStatePeerSet() {
        RaftState state = leaderState(0, Set.of(PEER));

        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationTracker(state, Set.of("node-3")));
    }

    @Test
    void snapshotIsImmutable() {
        ReplicationTracker tracker = new ReplicationTracker(leaderState(0, Set.of(PEER)), Set.of(PEER));

        assertThrows(UnsupportedOperationException.class,
                () -> tracker.snapshot().put("node-3", new ReplicationTracker.Progress(1L, 0L)));
    }

    private static RaftState leaderState(int entries, Set<String> peers) {
        RaftState state = new RaftState();
        state.startElection(LOCAL);
        for (int index = 1; index <= entries; index++) {
            state.append(RaftLogEntry.noOp(index, 1L));
        }
        state.becomeLeader(peers);
        return state;
    }

    private void drain() {
        Runnable task;
        while ((task = loop.poll()) != null) {
            task.run();
        }
    }

    private static final class FakeRpcClient implements RaftTransportAdapter.RaftRpcClient {
        private final List<MessageEnvelope<AppendEntriesRequestDto>> requests = new ArrayList<>();

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(
                String peerId, MessageEnvelope<AppendEntriesRequestDto> request) {
            requests.add(request);
            long matchIndex = request.getPayload().getPrevLogIndex()
                    + request.getPayload().getEntries().size();
            try {
                return CompletableFuture.completedFuture(MessageCodec.decode(MessageCodec.encode(
                        MessageEnvelope.createResponse(request.getMessageId(),
                                MessageType.APPEND_ENTRIES_RESPONSE, peerId,
                                AppendEntriesResponseDto.success(request.getPayload().getTerm(), matchIndex)))));
            } catch (IOException failure) {
                throw new IllegalStateException("could not encode fake AppendEntries response", failure);
            }
        }
    }
}
