package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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
import vn.nhom6.railway.raft.election.PersistentElectionState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bridge test between the D11 Leader-side {@link RaftTransportAdapter} and the D09 Follower-side
 * {@link AppendEntriesHandler} (SPEC-03 sections 6 and 6.1; SPEC-04 sections 3.5 and 4).
 *
 * <p><strong>Honesty boundary.</strong> The Leader is a test object, the peer transport is
 * {@code FakeRaftRpcClient}, the follower storage is {@link FakeRaftStorage} and the timer is a
 * recording hook. This test does <strong>not</strong> use TCP, a real Leader, a 5-Node cluster,
 * majority counting, Raft commit, state-machine apply or PostgreSQL/JDBC durability. It proves only
 * that the adapter's wire request is accepted byte-for-byte by the follower handler and that the
 * handler's real response is accepted by the adapter. The real TCP check is deferred until
 * TV3-D11.
 */
class TransportToFollowerRoundTripTest {

    private static final String LEADER = "node-1";
    private static final String FOLLOWER = "node-2";

    private static final String CANONICAL_BOOK = "{\"issuedAt\":\"2026-10-29T10:00:00.000Z\","
            + "\"passengerName\":\"Khach demo\",\"requestId\":\"r-1\",\"seatId\":\"G05\","
            + "\"ticketId\":\"t-1\",\"tripId\":\"SE-001\"}";

    private final FakeRpcClient rpc = new FakeRpcClient();
    private final Queue<Runnable> loop = new ConcurrentLinkedQueue<>();
    private final List<Exception> failures = new ArrayList<>();
    private final List<RaftTransportAdapter.PeerAppendResult> results = new ArrayList<>();
    private final ScheduledThreadPoolExecutor scheduler = scheduler();

    @AfterEach
    void stopScheduler() {
        scheduler.shutdownNow();
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static ScheduledThreadPoolExecutor scheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "d11-round-trip-timeout");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /** Single-request fake client; responses are completed explicitly by the test. */
    static final class FakeRpcClient implements RaftTransportAdapter.RaftRpcClient {

        final List<MessageEnvelope<AppendEntriesRequestDto>> requests = new ArrayList<>();
        final List<CompletableFuture<MessageEnvelope<JsonNode>>> responses = new ArrayList<>();

        @Override
        public CompletableFuture<MessageEnvelope<JsonNode>> exchange(String peerId,
                MessageEnvelope<AppendEntriesRequestDto> request) {
            requests.add(request);
            var future = new CompletableFuture<MessageEnvelope<JsonNode>>();
            responses.add(future);
            return future;
        }

        void respond(int index, String sender, String correlation, Object payload) throws Exception {
            responses.get(index).complete(MessageCodec.decode(MessageCodec.encode(
                    MessageEnvelope.createResponse(correlation, MessageType.APPEND_ENTRIES_RESPONSE,
                            sender, payload))));
        }
    }

    /** Records timer calls without a real scheduler. */
    private static final class RecordingTimer implements AppendEntriesHandler.TimerHook {

        final List<String> events = new ArrayList<>();

        @Override
        public void onValidAppendEntries(long requestTerm) {
            events.add("onValidAppendEntries(" + requestTerm + ")");
        }

        @Override
        public void start() {
            events.add("start");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }

    private record Fixture(FakeRaftStorage storage, RaftState state,
            PersistentLogAppender appender, AppendEntriesHandler handler, RecordingTimer timer) {
    }

    private static Fixture fixture(long term, String votedFor, long lastApplied,
            RaftLogEntry... entries) throws RaftStorage.RaftStorageException {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, votedFor, List.of(entries), lastApplied);
        RaftStorage.PersistentState persisted = storage.load();
        RaftState state = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        PersistentLogAppender appender = new PersistentLogAppender(storage);
        appender.recover();
        PersistentElectionState persistence = new PersistentElectionState(state, storage);
        RecordingTimer timer = new RecordingTimer();
        AppendEntriesHandler handler = new AppendEntriesHandler(
                persistence, appender, new LogConflictResolver(appender), timer);
        return new Fixture(storage, state, appender, handler, timer);
    }

    private RaftTransportAdapter adapter() {
        return new RaftTransportAdapter(LEADER, rpc, Duration.ofSeconds(30L), loop::add,
                scheduler, failures::add);
    }

    private void drain() {
        Runnable task;
        while ((task = loop.poll()) != null) {
            task.run();
        }
    }

    private static RaftNode.AppendEntriesRequest request(long term, long prevLogIndex,
            long prevLogTerm, long leaderCommit, RaftLogEntry... entries) {
        return new RaftNode.AppendEntriesRequest(term, LEADER, prevLogIndex, prevLogTerm,
                List.of(entries), leaderCommit);
    }

    private static AppendEntriesRequestDto decode(
            MessageEnvelope<AppendEntriesRequestDto> envelope) throws Exception {
        return MessageCodec.decode(MessageCodec.encode(envelope),
                AppendEntriesRequestDto.class).getPayload();
    }

    private static Map<String, Object> responsePayload(RaftNode.AppendEntriesResponse response) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("term", response.term());
        payload.put("success", response.success());
        payload.put("matchIndex", response.matchIndex());
        return payload;
    }

    // -------------------------------------------------------------------------------------------
    // Adapter -> Follower
    // -------------------------------------------------------------------------------------------

    @Test
    void adapterOutputIsAcceptedByTheFollowerHandlerByteForByte() throws Exception {
        Fixture follower = fixture(2L, null, 0L);
        RaftLogEntry leaderEntry =
                new RaftLogEntry(1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, CANONICAL_BOOK);
        var adapter = adapter();

        adapter.sendAppendEntries(FOLLOWER, request(2L, 0L, 0L, 0L, leaderEntry), results::add);

        assertEquals(1, rpc.requests.size());
        AppendEntriesRequestDto dto = decode(rpc.requests.get(0));
        RaftNode.AppendEntriesResponse response = follower.handler.handle(dto, LEADER);

        assertTrue(response.success());
        assertEquals(2L, response.term());
        assertEquals(1L, response.matchIndex());
        assertEquals(List.of(leaderEntry), follower.storage().storedLog(),
                "the Follower must persist exactly the Leader's entry, including the payload bytes");
        assertEquals(List.of(leaderEntry), follower.state().entries());
        assertEquals(CANONICAL_BOOK, follower.storage().storedLog().get(0).commandPayload());
        assertTrue(results.isEmpty(), "the adapter still waits for the peer response");
    }

    // -------------------------------------------------------------------------------------------
    // Follower -> Adapter
    // -------------------------------------------------------------------------------------------

    @Test
    void followerResponseIsAcceptedByTheAdapter() throws Exception {
        Fixture follower = fixture(2L, null, 0L);
        RaftLogEntry leaderEntry =
                new RaftLogEntry(1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, CANONICAL_BOOK);
        var adapter = adapter();

        adapter.sendAppendEntries(FOLLOWER, request(2L, 0L, 0L, 0L, leaderEntry), results::add);
        MessageEnvelope<AppendEntriesRequestDto> sent = rpc.requests.get(0);
        RaftNode.AppendEntriesResponse response = follower.handler.handle(decode(sent), LEADER);

        rpc.respond(0, FOLLOWER, sent.getMessageId(), responsePayload(response));
        drain();

        RaftTransportAdapter.Replied replied =
                assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));
        assertEquals(response, replied.response());
        assertEquals(FOLLOWER, replied.peerId());
        assertEquals(2L, replied.response().term());
        assertTrue(replied.response().success());
        assertEquals(1L, replied.response().matchIndex());
    }

    @Test
    void logMismatchRoundTrip() throws Exception {
        Fixture follower = fixture(2L, null, 0L, RaftLogEntry.noOp(1L, 1L));
        var adapter = adapter();
        RaftLogEntry farEntry = RaftLogEntry.noOp(3L, 2L);

        adapter.sendAppendEntries(FOLLOWER, request(2L, 2L, 1L, 0L, farEntry), results::add);
        MessageEnvelope<AppendEntriesRequestDto> sent = rpc.requests.get(0);
        RaftNode.AppendEntriesResponse response = follower.handler.handle(decode(sent), LEADER);

        assertFalse(response.success(), "prevLogIndex 2 is missing on the Follower");
        assertEquals(0L, response.matchIndex());

        rpc.respond(0, FOLLOWER, sent.getMessageId(), responsePayload(response));
        drain();

        RaftTransportAdapter.Replied replied =
                assertInstanceOf(RaftTransportAdapter.Replied.class, results.get(0));
        assertFalse(replied.response().success());
        assertEquals(0L, replied.response().matchIndex());
    }

    // -------------------------------------------------------------------------------------------
    // Why the canonical-payload guard exists
    // -------------------------------------------------------------------------------------------

    @Test
    void nonCanonicalLeaderPayloadWouldDivergeAfterFailover() throws Exception {
        Fixture follower = fixture(2L, null, 0L);
        var adapter = adapter();
        String unsorted = "{\"ticketId\":\"t-1\",\"requestId\":\"r-1\"}";

        assertThrows(IllegalArgumentException.class,
                () -> adapter.sendAppendEntries(FOLLOWER,
                        request(2L, 0L, 0L, 0L,
                                new RaftLogEntry(1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                                        unsorted)),
                        results::add));
        assertTrue(rpc.requests.isEmpty(), "the adapter must reject a non-canonical payload");

        // Bypassing the adapter shows the divergence the guard prevents: the Follower stores the
        // canonical form, not the Leader's original string. This is a test demonstration, not a
        // failover or replication claim.
        Map<String, Object> command = new LinkedHashMap<>();
        command.put("ticketId", "t-1");
        command.put("requestId", "r-1");
        AppendEntriesRequestDto dto = new AppendEntriesRequestDto(2L, LEADER, 0L, 0L,
                List.of(new AppendEntriesRequestDto.LogEntryDto(
                        1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, command)),
                0L);

        assertTrue(follower.handler.handle(dto, LEADER).success());

        String stored = follower.storage().storedLog().get(0).commandPayload();
        assertNotEquals(unsorted, stored);
        assertEquals("{\"requestId\":\"r-1\",\"ticketId\":\"t-1\"}", stored,
                "D09 stores compact JSON with ordered keys; the Leader must send the same bytes");
    }
}
