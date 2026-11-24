package vn.nhom6.railway.networking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRaftBridge;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRequestResultStore;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftStorage;

/**
 * Full 5-node real-TCP end-to-end integration test exercising the exact production
 * {@link ElectionRuntime} + {@link NodeMain} wiring across 10 real TCP sockets:
 * election, heartbeat, log replication, Figure-8 majority commit, NO_OP read barrier,
 * idempotency, conflict detection, 6-client concurrency (AC03), 3/5 failover (AC05B),
 * 2/5 minority safety (AC06), and restart catch-up (AC07).
 */
class FiveNodeTcpConsensusIntegrationTest {

    private static final List<String> NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    private final Map<String, Integer> clientPorts = new LinkedHashMap<>();
    private final Map<String, Integer> raftPorts = new LinkedHashMap<>();
    private final Map<String, InMemoryDurableStore> stores = new LinkedHashMap<>();
    private final Map<String, NodeMain> nodes = new LinkedHashMap<>();
    private String peersEnv;

    @AfterEach
    void tearDown() {
        for (NodeMain node : nodes.values()) {
            try {
                node.stop();
            } catch (Exception ignored) { }
        }
        nodes.clear();
    }

    @Test
    void fullFiveNodeTcpEndToEndBookingConcurrencyFailoverMinorityAndRecovery() throws Exception {
        // Allocate 10 distinct loopback TCP ports (5 client + 5 peer) and start all 5 real NodeMains.
        StringBuilder peersBuilder = new StringBuilder();
        for (int i = 0; i < NODE_IDS.size(); i++) {
            String id = NODE_IDS.get(i);
            clientPorts.put(id, freePort());
            raftPorts.put(id, freePort());
            stores.put(id, new InMemoryDurableStore());
            if (i > 0) peersBuilder.append(',');
            peersBuilder.append(id).append("@127.0.0.1:").append(raftPorts.get(id));
        }
        peersEnv = peersBuilder.toString();

        for (String id : NODE_IDS) {
            startNode(id);
        }

        // 1. Wait for unique Leader election across all 5 real TCP nodes & verify NODE_STATUS.
        String leader = awaitUniqueLeader(NODE_IDS, 12_000L);
        String follower = NODE_IDS.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        JsonNode leaderStatus = sendTcp(leader, MessageType.NODE_STATUS, emptyObj()).getPayload();
        assertEquals("LEADER", leaderStatus.get("role").asText());
        assertEquals(leader, leaderStatus.get("leaderId").asText());
        assertTrue(leaderStatus.get("storageHealthy").asBoolean());

        // 2. Follower rejects BOOK_TICKET & GET_SEATS with NOT_LEADER + real leaderHint.
        String redirectReqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> followerBookReply = sendTcp(
                follower, MessageType.BOOK_TICKET, bookPayload(redirectReqId, "G01", "Redirect Test"));
        assertEquals(MessageType.ERROR_RESPONSE, followerBookReply.getType());
        assertEquals("NOT_LEADER", followerBookReply.getPayload().get("errorCode").asText());
        assertNotEquals(follower, followerBookReply.getPayload().get("leaderHint").asText());

        MessageEnvelope<JsonNode> followerSeatsReply = sendTcp(
                follower, MessageType.GET_SEATS, seatsPayload("SE-001"));
        assertEquals(MessageType.ERROR_RESPONSE, followerSeatsReply.getType());
        assertEquals("NOT_LEADER", followerSeatsReply.getPayload().get("errorCode").asText());

        // Leader serves GET_SEATS via real NO_OP read barrier (10 AVAILABLE seats).
        MessageEnvelope<JsonNode> seatsReply = sendTcp(leader, MessageType.GET_SEATS, seatsPayload("SE-001"));
        assertEquals(MessageType.GET_SEATS_RESPONSE, seatsReply.getType());
        assertEquals(leader, seatsReply.getPayload().get("leaderId").asText());
        assertTrue(seatsReply.getPayload().get("readIndex").asLong() >= 1L);
        assertEquals(10, seatsReply.getPayload().get("seats").size());

        // 3. BOOK_TICKET (G01) commits over Raft & applies across all 5 nodes; verify idempotency & conflict.
        String reqG01 = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> bookG01 = sendTcp(
                leader, MessageType.BOOK_TICKET, bookPayload(reqG01, "G01", "Nguyen Van A"));
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookG01.getType());
        assertEquals("CONFIRMED", bookG01.getPayload().get("outcome").asText());
        String ticketG01 = bookG01.getPayload().get("ticketId").asText();
        long logIdxG01 = bookG01.getPayload().get("appliedLogIndex").asLong();
        assertNotNull(ticketG01);
        assertTrue(logIdxG01 >= 1L);

        awaitAllNodesSeatBooked(NODE_IDS, "G01", ticketG01, 5_000L);

        // Idempotent retry: same requestId + same payload -> exact same ticketId & appliedLogIndex
        MessageEnvelope<JsonNode> retryG01 = sendTcp(
                leader, MessageType.BOOK_TICKET, bookPayload(reqG01, "G01", "Nguyen Van A"));
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, retryG01.getType());
        assertEquals("CONFIRMED", retryG01.getPayload().get("outcome").asText());
        assertEquals(ticketG01, retryG01.getPayload().get("ticketId").asText());
        assertEquals(logIdxG01, retryG01.getPayload().get("appliedLogIndex").asLong());

        // Conflict: same requestId + different seat -> REQUEST_ID_CONFLICT
        MessageEnvelope<JsonNode> conflictG01 = sendTcp(
                leader, MessageType.BOOK_TICKET, bookPayload(reqG01, "G02", "Nguyen Van A"));
        assertEquals(MessageType.ERROR_RESPONSE, conflictG01.getType());
        assertEquals("REQUEST_ID_CONFLICT", conflictG01.getPayload().get("errorCode").asText());

        // GET_REQUEST_RESULT via NO_OP read barrier on Leader (known + unknown) and Follower (NOT_LEADER)
        MessageEnvelope<JsonNode> lookupKnown = sendTcp(leader, MessageType.GET_REQUEST_RESULT, lookupPayload(reqG01));
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, lookupKnown.getType());
        assertEquals("APPLIED_SUCCESS", lookupKnown.getPayload().get("state").asText());
        assertEquals(ticketG01, lookupKnown.getPayload().get("ticketId").asText());
        assertEquals(logIdxG01, lookupKnown.getPayload().get("appliedLogIndex").asLong());

        MessageEnvelope<JsonNode> lookupUnknown = sendTcp(
                leader, MessageType.GET_REQUEST_RESULT, lookupPayload(UUID.randomUUID().toString()));
        assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, lookupUnknown.getType());
        assertEquals("UNKNOWN", lookupUnknown.getPayload().get("state").asText());

        MessageEnvelope<JsonNode> lookupFollower = sendTcp(
                follower, MessageType.GET_REQUEST_RESULT, lookupPayload(reqG01));
        assertEquals(MessageType.ERROR_RESPONSE, lookupFollower.getType());
        assertEquals("NOT_LEADER", lookupFollower.getPayload().get("errorCode").asText());

        // 4. AC03: 6 concurrent TCP clients race for G05 -> exactly 1 CONFIRMED, 5 REJECTED(SEAT_ALREADY_BOOKED).
        ExecutorService pool = Executors.newFixedThreadPool(6);
        try {
            CountDownLatch ready = new CountDownLatch(6);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<MessageEnvelope<JsonNode>>> futures = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                final int clientNum = i + 1;
                Callable<MessageEnvelope<JsonNode>> task = () -> {
                    ready.countDown();
                    assertTrue(go.await(5, TimeUnit.SECONDS));
                    return sendTcp(leader, MessageType.BOOK_TICKET,
                            bookPayload(UUID.randomUUID().toString(), "G05", "Concurrent Client " + clientNum));
                };
                futures.add(pool.submit(task));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();

            int confirmed = 0;
            int rejected = 0;
            String winnerTicket = null;
            for (Future<MessageEnvelope<JsonNode>> f : futures) {
                MessageEnvelope<JsonNode> resp = f.get(10, TimeUnit.SECONDS);
                assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());
                String outcome = resp.getPayload().get("outcome").asText();
                if ("CONFIRMED".equals(outcome)) {
                    confirmed++;
                    winnerTicket = resp.getPayload().get("ticketId").asText();
                } else if ("REJECTED".equals(outcome)) {
                    assertEquals("SEAT_ALREADY_BOOKED", resp.getPayload().get("reasonCode").asText());
                    rejected++;
                }
            }
            assertEquals(1, confirmed, "Exactly 1 of 6 concurrent clients must win G05");
            assertEquals(5, rejected, "Exactly 5 of 6 concurrent clients must be REJECTED(SEAT_ALREADY_BOOKED)");
            awaitAllNodesSeatBooked(NODE_IDS, "G05", winnerTicket, 5_000L);
        } finally {
            pool.shutdownNow();
        }

        // 5. AC05B: Stop Leader + 1 Follower (3/5 quorum remains) -> elect new Leader & confirm G06.
        List<String> stopped = new ArrayList<>();
        stopped.add(leader);
        stopped.add(follower);
        stopNode(leader);
        stopNode(follower);

        List<String> quorum3 = NODE_IDS.stream().filter(id -> !stopped.contains(id)).toList();
        String newLeader = awaitUniqueLeader(quorum3, 12_000L);
        assertFalse(stopped.contains(newLeader));

        MessageEnvelope<JsonNode> bookG06 = sendTcp(
                newLeader, MessageType.BOOK_TICKET,
                bookPayload(UUID.randomUUID().toString(), "G06", "Failover Passenger"));
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookG06.getType());
        assertEquals("CONFIRMED", bookG06.getPayload().get("outcome").asText());
        String ticketG06 = bookG06.getPayload().get("ticketId").asText();
        awaitAllNodesSeatBooked(quorum3, "G06", ticketG06, 5_000L);

        // 6. AC06: Stop 1 more Follower (2/5 minority remains) -> G07 MUST NOT confirm!
        String thirdStopped = quorum3.stream().filter(id -> !id.equals(newLeader)).findFirst().orElseThrow();
        stopped.add(thirdStopped);
        stopNode(thirdStopped);

        MessageEnvelope<JsonNode> minorityBook = sendTcp(
                newLeader, MessageType.BOOK_TICKET,
                bookPayload(UUID.randomUUID().toString(), "G07", "Minority Passenger"));
        if (minorityBook.getType() == MessageType.ERROR_RESPONSE) {
            String err = minorityBook.getPayload().get("errorCode").asText();
            assertTrue(Set.of("NO_QUORUM", "NOT_LEADER", "TIMEOUT_UNKNOWN").contains(err),
                    "Unexpected minority errorCode: " + err);
        } else {
            assertEquals(MessageType.BOOK_TICKET_RESPONSE, minorityBook.getType());
            assertEquals("UNKNOWN", minorityBook.getPayload().get("outcome").asText());
        }
        for (String id : NODE_IDS) {
            assertEquals("AVAILABLE", stores.get(id).readSnapshot().get("G07").status(),
                    "G07 must never be booked in 2/5 minority on " + id);
        }

        // 7. AC07: Restart all 3 stopped nodes (with persisted state) -> cluster heals & all 5 converge!
        for (String id : stopped) {
            startNode(id);
        }
        awaitUniqueLeader(NODE_IDS, 12_000L);
        awaitAllNodesSeatBooked(NODE_IDS, "G01", ticketG01, 8_000L);
        awaitAllNodesSeatBooked(NODE_IDS, "G06", ticketG06, 8_000L);
    }

    private void startNode(String nodeId) throws Exception {
        NodeConfig cfg = new NodeConfig(
                nodeId, "railway-test-5",
                "127.0.0.1", clientPorts.get(nodeId),
                "127.0.0.1", raftPorts.get(nodeId),
                null, 16, 4000);
        Map<String, String> env = Map.of(
                "NODE_PEERS", peersEnv,
                "RAFT_HEARTBEAT_INTERVAL_MS", "50",
                "RAFT_ELECTION_TIMEOUT_MIN_MS", "150",
                "RAFT_ELECTION_TIMEOUT_MAX_MS", "300",
                "TCP_CONNECT_TIMEOUT_MS", "300",
                "TCP_RESPONSE_DEADLINE_MS", "1800");
        InMemoryDurableStore store = stores.get(nodeId);
        CommittedCommandApplier applier = new CommittedCommandApplier(store, res -> { });
        ElectionRuntime runtime = new ElectionRuntime(
                cfg, env, store, store, applier::applyCommitted, store::findStoredResult,
                store::findFingerprint,
                err -> { /* keep test diagnostics visible */ });
        NodeMain node = NodeMain.withCustomElectionRuntime(cfg, runtime, store, store);
        node.start();
        nodes.put(nodeId, node);
    }

    private void stopNode(String nodeId) {
        NodeMain node = nodes.remove(nodeId);
        if (node != null) {
            node.stop();
        }
    }

    private String awaitUniqueLeader(List<String> activeIds, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String lastLeader = null;
        int stableCount = 0;
        while (System.currentTimeMillis() < deadline) {
            List<String> leaders = new ArrayList<>();
            long maxTerm = -1;
            boolean allSameTerm = true;
            for (String id : activeIds) {
                try {
                    MessageEnvelope<JsonNode> resp = sendTcp(id, MessageType.NODE_STATUS, emptyObj());
                    JsonNode p = resp.getPayload();
                    long term = p.get("term").asLong();
                    if (maxTerm == -1) maxTerm = term;
                    else if (term != maxTerm) allSameTerm = false;
                    if ("LEADER".equals(p.get("role").asText())) {
                        leaders.add(id);
                    }
                } catch (Exception ignored) {
                    allSameTerm = false;
                }
            }
            if (leaders.size() == 1 && allSameTerm && maxTerm >= 1) {
                if (leaders.get(0).equals(lastLeader)) {
                    stableCount++;
                    if (stableCount >= 3) {
                        return lastLeader;
                    }
                } else {
                    lastLeader = leaders.get(0);
                    stableCount = 1;
                }
            } else {
                stableCount = 0;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("No stable unique leader elected among " + activeIds + " within " + timeoutMs + "ms");
    }

    private void awaitAllNodesSeatBooked(List<String> targetNodes, String seatId, String expectedTicketId, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allMatch = true;
            for (String id : targetNodes) {
                BookingRequestHandler.SeatState seat = stores.get(id).readSnapshot().get(seatId);
                if (seat == null || !"BOOKED".equals(seat.status()) || !expectedTicketId.equals(seat.ticketId())) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) {
                return;
            }
            Thread.sleep(60L);
        }
        throw new AssertionError("Nodes " + targetNodes + " did not converge on " + seatId + "=" + expectedTicketId);
    }

    private MessageEnvelope<JsonNode> sendTcp(String nodeId, MessageType type, ObjectNode payload) throws Exception {
        MessageEnvelope<ObjectNode> req = MessageEnvelope.createRequest(type, "test-client", payload);
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);
            socket.connect(new InetSocketAddress("127.0.0.1", clientPorts.get(nodeId)), 1500);
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            InputStream in = socket.getInputStream();
            FrameCodec.writeMessage(out, req);
            byte[] frame = FrameCodec.readFrame(in);
            MessageEnvelope<JsonNode> resp = MessageCodec.decode(frame);
            assertEquals(req.getMessageId(), resp.getCorrelationId(), "correlationId must match messageId");
            return resp;
        }
    }

    private static ObjectNode emptyObj() {
        return MessageCodec.getMapper().createObjectNode();
    }

    private static ObjectNode seatsPayload(String tripId) {
        ObjectNode node = emptyObj();
        node.put("tripId", tripId);
        return node;
    }

    private static ObjectNode bookPayload(String requestId, String seatId, String passengerName) {
        ObjectNode node = emptyObj();
        node.put("requestId", requestId);
        node.put("tripId", "SE-001");
        node.put("seatId", seatId);
        node.put("passengerName", passengerName);
        return node;
    }

    private static ObjectNode lookupPayload(String requestId) {
        ObjectNode node = emptyObj();
        node.put("requestId", requestId);
        return node;
    }

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    /**
     * Thread-safe in-memory per-node durable store persisting Raft meta/log + Booking state
     * across node stop/restart cycles during real TCP cluster tests.
     */
    private static final class InMemoryDurableStore
            implements RaftStorage, BookingRepository, BookingRequestHandler.SeatSnapshotReader {

        private long currentTerm = 0L;
        private String votedFor = null;
        private final List<RaftLogEntry> log = new ArrayList<>();

        private final BookingStateMachine stateMachine = new BookingStateMachine();
        private final Map<Long, BookingStateMachine.ApplyOutcome> appliedByIndex = new LinkedHashMap<>();
        private final Map<String, JdbcRequestResultStore.StoredRequestResult> storedResults = new LinkedHashMap<>();
        private final Map<String, String> seatTicketIds = new LinkedHashMap<>();

        InMemoryDurableStore() {
            for (int i = 1; i <= 10; i++) {
                seatTicketIds.put(String.format("G%02d", i), null);
            }
        }

        @Override
        public synchronized PersistentState load() {
            return new PersistentState(currentTerm, votedFor, List.copyOf(log), stateMachine.lastApplied());
        }

        @Override
        public synchronized void saveTermAndVote(long term, String vote) {
            if (term < currentTerm) {
                throw new IllegalArgumentException("Term cannot decrease");
            }
            this.currentTerm = term;
            this.votedFor = vote;
        }

        @Override
        public synchronized void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            if (fromIndex <= stateMachine.lastApplied()) {
                throw new IllegalStateException("Cannot truncate committed/applied entries");
            }
            while (log.size() >= fromIndex) {
                log.remove(log.size() - 1);
            }
            log.addAll(entries);
        }

        @Override
        public synchronized long loadLastApplied() {
            return stateMachine.lastApplied();
        }

        @Override
        public synchronized long lastApplied() {
            return stateMachine.lastApplied();
        }

        @Override
        public synchronized BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            BookingStateMachine.ApplyOutcome outcome = stateMachine.apply(logIndex, command);
            appliedByIndex.put(logIndex, outcome);
            if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult biz && !biz.isReplay()) {
                ApplyResult res = biz.applyResult();
                String stateStr = res.outcome() == ApplyResult.Outcome.CONFIRMED
                        ? "APPLIED_SUCCESS" : "APPLIED_REJECTED";
                if (command instanceof BookingCommand.BookTicket book) {
                    String fp = BookingStateMachine.computeBookFingerprint(
                            book.tripId(), book.seatId(), book.passengerName());
                    if (res.outcome() == ApplyResult.Outcome.CONFIRMED) {
                        seatTicketIds.put(book.seatId(), book.ticketId());
                    }
                    storedResults.put(book.requestId(), new JdbcRequestResultStore.StoredRequestResult(
                            book.requestId(), "BOOK_TICKET", fp, stateStr,
                            res.reasonCode(), res.ticketId(), logIndex, "{}"));
                } else if (command instanceof BookingCommand.CancelTicket cancel) {
                    String fp = BookingStateMachine.computeCancelFingerprint(cancel.ticketId());
                    if (res.outcome() == ApplyResult.Outcome.CONFIRMED) {
                        seatTicketIds.replaceAll((s, tid) -> cancel.ticketId().equals(tid) ? null : tid);
                    }
                    storedResults.put(cancel.requestId(), new JdbcRequestResultStore.StoredRequestResult(
                            cancel.requestId(), "CANCEL_TICKET", fp, stateStr,
                            res.reasonCode(), res.ticketId(), logIndex, "{}"));
                }
            }
            return outcome;
        }

        @Override
        public synchronized Optional<ApplyResult> findResultByRequestId(String requestId) {
            return Optional.ofNullable(storedResults.get(requestId))
                    .map(JdbcRequestResultStore.StoredRequestResult::toApplyResult);
        }

        @Override
        public synchronized Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(appliedByIndex.get(logIndex));
        }

        @Override
        public synchronized Map<String, String> readSeatSnapshot() {
            Map<String, String> map = new LinkedHashMap<>();
            seatTicketIds.forEach((seat, tid) -> map.put(seat, tid == null ? "AVAILABLE" : "BOOKED"));
            return Collections.unmodifiableMap(map);
        }

        @Override
        public synchronized Map<String, BookingRequestHandler.SeatState> readSnapshot() {
            Map<String, BookingRequestHandler.SeatState> map = new LinkedHashMap<>();
            seatTicketIds.forEach((seat, tid) -> map.put(
                    seat, new BookingRequestHandler.SeatState(tid == null ? "AVAILABLE" : "BOOKED", tid)));
            return Collections.unmodifiableMap(map);
        }

        synchronized Optional<JdbcRequestResultStore.StoredRequestResult> findStoredResult(String requestId) {
            return Optional.ofNullable(storedResults.get(requestId));
        }

        synchronized Optional<String> findFingerprint(String requestId) {
            return Optional.ofNullable(storedResults.get(requestId))
                    .map(JdbcRequestResultStore.StoredRequestResult::intentFingerprint);
        }
    }
}
