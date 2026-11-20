package vn.nhom6.railway.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.networking.ConnectionManager;
import vn.nhom6.railway.networking.ResultLookup;
import vn.nhom6.railway.networking.RetryPolicy;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV6-D19: actual BookingRequestHandler/ConnectionManager/shared codecs with fake
 * barrier, storage and loopback faults. No SQL, Docker, real Raft, or AC10 G5 claim.
 * GET_REQUEST_RESULT routing below is a SPEC-03 fixture: baseline has no production
 * lookup handler. Uses existing ResultLookup.LookupResult, not a new wire DTO.
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
class ReadBarrierAndRetryTest {
    private static final Duration HANDLER_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void staleFollowerCannotExposeCachedSeats() throws Exception {
        Fixture f = new Fixture();
        f.follower = true;
        ErrorResponseDto error = error(f.exchange(seatsRequest()), "NOT_LEADER");
        assertEquals("node-4", error.getLeaderHint());
        assertEquals(0, f.snapshotReads.get());
        assertEquals(0, f.noOps.get());
    }

    @Test
    void noQuorumReadReturnsReadUnavailableWithoutSnapshot() throws Exception {
        Fixture f = new Fixture();
        f.noQuorum = true;
        error(f.exchange(seatsRequest()), "READ_UNAVAILABLE");
        assertEquals(0, f.snapshotReads.get());
    }

    @Test
    void noQuorumWriteAndRetryPreserveIntentAndRequestId() throws Exception {
        Fixture f = new Fixture();
        MessageEnvelope<JsonNode> first = bookRequest(UUID.randomUUID().toString(), "G05");
        f.noQuorum = true;
        assertEquals(first.getPayload().get("requestId").asText(), error(f.exchange(first), "NO_QUORUM").getRequestId());
        assertEquals(0, f.effects.get());
        f.noQuorum = false;
        MessageEnvelope<JsonNode> retry = retry(first);
        assertSameIntent(first, retry);
        BookTicketResponseDto result = book(f.exchange(retry));
        assertEquals("CONFIRMED", result.getOutcome());
        assertEquals(first.getPayload().get("requestId").asText(), result.getRequestId());
        assertEquals(1, f.effects.get());
    }

    @Test
    void barrierCommittedButNotAppliedCannotReadSnapshot() throws Exception {
        Fixture f = new Fixture();
        f.laggingApply = true;
        error(f.exchange(seatsRequest()), "READ_UNAVAILABLE");
        assertEquals(1, f.noOps.get());
        assertEquals(40, f.repo.lastApplied());
        assertEquals(0, f.snapshotReads.get());
    }

    @Test
    void changedLeaderDuringBarrierRejectsRead() throws Exception {
        Fixture f = new Fixture();
        f.changedLeader = true;
        assertEquals("node-4", error(f.exchange(seatsRequest()), "NOT_LEADER").getLeaderHint());
        assertEquals(0, f.snapshotReads.get());
    }

    @Test
    void pendingNoOpMustCompleteAndApplyBeforeSnapshotIsRead() throws Exception {
        Fixture f = new Fixture();
        CountDownLatch entered = new CountDownLatch(1);
        CompletableFuture<BookingRequestHandler.ReadBarrierResult> barrier = new CompletableFuture<>();
        BookingRequestHandler handler = f.handler(timeout -> { entered.countDown(); return barrier; });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            MessageEnvelope<JsonNode> request = seatsRequest();
            Future<MessageEnvelope<JsonNode>> response = worker.submit(() -> wire(handler.handleBooking(wire(request))));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertFalse(response.isDone());
            assertEquals(0, f.snapshotReads.get());
            f.repo.applied = 41;
            barrier.complete(new BookingRequestHandler.ReadBarrierResult(41, "node-3"));
            GetSeatsResponseDto seats = MessageCodec.decodePayload(response.get(2, TimeUnit.SECONDS), GetSeatsResponseDto.class);
            seats.validate();
            assertEquals(41L, seats.getReadIndex());
            assertEquals(1, f.snapshotReads.get());
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void eachAuthoritativeReadUsesAFreshNoOp() throws Exception {
        Fixture f = new Fixture();
        GetSeatsResponseDto first = MessageCodec.decodePayload(f.exchange(seatsRequest()), GetSeatsResponseDto.class);
        GetSeatsResponseDto second = MessageCodec.decodePayload(f.exchange(seatsRequest()), GetSeatsResponseDto.class);
        first.validate();
        second.validate();
        assertEquals(41L, first.getReadIndex());
        assertEquals(42L, second.getReadIndex());
        assertEquals(2, f.noOps.get());
        assertEquals(2, f.snapshotReads.get());
        assertTrue(f.repo.results.isEmpty(), "NO_OP must not create a business result");
    }

    @Test
    void lostResponseThenRetryKeepsRequestIdAndSingleFixtureEffect() throws Exception {
        Fixture f = new Fixture();
        MessageEnvelope<JsonNode> first = bookRequest(UUID.randomUUID().toString(), "G05");
        try (LostResponseServer server = new LostResponseServer(f, false);
             ConnectionManager client = server.client(null)) {
            BookTicketResponseDto unknown = book(wire(client.sendAndReceive(first)));
            assertEquals("UNKNOWN", unknown.getOutcome());
            assertEquals("TIMEOUT_UNKNOWN", unknown.getReasonCode());
            assertFalse(client.isConnected(), "Timeout socket must be discarded");
            assertTrue(server.dropped.await(2, TimeUnit.SECONDS));
            MessageEnvelope<JsonNode> retry = retry(first);
            assertSameIntent(first, retry);
            BookTicketResponseDto confirmed = book(wire(client.sendAndReceive(retry)));
            assertEquals("CONFIRMED", confirmed.getOutcome());
            assertEquals(unknown.getRequestId(), confirmed.getRequestId());
            assertEquals(41L, confirmed.getAppliedLogIndex());
            assertEquals(1, f.effects.get());
            assertEquals(2, server.received.size());
            assertSameIntent(server.received.get(0), server.received.get(1));
        }
    }

    @Test
    void lostResponseCanResolveTypedLookupAfterFreshBarrierWithoutResubmit() throws Exception {
        Fixture f = new Fixture();
        MessageEnvelope<JsonNode> first = bookRequest(UUID.randomUUID().toString(), "G05");
        try (LostResponseServer server = new LostResponseServer(f, false);
             ConnectionManager client = server.client(f)) {
            BookTicketResponseDto result = book(wire(client.sendAndReceive(first)));
            assertEquals("CONFIRMED", result.getOutcome());
            assertEquals(first.getPayload().get("requestId").asText(), result.getRequestId());
            assertEquals(41L, result.getAppliedLogIndex(), "Lookup keeps original result index, not NO_OP index");
            assertEquals(1, f.effects.get());
            assertEquals(1, f.submissions.get());
            assertEquals(1, f.noOps.get());
            assertEquals(42L, f.repo.lastApplied());
            assertEquals(1, server.received.size());
        }
    }

    @Test
    void lookupBarrierFailureCannotLeakCachedAppliedResultAfterResponseLoss() throws Exception {
        Fixture f = new Fixture();
        MessageEnvelope<JsonNode> first = bookRequest(UUID.randomUUID().toString(), "G05");
        ResultLookup unavailableLookup = id -> { f.noQuorum = true; return f.lookup(id); };
        try (LostResponseServer server = new LostResponseServer(f, false);
             ConnectionManager client = server.client(unavailableLookup)) {
            assertEquals("UNKNOWN", book(wire(client.sendAndReceive(first))).getOutcome());
            assertTrue(server.dropped.await(2, TimeUnit.SECONDS));
            assertEquals(1, f.repo.results.size(), "Fixture has an applied result to protect from stale lookup");
            assertEquals(0, f.repo.lookupReads.get(), "Failed barrier must not query stored results");
        }
    }

    @Test
    void lookupUnknownDoesNotPermitANewIdWhenRetryingLostBeforeApply() throws Exception {
        Fixture f = new Fixture();
        MessageEnvelope<JsonNode> first = bookRequest(UUID.randomUUID().toString(), "G05");
        try (LostResponseServer server = new LostResponseServer(f, true);
             ConnectionManager client = server.client(f)) {
            BookTicketResponseDto unknown = book(wire(client.sendAndReceive(first)));
            assertEquals("UNKNOWN", unknown.getOutcome());
            assertEquals(0, f.effects.get());
            MessageEnvelope<JsonNode> retry = retry(first);
            assertSameIntent(first, retry);
            BookTicketResponseDto confirmed = book(wire(client.sendAndReceive(retry)));
            assertEquals("CONFIRMED", confirmed.getOutcome());
            assertEquals(unknown.getRequestId(), confirmed.getRequestId());
            assertEquals(1, f.effects.get());
        }
    }

    @Test
    void lookupFixturePreservesPendingUnknownAndAppliedRejectedFieldCombinations() throws Exception {
        Fixture f = new Fixture();
        String pending = UUID.randomUUID().toString();
        f.pending.add(pending);
        ResultLookup.LookupResult pendingResult = f.lookup(pending);
        assertTrue(pendingResult.isPending());
        assertEquals("BOOK_TICKET", pendingResult.commandType());
        assertNull(pendingResult.ticketId());
        assertNull(pendingResult.reasonCode());
        assertNull(pendingResult.appliedLogIndex());
        ResultLookup.LookupResult unknown = f.lookup(UUID.randomUUID().toString());
        assertTrue(unknown.isUnknown());
        assertNull(unknown.commandType());
        assertNull(unknown.ticketId());
        assertNull(unknown.reasonCode());
        assertNull(unknown.appliedLogIndex());
        String rejected = UUID.randomUUID().toString();
        f.repo.results.put(rejected, ApplyResult.bookRejected(rejected, "SEAT_ALREADY_BOOKED", 41));
        ResultLookup.LookupResult result = f.lookup(rejected);
        assertTrue(result.isRejected());
        assertEquals("SEAT_ALREADY_BOOKED", result.reasonCode());
        assertNull(result.ticketId());
        assertEquals(41L, result.appliedLogIndex());
        assertEquals(3, f.noOps.get(), "Each lookup must obtain its own fresh barrier");
    }

    @Test
    void staleFollowerLookupReturnsNotLeaderRatherThanUnknownOrCachedSuccess() throws Exception {
        Fixture f = new Fixture();
        f.follower = true;
        MessageEnvelope<JsonNode> request = request(MessageType.GET_REQUEST_RESULT,
                Map.of("requestId", UUID.randomUUID().toString()));
        assertEquals("node-4", error(f.exchange(request), "NOT_LEADER").getLeaderHint());
        assertEquals(0, f.repo.lookupReads.get());
    }

    @Test
    void retryWithChangedPayloadCannotOverwriteOriginalResult() throws Exception {
        Fixture f = new Fixture();
        String id = UUID.randomUUID().toString();
        BookTicketResponseDto original = book(f.exchange(bookRequest(id, "G05")));
        assertEquals(id, error(f.exchange(bookRequest(id, "G06")), "REQUEST_ID_CONFLICT").getRequestId());
        ResultLookup.LookupResult lookup = f.lookup(id);
        assertEquals(original.getTicketId(), lookup.ticketId());
        assertEquals(original.getAppliedLogIndex(), lookup.appliedLogIndex());
        assertEquals(1, f.effects.get());
    }

    private static MessageEnvelope<JsonNode> request(MessageType type, Object payload) {
        return MessageEnvelope.createRequest(type, "client-tv6-d19-fixture", MessageCodec.getMapper().valueToTree(payload));
    }

    private static MessageEnvelope<JsonNode> bookRequest(String id, String seat) {
        return request(MessageType.BOOK_TICKET, new BookTicketRequestDto(id, "SE-001", seat, "Khach fixture"));
    }

    private static MessageEnvelope<JsonNode> seatsRequest() {
        return request(MessageType.GET_SEATS, Map.of("tripId", "SE-001"));
    }

    private static MessageEnvelope<JsonNode> retry(MessageEnvelope<JsonNode> original) {
        return request(original.getType(), original.getPayload().deepCopy());
    }

    private static void assertSameIntent(MessageEnvelope<JsonNode> original, MessageEnvelope<JsonNode> retry) {
        assertEquals(original.getPayload(), retry.getPayload());
        assertNotEquals(original.getMessageId(), retry.getMessageId());
    }

    private static MessageEnvelope<JsonNode> wire(MessageEnvelope<?> message) throws IOException {
        return FrameCodec.readMessage(new ByteArrayInputStream(FrameCodec.frame(MessageCodec.encode(message))));
    }

    private static ErrorResponseDto error(MessageEnvelope<JsonNode> response, String code) throws Exception {
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        ErrorResponseDto error = MessageCodec.decodePayload(response, ErrorResponseDto.class);
        error.validate();
        assertEquals(code, error.getErrorCode());
        return error;
    }

    private static BookTicketResponseDto book(MessageEnvelope<JsonNode> response) throws Exception {
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, response.getType());
        BookTicketResponseDto result = MessageCodec.decodePayload(response, BookTicketResponseDto.class);
        result.validate();
        return result;
    }

    /** Deliberately fake state/commit/barrier. No claim to durable apply or majority agreement. */
    private static final class Fixture implements ResultLookup {
        final FakeRepository repo = new FakeRepository();
        final AtomicInteger snapshotReads = new AtomicInteger();
        final AtomicInteger noOps = new AtomicInteger();
        final AtomicInteger effects = new AtomicInteger();
        final AtomicInteger submissions = new AtomicInteger();
        final AtomicInteger tickets = new AtomicInteger();
        final Map<String, List<String>> intents = new ConcurrentHashMap<>();
        final Set<String> pending = ConcurrentHashMap.newKeySet();
        boolean follower, noQuorum, laggingApply, changedLeader;

        Fixture() { System.out.println("MODE=FAKE_BARRIER_STORAGE; production lookup handler not exercised; AC10_REAL_G5=NOT_RUN"); }

        BookingRequestHandler handler(BookingRequestHandler.ReadBarrier barrier) {
            return new BookingRequestHandler(this::submit, barrier, () -> "fixture-ticket-" + tickets.incrementAndGet(),
                    Clock.fixed(Instant.parse("2026-10-29T10:00:00Z"), ZoneOffset.UTC), repo, () -> {
                        snapshotReads.incrementAndGet();
                        Map<String, BookingRequestHandler.SeatState> seats = new LinkedHashMap<>();
                        for (int n = 1; n <= 10; n++) { seats.put(String.format("G%02d", n), new BookingRequestHandler.SeatState("AVAILABLE", null)); }
                        return seats;
                    }, "node-3", HANDLER_TIMEOUT);
        }

        CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) throws Exception {
            submissions.incrementAndGet();
            if (follower) { throw new BookingRequestHandler.NotLeaderException("node-4"); }
            if (noQuorum) { throw new BookingRequestHandler.NoQuorumException(); }
            List<String> intent = List.of(command.tripId(), command.seatId(), command.passengerName());
            ApplyResult previous = repo.results.get(command.requestId());
            if (previous != null) {
                if (!intent.equals(intents.get(command.requestId()))) { throw new BookingRequestHandler.RequestIdConflictException(); }
                return CompletableFuture.completedFuture(previous);
            }
            ApplyResult result = ApplyResult.bookConfirmed(command.requestId(), command.ticketId(), ++repo.applied);
            repo.results.put(command.requestId(), result);
            intents.put(command.requestId(), intent);
            effects.incrementAndGet();
            return CompletableFuture.completedFuture(result);
        }

        CompletionStage<BookingRequestHandler.ReadBarrierResult> barrier(Duration timeout) throws Exception {
            if (follower) { throw new BookingRequestHandler.NotLeaderException("node-4"); }
            if (noQuorum) { throw new BookingRequestHandler.NoQuorumException(); }
            noOps.incrementAndGet();
            long index = repo.applied + 1;
            if (!laggingApply) { repo.applied = index; }
            return CompletableFuture.completedFuture(new BookingRequestHandler.ReadBarrierResult(index, changedLeader ? "node-4" : "node-3"));
        }

        MessageEnvelope<JsonNode> exchange(MessageEnvelope<JsonNode> request) throws Exception {
            MessageEnvelope<JsonNode> decoded = wire(request);
            MessageEnvelope<JsonNode> response = wire(decoded.getType() == MessageType.GET_REQUEST_RESULT
                    ? lookupEnvelope(decoded) : handler(this::barrier).handleBooking(decoded));
            assertEquals(request.getMessageId(), response.getCorrelationId());
            assertEquals("node-3", response.getSenderId());
            System.out.println("FAKE_EXCHANGE type=" + request.getType() + " requestId=" + request.getPayload().path("requestId").asText("-")
                    + " messageId=" + request.getMessageId() + " correlationId=" + response.getCorrelationId()
                    + " response=" + response.getType() + " payload=" + response.getPayload());
            return response;
        }

        MessageEnvelope<?> lookupEnvelope(MessageEnvelope<JsonNode> request) throws Exception {
            String id = request.getPayload().path("requestId").asText();
            try {
                BookingRequestHandler.ReadBarrierResult read = barrier(HANDLER_TIMEOUT).toCompletableFuture().get(2, TimeUnit.SECONDS);
                if (!read.leaderId().equals("node-3")) { throw new BookingRequestHandler.NotLeaderException(read.leaderId()); }
                if (repo.lastApplied() < read.readIndex()) { throw new BookingRequestHandler.ReadUnavailableException("fake apply lag"); }
                Optional<ApplyResult> stored = repo.findResultByRequestId(id);
                LookupResult result = stored.map(value -> value.outcome() == ApplyResult.Outcome.CONFIRMED
                        ? LookupResult.appliedSuccess(id, "BOOK_TICKET", value.ticketId(), value.appliedLogIndex())
                        : LookupResult.appliedRejected(id, "BOOK_TICKET", value.reasonCode(), value.appliedLogIndex()))
                        .orElseGet(() -> pending.contains(id) ? LookupResult.pending(id, "BOOK_TICKET") : LookupResult.unknown(id));
                // Canonical six-field fixture; do not serialize LookupResult's convenience isX getters as wire fields.
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("requestId", result.requestId()); payload.put("state", result.state());
                payload.put("commandType", result.commandType()); payload.put("ticketId", result.ticketId());
                payload.put("reasonCode", result.reasonCode()); payload.put("appliedLogIndex", result.appliedLogIndex());
                return MessageEnvelope.createResponse(request.getMessageId(), MessageType.GET_REQUEST_RESULT_RESPONSE, "node-3", payload);
            } catch (BookingRequestHandler.NotLeaderException exception) {
                return MessageCodec.createErrorResponse(request.getMessageId(), "NOT_LEADER", "Fake follower lookup blocked", exception.leaderHint(), null, id, "node-3");
            } catch (BookingRequestHandler.NoQuorumException | BookingRequestHandler.ReadUnavailableException exception) {
                return MessageCodec.createErrorResponse(request.getMessageId(), "READ_UNAVAILABLE", "Fake lookup barrier unavailable", null, null, id, "node-3");
            }
        }

        @Override public LookupResult lookup(String id) throws Exception {
            MessageEnvelope<JsonNode> response = exchange(request(MessageType.GET_REQUEST_RESULT, Map.of("requestId", id)));
            if (response.getType() == MessageType.ERROR_RESPONSE) {
                throw new IOException(MessageCodec.decodePayload(response, ErrorResponseDto.class).getErrorCode());
            }
            assertEquals(MessageType.GET_REQUEST_RESULT_RESPONSE, response.getType());
            Set<String> fields = new java.util.HashSet<>();
            response.getPayload().fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("requestId", "state", "commandType", "ticketId", "reasonCode", "appliedLogIndex"), fields);
            LookupResult result = MessageCodec.decodePayload(response, LookupResult.class);
            assertEquals(id, result.requestId());
            return result;
        }
    }

    private static final class FakeRepository implements BookingRepository {
        volatile long applied = 40;
        final Map<String, ApplyResult> results = new ConcurrentHashMap<>();
        final AtomicInteger lookupReads = new AtomicInteger();
        @Override public long lastApplied() { return applied; }
        @Override public Optional<ApplyResult> findResultByRequestId(String id) { lookupReads.incrementAndGet(); return Optional.ofNullable(results.get(id)); }
        @Override public BookingStateMachine.ApplyOutcome applyAtomically(long index, BookingCommand command) { throw new UnsupportedOperationException("No actual DB/apply in fixture"); }
        @Override public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long index) { throw new UnsupportedOperationException("No actual DB/apply in fixture"); }
        @Override public Map<String, String> readSeatSnapshot() { throw new UnsupportedOperationException("Use counted snapshot fixture"); }
    }

    /** The first connection holds its reply until the actual ConnectionManager read deadline expires. */
    private static final class LostResponseServer implements AutoCloseable {
        final ServerSocket listener = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        final ExecutorService workers = Executors.newFixedThreadPool(2);
        final ExecutorService acceptor = Executors.newSingleThreadExecutor();
        final CountDownLatch closed = new CountDownLatch(1);
        final CountDownLatch dropped = new CountDownLatch(1);
        final AtomicBoolean dropFirst = new AtomicBoolean(true);
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final List<MessageEnvelope<JsonNode>> received = new CopyOnWriteArrayList<>();
        final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        LostResponseServer(Fixture fixture, boolean dropBeforeHandle) throws IOException {
            listener.setSoTimeout(5000);
            acceptor.submit(() -> {
                try {
                    while (closed.getCount() > 0) {
                        Socket socket = listener.accept();
                        socket.setSoTimeout(2000);
                        sockets.add(socket);
                        workers.submit(() -> {
                            try (socket) {
                                MessageEnvelope<JsonNode> request = FrameCodec.readMessage(socket.getInputStream());
                                received.add(request);
                                boolean drop = dropFirst.getAndSet(false);
                                MessageEnvelope<JsonNode> response = drop && dropBeforeHandle ? null : fixture.exchange(request);
                                if (drop) {
                                    dropped.countDown();
                                    if (!closed.await(5, TimeUnit.SECONDS)) { throw new IOException("Fixture cleanup deadline"); }
                                } else { FrameCodec.writeMessage(socket.getOutputStream(), response); }
                            } catch (Throwable exception) { if (closed.getCount() > 0) { failures.add(exception); } }
                            finally { sockets.remove(socket); }
                        });
                    }
                } catch (IOException exception) { if (closed.getCount() > 0) { failures.add(exception); } }
            });
        }

        ConnectionManager client(ResultLookup lookup) {
            return new ConnectionManager(InetAddress.getLoopbackAddress().getHostAddress(), listener.getLocalPort(),
                    new RetryPolicy(1, 0, 0, 1.0, 1000, 300), lookup);
        }

        @Override public void close() throws Exception {
            closed.countDown();
            listener.close();
            for (Socket socket : sockets) { try { socket.close(); } catch (IOException ignored) { } }
            acceptor.shutdownNow();
            workers.shutdownNow();
            assertTrue(acceptor.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(workers.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(failures.isEmpty(), () -> "Fake TCP worker failed: " + failures);
        }
    }
}
