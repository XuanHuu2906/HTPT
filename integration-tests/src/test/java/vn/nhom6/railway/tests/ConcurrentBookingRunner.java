package vn.nhom6.railway.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TV6-D17: six distinct BOOK(G05) intentions released through one latch over TCP.
 * No retry or redirect: UNKNOWN retains its original requestId for later lookup.
 * Default CLI runs fake TCP tests only; real Raft/SQL G5 acceptance is separate.
 *
 * <pre>
 * mvn -pl integration-tests -am -Dtest=ConcurrentBookingRunner -Dsurefire.failIfNoSpecifiedTests=false test
 * java -cp &lt;test-classpath&gt; vn.nhom6.railway.tests.ConcurrentBookingRunner --self-test
 * java -cp &lt;test-classpath&gt; vn.nhom6.railway.tests.ConcurrentBookingRunner --live HOST PORT NODE_ID [TIMEOUT_MS]
 * </pre>
 * Live mode sends six real booking intentions: use a prepared G5 test cluster.
 * Target node ID is supplied explicitly; no node is assumed to be the Leader.
 * Per-request telemetry goes to stdout and can be redirected to an evidence log.
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS)
public final class ConcurrentBookingRunner {
    public static final int CLIENT_COUNT = 6;
    private static final String TRIP = "SE-001";
    private static final String SEAT = "G05";
    private static final Set<String> BOOK_FIELDS = Set.of("requestId", "outcome", "ticketId", "tripId",
            "seatId", "reasonCode", "appliedLogIndex");
    private static final Set<String> ERROR_FIELDS = Set.of("errorCode", "message", "leaderHint", "term", "requestId");

    /** Client telemetry only, not a second wire DTO. Latency is measured by nanoTime. */
    public record Result(int client, String requestId, String messageId, long latencyNanos,
                         String outcome, String reasonCode, Long appliedLogIndex, String detail) { }

    public static List<Result> run(String host, int port, String nodeId, int timeoutMs) throws Exception {
        if (host == null || host.isBlank() || port < 1 || port > 65535 || nodeId == null
                || !nodeId.matches("node-[1-5]") || timeoutMs < 100 || timeoutMs > 30000) {
            throw new IllegalArgumentException("Expected host, port 1..65535, node-1..node-5, timeout 100..30000ms");
        }
        ExecutorService workers = Executors.newFixedThreadPool(CLIENT_COUNT);
        Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        CountDownLatch ready = new CountDownLatch(CLIENT_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Result>> futures = new ArrayList<>();
        try {
            for (int index = 1; index <= CLIENT_COUNT; index++) {
                int client = index;
                BookTicketRequestDto payload = new BookTicketRequestDto(UUID.randomUUID().toString(),
                        TRIP, SEAT, "Khach demo " + client);
                payload.validate();
                MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                        MessageType.BOOK_TICKET, "client-load-" + client, payload);
                futures.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return attempt(host, port, nodeId, timeoutMs, client, request, sockets);
                }));
            }
            if (!ready.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Six client workers did not reach the start latch");
            }
            System.out.println("LOAD_READY clients=6 seat=G05 startLatch=shared; releasing");
            start.countDown();
            List<Result> results = new ArrayList<>();
            for (Future<Result> future : futures) {
                results.add(future.get(timeoutMs + 3000L, TimeUnit.MILLISECONDS));
            }
            for (Result result : results) {
                System.out.printf(Locale.ROOT,
                        "client=%d requestId=%s messageId=%s trip=SE-001 seat=G05 latencyMs=%.3f outcome=%s reasonCode=%s appliedLogIndex=%s detail=%s%n",
                        result.client(), result.requestId(), result.messageId(), result.latencyNanos() / 1_000_000.0,
                        result.outcome(), result.reasonCode(), result.appliedLogIndex(), result.detail());
            }
            return List.copyOf(results);
        } finally {
            start.countDown();
            futures.forEach(future -> future.cancel(true));
            sockets.forEach(ConcurrentBookingRunner::closeSocket);
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.SECONDS)) {
                throw new IOException("Client workers did not stop");
            }
        }
    }

    private static Result attempt(String host, int port, String nodeId, int timeoutMs, int client,
                                  MessageEnvelope<BookTicketRequestDto> request, Set<Socket> sockets) {
        long began = System.nanoTime();
        long deadline = began + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String id = request.getPayload().getRequestId();
        Socket socket = new Socket();
        sockets.add(socket);
        try (socket) {
            socket.connect(new InetSocketAddress(host, port), remainingMillis(deadline));
            FrameCodec.writeMessage(socket.getOutputStream(), request);
            InputStream input = new FilterInputStream(socket.getInputStream()) {
                @Override public int read() throws IOException {
                    socket.setSoTimeout(remainingMillis(deadline));
                    return in.read();
                }
                @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                    socket.setSoTimeout(remainingMillis(deadline));
                    return in.read(bytes, offset, length);
                }
            };
            MessageEnvelope<JsonNode> response = FrameCodec.readMessage(input);
            require(request.getMessageId().equals(response.getCorrelationId()), "correlation mismatch");
            require(nodeId.equals(response.getSenderId()), "responding node mismatch");
            JsonNode payload = response.getPayload();
            if (response.getType() == MessageType.BOOK_TICKET_RESPONSE) {
                require(fields(payload).equals(BOOK_FIELDS), "BOOK response field set mismatch");
                BookTicketResponseDto book = MessageCodec.decodePayload(response, BookTicketResponseDto.class);
                book.validate();
                require(id.equals(book.getRequestId()) && TRIP.equals(payload.path("tripId").asText())
                        && SEAT.equals(book.getSeatId()), "business request mismatch");
                return new Result(client, id, request.getMessageId(), System.nanoTime() - began,
                        book.getOutcome(), book.getReasonCode(), book.getAppliedLogIndex(), "BOOK_TICKET_RESPONSE");
            }
            require(response.getType() == MessageType.ERROR_RESPONSE, "unexpected response type");
            require(fields(payload).equals(ERROR_FIELDS), "ERROR response field set mismatch");
            ErrorResponseDto error = MessageCodec.decodePayload(response, ErrorResponseDto.class);
            error.validate();
            require(error.getRequestId() == null || id.equals(error.getRequestId()), "error requestId mismatch");
            // Protocol/routing errors do not establish a durable business rejection.
            return new Result(client, id, request.getMessageId(), System.nanoTime() - began,
                    "UNKNOWN", error.getErrorCode(), null, "ERROR_RESPONSE leaderHint=" + error.getLeaderHint());
        } catch (IOException | RuntimeException exception) {
            return new Result(client, id, request.getMessageId(), System.nanoTime() - began,
                    "UNKNOWN", "TIMEOUT_UNKNOWN", null, exception.getClass().getSimpleName());
        } finally { sockets.remove(socket); }
    }

    private static int remainingMillis(long deadline) throws SocketTimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) { throw new SocketTimeoutException("Request deadline elapsed"); }
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    private static Set<String> fields(JsonNode node) throws ProtocolException {
        require(node != null && node.isObject(), "payload must be an object");
        Set<String> names = new java.util.HashSet<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static void require(boolean condition, String message) throws ProtocolException {
        if (!condition) { throw new ProtocolException(message); }
    }

    private static void closeSocket(Socket socket) {
        try { socket.close(); } catch (IOException ignored) { }
    }

    @Test
    public void sixConcurrentRequestsUseDistinctIdsAndOneFixtureWinner() throws Exception {
        try (FakeTcpServer fixture = new FakeTcpServer(Scenario.NORMAL)) {
            List<Result> results = runFixture(fixture, 3000);
            assertEquals(1, results.stream().filter(r -> r.outcome().equals("CONFIRMED")).count());
            assertEquals(5, results.stream().filter(r -> r.outcome().equals("REJECTED")
                    && "SEAT_ALREADY_BOOKED".equals(r.reasonCode())).count());
            assertTrue(results.stream().allMatch(r -> r.appliedLogIndex() != null && r.appliedLogIndex() >= 1));
            assertEquals(CLIENT_COUNT, fixture.receivedBeforeFirstResponse.get());
        }
    }

    @Test public void disconnectedResponsesStayUnknown() throws Exception { assertUnknownFixture(Scenario.DROP); }
    @Test public void slowResponsesRespectRequestDeadline() throws Exception { assertUnknownFixture(Scenario.DELAY); }
    @Test public void wrongCorrelationCannotConfirmAnotherRequest() throws Exception { assertUnknownFixture(Scenario.WRONG_CORRELATION); }
    @Test public void wrongBusinessRequestIdCannotConfirm() throws Exception { assertUnknownFixture(Scenario.WRONG_REQUEST_ID); }
    @Test public void wrongRespondingNodeCannotConfirm() throws Exception { assertUnknownFixture(Scenario.WRONG_SENDER); }
    @Test public void unknownBusinessOutcomeIsPreserved() throws Exception { assertUnknownFixture(Scenario.UNKNOWN); }

    @Test
    public void notLeaderUsesErrorCodeAndNeverParsesSuccessFromMessage() throws Exception {
        try (FakeTcpServer fixture = new FakeTcpServer(Scenario.NOT_LEADER)) {
            List<Result> results = runFixture(fixture, 3000);
            assertTrue(results.stream().allMatch(r -> r.outcome().equals("UNKNOWN")
                    && r.reasonCode().equals("NOT_LEADER") && r.appliedLogIndex() == null));
        }
    }

    private static void assertUnknownFixture(Scenario scenario) throws Exception {
        try (FakeTcpServer fixture = new FakeTcpServer(scenario)) {
            List<Result> results = runFixture(fixture, scenario == Scenario.DELAY ? 250 : 3000);
            assertTrue(results.stream().allMatch(r -> r.outcome().equals("UNKNOWN") && r.appliedLogIndex() == null));
            if (scenario == Scenario.DELAY) {
                assertTrue(results.stream().allMatch(r -> r.detail().equals("SocketTimeoutException")));
            }
        }
    }

    private static List<Result> runFixture(FakeTcpServer fixture, int timeoutMs) throws Exception {
        System.out.println("MODE=FAKE_TCP scenario=" + fixture.scenario + "; real_Raft_G5=NOT_RUN");
        List<Result> results = run(InetAddress.getLoopbackAddress().getHostAddress(), fixture.port(), "node-3", timeoutMs);
        fixture.awaitHandled();
        assertEquals(CLIENT_COUNT, results.size());
        assertEquals(CLIENT_COUNT, results.stream().map(Result::requestId).distinct().count());
        assertEquals(CLIENT_COUNT, results.stream().map(Result::messageId).distinct().count());
        assertTrue(results.stream().allMatch(r -> !r.requestId().equals(r.messageId()) && r.latencyNanos() >= 0));
        Set<String> sentIds = new java.util.HashSet<>();
        Set<String> sentMessages = new java.util.HashSet<>();
        for (MessageEnvelope<BookTicketRequestDto> request : fixture.received) {
            assertEquals(MessageType.BOOK_TICKET, request.getType());
            assertNull(request.getCorrelationId());
            assertEquals(TRIP, request.getPayload().getTripId());
            assertEquals(SEAT, request.getPayload().getSeatId());
            sentIds.add(request.getPayload().getRequestId());
            sentMessages.add(request.getMessageId());
        }
        assertEquals(CLIENT_COUNT, fixture.received.size());
        assertEquals(sentIds, new java.util.HashSet<>(results.stream().map(Result::requestId).toList()));
        assertEquals(sentMessages, new java.util.HashSet<>(results.stream().map(Result::messageId).toList()));
        return results;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length == 1 && args[0].equals("--self-test")) {
            ConcurrentBookingRunner tests = new ConcurrentBookingRunner();
            tests.sixConcurrentRequestsUseDistinctIdsAndOneFixtureWinner();
            tests.disconnectedResponsesStayUnknown();
            tests.slowResponsesRespectRequestDeadline();
            tests.wrongCorrelationCannotConfirmAnotherRequest();
            tests.wrongBusinessRequestIdCannotConfirm();
            tests.wrongRespondingNodeCannotConfirm();
            tests.unknownBusinessOutcomeIsPreserved();
            tests.notLeaderUsesErrorCodeAndNeverParsesSuccessFromMessage();
            System.out.println("FIXTURE PASS tests=8; six distinct requestIds per scenario; real_Raft_G5=NOT_RUN");
            return;
        }
        if (!args[0].equals("--live") || args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException("Use --self-test or --live HOST PORT NODE_ID [TIMEOUT_MS]");
        }
        System.out.println("MODE=LIVE_LOAD; BOOK G05 x6; this does not certify real Raft/SQL G5 acceptance");
        List<Result> results = run(args[1], Integer.parseInt(args[2]), args[3], args.length == 5 ? Integer.parseInt(args[4]) : 3000);
        long confirmed = results.stream().filter(r -> r.outcome().equals("CONFIRMED")).count();
        long unknown = results.stream().filter(r -> r.outcome().equals("UNKNOWN")).count();
        System.out.println("LIVE_LOAD_COMPLETE confirmed=" + confirmed + " unknown=" + unknown + "; real_Raft_G5=NOT_VERIFIED");
        if (confirmed > 1) { throw new IllegalStateException("Multiple CONFIRMED results for G05; inspect real logs/SQL"); }
        if (unknown > 0) { System.exit(2); }
    }

    private enum Scenario { NORMAL, DROP, DELAY, WRONG_CORRELATION, WRONG_REQUEST_ID, WRONG_SENDER, UNKNOWN, NOT_LEADER }

    /** Test-only loopback server; an atomic fixture winner is not a Raft implementation. */
    private static final class FakeTcpServer implements AutoCloseable {
        final Scenario scenario;
        final ServerSocket server;
        final ExecutorService acceptor = Executors.newSingleThreadExecutor();
        final ExecutorService handlers = Executors.newFixedThreadPool(CLIENT_COUNT);
        final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
        final ConcurrentLinkedQueue<MessageEnvelope<BookTicketRequestDto>> received = new ConcurrentLinkedQueue<>();
        final List<Future<?>> tasks = new CopyOnWriteArrayList<>();
        final CountDownLatch arrived = new CountDownLatch(CLIENT_COUNT);
        final AtomicBoolean winner = new AtomicBoolean();
        final AtomicLong indexes = new AtomicLong();
        final AtomicLong receivedBeforeFirstResponse = new AtomicLong();
        final Future<?> accepting;

        FakeTcpServer(Scenario scenario) throws IOException {
            this.scenario = scenario;
            server = new ServerSocket(0, CLIENT_COUNT, InetAddress.getLoopbackAddress());
            server.setSoTimeout(5000);
            accepting = acceptor.submit(() -> {
                for (int n = 0; n < CLIENT_COUNT; n++) {
                    Socket socket = server.accept();
                    socket.setSoTimeout(3000);
                    openSockets.add(socket);
                    tasks.add(handlers.submit(() -> { handle(socket); return null; }));
                }
                return null;
            });
        }

        int port() { return server.getLocalPort(); }

        void handle(Socket socket) throws Exception {
            try (socket) {
                MessageEnvelope<BookTicketRequestDto> request = FrameCodec.readMessage(socket.getInputStream(), BookTicketRequestDto.class);
                request.getPayload().validate();
                received.add(request);
                arrived.countDown();
                if (!arrived.await(3, TimeUnit.SECONDS)) { throw new IOException("Fixture did not receive six concurrent requests"); }
                receivedBeforeFirstResponse.compareAndSet(0, received.size());
                if (scenario == Scenario.DROP) { return; }
                if (scenario == Scenario.DELAY) { Thread.sleep(500); }
                String id = scenario == Scenario.WRONG_REQUEST_ID ? UUID.randomUUID().toString() : request.getPayload().getRequestId();
                Object payload;
                MessageType type;
                if (scenario == Scenario.NOT_LEADER) {
                    type = MessageType.ERROR_RESPONSE;
                    payload = new ErrorResponseDto("NOT_LEADER", "CONFIRMED is only misleading fixture message text", "node-2", 8L, id);
                } else {
                    type = MessageType.BOOK_TICKET_RESPONSE;
                    if (scenario == Scenario.UNKNOWN) { payload = BookTicketResponseDto.unknown(id, TRIP, SEAT); }
                    else {
                        long index = indexes.incrementAndGet();
                        payload = winner.compareAndSet(false, true)
                                ? BookTicketResponseDto.confirmed(id, TRIP, SEAT, "fixture-ticket-G05", index)
                                : BookTicketResponseDto.rejected(id, TRIP, SEAT, "SEAT_ALREADY_BOOKED", index);
                    }
                }
                String correlation = scenario == Scenario.WRONG_CORRELATION ? UUID.randomUUID().toString() : request.getMessageId();
                String sender = scenario == Scenario.WRONG_SENDER ? "node-4" : "node-3";
                MessageEnvelope<?> response = MessageEnvelope.createResponse(correlation, type, sender, payload);
                try { FrameCodec.writeMessage(socket.getOutputStream(), response); }
                catch (IOException exception) { if (scenario != Scenario.DELAY) { throw exception; } }
            } finally { openSockets.remove(socket); }
        }

        void awaitHandled() throws Exception {
            accepting.get(5, TimeUnit.SECONDS);
            assertEquals(CLIENT_COUNT, tasks.size());
            for (Future<?> task : tasks) { task.get(5, TimeUnit.SECONDS); }
        }

        @Override public void close() throws Exception {
            server.close();
            openSockets.forEach(ConcurrentBookingRunner::closeSocket);
            acceptor.shutdownNow();
            handlers.shutdownNow();
            assertTrue(acceptor.awaitTermination(3, TimeUnit.SECONDS), "Fixture acceptor did not stop");
            assertTrue(handlers.awaitTermination(3, TimeUnit.SECONDS), "Fixture handlers did not stop");
        }
    }
}
