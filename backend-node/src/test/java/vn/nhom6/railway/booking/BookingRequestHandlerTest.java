package vn.nhom6.railway.booking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookingRequestHandlerTest {
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();
    private static final String REQUEST_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQUEST_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQUEST_3 = "33333333-3333-3333-3333-333333333333";
    private static final Instant FIXED_TIME = Instant.parse("2026-11-11T00:00:00.123Z");
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void registersOnlyBookAndGetSeatsWithTv3Registry() {
        FakeRepository repository = new FakeRepository();
        BookingRequestHandler handler = handler(
                command -> new CompletableFuture<>(),
                timeout -> CompletableFuture.completedFuture(
                        new BookingRequestHandler.ReadBarrierResult(1, "node-1")),
                repository,
                repository::snapshot);
        HandlerRegistry registry = handler.register(new HandlerRegistry());

        assertTrue(registry.hasHandler(MessageType.BOOK_TICKET));
        assertTrue(registry.hasHandler(MessageType.GET_SEATS));
        assertFalse(registry.hasHandler(MessageType.CANCEL_TICKET));
    }

    @Test
    void test01_validBook_confirmedPayload_injectedValues_correlatedMessageId() throws Exception {
        FakeRepository repository = new FakeRepository();
        InMemoryCommitter committer = new InMemoryCommitter(repository);
        BookingRequestHandler handler = handler(
                committer,
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        MessageEnvelope<JsonNode> firstRequest = bookRequest(REQUEST_1, "G01", "Linh");
        MessageEnvelope<?> confirmed = handler.handleBooking(firstRequest);
        assertBookResponse(confirmed, "CONFIRMED", "ticket-test-1", null, 1L);
        assertEquals(firstRequest.getMessageId(), confirmed.getCorrelationId());
        assertNotEquals(firstRequest.getMessageId(), confirmed.getMessageId());
        assertEquals(FIXED_TIME, committer.lastCommand().issuedAt());
        assertEquals("ticket-test-1", committer.lastCommand().ticketId());
    }

    @Test
    void test02_malformedBookPayload_invalidRequest_commitPathNotInvoked() throws Exception {
        FakeRepository repository = new FakeRepository();
        AtomicInteger submitCount = new AtomicInteger();
        BookingRequestHandler handler = handler(
                command -> {
                    submitCount.incrementAndGet();
                    return CompletableFuture.completedFuture(
                            ApplyResult.bookConfirmed(command.requestId(), command.ticketId(), 1));
                },
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        List<String> invalidPayloads = List.of(
                "{\"requestId\":\"" + REQUEST_1 + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"G11\",\"passengerName\":\"Tuấn\"}",
                "{\"requestId\":\"not-a-uuid\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"G01\",\"passengerName\":\"Tuấn\"}",
                "{\"requestId\":\"" + REQUEST_1 + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"G01\"}",
                "{\"requestId\":\"" + REQUEST_1 + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"G01\",\"passengerName\":\"Tuấn\",\"extra\":1}",
                "{\"requestId\":\"" + REQUEST_1 + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"G01\",\"passengerName\":\"Tuấn\","
                        + "\"ticketId\":\"client-ticket\"}",
                "{\"requestId\":\"" + REQUEST_1 + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":1,\"passengerName\":\"Tuấn\"}");

        for (String invalidPayload : invalidPayloads) {
            MessageEnvelope<?> response =
                    handler.handleBooking(request(MessageType.BOOK_TICKET, invalidPayload));
            assertError("INVALID_REQUEST", response);
        }
        assertEquals(0, submitCount.get(), "invalid payloads must not reach commit submission");
    }

    @Test
    void test03_seatAlreadyBooked_rejected_onlyOneActiveTicket() throws Exception {
        FakeRepository repository = new FakeRepository();
        InMemoryCommitter committer = new InMemoryCommitter(repository);
        BookingRequestHandler handler = handler(
                committer,
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));
        MessageEnvelope<?> rejected =
                handler.handleBooking(bookRequest(REQUEST_2, "G01", "Mai"));
        assertBookResponse(rejected, "REJECTED", null,
                ApplyResult.SEAT_ALREADY_BOOKED, 2L);
        assertEquals(1, repository.activeTicketCount("G01"));
        assertEquals(1, repository.ticketCount());
    }

    @Test
    void test04_retrySameRequestIdDifferentMessageId_sameResult_noExtraTicket() throws Exception {
        FakeRepository repository = new FakeRepository();
        InMemoryCommitter committer = new InMemoryCommitter(repository);
        BookingRequestHandler handler = handler(
                committer,
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        MessageEnvelope<JsonNode> firstRequest = bookRequest(REQUEST_1, "G01", "Linh");
        MessageEnvelope<?> confirmed = handler.handleBooking(firstRequest);
        assertBookResponse(confirmed, "CONFIRMED", "ticket-test-1", null, 1L);

        MessageEnvelope<JsonNode> replayRequest = bookRequest(REQUEST_1, "G01", "Linh");
        MessageEnvelope<?> replay = handler.handleBooking(replayRequest);
        assertBookResponse(replay, "CONFIRMED", "ticket-test-1", null, 1L);
        assertEquals(replayRequest.getMessageId(), replay.getCorrelationId());
        assertNotEquals(confirmed.getMessageId(), replay.getMessageId());
        assertEquals(1, repository.ticketCount());
    }

    @Test
    void test05_sameRequestIdDifferentPayload_requestIdConflict() throws Exception {
        FakeRepository repository = new FakeRepository();
        InMemoryCommitter committer = new InMemoryCommitter(repository);
        BookingRequestHandler handler = handler(
                committer,
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));
        MessageEnvelope<?> conflict =
                handler.handleBooking(bookRequest(REQUEST_1, "G02", "Linh"));
        assertError("REQUEST_ID_CONFLICT", conflict);
    }

    @Test
    void test06_timeout_outcomeUnknown_noPersistedRequestResults() throws Exception {
        FakeRepository repository = new FakeRepository();
        BookingRequestHandler handler = handler(
                command -> new CompletableFuture<>(),
                successfulBarrier(repository),
                repository,
                repository::snapshot,
                Duration.ofMillis(5));

        MessageEnvelope<?> response =
                handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));

        assertBookResponse(response, "UNKNOWN", null, ApplyResult.TIMEOUT_UNKNOWN, null);
        assertTrue(repository.findResultByRequestId(REQUEST_1).isEmpty());
        assertEquals(0, repository.ticketCount());
    }

    @Test
    void test07_notLeaderAndNoQuorum_neverConfirmed() throws Exception {
        FakeRepository repository = new FakeRepository();
        BookingRequestHandler notLeader = handler(
                command -> {
                    throw new BookingRequestHandler.NotLeaderException("node-2");
                },
                successfulBarrier(repository),
                repository,
                repository::snapshot);
        MessageEnvelope<?> notLeaderResponse =
                notLeader.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));
        assertError("NOT_LEADER", notLeaderResponse);
        ErrorResponseDto notLeaderPayload = errorPayload(notLeaderResponse);
        assertEquals("node-2", notLeaderPayload.getLeaderHint());

        BookingRequestHandler noQuorum = handler(
                command -> {
                    throw new BookingRequestHandler.NoQuorumException();
                },
                successfulBarrier(repository),
                repository,
                repository::snapshot);
        assertError("NO_QUORUM",
                noQuorum.handleBooking(bookRequest(REQUEST_2, "G02", "Mai")));
        assertEquals(0, repository.ticketCount());
    }

    @Test
    void test08_doesNotIssueConfirmedBeforeApplyCompletes() throws Exception {
        FakeRepository repository = new FakeRepository();
        CompletableFuture<ApplyResult> resultFuture = new CompletableFuture<>();
        CountDownLatch submitted = new CountDownLatch(1);
        BookingRequestHandler handler = handler(
                command -> {
                    submitted.countDown();
                    return resultFuture;
                },
                successfulBarrier(repository),
                repository,
                repository::snapshot);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<MessageEnvelope<?>> responseFuture = CompletableFuture.supplyAsync(
                    () -> handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh")), executor);
            assertTrue(submitted.await(1, TimeUnit.SECONDS));
            assertFalse(responseFuture.isDone(), "handler must wait for durable apply result");
            resultFuture.complete(ApplyResult.bookConfirmed(REQUEST_1, "ticket-applied", 1));
            assertBookResponse(responseFuture.get(1, TimeUnit.SECONDS),
                    "CONFIRMED", "ticket-applied", null, 1L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void test09_getSeats_orderedTenSeats_bookedWithTicketId_barrierFailures() throws Exception {
        FakeRepository repository = new FakeRepository();
        InMemoryCommitter committer = new InMemoryCommitter(repository);
        BookingRequestHandler handler = handler(
                committer,
                successfulBarrier(repository),
                repository,
                repository::snapshot);
        handler.handleBooking(bookRequest(REQUEST_1, "G02", "Linh"));

        MessageEnvelope<?> response =
                handler.handleBooking(request(MessageType.GET_SEATS, "{\"tripId\":\"SE-001\"}"));
        assertEquals(MessageType.GET_SEATS_RESPONSE, response.getType());
        assertNotEquals(response.getCorrelationId(), response.getMessageId());
        assertTrue(response.getCorrelationId() != null);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = MAPPER.convertValue(response.getPayload(), Map.class);
        assertEquals("SE-001", payload.get("tripId"));
        assertEquals("node-1", payload.get("leaderId"));
        assertEquals(1L, ((Number) payload.get("readIndex")).longValue());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> seats = (List<Map<String, Object>>) payload.get("seats");
        assertEquals(10, seats.size());
        for (int index = 0; index < 10; index++) {
            assertEquals("G" + String.format("%02d", index + 1), seats.get(index).get("seatId"));
        }
        assertEquals("BOOKED", seats.get(1).get("status"));
        assertEquals("ticket-test-1", seats.get(1).get("ticketId"));
        assertNull(seats.get(0).get("ticketId"));

        BookingRequestHandler failedBarrier = handler(
                command -> new CompletableFuture<>(),
                timeout -> CompletableFuture.failedFuture(
                        new BookingRequestHandler.ReadUnavailableException("no quorum")),
                repository,
                repository::snapshot);
        assertError("READ_UNAVAILABLE",
                failedBarrier.handleBooking(request(MessageType.GET_SEATS, "{\"tripId\":\"SE-001\"}")));

        BookingRequestHandler changedLeader = handler(
                command -> new CompletableFuture<>(),
                timeout -> CompletableFuture.completedFuture(
                        new BookingRequestHandler.ReadBarrierResult(2, "node-2")),
                repository,
                repository::snapshot);
        assertError("NOT_LEADER",
                changedLeader.handleBooking(request(MessageType.GET_SEATS, "{\"tripId\":\"SE-001\"}")));
    }

    @Test
    void test10_unsupportedMessageType_errorResponse() throws Exception {
        FakeRepository repository = new FakeRepository();
        BookingRequestHandler handler = handler(
                command -> new CompletableFuture<>(),
                successfulBarrier(repository),
                repository,
                repository::snapshot);

        assertError("INVALID_REQUEST",
                handler.handleBooking(request(MessageType.NODE_STATUS, "{\"tripId\":\"SE-001\"}")));
    }

    @Test
    void postgresBackedHandlerExercisesCommittedApplyAndPrintsDatabaseSnapshot() throws Exception {
        String jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
        String username = System.getenv("RAILWAY_TEST_DB_USER");
        String password = System.getenv("RAILWAY_TEST_DB_PASSWORD");
        Assumptions.assumeTrue(
                hasText(jdbcUrl) && hasText(username) && hasText(password),
                "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD are required");

        try (TestDatabase database = TestDatabase.open(jdbcUrl, username, password)) {
            JdbcBookingRepository repository = new JdbcBookingRepository(database.connectionFactory());
            JdbcCommittedCommandApplier applier =
                    new JdbcCommittedCommandApplier(database.connectionFactory());
            DatabaseCommitter committer = new DatabaseCommitter(database, repository, applier);
            BookingRequestHandler handler = handler(
                    committer,
                    timeout -> {
                        long index = repository.lastApplied() + 1;
                        database.insertCommittedLog(index, new BookingCommand.NoOp());
                        applier.applyCommitted(index, new BookingCommand.NoOp());
                        return CompletableFuture.completedFuture(
                                new BookingRequestHandler.ReadBarrierResult(index, "node-1"));
                    },
                    repository,
                    database::readSeatSnapshot);

            MessageEnvelope<?> confirmed =
                    handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));
            assertBookResponse(confirmed, "CONFIRMED", "ticket-db-1", null, 1L);

            MessageEnvelope<?> replay =
                    handler.handleBooking(bookRequest(REQUEST_1, "G01", "Linh"));
            assertBookResponse(replay, "CONFIRMED", "ticket-db-1", null, 1L);
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM tickets"));

            assertError("REQUEST_ID_CONFLICT",
                    handler.handleBooking(bookRequest(REQUEST_1, "G02", "Linh")));

            MessageEnvelope<?> rejected =
                    handler.handleBooking(bookRequest(REQUEST_2, "G01", "Mai"));
            assertBookResponse(rejected, "REJECTED", null,
                    ApplyResult.SEAT_ALREADY_BOOKED, 4L);

            BookingRequestHandler timedOut = handler(
                    command -> new CompletableFuture<>(),
                    timeout -> CompletableFuture.completedFuture(
                            new BookingRequestHandler.ReadBarrierResult(
                                    repository.lastApplied() + 1, "node-1")),
                    repository,
                    database::readSeatSnapshot,
                    Duration.ofMillis(5));
            assertBookResponse(
                    timedOut.handleBooking(bookRequest(REQUEST_3, "G03", "Nam")),
                    "UNKNOWN", null, ApplyResult.TIMEOUT_UNKNOWN, null);
            assertEquals(0, database.queryInt(
                    "SELECT COUNT(*) FROM request_results WHERE request_id = '" + REQUEST_3 + "'"));

            MessageEnvelope<?> seatsResponse =
                    handler.handleBooking(request(MessageType.GET_SEATS, "{\"tripId\":\"SE-001\"}"));
            @SuppressWarnings("unchecked")
            Map<String, Object> seatsPayload =
                    MAPPER.convertValue(seatsResponse.getPayload(), Map.class);
            assertEquals(5L, ((Number) seatsPayload.get("readIndex")).longValue());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> seats = (List<Map<String, Object>>) seatsPayload.get("seats");
            assertEquals(10, seats.size());
            assertEquals("ticket-db-1", seats.get(0).get("ticketId"));

            database.printAndAssertSnapshot();
        }
    }

    private static BookingRequestHandler handler(
            BookingRequestHandler.CommandSubmitter submitter,
            BookingRequestHandler.ReadBarrier readBarrier,
            BookingRepository repository,
            BookingRequestHandler.SeatSnapshotReader snapshotReader) {
        return handler(submitter, readBarrier, repository, snapshotReader, TEST_TIMEOUT);
    }

    private static BookingRequestHandler handler(
            BookingRequestHandler.CommandSubmitter submitter,
            BookingRequestHandler.ReadBarrier readBarrier,
            BookingRepository repository,
            BookingRequestHandler.SeatSnapshotReader snapshotReader,
            Duration timeout) {
        AtomicInteger ticketSequence = new AtomicInteger();
        return new BookingRequestHandler(
                submitter,
                readBarrier,
                () -> "ticket-test-" + ticketSequence.incrementAndGet(),
                Clock.fixed(FIXED_TIME, ZoneOffset.UTC),
                repository,
                snapshotReader,
                "node-1",
                timeout);
    }

    private static BookingRequestHandler.ReadBarrier successfulBarrier(FakeRepository repository) {
        return timeout -> CompletableFuture.completedFuture(
                new BookingRequestHandler.ReadBarrierResult(
                        Math.max(1, repository.lastApplied()), "node-1"));
    }

    private static MessageEnvelope<JsonNode> bookRequest(
            String requestId, String seatId, String passengerName) {
        return request(MessageType.BOOK_TICKET,
                "{\"requestId\":\"" + requestId + "\",\"tripId\":\"SE-001\","
                        + "\"seatId\":\"" + seatId + "\",\"passengerName\":\""
                        + passengerName + "\"}");
    }

    private static MessageEnvelope<JsonNode> request(MessageType type, String payload)
            throws RuntimeException {
        try {
            return MessageEnvelope.createRequest(type, "client-test", MAPPER.readTree(payload));
        } catch (IOException exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }

    private static void assertBookResponse(
            MessageEnvelope<?> response,
            String outcome,
            String ticketId,
            String reasonCode,
            Long appliedLogIndex) {
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, response.getType());
        assertTrue(MessageEnvelope.isValidUuid(response.getMessageId()));
        assertTrue(MessageEnvelope.isValidUuid(response.getCorrelationId()));
        assertNotEquals(response.getCorrelationId(), response.getMessageId());
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = MAPPER.convertValue(response.getPayload(), Map.class);
        assertEquals(outcome, payload.get("outcome"));
        assertEquals(ticketId, payload.get("ticketId"));
        assertEquals(reasonCode, payload.get("reasonCode"));
        assertEquals(appliedLogIndex, payload.get("appliedLogIndex"));
    }

    private static void assertError(String code, MessageEnvelope<?> response) {
        assertEquals(MessageType.ERROR_RESPONSE, response.getType());
        assertEquals(code, errorPayload(response).getErrorCode());
    }

    private static ErrorResponseDto errorPayload(MessageEnvelope<?> response) {
        return MAPPER.convertValue(response.getPayload(), ErrorResponseDto.class);
    }

    private static final class FakeRepository implements BookingRepository {
        private final Map<String, SeatState> seats = new LinkedHashMap<>();
        private final Map<String, BookingCommand.BookTicket> commands = new LinkedHashMap<>();
        private final Map<String, ApplyResult> results = new LinkedHashMap<>();
        private long lastApplied;

        private FakeRepository() {
            for (int index = 1; index <= 10; index++) {
                seats.put("G" + String.format("%02d", index),
                        new SeatState("AVAILABLE", null));
            }
        }

        @Override
        public long lastApplied() {
            return lastApplied;
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            if (logIndex != lastApplied + 1) {
                throw new IllegalStateException("fake apply index out of order");
            }
            BookingStateMachine.ApplyOutcome outcome;
            if (command instanceof BookingCommand.BookTicket book) {
                BookingCommand.BookTicket previous = commands.get(book.requestId());
                if (previous != null) {
                    if (!sameIntent(previous, book)) {
                        outcome = new BookingStateMachine.ApplyOutcome.RequestIdConflict(
                                book.requestId(), logIndex);
                    } else {
                        outcome = new BookingStateMachine.ApplyOutcome.BusinessResult(
                                results.get(book.requestId()), true);
                    }
                } else if ("BOOKED".equals(seats.get(book.seatId()).status())) {
                    ApplyResult result = ApplyResult.bookRejected(
                            book.requestId(), ApplyResult.SEAT_ALREADY_BOOKED, logIndex);
                    commands.put(book.requestId(), book);
                    results.put(book.requestId(), result);
                    outcome = new BookingStateMachine.ApplyOutcome.BusinessResult(result, false);
                } else {
                    ApplyResult result = ApplyResult.bookConfirmed(
                            book.requestId(), book.ticketId(), logIndex);
                    commands.put(book.requestId(), book);
                    results.put(book.requestId(), result);
                    seats.put(book.seatId(), new SeatState("BOOKED", book.ticketId()));
                    outcome = new BookingStateMachine.ApplyOutcome.BusinessResult(result, false);
                }
            } else if (command instanceof BookingCommand.NoOp) {
                outcome = new BookingStateMachine.ApplyOutcome.NoOpApplied(logIndex);
            } else {
                throw new IllegalArgumentException("Unsupported fake command");
            }
            lastApplied = logIndex;
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return Optional.ofNullable(results.get(requestId));
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.empty();
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            Map<String, String> snapshot = new LinkedHashMap<>();
            seats.forEach((seatId, state) -> snapshot.put(seatId, state.status()));
            return snapshot;
        }

        private Map<String, BookingRequestHandler.SeatState> snapshot() {
            Map<String, BookingRequestHandler.SeatState> result = new LinkedHashMap<>();
            seats.forEach((seatId, state) ->
                    result.put(seatId, new BookingRequestHandler.SeatState(
                            state.status(), state.ticketId())));
            return result;
        }

        private int ticketCount() {
            return (int) seats.values().stream().filter(state -> state.ticketId() != null).count();
        }

        private int activeTicketCount(String seatId) {
            return "BOOKED".equals(seats.get(seatId).status()) ? 1 : 0;
        }

        private static boolean sameIntent(
                BookingCommand.BookTicket first, BookingCommand.BookTicket second) {
            return first.tripId().equals(second.tripId())
                    && first.seatId().equals(second.seatId())
                    && first.passengerName().equals(second.passengerName());
        }
    }

    /** Test-only LOCAL one-node commit fixture; it is never used by runtime code. */
    private static final class InMemoryCommitter implements BookingRequestHandler.CommandSubmitter {
        private final FakeRepository repository;
        private BookingCommand.BookTicket lastCommand;

        private InMemoryCommitter(FakeRepository repository) {
            this.repository = repository;
        }

        @Override
        public CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) {
            lastCommand = command;
            long index = repository.lastApplied() + 1;
            BookingStateMachine.ApplyOutcome outcome = repository.applyAtomically(index, command);
            if (outcome instanceof BookingStateMachine.ApplyOutcome.RequestIdConflict) {
                return CompletableFuture.failedFuture(
                        new BookingRequestHandler.RequestIdConflictException());
            }
            return CompletableFuture.completedFuture(
                    ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome).applyResult());
        }

        private BookingCommand.BookTicket lastCommand() {
            return lastCommand;
        }
    }

    private record SeatState(String status, String ticketId) {}

    /** Test-only LOCAL adapter that drives the existing committed JDBC applier. */
    private static final class DatabaseCommitter implements BookingRequestHandler.CommandSubmitter {
        private final TestDatabase database;
        private final JdbcBookingRepository repository;
        private final JdbcCommittedCommandApplier applier;

        private DatabaseCommitter(
                TestDatabase database,
                JdbcBookingRepository repository,
                JdbcCommittedCommandApplier applier) {
            this.database = database;
            this.repository = repository;
            this.applier = applier;
        }

        @Override
        public CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) throws Exception {
            long index = repository.lastApplied() + 1;
            database.insertCommittedLog(index, command);
            BookingStateMachine.ApplyOutcome outcome =
                    applier.applyCommitted(index, command).orElseThrow();
            if (outcome instanceof BookingStateMachine.ApplyOutcome.RequestIdConflict) {
                throw new BookingRequestHandler.RequestIdConflictException();
            }
            return CompletableFuture.completedFuture(
                    ((BookingStateMachine.ApplyOutcome.BusinessResult) outcome).applyResult());
        }
    }

    private static final class TestDatabase implements AutoCloseable {
        private final String baseUrl;
        private final String username;
        private final String password;
        private final String schema;
        private final vn.nhom6.railway.booking.ConnectionFactory connectionFactory;

        private TestDatabase(String jdbcUrl, String username, String password) throws Exception {
            this.baseUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = "railway_test_" + UUID.randomUUID().toString().replace("-", "");
            createSchema();
            try {
                URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
                String dbName = uri.getPath().substring(1) + "?currentSchema=" + schema;
                DbConfig config = new DbConfig(
                        uri.getHost(), uri.getPort() < 0 ? 5432 : uri.getPort(),
                        dbName, username, password);
                this.connectionFactory = new vn.nhom6.railway.booking.ConnectionFactory(config);
                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
            } catch (Exception exception) {
                try {
                    dropSchema();
                } catch (SQLException cleanupException) {
                    exception.addSuppressed(cleanupException);
                }
                throw exception;
            }
        }

        private static TestDatabase open(String jdbcUrl, String username, String password)
                throws Exception {
            return new TestDatabase(jdbcUrl, username, password);
        }

        private vn.nhom6.railway.booking.ConnectionFactory connectionFactory() {
            return connectionFactory;
        }

        private void insertCommittedLog(long index, BookingCommand command) throws Exception {
            Map<String, Object> json = new LinkedHashMap<>();
            if (command instanceof BookingCommand.BookTicket book) {
                json.put("requestId", book.requestId());
                json.put("tripId", book.tripId());
                json.put("seatId", book.seatId());
                json.put("passengerName", book.passengerName());
                json.put("ticketId", book.ticketId());
                json.put("issuedAt", book.issuedAt().toString());
            }
            try (Connection connection = connectionFactory.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "INSERT INTO raft_log(log_index, term, command_type, command_json) "
                                    + "VALUES (?, 1, ?, ?::jsonb)")) {
                statement.setLong(1, index);
                statement.setString(2, command.type().name());
                statement.setString(3, MAPPER.writeValueAsString(json));
                statement.executeUpdate();
            }
        }

        private Map<String, BookingRequestHandler.SeatState> readSeatSnapshot() {
            Map<String, BookingRequestHandler.SeatState> result = new LinkedHashMap<>();
            try (Connection connection = connectionFactory.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT seat_id, status, current_ticket_id FROM seats "
                                    + "WHERE trip_id = 'SE-001' ORDER BY seat_id");
                    ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.put(rows.getString(1),
                            new BookingRequestHandler.SeatState(rows.getString(2), rows.getString(3)));
                }
            } catch (SQLException exception) {
                throw new BookingRepository.StorageException("snapshot query failed", exception);
            }
            return result;
        }

        private int queryInt(String sql) throws SQLException {
            try (Connection connection = connectionFactory.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }

        private void printAndAssertSnapshot() throws SQLException {
            System.out.println("=== FAKE/LOCAL PostgreSQL snapshot (1 Node; no Raft cluster proof) ===");
            printQuery("seats", "SELECT trip_id, seat_id, status, current_ticket_id "
                    + "FROM seats ORDER BY trip_id, seat_id");
            printQuery("tickets", "SELECT ticket_id, trip_id, seat_id, passenger_name, status, issued_at "
                    + "FROM tickets ORDER BY ticket_id");
            printQuery("request_results", "SELECT request_id, command_type, state, reason_code, "
                    + "ticket_id, applied_log_index, result_json FROM request_results ORDER BY request_id");
            printQuery("business_events", "SELECT log_index, request_id, event_type, event_json "
                    + "FROM business_events ORDER BY log_index");
            printQuery("raft_apply_progress", "SELECT singleton, last_applied "
                    + "FROM raft_apply_progress ORDER BY singleton");

            int totalSeats = queryInt("SELECT COUNT(*) FROM seats WHERE trip_id = 'SE-001'");
            int duplicateActiveSeats = queryInt("SELECT COUNT(*) FROM (SELECT trip_id, seat_id "
                    + "FROM tickets WHERE status = 'ACTIVE' GROUP BY trip_id, seat_id "
                    + "HAVING COUNT(*) > 1) duplicate_active");
            int duplicateRequestIds = queryInt("SELECT COUNT(*) FROM (SELECT request_id "
                    + "FROM request_results GROUP BY request_id HAVING COUNT(*) > 1) duplicate_requests");
            long lastApplied = queryLong(
                    "SELECT last_applied FROM raft_apply_progress WHERE singleton = 1");
            System.out.println("SPEC-07 §5 checks: seats=" + totalSeats
                    + " (expected 10), duplicate ACTIVE seats=" + duplicateActiveSeats
                    + " (expected 0), duplicate requestIds=" + duplicateRequestIds
                    + " (expected 0), last_applied=" + lastApplied);
            assertEquals(10, totalSeats);
            assertEquals(0, duplicateActiveSeats);
            assertEquals(0, duplicateRequestIds);
            assertEquals(5L, lastApplied);
            System.out.println("=== END FAKE/LOCAL PostgreSQL snapshot ===");
        }

        private void printQuery(String label, String sql) throws SQLException {
            System.out.println("-- " + label + ": " + sql);
            try (Connection connection = connectionFactory.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                var metadata = resultSet.getMetaData();
                List<String> columns = new ArrayList<>();
                for (int index = 1; index <= metadata.getColumnCount(); index++) {
                    columns.add(metadata.getColumnLabel(index));
                }
                System.out.println(String.join(" | ", columns));
                while (resultSet.next()) {
                    List<String> values = new ArrayList<>();
                    for (int index = 1; index <= metadata.getColumnCount(); index++) {
                        values.add(String.valueOf(resultSet.getObject(index)));
                    }
                    System.out.println(String.join(" | ", values));
                }
            }
        }

        private long queryLong(String sql) throws SQLException {
            try (Connection connection = connectionFactory.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                assertTrue(resultSet.next());
                return resultSet.getLong(1);
            }
        }

        private void createSchema() throws SQLException {
            try (Connection connection = DriverManager.getConnection(baseUrl, username, password);
                    Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + schema + "\"");
            }
        }

        private void runMigration(String migrationName) throws IOException, SQLException {
            Path migrationPath = Path.of("database", "migration", migrationName);
            if (!Files.isRegularFile(migrationPath)) {
                migrationPath = Path.of("..", "database", "migration", migrationName);
            }
            String migration = Files.readString(migrationPath, StandardCharsets.UTF_8);
            try (Connection connection = connectionFactory.getConnection();
                    Statement statement = connection.createStatement()) {
                for (String sql : migration.split(";")) {
                    if (!sql.isBlank()) {
                        statement.execute(sql);
                    }
                }
            }
        }

        private void dropSchema() throws SQLException {
            try (Connection connection = DriverManager.getConnection(baseUrl, username, password);
                    Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
        }

        @Override
        public void close() throws SQLException {
            dropSchema();
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
