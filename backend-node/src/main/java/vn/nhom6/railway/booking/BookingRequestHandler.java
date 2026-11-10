package vn.nhom6.railway.booking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Handles the v1 booking and seat-query messages at the TCP dispatcher boundary.
 *
 * <p>Raft commit, read-barrier, and complete seat-snapshot implementations are injected by the
 * Node composition root; this class does not create sockets or perform JDBC itself.
 */
public final class BookingRequestHandler implements HandlerRegistry.BookingHandler {
    private static final Logger LOGGER = Logger.getLogger(BookingRequestHandler.class.getName());
    private static final Pattern LEADER_ID_PATTERN = Pattern.compile("^node-[1-5]$");
    private static final Set<String> BOOK_FIELDS =
            Set.of("requestId", "tripId", "seatId", "passengerName");
    private static final Set<String> GET_SEATS_FIELDS = Set.of("tripId");
    private static final List<String> SEAT_IDS = List.of(
            "G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09", "G10");

    private final CommandSubmitter commandSubmitter;
    private final ReadBarrier readBarrier;
    private final Supplier<String> ticketIdSupplier;
    private final Clock clock;
    private final BookingRepository repository;
    private final SeatSnapshotReader seatSnapshotReader;
    private final String leaderId;
    private final String senderId;
    private final Duration timeout;

    public BookingRequestHandler(
            CommandSubmitter commandSubmitter,
            ReadBarrier readBarrier,
            Supplier<String> ticketIdSupplier,
            Clock clock,
            BookingRepository repository,
            String leaderId,
            Duration timeout) {
        this(
                commandSubmitter,
                readBarrier,
                ticketIdSupplier,
                clock,
                repository,
                repository instanceof SeatSnapshotReader ssr ? ssr : () -> defaultSnapshot(repository),
                leaderId,
                timeout);
    }

    public BookingRequestHandler(
            CommandSubmitter commandSubmitter,
            ReadBarrier readBarrier,
            Supplier<String> ticketIdSupplier,
            Clock clock,
            BookingRepository repository,
            SeatSnapshotReader seatSnapshotReader,
            String leaderId,
            Duration timeout) {
        this.commandSubmitter = Objects.requireNonNull(commandSubmitter, "commandSubmitter");
        this.readBarrier = Objects.requireNonNull(readBarrier, "readBarrier");
        this.ticketIdSupplier = Objects.requireNonNull(ticketIdSupplier, "ticketIdSupplier");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.repository = Objects.requireNonNull(repository, "repository");
        this.seatSnapshotReader = Objects.requireNonNull(seatSnapshotReader, "seatSnapshotReader");
        if (leaderId == null || !LEADER_ID_PATTERN.matcher(leaderId).matches()) {
            throw new IllegalArgumentException("leaderId must be node-1 through node-5");
        }
        this.leaderId = leaderId;
        this.senderId = leaderId;
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive and finite");
        }
        try {
            timeout.toNanos();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("timeout is too large", exception);
        }
        this.timeout = timeout;
    }

    /** Registers only the two P0 request types implemented by this handler. */
    public HandlerRegistry register(HandlerRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register(MessageType.BOOK_TICKET, this::handleBooking);
        registry.register(MessageType.GET_SEATS, this::handleBooking);
        return registry;
    }

    @Override
    public MessageEnvelope<?> handleBooking(MessageEnvelope<JsonNode> request) {
        if (request == null || !MessageEnvelope.isValidUuid(request.getMessageId())) {
            return null;
        }
        try {
            request.validate();
        } catch (IllegalArgumentException exception) {
            return error(request, "INVALID_REQUEST", "Invalid request envelope", null, null);
        }

        if (request.getType() == MessageType.BOOK_TICKET) {
            return handleBook(request);
        }
        if (request.getType() == MessageType.GET_SEATS) {
            return handleGetSeats(request);
        }
        return error(request, "INVALID_REQUEST", "Unsupported request type", null,
                extractRequestId(request.getPayload()));
    }

    private MessageEnvelope<?> handleBook(MessageEnvelope<JsonNode> request) {
        JsonNode payload = request.getPayload();
        String requestId = extractRequestId(payload);
        String tripId;
        String seatId;
        String passengerName;
        try {
            requireObjectWithFields(payload, BOOK_FIELDS);
            requestId = requiredText(payload, "requestId");
            tripId = requiredText(payload, "tripId");
            seatId = requiredText(payload, "seatId");
            passengerName = requiredText(payload, "passengerName");
            BookingCommand.requireValidRequestId(requestId);
            BookingCommand.requireValidTripId(tripId);
            BookingCommand.requireValidSeatId(seatId);
            passengerName = BookingCommand.requireValidPassengerName(passengerName);
        } catch (IllegalArgumentException exception) {
            return error(request, "INVALID_REQUEST", "Invalid BOOK_TICKET payload", null, requestId);
        }

        try {
            String ticketId = ticketIdSupplier.get();
            Instant issuedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
            BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                    requestId, tripId, seatId, passengerName, ticketId, issuedAt);
            ApplyResult result = awaitApply(command);
            return bookResponse(request, requestId, tripId, seatId, result);
        } catch (NotLeaderException exception) {
            return error(request, "NOT_LEADER", "This node is not the current leader",
                    exception.leaderHint(), requestId);
        } catch (NoQuorumException exception) {
            return error(request, "NO_QUORUM", "The command could not be committed by a quorum",
                    null, requestId);
        } catch (RequestIdConflictException exception) {
            return error(request, "REQUEST_ID_CONFLICT",
                    "requestId was already used for a different booking intent", null, requestId);
        } catch (TimeoutException exception) {
            return bookResponse(
                    request, requestId, tripId, seatId, ApplyResult.bookUnknown(requestId));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return bookResponse(
                    request, requestId, tripId, seatId, ApplyResult.bookUnknown(requestId));
        } catch (BookingRepository.StorageException exception) {
            logDatabaseFailure("BOOK_TICKET", exception);
            return error(request, "INTERNAL_ERROR", "Booking storage operation failed", null, requestId);
        } catch (RuntimeException exception) {
            LOGGER.log(Level.SEVERE, "BOOK_TICKET processing failed", exception);
            return error(request, "INTERNAL_ERROR", "Booking request could not be processed", null, requestId);
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "BOOK_TICKET commit path failed", exception);
            return error(request, "INTERNAL_ERROR", "Booking request could not be processed", null, requestId);
        }
    }

    private ApplyResult awaitApply(BookingCommand.BookTicket command)
            throws Exception {
        CompletionStage<ApplyResult> stage = Objects.requireNonNull(
                commandSubmitter.submit(command), "commandSubmitter returned null");
        try {
            ApplyResult result = stage.toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (result == null || !command.requestId().equals(result.requestId())
                    || !result.isPersistable()) {
                throw new IllegalStateException("Commit path returned an invalid apply result");
            }
            return result;
        } catch (ExecutionException | CompletionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Commit path failed", cause);
        }
    }

    private MessageEnvelope<?> handleGetSeats(MessageEnvelope<JsonNode> request) {
        String requestId = extractRequestId(request.getPayload());
        try {
            requireObjectWithFields(request.getPayload(), GET_SEATS_FIELDS);
            String tripId = requiredText(request.getPayload(), "tripId");
            BookingCommand.requireValidTripId(tripId);

            ReadBarrierResult barrierResult = awaitReadBarrier();
            if (!leaderId.equals(barrierResult.leaderId())) {
                throw new NotLeaderException(barrierResult.leaderId());
            }
            if (barrierResult.readIndex() < 1) {
                throw new ReadUnavailableException("readIndex must be >= 1");
            }
            if (repository.lastApplied() < barrierResult.readIndex()) {
                throw new ReadUnavailableException("read barrier is not applied locally");
            }

            Map<String, SeatState> snapshot = Objects.requireNonNull(
                    seatSnapshotReader.readSnapshot(), "seatSnapshotReader returned null");
            List<Map<String, Object>> seats = toResponseSeats(snapshot);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("tripId", tripId);
            response.put("seats", seats);
            response.put("readIndex", barrierResult.readIndex());
            response.put("leaderId", leaderId);
            return MessageEnvelope.createResponse(
                    request.getMessageId(), MessageType.GET_SEATS_RESPONSE, senderId, response);
        } catch (IllegalArgumentException exception) {
            return error(request, "INVALID_REQUEST", "Invalid GET_SEATS payload", null, requestId);
        } catch (NotLeaderException exception) {
            return error(request, "NOT_LEADER", "This node is not the current leader",
                    exception.leaderHint(), requestId);
        } catch (TimeoutException | ReadUnavailableException exception) {
            return error(request, "READ_UNAVAILABLE", "A current read barrier is unavailable",
                    null, requestId);
        } catch (BookingRepository.StorageException exception) {
            logDatabaseFailure("GET_SEATS", exception);
            return error(request, "INTERNAL_ERROR", "Booking storage operation failed", null, requestId);
        } catch (RuntimeException exception) {
            LOGGER.log(Level.SEVERE, "GET_SEATS processing failed", exception);
            return error(request, "INTERNAL_ERROR", "Seat snapshot could not be read", null, requestId);
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "GET_SEATS read barrier failed", exception);
            return error(request, "READ_UNAVAILABLE", "A current read barrier is unavailable",
                    null, requestId);
        }
    }

    private ReadBarrierResult awaitReadBarrier() throws Exception {
        CompletionStage<ReadBarrierResult> stage = Objects.requireNonNull(
                readBarrier.appendAndAwaitNoOp(timeout), "readBarrier returned null");
        try {
            return Objects.requireNonNull(
                    stage.toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS),
                    "readBarrier completed with null");
        } catch (ExecutionException | CompletionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Read barrier failed", cause);
        }
    }

    private static List<Map<String, Object>> toResponseSeats(Map<String, SeatState> snapshot) {
        if (snapshot.size() != SEAT_IDS.size()) {
            throw new BookingRepository.StorageException("Seat snapshot must contain exactly 10 seats");
        }
        List<Map<String, Object>> seats = new ArrayList<>(SEAT_IDS.size());
        for (String seatId : SEAT_IDS) {
            SeatState state = snapshot.get(seatId);
            if (state == null) {
                throw new BookingRepository.StorageException("Seat snapshot is missing " + seatId);
            }
            boolean available = "AVAILABLE".equals(state.status());
            if (!available && !"BOOKED".equals(state.status())) {
                throw new BookingRepository.StorageException("Invalid status in seat snapshot");
            }
            if (available == (state.ticketId() != null)) {
                throw new BookingRepository.StorageException("Inconsistent seat/ticket snapshot");
            }
            if (state.ticketId() != null) {
                BookingCommand.requireValidTicketId(state.ticketId());
            }
            Map<String, Object> seat = new LinkedHashMap<>();
            seat.put("seatId", seatId);
            seat.put("status", state.status());
            seat.put("ticketId", state.ticketId());
            seats.add(seat);
        }
        return seats;
    }

    private MessageEnvelope<?> bookResponse(
            MessageEnvelope<JsonNode> request,
            String requestId,
            String tripId,
            String seatId,
            ApplyResult result) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("requestId", requestId);
        payload.put("outcome", result.outcome().name());
        payload.put("ticketId", result.ticketId());
        payload.put("tripId", tripId);
        payload.put("seatId", seatId);
        payload.put("reasonCode", result.reasonCode());
        payload.put("appliedLogIndex", result.appliedLogIndex());
        return MessageEnvelope.createResponse(
                request.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, senderId, payload);
    }

    private MessageEnvelope<ErrorResponseDto> error(
            MessageEnvelope<JsonNode> request,
            String errorCode,
            String message,
            String leaderHint,
            String requestId) {
        return MessageCodec.createErrorResponse(
                request.getMessageId(), errorCode, message, leaderHint, null, requestId, senderId);
    }

    private static void requireObjectWithFields(JsonNode payload, Set<String> fields) {
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("payload must be an object");
        }
        if (payload.size() != fields.size()) {
            throw new IllegalArgumentException("payload has missing or unexpected fields");
        }
        var names = payload.fieldNames();
        while (names.hasNext()) {
            if (!fields.contains(names.next())) {
                throw new IllegalArgumentException("payload contains an unexpected field");
            }
        }
        for (String field : fields) {
            if (!payload.has(field) || payload.get(field).isNull()) {
                throw new IllegalArgumentException("payload is missing a required field");
            }
        }
    }

    private static String requiredText(JsonNode payload, String field) {
        JsonNode value = payload.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("payload field must be a string");
        }
        return value.textValue();
    }

    private static String extractRequestId(JsonNode payload) {
        if (payload != null && payload.isObject() && payload.has("requestId")
                && payload.get("requestId").isTextual()) {
            String requestId = payload.get("requestId").textValue();
            if (BookingCommand.UUID_PATTERN.matcher(requestId).matches()) {
                return requestId;
            }
        }
        return null;
    }

    private static Map<String, SeatState> defaultSnapshot(BookingRepository repository) {
        Map<String, String> statuses = repository.readSeatSnapshot();
        Map<String, SeatState> snapshot = new LinkedHashMap<>();
        statuses.forEach((seat, status) -> snapshot.put(seat, new SeatState(status, null)));
        return snapshot;
    }

    private static void logDatabaseFailure(String operation, BookingRepository.StorageException exception) {
        LOGGER.log(Level.SEVERE, operation + " failed due to a booking database error", exception);
    }

    @FunctionalInterface
    public interface CommandSubmitter {
        CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) throws Exception;
    }

    @FunctionalInterface
    public interface ReadBarrier {
        CompletionStage<ReadBarrierResult> appendAndAwaitNoOp(Duration timeout) throws Exception;
    }

    @FunctionalInterface
    public interface SeatSnapshotReader {
        /** Returns status and current ticket IDs read from one consistent database snapshot. */
        Map<String, SeatState> readSnapshot();
    }

    public record ReadBarrierResult(long readIndex, String leaderId) {}

    public record SeatState(String status, String ticketId) {}

    public static class NotLeaderException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String leaderHint;

        public NotLeaderException(String leaderHint) {
            this.leaderHint = leaderHint;
        }

        public String leaderHint() {
            return leaderHint;
        }
    }

    public static class NoQuorumException extends Exception {
        private static final long serialVersionUID = 1L;

        public NoQuorumException() {}
    }

    public static class RequestIdConflictException extends Exception {
        private static final long serialVersionUID = 1L;

        public RequestIdConflictException() {}
    }

    public static class ReadUnavailableException extends Exception {
        private static final long serialVersionUID = 1L;

        public ReadUnavailableException(String message) {
            super(message);
        }
    }
}
