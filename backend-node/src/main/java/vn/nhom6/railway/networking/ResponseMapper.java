package vn.nhom6.railway.networking;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standardized response and error mapper according to SPEC-03 Section 4.2, 4.4, and Section 7 (TASK TV3-D19).
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li><b>Strict BOOK Outcome Segregation:</b> Maps domain {@link ApplyResult} to {@link BookTicketResponseDto}
 *       with exact outcomes: {@code CONFIRMED}, {@code REJECTED}, or {@code UNKNOWN}.</li>
 *   <li><b>Strict Transport Error Isolation:</b> Network, routing, quorum, and read barrier failures
 *       ({@code NOT_LEADER}, {@code NO_QUORUM}, {@code READ_UNAVAILABLE}) produce {@link MessageType#ERROR_RESPONSE}
 *       and are <b>NEVER</b> converted into business {@code REJECTED} outcomes.</li>
 *   <li><b>Strict GET_REQUEST_RESULT State Segregation:</b> Maps {@link ResultLookup.LookupResult} to
 *       {@link GetRequestResultResponseDto} states: {@code APPLIED_SUCCESS}, {@code APPLIED_REJECTED},
 *       {@code PENDING}, or {@code UNKNOWN}.</li>
 *   <li><b>Correlation & RequestId Invariants:</b> Guaranteed 1-to-1 correlationId mapping and verbatim
 *       preservation of client business {@code requestId}.</li>
 * </ul>
 */
public final class ResponseMapper {

    private static final Logger LOGGER = Logger.getLogger(ResponseMapper.class.getName());

    private ResponseMapper() {
        // Utility class
    }

    /**
     * Maps a domain {@link ApplyResult} to a strongly-typed {@link BookTicketResponseDto} envelope.
     *
     * @param requestMessageId Incoming request's messageId (used as correlationId)
     * @param senderId         Local node ID
     * @param tripId           Target tripId (must be SE-001)
     * @param seatId           Target seatId (G01-G10)
     * @param result           Domain apply result from StateMachine
     * @return Validated {@link MessageEnvelope} with {@link BookTicketResponseDto}
     */
    public static MessageEnvelope<BookTicketResponseDto> toBookTicketResponse(
            String requestMessageId,
            String senderId,
            String tripId,
            String seatId,
            ApplyResult result) {
        Objects.requireNonNull(requestMessageId, "requestMessageId must not be null");
        Objects.requireNonNull(senderId, "senderId must not be null");
        Objects.requireNonNull(result, "ApplyResult must not be null");

        String outcomeStr = result.outcome().name();
        BookTicketResponseDto responseDto = new BookTicketResponseDto(
                result.requestId(),
                outcomeStr,
                result.ticketId(),
                tripId,
                seatId,
                result.reasonCode(),
                result.appliedLogIndex()
        );
        responseDto.validate();

        return MessageEnvelope.createResponse(
                requestMessageId,
                MessageType.BOOK_TICKET_RESPONSE,
                senderId,
                responseDto
        );
    }

    /**
     * Maps a {@link ResultLookup.LookupResult} to a strongly-typed {@link GetRequestResultResponseDto} envelope.
     *
     * @param requestMessageId Incoming request's messageId (used as correlationId)
     * @param senderId         Local node ID
     * @param lookupResult     Lookup query result
     * @return Validated {@link MessageEnvelope} with {@link GetRequestResultResponseDto}
     */
    public static MessageEnvelope<GetRequestResultResponseDto> toGetRequestResultResponse(
            String requestMessageId,
            String senderId,
            ResultLookup.LookupResult lookupResult) {
        Objects.requireNonNull(requestMessageId, "requestMessageId must not be null");
        Objects.requireNonNull(senderId, "senderId must not be null");
        Objects.requireNonNull(lookupResult, "lookupResult must not be null");

        GetRequestResultResponseDto dto = new GetRequestResultResponseDto(
                lookupResult.requestId(),
                lookupResult.state(),
                lookupResult.commandType(),
                lookupResult.ticketId(),
                lookupResult.reasonCode(),
                lookupResult.appliedLogIndex()
        );
        dto.validate();

        return MessageEnvelope.createResponse(
                requestMessageId,
                MessageType.GET_REQUEST_RESULT_RESPONSE,
                senderId,
                dto
        );
    }

    /**
     * Builds a standard {@code NOT_LEADER} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toNotLeader(
            String requestMessageId,
            String senderId,
            String requestId,
            String leaderHint,
            Long term) {
        return ErrorReplyFactory.createNotLeader(requestMessageId, requestId, senderId, leaderHint, term);
    }

    /**
     * Builds a standard {@code NO_QUORUM} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toNoQuorum(
            String requestMessageId,
            String senderId,
            String requestId,
            Long term,
            String leaderHint) {
        String message = "Write command could not be committed by a majority quorum (term " + (term != null ? term : "?") + ")";
        return ErrorReplyFactory.createNoQuorum(requestMessageId, requestId, senderId, message, term, leaderHint);
    }

    /**
     * Builds a standard {@code READ_UNAVAILABLE} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toReadUnavailable(
            String requestMessageId,
            String senderId,
            String requestId,
            Long term,
            String leaderHint) {
        String message = "A current read barrier is unavailable or timed out; cannot prove authoritative state";
        return ErrorReplyFactory.createReadUnavailable(requestMessageId, requestId, senderId, message, term, leaderHint);
    }

    /**
     * Builds a standard {@code REQUEST_ID_CONFLICT} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toRequestIdConflict(
            String requestMessageId,
            String senderId,
            String requestId,
            String message) {
        String msg = (message != null && !message.isBlank())
                ? message
                : "requestId was already used with conflicting intent";
        return ErrorReplyFactory.createRequestIdConflict(requestMessageId, requestId, senderId, msg);
    }

    /**
     * Builds a standard {@code INVALID_REQUEST} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toInvalidRequest(
            String requestMessageId,
            String senderId,
            String requestId,
            String message) {
        return ErrorReplyFactory.createInvalidRequest(requestMessageId, requestId, senderId, message);
    }

    /**
     * Builds a standard {@code INTERNAL_ERROR} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> toInternalError(
            String requestMessageId,
            String senderId,
            String requestId,
            String message,
            Long term,
            String leaderHint) {
        return ErrorReplyFactory.createInternalError(requestMessageId, requestId, senderId, message, term, leaderHint);
    }

    /**
     * Inspects, unrolls, and maps any {@link Throwable} caught during request dispatch into its exact SPEC-03 error code.
     *
     * @param requestMessageId Incoming request's messageId
     * @param senderId         Local node ID
     * @param requestId        Client business requestId
     * @param throwable        Exception or error thrown
     * @param leaderRouter     Optional leader router for extracting current term and leaderHint
     * @return Validated {@link MessageEnvelope} of type {@link MessageType#ERROR_RESPONSE}
     */
    public static MessageEnvelope<ErrorResponseDto> mapThrowable(
            String requestMessageId,
            String senderId,
            String requestId,
            Throwable throwable,
            LeaderRouter leaderRouter) {
        Throwable cause = unwrap(throwable);

        Long termVal = null;
        String hintVal = null;
        if (leaderRouter != null) {
            try {
                LeaderRouter.RoleSnapshot snapshot = leaderRouter.getRoleSnapshot();
                if (snapshot != null) {
                    termVal = snapshot.term();
                }
            } catch (Exception ignored) {}
            try {
                hintVal = leaderRouter.resolveLeaderHint();
            } catch (Exception ignored) {}
        }
        final Long currentTerm = termVal;
        final String leaderHint = hintVal;

        // 1. NotLeaderException
        BookingRequestHandler.NotLeaderException notLeaderEx = findException(cause, BookingRequestHandler.NotLeaderException.class);
        if (notLeaderEx != null) {
            String hint = notLeaderEx.leaderHint();
            if (hint == null || hint.isBlank() || hint.equals(senderId)) {
                hint = leaderHint;
            }
            final String finalHint = hint;
            LOGGER.info(() -> "[" + senderId + "] Mapping NotLeaderException to NOT_LEADER (hint=" + finalHint + ", term=" + currentTerm + ")");
            return toNotLeader(requestMessageId, senderId, requestId, finalHint, currentTerm);
        }

        // 2. NoQuorumException
        BookingRequestHandler.NoQuorumException noQuorumEx = findException(cause, BookingRequestHandler.NoQuorumException.class);
        if (noQuorumEx != null) {
            LOGGER.warning(() -> "[" + senderId + "] Mapping NoQuorumException to NO_QUORUM (term=" + currentTerm + ")");
            return toNoQuorum(requestMessageId, senderId, requestId, currentTerm, leaderHint);
        }

        // 3. ReadUnavailableException
        BookingRequestHandler.ReadUnavailableException readUnavailEx = findException(cause, BookingRequestHandler.ReadUnavailableException.class);
        if (readUnavailEx != null) {
            LOGGER.warning(() -> "[" + senderId + "] Mapping ReadUnavailableException to READ_UNAVAILABLE: " + readUnavailEx.getMessage());
            return toReadUnavailable(requestMessageId, senderId, requestId, currentTerm, leaderHint);
        }

        // 4. RequestIdConflictException
        BookingRequestHandler.RequestIdConflictException conflictEx = findException(cause, BookingRequestHandler.RequestIdConflictException.class);
        if (conflictEx != null) {
            LOGGER.warning(() -> "[" + senderId + "] Mapping RequestIdConflictException to REQUEST_ID_CONFLICT");
            return toRequestIdConflict(requestMessageId, senderId, requestId, "requestId was already used with conflicting intent");
        }

        // 5. IllegalArgumentException (Validation failure)
        if (cause instanceof IllegalArgumentException illegalArg) {
            LOGGER.warning(() -> "[" + senderId + "] Mapping IllegalArgumentException to INVALID_REQUEST: " + illegalArg.getMessage());
            return toInvalidRequest(requestMessageId, senderId, requestId,
                    illegalArg.getMessage() != null ? illegalArg.getMessage() : "Invalid request parameter");
        }

        // 6. TimeoutException
        if (cause instanceof TimeoutException) {
            LOGGER.warning(() -> "[" + senderId + "] Mapping TimeoutException to READ_UNAVAILABLE / INTERNAL_ERROR");
            return toReadUnavailable(requestMessageId, senderId, requestId, currentTerm, leaderHint);
        }

        // 7. Unclassified / Internal Error
        LOGGER.log(Level.SEVERE, "[" + senderId + "] Handler threw unhandled exception, mapping to INTERNAL_ERROR: " + cause.getMessage(), cause);
        String msg = "Internal server error: " + (cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName());
        return toInternalError(requestMessageId, senderId, requestId, msg, currentTerm, leaderHint);
    }

    /**
     * Unwraps execution and completion exceptions to find root cause.
     */
    public static Throwable unwrap(Throwable t) {
        Throwable current = t;
        while (current instanceof ExecutionException || current instanceof CompletionException) {
            if (current.getCause() != null) {
                current = current.getCause();
            } else {
                break;
            }
        }
        return current != null ? current : t;
    }

    /**
     * Traverses the exception cause chain to locate a target exception type.
     */
    public static <T extends Throwable> T findException(Throwable t, Class<T> targetClass) {
        Throwable current = t;
        while (current != null) {
            if (targetClass.isInstance(current)) {
                return targetClass.cast(current);
            }
            current = current.getCause();
        }
        return null;
    }
}
