package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Objects;

/**
 * Factory for building standardized, strictly-validated {@link MessageEnvelope} of type
 * {@link MessageType#ERROR_RESPONSE} according to SPEC-03 Section 7.
 *
 * <p>All five fields ({@code errorCode}, {@code message}, {@code leaderHint}, {@code term},
 * {@code requestId}) are consistently formatted and validated before returning the envelope.
 */
public final class ErrorReplyFactory {

    private ErrorReplyFactory() {
        // Utility class
    }

    /**
     * Builds a standard {@code NOT_LEADER} error envelope per SPEC-03 Section 7.
     *
     * @param correlationId MessageId of the incoming request
     * @param requestId     Client business request UUID (must be preserved verbatim)
     * @param localNodeId   The ID of the local node issuing this response
     * @param leaderHint    Known leader ID or selected fallback node (nullable, but non-null when available)
     * @param term          Current Raft term (nullable, but non-null when consensus state initialized)
     * @return Validated {@link MessageEnvelope} containing {@link ErrorResponseDto}
     */
    public static MessageEnvelope<ErrorResponseDto> createNotLeader(
            String correlationId,
            String requestId,
            String localNodeId,
            String leaderHint,
            Long term) {
        String message = "Node " + localNodeId + " is not the leader. Current leader hint: "
                + (leaderHint != null ? leaderHint : "unknown")
                + (term != null ? " (term " + term + ")" : "");
        return createError(correlationId, "NOT_LEADER", message, leaderHint, term, requestId, localNodeId);
    }

    /**
     * Builds a standard {@code INVALID_REQUEST} error envelope per SPEC-03 Section 7.
     *
     * @param correlationId MessageId of the incoming request
     * @param requestId     Client business request UUID (nullable if parse failed before requestId)
     * @param localNodeId   The ID of the local node issuing this response
     * @param message       Human-readable description of the validation failure
     * @return Validated {@link MessageEnvelope} containing {@link ErrorResponseDto}
     */
    public static MessageEnvelope<ErrorResponseDto> createInvalidRequest(
            String correlationId,
            String requestId,
            String localNodeId,
            String message) {
        return createError(correlationId, "INVALID_REQUEST", message, null, null, requestId, localNodeId);
    }

    /**
     * Builds a standard {@code NO_QUORUM} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> createNoQuorum(
            String correlationId,
            String requestId,
            String localNodeId,
            String message,
            Long term,
            String leaderHint) {
        return createError(correlationId, "NO_QUORUM", message, leaderHint, term, requestId, localNodeId);
    }

    /**
     * Builds a standard {@code READ_UNAVAILABLE} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> createReadUnavailable(
            String correlationId,
            String localNodeId,
            String message,
            Long term,
            String leaderHint) {
        return createError(correlationId, "READ_UNAVAILABLE", message, leaderHint, term, null, localNodeId);
    }

    /**
     * Builds a standard {@code INTERNAL_ERROR} error envelope per SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> createInternalError(
            String correlationId,
            String requestId,
            String localNodeId,
            String message,
            Long term,
            String leaderHint) {
        return createError(correlationId, "INTERNAL_ERROR", message, leaderHint, term, requestId, localNodeId);
    }

    /**
     * Core factory method creating and validating {@link ErrorResponseDto} inside a {@link MessageEnvelope}.
     *
     * @param correlationId Must match incoming request's messageId
     * @param errorCode     Standard error code string (non-null, non-blank)
     * @param message       Human-readable message (non-null, non-blank)
     * @param leaderHint    Nullable leader hint
     * @param term          Nullable term (must be >= 0 when present)
     * @param requestId     Nullable client business requestId
     * @param senderId      Local node ID (non-null)
     * @return Validated envelope
     */
    public static MessageEnvelope<ErrorResponseDto> createError(
            String correlationId,
            String errorCode,
            String message,
            String leaderHint,
            Long term,
            String requestId,
            String senderId) {
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(senderId, "senderId must not be null");

        ErrorResponseDto dto = new ErrorResponseDto(errorCode, message, leaderHint, term, requestId);
        dto.validate();

        return MessageEnvelope.createResponse(correlationId, MessageType.ERROR_RESPONSE, senderId, dto);
    }

    /**
     * Robust extraction of business {@code requestId} from arbitrary request envelopes.
     *
     * <p>Preserves the exact client UUID string without any modification or truncation.
     * Returns {@code null} if the payload does not contain a valid UUID {@code requestId}.
     *
     * @param envelope Incoming request envelope
     * @return Extracted UUID string or null
     */
    public static String extractRequestId(MessageEnvelope<?> envelope) {
        if (envelope == null || envelope.getPayload() == null) {
            return null;
        }
        Object payload = envelope.getPayload();
        if (payload instanceof JsonNode jsonNode) {
            if (jsonNode.hasNonNull("requestId") && jsonNode.get("requestId").isTextual()) {
                String id = jsonNode.get("requestId").textValue();
                if (MessageEnvelope.isValidUuid(id)) {
                    return id;
                }
            }
        } else if (payload instanceof BookTicketRequestDto bookDto) {
            String id = bookDto.getRequestId();
            if (MessageEnvelope.isValidUuid(id)) {
                return id;
            }
        }
        return null;
    }
}
