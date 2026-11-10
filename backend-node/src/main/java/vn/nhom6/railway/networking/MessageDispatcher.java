package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Routes incoming {@link MessageEnvelope} requests to registered domain handlers.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Enforces channel and port isolation (e.g. rejects Raft RPCs on client port per SPEC-08 Section 64).</li>
 *   <li>Rejects invalid types and unexpected response types on the request dispatcher per SPEC-03 Section 3.1.</li>
 *   <li>Delegates processing to handlers registered in {@link HandlerRegistry}.</li>
 *   <li>Fail-safe exception handling returning standard {@link ErrorResponseDto} without crashing the node.</li>
 *   <li>Decouples transport from business logic (transport handles I/O, handlers implement business).</li>
 * </ul>
 */
public class MessageDispatcher implements TcpServer.MessageHandler {

    private static final Logger LOGGER = Logger.getLogger(MessageDispatcher.class.getName());
    private static final Pattern UUID_PATTERN = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    /**
     * Channel type defining which category of messages are accepted on this port/connection.
     */
    public enum ChannelType {
        /** Client-facing port (allows booking requests, queries; strictly rejects Raft RPCs). */
        CLIENT,
        /** Node peer port (allows Raft RPCs: REQUEST_VOTE, APPEND_ENTRIES; strictly rejects booking requests). */
        PEER,
        /** Diagnostic monitor port (allows NODE_STATUS; strictly rejects others). */
        MONITOR,
        /** Unrestricted channel (accepts all registered message types, useful for development or tests). */
        ALL
    }

    private final HandlerRegistry registry;
    private final ChannelType channelType;
    private final String nodeId;

    public MessageDispatcher(HandlerRegistry registry) {
        this(registry, ChannelType.ALL, "node-server");
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType) {
        this(registry, channelType, "node-server");
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType, String nodeId) {
        this.registry = Objects.requireNonNull(registry, "HandlerRegistry must not be null");
        this.channelType = Objects.requireNonNull(channelType, "ChannelType must not be null");
        this.nodeId = (nodeId != null && !nodeId.isBlank()) ? nodeId : "node-server";
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) {
        return dispatch(request);
    }

    /**
     * Dispatches an incoming request message envelope to the appropriate handler.
     *
     * @param request Incoming request envelope
     * @return Response envelope to send back, or {@code null} if no response or message is uncorrelatable
     */
    public MessageEnvelope<?> dispatch(MessageEnvelope<JsonNode> request) {
        if (request == null) {
            LOGGER.warning("Received null request envelope in MessageDispatcher");
            return null;
        }

        String messageId = request.getMessageId();
        if (!isValidUuid(messageId)) {
            // Per SPEC-03 Section 2: Only send ERROR_RESPONSE when messageId is a valid UUID for correlation
            LOGGER.warning(() -> "Cannot correlate error response: invalid or missing messageId: " + messageId);
            return null;
        }

        MessageType type = request.getType();
        String requestId = extractRequestId(request);

        // 1. Enforce Protocol Version (SPEC-03 Section 3.0)
        if (request.getProtocolVersion() != MessageEnvelope.CURRENT_PROTOCOL_VERSION) {
            LOGGER.warning(() -> "Unsupported protocolVersion: " + request.getProtocolVersion() + ", messageId=" + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Unsupported protocolVersion: " + request.getProtocolVersion() + ", expected: " + MessageEnvelope.CURRENT_PROTOCOL_VERSION, requestId);
        }

        // 2. Check for missing type
        if (type == null) {
            LOGGER.warning("Missing or null message type in request: " + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Missing or null message type", requestId);
        }

        // 3. Reject response messages arriving on the request dispatcher (SPEC-03 Section 3.1)
        if (type.isResponse()) {
            LOGGER.warning(() -> "Unexpected response message type arriving on request dispatcher: " + type + ", messageId=" + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Unexpected response message type on request dispatcher: " + type, requestId);
        }

        // 4. Enforce null correlationId on Requests (SPEC-03 Section 3.0)
        if (request.getCorrelationId() != null) {
            LOGGER.warning(() -> "Request envelope must have null correlationId, got: " + request.getCorrelationId() + ", messageId=" + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Request envelope must have null correlationId", requestId);
        }

        // 5. Check for missing or blank senderId
        if (request.getSenderId() == null || request.getSenderId().isBlank()) {
            LOGGER.warning(() -> "Missing or blank senderId in request: " + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Missing or blank senderId", requestId);
        }

        // 6. Check for missing or blank sentAt
        if (request.getSentAt() == null || request.getSentAt().isBlank()) {
            LOGGER.warning(() -> "Missing or blank sentAt in request: " + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Missing or blank sentAt", requestId);
        }

        // 7. Check for missing payload
        if (request.getPayload() == null) {
            LOGGER.warning(() -> "Missing payload in request: " + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Missing payload in request", requestId);
        }

        // 8. Enforce Channel / Port Isolation (SPEC-08 Section 64 & SPEC-07 TC-TCP-04)
        if (channelType == ChannelType.CLIENT && type.isRaftRpc()) {
            LOGGER.warning(() -> "Raft RPC rejected on CLIENT channel: " + type + " from sender=" + request.getSenderId());
            return createErrorResponse(messageId, "INVALID_REQUEST", "Raft RPC messages are not allowed on client channel: " + type, requestId);
        }
        if (channelType == ChannelType.PEER && !type.isRaftRpc()) {
            LOGGER.warning(() -> "Non-Raft message rejected on PEER channel: " + type + " from sender=" + request.getSenderId());
            return createErrorResponse(messageId, "INVALID_REQUEST", "Non-Raft messages are not allowed on peer channel: " + type, requestId);
        }
        if (channelType == ChannelType.MONITOR && type != MessageType.NODE_STATUS) {
            LOGGER.warning(() -> "Non-monitor message rejected on MONITOR channel: " + type + " from sender=" + request.getSenderId());
            return createErrorResponse(messageId, "INVALID_REQUEST", "Only NODE_STATUS messages are allowed on monitor channel: " + type, requestId);
        }

        // 9. Lookup handler in registry
        HandlerRegistry.RequestHandler handler = registry.getHandler(type);
        if (handler == null) {
            LOGGER.warning(() -> "No handler registered for message type: " + type);
            return createErrorResponse(messageId, "INVALID_REQUEST", "No handler registered for message type: " + type, requestId);
        }

        // 10. Execute handler and catch any exceptions safely
        try {
            MessageEnvelope<?> response = handler.handle(request);
            if (response != null && response.getCorrelationId() == null) {
                // Ensure correlationId invariant is never violated
                LOGGER.warning(() -> "Handler response missing correlationId, attaching request messageId=" + messageId);
                return MessageEnvelope.createResponse(messageId, response.getType(), response.getSenderId(), response.getPayload());
            }
            return response;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.WARNING, "Handler validation failed for type " + type + ": " + ex.getMessage(), ex);
            return createErrorResponse(messageId, "INVALID_REQUEST", ex.getMessage() != null ? ex.getMessage() : "Invalid request parameter", requestId);
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Handler threw unhandled exception for type " + type + ": " + t.getMessage(), t);
            return createErrorResponse(messageId, "INTERNAL_ERROR", "Internal server error: " + (t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName()), requestId);
        }
    }

    private MessageEnvelope<ErrorResponseDto> createErrorResponse(String requestMessageId, String errorCode, String message, String requestId) {
        return MessageCodec.createErrorResponse(requestMessageId, errorCode, message, null, null, requestId, nodeId);
    }

    private static String extractRequestId(MessageEnvelope<JsonNode> request) {
        if (request != null && request.getPayload() != null && request.getPayload().has("requestId")) {
            JsonNode reqIdNode = request.getPayload().get("requestId");
            if (reqIdNode.isTextual()) {
                String text = reqIdNode.asText().strip();
                if (isValidUuid(text)) {
                    return text;
                }
            }
        }
        return null;
    }

    private static boolean isValidUuid(String str) {
        return str != null && UUID_PATTERN.matcher(str).matches();
    }

    public HandlerRegistry getRegistry() {
        return registry;
    }

    public ChannelType getChannelType() {
        return channelType;
    }

    public String getNodeId() {
        return nodeId;
    }
}
