package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
     * Message types requiring Raft Leader authority (write commands and authoritative state queries).
     */
    public static final Set<MessageType> LEADER_ONLY_MESSAGE_TYPES = Set.of(
            MessageType.BOOK_TICKET,
            MessageType.CANCEL_TICKET,
            MessageType.GET_REQUEST_RESULT
    );

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

    /**
     * Default maximum concurrent in-flight requests per dispatcher.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT = 1000;

    /**
     * Context tracking an active in-flight request within MessageDispatcher.
     */
    public record InFlightContext(
            String messageId,
            String requestId,
            MessageType type,
            String senderId,
            long startTimeNanos
    ) {}

    /**
     * Hook interface for filtering or dropping messages in test environments (e.g. simulated network partitions).
     */
    @FunctionalInterface
    public interface MessageFilterHook {
        /**
         * Evaluates whether an incoming message should be delivered or dropped.
         *
         * @param senderId Sender ID from message envelope
         * @param recipientNodeId Local node ID receiving the message
         * @param message Decoded message envelope
         * @return true to allow message delivery, false to drop silently
         */
        boolean shouldDeliver(String senderId, String recipientNodeId, MessageEnvelope<?> message);
    }

    private final HandlerRegistry registry;
    private final ChannelType channelType;
    private final String nodeId;
    private final int maxInFlightRequests;
    private volatile LeaderRouter leaderRouter;
    private volatile MessageFilterHook filterHook;

    private final ConcurrentMap<String, InFlightContext> activeInFlight = new ConcurrentHashMap<>();

    public MessageDispatcher(HandlerRegistry registry) {
        this(registry, ChannelType.ALL, "node-server", null, DEFAULT_MAX_IN_FLIGHT);
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType) {
        this(registry, channelType, "node-server", null, DEFAULT_MAX_IN_FLIGHT);
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType, String nodeId) {
        this(registry, channelType, nodeId, null, DEFAULT_MAX_IN_FLIGHT);
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType, String nodeId, LeaderRouter leaderRouter) {
        this(registry, channelType, nodeId, leaderRouter, DEFAULT_MAX_IN_FLIGHT);
    }

    public MessageDispatcher(HandlerRegistry registry, ChannelType channelType, String nodeId, LeaderRouter leaderRouter, int maxInFlightRequests) {
        this.registry = Objects.requireNonNull(registry, "HandlerRegistry must not be null");
        this.channelType = Objects.requireNonNull(channelType, "ChannelType must not be null");
        this.nodeId = (nodeId != null && !nodeId.isBlank()) ? nodeId : "node-server";
        this.leaderRouter = leaderRouter;
        this.maxInFlightRequests = (maxInFlightRequests > 0) ? maxInFlightRequests : DEFAULT_MAX_IN_FLIGHT;
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

        // 8a. Test Network Filter Hook (Network Partition & Fault Simulation - TV3-D21)
        if (filterHook != null && !filterHook.shouldDeliver(request.getSenderId(), this.nodeId, request)) {
            LOGGER.info(() -> "[" + this.nodeId + "] Message dropped by TestNetworkFilter: from=" 
                    + request.getSenderId() + " to=" + this.nodeId + " type=" + type + ", messageId=" + messageId);
            return null; // Silently drop, simulating network partition packet loss
        }

        // 8b. Enforce In-Flight Tracking and Limits (SPEC-03 §1, §3.0 & TV3-D18)
        if (activeInFlight.containsKey(messageId)) {
            LOGGER.warning(() -> "Duplicate in-flight messageId rejected: " + messageId);
            return createErrorResponse(messageId, "INVALID_REQUEST", "Duplicate in-flight messageId: " + messageId, requestId);
        }
        if (activeInFlight.size() >= maxInFlightRequests) {
            LOGGER.warning(() -> "Server in-flight request limit reached (" + maxInFlightRequests + "), rejecting messageId: " + messageId);
            return createErrorResponse(messageId, "INTERNAL_ERROR", "Server in-flight request limit reached (" + maxInFlightRequests + ")", requestId);
        }

        activeInFlight.put(messageId, new InFlightContext(messageId, requestId, type, request.getSenderId(), System.nanoTime()));
        try {
            // 9. Enforce Leader Authority / Redirection for write commands per SPEC-02 §4 and SPEC-03 §7 (TV3-D17)
            if (leaderRouter != null && isLeaderOnlyRequest(type)) {
                try {
                    LeaderRouter.RoutingOutcome outcome = leaderRouter.route(request);
                    if (outcome != null && outcome.isHandled()) {
                        MessageEnvelope<?> redirectResponse = outcome.getResponse();
                        if (redirectResponse != null) {
                            LOGGER.info(() -> "[" + nodeId + "] Intercepted write request " + type + " for leader redirect; returning "
                                    + redirectResponse.getType() + " (correlationId=" + redirectResponse.getCorrelationId() + ")");
                            return ensureStrictCorrelation(redirectResponse, messageId, requestId);
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.log(Level.WARNING, "[" + nodeId + "] Leader routing check failed for " + type + ", messageId=" + messageId + ": " + ex.getMessage(), ex);
                    return createErrorResponse(messageId, "INTERNAL_ERROR", "Leader routing failed: " + ex.getMessage(), requestId);
                }
            }

            // 10. Lookup handler in registry
            HandlerRegistry.RequestHandler handler = registry.getHandler(type);
            if (handler == null) {
                LOGGER.warning(() -> "No handler registered for message type: " + type);
                return createErrorResponse(messageId, "INVALID_REQUEST", "No handler registered for message type: " + type, requestId);
            }

            // 11. Execute handler and catch any exceptions safely
            try {
                MessageEnvelope<?> response = handler.handle(request);
                return ensureStrictCorrelation(response, messageId, requestId);
            } catch (Throwable t) {
                MessageEnvelope<ErrorResponseDto> errorResponse = ResponseMapper.mapThrowable(
                        messageId, nodeId, requestId, t, leaderRouter
                );
                return ensureStrictCorrelation(errorResponse, messageId, requestId);
            }
        } finally {
            activeInFlight.remove(messageId);
        }
    }

    private MessageEnvelope<?> handleNotLeaderException(String messageId, String requestId, String explicitHint) {
        String hint = explicitHint;
        Long term = null;
        if (leaderRouter != null) {
            try {
                LeaderRouter.RoleSnapshot snapshot = leaderRouter.getRoleSnapshot();
                if (snapshot != null) {
                    term = snapshot.term();
                }
            } catch (Exception ignored) {}
            if (hint == null || hint.isBlank() || hint.equals(nodeId)) {
                hint = leaderRouter.resolveLeaderHint();
            }
        }
        return ErrorReplyFactory.createNotLeader(messageId, requestId, nodeId, hint, term);
    }

    private static BookingRequestHandler.NotLeaderException findNotLeaderException(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof BookingRequestHandler.NotLeaderException notLeaderEx) {
                return notLeaderEx;
            }
            current = current.getCause();
        }
        return null;
    }

    private MessageEnvelope<?> ensureStrictCorrelation(MessageEnvelope<?> response, String expectedCorrelationId, String requestId) {
        if (response == null) {
            return null;
        }
        if (expectedCorrelationId.equals(response.getCorrelationId())) {
            return response;
        }
        LOGGER.warning(() -> "CorrelationId mismatch or missing in response! Expected: "
                + expectedCorrelationId + ", actual: " + response.getCorrelationId()
                + ". Recreating envelope to preserve transport correlation invariant.");
        return MessageEnvelope.createResponse(
                expectedCorrelationId,
                response.getType(),
                response.getSenderId(),
                response.getPayload()
        );
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

    public LeaderRouter getLeaderRouter() {
        return leaderRouter;
    }

    public void setLeaderRouter(LeaderRouter leaderRouter) {
        this.leaderRouter = leaderRouter;
    }

    public MessageFilterHook getFilterHook() {
        return filterHook;
    }

    public void setFilterHook(MessageFilterHook filterHook) {
        this.filterHook = filterHook;
    }

    public boolean isLeaderOnlyRequest(MessageType type) {
        return type != null && LEADER_ONLY_MESSAGE_TYPES.contains(type);
    }

    public int getActiveInFlightCount() {
        return activeInFlight.size();
    }

    public boolean isInFlight(String messageId) {
        return messageId != null && activeInFlight.containsKey(messageId);
    }

    public InFlightContext getInFlightContext(String messageId) {
        return messageId != null ? activeInFlight.get(messageId) : null;
    }

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }
}
