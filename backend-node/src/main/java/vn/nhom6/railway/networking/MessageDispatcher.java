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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
     * Frozen wire protocol version for release v1 (SPEC-03 Section 3.0 & TV3-D28).
     */
    public static final int FROZEN_PROTOCOL_VERSION = MessageEnvelope.CURRENT_PROTOCOL_VERSION;

    /**
     * Immutable set of all 17 {@link MessageType} values frozen under protocol v1.
     */
    public static final Set<MessageType> FROZEN_V1_MESSAGE_TYPES = Set.of(MessageType.values());

    /**
     * Standard client-facing TCP port on LAN per SPEC-08 Section 3 and configs/lan/PORTS.md.
     */
    public static final int STANDARD_CLIENT_PORT = 9100;

    /**
     * Minimum and maximum Windows/host-published Dev client TCP ports per SPEC-08 Section 3 and configs/dev/cluster.properties.
     */
    public static final int DEV_CLIENT_PORT_MIN = 9101;
    public static final int DEV_CLIENT_PORT_MAX = 9105;

    /**
     * Standard Raft peer-to-peer consensus TCP port on LAN per SPEC-08 Section 3 and configs/lan/PORTS.md.
     */
    public static final int STANDARD_PEER_PORT = 9200;

    /**
     * Minimum and maximum Windows/host-published Dev Raft peer TCP ports per SPEC-08 Section 3 and configs/dev/cluster.properties.
     */
    public static final int DEV_PEER_PORT_MIN = 9201;
    public static final int DEV_PEER_PORT_MAX = 9205;

    /**
     * Returns whether the given wire {@code protocolVersion} matches the frozen v1 release contract.
     */
    public static boolean isSupportedProtocolVersion(int protocolVersion) {
        return protocolVersion == FROZEN_PROTOCOL_VERSION;
    }

    /**
     * Returns whether the given {@link MessageType} belongs to the frozen v1 contract set.
     */
    public static boolean isFrozenV1MessageType(MessageType type) {
        return type != null && FROZEN_V1_MESSAGE_TYPES.contains(type);
    }

    /**
     * Checks whether a TCP port corresponds to a standard Client channel port
     * (LAN {@code 9100} or Windows dev host published {@code 9101..9105}) per SPEC-08 §3.
     *
     * @param port Socket port number
     * @return {@code true} if the port is designated for CLIENT traffic
     */
    public static boolean isStandardClientPort(int port) {
        return port == STANDARD_CLIENT_PORT || (port >= DEV_CLIENT_PORT_MIN && port <= DEV_CLIENT_PORT_MAX);
    }

    /**
     * Checks whether a TCP port corresponds to a standard Raft Peer channel port
     * (LAN {@code 9200} or Windows dev host published {@code 9201..9205}) per SPEC-08 §3.
     *
     * @param port Socket port number
     * @return {@code true} if the port is designated for PEER Raft traffic
     */
    public static boolean isStandardPeerPort(int port) {
        return port == STANDARD_PEER_PORT || (port >= DEV_PEER_PORT_MIN && port <= DEV_PEER_PORT_MAX);
    }

    /**
     * Resolves the expected {@link ChannelType} based on standard SPEC-08 port mapping.
     *
     * <p>Port mapping rules per SPEC-08 §3 and configs/lan/PORTS.md:
     * <ul>
     *   <li>Port 9100 (or dev host published 9101..9105) maps to {@link ChannelType#CLIENT}.</li>
     *   <li>Port 9200 (or dev host published 9201..9205) maps to {@link ChannelType#PEER}.</li>
     *   <li>Any other port defaults to {@link ChannelType#ALL}.</li>
     * </ul>
     *
     * @param port Socket port number
     * @return Resolved ChannelType
     */
    public static ChannelType resolveChannelForPort(int port) {
        if (isStandardClientPort(port)) {
            return ChannelType.CLIENT;
        }
        if (isStandardPeerPort(port)) {
            return ChannelType.PEER;
        }
        return ChannelType.ALL;
    }

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
     * Thread-safe per-node diagnostics metrics snapshot for protocol/concurrency regression checks.
     */
    public record DispatcherMetrics(
            long completedCount,
            long rejectedCount,
            long exceptionCount,
            long droppedCount,
            int peakInFlightCount,
            int currentInFlightCount
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

    /**
     * Test-only hook interface for simulating outbound response packet loss after a request
     * has already been handled/committed (e.g. SPEC-07 TC-AC10 post-apply response drop).
     */
    @FunctionalInterface
    public interface ResponseFilterHook {
        /**
         * Evaluates whether a correlated response envelope should be written back on the TCP socket.
         *
         * @param responderNodeId Local node ID sending the response
         * @param senderId Original requester ID
         * @param request Decoded incoming request envelope
         * @param response Produced and correlated response envelope
         * @return {@code true} to send response normally, {@code false} to drop silently
         */
        boolean shouldSendResponse(
                String responderNodeId,
                String senderId,
                MessageEnvelope<JsonNode> request,
                MessageEnvelope<?> response
        );
    }

    private final HandlerRegistry registry;
    private final ChannelType channelType;
    private final String nodeId;
    private final int maxInFlightRequests;
    private volatile LeaderRouter leaderRouter;
    private volatile MessageFilterHook filterHook;
    private volatile ResponseFilterHook responseFilterHook;
    private volatile Set<String> allowedPeerIds;

    private final ConcurrentMap<String, InFlightContext> activeInFlight = new ConcurrentHashMap<>();
    private final AtomicLong completedCount = new AtomicLong(0);
    private final AtomicLong rejectedCount = new AtomicLong(0);
    private final AtomicLong exceptionCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicInteger peakInFlightCount = new AtomicInteger(0);

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

        // 1. Enforce Protocol Version (SPEC-03 Section 3.0 & TV3-D28 freeze)
        if (!isSupportedProtocolVersion(request.getProtocolVersion())) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Unsupported protocolVersion: " + request.getProtocolVersion() + ", expected: " + FROZEN_PROTOCOL_VERSION,
                    requestId);
        }

        // 2. Check for missing type
        if (type == null) {
            return logAndCreateError(messageId, "INVALID_REQUEST", "Missing or null message type", requestId);
        }

        // 3. Reject response messages arriving on the request dispatcher (SPEC-03 Section 3.1)
        if (type.isResponse()) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Unexpected response message type on request dispatcher: " + type, requestId);
        }

        // 4. Enforce null correlationId on Requests (SPEC-03 Section 3.0)
        if (request.getCorrelationId() != null) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Request envelope must have null correlationId, got: " + request.getCorrelationId(), requestId);
        }

        // 5. Check for missing or blank senderId
        if (request.getSenderId() == null || request.getSenderId().isBlank()) {
            return logAndCreateError(messageId, "INVALID_REQUEST", "Missing or blank senderId", requestId);
        }

        // 6. Check for missing or blank sentAt
        if (request.getSentAt() == null || request.getSentAt().isBlank()) {
            return logAndCreateError(messageId, "INVALID_REQUEST", "Missing or blank sentAt", requestId);
        }

        // 7. Check for missing payload
        if (request.getPayload() == null) {
            return logAndCreateError(messageId, "INVALID_REQUEST", "Missing payload in request", requestId);
        }

        // 8. Enforce Channel / Port Isolation (SPEC-08 Section 64 & SPEC-07 TC-TCP-04)
        if (channelType == ChannelType.CLIENT && type.isRaftRpc()) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Raft RPC messages are not allowed on client channel: " + type + " from sender=" + request.getSenderId(), requestId);
        }
        if (channelType == ChannelType.PEER && !type.isRaftRpc()) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Non-Raft messages are not allowed on peer channel: " + type + " from sender=" + request.getSenderId(), requestId);
        }
        if (channelType == ChannelType.MONITOR && type != MessageType.NODE_STATUS) {
            return logAndCreateError(messageId, "INVALID_REQUEST",
                    "Only NODE_STATUS messages are allowed on monitor channel: " + type + " from sender=" + request.getSenderId(), requestId);
        }

        // 8a-peer. Optional Peer Sender Allowlist Check (SPEC-08 §3.1 configuration sanity guard; NOT cryptographic auth)
        Set<String> peerAllowlist = this.allowedPeerIds;
        if (channelType == ChannelType.PEER && peerAllowlist != null) {
            String sender = request.getSenderId().strip();
            if (this.nodeId.equals(sender) || !peerAllowlist.contains(sender)) {
                return logAndCreateError(messageId, "INVALID_REQUEST",
                        "SenderId is not an allowed remote peer on PEER channel: " + sender, requestId);
            }
        }

        // 8a. Test Network Filter Hook (Network Partition & Fault Simulation - TV3-D21)
        if (filterHook != null && !filterHook.shouldDeliver(request.getSenderId(), this.nodeId, request)) {
            droppedCount.incrementAndGet();
            LOGGER.info(() -> "[" + this.nodeId + "] Message dropped by TestNetworkFilter: from=" 
                    + request.getSenderId() + " to=" + this.nodeId + " type=" + type + ", messageId=" + messageId);
            return null; // Silently drop, simulating network partition packet loss
        }

        // 8b. Enforce In-Flight Tracking and Limits (SPEC-03 §1, §3.0 & TV3-D18)
        if (activeInFlight.containsKey(messageId)) {
            return logAndCreateError(messageId, "INVALID_REQUEST", "Duplicate in-flight messageId: " + messageId, requestId);
        }
        if (activeInFlight.size() >= maxInFlightRequests) {
            return logAndCreateError(messageId, "INTERNAL_ERROR",
                    "Server in-flight request limit reached (" + maxInFlightRequests + ")", requestId);
        }

        activeInFlight.put(messageId, new InFlightContext(messageId, requestId, type, request.getSenderId(), System.nanoTime()));
        updatePeakInFlight(activeInFlight.size());
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
                            completedCount.incrementAndGet();
                            return finalizeResponse(redirectResponse, request, messageId, requestId);
                        }
                    }
                } catch (Exception ex) {
                    exceptionCount.incrementAndGet();
                    LOGGER.log(Level.WARNING, "[" + nodeId + "] Leader routing check failed for " + type + ", messageId=" + messageId + ": " + ex.getMessage(), ex);
                    return createErrorResponse(messageId, "INTERNAL_ERROR", "Leader routing failed: " + ex.getMessage(), requestId);
                }
            }

            // 10. Lookup handler in registry
            HandlerRegistry.RequestHandler handler = registry.getHandler(type);
            if (handler == null) {
                return logAndCreateError(messageId, "INVALID_REQUEST", "No handler registered for message type: " + type, requestId);
            }

            // 11. Execute handler and catch any exceptions safely
            try {
                MessageEnvelope<?> response = handler.handle(request);
                completedCount.incrementAndGet();
                return finalizeResponse(response, request, messageId, requestId);
            } catch (Throwable t) {
                exceptionCount.incrementAndGet();
                LOGGER.log(Level.WARNING, "[" + nodeId + "] Exception while handling " + type
                        + " (messageId=" + messageId + ", requestId=" + requestId + "): " + t.getMessage(), t);
                MessageEnvelope<ErrorResponseDto> errorResponse = ResponseMapper.mapThrowable(
                        messageId, nodeId, requestId, t, leaderRouter
                );
                return finalizeResponse(errorResponse, request, messageId, requestId);
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

    private MessageEnvelope<?> finalizeResponse(
            MessageEnvelope<?> response,
            MessageEnvelope<JsonNode> request,
            String expectedCorrelationId,
            String requestId) {
        MessageEnvelope<?> correlated = ensureStrictCorrelation(response, expectedCorrelationId, requestId);
        if (correlated == null) {
            return null;
        }
        ResponseFilterHook outboundHook = this.responseFilterHook;
        if (outboundHook != null && !outboundHook.shouldSendResponse(this.nodeId, request.getSenderId(), request, correlated)) {
            droppedCount.incrementAndGet();
            LOGGER.info(() -> "[" + this.nodeId + "] Response dropped by ResponseFilterHook: to="
                    + request.getSenderId() + " type=" + correlated.getType()
                    + ", correlationId=" + correlated.getCorrelationId());
            return null;
        }
        return correlated;
    }

    private void updatePeakInFlight(int currentSize) {
        int prev;
        do {
            prev = peakInFlightCount.get();
            if (currentSize <= prev) {
                return;
            }
        } while (!peakInFlightCount.compareAndSet(prev, currentSize));
    }

    private MessageEnvelope<ErrorResponseDto> logAndCreateError(
            String requestMessageId,
            String errorCode,
            String reason,
            String requestId) {
        LOGGER.warning(() -> "[" + nodeId + "] Rejected request (" + channelType + ", errorCode=" + errorCode
                + ", messageId=" + requestMessageId + ", requestId=" + requestId + "): " + reason);
        return createErrorResponse(requestMessageId, errorCode, reason, requestId);
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
        rejectedCount.incrementAndGet();
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

    public ResponseFilterHook getResponseFilterHook() {
        return responseFilterHook;
    }

    public void setResponseFilterHook(ResponseFilterHook responseFilterHook) {
        this.responseFilterHook = responseFilterHook;
    }

    /**
     * Returns the configured remote/cluster peer ID allowlist for the {@link ChannelType#PEER} channel,
     * or {@code null} when no allowlist filter is active.
     */
    public Set<String> getAllowedPeerIds() {
        return allowedPeerIds;
    }

    /**
     * Configures an allowlist of known peer IDs accepted on the {@link ChannelType#PEER} channel per SPEC-08 §3.1.
     *
     * <p>Note per SPEC-08 §3.1: checking {@code senderId} against the configured peer topology is a routing/sanity
     * guard only and MUST NOT be advertised as cryptographic authentication; LAN firewall isolation on port 9200
     * provides host-level restriction.
     *
     * @param allowedPeerIds Set of valid voter IDs (e.g. {@code node-1..node-5}), or {@code null} to disable
     */
    public void setAllowedPeerIds(Set<String> allowedPeerIds) {
        this.allowedPeerIds = (allowedPeerIds == null) ? null : Set.copyOf(allowedPeerIds);
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

    /**
     * Returns an immutable per-node telemetry snapshot of dispatched, rejected, failed, dropped,
     * and peak concurrent in-flight requests.
     */
    public DispatcherMetrics getMetrics() {
        return new DispatcherMetrics(
                completedCount.get(),
                rejectedCount.get(),
                exceptionCount.get(),
                droppedCount.get(),
                peakInFlightCount.get(),
                activeInFlight.size()
        );
    }
}
