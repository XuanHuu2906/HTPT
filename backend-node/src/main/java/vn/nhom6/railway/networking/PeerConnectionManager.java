package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages peer TCP connection pools, post-election routing refresh, safe leader redirection,
 * and request state recovery via GET_REQUEST_RESULT (TASK TV3-D22).
 *
 * <p>Key Invariants (AGENTS.md §5 & SPEC-03 §4.4):
 * <ul>
 *   <li><b>Post-Election Routing Refresh:</b> Upon new leader election, stale/poisoned connections
 *       to the old leader are safely closed and traffic is routed to the new leader.</li>
 *   <li><b>RequestId Invariance:</b> The client's business {@code requestId} is strictly preserved
 *       across all redirects, retries, and GET_REQUEST_RESULT queries. Never alters requestId.</li>
 *   <li><b>GET_REQUEST_RESULT Before Retry:</b> If a request enters an {@code UNKNOWN} state due to
 *       leader failure, the system queries {@code GET_REQUEST_RESULT} with the original requestId
 *       instead of blindly resubmitting {@code BOOK_TICKET}, preventing duplicate ticket bookings.</li>
 *   <li><b>Decoupled Fake Routing:</b> Utilizes {@link LeaderRoutingProvider} allowing standalone testing
 *       without waiting for dynamic Raft election code from TV5.</li>
 * </ul>
 */
public class PeerConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(PeerConnectionManager.class.getName());
    public static final int DEFAULT_MAX_REDIRECTS = 3;

    /**
     * Provider boundary for dynamic or stubbed leader routing information.
     */
    @FunctionalInterface
    public interface LeaderRoutingProvider {
        LeaderInfo getLeaderInfo();

        record LeaderInfo(String leaderId, long term) {}

        static LeaderRoutingProvider fixed(String leaderId, long term) {
            LeaderInfo info = new LeaderInfo(leaderId, term);
            return () -> info;
        }
    }

    private final String localId;
    private final ConcurrentMap<String, InetSocketAddress> peerEndpoints = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectionManager> connections = new ConcurrentHashMap<>();
    private final RetryPolicy retryPolicy;
    private final int maxRedirects;

    private final AtomicReference<String> currentLeaderId = new AtomicReference<>(null);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile LeaderRoutingProvider routingProvider;

    public PeerConnectionManager(String localId, Map<String, InetSocketAddress> peerEndpoints) {
        this(localId, peerEndpoints, RetryPolicy.defaultPolicy(), DEFAULT_MAX_REDIRECTS, null);
    }

    public PeerConnectionManager(String localId, Map<String, InetSocketAddress> peerEndpoints, RetryPolicy retryPolicy) {
        this(localId, peerEndpoints, retryPolicy, DEFAULT_MAX_REDIRECTS, null);
    }

    public PeerConnectionManager(
            String localId,
            Map<String, InetSocketAddress> peerEndpoints,
            RetryPolicy retryPolicy,
            LeaderRoutingProvider routingProvider) {
        this(localId, peerEndpoints, retryPolicy, DEFAULT_MAX_REDIRECTS, routingProvider);
    }

    public PeerConnectionManager(
            String localId,
            Map<String, InetSocketAddress> peerEndpoints,
            RetryPolicy retryPolicy,
            int maxRedirects,
            LeaderRoutingProvider routingProvider) {
        this.localId = Objects.requireNonNull(localId, "localId must not be null");
        this.retryPolicy = (retryPolicy != null) ? retryPolicy : RetryPolicy.defaultPolicy();
        this.maxRedirects = Math.max(1, maxRedirects);
        this.routingProvider = routingProvider;

        if (peerEndpoints != null) {
            this.peerEndpoints.putAll(peerEndpoints);
        }
    }

    // =========================================================================
    // 1. Peer Endpoints Management
    // =========================================================================

    public void setPeerEndpoint(String peerId, InetSocketAddress address) {
        Objects.requireNonNull(peerId, "peerId must not be null");
        Objects.requireNonNull(address, "address must not be null");
        peerEndpoints.put(peerId, address);
    }

    public void setPeerEndpoint(String peerId, String host, int port) {
        setPeerEndpoint(peerId, new InetSocketAddress(host, port));
    }

    public Map<String, InetSocketAddress> getPeerEndpoints() {
        return Collections.unmodifiableMap(peerEndpoints);
    }

    public InetSocketAddress getPeerEndpoint(String peerId) {
        return peerEndpoints.get(peerId);
    }

    // =========================================================================
    // 2. Post-Election Routing Refresh
    // =========================================================================

    /**
     * Refreshes routing upon receiving notification of an election completion.
     * Safely closes the stale connection to the previous leader if changed.
     *
     * @param newLeaderId The newly elected leader ID
     * @param newTerm The new election term
     */
    public void onElectionCompleted(String newLeaderId, long newTerm) {
        Objects.requireNonNull(newLeaderId, "newLeaderId must not be null");
        String oldLeader = currentLeaderId.getAndSet(newLeaderId);
        long oldTerm = currentTerm.getAndSet(newTerm);

        LOGGER.info(() -> String.format(
                "[%s] Leadership transition recorded: oldLeader=%s (term=%d) -> newLeader=%s (term=%d)",
                localId, oldLeader, oldTerm, newLeaderId, newTerm
        ));

        if (oldLeader != null && !oldLeader.equals(newLeaderId)) {
            // Safely close and evict stale connection to old leader
            closeConnection(oldLeader);
        }
    }

    /**
     * Refreshes routing based on a leader hint (e.g. from a NOT_LEADER error response).
     */
    public void refreshRouting(String leaderHint, long term) {
        if (leaderHint == null || leaderHint.isBlank()) {
            return;
        }
        String cleanHint = leaderHint.trim();
        if (peerEndpoints.containsKey(cleanHint)) {
            String old = currentLeaderId.getAndSet(cleanHint);
            if (term > currentTerm.get()) {
                currentTerm.set(term);
            }
            if (old != null && !old.equals(cleanHint)) {
                LOGGER.info(() -> "[" + localId + "] Refreshed leader route via hint: " + old + " -> " + cleanHint);
                closeConnection(old);
            }
        }
    }

    public String getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public void setRoutingProvider(LeaderRoutingProvider provider) {
        this.routingProvider = provider;
    }

    public LeaderRoutingProvider getRoutingProvider() {
        return routingProvider;
    }

    // =========================================================================
    // 3. Connection Pool Management
    // =========================================================================

    /**
     * Retrieves or lazily creates a {@link ConnectionManager} to the specified peer.
     */
    public ConnectionManager getOrCreateConnection(String peerId) throws IOException {
        Objects.requireNonNull(peerId, "peerId must not be null");
        InetSocketAddress address = peerEndpoints.get(peerId);
        if (address == null) {
            throw new IllegalArgumentException("Unknown peerId (no endpoint registered): " + peerId);
        }

        return connections.computeIfAbsent(peerId, id ->
                new ConnectionManager(address.getHostString(), address.getPort(), retryPolicy));
    }

    /**
     * Closes and evicts a specific peer connection.
     */
    public void closeConnection(String peerId) {
        if (peerId == null) return;
        ConnectionManager conn = connections.remove(peerId);
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignored) {}
            LOGGER.info(() -> "[" + localId + "] Closed connection to peer: " + peerId);
        }
    }

    public boolean hasActiveConnection(String peerId) {
        if (peerId == null) return false;
        ConnectionManager conn = connections.get(peerId);
        return conn != null && conn.isConnected();
    }

    public int getActiveConnectionCount() {
        int count = 0;
        for (ConnectionManager conn : connections.values()) {
            if (conn.isConnected()) {
                count++;
            }
        }
        return count;
    }

    // =========================================================================
    // 4. Leader Discovery & Communication with Automatic Redirection
    // =========================================================================

    /**
     * Discovers the current leader using routing provider or cached leader.
     */
    public String getOrDiscoverLeader() throws IOException {
        String leader = currentLeaderId.get();
        if (leader != null && peerEndpoints.containsKey(leader)) {
            return leader;
        }

        if (routingProvider != null) {
            LeaderRoutingProvider.LeaderInfo info = routingProvider.getLeaderInfo();
            if (info != null && info.leaderId() != null && peerEndpoints.containsKey(info.leaderId())) {
                onElectionCompleted(info.leaderId(), info.term());
                return info.leaderId();
            }
        }

        // Fallback: Pick first known peer if no leader is established
        if (!peerEndpoints.isEmpty()) {
            return peerEndpoints.keySet().iterator().next();
        }

        throw new IOException("No known leader or peer endpoints available");
    }

    /**
     * Sends a request to the currently recognized leader, automatically following
     * {@code NOT_LEADER} redirections up to {@code maxRedirects}.
     *
     * <p>CRITICAL: Generates a fresh transport {@code messageId} on each redirect step,
     * while strictly preserving the inner business {@code requestId} intact.
     */
    public <T> MessageEnvelope<T> sendToLeader(MessageEnvelope<?> request, Class<T> responsePayloadClass) throws IOException {
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(responsePayloadClass, "responsePayloadClass must not be null");

        String originalRequestId = ErrorReplyFactory.extractRequestId(request);
        String targetPeer = getOrDiscoverLeader();
        Set<String> visitedPeers = new HashSet<>();
        int redirectCount = 0;

        MessageEnvelope<?> currentRequest = request;

        while (true) {
            visitedPeers.add(targetPeer);
            ConnectionManager conn = getOrCreateConnection(targetPeer);

            MessageEnvelope<JsonNode> rawResp;
            try {
                rawResp = conn.sendAndReceive(currentRequest, JsonNode.class);
            } catch (IOException ioEx) {
                // Connection or read timeout: mark target as potentially dead
                LOGGER.log(Level.WARNING, "[" + localId + "] Connection to leader " + targetPeer
                        + " failed (" + ioEx.getMessage() + "); invalidating connection");
                closeConnection(targetPeer);
                if (targetPeer.equals(currentLeaderId.get())) {
                    currentLeaderId.set(null);
                }
                throw ioEx;
            }

            if (rawResp == null) {
                throw new IOException("Null response received from " + targetPeer);
            }

            // Check if response is NOT_LEADER redirection
            if (rawResp.getType() == MessageType.ERROR_RESPONSE && rawResp.getPayload() != null) {
                ErrorResponseDto errorDto;
                try {
                    errorDto = MessageCodec.getMapper().treeToValue(rawResp.getPayload(), ErrorResponseDto.class);
                } catch (Exception ex) {
                    errorDto = null;
                }

                if (errorDto != null && "NOT_LEADER".equals(errorDto.getErrorCode())) {
                    String leaderHint = errorDto.getLeaderHint();
                    Long term = errorDto.getTerm();
                    final String fromPeer = targetPeer;

                    LOGGER.info(() -> String.format(
                            "[%s] Received NOT_LEADER from %s (hint=%s, term=%s)",
                            localId, fromPeer, leaderHint, term
                    ));

                    if (leaderHint != null && peerEndpoints.containsKey(leaderHint)
                            && !visitedPeers.contains(leaderHint)
                            && redirectCount < maxRedirects) {

                        redirectCount++;
                        refreshRouting(leaderHint, term != null ? term : currentTerm.get());
                        targetPeer = leaderHint;

                        // Create next hop envelope with fresh transport messageId, preserving payload & requestId
                        currentRequest = MessageEnvelope.createRequest(
                                request.getType(),
                                localId,
                                request.getPayload()
                        );
                        continue;
                    }
                }
            }

            // Success or terminal response: convert payload to requested class
            if (responsePayloadClass == JsonNode.class) {
                @SuppressWarnings("unchecked")
                MessageEnvelope<T> cast = (MessageEnvelope<T>) rawResp;
                return cast;
            }

            T typedPayload = null;
            if (rawResp.getPayload() != null) {
                typedPayload = MessageCodec.getMapper().treeToValue(rawResp.getPayload(), responsePayloadClass);
            }

            return new MessageEnvelope<>(
                    rawResp.getProtocolVersion(),
                    rawResp.getMessageId(),
                    rawResp.getCorrelationId(),
                    rawResp.getType(),
                    rawResp.getSenderId(),
                    rawResp.getSentAt(),
                    typedPayload
            );
        }
    }

    // =========================================================================
    // 5. State Uncertainty Resolution via GET_REQUEST_RESULT
    // =========================================================================

    /**
     * Queries the durable execution outcome of a previously submitted command via {@code GET_REQUEST_RESULT}
     * (SPEC-03 §4.4).
     *
     * @param requestId Original business request UUID
     * @return Deterministic {@link ResultLookup.LookupResult}
     */
    public ResultLookup.LookupResult queryRequestResult(String requestId) throws IOException {
        Objects.requireNonNull(requestId, "requestId must not be null");
        GetRequestResultRequestDto queryDto = new GetRequestResultRequestDto(requestId);
        queryDto.validate();

        MessageEnvelope<GetRequestResultRequestDto> reqEnv = MessageEnvelope.createRequest(
                MessageType.GET_REQUEST_RESULT,
                localId,
                queryDto
        );

        MessageEnvelope<JsonNode> respEnv;
        try {
            respEnv = sendToLeader(reqEnv, JsonNode.class);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[" + localId + "] GET_REQUEST_RESULT query failed for " + requestId + ": " + ex.getMessage());
            return ResultLookup.LookupResult.unknown(requestId);
        }

        if (respEnv == null || respEnv.getPayload() == null) {
            return ResultLookup.LookupResult.unknown(requestId);
        }

        if (respEnv.getType() == MessageType.GET_REQUEST_RESULT_RESPONSE) {
            GetRequestResultResponseDto dto = MessageCodec.getMapper()
                    .treeToValue(respEnv.getPayload(), GetRequestResultResponseDto.class);
            dto.validate();

            String state = dto.getState();
            if (GetRequestResultResponseDto.STATE_APPLIED_SUCCESS.equals(state)) {
                return ResultLookup.LookupResult.appliedSuccess(
                        requestId,
                        dto.getCommandType(),
                        dto.getTicketId(),
                        dto.getAppliedLogIndex() != null ? dto.getAppliedLogIndex() : 0L
                );
            } else if (GetRequestResultResponseDto.STATE_APPLIED_REJECTED.equals(state)) {
                return ResultLookup.LookupResult.appliedRejected(
                        requestId,
                        dto.getCommandType(),
                        dto.getReasonCode(),
                        dto.getAppliedLogIndex() != null ? dto.getAppliedLogIndex() : 0L
                );
            } else if (GetRequestResultResponseDto.STATE_PENDING.equals(state)) {
                return ResultLookup.LookupResult.pending(requestId, dto.getCommandType());
            } else {
                return ResultLookup.LookupResult.unknown(requestId);
            }
        }

        return ResultLookup.LookupResult.unknown(requestId);
    }

    // =========================================================================
    // 6. Safe Booking Execution with Leader Failover (requestId Invariance)
    // =========================================================================

    /**
     * Executes a booking request safely across leader failure and re-election.
     *
     * <p>SAFETY INVARIANT: If the original leader crashes or times out before returning
     * a response, this method does NOT issue a new {@code BOOK_TICKET} request!
     * Instead, it queries {@code GET_REQUEST_RESULT} with the exact original {@code requestId}.
     * Only if the query confirms the request was never applied does it resubmit with the same {@code requestId}.
     *
     * @param request The booking request payload
     * @return Deterministic {@link BookTicketResponseDto}
     */
    public BookTicketResponseDto executeBookingWithLeaderFailover(BookTicketRequestDto request) throws IOException {
        Objects.requireNonNull(request, "request must not be null");
        request.validate();
        String requestId = request.getRequestId();

        MessageEnvelope<BookTicketRequestDto> reqEnv = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET,
                localId,
                request
        );

        boolean transportFailed = false;
        try {
            MessageEnvelope<JsonNode> respEnv = sendToLeader(reqEnv, JsonNode.class);
            if (respEnv != null && respEnv.getPayload() != null) {
                if (respEnv.getType() == MessageType.BOOK_TICKET_RESPONSE) {
                    BookTicketResponseDto bookResp = MessageCodec.getMapper()
                            .treeToValue(respEnv.getPayload(), BookTicketResponseDto.class);
                    bookResp.validate();
                    return bookResp;
                } else if (respEnv.getType() == MessageType.ERROR_RESPONSE) {
                    ErrorResponseDto err = MessageCodec.getMapper()
                            .treeToValue(respEnv.getPayload(), ErrorResponseDto.class);
                    if ("SEAT_ALREADY_BOOKED".equals(err.getErrorCode()) || "INVALID_REQUEST".equals(err.getErrorCode())) {
                        return new BookTicketResponseDto(
                                requestId,
                                "REJECTED",
                                null,
                                request.getTripId(),
                                request.getSeatId(),
                                err.getErrorCode(),
                                null
                        );
                    } else {
                        // Transient/recoverable server error (INTERNAL_ERROR, NO_QUORUM, NOT_LEADER) -> treat as transport uncertainty
                        transportFailed = true;
                    }
                }
            } else {
                transportFailed = true;
            }
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[" + localId + "] Initial BOOK_TICKET delivery failed for requestId="
                    + requestId + ": " + ex.getMessage());
            transportFailed = true;
        }

        // =====================================================================
        // STATE UNCERTAINTY (UNKNOWN) -> DO NOT BLINDLY RESUBMIT BOOK_TICKET!
        // =====================================================================
        LOGGER.info(() -> "[" + localId + "] RequestId=" + requestId + " outcome is uncertain; querying GET_REQUEST_RESULT...");

        // If leader connection dropped, synchronize with routing provider if available
        if (currentLeaderId.get() == null && routingProvider != null) {
            LeaderRoutingProvider.LeaderInfo info = routingProvider.getLeaderInfo();
            if (info != null && info.leaderId() != null && peerEndpoints.containsKey(info.leaderId())) {
                onElectionCompleted(info.leaderId(), info.term());
            }
        }

        ResultLookup.LookupResult lookup = queryRequestResult(requestId);
        if (lookup.isSuccess()) {
            LOGGER.info(() -> "[" + localId + "] RequestId=" + requestId + " was already committed on new leader; returning CONFIRMED (ticketId=" + lookup.ticketId() + ")");
            return new BookTicketResponseDto(
                    requestId,
                    "CONFIRMED",
                    lookup.ticketId(),
                    request.getTripId(),
                    request.getSeatId(),
                    null,
                    lookup.appliedLogIndex()
            );
        } else if (lookup.isRejected()) {
            LOGGER.info(() -> "[" + localId + "] RequestId=" + requestId + " was rejected on cluster; returning REJECTED (reasonCode=" + lookup.reasonCode() + ")");
            return new BookTicketResponseDto(
                    requestId,
                    "REJECTED",
                    null,
                    request.getTripId(),
                    request.getSeatId(),
                    lookup.reasonCode(),
                    lookup.appliedLogIndex()
            );
        }

        // If UNKNOWN, the request never reached consensus before crash.
        // Resubmit BOOK_TICKET to the new Leader using the EXACT SAME requestId!
        LOGGER.info(() -> "[" + localId + "] RequestId=" + requestId + " not found on new leader (UNKNOWN); resubmitting with same requestId...");
        MessageEnvelope<BookTicketRequestDto> retryEnv = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET,
                localId,
                request
        );

        MessageEnvelope<JsonNode> retryResp;
        try {
            retryResp = sendToLeader(retryEnv, JsonNode.class);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[" + localId + "] Retry BOOK_TICKET failed: " + ex.getMessage());
            return new BookTicketResponseDto(
                    requestId,
                    "UNKNOWN",
                    null,
                    request.getTripId(),
                    request.getSeatId(),
                    "TIMEOUT_UNKNOWN",
                    null
            );
        }

        if (retryResp != null && retryResp.getPayload() != null) {
            if (retryResp.getType() == MessageType.BOOK_TICKET_RESPONSE) {
                BookTicketResponseDto bookResp = MessageCodec.getMapper()
                        .treeToValue(retryResp.getPayload(), BookTicketResponseDto.class);
                bookResp.validate();
                return bookResp;
            } else if (retryResp.getType() == MessageType.ERROR_RESPONSE) {
                ErrorResponseDto err = MessageCodec.getMapper()
                        .treeToValue(retryResp.getPayload(), ErrorResponseDto.class);
                String reason = "SEAT_ALREADY_BOOKED".equals(err.getErrorCode())
                        ? "SEAT_ALREADY_BOOKED" : "INVALID_REQUEST";
                return new BookTicketResponseDto(
                        requestId,
                        "REJECTED",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        reason,
                        null
                );
            }
        }

        return new BookTicketResponseDto(
                requestId,
                "UNKNOWN",
                null,
                request.getTripId(),
                request.getSeatId(),
                "TIMEOUT_UNKNOWN",
                null
        );
    }

    @Override
    public void close() {
        for (String peerId : new ArrayList<>(connections.keySet())) {
            closeConnection(peerId);
        }
        connections.clear();
        LOGGER.info(() -> "[" + localId + "] PeerConnectionManager closed.");
    }
}
