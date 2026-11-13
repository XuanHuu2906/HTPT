package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.RaftElectionComponent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Router and director for client requests requiring Raft Leader authority (TASK TV3-D13).
 *
 * <p>Key Guarantees & Constraints:
 * <ul>
 *   <li><b>SPEC-02 §4 Redirection:</b> When this node is not the Leader, returns {@code NOT_LEADER} error
 *       with {@code leaderHint} and current {@code term}.</li>
 *   <li><b>Fallback Node List:</b> When {@code leaderHint} is unknown (null or pointing to self), automatically
 *       falls back to a known cluster voter from the configured node list (excluding {@code localNodeId}).</li>
 *   <li><b>Strict Loop Avoidance:</b>
 *     <ul>
 *       <li>Never self-hints: {@code leaderHint} can never equal {@code localNodeId} on a non-leader.</li>
 *       <li>Forwarding loop prevention: Forwarding is limited to at most 1 hop (rejects if sender is a peer voter
 *           or if {@code requestId} has already been forwarded by this node).</li>
 *     </ul>
 *   </li>
 *   <li><b>RequestId Invariance:</b> The client's business {@code requestId} is strictly preserved across
 *       all redirect responses and forwarded envelopes.</li>
 *   <li><b>Supported Modes:</b> Defaults to {@link Mode#REDIRECT} (APPROVED-v1 per SPEC-02 §4); also supports
 *       transparent server-side proxying via {@link Mode#FORWARD}.</li>
 * </ul>
 */
public class LeaderRouter {

    private static final Logger LOGGER = Logger.getLogger(LeaderRouter.class.getName());
    public static final List<String> DEFAULT_FIVE_VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    /**
     * Routing mode when the local node is not the Leader.
     */
    public enum Mode {
        /**
         * Standard APPROVED-v1 behavior: return NOT_LEADER with leaderHint and term for client to redirect.
         */
        REDIRECT,
        /**
         * Transparent proxy mode: forward the request to the known leader once and return result to client.
         */
        FORWARD
    }

    /**
     * Snapshot of the current consensus role, term, and leader hint.
     */
    public record RoleSnapshot(
            Role role,
            long term,
            String leaderHint
    ) {}

    /**
     * Functional provider boundary for obtaining current role state.
     */
    @FunctionalInterface
    public interface RoleProvider {
        RoleSnapshot getRole();

        static RoleProvider fixed(Role role, long term, String leaderHint) {
            RoleSnapshot snapshot = new RoleSnapshot(role, term, leaderHint);
            return () -> snapshot;
        }

        static RoleProvider fromBookingService(BookingService bookingService) {
            Objects.requireNonNull(bookingService, "bookingService must not be null");
            return () -> new RoleSnapshot(
                    bookingService.isLeader() ? Role.LEADER : Role.FOLLOWER,
                    0L,
                    bookingService.getLeaderHint()
            );
        }

        static RoleProvider fromRaftNode(RaftNode raftNode) {
            Objects.requireNonNull(raftNode, "raftNode must not be null");
            return () -> new RoleSnapshot(
                    raftNode.role(),
                    raftNode.currentTerm(),
                    raftNode.leaderId().orElse(null)
            );
        }

        static RoleProvider fromElectionComponent(RaftElectionComponent election) {
            Objects.requireNonNull(election, "election must not be null");
            return () -> {
                try {
                    RaftElectionComponent.Status s = election.status();
                    Role r = Role.valueOf(s.role());
                    return new RoleSnapshot(r, s.currentTerm(), null);
                } catch (Exception ex) {
                    return new RoleSnapshot(Role.FOLLOWER, 0L, null);
                }
            };
        }
    }

    /**
     * Forwarder callback for forwarding request to a target leader node.
     */
    @FunctionalInterface
    public interface RequestForwarder {
        MessageEnvelope<?> forward(String targetNodeId, MessageEnvelope<JsonNode> request) throws Exception;
    }

    private final String localNodeId;
    private final RoleProvider roleProvider;
    private final List<String> clusterNodes;
    private final List<String> fallbackNodes;
    private final AtomicInteger fallbackCounter = new AtomicInteger(0);

    private volatile Mode mode = Mode.REDIRECT;
    private volatile RequestForwarder forwarder;

    // Bounded LRU cache of forwarded requestIds to prevent forward loops (max 5000 items)
    private final Set<String> forwardedRequestIds = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > 5000;
                }
            })
    );

    public LeaderRouter(String localNodeId, RoleProvider roleProvider) {
        this(localNodeId, roleProvider, DEFAULT_FIVE_VOTERS, Mode.REDIRECT, null);
    }

    public LeaderRouter(String localNodeId, RoleProvider roleProvider, List<String> clusterNodes) {
        this(localNodeId, roleProvider, clusterNodes, Mode.REDIRECT, null);
    }

    public LeaderRouter(
            String localNodeId,
            RoleProvider roleProvider,
            List<String> clusterNodes,
            Mode mode,
            RequestForwarder forwarder) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
        this.roleProvider = Objects.requireNonNull(roleProvider, "roleProvider must not be null");
        List<String> nodes = (clusterNodes != null && !clusterNodes.isEmpty())
                ? List.copyOf(clusterNodes)
                : DEFAULT_FIVE_VOTERS;
        this.clusterNodes = nodes;
        // Fallback list includes all configured cluster nodes EXCEPT localNodeId
        this.fallbackNodes = nodes.stream()
                .filter(id -> !id.equals(localNodeId))
                .toList();
        this.mode = (mode != null) ? mode : Mode.REDIRECT;
        this.forwarder = forwarder;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = Objects.requireNonNull(mode, "mode must not be null");
    }

    public RequestForwarder getForwarder() {
        return forwarder;
    }

    public void setForwarder(RequestForwarder forwarder) {
        this.forwarder = forwarder;
    }

    public String getLocalNodeId() {
        return localNodeId;
    }

    public List<String> getClusterNodes() {
        return clusterNodes;
    }

    public List<String> getFallbackNodes() {
        return fallbackNodes;
    }

    /**
     * Resolves the authoritative or best-effort leader hint.
     *
     * <p>Rules:
     * <ol>
     *   <li>If {@code leaderHint} from consensus state is non-null, valid, and not {@code localNodeId}, returns it.</li>
     *   <li>If {@code leaderHint} is null, blank, not in cluster, or self-referential (equals {@code localNodeId}),
     *       rotates to the next available fallback candidate from {@link #fallbackNodes}.</li>
     *   <li>Guarantees {@code localNodeId} is NEVER returned as hint when node is not leader.</li>
     * </ol>
     *
     * @return Resolved peer node ID hint
     */
    public String resolveLeaderHint() {
        RoleSnapshot snapshot = roleProvider.getRole();
        String hint = snapshot.leaderHint();

        // 1. Direct valid hint (must not be self)
        if (hint != null && !hint.isBlank() && !hint.equals(localNodeId) && clusterNodes.contains(hint)) {
            return hint;
        }

        // 2. Select next fallback candidate from cluster nodes
        return selectFallbackNode();
    }

    /**
     * Selects the next candidate node from the fallback list (excluding self).
     */
    public String selectFallbackNode() {
        if (fallbackNodes.isEmpty()) {
            return null;
        }
        int idx = Math.abs(fallbackCounter.getAndIncrement() % fallbackNodes.size());
        return fallbackNodes.get(idx);
    }

    /**
     * Main routing entry point for incoming client requests.
     *
     * @param request Incoming request envelope
     * @return {@link RoutingOutcome} indicating whether to execute locally or containing the handled response
     * @throws Exception If unexpected forward failure occurs
     */
    public RoutingOutcome route(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return RoutingOutcome.unhandled();
        }

        RoleSnapshot snapshot = roleProvider.getRole();
        if (snapshot.role() == Role.LEADER) {
            // Local node is leader: proceed to local business execution
            return RoutingOutcome.executeLocally();
        }

        // Local node is FOLLOWER or CANDIDATE: write requests require leader redirection or forward
        String requestId = ErrorReplyFactory.extractRequestId(request);
        String messageId = request.getMessageId();
        String leaderHint = resolveLeaderHint();
        long term = snapshot.term();

        // 1. Check FORWARD Mode
        if (mode == Mode.FORWARD && forwarder != null && leaderHint != null) {
            if (isForwardLoop(request, requestId)) {
                LOGGER.warning(() -> "[" + localNodeId + "] Forward loop detected for requestId=" + requestId
                        + ", senderId=" + request.getSenderId() + ". Halting forward and rejecting as NOT_LEADER.");
                MessageEnvelope<ErrorResponseDto> loopReject = ErrorReplyFactory.createNotLeader(
                        messageId, requestId, localNodeId, leaderHint, term
                );
                return RoutingOutcome.handled(loopReject);
            }

            try {
                if (requestId != null) {
                    forwardedRequestIds.add(requestId);
                }
                LOGGER.info(() -> "[" + localNodeId + "] Forwarding request messageId=" + messageId
                        + ", requestId=" + requestId + " to leader " + leaderHint);
                MessageEnvelope<?> forwardedResponse = forwarder.forward(leaderHint, request);
                return RoutingOutcome.handled(forwardedResponse);
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING, "[" + localNodeId + "] Forwarding to " + leaderHint + " failed: "
                        + ex.getMessage() + ", falling back to NOT_LEADER reply");
            }
        }

        // 2. REDIRECT Mode (or fallback on forward failure)
        LOGGER.info(() -> "[" + localNodeId + "] Rejecting request as NOT_LEADER (role=" + snapshot.role()
                + ", term=" + term + ", hint=" + leaderHint + ")");
        MessageEnvelope<ErrorResponseDto> notLeaderResp = ErrorReplyFactory.createNotLeader(
                messageId, requestId, localNodeId, leaderHint, term
        );
        return RoutingOutcome.handled(notLeaderResp);
    }

    /**
     * Loop detection logic for forwarding mode.
     */
    private boolean isForwardLoop(MessageEnvelope<JsonNode> request, String requestId) {
        // Condition A: Request originated from a peer cluster node (already hopped once)
        if (request.getSenderId() != null && clusterNodes.contains(request.getSenderId())) {
            return true;
        }
        // Condition B: Request with this business requestId was already forwarded by this node
        if (requestId != null && forwardedRequestIds.contains(requestId)) {
            return true;
        }
        return false;
    }

    /**
     * Routing outcome encapsulation.
     */
    public static final class RoutingOutcome {
        private final boolean executeLocally;
        private final MessageEnvelope<?> response;

        private RoutingOutcome(boolean executeLocally, MessageEnvelope<?> response) {
            this.executeLocally = executeLocally;
            this.response = response;
        }

        public static RoutingOutcome executeLocally() {
            return new RoutingOutcome(true, null);
        }

        public static RoutingOutcome handled(MessageEnvelope<?> response) {
            return new RoutingOutcome(false, response);
        }

        public static RoutingOutcome unhandled() {
            return new RoutingOutcome(false, null);
        }

        public boolean shouldExecuteLocally() {
            return executeLocally;
        }

        public boolean isHandled() {
            return !executeLocally && response != null;
        }

        public MessageEnvelope<?> getResponse() {
            return response;
        }
    }
}
