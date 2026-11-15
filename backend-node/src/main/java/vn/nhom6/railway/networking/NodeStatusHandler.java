package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.election.RaftElectionComponent;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Handler for diagnostic {@link MessageType#NODE_STATUS} requests from Monitor or Clients (TASK TV3-D15).
 *
 * <p>Key Guarantees & Constraints (SPEC-02 §5, SPEC-03 §5):
 * <ul>
 *   <li><b>Local Observation:</b> This snapshot reflects only the local node's perspective at
 *       {@code observedAt}. It may lag behind cluster consensus and must not be used to confirm writes.</li>
 *   <li><b>Truthful Diagnostic:</b> If consensus or storage is not initialized, {@code role} and {@code term}
 *       are {@code null}. The handler never fabricates fake states (e.g. fake {@code FOLLOWER}/term 0) to hide faults.</li>
 *   <li><b>Peer Connectivity:</b> Populates exactly 4 peer entries for a 5-node cluster (excluding self),
 *       where {@code true} = last RPC succeeded, {@code false} = failed/timeout, {@code null} = unobserved.</li>
 * </ul>
 */
public class NodeStatusHandler implements HandlerRegistry.RequestHandler, HandlerRegistry.StatusHandler {

    private static final Logger LOGGER = Logger.getLogger(NodeStatusHandler.class.getName());
    public static final List<String> ALL_FIVE_NODES = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    /**
     * Functional interface supplying state observations to this handler.
     */
    @FunctionalInterface
    public interface StatusProvider {
        StatusSnapshot getStatus();

        /**
         * Adapter extracting state directly from TV4-D01 {@link RaftState}.
         */
        static StatusProvider fromRaftState(
                String nodeId,
                RaftState raftState,
                boolean storageHealthy,
                Map<String, Boolean> peerConnectivity) {
            Objects.requireNonNull(nodeId, "nodeId must not be null");
            Objects.requireNonNull(raftState, "raftState must not be null");
            return () -> new StatusSnapshot(
                    raftState.role() != null ? raftState.role().name() : null,
                    raftState.currentTerm(),
                    raftState.leaderId(),
                    raftState.lastLogIndex(),
                    raftState.commitIndex(),
                    raftState.lastApplied(),
                    storageHealthy,
                    peerConnectivity
            );
        }

        /**
         * Adapter extracting state from TV5 {@link RaftElectionComponent}.
         */
        static StatusProvider fromElectionComponent(
                String nodeId,
                RaftElectionComponent election,
                Map<String, Boolean> peerConnectivity) {
            Objects.requireNonNull(nodeId, "nodeId must not be null");
            Objects.requireNonNull(election, "election must not be null");
            return () -> {
                try {
                    RaftElectionComponent.Status s = election.status();
                    return new StatusSnapshot(
                            s.role(),
                            s.currentTerm(),
                            null,
                            null,
                            s.commitIndex(),
                            s.lastApplied(),
                            s.storageHealthy(),
                            peerConnectivity
                    );
                } catch (Exception ex) {
                    return new StatusSnapshot(null, null, null, null, null, null, false, peerConnectivity);
                }
            };
        }

        /**
         * Static helper for mock or fixed testing snapshots.
         */
        static StatusProvider staticSnapshot(StatusSnapshot snapshot) {
            Objects.requireNonNull(snapshot, "snapshot must not be null");
            return () -> snapshot;
        }
    }

    /**
     * Immutable snapshot model capturing the node's local diagnostic observation.
     */
    public record StatusSnapshot(
            String role,
            Long currentTerm,
            String leaderId,
            Long lastLogIndex,
            Long commitIndex,
            Long lastApplied,
            boolean storageHealthy,
            Map<String, Boolean> peerConnectivity
    ) {
        public static StatusSnapshot uninitialized(String nodeId) {
            return new StatusSnapshot(null, null, null, null, null, null, false, defaultPeers(nodeId));
        }

        public static Map<String, Boolean> defaultPeers(String selfNodeId) {
            Map<String, Boolean> map = new LinkedHashMap<>();
            for (String id : ALL_FIVE_NODES) {
                if (!id.equals(selfNodeId)) {
                    map.put(id, false);
                }
            }
            return Collections.unmodifiableMap(map);
        }
    }

    private final String nodeId;
    private final StatusProvider statusProvider;

    public NodeStatusHandler(String nodeId, StatusProvider statusProvider) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.statusProvider = Objects.requireNonNull(statusProvider, "statusProvider must not be null");
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return null;
        }

        StatusSnapshot snapshot = statusProvider.getStatus();
        if (snapshot == null) {
            snapshot = StatusSnapshot.uninitialized(nodeId);
        }

        // Build 4 peer connectivity map (excluding self)
        Map<String, Boolean> connectivity = buildPeerConnectivity(snapshot.peerConnectivity());

        String observedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();

        NodeStatusDto statusDto = new NodeStatusDto(
                nodeId,
                snapshot.role(),
                snapshot.currentTerm(),
                snapshot.leaderId(),
                snapshot.lastLogIndex(),
                snapshot.commitIndex(),
                snapshot.lastApplied(),
                snapshot.storageHealthy(),
                connectivity,
                observedAt
        );

        statusDto.validate();

        LOGGER.fine(() -> "[" + nodeId + "] Serving NODE_STATUS_RESPONSE at " + observedAt
                + " (role=" + statusDto.getRole() + ", term=" + statusDto.getTerm() + ")");

        return MessageEnvelope.createResponse(
                request.getMessageId(),
                MessageType.NODE_STATUS_RESPONSE,
                nodeId,
                statusDto
        );
    }

    @Override
    public MessageEnvelope<?> handleStatus(MessageEnvelope<JsonNode> request) throws Exception {
        return handle(request);
    }

    private Map<String, Boolean> buildPeerConnectivity(Map<String, Boolean> raw) {
        Map<String, Boolean> result = new LinkedHashMap<>();
        for (String peerId : ALL_FIVE_NODES) {
            if (!peerId.equals(nodeId)) {
                Boolean state = (raw != null) ? raw.get(peerId) : null;
                result.put(peerId, state != null ? state : false);
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
