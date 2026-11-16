package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.raft.core.RaftState;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NodeStatusHandler Unit Tests (TV3-D15)")
public class NodeStatusHandlerTest {

    private MessageEnvelope<JsonNode> createStatusRequest(String senderId) throws Exception {
        MessageEnvelope<?> req = MessageEnvelope.createRequest(MessageType.NODE_STATUS, senderId, Map.of());
        byte[] bytes = MessageCodec.encode(req);
        return MessageCodec.decode(bytes);
    }

    @Test
    @DisplayName("Should return full status snapshot for LEADER with peer connectivity")
    public void testLeaderStatusSnapshot() throws Exception {
        Map<String, Boolean> peers = new LinkedHashMap<>();
        peers.put("node-2", true);
        peers.put("node-3", true);
        peers.put("node-4", false);
        peers.put("node-5", true);

        NodeStatusHandler.StatusSnapshot snapshot = new NodeStatusHandler.StatusSnapshot(
                "LEADER",
                2L,
                "node-1",
                15L,
                12L,
                12L,
                true,
                peers
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-1", () -> snapshot);
        MessageEnvelope<JsonNode> request = createStatusRequest("monitor-agent");

        MessageEnvelope<?> responseEnv = handler.handle(request);
        assertNotNull(responseEnv);
        assertEquals(MessageType.NODE_STATUS_RESPONSE, responseEnv.getType());
        assertEquals("node-1", responseEnv.getSenderId());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());

        assertTrue(responseEnv.getPayload() instanceof NodeStatusDto);
        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        dto.validate();

        assertEquals("node-1", dto.getNodeId());
        assertEquals("LEADER", dto.getRole());
        assertTrue(dto.isLeader());
        assertFalse(dto.isFollower());
        assertFalse(dto.isCandidate());
        assertEquals(2L, dto.getTerm());
        assertEquals("node-1", dto.getLeaderId());
        assertEquals(15L, dto.getLastLogIndex());
        assertEquals(12L, dto.getCommitIndex());
        assertEquals(12L, dto.getLastApplied());
        assertTrue(dto.isStorageHealthy());

        assertNotNull(dto.getObservedAt());
        assertDoesNotThrow(() -> Instant.parse(dto.getObservedAt()));

        Map<String, Boolean> connectivity = dto.getPeerConnectivity();
        assertNotNull(connectivity);
        assertEquals(4, connectivity.size());
        assertFalse(connectivity.containsKey("node-1"), "Must not contain self node in peerConnectivity");
        assertEquals(Boolean.TRUE, connectivity.get("node-2"));
        assertEquals(Boolean.TRUE, connectivity.get("node-3"));
        assertEquals(Boolean.FALSE, connectivity.get("node-4"));
        assertEquals(Boolean.TRUE, connectivity.get("node-5"));
    }

    @Test
    @DisplayName("Should return FOLLOWER status snapshot with leader hint")
    public void testFollowerStatusSnapshot() throws Exception {
        Map<String, Boolean> peers = new LinkedHashMap<>();
        peers.put("node-1", true);
        peers.put("node-3", true);
        peers.put("node-4", true);
        peers.put("node-5", false);

        NodeStatusHandler.StatusSnapshot snapshot = new NodeStatusHandler.StatusSnapshot(
                "FOLLOWER",
                3L,
                "node-1",
                25L,
                24L,
                24L,
                true,
                peers
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-2", () -> snapshot);
        MessageEnvelope<JsonNode> request = createStatusRequest("client-app");

        MessageEnvelope<?> responseEnv = handler.handle(request);
        assertNotNull(responseEnv);
        assertEquals(MessageType.NODE_STATUS_RESPONSE, responseEnv.getType());
        assertEquals("node-2", responseEnv.getSenderId());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());

        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        dto.validate();

        assertEquals("node-2", dto.getNodeId());
        assertEquals("FOLLOWER", dto.getRole());
        assertTrue(dto.isFollower());
        assertFalse(dto.isLeader());
        assertEquals("node-1", dto.getLeaderId());
        assertEquals(3L, dto.getTerm());
        assertEquals(25L, dto.getLastLogIndex());
        assertEquals(24L, dto.getCommitIndex());
        assertEquals(24L, dto.getLastApplied());
        assertTrue(dto.isStorageHealthy());

        Map<String, Boolean> connectivity = dto.getPeerConnectivity();
        assertEquals(4, connectivity.size());
        assertFalse(connectivity.containsKey("node-2"));
        assertTrue(connectivity.containsKey("node-1"));
    }

    @Test
    @DisplayName("Should handle uninitialized consensus truthfully without fabricating fake states")
    public void testUninitializedConsensus() throws Exception {
        NodeStatusHandler handler = new NodeStatusHandler("node-3",
                () -> NodeStatusHandler.StatusSnapshot.uninitialized("node-3"));

        MessageEnvelope<JsonNode> request = createStatusRequest("monitor-probe");
        MessageEnvelope<?> responseEnv = handler.handle(request);

        assertNotNull(responseEnv);
        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        dto.validate();

        assertEquals("node-3", dto.getNodeId());
        assertNull(dto.getRole());
        assertNull(dto.getTerm());
        assertNull(dto.getLeaderId());
        assertNull(dto.getLastLogIndex());
        assertNull(dto.getCommitIndex());
        assertNull(dto.getLastApplied());
        assertFalse(dto.isStorageHealthy());

        Map<String, Boolean> peers = dto.getPeerConnectivity();
        assertEquals(4, peers.size());
        assertFalse(peers.containsKey("node-3"));
        assertEquals(Boolean.FALSE, peers.get("node-1"));
        assertEquals(Boolean.FALSE, peers.get("node-2"));
        assertEquals(Boolean.FALSE, peers.get("node-4"));
        assertEquals(Boolean.FALSE, peers.get("node-5"));
    }

    @Test
    @DisplayName("Should handle CANDIDATE role with unobserved/missing peer keys gracefully")
    public void testCandidateRoleWithIncompletePeerMap() throws Exception {
        // Only provide partial map; handler must populate missing peers as false
        Map<String, Boolean> partialPeers = Map.of("node-1", true);

        NodeStatusHandler.StatusSnapshot snapshot = new NodeStatusHandler.StatusSnapshot(
                "CANDIDATE",
                5L,
                null,
                10L,
                8L,
                8L,
                true,
                partialPeers
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-4", () -> snapshot);
        MessageEnvelope<JsonNode> request = createStatusRequest("monitor-1");
        MessageEnvelope<?> responseEnv = handler.handle(request);

        assertNotNull(responseEnv);
        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        dto.validate();

        assertEquals("node-4", dto.getNodeId());
        assertEquals("CANDIDATE", dto.getRole());
        assertTrue(dto.isCandidate());
        assertNull(dto.getLeaderId());

        Map<String, Boolean> peers = dto.getPeerConnectivity();
        assertEquals(4, peers.size());
        assertEquals(Boolean.TRUE, peers.get("node-1"));
        assertEquals(Boolean.FALSE, peers.get("node-2"));
        assertEquals(Boolean.FALSE, peers.get("node-3"));
        assertEquals(Boolean.FALSE, peers.get("node-5"));
    }

    @Test
    @DisplayName("Should reflect storage unhealthy condition")
    public void testStorageUnhealthy() throws Exception {
        NodeStatusHandler.StatusSnapshot snapshot = new NodeStatusHandler.StatusSnapshot(
                "FOLLOWER",
                1L,
                null,
                5L,
                5L,
                5L,
                false, // storage failed
                Collections.emptyMap()
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-5", () -> snapshot);
        MessageEnvelope<?> responseEnv = handler.handle(createStatusRequest("monitor-diag"));

        assertNotNull(responseEnv);
        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        assertFalse(dto.isStorageHealthy());
    }

    @Test
    @DisplayName("Should extract status correctly via StatusProvider.fromRaftState adapter")
    public void testFromRaftStateAdapter() throws Exception {
        RaftState raftState = new RaftState();
        raftState.startElection("node-1");
        raftState.becomeLeader(Set.of("node-2", "node-3", "node-4", "node-5"));

        Map<String, Boolean> peers = Map.of("node-2", true, "node-3", true, "node-4", false, "node-5", false);
        NodeStatusHandler.StatusProvider provider = NodeStatusHandler.StatusProvider.fromRaftState(
                "node-1", raftState, true, peers
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-1", provider);
        MessageEnvelope<?> responseEnv = handler.handle(createStatusRequest("test-caller"));

        assertNotNull(responseEnv);
        NodeStatusDto dto = (NodeStatusDto) responseEnv.getPayload();
        assertEquals("node-1", dto.getNodeId());
        assertEquals("LEADER", dto.getRole());
        assertEquals(raftState.currentTerm(), dto.getTerm());
        assertTrue(dto.isStorageHealthy());
        assertEquals(Boolean.TRUE, dto.getPeerConnectivity().get("node-2"));
        assertEquals(Boolean.FALSE, dto.getPeerConnectivity().get("node-4"));
    }

    @Test
    @DisplayName("Should return null when request envelope is null")
    public void testNullRequestReturnsNull() throws Exception {
        NodeStatusHandler handler = new NodeStatusHandler("node-1",
                () -> NodeStatusHandler.StatusSnapshot.uninitialized("node-1"));
        assertNull(handler.handle(null));
    }

    @Test
    @DisplayName("Should serialize and deserialize through MessageCodec without data loss")
    public void testMessageCodecRoundTrip() throws Exception {
        NodeStatusHandler.StatusSnapshot snapshot = new NodeStatusHandler.StatusSnapshot(
                "LEADER",
                10L,
                "node-1",
                100L,
                95L,
                95L,
                true,
                Map.of("node-2", true, "node-3", true, "node-4", true, "node-5", false)
        );

        NodeStatusHandler handler = new NodeStatusHandler("node-1", () -> snapshot);
        MessageEnvelope<?> responseEnv = handler.handle(createStatusRequest("monitor-client"));

        byte[] encodedBytes = MessageCodec.encode(responseEnv);
        MessageEnvelope<JsonNode> decodedGeneric = MessageCodec.decode(encodedBytes);

        assertEquals(MessageType.NODE_STATUS_RESPONSE, decodedGeneric.getType());
        assertEquals("node-1", decodedGeneric.getSenderId());

        NodeStatusDto decodedDto = MessageCodec.decodePayload(decodedGeneric, NodeStatusDto.class);
        decodedDto.validate();

        assertEquals("node-1", decodedDto.getNodeId());
        assertEquals("LEADER", decodedDto.getRole());
        assertEquals(10L, decodedDto.getTerm());
        assertEquals("node-1", decodedDto.getLeaderId());
        assertEquals(100L, decodedDto.getLastLogIndex());
        assertEquals(95L, decodedDto.getCommitIndex());
        assertEquals(95L, decodedDto.getLastApplied());
        assertTrue(decodedDto.isStorageHealthy());
        assertEquals(4, decodedDto.getPeerConnectivity().size());
        assertEquals(Boolean.TRUE, decodedDto.getPeerConnectivity().get("node-2"));
        assertEquals(Boolean.FALSE, decodedDto.getPeerConnectivity().get("node-5"));
    }
}
