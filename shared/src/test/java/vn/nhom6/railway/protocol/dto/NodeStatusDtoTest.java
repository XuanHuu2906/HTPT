package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for NodeStatusDto (SPEC-03 §5)")
public class NodeStatusDtoTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("Should serialize and deserialize matching SPEC-03 Section 5 golden example")
    public void testGoldenJsonSerialization() throws Exception {
        Map<String, Boolean> peers = Map.of(
                "node-1", true,
                "node-2", false,
                "node-4", false,
                "node-5", true
        );

        NodeStatusDto dto = new NodeStatusDto(
                "node-3",
                "LEADER",
                8L,
                "node-3",
                45L,
                43L,
                43L,
                true,
                peers,
                "2026-10-29T10:00:00Z"
        );
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"nodeId\":\"node-3\""));
        assertTrue(json.contains("\"role\":\"LEADER\""));
        assertTrue(json.contains("\"term\":8"));
        assertTrue(json.contains("\"leaderId\":\"node-3\""));
        assertTrue(json.contains("\"lastLogIndex\":45"));
        assertTrue(json.contains("\"commitIndex\":43"));
        assertTrue(json.contains("\"lastApplied\":43"));
        assertTrue(json.contains("\"storageHealthy\":true"));
        assertTrue(json.contains("\"observedAt\":\"2026-10-29T10:00:00Z\""));

        NodeStatusDto deserialized = mapper.readValue(json, NodeStatusDto.class);
        assertEquals(dto, deserialized);
        assertTrue(deserialized.isLeader());
        assertFalse(deserialized.isFollower());
        assertTrue(deserialized.isStorageHealthy());
        assertEquals(8L, deserialized.getTerm());
    }

    @Test
    @DisplayName("Should allow null role and term when uninitialized without fabricating fake state")
    public void testUninitializedConsensusState() {
        NodeStatusDto dto = new NodeStatusDto(
                "node-2",
                null, // No consensus yet
                null, // No reliable term
                null,
                null,
                null,
                null,
                false, // Storage down
                Map.of("node-1", false, "node-3", false, "node-4", false, "node-5", false),
                "2026-10-29T10:00:00Z"
        );
        dto.validate(); // Must pass validation per SPEC-03 Section 5

        assertNull(dto.getRole());
        assertNull(dto.getTerm());
        assertFalse(dto.isLeader());
        assertFalse(dto.isFollower());
        assertFalse(dto.isStorageHealthy());
    }

    @Test
    @DisplayName("Should reject invalid fields per SPEC-03 Section 5 rules")
    public void testValidationFailures() {
        Map<String, Boolean> peers = Map.of("node-1", true);

        // Invalid nodeId
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("invalid-node", "LEADER", 1L, null, 0L, 0L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto(null, "LEADER", 1L, null, 0L, 0L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());

        // Invalid role
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "PRESIDENT", 1L, null, 0L, 0L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());

        // Negative values
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", -1L, null, 0L, 0L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", 1L, null, -1L, 0L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", 1L, null, 0L, -5L, 0L, true, peers, "2026-10-29T10:00:00Z").validate());
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", 1L, null, 0L, 0L, -1L, true, peers, "2026-10-29T10:00:00Z").validate());

        // Null storageHealthy
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", 1L, null, 0L, 0L, 0L, null, peers, "2026-10-29T10:00:00Z").validate());

        // Blank observedAt
        assertThrows(IllegalArgumentException.class, () ->
                new NodeStatusDto("node-1", "LEADER", 1L, null, 0L, 0L, 0L, true, peers, "   ").validate());
    }
}
