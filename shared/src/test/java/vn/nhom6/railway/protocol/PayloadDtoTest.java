package vn.nhom6.railway.protocol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PayloadDtoTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("GetSeatsRequestDto: should serialize, deserialize and validate according to SPEC-03 Section 4.1")
    void testGetSeatsRequestDto() throws Exception {
        GetSeatsRequestDto dto = new GetSeatsRequestDto("SE-001");
        dto.validate();

        String json = objectMapper.writeValueAsString(dto);
        assertTrue(json.contains("\"tripId\":\"SE-001\""));

        GetSeatsRequestDto deserialized = objectMapper.readValue(json, GetSeatsRequestDto.class);
        assertEquals(dto, deserialized);
        assertEquals("SE-001", deserialized.getTripId());

        // Negative cases
        assertThrows(IllegalArgumentException.class, () -> new GetSeatsRequestDto(null).validate());
        assertThrows(IllegalArgumentException.class, () -> new GetSeatsRequestDto("   ").validate());
        assertThrows(IllegalArgumentException.class, () -> new GetSeatsRequestDto("SE-999").validate());
    }

    @Test
    @DisplayName("BookTicketRequestDto: should serialize, deserialize and validate according to SPEC-03 Section 4.2")
    void testBookTicketRequestDto() throws Exception {
        BookTicketRequestDto dto = new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2",
                "SE-001",
                "G05",
                "Nguyễn Văn A"
        );
        dto.validate();

        String json = objectMapper.writeValueAsString(dto);
        assertTrue(json.contains("\"requestId\":\"542e32fb-b657-4478-9cea-52286822d1d2\""));
        assertTrue(json.contains("\"tripId\":\"SE-001\""));
        assertTrue(json.contains("\"seatId\":\"G05\""));
        assertTrue(json.contains("\"passengerName\":\"Nguyễn Văn A\""));

        BookTicketRequestDto deserialized = objectMapper.readValue(json, BookTicketRequestDto.class);
        assertEquals(dto, deserialized);

        // Validation for seats G01 to G10
        for (int i = 1; i <= 10; i++) {
            String seat = String.format("G%02d", i);
            BookTicketRequestDto validSeatDto = new BookTicketRequestDto(
                    "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", seat, "Khach demo");
            assertDoesNotThrow(validSeatDto::validate);
        }

        // Negative: invalid seat format
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G00", "Khach").validate());
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G11", "Khach").validate());
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "A01", "Khach").validate());

        // Negative: invalid UUID
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "not-a-uuid", "SE-001", "G01", "Khach").validate());

        // Negative: invalid passengerName (blank or null)
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G01", "   ").validate());
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G01", null).validate());

        // Negative: passengerName containing null character U+0000
        assertThrows(IllegalArgumentException.class, () -> new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G01", "Khach\u0000Demo").validate());
    }

    @Test
    @DisplayName("RequestVoteRequestDto: should serialize, deserialize and validate according to SPEC-03 Section 6")
    void testRequestVoteRequestDto() throws Exception {
        RequestVoteRequestDto dto = new RequestVoteRequestDto(8, "node-3", 42, 7);
        dto.validate();

        String json = objectMapper.writeValueAsString(dto);
        JsonNode root = objectMapper.readTree(json);
        assertEquals(8, root.get("term").asLong());
        assertEquals("node-3", root.get("candidateId").asText());
        assertEquals(42, root.get("lastLogIndex").asLong());
        assertEquals(7, root.get("lastLogTerm").asLong());

        RequestVoteRequestDto deserialized = objectMapper.readValue(json, RequestVoteRequestDto.class);
        assertEquals(dto, deserialized);

        // Negative cases
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequestDto(-1, "node-1", 0, 0).validate());
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequestDto(1, "", 0, 0).validate());
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequestDto(1, null, 0, 0).validate());
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequestDto(1, "node-1", -1, 0).validate());
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequestDto(1, "node-1", 0, -1).validate());
    }

    @Test
    @DisplayName("AppendEntriesRequestDto: should support heartbeats and log entries according to SPEC-03 Section 6")
    void testAppendEntriesRequestDto() throws Exception {
        // 1. Heartbeat RPC (entries is empty)
        AppendEntriesRequestDto heartbeat = AppendEntriesRequestDto.createHeartbeat(8, "node-3", 42, 7, 42);
        heartbeat.validate();
        assertTrue(heartbeat.isHeartbeat());
        assertEquals(0, heartbeat.getEntries().size());

        String hbJson = objectMapper.writeValueAsString(heartbeat);
        AppendEntriesRequestDto hbDeserialized = objectMapper.readValue(hbJson, AppendEntriesRequestDto.class);
        assertEquals(heartbeat, hbDeserialized);
        assertTrue(hbDeserialized.isHeartbeat());

        // 2. Replication RPC with entries
        Map<String, Object> commandMap = Map.of(
                "requestId", "542e32fb-b657-4478-9cea-52286822d1d2",
                "tripId", "SE-001",
                "seatId", "G05",
                "passengerName", "Khach demo",
                "ticketId", "ticket-uuid",
                "issuedAt", "2026-10-29T10:00:00.000Z"
        );
        AppendEntriesRequestDto.LogEntryDto entry = new AppendEntriesRequestDto.LogEntryDto(
                43, 8, "BOOK_TICKET", commandMap
        );

        AppendEntriesRequestDto replication = new AppendEntriesRequestDto(
                8, "node-3", 42, 7, List.of(entry), 43
        );
        replication.validate();
        assertFalse(replication.isHeartbeat());
        assertEquals(1, replication.getEntries().size());

        String repJson = objectMapper.writeValueAsString(replication);
        assertTrue(repJson.contains("\"commandType\":\"BOOK_TICKET\""));
        assertTrue(repJson.contains("\"leaderCommit\":43"));

        AppendEntriesRequestDto repDeserialized = objectMapper.readValue(repJson, AppendEntriesRequestDto.class);
        assertEquals(replication, repDeserialized);
        assertEquals(43, repDeserialized.getEntries().get(0).getIndex());

        // 3. Rule: (prevLogIndex=0) requires prevLogTerm=0 (SPEC-03 Section 6.1)
        AppendEntriesRequestDto invalidPrevLog = new AppendEntriesRequestDto(1, "node-1", 0, 1, List.of(), 0);
        assertThrows(IllegalArgumentException.class, invalidPrevLog::validate);

        AppendEntriesRequestDto validGenesis = new AppendEntriesRequestDto(1, "node-1", 0, 0, List.of(), 0);
        assertDoesNotThrow(validGenesis::validate);
    }

    @Test
    @DisplayName("MessageEnvelope wrapping: should envelope all DTOs cleanly")
    void testMessageEnvelopeIntegration() throws Exception {
        BookTicketRequestDto bookDto = new BookTicketRequestDto(
                "542e32fb-b657-4478-9cea-52286822d1d2", "SE-001", "G05", "Khach demo"
        );
        MessageEnvelope<BookTicketRequestDto> envelope = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET,
                "client-1",
                bookDto
        );
        envelope.validate();

        String json = objectMapper.writeValueAsString(envelope);
        assertTrue(json.contains("\"type\":\"BOOK_TICKET\""));
        assertTrue(json.contains("\"seatId\":\"G05\""));

        MessageEnvelope<BookTicketRequestDto> deserialized = objectMapper.readValue(
                json,
                new TypeReference<MessageEnvelope<BookTicketRequestDto>>() {}
        );
        assertEquals(MessageType.BOOK_TICKET, deserialized.getType());
        assertEquals("G05", deserialized.getPayload().getSeatId());
    }

    public static void main(String[] args) throws Exception {
        PayloadDtoTest test = new PayloadDtoTest();
        System.out.println("[TEST 1] testGetSeatsRequestDto...");
        test.testGetSeatsRequestDto();
        System.out.println("-> PASS");

        System.out.println("[TEST 2] testBookTicketRequestDto...");
        test.testBookTicketRequestDto();
        System.out.println("-> PASS");

        System.out.println("[TEST 3] testRequestVoteRequestDto...");
        test.testRequestVoteRequestDto();
        System.out.println("-> PASS");

        System.out.println("[TEST 4] testAppendEntriesRequestDto...");
        test.testAppendEntriesRequestDto();
        System.out.println("-> PASS");

        System.out.println("[TEST 5] testMessageEnvelopeIntegration...");
        test.testMessageEnvelopeIntegration();
        System.out.println("-> PASS");

        System.out.println("\n=== ALL PAYLOAD DTO TESTS COMPLETED: 5/5 PASSED ===");
    }
}
