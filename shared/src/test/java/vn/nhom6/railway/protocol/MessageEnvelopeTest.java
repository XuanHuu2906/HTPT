package vn.nhom6.railway.protocol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MessageEnvelopeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Should serialize and deserialize standard SPEC-03 request envelope")
    void testSerializationRoundTripRequest() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("requestId", "542e32fb-b657-4478-9cea-52286822d1d2");
        payload.put("tripId", "SE-001");
        payload.put("seatId", "G05");
        payload.put("passengerName", "Khach demo");

        MessageEnvelope<Map<String, Object>> envelope = new MessageEnvelope<>(
                1,
                "b4c410df-2b27-45dd-81ac-b7f9c0ec21e4",
                null,
                MessageType.BOOK_TICKET,
                "client-7",
                "2026-10-29T10:00:00Z",
                payload
        );

        envelope.validate();

        String json = objectMapper.writeValueAsString(envelope);
        assertNotNull(json);

        // Verify JSON contains expected fields and explicit "correlationId":null
        assertTrue(json.contains("\"protocolVersion\":1"));
        assertTrue(json.contains("\"messageId\":\"b4c410df-2b27-45dd-81ac-b7f9c0ec21e4\""));
        assertTrue(json.contains("\"correlationId\":null"));
        assertTrue(json.contains("\"type\":\"BOOK_TICKET\""));
        assertTrue(json.contains("\"senderId\":\"client-7\""));
        assertTrue(json.contains("\"sentAt\":\"2026-10-29T10:00:00Z\""));
        assertTrue(json.contains("\"requestId\":\"542e32fb-b657-4478-9cea-52286822d1d2\""));

        // Deserialize back
        MessageEnvelope<Map<String, Object>> deserialized = objectMapper.readValue(
                json,
                new TypeReference<MessageEnvelope<Map<String, Object>>>() {}
        );

        assertEquals(envelope, deserialized);
        assertNull(deserialized.getCorrelationId());
        assertEquals(MessageType.BOOK_TICKET, deserialized.getType());
        assertEquals("client-7", deserialized.getSenderId());
        assertEquals("542e32fb-b657-4478-9cea-52286822d1d2", deserialized.getPayload().get("requestId"));
    }

    @Test
    @DisplayName("Should serialize response envelope with correlationId matching request messageId")
    void testResponseEnvelopeFactory() throws Exception {
        Map<String, String> reqPayload = Map.of("tripId", "SE-001");
        MessageEnvelope<Map<String, String>> request = MessageEnvelope.createRequest(
                MessageType.GET_SEATS,
                "client-1",
                reqPayload
        );
        request.validate();
        assertNull(request.getCorrelationId());
        assertEquals(1, request.getProtocolVersion());

        Map<String, Object> respPayload = Map.of("tripId", "SE-001", "readIndex", 42L);
        MessageEnvelope<Map<String, Object>> response = MessageEnvelope.createResponse(
                request.getMessageId(),
                MessageType.GET_SEATS_RESPONSE,
                "node-3",
                respPayload
        );
        response.validate();

        assertEquals(request.getMessageId(), response.getCorrelationId());
        assertNotEquals(request.getMessageId(), response.getMessageId());
        assertEquals(MessageType.GET_SEATS_RESPONSE, response.getType());
        assertEquals("node-3", response.getSenderId());

        String json = objectMapper.writeValueAsString(response);
        JsonNode root = objectMapper.readTree(json);
        assertEquals(request.getMessageId(), root.get("correlationId").asText());
    }

    @Test
    @DisplayName("MessageType enum should contain all 17 types specified in SPEC-03 Section 3.1")
    void testMessageTypeEnumCompleteness() {
        assertEquals(17, MessageType.values().length);

        assertNotNull(MessageType.valueOf("GET_SEATS"));
        assertNotNull(MessageType.valueOf("GET_SEATS_RESPONSE"));
        assertNotNull(MessageType.valueOf("BOOK_TICKET"));
        assertNotNull(MessageType.valueOf("BOOK_TICKET_RESPONSE"));
        assertNotNull(MessageType.valueOf("CANCEL_TICKET"));
        assertNotNull(MessageType.valueOf("CANCEL_TICKET_RESPONSE"));
        assertNotNull(MessageType.valueOf("GET_REQUEST_RESULT"));
        assertNotNull(MessageType.valueOf("GET_REQUEST_RESULT_RESPONSE"));
        assertNotNull(MessageType.valueOf("GET_TICKET_HISTORY"));
        assertNotNull(MessageType.valueOf("GET_TICKET_HISTORY_RESPONSE"));
        assertNotNull(MessageType.valueOf("NODE_STATUS"));
        assertNotNull(MessageType.valueOf("NODE_STATUS_RESPONSE"));
        assertNotNull(MessageType.valueOf("REQUEST_VOTE"));
        assertNotNull(MessageType.valueOf("REQUEST_VOTE_RESPONSE"));
        assertNotNull(MessageType.valueOf("APPEND_ENTRIES"));
        assertNotNull(MessageType.valueOf("APPEND_ENTRIES_RESPONSE"));
        assertNotNull(MessageType.valueOf("ERROR_RESPONSE"));

        assertTrue(MessageType.BOOK_TICKET_RESPONSE.isResponse());
        assertFalse(MessageType.BOOK_TICKET.isResponse());
        assertTrue(MessageType.ERROR_RESPONSE.isResponse());

        assertTrue(MessageType.REQUEST_VOTE.isRaftRpc());
        assertTrue(MessageType.APPEND_ENTRIES_RESPONSE.isRaftRpc());
        assertFalse(MessageType.BOOK_TICKET.isRaftRpc());
    }

    @Test
    @DisplayName("Validation should fail on illegal or null envelope fields")
    void testEnvelopeValidationRules() {
        // 1. Wrong protocol version
        MessageEnvelope<String> e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setProtocolVersion(2);
        assertThrows(IllegalArgumentException.class, e::validate);

        // 2. Invalid messageId
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setMessageId("not-a-uuid");
        assertThrows(IllegalArgumentException.class, e::validate);

        // 3. Null messageId
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setMessageId(null);
        assertThrows(IllegalArgumentException.class, e::validate);

        // 4. Invalid correlationId (if present, must be UUID)
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setCorrelationId("invalid-uuid");
        assertThrows(IllegalArgumentException.class, e::validate);

        // 5. Null type
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setType(null);
        assertThrows(IllegalArgumentException.class, e::validate);

        // 6. Blank senderId
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "   ", "test");
        assertThrows(IllegalArgumentException.class, e::validate);

        // 7. Invalid sentAt
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", "test");
        e.setSentAt("not-an-iso-timestamp");
        assertThrows(IllegalArgumentException.class, e::validate);

        // 8. Null payload
        e = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client-1", null);
        assertThrows(IllegalArgumentException.class, e::validate);
    }

    public static void main(String[] args) throws Exception {
        MessageEnvelopeTest test = new MessageEnvelopeTest();
        System.out.println("[TEST 1] testSerializationRoundTripRequest...");
        test.testSerializationRoundTripRequest();
        System.out.println("-> PASS");

        System.out.println("[TEST 2] testResponseEnvelopeFactory...");
        test.testResponseEnvelopeFactory();
        System.out.println("-> PASS");

        System.out.println("[TEST 3] testMessageTypeEnumCompleteness...");
        test.testMessageTypeEnumCompleteness();
        System.out.println("-> PASS");

        System.out.println("[TEST 4] testEnvelopeValidationRules...");
        test.testEnvelopeValidationRules();
        System.out.println("-> PASS");

        System.out.println("\n=== ALL TESTS COMPLETED: 4/4 PASSED ===");
    }
}
