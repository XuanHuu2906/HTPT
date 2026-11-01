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
        payload);

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
        new TypeReference<MessageEnvelope<Map<String, Object>>>() {
        });

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
        reqPayload);
    request.validate();
    assertNull(request.getCorrelationId());
    assertEquals(1, request.getProtocolVersion());

    Map<String, Object> respPayload = Map.of("tripId", "SE-001", "readIndex", 42L);
    MessageEnvelope<Map<String, Object>> response = MessageEnvelope.createResponse(
        request.getMessageId(),
        MessageType.GET_SEATS_RESPONSE,
        "node-3",
        respPayload);
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
  @DisplayName("Validation rejects invalid envelope fields")
  void testEnvelopeValidationRules() {
    Map<String, String> validPayload = Map.of("tripId", "SE-001");

    var e1 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e1.setProtocolVersion(2);
    assertThrows(IllegalArgumentException.class, e1::validate);

    var e2 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e2.setMessageId("not-a-uuid");
    assertThrows(IllegalArgumentException.class, e2::validate);

    var e3 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e3.setMessageId(null);
    assertThrows(IllegalArgumentException.class, e3::validate);

    var e4 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e4.setCorrelationId(
        "542e32fb-b657-4478-9cea-52286822d1d2");
    assertThrows(IllegalArgumentException.class, e4::validate);

    var e5 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e5.setType(null);
    assertThrows(IllegalArgumentException.class, e5::validate);

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "   ", validPayload));

    var e7 = MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", validPayload);
    e7.setSentAt("invalid-date");
    assertThrows(IllegalArgumentException.class, e7::validate);

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", null));
  }

  @Test
  @DisplayName("Payload must be a JSON object")
  void testPayloadObjectValidation() {
    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", "hello"));

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", 123));

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1", true));

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1",
        new String[] { "G01", "G02" }));

    assertDoesNotThrow(() -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS, "client-1",
        Map.of("tripId", "SE-001")));

    assertDoesNotThrow(() -> MessageEnvelope.createRequest(
        MessageType.APPEND_ENTRIES, "node-1",
        Map.of()));
  }

  @Test
  @DisplayName("CorrelationId follows request-response rules")
  void testCorrelationValidation() {
    var request = MessageEnvelope.createRequest(
        MessageType.GET_SEATS,
        "client-1",
        Map.of("tripId", "SE-001"));

    assertNull(request.getCorrelationId());

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createRequest(
        MessageType.GET_SEATS_RESPONSE,
        "node-1",
        Map.of("tripId", "SE-001")));

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createResponse(
        null,
        MessageType.GET_SEATS_RESPONSE,
        "node-1",
        Map.of("tripId", "SE-001")));

    assertThrows(IllegalArgumentException.class, () -> MessageEnvelope.createResponse(
        request.getMessageId(),
        MessageType.GET_SEATS,
        "node-1",
        Map.of("tripId", "SE-001")));

    var response = MessageEnvelope.createResponse(
        request.getMessageId(),
        MessageType.GET_SEATS_RESPONSE,
        "node-1",
        Map.of("tripId", "SE-001"));

    assertEquals(
        request.getMessageId(),
        response.getCorrelationId());
  }

  @Test
  @DisplayName("Timestamp must use UTC millisecond precision")
  void testTimestampValidation() {
    var envelope = MessageEnvelope.createRequest(
        MessageType.GET_SEATS,
        "client-1",
        Map.of("tripId", "SE-001"));

    envelope.setSentAt("2026-10-29T10:00:00.123Z");
    assertDoesNotThrow(envelope::validate);

    envelope.setSentAt("2026-10-29T10:00:00Z");
    assertDoesNotThrow(envelope::validate);

    envelope.setSentAt("2026-10-29T10:00:00.123456Z");
    assertThrows(IllegalArgumentException.class,
        envelope::validate);

    envelope.setSentAt("2026-10-29T10:00:00.1Z");
    assertThrows(IllegalArgumentException.class,
        envelope::validate);

    envelope.setSentAt("2026-10-29T17:00:00+07:00");
    assertThrows(IllegalArgumentException.class,
        envelope::validate);
  }

  @Test
  @DisplayName("Jackson rejects invalid envelopes on deserialize")
  void testDeserializationValidation() throws Exception {
    var valid = MessageEnvelope.createRequest(
        MessageType.GET_SEATS,
        "client-1",
        Map.of("tripId", "SE-001"));

    String json = objectMapper.writeValueAsString(valid);

    var parsed = objectMapper.readValue(
        json,
        new TypeReference<MessageEnvelope<Object>>() {
        });

    assertEquals(valid.getMessageId(), parsed.getMessageId());

    var original = (com.fasterxml.jackson.databind.node.ObjectNode) objectMapper.readTree(json);

    var wrongVersion = original.deepCopy();
    wrongVersion.put("protocolVersion", 99);

    assertThrows(
        com.fasterxml.jackson.core.JsonProcessingException.class,
        () -> objectMapper.readValue(
            objectMapper.writeValueAsString(wrongVersion),
            new TypeReference<MessageEnvelope<Object>>() {
            }));

    var wrongPayload = original.deepCopy();
    wrongPayload.put("payload", "not-an-object");

    assertThrows(
        com.fasterxml.jackson.core.JsonProcessingException.class,
        () -> objectMapper.readValue(
            objectMapper.writeValueAsString(wrongPayload),
            new TypeReference<MessageEnvelope<Object>>() {
            }));

    var missingCorrelation = original.deepCopy();
    missingCorrelation.remove("correlationId");

    assertThrows(
        com.fasterxml.jackson.core.JsonProcessingException.class,
        () -> objectMapper.readValue(
            objectMapper.writeValueAsString(missingCorrelation),
            new TypeReference<MessageEnvelope<Object>>() {
            }));
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
