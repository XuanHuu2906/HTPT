package vn.nhom6.railway.protocol;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecNegativeTest {

    private static JsonNode invalidFixtures;

    @BeforeAll
    static void loadInvalidFixtures() throws IOException {
        try (InputStream is = MessageCodecNegativeTest.class.getClassLoader().getResourceAsStream("fixtures/invalid-protocol-v1.json")) {
            assertNotNull(is, "fixtures/invalid-protocol-v1.json must be present in classpath");
            // Use ObjectMapper without duplicate detection to load the test fixture definition map itself
            invalidFixtures = MessageCodec.getMapper().readTree(is);
        }
    }

    @Test
    @DisplayName("Negative 1: should reject JSON with duplicate keys (SPEC-03 Section 3.0)")
    void testRejectDuplicateKeys() {
        String json = invalidFixtures.get("duplicateKeys").asText();

        JsonParseException ex = assertThrows(JsonParseException.class, () -> MessageCodec.decode(json));
        assertTrue(ex.getMessage().contains("Duplicate field"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex));
    }

    @Test
    @DisplayName("Negative 2: should reject JSON with unknown properties (SPEC-03 Section 3.0)")
    void testRejectUnknownProperties() {
        String json = invalidFixtures.get("unknownProperties").asText();

        UnrecognizedPropertyException ex = assertThrows(
                UnrecognizedPropertyException.class,
                () -> MessageCodec.decode(json, BookTicketRequestDto.class)
        );
        assertTrue(ex.getMessage().contains("unknownPropertyField"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex));
    }

    @Test
    @DisplayName("Negative 3: should reject unknown enum values (SPEC-03 Section 3.0)")
    void testRejectUnknownEnum() {
        String json = invalidFixtures.get("unknownEnum").asText();

        InvalidFormatException ex = assertThrows(
                InvalidFormatException.class,
                () -> MessageCodec.decode(json)
        );
        assertTrue(ex.getMessage().contains("not one of the values accepted for Enum class"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex));
    }

    @Test
    @DisplayName("Negative 4: should reject string-to-number and number-to-string coercion (SPEC-03 Section 3.0)")
    void testRejectScalarCoercion() {
        // String to number coercion rejection
        String strToNum = invalidFixtures.get("stringToNumberCoercion").asText();
        MismatchedInputException ex1 = assertThrows(
                MismatchedInputException.class,
                () -> MessageCodec.getMapper().readValue(strToNum, RequestVoteRequestDto.class)
        );
        assertTrue(ex1.getMessage().contains("Cannot coerce String value"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex1));

        // Number to string coercion rejection
        String numToStr = invalidFixtures.get("numberToStringCoercion").asText();
        MismatchedInputException ex2 = assertThrows(
                MismatchedInputException.class,
                () -> MessageCodec.getMapper().readValue(numToStr, GetSeatsRequestDto.class)
        );
        assertTrue(ex2.getMessage().contains("Cannot coerce Integer value"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex2));
    }

    @Test
    @DisplayName("Negative 5: should reject integer values exceeding Long.MAX_VALUE without wrapping (SPEC-03 Section 3.0)")
    void testRejectNumberOverflow() {
        String json = invalidFixtures.get("numberOverflow").asText();

        JsonMappingException ex = assertThrows(
                JsonMappingException.class,
                () -> MessageCodec.getMapper().readValue(json, RequestVoteRequestDto.class)
        );
        assertTrue(ex.getMessage().contains("out of range of long"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex));
    }

    @Test
    @DisplayName("Negative 6: should reject illegal null fields and invalid UUIDs during validation")
    void testRejectIllegalNullAndInvalidUuid() {
        // Illegal null messageId
        String nullIdJson = invalidFixtures.get("illegalNullMessageId").asText();
        Exception ex1 = assertThrows(Exception.class, () -> {
            MessageEnvelope<GetSeatsRequestDto> env = MessageCodec.decode(nullIdJson, GetSeatsRequestDto.class);
            env.validate();
        });
        assertTrue(ex1.getMessage().contains("messageId"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex1));

        // Invalid UUID
        String invalidUuidJson = invalidFixtures.get("invalidUuid").asText();
        Exception ex2 = assertThrows(Exception.class, () -> {
            MessageEnvelope<GetSeatsRequestDto> env = MessageCodec.decode(invalidUuidJson, GetSeatsRequestDto.class);
            env.validate();
        });
        assertTrue(ex2.getMessage().contains("messageId"));
        assertEquals("INVALID_REQUEST", MessageCodec.classifyErrorCode(ex2));
    }

    @Test
    @DisplayName("Negative 7: should reject unsupported protocolVersion and classify as UNSUPPORTED_VERSION")
    void testRejectUnsupportedVersion() {
        String json = invalidFixtures.get("unsupportedVersion").asText();
        Exception ex = assertThrows(Exception.class, () -> {
            MessageEnvelope<GetSeatsRequestDto> env = MessageCodec.decode(json, GetSeatsRequestDto.class);
            env.validate();
        });
        assertTrue(ex.getMessage().contains("Unsupported protocolVersion"));
        assertEquals("UNSUPPORTED_VERSION", MessageCodec.classifyErrorCode(ex));
    }

    @Test
    @DisplayName("Negative 8: should catch corrupt bytes and malformed JSON safely without crashing JVM")
    void testTolerateMalformedJsonAndCorruptBytes() {
        // Corrupt random binary bytes
        byte[] corruptBytes = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xC0, 0x00, 0x7B};
        assertThrows(IOException.class, () -> MessageCodec.decode(corruptBytes));

        // Malformed incomplete JSON string
        String malformedJson = invalidFixtures.get("malformedJson").asText();
        assertThrows(IOException.class, () -> MessageCodec.decode(malformedJson));

        // Null and blank string checks
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode((String) null));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode("   "));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode((byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> MessageCodec.decode(new byte[0]));
    }

    @Test
    @DisplayName("Utility: createErrorResponse should generate valid SPEC-03 Section 7 error envelope")
    void testCreateErrorResponse() throws Exception {
        String reqMsgId = "b4c410df-2b27-45dd-81ac-b7f9c0ec21e4";
        String reqBizId = "542e32fb-b657-4478-9cea-52286822d1d2";
        MessageEnvelope<ErrorResponseDto> errorEnv = MessageCodec.createErrorResponse(
                reqMsgId,
                "INVALID_REQUEST",
                "Malformed request payload",
                null,
                null,
                reqBizId,
                "node-1"
        );
        errorEnv.validate();
        errorEnv.getPayload().validate();

        assertEquals(MessageType.ERROR_RESPONSE, errorEnv.getType());
        assertEquals(reqMsgId, errorEnv.getCorrelationId());
        assertEquals("INVALID_REQUEST", errorEnv.getPayload().getErrorCode());
        assertEquals("Malformed request payload", errorEnv.getPayload().getMessage());
        assertEquals(reqBizId, errorEnv.getPayload().getRequestId());
        assertNull(errorEnv.getPayload().getLeaderHint());
        assertNull(errorEnv.getPayload().getTerm());

        // Round trip error envelope
        byte[] bytes = MessageCodec.encode(errorEnv);
        MessageEnvelope<ErrorResponseDto> decoded = MessageCodec.decode(bytes, ErrorResponseDto.class);
        assertEquals(errorEnv, decoded);
    }

    public static void main(String[] args) throws Exception {
        loadInvalidFixtures();
        MessageCodecNegativeTest test = new MessageCodecNegativeTest();

        System.out.println("[TEST 1] testRejectDuplicateKeys...");
        test.testRejectDuplicateKeys();
        System.out.println("-> PASS");

        System.out.println("[TEST 2] testRejectUnknownProperties...");
        test.testRejectUnknownProperties();
        System.out.println("-> PASS");

        System.out.println("[TEST 3] testRejectUnknownEnum...");
        test.testRejectUnknownEnum();
        System.out.println("-> PASS");

        System.out.println("[TEST 4] testRejectScalarCoercion...");
        test.testRejectScalarCoercion();
        System.out.println("-> PASS");

        System.out.println("[TEST 5] testRejectNumberOverflow...");
        test.testRejectNumberOverflow();
        System.out.println("-> PASS");

        System.out.println("[TEST 6] testRejectIllegalNullAndInvalidUuid...");
        test.testRejectIllegalNullAndInvalidUuid();
        System.out.println("-> PASS");

        System.out.println("[TEST 7] testRejectUnsupportedVersion...");
        test.testRejectUnsupportedVersion();
        System.out.println("-> PASS");

        System.out.println("[TEST 8] testTolerateMalformedJsonAndCorruptBytes...");
        test.testTolerateMalformedJsonAndCorruptBytes();
        System.out.println("-> PASS");

        System.out.println("[TEST 9] testCreateErrorResponse...");
        test.testCreateErrorResponse();
        System.out.println("-> PASS");

        System.out.println("\n=== ALL NEGATIVE AND HARDENING TESTS COMPLETED: 9/9 PASSED ===");
    }
}
