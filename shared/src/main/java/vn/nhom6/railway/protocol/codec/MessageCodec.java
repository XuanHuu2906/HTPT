package vn.nhom6.railway.protocol.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;

/**
 * Central JSON MessageCodec according to SPEC-03 Section 1, Section 3, Section 3.0, and Section 7.
 *
 * <p>Encodes and decodes {@link MessageEnvelope} and payload DTOs to/from UTF-8 JSON.
 * Configured with strict defense rules:
 * <ul>
 *   <li>Strict duplicate key detection (rejects duplicate JSON keys)</li>
 *   <li>Fail on unknown properties (rejects unexpected fields)</li>
 *   <li>Disabled scalar coercion (prevents string-to-number / number-to-string coercion)</li>
 *   <li>Explicit null serialization for correlationId in requests</li>
 * </ul>
 */
public final class MessageCodec {

    private static final ObjectMapper MAPPER = createConfiguredMapper();

    private MessageCodec() {
        // Utility class
    }

    private static ObjectMapper createConfiguredMapper() {
        // Enforce strict duplicate key detection (SPEC-03 Section 3.0)
        JsonFactory jsonFactory = JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();

        ObjectMapper mapper = new ObjectMapper(jsonFactory);

        // Ensure null fields (like correlationId: null) are always present in JSON (SPEC-03 Section 3)
        mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);

        // Strict mode: Reject unknown properties with error (SPEC-03 Section 3.0)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        // Strict coercion: Reject string-to-number, number-to-string, and boolean coercion (SPEC-03 Section 3.0)
        mapper.coercionConfigDefaults()
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);

        return mapper;
    }

    /**
     * Returns the shared Jackson {@link ObjectMapper} configured for the railway protocol.
     */
    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    /**
     * Encodes a {@link MessageEnvelope} into a UTF-8 JSON byte array.
     *
     * @param envelope The envelope to encode (must not be null)
     * @return UTF-8 encoded JSON bytes
     * @throws IOException If serialization fails
     */
    public static byte[] encode(MessageEnvelope<?> envelope) throws IOException {
        if (envelope == null) {
            throw new IllegalArgumentException("envelope must not be null");
        }
        return MAPPER.writeValueAsBytes(envelope);
    }

    /**
     * Encodes a {@link MessageEnvelope} into a JSON string.
     *
     * @param envelope The envelope to encode (must not be null)
     * @return JSON string
     * @throws JsonProcessingException If serialization fails
     */
    public static String encodeToString(MessageEnvelope<?> envelope) throws JsonProcessingException {
        if (envelope == null) {
            throw new IllegalArgumentException("envelope must not be null");
        }
        return MAPPER.writeValueAsString(envelope);
    }

    /**
     * Decodes a UTF-8 JSON byte array into a raw {@link MessageEnvelope} holding a {@link JsonNode} payload.
     * Useful for two-phase decoding where message type is inspected before mapping payload DTO.
     *
     * @param bytes UTF-8 JSON bytes (must not be null or empty)
     * @return Envelope with JsonNode payload
     * @throws IOException If parsing fails
     */
    public static MessageEnvelope<JsonNode> decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("bytes must not be null or empty");
        }
        return MAPPER.readValue(bytes, new TypeReference<MessageEnvelope<JsonNode>>() {});
    }

    /**
     * Decodes a JSON string into a raw {@link MessageEnvelope} holding a {@link JsonNode} payload.
     *
     * @param json JSON string (must not be null or blank)
     * @return Envelope with JsonNode payload
     * @throws IOException If parsing fails
     */
    public static MessageEnvelope<JsonNode> decode(String json) throws IOException {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("json must not be null or blank");
        }
        return MAPPER.readValue(json, new TypeReference<MessageEnvelope<JsonNode>>() {});
    }

    /**
     * Decodes a UTF-8 JSON byte array directly into a {@link MessageEnvelope} with the specified payload type.
     *
     * @param bytes UTF-8 JSON bytes
     * @param payloadClass Class of the payload DTO
     * @param <T> Type of the payload
     * @return Strongly-typed envelope
     * @throws IOException If parsing fails
     */
    public static <T> MessageEnvelope<T> decode(byte[] bytes, Class<T> payloadClass) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("bytes must not be null or empty");
        }
        if (payloadClass == null) {
            throw new IllegalArgumentException("payloadClass must not be null");
        }
        JavaType envelopeType = MAPPER.getTypeFactory().constructParametricType(MessageEnvelope.class, payloadClass);
        return MAPPER.readValue(bytes, envelopeType);
    }

    /**
     * Decodes a JSON string directly into a {@link MessageEnvelope} with the specified payload type.
     *
     * @param json JSON string
     * @param payloadClass Class of the payload DTO
     * @param <T> Type of the payload
     * @return Strongly-typed envelope
     * @throws IOException If parsing fails
     */
    public static <T> MessageEnvelope<T> decode(String json, Class<T> payloadClass) throws IOException {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("json must not be null or blank");
        }
        if (payloadClass == null) {
            throw new IllegalArgumentException("payloadClass must not be null");
        }
        JavaType envelopeType = MAPPER.getTypeFactory().constructParametricType(MessageEnvelope.class, payloadClass);
        return MAPPER.readValue(json, envelopeType);
    }

    /**
     * Converts the {@link JsonNode} payload of a raw envelope into a strongly-typed DTO.
     *
     * @param rawEnvelope The raw envelope with JsonNode payload
     * @param payloadClass Target payload class
     * @param <T> Target payload type
     * @return Deserialized payload DTO
     * @throws JsonProcessingException If tree conversion fails
     */
    public static <T> T decodePayload(MessageEnvelope<JsonNode> rawEnvelope, Class<T> payloadClass) throws JsonProcessingException {
        if (rawEnvelope == null || rawEnvelope.getPayload() == null) {
            throw new IllegalArgumentException("rawEnvelope and its payload must not be null");
        }
        if (payloadClass == null) {
            throw new IllegalArgumentException("payloadClass must not be null");
        }
        return MAPPER.treeToValue(rawEnvelope.getPayload(), payloadClass);
    }

    /**
     * Classifies a parsing or validation exception into standard SPEC-03 Section 7 error codes.
     *
     * @param ex The exception thrown during decode or validation
     * @return "UNSUPPORTED_VERSION", "INVALID_REQUEST", or "INTERNAL_ERROR"
     */
    public static String classifyErrorCode(Throwable ex) {
        if (ex == null) {
            return "INTERNAL_ERROR";
        }
        String msg = ex.getMessage();
        if (msg != null && (msg.contains("Unsupported protocolVersion") || msg.contains("UNSUPPORTED_VERSION"))) {
            return "UNSUPPORTED_VERSION";
        }
        if (ex instanceof JsonProcessingException || ex instanceof IllegalArgumentException) {
            return "INVALID_REQUEST";
        }
        return "INTERNAL_ERROR";
    }

    /**
     * Helper factory to build a standard ERROR_RESPONSE envelope according to SPEC-03 Section 7.
     */
    public static MessageEnvelope<ErrorResponseDto> createErrorResponse(
            String requestMessageId,
            String errorCode,
            String message,
            String leaderHint,
            Long term,
            String requestId,
            String senderId) {
        ErrorResponseDto payload = new ErrorResponseDto(errorCode, message, leaderHint, term, requestId);
        payload.validate();
        return MessageEnvelope.createResponse(requestMessageId, MessageType.ERROR_RESPONSE, senderId, payload);
    }
}
