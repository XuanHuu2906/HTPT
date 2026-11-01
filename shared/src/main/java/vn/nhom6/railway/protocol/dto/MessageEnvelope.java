package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Standard Envelope wrapping all TCP JSON messages according to SPEC-03 Section
 * 3.
 *
 * <p>
 * Field constraints (SPEC-03 Section 3.0):
 * <ul>
 * <li>protocolVersion: integer exactly 1</li>
 * <li>messageId: UUID in 8-4-4-4-12 lowercase hex format</li>
 * <li>correlationId: UUID or null (null for requests; UUID of original request
 * for responses)</li>
 * <li>type: MessageType enum value</li>
 * <li>senderId: non-empty string identifier (e.g. "client-1", "node-1")</li>
 * <li>sentAt: UTC ISO-8601 timestamp string</li>
 * <li>payload: non-null message payload object</li>
 * </ul>
 *
 * @param <T> Payload object type
 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public class MessageEnvelope<T> {

  public static final int CURRENT_PROTOCOL_VERSION = 1;

  private static final Pattern UUID_PATTERN = Pattern.compile(
      "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  private static final Pattern UTC_TIMESTAMP_PATTERN = Pattern.compile(
      "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?Z$");

  private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper();

  @JsonProperty("protocolVersion")
  private int protocolVersion = CURRENT_PROTOCOL_VERSION;

  @JsonProperty("messageId")
  private String messageId;

  @JsonProperty("correlationId")
  private String correlationId;

  @JsonProperty("type")
  private MessageType type;

  @JsonProperty("senderId")
  private String senderId;

  @JsonProperty("sentAt")
  private String sentAt;

  @JsonProperty("payload")
  private T payload;

  public MessageEnvelope() {
  }

  @JsonCreator
  public MessageEnvelope(
      @JsonProperty(value = "protocolVersion", required = true) int protocolVersion,

      @JsonProperty(value = "messageId", required = true) String messageId,

      @JsonProperty(value = "correlationId", required = true) String correlationId,

      @JsonProperty(value = "type", required = true) MessageType type,

      @JsonProperty(value = "senderId", required = true) String senderId,

      @JsonProperty(value = "sentAt", required = true) String sentAt,

      @JsonProperty(value = "payload", required = true) T payload) {

    this.protocolVersion = protocolVersion;
    this.messageId = messageId;
    this.correlationId = correlationId;
    this.type = type;
    this.senderId = senderId;
    this.sentAt = sentAt;
    this.payload = payload;

    validate();
  }

  /**
   * Factory method to build a new request envelope with auto-generated messageId
   * and sentAt.
   */
  public static <P> MessageEnvelope<P> createRequest(MessageType type, String senderId, P payload) {
    return new MessageEnvelope<>(
        CURRENT_PROTOCOL_VERSION,
        UUID.randomUUID().toString(),
        null,
        type,
        senderId,
        Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
        payload);
  }

  /**
   * Factory method to build a response envelope correlated to a given request
   * messageId.
   */
  public static <P> MessageEnvelope<P> createResponse(String requestMessageId, MessageType type, String senderId,
      P payload) {
    return new MessageEnvelope<>(
        CURRENT_PROTOCOL_VERSION,
        UUID.randomUUID().toString(),
        requestMessageId,
        type,
        senderId,
        Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
        payload);
  }

  /**
   * Validates envelope fields against SPEC-03 Section 3 rules.
   *
   * @throws IllegalArgumentException if any field violates the specification
   */
  public void validate() {
    if (protocolVersion != CURRENT_PROTOCOL_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported protocolVersion: " + protocolVersion + ", expected: " + CURRENT_PROTOCOL_VERSION);
    }
    if (messageId == null || !isValidUuid(messageId)) {
      throw new IllegalArgumentException("Invalid messageId: must be a valid lowercase UUID, got: " + messageId);
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (type.isResponse()) {
      if (!isValidUuid(correlationId)) {
        throw new IllegalArgumentException(
            "Response phải có correlationId là UUID hợp lệ");
      }
    } else {
      if (correlationId != null) {
        throw new IllegalArgumentException(
            "Request phải có correlationId = null");
      }
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (senderId == null || senderId.isBlank()) {
      throw new IllegalArgumentException("senderId must not be null or blank");
    }
    if (sentAt == null || sentAt.isBlank()) {
      throw new IllegalArgumentException("sentAt must not be null or blank");
    }
    if (!UTC_TIMESTAMP_PATTERN.matcher(sentAt).matches()) {
      throw new IllegalArgumentException(
          "sentAt phải là UTC ISO-8601 với millisecond "
              + "hoặc dạng rút gọn không có phần thập phân");
    }
    try {
      Instant.parse(sentAt);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid sentAt: must be valid ISO-8601 UTC timestamp, got: " + sentAt, e);
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    final JsonNode payloadNode;

    try {
      payloadNode = PAYLOAD_MAPPER.valueToTree(payload);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "payload không thể chuyển thành JSON object", e);
    }

    if (payloadNode == null || !payloadNode.isObject()) {
      throw new IllegalArgumentException(
          "payload bắt buộc phải là JSON object");
    }
  }

  public static boolean isValidUuid(String val) {
    return val != null && UUID_PATTERN.matcher(val).matches();
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(int protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public String getMessageId() {
    return messageId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public MessageType getType() {
    return type;
  }

  public void setType(MessageType type) {
    this.type = type;
  }

  public String getSenderId() {
    return senderId;
  }

  public void setSenderId(String senderId) {
    this.senderId = senderId;
  }

  public String getSentAt() {
    return sentAt;
  }

  public void setSentAt(String sentAt) {
    this.sentAt = sentAt;
  }

  public T getPayload() {
    return payload;
  }

  public void setPayload(T payload) {
    this.payload = payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    MessageEnvelope<?> that = (MessageEnvelope<?>) o;
    return protocolVersion == that.protocolVersion &&
        Objects.equals(messageId, that.messageId) &&
        Objects.equals(correlationId, that.correlationId) &&
        type == that.type &&
        Objects.equals(senderId, that.senderId) &&
        Objects.equals(sentAt, that.sentAt) &&
        Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(protocolVersion, messageId, correlationId, type, senderId, sentAt, payload);
  }

  @Override
  public String toString() {
    return "MessageEnvelope{" +
        "protocolVersion=" + protocolVersion +
        ", messageId='" + messageId + '\'' +
        ", correlationId='" + correlationId + '\'' +
        ", type=" + type +
        ", senderId='" + senderId + '\'' +
        ", sentAt='" + sentAt + '\'' +
        ", payload=" + payload +
        '}';
  }
}
