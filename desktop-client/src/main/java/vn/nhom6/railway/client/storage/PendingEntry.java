package vn.nhom6.railway.client.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

/**
 * Bản ghi bất biến đại diện cho một yêu cầu nghiệp vụ được lưu trong journal (TASK TV1-D14).
 * <p>
 * Lưu trữ nguyên văn {@code payload} để đảm bảo khi retry sẽ gửi chính xác từng byte,
 * kèm {@code payloadHash} (SHA-256) để phát hiện hư hỏng hoặc can thiệp dữ liệu ngoài ý muốn.
 */
public final class PendingEntry {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("requestId")
    private final String requestId;

    @JsonProperty("commandType")
    private final String commandType;

    @JsonProperty("payload")
    private final String payload;

    @JsonProperty("payloadHash")
    private final String payloadHash;

    @JsonProperty("state")
    private final PendingState state;

    @JsonProperty("createdAt")
    private final String createdAt;

    @JsonProperty("lastAttemptAt")
    private final String lastAttemptAt;

    @JsonProperty("attemptCount")
    private final int attemptCount;

    @JsonProperty("lastKnownNodeId")
    private final String lastKnownNodeId;

    @JsonProperty("finalOutcome")
    private final String finalOutcome;

    @JsonProperty("schemaVersion")
    private final int schemaVersion;

    @JsonCreator
    public PendingEntry(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("commandType") String commandType,
            @JsonProperty("payload") String payload,
            @JsonProperty("payloadHash") String payloadHash,
            @JsonProperty("state") PendingState state,
            @JsonProperty("createdAt") String createdAt,
            @JsonProperty("lastAttemptAt") String lastAttemptAt,
            @JsonProperty("attemptCount") int attemptCount,
            @JsonProperty("lastKnownNodeId") String lastKnownNodeId,
            @JsonProperty("finalOutcome") String finalOutcome,
            @JsonProperty("schemaVersion") int schemaVersion) {
        this.requestId = Objects.requireNonNull(requestId, "requestId không được null");
        this.commandType = Objects.requireNonNull(commandType, "commandType không được null");
        this.payload = Objects.requireNonNull(payload, "payload không được null");
        this.payloadHash = (payloadHash != null) ? payloadHash : computePayloadHash(payload);
        this.state = (state != null) ? state : PendingState.PREPARED;
        this.createdAt = (createdAt != null) ? createdAt : Instant.now().toString();
        this.lastAttemptAt = lastAttemptAt;
        this.attemptCount = Math.max(0, attemptCount);
        this.lastKnownNodeId = lastKnownNodeId;
        this.finalOutcome = finalOutcome;
        this.schemaVersion = (schemaVersion > 0) ? schemaVersion : 1;
    }

    /**
     * Constructor thuận tiện nhận Instant.
     */
    public PendingEntry(
            String requestId,
            String commandType,
            String payload,
            String payloadHash,
            PendingState state,
            Instant createdAt,
            Instant lastAttemptAt,
            int attemptCount,
            String lastKnownNodeId,
            String finalOutcome,
            int schemaVersion) {
        this(requestId, commandType, payload, payloadHash, state,
                createdAt != null ? createdAt.toString() : Instant.now().toString(),
                lastAttemptAt != null ? lastAttemptAt.toString() : null,
                attemptCount, lastKnownNodeId, finalOutcome, schemaVersion);
    }

    /**
     * Tạo bản ghi mới ở trạng thái PREPARED.
     */
    public static PendingEntry createPrepared(String requestId, String commandType, String payload) {
        String hash = computePayloadHash(payload);
        return new PendingEntry(requestId, commandType, payload, hash, PendingState.PREPARED,
                Instant.now(), null, 0, null, null, 1);
    }

    /**
     * Tính toán mã băm SHA-256 từ chuỗi JSON payload nguyên văn.
     */
    public static String computePayloadHash(String payload) {
        if (payload == null) {
            return "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Hệ thống không hỗ trợ thuật toán SHA-256", e);
        }
    }

    /**
     * Kiểm tra tính toàn vẹn của payload với payloadHash.
     */
    @JsonIgnore
    public boolean isPayloadValid() {
        return payload != null && computePayloadHash(payload).equalsIgnoreCase(payloadHash);
    }

    /**
     * Ghi nhận lần gửi mới (chuyển sang SENT_UNRESOLVED, tăng số lần thử).
     */
    public PendingEntry withAttempt(Instant attemptTime, String nodeId) {
        return new PendingEntry(
                this.requestId,
                this.commandType,
                this.payload,
                this.payloadHash,
                PendingState.SENT_UNRESOLVED,
                this.createdAt,
                attemptTime != null ? attemptTime.toString() : Instant.now().toString(),
                this.attemptCount + 1,
                nodeId != null ? nodeId : this.lastKnownNodeId,
                this.finalOutcome,
                this.schemaVersion
        );
    }

    /**
     * Ghi nhận kết quả cuối cùng từ Node (chuyển sang RESOLVED).
     * Chỉ chấp nhận outcome là CONFIRMED hoặc REJECTED (hoặc APPLIED_SUCCESS/APPLIED_REJECTED).
     */
    public PendingEntry withResolved(String outcome, Instant resolvedAt) {
        if (outcome == null || (!outcome.equals("CONFIRMED") && !outcome.equals("REJECTED")
                && !outcome.equals("APPLIED_SUCCESS") && !outcome.equals("APPLIED_REJECTED"))) {
            throw new IllegalArgumentException("Không thể chuyển sang RESOLVED với outcome không hợp lệ: " + outcome);
        }
        String normalizedOutcome = outcome.equals("APPLIED_SUCCESS") ? "CONFIRMED"
                : (outcome.equals("APPLIED_REJECTED") ? "REJECTED" : outcome);

        return new PendingEntry(
                this.requestId,
                this.commandType,
                this.payload,
                this.payloadHash,
                PendingState.RESOLVED,
                this.createdAt,
                resolvedAt != null ? resolvedAt.toString() : Instant.now().toString(),
                this.attemptCount,
                this.lastKnownNodeId,
                normalizedOutcome,
                this.schemaVersion
        );
    }

    /**
     * Chuyển trạng thái tùy biến (không cho phép tự chuyển sang RESOLVED mà không có outcome).
     */
    public PendingEntry withState(PendingState newState) {
        if (newState == PendingState.RESOLVED && (this.finalOutcome == null || this.finalOutcome.isBlank())) {
            throw new IllegalStateException("Không được phép chuyển sang RESOLVED nếu chưa có outcome cuối cùng từ server");
        }
        return new PendingEntry(
                this.requestId,
                this.commandType,
                this.payload,
                this.payloadHash,
                newState,
                this.createdAt,
                this.lastAttemptAt,
                this.attemptCount,
                this.lastKnownNodeId,
                this.finalOutcome,
                this.schemaVersion
        );
    }

    /**
     * Trích xuất tóm tắt nghiệp vụ (ví dụ: "Ghế G05 (SE-001)") mà KHÔNG làm lộ tên hành khách.
     */
    @JsonIgnore
    public String getSummary() {
        if (payload == null || payload.isBlank()) {
            return commandType;
        }
        try {
            JsonNode node = OBJECT_MAPPER.readTree(payload);
            String seat = node.has("seatId") ? node.get("seatId").asText() : "";
            String trip = node.has("tripId") ? node.get("tripId").asText() : "";
            if (!seat.isEmpty()) {
                return "Ghế " + seat + (trip.isEmpty() ? "" : " (" + trip + ")");
            }
            if (node.has("ticketId")) {
                return "Vé " + node.get("ticketId").asText();
            }
        } catch (Exception ignored) {
            // Không ngắt luồng nếu parse lỗi
        }
        return commandType;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getPayload() {
        return payload;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    public PendingState getState() {
        return state;
    }

    @JsonIgnore
    public Instant getCreatedAt() {
        return createdAt != null ? Instant.parse(createdAt) : null;
    }

    @JsonProperty("createdAt")
    public String getCreatedAtString() {
        return createdAt;
    }

    @JsonIgnore
    public Instant getLastAttemptAt() {
        return lastAttemptAt != null ? Instant.parse(lastAttemptAt) : null;
    }

    @JsonProperty("lastAttemptAt")
    public String getLastAttemptAtString() {
        return lastAttemptAt;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public String getLastKnownNodeId() {
        return lastKnownNodeId;
    }

    public String getFinalOutcome() {
        return finalOutcome;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PendingEntry that = (PendingEntry) o;
        return attemptCount == that.attemptCount &&
                schemaVersion == that.schemaVersion &&
                Objects.equals(requestId, that.requestId) &&
                Objects.equals(commandType, that.commandType) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(payloadHash, that.payloadHash) &&
                state == that.state &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(lastAttemptAt, that.lastAttemptAt) &&
                Objects.equals(lastKnownNodeId, that.lastKnownNodeId) &&
                Objects.equals(finalOutcome, that.finalOutcome);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, commandType, payloadHash, state);
    }

    @Override
    public String toString() {
        // Tuân thủ quy tắc bảo mật riêng tư: Không in nội dung payload ra toString()
        return "PendingEntry{" +
                "requestId='" + requestId + '\'' +
                ", commandType='" + commandType + '\'' +
                ", state=" + state +
                ", attempts=" + attemptCount +
                ", payloadBytes=" + (payload != null ? payload.length() : 0) +
                ", outcome=" + finalOutcome +
                '}';
    }
}
