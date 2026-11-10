package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Payload DTO for ERROR_RESPONSE according to SPEC-03 Section 7.
 *
 * <p>All 5 fields must be present in JSON:
 * <ul>
 *   <li>errorCode: non-null string code (e.g. INVALID_REQUEST, UNSUPPORTED_VERSION, NOT_LEADER)</li>
 *   <li>message: non-null human-readable error description</li>
 *   <li>leaderHint: nullable node ID hint (e.g. "node-3" or null)</li>
 *   <li>term: nullable current Raft term (or null if not yet initialized)</li>
 *   <li>requestId: nullable client business request UUID (or null if error occurred before/without requestId)</li>
 * </ul>
 */
public class ErrorResponseDto {

    @JsonProperty("errorCode")
    private String errorCode;

    @JsonProperty("message")
    private String message;

    @JsonProperty("leaderHint")
    private String leaderHint;

    @JsonProperty("term")
    private Long term;

    @JsonProperty("requestId")
    private String requestId;

    public ErrorResponseDto() {
    }

    @JsonCreator
    public ErrorResponseDto(
            @JsonProperty("errorCode") String errorCode,
            @JsonProperty("message") String message,
            @JsonProperty("leaderHint") String leaderHint,
            @JsonProperty("term") Long term,
            @JsonProperty("requestId") String requestId) {
        this.errorCode = errorCode;
        this.message = message;
        this.leaderHint = leaderHint;
        this.term = term;
        this.requestId = requestId;
    }

    /**
     * Factory method for client request validation and syntax errors.
     */
    public static ErrorResponseDto invalidRequest(String message, String requestId) {
        return new ErrorResponseDto("INVALID_REQUEST", message, null, null, requestId);
    }

    /**
     * Factory method for unsupported protocol versions.
     */
    public static ErrorResponseDto unsupportedVersion(String message) {
        return new ErrorResponseDto("UNSUPPORTED_VERSION", message, null, null, null);
    }

    /**
     * Validates fields according to SPEC-03 Section 7 rules.
     */
    public void validate() {
        if (errorCode == null || errorCode.isBlank()) {
            throw new IllegalArgumentException("errorCode must not be null or blank");
        }
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be null or blank");
        }
        if (term != null && term < 0) {
            throw new IllegalArgumentException("term must be >= 0 when present, got: " + term);
        }
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getLeaderHint() {
        return leaderHint;
    }

    public void setLeaderHint(String leaderHint) {
        this.leaderHint = leaderHint;
    }

    public Long getTerm() {
        return term;
    }

    public void setTerm(Long term) {
        this.term = term;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorResponseDto that = (ErrorResponseDto) o;
        return Objects.equals(errorCode, that.errorCode) &&
                Objects.equals(message, that.message) &&
                Objects.equals(leaderHint, that.leaderHint) &&
                Objects.equals(term, that.term) &&
                Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorCode, message, leaderHint, term, requestId);
    }

    @Override
    public String toString() {
        return "ErrorResponseDto{" +
                "errorCode='" + errorCode + '\'' +
                ", message='" + message + '\'' +
                ", leaderHint='" + leaderHint + '\'' +
                ", term=" + term +
                ", requestId='" + requestId + '\'' +
                '}';
    }
}
