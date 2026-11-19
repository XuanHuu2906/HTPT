package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Request payload for GET_REQUEST_RESULT query according to SPEC-03 Section 4.4.
 *
 * <p>Queries the durable business outcome or in-flight status of a previously submitted command.
 */
public class GetRequestResultRequestDto {

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    );

    @JsonProperty("requestId")
    private String requestId;

    public GetRequestResultRequestDto() {
    }

    @JsonCreator
    public GetRequestResultRequestDto(@JsonProperty("requestId") String requestId) {
        this.requestId = requestId;
    }

    /**
     * Validates that requestId is present and is a valid lowercase UUID.
     */
    public void validate() {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be null or blank");
        }
        if (!UUID_PATTERN.matcher(requestId.strip()).matches()) {
            throw new IllegalArgumentException("requestId must be a valid lowercase UUID (8-4-4-4-12), got: " + requestId);
        }
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
        GetRequestResultRequestDto that = (GetRequestResultRequestDto) o;
        return Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }

    @Override
    public String toString() {
        return "GetRequestResultRequestDto{" +
                "requestId='" + requestId + '\'' +
                '}';
    }
}
