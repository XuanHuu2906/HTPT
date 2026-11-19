package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Response payload for GET_REQUEST_RESULT query according to SPEC-03 Section 4.4.
 */
public class GetRequestResultResponseDto {

    public static final String STATE_APPLIED_SUCCESS = "APPLIED_SUCCESS";
    public static final String STATE_APPLIED_REJECTED = "APPLIED_REJECTED";
    public static final String STATE_PENDING = "PENDING";
    public static final String STATE_UNKNOWN = "UNKNOWN";

    private static final Set<String> ALLOWED_STATES = Set.of(
            STATE_APPLIED_SUCCESS,
            STATE_APPLIED_REJECTED,
            STATE_PENDING,
            STATE_UNKNOWN
    );

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    );

    private static final Pattern TICKET_ID_PATTERN = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

    @JsonProperty("requestId")
    private String requestId;

    @JsonProperty("state")
    private String state;

    @JsonProperty("commandType")
    private String commandType;

    @JsonProperty("ticketId")
    private String ticketId;

    @JsonProperty("reasonCode")
    private String reasonCode;

    @JsonProperty("appliedLogIndex")
    private Long appliedLogIndex;

    public GetRequestResultResponseDto() {
    }

    @JsonCreator
    public GetRequestResultResponseDto(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("state") String state,
            @JsonProperty("commandType") String commandType,
            @JsonProperty("ticketId") String ticketId,
            @JsonProperty("reasonCode") String reasonCode,
            @JsonProperty("appliedLogIndex") Long appliedLogIndex) {
        this.requestId = requestId;
        this.state = state;
        this.commandType = commandType;
        this.ticketId = ticketId;
        this.reasonCode = reasonCode;
        this.appliedLogIndex = appliedLogIndex;
    }

    public static GetRequestResultResponseDto appliedSuccess(String requestId, String commandType, String ticketId, long appliedLogIndex) {
        return new GetRequestResultResponseDto(requestId, STATE_APPLIED_SUCCESS, commandType, ticketId, null, appliedLogIndex);
    }

    public static GetRequestResultResponseDto appliedRejected(String requestId, String commandType, String ticketId, String reasonCode, long appliedLogIndex) {
        return new GetRequestResultResponseDto(requestId, STATE_APPLIED_REJECTED, commandType, ticketId, reasonCode, appliedLogIndex);
    }

    public static GetRequestResultResponseDto pending(String requestId, String commandType) {
        return new GetRequestResultResponseDto(requestId, STATE_PENDING, commandType, null, null, null);
    }

    public static GetRequestResultResponseDto unknown(String requestId) {
        return new GetRequestResultResponseDto(requestId, STATE_UNKNOWN, null, null, null, null);
    }

    /**
     * Validates field combinations according to SPEC-03 Section 4.4 table and invariants.
     */
    public void validate() {
        if (requestId == null || !UUID_PATTERN.matcher(requestId.strip()).matches()) {
            throw new IllegalArgumentException("requestId must be a valid lowercase UUID, got: " + requestId);
        }
        if (state == null || !ALLOWED_STATES.contains(state)) {
            throw new IllegalArgumentException("state must be APPLIED_SUCCESS, APPLIED_REJECTED, PENDING, or UNKNOWN, got: " + state);
        }
        if (ticketId != null && !TICKET_ID_PATTERN.matcher(ticketId).matches()) {
            throw new IllegalArgumentException("ticketId must match [A-Za-z0-9-]{1,64}, got: " + ticketId);
        }

        switch (state) {
            case STATE_APPLIED_SUCCESS -> {
                if (commandType == null || commandType.isBlank()) {
                    throw new IllegalArgumentException("commandType must not be null when APPLIED_SUCCESS");
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException("appliedLogIndex must be >= 1 when APPLIED_SUCCESS, got: " + appliedLogIndex);
                }
                if (reasonCode != null) {
                    throw new IllegalArgumentException("reasonCode must be null when APPLIED_SUCCESS, got: " + reasonCode);
                }
                if ("BOOK_TICKET".equals(commandType) && ticketId == null) {
                    throw new IllegalArgumentException("ticketId must not be null for BOOK_TICKET with APPLIED_SUCCESS");
                }
            }
            case STATE_APPLIED_REJECTED -> {
                if (commandType == null || commandType.isBlank()) {
                    throw new IllegalArgumentException("commandType must not be null when APPLIED_REJECTED");
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException("appliedLogIndex must be >= 1 when APPLIED_REJECTED, got: " + appliedLogIndex);
                }
                if (reasonCode == null || reasonCode.isBlank()) {
                    throw new IllegalArgumentException("reasonCode must not be null or blank when APPLIED_REJECTED");
                }
                if ("BOOK_TICKET".equals(commandType) && ticketId != null) {
                    throw new IllegalArgumentException("ticketId must be null for BOOK_TICKET with APPLIED_REJECTED, got: " + ticketId);
                }
            }
            case STATE_PENDING -> {
                if (commandType == null || commandType.isBlank()) {
                    throw new IllegalArgumentException("commandType must not be null when PENDING");
                }
                if (ticketId != null) {
                    throw new IllegalArgumentException("ticketId must be null when PENDING");
                }
                if (reasonCode != null) {
                    throw new IllegalArgumentException("reasonCode must be null when PENDING");
                }
                if (appliedLogIndex != null) {
                    throw new IllegalArgumentException("appliedLogIndex must be null when PENDING");
                }
            }
            case STATE_UNKNOWN -> {
                if (commandType != null) {
                    throw new IllegalArgumentException("commandType must be null when UNKNOWN");
                }
                if (ticketId != null) {
                    throw new IllegalArgumentException("ticketId must be null when UNKNOWN");
                }
                if (reasonCode != null) {
                    throw new IllegalArgumentException("reasonCode must be null when UNKNOWN");
                }
                if (appliedLogIndex != null) {
                    throw new IllegalArgumentException("appliedLogIndex must be null when UNKNOWN");
                }
            }
        }
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCommandType() {
        return commandType;
    }

    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }

    public String getTicketId() {
        return ticketId;
    }

    public void setTicketId(String ticketId) {
        this.ticketId = ticketId;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public void setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public Long getAppliedLogIndex() {
        return appliedLogIndex;
    }

    public void setAppliedLogIndex(Long appliedLogIndex) {
        this.appliedLogIndex = appliedLogIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetRequestResultResponseDto that = (GetRequestResultResponseDto) o;
        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(state, that.state) &&
                Objects.equals(commandType, that.commandType) &&
                Objects.equals(ticketId, that.ticketId) &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(appliedLogIndex, that.appliedLogIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, state, commandType, ticketId, reasonCode, appliedLogIndex);
    }

    @Override
    public String toString() {
        return "GetRequestResultResponseDto{" +
                "requestId='" + requestId + '\'' +
                ", state='" + state + '\'' +
                ", commandType='" + commandType + '\'' +
                ", ticketId='" + ticketId + '\'' +
                ", reasonCode='" + reasonCode + '\'' +
                ", appliedLogIndex=" + appliedLogIndex +
                '}';
    }
}
