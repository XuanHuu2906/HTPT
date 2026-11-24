package vn.nhom6.railway.client.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response payload cho GET_REQUEST_RESULT theo SPEC-03 Section 4.4.
 */
public class GetRequestResultResponseDto {

    public static final String STATE_APPLIED_SUCCESS = "APPLIED_SUCCESS";
    public static final String STATE_APPLIED_REJECTED = "APPLIED_REJECTED";
    public static final String STATE_PENDING = "PENDING";
    public static final String STATE_UNKNOWN = "UNKNOWN";

    private final String requestId;
    private final String state; // "APPLIED_SUCCESS", "APPLIED_REJECTED", "PENDING", "UNKNOWN"
    private final String commandType; // "BOOK_TICKET", "CANCEL_TICKET"
    private final String ticketId;
    private final String reasonCode;
    private final Long appliedLogIndex;
    private final boolean mock;

    @JsonCreator
    public GetRequestResultResponseDto(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("state") String state,
            @JsonProperty("commandType") String commandType,
            @JsonProperty("ticketId") String ticketId,
            @JsonProperty("reasonCode") String reasonCode,
            @JsonProperty("appliedLogIndex") Long appliedLogIndex,
            @JsonProperty("mock") boolean mock) {
        this.requestId = requestId;
        this.state = state;
        this.commandType = commandType;
        this.ticketId = ticketId;
        this.reasonCode = reasonCode;
        this.appliedLogIndex = appliedLogIndex;
        this.mock = mock;
    }

    public GetRequestResultResponseDto(String requestId, String state, String commandType,
                                       String ticketId, String reasonCode, Long appliedLogIndex) {
        this(requestId, state, commandType, ticketId, reasonCode, appliedLogIndex, false);
    }

    public static GetRequestResultResponseDto unknown(String requestId, String reasonCode) {
        return new GetRequestResultResponseDto(requestId, STATE_UNKNOWN, null, null, reasonCode, null, false);
    }

    public static GetRequestResultResponseDto unknown(String requestId) {
        return unknown(requestId, null);
    }

    public String getRequestId() {
        return requestId;
    }

    public String getState() {
        return state;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getTicketId() {
        return ticketId;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public Long getAppliedLogIndex() {
        return appliedLogIndex;
    }

    public boolean isMock() {
        return mock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetRequestResultResponseDto that = (GetRequestResultResponseDto) o;
        return mock == that.mock &&
                Objects.equals(requestId, that.requestId) &&
                Objects.equals(state, that.state) &&
                Objects.equals(commandType, that.commandType) &&
                Objects.equals(ticketId, that.ticketId) &&
                Objects.equals(reasonCode, that.reasonCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, state, commandType, ticketId, reasonCode, mock);
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
                ", mock=" + mock +
                '}';
    }
}
