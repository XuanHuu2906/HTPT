package vn.nhom6.railway.client.network;

import java.util.Objects;

/**
 * Response payload cho GET_REQUEST_RESULT theo SPEC-03 Section 4.4.
 */
public class GetRequestResultResponseDto {
    private final String requestId;
    private final String state; // "APPLIED_SUCCESS", "APPLIED_REJECTED", "PENDING", "UNKNOWN"
    private final String commandType; // "BOOK_TICKET", "CANCEL_TICKET"
    private final String ticketId;
    private final String reasonCode;
    private final Long appliedLogIndex;
    private final boolean mock;

    public GetRequestResultResponseDto(String requestId, String state, String commandType,
                                       String ticketId, String reasonCode, Long appliedLogIndex,
                                       boolean mock) {
        this.requestId = requestId;
        this.state = state;
        this.commandType = commandType;
        this.ticketId = ticketId;
        this.reasonCode = reasonCode;
        this.appliedLogIndex = appliedLogIndex;
        this.mock = mock;
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
                ", mock=" + mock +
                '}';
    }
}
