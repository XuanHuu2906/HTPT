package vn.nhom6.railway.client.network;

import java.util.Objects;

/**
 * Request payload cho CANCEL_TICKET theo SPEC-03 Section 4.3.
 */
public class CancelTicketRequestDto {
    private final String requestId;
    private final String ticketId;

    public CancelTicketRequestDto(String requestId, String ticketId) {
        this.requestId = requestId;
        this.ticketId = ticketId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getTicketId() {
        return ticketId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelTicketRequestDto that = (CancelTicketRequestDto) o;
        return Objects.equals(requestId, that.requestId) && Objects.equals(ticketId, that.ticketId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, ticketId);
    }

    @Override
    public String toString() {
        return "CancelTicketRequestDto{requestId='" + requestId + "', ticketId='" + ticketId + "'}";
    }
}
