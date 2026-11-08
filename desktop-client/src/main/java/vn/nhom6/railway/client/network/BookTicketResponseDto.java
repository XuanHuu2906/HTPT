package vn.nhom6.railway.client.network;

import java.util.Objects;

/**
 * Response payload cho BOOK_TICKET theo SPEC-03 Section 4.2.
 */
public class BookTicketResponseDto {
    private final String requestId;
    private final String outcome; // "CONFIRMED", "REJECTED", "UNKNOWN"
    private final String ticketId;
    private final String tripId;
    private final String seatId;
    private final String reasonCode; // e.g. "SEAT_ALREADY_BOOKED", "TIMEOUT_UNKNOWN"
    private final Long appliedLogIndex;
    private final boolean mock;
    private final String leaderHint; // nullable, e.g. "node-2"
    private final String message;

    public BookTicketResponseDto(String requestId, String outcome, String ticketId, String tripId,
                                 String seatId, String reasonCode, Long appliedLogIndex,
                                 boolean mock, String leaderHint, String message) {
        this.requestId = requestId;
        this.outcome = outcome;
        this.ticketId = ticketId;
        this.tripId = tripId;
        this.seatId = seatId;
        this.reasonCode = reasonCode;
        this.appliedLogIndex = appliedLogIndex;
        this.mock = mock;
        this.leaderHint = leaderHint;
        this.message = message;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getOutcome() {
        return outcome;
    }

    public String getTicketId() {
        return ticketId;
    }

    public String getTripId() {
        return tripId;
    }

    public String getSeatId() {
        return seatId;
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

    public String getLeaderHint() {
        return leaderHint;
    }

    public String getMessage() {
        return message;
    }

    public boolean isConfirmed() {
        return "CONFIRMED".equalsIgnoreCase(outcome);
    }

    public boolean isRejected() {
        return "REJECTED".equalsIgnoreCase(outcome);
    }

    public boolean isUnknown() {
        return "UNKNOWN".equalsIgnoreCase(outcome);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookTicketResponseDto that = (BookTicketResponseDto) o;
        return mock == that.mock &&
                Objects.equals(requestId, that.requestId) &&
                Objects.equals(outcome, that.outcome) &&
                Objects.equals(ticketId, that.ticketId) &&
                Objects.equals(tripId, that.tripId) &&
                Objects.equals(seatId, that.seatId) &&
                Objects.equals(reasonCode, that.reasonCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, outcome, ticketId, tripId, seatId, reasonCode, mock);
    }

    @Override
    public String toString() {
        return "BookTicketResponseDto{" +
                "requestId='" + requestId + '\'' +
                ", outcome='" + outcome + '\'' +
                ", ticketId='" + ticketId + '\'' +
                ", seatId='" + seatId + '\'' +
                ", reasonCode='" + reasonCode + '\'' +
                ", mock=" + mock +
                ", leaderHint='" + leaderHint + '\'' +
                '}';
    }
}
