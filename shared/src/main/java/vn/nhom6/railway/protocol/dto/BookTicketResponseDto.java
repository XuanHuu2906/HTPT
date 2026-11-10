package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Response payload for BOOK_TICKET message according to SPEC-03 Section 4.2.
 */
public class BookTicketResponseDto {

    public static final String DEFAULT_TRIP_ID = "SE-001";
    public static final String OUTCOME_CONFIRMED = "CONFIRMED";
    public static final String OUTCOME_REJECTED = "REJECTED";
    public static final String OUTCOME_UNKNOWN = "UNKNOWN";

    public static final String REASON_SEAT_ALREADY_BOOKED = "SEAT_ALREADY_BOOKED";
    public static final String REASON_INVALID_REQUEST = "INVALID_REQUEST";
    public static final String REASON_TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN";

    private static final Set<String> ALLOWED_OUTCOMES = Set.of(
            OUTCOME_CONFIRMED, OUTCOME_REJECTED, OUTCOME_UNKNOWN
    );

    private static final Set<String> REJECTED_REASONS = Set.of(
            REASON_SEAT_ALREADY_BOOKED, REASON_INVALID_REQUEST
    );

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    );

    private static final Pattern SEAT_PATTERN = Pattern.compile("^G(0[1-9]|10)$");
    private static final Pattern TICKET_ID_PATTERN = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

    @JsonProperty("requestId")
    private String requestId;

    @JsonProperty("outcome")
    private String outcome;

    @JsonProperty("ticketId")
    private String ticketId;

    @JsonProperty("tripId")
    private String tripId;

    @JsonProperty("seatId")
    private String seatId;

    @JsonProperty("reasonCode")
    private String reasonCode;

    @JsonProperty("appliedLogIndex")
    private Long appliedLogIndex;

    public BookTicketResponseDto() {
        this.tripId = DEFAULT_TRIP_ID;
    }

    @JsonCreator
    public BookTicketResponseDto(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("outcome") String outcome,
            @JsonProperty("ticketId") String ticketId,
            @JsonProperty("tripId") String tripId,
            @JsonProperty("seatId") String seatId,
            @JsonProperty("reasonCode") String reasonCode,
            @JsonProperty("appliedLogIndex") Long appliedLogIndex) {
        this.requestId = requestId;
        this.outcome = outcome;
        this.ticketId = ticketId;
        this.tripId = tripId != null ? tripId : DEFAULT_TRIP_ID;
        this.seatId = seatId;
        this.reasonCode = reasonCode;
        this.appliedLogIndex = appliedLogIndex;
    }

    /**
     * Factory for a CONFIRMED booking response.
     */
    public static BookTicketResponseDto confirmed(String requestId, String tripId, String seatId, String ticketId, long appliedLogIndex) {
        return new BookTicketResponseDto(requestId, OUTCOME_CONFIRMED, ticketId, tripId, seatId, null, appliedLogIndex);
    }

    /**
     * Factory for a REJECTED booking response.
     */
    public static BookTicketResponseDto rejected(String requestId, String tripId, String seatId, String reasonCode, long appliedLogIndex) {
        return new BookTicketResponseDto(requestId, OUTCOME_REJECTED, null, tripId, seatId, reasonCode, appliedLogIndex);
    }

    /**
     * Factory for an UNKNOWN booking response.
     */
    public static BookTicketResponseDto unknown(String requestId, String tripId, String seatId) {
        return new BookTicketResponseDto(requestId, OUTCOME_UNKNOWN, null, tripId, seatId, REASON_TIMEOUT_UNKNOWN, null);
    }

    /**
     * Validates field combinations according to SPEC-03 Section 4.2 table.
     */
    public void validate() {
        if (requestId == null || !UUID_PATTERN.matcher(requestId).matches()) {
            throw new IllegalArgumentException("requestId must be a valid lowercase UUID, got: " + requestId);
        }
        if (tripId == null || !DEFAULT_TRIP_ID.equals(tripId.strip())) {
            throw new IllegalArgumentException("tripId must be " + DEFAULT_TRIP_ID + ", got: " + tripId);
        }
        if (seatId == null || !SEAT_PATTERN.matcher(seatId.strip()).matches()) {
            throw new IllegalArgumentException("seatId must be within G01-G10, got: " + seatId);
        }
        if (outcome == null || !ALLOWED_OUTCOMES.contains(outcome)) {
            throw new IllegalArgumentException("outcome must be CONFIRMED, REJECTED, or UNKNOWN, got: " + outcome);
        }

        switch (outcome) {
            case OUTCOME_CONFIRMED -> {
                if (ticketId == null || !TICKET_ID_PATTERN.matcher(ticketId).matches()) {
                    throw new IllegalArgumentException("ticketId must be valid ASCII [A-Za-z0-9-]{1,64} when CONFIRMED, got: " + ticketId);
                }
                if (reasonCode != null) {
                    throw new IllegalArgumentException("reasonCode must be null when CONFIRMED, got: " + reasonCode);
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException("appliedLogIndex must be >= 1 when CONFIRMED, got: " + appliedLogIndex);
                }
            }
            case OUTCOME_REJECTED -> {
                if (ticketId != null) {
                    throw new IllegalArgumentException("ticketId must be null when REJECTED, got: " + ticketId);
                }
                if (reasonCode == null || !REJECTED_REASONS.contains(reasonCode)) {
                    throw new IllegalArgumentException("reasonCode must be in " + REJECTED_REASONS + " when REJECTED, got: " + reasonCode);
                }
                if (appliedLogIndex == null || appliedLogIndex < 1) {
                    throw new IllegalArgumentException("appliedLogIndex must be >= 1 when REJECTED, got: " + appliedLogIndex);
                }
            }
            case OUTCOME_UNKNOWN -> {
                if (ticketId != null) {
                    throw new IllegalArgumentException("ticketId must be null when UNKNOWN, got: " + ticketId);
                }
                if (!REASON_TIMEOUT_UNKNOWN.equals(reasonCode)) {
                    throw new IllegalArgumentException("reasonCode must be " + REASON_TIMEOUT_UNKNOWN + " when UNKNOWN, got: " + reasonCode);
                }
                if (appliedLogIndex != null) {
                    throw new IllegalArgumentException("appliedLogIndex must be null when UNKNOWN, got: " + appliedLogIndex);
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

    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    public String getTicketId() {
        return ticketId;
    }

    public void setTicketId(String ticketId) {
        this.ticketId = ticketId;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getSeatId() {
        return seatId;
    }

    public void setSeatId(String seatId) {
        this.seatId = seatId;
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

    @JsonIgnore
    public boolean isConfirmed() {
        return OUTCOME_CONFIRMED.equalsIgnoreCase(outcome);
    }

    @JsonIgnore
    public boolean isRejected() {
        return OUTCOME_REJECTED.equalsIgnoreCase(outcome);
    }

    @JsonIgnore
    public boolean isUnknown() {
        return OUTCOME_UNKNOWN.equalsIgnoreCase(outcome);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookTicketResponseDto that = (BookTicketResponseDto) o;
        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(outcome, that.outcome) &&
                Objects.equals(ticketId, that.ticketId) &&
                Objects.equals(tripId, that.tripId) &&
                Objects.equals(seatId, that.seatId) &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(appliedLogIndex, that.appliedLogIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, outcome, ticketId, tripId, seatId, reasonCode, appliedLogIndex);
    }

    @Override
    public String toString() {
        return "BookTicketResponseDto{" +
                "requestId='" + requestId + '\'' +
                ", outcome='" + outcome + '\'' +
                ", ticketId='" + ticketId + '\'' +
                ", tripId='" + tripId + '\'' +
                ", seatId='" + seatId + '\'' +
                ", reasonCode='" + reasonCode + '\'' +
                ", appliedLogIndex=" + appliedLogIndex +
                '}';
    }
}
