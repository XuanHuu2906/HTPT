package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Request payload for BOOK_TICKET message according to SPEC-03 Section 4.2.
 */
public class BookTicketRequestDto {

    public static final String DEFAULT_TRIP_ID = "SE-001";

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    );

    private static final Pattern SEAT_PATTERN = Pattern.compile("^G(0[1-9]|10)$");

    @JsonProperty("requestId")
    private String requestId;

    @JsonProperty("tripId")
    private String tripId;

    @JsonProperty("seatId")
    private String seatId;

    @JsonProperty("passengerName")
    private String passengerName;

    public BookTicketRequestDto() {
        this.tripId = DEFAULT_TRIP_ID;
    }

    @JsonCreator
    public BookTicketRequestDto(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("tripId") String tripId,
            @JsonProperty("seatId") String seatId,
            @JsonProperty("passengerName") String passengerName) {
        this.requestId = requestId;
        this.tripId = tripId;
        this.seatId = seatId;
        this.passengerName = passengerName;
    }

    /**
     * Validates fields according to SPEC-03 Section 4.2 and SPEC-05 Section 2.
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
        if (passengerName == null) {
            throw new IllegalArgumentException("passengerName must not be null");
        }
        String strippedName = passengerName.strip();
        if (strippedName.isEmpty()) {
            throw new IllegalArgumentException("passengerName must not be blank");
        }
        if (strippedName.contains("\u0000")) {
            throw new IllegalArgumentException("passengerName must not contain U+0000");
        }
        int codePointCount = strippedName.codePointCount(0, strippedName.length());
        if (codePointCount > 120) {
            throw new IllegalArgumentException("passengerName must have between 1 and 120 code points, got: " + codePointCount);
        }
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
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

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookTicketRequestDto that = (BookTicketRequestDto) o;
        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(tripId, that.tripId) &&
                Objects.equals(seatId, that.seatId) &&
                Objects.equals(passengerName, that.passengerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, tripId, seatId, passengerName);
    }

    @Override
    public String toString() {
        return "BookTicketRequestDto{" +
                "requestId='" + requestId + '\'' +
                ", tripId='" + tripId + '\'' +
                ", seatId='" + seatId + '\'' +
                ", passengerName='" + passengerName + '\'' +
                '}';
    }
}
