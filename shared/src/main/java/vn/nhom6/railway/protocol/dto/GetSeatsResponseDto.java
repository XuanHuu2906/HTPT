package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Response payload for GET_SEATS message according to SPEC-03 Section 4.1.
 */
public class GetSeatsResponseDto {

    public static final String DEFAULT_TRIP_ID = "SE-001";
    public static final int EXPECTED_SEAT_COUNT = 10;

    @JsonProperty("tripId")
    private String tripId;

    @JsonProperty("seats")
    private List<SeatDto> seats;

    @JsonProperty("readIndex")
    private Long readIndex;

    @JsonProperty("leaderId")
    private String leaderId;

    public GetSeatsResponseDto() {
        this.tripId = DEFAULT_TRIP_ID;
        this.seats = new ArrayList<>();
    }

    @JsonCreator
    public GetSeatsResponseDto(
            @JsonProperty("tripId") String tripId,
            @JsonProperty("seats") List<SeatDto> seats,
            @JsonProperty("readIndex") Long readIndex,
            @JsonProperty("leaderId") String leaderId) {
        this.tripId = tripId;
        this.seats = seats != null ? new ArrayList<>(seats) : new ArrayList<>();
        this.readIndex = readIndex;
        this.leaderId = leaderId;
    }

    /**
     * Validates fields according to SPEC-03 Section 4.1.
     */
    public void validate() {
        if (tripId == null || !DEFAULT_TRIP_ID.equals(tripId.strip())) {
            throw new IllegalArgumentException("tripId must be " + DEFAULT_TRIP_ID + ", got: " + tripId);
        }
        if (seats == null || seats.size() != EXPECTED_SEAT_COUNT) {
            throw new IllegalArgumentException("seats must contain exactly " + EXPECTED_SEAT_COUNT + " seats, got: " + (seats != null ? seats.size() : "null"));
        }
        for (int i = 0; i < EXPECTED_SEAT_COUNT; i++) {
            SeatDto seat = seats.get(i);
            if (seat == null) {
                throw new IllegalArgumentException("Seat at index " + i + " must not be null");
            }
            seat.validate();
            String expectedSeatId = String.format("G%02d", i + 1);
            if (!expectedSeatId.equals(seat.getSeatId())) {
                throw new IllegalArgumentException("Expected seatId " + expectedSeatId + " at index " + i + ", got: " + seat.getSeatId());
            }
        }
        if (leaderId == null || leaderId.isBlank()) {
            throw new IllegalArgumentException("leaderId must not be null or blank in successful GET_SEATS response");
        }
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public List<SeatDto> getSeats() {
        return seats != null ? Collections.unmodifiableList(seats) : Collections.emptyList();
    }

    public void setSeats(List<SeatDto> seats) {
        this.seats = seats != null ? new ArrayList<>(seats) : new ArrayList<>();
    }

    public Long getReadIndex() {
        return readIndex;
    }

    public void setReadIndex(Long readIndex) {
        this.readIndex = readIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSeatsResponseDto that = (GetSeatsResponseDto) o;
        return Objects.equals(tripId, that.tripId) &&
                Objects.equals(seats, that.seats) &&
                Objects.equals(readIndex, that.readIndex) &&
                Objects.equals(leaderId, that.leaderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripId, seats, readIndex, leaderId);
    }

    @Override
    public String toString() {
        return "GetSeatsResponseDto{" +
                "tripId='" + tripId + '\'' +
                ", seatsCount=" + (seats != null ? seats.size() : 0) +
                ", readIndex=" + readIndex +
                ", leaderId='" + leaderId + '\'' +
                '}';
    }
}
