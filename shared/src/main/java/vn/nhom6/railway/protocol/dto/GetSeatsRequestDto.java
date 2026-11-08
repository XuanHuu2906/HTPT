package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Request payload for GET_SEATS message according to SPEC-03 Section 4.1.
 */
public class GetSeatsRequestDto {

    public static final String DEFAULT_TRIP_ID = "SE-001";

    @JsonProperty("tripId")
    private String tripId;

    public GetSeatsRequestDto() {
        this.tripId = DEFAULT_TRIP_ID;
    }

    @JsonCreator
    public GetSeatsRequestDto(@JsonProperty("tripId") String tripId) {
        this.tripId = tripId;
    }

    /**
     * Validates fields according to SPEC-03 Section 4.1 and Section 8.
     */
    public void validate() {
        if (tripId == null || tripId.isBlank()) {
            throw new IllegalArgumentException("tripId must not be null or blank");
        }
        if (!DEFAULT_TRIP_ID.equals(tripId.strip())) {
            throw new IllegalArgumentException("tripId must be " + DEFAULT_TRIP_ID + " in v1, got: " + tripId);
        }
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSeatsRequestDto that = (GetSeatsRequestDto) o;
        return Objects.equals(tripId, that.tripId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripId);
    }

    @Override
    public String toString() {
        return "GetSeatsRequestDto{" +
                "tripId='" + tripId + '\'' +
                '}';
    }
}
