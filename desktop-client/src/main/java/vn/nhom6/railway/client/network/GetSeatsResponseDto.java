package vn.nhom6.railway.client.network;

import java.util.Collections;
import java.util.List;

/**
 * Response payload cho GET_SEATS theo SPEC-03 Section 4.1.
 */
public class GetSeatsResponseDto {
    private final String tripId;
    private final List<SeatInfoDto> seats;
    private final Long readIndex;
    private final String leaderId;
    private final boolean mock;

    public GetSeatsResponseDto(String tripId, List<SeatInfoDto> seats, Long readIndex, String leaderId, boolean mock) {
        this.tripId = tripId;
        this.seats = seats != null ? Collections.unmodifiableList(seats) : Collections.emptyList();
        this.readIndex = readIndex;
        this.leaderId = leaderId;
        this.mock = mock;
    }

    public String getTripId() {
        return tripId;
    }

    public List<SeatInfoDto> getSeats() {
        return seats;
    }

    public Long getReadIndex() {
        return readIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isMock() {
        return mock;
    }

    @Override
    public String toString() {
        return "GetSeatsResponseDto{tripId='" + tripId + "', seatsCount=" + seats.size() +
                ", readIndex=" + readIndex + ", leaderId='" + leaderId + "', mock=" + mock + "}";
    }
}
