package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Seat representation inside GET_SEATS_RESPONSE according to SPEC-03 Section 4.1.
 */
public class SeatDto {

    private static final Pattern SEAT_PATTERN = Pattern.compile("^G(0[1-9]|10)$");

    @JsonProperty("seatId")
    private String seatId;

    @JsonProperty("status")
    private String status;

    @JsonProperty("ticketId")
    private String ticketId;

    public SeatDto() {
    }

    @JsonCreator
    public SeatDto(
            @JsonProperty("seatId") String seatId,
            @JsonProperty("status") String status,
            @JsonProperty("ticketId") String ticketId) {
        this.seatId = seatId;
        this.status = status;
        this.ticketId = ticketId;
    }

    /**
     * Validates seat constraints per SPEC-03 Section 4.1 and SPEC-05 Section 4.
     */
    public void validate() {
        if (seatId == null || !SEAT_PATTERN.matcher(seatId.strip()).matches()) {
            throw new IllegalArgumentException("seatId must be within G01-G10, got: " + seatId);
        }
        if (status == null || (!"AVAILABLE".equals(status) && !"BOOKED".equals(status))) {
            throw new IllegalArgumentException("status must be either AVAILABLE or BOOKED, got: " + status);
        }
        if ("AVAILABLE".equals(status) && ticketId != null) {
            throw new IllegalArgumentException("ticketId must be null when status is AVAILABLE");
        }
        if ("BOOKED".equals(status) && (ticketId == null || ticketId.isBlank())) {
            throw new IllegalArgumentException("ticketId must not be null/blank when status is BOOKED");
        }
    }

    public String getSeatId() {
        return seatId;
    }

    public void setSeatId(String seatId) {
        this.seatId = seatId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTicketId() {
        return ticketId;
    }

    public void setTicketId(String ticketId) {
        this.ticketId = ticketId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SeatDto seatDto = (SeatDto) o;
        return Objects.equals(seatId, seatDto.seatId) &&
                Objects.equals(status, seatDto.status) &&
                Objects.equals(ticketId, seatDto.ticketId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seatId, status, ticketId);
    }

    @Override
    public String toString() {
        return "SeatDto{" +
                "seatId='" + seatId + '\'' +
                ", status='" + status + '\'' +
                ", ticketId='" + ticketId + '\'' +
                '}';
    }
}
