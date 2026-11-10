package vn.nhom6.railway.client.network;

import java.util.Objects;

/**
 * Thông tin một ghế trong chuyến tàu theo SPEC-03 Section 4.1.
 */
public class SeatInfoDto {
    private final String seatId;
    private final String status; // "AVAILABLE" hoặc "BOOKED"
    private final String ticketId;

    public SeatInfoDto(String seatId, String status, String ticketId) {
        this.seatId = seatId;
        this.status = status;
        this.ticketId = ticketId;
    }

    public String getSeatId() {
        return seatId;
    }

    public String getStatus() {
        return status;
    }

    public String getTicketId() {
        return ticketId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SeatInfoDto that = (SeatInfoDto) o;
        return Objects.equals(seatId, that.seatId) &&
                Objects.equals(status, that.status) &&
                Objects.equals(ticketId, that.ticketId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seatId, status, ticketId);
    }

    @Override
    public String toString() {
        return "SeatInfoDto{seatId='" + seatId + "', status='" + status + "', ticketId='" + ticketId + "'}";
    }
}
