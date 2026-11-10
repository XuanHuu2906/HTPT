package vn.nhom6.railway.networking;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.util.List;

/**
 * Service boundary interface for booking commands and seat inquiries.
 * Decouples transport networking from domain/Raft execution.
 */
public interface BookingService {

    /**
     * Attempts to book a single seat on a trip.
     *
     * @param requestId Unique UUID for idempotent tracking
     * @param tripId Trip identifier (must be "SE-001")
     * @param seatId Seat identifier ("G01" to "G10")
     * @param passengerName Passenger full name
     * @return Deterministic {@link ApplyResult} outcome
     * @throws Exception If processing fails
     */
    ApplyResult bookTicket(String requestId, String tripId, String seatId, String passengerName) throws Exception;

    /**
     * Retrieves a consistent snapshot of all 10 seats for the trip.
     *
     * @param tripId Trip identifier (must be "SE-001")
     * @return {@link SeatSnapshot} with 10 seats, read index, and current leader ID
     * @throws Exception If retrieval fails
     */
    SeatSnapshot getSeats(String tripId) throws Exception;

    /**
     * Checks if this node is currently the Raft leader authorized to process client writes/reads.
     *
     * @return {@code true} if leader, {@code false} otherwise
     */
    boolean isLeader();

    /**
     * Returns the currently known leader ID, or {@code null} if unknown.
     *
     * @return Leader node ID or null
     */
    String getLeaderHint();

    /**
     * Snapshot record for 10 seats according to SPEC-03 Section 4.1.
     */
    record SeatSnapshot(
            String tripId,
            List<SeatDto> seats,
            long readIndex,
            String leaderId
    ) {}
}
