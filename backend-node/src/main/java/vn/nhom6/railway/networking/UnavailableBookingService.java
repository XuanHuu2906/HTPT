package vn.nhom6.railway.networking;

import vn.nhom6.railway.booking.ApplyResult;

/** Fail-closed default until the composition root supplies the real Raft booking path. */
final class UnavailableBookingService implements BookingService {
    @Override public boolean isLeader() { return false; }
    @Override public String getLeaderHint() { return null; }
    @Override public ApplyResult bookTicket(String requestId, String tripId,
            String seatId, String passengerName) {
        throw new IllegalStateException("Raft commit and JDBC apply are not wired");
    }
    @Override public SeatSnapshot getSeats(String tripId) {
        throw new IllegalStateException("Raft read barrier is not wired");
    }
}
