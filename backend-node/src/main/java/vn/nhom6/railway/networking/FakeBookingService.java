package vn.nhom6.railway.networking;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory 1-node booking service implementation wrapping {@link BookingStateMachine}.
 *
 * <p>Operates deterministically according to SPEC-05 Section 3 without requiring PostgreSQL
 * or multi-node Raft consensus. Can be configured for simulated leader failover or timeouts.
 */
public class FakeBookingService implements BookingService {

    private final String nodeId;
    private final BookingStateMachine stateMachine;
    private final AtomicLong logIndexCounter = new AtomicLong(0);

    private final AtomicBoolean leader = new AtomicBoolean(true);
    private volatile String leaderHint;
    private final AtomicBoolean simulateUnknown = new AtomicBoolean(false);

    public FakeBookingService(String nodeId) {
        this(nodeId, new BookingStateMachine());
    }

    public FakeBookingService(String nodeId, BookingStateMachine stateMachine) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine must not be null");
        this.leaderHint = nodeId;
    }

    @Override
    public synchronized ApplyResult bookTicket(
            String requestId, String tripId, String seatId, String passengerName) throws Exception {

        if (simulateUnknown.get()) {
            return ApplyResult.bookUnknown(requestId);
        }

        // Generate deterministic/valid ticketId candidate per SPEC-03 Section 3.0: [A-Za-z0-9-]{1,64}
        String ticketId = "TKT-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();

        BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                requestId,
                tripId,
                seatId,
                passengerName,
                ticketId,
                Instant.now().truncatedTo(ChronoUnit.MILLIS)
        );

        long nextIndex = logIndexCounter.incrementAndGet();
        BookingStateMachine.ApplyOutcome outcome = stateMachine.apply(nextIndex, command);

        if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult business) {
            return business.applyResult();
        } else if (outcome instanceof BookingStateMachine.ApplyOutcome.RequestIdConflict conflict) {
            throw new IllegalArgumentException(
                    "RequestId " + conflict.requestId() + " was previously used with conflicting intent"
            );
        } else {
            throw new IllegalStateException("Unexpected outcome type from BookingStateMachine: " + outcome);
        }
    }

    @Override
    public synchronized SeatSnapshot getSeats(String tripId) {
        if (!"SE-001".equals(tripId)) {
            throw new IllegalArgumentException("Invalid tripId: " + tripId + ", must be SE-001");
        }

        List<SeatDto> seats = new ArrayList<>(10);
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            String status = stateMachine.seatStatus(seatId).orElse("AVAILABLE");
            String ticketId = stateMachine.seatCurrentTicketId(seatId).orElse(null);
            seats.add(new SeatDto(seatId, status, ticketId));
        }

        return new SeatSnapshot(tripId, seats, stateMachine.lastApplied(), nodeId);
    }

    @Override
    public boolean isLeader() {
        return leader.get();
    }

    @Override
    public String getLeaderHint() {
        return leaderHint;
    }

    public void setLeader(boolean isLeader, String leaderHint) {
        this.leader.set(isLeader);
        this.leaderHint = leaderHint;
    }

    public void setSimulateUnknown(boolean enable) {
        this.simulateUnknown.set(enable);
    }

    public BookingStateMachine getStateMachine() {
        return stateMachine;
    }
}
