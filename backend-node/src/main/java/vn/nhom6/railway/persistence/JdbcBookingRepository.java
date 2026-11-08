package vn.nhom6.railway.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.ConnectionFactory;

/** PostgreSQL repository that applies each committed booking command atomically. */
public final class JdbcBookingRepository implements BookingRepository {
    private static final Logger LOGGER =
            Logger.getLogger(JdbcBookingRepository.class.getName());
    private static final String TRIP_ID = "SE-001";
    private static final String PROGRESS_SELECT =
            "SELECT last_applied FROM raft_apply_progress WHERE singleton = ?";
    private static final String PROGRESS_LOCK_SELECT =
            "SELECT last_applied FROM raft_apply_progress WHERE singleton = ? FOR UPDATE";
    private static final String STORED_RESULT_SELECT =
            "SELECT command_type, intent_fingerprint, result_json ->> 'requestId', "
                    + "result_json ->> 'outcome', result_json ->> 'ticketId', "
                    + "result_json ->> 'reasonCode', result_json ->> 'appliedLogIndex' "
                    + "FROM request_results WHERE request_id = ?";
    private static final String STORED_RESULT_LOCK_SELECT = STORED_RESULT_SELECT + " FOR UPDATE";

    @FunctionalInterface
    interface ConnectionProvider {
        Connection getConnection();
    }

    private final ConnectionProvider connectionProvider;

    public JdbcBookingRepository(ConnectionFactory connectionFactory) {
        this(Objects.requireNonNull(connectionFactory, "connectionFactory")::getConnection);
    }

    JdbcBookingRepository(ConnectionProvider connectionProvider) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
    }

    @Override
    public long lastApplied() {
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(PROGRESS_SELECT)) {
            statement.setShort(1, (short) 1);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw integrityFailure("Missing raft_apply_progress singleton row");
                }
                long lastApplied = resultSet.getLong(1);
                if (resultSet.next()) {
                    throw integrityFailure("Multiple raft_apply_progress singleton rows");
                }
                return lastApplied;
            }
        } catch (SQLException exception) {
            throw storageFailure("read lastApplied", exception);
        }
    }

    @Override
    public BookingStateMachine.ApplyOutcome applyAtomically(
            long logIndex, BookingCommand command) {
        Objects.requireNonNull(command, "command");
        if (logIndex < 1) {
            throw new IllegalArgumentException("logIndex phải >= 1: " + logIndex);
        }

        try (Connection connection = connectionProvider.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                connection.setAutoCommit(false);
                BookingStateMachine.ApplyOutcome outcome =
                        applyInTransaction(connection, logIndex, command);
                connection.commit();
                return outcome;
            } catch (SQLException exception) {
                rollback(connection, exception);
                throw storageFailure("apply committed command at index " + logIndex, exception);
            } catch (RuntimeException exception) {
                rollback(connection, exception);
                throw exception;
            } finally {
                restoreAutoCommit(connection, originalAutoCommit);
            }
        } catch (SQLException exception) {
            throw storageFailure("close apply connection at index " + logIndex, exception);
        }
    }

    private BookingStateMachine.ApplyOutcome applyInTransaction(
            Connection connection, long logIndex, BookingCommand command) throws SQLException {
        long lastApplied = lockAndReadLastApplied(connection);
        if (lastApplied == Long.MAX_VALUE || logIndex != lastApplied + 1) {
            throw new IllegalStateException(
                    "Committed index sai thứ tự: cần " + (lastApplied + 1)
                    + " nhưng nhận " + logIndex + " (lastApplied=" + lastApplied + ")");
        }
        verifyCommittedCommand(connection, logIndex, command);

        BookingStateMachine.ApplyOutcome outcome;
        if (command instanceof BookingCommand.NoOp) {
            outcome = new BookingStateMachine.ApplyOutcome.NoOpApplied(logIndex);
        } else if (command instanceof BookingCommand.BookTicket book) {
            outcome = applyBook(connection, logIndex, book);
        } else if (command instanceof BookingCommand.CancelTicket cancel) {
            outcome = applyCancel(connection, logIndex, cancel);
        } else {
            throw new IllegalStateException("Unsupported booking command: " + command.getClass());
        }

        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE raft_apply_progress SET last_applied = ? "
                        + "WHERE singleton = ? AND last_applied = ?")) {
            statement.setLong(1, logIndex);
            statement.setShort(2, (short) 1);
            statement.setLong(3, lastApplied);
            if (statement.executeUpdate() != 1) {
                throw integrityFailure("raft_apply_progress changed during locked apply");
            }
        }
        return outcome;
    }

    private long lockAndReadLastApplied(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                PROGRESS_LOCK_SELECT)) {
            statement.setShort(1, (short) 1);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw integrityFailure("Missing raft_apply_progress singleton row");
                }
                long lastApplied = resultSet.getLong(1);
                if (resultSet.next()) {
                    throw integrityFailure("Multiple raft_apply_progress singleton rows");
                }
                return lastApplied;
            }
        }
    }

    private void verifyCommittedCommand(
            Connection connection, long logIndex, BookingCommand command) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT command_type, command_json ->> 'requestId', "
                        + "command_json ->> 'tripId', command_json ->> 'seatId', "
                        + "command_json ->> 'passengerName', command_json ->> 'ticketId', "
                        + "command_json ->> 'issuedAt', command_json ->> 'cancelledAt' "
                        + "FROM raft_log WHERE log_index = ?")) {
            statement.setLong(1, logIndex);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw integrityFailure("Committed raft_log entry is missing at index " + logIndex);
                }
                String commandType = resultSet.getString(1);
                boolean matches;
                if (command instanceof BookingCommand.NoOp) {
                    matches = BookingCommand.Type.NO_OP.name().equals(commandType);
                } else if (command instanceof BookingCommand.BookTicket book) {
                    matches = BookingCommand.Type.BOOK_TICKET.name().equals(commandType)
                            && book.requestId().equals(resultSet.getString(2))
                            && book.tripId().equals(resultSet.getString(3))
                            && book.seatId().equals(resultSet.getString(4))
                            && book.passengerName().equals(resultSet.getString(5))
                            && book.ticketId().equals(resultSet.getString(6))
                            && book.issuedAt().equals(parseInstant(resultSet.getString(7)));
                } else if (command instanceof BookingCommand.CancelTicket cancel) {
                    matches = BookingCommand.Type.CANCEL_TICKET.name().equals(commandType)
                            && cancel.requestId().equals(resultSet.getString(2))
                            && cancel.ticketId().equals(resultSet.getString(6))
                            && cancel.cancelledAt().equals(parseInstant(resultSet.getString(8)));
                } else {
                    matches = false;
                }
                if (resultSet.next()) {
                    throw integrityFailure("Duplicate raft_log entry at index " + logIndex);
                }
                if (!matches) {
                    throw integrityFailure("Command does not match committed raft_log index " + logIndex);
                }
            }
        }
    }

    private static Instant parseInstant(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (RuntimeException exception) {
            throw new BookingRepository.StorageException(
                    "Invalid timestamp in committed raft_log command", exception);
        }
    }

    private BookingStateMachine.ApplyOutcome applyBook(
            Connection connection, long logIndex, BookingCommand.BookTicket book) throws SQLException {
        String fingerprint = BookingStateMachine.computeBookFingerprint(
                book.tripId(), book.seatId(), book.passengerName());
        Optional<StoredRequestResult> existing = findStoredResult(connection, book.requestId(), true);
        if (existing.isPresent()) {
            StoredRequestResult stored = existing.orElseThrow();
            if (stored.commandType.equals(BookingCommand.Type.BOOK_TICKET.name())
                    && stored.fingerprint.equals(fingerprint)) {
                return new BookingStateMachine.ApplyOutcome.BusinessResult(
                        stored.result, true);
            }
            return new BookingStateMachine.ApplyOutcome.RequestIdConflict(book.requestId(), logIndex);
        }

        SeatRow seat = lockSeat(connection, book.tripId(), book.seatId());
        if (seat == null) {
            throw integrityFailure("Missing seat " + book.tripId() + "/" + book.seatId());
        }

        ApplyResult result;
        String eventType;
        if ("AVAILABLE".equals(seat.status)) {
            if (seat.currentTicketId != null) {
                throw integrityFailure("AVAILABLE seat has current_ticket_id: " + book.seatId());
            }
            insertTicket(connection, book);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE seats SET status = ?, current_ticket_id = ? "
                            + "WHERE trip_id = ? AND seat_id = ? AND status = ? "
                            + "AND current_ticket_id IS NULL")) {
                statement.setString(1, "BOOKED");
                statement.setString(2, book.ticketId());
                statement.setString(3, book.tripId());
                statement.setString(4, book.seatId());
                statement.setString(5, "AVAILABLE");
                if (statement.executeUpdate() != 1) {
                    throw integrityFailure("Seat changed while booking " + book.seatId());
                }
            }
            result = ApplyResult.bookConfirmed(book.requestId(), book.ticketId(), logIndex);
            eventType = "BOOK_CONFIRMED";
        } else if ("BOOKED".equals(seat.status)) {
            if (seat.currentTicketId == null) {
                throw integrityFailure("BOOKED seat has no current_ticket_id: " + book.seatId());
            }
            result = ApplyResult.bookRejected(
                    book.requestId(), ApplyResult.SEAT_ALREADY_BOOKED, logIndex);
            eventType = "BOOK_REJECTED";
        } else {
            throw integrityFailure("Unknown seat status for " + book.seatId() + ": " + seat.status);
        }

        String resultJson = bookResponseJson(book.requestId(), result, book.tripId(), book.seatId());
        insertRequestResult(connection, book.requestId(), BookingCommand.Type.BOOK_TICKET,
                fingerprint, result, resultJson);
        insertBusinessEvent(connection, logIndex, book.requestId(), eventType, resultJson);
        return new BookingStateMachine.ApplyOutcome.BusinessResult(result, false);
    }

    private BookingStateMachine.ApplyOutcome applyCancel(
            Connection connection, long logIndex, BookingCommand.CancelTicket cancel)
            throws SQLException {
        String fingerprint = BookingStateMachine.computeCancelFingerprint(cancel.ticketId());
        Optional<StoredRequestResult> existing = findStoredResult(connection, cancel.requestId(), true);
        if (existing.isPresent()) {
            StoredRequestResult stored = existing.orElseThrow();
            if (stored.commandType.equals(BookingCommand.Type.CANCEL_TICKET.name())
                    && stored.fingerprint.equals(fingerprint)) {
                return new BookingStateMachine.ApplyOutcome.BusinessResult(stored.result, true);
            }
            return new BookingStateMachine.ApplyOutcome.RequestIdConflict(cancel.requestId(), logIndex);
        }

        TicketRow ticket = lockTicket(connection, cancel.ticketId());
        ApplyResult result;
        String eventType;
        if (ticket == null || !"ACTIVE".equals(ticket.status)) {
            result = ApplyResult.cancelRejected(cancel.requestId(), cancel.ticketId(), logIndex);
            eventType = "CANCEL_REJECTED";
        } else {
            SeatRow seat = lockSeat(connection, ticket.tripId, ticket.seatId);
            if (seat == null || !"BOOKED".equals(seat.status)
                    || !cancel.ticketId().equals(seat.currentTicketId)) {
                throw integrityFailure("ACTIVE ticket " + cancel.ticketId()
                        + " is not linked to its BOOKED seat " + ticket.seatId);
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE tickets SET status = ?, cancelled_at = ? "
                            + "WHERE ticket_id = ? AND status = ?")) {
                statement.setString(1, "CANCELLED");
                statement.setTimestamp(2, Timestamp.from(cancel.cancelledAt()));
                statement.setString(3, cancel.ticketId());
                statement.setString(4, "ACTIVE");
                if (statement.executeUpdate() != 1) {
                    throw integrityFailure("Ticket changed while cancelling " + cancel.ticketId());
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE seats SET status = ?, current_ticket_id = NULL "
                            + "WHERE trip_id = ? AND seat_id = ? AND current_ticket_id = ? "
                            + "AND status = ?")) {
                statement.setString(1, "AVAILABLE");
                statement.setString(2, ticket.tripId);
                statement.setString(3, ticket.seatId);
                statement.setString(4, cancel.ticketId());
                statement.setString(5, "BOOKED");
                if (statement.executeUpdate() != 1) {
                    throw integrityFailure("Seat link changed while cancelling " + cancel.ticketId());
                }
            }
            result = ApplyResult.cancelConfirmed(cancel.requestId(), cancel.ticketId(), logIndex);
            eventType = "CANCEL_CONFIRMED";
        }

        String resultJson = cancelResponseJson(cancel.requestId(), result);
        insertRequestResult(connection, cancel.requestId(), BookingCommand.Type.CANCEL_TICKET,
                fingerprint, result, resultJson);
        insertBusinessEvent(connection, logIndex, cancel.requestId(), eventType, resultJson);
        return new BookingStateMachine.ApplyOutcome.BusinessResult(result, false);
    }

    private static SeatRow lockSeat(Connection connection, String tripId, String seatId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT status, current_ticket_id FROM seats "
                        + "WHERE trip_id = ? AND seat_id = ? FOR UPDATE")) {
            statement.setString(1, tripId);
            statement.setString(2, seatId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                SeatRow seat = new SeatRow(resultSet.getString(1), resultSet.getString(2));
                if (resultSet.next()) {
                    throw integrityFailure("Duplicate seat " + tripId + "/" + seatId);
                }
                return seat;
            }
        }
    }

    private static TicketRow lockTicket(Connection connection, String ticketId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT trip_id, seat_id, status FROM tickets WHERE ticket_id = ? FOR UPDATE")) {
            statement.setString(1, ticketId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                TicketRow ticket = new TicketRow(
                        resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                if (resultSet.next()) {
                    throw integrityFailure("Duplicate ticket " + ticketId);
                }
                return ticket;
            }
        }
    }

    private static void insertTicket(Connection connection, BookingCommand.BookTicket book)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO tickets(ticket_id, trip_id, seat_id, passenger_name, status, issued_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?)")) {
            statement.setString(1, book.ticketId());
            statement.setString(2, book.tripId());
            statement.setString(3, book.seatId());
            statement.setString(4, book.passengerName());
            statement.setString(5, "ACTIVE");
            statement.setTimestamp(6, Timestamp.from(book.issuedAt()));
            if (statement.executeUpdate() != 1) {
                throw integrityFailure("Ticket insert did not affect one row");
            }
        }
    }

    private static Optional<StoredRequestResult> findStoredResult(
            Connection connection, String requestId, boolean forUpdate) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                forUpdate ? STORED_RESULT_LOCK_SELECT : STORED_RESULT_SELECT)) {
            statement.setObject(1, java.util.UUID.fromString(requestId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                String commandType = resultSet.getString(1);
                String fingerprint = resultSet.getString(2).trim();
                ApplyResult result = toApplyResult(
                        commandType, resultSet.getString(3), resultSet.getString(4),
                        resultSet.getString(5), resultSet.getString(6), resultSet.getString(7));
                if (!requestId.equals(result.requestId())) {
                    throw integrityFailure("result_json requestId does not match request_results key");
                }
                if (resultSet.next()) {
                    throw integrityFailure("Duplicate request_results requestId " + requestId);
                }
                return Optional.of(new StoredRequestResult(commandType, fingerprint, result));
            }
        }
    }

    private static void insertRequestResult(
            Connection connection,
            String requestId,
            BookingCommand.Type type,
            String fingerprint,
            ApplyResult result,
            String resultJson) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO request_results(request_id, command_type, intent_fingerprint, "
                        + "state, reason_code, ticket_id, applied_log_index, result_json) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)")) {
            statement.setObject(1, java.util.UUID.fromString(requestId));
            statement.setString(2, type.name());
            statement.setString(3, fingerprint);
            statement.setString(4, result.outcome() == ApplyResult.Outcome.CONFIRMED
                    ? "APPLIED_SUCCESS" : "APPLIED_REJECTED");
            statement.setString(5, result.reasonCode());
            statement.setString(6, result.ticketId());
            statement.setLong(7, result.appliedLogIndex());
            statement.setString(8, resultJson);
            if (statement.executeUpdate() != 1) {
                throw integrityFailure("request_results insert did not affect one row");
            }
        }
    }

    private static void insertBusinessEvent(
            Connection connection, long logIndex, String requestId, String eventType, String resultJson)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO business_events(log_index, request_id, event_type, event_json) "
                        + "VALUES (?, ?, ?, ?::jsonb)")) {
            statement.setLong(1, logIndex);
            statement.setObject(2, java.util.UUID.fromString(requestId));
            statement.setString(3, eventType);
            statement.setString(4, resultJson);
            if (statement.executeUpdate() != 1) {
                throw integrityFailure("business_events insert did not affect one row");
            }
        }
    }

    @Override
    public Optional<ApplyResult> findResultByRequestId(String requestId) {
        BookingCommand.requireValidRequestId(requestId);
        try (Connection connection = connectionProvider.getConnection()) {
            return findStoredResult(connection, requestId, false).map(StoredRequestResult::result);
        } catch (SQLException exception) {
            throw storageFailure("read request result", exception);
        }
    }

    @Override
    public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
        if (logIndex < 1) {
            return Optional.empty();
        }
        try (Connection connection = connectionProvider.getConnection()) {
            AppliedLogEntry entry;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT l.command_type, l.command_json ->> 'requestId', "
                            + "l.command_json ->> 'tripId', l.command_json ->> 'seatId', "
                            + "l.command_json ->> 'passengerName', l.command_json ->> 'ticketId', "
                            + "p.last_applied FROM raft_apply_progress p "
                            + "LEFT JOIN raft_log l ON l.log_index = ? WHERE p.singleton = ?")) {
                statement.setLong(1, logIndex);
                statement.setShort(2, (short) 1);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw integrityFailure("Missing raft_apply_progress singleton row");
                    }
                    long lastApplied = resultSet.getLong(7);
                    if (logIndex > lastApplied) {
                        return Optional.empty();
                    }
                    entry = new AppliedLogEntry(
                            resultSet.getString(1), resultSet.getString(2), resultSet.getString(3),
                            resultSet.getString(4), resultSet.getString(5), resultSet.getString(6));
                }
            }

            if (entry.commandType == null) {
                throw integrityFailure("Applied raft_log entry is missing at index " + logIndex);
            }
            if (entry.commandType.equals(BookingCommand.Type.NO_OP.name())) {
                return Optional.of(new BookingStateMachine.ApplyOutcome.NoOpApplied(logIndex));
            }
            if (entry.requestId == null) {
                throw integrityFailure("Applied business command has no requestId at " + logIndex);
            }
            String fingerprint;
            if (BookingCommand.Type.BOOK_TICKET.name().equals(entry.commandType)) {
                if (entry.tripId == null || entry.seatId == null || entry.passengerName == null) {
                    throw integrityFailure("Applied BOOK command is incomplete at index " + logIndex);
                }
                fingerprint = BookingStateMachine.computeBookFingerprint(
                        entry.tripId, entry.seatId, entry.passengerName);
            } else if (BookingCommand.Type.CANCEL_TICKET.name().equals(entry.commandType)) {
                if (entry.ticketId == null) {
                    throw integrityFailure("Applied CANCEL command has no ticketId at " + logIndex);
                }
                fingerprint = BookingStateMachine.computeCancelFingerprint(entry.ticketId);
            } else {
                throw integrityFailure("Unknown applied command type at index " + logIndex);
            }
            Optional<StoredRequestResult> stored =
                    findStoredResult(connection, entry.requestId, false);
            if (stored.isEmpty()) {
                throw integrityFailure("Applied request result is missing at index " + logIndex);
            }
            StoredRequestResult value = stored.orElseThrow();
            if (!value.commandType.equals(entry.commandType) || !value.fingerprint.equals(fingerprint)) {
                return Optional.of(new BookingStateMachine.ApplyOutcome.RequestIdConflict(
                        entry.requestId, logIndex));
            }
            return Optional.of(new BookingStateMachine.ApplyOutcome.BusinessResult(
                    value.result, value.result.appliedLogIndex() != logIndex));
        } catch (SQLException exception) {
            throw storageFailure("read applied outcome", exception);
        }
    }

    @Override
    public Map<String, String> readSeatSnapshot() {
        Map<String, String> seats = new LinkedHashMap<>();
        try (Connection connection = connectionProvider.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT seat_id, status, current_ticket_id FROM seats "
                                + "WHERE trip_id = ? ORDER BY seat_id")) {
            statement.setString(1, TRIP_ID);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String seatId = resultSet.getString(1);
                    String status = resultSet.getString(2);
                    String ticketId = resultSet.getString(3);
                    if (seats.putIfAbsent(seatId, status) != null) {
                        throw integrityFailure("Duplicate seat in snapshot: " + seatId);
                    }
                    if ("AVAILABLE".equals(status) != (ticketId == null)
                            || !("AVAILABLE".equals(status) || "BOOKED".equals(status))) {
                        throw integrityFailure("Inconsistent seat state in snapshot: " + seatId);
                    }
                }
            }
        } catch (SQLException exception) {
            throw storageFailure("read seat snapshot", exception);
        }
        if (seats.size() != 10) {
            throw integrityFailure("Expected 10 seats for " + TRIP_ID + " but found " + seats.size());
        }
        for (int number = 1; number <= 10; number++) {
            String expected = String.format(java.util.Locale.ROOT, "G%02d", number);
            if (!seats.containsKey(expected)) {
                throw integrityFailure("Missing seat in snapshot: " + expected);
            }
        }
        return Collections.unmodifiableMap(seats);
    }

    private static ApplyResult toApplyResult(
            String commandType,
            String requestId,
            String outcome,
            String ticketId,
            String reasonCode,
            String logIndex) {
        if (requestId == null || outcome == null || logIndex == null) {
            throw integrityFailure("Incomplete persisted request result");
        }
        long appliedLogIndex;
        try {
            appliedLogIndex = Long.parseLong(logIndex);
        } catch (NumberFormatException exception) {
            throw new BookingRepository.StorageException("Invalid appliedLogIndex in result_json", exception);
        }
        if ("CONFIRMED".equals(outcome)) {
            try {
                return BookingCommand.Type.BOOK_TICKET.name().equals(commandType)
                        ? ApplyResult.bookConfirmed(requestId, ticketId, appliedLogIndex)
                        : BookingCommand.Type.CANCEL_TICKET.name().equals(commandType)
                                ? ApplyResult.cancelConfirmed(requestId, ticketId, appliedLogIndex)
                                : failUnknownCommandType(commandType);
            } catch (IllegalArgumentException exception) {
                throw new BookingRepository.StorageException(
                        "Invalid persisted request result", exception);
            }
        }
        if ("REJECTED".equals(outcome)) {
            try {
                return BookingCommand.Type.BOOK_TICKET.name().equals(commandType)
                        ? ApplyResult.bookRejected(requestId, reasonCode, appliedLogIndex)
                        : BookingCommand.Type.CANCEL_TICKET.name().equals(commandType)
                                ? ApplyResult.cancelRejected(requestId, ticketId, appliedLogIndex)
                                : failUnknownCommandType(commandType);
            } catch (IllegalArgumentException exception) {
                throw new BookingRepository.StorageException(
                        "Invalid persisted request result", exception);
            }
        }
        throw integrityFailure("Non-persistable result outcome in request_results: " + outcome);
    }

    private static String bookResponseJson(
            String requestId, ApplyResult result, String tripId, String seatId) {
        return "{\"requestId\":" + jsonString(requestId)
                + ",\"outcome\":" + jsonString(result.outcome().name())
                + ",\"ticketId\":" + jsonString(result.ticketId())
                + ",\"tripId\":" + jsonString(tripId)
                + ",\"seatId\":" + jsonString(seatId)
                + ",\"reasonCode\":" + jsonString(result.reasonCode())
                + ",\"appliedLogIndex\":" + result.appliedLogIndex() + "}";
    }

    private static String cancelResponseJson(String requestId, ApplyResult result) {
        return "{\"requestId\":" + jsonString(requestId)
                + ",\"outcome\":" + jsonString(result.outcome().name())
                + ",\"ticketId\":" + jsonString(result.ticketId())
                + ",\"reasonCode\":" + jsonString(result.reasonCode())
                + ",\"appliedLogIndex\":" + result.appliedLogIndex() + "}";
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder json = new StringBuilder(value.length() + 2).append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (character < 0x20) {
                        json.append(String.format("\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        return json.append('"').toString();
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
            LOGGER.log(Level.SEVERE, "PostgreSQL rollback failed; SQLState={0}, vendorCode={1}",
                    new Object[] {rollbackFailure.getSQLState(), rollbackFailure.getErrorCode()});
        }
    }

    private static void restoreAutoCommit(Connection connection, boolean originalAutoCommit) {
        try {
            if (!connection.isClosed() && connection.getAutoCommit() != originalAutoCommit) {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException exception) {
            LOGGER.log(Level.SEVERE, "Could not restore JDBC autoCommit; SQLState={0}, vendorCode={1}",
                    new Object[] {exception.getSQLState(), exception.getErrorCode()});
        }
    }

    private static BookingRepository.StorageException storageFailure(
            String operation, SQLException cause) {
        LOGGER.log(Level.SEVERE,
                "PostgreSQL {0} failed; SQLState={1}, vendorCode={2}",
                new Object[] {operation, cause.getSQLState(), cause.getErrorCode()});
        return new BookingRepository.StorageException(
                "PostgreSQL " + operation + " failed (SQLState=" + cause.getSQLState()
                        + ", vendorCode=" + cause.getErrorCode() + ")", cause);
    }

    private static BookingRepository.StorageException integrityFailure(String message) {
        LOGGER.log(Level.SEVERE, "PostgreSQL booking data integrity failure: {0}", message);
        return new BookingRepository.StorageException("Database integrity violation: " + message);
    }

    private static ApplyResult failUnknownCommandType(String commandType) {
        throw integrityFailure("Unknown request_results command_type: " + commandType);
    }

    private record SeatRow(String status, String currentTicketId) {}

    private record TicketRow(String tripId, String seatId, String status) {}

    private record StoredRequestResult(
            String commandType, String fingerprint, ApplyResult result) {}

    private record AppliedLogEntry(
            String commandType,
            String requestId,
            String tripId,
            String seatId,
            String passengerName,
            String ticketId) {}
}
