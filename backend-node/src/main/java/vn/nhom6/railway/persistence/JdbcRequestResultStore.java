package vn.nhom6.railway.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;

/** Transaction-participating JDBC access to durable business results keyed by requestId. */
public final class JdbcRequestResultStore {
    private static final Logger LOGGER =
            Logger.getLogger(JdbcRequestResultStore.class.getName());
    private static final String SELECT =
            "SELECT command_type, intent_fingerprint, state, reason_code, ticket_id, "
                    + "applied_log_index, result_json::text, result_json ->> 'requestId', "
                    + "result_json ->> 'outcome' FROM request_results WHERE request_id = ?";
    private static final String INSERT =
            "INSERT INTO request_results(request_id, command_type, intent_fingerprint, "
                    + "state, reason_code, ticket_id, applied_log_index, result_json) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

    public enum Classification {
        NEW,
        REPLAY,
        CONFLICT
    }

    /** A durable first result. The JSON is the protocol response payload, not its envelope. */
    public record StoredRequestResult(
            String requestId,
            String commandType,
            String intentFingerprint,
            String state,
            String reasonCode,
            String ticketId,
            long appliedLogIndex,
            String resultJson) {

        /** Reconstructs the persisted business outcome and verifies its key response fields. */
        public ApplyResult toApplyResult() {
            String outcome = switch (state) {
                case "APPLIED_SUCCESS" -> "CONFIRMED";
                case "APPLIED_REJECTED" -> "REJECTED";
                default -> throw integrityFailure("Unknown request_results state: " + state);
            };
            if (appliedLogIndex < 1) {
                throw integrityFailure("Invalid applied_log_index in request_results");
            }

            ApplyResult result;
            try {
                if (BookingCommand.Type.BOOK_TICKET.name().equals(commandType)) {
                    result = "CONFIRMED".equals(outcome)
                            ? ApplyResult.bookConfirmed(requestId, ticketId, appliedLogIndex)
                            : ApplyResult.bookRejected(requestId, reasonCode, appliedLogIndex);
                } else if (BookingCommand.Type.CANCEL_TICKET.name().equals(commandType)) {
                    result = "CONFIRMED".equals(outcome)
                            ? ApplyResult.cancelConfirmed(requestId, ticketId, appliedLogIndex)
                            : ApplyResult.cancelRejected(requestId, ticketId, appliedLogIndex);
                } else {
                    throw integrityFailure("Unknown request_results command_type: " + commandType);
                }
            } catch (IllegalArgumentException exception) {
                throw integrityFailure("Invalid persisted request result", exception);
            }
            return result;
        }
    }

    /** Reads one result using the caller's connection and transaction. */
    public Optional<StoredRequestResult> find(Connection connection, String requestId) {
        return find(connection, requestId, false);
    }

    Optional<StoredRequestResult> findForUpdate(Connection connection, String requestId) {
        return find(connection, requestId, true);
    }

    private Optional<StoredRequestResult> find(
            Connection connection, String requestId, boolean forUpdate) {
        Objects.requireNonNull(connection, "connection");
        BookingCommand.requireValidRequestId(requestId);
        String sql = forUpdate ? SELECT + " FOR UPDATE" : SELECT;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, java.util.UUID.fromString(requestId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                String commandType = resultSet.getString(1);
                String fingerprint = resultSet.getString(2).trim();
                String state = resultSet.getString(3);
                String reasonCode = resultSet.getString(4);
                String ticketId = resultSet.getString(5);
                long appliedLogIndex = resultSet.getLong(6);
                String resultJson = resultSet.getString(7);
                String payloadRequestId = resultSet.getString(8);
                String payloadOutcome = resultSet.getString(9);

                String expectedOutcome = switch (state) {
                    case "APPLIED_SUCCESS" -> "CONFIRMED";
                    case "APPLIED_REJECTED" -> "REJECTED";
                    default -> throw integrityFailure(
                            "Unknown request_results state: " + state);
                };
                StoredRequestResult stored = new StoredRequestResult(
                        requestId, commandType, fingerprint, state, reasonCode,
                        ticketId, appliedLogIndex, resultJson);
                if (!requestId.equals(payloadRequestId) || !expectedOutcome.equals(payloadOutcome)) {
                    throw integrityFailure(
                            "result_json does not match request_results key and state");
                }
                stored.toApplyResult();
                if (resultSet.next()) {
                    throw integrityFailure("Duplicate request_results requestId " + requestId);
                }
                return Optional.of(stored);
            }
        } catch (SQLException exception) {
            throw storageFailure("read request result", exception);
        }
    }

    /**
     * Inserts an immutable persistable result; BOOK needs its trip/seat response fields, while
     * CANCEL does not. Returns the exact response payload also used by the business-event row.
     */
    public String insert(
            Connection connection,
            String requestId,
            String commandType,
            String fingerprint,
            ApplyResult result,
            String tripId,
            String seatId) {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(result, "result");
        BookingCommand.requireValidRequestId(requestId);
        if (!requestId.equals(result.requestId())) {
            throw new IllegalArgumentException("requestId must match ApplyResult.requestId");
        }
        if (!BookingCommand.Type.BOOK_TICKET.name().equals(commandType)
                && !BookingCommand.Type.CANCEL_TICKET.name().equals(commandType)) {
            throw new IllegalArgumentException("Unsupported request result command_type: " + commandType);
        }
        if (fingerprint == null || !fingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("intent fingerprint must be 64 lowercase hex characters");
        }
        if (!result.isPersistable()) {
            throw new IllegalArgumentException(
                    "UNKNOWN results must not be stored in request_results");
        }
        result = validateCommandResult(commandType, result);
        String resultJson;
        if (BookingCommand.Type.BOOK_TICKET.name().equals(commandType)) {
            if (tripId == null || seatId == null) {
                throw new IllegalArgumentException("BOOK result requires tripId and seatId");
            }
            resultJson = bookResponseJson(result, tripId, seatId);
        } else {
            if (tripId != null || seatId != null) {
                throw new IllegalArgumentException("CANCEL result must not include tripId or seatId");
            }
            resultJson = cancelResponseJson(result);
        }

        try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
            statement.setObject(1, java.util.UUID.fromString(requestId));
            statement.setString(2, commandType);
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
            return resultJson;
        } catch (SQLException exception) {
            throw storageFailure("insert request result", exception);
        }
    }

    public static Classification classify(
            Optional<StoredRequestResult> stored,
            String commandType,
            String fingerprint) {
        Objects.requireNonNull(stored, "stored");
        Objects.requireNonNull(commandType, "commandType");
        Objects.requireNonNull(fingerprint, "fingerprint");
        if (stored.isEmpty()) {
            return Classification.NEW;
        }
        StoredRequestResult existing = stored.orElseThrow();
        return existing.commandType().equals(commandType)
                        && existing.intentFingerprint().equals(fingerprint)
                ? Classification.REPLAY
                : Classification.CONFLICT;
    }

    private static ApplyResult validateCommandResult(String commandType, ApplyResult result) {
        try {
            ApplyResult canonical;
            if (BookingCommand.Type.BOOK_TICKET.name().equals(commandType)) {
                canonical = switch (result.outcome()) {
                    case CONFIRMED -> ApplyResult.bookConfirmed(
                            result.requestId(), result.ticketId(), result.appliedLogIndex());
                    case REJECTED -> ApplyResult.bookRejected(
                            result.requestId(), result.reasonCode(), result.appliedLogIndex());
                    case UNKNOWN -> throw new IllegalArgumentException(
                            "UNKNOWN results must not be stored in request_results");
                };
            } else {
                canonical = switch (result.outcome()) {
                    case CONFIRMED -> ApplyResult.cancelConfirmed(
                                result.requestId(), result.ticketId(), result.appliedLogIndex());
                    case REJECTED -> ApplyResult.cancelRejected(
                                result.requestId(), result.ticketId(), result.appliedLogIndex());
                    case UNKNOWN -> throw new IllegalArgumentException(
                                "UNKNOWN results must not be stored in request_results");
                };
            }
            if (!canonical.equals(result)) {
                throw new IllegalArgumentException(
                            "ApplyResult fields do not match command_type semantics");
            }
            return canonical;
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "ApplyResult does not match request command_type", exception);
        }
    }

    private static String bookResponseJson(ApplyResult result, String tripId, String seatId) {
        return "{\"requestId\":" + jsonString(result.requestId())
                + ",\"outcome\":" + jsonString(result.outcome().name())
                + ",\"ticketId\":" + jsonString(result.ticketId())
                + ",\"tripId\":" + jsonString(tripId)
                + ",\"seatId\":" + jsonString(seatId)
                + ",\"reasonCode\":" + jsonString(result.reasonCode())
                + ",\"appliedLogIndex\":" + result.appliedLogIndex() + "}";
    }

    private static String cancelResponseJson(ApplyResult result) {
        return "{\"requestId\":" + jsonString(result.requestId())
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
                    if (character < 0x20
                            || (Character.isSurrogate(character)
                                    && (index + 1 == value.length()
                                            || !Character.isSurrogatePair(
                                                    character, value.charAt(index + 1)))
                                    && (index == 0
                                            || !Character.isSurrogatePair(
                                                    value.charAt(index - 1), character)))) {
                        json.append(String.format("\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        return json.append('"').toString();
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
        LOGGER.log(Level.SEVERE, "PostgreSQL request_results integrity failure: {0}", message);
        return new BookingRepository.StorageException("Database integrity violation: " + message);
    }

    private static BookingRepository.StorageException integrityFailure(
            String message, Throwable cause) {
        LOGGER.log(Level.SEVERE, "PostgreSQL request_results integrity failure: {0}", message);
        return new BookingRepository.StorageException("Database integrity violation: " + message, cause);
    }
}
