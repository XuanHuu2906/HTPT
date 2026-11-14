package vn.nhom6.railway.networking;

import java.util.Objects;

/**
 * Service boundary interface for looking up business command execution status by {@code requestId}
 * (SPEC-03 Section 4.4 and TASK TV3-D14).
 *
 * <p>Enables safe resolution of {@code UNKNOWN} states after a socket read/write timeout occurs.
 */
@FunctionalInterface
public interface ResultLookup {

    /**
     * Queries the state of a previously submitted command by its unique requestId.
     *
     * @param requestId Unique UUID of the intent
     * @return Deterministic {@link LookupResult}
     * @throws Exception If communication or storage barrier fails
     */
    LookupResult lookup(String requestId) throws Exception;

    /**
     * Strongly-typed result model for GET_REQUEST_RESULT query (SPEC-03 §4.4).
     *
     * @param requestId Unique business intent UUID
     * @param state "APPLIED_SUCCESS", "APPLIED_REJECTED", "PENDING", or "UNKNOWN"
     * @param commandType Command type (e.g. "BOOK_TICKET", "CANCEL_TICKET")
     * @param ticketId Issued ticket ID (non-null for APPLIED_SUCCESS on BOOK_TICKET)
     * @param reasonCode Rejection reason code (e.g. "SEAT_ALREADY_BOOKED")
     * @param appliedLogIndex Raft log index where entry was applied
     */
    record LookupResult(
            String requestId,
            String state,
            String commandType,
            String ticketId,
            String reasonCode,
            Long appliedLogIndex
    ) {
        public LookupResult {
            Objects.requireNonNull(requestId, "requestId must not be null");
            Objects.requireNonNull(state, "state must not be null");
        }

        public static LookupResult appliedSuccess(String requestId, String commandType, String ticketId, long appliedLogIndex) {
            return new LookupResult(requestId, "APPLIED_SUCCESS", commandType, ticketId, null, appliedLogIndex);
        }

        public static LookupResult appliedRejected(String requestId, String commandType, String reasonCode, long appliedLogIndex) {
            return new LookupResult(requestId, "APPLIED_REJECTED", commandType, null, reasonCode, appliedLogIndex);
        }

        public static LookupResult pending(String requestId, String commandType) {
            return new LookupResult(requestId, "PENDING", commandType, null, null, null);
        }

        public static LookupResult unknown(String requestId) {
            return new LookupResult(requestId, "UNKNOWN", null, null, null, null);
        }

        public boolean isSuccess() {
            return "APPLIED_SUCCESS".equalsIgnoreCase(state);
        }

        public boolean isRejected() {
            return "APPLIED_REJECTED".equalsIgnoreCase(state);
        }

        public boolean isPending() {
            return "PENDING".equalsIgnoreCase(state);
        }

        public boolean isUnknown() {
            return "UNKNOWN".equalsIgnoreCase(state);
        }
    }
}
