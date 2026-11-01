package vn.nhom6.railway.protocol.dto;

/**
 * Message types defined in SPEC-03 Section 3.1.
 * Single source of truth for message envelope type identifiers.
 */
public enum MessageType {
    // Client <-> Node: Booking & seat queries
    GET_SEATS,
    GET_SEATS_RESPONSE,
    BOOK_TICKET,
    BOOK_TICKET_RESPONSE,
    CANCEL_TICKET,
    CANCEL_TICKET_RESPONSE,
    GET_REQUEST_RESULT,
    GET_REQUEST_RESULT_RESPONSE,
    GET_TICKET_HISTORY,
    GET_TICKET_HISTORY_RESPONSE,

    // Monitor <-> Node: Local diagnostics
    NODE_STATUS,
    NODE_STATUS_RESPONSE,

    // Node <-> Node: Raft RPCs
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,

    // Generic error response for any failed request
    ERROR_RESPONSE;

    /**
     * Check if this message type represents a response.
     */
    public boolean isResponse() {
        return this.name().endsWith("_RESPONSE");
    }

    /**
     * Check if this message type is an internal Raft RPC between nodes.
     */
    public boolean isRaftRpc() {
        return this == REQUEST_VOTE 
            || this == REQUEST_VOTE_RESPONSE 
            || this == APPEND_ENTRIES 
            || this == APPEND_ENTRIES_RESPONSE;
    }
}
