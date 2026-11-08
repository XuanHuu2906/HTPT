package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry mapping {@link MessageType} to dedicated request handlers.
 *
 * <p>Supports modular registration for:
 * <ul>
 *   <li>{@link BookingHandler} (TV2): Booking commands, queries, and seat lookups.</li>
 *   <li>{@link RaftHandler} (TV4 / TV5): RequestVote and AppendEntries RPCs.</li>
 *   <li>{@link StatusHandler} (TV6): Node diagnostics and monitoring status queries.</li>
 * </ul>
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}.</p>
 */
public class HandlerRegistry {

    @FunctionalInterface
    public interface RequestHandler {
        /**
         * Processes an incoming message envelope and returns a response envelope,
         * or {@code null} if no response is required.
         *
         * @param request The incoming request envelope
         * @return Response envelope or null
         * @throws Exception If handling fails
         */
        MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception;
    }

    @FunctionalInterface
    public interface BookingHandler {
        /**
         * Handles booking requests (BOOK_TICKET, GET_SEATS, CANCEL_TICKET, GET_REQUEST_RESULT, GET_TICKET_HISTORY).
         */
        MessageEnvelope<?> handleBooking(MessageEnvelope<JsonNode> request) throws Exception;
    }

    @FunctionalInterface
    public interface RaftHandler {
        /**
         * Handles Raft RPC requests (REQUEST_VOTE, APPEND_ENTRIES).
         */
        MessageEnvelope<?> handleRaftRpc(MessageEnvelope<JsonNode> request) throws Exception;
    }

    @FunctionalInterface
    public interface StatusHandler {
        /**
         * Handles node status queries (NODE_STATUS).
         */
        MessageEnvelope<?> handleStatus(MessageEnvelope<JsonNode> request) throws Exception;
    }

    private final Map<MessageType, RequestHandler> handlers = new ConcurrentHashMap<>();

    /**
     * Registers a generic handler for a specific {@link MessageType}.
     *
     * @param type The message type to register
     * @param handler The handler implementation
     * @return This registry instance for chaining
     */
    public HandlerRegistry register(MessageType type, RequestHandler handler) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        handlers.put(type, handler);
        return this;
    }

    /**
     * Registers a {@link BookingHandler} for all booking-related message types.
     *
     * @param handler Booking handler
     * @return This registry instance for chaining
     */
    public HandlerRegistry registerBookingHandler(BookingHandler handler) {
        Objects.requireNonNull(handler, "BookingHandler must not be null");
        register(MessageType.BOOK_TICKET, handler::handleBooking);
        register(MessageType.GET_SEATS, handler::handleBooking);
        register(MessageType.CANCEL_TICKET, handler::handleBooking);
        register(MessageType.GET_REQUEST_RESULT, handler::handleBooking);
        register(MessageType.GET_TICKET_HISTORY, handler::handleBooking);
        return this;
    }

    /**
     * Registers a {@link RaftHandler} for all Raft RPC message types.
     *
     * @param handler Raft RPC handler
     * @return This registry instance for chaining
     */
    public HandlerRegistry registerRaftHandler(RaftHandler handler) {
        Objects.requireNonNull(handler, "RaftHandler must not be null");
        register(MessageType.REQUEST_VOTE, handler::handleRaftRpc);
        register(MessageType.APPEND_ENTRIES, handler::handleRaftRpc);
        return this;
    }

    /**
     * Registers a {@link StatusHandler} for node status queries.
     *
     * @param handler Status handler
     * @return This registry instance for chaining
     */
    public HandlerRegistry registerStatusHandler(StatusHandler handler) {
        Objects.requireNonNull(handler, "StatusHandler must not be null");
        register(MessageType.NODE_STATUS, handler::handleStatus);
        return this;
    }

    /**
     * Retrieves the registered handler for the given message type.
     *
     * @param type Message type
     * @return Registered handler, or {@code null} if none is registered
     */
    public RequestHandler getHandler(MessageType type) {
        if (type == null) {
            return null;
        }
        return handlers.get(type);
    }

    /**
     * Checks if a handler is registered for the given message type.
     *
     * @param type Message type
     * @return {@code true} if a handler exists, {@code false} otherwise
     */
    public boolean hasHandler(MessageType type) {
        return type != null && handlers.containsKey(type);
    }

    /**
     * Removes all registered handlers.
     */
    public void clear() {
        handlers.clear();
    }
}
