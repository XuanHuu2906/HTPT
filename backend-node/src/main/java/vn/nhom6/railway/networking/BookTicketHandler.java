package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for {@link MessageType#BOOK_TICKET} incoming requests.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Validates that this node is currently the leader; if not, responds with {@code NOT_LEADER} error.</li>
 *   <li>Deserializes and strictly validates {@link BookTicketRequestDto}.</li>
 *   <li>Returns {@code INVALID_REQUEST} error envelope on validation failure.</li>
 *   <li>Delegates command execution to {@link BookingService}.</li>
 *   <li>Encapsulates result into a strongly-typed {@link BookTicketResponseDto} correlated to the request.</li>
 * </ul>
 */
public class BookTicketHandler implements HandlerRegistry.RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(BookTicketHandler.class.getName());

    private final BookingService bookingService;
    private final String nodeId;

    public BookTicketHandler(BookingService bookingService, String nodeId) {
        this.bookingService = Objects.requireNonNull(bookingService, "bookingService must not be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return null;
        }

        String messageId = request.getMessageId();
        String requestId = extractRequestId(request);

        // 1. Enforce Leader authority per SPEC-02 Section 4 and SPEC-03 Section 7
        if (!bookingService.isLeader()) {
            String leaderHint = bookingService.getLeaderHint();
            LOGGER.info(() -> "[" + nodeId + "] Rejecting BOOK_TICKET as NOT_LEADER, leaderHint=" + leaderHint);
            return MessageCodec.createErrorResponse(
                    messageId,
                    "NOT_LEADER",
                    "This node is not the current Raft leader. Please redirect to leader.",
                    leaderHint,
                    null,
                    requestId,
                    nodeId
            );
        }

        // 2. Decode and validate request payload
        BookTicketRequestDto requestDto;
        try {
            requestDto = MessageCodec.decodePayload(request, BookTicketRequestDto.class);
            if (requestDto == null) {
                return MessageCodec.createErrorResponse(
                        messageId, "INVALID_REQUEST", "Missing book ticket request payload", null, null, requestId, nodeId
                );
            }
            requestDto.validate();
        } catch (IllegalArgumentException | IOException ex) {
            LOGGER.log(Level.WARNING, "BOOK_TICKET validation failure: " + ex.getMessage());
            return MessageCodec.createErrorResponse(
                    messageId, "INVALID_REQUEST", ex.getMessage(), null, null, requestId, nodeId
            );
        }

        // 3. Execute booking via service
        ApplyResult result = bookingService.bookTicket(
                requestDto.getRequestId(),
                requestDto.getTripId(),
                requestDto.getSeatId(),
                requestDto.getPassengerName()
        );

        // 4. Construct typed response DTO
        BookTicketResponseDto responseDto = new BookTicketResponseDto(
                result.requestId(),
                result.outcome().name(),
                result.ticketId(),
                requestDto.getTripId(),
                requestDto.getSeatId(),
                result.reasonCode(),
                result.appliedLogIndex()
        );
        responseDto.validate();

        // 5. Correlate and return response envelope
        return MessageEnvelope.createResponse(
                messageId,
                MessageType.BOOK_TICKET_RESPONSE,
                nodeId,
                responseDto
        );
    }

    private static String extractRequestId(MessageEnvelope<JsonNode> request) {
        if (request != null && request.getPayload() != null && request.getPayload().has("requestId")) {
            JsonNode reqIdNode = request.getPayload().get("requestId");
            if (reqIdNode.isTextual()) {
                return reqIdNode.asText();
            }
        }
        return null;
    }
}
