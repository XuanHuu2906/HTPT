package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for {@link MessageType#GET_SEATS} queries.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Enforces leader read barrier per SPEC-03 Section 3.1 & SPEC-07 TC-READ-01.</li>
 *   <li>Deserializes and validates {@link GetSeatsRequestDto}.</li>
 *   <li>Returns {@code INVALID_REQUEST} error envelope on validation failure.</li>
 *   <li>Fetches consistent snapshot of 10 seats from {@link BookingService}.</li>
 *   <li>Encapsulates result into a strongly-typed {@link GetSeatsResponseDto} correlated to the request.</li>
 * </ul>
 */
public class GetSeatsHandler implements HandlerRegistry.RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(GetSeatsHandler.class.getName());

    private final BookingService bookingService;
    private final String nodeId;

    public GetSeatsHandler(BookingService bookingService, String nodeId) {
        this.bookingService = Objects.requireNonNull(bookingService, "bookingService must not be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return null;
        }

        String messageId = request.getMessageId();

        // 1. Enforce Leader authority per SPEC-03 Section 3.1
        if (!bookingService.isLeader()) {
            String leaderHint = bookingService.getLeaderHint();
            LOGGER.info(() -> "[" + nodeId + "] Rejecting GET_SEATS as NOT_LEADER, leaderHint=" + leaderHint);
            return MessageCodec.createErrorResponse(
                    messageId,
                    "NOT_LEADER",
                    "This node is not the current Raft leader. Please redirect to leader.",
                    leaderHint,
                    null,
                    null,
                    nodeId
            );
        }

        // 2. Decode and validate request payload
        GetSeatsRequestDto requestDto;
        try {
            requestDto = MessageCodec.decodePayload(request, GetSeatsRequestDto.class);
            if (requestDto == null) {
                return MessageCodec.createErrorResponse(
                        messageId, "INVALID_REQUEST", "Missing get seats request payload", null, null, null, nodeId
                );
            }
            requestDto.validate();
        } catch (IllegalArgumentException | IOException ex) {
            LOGGER.log(Level.WARNING, "GET_SEATS validation failure: " + ex.getMessage());
            return MessageCodec.createErrorResponse(
                    messageId, "INVALID_REQUEST", ex.getMessage(), null, null, null, nodeId
            );
        }

        // 3. Fetch consistent seats snapshot
        BookingService.SeatSnapshot snapshot = bookingService.getSeats(requestDto.getTripId());

        // 4. Construct typed response DTO
        GetSeatsResponseDto responseDto = new GetSeatsResponseDto(
                snapshot.tripId(),
                snapshot.seats(),
                snapshot.readIndex(),
                snapshot.leaderId()
        );
        responseDto.validate();

        // 5. Correlate and return response envelope
        return MessageEnvelope.createResponse(
                messageId,
                MessageType.GET_SEATS_RESPONSE,
                nodeId,
                responseDto
        );
    }
}
