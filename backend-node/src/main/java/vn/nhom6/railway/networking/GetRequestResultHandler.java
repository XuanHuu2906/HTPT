package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for {@link MessageType#GET_REQUEST_RESULT} incoming queries (SPEC-03 Section 4.4 and TASK TV3-D19).
 *
 * <p>Enforces:
 * <ul>
 *   <li><b>Follower Redirection:</b> Non-leader nodes return {@code ERROR_RESPONSE(NOT_LEADER)} with {@code leaderHint}.</li>
 *   <li><b>Read Barrier Verification:</b> Ensures queries only return authoritative results post-barrier;
 *       if barrier is unavailable, returns {@code ERROR_RESPONSE(READ_UNAVAILABLE)}.</li>
 *   <li><b>Strict DTO Mapping:</b> Uses {@link ResponseMapper#toGetRequestResultResponse} to build typed envelopes.</li>
 * </ul>
 */
public class GetRequestResultHandler implements HandlerRegistry.RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(GetRequestResultHandler.class.getName());

    private final ResultLookup resultLookup;
    private final String nodeId;
    private final LeaderRouter leaderRouter;

    public GetRequestResultHandler(ResultLookup resultLookup, String nodeId) {
        this(resultLookup, nodeId, null);
    }

    public GetRequestResultHandler(ResultLookup resultLookup, String nodeId, LeaderRouter leaderRouter) {
        this.resultLookup = Objects.requireNonNull(resultLookup, "resultLookup must not be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.leaderRouter = leaderRouter;
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return null;
        }

        String messageId = request.getMessageId();
        String extractedRequestId = ErrorReplyFactory.extractRequestId(request);

        // 1. Enforce Leader authority per SPEC-03 §4.4: "Follower luôn trả NOT_LEADER"
        if (leaderRouter != null) {
            LeaderRouter.RoutingOutcome outcome = leaderRouter.route(request);
            if (outcome != null && outcome.isHandled()) {
                return outcome.getResponse();
            }
        }

        // 2. Decode and validate request payload
        GetRequestResultRequestDto requestDto;
        try {
            requestDto = MessageCodec.decodePayload(request, GetRequestResultRequestDto.class);
            if (requestDto == null) {
                return ResponseMapper.toInvalidRequest(
                        messageId, nodeId, extractedRequestId, "Missing GET_REQUEST_RESULT payload"
                );
            }
            requestDto.validate();
        } catch (IllegalArgumentException | IOException ex) {
            LOGGER.log(Level.WARNING, "GET_REQUEST_RESULT validation failure: " + ex.getMessage());
            return ResponseMapper.toInvalidRequest(
                    messageId, nodeId, extractedRequestId, ex.getMessage()
            );
        }

        String targetRequestId = requestDto.getRequestId();

        // 3. Query ResultLookup boundary (under read barrier)
        ResultLookup.LookupResult lookupResult;
        try {
            lookupResult = resultLookup.lookup(targetRequestId);
            if (lookupResult == null) {
                lookupResult = ResultLookup.LookupResult.unknown(targetRequestId);
            }
        } catch (BookingRequestHandler.NotLeaderException ex) {
            Long term = leaderRouter != null ? leaderRouter.getRoleSnapshot().term() : null;
            return ResponseMapper.toNotLeader(messageId, nodeId, targetRequestId, ex.leaderHint(), term);
        } catch (BookingRequestHandler.ReadUnavailableException ex) {
            Long term = leaderRouter != null ? leaderRouter.getRoleSnapshot().term() : null;
            return ResponseMapper.toReadUnavailable(messageId, nodeId, targetRequestId, term, null);
        }

        // 4. Map to strongly-typed response DTO envelope
        return ResponseMapper.toGetRequestResultResponse(messageId, nodeId, lookupResult);
    }

    public ResultLookup getResultLookup() {
        return resultLookup;
    }

    public String getNodeId() {
        return nodeId;
    }

    public LeaderRouter getLeaderRouter() {
        return leaderRouter;
    }
}
