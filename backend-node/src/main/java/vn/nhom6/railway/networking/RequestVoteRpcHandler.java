package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server-side inbound RPC handler for {@link MessageType#REQUEST_VOTE}.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Validates the wire schema and structure of incoming {@link RequestVoteRequestDto}.</li>
 *   <li>Enforces peer identity rules: {@code senderId == candidateId} and {@code senderId != localNodeId}.</li>
 *   <li>Delegates consensus execution to {@link RaftRpcProcessor}.</li>
 *   <li>Constructs a strictly correlated {@link MessageType#REQUEST_VOTE_RESPONSE} envelope.</li>
 *   <li>Protects node integrity with fail-closed error handling (SPEC-03 Section 7).</li>
 * </ul>
 */
public class RequestVoteRpcHandler implements HandlerRegistry.RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(RequestVoteRpcHandler.class.getName());

    private final RaftRpcProcessor processor;
    private final String localNodeId;

    public RequestVoteRpcHandler(RaftRpcProcessor processor, String localNodeId) {
        this.processor = Objects.requireNonNull(processor, "RaftRpcProcessor must not be null");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
    }

    @Override
    public MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request) throws Exception {
        if (request == null) {
            return null;
        }

        String messageId = request.getMessageId();
        if (messageId == null || messageId.isBlank()) {
            return null;
        }

        // 1. Decode & validate RequestVoteRequestDto
        RequestVoteRequestDto dto;
        try {
            dto = MessageCodec.decodePayload(request, RequestVoteRequestDto.class);
            if (dto == null) {
                throw new IllegalArgumentException("RequestVote payload must not be null");
            }
            dto.validate();
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, () -> "[" + localNodeId + "] Malformed RequestVote payload: " + ex.getMessage());
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "Malformed RequestVote payload: " + ex.getMessage(),
                    null,
                    null,
                    null,
                    localNodeId
            );
        }

        // 2. Validate sender identity
        String senderId = request.getSenderId();
        if (senderId == null || senderId.isBlank()) {
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "senderId must not be null or blank",
                    null,
                    null,
                    null,
                    localNodeId
            );
        }
        if (!senderId.equals(dto.getCandidateId())) {
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "senderId '" + senderId + "' does not match candidateId '" + dto.getCandidateId() + "'",
                    null,
                    null,
                    null,
                    localNodeId
            );
        }
        if (localNodeId.equals(senderId)) {
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "Node cannot process remote vote request for itself",
                    null,
                    null,
                    null,
                    localNodeId
            );
        }

        // 3. Delegate to processor & return correlated response
        try {
            RequestVoteResponseDto responseDto = processor.handleRequestVote(dto, senderId);
            if (responseDto == null) {
                throw new IllegalStateException("RaftRpcProcessor returned null response");
            }
            responseDto.validate();

            return MessageEnvelope.createResponse(
                    messageId,
                    MessageType.REQUEST_VOTE_RESPONSE,
                    localNodeId,
                    responseDto
            );
        } catch (IllegalArgumentException ex) {
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    ex.getMessage(),
                    null,
                    null,
                    null,
                    localNodeId
            );
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Internal error processing RequestVote from " + senderId, ex);
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INTERNAL_ERROR",
                    "Internal error processing RequestVote: " + ex.getMessage(),
                    null,
                    null,
                    null,
                    localNodeId
            );
        }
    }

    public RaftRpcProcessor getProcessor() {
        return processor;
    }

    public String getLocalNodeId() {
        return localNodeId;
    }
}
