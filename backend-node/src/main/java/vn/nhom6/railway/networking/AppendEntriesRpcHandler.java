package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server-side inbound RPC handler for {@link MessageType#APPEND_ENTRIES}.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Validates the wire schema and structure of incoming {@link AppendEntriesRequestDto}.</li>
 *   <li>Enforces peer identity rules: {@code senderId == leaderId}.</li>
 *   <li>Delegates consensus execution (log replication or heartbeat) to {@link RaftRpcProcessor}.</li>
 *   <li>Constructs a strictly correlated {@link MessageType#APPEND_ENTRIES_RESPONSE} envelope.</li>
 *   <li>Protects node integrity with fail-closed error handling (SPEC-03 Section 7).</li>
 * </ul>
 */
public class AppendEntriesRpcHandler implements HandlerRegistry.RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(AppendEntriesRpcHandler.class.getName());

    private final RaftRpcProcessor processor;
    private final String localNodeId;

    public AppendEntriesRpcHandler(RaftRpcProcessor processor, String localNodeId) {
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

        // 1. Decode & validate AppendEntriesRequestDto
        AppendEntriesRequestDto dto;
        try {
            dto = MessageCodec.decodePayload(request, AppendEntriesRequestDto.class);
            if (dto == null) {
                throw new IllegalArgumentException("AppendEntries payload must not be null");
            }
            dto.validate();
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, () -> "[" + localNodeId + "] Malformed AppendEntries payload: " + ex.getMessage());
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "Malformed AppendEntries payload: " + ex.getMessage(),
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
        if (!senderId.equals(dto.getLeaderId())) {
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INVALID_REQUEST",
                    "senderId '" + senderId + "' does not match leaderId '" + dto.getLeaderId() + "'",
                    null,
                    null,
                    null,
                    localNodeId
            );
        }

        // 3. Delegate to processor & return correlated response
        try {
            AppendEntriesResponseDto responseDto = processor.handleAppendEntries(dto, senderId);
            if (responseDto == null) {
                throw new IllegalStateException("RaftRpcProcessor returned null response");
            }
            responseDto.validate();

            return MessageEnvelope.createResponse(
                    messageId,
                    MessageType.APPEND_ENTRIES_RESPONSE,
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
            LOGGER.log(Level.SEVERE, "Internal error processing AppendEntries from " + senderId, ex);
            return MessageCodec.createErrorResponse(
                    messageId,
                    "INTERNAL_ERROR",
                    "Internal error processing AppendEntries: " + ex.getMessage(),
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
