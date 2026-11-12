package vn.nhom6.railway.protocol;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteResponseDto;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RaftResponseDtoTest {

    @Test
    @DisplayName("RequestVoteResponseDto: should serialize, deserialize and validate according to SPEC-03 Section 6")
    void testRequestVoteResponseDto() throws Exception {
        // Factory methods
        RequestVoteResponseDto granted = RequestVoteResponseDto.granted(8);
        granted.validate();
        assertEquals(8, granted.getTerm());
        assertTrue(granted.isVoteGranted());

        RequestVoteResponseDto rejected = RequestVoteResponseDto.rejected(8);
        rejected.validate();
        assertEquals(8, rejected.getTerm());
        assertFalse(rejected.isVoteGranted());

        // Serde with MessageCodec
        String requestMsgId = UUID.randomUUID().toString();
        MessageEnvelope<RequestVoteResponseDto> envelope = MessageEnvelope.createResponse(
                requestMsgId,
                MessageType.REQUEST_VOTE_RESPONSE,
                "node-2",
                granted
        );

        byte[] encoded = MessageCodec.encode(envelope);
        MessageEnvelope<RequestVoteResponseDto> decoded = MessageCodec.decode(encoded, RequestVoteResponseDto.class);

        assertEquals(MessageType.REQUEST_VOTE_RESPONSE, decoded.getType());
        assertEquals(requestMsgId, decoded.getCorrelationId());
        assertEquals(granted, decoded.getPayload());
        assertEquals(8, decoded.getPayload().getTerm());
        assertTrue(decoded.getPayload().isVoteGranted());

        // Negative cases
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteResponseDto(-1, true).validate());
    }

    @Test
    @DisplayName("AppendEntriesResponseDto: should serialize, deserialize and validate according to SPEC-03 Section 6")
    void testAppendEntriesResponseDto() throws Exception {
        // Success factory method
        AppendEntriesResponseDto success = AppendEntriesResponseDto.success(8, 43);
        success.validate();
        assertEquals(8, success.getTerm());
        assertTrue(success.isSuccess());
        assertEquals(43, success.getMatchIndex());

        // Failure factory method
        AppendEntriesResponseDto failure = AppendEntriesResponseDto.failure(8);
        failure.validate();
        assertEquals(8, failure.getTerm());
        assertFalse(failure.isSuccess());
        assertEquals(0, failure.getMatchIndex());

        // Serde with MessageCodec
        String requestMsgId = UUID.randomUUID().toString();
        MessageEnvelope<AppendEntriesResponseDto> envelope = MessageEnvelope.createResponse(
                requestMsgId,
                MessageType.APPEND_ENTRIES_RESPONSE,
                "node-2",
                success
        );

        byte[] encoded = MessageCodec.encode(envelope);
        MessageEnvelope<AppendEntriesResponseDto> decoded = MessageCodec.decode(encoded, AppendEntriesResponseDto.class);

        assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, decoded.getType());
        assertEquals(requestMsgId, decoded.getCorrelationId());
        assertEquals(success, decoded.getPayload());
        assertEquals(8, decoded.getPayload().getTerm());
        assertTrue(decoded.getPayload().isSuccess());
        assertEquals(43, decoded.getPayload().getMatchIndex());

        // Negative cases
        assertThrows(IllegalArgumentException.class, () -> new AppendEntriesResponseDto(-1, true, 43).validate());
        assertThrows(IllegalArgumentException.class, () -> new AppendEntriesResponseDto(8, true, -1).validate());
    }
}
