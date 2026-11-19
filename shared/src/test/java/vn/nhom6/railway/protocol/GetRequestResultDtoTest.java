package vn.nhom6.railway.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.GetRequestResultRequestDto;
import vn.nhom6.railway.protocol.dto.GetRequestResultResponseDto;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for GetRequestResultRequestDto and GetRequestResultResponseDto (SPEC-03 §4.4)")
public class GetRequestResultDtoTest {

    private final ObjectMapper mapper = MessageCodec.getMapper();

    @Test
    @DisplayName("GetRequestResultRequestDto: serialization round-trip and validation")
    public void testRequestDtoRoundTrip() throws Exception {
        String reqId = UUID.randomUUID().toString();
        GetRequestResultRequestDto dto = new GetRequestResultRequestDto(reqId);
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"requestId\":\"" + reqId + "\""));

        GetRequestResultRequestDto decoded = mapper.readValue(json, GetRequestResultRequestDto.class);
        assertEquals(dto, decoded);
        assertEquals(reqId, decoded.getRequestId());

        // Negative cases
        assertThrows(IllegalArgumentException.class, () -> new GetRequestResultRequestDto(null).validate());
        assertThrows(IllegalArgumentException.class, () -> new GetRequestResultRequestDto("   ").validate());
        assertThrows(IllegalArgumentException.class, () -> new GetRequestResultRequestDto("not-a-valid-uuid").validate());
    }

    @Test
    @DisplayName("GetRequestResultResponseDto: APPLIED_SUCCESS state validation and serialization")
    public void testResponseDtoAppliedSuccess() throws Exception {
        String reqId = UUID.randomUUID().toString();
        GetRequestResultResponseDto dto = GetRequestResultResponseDto.appliedSuccess(
                reqId, "BOOK_TICKET", "TKT-123456", 43L
        );
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"state\":\"APPLIED_SUCCESS\""));
        assertTrue(json.contains("\"ticketId\":\"TKT-123456\""));
        assertTrue(json.contains("\"appliedLogIndex\":43"));

        GetRequestResultResponseDto decoded = mapper.readValue(json, GetRequestResultResponseDto.class);
        assertEquals(dto, decoded);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_SUCCESS, decoded.getState());
        assertEquals("BOOK_TICKET", decoded.getCommandType());
        assertEquals("TKT-123456", decoded.getTicketId());
        assertNull(decoded.getReasonCode());
        assertEquals(43L, decoded.getAppliedLogIndex());

        // Negative: null ticketId for BOOK_TICKET APPLIED_SUCCESS
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "BOOK_TICKET", null, null, 43L).validate());

        // Negative: appliedLogIndex < 1
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "BOOK_TICKET", "TKT-01", null, 0L).validate());

        // Negative: non-null reasonCode
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "BOOK_TICKET", "TKT-01", "SEAT_ALREADY_BOOKED", 43L).validate());
    }

    @Test
    @DisplayName("GetRequestResultResponseDto: APPLIED_REJECTED state validation and serialization")
    public void testResponseDtoAppliedRejected() throws Exception {
        String reqId = UUID.randomUUID().toString();
        GetRequestResultResponseDto dto = GetRequestResultResponseDto.appliedRejected(
                reqId, "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", 45L
        );
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"state\":\"APPLIED_REJECTED\""));
        assertTrue(json.contains("\"reasonCode\":\"SEAT_ALREADY_BOOKED\""));
        assertTrue(json.contains("\"appliedLogIndex\":45"));

        GetRequestResultResponseDto decoded = mapper.readValue(json, GetRequestResultResponseDto.class);
        assertEquals(dto, decoded);
        assertEquals(GetRequestResultResponseDto.STATE_APPLIED_REJECTED, decoded.getState());
        assertNull(decoded.getTicketId());
        assertEquals("SEAT_ALREADY_BOOKED", decoded.getReasonCode());

        // Negative: ticketId present for BOOK_TICKET REJECTED
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "APPLIED_REJECTED", "BOOK_TICKET", "TKT-123", "SEAT_ALREADY_BOOKED", 45L).validate());

        // Negative: null reasonCode
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "APPLIED_REJECTED", "BOOK_TICKET", null, null, 45L).validate());
    }

    @Test
    @DisplayName("GetRequestResultResponseDto: PENDING state validation and serialization")
    public void testResponseDtoPending() throws Exception {
        String reqId = UUID.randomUUID().toString();
        GetRequestResultResponseDto dto = GetRequestResultResponseDto.pending(reqId, "BOOK_TICKET");
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"state\":\"PENDING\""));
        assertTrue(json.contains("\"commandType\":\"BOOK_TICKET\""));

        GetRequestResultResponseDto decoded = mapper.readValue(json, GetRequestResultResponseDto.class);
        assertEquals(dto, decoded);
        assertEquals(GetRequestResultResponseDto.STATE_PENDING, decoded.getState());
        assertEquals("BOOK_TICKET", decoded.getCommandType());
        assertNull(decoded.getTicketId());
        assertNull(decoded.getReasonCode());
        assertNull(decoded.getAppliedLogIndex());

        // Negative: appliedLogIndex present in PENDING
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "PENDING", "BOOK_TICKET", null, null, 10L).validate());
    }

    @Test
    @DisplayName("GetRequestResultResponseDto: UNKNOWN state validation and serialization")
    public void testResponseDtoUnknown() throws Exception {
        String reqId = UUID.randomUUID().toString();
        GetRequestResultResponseDto dto = GetRequestResultResponseDto.unknown(reqId);
        dto.validate();

        String json = mapper.writeValueAsString(dto);
        assertTrue(json.contains("\"state\":\"UNKNOWN\""));

        GetRequestResultResponseDto decoded = mapper.readValue(json, GetRequestResultResponseDto.class);
        assertEquals(dto, decoded);
        assertEquals(GetRequestResultResponseDto.STATE_UNKNOWN, decoded.getState());
        assertNull(decoded.getCommandType());
        assertNull(decoded.getTicketId());
        assertNull(decoded.getReasonCode());
        assertNull(decoded.getAppliedLogIndex());

        // Negative: commandType present in UNKNOWN
        assertThrows(IllegalArgumentException.class, () ->
                new GetRequestResultResponseDto(reqId, "UNKNOWN", "BOOK_TICKET", null, null, null).validate());
    }
}
