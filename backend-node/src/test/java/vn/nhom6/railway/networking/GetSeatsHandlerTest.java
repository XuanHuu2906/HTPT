package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GetSeatsHandler Unit Tests")
public class GetSeatsHandlerTest {

    private FakeBookingService fakeService;
    private GetSeatsHandler handler;

    @BeforeEach
    public void setUp() {
        fakeService = new FakeBookingService("node-1");
        handler = new GetSeatsHandler(fakeService, "node-1");
    }

    private MessageEnvelope<JsonNode> createGetSeatsRequestEnvelope(String tripId) throws Exception {
        GetSeatsRequestDto dto = new GetSeatsRequestDto(tripId);
        MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-1", dto
        );
        byte[] bytes = MessageCodec.encode(req);
        return MessageCodec.decode(bytes);
    }

    @Test
    @DisplayName("Should return exactly 10 AVAILABLE seats initially for SE-001")
    public void testGetAllSeatsInitiallyAvailable() throws Exception {
        MessageEnvelope<JsonNode> request = createGetSeatsRequestEnvelope("SE-001");

        MessageEnvelope<?> responseEnv = handler.handle(request);
        assertNotNull(responseEnv);
        assertEquals(MessageType.GET_SEATS_RESPONSE, responseEnv.getType());
        assertEquals("node-1", responseEnv.getSenderId());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());

        GetSeatsResponseDto payload = (GetSeatsResponseDto) responseEnv.getPayload();
        payload.validate();
        assertEquals("SE-001", payload.getTripId());
        assertEquals(0L, payload.getReadIndex());
        assertEquals("node-1", payload.getLeaderId());

        List<SeatDto> seats = payload.getSeats();
        assertEquals(10, seats.size());
        for (int i = 0; i < 10; i++) {
            SeatDto seat = seats.get(i);
            String expectedSeatId = String.format("G%02d", i + 1);
            assertEquals(expectedSeatId, seat.getSeatId());
            assertEquals("AVAILABLE", seat.getStatus());
            assertNull(seat.getTicketId());
        }
    }

    @Test
    @DisplayName("Should reflect booked seats after booking commands are executed")
    public void testGetSeatsAfterBooking() throws Exception {
        // Book G03 and G07 via fakeService
        fakeService.bookTicket(UUID.randomUUID().toString(), "SE-001", "G03", "Pass 1");
        fakeService.bookTicket(UUID.randomUUID().toString(), "SE-001", "G07", "Pass 2");

        MessageEnvelope<JsonNode> request = createGetSeatsRequestEnvelope("SE-001");
        MessageEnvelope<?> responseEnv = handler.handle(request);
        GetSeatsResponseDto payload = (GetSeatsResponseDto) responseEnv.getPayload();
        payload.validate();

        assertEquals(2L, payload.getReadIndex());

        List<SeatDto> seats = payload.getSeats();
        assertEquals("BOOKED", seats.get(2).getStatus()); // G03 (index 2)
        assertNotNull(seats.get(2).getTicketId());

        assertEquals("BOOKED", seats.get(6).getStatus()); // G07 (index 6)
        assertNotNull(seats.get(6).getTicketId());

        assertEquals("AVAILABLE", seats.get(0).getStatus()); // G01
        assertNull(seats.get(0).getTicketId());
    }

    @Test
    @DisplayName("Should return INVALID_REQUEST when tripId is invalid")
    public void testGetSeatsInvalidTripId() throws Exception {
        MessageEnvelope<JsonNode> request = createGetSeatsRequestEnvelope("SE-999");
        MessageEnvelope<?> responseEnv = handler.handle(request);

        assertEquals(MessageType.ERROR_RESPONSE, responseEnv.getType());
        ErrorResponseDto err = (ErrorResponseDto) responseEnv.getPayload();
        assertEquals("INVALID_REQUEST", err.getErrorCode());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());
    }

    @Test
    @DisplayName("Should return NOT_LEADER error when node is not leader")
    public void testGetSeatsNotLeader() throws Exception {
        fakeService.setLeader(false, "node-3");

        MessageEnvelope<JsonNode> request = createGetSeatsRequestEnvelope("SE-001");
        MessageEnvelope<?> responseEnv = handler.handle(request);

        assertEquals(MessageType.ERROR_RESPONSE, responseEnv.getType());
        ErrorResponseDto err = (ErrorResponseDto) responseEnv.getPayload();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-3", err.getLeaderHint());
    }
}
