package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BookTicketHandler Unit Tests")
public class BookTicketHandlerTest {

    private FakeBookingService fakeService;
    private BookTicketHandler handler;

    @BeforeEach
    public void setUp() {
        fakeService = new FakeBookingService("node-1");
        handler = new BookTicketHandler(fakeService, "node-1");
    }

    private MessageEnvelope<JsonNode> createBookRequestEnvelope(String requestId, String tripId, String seatId, String name) throws Exception {
        BookTicketRequestDto dto = new BookTicketRequestDto(requestId, tripId, seatId, name);
        MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-1", dto
        );
        byte[] bytes = MessageCodec.encode(req);
        return MessageCodec.decode(bytes);
    }

    @Test
    @DisplayName("Should confirm booking when seat is available")
    public void testBookTicketSuccess() throws Exception {
        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> request = createBookRequestEnvelope(reqId, "SE-001", "G05", "Nguyen Van A");

        MessageEnvelope<?> responseEnv = handler.handle(request);
        assertNotNull(responseEnv);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, responseEnv.getType());
        assertEquals("node-1", responseEnv.getSenderId());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());

        BookTicketResponseDto payload = (BookTicketResponseDto) responseEnv.getPayload();
        payload.validate();
        assertTrue(payload.isConfirmed());
        assertEquals(reqId, payload.getRequestId());
        assertEquals("SE-001", payload.getTripId());
        assertEquals("G05", payload.getSeatId());
        assertNotNull(payload.getTicketId());
        assertNull(payload.getReasonCode());
        assertEquals(1L, payload.getAppliedLogIndex());
    }

    @Test
    @DisplayName("Should reject booking with SEAT_ALREADY_BOOKED when seat was already taken")
    public void testBookTicketSeatAlreadyBooked() throws Exception {
        String firstReqId = UUID.randomUUID().toString();
        handler.handle(createBookRequestEnvelope(firstReqId, "SE-001", "G05", "User One"));

        String secondReqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> secondRequest = createBookRequestEnvelope(secondReqId, "SE-001", "G05", "User Two");

        MessageEnvelope<?> responseEnv = handler.handle(secondRequest);
        assertNotNull(responseEnv);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, responseEnv.getType());

        BookTicketResponseDto payload = (BookTicketResponseDto) responseEnv.getPayload();
        payload.validate();
        assertTrue(payload.isRejected());
        assertEquals(secondReqId, payload.getRequestId());
        assertNull(payload.getTicketId());
        assertEquals(BookTicketResponseDto.REASON_SEAT_ALREADY_BOOKED, payload.getReasonCode());
        assertEquals(2L, payload.getAppliedLogIndex());
    }

    @Test
    @DisplayName("Should return identical CONFIRMED response on retry with same requestId (Idempotency AC10)")
    public void testBookTicketIdempotentRetry() throws Exception {
        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> firstRequest = createBookRequestEnvelope(reqId, "SE-001", "G03", "Khach Hang");

        MessageEnvelope<?> firstResp = handler.handle(firstRequest);
        BookTicketResponseDto firstPayload = (BookTicketResponseDto) firstResp.getPayload();
        assertTrue(firstPayload.isConfirmed());
        String ticketId = firstPayload.getTicketId();

        // Retry with same requestId and intent
        MessageEnvelope<JsonNode> retryRequest = createBookRequestEnvelope(reqId, "SE-001", "G03", "Khach Hang");
        MessageEnvelope<?> retryResp = handler.handle(retryRequest);
        BookTicketResponseDto retryPayload = (BookTicketResponseDto) retryResp.getPayload();

        assertTrue(retryPayload.isConfirmed());
        assertEquals(ticketId, retryPayload.getTicketId());
        assertEquals(firstPayload.getAppliedLogIndex(), retryPayload.getAppliedLogIndex());
    }

    @Test
    @DisplayName("Should reject with INVALID_REQUEST when input parameters violate SPEC-03 / SPEC-05")
    public void testBookTicketValidationFailures() throws Exception {
        // 1. Invalid seatId
        MessageEnvelope<JsonNode> invalidSeat = createBookRequestEnvelope(
                UUID.randomUUID().toString(), "SE-001", "G11", "Name"
        );
        MessageEnvelope<?> resp1 = handler.handle(invalidSeat);
        assertEquals(MessageType.ERROR_RESPONSE, resp1.getType());
        ErrorResponseDto err1 = (ErrorResponseDto) resp1.getPayload();
        assertEquals("INVALID_REQUEST", err1.getErrorCode());

        // 2. Invalid tripId
        MessageEnvelope<JsonNode> invalidTrip = createBookRequestEnvelope(
                UUID.randomUUID().toString(), "SE-999", "G01", "Name"
        );
        MessageEnvelope<?> resp2 = handler.handle(invalidTrip);
        assertEquals(MessageType.ERROR_RESPONSE, resp2.getType());
        ErrorResponseDto err2 = (ErrorResponseDto) resp2.getPayload();
        assertEquals("INVALID_REQUEST", err2.getErrorCode());

        // 3. Blank passenger name
        MessageEnvelope<JsonNode> blankName = createBookRequestEnvelope(
                UUID.randomUUID().toString(), "SE-001", "G01", "   "
        );
        MessageEnvelope<?> resp3 = handler.handle(blankName);
        assertEquals(MessageType.ERROR_RESPONSE, resp3.getType());
        ErrorResponseDto err3 = (ErrorResponseDto) resp3.getPayload();
        assertEquals("INVALID_REQUEST", err3.getErrorCode());
    }

    @Test
    @DisplayName("Should return NOT_LEADER error with leaderHint when node is not leader")
    public void testBookTicketNotLeader() throws Exception {
        fakeService.setLeader(false, "node-2");

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> request = createBookRequestEnvelope(reqId, "SE-001", "G01", "Khach");

        MessageEnvelope<?> resp = handler.handle(request);
        assertEquals(MessageType.ERROR_RESPONSE, resp.getType());
        assertEquals(request.getMessageId(), resp.getCorrelationId());

        ErrorResponseDto err = (ErrorResponseDto) resp.getPayload();
        err.validate();
        assertEquals("NOT_LEADER", err.getErrorCode());
        assertEquals("node-2", err.getLeaderHint());
        assertEquals(reqId, err.getRequestId());
    }

    @Test
    @DisplayName("Should return UNKNOWN outcome when booking times out (simulated)")
    public void testBookTicketSimulateUnknown() throws Exception {
        fakeService.setSimulateUnknown(true);

        String reqId = UUID.randomUUID().toString();
        MessageEnvelope<JsonNode> request = createBookRequestEnvelope(reqId, "SE-001", "G01", "Khach");

        MessageEnvelope<?> resp = handler.handle(request);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, resp.getType());

        BookTicketResponseDto payload = (BookTicketResponseDto) resp.getPayload();
        payload.validate();
        assertTrue(payload.isUnknown());
        assertNull(payload.getTicketId());
        assertEquals(BookTicketResponseDto.REASON_TIMEOUT_UNKNOWN, payload.getReasonCode());
        assertNull(payload.getAppliedLogIndex());
    }
}
