package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("1-Node Booking TCP End-to-End Integration Test (Gate G3)")
public class BookingTcpIntegrationTest {

    private final List<AutoCloseable> resources = new ArrayList<>();
    private NodeMain node;
    private TcpClient client;

    @BeforeEach
    public void setUp() throws IOException {
        // Start live NodeMain on random dynamic ports
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        node = new NodeMain(config);
        resources.add(node);
        node.start();

        client = new TcpClient("127.0.0.1", node.getClientPort(), 3000, 5000);
        resources.add(client);
        client.connect();
        assertTrue(client.isConnected(), "Client must be connected to NodeMain client port");
    }

    @AfterEach
    public void tearDown() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    @Test
    @DisplayName("Should execute full 1-node booking workflow over real TCP socket")
    public void testFullBookingWorkflowOverTcp() throws Exception {
        // 1. Initial GET_SEATS query over TCP
        GetSeatsRequestDto getSeatsDto = new GetSeatsRequestDto("SE-001");
        MessageEnvelope<GetSeatsRequestDto> getSeatsReq = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-app-1", getSeatsDto
        );

        MessageEnvelope<GetSeatsResponseDto> getSeatsResp = client.sendAndReceive(
                getSeatsReq, GetSeatsResponseDto.class
        );
        assertNotNull(getSeatsResp);
        assertEquals(MessageType.GET_SEATS_RESPONSE, getSeatsResp.getType());
        assertEquals(getSeatsReq.getMessageId(), getSeatsResp.getCorrelationId());

        GetSeatsResponseDto initialSeats = getSeatsResp.getPayload();
        initialSeats.validate();
        assertEquals(10, initialSeats.getSeats().size());
        for (SeatDto seat : initialSeats.getSeats()) {
            assertEquals("AVAILABLE", seat.getStatus());
            assertNull(seat.getTicketId());
        }

        // 2. Successful BOOK_TICKET for G05
        String req1Id = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto1 = new BookTicketRequestDto(req1Id, "SE-001", "G05", "Passenger Alpha");
        MessageEnvelope<BookTicketRequestDto> bookReq1 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app-1", bookDto1
        );

        MessageEnvelope<BookTicketResponseDto> bookResp1 = client.sendAndReceive(
                bookReq1, BookTicketResponseDto.class
        );
        assertNotNull(bookResp1);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, bookResp1.getType());
        assertEquals(bookReq1.getMessageId(), bookResp1.getCorrelationId());

        BookTicketResponseDto confirmPayload = bookResp1.getPayload();
        confirmPayload.validate();
        assertTrue(confirmPayload.isConfirmed());
        assertEquals(req1Id, confirmPayload.getRequestId());
        assertEquals("G05", confirmPayload.getSeatId());
        String ticketId = confirmPayload.getTicketId();
        assertNotNull(ticketId);
        assertEquals(1L, confirmPayload.getAppliedLogIndex());

        // 3. Second BOOK_TICKET for same seat G05 with new requestId -> REJECTED
        String req2Id = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto2 = new BookTicketRequestDto(req2Id, "SE-001", "G05", "Passenger Beta");
        MessageEnvelope<BookTicketRequestDto> bookReq2 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app-2", bookDto2
        );

        MessageEnvelope<BookTicketResponseDto> bookResp2 = client.sendAndReceive(
                bookReq2, BookTicketResponseDto.class
        );
        assertNotNull(bookResp2);
        BookTicketResponseDto rejectedPayload = bookResp2.getPayload();
        rejectedPayload.validate();
        assertTrue(rejectedPayload.isRejected());
        assertEquals(req2Id, rejectedPayload.getRequestId());
        assertEquals(BookTicketResponseDto.REASON_SEAT_ALREADY_BOOKED, rejectedPayload.getReasonCode());
        assertNull(rejectedPayload.getTicketId());
        assertEquals(2L, rejectedPayload.getAppliedLogIndex());

        // 4. Retry first BOOK_TICKET with same requestId req1Id -> CONFIRMED (Idempotent replay)
        MessageEnvelope<BookTicketRequestDto> retryReq1 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app-1", bookDto1
        );
        MessageEnvelope<BookTicketResponseDto> retryResp1 = client.sendAndReceive(
                retryReq1, BookTicketResponseDto.class
        );
        BookTicketResponseDto retryPayload = retryResp1.getPayload();
        retryPayload.validate();
        assertTrue(retryPayload.isConfirmed());
        assertEquals(ticketId, retryPayload.getTicketId(), "Idempotent replay must return original ticketId");
        assertEquals(confirmPayload.getAppliedLogIndex(), retryPayload.getAppliedLogIndex());

        // 5. Query GET_SEATS again to verify state machine reflects booking
        MessageEnvelope<GetSeatsRequestDto> secondGetSeatsReq = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-app-1", getSeatsDto
        );
        MessageEnvelope<GetSeatsResponseDto> secondGetSeatsResp = client.sendAndReceive(
                secondGetSeatsReq, GetSeatsResponseDto.class
        );
        GetSeatsResponseDto updatedSeats = secondGetSeatsResp.getPayload();
        updatedSeats.validate();
        SeatDto g05 = updatedSeats.getSeats().stream()
                .filter(s -> "G05".equals(s.getSeatId()))
                .findFirst()
                .orElseThrow();
        assertEquals("BOOKED", g05.getStatus());
        assertEquals(ticketId, g05.getTicketId());

        // 6. Negative test: Send invalid booking request -> Expect ERROR_RESPONSE(INVALID_REQUEST)
        BookTicketRequestDto invalidDto = new BookTicketRequestDto(
                UUID.randomUUID().toString(), "SE-001", "G99", "Invalid Seat"
        );
        MessageEnvelope<BookTicketRequestDto> invalidReq = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app-1", invalidDto
        );
        MessageEnvelope<ErrorResponseDto> errResp = client.sendAndReceive(
                invalidReq, ErrorResponseDto.class
        );
        assertNotNull(errResp);
        assertEquals(MessageType.ERROR_RESPONSE, errResp.getType());
        assertEquals("INVALID_REQUEST", errResp.getPayload().getErrorCode());
        assertEquals(invalidReq.getMessageId(), errResp.getCorrelationId());
    }
}
