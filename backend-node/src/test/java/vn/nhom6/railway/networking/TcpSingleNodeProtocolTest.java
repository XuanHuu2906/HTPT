package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TCP Single-Node Protocol Tests & Hardening (TV3-D10 / Gate G3)")
public class TcpSingleNodeProtocolTest {

    private final List<AutoCloseable> resources = new ArrayList<>();
    private NodeMain node;
    private int clientPort;

    @BeforeEach
    public void setUp() throws IOException {
        NodeConfig config = new NodeConfig("node-1", "railway-demo-5", "127.0.0.1", 0, "127.0.0.1", 0, null, 8, 5000);
        node = new NodeMain(config, new FakeBookingService(config.nodeId()));
        resources.add(node);
        node.start();
        clientPort = node.getClientPort();
        assertTrue(clientPort > 0, "Client port must be bound to a dynamic port > 0");
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

    private TcpClient createConnectedClient() throws IOException {
        TcpClient client = new TcpClient("127.0.0.1", clientPort, 2000, 5000);
        resources.add(client);
        client.connect();
        assertTrue(client.isConnected());
        return client;
    }

    @Test
    @DisplayName("Smoke Test: PING raw frame receives PONG response")
    public void testSmokePingPong() throws Exception {
        TcpClient client = createConnectedClient();
        byte[] response = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(response, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Smoke Test: NODE_STATUS request receives NODE_STATUS_RESPONSE")
    public void testSmokeNodeStatus() throws Exception {
        TcpClient client = createConnectedClient();
        MessageEnvelope<JsonNode> statusReq = MessageEnvelope.createRequest(
                MessageType.NODE_STATUS, "monitor-app", MessageCodec.getMapper().createObjectNode()
        );

        MessageEnvelope<JsonNode> response = client.sendAndReceive(statusReq);
        assertNotNull(response);
        assertEquals(MessageType.NODE_STATUS_RESPONSE, response.getType());
        assertEquals("node-1", response.getSenderId());
        assertEquals(statusReq.getMessageId(), response.getCorrelationId());

        JsonNode payload = response.getPayload();
        assertEquals("node-1", payload.get("nodeId").asText());
        assertEquals("FOLLOWER", payload.get("role").asText());
        assertNotNull(payload.get("observedAt"));
    }

    @Test
    @DisplayName("Smoke Test: GET_SEATS and BOOK_TICKET full workflow with idempotency retry")
    public void testSmokeGetSeatsAndBookTicketWorkflow() throws Exception {
        TcpClient client = createConnectedClient();

        // 1. Initial GET_SEATS
        GetSeatsRequestDto getSeatsDto = new GetSeatsRequestDto("SE-001");
        MessageEnvelope<GetSeatsRequestDto> seatsReq = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-app", getSeatsDto
        );
        MessageEnvelope<GetSeatsResponseDto> seatsResp = client.sendAndReceive(seatsReq, GetSeatsResponseDto.class);
        assertNotNull(seatsResp);
        assertEquals(10, seatsResp.getPayload().getSeats().size());

        // 2. Book seat G03
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G03", "Khach Hang Alpha");
        MessageEnvelope<BookTicketRequestDto> bookReq = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );
        MessageEnvelope<BookTicketResponseDto> bookResp = client.sendAndReceive(bookReq, BookTicketResponseDto.class);
        assertNotNull(bookResp);
        assertTrue(bookResp.getPayload().isConfirmed());
        assertEquals("G03", bookResp.getPayload().getSeatId());
        String ticketId = bookResp.getPayload().getTicketId();
        assertNotNull(ticketId);

        // 3. Duplicate booking on G03 with different requestId -> REJECTED
        String reqId2 = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto2 = new BookTicketRequestDto(reqId2, "SE-001", "G03", "Khach Hang Beta");
        MessageEnvelope<BookTicketRequestDto> bookReq2 = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app-2", bookDto2
        );
        MessageEnvelope<BookTicketResponseDto> bookResp2 = client.sendAndReceive(bookReq2, BookTicketResponseDto.class);
        assertNotNull(bookResp2);
        assertTrue(bookResp2.getPayload().isRejected());
        assertEquals(BookTicketResponseDto.REASON_SEAT_ALREADY_BOOKED, bookResp2.getPayload().getReasonCode());

        // 4. Retry first booking with same requestId -> CONFIRMED (Idempotent replay AC10)
        MessageEnvelope<BookTicketResponseDto> retryResp = client.sendAndReceive(bookReq, BookTicketResponseDto.class);
        assertNotNull(retryResp);
        assertTrue(retryResp.getPayload().isConfirmed());
        assertEquals(ticketId, retryResp.getPayload().getTicketId());

        // 5. Query GET_SEATS again to verify G03 is BOOKED
        MessageEnvelope<GetSeatsResponseDto> updatedSeatsResp = client.sendAndReceive(seatsReq, GetSeatsResponseDto.class);
        SeatDto g03 = updatedSeatsResp.getPayload().getSeats().get(2);
        assertEquals("G03", g03.getSeatId());
        assertEquals("BOOKED", g03.getStatus());
        assertEquals(ticketId, g03.getTicketId());
    }

    @Test
    @DisplayName("Golden Fixture: BOOK_TICKET with complex Vietnamese Unicode name round-trip over TCP")
    public void testGoldenFixturesBookAndSeats() throws Exception {
        TcpClient client = createConnectedClient();

        String vietnameseName = "Nguyễn Trần Khánh Đan - Phòng vé Hà Nội";
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G08", vietnameseName);
        MessageEnvelope<BookTicketRequestDto> bookReq = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-golden", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> resp = client.sendAndReceive(bookReq, BookTicketResponseDto.class);
        assertNotNull(resp);
        assertEquals(bookReq.getMessageId(), resp.getCorrelationId());
        assertTrue(resp.getPayload().isConfirmed());
        assertEquals("G08", resp.getPayload().getSeatId());
        assertEquals(reqId, resp.getPayload().getRequestId());
    }

    @Test
    @DisplayName("Disconnect Resilience: Abrupt client disconnect cleans up without leaking or crashing server")
    public void testDisconnectResilienceClientAbruptClose() throws Exception {
        // Client 1 connects, sends PING, then abruptly closes socket
        try (Socket rawSocket = new Socket("127.0.0.1", clientPort)) {
            OutputStream out = new BufferedOutputStream(rawSocket.getOutputStream());
            InputStream in = rawSocket.getInputStream();

            FrameCodec.writeFrame(out, "PING".getBytes(StandardCharsets.UTF_8));
            byte[] resp = FrameCodec.readFrame(in);
            assertEquals("PONG", new String(resp, StandardCharsets.UTF_8));

            // Force abrupt socket close
            rawSocket.close();
        }

        // Wait brief moment for server socket cleanup
        Thread.sleep(100);

        // Client 2 connects immediately and operates without issue
        TcpClient client2 = createConnectedClient();
        byte[] ping2 = client2.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(ping2, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Disconnect Resilience: Rapid connect-and-disconnect loop does not exhaust server threads")
    public void testRapidConnectDisconnectLoop() throws Exception {
        for (int i = 0; i < 15; i++) {
            try (Socket s = new Socket("127.0.0.1", clientPort)) {
                assertTrue(s.isConnected());
                // Immediately close without sending frames
            }
        }

        // After 15 rapid disconnects, server still handles traffic cleanly
        TcpClient client = createConnectedClient();
        GetSeatsRequestDto getSeatsDto = new GetSeatsRequestDto("SE-001");
        MessageEnvelope<GetSeatsRequestDto> seatsReq = MessageEnvelope.createRequest(
                MessageType.GET_SEATS, "client-app", getSeatsDto
        );
        MessageEnvelope<GetSeatsResponseDto> resp = client.sendAndReceive(seatsReq, GetSeatsResponseDto.class);
        assertNotNull(resp);
        assertEquals(10, resp.getPayload().getSeats().size());
    }

    @Test
    @DisplayName("Disconnect Resilience: Malformed frame length (> 1 MiB) causes server to close connection per SPEC-03 §2")
    public void testMalformedFrameLengthClosesConnection() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", clientPort)) {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            InputStream in = socket.getInputStream();

            // Send length prefix 2,000,000 (> 1,048,576 byte limit in FrameCodec & SPEC-03 Section 2)
            dos.writeInt(2_000_000);
            dos.flush();

            // Server must close the connection without sending response
            int b = in.read();
            assertEquals(-1, b, "Server must close connection on framing violation");
        }

        // Normal client can still connect afterwards
        TcpClient normalClient = createConnectedClient();
        byte[] resp = normalClient.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(resp, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Disconnect Resilience: Corrupt non-JSON bytes cause server to close connection per SPEC-03 §2")
    public void testCorruptJsonClosesConnection() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", clientPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            byte[] corruptData = "{broken json content not valid at all...".getBytes(StandardCharsets.UTF_8);
            FrameCodec.writeFrame(out, corruptData);

            // Server attempts to decode as JSON, fails, and closes connection per SPEC-03 Section 2
            int b = in.read();
            assertEquals(-1, b, "Server must close connection on corrupt JSON frame");
        }

        // Normal client can still connect afterwards
        TcpClient normalClient = createConnectedClient();
        byte[] resp = normalClient.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(resp, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Correlation Integrity: Multiple sequential requests have strict 1-to-1 correlationId matching")
    public void testCorrelationIdStrictMatching() throws Exception {
        TcpClient client = createConnectedClient();

        for (int i = 1; i <= 5; i++) {
            GetSeatsRequestDto dto = new GetSeatsRequestDto("SE-001");
            MessageEnvelope<GetSeatsRequestDto> req = MessageEnvelope.createRequest(
                    MessageType.GET_SEATS, "client-seq-" + i, dto
            );

            MessageEnvelope<GetSeatsResponseDto> resp = client.sendAndReceive(req, GetSeatsResponseDto.class);
            assertNotNull(resp);
            assertEquals(req.getMessageId(), resp.getCorrelationId(), "correlationId must strictly match request messageId");
        }
    }

    @Test
    @DisplayName("Correlation Integrity: Request with non-null correlationId fails envelope validation and server closes connection per SPEC-03 §2")
    public void testRequestWithNonNullCorrelationIdRejected() throws Exception {
        TcpClient client = createConnectedClient();

        // Build raw JSON with non-null correlationId on a request
        String fakeMsgId = UUID.randomUUID().toString();
        String fakeCorrId = UUID.randomUUID().toString();
        String rawJson = "{\"protocolVersion\":1,\"messageId\":\"" + fakeMsgId + "\",\"correlationId\":\"" + fakeCorrId + "\",\"type\":\"GET_SEATS\",\"senderId\":\"client-1\",\"sentAt\":\"2026-10-29T10:00:00Z\",\"payload\":{\"tripId\":\"SE-001\"}}";

        // Server fails to decode envelope due to SPEC-03 §3.0 rule and closes connection per SPEC-03 §2
        assertThrows(EOFException.class, () -> client.sendAndReceive(rawJson.getBytes(StandardCharsets.UTF_8)));

        // Normal client can still connect afterwards
        TcpClient normalClient = createConnectedClient();
        byte[] resp = normalClient.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(resp, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Correlation Integrity: Request with invalid non-UUID messageId is dropped without uncorrelatable response")
    public void testRequestWithInvalidMessageIdDropped() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", clientPort)) {
            socket.setSoTimeout(1000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // Request with invalid messageId (not a UUID)
            String rawJson = "{\"protocolVersion\":1,\"messageId\":\"not-a-valid-uuid\",\"correlationId\":null,\"type\":\"GET_SEATS\",\"senderId\":\"client-1\",\"sentAt\":\"2026-10-29T10:00:00Z\",\"payload\":{\"tripId\":\"SE-001\"}}";
            FrameCodec.writeFrame(out, rawJson.getBytes(StandardCharsets.UTF_8));

            // Server drops it because messageId cannot be correlated (SPEC-03 Section 2)
            // Socket read should either time out or return nothing
            assertThrows(Exception.class, () -> FrameCodec.readFrame(in));
        }

        // Server remains completely healthy
        TcpClient normalClient = createConnectedClient();
        byte[] ping = normalClient.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG", new String(ping, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Port Isolation: Raft RPC (REQUEST_VOTE) on Client Port is strictly rejected with INVALID_REQUEST")
    public void testPortIsolationRaftRpcOnClientPortRejected() throws Exception {
        TcpClient client = createConnectedClient();

        RequestVoteRequestDto votePayload = new RequestVoteRequestDto(2L, "node-attacker", 0L, 0L);
        MessageEnvelope<RequestVoteRequestDto> voteReq = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, "node-attacker", votePayload
        );

        MessageEnvelope<ErrorResponseDto> errResp = client.sendAndReceive(voteReq, ErrorResponseDto.class);
        assertNotNull(errResp);
        assertEquals(MessageType.ERROR_RESPONSE, errResp.getType());
        assertEquals(voteReq.getMessageId(), errResp.getCorrelationId());
        assertEquals("INVALID_REQUEST", errResp.getPayload().getErrorCode());
        assertTrue(errResp.getPayload().getMessage().contains("client channel"));
    }
}
