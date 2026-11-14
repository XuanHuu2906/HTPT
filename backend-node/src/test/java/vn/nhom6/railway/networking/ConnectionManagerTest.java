package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for ConnectionManager (TASK TV3-D14)")
public class ConnectionManagerTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        resources.clear();
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
    @DisplayName("Should connect, report isConnected, and close cleanly")
    public void testConnectAndClose() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort());
        resources.add(manager);

        assertFalse(manager.isConnected());
        manager.connect();
        assertTrue(manager.isConnected());

        manager.close();
        assertFalse(manager.isConnected());
    }

    @Test
    @DisplayName("Should reconnect cleanly on demand")
    public void testReconnect() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort());
        resources.add(manager);

        manager.connect();
        assertTrue(manager.isConnected());

        manager.reconnect();
        assertTrue(manager.isConnected());
    }

    @Test
    @DisplayName("BẤT BIẾN AN TOÀN: Timeout không được đổi thành REJECTED/BOOK_FAILED, trả về UNKNOWN (TIMEOUT_UNKNOWN)")
    public void testReadTimeoutReturnsUnknownNeverRejected() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        AtomicBoolean serverRunning = new AtomicBoolean(true);
        Thread serverThread = new Thread(() -> {
            try (Socket s = server.accept()) {
                InputStream in = s.getInputStream();
                // Read incoming frame from client
                FrameCodec.readFrame(in);
                // Sleep longer than readTimeout to force client SocketTimeoutException
                Thread.sleep(400);
            } catch (Exception ignored) {}
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Configure short read timeout (100ms)
        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy);
        resources.add(manager);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G05", "Passenger Timeout");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> responseEnv = manager.sendAndReceive(request, BookTicketResponseDto.class);

        // Verification of distributed invariants:
        assertNotNull(responseEnv);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, responseEnv.getType());
        assertEquals(request.getMessageId(), responseEnv.getCorrelationId());

        BookTicketResponseDto responseDto = responseEnv.getPayload();
        assertNotNull(responseDto);

        // 1. MUST NOT BE REJECTED OR CONFIRMED
        assertFalse(responseDto.isConfirmed(), "Timed-out request must not be reported as CONFIRMED");
        assertFalse(responseDto.isRejected(), "BẤT BIẾN: Timed-out request TUYỆT ĐỐI KHÔNG ĐƯỢC CHUYỂN THÀNH REJECTED");
        assertNotEquals("BOOK_FAILED", responseDto.getReasonCode(), "Timeout không được gán mã BOOK_FAILED");

        // 2. MUST BE UNKNOWN with TIMEOUT_UNKNOWN
        assertEquals("UNKNOWN", responseDto.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", responseDto.getReasonCode());
        assertNull(responseDto.getTicketId());

        // 3. RequestId MUST BE PRESERVED VERBATIM
        assertEquals(reqId, responseDto.getRequestId(), "requestId phải được giữ nguyên vẹn 100%");

        // 4. Socket must have been safely closed on timeout
        assertFalse(manager.isConnected(), "Poisoned socket must be closed immediately upon timeout");
    }

    @Test
    @DisplayName("Should support untyped sendAndReceive returning JsonNode with UNKNOWN on timeout")
    public void testUntypedSendAndReceiveTimeout() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        Thread serverThread = new Thread(() -> {
            try (Socket s = server.accept()) {
                FrameCodec.readFrame(s.getInputStream());
                Thread.sleep(300);
            } catch (Exception ignored) {}
        });
        serverThread.setDaemon(true);
        serverThread.start();

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy);
        resources.add(manager);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G05", "Passenger JsonNode");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<JsonNode> responseEnv = manager.sendAndReceive(request);
        assertNotNull(responseEnv);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, responseEnv.getType());

        JsonNode payload = responseEnv.getPayload();
        assertEquals("UNKNOWN", payload.get("outcome").asText());
        assertEquals("TIMEOUT_UNKNOWN", payload.get("reasonCode").asText());
        assertEquals(reqId, payload.get("requestId").asText());
    }

    @Test
    @DisplayName("Should recover CONFIRMED outcome when FakeResultLookup reports APPLIED_SUCCESS on timeout")
    public void testReadTimeoutWithLookupSuccess() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        Thread serverThread = new Thread(() -> {
            try (Socket s = server.accept()) {
                FrameCodec.readFrame(s.getInputStream());
                Thread.sleep(300); // Trigger timeout
            } catch (Exception ignored) {}
        });
        serverThread.setDaemon(true);
        serverThread.start();

        String reqId = UUID.randomUUID().toString();
        FakeResultLookup fakeLookup = new FakeResultLookup();
        fakeLookup.registerSuccess(reqId, "BOOK_TICKET", "TKT-CONFIRMED-99", 42L);

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy, fakeLookup);
        resources.add(manager);

        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G08", "Passenger Alpha");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> responseEnv = manager.sendAndReceive(request, BookTicketResponseDto.class);
        BookTicketResponseDto responseDto = responseEnv.getPayload();

        assertTrue(responseDto.isConfirmed());
        assertEquals("TKT-CONFIRMED-99", responseDto.getTicketId());
        assertEquals(42L, responseDto.getAppliedLogIndex());
        assertEquals(reqId, responseDto.getRequestId());
        assertEquals(1, fakeLookup.getLookupCallCount());
    }

    @Test
    @DisplayName("Should recover REJECTED outcome from DB state when FakeResultLookup reports APPLIED_REJECTED")
    public void testReadTimeoutWithLookupRejected() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        Thread serverThread = new Thread(() -> {
            try (Socket s = server.accept()) {
                FrameCodec.readFrame(s.getInputStream());
                Thread.sleep(300); // Trigger timeout
            } catch (Exception ignored) {}
        });
        serverThread.setDaemon(true);
        serverThread.start();

        String reqId = UUID.randomUUID().toString();
        FakeResultLookup fakeLookup = new FakeResultLookup();
        fakeLookup.registerRejected(reqId, "BOOK_TICKET", "SEAT_ALREADY_BOOKED", 43L);

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy, fakeLookup);
        resources.add(manager);

        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G08", "Passenger Beta");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> responseEnv = manager.sendAndReceive(request, BookTicketResponseDto.class);
        BookTicketResponseDto responseDto = responseEnv.getPayload();

        assertTrue(responseDto.isRejected());
        assertEquals("SEAT_ALREADY_BOOKED", responseDto.getReasonCode());
        assertEquals(43L, responseDto.getAppliedLogIndex());
        assertEquals(reqId, responseDto.getRequestId());
        assertEquals(1, fakeLookup.getLookupCallCount());
    }

    @Test
    @DisplayName("Should fallback to UNKNOWN when FakeResultLookup throws an exception")
    public void testReadTimeoutWithLookupException() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        Thread serverThread = new Thread(() -> {
            try (Socket s = server.accept()) {
                FrameCodec.readFrame(s.getInputStream());
                Thread.sleep(300); // Trigger timeout
            } catch (Exception ignored) {}
        });
        serverThread.setDaemon(true);
        serverThread.start();

        String reqId = UUID.randomUUID().toString();
        FakeResultLookup fakeLookup = new FakeResultLookup();
        fakeLookup.setThrowException(true, new java.io.IOException("Database connection dead"));

        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy, fakeLookup);
        resources.add(manager);

        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G01", "Passenger Gamma");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> responseEnv = manager.sendAndReceive(request, BookTicketResponseDto.class);
        BookTicketResponseDto responseDto = responseEnv.getPayload();

        assertEquals("UNKNOWN", responseDto.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", responseDto.getReasonCode());
        assertEquals(reqId, responseDto.getRequestId());
    }
}
