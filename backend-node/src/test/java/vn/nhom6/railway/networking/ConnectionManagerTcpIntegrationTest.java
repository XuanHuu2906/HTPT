package vn.nhom6.railway.networking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TCP Socket Integration Tests for ConnectionManager (TASK TV3-D14)")
public class ConnectionManagerTcpIntegrationTest {

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
    @DisplayName("Normal TCP booking workflow through ConnectionManager")
    public void testNormalBookingWorkflow() throws Exception {
        NodeConfig config = new NodeConfig("node-1", "cluster-1", "127.0.0.1", 0, "127.0.0.1", 0, null, 4, 3000);
        FakeBookingService service = new FakeBookingService("node-1");
        service.setLeader(true, "node-1");

        NodeMain node = new NodeMain(config, service);
        resources.add(node);
        node.start();

        ConnectionManager manager = new ConnectionManager("127.0.0.1", node.getClientPort());
        resources.add(manager);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookDto = new BookTicketRequestDto(reqId, "SE-001", "G01", "Normal Passenger");
        MessageEnvelope<BookTicketRequestDto> request = MessageEnvelope.createRequest(
                MessageType.BOOK_TICKET, "client-app", bookDto
        );

        MessageEnvelope<BookTicketResponseDto> responseEnv = manager.sendAndReceive(request, BookTicketResponseDto.class);
        assertNotNull(responseEnv);
        assertEquals(MessageType.BOOK_TICKET_RESPONSE, responseEnv.getType());

        BookTicketResponseDto responseDto = responseEnv.getPayload();
        assertTrue(responseDto.isConfirmed());
        assertEquals(reqId, responseDto.getRequestId());
        assertNotNull(responseDto.getTicketId());
    }

    @Test
    @DisplayName("Timeout on TCP socket returns UNKNOWN (TIMEOUT_UNKNOWN) and safely reconnects on next request")
    public void testTimeoutAndSubsequentReconnect() throws Exception {
        ServerSocket server = new ServerSocket(0);
        resources.add(server);

        AtomicBoolean delayActive = new AtomicBoolean(true);
        AtomicInteger requestsReceived = new AtomicInteger(0);

        Thread serverThread = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    Socket s = server.accept();
                    new Thread(() -> {
                        try (Socket clientSocket = s) {
                            InputStream in = clientSocket.getInputStream();
                            OutputStream out = clientSocket.getOutputStream();

                            while (!clientSocket.isClosed()) {
                                byte[] reqFrame = FrameCodec.readFrame(in);
                                requestsReceived.incrementAndGet();
                                MessageEnvelope<?> req = MessageCodec.decode(reqFrame);

                                if (delayActive.get()) {
                                    // Delay past read timeout
                                    Thread.sleep(300);
                                }

                                BookTicketResponseDto confirm = new BookTicketResponseDto(
                                        ErrorReplyFactory.extractRequestId(req),
                                        "CONFIRMED", "TKT-OK", "SE-001", "G02", null, 10L
                                );
                                MessageEnvelope<BookTicketResponseDto> resp = MessageEnvelope.createResponse(
                                        req.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "server", confirm
                                );
                                FrameCodec.writeFrame(out, MessageCodec.encode(resp));
                            }
                        } catch (Exception ignored) {}
                    }).start();
                } catch (Exception ignored) {}
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // 1. First Request: Experiences Timeout (readTimeout = 100ms)
        RetryPolicy policy = new RetryPolicy(1, 0, 0, 1.0, 2000, 100);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", server.getLocalPort(), policy);
        resources.add(manager);

        String req1Id = UUID.randomUUID().toString();
        BookTicketRequestDto book1 = new BookTicketRequestDto(req1Id, "SE-001", "G02", "Passenger Timeout Test");
        MessageEnvelope<BookTicketRequestDto> req1 = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client", book1);

        MessageEnvelope<BookTicketResponseDto> resp1Env = manager.sendAndReceive(req1, BookTicketResponseDto.class);
        BookTicketResponseDto resp1Dto = resp1Env.getPayload();

        // Must be UNKNOWN (TIMEOUT_UNKNOWN), NOT REJECTED
        assertFalse(resp1Dto.isConfirmed());
        assertFalse(resp1Dto.isRejected(), "BẤT BIẾN: Không được đổi timeout thành REJECTED");
        assertEquals("UNKNOWN", resp1Dto.getOutcome());
        assertEquals("TIMEOUT_UNKNOWN", resp1Dto.getReasonCode());
        assertEquals(req1Id, resp1Dto.getRequestId(), "requestId phải được giữ nguyên");
        assertFalse(manager.isConnected(), "Socket dở dang phải được đóng an toàn");

        // 2. Second Request: Server is now fast, manager automatically establishes fresh connection
        delayActive.set(false);

        String req2Id = UUID.randomUUID().toString();
        BookTicketRequestDto book2 = new BookTicketRequestDto(req2Id, "SE-001", "G03", "Passenger Recovery Test");
        MessageEnvelope<BookTicketRequestDto> req2 = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client", book2);

        MessageEnvelope<BookTicketResponseDto> resp2Env = manager.sendAndReceive(req2, BookTicketResponseDto.class);
        BookTicketResponseDto resp2Dto = resp2Env.getPayload();

        // Must be CONFIRMED on newly reconnected socket
        assertTrue(resp2Dto.isConfirmed());
        assertEquals(req2Id, resp2Dto.getRequestId());
        assertEquals("TKT-OK", resp2Dto.getTicketId());
        assertTrue(manager.isConnected(), "ConnectionManager must be reconnected on fresh socket");
    }

    @Test
    @DisplayName("Transient connection error retries with backoff up to maxAttempts")
    public void testConnectionRefusedRetries() throws Exception {
        // Find an unused closed port
        ServerSocket temp = new ServerSocket(0);
        int closedPort = temp.getLocalPort();
        temp.close();

        // 3 attempts, 50ms initial backoff, 2.0 multiplier
        RetryPolicy policy = new RetryPolicy(3, 50, 500, 2.0, 1000, 1000);
        ConnectionManager manager = new ConnectionManager("127.0.0.1", closedPort, policy);
        resources.add(manager);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto book = new BookTicketRequestDto(reqId, "SE-001", "G01", "Passenger Refused");
        MessageEnvelope<BookTicketRequestDto> req = MessageEnvelope.createRequest(MessageType.BOOK_TICKET, "client", book);

        long start = System.currentTimeMillis();
        assertThrows(IOException.class, () -> manager.sendAndReceive(req));
        long elapsed = System.currentTimeMillis() - start;

        // At least initialBackoff (50ms) + nextBackoff (100ms) = 150ms of backoff elapsed
        assertTrue(elapsed >= 100, "Must have performed backoff between retry attempts, elapsed: " + elapsed);
        assertFalse(manager.isConnected());
    }
}
