package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.SeatInfoDto;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử cho {@link SocketBookingClient} sử dụng server giả trong cùng JVM (TASK TV1-D09).
 * <p>
 * Lưu ý: Các kiểm thử sử dụng {@link FakeTcpServer} (FAKE SERVER, không phải Node thật).
 * Kiểm thử với Node thật chỉ chạy khi có biến môi trường NODE_HOST và NODE_PORT.
 */
class SocketBookingClientTest {

    private FakeTcpServer fakeServer;
    private SocketBookingClient client;

    @BeforeEach
    void setUp() throws IOException {
        fakeServer = new FakeTcpServer();
        client = new SocketBookingClient("127.0.0.1", fakeServer.getPort(), 2000, 3000, "client-test");
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    @Test
    @DisplayName("Frame gửi đi đúng chuẩn SPEC-03: 4-byte BE length + JSON, messageId mới, requestId giữ nguyên trong payload")
    void testFrameFormatAndEnvelopeStructure() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G01", "Nguyen Van Test");

        fakeServer.setHandler(reqEnv -> {
            ObjectNode payload = MessageCodec.getMapper().createObjectNode();
            payload.put("requestId", reqId);
            payload.put("outcome", "CONFIRMED");
            payload.put("ticketId", "TCK-001");
            payload.put("tripId", "SE-001");
            payload.put("seatId", "G01");
            payload.putNull("reasonCode");
            payload.put("appliedLogIndex", 101L);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1", payload);
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("CONFIRMED", response.getOutcome());
        assertEquals("TCK-001", response.getTicketId());
        assertFalse(response.isMock(), "Kết quả từ SocketBookingClient không được mang cờ MOCK (isMock() == false)");

        // Kiểm tra request đã nhận tại server giả
        assertEquals(1, fakeServer.getReceivedRequests().size());
        MessageEnvelope<JsonNode> received = fakeServer.getReceivedRequests().get(0);
        assertEquals(1, received.getProtocolVersion());
        assertNotNull(received.getMessageId());
        assertNull(received.getCorrelationId());
        assertEquals(MessageType.BOOK_TICKET, received.getType());
        assertEquals("client-test", received.getSenderId());
        assertEquals(reqId, received.getPayload().get("requestId").asText());
        assertEquals("G01", received.getPayload().get("seatId").asText());
    }

    @Test
    @DisplayName("Retry cùng thao tác đặt vé: requestId giữ nguyên, messageId đổi mới")
    void testRetryPreservesRequestIdWithNewMessageId() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G02", "Khach Retry");

        fakeServer.setHandler(reqEnv -> {
            ObjectNode payload = MessageCodec.getMapper().createObjectNode();
            payload.put("requestId", reqId);
            payload.put("outcome", "CONFIRMED");
            payload.put("ticketId", "TCK-RETRY");
            payload.put("tripId", "SE-001");
            payload.put("seatId", "G02");
            payload.putNull("reasonCode");
            payload.put("appliedLogIndex", 102L);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1", payload);
        });

        // Lần 1
        client.book(bookReq).get(3, TimeUnit.SECONDS);
        // Lần 2 (retry cùng request)
        client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertEquals(2, fakeServer.getReceivedRequests().size());
        MessageEnvelope<JsonNode> req1 = fakeServer.getReceivedRequests().get(0);
        MessageEnvelope<JsonNode> req2 = fakeServer.getReceivedRequests().get(1);

        // requestId trong payload phải giống hệt nhau
        assertEquals(req1.getPayload().get("requestId").asText(), req2.getPayload().get("requestId").asText());
        assertEquals(reqId, req1.getPayload().get("requestId").asText());

        // messageId phải khác nhau (mỗi lần gửi một messageId mới)
        assertNotEquals(req1.getMessageId(), req2.getMessageId());
    }

    @Test
    @DisplayName("Đặt vé bị từ chối SEAT_ALREADY_BOOKED: outcome REJECTED, isMock = false")
    void testBookTicketRejectedSeatAlreadyBooked() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G03", "Khach Trung Ghe");

        fakeServer.setHandler(reqEnv -> {
            ObjectNode payload = MessageCodec.getMapper().createObjectNode();
            payload.put("requestId", reqId);
            payload.put("outcome", "REJECTED");
            payload.putNull("ticketId");
            payload.put("tripId", "SE-001");
            payload.put("seatId", "G03");
            payload.put("reasonCode", "SEAT_ALREADY_BOOKED");
            payload.put("appliedLogIndex", 103L);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.BOOK_TICKET_RESPONSE, "node-1", payload);
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("REJECTED", response.getOutcome());
        assertTrue(response.isRejected());
        assertEquals("SEAT_ALREADY_BOOKED", response.getReasonCode());
        assertNull(response.getTicketId());
        assertFalse(response.isMock());
    }

    @Test
    @DisplayName("Phản hồi lỗi hạ tầng NOT_LEADER: có gợi ý leaderHint, outcome UNKNOWN, isMock = false")
    void testBookTicketErrorNotLeader() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khach Gui Nham Node");

        fakeServer.setHandler(reqEnv -> {
            ErrorResponseDto err = new ErrorResponseDto("NOT_LEADER", "Node hien tai khong phai Leader", "node-3", 5L, reqId);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.ERROR_RESPONSE, "node-1", err);
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertEquals("NOT_LEADER", response.getReasonCode());
        assertEquals("node-3", response.getLeaderHint());
        assertFalse(response.isMock());
    }

    @Test
    @DisplayName("Phản hồi lỗi cụm NO_QUORUM: outcome UNKNOWN, reasonCode NO_QUORUM, isMock = false")
    void testBookTicketErrorNoQuorum() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G05", "Khach Mat Quorum");

        fakeServer.setHandler(reqEnv -> {
            ErrorResponseDto err = new ErrorResponseDto("NO_QUORUM", "Cum Raft mat da so voter", null, 5L, reqId);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.ERROR_RESPONSE, "node-1", err);
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome());
        assertEquals("NO_QUORUM", response.getReasonCode());
        assertFalse(response.isMock());
    }

    @Test
    @DisplayName("Phản hồi lỗi REQUEST_ID_CONFLICT: outcome REJECTED, reasonCode REQUEST_ID_CONFLICT")
    void testBookTicketErrorRequestIdConflict() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G06", "Khach Xung Dot RequestId");

        fakeServer.setHandler(reqEnv -> {
            ErrorResponseDto err = new ErrorResponseDto("REQUEST_ID_CONFLICT", "RequestId da ton tai voi payload khac", null, 5L, reqId);
            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.ERROR_RESPONSE, "node-1", err);
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("REJECTED", response.getOutcome());
        assertEquals("REQUEST_ID_CONFLICT", response.getReasonCode());
        assertFalse(response.isMock());
    }

    @Test
    @DisplayName("Read Timeout sau khi đã gửi lệnh BOOK: trả về UNKNOWN (TIMEOUT_UNKNOWN), không thành công và không REJECTED")
    void testBookTicketReadTimeoutReturnsUnknown() throws Exception {
        // Cấu hình client với read timeout ngắn 400ms để test nhanh
        SocketBookingClient quickTimeoutClient = new SocketBookingClient("127.0.0.1", fakeServer.getPort(), 2000, 400, "client-timeout-test");
        fakeServer.setDelayMs(1000); // Server phản hồi chậm hơn timeout

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G07", "Khach Timeout");

        BookTicketResponseDto response = quickTimeoutClient.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("UNKNOWN", response.getOutcome(), "Read timeout bắt buộc phải trả về outcome UNKNOWN");
        assertEquals("TIMEOUT_UNKNOWN", response.getReasonCode());
        assertFalse(response.isConfirmed());
        assertFalse(response.isRejected());
        assertFalse(response.isMock());

        quickTimeoutClient.close();
    }

    @Test
    @DisplayName("Mất kết nối / Connection Refused: hoàn thành exceptionally với CompletionException/IOException")
    void testConnectionRefusedThrowsException() {
        // Kết nối tới cổng không tồn tại
        SocketBookingClient deadClient = new SocketBookingClient("127.0.0.1", 65530, 500, 500, "dead-client");
        BookTicketRequestDto bookReq = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G08", "Khach Mat Ket Noi");

        CompletableFuture<BookTicketResponseDto> future = deadClient.book(bookReq);
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));

        assertTrue(ex.getCause() instanceof CompletionException || ex.getCause() instanceof IOException);
        deadClient.close();
    }

    @Test
    @DisplayName("Server đóng kết nối sau khi nhận request: hoàn thành exceptionally")
    void testServerClosesConnectionThrowsException() {
        fakeServer.setCloseAfterRead(true);
        BookTicketRequestDto bookReq = new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G08", "Khach Server Close");

        CompletableFuture<BookTicketResponseDto> future = client.book(bookReq);
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));

        assertTrue(ex.getCause() instanceof CompletionException || ex.getCause() instanceof IOException);
    }

    @Test
    @DisplayName("Phân mảnh khung TCP (TCP Chunk Fragmentation): FrameCodec đọc đủ byte và parse thành công")
    void testFragmentedTcpStreamHandledCorrectly() throws Exception {
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(reqId, "SE-001", "G09", "Khach Phan Manh");

        fakeServer.setCustomResponseSender((socket, requestEnvelope) -> {
            ObjectNode payload = MessageCodec.getMapper().createObjectNode();
            payload.put("requestId", reqId);
            payload.put("outcome", "CONFIRMED");
            payload.put("ticketId", "TCK-FRAGMENT");
            payload.put("tripId", "SE-001");
            payload.put("seatId", "G09");
            payload.putNull("reasonCode");
            payload.put("appliedLogIndex", 200L);

            MessageEnvelope<?> responseEnv = MessageEnvelope.createResponse(
                    requestEnvelope.getMessageId(),
                    MessageType.BOOK_TICKET_RESPONSE,
                    "node-1",
                    payload
            );

            byte[] fullFrame = FrameCodec.frame(MessageCodec.encode(responseEnv));
            OutputStream out = socket.getOutputStream();

            // Cắt frame thành 2 phần: phần đầu 6 bytes, sleep 30ms, rồi gửi phần còn lại
            out.write(fullFrame, 0, 6);
            out.flush();
            try {
                Thread.sleep(30);
            } catch (InterruptedException ignored) {}
            out.write(fullFrame, 6, fullFrame.length - 6);
            out.flush();
        });

        BookTicketResponseDto response = client.book(bookReq).get(3, TimeUnit.SECONDS);

        assertNotNull(response);
        assertEquals("CONFIRMED", response.getOutcome());
        assertEquals("TCK-FRAGMENT", response.getTicketId());
    }

    @Test
    @DisplayName("Lấy sơ đồ 10 ghế GET_SEATS thành công: parse đủ danh sách ghế, readIndex, leaderId, isMock = false")
    void testGetSeatsSuccess() throws Exception {
        GetSeatsRequestDto getSeatsReq = new GetSeatsRequestDto("SE-001");

        fakeServer.setHandler(reqEnv -> {
            ObjectMapper mapper = MessageCodec.getMapper();
            ObjectNode payload = mapper.createObjectNode();
            payload.put("tripId", "SE-001");
            payload.put("readIndex", 42L);
            payload.put("leaderId", "node-3");

            ArrayNode seatsArray = mapper.createArrayNode();
            for (int i = 1; i <= 10; i++) {
                String seatId = String.format("G%02d", i);
                ObjectNode seat = mapper.createObjectNode();
                seat.put("seatId", seatId);
                if (i == 2 || i == 5) {
                    seat.put("status", "BOOKED");
                    seat.put("ticketId", "TCK-SE001-" + seatId);
                } else {
                    seat.put("status", "AVAILABLE");
                    seat.putNull("ticketId");
                }
                seatsArray.add(seat);
            }
            payload.set("seats", seatsArray);

            return MessageEnvelope.createResponse(reqEnv.getMessageId(), MessageType.GET_SEATS_RESPONSE, "node-3", payload);
        });

        GetSeatsResponseDto seatsRes = client.getSeats(getSeatsReq).get(3, TimeUnit.SECONDS);

        assertNotNull(seatsRes);
        assertEquals("SE-001", seatsRes.getTripId());
        assertEquals("node-3", seatsRes.getLeaderId());
        assertEquals(42L, seatsRes.getReadIndex());
        assertEquals(10, seatsRes.getSeats().size());
        assertFalse(seatsRes.isMock(), "GetSeatsResponseDto từ Socket không được mang cờ MOCK");

        SeatInfoDto seatG02 = seatsRes.getSeats().get(1);
        assertEquals("G02", seatG02.getSeatId());
        assertEquals("BOOKED", seatG02.getStatus());
        assertEquals("TCK-SE001-G02", seatG02.getTicketId());

        SeatInfoDto seatG01 = seatsRes.getSeats().get(0);
        assertEquals("G01", seatG01.getSeatId());
        assertEquals("AVAILABLE", seatG01.getStatus());
        assertNull(seatG01.getTicketId());
    }

    @Test
    @DisplayName("getRequestResult đánh dấu BLOCKED: ném UnsupportedOperationException chứa 'BLOCKED: thiếu DTO'")
    void testGetRequestResultThrowsBlockedException() {
        CompletableFuture<?> future = client.getRequestResult("REQ-123");
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
        assertTrue(ex.getCause().getMessage().contains("BLOCKED"));
    }

    @Test
    @DisplayName("cancel đánh dấu ngoài phạm vi D09: ném UnsupportedOperationException")
    void testCancelThrowsUnsupportedException() {
        CancelTicketRequestDto cancelReq = new CancelTicketRequestDto(UUID.randomUUID().toString(), "TCK-123");
        CompletableFuture<?> future = client.cancel(cancelReq);
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
        assertTrue(ex.getCause().getMessage().contains("ngoài phạm vi D09"));
    }

    @Test
    @DisplayName("ClientFactory hỗ trợ chọn client qua -Dbooking.client: socket vs mock")
    void testClientFactoryConfigurationSelection() {
        // Lưu giá trị ban đầu
        String original = System.getProperty("booking.client");
        try {
            System.setProperty("booking.client", "socket");
            ClientFactory.resetToConfiguredClient();
            assertTrue(ClientFactory.getBookingClient() instanceof SocketBookingClient);

            System.setProperty("booking.client", "mock");
            ClientFactory.resetToConfiguredClient();
            assertFalse(ClientFactory.getBookingClient() instanceof SocketBookingClient);
        } finally {
            if (original != null) {
                System.setProperty("booking.client", original);
            } else {
                System.clearProperty("booking.client");
            }
            ClientFactory.resetToConfiguredClient();
        }
    }

    @Test
    @DisplayName("Kiểm tra không dùng thư viện REST/HTTP nào trong classpath desktop-client")
    void testNoHttpClassesUsed() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.apache.http.client.HttpClient"));
        assertThrows(ClassNotFoundException.class, () -> Class.forName("okhttp3.OkHttpClient"));
    }

    @Test
    @DisplayName("Test tích hợp với Node thật (chỉ chạy khi có biến môi trường NODE_HOST)")
    @EnabledIfEnvironmentVariable(named = "NODE_HOST", matches = ".+")
    void testIntegrationWithRealNode() {
        // Không chạy khi không có môi trường thật
        fail("Không có Node thật đang chạy trong môi trường test");
    }

    // =========================================================================
    // Fake TCP Server trong cùng JVM phục vụ test (FAKE SERVER, không phải Node thật)
    // =========================================================================

    static class FakeTcpServer implements AutoCloseable {

        @FunctionalInterface
        interface CustomResponseSender {
            void sendResponse(Socket socket, MessageEnvelope<JsonNode> request) throws Exception;
        }

        private final ServerSocket serverSocket;
        private final Thread acceptThread;
        private volatile Function<MessageEnvelope<JsonNode>, MessageEnvelope<?>> handler;
        private volatile CustomResponseSender customResponseSender;
        private volatile long delayMs = 0;
        private volatile boolean closeAfterRead = false;
        private final List<MessageEnvelope<JsonNode>> receivedRequests = new CopyOnWriteArrayList<>();

        public FakeTcpServer() throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.acceptThread = new Thread(this::acceptLoop, "fake-tcp-server-acceptor");
            this.acceptThread.setDaemon(true);
            this.acceptThread.start();
        }

        public int getPort() {
            return serverSocket.getLocalPort();
        }

        public void setHandler(Function<MessageEnvelope<JsonNode>, MessageEnvelope<?>> handler) {
            this.handler = handler;
            this.customResponseSender = null;
        }

        public void setCustomResponseSender(CustomResponseSender sender) {
            this.customResponseSender = sender;
            this.handler = null;
        }

        public void setDelayMs(long delayMs) {
            this.delayMs = delayMs;
        }

        public void setCloseAfterRead(boolean closeAfterRead) {
            this.closeAfterRead = closeAfterRead;
        }

        public List<MessageEnvelope<JsonNode>> getReceivedRequests() {
            return receivedRequests;
        }

        private void acceptLoop() {
            try {
                while (!serverSocket.isClosed()) {
                    Socket clientSocket = serverSocket.accept();
                    Thread handlerThread = new Thread(() -> handleClient(clientSocket), "fake-client-handler");
                    handlerThread.setDaemon(true);
                    handlerThread.start();
                }
            } catch (IOException ignored) {
            }
        }

        private void handleClient(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = new BufferedOutputStream(socket.getOutputStream());

                while (!socket.isClosed()) {
                    byte[] requestPayload;
                    try {
                        requestPayload = FrameCodec.readFrame(in);
                    } catch (EOFException e) {
                        break;
                    }

                    MessageEnvelope<JsonNode> requestEnvelope = MessageCodec.decode(requestPayload);
                    receivedRequests.add(requestEnvelope);

                    if (closeAfterRead) {
                        socket.close();
                        break;
                    }

                    if (delayMs > 0) {
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ignored) {
                        }
                    }

                    if (customResponseSender != null) {
                        customResponseSender.sendResponse(socket, requestEnvelope);
                    } else if (handler != null) {
                        MessageEnvelope<?> response = handler.apply(requestEnvelope);
                        if (response != null) {
                            FrameCodec.writeMessage(out, response);
                        }
                    }
                }
            } catch (Exception ignored) {
            }
        }

        @Override
        public void close() {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
