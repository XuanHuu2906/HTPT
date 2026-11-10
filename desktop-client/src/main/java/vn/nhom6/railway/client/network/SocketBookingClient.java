package vn.nhom6.railway.client.network;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Triển khai {@link BookingClient} kết nối TCP Socket tới Node backend thật
 * theo đặc tả SPEC-03 Section 2, Section 3, Section 4.1 và Section 4.2.
 *
 * <p>Đặc tính kỹ thuật:
 * <ul>
 *   <li>Sử dụng framing 4-byte big-endian length prefix + UTF-8 JSON envelope của module {@code shared}.</li>
 *   <li>Chạy hoàn toàn bất đồng bộ trên daemon thread pool, không chặn JavaFX thread.</li>
 *   <li>Tuyệt đối không dùng thư viện HTTP hay REST.</li>
 *   <li>Kết quả trả về mang cờ {@code mock = false} (dữ liệu thật từ socket).</li>
 *   <li>Tuân thủ timeout: SocketTimeout sau khi gửi BOOK ánh xạ về kết quả UNKNOWN (TIMEOUT_UNKNOWN)
 *       để UI tra cứu lại cùng requestId, không tự ý chuyển thành REJECTED hay sinh requestId mới.</li>
 *   <li>Ghi log mỗi lời gọi gồm requestId, messageId, type, endpoint, kết quả/mã lỗi, thời gian phản hồi.</li>
 * </ul>
 */
public class SocketBookingClient implements BookingClient, AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(SocketBookingClient.class.getName());

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9101; // Client port của node-1 theo configs/dev/cluster.properties
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 10000;
    public static final String DEFAULT_SENDER_ID = "client-desktop";

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final String senderId;
    private final ExecutorService executor;

    private final Object socketLock = new Object();
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "socket-booking-client-worker");
        t.setDaemon(true);
        return t;
    };

    /**
     * Khởi tạo với host và port cấu hình tự động từ System properties hoặc file cấu hình.
     */
    public SocketBookingClient() {
        this(resolveConfiguredHost(), resolveConfiguredPort());
    }

    /**
     * Khởi tạo với host và port chỉ định.
     */
    public SocketBookingClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_SENDER_ID);
    }

    /**
     * Khởi tạo với đầy đủ thông số kết nối và timeout.
     */
    public SocketBookingClient(String host, int port, int connectTimeoutMs, int readTimeoutMs, String senderId) {
        this.host = Objects.requireNonNull(host, "host không được null");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port ngoài phạm vi [1, 65535]: " + port);
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs phải > 0: " + connectTimeoutMs);
        }
        if (readTimeoutMs < 0) {
            throw new IllegalArgumentException("readTimeoutMs không được âm: " + readTimeoutMs);
        }
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.senderId = (senderId != null && !senderId.isBlank()) ? senderId : DEFAULT_SENDER_ID;
        this.executor = Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public String getSenderId() {
        return senderId;
    }

    // =========================================================================
    // BookingClient Interface Methods
    // =========================================================================

    /**
     * Lấy sơ đồ ghế qua TCP (SPEC-03 §4.1 - GET_SEATS).
     */
    @Override
    public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request không được null"));
        }

        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            MessageEnvelope<GetSeatsRequestDto> reqEnvelope = MessageEnvelope.createRequest(
                    MessageType.GET_SEATS,
                    senderId,
                    request
            );
            String messageId = reqEnvelope.getMessageId();

            try {
                MessageEnvelope<JsonNode> resEnvelope = sendAndReceive(reqEnvelope);
                MessageType resType = resEnvelope.getType();
                long elapsed = System.currentTimeMillis() - startTime;

                if (resType == MessageType.GET_SEATS_RESPONSE) {
                    JsonNode payload = resEnvelope.getPayload();
                    String tripId = (payload != null && payload.hasNonNull("tripId"))
                            ? payload.get("tripId").asText() : request.getTripId();
                    Long readIndex = (payload != null && payload.hasNonNull("readIndex"))
                            ? payload.get("readIndex").asLong() : null;
                    String leaderId = (payload != null && payload.hasNonNull("leaderId"))
                            ? payload.get("leaderId").asText() : null;

                    List<SeatInfoDto> seats = new ArrayList<>();
                    if (payload != null && payload.has("seats") && payload.get("seats").isArray()) {
                        for (JsonNode seatNode : payload.get("seats")) {
                            String seatId = seatNode.hasNonNull("seatId") ? seatNode.get("seatId").asText() : "";
                            String status = seatNode.hasNonNull("status") ? seatNode.get("status").asText() : "AVAILABLE";
                            String ticketId = seatNode.hasNonNull("ticketId") ? seatNode.get("ticketId").asText() : null;
                            seats.add(new SeatInfoDto(seatId, status, ticketId));
                        }
                    }

                    logCall("N/A", messageId, "GET_SEATS", "SUCCESS", elapsed);
                    return new GetSeatsResponseDto(tripId, seats, readIndex, leaderId, false);

                } else if (resType == MessageType.ERROR_RESPONSE) {
                    ErrorResponseDto err = MessageCodec.decodePayload(resEnvelope, ErrorResponseDto.class);
                    logCall("N/A", messageId, "GET_SEATS", err.getErrorCode(), elapsed);
                    throw new IOException("Lỗi máy chủ (" + err.getErrorCode() + "): " + err.getMessage());
                } else {
                    logCall("N/A", messageId, "GET_SEATS", "UNKNOWN_TYPE_" + resType, elapsed);
                    throw new IOException("Loại phản hồi không mong đợi cho GET_SEATS: " + resType);
                }
            } catch (IOException e) {
                long elapsed = System.currentTimeMillis() - startTime;
                logCall("N/A", messageId, "GET_SEATS", "IO_ERROR", elapsed);
                throw new CompletionException(e);
            }
        }, executor);
    }

    /**
     * Gửi yêu cầu đặt vé qua TCP (SPEC-03 §4.2 - BOOK_TICKET).
     * <p>
     * Giữ nguyên {@code requestId} từ lớp gọi (ViewModel) truyền vào, không tự sinh requestId mới.
     * Mỗi lần gửi sinh {@code messageId} mới theo chuẩn envelope.
     */
    @Override
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request không được null"));
        }

        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            String requestId = request.getRequestId();

            MessageEnvelope<BookTicketRequestDto> reqEnvelope = MessageEnvelope.createRequest(
                    MessageType.BOOK_TICKET,
                    senderId,
                    request
            );
            String messageId = reqEnvelope.getMessageId();

            try {
                MessageEnvelope<JsonNode> resEnvelope = sendAndReceive(reqEnvelope);
                MessageType resType = resEnvelope.getType();
                long elapsed = System.currentTimeMillis() - startTime;

                if (resType == MessageType.BOOK_TICKET_RESPONSE) {
                    JsonNode payload = resEnvelope.getPayload();
                    String outRequestId = (payload != null && payload.hasNonNull("requestId"))
                            ? payload.get("requestId").asText() : requestId;
                    String outcome = (payload != null && payload.hasNonNull("outcome"))
                            ? payload.get("outcome").asText() : "UNKNOWN";
                    String ticketId = (payload != null && payload.hasNonNull("ticketId"))
                            ? payload.get("ticketId").asText() : null;
                    String tripId = (payload != null && payload.hasNonNull("tripId"))
                            ? payload.get("tripId").asText() : request.getTripId();
                    String seatId = (payload != null && payload.hasNonNull("seatId"))
                            ? payload.get("seatId").asText() : request.getSeatId();
                    String reasonCode = (payload != null && payload.hasNonNull("reasonCode"))
                            ? payload.get("reasonCode").asText() : null;
                    Long appliedLogIndex = (payload != null && payload.hasNonNull("appliedLogIndex"))
                            ? payload.get("appliedLogIndex").asLong() : null;

                    logCall(requestId, messageId, "BOOK_TICKET", outcome, elapsed);
                    return new BookTicketResponseDto(
                            outRequestId,
                            outcome,
                            ticketId,
                            tripId,
                            seatId,
                            reasonCode,
                            appliedLogIndex,
                            false, // Không phải mock!
                            null,
                            "CONFIRMED".equalsIgnoreCase(outcome) ? "Đặt vé thành công" : "Bị từ chối"
                    );

                } else if (resType == MessageType.ERROR_RESPONSE) {
                    ErrorResponseDto err = MessageCodec.decodePayload(resEnvelope, ErrorResponseDto.class);
                    String errorCode = err.getErrorCode();
                    String leaderHint = err.getLeaderHint();
                    String message = err.getMessage();

                    logCall(requestId, messageId, "BOOK_TICKET", errorCode, elapsed);

                    if ("NOT_LEADER".equalsIgnoreCase(errorCode)) {
                        return new BookTicketResponseDto(requestId, "UNKNOWN", null, request.getTripId(),
                                request.getSeatId(), "NOT_LEADER", null, false, leaderHint, message);
                    } else if ("NO_QUORUM".equalsIgnoreCase(errorCode)) {
                        return new BookTicketResponseDto(requestId, "UNKNOWN", null, request.getTripId(),
                                request.getSeatId(), "NO_QUORUM", null, false, null, message);
                    } else if ("REQUEST_ID_CONFLICT".equalsIgnoreCase(errorCode)) {
                        return new BookTicketResponseDto(requestId, "REJECTED", null, request.getTripId(),
                                request.getSeatId(), "REQUEST_ID_CONFLICT", null, false, null, message);
                    } else if ("TIMEOUT_UNKNOWN".equalsIgnoreCase(errorCode)) {
                        return new BookTicketResponseDto(requestId, "UNKNOWN", null, request.getTripId(),
                                request.getSeatId(), "TIMEOUT_UNKNOWN", null, false, null, message);
                    } else {
                        return new BookTicketResponseDto(requestId, "REJECTED", null, request.getTripId(),
                                request.getSeatId(), errorCode, null, false, leaderHint, message);
                    }
                } else {
                    logCall(requestId, messageId, "BOOK_TICKET", "UNKNOWN_TYPE_" + resType, elapsed);
                    return new BookTicketResponseDto(requestId, "UNKNOWN", null, request.getTripId(),
                            request.getSeatId(), "UNEXPECTED_RESPONSE_TYPE", null, false, null,
                            "Loại phản hồi không hợp lệ: " + resType);
                }
            } catch (SocketTimeoutException e) {
                long elapsed = System.currentTimeMillis() - startTime;
                logCall(requestId, messageId, "BOOK_TICKET", "TIMEOUT_UNKNOWN", elapsed);
                // Timeout sau khi đã gửi lệnh BOOK → kết quả UNKNOWN (không ánh xạ thành REJECTED)
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Hết thời gian chờ phản hồi từ máy chủ (read timeout)"
                );
            } catch (IOException e) {
                long elapsed = System.currentTimeMillis() - startTime;
                logCall(requestId, messageId, "BOOK_TICKET", "CONNECTION_LOST", elapsed);
                throw new CompletionException("Mất kết nối tới cụm máy chủ: " + e.getMessage(), e);
            } catch (Exception e) {
                long elapsed = System.currentTimeMillis() - startTime;
                logCall(requestId, messageId, "BOOK_TICKET", "INTERNAL_ERROR", elapsed);
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        "INTERNAL_ERROR",
                        null,
                        false,
                        null,
                        "Lỗi xử lý phản hồi: " + e.getMessage()
                );
            }
        }, executor);
    }

    /**
     * Hủy vé (CANCEL_TICKET): Ngoài phạm vi D09 (chỉ yêu cầu BOOK_TICKET, GET_SEATS, GET_REQUEST_RESULT).
     */
    @Override
    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("CANCEL chưa hỗ trợ ở SocketBookingClient (ngoài phạm vi D09)")
        );
    }

    /**
     * Tra cứu kết quả yêu cầu (GET_REQUEST_RESULT):
     * Do TV3 chưa cung cấp DTO GetRequestResultRequestDto trong module {@code shared},
     * phương thức đánh dấu BLOCKED theo quy định Bước 0 của SPEC-03/D09.
     */
    @Override
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("BLOCKED: thiếu DTO GET_REQUEST_RESULT từ TV3")
        );
    }

    // =========================================================================
    // TCP Socket Transport & Framing
    // =========================================================================

    private MessageEnvelope<JsonNode> sendAndReceive(MessageEnvelope<?> requestEnvelope) throws IOException {
        synchronized (socketLock) {
            ensureConnected();
            try {
                FrameCodec.writeMessage(out, requestEnvelope);
                byte[] responseBytes = FrameCodec.readFrame(in);
                MessageEnvelope<JsonNode> responseEnvelope = MessageCodec.decode(responseBytes);

                // Kiểm tra correlationId khớp messageId gửi đi per SPEC-03 Section 2 & Section 3
                String expectedCorrelationId = requestEnvelope.getMessageId();
                String actualCorrelationId = responseEnvelope.getCorrelationId();
                if (!Objects.equals(expectedCorrelationId, actualCorrelationId)) {
                    throw new IOException("Lỗi tương quan gói tin: correlationId không khớp (kỳ vọng: "
                            + expectedCorrelationId + ", nhận được: " + actualCorrelationId + ")");
                }

                return responseEnvelope;
            } catch (IOException e) {
                // Đóng socket khi gặp lỗi byte-framing hoặc timeout để không để sót stream bẩn
                closeSocket();
                throw e;
            }
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) {
            return;
        }

        closeSocket();

        socket = new Socket();
        socket.setTcpNoDelay(true);
        if (readTimeoutMs > 0) {
            socket.setSoTimeout(readTimeoutMs);
        }

        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        in = socket.getInputStream();
        out = new BufferedOutputStream(socket.getOutputStream());
    }

    private void closeSocket() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            socket = null;
            in = null;
            out = null;
        }
    }

    private void logCall(String requestId, String messageId, String type, String outcome, long elapsedMs) {
        String logLine = String.format("[TCP-CLIENT] requestId=%s, messageId=%s, type=%s, endpoint=%s:%d, outcome=%s, elapsedMs=%dms",
                requestId != null ? requestId : "N/A",
                messageId != null ? messageId : "N/A",
                type,
                host,
                port,
                outcome,
                elapsedMs);
        LOGGER.log(System.Logger.Level.INFO, logLine);
        System.out.println(logLine);
    }

    @Override
    public void close() {
        synchronized (socketLock) {
            closeSocket();
        }
        executor.shutdownNow();
    }

    // =========================================================================
    // Configuration Resolution
    // =========================================================================

    private static String resolveConfiguredHost() {
        String sysHost = System.getProperty("booking.client.host");
        if (sysHost == null || sysHost.isBlank()) {
            sysHost = System.getProperty("booking.server.host");
        }
        if (sysHost != null && !sysHost.isBlank()) {
            return sysHost.strip();
        }

        Properties props = loadProperties("configs/dev/cluster.properties");
        if (props == null) {
            props = loadProperties("configs/lan/node-1.properties");
        }
        if (props != null) {
            String propHost = props.getProperty("node.node-1.client.host");
            if (propHost == null || propHost.isBlank()) {
                propHost = props.getProperty("client.host");
            }
            if (propHost != null && !propHost.isBlank() && !"0.0.0.0".equals(propHost.strip())) {
                return propHost.strip();
            }
        }
        return DEFAULT_HOST;
    }

    private static int resolveConfiguredPort() {
        String sysPort = System.getProperty("booking.client.port");
        if (sysPort == null || sysPort.isBlank()) {
            sysPort = System.getProperty("booking.server.port");
        }
        if (sysPort != null && !sysPort.isBlank()) {
            try {
                return Integer.parseInt(sysPort.strip());
            } catch (NumberFormatException ignored) {
            }
        }

        Properties props = loadProperties("configs/dev/cluster.properties");
        if (props == null) {
            props = loadProperties("configs/lan/node-1.properties");
        }
        if (props != null) {
            String propPort = props.getProperty("node.node-1.client.port");
            if (propPort == null || propPort.isBlank()) {
                propPort = props.getProperty("client.port");
            }
            if (propPort != null && !propPort.isBlank()) {
                try {
                    return Integer.parseInt(propPort.strip());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return DEFAULT_PORT;
    }

    private static Properties loadProperties(String relativePath) {
        File file = new File(relativePath);
        if (!file.exists() && new File("../" + relativePath).exists()) {
            file = new File("../" + relativePath);
        }
        if (file.exists() && file.isFile()) {
            Properties properties = new Properties();
            try (InputStream is = new FileInputStream(file)) {
                properties.load(is);
                return properties;
            } catch (IOException ignored) {
            }
        }
        return null;
    }
}
