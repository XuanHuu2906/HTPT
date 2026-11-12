package vn.nhom6.railway.client.smoke;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.GetSeatsResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.SeatDto;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FAKE SERVER — không phải Node thật.
 * <p>
 * Máy chủ TCP giả lập phục vụ kiểm thử đơn node (Single-Node Smoke Test) cho Desktop Client (TASK TV1-D10).
 * <p>
 * Đặc tính kỹ thuật:
 * <ul>
 *   <li>Tuân thủ 100% hợp đồng SPEC-03: 4-byte BE length framing + UTF-8 JSON envelope.</li>
 *   <li>Sử dụng trực tiếp {@link FrameCodec}, {@link MessageCodec} và các DTO từ module {@code shared};
 *       tuyệt đối không tự viết lại framing hay parse JSON thủ công.</li>
 *   <li>Khởi chạy trên cổng ngẫu nhiên (port 0) để tránh xung đột cổng khi chạy test song song.</li>
 *   <li>Quản lý trạng thái 10 ghế (G01–G10) nhất quán: kiểm tra đặt vé thành công, idempotency và conflict.</li>
 *   <li>Đếm số vé ACTIVE trên mỗi ghế để xác nhận bất biến: "retry n lần cùng requestId = duy nhất 1 vé".</li>
 * </ul>
 */
public class FakeNodeServer implements AutoCloseable {

    public static final String DEFAULT_NODE_ID = "node-1";
    public static final String DEFAULT_TRIP_ID = "SE-001";
    public static final int SEAT_COUNT = 10;

    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String nodeId;

    // Trạng thái ghế: G01 -> SeatState
    private final Map<String, SeatState> seatStates = new ConcurrentHashMap<>();

    // Lưu trữ kết quả đặt vé theo requestId để bảo đảm tính idempotent (SPEC-03 §4.2, §4.4)
    private final Map<String, StoredBooking> storedBookings = new ConcurrentHashMap<>();

    // Đếm số vé ACTIVE trên mỗi ghế (phục vụ assert: retry nhiều lần chỉ có đúng 1 vé ACTIVE)
    private final Map<String, Integer> activeTicketsPerSeat = new ConcurrentHashMap<>();

    // Đếm số lần nhận được mỗi requestId
    private final Map<String, Integer> requestAttempts = new ConcurrentHashMap<>();

    // Danh sách toàn bộ bản tin nhận được để audit trong test
    private final List<MessageEnvelope<JsonNode>> receivedRequests = new CopyOnWriteArrayList<>();

    // Bộ đếm chỉ số log và readIndex
    private final AtomicLong logIndexSequence = new AtomicLong(100);
    private final AtomicLong readIndexSequence = new AtomicLong(40);

    // Cấu hình hành vi kiểm thử (delay, đóng ngắt kết nối giữa chừng)
    private volatile long delayMs = 0;
    private volatile boolean closeAfterRead = false;

    // Cấu hình trạng thái trả về cho bản tin NODE_STATUS (TASK D11)
    private volatile String configuredNodeId;
    private volatile String configuredRole = "LEADER";
    private volatile Long configuredTerm = 1L;
    private volatile String configuredLeaderId;
    private volatile boolean storageHealthy = true;

    // Cấu hình phản hồi giả định cho kiểm thử phân tán (TASK TV1-D12)
    private volatile boolean forceNotLeader = false;
    private volatile String notLeaderHint = null;
    private volatile Long notLeaderTerm = 1L;
    private volatile String forcedErrorCode = null;
    private volatile String forcedErrorMessage = null;
    private volatile boolean simulateTimeoutOnBook = false;
    private volatile long timeoutSleepMs = 10000;

    public static record SeatState(String seatId, String status, String ticketId) {}

    public static record StoredBooking(
            String requestId,
            String tripId,
            String seatId,
            String passengerName,
            BookTicketResponseDto responseDto,
            boolean isConflict
    ) {}

    public FakeNodeServer() throws IOException {
        this(DEFAULT_NODE_ID);
    }

    public FakeNodeServer(String nodeId) throws IOException {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId không được null");
        this.serverSocket = new ServerSocket(0);
        initDefaultSeats();

        this.acceptThread = new Thread(this::acceptLoop, "fake-node-server-accept-" + getPort());
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Khởi tạo mặc định 10 ghế G01–G10 ở trạng thái AVAILABLE.
     */
    public void initDefaultSeats() {
        seatStates.clear();
        for (int i = 1; i <= SEAT_COUNT; i++) {
            String seatId = String.format("G%02d", i);
            seatStates.put(seatId, new SeatState(seatId, "AVAILABLE", null));
            activeTicketsPerSeat.put(seatId, 0);
        }
    }

    /**
     * Cấu hình trạng thái ban đầu của một ghế (phục vụ kịch bản ghế đã được đặt trước).
     */
    public void setSeatStatus(String seatId, String status, String ticketId) {
        seatStates.put(seatId, new SeatState(seatId, status, ticketId));
        if ("BOOKED".equalsIgnoreCase(status)) {
            activeTicketsPerSeat.put(seatId, 1);
        } else {
            activeTicketsPerSeat.put(seatId, 0);
        }
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public void setCloseAfterRead(boolean closeAfterRead) {
        this.closeAfterRead = closeAfterRead;
    }

    public int getActiveTicketCount(String seatId) {
        return activeTicketsPerSeat.getOrDefault(seatId, 0);
    }

    public int getRequestAttemptCount(String requestId) {
        return requestAttempts.getOrDefault(requestId, 0);
    }

    public List<MessageEnvelope<JsonNode>> getReceivedRequests() {
        return Collections.unmodifiableList(receivedRequests);
    }

    public void resetAudit() {
        receivedRequests.clear();
        requestAttempts.clear();
    }

    private void acceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(true);
                Thread handlerThread = new Thread(() -> handleClient(clientSocket),
                        "fake-node-client-handler-" + clientSocket.getPort());
                handlerThread.setDaemon(true);
                handlerThread.start();
            } catch (SocketException e) {
                // ServerSocket đã đóng trong close()
                break;
            } catch (IOException e) {
                if (!running.get()) {
                    break;
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            while (running.get() && !socket.isClosed()) {
                byte[] frameBytes;
                try {
                    frameBytes = FrameCodec.readFrame(in);
                } catch (EOFException e) {
                    break;
                } catch (SocketException e) {
                    break;
                }

                MessageEnvelope<JsonNode> requestEnv = MessageCodec.decode(frameBytes);
                receivedRequests.add(requestEnv);

                if (closeAfterRead) {
                    socket.close();
                    break;
                }

                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                MessageEnvelope<?> responseEnv = processRequest(requestEnv);
                if (responseEnv != null) {
                    FrameCodec.writeMessage(out, responseEnv);
                } else {
                    socket.close();
                    break;
                }
            }
        } catch (Exception ignored) {
        }
    }

    private MessageEnvelope<?> processRequest(MessageEnvelope<JsonNode> requestEnv) {
        MessageType type = requestEnv.getType();
        String reqMessageId = requestEnv.getMessageId();
        JsonNode payload = requestEnv.getPayload();

        if (type == null) {
            return createErrorResponse(reqMessageId, "INVALID_REQUEST", "Loại bản tin không được null", null);
        }

        return switch (type) {
            case NODE_STATUS -> handleNodeStatus(reqMessageId);
            case GET_SEATS -> handleGetSeats(reqMessageId, payload);
            case BOOK_TICKET -> handleBookTicket(reqMessageId, payload);
            case GET_REQUEST_RESULT -> handleGetRequestResult(reqMessageId, payload);
            default -> createErrorResponse(reqMessageId, "INVALID_REQUEST",
                    "Loại bản tin chưa hỗ trợ trên FakeNodeServer: " + type, null);
        };
    }

    public void setNodeStatus(String nodeId, String role, Long term, String leaderId) {
        this.configuredNodeId = nodeId;
        this.configuredRole = role;
        this.configuredTerm = term;
        this.configuredLeaderId = leaderId;
    }

    public void setStorageHealthy(boolean storageHealthy) {
        this.storageHealthy = storageHealthy;
    }

    public void setNotLeader(String leaderHint, Long term) {
        this.forceNotLeader = true;
        this.notLeaderHint = leaderHint;
        this.notLeaderTerm = (term != null) ? term : 1L;
        this.configuredRole = "FOLLOWER";
        this.configuredLeaderId = leaderHint;
    }

    public void setLeader() {
        this.forceNotLeader = false;
        this.notLeaderHint = null;
        this.configuredRole = "LEADER";
        this.configuredLeaderId = this.nodeId;
        this.forcedErrorCode = null;
        this.simulateTimeoutOnBook = false;
    }

    public void setForcedError(String errorCode, String message) {
        this.forcedErrorCode = errorCode;
        this.forcedErrorMessage = message;
    }

    public void setSimulateTimeoutOnBook(boolean simulate, long sleepMs) {
        this.simulateTimeoutOnBook = simulate;
        this.timeoutSleepMs = sleepMs;
    }

    private MessageEnvelope<?> handleNodeStatus(String correlationId) {
        ObjectNode statusPayload = MessageCodec.getMapper().createObjectNode();
        String effectiveNodeId = (configuredNodeId != null) ? configuredNodeId : nodeId;
        statusPayload.put("nodeId", effectiveNodeId);
        statusPayload.put("role", configuredRole != null ? configuredRole : "UNKNOWN");
        if (configuredTerm != null) {
            statusPayload.put("term", configuredTerm);
        } else {
            statusPayload.putNull("term");
        }
        if (configuredLeaderId != null) {
            statusPayload.put("leaderId", configuredLeaderId);
        } else {
            statusPayload.putNull("leaderId");
        }
        statusPayload.put("lastLogIndex", logIndexSequence.get());
        statusPayload.put("commitIndex", logIndexSequence.get());
        statusPayload.put("lastApplied", logIndexSequence.get());
        statusPayload.put("storageHealthy", storageHealthy);

        ObjectNode peerConn = statusPayload.putObject("peerConnectivity");
        peerConn.put("node-2", true);
        peerConn.put("node-3", true);
        peerConn.put("node-4", true);
        peerConn.put("node-5", true);

        statusPayload.put("observedAt", Instant.now().toString());

        return MessageEnvelope.createResponse(
                correlationId,
                MessageType.NODE_STATUS_RESPONSE,
                effectiveNodeId,
                statusPayload
        );
    }

    private MessageEnvelope<?> handleGetSeats(String correlationId, JsonNode payload) {
        if (forceNotLeader) {
            ErrorResponseDto errorDto = new ErrorResponseDto("NOT_LEADER",
                    "Node " + nodeId + " không phải là Leader", notLeaderHint, notLeaderTerm, null);
            return MessageEnvelope.createResponse(correlationId, MessageType.ERROR_RESPONSE, nodeId, errorDto);
        }

        String tripId = DEFAULT_TRIP_ID;
        if (payload != null && payload.hasNonNull("tripId")) {
            tripId = payload.get("tripId").asText();
        }

        List<SeatDto> seats = new ArrayList<>();
        for (int i = 1; i <= SEAT_COUNT; i++) {
            String seatId = String.format("G%02d", i);
            SeatState state = seatStates.getOrDefault(seatId, new SeatState(seatId, "AVAILABLE", null));
            seats.add(new SeatDto(seatId, state.status(), state.ticketId()));
        }

        long readIndex = readIndexSequence.incrementAndGet();
        GetSeatsResponseDto responseDto = new GetSeatsResponseDto(tripId, seats, readIndex, nodeId);

        return MessageEnvelope.createResponse(
                correlationId,
                MessageType.GET_SEATS_RESPONSE,
                nodeId,
                responseDto
        );
    }

    private synchronized MessageEnvelope<?> handleBookTicket(String correlationId, JsonNode payload) {
        if (payload == null) {
            return createErrorResponse(correlationId, "INVALID_REQUEST", "Payload đặt vé không được null", null);
        }

        String requestId = payload.hasNonNull("requestId") ? payload.get("requestId").asText() : null;
        String tripId = payload.hasNonNull("tripId") ? payload.get("tripId").asText() : DEFAULT_TRIP_ID;
        String seatId = payload.hasNonNull("seatId") ? payload.get("seatId").asText() : null;
        String passengerName = payload.hasNonNull("passengerName") ? payload.get("passengerName").asText() : "";

        if (requestId == null || requestId.isBlank()) {
            return createErrorResponse(correlationId, "INVALID_REQUEST", "Thiếu requestId trong payload", null);
        }

        requestAttempts.merge(requestId, 1, Integer::sum);

        // Mô phỏng timeout / rớt mạng sau khi đã nhận lệnh (Ca 9)
        if (simulateTimeoutOnBook) {
            String ticketId = "TCK-" + (seatId != null ? seatId : "G01") + "-" + (requestId.length() >= 8 ? requestId.substring(0, 8) : requestId);
            long appliedIndex = logIndexSequence.incrementAndGet();
            BookTicketResponseDto confirmedDto = BookTicketResponseDto.confirmed(
                    requestId,
                    tripId,
                    seatId != null ? seatId : "G01",
                    ticketId,
                    appliedIndex
            );
            storedBookings.put(requestId, new StoredBooking(requestId, tripId, seatId, passengerName, confirmedDto, false));
            try {
                Thread.sleep(timeoutSleepMs);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            return null; // Quá timeout, ngắt kết nối
        }

        // Mô phỏng mã lỗi ép buộc (NO_QUORUM, INTERNAL_ERROR,...)
        if (forcedErrorCode != null) {
            return createErrorResponse(correlationId, forcedErrorCode,
                    forcedErrorMessage != null ? forcedErrorMessage : "Lỗi giả lập: " + forcedErrorCode, requestId);
        }

        // Mô phỏng Follower từ chối kèm leaderHint
        if (forceNotLeader) {
            ErrorResponseDto errorDto = new ErrorResponseDto("NOT_LEADER",
                    "Node " + nodeId + " không phải là Leader", notLeaderHint, notLeaderTerm, requestId);
            return MessageEnvelope.createResponse(correlationId, MessageType.ERROR_RESPONSE, nodeId, errorDto);
        }

        // 1. Kiểm tra tính Idempotent / Trùng lặp theo SPEC-03 §4.2, §4.4 và SPEC-05 §3.2
        if (storedBookings.containsKey(requestId)) {
            StoredBooking stored = storedBookings.get(requestId);
            // Kiểm tra xem payload có trùng khớp với lần gửi ban đầu không
            boolean samePayload = Objects.equals(stored.tripId(), tripId)
                    && Objects.equals(stored.seatId(), seatId)
                    && Objects.equals(stored.passengerName(), passengerName);

            if (samePayload) {
                // CÙNG requestId + CÙNG payload: Trả về ĐÚNG kết quả cũ đã lưu, KHÔNG cấp vé thứ hai
                return MessageEnvelope.createResponse(
                        correlationId,
                        MessageType.BOOK_TICKET_RESPONSE,
                        nodeId,
                        stored.responseDto()
                );
            } else {
                // CÙNG requestId + KHÁC payload: Xung đột REQUEST_ID_CONFLICT
                return createErrorResponse(
                        correlationId,
                        "REQUEST_ID_CONFLICT",
                        "Xung đột requestId: payload gửi lại khác với yêu cầu ban đầu (" + stored.seatId() + " != " + seatId + ")",
                        requestId
                );
            }
        }

        // 2. Yêu cầu mới: Kiểm tra tình trạng ghế
        SeatState currentSeat = seatStates.get(seatId);
        if (currentSeat == null) {
            return createErrorResponse(correlationId, "INVALID_REQUEST", "Mã ghế không hợp lệ: " + seatId, requestId);
        }

        long appliedIndex = logIndexSequence.incrementAndGet();

        if ("BOOKED".equalsIgnoreCase(currentSeat.status())) {
            // Ghế đã có người đặt -> REJECTED SEAT_ALREADY_BOOKED
            BookTicketResponseDto rejectedDto = BookTicketResponseDto.rejected(
                    requestId,
                    tripId,
                    seatId,
                    "SEAT_ALREADY_BOOKED",
                    appliedIndex
            );

            storedBookings.put(requestId, new StoredBooking(requestId, tripId, seatId, passengerName, rejectedDto, false));

            return MessageEnvelope.createResponse(
                    correlationId,
                    MessageType.BOOK_TICKET_RESPONSE,
                    nodeId,
                    rejectedDto
            );
        }

        // 3. Ghế còn trống -> Đặt thành công CONFIRMED
        String ticketId = "TCK-" + seatId + "-" + (requestId.length() >= 8 ? requestId.substring(0, 8) : requestId);
        seatStates.put(seatId, new SeatState(seatId, "BOOKED", ticketId));
        activeTicketsPerSeat.merge(seatId, 1, Integer::sum);

        BookTicketResponseDto confirmedDto = BookTicketResponseDto.confirmed(
                requestId,
                tripId,
                seatId,
                ticketId,
                appliedIndex
        );

        storedBookings.put(requestId, new StoredBooking(requestId, tripId, seatId, passengerName, confirmedDto, false));

        return MessageEnvelope.createResponse(
                correlationId,
                MessageType.BOOK_TICKET_RESPONSE,
                nodeId,
                confirmedDto
        );
    }

    private MessageEnvelope<?> handleGetRequestResult(String correlationId, JsonNode payload) {
        String requestId = payload != null && payload.hasNonNull("requestId") ? payload.get("requestId").asText() : null;

        ObjectNode resultPayload = MessageCodec.getMapper().createObjectNode();
        resultPayload.put("requestId", requestId != null ? requestId : "");

        if (requestId != null && storedBookings.containsKey(requestId)) {
            StoredBooking stored = storedBookings.get(requestId);
            BookTicketResponseDto dto = stored.responseDto();

            if ("CONFIRMED".equalsIgnoreCase(dto.getOutcome())) {
                resultPayload.put("state", "APPLIED_SUCCESS");
                resultPayload.put("commandType", "BOOK_TICKET");
                resultPayload.put("ticketId", dto.getTicketId());
                resultPayload.putNull("reasonCode");
                resultPayload.put("appliedLogIndex", dto.getAppliedLogIndex());
            } else {
                resultPayload.put("state", "APPLIED_REJECTED");
                resultPayload.put("commandType", "BOOK_TICKET");
                resultPayload.putNull("ticketId");
                resultPayload.put("reasonCode", dto.getReasonCode());
                resultPayload.put("appliedLogIndex", dto.getAppliedLogIndex());
            }
        } else {
            resultPayload.put("state", "UNKNOWN");
            resultPayload.putNull("commandType");
            resultPayload.putNull("ticketId");
            resultPayload.putNull("reasonCode");
            resultPayload.putNull("appliedLogIndex");
        }

        return MessageEnvelope.createResponse(
                correlationId,
                MessageType.GET_REQUEST_RESULT_RESPONSE,
                nodeId,
                resultPayload
        );
    }

    private MessageEnvelope<ErrorResponseDto> createErrorResponse(String correlationId, String errorCode, String message, String requestId) {
        ErrorResponseDto errorDto = new ErrorResponseDto(errorCode, message, null, 1L, requestId);
        return MessageEnvelope.createResponse(
                correlationId,
                MessageType.ERROR_RESPONSE,
                nodeId,
                errorDto
        );
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
