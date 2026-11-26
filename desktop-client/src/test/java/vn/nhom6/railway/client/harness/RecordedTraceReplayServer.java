package vn.nhom6.railway.client.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MÁY CHỦ PHÁT LẠI TRACE SỰ CỐ LAN (TASK TV1-D26).
 * <p>
 * Nguồn dữ liệu: RECORDED-FIXTURE / FAKE loopback.
 * Mô phỏng chính xác các sự cố phân tán trên LAN theo SPEC-03 / SPEC-07:
 * - Leader lost / drop connection mid-flight
 * - NO_QUORUM (cụm mất đa số 2/5)
 * - NOT_LEADER với leaderHint chuyển hướng
 * - Mất gói / Timeout phản hồi
 */
public class RecordedTraceReplayServer implements AutoCloseable {

    private static final DateTimeFormatter ISO_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    public enum FaultMode {
        NORMAL,
        DROP_CONNECTION,
        NO_QUORUM,
        NOT_LEADER,
        TIMEOUT
    }

    private final String nodeId;
    private final ServerSocket serverSocket;
    private final int port;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile FaultMode currentMode = FaultMode.NORMAL;
    private volatile String leaderHint = "node-2";
    private volatile long timeoutDelayMs = 3000;
    private final List<String> eventTraceLog = new CopyOnWriteArrayList<>();

    public RecordedTraceReplayServer(String nodeId) throws IOException {
        this.nodeId = nodeId;
        this.serverSocket = new ServerSocket(0);
        this.port = serverSocket.getLocalPort();
        startAcceptLoop();
    }

    public int getPort() {
        return port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setFaultMode(FaultMode mode) {
        this.currentMode = mode;
    }

    public void setLeaderHint(String hint) {
        this.leaderHint = hint;
    }

    public void setTimeoutDelayMs(long delayMs) {
        this.timeoutDelayMs = delayMs;
    }

    public List<String> getEventTraceLog() {
        return eventTraceLog;
    }

    private void startAcceptLoop() {
        threadPool.submit(() -> {
            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    threadPool.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    if (!running.get()) break;
                }
            }
        });
    }

    private void handleClient(Socket socket) {
        try (socket;
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            while (running.get() && !socket.isClosed()) {
                int length;
                try {
                    length = dis.readInt();
                } catch (IOException e) {
                    break;
                }

                if (length <= 0 || length > 1024 * 1024) {
                    break;
                }

                byte[] payloadBytes = new byte[length];
                dis.readFully(payloadBytes);
                String jsonStr = new String(payloadBytes, StandardCharsets.UTF_8);
                JsonNode root = mapper.readTree(jsonStr);

                String msgType = root.path("type").asText("UNKNOWN");
                String msgId = root.path("messageId").asText(UUID.randomUUID().toString());
                String reqId = root.path("payload").path("requestId").asText("N/A");

                eventTraceLog.add(String.format("%s | RECV | type=%s | reqId=%s | mode=%s",
                        Instant.now(), msgType, reqId, currentMode));

                // Xử lý theo FaultMode
                switch (currentMode) {
                    case DROP_CONNECTION -> {
                        eventTraceLog.add(String.format("%s | ACTION | DROP_CONNECTION for reqId=%s", Instant.now(), reqId));
                        socket.close();
                        return;
                    }
                    case NO_QUORUM -> {
                        eventTraceLog.add(String.format("%s | ACTION | SEND_NO_QUORUM for reqId=%s", Instant.now(), reqId));
                        sendErrorResponse(dos, msgId, reqId, "NO_QUORUM", "⚡ Không đủ đa số (Quorum 3/5) để cam kết an toàn", null);
                    }
                    case NOT_LEADER -> {
                        eventTraceLog.add(String.format("%s | ACTION | SEND_NOT_LEADER hint=%s for reqId=%s", Instant.now(), leaderHint, reqId));
                        sendErrorResponse(dos, msgId, reqId, "NOT_LEADER", "Node is follower, please redirect to leader", leaderHint);
                    }
                    case TIMEOUT -> {
                        eventTraceLog.add(String.format("%s | ACTION | TIMEOUT_HOLD for reqId=%s (%d ms)", Instant.now(), reqId, timeoutDelayMs));
                        try {
                            Thread.sleep(timeoutDelayMs);
                        } catch (InterruptedException ignored) {}
                        if (!socket.isClosed()) {
                            sendSuccessResponse(dos, msgType, msgId, reqId);
                        }
                    }
                    case NORMAL -> {
                        sendSuccessResponse(dos, msgType, msgId, reqId);
                    }
                }
            }
        } catch (IOException ignored) {
        }
    }

    private void sendErrorResponse(DataOutputStream dos, String correlationId, String requestId, String errorCode, String message, String hint) throws IOException {
        ObjectNode env = mapper.createObjectNode();
        env.put("protocolVersion", 1);
        env.put("messageId", UUID.randomUUID().toString());
        env.put("correlationId", correlationId);
        env.put("type", "ERROR_RESPONSE");
        env.put("senderId", nodeId);
        env.put("sentAt", ISO_FORMATTER.format(Instant.now()));

        ObjectNode payload = env.putObject("payload");
        payload.put("errorCode", errorCode);
        payload.put("message", message);
        payload.put("requestId", requestId);
        payload.put("term", 2L);
        if (hint != null) {
            payload.put("leaderHint", hint);
        } else {
            payload.putNull("leaderHint");
        }

        byte[] bytes = mapper.writeValueAsBytes(env);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        dos.flush();
    }

    private void sendSuccessResponse(DataOutputStream dos, String reqType, String correlationId, String requestId) throws IOException {
        ObjectNode env = mapper.createObjectNode();
        env.put("protocolVersion", 1);
        env.put("messageId", UUID.randomUUID().toString());
        env.put("correlationId", correlationId);
        env.put("senderId", nodeId);
        env.put("sentAt", ISO_FORMATTER.format(Instant.now()));

        ObjectNode payload = env.putObject("payload");
        if ("BOOK_TICKET".equals(reqType)) {
            env.put("type", "BOOK_TICKET_RESPONSE");
            payload.put("requestId", requestId);
            payload.put("outcome", "CONFIRMED");
            payload.put("ticketId", "TCK-" + UUID.randomUUID().toString().substring(0, 8));
            payload.putNull("reasonCode");
            payload.put("appliedLogIndex", 100L);
            payload.put("mock", false);
        } else if ("GET_SEATS".equals(reqType)) {
            env.put("type", "GET_SEATS_RESPONSE");
            payload.put("tripId", "SE-001");
            payload.putArray("seats");
            payload.put("readIndex", 100L);
            payload.put("leaderId", nodeId);
            payload.put("mock", false);
        } else {
            env.put("type", "NODE_STATUS_RESPONSE");
            payload.put("nodeId", nodeId);
            payload.put("role", "LEADER");
            payload.put("term", 2L);
            payload.put("appliedIndex", 100L);
        }

        byte[] bytes = mapper.writeValueAsBytes(env);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        dos.flush();
    }

    @Override
    public void close() {
        running.set(false);
        try {
            serverSocket.close();
        } catch (IOException ignored) {}
        threadPool.shutdownNow();
    }
}
