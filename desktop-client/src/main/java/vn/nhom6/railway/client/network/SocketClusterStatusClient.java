package vn.nhom6.railway.client.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
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
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Triển khai {@link ClusterStatusClient} kết nối TCP Socket tới Node backend thật
 * theo đặc tả SPEC-03 Section 5 (Diagnostic NODE_STATUS).
 *
 * <p>Đặc tính kỹ thuật:
 * <ul>
 *   <li>Tuân thủ 100% hợp đồng SPEC-03: 4-byte BE length framing + UTF-8 JSON envelope.</li>
 *   <li>Gửi {@link MessageType#NODE_STATUS} với payload rỗng {@code {}}, nhận {@link MessageType#NODE_STATUS_RESPONSE}.</li>
 *   <li>Chạy hoàn toàn bất đồng bộ trên daemon thread, không chặn JavaFX Application Thread.</li>
 *   <li>Tuyệt đối KHÔNG dùng HTTP/REST hay Web framework (AGENTS.md).</li>
 *   <li>Luôn trả về nguồn dữ liệu là {@link DataSource#REAL}.</li>
 * </ul>
 */
public class SocketClusterStatusClient implements ClusterStatusClient {

    private static final System.Logger LOGGER = System.getLogger(SocketClusterStatusClient.class.getName());

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9101;
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 3000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 4000;
    public static final String DEFAULT_SENDER_ID = "client-status-monitor";

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final String senderId;
    private final ExecutorService executor;

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "socket-cluster-status-worker");
        t.setDaemon(true);
        return t;
    };

    public SocketClusterStatusClient() {
        this(resolveConfiguredHost(), resolveConfiguredPort());
    }

    public SocketClusterStatusClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_SENDER_ID);
    }

    public SocketClusterStatusClient(String host, int port, int connectTimeoutMs, int readTimeoutMs, String senderId) {
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

    @Override
    public CompletableFuture<NodeStatus> getNodeStatus() {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            ObjectNode emptyPayload = MessageCodec.getMapper().createObjectNode();
            MessageEnvelope<ObjectNode> reqEnvelope = MessageEnvelope.createRequest(
                    MessageType.NODE_STATUS,
                    senderId,
                    emptyPayload
            );
            String messageId = reqEnvelope.getMessageId();

            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                if (readTimeoutMs > 0) {
                    socket.setSoTimeout(readTimeoutMs);
                }
                socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);

                try (OutputStream out = new BufferedOutputStream(socket.getOutputStream());
                     InputStream in = socket.getInputStream()) {

                    FrameCodec.writeMessage(out, reqEnvelope);
                    byte[] responseBytes = FrameCodec.readFrame(in);
                    MessageEnvelope<JsonNode> resEnvelope = MessageCodec.decode(responseBytes);

                    long elapsed = System.currentTimeMillis() - startTime;

                    // Kiểm tra correlationId khớp messageId gửi đi
                    String expectedCorrId = reqEnvelope.getMessageId();
                    String actualCorrId = resEnvelope.getCorrelationId();
                    if (!Objects.equals(expectedCorrId, actualCorrId)) {
                        throw new IOException("Lỗi tương quan gói tin: correlationId không khớp (kỳ vọng: "
                                + expectedCorrId + ", nhận: " + actualCorrId + ")");
                    }

                    MessageType type = resEnvelope.getType();
                    if (type == MessageType.NODE_STATUS_RESPONSE) {
                        JsonNode payload = resEnvelope.getPayload();
                        NodeStatus status = parseNodeStatus(payload);
                        logCall(messageId, "NODE_STATUS_RESPONSE", "SUCCESS", elapsed, status.nodeId(), status.role());
                        return status;
                    } else if (type == MessageType.ERROR_RESPONSE) {
                        ErrorResponseDto err = MessageCodec.getMapper().treeToValue(
                                resEnvelope.getPayload(), ErrorResponseDto.class
                        );
                        logCall(messageId, "ERROR_RESPONSE", err != null ? err.getErrorCode() : "UNKNOWN_ERROR", elapsed, null, null);
                        throw new IOException("Node trả lời ERROR_RESPONSE: " + (err != null ? err.getMessage() : "unknown"));
                    } else {
                        logCall(messageId, type != null ? type.name() : "UNKNOWN", "UNEXPECTED_TYPE", elapsed, null, null);
                        throw new IOException("Nhận loại bản tin không mong đợi: " + type);
                    }
                }
            } catch (Exception e) {
                long elapsed = System.currentTimeMillis() - startTime;
                logCall(messageId, "EXCEPTION", e.getClass().getSimpleName() + ": " + e.getMessage(), elapsed, null, null);
                throw new RuntimeException("Lỗi kết nối Socket tới " + host + ":" + port + " (" + e.getMessage() + ")", e);
            }
        }, executor);
    }

    private NodeStatus parseNodeStatus(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            return NodeStatus.emptyUnknown();
        }

        String nodeId = payload.hasNonNull("nodeId") ? payload.get("nodeId").asText() : null;
        String role = payload.hasNonNull("role") ? payload.get("role").asText() : "UNKNOWN";
        Long term = payload.hasNonNull("term") ? payload.get("term").asLong() : null;
        String leaderId = payload.hasNonNull("leaderId") ? payload.get("leaderId").asText() : null;
        if (leaderId == null && payload.hasNonNull("leaderHint")) {
            leaderId = payload.get("leaderHint").asText();
        }
        Long lastLogIndex = payload.hasNonNull("lastLogIndex") ? payload.get("lastLogIndex").asLong() : null;
        Long commitIndex = payload.hasNonNull("commitIndex") ? payload.get("commitIndex").asLong() : null;
        Long lastApplied = payload.hasNonNull("lastApplied") ? payload.get("lastApplied").asLong() : null;
        Boolean storageHealthy = payload.hasNonNull("storageHealthy") ? payload.get("storageHealthy").asBoolean() : null;

        Instant observedAt = Instant.now();
        if (payload.hasNonNull("observedAt")) {
            try {
                observedAt = Instant.parse(payload.get("observedAt").asText());
            } catch (Exception ignored) {
            }
        }

        return new NodeStatus(
                nodeId,
                role,
                term,
                leaderId,
                lastLogIndex,
                commitIndex,
                lastApplied,
                storageHealthy,
                observedAt
        );
    }

    private void logCall(String messageId, String type, String outcome, long elapsedMs, String nodeId, String role) {
        String logEntry = String.format(
                "[STATUS-CLIENT] messageId=%s, type=%s, endpoint=%s:%d, outcome=%s, elapsedMs=%dms, node=%s, role=%s",
                messageId, type, host, port, outcome, elapsedMs, nodeId != null ? nodeId : "-", role != null ? role : "-"
        );
        LOGGER.log(System.Logger.Level.INFO, logEntry);
        System.out.println(logEntry);
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.REAL;
    }

    @Override
    public String getEndpoint() {
        return host + ":" + port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    public static String resolveConfiguredHost() {
        String sysHost = System.getProperty("node.host");
        if (sysHost == null || sysHost.isBlank()) {
            sysHost = System.getProperty("cluster.host");
        }
        if (sysHost == null || sysHost.isBlank()) {
            sysHost = System.getProperty("booking.client.host");
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

    public static int resolveConfiguredPort() {
        String sysPort = System.getProperty("node.port");
        if (sysPort == null || sysPort.isBlank()) {
            sysPort = System.getProperty("cluster.port");
        }
        if (sysPort == null || sysPort.isBlank()) {
            sysPort = System.getProperty("booking.client.port");
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
