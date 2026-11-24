package vn.nhom6.railway.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

/**
 * Live TCP implementation of {@link MonitorTcpClient.NodeStatusAdapter} (SPEC-03 §5).
 *
 * <p>Sends a 4-byte length-prefixed {@link MessageType#NODE_STATUS} JSON envelope to each
 * configured backend node's client port and validates the correlated
 * {@link MessageType#NODE_STATUS_RESPONSE}. Observation-only: never mutates Raft state.
 */
public final class TcpNodeStatusAdapter implements MonitorTcpClient.NodeStatusAdapter {

    public static final String DEFAULT_SENDER_ID = "desktop-monitor";
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(1000);
    public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(1500);
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(1000);

    private final Map<String, InetSocketAddress> endpoints;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final String senderId;

    public TcpNodeStatusAdapter(Map<String, InetSocketAddress> endpoints) {
        this(endpoints, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_SENDER_ID);
    }

    public TcpNodeStatusAdapter(
            Map<String, InetSocketAddress> endpoints,
            Duration connectTimeout,
            Duration readTimeout,
            String senderId) {
        Objects.requireNonNull(endpoints, "endpoints");
        Map<String, InetSocketAddress> copy = new LinkedHashMap<>();
        for (String nodeId : MonitorTcpClient.NODE_IDS) {
            InetSocketAddress addr = endpoints.get(nodeId);
            if (addr == null) {
                throw new IllegalArgumentException("Missing endpoint for " + nodeId);
            }
            copy.put(nodeId, addr);
        }
        this.endpoints = Collections.unmodifiableMap(copy);
        this.connectTimeoutMs = positiveMillis(connectTimeout, "connectTimeout");
        this.readTimeoutMs = positiveMillis(readTimeout, "readTimeout");
        this.senderId = (senderId != null && !senderId.isBlank()) ? senderId.strip() : DEFAULT_SENDER_ID;
    }

    @Override
    public JsonNode fetch(String nodeId) throws IOException {
        InetSocketAddress endpoint = endpoints.get(nodeId);
        if (endpoint == null) {
            throw new IllegalArgumentException("Unknown nodeId: " + nodeId);
        }

        ObjectNode emptyPayload = MessageCodec.getMapper().createObjectNode();
        MessageEnvelope<ObjectNode> request = MessageEnvelope.createRequest(
                MessageType.NODE_STATUS, senderId, emptyPayload);

        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(readTimeoutMs);
            socket.connect(endpoint, connectTimeoutMs);

            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            InputStream in = socket.getInputStream();

            FrameCodec.writeMessage(out, request);
            byte[] responseBytes = FrameCodec.readFrame(in);
            MessageEnvelope<JsonNode> response = MessageCodec.decode(responseBytes);
            response.validate();

            if (!Objects.equals(request.getMessageId(), response.getCorrelationId())) {
                throw new IOException("CorrelationId mismatch from " + nodeId);
            }
            if (response.getType() != MessageType.NODE_STATUS_RESPONSE) {
                throw new IOException("Unexpected response type from " + nodeId + ": " + response.getType());
            }
            JsonNode payload = response.getPayload();
            if (payload == null || !payload.isObject()) {
                throw new IOException("Missing NODE_STATUS_RESPONSE payload from " + nodeId);
            }
            return payload;
        }
    }

    public Map<String, InetSocketAddress> endpoints() {
        return endpoints;
    }

    /** Default local Docker/dev endpoints (127.0.0.1:9101..9105). */
    public static Map<String, InetSocketAddress> defaultDevEndpoints() {
        Map<String, InetSocketAddress> map = new LinkedHashMap<>();
        for (int i = 1; i <= 5; i++) {
            map.put("node-" + i, new InetSocketAddress("127.0.0.1", 9100 + i));
        }
        return Collections.unmodifiableMap(map);
    }

    /** Loads 5-node client endpoints and monitor timing settings from a cluster.properties file. */
    public static MonitorClusterConfig loadConfig(Path propertiesFile) throws IOException {
        Objects.requireNonNull(propertiesFile, "propertiesFile");
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        }
        return parseConfig(props);
    }

    /**
     * Resolves monitor config from {@code -Dmonitor.config}, {@code MONITOR_CONFIG},
     * {@code configs/dev/cluster.properties}, {@code ../configs/dev/cluster.properties},
     * or falls back to {@link #defaultDevEndpoints()}.
     */
    public static MonitorClusterConfig loadDefaultConfig() {
        String explicit = System.getProperty("monitor.config");
        if (explicit == null || explicit.isBlank()) {
            explicit = System.getenv("MONITOR_CONFIG");
        }
        if (explicit != null && !explicit.isBlank()) {
            Path path = Path.of(explicit.strip());
            if (Files.isRegularFile(path)) {
                try {
                    return loadConfig(path);
                } catch (IOException ignored) {
                    // Fall through to default dev endpoints if unreadable
                }
            }
        }
        for (Path candidate : new Path[] {
                Path.of("configs/dev/cluster.properties"),
                Path.of("../configs/dev/cluster.properties") }) {
            if (Files.isRegularFile(candidate)) {
                try {
                    return loadConfig(candidate);
                } catch (IOException ignored) {
                    // Try next candidate
                }
            }
        }
        return new MonitorClusterConfig(
                defaultDevEndpoints(),
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_READ_TIMEOUT,
                DEFAULT_POLL_INTERVAL);
    }

    static MonitorClusterConfig parseConfig(Properties props) {
        Map<String, InetSocketAddress> endpoints = new LinkedHashMap<>();
        for (int i = 1; i <= 5; i++) {
            String nodeId = "node-" + i;
            String host = props.getProperty("node." + nodeId + ".client.host", "127.0.0.1").strip();
            String portText = props.getProperty("node." + nodeId + ".client.port", String.valueOf(9100 + i)).strip();
            int port = Integer.parseInt(portText);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Invalid port for " + nodeId + ": " + port);
            }
            endpoints.put(nodeId, new InetSocketAddress(host, port));
        }
        Duration connectTimeout = Duration.ofMillis(parsePositiveLong(
                props.getProperty("client.connect-timeout-ms"), DEFAULT_CONNECT_TIMEOUT.toMillis()));
        Duration readTimeout = Duration.ofMillis(parsePositiveLong(
                props.getProperty("monitor.request-timeout-ms"), DEFAULT_READ_TIMEOUT.toMillis()));
        Duration pollInterval = Duration.ofMillis(parsePositiveLong(
                props.getProperty("monitor.poll-interval-ms"), DEFAULT_POLL_INTERVAL.toMillis()));
        return new MonitorClusterConfig(
                Collections.unmodifiableMap(endpoints), connectTimeout, readTimeout, pollInterval);
    }

    private static int positiveMillis(Duration duration, String name) {
        long ms = Objects.requireNonNull(duration, name).toMillis();
        if (ms <= 0 || ms > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be positive ms, was " + duration);
        }
        return (int) ms;
    }

    private static long parsePositiveLong(String raw, long fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            long value = Long.parseLong(raw.strip());
            return value > 0 ? value : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    public record MonitorClusterConfig(
            Map<String, InetSocketAddress> endpoints,
            Duration connectTimeout,
            Duration readTimeout,
            Duration pollInterval) { }
}
