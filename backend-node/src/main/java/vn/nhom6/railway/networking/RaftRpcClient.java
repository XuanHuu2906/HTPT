package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.*;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * High-performance Asynchronous Raft RPC Client for Node-to-Node communication.
 *
 * <p>Architectural guarantees:
 * <ul>
 *   <li><b>Async Non-blocking API:</b> {@code sendRequestVote} and {@code sendAppendEntries} return {@link CompletableFuture}.</li>
 *   <li><b>Persistent Connection Reuse:</b> Maintains a dedicated, long-lived TCP connection per peer node.</li>
 *   <li><b>Correlation Multiplexing:</b> Pairs concurrent requests and responses over a single TCP stream via {@code messageId} and {@code correlationId}.</li>
 *   <li><b>Backpressure & Timeout Defense:</b> Enforced by {@link RpcCorrelationRegistry} to prevent resource exhaustion and buffer bloat.</li>
 *   <li><b>Wire Framing Serialization:</b> Writes length-prefixed UTF-8 JSON frames guarded by a per-socket write lock per SPEC-03 Section 2.</li>
 *   <li><b>Failure Resilience:</b> Detects connection drops immediately, cancels in-flight requests, and lazily reconnects on the next RPC.</li>
 * </ul>
 */
public class RaftRpcClient implements AutoCloseable {

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    public static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofSeconds(3);

    private final String localNodeId;
    private final ConcurrentMap<String, InetSocketAddress> peerEndpoints = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PeerConnection> connections = new ConcurrentHashMap<>();
    private final RpcCorrelationRegistry correlationRegistry;
    private final boolean ownsRegistry;
    private final int connectTimeoutMs;
    private final Duration defaultRpcTimeout;

    private final java.util.concurrent.ExecutorService runtimeIo = new java.util.concurrent.ThreadPoolExecutor(
            4, 4, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(64), task -> {
                Thread thread = new Thread(task, "raft-tcp-send");
                thread.setDaemon(true);
                return thread;
            });

    private volatile boolean closed = false;

    public RaftRpcClient(String localNodeId, Map<String, InetSocketAddress> peerEndpoints) {
        this(localNodeId, peerEndpoints, RpcCorrelationRegistry.DEFAULT_MAX_IN_FLIGHT_PER_PEER,
                DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_RPC_TIMEOUT);
    }

    public RaftRpcClient(
            String localNodeId,
            Map<String, InetSocketAddress> peerEndpoints,
            int maxInFlightPerPeer,
            int connectTimeoutMs,
            Duration defaultRpcTimeout) {
        this(localNodeId, peerEndpoints, new RpcCorrelationRegistry(maxInFlightPerPeer), true,
                connectTimeoutMs, defaultRpcTimeout);
    }

    public RaftRpcClient(
            String localNodeId,
            Map<String, InetSocketAddress> peerEndpoints,
            RpcCorrelationRegistry correlationRegistry,
            int connectTimeoutMs,
            Duration defaultRpcTimeout) {
        this(localNodeId, peerEndpoints, correlationRegistry, false, connectTimeoutMs, defaultRpcTimeout);
    }

    private RaftRpcClient(
            String localNodeId,
            Map<String, InetSocketAddress> peerEndpoints,
            RpcCorrelationRegistry correlationRegistry,
            boolean ownsRegistry,
            int connectTimeoutMs,
            Duration defaultRpcTimeout) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
        this.correlationRegistry = Objects.requireNonNull(correlationRegistry, "correlationRegistry must not be null");
        this.ownsRegistry = ownsRegistry;
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive: " + connectTimeoutMs);
        }
        this.connectTimeoutMs = connectTimeoutMs;
        this.defaultRpcTimeout = Objects.requireNonNull(defaultRpcTimeout, "defaultRpcTimeout must not be null");

        if (peerEndpoints != null) {
            this.peerEndpoints.putAll(peerEndpoints);
        }
    }

    /**
     * Registers or updates a peer endpoint.
     */
    public void setPeerEndpoint(String peerId, InetSocketAddress address) {
        Objects.requireNonNull(peerId, "peerId must not be null");
        Objects.requireNonNull(address, "address must not be null");
        peerEndpoints.put(peerId, address);
    }

    /**
     * Registers or updates a peer endpoint by host and port.
     */
    public void setPeerEndpoint(String peerId, String host, int port) {
        setPeerEndpoint(peerId, new InetSocketAddress(host, port));
    }

    /**
     * Sends an asynchronous REQUEST_VOTE RPC with the default RPC timeout.
     */
    public CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> sendRequestVote(
            String peerId,
            RequestVoteRequestDto request) {
        return sendRequestVote(peerId, request, defaultRpcTimeout);
    }

    /**
     * Sends an asynchronous REQUEST_VOTE RPC with an explicit deadline timeout.
     *
     * @param peerId Target candidate/follower peer ID
     * @param request Request payload DTO
     * @param timeout Request deadline
     * @return CompletableFuture completing with the typed response envelope
     */
    public CompletableFuture<MessageEnvelope<RequestVoteResponseDto>> sendRequestVote(
            String peerId,
            RequestVoteRequestDto request,
            Duration timeout) {
        Objects.requireNonNull(request, "request must not be null");
        request.validate();
        MessageEnvelope<RequestVoteRequestDto> envelope = MessageEnvelope.createRequest(
                MessageType.REQUEST_VOTE, localNodeId, request);
        return sendEnvelope(peerId, envelope, timeout, RequestVoteResponseDto.class);
    }

    /**
     * Sends an asynchronous APPEND_ENTRIES RPC (log append or heartbeat) with the default RPC timeout.
     */
    public CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> sendAppendEntries(
            String peerId,
            AppendEntriesRequestDto request) {
        return sendAppendEntries(peerId, request, defaultRpcTimeout);
    }

    /**
     * Sends an asynchronous APPEND_ENTRIES RPC with an explicit deadline timeout.
     *
     * @param peerId Target follower peer ID
     * @param request Request payload DTO (empty entries list represents a heartbeat)
     * @param timeout Request deadline
     * @return CompletableFuture completing with the typed response envelope
     */
    public CompletableFuture<MessageEnvelope<AppendEntriesResponseDto>> sendAppendEntries(
            String peerId,
            AppendEntriesRequestDto request,
            Duration timeout) {
        Objects.requireNonNull(request, "request must not be null");
        request.validate();
        MessageEnvelope<AppendEntriesRequestDto> envelope = MessageEnvelope.createRequest(
                MessageType.APPEND_ENTRIES, localNodeId, request);
        return sendEnvelope(peerId, envelope, timeout, AppendEntriesResponseDto.class);
    }

    /**
     * Generic asynchronous RPC sender.
     */
    public <T> CompletableFuture<MessageEnvelope<T>> sendRpc(
            String peerId,
            MessageType type,
            Object payload,
            Duration timeout,
            Class<T> responsePayloadClass) {
        MessageEnvelope<?> envelope = MessageEnvelope.createRequest(type, localNodeId, payload);
        return sendEnvelope(peerId, envelope, timeout, responsePayloadClass);
    }

    /** Preserves adapter messageId/correlation and keeps connect/write off the Raft loop. */
    public CompletableFuture<MessageEnvelope<com.fasterxml.jackson.databind.JsonNode>> exchange(
            String peerId, MessageEnvelope<?> request) {
        try {
            return CompletableFuture.supplyAsync(() -> sendEnvelope(peerId, request,
                    defaultRpcTimeout, com.fasterxml.jackson.databind.JsonNode.class), runtimeIo)
                    .thenCompose(future -> future);
        } catch (java.util.concurrent.RejectedExecutionException unavailable) {
            return CompletableFuture.failedFuture(unavailable);
        }
    }

    private <T> CompletableFuture<MessageEnvelope<T>> sendEnvelope(
            String peerId,
            MessageEnvelope<?> envelope,
            Duration timeout,
            Class<T> responsePayloadClass) {

        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("RaftRpcClient is closed"));
        }
        Objects.requireNonNull(peerId, "peerId must not be null");
        Objects.requireNonNull(envelope, "envelope must not be null");

        // 1. Register with correlation registry (checks backpressure)
        CompletableFuture<MessageEnvelope<T>> future;
        try {
            future = correlationRegistry.register(
                    envelope.getMessageId(), peerId, timeout, responsePayloadClass);
        } catch (RpcBackpressureException e) {
            return CompletableFuture.failedFuture(e);
        }

        // 2. Transmit frame over reused peer socket
        try {
            byte[] frameBytes = MessageCodec.encode(envelope);
            PeerConnection conn = getOrCreateConnection(peerId);
            conn.send(frameBytes);
        } catch (Exception e) {
            correlationRegistry.fail(envelope.getMessageId(), e);
            return CompletableFuture.failedFuture(e);
        }

        return future;
    }

    private PeerConnection getOrCreateConnection(String peerId) throws IOException {
        if (closed) {
            throw new IllegalStateException("RaftRpcClient is closed");
        }

        PeerConnection conn = connections.get(peerId);
        if (conn != null && conn.isConnected()) {
            return conn;
        }

        synchronized (connections) {
            conn = connections.get(peerId);
            if (conn != null && conn.isConnected()) {
                return conn;
            }

            InetSocketAddress address = peerEndpoints.get(peerId);
            if (address == null) {
                throw new IllegalArgumentException("No endpoint configured for peer: " + peerId);
            }

            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            try {
                InetSocketAddress resolved = address.isUnresolved()
                        ? new InetSocketAddress(address.getHostString(), address.getPort()) : address;
                socket.connect(resolved, connectTimeoutMs);
            } catch (IOException | RuntimeException connectFailure) {
                try { socket.close(); } catch (IOException ignored) { }
                throw connectFailure;
            }

            conn = new PeerConnection(peerId, address, socket);
            connections.put(peerId, conn);
            conn.startReader();
            return conn;
        }
    }

    private void handleConnectionClosed(String peerId, PeerConnection connection, Throwable cause) {
        connections.remove(peerId, connection);
        connection.closeQuietly();
        correlationRegistry.failAll(peerId, cause != null ? cause : new IOException("Connection closed to peer " + peerId));
    }

    public RpcCorrelationRegistry getCorrelationRegistry() {
        return correlationRegistry;
    }

    public Map<String, InetSocketAddress> getPeerEndpoints() {
        return Collections.unmodifiableMap(peerEndpoints);
    }

    public boolean isConnectedTo(String peerId) {
        PeerConnection conn = connections.get(peerId);
        return conn != null && conn.isConnected();
    }

    @Override
    public void close() {
        runtimeIo.shutdownNow();
        closed = true;
        synchronized (connections) {
            connections.forEach((peerId, conn) -> {
                conn.closeQuietly();
                correlationRegistry.failAll(peerId, new IOException("RaftRpcClient closed"));
            });
            connections.clear();
        }
        if (ownsRegistry) {
            correlationRegistry.close();
        }
    }

    /**
     * Managed connection to a single peer node.
     */
    private final class PeerConnection {
        private final String peerId;
        private final InetSocketAddress address;
        private final Socket socket;
        private final OutputStream out;
        private final InputStream in;
        private final Object writeLock = new Object();
        private final Thread readerThread;
        private volatile boolean running = true;

        PeerConnection(String peerId, InetSocketAddress address, Socket socket) throws IOException {
            this.peerId = peerId;
            this.address = address;
            this.socket = socket;
            this.out = new BufferedOutputStream(socket.getOutputStream());
            this.in = socket.getInputStream();
            this.readerThread = new Thread(this::readerLoop, "raft-rpc-reader-" + peerId);
            this.readerThread.setDaemon(true);
        }

        void startReader() {
            readerThread.start();
        }

        boolean isConnected() {
            return running && socket.isConnected() && !socket.isClosed();
        }

        void send(byte[] frameBytes) throws IOException {
            synchronized (writeLock) {
                if (!isConnected()) {
                    throw new IOException("Socket to peer " + peerId + " is disconnected");
                }
                FrameCodec.writeFrame(out, frameBytes);
            }
        }

        void closeQuietly() {
            running = false;
            try {
                socket.close();
            } catch (IOException ignored) {}
        }

        private void readerLoop() {
            Throwable exitCause = null;
            try {
                while (running && !socket.isClosed()) {
                    byte[] frame;
                    try {
                        frame = FrameCodec.readFrame(in);
                    } catch (EOFException | SocketException e) {
                        exitCause = e;
                        break;
                    }

                    try {
                        MessageEnvelope<JsonNode> envelope = MessageCodec.decode(frame);
                        String correlationId = envelope.getCorrelationId();
                        if (correlationId != null) {
                            correlationRegistry.complete(correlationId, envelope);
                        }
                    } catch (Exception ex) {
                        // Bad frame or corrupt JSON from peer closes socket per SPEC-03 / SPEC-07
                        exitCause = ex;
                        break;
                    }
                }
            } catch (Exception ex) {
                exitCause = ex;
            } finally {
                handleConnectionClosed(peerId, this, exitCause);
            }
        }
    }
}
