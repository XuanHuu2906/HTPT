package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Objects;

/**
 * Blocking TCP Socket Client for Node-to-Node RPC and Client-to-Node communication.
 *
 * <p>Features:
 * <ul>
 *   <li>Configurable connection timeout and read timeout ({@code SO_TIMEOUT}).</li>
 *   <li>Disables Nagle's algorithm ({@code TCP_NODELAY = true}) for low-latency RPC.</li>
 *   <li>Serialized writer lock per socket per SPEC-03 Section 2.</li>
 *   <li>Supports synchronous Request-Response RPC and raw framed bytes.</li>
 *   <li>Graceful resource management ({@code AutoCloseable}).</li>
 * </ul>
 */
public class TcpClient implements AutoCloseable {

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 10_000;

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private final Object writeLock = new Object();
    private final Object readLock = new Object();
    private final Object rpcLock = new Object();

    public TcpClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public TcpClient(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port out of bounds [1, 65535]: " + port);
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive: " + connectTimeoutMs);
        }
        if (readTimeoutMs < 0) {
            throw new IllegalArgumentException("readTimeoutMs must not be negative: " + readTimeoutMs);
        }
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /**
     * Establishes a TCP connection to the configured host and port.
     *
     * @throws SocketTimeoutException If connect timeout expires before connection is established
     * @throws IOException If I/O or network error occurs
     */
    public synchronized void connect() throws IOException {
        if (isConnected()) {
            return;
        }

        socket = new Socket();
        socket.setTcpNoDelay(true);
        if (readTimeoutMs > 0) {
            socket.setSoTimeout(readTimeoutMs);
        }

        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        in = socket.getInputStream();
        out = new BufferedOutputStream(socket.getOutputStream());
    }

    /**
     * Checks if the client has an active open connection.
     */
    public synchronized boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    /**
     * Ensures the socket is connected, establishing the connection if needed.
     */
    private void ensureConnected() throws IOException {
        if (!isConnected()) {
            connect();
        }
    }

    /**
     * Sends a raw payload frame (one-way).
     *
     * @param payload Raw payload bytes
     * @throws IOException If write fails
     */
    public void send(byte[] payload) throws IOException {
        synchronized (writeLock) {
            ensureConnected();
            FrameCodec.writeFrame(out, payload);
        }
    }

    /**
     * Sends a {@link MessageEnvelope} (one-way).
     *
     * @param envelope Message envelope
     * @throws IOException If encoding or write fails
     */
    public void send(MessageEnvelope<?> envelope) throws IOException {
        byte[] bytes = MessageCodec.encode(envelope);
        send(bytes);
    }

    /**
     * Reads a single length-prefixed frame from the socket.
     *
     * @return Raw payload bytes
     * @throws EOFException If server closed connection
     * @throws SocketTimeoutException If read timeout expires
     * @throws IOException If I/O error occurs
     */
    public byte[] receive() throws IOException {
        synchronized (readLock) {
            ensureConnected();
            return FrameCodec.readFrame(in);
        }
    }

    /**
     * Reads a single length-prefixed message envelope from the socket.
     *
     * @return Decoded envelope with JsonNode payload
     * @throws IOException If read or decoding fails
     */
    public MessageEnvelope<JsonNode> receiveMessage() throws IOException {
        byte[] frame = receive();
        return MessageCodec.decode(frame);
    }

    /**
     * Reads a single length-prefixed message envelope and decodes the payload into the given class.
     *
     * @param payloadClass Target payload DTO class
     * @param <T> Payload type
     * @return Decoded envelope
     * @throws IOException If read or decoding fails
     */
    public <T> MessageEnvelope<T> receiveMessage(Class<T> payloadClass) throws IOException {
        byte[] frame = receive();
        return MessageCodec.decode(frame, payloadClass);
    }

    /**
     * Sends a raw payload frame and waits synchronously for the response frame.
     *
     * @param payload Request payload bytes
     * @return Response payload bytes
     * @throws SocketTimeoutException If read timeout expires
     * @throws IOException If send or receive fails
     */
    public byte[] sendAndReceive(byte[] payload) throws IOException {
        synchronized (rpcLock) {
            ensureConnected();
            synchronized (writeLock) {
                FrameCodec.writeFrame(out, payload);
            }
            synchronized (readLock) {
                return FrameCodec.readFrame(in);
            }
        }
    }

    /**
     * Sends a request envelope and waits synchronously for the response envelope.
     *
     * @param request Request envelope
     * @return Response envelope with generic JsonNode payload
     * @throws IOException If transmission or decoding fails
     */
    public MessageEnvelope<JsonNode> sendAndReceive(MessageEnvelope<?> request) throws IOException {
        byte[] reqBytes = MessageCodec.encode(request);
        byte[] respBytes = sendAndReceive(reqBytes);
        return MessageCodec.decode(respBytes);
    }

    /**
     * Sends a request envelope and waits synchronously for the typed response envelope.
     *
     * @param request Request envelope
     * @param responsePayloadClass Expected response payload class
     * @param <T> Payload type
     * @return Decoded response envelope
     * @throws IOException If transmission or decoding fails
     */
    public <T> MessageEnvelope<T> sendAndReceive(MessageEnvelope<?> request, Class<T> responsePayloadClass) throws IOException {
        byte[] reqBytes = MessageCodec.encode(request);
        byte[] respBytes = sendAndReceive(reqBytes);
        return MessageCodec.decode(respBytes, responsePayloadClass);
    }

    /**
     * Closes the active socket and I/O streams.
     */
    public synchronized void disconnect() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException ignored) {}
            out = null;
        }
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {}
            in = null;
        }
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {}
            socket = null;
        }
    }

    @Override
    public void close() {
        disconnect();
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
}
