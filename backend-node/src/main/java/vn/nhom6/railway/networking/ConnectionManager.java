package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.BookTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Robust TCP Connection Manager with timeout enforcement, safe reconnect, and non-destructive
 * timeout mapping (TASK TV3-D14).
 *
 * <p>Key Invariants (AGENTS.md §5 & SPEC-00 §5.10):
 * <ul>
 *   <li><b>Safe Reconnection:</b> A socket experiencing a read/write timeout is immediately closed
 *       to prevent partial frame reading; fresh connections are established transparently.</li>
 *   <li><b>Never Convert Timeout to REJECTED / BOOK_FAILED:</b> In-flight timeouts cannot infer
 *       failure. The outcome is returned as {@code UNKNOWN} with code {@code TIMEOUT_UNKNOWN}.</li>
 *   <li><b>RequestId Invariance:</b> The client's original {@code requestId} is strictly preserved.</li>
 *   <li><b>ResultLookup Integration:</b> Transparently queries {@link ResultLookup} (or {@link FakeResultLookup})
 *       to recover definitive {@code CONFIRMED} or {@code REJECTED} outcomes when available.</li>
 * </ul>
 */
public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class.getName());

    private final String host;
    private final int port;
    private volatile RetryPolicy retryPolicy;
    private volatile ResultLookup resultLookup;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private final Object connectionLock = new Object();
    private final Object rpcLock = new Object();

    public ConnectionManager(String host, int port) {
        this(host, port, RetryPolicy.defaultPolicy(), null);
    }

    public ConnectionManager(String host, int port, RetryPolicy retryPolicy) {
        this(host, port, retryPolicy, null);
    }

    public ConnectionManager(String host, int port, RetryPolicy retryPolicy, ResultLookup resultLookup) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port out of bounds [1, 65535]: " + port);
        }
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        this.resultLookup = resultLookup;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    }

    public ResultLookup getResultLookup() {
        return resultLookup;
    }

    public void setResultLookup(ResultLookup resultLookup) {
        this.resultLookup = resultLookup;
    }

    /**
     * Checks if the socket is actively connected and not closed.
     */
    public boolean isConnected() {
        synchronized (connectionLock) {
            return socket != null && socket.isConnected() && !socket.isClosed();
        }
    }

    /**
     * Establishes a TCP connection enforcing {@code connectTimeoutMs} and {@code readTimeoutMs}.
     */
    public void connect() throws IOException {
        synchronized (connectionLock) {
            if (isConnected()) {
                return;
            }
            closeInternal();

            Socket s = new Socket();
            s.setTcpNoDelay(true);
            if (retryPolicy.readTimeoutMs() > 0) {
                s.setSoTimeout(retryPolicy.readTimeoutMs());
            }
            s.connect(new InetSocketAddress(host, port), retryPolicy.connectTimeoutMs());

            this.socket = s;
            this.in = s.getInputStream();
            this.out = new BufferedOutputStream(s.getOutputStream());
        }
    }

    /**
     * Cleanly closes the existing connection and establishes a fresh one.
     */
    public void reconnect() throws IOException {
        synchronized (connectionLock) {
            close();
            connect();
        }
    }

    /**
     * Cleanly closes the socket and underlying streams without throwing exceptions.
     */
    @Override
    public void close() {
        synchronized (connectionLock) {
            closeInternal();
        }
    }

    private void closeInternal() {
        if (in != null) {
            try { in.close(); } catch (Exception ignored) {}
            in = null;
        }
        if (out != null) {
            try { out.close(); } catch (Exception ignored) {}
            out = null;
        }
        if (socket != null) {
            try { socket.close(); } catch (Exception ignored) {}
            socket = null;
        }
    }

    /**
     * Sends a request envelope and waits synchronously for the response envelope (generic JsonNode payload).
     *
     * @param request The request envelope
     * @return Response envelope
     * @throws IOException If fatal non-retryable I/O error occurs
     */
    public MessageEnvelope<JsonNode> sendAndReceive(MessageEnvelope<?> request) throws IOException {
        return sendAndReceive(request, JsonNode.class);
    }

    /**
     * Sends a request envelope and waits synchronously for the response envelope, decoding into {@code responsePayloadClass}.
     *
     * @param request The request envelope
     * @param responsePayloadClass Target response payload class
     * @param <T> Payload type
     * @return Response envelope
     * @throws IOException If fatal non-retryable I/O error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> MessageEnvelope<T> sendAndReceive(MessageEnvelope<?> request, Class<T> responsePayloadClass) throws IOException {
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(responsePayloadClass, "responsePayloadClass must not be null");

        String requestId = ErrorReplyFactory.extractRequestId(request);
        String messageId = request.getMessageId();

        synchronized (rpcLock) {
            // 1. Establish connection with transient retry
            ensureConnectedWithRetry();

            // 2. Perform Send and Receive with Timeout Protection
            byte[] reqBytes = MessageCodec.encode(request);
            byte[] respBytes;
            try {
                synchronized (connectionLock) {
                    FrameCodec.writeFrame(out, reqBytes);
                    respBytes = FrameCodec.readFrame(in);
                }
            } catch (SocketTimeoutException timeoutEx) {
                LOGGER.log(Level.WARNING, "Read/write timeout for messageId=" + messageId
                        + ", requestId=" + requestId + ": " + timeoutEx.getMessage());
                // Safe close poisoned connection
                close();

                // Distributed safety resolution: Attempt ResultLookup before defaulting to UNKNOWN
                return resolveTimeoutOutcome(messageId, requestId, request, responsePayloadClass);
            } catch (IOException ioEx) {
                LOGGER.log(Level.WARNING, "I/O error during communication: " + ioEx.getMessage());
                close();
                if (RetryPolicy.isTimeout(ioEx)) {
                    return resolveTimeoutOutcome(messageId, requestId, request, responsePayloadClass);
                }
                throw ioEx;
            }

            // 3. Decode successful response
            if (responsePayloadClass == JsonNode.class) {
                return (MessageEnvelope<T>) MessageCodec.decode(respBytes);
            }
            return MessageCodec.decode(respBytes, responsePayloadClass);
        }
    }

    /**
     * Establishes connection using configured {@link RetryPolicy} for transient errors.
     */
    private void ensureConnectedWithRetry() throws IOException {
        synchronized (connectionLock) {
            if (isConnected()) {
                return;
            }
            int attempts = 0;
            while (true) {
                try {
                    connect();
                    return;
                } catch (IOException ex) {
                    attempts++;
                    if (!RetryPolicy.isTransientConnectionError(ex) || !retryPolicy.canRetry(attempts - 1)) {
                        closeInternal();
                        throw ex;
                    }
                    long backoff = retryPolicy.computeBackoffMs(attempts - 1);
                    LOGGER.info("Connection attempt " + attempts + " failed (" + ex.getMessage()
                            + "). Retrying in " + backoff + "ms...");
                    if (backoff > 0) {
                        try {
                            Thread.sleep(backoff);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Connection retry interrupted", ie);
                        }
                    }
                }
            }
        }
    }

    /**
     * Resolves a timeout into a definitive outcome via {@link ResultLookup} if available,
     * or defaults safely to {@code UNKNOWN} (SPEC-00 §5.10).
     *
     * <p>CRITICAL: NEVER converts timeout into {@code REJECTED} or {@code BOOK_FAILED}.
     */
    @SuppressWarnings("unchecked")
    private <T> MessageEnvelope<T> resolveTimeoutOutcome(
            String messageId,
            String requestId,
            MessageEnvelope<?> request,
            Class<T> responsePayloadClass) {

        // Step A: Attempt ResultLookup query if available
        if (resultLookup != null && requestId != null) {
            try {
                LOGGER.info("Querying ResultLookup for timed-out requestId: " + requestId);
                ResultLookup.LookupResult lookup = resultLookup.lookup(requestId);
                if (lookup != null) {
                    if (lookup.isSuccess()) {
                        LOGGER.info("Lookup recovered CONFIRMED state for requestId: " + requestId);
                        BookTicketResponseDto confirmDto = new BookTicketResponseDto(
                                requestId,
                                "CONFIRMED",
                                lookup.ticketId(),
                                extractTripId(request),
                                extractSeatId(request),
                                null,
                                lookup.appliedLogIndex()
                        );
                        return packageResponse(messageId, MessageType.BOOK_TICKET_RESPONSE, confirmDto, responsePayloadClass);
                    } else if (lookup.isRejected()) {
                        LOGGER.info("Lookup recovered REJECTED state for requestId: " + requestId);
                        BookTicketResponseDto rejectDto = new BookTicketResponseDto(
                                requestId,
                                "REJECTED",
                                null,
                                extractTripId(request),
                                extractSeatId(request),
                                lookup.reasonCode(),
                                lookup.appliedLogIndex()
                        );
                        return packageResponse(messageId, MessageType.BOOK_TICKET_RESPONSE, rejectDto, responsePayloadClass);
                    }
                }
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING, "ResultLookup query failed for requestId: " + requestId + ": " + ex.getMessage());
            }
        }

        // Step B: Default to UNKNOWN (TIMEOUT_UNKNOWN) preserving original requestId verbatim
        LOGGER.info("Returning UNKNOWN (TIMEOUT_UNKNOWN) for requestId: " + requestId);
        if (request.getType() == MessageType.BOOK_TICKET || responsePayloadClass == BookTicketResponseDto.class) {
            BookTicketResponseDto unknownDto = new BookTicketResponseDto(
                    requestId,
                    "UNKNOWN",
                    null,
                    extractTripId(request),
                    extractSeatId(request),
                    "TIMEOUT_UNKNOWN",
                    null
            );
            return packageResponse(messageId, MessageType.BOOK_TICKET_RESPONSE, unknownDto, responsePayloadClass);
        }

        ErrorResponseDto errorDto = new ErrorResponseDto(
                "TIMEOUT_UNKNOWN",
                "Socket timeout expired while awaiting server response",
                null,
                null,
                requestId
        );
        return packageResponse(messageId, MessageType.ERROR_RESPONSE, errorDto, responsePayloadClass);
    }

    @SuppressWarnings("unchecked")
    private <T> MessageEnvelope<T> packageResponse(
            String correlationId,
            MessageType type,
            Object payload,
            Class<T> targetClass) {

        if (targetClass.isInstance(payload)) {
            return MessageEnvelope.createResponse(correlationId, type, "connection-manager", (T) payload);
        }
        if (targetClass == JsonNode.class) {
            JsonNode tree = MessageCodec.getMapper().valueToTree(payload);
            return (MessageEnvelope<T>) MessageEnvelope.createResponse(correlationId, type, "connection-manager", tree);
        }
        T converted = MessageCodec.getMapper().convertValue(payload, targetClass);
        return MessageEnvelope.createResponse(correlationId, type, "connection-manager", converted);
    }

    private static String extractTripId(MessageEnvelope<?> request) {
        Object payload = request.getPayload();
        if (payload instanceof BookTicketRequestDto dto) {
            return dto.getTripId();
        }
        if (payload instanceof JsonNode json && json.hasNonNull("tripId")) {
            return json.get("tripId").asText();
        }
        return "SE-001";
    }

    private static String extractSeatId(MessageEnvelope<?> request) {
        Object payload = request.getPayload();
        if (payload instanceof BookTicketRequestDto dto) {
            return dto.getSeatId();
        }
        if (payload instanceof JsonNode json && json.hasNonNull("seatId")) {
            return json.get("seatId").asText();
        }
        return null;
    }
}
