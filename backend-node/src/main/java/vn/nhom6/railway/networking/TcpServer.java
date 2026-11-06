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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-pooled TCP Socket Server for Node and Peer communication.
 *
 * <p>Features:
 * <ul>
 *   <li>Non-blocking accept loop running on a dedicated acceptor thread.</li>
 *   <li>Bounded thread pool to prevent thread exhaustion and memory overflow.</li>
 *   <li>Disables Nagle's algorithm ({@code TCP_NODELAY = true}) for low-latency RPC.</li>
 *   <li>Configurable socket read timeout ({@code SO_TIMEOUT}).</li>
 *   <li>Serialized socket writer per client socket per SPEC-03 Section 2.</li>
 *   <li>Graceful resource management ({@code AutoCloseable}) releasing all client sockets and thread pool on stop.</li>
 *   <li>Clean handling of EOF without crashing or throwing spurious exceptions.</li>
 * </ul>
 */
public class TcpServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(TcpServer.class.getName());

    public static final int DEFAULT_MAX_THREADS = 32;
    public static final int DEFAULT_SO_TIMEOUT_MS = 15_000;

    @FunctionalInterface
    public interface MessageHandler {
        /**
         * Handles a decoded {@link MessageEnvelope} and returns a response envelope,
         * or {@code null} if no response is required.
         *
         * @param request Decoded request envelope with generic JsonNode payload
         * @return Response envelope or null
         */
        MessageEnvelope<?> handle(MessageEnvelope<JsonNode> request);
    }

    @FunctionalInterface
    public interface FrameHandler {
        /**
         * Handles raw frame payload bytes and returns response frame payload bytes,
         * or {@code null} if no response is required.
         *
         * @param requestPayload Frame payload bytes
         * @return Response frame payload bytes or null
         */
        byte[] handle(byte[] requestPayload);
    }

    private final String host;
    private final int requestedPort;
    private final int maxThreads;
    private final int soTimeoutMs;

    private volatile MessageHandler messageHandler;
    private volatile FrameHandler frameHandler;

    private ServerSocket serverSocket;
    private ExecutorService workerPool;
    private Thread acceptThread;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<Socket> activeSockets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile int boundPort = -1;

    public TcpServer(int port) {
        this(null, port, DEFAULT_MAX_THREADS, DEFAULT_SO_TIMEOUT_MS);
    }

    public TcpServer(String host, int port) {
        this(host, port, DEFAULT_MAX_THREADS, DEFAULT_SO_TIMEOUT_MS);
    }

    public TcpServer(String host, int port, int maxThreads, int soTimeoutMs) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port out of bounds [0, 65535]: " + port);
        }
        if (maxThreads <= 0) {
            throw new IllegalArgumentException("maxThreads must be positive: " + maxThreads);
        }
        if (soTimeoutMs < 0) {
            throw new IllegalArgumentException("soTimeoutMs must not be negative: " + soTimeoutMs);
        }
        this.host = host;
        this.requestedPort = port;
        this.maxThreads = maxThreads;
        this.soTimeoutMs = soTimeoutMs;
    }

    public TcpServer setMessageHandler(MessageHandler handler) {
        this.messageHandler = handler;
        return this;
    }

    public TcpServer setFrameHandler(FrameHandler handler) {
        this.frameHandler = handler;
        return this;
    }

    /**
     * Starts the TCP server socket and the accept loop.
     *
     * @throws IOException If binding fails
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        if (host != null && !host.isBlank()) {
            serverSocket.bind(new InetSocketAddress(host, requestedPort));
        } else {
            serverSocket.bind(new InetSocketAddress(requestedPort));
        }

        boundPort = serverSocket.getLocalPort();
        running.set(true);

        workerPool = Executors.newFixedThreadPool(maxThreads, new ServerThreadFactory("tcp-worker-" + boundPort));

        acceptThread = new Thread(this::acceptLoop, "tcp-acceptor-" + boundPort);
        acceptThread.setDaemon(true);
        acceptThread.start();

        LOGGER.info(() -> "TcpServer started on port " + boundPort + " (maxThreads=" + maxThreads + ", soTimeout=" + soTimeoutMs + "ms)");
    }

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                if (!running.get()) {
                    closeSilently(clientSocket);
                    break;
                }

                clientSocket.setTcpNoDelay(true);
                if (soTimeoutMs > 0) {
                    clientSocket.setSoTimeout(soTimeoutMs);
                }

                activeSockets.add(clientSocket);

                try {
                    workerPool.submit(() -> handleClient(clientSocket));
                } catch (Exception rejected) {
                    // Pool shutting down or queue exhausted
                    activeSockets.remove(clientSocket);
                    closeSilently(clientSocket);
                }
            } catch (SocketException se) {
                if (!running.get()) {
                    break; // Expected when serverSocket.close() is called
                }
                LOGGER.log(Level.WARNING, "SocketException in TcpServer accept loop", se);
            } catch (IOException ioe) {
                if (!running.get()) {
                    break;
                }
                LOGGER.log(Level.WARNING, "IOException in TcpServer accept loop", ioe);
            }
        }
    }

    private void handleClient(Socket socket) {
        final Object writeLock = new Object();
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());

            while (running.get() && !socket.isClosed()) {
                byte[] framePayload;
                try {
                    framePayload = FrameCodec.readFrame(in);
                } catch (EOFException eof) {
                    // Clean EOF: client closed socket gracefully
                    break;
                } catch (SocketException se) {
                    // Client disconnected or connection reset
                    break;
                } catch (IllegalArgumentException malformed) {
                    // Framing bounds violated (length > 1 MiB or <= 0): close per SPEC-03 Section 2
                    LOGGER.log(Level.WARNING, "Malformed frame received from " + socket.getRemoteSocketAddress() + ": " + malformed.getMessage());
                    break;
                }

                if (framePayload == null) {
                    break;
                }

                if (frameHandler != null) {
                    byte[] responseBytes = frameHandler.handle(framePayload);
                    if (responseBytes != null) {
                        synchronized (writeLock) {
                            FrameCodec.writeFrame(out, responseBytes);
                        }
                    }
                } else if (messageHandler != null) {
                    try {
                        MessageEnvelope<JsonNode> requestEnvelope = MessageCodec.decode(framePayload);
                        MessageEnvelope<?> responseEnvelope = messageHandler.handle(requestEnvelope);
                        if (responseEnvelope != null) {
                            synchronized (writeLock) {
                                FrameCodec.writeMessage(out, responseEnvelope);
                            }
                        }
                    } catch (Exception ex) {
                        LOGGER.log(Level.WARNING, "Failed to process message envelope: " + ex.getMessage(), ex);
                        // Malformed envelope: close connection per SPEC-03 Section 2
                        break;
                    }
                }
            }
        } catch (IOException ioe) {
            if (running.get()) {
                LOGGER.log(Level.FINE, "Client socket I/O error from " + socket.getRemoteSocketAddress() + ": " + ioe.getMessage());
            }
        } finally {
            activeSockets.remove(socket);
            closeSilently(socket);
        }
    }

    /**
     * Returns the port the server is bound to, or -1 if not started.
     */
    public int getPort() {
        return boundPort;
    }

    /**
     * Checks if the server is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Returns the current number of active client connections.
     */
    public int getActiveConnectionCount() {
        return activeSockets.size();
    }

    /**
     * Stops the server, closes all active client connections, and terminates the thread pool.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 1. Close ServerSocket to unblock accept()
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }

        // 2. Close all active client sockets
        for (Socket socket : activeSockets) {
            closeSilently(socket);
        }
        activeSockets.clear();

        // 3. Shutdown thread pool
        if (workerPool != null) {
            workerPool.shutdownNow();
            try {
                if (!workerPool.awaitTermination(2, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        // 4. Wait for acceptor thread
        if (acceptThread != null && acceptThread.isAlive()) {
            try {
                acceptThread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        LOGGER.info(() -> "TcpServer on port " + boundPort + " stopped.");
    }

    @Override
    public void close() {
        stop();
    }

    private static void closeSilently(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private static class ServerThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        ServerThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
