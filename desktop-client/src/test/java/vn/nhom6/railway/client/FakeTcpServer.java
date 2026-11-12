package vn.nhom6.railway.client;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Fake TCP Server trong cùng JVM phục vụ test (FAKE SERVER, không phải Node thật).
 * <p>
 * Phục vụ các kiểm thử giao tiếp mạng của {@link vn.nhom6.railway.client.network.SocketBookingClient}.
 */
public class FakeTcpServer implements AutoCloseable {

    @FunctionalInterface
    public interface CustomResponseSender {
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

