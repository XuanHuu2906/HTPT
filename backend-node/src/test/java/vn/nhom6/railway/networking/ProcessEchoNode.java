package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.FrameCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone Java process runner used for multi-process TCP verification.
 *
 * <p>Arguments:
 * <ul>
 *   <li>args[0]: local server port (int)</li>
 *   <li>args[1]: target peer port (int)</li>
 *   <li>args[2]: number of messages to exchange (int)</li>
 * </ul>
 */
public class ProcessEchoNode {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: ProcessEchoNode <localPort> <targetPort> <messageCount>");
            System.exit(1);
        }

        int localPort = Integer.parseInt(args[0]);
        int targetPort = Integer.parseInt(args[1]);
        int messageCount = Integer.parseInt(args[2]);

        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch receivedLatch = new CountDownLatch(messageCount);

        try (TcpServer server = new TcpServer("127.0.0.1", localPort, 16, 10_000)) {
            // Setup echo handler on local server
            server.setFrameHandler(requestPayload -> {
                String reqStr = new String(requestPayload, StandardCharsets.UTF_8);
                int count = receivedCount.incrementAndGet();
                receivedLatch.countDown();
                // Reply with ECHO prefix
                return ("ECHO:" + reqStr).getBytes(StandardCharsets.UTF_8);
            });

            server.start();
            System.out.println("READY:" + server.getPort());
            System.out.flush();
            System.out.println("ProcessEchoNode: Server started on port " + server.getPort());

            // Connect to peer target port with retry loop to give peer time to start
            TcpClient client = null;
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                try {
                    client = new TcpClient("127.0.0.1", targetPort, 1000, 5000);
                    client.connect();
                    break;
                } catch (IOException e) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (client == null || !client.isConnected()) {
                System.err.println("ProcessEchoNode: Failed to connect to target port " + targetPort);
                System.exit(2);
            }

            final TcpClient activeClient = client;
            try (activeClient) {
                System.out.println("ProcessEchoNode: Connected to target port " + targetPort + ", sending " + messageCount + " messages...");
                for (int i = 1; i <= messageCount; i++) {
                    String msg = "MSG-FROM-SUBPROCESS-" + i;
                    byte[] response = client.sendAndReceive(msg.getBytes(StandardCharsets.UTF_8));
                    String respStr = new String(response, StandardCharsets.UTF_8);
                    if (!respStr.equals("ECHO:" + msg)) {
                        System.err.println("ProcessEchoNode: Unexpected response: " + respStr + " for msg: " + msg);
                        System.exit(3);
                    }
                }
                System.out.println("ProcessEchoNode: Successfully sent " + messageCount + " messages and verified responses.");

                // Wait for peer to send all its messages to our local server
                boolean allReceived = receivedLatch.await(10, TimeUnit.SECONDS);
                if (!allReceived) {
                    System.err.println("ProcessEchoNode: Timed out waiting for peer messages. Received: " + receivedCount.get() + "/" + messageCount);
                    System.exit(4);
                }
                System.out.println("ProcessEchoNode: All " + messageCount + " messages from peer successfully processed.");
            }

            System.out.println("ProcessEchoNode: Exiting with success (code 0).");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(99);
        }
    }
}
