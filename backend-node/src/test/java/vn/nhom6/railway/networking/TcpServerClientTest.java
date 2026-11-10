package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TcpServerClientTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    public void cleanup() {
        for (AutoCloseable res : resources) {
            try {
                res.close();
            } catch (Exception ignored) {}
        }
        resources.clear();
    }

    private int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    public void testServerLifecycleAndDynamicPort() throws Exception {
        TcpServer server = new TcpServer("127.0.0.1", 0, 8, 5000);
        resources.add(server);

        assertFalse(server.isRunning(), "Server should not be running before start()");
        assertEquals(-1, server.getPort());

        server.setFrameHandler(payload -> ("PONG:" + new String(payload, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        server.start();

        assertTrue(server.isRunning(), "Server should be running after start()");
        int port = server.getPort();
        assertTrue(port > 0, "Bound port must be positive: " + port);

        TcpClient client = new TcpClient("127.0.0.1", port, 2000, 2000);
        resources.add(client);
        client.connect();
        assertTrue(client.isConnected());

        byte[] reply = client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8));
        assertEquals("PONG:PING", new String(reply, StandardCharsets.UTF_8));

        client.disconnect();
        assertFalse(client.isConnected());

        server.stop();
        assertFalse(server.isRunning());
    }

    @Test
    public void testConnectionTimeout() {
        // Connect to a non-listening port with short timeout
        int nonListeningPort = 49151;
        TcpClient client = new TcpClient("127.0.0.1", nonListeningPort, 200, 200);
        resources.add(client);

        assertThrows(IOException.class, client::connect, "Should throw IOException/ConnectException on non-listening port");
    }

    @Test
    public void testReadTimeout() throws Exception {
        TcpServer server = new TcpServer("127.0.0.1", 0, 4, 5000);
        resources.add(server);
        // Server receives frame but sleeps without replying
        server.setFrameHandler(payload -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
            return payload;
        });
        server.start();

        TcpClient client = new TcpClient("127.0.0.1", server.getPort(), 2000, 300);
        resources.add(client);
        client.connect();

        assertThrows(SocketTimeoutException.class, () -> {
            client.sendAndReceive("HELLO".getBytes(StandardCharsets.UTF_8));
        }, "Should throw SocketTimeoutException when server takes longer than readTimeoutMs");
    }

    @Test
    public void testThreadPoolBoundedAndConcurrentClients() throws Exception {
        int maxThreads = 4;
        TcpServer server = new TcpServer("127.0.0.1", 0, maxThreads, 5000);
        resources.add(server);

        server.setFrameHandler(payload -> ("ACK:" + new String(payload, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        server.start();

        int clientCount = 10;
        int messagesPerClient = 10;
        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        CountDownLatch latch = new CountDownLatch(clientCount);
        AtomicInteger totalSuccess = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            futures.add(executor.submit(() -> {
                try (TcpClient client = new TcpClient("127.0.0.1", server.getPort(), 2000, 5000)) {
                    client.connect();
                    for (int m = 0; m < messagesPerClient; m++) {
                        String msg = "C" + clientId + "-M" + m;
                        byte[] resp = client.sendAndReceive(msg.getBytes(StandardCharsets.UTF_8));
                        if (("ACK:" + msg).equals(new String(resp, StandardCharsets.UTF_8))) {
                            totalSuccess.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }));
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "All client threads should finish within deadline");
        assertEquals(clientCount * messagesPerClient, totalSuccess.get(), "All messages from all concurrent clients must succeed");
    }

    @Test
    public void testTwoNodesExchange100MessagesOnTwoPorts() throws Exception {
        // Node 1 setup on Port 1
        TcpServer server1 = new TcpServer("127.0.0.1", 0, 8, 10_000);
        resources.add(server1);
        server1.setFrameHandler(payload -> ("REPLY_FROM_NODE1:" + new String(payload, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        server1.start();
        int port1 = server1.getPort();

        // Node 2 setup on Port 2
        TcpServer server2 = new TcpServer("127.0.0.1", 0, 8, 10_000);
        resources.add(server2);
        server2.setFrameHandler(payload -> ("REPLY_FROM_NODE2:" + new String(payload, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        server2.start();
        int port2 = server2.getPort();

        assertNotEquals(port1, port2, "Ports must be distinct");

        // Client 1 (at Node 1) connects to Node 2 (port2)
        TcpClient client1 = new TcpClient("127.0.0.1", port2, 2000, 5000);
        resources.add(client1);
        client1.connect();

        // Client 2 (at Node 2) connects to Node 1 (port1)
        TcpClient client2 = new TcpClient("127.0.0.1", port1, 2000, 5000);
        resources.add(client2);
        client2.connect();

        int messageCount = 100;
        // Client 1 sends 100 messages to Node 2
        for (int i = 1; i <= messageCount; i++) {
            String msg = "NODE1_TO_NODE2_" + i;
            byte[] resp = client1.sendAndReceive(msg.getBytes(StandardCharsets.UTF_8));
            assertEquals("REPLY_FROM_NODE2:" + msg, new String(resp, StandardCharsets.UTF_8));
        }

        // Client 2 sends 100 messages to Node 1
        for (int i = 1; i <= messageCount; i++) {
            String msg = "NODE2_TO_NODE1_" + i;
            byte[] resp = client2.sendAndReceive(msg.getBytes(StandardCharsets.UTF_8));
            assertEquals("REPLY_FROM_NODE1:" + msg, new String(resp, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testTwoDistinctJavaProcessesExchange100Messages() throws Exception {
        int messageCount = 100;
        AtomicInteger serverReceived = new AtomicInteger(0);

        // Parent JVM process starts Server 1 on ephemeral port (0)
        TcpServer server1 = new TcpServer("127.0.0.1", 0, 8, 10_000);
        resources.add(server1);
        server1.setFrameHandler(payload -> {
            serverReceived.incrementAndGet();
            String req = new String(payload, StandardCharsets.UTF_8);
            return ("ECHO:" + req).getBytes(StandardCharsets.UTF_8);
        });
        server1.start();
        int port1 = server1.getPort();
        assertTrue(port1 > 0, "Parent server port must be positive: " + port1);

        // Spawn child JVM process running ProcessEchoNode on ephemeral port (0) targeting port1
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "vn.nhom6.railway.networking.ProcessEchoNode",
                "0",
                String.valueOf(port1),
                String.valueOf(messageCount)
        );
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process childProcess = pb.start();

        java.io.BufferedReader stdoutReader = new java.io.BufferedReader(new java.io.InputStreamReader(childProcess.getInputStream()));
        String line;
        int port2 = -1;
        while ((line = stdoutReader.readLine()) != null) {
            if (line.startsWith("READY:")) {
                port2 = Integer.parseInt(line.substring("READY:".length()).trim());
                break;
            }
        }
        assertTrue(port2 > 0, "Child process should report its listening port");
        assertNotEquals(port1, port2, "Parent port and child port must be distinct");

        Thread drainThread = new Thread(() -> {
            try {
                String l;
                while ((l = stdoutReader.readLine()) != null) {
                    System.out.println("[ChildProcess] " + l);
                }
            } catch (IOException ignored) {}
        });
        drainThread.setDaemon(true);
        drainThread.start();

        // Connect from Parent JVM to child process Server on port2
        TcpClient parentClient = null;
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                parentClient = new TcpClient("127.0.0.1", port2, 1000, 5000);
                parentClient.connect();
                break;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }

        assertNotNull(parentClient, "Parent client should be created");
        assertTrue(parentClient.isConnected(), "Parent client must connect to child process server");
        resources.add(parentClient);

        // Parent sends 100 messages to Child process
        for (int i = 1; i <= messageCount; i++) {
            String msg = "PARENT_MSG_" + i;
            byte[] resp = parentClient.sendAndReceive(msg.getBytes(StandardCharsets.UTF_8));
            assertEquals("ECHO:" + msg, new String(resp, StandardCharsets.UTF_8));
        }

        // Wait for child process to finish its 100 sends and exit
        boolean exited = childProcess.waitFor(15, TimeUnit.SECONDS);
        assertTrue(exited, "Child process should terminate within timeout");
        assertEquals(0, childProcess.exitValue(), "Child process must exit with code 0");

        assertEquals(messageCount, serverReceived.get(), "Parent server must receive all 100 messages from child process");
    }

    @Test
    public void testCleanCloseReleasesResources() throws Exception {
        TcpServer server = new TcpServer("127.0.0.1", 0, 4, 1000);
        server.setFrameHandler(payload -> payload);
        server.start();
        int port = server.getPort();
        assertTrue(server.isRunning());

        TcpClient client = new TcpClient("127.0.0.1", port, 1000, 1000);
        client.connect();
        assertTrue(client.isConnected());
        assertEquals("PING", new String(client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        // Stop server
        server.stop();
        assertFalse(server.isRunning());
        assertEquals(0, server.getActiveConnectionCount());

        // Client attempting to communicate now will hit EOFException or IOException
        assertThrows(IOException.class, () -> client.sendAndReceive("PING".getBytes(StandardCharsets.UTF_8)));
        client.close();

        // Connecting to closed port throws IOException/ConnectException
        TcpClient newClient = new TcpClient("127.0.0.1", port, 200, 200);
        assertThrows(IOException.class, newClient::connect);
    }

    public static void main(String[] args) {
        System.out.println("Starting TcpServerClientTest runner...");
        TcpServerClientTest test = new TcpServerClientTest();
        int passed = 0;
        int total = 7;

        try {
            System.out.println("[TEST 1] testServerLifecycleAndDynamicPort...");
            test.testServerLifecycleAndDynamicPort();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 2] testConnectionTimeout...");
            test.testConnectionTimeout();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 3] testReadTimeout...");
            test.testReadTimeout();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 4] testThreadPoolBoundedAndConcurrentClients...");
            test.testThreadPoolBoundedAndConcurrentClients();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 5] testTwoNodesExchange100MessagesOnTwoPorts...");
            test.testTwoNodesExchange100MessagesOnTwoPorts();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 6] testTwoDistinctJavaProcessesExchange100Messages...");
            test.testTwoDistinctJavaProcessesExchange100Messages();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("[TEST 7] testCleanCloseReleasesResources...");
            test.testCleanCloseReleasesResources();
            test.cleanup();
            System.out.println("-> PASS");
            passed++;

            System.out.println("\n=== ALL TCP SERVER/CLIENT TESTS COMPLETED: " + passed + "/" + total + " PASSED ===");
        } catch (Throwable t) {
            System.err.println("Test failed: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }
}
