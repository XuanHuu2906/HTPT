package vn.nhom6.railway.networking;

import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main application lifecycle entry point for a Railway Distributed Backend Node.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Loads and validates {@link NodeConfig} from properties or environment.</li>
 *   <li>Initializes and starts Client TCP Server (for Desktop Client and Monitor).</li>
 *   <li>Initializes and starts Raft Peer TCP Server (for Node-to-Node consensus RPCs).</li>
 *   <li>Enforces channel and port isolation via dedicated {@link MessageDispatcher} instances.</li>
 *   <li>Provides baseline diagnostic handlers for {@code PING} (raw frame) and {@code NODE_STATUS}.</li>
 *   <li>Connects to local PostgreSQL instance via TV2's {@link ConnectionFactory} when configured.</li>
 *   <li>Registers clean shutdown hook to release sockets and ports gracefully.</li>
 * </ul>
 */
public class NodeMain implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(NodeMain.class.getName());
    public static final String APP_VERSION = "1.0.0-SNAPSHOT";

    private final NodeConfig config;
    private final HandlerRegistry clientRegistry;
    private final HandlerRegistry raftRegistry;
    private final MessageDispatcher clientDispatcher;
    private final MessageDispatcher raftDispatcher;
    private final TcpServer clientServer;
    private final TcpServer raftServer;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread shutdownHook;

    public NodeMain(NodeConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");

        this.clientRegistry = new HandlerRegistry();
        this.raftRegistry = new HandlerRegistry();

        this.clientDispatcher = new MessageDispatcher(clientRegistry, MessageDispatcher.ChannelType.CLIENT, config.nodeId());
        this.raftDispatcher = new MessageDispatcher(raftRegistry, MessageDispatcher.ChannelType.PEER, config.nodeId());

        this.clientServer = new TcpServer(config.clientHost(), config.clientPort(), config.maxThreads(), config.soTimeoutMs());
        this.clientServer.setMessageHandler(clientDispatcher);

        this.raftServer = new TcpServer(config.raftHost(), config.raftPort(), config.maxThreads(), config.soTimeoutMs());
        this.raftServer.setMessageHandler(raftDispatcher);

        configureDefaultHandlers();
    }

    private void configureDefaultHandlers() {
        // 1. Raw Frame Handler on Client Server: PING -> PONG
        clientServer.setFrameHandler(payload -> {
            if (payload != null && payload.length == 4
                    && payload[0] == 'P' && payload[1] == 'I' && payload[2] == 'N' && payload[3] == 'G') {
                return "PONG".getBytes(StandardCharsets.UTF_8);
            }
            return null; // Fallthrough to MessageDispatcher
        });

        // 2. Status Handler on Client Server: NODE_STATUS -> NODE_STATUS_RESPONSE (SPEC-03 §5.1)
        clientRegistry.registerStatusHandler(request -> {
            Map<String, Object> statusPayload = new LinkedHashMap<>();
            statusPayload.put("nodeId", config.nodeId());
            statusPayload.put("role", "FOLLOWER");
            statusPayload.put("currentTerm", 0L);
            statusPayload.put("commitIndex", 0L);
            statusPayload.put("lastApplied", 0L);
            statusPayload.put("storageHealthy", config.dbConfig() != null);
            statusPayload.put("peerConnectivity", Map.of(
                    "node-2", false,
                    "node-3", false,
                    "node-4", false,
                    "node-5", false
            ));
            statusPayload.put("observedAt", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

            return MessageEnvelope.createResponse(
                    request.getMessageId(),
                    MessageType.NODE_STATUS_RESPONSE,
                    config.nodeId(),
                    statusPayload
            );
        });
    }

    /**
     * Starts the backend node runtime, binding servers and testing database connectivity.
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        printBanner();

        // 1. Start Client TCP Server
        clientServer.start();
        LOGGER.info(() -> "[" + config.nodeId() + "] Client TCP Server bound to " + config.clientHost() + ":" + clientServer.getPort());

        // 2. Start Raft Peer TCP Server
        raftServer.start();
        LOGGER.info(() -> "[" + config.nodeId() + "] Raft Peer TCP Server bound to " + config.raftHost() + ":" + raftServer.getPort());

        System.out.println("[READY] " + config.nodeId() + " client=" + clientServer.getPort() + " raft=" + raftServer.getPort());
        System.out.flush();

        // 3. Test Database Connectivity (optional / non-blocking)
        if (config.dbConfig() != null) {
            checkDatabaseConnection();
        } else {
            LOGGER.info(() -> "[" + config.nodeId() + "] Running without PostgreSQL configuration (in-memory mode).");
        }

        // 4. Register Shutdown Hook
        shutdownHook = new Thread(this::stop, "node-shutdown-hook-" + config.nodeId());
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        running.set(true);
        LOGGER.info(() -> "Backend Node [" + config.nodeId() + "] initialized and ready for traffic.");
    }

    private void checkDatabaseConnection() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory(config.dbConfig(), 2, 2);
            try (Connection conn = connectionFactory.getConnection()) {
                LOGGER.info(() -> "[" + config.nodeId() + "] Database connection verified: " + config.dbConfig().jdbcUrl());
            }
        } catch (Exception e) {
            LOGGER.warning(() -> "[" + config.nodeId() + "] Database check failed for " + config.dbConfig().jdbcUrl() + ": " + e.getMessage() + ". Continuing node startup.");
        }
    }

    private void printBanner() {
        System.out.println("================================================================================");
        System.out.println(" RAILWAY DISTRIBUTED SYSTEM - BACKEND NODE RUNTIME");
        System.out.println("================================================================================");
        System.out.println("  Node ID          : " + config.nodeId());
        System.out.println("  Cluster ID       : " + config.clusterId());
        System.out.println("  Client Port      : " + config.clientPort() + " (Host: " + config.clientHost() + ")");
        System.out.println("  Raft Peer Port   : " + config.raftPort() + " (Host: " + config.raftHost() + ")");
        if (config.dbConfig() != null) {
            System.out.println("  Database Target  : " + config.dbConfig().user() + "@" + config.dbConfig().host() + ":" + config.dbConfig().port() + "/" + config.dbConfig().dbName());
        } else {
            System.out.println("  Database Target  : None (In-Memory / Test Mode)");
        }
        System.out.println("================================================================================");
    }

    /**
     * Stops the backend node, releasing network servers and sockets cleanly.
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        LOGGER.info(() -> "Shutting down Backend Node [" + config.nodeId() + "]...");

        // Stop network servers
        clientServer.stop();
        raftServer.stop();

        // Deregister shutdown hook if called programmatically
        if (shutdownHook != null && Thread.currentThread() != shutdownHook) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {}
        }

        LOGGER.info(() -> "Backend Node [" + config.nodeId() + "] shutdown cleanly completed.");
    }

    @Override
    public void close() {
        stop();
    }

    public NodeConfig getConfig() {
        return config;
    }

    public int getClientPort() {
        return clientServer.getPort();
    }

    public int getRaftPort() {
        return raftServer.getPort();
    }

    public boolean isRunning() {
        return running.get();
    }

    public HandlerRegistry getClientRegistry() {
        return clientRegistry;
    }

    public HandlerRegistry getRaftRegistry() {
        return raftRegistry;
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            String firstArg = args[0].trim();
            if ("--help".equalsIgnoreCase(firstArg) || "-h".equalsIgnoreCase(firstArg)) {
                System.out.println("Usage: NodeMain [<config-properties-file>]");
                System.out.println("Options:");
                System.out.println("  -h, --help     Show this help message");
                System.out.println("  -v, --version  Show application version");
                System.exit(0);
            }
            if ("--version".equalsIgnoreCase(firstArg) || "-v".equalsIgnoreCase(firstArg)) {
                System.out.println("Railway Distributed Backend Node v" + APP_VERSION);
                System.exit(0);
            }
        }

        String configPath = args.length > 0 ? args[0] : null;
        try {
            NodeConfig config = NodeConfig.load(configPath);
            NodeMain node = new NodeMain(config);
            node.start();

            // Keep main process thread alive
            Thread.currentThread().join();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.info("Main thread interrupted, exiting.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Fatal error during NodeMain startup: " + e.getMessage(), e);
            System.exit(1);
        }
    }
}
