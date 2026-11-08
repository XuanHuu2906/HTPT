package vn.nhom6.railway.networking;

import vn.nhom6.railway.persistence.DbConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Immutable configuration for a single Railway Distributed Node process.
 *
 * <p>Loads parameters from:
 * <ol>
 *   <li>Properties file (e.g. {@code configs/lan/node-1.properties}).</li>
 *   <li>Environment variables (e.g. {@code NODE_ID}, {@code DB_HOST}, {@code NODE_CLIENT_BIND}).</li>
 *   <li>System properties or sensible developer defaults.</li>
 * </ol>
 */
public record NodeConfig(
        String nodeId,
        String clusterId,
        String clientHost,
        int clientPort,
        String raftHost,
        int raftPort,
        DbConfig dbConfig,
        int maxThreads,
        int soTimeoutMs
) {
    private static final Logger LOGGER = Logger.getLogger(NodeConfig.class.getName());

    public static final String DEFAULT_CLUSTER_ID = "railway-demo-5";
    public static final int DEFAULT_MAX_THREADS = 32;
    public static final int DEFAULT_SO_TIMEOUT_MS = 15_000;

    public NodeConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        clientHost = (clientHost == null || clientHost.isBlank()) ? "0.0.0.0" : clientHost;
        raftHost = (raftHost == null || raftHost.isBlank()) ? "0.0.0.0" : raftHost;
        if (clientPort < 0 || clientPort > 65535) {
            throw new IllegalArgumentException("clientPort out of range [0, 65535]: " + clientPort);
        }
        if (raftPort < 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort out of range [0, 65535]: " + raftPort);
        }
        if (maxThreads <= 0) {
            throw new IllegalArgumentException("maxThreads must be positive: " + maxThreads);
        }
        if (soTimeoutMs < 0) {
            throw new IllegalArgumentException("soTimeoutMs must not be negative: " + soTimeoutMs);
        }
    }

    /**
     * Loads node configuration from an optional file path and falls back to environment variables.
     *
     * @param configFilePath Path to a properties file (may be null or blank)
     * @return Resolved NodeConfig
     * @throws IOException If file is specified but cannot be read
     */
    public static NodeConfig load(String configFilePath) throws IOException {
        Properties props = new Properties();

        if (configFilePath != null && !configFilePath.isBlank()) {
            File file = new File(configFilePath);
            if (!file.exists()) {
                throw new IOException("Configuration file not found: " + configFilePath);
            }
            try (InputStream in = new FileInputStream(file)) {
                props.load(in);
                LOGGER.info(() -> "Loaded configuration from file: " + file.getAbsolutePath());
            }
        }

        return fromProperties(props, System.getenv());
    }

    /**
     * Constructs a {@link NodeConfig} by merging properties and environment variables.
     * Environment variables take precedence over properties.
     */
    public static NodeConfig fromProperties(Properties props, Map<String, String> env) {
        Objects.requireNonNull(props, "props must not be null");
        Objects.requireNonNull(env, "env must not be null");

        // 1. Node ID
        String nodeId = getFirstNonBlank(env.get("NODE_ID"), props.getProperty("node.id"), "node-1");

        // 2. Cluster ID
        String clusterId = getFirstNonBlank(env.get("CLUSTER_ID"), props.getProperty("cluster.id"), DEFAULT_CLUSTER_ID);

        // 3. Client Bind
        String clientHost = getFirstNonBlank(env.get("CLIENT_HOST"), props.getProperty("client.host"), "0.0.0.0");
        int clientPort = parseClientPort(env, props, 9100);

        // 4. Raft Peer Bind
        String raftHost = getFirstNonBlank(env.get("RAFT_HOST"), env.get("PEER_HOST"), props.getProperty("raft.host"), props.getProperty("peer.host"), "0.0.0.0");
        int raftPort = parseRaftPort(env, props, 9200);

        // 5. Database Config
        DbConfig dbConfig = resolveDbConfig(props, env);

        // 6. Max threads & timeout
        int maxThreads = parseInt(getFirstNonBlank(env.get("MAX_THREADS"), props.getProperty("max.threads")), DEFAULT_MAX_THREADS);
        int soTimeoutMs = parseInt(getFirstNonBlank(env.get("SO_TIMEOUT_MS"), props.getProperty("so.timeout.ms")), DEFAULT_SO_TIMEOUT_MS);

        return new NodeConfig(nodeId, clusterId, clientHost, clientPort, raftHost, raftPort, dbConfig, maxThreads, soTimeoutMs);
    }

    private static int parseClientPort(Map<String, String> env, Properties props, int defaultPort) {
        if (env.containsKey("NODE_CLIENT_BIND")) {
            String bind = env.get("NODE_CLIENT_BIND");
            int colon = bind.lastIndexOf(':');
            if (colon >= 0) {
                return Integer.parseInt(bind.substring(colon + 1).trim());
            }
        }
        String portStr = getFirstNonBlank(env.get("CLIENT_PORT"), props.getProperty("client.port"));
        return portStr != null ? Integer.parseInt(portStr.trim()) : defaultPort;
    }

    private static int parseRaftPort(Map<String, String> env, Properties props, int defaultPort) {
        if (env.containsKey("NODE_PEER_BIND")) {
            String bind = env.get("NODE_PEER_BIND");
            int colon = bind.lastIndexOf(':');
            if (colon >= 0) {
                return Integer.parseInt(bind.substring(colon + 1).trim());
            }
        }
        String portStr = getFirstNonBlank(env.get("RAFT_PORT"), env.get("PEER_PORT"), props.getProperty("raft.port"), props.getProperty("peer.port"));
        return portStr != null ? Integer.parseInt(portStr.trim()) : defaultPort;
    }

    private static DbConfig resolveDbConfig(Properties props, Map<String, String> env) {
        String dbHost = getFirstNonBlank(env.get("DB_HOST"), props.getProperty("db.host"));
        String dbPortStr = getFirstNonBlank(env.get("DB_PORT"), props.getProperty("db.port"));
        String dbName = getFirstNonBlank(env.get("DB_NAME"), props.getProperty("db.name"));
        String dbUser = getFirstNonBlank(env.get("DB_USER"), props.getProperty("db.user"));
        String dbPassword = getFirstNonBlank(env.get("DB_PASSWORD"), props.getProperty("db.password"));

        if (dbHost == null || dbName == null || dbUser == null || dbPassword == null) {
            return null; // DB config is not specified
        }

        int dbPort = (dbPortStr != null && !dbPortStr.isBlank()) ? Integer.parseInt(dbPortStr.trim()) : 5432;
        try {
            return new DbConfig(dbHost, dbPort, dbName, dbUser, dbPassword);
        } catch (IllegalArgumentException e) {
            LOGGER.warning(() -> "Skipping invalid DbConfig: " + e.getMessage());
            return null;
        }
    }

    private static String getFirstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v.trim();
            }
        }
        return null;
    }

    private static int parseInt(String str, int defaultValue) {
        if (str == null || str.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(str.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
