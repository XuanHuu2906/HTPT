package vn.nhom6.railway.client.network;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Danh bạ ánh xạ các Node trong cụm phân tán (nodeId -> {@link NodeEndpoint}) theo TASK TV1-D12 & TV1-D16.
 * <p>
 * Nạp từ nguồn cấu hình duy nhất {@link ClientConfig} (client.properties) hoặc cấu hình tùy chỉnh.
 * Tuyệt đối không hardcode host/port/nodeId cố định trong logic nghiệp vụ.
 */
public class ClusterEndpoints {

    private final Map<String, NodeEndpoint> endpointMap;
    private final String defaultNodeId;

    public ClusterEndpoints(Map<String, NodeEndpoint> endpointMap, String defaultNodeId) {
        if (endpointMap == null || endpointMap.isEmpty()) {
            throw new IllegalArgumentException("endpointMap không được null hoặc rỗng");
        }
        this.endpointMap = Collections.unmodifiableMap(new LinkedHashMap<>(endpointMap));
        if (defaultNodeId != null && !defaultNodeId.isBlank() && this.endpointMap.containsKey(defaultNodeId.trim())) {
            this.defaultNodeId = defaultNodeId.trim();
        } else {
            this.defaultNodeId = this.endpointMap.keySet().iterator().next();
        }
    }

    public static ClusterEndpoints fromEndpoints(Collection<NodeEndpoint> endpoints, String defaultNodeId) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints không được rỗng");
        }
        Map<String, NodeEndpoint> map = new LinkedHashMap<>();
        for (NodeEndpoint ep : endpoints) {
            if (ep != null) {
                map.put(ep.nodeId(), ep);
            }
        }
        return new ClusterEndpoints(map, defaultNodeId);
    }

    public static ClusterEndpoints fromMap(Map<String, NodeEndpoint> map, String defaultNodeId) {
        return new ClusterEndpoints(map, defaultNodeId);
    }

    /**
     * Tạo {@link ClusterEndpoints} từ đối tượng cấu hình chuẩn {@link ClientConfig} (TASK TV1-D16).
     */
    public static ClusterEndpoints fromClientConfig(ClientConfig config) {
        Objects.requireNonNull(config, "config không được null");
        return fromEndpoints(config.getEndpoints(), config.getPreferredNode());
    }

    /**
     * Tạo {@link ClusterEndpoints} liên kết trực tiếp với {@link ClusterNodeSelector} (TASK TV1-D16).
     */
    public static ClusterEndpoints fromSelector(ClusterNodeSelector selector) {
        Objects.requireNonNull(selector, "selector không được null");
        return fromClientConfig(selector.getConfig());
    }

    public NodeEndpoint getEndpoint(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return null;
        }
        return endpointMap.get(nodeId.trim());
    }

    public boolean containsNode(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return false;
        }
        return endpointMap.containsKey(nodeId.trim());
    }

    public String getDefaultNodeId() {
        return defaultNodeId;
    }

    public NodeEndpoint getDefaultEndpoint() {
        return endpointMap.get(defaultNodeId);
    }

    public List<String> getAllNodeIds() {
        return new ArrayList<>(endpointMap.keySet());
    }

    public List<NodeEndpoint> getAllEndpoints() {
        return new ArrayList<>(endpointMap.values());
    }

    public int size() {
        return endpointMap.size();
    }

    /**
     * Nạp danh bạ từ đối tượng {@link Properties}.
     * Hỗ trợ định dạng key của TV3 (configs/dev/cluster.properties):
     * <ul>
     *   <li>{@code cluster.nodes=node-1,node-2,node-3,node-4,node-5}</li>
     *   <li>{@code node.<nodeId>.client.host=127.0.0.1}</li>
     *   <li>{@code node.<nodeId>.client.port=9101}</li>
     * </ul>
     */
    public static ClusterEndpoints loadFromProperties(Properties props) {
        Objects.requireNonNull(props, "props không được null");
        Map<String, NodeEndpoint> map = new LinkedHashMap<>();

        String nodesConfig = props.getProperty("cluster.nodes");
        List<String> nodeIds = new ArrayList<>();
        if (nodesConfig != null && !nodesConfig.isBlank()) {
            for (String n : nodesConfig.split(",")) {
                String trimmed = n.trim();
                if (!trimmed.isEmpty()) {
                    nodeIds.add(trimmed);
                }
            }
        }

        // Nếu không có cluster.nodes thì tự dò các key dạng node.<id>.client.host
        if (nodeIds.isEmpty()) {
            for (String key : props.stringPropertyNames()) {
                if (key.startsWith("node.") && key.endsWith(".client.host")) {
                    String[] parts = key.split("\\.");
                    if (parts.length >= 4) {
                        String id = parts[1].trim();
                        if (!nodeIds.contains(id)) {
                            nodeIds.add(id);
                        }
                    }
                }
            }
        }

        for (String id : nodeIds) {
            String host = props.getProperty("node." + id + ".client.host");
            if (host == null || host.isBlank()) {
                host = props.getProperty(id + ".client.host");
            }
            if (host == null || host.isBlank()) {
                continue;
            }
            host = host.trim();

            String portStr = props.getProperty("node." + id + ".client.port");
            if (portStr == null || portStr.isBlank()) {
                portStr = props.getProperty(id + ".client.port");
            }
            int port = 0;
            if (portStr != null && !portStr.isBlank()) {
                try {
                    port = Integer.parseInt(portStr.trim());
                } catch (NumberFormatException ignored) {}
            }
            if (port > 0 && port <= 65535) {
                map.put(id, new NodeEndpoint(id, host, port));
            }
        }

        if (map.isEmpty()) {
            // Nạp từ ClientConfig mặc định nếu Properties rỗng
            return fromClientConfig(ClientConfig.loadDefault());
        }

        String defaultNode = props.getProperty("cluster.default-node", nodeIds.isEmpty() ? map.keySet().iterator().next() : nodeIds.get(0));
        return new ClusterEndpoints(map, defaultNode);
    }

    /**
     * Nạp danh bạ từ file với mã hóa UTF-8.
     */
    public static ClusterEndpoints loadFromFile(File file) throws IOException {
        Objects.requireNonNull(file, "file không được null");
        Path path = file.toPath();
        Properties props = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            props.load(reader);
        }
        return loadFromProperties(props);
    }

    /**
     * Nạp cấu hình mặc định của hệ thống:
     * Gom về một nguồn duy nhất qua {@link ClientConfig#loadDefault()}.
     */
    public static ClusterEndpoints loadDefault() {
        String customPath = System.getProperty("railway.cluster.config");
        if (customPath != null && !customPath.isBlank()) {
            File f = new File(customPath);
            if (f.exists() && f.isFile()) {
                try {
                    return loadFromFile(f);
                } catch (IOException ignored) {}
            }
        }

        try {
            ClientConfig config = ClientConfig.loadDefault();
            return fromClientConfig(config);
        } catch (Exception ignored) {
            // Fallback an toàn nếu có ngoại lệ bất thường
            Properties fallback = new Properties();
            try (InputStream res = ClusterEndpoints.class.getResourceAsStream("/config/client.properties")) {
                if (res != null) {
                    try (BufferedReader r = new BufferedReader(new InputStreamReader(res, StandardCharsets.UTF_8))) {
                        fallback.load(r);
                        return loadFromProperties(fallback);
                    }
                }
            } catch (IOException ignored2) {}
            throw new IllegalStateException("Không thể nạp danh bạ cụm từ bất kỳ nguồn cấu hình nào");
        }
    }
}
