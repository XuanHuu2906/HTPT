package vn.nhom6.railway.client.network;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Danh bạ ánh xạ các Node trong cụm phân tán (nodeId -> {@link NodeEndpoint}) theo TASK TV1-D12.
 * <p>
 * Nạp từ file cấu hình {@code configs/dev/cluster.properties} của TV3 hoặc cấu hình tùy chỉnh.
 * Tuyệt đối không hardcode host/port cố định trong logic nghiệp vụ.
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
            String host = props.getProperty("node." + id + ".client.host", "127.0.0.1").trim();
            String portStr = props.getProperty("node." + id + ".client.port");
            int port = 9101;
            if (portStr != null && !portStr.isBlank()) {
                try {
                    port = Integer.parseInt(portStr.trim());
                } catch (NumberFormatException ignored) {}
            }
            map.put(id, new NodeEndpoint(id, host, port));
        }

        if (map.isEmpty()) {
            // Fallback cấu hình mặc định 5 node chuẩn theo SPEC-08
            map.put("node-1", new NodeEndpoint("node-1", "127.0.0.1", 9101));
            map.put("node-2", new NodeEndpoint("node-2", "127.0.0.1", 9102));
            map.put("node-3", new NodeEndpoint("node-3", "127.0.0.1", 9103));
            map.put("node-4", new NodeEndpoint("node-4", "127.0.0.1", 9104));
            map.put("node-5", new NodeEndpoint("node-5", "127.0.0.1", 9105));
        }

        String defaultNode = props.getProperty("cluster.default-node", nodeIds.isEmpty() ? "node-1" : nodeIds.get(0));
        return new ClusterEndpoints(map, defaultNode);
    }

    /**
     * Nạp danh bạ từ file.
     */
    public static ClusterEndpoints loadFromFile(File file) throws IOException {
        Objects.requireNonNull(file, "file không được null");
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            props.load(in);
        }
        return loadFromProperties(props);
    }

    /**
     * Nạp cấu hình mặc định của hệ thống:
     * 1. System property: railway.cluster.config
     * 2. File: configs/dev/cluster.properties
     * 3. File: ../configs/dev/cluster.properties
     * 4. Resource: /cluster.properties
     * 5. Mặc định 5 node (127.0.0.1:9101..9105)
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

        File devConfig = new File("configs/dev/cluster.properties");
        if (devConfig.exists() && devConfig.isFile()) {
            try {
                return loadFromFile(devConfig);
            } catch (IOException ignored) {}
        }

        File parentDevConfig = new File("../configs/dev/cluster.properties");
        if (parentDevConfig.exists() && parentDevConfig.isFile()) {
            try {
                return loadFromFile(parentDevConfig);
            } catch (IOException ignored) {}
        }

        try (InputStream res = ClusterEndpoints.class.getResourceAsStream("/cluster.properties")) {
            if (res != null) {
                Properties props = new Properties();
                props.load(res);
                return loadFromProperties(props);
            }
        } catch (IOException ignored) {}

        // Fallback chuẩn 5 node theo SPEC-08
        Properties fallbackProps = new Properties();
        fallbackProps.setProperty("cluster.nodes", "node-1,node-2,node-3,node-4,node-5");
        fallbackProps.setProperty("node.node-1.client.host", "127.0.0.1");
        fallbackProps.setProperty("node.node-1.client.port", "9101");
        fallbackProps.setProperty("node.node-2.client.host", "127.0.0.1");
        fallbackProps.setProperty("node.node-2.client.port", "9102");
        fallbackProps.setProperty("node.node-3.client.host", "127.0.0.1");
        fallbackProps.setProperty("node.node-3.client.port", "9103");
        fallbackProps.setProperty("node.node-4.client.host", "127.0.0.1");
        fallbackProps.setProperty("node.node-4.client.port", "9104");
        fallbackProps.setProperty("node.node-5.client.host", "127.0.0.1");
        fallbackProps.setProperty("node.node-5.client.port", "9105");
        return loadFromProperties(fallbackProps);
    }
}
