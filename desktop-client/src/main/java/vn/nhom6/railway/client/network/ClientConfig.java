package vn.nhom6.railway.client.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Quản lý nạp và thẩm tra (validation fail-fast) cấu hình kết nối cụm của Desktop Client (TASK TV1-D16).
 * <p>
 * Thứ tự ưu tiên nạp cấu hình:
 * <ol>
 *   <li>System properties ({@code -Dclient.config=<path>} và từng key {@code -Dcluster.profile=...}).</li>
 *   <li>Biến môi trường ({@code CLIENT_CONFIG}, {@code CLUSTER_PROFILE},...).</li>
 *   <li>File {@code client.properties} tại thư mục làm việc hiện tại.</li>
 *   <li>File mặc định trong classpath ({@code /config/client.properties}).</li>
 * </ol>
 * <p>
 * Tuyệt đối không hardcode Leader hay Docker hostname trong Java.
 */
public final class ClientConfig {

    private static final System.Logger LOGGER = System.getLogger(ClientConfig.class.getName());

    public static final String KEY_CLUSTER_PROFILE = "cluster.profile";
    public static final String KEY_CLIENT_MODE = "client.mode";
    public static final String KEY_RELEASE_MODE = "release.mode";
    public static final String KEY_RELEASE_VERSION = "release.version";
    public static final String KEY_CONNECT_TIMEOUT_MS = "client.connectTimeoutMs";
    public static final String KEY_READ_TIMEOUT_MS = "client.readTimeoutMs";
    public static final String KEY_MAX_REDIRECTS = "client.maxRedirects";
    public static final String KEY_PREFERRED_NODE = "client.preferredNode";
    public static final String KEY_SELECTION_STRATEGY = "client.selectionStrategy";
    public static final String KEY_FAILURE_COOLDOWN_MS = "client.failureCooldownMs";

    public static final String DEFAULT_PROFILE = "dev";
    public static final String DEFAULT_RELEASE_VERSION = "1.0.0-rc1";
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 2000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 5000;
    public static final int DEFAULT_MAX_REDIRECTS = 3;
    public static final long DEFAULT_FAILURE_COOLDOWN_MS = 15000L;
    public static final SelectionStrategy DEFAULT_STRATEGY = SelectionStrategy.ROUND_ROBIN;

    private static final Pattern DOCKER_SERVICE_PATTERN = Pattern.compile(
            "^(backend-node\\d*|pg\\d*|postgres(-\\d+)?|database)$", Pattern.CASE_INSENSITIVE
    );

    private final String profile;
    private final String clientMode;
    private final boolean releaseMode;
    private final String releaseVersion;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int maxRedirects;
    private final String preferredNode;
    private final SelectionStrategy selectionStrategy;
    private final long failureCooldownMs;
    private final List<NodeEndpoint> endpoints;
    private final Map<String, NodeEndpoint> endpointMap;
    private final Properties rawProperties;
    private final String sourceDescription;

    public ClientConfig(
            String profile,
            int connectTimeoutMs,
            int readTimeoutMs,
            int maxRedirects,
            String preferredNode,
            SelectionStrategy selectionStrategy,
            long failureCooldownMs,
            List<NodeEndpoint> endpoints,
            Properties rawProperties,
            String sourceDescription) {
        this.profile = Objects.requireNonNull(profile, "profile không được null").trim();
        this.rawProperties = (rawProperties != null) ? (Properties) rawProperties.clone() : new Properties();
        this.clientMode = this.rawProperties.getProperty(KEY_CLIENT_MODE, "socket").trim().toLowerCase();
        this.releaseMode = Boolean.parseBoolean(this.rawProperties.getProperty(KEY_RELEASE_MODE, "false").trim());
        String ver = this.rawProperties.getProperty(KEY_RELEASE_VERSION, DEFAULT_RELEASE_VERSION).trim();
        this.releaseVersion = ver.isEmpty() ? DEFAULT_RELEASE_VERSION : ver;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.maxRedirects = maxRedirects;
        this.preferredNode = (preferredNode != null && !preferredNode.isBlank()) ? preferredNode.trim() : null;
        this.selectionStrategy = (selectionStrategy != null) ? selectionStrategy : DEFAULT_STRATEGY;
        this.failureCooldownMs = failureCooldownMs;
        this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
        Map<String, NodeEndpoint> map = new LinkedHashMap<>();
        for (NodeEndpoint ep : endpoints) {
            map.put(ep.nodeId(), ep);
        }
        this.endpointMap = Collections.unmodifiableMap(map);
        this.sourceDescription = sourceDescription != null ? sourceDescription : "unknown";
    }

    public String getClientMode() {
        return clientMode;
    }

    public boolean isSocketMode() {
        return releaseMode
                || "socket".equalsIgnoreCase(clientMode)
                || "tcp".equalsIgnoreCase(clientMode)
                || "real".equalsIgnoreCase(clientMode);
    }

    public boolean isReleaseMode() {
        return releaseMode;
    }

    public String getReleaseVersion() {
        return releaseVersion;
    }

    /**
     * Kiểm tra nhanh xem cấu hình hệ thống hiện hành có đang bật chế độ RELEASE hay không.
     */
    public static boolean isReleaseModeActive() {
        String sysVal = System.getProperty(KEY_RELEASE_MODE);
        if (sysVal != null && !sysVal.isBlank()) {
            return Boolean.parseBoolean(sysVal.trim());
        }
        String envVal = System.getenv("RELEASE_MODE");
        if (envVal != null && !envVal.isBlank()) {
            return Boolean.parseBoolean(envVal.trim());
        }
        try (InputStream is = openClasspathResource("/config/release.properties")) {
            if (is != null) {
                Properties p = new Properties();
                try (Reader r = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                    p.load(r);
                }
                return Boolean.parseBoolean(p.getProperty(KEY_RELEASE_MODE, "false").trim());
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    public String getProfile() {
        return profile;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }

    public String getPreferredNode() {
        return preferredNode;
    }

    public SelectionStrategy getSelectionStrategy() {
        return selectionStrategy;
    }

    public long getFailureCooldownMs() {
        return failureCooldownMs;
    }

    public List<NodeEndpoint> getEndpoints() {
        return endpoints;
    }

    public List<String> getNodeIds() {
        return new ArrayList<>(endpointMap.keySet());
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

    public Properties getRawProperties() {
        return (Properties) rawProperties.clone();
    }

    public String getSourceDescription() {
        return sourceDescription;
    }

    // =========================================================================
    // FACTORY & LOADER METHODS
    // =========================================================================

    /**
     * Nạp cấu hình mặc định của hệ thống theo thứ tự ưu tiên 4 tầng:
     * 1. System property (-Dclient.config, -Dcluster.profile,...)
     * 2. Biến môi trường (CLIENT_CONFIG, CLUSTER_PROFILE,...)
     * 3. File client.properties tại thư mục làm việc hiện hành
     * 4. Classpath /config/client.properties
     */
    public static ClientConfig loadDefault() {
        return loadWithOverrides(null, null, null);
    }

    /**
     * Nạp cấu hình từ một tệp {@link Path} xác định.
     */
    public static ClientConfig fromFile(Path path) throws IOException {
        Objects.requireNonNull(path, "path không được null");
        if (!Files.exists(path) || !Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Tệp cấu hình không tồn tại hoặc không phải là tệp thường: " + path.toAbsolutePath());
        }
        Properties props = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            props.load(reader);
        }
        return fromProperties(props, "file:" + path.toAbsolutePath());
    }

    /**
     * Nạp cấu hình với hỗ trợ ghi đè linh hoạt qua tham số (phục vụ Unit Test kiểm tra thứ tự ưu tiên).
     *
     * @param customConfigPath đường dẫn tệp cấu hình chỉ định (có thể null)
     * @param sysPropsOverride bản đồ System Properties ghi đè (có thể null, nếu null dùng System.getProperties())
     * @param envVarsOverride  bản đồ Environment Variables ghi đè (có thể null, nếu null dùng System.getenv())
     */
    public static ClientConfig loadWithOverrides(
            Path customConfigPath,
            Map<String, String> sysPropsOverride,
            Map<String, String> envVarsOverride) {

        // 1. Khởi tạo Properties và nạp metadata phiên bản release (/config/release.properties)
        Properties merged = new Properties();
        String sourceDesc = "default-classpath";
        try (InputStream relIs = openClasspathResource("/config/release.properties")) {
            if (relIs != null) {
                try (Reader relReader = new InputStreamReader(relIs, StandardCharsets.UTF_8)) {
                    merged.load(relReader);
                }
            }
        } catch (IOException ignored) {}

        // 4. Classpath default (/config/client.properties hoặc /client.properties)
        try (InputStream is = openClasspathResource("/config/client.properties")) {
            if (is != null) {
                try (Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                    merged.load(reader);
                }
            } else {
                try (InputStream is2 = openClasspathResource("/client.properties")) {
                    if (is2 != null) {
                        try (Reader reader = new InputStreamReader(is2, StandardCharsets.UTF_8)) {
                            merged.load(reader);
                        }
                    }
                }
            }
        } catch (IOException ignored) {}

        // 3. File client.properties đặt tại thư mục làm việc
        Path fileInWorkDir = findFileInWorkDir("client.properties", "config/client.properties");
        if (fileInWorkDir != null) {
            try (BufferedReader reader = Files.newBufferedReader(fileInWorkDir, StandardCharsets.UTF_8)) {
                merged.load(reader);
                sourceDesc = "workdir:" + fileInWorkDir.toAbsolutePath();
            } catch (IOException ignored) {}
        }

        // 2. Tệp chỉ định qua biến môi trường CLIENT_CONFIG
        String envConfigPath = getEnvVal("CLIENT_CONFIG", envVarsOverride);
        if (envConfigPath != null && !envConfigPath.isBlank()) {
            Path p = Paths.get(envConfigPath.trim());
            if (Files.exists(p) && Files.isRegularFile(p)) {
                try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
                    merged.load(reader);
                    sourceDesc = "env-file:" + p.toAbsolutePath();
                } catch (IOException ignored) {}
            }
        }

        // 1. Tệp chỉ định qua System property -Dclient.config hoặc customConfigPath
        String sysConfigPath = getSysPropVal("client.config", sysPropsOverride);
        if (customConfigPath != null) {
            sysConfigPath = customConfigPath.toAbsolutePath().toString();
        }
        if (sysConfigPath != null && !sysConfigPath.isBlank()) {
            Path p = Paths.get(sysConfigPath.trim());
            if (Files.exists(p) && Files.isRegularFile(p)) {
                try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
                    merged.load(reader);
                    sourceDesc = "sysprop-file:" + p.toAbsolutePath();
                } catch (IOException e) {
                    throw new IllegalArgumentException("Không thể đọc tệp cấu hình chỉ định: " + p.toAbsolutePath(), e);
                }
            } else {
                throw new IllegalArgumentException("Tệp cấu hình chỉ định không tồn tại: " + p.toAbsolutePath());
            }
        }

        // Xác định profile mục tiêu trước khi nạp profile-specific files
        String resolvedProfile = merged.getProperty(KEY_CLUSTER_PROFILE, DEFAULT_PROFILE);
        String envProfile = getEnvVal("CLUSTER_PROFILE", envVarsOverride);
        if (envProfile != null && !envProfile.isBlank()) {
            resolvedProfile = envProfile.trim();
        }
        String sysProfile = getSysPropVal(KEY_CLUSTER_PROFILE, sysPropsOverride);
        if (sysProfile != null && !sysProfile.isBlank()) {
            resolvedProfile = sysProfile.trim();
        }

        // Nếu profile là lan và không chỉ định file riêng biệt qua -Dclient.config, nạp client-lan.properties (TV1-D25)
        if ("lan".equalsIgnoreCase(resolvedProfile) && customConfigPath == null && (sysConfigPath == null || sysConfigPath.isBlank())) {
            Path lanFile = findFileInWorkDir("client-lan.properties", "config/client-lan.properties");
            if (lanFile != null) {
                try (BufferedReader reader = Files.newBufferedReader(lanFile, StandardCharsets.UTF_8)) {
                    merged.load(reader);
                    sourceDesc = "workdir-lan:" + lanFile.toAbsolutePath();
                } catch (IOException ignored) {}
            } else {
                try (InputStream isLan = openClasspathResource("/config/client-lan.properties")) {
                    if (isLan != null) {
                        try (Reader reader = new InputStreamReader(isLan, StandardCharsets.UTF_8)) {
                            merged.load(reader);
                            sourceDesc = "classpath:/config/client-lan.properties";
                        }
                    } else {
                        try (InputStream isLan2 = openClasspathResource("/client-lan.properties")) {
                            if (isLan2 != null) {
                                try (Reader reader = new InputStreamReader(isLan2, StandardCharsets.UTF_8)) {
                                    merged.load(reader);
                                    sourceDesc = "classpath:/client-lan.properties";
                                }
                            }
                        }
                    }
                } catch (IOException ignored) {}
            }
        }

        // Áp dụng ghi đè từ Environment Variables (từng key)
        applyEnvOverride(merged, KEY_RELEASE_MODE, "RELEASE_MODE", envVarsOverride);
        applyEnvOverride(merged, KEY_RELEASE_VERSION, "RELEASE_VERSION", envVarsOverride);
        applyEnvOverride(merged, KEY_CLUSTER_PROFILE, "CLUSTER_PROFILE", envVarsOverride);
        applyEnvOverride(merged, KEY_CLIENT_MODE, "CLIENT_MODE", envVarsOverride);
        applyEnvOverride(merged, KEY_CLIENT_MODE, "BOOKING_CLIENT", envVarsOverride);
        applyEnvOverride(merged, "booking.client", "BOOKING_CLIENT", envVarsOverride);
        applyEnvOverride(merged, "cluster.client", "CLUSTER_CLIENT", envVarsOverride);
        applyEnvOverride(merged, "read.adapter", "READ_ADAPTER", envVarsOverride);
        applyEnvOverride(merged, KEY_CONNECT_TIMEOUT_MS, "CLIENT_CONNECT_TIMEOUT_MS", envVarsOverride);
        applyEnvOverride(merged, KEY_READ_TIMEOUT_MS, "CLIENT_READ_TIMEOUT_MS", envVarsOverride);
        applyEnvOverride(merged, KEY_MAX_REDIRECTS, "CLIENT_MAX_REDIRECTS", envVarsOverride);
        applyEnvOverride(merged, KEY_PREFERRED_NODE, "CLIENT_PREFERRED_NODE", envVarsOverride);
        applyEnvOverride(merged, KEY_SELECTION_STRATEGY, "CLIENT_SELECTION_STRATEGY", envVarsOverride);
        applyEnvOverride(merged, KEY_FAILURE_COOLDOWN_MS, "CLIENT_FAILURE_COOLDOWN_MS", envVarsOverride);

        // Áp dụng ghi đè từ System Properties (từng key - mức ưu tiên cao nhất)
        applySysPropOverride(merged, KEY_RELEASE_MODE, sysPropsOverride);
        applySysPropOverride(merged, KEY_RELEASE_VERSION, sysPropsOverride);
        applySysPropOverride(merged, KEY_CLUSTER_PROFILE, sysPropsOverride);
        applySysPropOverride(merged, "booking.client", sysPropsOverride);
        applySysPropOverride(merged, "cluster.client", sysPropsOverride);
        applySysPropOverride(merged, "read.adapter", sysPropsOverride);
        String explicitClientMode = getSysPropVal(KEY_CLIENT_MODE, sysPropsOverride);
        String sysBookingClient = getSysPropVal("booking.client", sysPropsOverride);
        if (explicitClientMode != null && !explicitClientMode.isBlank()) {
            merged.setProperty(KEY_CLIENT_MODE, explicitClientMode.trim());
        } else if (sysBookingClient != null && !sysBookingClient.isBlank()) {
            merged.setProperty(KEY_CLIENT_MODE, sysBookingClient.trim());
        }
        applySysPropOverride(merged, KEY_CONNECT_TIMEOUT_MS, sysPropsOverride);
        applySysPropOverride(merged, KEY_READ_TIMEOUT_MS, sysPropsOverride);
        applySysPropOverride(merged, KEY_MAX_REDIRECTS, sysPropsOverride);
        applySysPropOverride(merged, KEY_PREFERRED_NODE, sysPropsOverride);
        applySysPropOverride(merged, KEY_SELECTION_STRATEGY, sysPropsOverride);
        applySysPropOverride(merged, KEY_FAILURE_COOLDOWN_MS, sysPropsOverride);

        // Ngoài ra, ghi đè các key động liên quan đến endpoint từ System Properties (ví dụ: -Ddev.node.node-1.client.port=...)
        if (sysPropsOverride != null) {
            for (Map.Entry<String, String> entry : sysPropsOverride.entrySet()) {
                merged.setProperty(entry.getKey(), entry.getValue());
            }
        } else {
            for (String key : System.getProperties().stringPropertyNames()) {
                if (key.contains(".client.host") || key.contains(".client.port") || key.endsWith(".cluster.nodes")) {
                    merged.setProperty(key, System.getProperty(key));
                }
            }
        }

        return fromProperties(merged, sourceDesc);
    }

    /**
     * Tạo đối tượng {@link ClientConfig} từ {@link Properties} kèm validation fail-fast nghiêm ngặt.
     */
    public static ClientConfig fromProperties(Properties props, String sourceDescription) {
        Objects.requireNonNull(props, "props không được null");
        Properties effectiveProps = (Properties) props.clone();
        if (!effectiveProps.containsKey(KEY_RELEASE_MODE) && isReleaseModeActive()) {
            effectiveProps.setProperty(KEY_RELEASE_MODE, "true");
        }
        props = effectiveProps;

        // 1. Xác định profile
        String profile = props.getProperty(KEY_CLUSTER_PROFILE, DEFAULT_PROFILE).trim();
        if (profile.isBlank()) {
            throw new IllegalArgumentException("Cấu hình '" + KEY_CLUSTER_PROFILE + "' không được để trống (nguồn: " + sourceDescription + ")");
        }

        // 2. Đọc các tùy chọn chung
        int connectTimeoutMs = parsePositiveInt(props, KEY_CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS, sourceDescription);
        int readTimeoutMs = parsePositiveInt(props, KEY_READ_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, sourceDescription);
        int maxRedirects = parseNonNegativeInt(props, KEY_MAX_REDIRECTS, DEFAULT_MAX_REDIRECTS, sourceDescription);
        long failureCooldownMs = parseNonNegativeLong(props, KEY_FAILURE_COOLDOWN_MS, DEFAULT_FAILURE_COOLDOWN_MS, sourceDescription);

        String strategyStr = props.getProperty(KEY_SELECTION_STRATEGY, DEFAULT_STRATEGY.name());
        SelectionStrategy strategy = SelectionStrategy.fromString(strategyStr);

        String preferredNode = props.getProperty(KEY_PREFERRED_NODE, "").trim();

        // 3. Trích xuất danh sách Node cho profile được chọn
        List<String> nodeIds = resolveNodeIdsForProfile(props, profile);
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("Profile '" + profile + "' không tìm thấy Node nào hoặc không tồn tại trong cấu hình (nguồn: " + sourceDescription + ")");
        }

        // Kiểm tra nodeId duy nhất
        Set<String> uniqueIds = new LinkedHashSet<>();
        for (String id : nodeIds) {
            if (!uniqueIds.add(id)) {
                throw new IllegalArgumentException("Phát hiện NodeId trùng lặp '" + id + "' trong profile '" + profile + "' (nguồn: " + sourceDescription + ")");
            }
        }

        // 4. Kiểm tra preferredNode hợp lệ nếu có chỉ định
        if (!preferredNode.isBlank() && !uniqueIds.contains(preferredNode)) {
            throw new IllegalArgumentException("Cấu hình '" + KEY_PREFERRED_NODE + "=" + preferredNode +
                    "' không nằm trong danh sách Node khả dụng của profile '" + profile + "' " + uniqueIds +
                    " (nguồn: " + sourceDescription + ")");
        }

        // 5. Nạp và thẩm tra từng NodeEndpoint
        List<NodeEndpoint> endpoints = new ArrayList<>();
        for (String id : uniqueIds) {
            String hostKey = profile + ".node." + id + ".client.host";
            String host = props.getProperty(hostKey);
            if (host == null || host.isBlank()) {
                // Thử fallback không có prefix profile (chuẩn configs/dev/cluster.properties)
                hostKey = "node." + id + ".client.host";
                host = props.getProperty(hostKey);
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("Host của node '" + id + "' không được rỗng hoặc thiếu cấu hình (key: " + profile + ".node." + id + ".client.host, nguồn: " + sourceDescription + ")");
            }
            host = host.trim();

            String portKey = profile + ".node." + id + ".client.port";
            String portStr = props.getProperty(portKey);
            if (portStr == null || portStr.isBlank()) {
                portKey = "node." + id + ".client.port";
                portStr = props.getProperty(portKey);
            }
            if (portStr == null || portStr.isBlank()) {
                throw new IllegalArgumentException("Port của node '" + id + "' không được rỗng hoặc thiếu cấu hình (key: " + profile + ".node." + id + ".client.port, nguồn: " + sourceDescription + ")");
            }
            int port;
            try {
                port = Integer.parseInt(portStr.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Port của node '" + id + "' không phải số nguyên hợp lệ: '" + portStr + "' (key: " + portKey + ", nguồn: " + sourceDescription + ")");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port của node '" + id + "' phải nằm trong khoảng [1, 65535], nhận được: " + port + " (key: " + portKey + ", nguồn: " + sourceDescription + ")");
            }

            // Kiểm tra cảnh báo Docker internal hostname
            checkDockerInternalHostnameWarning(id, host);

            endpoints.add(new NodeEndpoint(id, host, port));
        }

        ClientConfig built = new ClientConfig(
                profile,
                connectTimeoutMs,
                readTimeoutMs,
                maxRedirects,
                preferredNode,
                strategy,
                failureCooldownMs,
                endpoints,
                props,
                sourceDescription
        );
        if (built.isReleaseMode()) {
            built.validateForRelease();
        }
        return built;
    }

    // =========================================================================
    // HELPER & VALIDATION METHODS
    // =========================================================================

    /**
     * Kiểm tra xem host có phải là placeholder IP chờ TV6-D24 bàn giao hay không (TASK TV1-D25).
     */
    public static boolean isPlaceholderHost(String host) {
        if (host == null || host.isBlank()) {
            return true;
        }
        String h = host.trim();
        if (h.contains("<") || h.contains(">") || h.toLowerCase().contains("placeholder")) {
            return true;
        }
        // Dải IP placeholder mẫu theo docs/specs/08-DEPLOYMENT.md và configs/lan/PORTS.md
        if (h.equals("192.168.1.101") || h.equals("192.168.1.102") || h.equals("192.168.1.103") ||
            h.equals("192.168.1.104") || h.equals("192.168.1.105") ||
            h.equals("192.168.10.11") || h.equals("192.168.10.12") || h.equals("192.168.10.13") ||
            h.equals("192.168.10.14") || h.equals("192.168.10.15")) {
            return true;
        }
        return false;
    }

    /**
     * Kiểm tra xem cấu hình có chứa bất kỳ node nào sử dụng IP placeholder hay không.
     */
    public boolean hasPlaceholderEndpoints() {
        for (NodeEndpoint ep : endpoints) {
            if (isPlaceholderHost(ep.host())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Trả về danh sách định danh node và host đang ở trạng thái placeholder.
     */
    public List<String> getPlaceholderNodes() {
        List<String> list = new ArrayList<>();
        for (NodeEndpoint ep : endpoints) {
            if (isPlaceholderHost(ep.host())) {
                list.add(ep.nodeId() + " (" + ep.host() + ")");
            }
        }
        return list;
    }

    /**
     * Thẩm tra fail-fast: Từ chối chạy ở chế độ socket thật nếu cấu hình còn chứa IP placeholder (TASK TV1-D25).
     *
     * @throws IllegalStateException nếu có ít nhất 1 node chứa IP placeholder
     */
    public void validateNoPlaceholders() {
        List<String> placeholders = new ArrayList<>();
        for (NodeEndpoint ep : endpoints) {
            if (isPlaceholderHost(ep.host())) {
                placeholders.add(profile + ".node." + ep.nodeId() + ".client.host=" + ep.host()
                        + " (nodeId=" + ep.nodeId() + ")");
            }
        }
        if (!placeholders.isEmpty()) {
            throw new IllegalStateException(
                    "Cấu hình không hợp lệ — phát hiện IP placeholder chưa thay bằng địa chỉ thật (chờ TV6-D24 bàn giao): " +
                            String.join(", ", placeholders) +
                            ". Vui lòng cập nhật client-lan.properties hoặc biến môi trường trước khi chạy."
            );
        }
    }

    /**
     * Thẩm tra fail-fast nghiêm ngặt cho bản phát hành RELEASE (TASK TV1-D28):
     * <ul>
     *   <li>Chỉ chấp nhận {@code client.mode=socket} (từ chối mọi cấu hình {@code mock}, {@code fake}, {@code demo}).</li>
     *   <li>Từ chối các cờ phụ {@code booking.client=mock/fake}, {@code cluster.client=mock/fake}, {@code read.adapter=fake}.</li>
     *   <li>Từ chối IP placeholder và tên dịch vụ Docker nội bộ, nêu rõ khóa cấu hình và {@code nodeId}.</li>
     * </ul>
     */
    public void validateForRelease() {
        if (!"socket".equalsIgnoreCase(clientMode)
                && !"tcp".equalsIgnoreCase(clientMode)
                && !"real".equalsIgnoreCase(clientMode)) {
            throw new IllegalArgumentException(
                    "Chế độ RELEASE (release.mode=true) chỉ cho phép 'client.mode=socket'; từ chối cấu hình '"
                            + KEY_CLIENT_MODE + "=" + clientMode + "' (nguồn: " + sourceDescription + ").");
        }
        String legacyBooking = rawProperties.getProperty("booking.client", "").trim();
        if (!legacyBooking.isEmpty()
                && !"socket".equalsIgnoreCase(legacyBooking)
                && !"tcp".equalsIgnoreCase(legacyBooking)
                && !"real".equalsIgnoreCase(legacyBooking)) {
            throw new IllegalArgumentException(
                    "Chế độ RELEASE (release.mode=true) từ chối cấu hình 'booking.client=" + legacyBooking
                            + "' — chỉ được dùng SocketBookingClient thật.");
        }
        String legacyCluster = rawProperties.getProperty("cluster.client", "").trim();
        if (!legacyCluster.isEmpty()
                && !"socket".equalsIgnoreCase(legacyCluster)
                && !"tcp".equalsIgnoreCase(legacyCluster)
                && !"real".equalsIgnoreCase(legacyCluster)) {
            throw new IllegalArgumentException(
                    "Chế độ RELEASE (release.mode=true) từ chối cấu hình 'cluster.client=" + legacyCluster + "'.");
        }
        String legacyRead = rawProperties.getProperty("read.adapter", "").trim();
        if (!legacyRead.isEmpty()
                && !"socket".equalsIgnoreCase(legacyRead)
                && !"real".equalsIgnoreCase(legacyRead)) {
            throw new IllegalArgumentException(
                    "Chế độ RELEASE (release.mode=true) từ chối cấu hình 'read.adapter=" + legacyRead
                            + "' — không được phép dùng adapter giả lập.");
        }

        validateNoPlaceholders();

        for (NodeEndpoint ep : endpoints) {
            if (isDockerInternalHost(ep.host())) {
                String key = profile + ".node." + ep.nodeId() + ".client.host";
                throw new IllegalArgumentException(
                        "Chế độ RELEASE từ chối tên máy chủ nội bộ Docker tại khóa '" + key + "=" + ep.host()
                                + "' (nodeId=" + ep.nodeId() + "). Vui lòng cấu hình 127.0.0.1 hoặc IP LAN thật.");
            }
        }
    }

    /**
     * Kiểm tra tính hợp lệ của cấu hình khi khởi chạy ứng dụng Desktop.
     */
    public void validateForStartup() {
        if (releaseMode) {
            validateForRelease();
        } else if (isSocketMode() && "lan".equalsIgnoreCase(profile)) {
            validateNoPlaceholders();
        }
    }

    public static boolean isDockerInternalHost(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String h = host.trim().toLowerCase();
        if (h.equals("127.0.0.1") || h.equals("localhost") || h.equals("0.0.0.0")) {
            return false;
        }
        // Khớp các tên container phổ biến hoặc tên dịch vụ Docker
        if (DOCKER_SERVICE_PATTERN.matcher(h).matches()) {
            return true;
        }
        // Tên không có dấu chấm, không có dấu hai chấm và không phải localhost
        if (!h.contains(".") && !h.contains(":")) {
            return true;
        }
        return false;
    }

    private static void checkDockerInternalHostnameWarning(String nodeId, String host) {
        if (isDockerInternalHost(host)) {
            String warningMsg = String.format(
                    "[ClientConfig CẢNH BÁO] Host '%s' của node '%s' có dạng tên dịch vụ Docker nội bộ. " +
                            "Tên này chỉ phân giải trong mạng Docker, Desktop chạy trên Windows host nên dùng 127.0.0.1 hoặc IP LAN.",
                    host, nodeId
            );
            LOGGER.log(System.Logger.Level.WARNING, warningMsg);
            System.err.println(warningMsg);
        }
    }

    private static List<String> resolveNodeIdsForProfile(Properties props, String profile) {
        List<String> result = new ArrayList<>();

        // 1. Thử key: <profile>.cluster.nodes
        String nodesConfig = props.getProperty(profile + ".cluster.nodes");
        if (nodesConfig == null || nodesConfig.isBlank()) {
            // 2. Thử key phẳng: cluster.nodes
            nodesConfig = props.getProperty("cluster.nodes");
        }

        if (nodesConfig != null && !nodesConfig.isBlank()) {
            for (String part : nodesConfig.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
            return result;
        }

        // 3. Quét động các key dạng: <profile>.node.<nodeId>.client.host
        String prefix = profile + ".node.";
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix) && key.endsWith(".client.host")) {
                String sub = key.substring(prefix.length(), key.length() - ".client.host".length());
                if (!sub.isBlank() && !result.contains(sub)) {
                    result.add(sub);
                }
            }
        }

        // 4. Nếu vẫn rỗng, thử quét key dạng phẳng node.<nodeId>.client.host
        if (result.isEmpty()) {
            for (String key : props.stringPropertyNames()) {
                if (key.startsWith("node.") && key.endsWith(".client.host")) {
                    String sub = key.substring("node.".length(), key.length() - ".client.host".length());
                    if (!sub.isBlank() && !result.contains(sub)) {
                        result.add(sub);
                    }
                }
            }
        }

        return result;
    }

    private static int parsePositiveInt(Properties props, String key, int defaultVal, String src) {
        String val = props.getProperty(key);
        if (val == null || val.isBlank()) {
            return defaultVal;
        }
        try {
            int parsed = Integer.parseInt(val.trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException("Khóa '" + key + "' phải là số nguyên > 0, nhận được: " + parsed + " (nguồn: " + src + ")");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Khóa '" + key + "' không phải là số nguyên hợp lệ: '" + val + "' (nguồn: " + src + ")");
        }
    }

    private static int parseNonNegativeInt(Properties props, String key, int defaultVal, String src) {
        String val = props.getProperty(key);
        if (val == null || val.isBlank()) {
            return defaultVal;
        }
        try {
            int parsed = Integer.parseInt(val.trim());
            if (parsed < 0) {
                throw new IllegalArgumentException("Khóa '" + key + "' không được âm, nhận được: " + parsed + " (nguồn: " + src + ")");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Khóa '" + key + "' không phải là số nguyên hợp lệ: '" + val + "' (nguồn: " + src + ")");
        }
    }

    private static long parseNonNegativeLong(Properties props, String key, long defaultVal, String src) {
        String val = props.getProperty(key);
        if (val == null || val.isBlank()) {
            return defaultVal;
        }
        try {
            long parsed = Long.parseLong(val.trim());
            if (parsed < 0) {
                throw new IllegalArgumentException("Khóa '" + key + "' không được âm, nhận được: " + parsed + " (nguồn: " + src + ")");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Khóa '" + key + "' không phải là số nguyên hợp lệ: '" + val + "' (nguồn: " + src + ")");
        }
    }

    private static void applyEnvOverride(Properties props, String propKey, String envKey, Map<String, String> envMap) {
        String val = getEnvVal(envKey, envMap);
        if (val != null && !val.isBlank()) {
            props.setProperty(propKey, val.trim());
        }
    }

    private static void applySysPropOverride(Properties props, String propKey, Map<String, String> sysProps) {
        String val = getSysPropVal(propKey, sysProps);
        if (val != null && !val.isBlank()) {
            props.setProperty(propKey, val.trim());
        }
    }

    private static String getEnvVal(String envKey, Map<String, String> envMap) {
        if (envMap != null) {
            return envMap.get(envKey);
        }
        try {
            return System.getenv(envKey);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String getSysPropVal(String propKey, Map<String, String> sysProps) {
        if (sysProps != null) {
            return sysProps.get(propKey);
        }
        try {
            return System.getProperty(propKey);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static InputStream openClasspathResource(String resPath) {
        return ClientConfig.class.getResourceAsStream(resPath);
    }

    private static Path findFileInWorkDir(String... candidatePaths) {
        for (String c : candidatePaths) {
            Path p = Paths.get(c);
            if (Files.exists(p) && Files.isRegularFile(p)) {
                return p;
            }
            // Hỗ trợ cả khi chạy từ thư mục con desktop-client
            Path parentP = Paths.get("..", c);
            if (Files.exists(parentP) && Files.isRegularFile(parentP)) {
                return parentP;
            }
        }
        return null;
    }
}
