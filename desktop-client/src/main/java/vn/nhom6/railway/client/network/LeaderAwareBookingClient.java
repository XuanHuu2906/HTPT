package vn.nhom6.railway.client.network;

import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Decorator của {@link BookingClient} có khả năng tự động chuyển hướng theo {@code leaderHint} (TASK TV1-D12).
 * <p>
 * Nguyên tắc an toàn phân tán bắt buộc (AGENTS.md & SPEC-03):
 * <ul>
 *   <li><b>Giữ nguyên {@code requestId}:</b> requestId do UI/ViewModel truyền vào là bất biến suốt toàn bộ
 *       chu kỳ retry/redirect. Client TUYỆT ĐỐI KHÔNG sinh requestId mới.</li>
 *   <li><b>MessageId mới mỗi frame:</b> Mỗi lần gửi gói tin qua socket tới node mới đều sinh envelope có messageId mới.</li>
 *   <li><b>Redirect KHÔNG PHẢI là CONFIRMED:</b> Kết quả CONFIRMED chỉ được trả về khi Node Leader thật sự trả về CONFIRMED.
 *       Mọi bước redirect hay hint không bao giờ được tự ý suy đoán thành CONFIRMED.</li>
 *   <li><b>Xác thực leaderHint nghiêm ngặt:</b> Chỉ tin và kết nối tới nodeId nằm trong danh bạ cấu hình {@link ClusterEndpoints};
 *       từ chối hint rỗng, hint trỏ vào chính node từ chối, hoặc vòng lặp (A -> B -> A).</li>
 *   <li><b>Giới hạn chuyển hướng:</b> Tuân thủ {@link RedirectPolicy#maxRedirects()} và {@link RedirectPolicy#totalTimeoutMs()}.
 *       Dừng và trả mã lỗi rõ ràng {@code NOT_LEADER_UNRESOLVED} khi hết lượt hoặc không giải quyết được.</li>
 *   <li><b>Phân biệt trạng thái:</b> Lỗi trước khi gửi = {@code CONNECTION_FAILED}; timeout/mất kết nối sau khi gửi = {@code UNKNOWN}
 *       (không biến thành {@code REJECTED}); {@code NO_QUORUM} và {@code REQUEST_ID_CONFLICT} giữ nguyên không redirect.</li>
 *   <li><b>Ghi nhớ Leader:</b> Lưu {@code knownLeaderId} để các lệnh tiếp theo gửi thẳng tới Leader; xóa khi Leader đó offline hoặc từ chối.</li>
 *   <li><b>Bất đồng bộ:</b> Chạy hoàn toàn trên daemon thread pool, trả {@link CompletableFuture}, không chặn JavaFX thread.</li>
 * </ul>
 */
public class LeaderAwareBookingClient implements BookingClient, AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(LeaderAwareBookingClient.class.getName());

    public static final String ERROR_NOT_LEADER_UNRESOLVED = "NOT_LEADER_UNRESOLVED";
    public static final String ERROR_CONNECTION_FAILED = "CONNECTION_FAILED";

    private final ClusterEndpoints clusterEndpoints;
    private final Function<NodeEndpoint, BookingClient> clientFactory;
    private final RedirectPolicy redirectPolicy;
    private final ClusterConnectionViewModel connectionViewModel;
    private final ClusterNodeSelector nodeSelector;
    private final ExecutorService executor;

    private final AtomicReference<String> knownLeaderId = new AtomicReference<>(null);
    private final Map<String, BookingClient> clientCache = new ConcurrentHashMap<>();
    private final List<LeaderStateListener> leaderStateListeners = new CopyOnWriteArrayList<>();

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);
    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "leader-aware-client-worker-" + THREAD_COUNTER.getAndIncrement());
        t.setDaemon(true);
        return t;
    };

    public LeaderAwareBookingClient(
            ClusterEndpoints clusterEndpoints,
            Function<NodeEndpoint, BookingClient> clientFactory,
            RedirectPolicy redirectPolicy,
            ClusterConnectionViewModel connectionViewModel,
            String initialNodeId,
            ClusterNodeSelector nodeSelector) {
        this.clusterEndpoints = Objects.requireNonNull(clusterEndpoints, "clusterEndpoints không được null");
        this.clientFactory = (clientFactory != null) ? clientFactory : this::createDefaultSocketClient;
        this.redirectPolicy = (redirectPolicy != null) ? redirectPolicy : RedirectPolicy.defaultPolicy();
        this.connectionViewModel = connectionViewModel;
        this.nodeSelector = nodeSelector;
        this.executor = Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);

        if (initialNodeId != null && !initialNodeId.isBlank() && this.clusterEndpoints.containsNode(initialNodeId)) {
            this.knownLeaderId.set(initialNodeId.trim());
        }
    }

    public LeaderAwareBookingClient(
            ClusterEndpoints clusterEndpoints,
            Function<NodeEndpoint, BookingClient> clientFactory,
            RedirectPolicy redirectPolicy,
            ClusterConnectionViewModel connectionViewModel,
            String initialNodeId) {
        this(clusterEndpoints, clientFactory, redirectPolicy, connectionViewModel, initialNodeId, null);
    }

    public LeaderAwareBookingClient(
            ClusterEndpoints clusterEndpoints,
            ClusterConnectionViewModel connectionViewModel,
            ClusterNodeSelector nodeSelector) {
        this(clusterEndpoints, null, RedirectPolicy.defaultPolicy(), connectionViewModel, null, nodeSelector);
    }

    public LeaderAwareBookingClient(ClusterEndpoints clusterEndpoints) {
        this(clusterEndpoints, null, RedirectPolicy.defaultPolicy(), null, null);
    }

    public LeaderAwareBookingClient(ClusterEndpoints clusterEndpoints, ClusterConnectionViewModel connectionViewModel) {
        this(clusterEndpoints, null, RedirectPolicy.defaultPolicy(), connectionViewModel, null);
    }

    public LeaderAwareBookingClient(ClusterEndpoints clusterEndpoints, Function<NodeEndpoint, BookingClient> clientFactory) {
        this(clusterEndpoints, clientFactory, RedirectPolicy.defaultPolicy(), null, null);
    }

    private BookingClient createDefaultSocketClient(NodeEndpoint endpoint) {
        int timeoutMs = (int) redirectPolicy.stepTimeoutMs();
        return new SocketBookingClient(endpoint.host(), endpoint.port(), timeoutMs, timeoutMs, "client-desktop");
    }

    public String getKnownLeaderId() {
        return knownLeaderId.get();
    }

    public void setKnownLeaderId(String leaderId) {
        if (leaderId != null && clusterEndpoints.containsNode(leaderId)) {
            knownLeaderId.set(leaderId.trim());
        } else {
            knownLeaderId.set(null);
        }
    }

    public void resetKnownLeader() {
        knownLeaderId.set(null);
    }

    // =========================================================================
    // QUẢN LÝ LEADER STATE LISTENERS & FAILOVER DISCOVERY (TASK TV1-D21)
    // =========================================================================

    public void addLeaderStateListener(LeaderStateListener listener) {
        if (listener != null && !leaderStateListeners.contains(listener)) {
            leaderStateListeners.add(listener);
        }
    }

    public void removeLeaderStateListener(LeaderStateListener listener) {
        leaderStateListeners.remove(listener);
    }

    public List<LeaderStateListener> getLeaderStateListeners() {
        return List.copyOf(leaderStateListeners);
    }

    public void notifyLeaderLost(String oldLeaderId, String reason) {
        for (LeaderStateListener l : leaderStateListeners) {
            try {
                l.onLeaderLost(oldLeaderId, reason);
            } catch (Exception ignored) {
            }
        }
    }

    public void notifySearchingLeader() {
        for (LeaderStateListener l : leaderStateListeners) {
            try {
                l.onSearchingLeader();
            } catch (Exception ignored) {
            }
        }
    }

    public void notifyLeaderDiscovered(String newLeaderId, DataSource source) {
        for (LeaderStateListener l : leaderStateListeners) {
            try {
                l.onLeaderDiscovered(newLeaderId, source);
            } catch (Exception ignored) {
            }
        }
    }

    public void notifyNoLeaderAvailable() {
        for (LeaderStateListener l : leaderStateListeners) {
            try {
                l.onNoLeaderAvailable();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Dò tìm Leader khả dụng trong cụm bằng GET_SEATS (thao tác đọc theo SPEC-03).
     * TUYỆT ĐỐI không gửi lệnh BOOK hay CANCEL để dò đường.
     */
    public CompletableFuture<String> discoverLeader() {
        return CompletableFuture.supplyAsync(() -> {
            notifySearchingLeader();
            int maxRounds = 3;
            long backoffMs = 150;
            for (int round = 1; round <= maxRounds; round++) {
                for (NodeEndpoint ep : clusterEndpoints.getAllEndpoints()) {
                    String nodeId = ep.nodeId();
                    try {
                        BookingClient client = getOrCreateClient(nodeId);
                        GetSeatsResponseDto res = client.getSeats(new GetSeatsRequestDto("SE-001"))
                                .get(redirectPolicy.stepTimeoutMs(), TimeUnit.MILLISECONDS);
                        if (res != null) {
                            String discoveredLeader = (res.getLeaderId() != null && clusterEndpoints.containsNode(res.getLeaderId()))
                                    ? res.getLeaderId()
                                    : nodeId;
                            knownLeaderId.set(discoveredLeader);
                            if (connectionViewModel != null) {
                                connectionViewModel.applyLeaderHint(discoveredLeader, DataSource.REAL);
                            }
                            notifyLeaderDiscovered(discoveredLeader, DataSource.REAL);
                            return discoveredLeader;
                        }
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        if (cause instanceof NotLeaderException nle) {
                            String hint = nle.getLeaderHint();
                            if (hint != null && clusterEndpoints.containsNode(hint)) {
                                knownLeaderId.set(hint);
                                if (connectionViewModel != null) {
                                    connectionViewModel.applyLeaderHint(hint, DataSource.REAL);
                                }
                                notifyLeaderDiscovered(hint, DataSource.REAL);
                                return hint;
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(1000, backoffMs * 2);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            notifyNoLeaderAvailable();
            return null;
        }, executor);
    }

    public ClusterEndpoints getClusterEndpoints() {
        return clusterEndpoints;
    }

    public ClusterNodeSelector getNodeSelector() {
        return nodeSelector;
    }

    public RedirectPolicy getRedirectPolicy() {
        return redirectPolicy;
    }

    private BookingClient getOrCreateClient(String nodeId) {
        NodeEndpoint ep = clusterEndpoints.getEndpoint(nodeId);
        if (ep == null) {
            throw new IllegalArgumentException("Không tìm thấy endpoint cho nodeId: " + nodeId);
        }
        return clientCache.computeIfAbsent(nodeId, id -> clientFactory.apply(ep));
    }

    // =========================================================================
    // BOOK_TICKET CÓ CHUYỂN HƯỚNG LEADER
    // =========================================================================

    @Override
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request không được null"));
        }
        String requestId = request.getRequestId();
        if (requestId == null || requestId.isBlank()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("requestId không được null hoặc rỗng"));
        }

        return CompletableFuture.supplyAsync(() -> executeBookWithRedirect(request), executor);
    }

    private BookTicketResponseDto executeBookWithRedirect(BookTicketRequestDto request) {
        long startTime = System.currentTimeMillis();
        String requestId = request.getRequestId();
        Set<String> visitedNodes = new LinkedHashSet<>();
        int redirectCount = 0;

        String targetNodeId = knownLeaderId.get();
        if (targetNodeId == null || !clusterEndpoints.containsNode(targetNodeId)) {
            if (nodeSelector != null) {
                try {
                    NodeEndpoint sel = nodeSelector.selectNode();
                    targetNodeId = sel.nodeId();
                } catch (Exception e) {
                    targetNodeId = clusterEndpoints.getDefaultNodeId();
                }
            } else {
                targetNodeId = clusterEndpoints.getDefaultNodeId();
            }
        }

        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= redirectPolicy.totalTimeoutMs()) {
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "TOTAL_TIMEOUT_EXCEEDED", "TIMEOUT_UNKNOWN");
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Hết thời gian chờ tổng thể (" + redirectPolicy.totalTimeoutMs() + "ms) khi thực hiện chuyển hướng"
                );
            }

            if (targetNodeId == null || !clusterEndpoints.containsNode(targetNodeId)) {
                logStep(requestId, "N/A", "BOOK_TICKET", "UNKNOWN", targetNodeId, redirectCount,
                        "TARGET_NODE_NOT_IN_CONFIG", ERROR_NOT_LEADER_UNRESOLVED);
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        ERROR_NOT_LEADER_UNRESOLVED,
                        null,
                        false,
                        targetNodeId,
                        "Node đích không có trong danh bạ cấu hình: " + targetNodeId
                );
            }

            if (visitedNodes.contains(targetNodeId)) {
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "LOOP_DETECTED", ERROR_NOT_LEADER_UNRESOLVED);
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        ERROR_NOT_LEADER_UNRESOLVED,
                        null,
                        false,
                        targetNodeId,
                        "Phát hiện vòng lặp chuyển hướng: " + visitedNodes + " -> " + targetNodeId
                );
            }

            visitedNodes.add(targetNodeId);
            NodeEndpoint ep = clusterEndpoints.getEndpoint(targetNodeId);

            if (connectionViewModel != null) {
                connectionViewModel.updateConnectedNode(targetNodeId, ep.toAddressString());
            }

            BookingClient client;
            try {
                client = getOrCreateClient(targetNodeId);
            } catch (Exception e) {
                if (nodeSelector != null) {
                    nodeSelector.markFailed(targetNodeId);
                }
                if (targetNodeId.equals(knownLeaderId.get())) {
                    resetKnownLeader();
                }
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "CLIENT_INIT_FAILED: " + e.getMessage(), ERROR_CONNECTION_FAILED);
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        ERROR_CONNECTION_FAILED,
                        null,
                        false,
                        null,
                        "Không thể khởi tạo kết nối tới " + targetNodeId + ": " + e.getMessage()
                );
            }

            long remainingTotal = redirectPolicy.totalTimeoutMs() - (System.currentTimeMillis() - startTime);
            long stepTimeout = Math.min(redirectPolicy.stepTimeoutMs(), Math.max(100, remainingTotal));

            BookTicketResponseDto response;
            try {
                // Gửi nguyên request với cùng requestId và cùng payload
                response = client.book(request).get(stepTimeout, TimeUnit.MILLISECONDS);
                if (nodeSelector != null) {
                    nodeSelector.markSuccess(targetNodeId);
                }
            } catch (TimeoutException e) {
                if (nodeSelector != null) {
                    nodeSelector.markFailed(targetNodeId);
                }
                if (targetNodeId.equals(knownLeaderId.get())) {
                    resetKnownLeader();
                    notifyLeaderLost(targetNodeId, "Timeout phản hồi từ Leader (" + redirectPolicy.stepTimeoutMs() + "ms)");
                    notifySearchingLeader();
                }
                long totalElapsed = System.currentTimeMillis() - startTime;
                boolean isTotalTimeout = totalElapsed >= redirectPolicy.totalTimeoutMs() || remainingTotal <= stepTimeout;
                String msg = isTotalTimeout
                        ? "Hết thời gian chờ tổng thể của lệnh đặt vé (" + redirectPolicy.totalTimeoutMs() + "ms)"
                        : "Hết thời gian chờ phản hồi từ Node " + targetNodeId + " (" + redirectPolicy.stepTimeoutMs() + "ms)";
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        isTotalTimeout ? "TOTAL_TIMEOUT_EXCEEDED" : "STEP_TIMEOUT", "TIMEOUT_UNKNOWN");
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        msg
                );
            } catch (ExecutionException e) {
                if (nodeSelector != null) {
                    nodeSelector.markFailed(targetNodeId);
                }
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (targetNodeId.equals(knownLeaderId.get())) {
                    resetKnownLeader();
                    notifyLeaderLost(targetNodeId, "Lỗi kết nối socket: " + cause.getMessage());
                    notifySearchingLeader();
                }
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "CONNECTION_ERROR: " + cause.getMessage(), ERROR_CONNECTION_FAILED);
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        ERROR_CONNECTION_FAILED,
                        null,
                        false,
                        null,
                        "Lỗi kết nối tới Node " + targetNodeId + ": " + cause.getMessage()
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "INTERRUPTED", "UNKNOWN");
                return new BookTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        null,
                        request.getTripId(),
                        request.getSeatId(),
                        "INTERRUPTED",
                        null,
                        false,
                        null,
                        "Thao tác bị gián đoạn"
                );
            }

            // Phân tích kết quả nhận được
            if (response.isConfirmed()) {
                knownLeaderId.set(targetNodeId);
                notifyLeaderDiscovered(targetNodeId, DataSource.REAL);
                if (connectionViewModel != null) {
                    connectionViewModel.applyLeaderHint(targetNodeId, ClusterStatusClient.DataSource.REAL);
                }
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "CONFIRMED_BY_LEADER", "CONFIRMED");
                return response;
            }

            String reasonCode = response.getReasonCode();

            // Các trường hợp KHÔNG ĐƯỢC REDIRECT:
            // 1. REJECTED nghiệp vụ (ví dụ SEAT_ALREADY_BOOKED)
            // 2. REQUEST_ID_CONFLICT (xung đột payload cùng requestId)
            // 3. NO_QUORUM (cụm mất đa số voter, không redirect)
            if (response.isRejected()
                    || "REQUEST_ID_CONFLICT".equalsIgnoreCase(reasonCode)
                    || "NO_QUORUM".equalsIgnoreCase(reasonCode)) {
                logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "TERMINAL_STATUS: " + reasonCode, response.getOutcome());
                return response;
            }

            // Trường hợp NOT_LEADER: tiến hành kiểm tra leaderHint để redirect
            if ("NOT_LEADER".equalsIgnoreCase(reasonCode)) {
                String hint = response.getLeaderHint();

                if (hint == null || hint.isBlank()) {
                    if (targetNodeId.equals(knownLeaderId.get())) {
                        resetKnownLeader();
                        notifyLeaderLost(targetNodeId, "Node trả NOT_LEADER với hint rỗng");
                    }
                    notifySearchingLeader();
                    logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, "NONE", redirectCount,
                            "NOT_LEADER_HINT_NULL", ERROR_NOT_LEADER_UNRESOLVED);
                    return new BookTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            null,
                            request.getTripId(),
                            request.getSeatId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            null,
                            "Node " + targetNodeId + " trả NOT_LEADER nhưng không kèm leaderHint"
                    );
                }

                hint = hint.trim();

                // Kiểm tra 1: hint phải thuộc danh bạ cấu hình
                if (!clusterEndpoints.containsNode(hint)) {
                    logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, hint, redirectCount,
                            "UNTRUSTED_LEADER_HINT: " + hint, ERROR_NOT_LEADER_UNRESOLVED);
                    return new BookTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            null,
                            request.getTripId(),
                            request.getSeatId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            hint,
                            "Gợi ý leaderHint không nằm trong cấu hình cụm đã nạp: " + hint
                    );
                }

                // Kiểm tra 2: hint không được trùng chính node vừa từ chối
                if (hint.equals(targetNodeId)) {
                    logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, hint, redirectCount,
                            "SELF_REFERENCING_HINT", ERROR_NOT_LEADER_UNRESOLVED);
                    return new BookTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            null,
                            request.getTripId(),
                            request.getSeatId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            hint,
                            "Gợi ý leaderHint trùng chính Node vừa từ chối: " + hint
                    );
                }

                // Kiểm tra 3: hint không được nằm trong tập node đã ghé (chống vòng A -> B -> A)
                if (visitedNodes.contains(hint)) {
                    logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, hint, redirectCount,
                            "LOOP_DETECTED_ON_HINT", ERROR_NOT_LEADER_UNRESOLVED);
                    return new BookTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            null,
                            request.getTripId(),
                            request.getSeatId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            hint,
                            "Phát hiện vòng lặp chuyển hướng: " + visitedNodes + " -> " + hint
                    );
                }

                // Kiểm tra 4: không vượt quá số lần redirect tối đa
                if (redirectCount >= redirectPolicy.maxRedirects()) {
                    logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, hint, redirectCount,
                            "MAX_REDIRECTS_EXCEEDED (" + redirectPolicy.maxRedirects() + ")", ERROR_NOT_LEADER_UNRESOLVED);
                    return new BookTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            null,
                            request.getTripId(),
                            request.getSeatId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            hint,
                            "Vượt quá số lần chuyển hướng tối đa (" + redirectPolicy.maxRedirects() + ")"
                    );
                }

                // Hint hợp lệ -> chuẩn bị redirect sang Node tiếp theo
                redirectCount++;
                String fromNode = targetNodeId;
                targetNodeId = hint;
                knownLeaderId.set(hint);
                notifyLeaderDiscovered(hint, DataSource.REAL);

                if (connectionViewModel != null) {
                    connectionViewModel.applyLeaderHint(hint, ClusterStatusClient.DataSource.REAL);
                }

                logStep(requestId, "N/A", "BOOK_TICKET", fromNode, targetNodeId, redirectCount,
                        "REDIRECTING_TO_HINT", "REDIRECT");
                continue;
            }

            // Trường hợp các lỗi khác hoặc TIMEOUT_UNKNOWN từ server
            logStep(requestId, "N/A", "BOOK_TICKET", targetNodeId, targetNodeId, redirectCount,
                    "OTHER_RESPONSE: " + reasonCode, response.getOutcome());
            return response;
        }
    }

    // =========================================================================
    // GET_SEATS CÓ CHUYỂN HƯỚNG LEADER
    // =========================================================================

    @Override
    public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request không được null"));
        }
        return CompletableFuture.supplyAsync(() -> executeGetSeatsWithRedirect(request), executor);
    }

    private GetSeatsResponseDto executeGetSeatsWithRedirect(GetSeatsRequestDto request) {
        long startTime = System.currentTimeMillis();
        Set<String> visitedNodes = new LinkedHashSet<>();
        int redirectCount = 0;

        String targetNodeId = knownLeaderId.get();
        if (targetNodeId == null || !clusterEndpoints.containsNode(targetNodeId)) {
            if (nodeSelector != null) {
                try {
                    NodeEndpoint sel = nodeSelector.selectNode();
                    targetNodeId = sel.nodeId();
                } catch (Exception e) {
                    targetNodeId = clusterEndpoints.getDefaultNodeId();
                }
            } else {
                targetNodeId = clusterEndpoints.getDefaultNodeId();
            }
        }

        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= redirectPolicy.totalTimeoutMs()) {
                throw new CompletionException(new TimeoutException("Hết thời gian chờ tổng thể khi lấy sơ đồ ghế"));
            }

            if (targetNodeId == null || !clusterEndpoints.containsNode(targetNodeId) || visitedNodes.contains(targetNodeId)) {
                throw new CompletionException(new IOException("Không thể chuyển hướng GET_SEATS: node không hợp lệ hoặc lặp: " + targetNodeId));
            }

            visitedNodes.add(targetNodeId);
            NodeEndpoint ep = clusterEndpoints.getEndpoint(targetNodeId);

            if (connectionViewModel != null) {
                connectionViewModel.updateConnectedNode(targetNodeId, ep.toAddressString());
            }

            BookingClient client = getOrCreateClient(targetNodeId);

            try {
                long stepTimeout = redirectPolicy.stepTimeoutMs();
                GetSeatsResponseDto res = client.getSeats(request).get(stepTimeout, TimeUnit.MILLISECONDS);
                if (nodeSelector != null) {
                    nodeSelector.markSuccess(targetNodeId);
                }

                if (res.getLeaderId() != null && clusterEndpoints.containsNode(res.getLeaderId())) {
                    knownLeaderId.set(res.getLeaderId());
                    if (connectionViewModel != null) {
                        connectionViewModel.applyLeaderHint(res.getLeaderId(), ClusterStatusClient.DataSource.REAL);
                    }
                }
                return res;

            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof NotLeaderException nle) {
                    if (nodeSelector != null) {
                        nodeSelector.markSuccess(targetNodeId);
                    }
                    String hint = nle.getLeaderHint();
                    if (hint != null && clusterEndpoints.containsNode(hint) && !hint.equals(targetNodeId)
                            && !visitedNodes.contains(hint) && redirectCount < redirectPolicy.maxRedirects()) {
                        redirectCount++;
                        targetNodeId = hint;
                        knownLeaderId.set(hint);
                        if (connectionViewModel != null) {
                            connectionViewModel.applyLeaderHint(hint, ClusterStatusClient.DataSource.REAL);
                        }
                        continue;
                    }
                } else {
                    if (nodeSelector != null) {
                        nodeSelector.markFailed(targetNodeId);
                    }
                }
                throw new CompletionException(cause);
            } catch (Exception e) {
                if (nodeSelector != null) {
                    nodeSelector.markFailed(targetNodeId);
                }
                throw new CompletionException(e);
            }
        }
    }

    // =========================================================================
    // CANCEL_TICKET VÀ GET_REQUEST_RESULT (TASK TV1-D23)
    // =========================================================================

    @Override
    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request không được null"));
        }
        String requestId = request.getRequestId();
        if (requestId == null || requestId.isBlank()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("requestId không được null hoặc rỗng"));
        }
        return CompletableFuture.supplyAsync(() -> executeCancelWithRedirect(request), executor);
    }

    private CancelTicketResponseDto executeCancelWithRedirect(CancelTicketRequestDto request) {
        long startTime = System.currentTimeMillis();
        String requestId = request.getRequestId();
        Set<String> visitedNodes = new LinkedHashSet<>();
        int redirectCount = 0;

        String targetNodeId = knownLeaderId.get();
        if (targetNodeId == null || !clusterEndpoints.containsNode(targetNodeId)) {
            if (nodeSelector != null) {
                try {
                    NodeEndpoint sel = nodeSelector.selectNode();
                    targetNodeId = sel.nodeId();
                } catch (Exception e) {
                    targetNodeId = clusterEndpoints.getDefaultNodeId();
                }
            } else {
                targetNodeId = clusterEndpoints.getDefaultNodeId();
            }
        }

        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= redirectPolicy.totalTimeoutMs()) {
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "TOTAL_TIMEOUT_EXCEEDED", "TIMEOUT_UNKNOWN");
                return new CancelTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        request.getTicketId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Hết thời gian chờ tổng thể (" + redirectPolicy.totalTimeoutMs() + "ms) khi thực hiện chuyển hướng"
                );
            }

            if (visitedNodes.contains(targetNodeId)) {
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "REDIRECT_LOOP_DETECTED", "TIMEOUT_UNKNOWN");
                return new CancelTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        request.getTicketId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Phát hiện vòng lặp chuyển hướng qua node " + targetNodeId
                );
            }
            visitedNodes.add(targetNodeId);

            if (redirectCount >= redirectPolicy.maxRedirects()) {
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "MAX_REDIRECTS_EXCEEDED", "TIMEOUT_UNKNOWN");
                return new CancelTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        request.getTicketId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Đã vượt quá số lần chuyển hướng tối đa (" + redirectPolicy.maxRedirects() + ")"
                );
            }

            BookingClient client = getOrCreateClient(targetNodeId);
            CancelTicketResponseDto res;
            try {
                long remainingMs = redirectPolicy.totalTimeoutMs() - (System.currentTimeMillis() - startTime);
                long stepTimeout = Math.min(redirectPolicy.stepTimeoutMs(), Math.max(50, remainingMs));
                res = client.cancel(request).get(stepTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "STEP_TIMEOUT", "TIMEOUT_UNKNOWN");
                return new CancelTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        request.getTicketId(),
                        "TIMEOUT_UNKNOWN",
                        null,
                        false,
                        null,
                        "Hết thời gian chờ tại node " + targetNodeId
                );
            } catch (Exception e) {
                if (nodeSelector != null) {
                    nodeSelector.markFailed(targetNodeId);
                }
                Throwable cause = (e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e;
                if (targetNodeId.equals(knownLeaderId.get())) {
                    resetKnownLeader();
                    notifySearchingLeader();
                }
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "CONNECTION_ERROR: " + cause.getMessage(), ERROR_CONNECTION_FAILED);
                return new CancelTicketResponseDto(
                        requestId,
                        "UNKNOWN",
                        request.getTicketId(),
                        ERROR_CONNECTION_FAILED,
                        null,
                        false,
                        null,
                        "Lỗi kết nối tới node " + targetNodeId + ": " + cause.getMessage()
                );
            }

            if (res == null) {
                return new CancelTicketResponseDto(requestId, "UNKNOWN", request.getTicketId(),
                        "INTERNAL_ERROR", null, false, null, "Nhận phản hồi null từ node " + targetNodeId);
            }

            if (res.isConfirmed()) {
                knownLeaderId.set(targetNodeId);
                notifyLeaderDiscovered(targetNodeId, DataSource.REAL);
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "CONFIRMED_BY_LEADER", "CONFIRMED");
                return res;
            }

            String reasonCode = res.getReasonCode();
            if (res.isRejected()
                    || "REQUEST_ID_CONFLICT".equalsIgnoreCase(reasonCode)
                    || "NO_QUORUM".equalsIgnoreCase(reasonCode)) {
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, targetNodeId, redirectCount,
                        "TERMINAL_STATUS: " + reasonCode, res.getOutcome());
                return res;
            }

            if ("NOT_LEADER".equalsIgnoreCase(reasonCode)) {
                String hint = res.getLeaderHint();
                if (hint == null || hint.isBlank()) {
                    if (targetNodeId.equals(knownLeaderId.get())) {
                        resetKnownLeader();
                        notifyLeaderLost(targetNodeId, "Node trả NOT_LEADER với hint rỗng");
                    }
                    notifySearchingLeader();
                    logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, "NONE", redirectCount,
                            "NOT_LEADER_HINT_NULL", ERROR_NOT_LEADER_UNRESOLVED);
                    return new CancelTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            request.getTicketId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            null,
                            "Node " + targetNodeId + " trả NOT_LEADER nhưng không kèm leaderHint"
                    );
                }

                hint = hint.trim();
                if (!clusterEndpoints.containsNode(hint)) {
                    logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, hint, redirectCount,
                            "UNTRUSTED_LEADER_HINT: " + hint, ERROR_NOT_LEADER_UNRESOLVED);
                    return new CancelTicketResponseDto(
                            requestId,
                            "UNKNOWN",
                            request.getTicketId(),
                            ERROR_NOT_LEADER_UNRESOLVED,
                            null,
                            false,
                            null,
                            "leaderHint không tồn tại trong cluster: " + hint
                    );
                }

                redirectCount++;
                logStep(requestId, "N/A", "CANCEL_TICKET", targetNodeId, hint, redirectCount,
                        "REDIRECT_TO_HINT", "REDIRECTING");
                targetNodeId = hint;
                continue;
            }

            return res;
        }
    }

    @Override
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        String target = (knownLeaderId.get() != null) ? knownLeaderId.get() : clusterEndpoints.getDefaultNodeId();
        return getOrCreateClient(target).getRequestResult(requestId);
    }

    // =========================================================================
    // LOGGING VÀ TIỆN ÍCH
    // =========================================================================

    private void logStep(String requestId, String messageId, String type, String fromNode, String toNode,
                         int redirectCount, String reason, String outcome) {
        String line = String.format("[REDIRECT-CLIENT] requestId=%s, messageId=%s, type=%s, from=%s -> to=%s, redirectCount=%d, reason=%s, outcome=%s",
                requestId, messageId, type, fromNode, toNode, redirectCount, reason, outcome);
        LOGGER.log(System.Logger.Level.INFO, line);
        System.out.println(line);
    }

    @Override
    public void close() {
        executor.shutdownNow();
        for (BookingClient client : clientCache.values()) {
            if (client instanceof AutoCloseable ac) {
                try {
                    ac.close();
                } catch (Exception ignored) {
                }
            }
        }
        clientCache.clear();
    }
}
