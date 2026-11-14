package vn.nhom6.railway.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import vn.nhom6.railway.monitor.MonitorController.NodeRow;
import vn.nhom6.railway.protocol.codec.MessageCodec;

/**
 * TV6-D14 asynchronous status client. D14 uses FakeNodeStatusAdapter only;
 * the real NODE_STATUS transport is intentionally deferred until TV3-D15.
 * No local wire DTO is introduced: adapters supply the SPEC-03 section 5 payload.
 */
public final class MonitorTcpClient implements AutoCloseable {
    public static final List<String> NODE_IDS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");

    @FunctionalInterface
    public interface NodeStatusAdapter {
        /** Runs on a worker thread; return a payload or throw on transport failure. */
        JsonNode fetch(String nodeId) throws Exception;
    }

    private final NodeStatusAdapter adapter;
    private final long timeoutNanos;
    private final ThreadPoolExecutor workers = new ThreadPoolExecutor(5, 5, 0,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(5), runnable -> {
                Thread thread = new Thread(runnable, "monitor-status-worker");
                thread.setDaemon(true);
                return thread;
            });
    private final ScheduledExecutorService deadlines = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "monitor-status-deadline");
        thread.setDaemon(true);
        return thread;
    });
    private final ConcurrentHashMap<String, CompletableFuture<NodeRow>> pending = new ConcurrentHashMap<>();
    private boolean closed;

    public MonitorTcpClient(NodeStatusAdapter adapter, Duration timeout) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        timeoutNanos = positiveNanos(timeout);
    }

    /** Nonblocking, with at most one pending observation per node. */
    public synchronized CompletableFuture<NodeRow> poll(String nodeId) {
        requireNode(nodeId);
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Monitor client is closed"));
        }
        CompletableFuture<NodeRow> existing = pending.get(nodeId);
        if (existing != null && !existing.isDone()) {
            return existing;
        }
        CompletableFuture<NodeRow> result = new CompletableFuture<>();
        pending.put(nodeId, result);
        FutureTask<Void> work = new FutureTask<>(() -> {
            try {
                result.complete(toRow(nodeId, adapter.fetch(nodeId)));
            } catch (SocketTimeoutException exception) {
                result.complete(unavailable(nodeId, "TIMEOUT"));
            } catch (IOException exception) {
                result.complete(unavailable(nodeId, "OFFLINE"));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                result.complete(unavailable(nodeId, "UNKNOWN"));
            } catch (Exception exception) {
                result.complete(unavailable(nodeId, "UNKNOWN"));
            }
            return null;
        });
        var deadline = deadlines.schedule(() -> {
            if (result.complete(unavailable(nodeId, "TIMEOUT"))) {
                work.cancel(true);
            }
        }, timeoutNanos, TimeUnit.NANOSECONDS);
        result.whenComplete((row, error) -> {
            deadline.cancel(false);
            if (result.isCancelled()) {
                work.cancel(true);
            }
            pending.remove(nodeId, result);
        });
        try {
            workers.execute(work);
        } catch (RejectedExecutionException exception) {
            result.complete(unavailable(nodeId, "UNKNOWN"));
        }
        return result;
    }

    private static NodeRow toRow(String nodeId, JsonNode payload) {
        if (payload == null || !payload.isObject() || !nodeId.equals(payload.path("nodeId").asText())
                || !payload.path("storageHealthy").isBoolean()) {
            throw new IllegalArgumentException("Invalid NODE_STATUS payload");
        }
        JsonNode role = Objects.requireNonNull(payload.get("role"), "role");
        if (!role.isNull() && (!role.isTextual()
                || !List.of("LEADER", "FOLLOWER", "CANDIDATE").contains(role.textValue()))) {
            throw new IllegalArgumentException("Invalid NODE_STATUS role");
        }
        return new NodeRow(nodeId, role.isNull() ? null : role.textValue(), number(payload, "term"),
                payload.get("storageHealthy").booleanValue() ? "ONLINE" : "STORAGE_ERROR",
                number(payload, "commitIndex"), number(payload, "lastApplied"));
    }

    private static Long number(JsonNode payload, String field) {
        JsonNode value = Objects.requireNonNull(payload.get(field), field);
        if (value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw new IllegalArgumentException("Invalid NODE_STATUS " + field);
        }
        return value.longValue();
    }

    static NodeRow unavailable(String nodeId, String status) {
        // No response means no fabricated role/term/index or synthetic status payload.
        return new NodeRow(nodeId, null, null, status, null, null);
    }

    static long positiveNanos(Duration duration) {
        long nanos = Objects.requireNonNull(duration, "duration").toNanos();
        if (nanos <= 0) {
            throw new IllegalArgumentException("Duration must be positive");
        }
        return nanos;
    }

    private static void requireNode(String nodeId) {
        if (!NODE_IDS.contains(nodeId)) {
            throw new IllegalArgumentException("Expected node-1 through node-5");
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            pending.values().forEach(future -> future.cancel(true));
            workers.shutdownNow();
            deadlines.shutdownNow();
        }
    }

    /** Configurable in-memory SPEC-03 fixtures; never opens a socket or changes a real node. */
    public static final class FakeNodeStatusAdapter implements NodeStatusAdapter {
        private record Reply(JsonNode payload, long delayNanos, boolean offline) { }
        private final ConcurrentHashMap<String, Reply> replies = new ConcurrentHashMap<>();

        public FakeNodeStatusAdapter() {
            NODE_IDS.forEach(node -> setOnline(node, Duration.ZERO));
        }

        public void setOnline(String nodeId, Duration delay) {
            setFixture(nodeId, fixture(nodeId), delay);
        }

        public void setOffline(String nodeId) {
            requireNode(nodeId);
            replies.put(nodeId, new Reply(null, 0, true));
        }

        public void setFixture(String nodeId, JsonNode payload, Duration delay) {
            requireNode(nodeId);
            long nanos = Objects.requireNonNull(delay, "delay").toNanos();
            if (nanos < 0) {
                throw new IllegalArgumentException("Delay must not be negative");
            }
            replies.put(nodeId, new Reply(Objects.requireNonNull(payload, "payload").deepCopy(), nanos, false));
        }

        @Override
        public JsonNode fetch(String nodeId) throws IOException, InterruptedException {
            requireNode(nodeId);
            Reply reply = replies.get(nodeId);
            TimeUnit.NANOSECONDS.sleep(reply.delayNanos());
            if (reply.offline()) {
                throw new ConnectException("MOCK node offline: " + nodeId);
            }
            return reply.payload().deepCopy();
        }

        /** Fixed diagnostic fixture; observedAt is mock time, not the current cluster time. */
        public static ObjectNode fixture(String nodeId) {
            requireNode(nodeId);
            ObjectNode payload = MessageCodec.getMapper().createObjectNode();
            payload.put("nodeId", nodeId);
            payload.put("role", nodeId.equals("node-3") ? "LEADER" : "FOLLOWER");
            payload.put("term", 8L);
            payload.put("leaderId", "node-3");
            payload.put("lastLogIndex", 45L);
            payload.put("commitIndex", 43L);
            payload.put("lastApplied", nodeId.equals("node-1") ? 41L : 43L);
            payload.put("storageHealthy", true);
            ObjectNode peers = payload.putObject("peerConnectivity");
            NODE_IDS.stream().filter(peer -> !peer.equals(nodeId)).forEach(peer -> peers.put(peer, true));
            payload.put("observedAt", "2026-10-29T10:00:00.000Z");
            return payload;
        }
    }
}
