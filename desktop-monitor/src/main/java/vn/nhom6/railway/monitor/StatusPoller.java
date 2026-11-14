package vn.nhom6.railway.monitor;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javafx.application.Platform;
import vn.nhom6.railway.monitor.MonitorController.NodeRow;

/**
 * TV6-D14: polls the five nodes concurrently and updates the D13 table on the FX thread.
 * After loading main.fxml, create this poller with its controller and call start().
 * Example (all data remains MOCK):
 * <pre>
 * var fake = new MonitorTcpClient.FakeNodeStatusAdapter();
 * var client = new MonitorTcpClient(fake, Duration.ofMillis(500));
 * var poller = new StatusPoller(client, controller, Duration.ofSeconds(1));
 * poller.start(); // call poller.close() when the monitor closes
 * </pre>
 * Owns the supplied client. No connection to the real NODE_STATUS handler in D14.
 */
public final class StatusPoller implements AutoCloseable {
    private final MonitorTcpClient client;
    private final MonitorController controller;
    private final long intervalNanos;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "monitor-status-poller");
        thread.setDaemon(true);
        return thread;
    });
    private CompletableFuture<List<NodeRow>> current;
    private boolean started;
    private boolean closed;

    public StatusPoller(MonitorTcpClient client, MonitorController controller, Duration interval) {
        this.client = Objects.requireNonNull(client, "client");
        this.controller = Objects.requireNonNull(controller, "controller");
        Objects.requireNonNull(controller.nodeTable, "Load the D13 FXML before creating the poller");
        intervalNanos = MonitorTcpClient.positiveNanos(interval);
    }

    /** Idempotent start; repeated ticks reuse an unfinished cycle instead of piling up work. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Status poller is closed");
        }
        if (!started) {
            started = true;
            timer.scheduleWithFixedDelay(this::pollNow, 0, intervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    /** Returns immediately; the future completes after all five rows have been applied on FX. */
    public synchronized CompletableFuture<List<NodeRow>> pollNow() {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Status poller is closed"));
        }
        if (current != null && !current.isDone()) {
            return current;
        }
        CompletableFuture<List<NodeRow>> cycle = new CompletableFuture<>();
        current = cycle;
        var requests = MonitorTcpClient.NODE_IDS.stream().map(node -> client.poll(node)
                .exceptionally(error -> MonitorTcpClient.unavailable(node, "UNKNOWN"))).toList();
        CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).thenRun(() -> {
            // allOf has completed: join here never waits on the JavaFX thread.
            List<NodeRow> rows = requests.stream().map(CompletableFuture::join).toList();
            try {
                Platform.runLater(() -> {
                    synchronized (StatusPoller.this) {
                        if (closed || cycle.isDone()) {
                            cycle.cancel(false);
                            return;
                        }
                        try {
                            controller.nodeTable.getItems().setAll(rows);
                            cycle.complete(rows);
                        } catch (RuntimeException exception) {
                            cycle.completeExceptionally(exception);
                        }
                    }
                });
            } catch (IllegalStateException exception) {
                cycle.completeExceptionally(exception);
            }
        });
        return cycle;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            timer.shutdownNow();
            if (current != null) {
                current.cancel(false);
            }
            client.close();
        }
    }
}
