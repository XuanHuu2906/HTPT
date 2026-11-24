package vn.nhom6.railway.monitor;

import java.io.IOException;
import java.time.Duration;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

/**
 * JavaFX Desktop Monitor entry point for observing the 5-node Raft cluster in real time.
 *
 * <p>Modes controlled via {@code -Dmonitor.mode=socket|mock} (default: {@code socket}):
 * <ul>
 *   <li>{@code socket}: polls real backend nodes over SPEC-03 TCP {@code NODE_STATUS}.</li>
 *   <li>{@code mock}: runs with {@link MonitorTcpClient.FakeNodeStatusAdapter} fixtures.</li>
 * </ul>
 * Optional system properties: {@code -Dmonitor.config=<path>}, {@code -Dmonitor.pollIntervalMs=1000},
 * {@code -Dmonitor.timeoutMs=1500}.
 */
public final class MonitorApp extends Application {

    private static final int DEFAULT_WIDTH = 1140;
    private static final int DEFAULT_HEIGHT = 680;

    private StatusPoller poller;

    @Override
    public void start(Stage primaryStage) throws IOException {
        var fxml = getClass().getResource("/fxml/main.fxml");
        if (fxml == null) {
            throw new IllegalStateException("Missing /fxml/main.fxml resource in desktop-monitor");
        }

        FXMLLoader loader = new FXMLLoader(fxml);
        Parent root = loader.load();
        MonitorController controller = loader.getController();

        String mode = System.getProperty("monitor.mode", "socket").strip().toLowerCase();
        boolean isMock = "mock".equals(mode);

        TcpNodeStatusAdapter.MonitorClusterConfig config = TcpNodeStatusAdapter.loadDefaultConfig();
        Duration pollInterval = resolveDuration("monitor.pollIntervalMs", config.pollInterval());
        Duration timeout = resolveDuration("monitor.timeoutMs", config.readTimeout());

        MonitorTcpClient.NodeStatusAdapter adapter = isMock
                ? new MonitorTcpClient.FakeNodeStatusAdapter()
                : new TcpNodeStatusAdapter(config.endpoints(), config.connectTimeout(), timeout,
                        TcpNodeStatusAdapter.DEFAULT_SENDER_ID);

        MonitorTcpClient tcpClient = new MonitorTcpClient(adapter, timeout);
        this.poller = new StatusPoller(tcpClient, controller, pollInterval);

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        primaryStage.setTitle("Railway Cluster Monitor (5 Nodes) [" + (isMock ? "MOCK" : "LIVE TCP") + "]");
        primaryStage.setScene(scene);
        primaryStage.setMinWidth(920);
        primaryStage.setMinHeight(560);
        primaryStage.setOnCloseRequest(event -> {
            closePoller();
            Platform.exit();
        });
        primaryStage.show();

        poller.start();
    }

    @Override
    public void stop() {
        closePoller();
    }

    private void closePoller() {
        StatusPoller current = poller;
        poller = null;
        if (current != null) {
            current.close();
        }
    }

    private static Duration resolveDuration(String property, Duration fallback) {
        String raw = System.getProperty(property);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            long ms = Long.parseLong(raw.strip());
            return ms > 0 ? Duration.ofMillis(ms) : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
