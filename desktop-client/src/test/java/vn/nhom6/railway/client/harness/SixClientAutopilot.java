package vn.nhom6.railway.client.harness;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Group;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.transform.Scale;
import javafx.stage.Stage;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.SeatsRefreshService;
import vn.nhom6.railway.client.controller.SeatGridController;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.ClusterEndpoints;
import vn.nhom6.railway.client.network.ClusterNodeSelector;
import vn.nhom6.railway.client.network.LeaderAwareBookingClient;
import vn.nhom6.railway.client.network.RedirectPolicy;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Trình khởi chạy 1 cửa sổ JavaFX Desktop Client có tự động đặt ghế theo mốc giờ đồng bộ (TASK TV1-D18).
 * <p>
 * <b>CHỈ NẰM TRONG TEST SOURCES</b> — tuyệt đối không đưa autopilot hoặc nhãn FAKE vào {@code src/main}.
 * <p>
 * Lớp này KHÔNG kế thừa {@link Application} để có thể chạy trực tiếp bằng {@code java -cp ...} trên Windows
 * (gọi {@link Application#launch(Class, String...)} vào {@link AutopilotFxApp}).
 */
public final class SixClientAutopilot {

    private SixClientAutopilot() {
    }

    public static void main(String[] args) {
        Application.launch(AutopilotFxApp.class, args);
    }

    public static final class AutopilotFxApp extends Application {

        private ScheduledExecutorService scheduler;
        private LeaderAwareBookingClient customLeaderAwareClient;
        private final AtomicBoolean resultWritten = new AtomicBoolean(false);
        private final AtomicBoolean exiting = new AtomicBoolean(false);

        @Override
        public void start(Stage primaryStage) throws Exception {
            final int clientId = Integer.getInteger("client.id", 1);
            final String senderId = "client-" + clientId;
            final String mode = System.getProperty("scenario.mode", "fake").trim().toLowerCase();
            final boolean isRealMode = "real".equals(mode);
            final String targetSeat = System.getProperty("scenario.seat", "G02").trim().toUpperCase();
            final String passengerName = System.getProperty("scenario.passenger", "Hành khách Test " + clientId);
            final long startAtEpochMs = Long.getLong("scenario.startAt", System.currentTimeMillis() + 1500L);
            final double winX = Double.parseDouble(System.getProperty("scenario.window.x", "40"));
            final double winY = Double.parseDouble(System.getProperty("scenario.window.y", "40"));
            final double winW = Double.parseDouble(System.getProperty("scenario.window.w", "512"));
            final double winH = Double.parseDouble(System.getProperty("scenario.window.h", "415"));
            final String resultFileProp = System.getProperty("scenario.resultFile", "").trim();
            final String snapshotFileProp = System.getProperty("scenario.snapshotFile", "").trim();
            final String stopFileProp = System.getProperty("scenario.stopFile", "").trim();
            final long maxLifetimeSec = Long.getLong("scenario.maxLifetimeSec", 75L);

            // 1. Nạp ClientConfig (D16) và tiêm BookingClient dùng senderId = client-N (chuẩn SPEC-03 §1)
            ClientConfig cfg = ClientConfig.loadDefault();
            ClusterNodeSelector selector = new ClusterNodeSelector(cfg);
            ClusterEndpoints endpoints = ClusterEndpoints.fromSelector(selector);
            RedirectPolicy policy = new RedirectPolicy(
                    cfg.getMaxRedirects(),
                    cfg.getConnectTimeoutMs(),
                    cfg.getReadTimeoutMs()
            );

            customLeaderAwareClient = new LeaderAwareBookingClient(
                    endpoints,
                    ep -> new SocketBookingClient(
                            ep.host(), ep.port(), cfg.getConnectTimeoutMs(), cfg.getReadTimeoutMs(), senderId),
                    policy,
                    ClientFactory.getClusterConnectionViewModel(),
                    cfg.getPreferredNode(),
                    selector
            );

            ClientFactory.setClusterNodeSelector(selector);
            ClientFactory.setBookingClient(customLeaderAwareClient);
            ClientFactory.setReadAdapter(new BookingClientReadAdapter(customLeaderAwareClient));

            // 2. Nạp giao diện thật seat-grid.fxml (không sửa src/main)
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/seat-grid.fxml"));
            Parent seatGridRoot = loader.load();
            SeatGridController seatGridController = loader.getController();
            BookingViewModel bookingVm = ClientFactory.getBookingViewModel();
            SeatsRefreshService refreshService = seatGridController.getSeatsRefreshService();

            // Đảm bảo danh sách ghế + refreshService đã gắn vào BookingViewModel hiện hành
            bookingVm.setSeatsList(seatGridController.getSeats());
            bookingVm.setSeatsRefreshService(refreshService);

            // 3. Tạo tiêu đề cửa sổ & thanh tiêu đề tổng quan rõ ràng cho ảnh 3x2
            String windowTitle = isRealMode
                    ? "CLIENT-" + clientId + " — CỤM 5 NODE (REAL)"
                    : "CLIENT-" + clientId + " — FAKE SERVER (không phải Node thật)";
            String sourceTag = isRealMode ? "REAL" : "FAKE SERVER";

            Label clientHeaderLabel = new Label("CLIENT-" + clientId + " | " + sourceTag);
            clientHeaderLabel.setStyle("-fx-font-size: 13px; -fx-font-weight: bold; -fx-text-fill: #ffffff;");

            Label outcomePill = new Label("WAITING");
            outcomePill.setStyle("-fx-background-color: #475569; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;");

            Region spacer = new Region();
            HBox.setHgrow(spacer, Priority.ALWAYS);

            HBox headerRow1 = new HBox(8, clientHeaderLabel, spacer, outcomePill);
            headerRow1.setAlignment(Pos.CENTER_LEFT);

            Label detailLabel = new Label("Ghế mục tiêu: " + targetSeat + " | Đang chờ giờ đồng bộ...");
            detailLabel.setStyle("-fx-font-size: 11px; -fx-text-fill: #e2e8f0;");
            detailLabel.setWrapText(true);

            VBox topBanner = new VBox(4, headerRow1, detailLabel);
            topBanner.setPadding(new Insets(8, 10, 8, 10));
            topBanner.setStyle(isRealMode
                    ? "-fx-background-color: #0f172a; -fx-border-color: #334155; -fx-border-width: 0 0 2 0;"
                    : "-fx-background-color: #1e293b; -fx-border-color: #f59e0b; -fx-border-width: 0 0 2 0;");

            // Scale giao diện seat-grid.fxml thật để toàn bộ 10 ghế G01–G10 hiển thị trọn vẹn trong ô 3x2
            if (seatGridRoot instanceof Region region) {
                region.setPrefSize(920, 560);
                region.setMinSize(920, 560);
                region.setMaxSize(920, 560);
            }
            double innerW = Math.max(360, winW - 20);
            double innerH = Math.max(240, winH - 110);
            double scale = Math.min(innerW / 920.0, innerH / 560.0);
            seatGridRoot.getTransforms().add(new Scale(scale, scale, 0, 0));
            Group scaledGroup = new Group(seatGridRoot);

            VBox wrapper = new VBox(4, topBanner, scaledGroup);
            wrapper.setStyle("-fx-background-color: #f8fafc;");
            VBox.setVgrow(scaledGroup, Priority.ALWAYS);

            Scene scene = new Scene(wrapper, winW - 14, winH - 36);
            primaryStage.setTitle(windowTitle);
            primaryStage.setScene(scene);
            primaryStage.setX(winX);
            primaryStage.setY(winY);
            primaryStage.setOnCloseRequest(e -> shutdownGracefully());
            primaryStage.show();

            // 4. Lên lịch đồng bộ giờ bấm đặt ghế và theo dõi stopFile
            scheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "autopilot-client-" + clientId);
                t.setDaemon(true);
                return t;
            });

            final String requestId = UUID.randomUUID().toString();
            final long delayBeforeStartMs = Math.max(150L, startAtEpochMs - System.currentTimeMillis());

            scheduler.schedule(() -> Platform.runLater(() -> {
                // Chọn ghế mục tiêu trên SeatGridController thật
                seatGridController.handleSeatClick(targetSeat);
                long sentAtMs = System.currentTimeMillis();

                outcomePill.setText("SUBMITTING");
                outcomePill.setStyle("-fx-background-color: #2563eb; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;");
                detailLabel.setText("requestId: " + requestId + " | Đang gửi BOOK " + targetSeat + "...");

                BookTicketRequestDto req = new BookTicketRequestDto(requestId, "SE-001", targetSeat, passengerName);

                // Theo dõi khi UiState chuyển sang trạng thái kết thúc (kể cả UNKNOWN do timeout)
                bookingVm.uiStateProperty().addListener((obs, oldSt, newSt) -> {
                    if (newSt == null || newSt == BookingViewModel.UiState.IDLE
                            || newSt == BookingViewModel.UiState.LOADING
                            || newSt == BookingViewModel.UiState.SUBMITTING) {
                        return;
                    }
                    long elapsedMs = System.currentTimeMillis() - sentAtMs;
                    onTerminalStateReached(
                            clientId,
                            requestId,
                            targetSeat,
                            sourceTag,
                            sentAtMs,
                            elapsedMs,
                            newSt,
                            bookingVm,
                            seatGridController,
                            refreshService,
                            outcomePill,
                            detailLabel,
                            scene,
                            resultFileProp,
                            snapshotFileProp
                    );
                });

                bookingVm.submit(req);
            }), delayBeforeStartMs, TimeUnit.MILLISECONDS);

            // Theo dõi stopFile để tắt sạch cửa sổ và nhả journal.lock
            if (!stopFileProp.isBlank()) {
                Path stopPath = Paths.get(stopFileProp);
                scheduler.scheduleAtFixedRate(() -> {
                    if (Files.exists(stopPath)) {
                        Platform.runLater(this::shutdownGracefully);
                    }
                }, 300L, 200L, TimeUnit.MILLISECONDS);
            }

            // Timeout an toàn tránh tiến trình mồ côi
            scheduler.schedule(() -> Platform.runLater(this::shutdownGracefully), maxLifetimeSec, TimeUnit.SECONDS);
        }

        private void onTerminalStateReached(
                int clientId,
                String requestId,
                String targetSeat,
                String sourceTag,
                long sentAtMs,
                long elapsedMs,
                BookingViewModel.UiState finalState,
                BookingViewModel bookingVm,
                SeatGridController seatGridController,
                SeatsRefreshService refreshService,
                Label outcomePill,
                Label detailLabel,
                Scene scene,
                String resultFileProp,
                String snapshotFileProp) {

            if (!resultWritten.compareAndSet(false, true)) {
                return;
            }

            String reasonCode = bookingVm.getErrorCode();
            String pillText;
            String pillStyle;
            switch (finalState) {
                case CONFIRMED, SUCCESS -> {
                    pillText = "CONFIRMED";
                    pillStyle = "-fx-background-color: #16a34a; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;";
                }
                case REJECTED -> {
                    pillText = "REJECTED (" + (reasonCode != null && !reasonCode.isBlank() ? reasonCode : "SEAT_ALREADY_BOOKED") + ")";
                    pillStyle = "-fx-background-color: #dc2626; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;";
                }
                case UNKNOWN -> {
                    pillText = "UNKNOWN (PENDING)";
                    pillStyle = "-fx-background-color: #d97706; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;";
                }
                default -> {
                    pillText = finalState.name();
                    pillStyle = "-fx-background-color: #64748b; -fx-text-fill: #ffffff; -fx-font-weight: bold; -fx-font-size: 12px; -fx-padding: 2 10; -fx-background-radius: 10;";
                }
            }

            outcomePill.setText(pillText);
            outcomePill.setStyle(pillStyle);

            // Chờ 1 nhịp refresh ghế để G02 hiển thị BOOKED nhất quán (trừ khi UNKNOWN)
            Runnable finalizeAndWrite = () -> {
                SeatViewModel seatVm = seatGridController.getSeatViewModel(targetSeat);
                String seatStatus = (seatVm != null) ? seatVm.getStatus().name() : "UNKNOWN";
                String verLabel = (refreshService != null)
                        ? refreshService.getVerification().getDisplayName()
                        : "Chưa xác minh";
                int pendingCount = 0;
                PersistentPendingJournal j = ClientFactory.getJournal();
                if (j != null && !j.isClosed()) {
                    pendingCount = j.loadPending().size();
                }

                detailLabel.setText(String.format(
                        "reqId: %s | %s: %s [%s] | %dms | pending=%d",
                        requestId.substring(0, Math.min(13, requestId.length())) + "...",
                        targetSeat,
                        seatStatus,
                        verLabel,
                        elapsedMs,
                        pendingCount
                ));

                if (!snapshotFileProp.isBlank()) {
                    saveWindowSnapshot(scene, Paths.get(snapshotFileProp));
                }

                if (!resultFileProp.isBlank()) {
                    writeResultFile(
                            Paths.get(resultFileProp),
                            clientId,
                            requestId,
                            Instant.ofEpochMilli(sentAtMs).toString(),
                            finalState.name(),
                            reasonCode != null && !reasonCode.isBlank() ? reasonCode : "-",
                            elapsedMs,
                            seatStatus,
                            verLabel,
                            sourceTag,
                            pendingCount
                    );
                }
            };

            if (refreshService != null && finalState != BookingViewModel.UiState.UNKNOWN) {
                refreshService.refreshNow().whenComplete((snap, err) ->
                        scheduler.schedule(() -> Platform.runLater(finalizeAndWrite), 250L, TimeUnit.MILLISECONDS));
            } else {
                scheduler.schedule(() -> Platform.runLater(finalizeAndWrite), 250L, TimeUnit.MILLISECONDS);
            }
        }

        private static void saveWindowSnapshot(Scene scene, Path target) {
            try {
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                WritableImage img = scene.snapshot(null);
                int w = (int) img.getWidth();
                int h = (int) img.getHeight();
                BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
                PixelReader reader = img.getPixelReader();
                for (int y = 0; y < h; y++) {
                    for (int x = 0; x < w; x++) {
                        bi.setRGB(x, y, reader.getArgb(x, y));
                    }
                }
                ImageIO.write(bi, "png", target.toFile());
            } catch (Exception ignored) {
            }
        }

        private static void writeResultFile(
                Path resultPath,
                int clientId,
                String requestId,
                String sentAtIso,
                String finalState,
                String reasonCode,
                long elapsedMs,
                String seatStatus,
                String verificationLabel,
                String sourceTag,
                int pendingCount) {
            try {
                if (resultPath.getParent() != null) {
                    Files.createDirectories(resultPath.getParent());
                }
                StringBuilder sb = new StringBuilder();
                sb.append("clientId=").append(clientId).append("\n");
                sb.append("requestId=").append(requestId).append("\n");
                sb.append("sentAt=").append(sentAtIso).append("\n");
                sb.append("finalState=").append(finalState).append("\n");
                sb.append("reasonCode=").append(reasonCode).append("\n");
                sb.append("elapsedMs=").append(elapsedMs).append("\n");
                sb.append("seatStatus=").append(seatStatus).append("\n");
                sb.append("verification=").append(verificationLabel).append("\n");
                sb.append("source=").append(sourceTag).append("\n");
                sb.append("pendingCount=").append(pendingCount).append("\n");

                Path tmp = resultPath.resolveSibling(resultPath.getFileName() + ".tmp");
                Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
                Files.move(tmp, resultPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ignored) {
            }
        }

        private void shutdownGracefully() {
            if (!exiting.compareAndSet(false, true)) {
                return;
            }
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            if (customLeaderAwareClient != null) {
                try {
                    customLeaderAwareClient.close();
                } catch (Exception ignored) {
                }
            }
            PersistentPendingJournal j = ClientFactory.getJournal();
            if (j != null && !j.isClosed()) {
                try {
                    j.close();
                } catch (Exception ignored) {
                }
            }
            Platform.exit();
            System.exit(0);
        }
    }
}
