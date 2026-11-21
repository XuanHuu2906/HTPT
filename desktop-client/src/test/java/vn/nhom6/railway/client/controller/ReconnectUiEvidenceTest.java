package vn.nhom6.railway.client.controller;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.SnapshotParameters;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.stage.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import javax.imageio.ImageIO;
import java.awt.GraphicsEnvironment;
import java.awt.image.BufferedImage;
import java.io.File;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử tự động chụp ảnh bằng chứng giao diện Reconnect khi mất Leader và bảo toàn Pending Request (TASK TV1-D21).
 * <p>
 * <b>NHÃN BẮT BUỘC:</b> FAKE LEADER PROVIDER — không phải cụm thật.
 * <ul>
 *   <li>Chỉ chạy khi có cờ {@code -Dui.evidence=true} hoặc {@code -Dui.smoke=true}.</li>
 *   <li>Môi trường headless hoặc không có màn hình hiển thị sẽ tự động SKIPPED.</li>
 *   <li>Dựng trạng thái: Banner "Đang tìm Leader trong cụm (Reconnecting)... [FAKE]",
 *       ghế G08 UNKNOWN (viền cam) do timeout sau gửi,
 *       bảng Pending Journal hiển thị requestId và trạng thái SENT_UNRESOLVED nguyên vẹn,
 *       các ghế khác hiển thị trạng thái hợp lệ.</li>
 *   <li>Lưu file ảnh kết quả: {@code artifacts/evidence/D21/TV1-D21-ui.png} (kích thước tối thiểu 1040x720).</li>
 * </ul>
 */
@Timeout(value = 35, unit = TimeUnit.SECONDS)
public class ReconnectUiEvidenceTest {

    private Stage stage;
    private Scene scene;
    private MainController mainController;
    private SeatGridController seatGridController;

    @BeforeAll
    static void initJavaFxToolkit() {
        boolean enabled = Boolean.getBoolean("ui.evidence") || Boolean.getBoolean("ui.smoke")
                || "1".equals(System.getenv("UI_EVIDENCE"));
        if (!enabled || GraphicsEnvironment.isHeadless()) {
            return;
        }

        try {
            Platform.startup(() -> {});
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        boolean enabled = Boolean.getBoolean("ui.evidence") || Boolean.getBoolean("ui.smoke")
                || "1".equals(System.getenv("UI_EVIDENCE"));
        Assumptions.assumeTrue(enabled,
                "ReconnectUiEvidenceTest chỉ chạy khi có cờ -Dui.evidence=true hoặc -Dui.smoke=true");
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(),
                "Môi trường headless không có màn hình hiển thị -> SKIPPED (ghi NOT_RUN kèm lý do)");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
                Parent root = loader.load();
                mainController = loader.getController();

                stage = new Stage();
                stage.setTitle("FAKE LEADER PROVIDER — không phải cụm thật | TASK TV1-D21 RECONNECT & PRESERVE PENDING");
                scene = new Scene(root, 1040, 720);
                stage.setScene(scene);
                stage.show();

                // Chuyển sang màn hình chọn ghế
                mainController.showSeatGridView();
                seatGridController = mainController.getCachedSeatGridController();
            } catch (Throwable t) {
                err.set(t);
            } finally {
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Khởi tạo Stage JavaFX timeout");
        if (err.get() != null) {
            fail("Lỗi load giao diện: " + err.get().getMessage());
        }
        Thread.sleep(400); // Cho UI render hoàn tất
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stage != null) {
            CountDownLatch latch = new CountDownLatch(1);
            Platform.runLater(() -> {
                stage.close();
                latch.countDown();
            });
            latch.await(2, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Chụp ảnh bằng chứng giao diện Reconnect Controller D21")
    void testCaptureReconnectUiEvidenceSnapshot() throws Exception {
        File evidenceDir = resolveEvidenceDir();
        File targetFile = new File(evidenceDir, "TV1-D21-ui.png");

        runOnFxWait(() -> {
            if (seatGridController != null) {
                // Ghế G02: BOOKED
                SeatViewModel g02 = seatGridController.getSeatViewModel("G02");
                if (g02 != null) {
                    g02.setStatus(SeatStatus.BOOKED);
                }

                // Ghế G08: UNKNOWN (viền cam) do timeout sau gửi
                SeatViewModel g08 = seatGridController.getSeatViewModel("G08");
                if (g08 != null) {
                    g08.setStatus(SeatStatus.UNKNOWN);
                }

                // Các ghế còn lại giữ nguyên AVAILABLE
                for (String seatId : new String[]{"G01", "G03", "G04", "G05", "G06", "G07", "G09", "G10"}) {
                    SeatViewModel vm = seatGridController.getSeatViewModel(seatId);
                    if (vm != null) {
                        vm.setStatus(SeatStatus.AVAILABLE);
                    }
                }

                // Banner hiển thị thông báo Reconnecting đang tìm Leader
                seatGridController.getScreenStateModel().setState(
                        SeatViewModel.ScreenState.LOADING,
                        "⚠ Đang tìm Leader trong cụm (Reconnecting)... [FAKE LEADER PROVIDER — không phải cụm thật]"
                );
            }

            // Ghi nhận pending entry vào journal và nạp lại vào bảng Pending
            try {
                PersistentPendingJournal journal = ClientFactory.getJournal();
                if (journal != null) {
                    String reqId = "91ab28d1-7299-4a7b-a160-c3d9a1f28b7e";
                    String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G08\",\"passengerName\":\"Trần Văn Failover\"}";
                    PendingEntry entry = new PendingEntry(
                            reqId,
                            "BOOK_TICKET",
                            payload,
                            PendingEntry.computePayloadHash(payload),
                            PendingState.SENT_UNRESOLVED,
                            Instant.now(),
                            null,
                            1,
                            "node-1",
                            null,
                            1
                    );
                    journal.recordPrepared(entry);
                    journal.markSent(reqId);
                }
                PendingRequestsViewModel pvm = ClientFactory.getPendingRequestsViewModel();
                if (pvm != null) {
                    pvm.reloadFromJournal();
                }
            } catch (Exception ignored) {
            }
        });

        Thread.sleep(300); // Chờ giao diện render cập nhật
        captureSnapshot(targetFile);

        assertTrue(targetFile.exists(), "File ảnh bằng chứng TV1-D21-ui.png phải tồn tại");
        assertTrue(targetFile.length() > 10000, "File ảnh bằng chứng phải có kích thước hợp lệ");
    }

    private void runOnFxWait(Runnable action) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Platform.runLater(() -> {
            try {
                action.run();
            } finally {
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private void captureSnapshot(File targetFile) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<WritableImage> imgRef = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                if (scene != null && scene.getRoot() != null) {
                    WritableImage img = scene.getRoot().snapshot(new SnapshotParameters(), null);
                    imgRef.set(img);
                }
            } finally {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        WritableImage fxImg = imgRef.get();
        assertNotNull(fxImg);

        int width = (int) fxImg.getWidth();
        int height = (int) fxImg.getHeight();
        BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        PixelReader reader = fxImg.getPixelReader();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                bufferedImage.setRGB(x, y, reader.getArgb(x, y));
            }
        }

        ImageIO.write(bufferedImage, "png", targetFile);
        System.out.println("[EVIDENCE D21] Đã lưu ảnh: " + targetFile.getAbsolutePath() + " (" + width + "x" + height + ")");
    }

    private File resolveEvidenceDir() {
        File dir = new File("artifacts/evidence/D21").getAbsoluteFile();
        if (dir.getPath().contains("desktop-client")) {
            dir = new File("../artifacts/evidence/D21").getAbsoluteFile();
        }
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
}
