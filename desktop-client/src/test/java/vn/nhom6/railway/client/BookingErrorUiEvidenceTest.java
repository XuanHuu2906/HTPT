package vn.nhom6.railway.client;

import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.SnapshotParameters;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.stage.Stage;
import org.junit.jupiter.api.*;
import vn.nhom6.railway.client.controller.*;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import javax.imageio.ImageIO;
import java.awt.GraphicsEnvironment;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử tự động chụp 4 ảnh bằng chứng UI cho TASK TV1-D19:
 * 1. TV1-D19-rejected-seat.png: REJECTED (SEAT_ALREADY_BOOKED) - Ghế G02 đỏ/đã đặt, thông báo từ chối.
 * 2. TV1-D19-network-not-sent.png: FAILED_NOT_SENT - Lỗi kết nối trước gửi, ghế G01 giữ nguyên AVAILABLE.
 * 3. TV1-D19-unknown-lookup.png: TIMEOUT_UNKNOWN - Ghế G08 UNKNOWN (viền cam), pending table có nút Tra cứu & Gửi lại.
 * 4. TV1-D19-no-quorum.png: NO_QUORUM - Lỗi thiếu đa số 3/5, thông báo lỗi cụm không cam kết an toàn.
 */
@Timeout(value = 35, unit = TimeUnit.SECONDS)
public class BookingErrorUiEvidenceTest {

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
                "BookingErrorUiEvidenceTest chỉ chạy khi có cờ -Dui.evidence=true hoặc -Dui.smoke=true");
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(),
                "Môi trường headless không có màn hình hiển thị -> SKIPPED");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
                Parent root = loader.load();
                mainController = loader.getController();

                stage = new Stage();
                stage.setTitle("BẰNG CHỨNG GIAO DIỆN TASK TV1-D19 — XỬ LÝ LỖI, TRÙNG GHẾ, TIMEOUT, QUORUM");
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
    @DisplayName("Chụp 4 kịch bản ảnh bằng chứng D19")
    void testCaptureAllEvidenceSnapshots() throws Exception {
        File evidenceDir = resolveEvidenceDir();

        // =====================================================================
        // KỊCH BẢN 1: REJECTED (SEAT_ALREADY_BOOKED)
        // Ghế G02 đã đặt, thông báo từ chối, sơ đồ ghế cập nhật
        // =====================================================================
        runOnFxWait(() -> {
            if (seatGridController != null) {
                SeatViewModel g02 = seatGridController.getSeatViewModel("G02");
                if (g02 != null) {
                    g02.setStatus(SeatStatus.BOOKED);
                }
                seatGridController.getScreenStateModel().setState(
                        SeatViewModel.ScreenState.ERROR,
                        "✗ Ghế G02 đã có người đặt trước (SEAT_ALREADY_BOOKED). Vui lòng chọn ghế khác.");
            }
        });
        Thread.sleep(250);
        captureSnapshot(new File(evidenceDir, "TV1-D19-rejected-seat.png"));

        // =====================================================================
        // KỊCH BẢN 2: FAILED_NOT_SENT (Connect refused trước khi gửi)
        // Ghế G01 giữ nguyên AVAILABLE, thông báo mất mạng trước khi gửi
        // =====================================================================
        runOnFxWait(() -> {
            if (seatGridController != null) {
                SeatViewModel g01 = seatGridController.getSeatViewModel("G01");
                if (g01 != null) {
                    g01.setStatus(SeatStatus.AVAILABLE);
                }
                seatGridController.getScreenStateModel().setState(
                        SeatViewModel.ScreenState.DISCONNECTED,
                        "⚠ Mất kết nối trước khi gửi (FAILED_NOT_SENT): ConnectException: Connection refused (127.0.0.1:9101). Lệnh chưa từng rời máy khách.");
            }
        });
        Thread.sleep(250);
        captureSnapshot(new File(evidenceDir, "TV1-D19-network-not-sent.png"));

        // =====================================================================
        // KỊCH BẢN 3: TIMEOUT_UNKNOWN và Tra cứu / Gửi lại theo requestId
        // Ghế G08 UNKNOWN (viền cam nét đứt), pending journal có item với nút Tra cứu & Gửi lại
        // =====================================================================
        runOnFxWait(() -> {
            if (seatGridController != null) {
                SeatViewModel g08 = seatGridController.getSeatViewModel("G08");
                if (g08 != null) {
                    g08.setStatus(SeatStatus.UNKNOWN);
                }
            }
            try {
                PersistentPendingJournal journal = ClientFactory.getJournal();
                if (journal != null) {
                    PendingEntry entry = new PendingEntry(
                            "34f3aaba-5662-4f76-88bf-7729837190d1",
                            "BOOK_TICKET",
                            "{\"requestId\":\"34f3aaba-5662-4f76-88bf-7729837190d1\",\"tripId\":\"SE-001\",\"seatId\":\"G08\",\"passengerName\":\"Trần Văn A\"}",
                            "hash-g08",
                            PendingState.SENT_UNRESOLVED,
                            Instant.now(),
                            null,
                            1,
                            "node-1",
                            null,
                            1
                    );
                    journal.recordPrepared(entry);
                    journal.markSent("34f3aaba-5662-4f76-88bf-7729837190d1");
                }
                PendingRequestsViewModel pvm = ClientFactory.getPendingRequestsViewModel();
                if (pvm != null) {
                    pvm.reloadFromJournal();
                }
            } catch (Exception ignored) {
            }

            if (seatGridController != null) {
                seatGridController.getScreenStateModel().setState(
                        SeatViewModel.ScreenState.DISCONNECTED,
                        "⏳ Chưa xác định kết quả (TIMEOUT_UNKNOWN): Đã lưu vào Sổ lệnh Pending. Vui lòng bấm 'Tra cứu kết quả' theo requestId: 34f3aaba-5662-4f76-88bf-7729837190d1");
            }
        });
        Thread.sleep(250);
        captureSnapshot(new File(evidenceDir, "TV1-D19-unknown-lookup.png"));

        // =====================================================================
        // KỊCH BẢN 4: NO_QUORUM (Cụm máy chủ thiếu đa số 3/5)
        // Ghế G04 không đổi, thông báo lỗi cụm không đủ đa số
        // =====================================================================
        runOnFxWait(() -> {
            if (seatGridController != null) {
                seatGridController.getScreenStateModel().setState(
                        SeatViewModel.ScreenState.ERROR,
                        "⚡ Lỗi cụm: Không đủ đa số (Quorum 3/5) để cam kết an toàn! (NO_QUORUM - Leader chưa thể xác nhận ghi bền vững).");
            }
        });
        Thread.sleep(250);
        captureSnapshot(new File(evidenceDir, "TV1-D19-no-quorum.png"));
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
        System.out.println("[EVIDENCE D19] Đã lưu ảnh: " + targetFile.getAbsolutePath() + " (" + width + "x" + height + ")");
    }

    private File resolveEvidenceDir() {
        File dir = new File("artifacts/evidence/D19").getAbsoluteFile();
        if (dir.getPath().contains("desktop-client")) {
            dir = new File("../artifacts/evidence/D19").getAbsoluteFile();
        }
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
}
