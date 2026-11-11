package vn.nhom6.railway.client;

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
import vn.nhom6.railway.client.network.ClusterStatusClient.DataSource;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.NodeStatus;
import vn.nhom6.railway.client.viewmodel.ClusterConnectionViewModel;

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
 * Kiểm thử tự động sinh 4 ảnh bằng chứng UI cho TASK TV1-D11:
 * <ol>
 * <li>TV1-D11-unknown-initial.png: Trạng thái UNKNOWN ban đầu.</li>
 * <li>TV1-D11-connected-leader-node2.png: Kết nối tới Node 2 (Leader).</li>
 * <li>TV1-D11-follower-hint.png: Kết nối tới Node 1 (Follower có Leader Hint là
 * node-3).</li>
 * <li>TV1-D11-offline-unknown.png: Mất kết nối, chuyển UNKNOWN bảo lưu
 * lastUpdated.</li>
 * </ol>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class ClusterStatusUiEvidenceTest {

    private Stage stage;
    private Scene scene;
    private ClusterConnectionViewModel viewModel;
    private MockClusterStatusClient mockClient;

    @BeforeAll
    static void initJavaFxToolkit() {
        boolean enabled = Boolean.getBoolean("ui.evidence") || Boolean.getBoolean("ui.smoke")
                || "1".equals(System.getenv("UI_EVIDENCE"));
        if (!enabled || GraphicsEnvironment.isHeadless()) {
            return;
        }

        try {
            Platform.startup(() -> {
            });
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        boolean enabled = Boolean.getBoolean("ui.evidence") || Boolean.getBoolean("ui.smoke")
                || "1".equals(System.getenv("UI_EVIDENCE"));
        Assumptions.assumeTrue(enabled,
                "ClusterStatusUiEvidenceTest chỉ chạy khi có cờ -Dui.evidence=true hoặc -Dui.smoke=true");
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(),
                "Môi trường headless không có màn hình hiển thị -> SKIPPED");

        mockClient = new MockClusterStatusClient();
        mockClient.setSimulatedDelayMs(0);
        viewModel = new ClusterConnectionViewModel(mockClient);
        viewModel.setSyncMode(false);
        ClientFactory.setClusterStatusClient(mockClient);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
                Parent root = loader.load();

                stage = new Stage();
                stage.setTitle("BẰNG CHỨNG GIAO DIỆN TASK TV1-D11 — CLUSTER STATUS BAR");
                scene = new Scene(root, 1020, 660);
                stage.setScene(scene);
                stage.show();
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
        if (viewModel != null) {
            viewModel.close();
        }
    }

    @Test
    @DisplayName("Chụp 4 kịch bản ảnh bằng chứng D11")
    void testCaptureAllEvidenceSnapshots() throws Exception {
        File evidenceDir = resolveEvidenceDir();

        // =====================================================================
        // Kịch bản 1: UNKNOWN initial (mới mở app)
        // =====================================================================
        runOnFxWait(() -> {
            ClientFactory.getClusterConnectionViewModel().markUnknown();
        });
        captureSnapshot(new File(evidenceDir, "TV1-D11-unknown-initial.png"));

        // =====================================================================
        // Kịch bản 2: Kết nối tới Node 2 (Leader)
        // =====================================================================
        runOnFxWait(() -> {
            NodeStatus node2Leader = new NodeStatus(
                    "node-2",
                    "LEADER",
                    5L,
                    "node-2",
                    48L,
                    46L,
                    46L,
                    true,
                    Instant.now());
            ClientFactory.getClusterConnectionViewModel().applyStatus(node2Leader, DataSource.REAL);
        });
        captureSnapshot(new File(evidenceDir, "TV1-D11-connected-leader-node2.png"));

        // =====================================================================
        // Kịch bản 3: Kết nối tới Node 1 (Follower, Leader Hint = Node 3)
        // =====================================================================
        runOnFxWait(() -> {
            NodeStatus node1Follower = new NodeStatus(
                    "node-1",
                    "FOLLOWER",
                    8L,
                    "node-3",
                    52L,
                    50L,
                    50L,
                    true,
                    Instant.now());
            ClientFactory.getClusterConnectionViewModel().applyStatus(node1Follower, DataSource.REAL);
        });
        captureSnapshot(new File(evidenceDir, "TV1-D11-follower-hint.png"));

        // =====================================================================
        // Kịch bản 4: Mất kết nối / Offline -> UNKNOWN (bảo lưu lastUpdated)
        // =====================================================================
        runOnFxWait(() -> {
            ClientFactory.getClusterConnectionViewModel().markOffline("ConnectException: Connection refused",
                    DataSource.REAL);
        });
        captureSnapshot(new File(evidenceDir, "TV1-D11-offline-unknown.png"));

        // Xác nhận cả 4 file ảnh đều tồn tại và dung lượng > 0
        assertTrue(new File(evidenceDir, "TV1-D11-unknown-initial.png").length() > 0);
        assertTrue(new File(evidenceDir, "TV1-D11-connected-leader-node2.png").length() > 0);
        assertTrue(new File(evidenceDir, "TV1-D11-follower-hint.png").length() > 0);
        assertTrue(new File(evidenceDir, "TV1-D11-offline-unknown.png").length() > 0);
    }

    private void runOnFxWait(Runnable action) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Platform.runLater(() -> {
            try {
                action.run();
            } finally {
                latch.countDown();
            }
        });
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        Thread.sleep(150); // Cho UI render ổn định
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
        System.out.println("[EVIDENCE D11] Đã lưu ảnh: " + targetFile.getAbsolutePath());
    }

    private File resolveEvidenceDir() {
        File dir = new File("artifacts/evidence/D11").getAbsoluteFile();
        if (dir.getPath().contains("desktop-client")) {
            dir = new File("../artifacts/evidence/D11").getAbsoluteFile();
        }
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
}
