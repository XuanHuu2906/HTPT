package vn.nhom6.railway.client.smoke;

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
import vn.nhom6.railway.client.controller.MainController;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import javax.imageio.ImageIO;
import java.awt.GraphicsEnvironment;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI Smoke Test có giao diện JavaFX thật kết nối với Fake TCP Server (TASK TV1-D10).
 * <p>
 * Quy tắc thực thi:
 * <ul>
 *   <li>Chỉ chạy khi có cờ {@code -Dui.smoke=true} hoặc biến môi trường {@code UI_SMOKE=1}.</li>
 *   <li>Nếu không có cờ hoặc môi trường headless không có màn hình hiển thị: test tự động SKIPPED
 *       (không tính PASS giả).</li>
 *   <li>Tiêu đề cửa sổ: {@value #WINDOW_TITLE} để ảnh chụp bằng chứng không gây hiểu lầm là Backend thật.</li>
 *   <li>Chụp ảnh snapshot giao diện lưu vào {@code artifacts/evidence/D10/TV1-D10-ui.png}.</li>
 * </ul>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class UiSmokeTest {

    private static final Logger LOGGER = Logger.getLogger(UiSmokeTest.class.getName());
    public static final String WINDOW_TITLE = "SMOKE — FAKE TCP SERVER (không phải Backend thật)";

    private FakeNodeServer fakeServer;
    private SocketBookingClient socketClient;
    private Stage stage;

    @BeforeAll
    static void initJavaFxToolkit() {
        boolean enabled = Boolean.getBoolean("ui.smoke") || "1".equals(System.getenv("UI_SMOKE"));
        if (!enabled || GraphicsEnvironment.isHeadless()) {
            return;
        }

        try {
            Platform.startup(() -> {});
            Platform.setImplicitExit(false);
        } catch (IllegalStateException ignored) {
            // Toolkit đã được khởi tạo trước đó
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        boolean enabled = Boolean.getBoolean("ui.smoke") || "1".equals(System.getenv("UI_SMOKE"));
        Assumptions.assumeTrue(enabled,
                "UiSmokeTest chỉ chạy khi được kích hoạt rõ ràng bằng -Dui.smoke=true hoặc UI_SMOKE=1");

        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(),
                "Môi trường headless không có màn hình hiển thị -> SKIPPED (tuân thủ quy tắc trung thực AGENTS.md)");

        fakeServer = new FakeNodeServer("node-1");
        int port = fakeServer.getPort();
        LOGGER.info("[UI-SMOKE-SETUP] Khởi động FakeNodeServer tại cổng " + port);

        socketClient = new SocketBookingClient("127.0.0.1", port, 2000, 3000, "client-ui-smoke");
        ClientFactory.setBookingClient(socketClient);
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

        ClientFactory.resetToConfiguredClient();

        if (socketClient != null) {
            socketClient.close();
        }
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    @Test
    @DisplayName("UI Smoke: Mở sơ đồ ghế -> Đặt vé ghế G02 -> Xác nhận BOOKED -> Gửi lại duplicate -> Chụp ảnh bằng chứng")
    void testJavaFxUiSmokeWithFakeTcpServer() throws Exception {
        AtomicReference<MainController> controllerRef = new AtomicReference<>();
        AtomicReference<Scene> sceneRef = new AtomicReference<>();
        CountDownLatch loadLatch = new CountDownLatch(1);

        AtomicReference<Throwable> loadError = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
                Parent root = loader.load();
                MainController controller = loader.getController();
                controllerRef.set(controller);

                stage = new Stage();
                stage.setTitle(WINDOW_TITLE);
                Scene scene = new Scene(root, 1080, 720);
                stage.setScene(scene);
                sceneRef.set(scene);

                stage.show();
                // Điều hướng sang màn hình sơ đồ chọn ghế
                controller.showSeatGridView();
            } catch (Throwable t) {
                LOGGER.severe("[UI-SMOKE] Lỗi khi nạp FXML giao diện: " + t.getMessage());
                loadError.set(t);
            } finally {
                loadLatch.countDown();
            }
        });

        assertTrue(loadLatch.await(10, TimeUnit.SECONDS), "Thời gian nạp giao diện JavaFX vượt quá 10s");
        if (loadError.get() != null) {
            fail("Lỗi khi nạp giao diện JavaFX: " + loadError.get().getMessage(), loadError.get());
        }
        MainController controller = controllerRef.get();
        assertNotNull(controller, "MainController phải được nạp thành công");

        // Đợi 500ms để layout JavaFX render hoàn tất
        Thread.sleep(500);

        String smokeRequestId = UUID.randomUUID().toString();
        LOGGER.info("[UI-SMOKE] Bắt đầu đặt vé ghế G02 qua Socket với requestId=" + smokeRequestId);

        // Đặt vé ghế G02 thông qua ViewModel kết nối tới fake TCP server
        BookingViewModel bookingVm = ClientFactory.getBookingViewModel();
        BookTicketRequestDto bookReq = new BookTicketRequestDto(
                smokeRequestId,
                "SE-001",
                "G02",
                "Khách Hàng Smoke JavaFX"
        );

        BookTicketResponseDto response1 = bookingVm.book(bookReq).get(5, TimeUnit.SECONDS);
        assertNotNull(response1);
        assertEquals("CONFIRMED", response1.getOutcome(), "G02 phải được xác nhận đặt vé thành công");
        assertNotNull(response1.getTicketId());
        assertFalse(response1.isMock(), "Phản hồi qua socket không được mang cờ MOCK");

        // Gửi lại cùng yêu cầu (duplicate test)
        LOGGER.info("[UI-SMOKE] Gửi lại cùng yêu cầu đặt vé (duplicate) với requestId=" + smokeRequestId);
        BookTicketResponseDto response2 = bookingVm.book(bookReq).get(5, TimeUnit.SECONDS);
        assertNotNull(response2);
        assertEquals("CONFIRMED", response2.getOutcome());
        assertEquals(response1.getTicketId(), response2.getTicketId(), "Duplicate phải trả về cùng ticketId");
        assertEquals(1, fakeServer.getActiveTicketCount("G02"), "Server giả chỉ được phép ghi nhận duy nhất 1 vé ACTIVE");

        // Chụp ảnh snapshot giao diện và lưu vào artifacts/evidence/D10/TV1-D10-ui.png
        CountDownLatch snapshotLatch = new CountDownLatch(1);
        AtomicReference<WritableImage> imageRef = new AtomicReference<>();

        Platform.runLater(() -> {
            try {
                Scene currentScene = sceneRef.get();
                if (currentScene != null && currentScene.getRoot() != null) {
                    WritableImage snapshot = currentScene.getRoot().snapshot(new SnapshotParameters(), null);
                    imageRef.set(snapshot);
                }
            } finally {
                snapshotLatch.countDown();
            }
        });

        assertTrue(snapshotLatch.await(5, TimeUnit.SECONDS), "Chụp snapshot UI vượt quá thời gian");
        WritableImage fxImage = imageRef.get();
        assertNotNull(fxImage, "Ảnh snapshot giao diện không được null");

        // Lưu ảnh ra file PNG
        File rootDir = new File(".").getAbsoluteFile();
        if ("desktop-client".equals(rootDir.getName())) {
            rootDir = rootDir.getParentFile();
        }
        File evidenceDir = new File(rootDir, "artifacts/evidence/D10");
        if (!evidenceDir.exists()) {
            evidenceDir.mkdirs();
        }
        File pngFile = new File(evidenceDir, "TV1-D10-ui.png");

        int width = (int) fxImage.getWidth();
        int height = (int) fxImage.getHeight();
        BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        PixelReader reader = fxImage.getPixelReader();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                bufferedImage.setRGB(x, y, reader.getArgb(x, y));
            }
        }

        ImageIO.write(bufferedImage, "png", pngFile);
        assertTrue(pngFile.exists() && pngFile.length() > 0, "File ảnh bằng chứng TV1-D10-ui.png phải tồn tại");
        LOGGER.info("[UI-SMOKE-PASS] Đã chụp ảnh snapshot thành công: " + pngFile.getAbsolutePath());
    }
}
