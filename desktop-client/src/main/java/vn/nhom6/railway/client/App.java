package vn.nhom6.railway.client;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.stage.Stage;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Lớp khởi chạy ứng dụng JavaFX Desktop Client (Bán vé tàu cao tốc).
 * Giai đoạn D01: Chạy màn hình giao diện MOCK với 10 ghế G01–G10.
 */
public class App extends Application {

    private static final int DEFAULT_WIDTH = 1020;
    private static final int DEFAULT_HEIGHT = 660;

    @Override
    public void start(Stage primaryStage) throws IOException {
        URL fxmlLocation = getClass().getResource("/fxml/main.fxml");
        if (fxmlLocation == null) {
            throw new IllegalStateException("Không tìm thấy tệp giao diện /fxml/main.fxml trong resources!");
        }

        FXMLLoader loader = new FXMLLoader(fxmlLocation);
        Parent root = loader.load();

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);

        primaryStage.setTitle("Hệ thống Bán Vé Tàu Cao Tốc - SE-001 [MOCK - D01]");
        primaryStage.setScene(scene);
        primaryStage.setMinWidth(850);
        primaryStage.setMinHeight(550);
        primaryStage.show();

        // Tự động lưu ảnh chụp giao diện làm bằng chứng kiểm thử D01
        Platform.runLater(() -> {
            captureEvidenceSnapshot(scene);
            if (getParameters().getUnnamed().contains("--exit-after-capture")
                    || Boolean.getBoolean("exitAfterCapture")) {
                primaryStage.close();
            }
        });
    }

    /**
     * Chụp ảnh màn hình ứng dụng đang chạy và lưu vào artifacts/evidence/D01/TV1-D01-javafx-running.png
     */
    private void captureEvidenceSnapshot(Scene scene) {
        try {
            WritableImage image = scene.snapshot(null);
            int width = (int) image.getWidth();
            int height = (int) image.getHeight();

            BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            PixelReader reader = image.getPixelReader();
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    bufferedImage.setRGB(x, y, reader.getArgb(x, y));
                }
            }

            // Hỗ trợ cả khi chạy từ thư mục gốc HTPT hoặc từ module desktop-client
            File targetFile = new File("artifacts/evidence/D01/TV1-D01-javafx-running.png");
            if (!targetFile.getParentFile().exists() && new File("../artifacts").exists()) {
                targetFile = new File("../artifacts/evidence/D01/TV1-D01-javafx-running.png");
            }

            File parent = targetFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            ImageIO.write(bufferedImage, "png", targetFile);
            System.out.println("[EVIDENCE D01] Đã lưu ảnh chụp giao diện tại: " + targetFile.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("[EVIDENCE D01] Không thể lưu ảnh chụp: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
