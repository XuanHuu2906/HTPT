package vn.nhom6.railway.client;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.PixelReader;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import vn.nhom6.railway.client.network.ClientConfig;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Lớp khởi chạy ứng dụng JavaFX Desktop Client (Hệ thống bán vé tàu cao tốc SE-001 — Nhóm 6).
 * <p>
 * Bất biến chế độ RELEASE ({@code release.mode=true} — TASK TV1-D28):
 * <ul>
 *   <li>Thẩm tra cấu hình fail-fast trước khi nạp luồng giao diện đặt vé.</li>
 *   <li>Nếu cấu hình không hợp lệ (yêu cầu giả lập, chứa IP placeholder, thiếu endpoint...),
 *       ứng dụng KHÔNG khởi động luồng đặt vé, KHÔNG chuyển ngầm sang dữ liệu giả, mà hiển thị
 *       màn hình thông báo lỗi cấu hình rõ ràng.</li>
 * </ul>
 */
public class App extends Application {

    public static final int DEFAULT_WIDTH = 1080;
    public static final int DEFAULT_HEIGHT = 740;
    public static final int MIN_WIDTH = 1040;
    public static final int MIN_HEIGHT = 720;

    @Override
    public void start(Stage primaryStage) throws IOException {
        ClientConfig cfg;
        try {
            cfg = ClientConfig.loadDefault();
            cfg.validateForStartup();
            ClientFactory.initializeForConfig(cfg);
        } catch (RuntimeException configError) {
            showConfigErrorScreen(primaryStage, configError);
            return;
        }

        var fxmlLocation = getClass().getResource("/fxml/main.fxml");
        if (fxmlLocation == null) {
            throw new IllegalStateException("Không tìm thấy tệp giao diện /fxml/main.fxml trong resources!");
        }

        FXMLLoader loader = new FXMLLoader(fxmlLocation);
        Parent root = loader.load();

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);

        primaryStage.setTitle(buildWindowTitle(cfg));
        primaryStage.setScene(scene);
        primaryStage.setMinWidth(MIN_WIDTH);
        primaryStage.setMinHeight(MIN_HEIGHT);
        primaryStage.setOnCloseRequest(event -> {
            Platform.exit();
            System.exit(0);
        });
        primaryStage.show();

        final ClientConfig finalCfg = cfg;
        Platform.runLater(() -> {
            boolean exitAfter = getParameters().getUnnamed().contains("--exit-after-capture")
                    || Boolean.getBoolean("exitAfterCapture");
            if (Boolean.getBoolean("ui.evidence")) {
                String defaultPath = finalCfg.isReleaseMode()
                        ? "artifacts/evidence/D28/TV1-D28-ui.png"
                        : "artifacts/evidence/D01/TV1-D01-javafx-running.png";
                String targetPath = System.getProperty("ui.evidence.path", defaultPath);
                captureNodeSnapshot(root, targetPath);
            } else if (Boolean.getBoolean("d01.capture")) {
                captureNodeSnapshot(root, "artifacts/evidence/D01/TV1-D01-javafx-running.png");
            }
            if (exitAfter) {
                primaryStage.close();
            }
        });
    }

    public static String buildWindowTitle(ClientConfig cfg) {
        if (Boolean.getBoolean("ui.evidence.fake")) {
            return "RELEASE UI — KẾT NỐI FAKE (không phải giao dịch thật)";
        }
        if (cfg != null && cfg.isReleaseMode()) {
            return "Hệ thống Bán Vé Tàu Cao Tốc SE-001 — RELEASE v"
                    + cfg.getReleaseVersion() + " [" + cfg.getProfile().toUpperCase() + "]";
        }
        if (cfg != null && cfg.isSocketMode()) {
            return "Hệ thống Bán Vé Tàu Cao Tốc - SE-001 [CỤM 5 NODE - " + cfg.getProfile().toUpperCase() + "]";
        }
        return "Hệ thống Bán Vé Tàu Cao Tốc - SE-001 [DEV]";
    }

    private void showConfigErrorScreen(Stage primaryStage, Throwable configError) {
        Parent errorRoot = buildConfigErrorRoot(configError, () -> {
            Platform.exit();
            System.exit(1);
        });
        Scene errorScene = new Scene(errorRoot, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        var cssUrl = App.class.getResource("/css/railway.css");
        if (cssUrl != null) {
            errorScene.getStylesheets().add(cssUrl.toExternalForm());
        }
        primaryStage.setTitle("Hệ thống Bán Vé Tàu Cao Tốc SE-001 — LỖI CẤU HÌNH");
        primaryStage.setScene(errorScene);
        primaryStage.setMinWidth(MIN_WIDTH);
        primaryStage.setMinHeight(MIN_HEIGHT);
        primaryStage.show();
    }

    /**
     * Dựng giao diện thông báo lỗi cấu hình fail-fast (không nạp màn hình đặt vé, không treo ứng dụng).
     */
    public static Parent buildConfigErrorRoot(Throwable configError, Runnable onCloseAction) {
        VBox root = new VBox(16);
        root.setId("configErrorRoot");
        root.getStyleClass().add("config-error-root");
        root.setAlignment(Pos.CENTER);

        VBox card = new VBox(14);
        card.setId("configErrorCard");
        card.getStyleClass().add("config-error-card");

        Label title = new Label("⛔ KHÔNG THỂ KHỞI ĐỘNG LUỒNG ĐẶT VÉ — CẤU HÌNH KHÔNG HỢP LỆ");
        title.setId("configErrorTitleLabel");
        title.getStyleClass().add("config-error-title");
        title.setWrapText(true);

        String detailText = (configError != null && configError.getMessage() != null && !configError.getMessage().isBlank())
                ? configError.getMessage()
                : "Phát hiện tham số cấu hình không hợp lệ. Vui lòng kiểm tra tệp client.properties / client-lan.properties.";
        Label detail = new Label(detailText);
        detail.setId("configErrorDetailLabel");
        detail.getStyleClass().add("config-error-detail");
        detail.setWrapText(true);

        Label hint = new Label("Ứng dụng từ chối chuyển sang dữ liệu giả lập trong chế độ RELEASE. "
                + "Vui lòng hiệu chỉnh đúng cấu hình kết nối TCP Socket thật và khởi động lại.");
        hint.getStyleClass().add("search-msg-muted");
        hint.setWrapText(true);

        Button closeBtn = new Button("Đóng ứng dụng");
        closeBtn.setId("configErrorCloseBtn");
        closeBtn.getStyleClass().add("btn-primary");
        if (onCloseAction != null) {
            closeBtn.setOnAction(e -> onCloseAction.run());
        }

        card.getChildren().addAll(title, detail, hint, closeBtn);
        root.getChildren().add(card);
        return root;
    }

    /**
     * Chụp ảnh cây giao diện JavaFX bằng {@link Node#snapshot} + {@link ImageIO#write}.
     */
    public static void captureNodeSnapshot(Node node, String relativeTargetPath) {
        try {
            WritableImage image = node.snapshot(null, null);
            int width = (int) image.getWidth();
            int height = (int) image.getHeight();
            if (width <= 0 || height <= 0) {
                return;
            }

            BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            PixelReader reader = image.getPixelReader();
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    bufferedImage.setRGB(x, y, reader.getArgb(x, y));
                }
            }

            File targetFile = new File(relativeTargetPath);
            if (!targetFile.isAbsolute() && !targetFile.getParentFile().exists() && new File("../artifacts").exists()) {
                targetFile = new File("../" + relativeTargetPath);
            }

            File parent = targetFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            ImageIO.write(bufferedImage, "png", targetFile);
        } catch (Exception e) {
            System.getLogger(App.class.getName())
                    .log(System.Logger.Level.WARNING, "Không thể lưu ảnh chụp giao diện: " + e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        var j = ClientFactory.getJournal();
        if (j != null && !j.isClosed()) {
            try {
                j.close();
            } catch (Exception ignored) {
            }
        }
        super.stop();
        System.exit(0);
    }

    public static void main(String[] args) {
        launch(args);
    }
}
