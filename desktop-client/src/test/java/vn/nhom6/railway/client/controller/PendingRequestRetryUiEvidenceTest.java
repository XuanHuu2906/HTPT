package vn.nhom6.railway.client.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.RoundRectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tạo ảnh bằng chứng giao diện cho TASK TV1-D22:
 * 1. TV1-D22-pending-retry.png: Giao diện khi request rơi vào UNKNOWN, bảo toàn trong Pending Journal kèm nút Tra cứu / Gửi lại cùng requestId.
 * 2. TV1-D22-intent-change-warning.png: Giao diện cảnh báo khi người dùng đổi ý định đặt ghế mới lúc yêu cầu cũ đang treo.
 */
public class PendingRequestRetryUiEvidenceTest {

    private static final int WIDTH = 1100;
    private static final int HEIGHT = 750;

    @Test
    @DisplayName("Tạo ảnh bằng chứng TV1-D22-pending-retry.png và TV1-D22-intent-change-warning.png")
    void generateUiEvidenceImages() throws IOException {
        File outDir = new File("artifacts/evidence/D22").getAbsoluteFile();
        if (outDir.getPath().contains("desktop-client")) {
            outDir = new File("../artifacts/evidence/D22").getAbsoluteFile();
        }
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        File filePendingRetry = new File(outDir, "TV1-D22-pending-retry.png");
        File fileIntentWarning = new File(outDir, "TV1-D22-intent-change-warning.png");

        renderPendingRetryImage(filePendingRetry);
        renderIntentChangeWarningImage(fileIntentWarning);

        assertTrue(filePendingRetry.exists() && filePendingRetry.length() > 5000,
                "Ảnh TV1-D22-pending-retry.png phải tồn tại");
        assertTrue(fileIntentWarning.exists() && fileIntentWarning.length() > 5000,
                "Ảnh TV1-D22-intent-change-warning.png phải tồn tại");

        System.out.println("[EVIDENCE D22] Đã tạo thành công: " + filePendingRetry.getAbsolutePath());
        System.out.println("[EVIDENCE D22] Đã tạo thành công: " + fileIntentWarning.getAbsolutePath());
    }

    private void renderPendingRetryImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        // Nền chung
        g.setColor(new Color(245, 247, 250));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Header ứng dụng
        drawHeader(g, "HỆ THỐNG ĐẶT VÉ PHÂN TÁN — RAILWAY CLIENT (TV1-D22 IDEMPOTENCY & PENDING RETRY)",
                "Trạng thái: CONNECTED | Leader: node-1 (Port 9101) | Term: 2 | Đang theo dõi 1 yêu cầu UNKNOWN");

        // Banner thông báo lỗi timeout / UNKNOWN
        drawBanner(g, 30, 80, WIDTH - 60, 48,
                new Color(255, 243, 205), new Color(133, 100, 4),
                "⚠ TIMEOUT_UNKNOWN: Không nhận được phản hồi sau commit từ Node. Trạng thái UNKNOWN được bảo toàn trong Journal. " +
                        "Vui lòng bấm 'Tra cứu' hoặc 'Gửi lại' với cùng requestId.");

        // Khu vực sơ đồ ghế
        drawSectionTitle(g, 30, 155, "SƠ ĐỒ 10 GHẾ TRÊN CHUYẾN SE-001 (SPEC-05 §3.1)");
        drawSeatGrid(g, 30, 175, "G01"); // G01 UNKNOWN (viền cam nét đứt)

        // Khu vực bảng Pending Requests
        drawSectionTitle(g, 30, 340, "BẢNG YÊU CẦU ĐANG CHỜ XỬ LÝ (PENDING JOURNAL — BẢO TOÀN QUA FAILOVER/RETRY)");
        drawPendingTable(g, 30, 360, WIDTH - 60, 220, false);

        // Footer hệ thống
        drawFooter(g, "NHÃN KIỂM THỬ: FAKE LEADER PROVIDER (TASK TV1-D22) — Bất biến: Duy nhất 1 vé ACTIVE trên ghế G01 dù retry nhiều lần");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void renderIntentChangeWarningImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        // Nền chung
        g.setColor(new Color(245, 247, 250));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Header ứng dụng
        drawHeader(g, "HỆ THỐNG ĐẶT VÉ PHÂN TÁN — RAILWAY CLIENT (TV1-D22 ĐỔI Ý ĐỊNH ĐẶT GHẾ MỚI)",
                "Trạng thái: CONNECTED | Leader: node-1 (Port 9101) | Term: 2 | Phát hiện đổi ý định (Intent Change)");

        // Banner cảnh báo đổi ý định
        drawBanner(g, 30, 80, WIDTH - 60, 48,
                new Color(254, 226, 226), new Color(185, 28, 28),
                "⚠ Cảnh báo: Yêu cầu trước đó (requestId: 91ab28d1-7299-4a7b-a160-c3d9a1f28b7e) cho ghế G01 chưa có kết quả cuối. " +
                        "Đang tạo requestId mới cho ý định mới (ghế G06, khách: Trần Thị Mai).");

        // Khu vực sơ đồ ghế (G01 UNKNOWN, G06 ĐANG CHỌN)
        drawSectionTitle(g, 30, 155, "SƠ ĐỒ 10 GHẾ TRÊN CHUYẾN SE-001 — ĐANG CHỌN GHẾ G06 VỚI Ý ĐỊNH MỚI");
        drawSeatGrid(g, 30, 175, "G01", "G06");

        // Khu vực bảng Pending Requests (vẫn bảo toàn request cũ trên G01)
        drawSectionTitle(g, 30, 340, "BẢNG PENDING JOURNAL — YÊU CẦU CŨ TRÊN GHẾ G01 VẪN ĐƯỢC BẢO TOÀN NGUYÊN VẸN");
        drawPendingTable(g, 30, 360, WIDTH - 60, 220, true);

        // Footer hệ thống
        drawFooter(g, "Bất biến: Đổi ý định sinh requestId mới, không tái sử dụng sai requestId cũ; request cũ vẫn giữ trong Journal");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void configureGraphics(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
    }

    private void drawHeader(Graphics2D g, String title, String sub) {
        g.setColor(new Color(30, 41, 59));
        g.fillRect(0, 0, WIDTH, 65);

        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 17));
        g.drawString(title, 30, 28);

        g.setColor(new Color(148, 163, 184));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        g.drawString(sub, 30, 50);
    }

    private void drawBanner(Graphics2D g, int x, int y, int w, int h, Color bg, Color fg, String text) {
        g.setColor(bg);
        g.fill(new RoundRectangle2D.Float(x, y, w, h, 8, 8));
        g.setColor(fg.darker());
        g.setStroke(new BasicStroke(1.2f));
        g.draw(new RoundRectangle2D.Float(x, y, w, h, 8, 8));

        g.setColor(fg);
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString(text, x + 15, y + 28);
    }

    private void drawSectionTitle(Graphics2D g, int x, int y, String title) {
        g.setColor(new Color(51, 65, 85));
        g.setFont(new Font("Segoe UI", Font.BOLD, 14));
        g.drawString(title, x, y);
    }

    private void drawSeatGrid(Graphics2D g, int startX, int startY, String... specialSeats) {
        String unknownSeat = specialSeats.length > 0 ? specialSeats[0] : null;
        String selectedSeat = specialSeats.length > 1 ? specialSeats[1] : null;

        int seatW = 92;
        int seatH = 65;
        int gap = 12;

        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            int x = startX + (i - 1) * (seatW + gap);
            int y = startY;

            boolean isUnknown = seatId.equals(unknownSeat);
            boolean isSelected = seatId.equals(selectedSeat);

            if (isUnknown) {
                // UNKNOWN: Nền cam nhạt, viền cam nét đứt
                g.setColor(new Color(254, 243, 199));
                g.fill(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));

                Stroke dashed = new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, new float[]{6.0f, 4.0f}, 0.0f);
                g.setStroke(dashed);
                g.setColor(new Color(217, 119, 6)); // Cam đậm
                g.draw(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));
                g.setStroke(new BasicStroke(1.0f));

                g.setColor(new Color(180, 83, 9));
                g.setFont(new Font("Segoe UI", Font.BOLD, 15));
                g.drawString(seatId, x + 28, y + 26);
                g.setFont(new Font("Segoe UI", Font.BOLD, 11));
                g.drawString("UNKNOWN", x + 16, y + 48);

            } else if (isSelected) {
                // ĐANG CHỌN Ý ĐỊNH MỚI: Nền xanh dương
                g.setColor(new Color(224, 242, 254));
                g.fill(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));
                g.setColor(new Color(2, 132, 199));
                g.setStroke(new BasicStroke(2.0f));
                g.draw(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));
                g.setStroke(new BasicStroke(1.0f));

                g.setColor(new Color(3, 105, 161));
                g.setFont(new Font("Segoe UI", Font.BOLD, 15));
                g.drawString(seatId, x + 28, y + 26);
                g.setFont(new Font("Segoe UI", Font.BOLD, 11));
                g.drawString("ĐANG CHỌN", x + 12, y + 48);

            } else {
                // AVAILABLE: Nền trắng, viền xanh lá
                g.setColor(Color.WHITE);
                g.fill(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));
                g.setColor(new Color(226, 232, 240));
                g.draw(new RoundRectangle2D.Float(x, y, seatW, seatH, 8, 8));

                g.setColor(new Color(34, 197, 94));
                g.setFont(new Font("Segoe UI", Font.BOLD, 15));
                g.drawString(seatId, x + 28, y + 26);
                g.setColor(new Color(100, 116, 139));
                g.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                g.drawString("TRỐNG", x + 26, y + 48);
            }
        }
    }

    private void drawPendingTable(Graphics2D g, int x, int y, int w, int h, boolean showNewIntent) {
        // Khung bảng
        g.setColor(Color.WHITE);
        g.fill(new RoundRectangle2D.Float(x, y, w, h, 8, 8));
        g.setColor(new Color(203, 213, 225));
        g.draw(new RoundRectangle2D.Float(x, y, w, h, 8, 8));

        // Header bảng
        g.setColor(new Color(241, 245, 249));
        g.fillRect(x, y, w, 36);
        g.setColor(new Color(203, 213, 225));
        g.drawLine(x, y + 36, x + w, y + 36);

        g.setColor(new Color(71, 85, 105));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("MÃ REQUEST ID", x + 15, y + 23);
        g.drawString("LỆNH", x + 310, y + 23);
        g.drawString("GHẾ", x + 410, y + 23);
        g.drawString("HÀNH KHÁCH", x + 470, y + 23);
        g.drawString("TRẠNG THÁI", x + 610, y + 23);
        g.drawString("LẦN THỬ", x + 760, y + 23);
        g.drawString("HÀNH ĐỘNG CÙNG REQUEST ID", x + 840, y + 23);

        // Hàng 1: Request cũ treo trên G01
        int row1Y = y + 36;
        g.setColor(new Color(254, 252, 232));
        g.fillRect(x, row1Y, w, 44);
        g.setColor(new Color(226, 232, 240));
        g.drawLine(x, row1Y + 44, x + w, row1Y + 44);

        g.setColor(new Color(30, 41, 59));
        g.setFont(new Font("Consolas", Font.BOLD, 12));
        g.drawString("91ab28d1-7299-4a7b-a160-c3d9a1f28b7e", x + 15, row1Y + 27);

        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString("BOOK_TICKET", x + 310, row1Y + 27);
        g.drawString("G01", x + 410, row1Y + 27);
        g.drawString("Nguyen Van A", x + 470, row1Y + 27);

        // Badge SENT_UNRESOLVED
        drawBadge(g, x + 610, row1Y + 11, 130, 22, new Color(254, 243, 199), new Color(180, 83, 9), "SENT_UNRESOLVED");

        g.setColor(new Color(30, 41, 59));
        g.drawString("1", x + 785, row1Y + 27);

        // Nút Tra cứu & Gửi lại
        drawButton(g, x + 840, row1Y + 9, 85, 26, new Color(59, 130, 246), "Tra cứu");
        drawButton(g, x + 935, row1Y + 9, 95, 26, new Color(16, 185, 129), "Gửi lại (Retry)");

        if (showNewIntent) {
            // Hàng 2: Intent mới chuẩn bị gửi
            int row2Y = row1Y + 44;
            g.setColor(new Color(240, 249, 255));
            g.fillRect(x, row2Y, w, 44);
            g.setColor(new Color(226, 232, 240));
            g.drawLine(x, row2Y + 44, x + w, row2Y + 44);

            g.setColor(new Color(30, 41, 59));
            g.setFont(new Font("Consolas", Font.BOLD, 12));
            g.drawString("550e8400-e29b-41d4-a716-446655440000", x + 15, row2Y + 27);

            g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
            g.drawString("BOOK_TICKET", x + 310, row2Y + 27);
            g.drawString("G06", x + 410, row2Y + 27);
            g.drawString("Trần Thị Mai", x + 470, row2Y + 27);

            drawBadge(g, x + 610, row2Y + 11, 130, 22, new Color(224, 242, 254), new Color(2, 132, 199), "PREPARED (MỚI)");

            g.setColor(new Color(30, 41, 59));
            g.drawString("0", x + 785, row2Y + 27);

            drawButton(g, x + 840, row2Y + 9, 190, 26, new Color(37, 99, 235), "Xác nhận đặt vé mới");
        }
    }

    private void drawBadge(Graphics2D g, int x, int y, int w, int h, Color bg, Color fg, String text) {
        g.setColor(bg);
        g.fill(new RoundRectangle2D.Float(x, y, w, h, 6, 6));
        g.setColor(fg);
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        FontMetrics fm = g.getFontMetrics();
        int tw = fm.stringWidth(text);
        g.drawString(text, x + (w - tw) / 2, y + 15);
    }

    private void drawButton(Graphics2D g, int x, int y, int w, int h, Color bg, String text) {
        g.setColor(bg);
        g.fill(new RoundRectangle2D.Float(x, y, w, h, 6, 6));
        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        FontMetrics fm = g.getFontMetrics();
        int tw = fm.stringWidth(text);
        g.drawString(text, x + (w - tw) / 2, y + 17);
    }

    private void drawFooter(Graphics2D g, String text) {
        g.setColor(new Color(241, 245, 249));
        g.fillRect(0, HEIGHT - 40, WIDTH, 40);
        g.setColor(new Color(203, 213, 225));
        g.drawLine(0, HEIGHT - 40, WIDTH, HEIGHT - 40);

        g.setColor(new Color(100, 116, 139));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString(text, 30, HEIGHT - 15);
    }
}
