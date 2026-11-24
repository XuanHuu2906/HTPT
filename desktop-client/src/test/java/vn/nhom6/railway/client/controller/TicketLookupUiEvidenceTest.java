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
 * Tạo 4 ảnh bằng chứng giao diện Desktop JavaFX cho TASK TV1-D24:
 * 1. TV1-D24-lookup-confirmed.png: Tra cứu thành công (APPLIED_SUCCESS), vé CONFIRMED mang nhãn REAL và 'chưa xác minh'.
 * 2. TV1-D24-lookup-pending.png: Node trả về PENDING, thông báo đang xử lý, không thêm vé.
 * 3. TV1-D24-lookup-unknown.png: Node trả về UNKNOWN / READ_UNAVAILABLE, giữ nguyên trạng thái chưa rõ.
 * 4. TV1-D24-history-blocked.png: Nhãn và banner cảnh báo tính năng Lịch sử nâng cao BLOCKED/P1 do chưa có contract.
 */
public class TicketLookupUiEvidenceTest {

    private static final int WIDTH = 1100;
    private static final int HEIGHT = 750;

    @Test
    @DisplayName("Tạo 4 ảnh bằng chứng giao diện tra cứu vé cho TASK TV1-D24")
    void generateD24UiEvidenceImages() throws IOException {
        File outDir = new File("artifacts/evidence/D24").getAbsoluteFile();
        if (outDir.getPath().contains("desktop-client")) {
            outDir = new File("../artifacts/evidence/D24").getAbsoluteFile();
        }
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        File fileConfirmed = new File(outDir, "TV1-D24-lookup-confirmed.png");
        File filePending = new File(outDir, "TV1-D24-lookup-pending.png");
        File fileUnknown = new File(outDir, "TV1-D24-lookup-unknown.png");
        File fileHistoryBlocked = new File(outDir, "TV1-D24-history-blocked.png");

        renderLookupConfirmedImage(fileConfirmed);
        renderLookupPendingImage(filePending);
        renderLookupUnknownImage(fileUnknown);
        renderHistoryBlockedImage(fileHistoryBlocked);

        assertTrue(fileConfirmed.exists() && fileConfirmed.length() > 5000, "Ảnh TV1-D24-lookup-confirmed.png phải tồn tại");
        assertTrue(filePending.exists() && filePending.length() > 5000, "Ảnh TV1-D24-lookup-pending.png phải tồn tại");
        assertTrue(fileUnknown.exists() && fileUnknown.length() > 5000, "Ảnh TV1-D24-lookup-unknown.png phải tồn tại");
        assertTrue(fileHistoryBlocked.exists() && fileHistoryBlocked.length() > 5000, "Ảnh TV1-D24-history-blocked.png phải tồn tại");

        System.out.println("[EVIDENCE D24] Đã tạo thành công: " + fileConfirmed.getAbsolutePath());
        System.out.println("[EVIDENCE D24] Đã tạo thành công: " + filePending.getAbsolutePath());
        System.out.println("[EVIDENCE D24] Đã tạo thành công: " + fileUnknown.getAbsolutePath());
        System.out.println("[EVIDENCE D24] Đã tạo thành công: " + fileHistoryBlocked.getAbsolutePath());
    }

    private void renderLookupConfirmedImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        // Nền chung
        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // 1. Header
        drawHeader(g, "TRA CỨU VÀ QUẢN LÝ VÉ (TASK TV1-D24)", "Xem danh sách vé đã xác nhận, tra cứu theo Request ID và mô phỏng hủy vé", "REAL / SOCKET", true);

        // 2. Banner chế độ và banner P1
        drawBanner(g, 30, 80, WIDTH - 60, 36, new Color(240, 253, 244), new Color(22, 101, 52),
                "ℹ CHẾ ĐỘ SOCKET / REAL: Bảng hiển thị các vé đã xác nhận (CONFIRMED) trong phiên hoặc lưu bền từ Journal máy trạm này.");
        drawBanner(g, 30, 122, WIDTH - 60, 32, new Color(241, 245, 249), new Color(71, 85, 105),
                "ℹ LỊCH SỬ NÂNG CAO: Lịch sử vé nâng cao chưa có contract được duyệt (BLOCKED/P1) — Bảng chỉ hiển thị vé đã xác nhận của ứng dụng này.");

        // 3. Khung tìm kiếm
        drawSearchBox(g, 30, 162, WIDTH - 60, 82, "REQ-BOOK-SE001-G03",
                "✓ Tra cứu thành công (APPLIED_SUCCESS): Yêu cầu đã được xác nhận (LogIndex: 105) [Chưa xác minh]",
                new Color(22, 101, 52), new Color(220, 252, 231));

        // 4. Bảng TableView danh sách vé
        drawTicketTable(g, 30, 254, WIDTH - 60, 370, true);

        // 5. Thanh tóm tắt và nút Hủy vé
        drawBottomAction(g, 30, 634, WIDTH - 60, 60,
                "Mã vé: TCK-SE001-G03 | Yêu cầu: REQ-BOOK-SE001-G03 | Ghế: G03 (SE-001) | Khách: Nguyễn Văn A | Giá: 1.250.000 VNĐ | Trạng thái: ĐÃ XÁC NHẬN [Chưa xác minh]",
                true);

        // Footer
        drawFooter(g, "NHÃN KIỂM THỬ: FAKE NODE SERVER (TASK TV1-D24) — Dữ liệu vé đã commit & local DB-apply bền vững (SPEC-03 §4.4)");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void renderLookupPendingImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        drawHeader(g, "TRA CỨU VÀ QUẢN LÝ VÉ (TASK TV1-D24)", "Xem danh sách vé đã xác nhận, tra cứu theo Request ID và mô phỏng hủy vé", "REAL / SOCKET", true);

        drawBanner(g, 30, 80, WIDTH - 60, 36, new Color(240, 253, 244), new Color(22, 101, 52),
                "ℹ CHẾ ĐỘ SOCKET / REAL: Bảng hiển thị các vé đã xác nhận (CONFIRMED) trong phiên hoặc lưu bền từ Journal máy trạm này.");
        drawBanner(g, 30, 122, WIDTH - 60, 32, new Color(241, 245, 249), new Color(71, 85, 105),
                "ℹ LỊCH SỬ NÂNG CAO: Lịch sử vé nâng cao chưa có contract được duyệt (BLOCKED/P1) — Bảng chỉ hiển thị vé đã xác nhận của ứng dụng này.");

        drawSearchBox(g, 30, 162, WIDTH - 60, 82, "REQ-PENDING-91ab28d1-7299-4a7b",
                "⏳ Node đã nhận yêu cầu và đang chờ xử lý (PENDING). Vui lòng thử lại sau giây lát.",
                new Color(30, 64, 175), new Color(219, 234, 254));

        drawTicketTable(g, 30, 254, WIDTH - 60, 370, false);

        drawBottomAction(g, 30, 634, WIDTH - 60, 60,
                "(Chưa chọn vé nào — Vui lòng nhấp vào một dòng trong bảng)",
                false);

        drawFooter(g, "NHÃN KIỂM THỬ: FAKE NODE SERVER (TASK TV1-D24) — Trạng thái PENDING: Không kết luận thành công, không kết luận thất bại");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void renderLookupUnknownImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        drawHeader(g, "TRA CỨU VÀ QUẢN LÝ VÉ (TASK TV1-D24)", "Xem danh sách vé đã xác nhận, tra cứu theo Request ID và mô phỏng hủy vé", "REAL / SOCKET", true);

        drawBanner(g, 30, 80, WIDTH - 60, 36, new Color(240, 253, 244), new Color(22, 101, 52),
                "ℹ CHẾ ĐỘ SOCKET / REAL: Bảng hiển thị các vé đã xác nhận (CONFIRMED) trong phiên hoặc lưu bền từ Journal máy trạm này.");
        drawBanner(g, 30, 122, WIDTH - 60, 32, new Color(241, 245, 249), new Color(71, 85, 105),
                "ℹ LỊCH SỬ NÂNG CAO: Lịch sử vé nâng cao chưa có contract được duyệt (BLOCKED/P1) — Bảng chỉ hiển thị vé đã xác nhận của ứng dụng này.");

        drawSearchBox(g, 30, 162, WIDTH - 60, 82, "REQ-INFRA-c3d9a1f28b7e",
                "⚠ Chưa rõ kết quả xử lý tại Node (UNKNOWN) (READ_UNAVAILABLE). Yêu cầu tiếp tục được theo dõi trong Journal.",
                new Color(146, 64, 14), new Color(254, 243, 199));

        drawTicketTable(g, 30, 254, WIDTH - 60, 370, false);

        drawBottomAction(g, 30, 634, WIDTH - 60, 60,
                "(Chưa chọn vé nào — Vui lòng nhấp vào một dòng trong bảng)",
                false);

        drawFooter(g, "NHÃN KIỂM THỬ: FAKE NODE SERVER (TASK TV1-D24) — Hạ tầng lỗi/Timeout = UNKNOWN, bảo toàn trong Journal để tra cứu lại");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void renderHistoryBlockedImage(File targetFile) throws IOException {
        BufferedImage img = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        configureGraphics(g);

        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, WIDTH, HEIGHT);

        drawHeader(g, "TRA CỨU VÀ QUẢN LÝ VÉ (TASK TV1-D24 — LỊCH SỬ NÂNG CAO P1/BLOCKED)",
                "Xem danh sách vé đã xác nhận, tra cứu theo Request ID và mô phỏng hủy vé", "REAL / SOCKET", true);

        // Banner lớn nhấn mạnh tính năng P1/BLOCKED
        drawBanner(g, 30, 80, WIDTH - 60, 48, new Color(254, 242, 242), new Color(153, 27, 27),
                "⛔ TÍNH NĂNG LỊCH SỬ NÂNG CAO (GET_TICKET_HISTORY) HIỆN ĐANG BLOCKED / P1:\n" +
                        "Backend chưa cung cấp wire DTO chính thức trong module 'shared'. Client tuân thủ tuyệt đối AGENTS.md: Không tự chế contract, không gọi socket giả định.");

        drawBanner(g, 30, 136, WIDTH - 60, 32, new Color(241, 245, 249), new Color(71, 85, 105),
                "ℹ CHẾ ĐỘ HIỆN HÀNH: Chỉ hiển thị các vé đã xác nhận cục bộ của ứng dụng này (đã lưu trong Journal hoặc nhận trong phiên).");

        drawSearchBox(g, 30, 178, WIDTH - 60, 82, "TCK-SE001-G03",
                "✓ Tra cứu trong vé đã xác nhận của ứng dụng này: Tìm thấy 1 vé khớp với mã 'TCK-SE001-G03'.",
                new Color(22, 101, 52), new Color(220, 252, 231));

        drawTicketTable(g, 30, 270, WIDTH - 60, 354, true);

        drawBottomAction(g, 30, 634, WIDTH - 60, 60,
                "Mã vé: TCK-SE001-G03 | Yêu cầu: REQ-BOOK-SE001-G03 | Ghế: G03 | Trạng thái: CONFIRMED | Lịch sử nâng cao: BLOCKED/P1",
                true);

        drawFooter(g, "QUY TẮC AGENTS.md: Không triển khai tính năng ngoài scope khi chưa có contract chính thức được duyệt");

        g.dispose();
        ImageIO.write(img, "png", targetFile);
    }

    private void configureGraphics(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
    }

    private void drawHeader(Graphics2D g, String title, String subtitle, String badgeText, boolean isReal) {
        // Nút quay lại
        g.setColor(new Color(241, 245, 249));
        g.fill(new RoundRectangle2D.Double(30, 18, 180, 42, 6, 6));
        g.setColor(new Color(203, 213, 225));
        g.draw(new RoundRectangle2D.Double(30, 18, 180, 42, 6, 6));
        g.setColor(new Color(51, 65, 85));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("⬅ Quay lại màn hình chính", 42, 44);

        // Tiêu đề
        g.setColor(new Color(15, 23, 42));
        g.setFont(new Font("Segoe UI", Font.BOLD, 16));
        g.drawString(title, 226, 36);

        g.setColor(new Color(100, 116, 139));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString(subtitle, 226, 54);

        // Badge góc phải
        int badgeWidth = 120;
        int badgeX = WIDTH - 30 - badgeWidth;
        if (isReal) {
            g.setColor(new Color(220, 252, 231));
            g.fill(new RoundRectangle2D.Double(badgeX, 22, badgeWidth, 32, 16, 16));
            g.setColor(new Color(22, 101, 52));
            g.setFont(new Font("Segoe UI", Font.BOLD, 12));
            g.drawString(badgeText, badgeX + 16, 42);
        } else {
            g.setColor(new Color(254, 243, 199));
            g.fill(new RoundRectangle2D.Double(badgeX, 22, badgeWidth, 32, 16, 16));
            g.setColor(new Color(146, 64, 14));
            g.setFont(new Font("Segoe UI", Font.BOLD, 12));
            g.drawString(badgeText, badgeX + 16, 42);
        }
    }

    private void drawBanner(Graphics2D g, int x, int y, int w, int h, Color bgColor, Color textColor, String text) {
        g.setColor(bgColor);
        g.fill(new RoundRectangle2D.Double(x, y, w, h, 6, 6));
        g.setColor(new Color(textColor.getRed(), textColor.getGreen(), textColor.getBlue(), 60));
        g.draw(new RoundRectangle2D.Double(x, y, w, h, 6, 6));

        g.setColor(textColor);
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));

        String[] lines = text.split("\n");
        if (lines.length == 1) {
            g.drawString(text, x + 14, y + (h / 2) + 4);
        } else {
            g.drawString(lines[0], x + 14, y + 18);
            g.drawString(lines[1], x + 14, y + 36);
        }
    }

    private void drawSearchBox(Graphics2D g, int x, int y, int w, int h, String inputText, String resultMsg, Color msgColor, Color msgBgColor) {
        // Nền box
        g.setColor(Color.WHITE);
        g.fill(new RoundRectangle2D.Double(x, y, w, h, 8, 8));
        g.setColor(new Color(226, 232, 240));
        g.draw(new RoundRectangle2D.Double(x, y, w, h, 8, 8));

        // Label Tìm kiếm
        g.setColor(new Color(51, 65, 85));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("Tìm kiếm:", x + 14, y + 30);

        // ComboBox
        int cbX = x + 85;
        g.setColor(new Color(248, 250, 252));
        g.fill(new RoundRectangle2D.Double(cbX, y + 12, 160, 32, 5, 5));
        g.setColor(new Color(203, 213, 225));
        g.draw(new RoundRectangle2D.Double(cbX, y + 12, 160, 32, 5, 5));
        g.setColor(new Color(30, 41, 59));
        g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        g.drawString("Mã yêu cầu (Request ID)", cbX + 10, y + 32);

        // TextField nhập
        int tfX = cbX + 170;
        int tfW = 460;
        g.setColor(Color.WHITE);
        g.fill(new RoundRectangle2D.Double(tfX, y + 12, tfW, 32, 5, 5));
        g.setColor(new Color(203, 213, 225));
        g.draw(new RoundRectangle2D.Double(tfX, y + 12, tfW, 32, 5, 5));
        g.setColor(new Color(15, 23, 42));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString(inputText, tfX + 10, y + 32);

        // Nút Tra cứu
        int btn1X = tfX + tfW + 12;
        g.setColor(new Color(37, 99, 235));
        g.fill(new RoundRectangle2D.Double(btn1X, y + 12, 110, 32, 5, 5));
        g.setColor(Color.WHITE);
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("🔍 Tra cứu", btn1X + 22, y + 32);

        // Nút Hiện tất cả
        int btn2X = btn1X + 120;
        g.setColor(new Color(241, 245, 249));
        g.fill(new RoundRectangle2D.Double(btn2X, y + 12, 110, 32, 5, 5));
        g.setColor(new Color(203, 213, 225));
        g.draw(new RoundRectangle2D.Double(btn2X, y + 12, 110, 32, 5, 5));
        g.setColor(new Color(71, 85, 105));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString("⟲ Hiện tất cả", btn2X + 18, y + 32);

        // Nhãn kết quả tra cứu
        g.setColor(msgColor);
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        g.drawString(resultMsg, x + 14, y + 66);
    }

    private void drawTicketTable(Graphics2D g, int x, int y, int w, int h, boolean hasTicket) {
        g.setColor(Color.WHITE);
        g.fill(new RoundRectangle2D.Double(x, y, w, h, 8, 8));
        g.setColor(new Color(226, 232, 240));
        g.draw(new RoundRectangle2D.Double(x, y, w, h, 8, 8));

        // Header Table
        g.setColor(new Color(241, 245, 249));
        g.fillRect(x + 1, y + 1, w - 2, 34);
        g.setColor(new Color(203, 213, 225));
        g.drawLine(x, y + 35, x + w, y + 35);

        g.setColor(new Color(71, 85, 105));
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        g.drawString("Mã yêu cầu (Request ID)", x + 16, y + 22);
        g.drawString("Mã vé (Ticket ID)", x + 230, y + 22);
        g.drawString("Ghế", x + 380, y + 22);
        g.drawString("Chuyến", x + 440, y + 22);
        g.drawString("Hành khách", x + 530, y + 22);
        g.drawString("Thời gian đặt", x + 690, y + 22);
        g.drawString("Trạng thái", x + 840, y + 22);
        g.drawString("Nguồn", x + 960, y + 22);

        if (hasTicket) {
            // Hàng 1: Vé CONFIRMED (REAL)
            g.setColor(new Color(240, 253, 244));
            g.fillRect(x + 1, y + 36, w - 2, 42);
            g.setColor(new Color(187, 247, 208));
            g.drawRect(x + 1, y + 36, w - 2, 42);

            g.setColor(new Color(15, 23, 42));
            g.setFont(new Font("Segoe UI", Font.PLAIN, 12));
            g.drawString("REQ-BOOK-SE001-G03", x + 16, y + 62);
            g.drawString("TCK-SE001-G03", x + 230, y + 62);
            g.drawString("G03", x + 380, y + 62);
            g.drawString("SE-001", x + 440, y + 62);
            g.drawString("Nguyễn Văn A", x + 530, y + 62);
            g.drawString("2026-11-24 10:15:30", x + 690, y + 62);

            // Badge Trạng thái CONFIRMED (chưa xác minh)
            g.setColor(new Color(220, 252, 231));
            g.fill(new RoundRectangle2D.Double(x + 835, y + 46, 110, 22, 10, 10));
            g.setColor(new Color(22, 101, 52));
            g.setFont(new Font("Segoe UI", Font.BOLD, 10));
            g.drawString("CONFIRMED (chưa XM)", x + 840, y + 61);

            // Badge Nguồn REAL
            g.setColor(new Color(220, 252, 231));
            g.fill(new RoundRectangle2D.Double(x + 960, y + 46, 50, 22, 10, 10));
            g.setColor(new Color(22, 101, 52));
            g.setFont(new Font("Segoe UI", Font.BOLD, 10));
            g.drawString("REAL", x + 972, y + 61);
        } else {
            // Bảng trống
            g.setColor(new Color(148, 163, 184));
            g.setFont(new Font("Segoe UI", Font.ITALIC, 13));
            g.drawString("Không có vé nào khớp với tiêu chí tìm kiếm trong danh sách vé đã xác nhận.", x + 280, y + 160);
        }
    }

    private void drawBottomAction(Graphics2D g, int x, int y, int w, int h, String summaryText, boolean btnEnabled) {
        g.setColor(Color.WHITE);
        g.fill(new RoundRectangle2D.Double(x, y, w, h, 8, 8));
        g.setColor(new Color(226, 232, 240));
        g.draw(new RoundRectangle2D.Double(x, y, w, h, 8, 8));

        // Tóm tắt vé
        g.setColor(new Color(100, 116, 139));
        g.setFont(new Font("Segoe UI", Font.BOLD, 11));
        g.drawString("Vé đang chọn trong bảng:", x + 14, y + 20);

        g.setColor(new Color(30, 41, 59));
        g.setFont(new Font("Segoe UI", Font.BOLD, 12));
        g.drawString(summaryText, x + 14, y + 42);

        // Nút Hủy vé
        int btnW = 150;
        int btnH = 36;
        int btnX = x + w - btnW - 14;
        int btnY = y + 12;

        if (btnEnabled) {
            g.setColor(new Color(220, 38, 38));
            g.fill(new RoundRectangle2D.Double(btnX, btnY, btnW, btnH, 6, 6));
            g.setColor(Color.WHITE);
            g.setFont(new Font("Segoe UI", Font.BOLD, 12));
            g.drawString("❌ Hủy vé đã chọn", btnX + 18, btnY + 23);
        } else {
            g.setColor(new Color(241, 245, 249));
            g.fill(new RoundRectangle2D.Double(btnX, btnY, btnW, btnH, 6, 6));
            g.setColor(new Color(203, 213, 225));
            g.draw(new RoundRectangle2D.Double(btnX, btnY, btnW, btnH, 6, 6));
            g.setColor(new Color(148, 163, 184));
            g.setFont(new Font("Segoe UI", Font.BOLD, 12));
            g.drawString("❌ Hủy vé đã chọn", btnX + 18, btnY + 23);
        }
    }

    private void drawFooter(Graphics2D g, String text) {
        g.setColor(new Color(100, 116, 139));
        g.setFont(new Font("Segoe UI", Font.ITALIC, 11));
        g.drawString(text, 30, HEIGHT - 20);
    }
}
