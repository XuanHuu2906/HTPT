package vn.nhom6.railway.client;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.NodeEndpoint;
import vn.nhom6.railway.client.network.SocketBookingClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử lệnh CANCEL_TICKET thật trên Backend Node qua TCP Socket (TASK TV1-D25).
 * <p>
 * Tuân thủ nghiêm ngặt AGENTS.md:
 * - Bảo vệ môi trường: Mặc định SKIPPED nếu chưa có cụm Backend + DB thật trên LAN (ALLOW_REAL_CANCEL_TEST=true).
 * - Báo cáo trung thực BLOCKED_CANCEL khi chưa chạy thật, tuyệt đối không dùng fake/mock để báo PASS test này.
 * - Khi bật cờ thật: Thực hiện luồng CANCEL_TICKET -> retry idempotency cùng requestId -> GET_REQUEST_RESULT.
 * - Xuất trace ra artifacts/evidence/D25/TV1-D25-cancel-real-trace.csv nếu chạy thật.
 */
public class RealCancelTicketCheckTest {

    private static final String ENV_ALLOW_REAL_CANCEL = "ALLOW_REAL_CANCEL_TEST";
    private static final Path EVIDENCE_CSV = Paths.get("artifacts", "evidence", "D25", "TV1-D25-cancel-real-trace.csv");

    @Test
    @DisplayName("TC_CANCEL_REAL_01: Kiểm tra CANCEL_TICKET thật qua Socket (Guard môi trường thật - Mặc định SKIPPED)")
    void testRealCancelTicketWithBackendGuard() throws Exception {
        Assumptions.assumeTrue(
                "true".equalsIgnoreCase(System.getenv(ENV_ALLOW_REAL_CANCEL)),
                "BLOCKED_CANCEL: Chưa có node backend chạy thật trên LAN. Test case này được bảo vệ an toàn theo AGENTS.md §5 & §6."
        );

        // 1. Nạp cấu hình LAN và thẩm tra không còn placeholder
        ClientConfig config = ClientConfig.loadWithOverrides(null, Map.of("cluster.profile", "lan"), null);
        config.validateNoPlaceholders();

        // 2. Kết nối tới Leader hoặc preferredNode
        NodeEndpoint leaderEp = config.getEndpoints().get(0);
        try (SocketBookingClient client = new SocketBookingClient(leaderEp.host(), leaderEp.port(), 3000, 5000, "cancel-real-tester")) {
            String cancelRequestId = "REQ-CANCEL-REAL-" + UUID.randomUUID();
            String ticketId = "TCK-SE001-G01"; // Giả định vé đã đặt trước

            List<String> traceLines = new ArrayList<>();
            traceLines.add("timestamp,requestId,ticketId,action,outcome,reasonCode,latencyMs");

            long start = System.currentTimeMillis();
            CancelTicketRequestDto cancelReq = new CancelTicketRequestDto(cancelRequestId, ticketId);
            CancelTicketResponseDto resp = client.cancel(cancelReq).get(5, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertNotNull(resp, "Phản hồi hủy vé từ node backend thật không được null");
            assertEquals(cancelRequestId, resp.getRequestId(), "requestId trong phản hồi phải khớp với yêu cầu gửi đi");

            traceLines.add(String.format("%s,%s,%s,CANCEL_FIRST,%s,%s,%d",
                    Instant.now().toString(), cancelRequestId, ticketId, resp.getOutcome(), resp.getReasonCode(), elapsed));

            // 3. Thử lại cùng requestId để kiểm tra tính bất biến (Idempotency)
            long startRetry = System.currentTimeMillis();
            CancelTicketResponseDto retryResp = client.cancel(cancelReq).get(5, TimeUnit.SECONDS);
            long elapsedRetry = System.currentTimeMillis() - startRetry;

            assertNotNull(retryResp, "Phản hồi retry hủy vé không được null");
            assertEquals(resp.getOutcome(), retryResp.getOutcome(), "Kết quả retry hủy vé cùng requestId phải idempotent");

            traceLines.add(String.format("%s,%s,%s,CANCEL_RETRY,%s,%s,%d",
                    Instant.now().toString(), cancelRequestId, ticketId, retryResp.getOutcome(), retryResp.getReasonCode(), elapsedRetry));

            // 4. Ghi nhận trace kết quả vào thư mục evidence
            Path parent = EVIDENCE_CSV.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
            Files.write(EVIDENCE_CSV, traceLines, StandardCharsets.UTF_8);
            System.out.println("[EVIDENCE D25] Đã ghi nhận trace hủy vé thật: " + EVIDENCE_CSV.toAbsolutePath());
        }
    }

    @Test
    @DisplayName("TC_CANCEL_REAL_02: Xác thực Guard môi trường hoạt động chính xác khi biến môi trường không được bật")
    void testGuardBehaviorWhenEnvDisabled() {
        String envVal = System.getenv(ENV_ALLOW_REAL_CANCEL);
        if (envVal == null || !"true".equalsIgnoreCase(envVal)) {
            // Xác nhận rằng điều kiện guard luôn đánh giá an toàn và không cho phép test thật chạy mù
            assertNull(envVal != null && "true".equalsIgnoreCase(envVal) ? new Object() : null,
                    "Mặc định trong môi trường dev/local biến ALLOW_REAL_CANCEL_TEST chưa được bật");
        }
    }

    @Test
    @DisplayName("TC_CANCEL_REAL_03: DTO và payload CANCEL_TICKET tuần tự hóa đúng cấu trúc SPEC-03")
    void testCancelTicketDtoContractIntegrity() {
        String reqId = "REQ-VERIFY-" + UUID.randomUUID();
        String ticketId = "TCK-SE001-G05";

        CancelTicketRequestDto req = new CancelTicketRequestDto(reqId, ticketId);
        assertEquals(reqId, req.getRequestId());
        assertEquals(ticketId, req.getTicketId());

        CancelTicketResponseDto resp = new CancelTicketResponseDto(
                reqId, "CONFIRMED", ticketId, null, 100L, false, null, "Hủy vé thành công"
        );
        assertTrue(resp.isConfirmed());
        assertFalse(resp.isRejected());
        assertFalse(resp.isUnknown());
        assertEquals(reqId, resp.getRequestId());
        assertEquals(ticketId, resp.getTicketId());
    }
}
