package vn.nhom6.railway.client.smoke;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.SeatInfoDto;
import vn.nhom6.railway.client.network.SocketBookingClient;
import vn.nhom6.railway.client.viewmodel.BookingViewModel;
import vn.nhom6.railway.client.viewmodel.BookingViewModel.UiState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử khói đơn Node (Single-Node Smoke Test) không phụ thuộc Backend thật (TASK TV1-D10).
 * <p>
 * Đặc tính:
 * <ul>
 *   <li>Headless hoàn toàn, không yêu cầu môi trường đồ họa JavaFX (chạy được trên mọi CI/CD và terminal).</li>
 *   <li>Sử dụng {@link SocketBookingClient} (D09) thật kết nối tới {@link FakeNodeServer} theo SPEC-03.</li>
 *   <li>Hỗ trợ chuyển mục tiêu sang Node thật khi có cấu hình {@code NODE_HOST} và {@code NODE_PORT}
 *       (mặc định chạy với server giả lập).</li>
 *   <li>Kiểm tra 3 kịch bản cốt lõi: BOOK ghế G02, GET_SEATS, Duplicate request (cùng requestId).</li>
 *   <li>Mọi bước đều ghi log requestId rõ ràng.</li>
 * </ul>
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
public class SingleNodeSmokeTest {

    private static final Logger LOGGER = Logger.getLogger(SingleNodeSmokeTest.class.getName());

    private FakeNodeServer fakeServer;
    private SocketBookingClient client;
    private BookingViewModel viewModel;
    private boolean isRealNode = false;

    @BeforeEach
    void setUp() throws IOException {
        String realHost = System.getProperty("NODE_HOST", System.getenv("NODE_HOST"));
        String realPortStr = System.getProperty("NODE_PORT", System.getenv("NODE_PORT"));

        if (realHost != null && !realHost.isBlank() && realPortStr != null && !realPortStr.isBlank()) {
            int realPort = Integer.parseInt(realPortStr.trim());
            LOGGER.info("[SMOKE-SETUP] Sử dụng REAL NODE tại " + realHost + ":" + realPort);
            client = new SocketBookingClient(realHost, realPort, 3000, 5000, "client-smoke-test");
            isRealNode = true;
        } else {
            fakeServer = new FakeNodeServer("node-1");
            int port = fakeServer.getPort();
            LOGGER.info("[SMOKE-SETUP] Khởi động FAKE SERVER tại 127.0.0.1:" + port);
            client = new SocketBookingClient("127.0.0.1", port, 2000, 3000, "client-smoke-test");
            isRealNode = false;
        }

        viewModel = new BookingViewModel(client);
        viewModel.setSyncMode(true); // Thực thi đồng bộ trong test headless
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    @Test
    @DisplayName("1. Preflight smoke: Kết nối TCP thành công và phản hồi không mang cờ MOCK")
    void testPreflightSmoke() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            String checkRequestId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-1] Preflight check với requestId=" + checkRequestId);

            GetSeatsRequestDto request = new GetSeatsRequestDto("SE-001");
            GetSeatsResponseDto response = client.getSeats(request).get(3, TimeUnit.SECONDS);

            assertNotNull(response, "Phản hồi GET_SEATS không được null");
            assertEquals("SE-001", response.getTripId());
            assertFalse(response.isMock(), "Kết quả từ SocketBookingClient phải có isMock() == false");
            LOGGER.info("[SMOKE-STEP-1-PASS] Preflight smoke thành công!");
        });
    }

    @Test
    @DisplayName("2. GET_SEATS: Nhận đủ đúng 10 ghế G01-G10, ghế G02 ở trạng thái AVAILABLE")
    void testGetSeatsTenSeatsAndG02Available() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            String seatsRequestId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-2] GET_SEATS requestId=" + seatsRequestId);

            GetSeatsResponseDto response = client.getSeats(new GetSeatsRequestDto("SE-001")).get(3, TimeUnit.SECONDS);

            assertNotNull(response);
            List<SeatInfoDto> seats = response.getSeats();
            assertNotNull(seats);
            assertEquals(10, seats.size(), "Bản đồ ghế phải có đúng 10 ghế G01-G10");

            SeatInfoDto seatG02 = seats.stream()
                    .filter(s -> "G02".equalsIgnoreCase(s.getSeatId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Không tìm thấy ghế G02"));

            assertEquals("AVAILABLE", seatG02.getStatus(), "Ban đầu ghế G02 phải ở trạng thái AVAILABLE");
            assertNull(seatG02.getTicketId(), "Ghế AVAILABLE không được có ticketId");
            LOGGER.info("[SMOKE-STEP-2-PASS] GET_SEATS thành công với đủ 10 ghế, G02 AVAILABLE");
        });
    }

    @Test
    @DisplayName("3. BOOK G02: Đặt vé thành công (CONFIRMED), sau đó GET_SEATS thấy G02 BOOKED và 9 ghế còn lại AVAILABLE")
    void testBookG02SuccessAndVerifySeatState() {
        assertTimeoutPreemptively(Duration.ofSeconds(8), () -> {
            String bookRequestId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-3] BOOK G02 với requestId=" + bookRequestId);

            BookTicketRequestDto bookReq = new BookTicketRequestDto(
                    bookRequestId,
                    "SE-001",
                    "G02",
                    "Nguyễn Văn Smoke G02"
            );

            // Đặt vé qua SocketBookingClient
            BookTicketResponseDto bookRes = client.book(bookReq).get(3, TimeUnit.SECONDS);

            assertNotNull(bookRes);
            assertEquals("CONFIRMED", bookRes.getOutcome(), "Outcome phải là CONFIRMED");
            assertTrue(bookRes.isConfirmed());
            assertNotNull(bookRes.getTicketId(), "ticketId không được null khi đặt vé thành công");
            assertTrue(bookRes.getTicketId().contains("G02"), "ticketId phải chứa mã ghế G02");
            assertNull(bookRes.getReasonCode(), "reasonCode phải null khi thành công");
            assertNotNull(bookRes.getAppliedLogIndex(), "appliedLogIndex phải có giá trị >= 1");
            assertTrue(bookRes.getAppliedLogIndex() >= 1L);
            assertFalse(bookRes.isMock(), "isMock() phải là false");

            LOGGER.info("[SMOKE-STEP-3] Đặt vé G02 thành công với ticketId=" + bookRes.getTicketId() + ", logIndex=" + bookRes.getAppliedLogIndex());

            // Kiểm tra trạng thái ghế sau khi đặt: G02 phải BOOKED, 9 ghế khác AVAILABLE
            GetSeatsResponseDto seatsRes = client.getSeats(new GetSeatsRequestDto("SE-001")).get(3, TimeUnit.SECONDS);
            assertNotNull(seatsRes);
            assertEquals(10, seatsRes.getSeats().size());

            for (SeatInfoDto seat : seatsRes.getSeats()) {
                if ("G02".equalsIgnoreCase(seat.getSeatId())) {
                    assertEquals("BOOKED", seat.getStatus(), "Ghế G02 phải chuyển sang BOOKED");
                    assertNotNull(seat.getTicketId(), "Ghế G02 BOOKED phải có ticketId");
                } else {
                    assertEquals("AVAILABLE", seat.getStatus(), "Ghế " + seat.getSeatId() + " phải là AVAILABLE");
                    assertNull(seat.getTicketId());
                }
            }

            // Kiểm chứng đồng bộ với SeatViewModel
            SeatViewModel seatVm = new SeatViewModel("G02", SeatStatus.AVAILABLE);
            seatVm.setStatus(SeatStatus.BOOKED);
            assertTrue(seatVm.isBooked());
            assertEquals("G02\n[✗ Đã đặt]", seatVm.getDisplayText());

            LOGGER.info("[SMOKE-STEP-3-PASS] BOOK G02 và kiểm tra trạng thái ghế thành công!");
        });
    }

    @Test
    @DisplayName("4. Duplicate: Gửi lại BOOK G02 với CÙNG requestId (1 lần rồi 10 lần) -> Kết quả không đổi, duy nhất 1 vé ACTIVE")
    void testDuplicateRequestMaintainsIdempotencyAndSingleActiveTicket() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            String duplicateRequestId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-4] Bắt đầu kiểm tra Duplicate với requestId=" + duplicateRequestId);

            BookTicketRequestDto originalRequest = new BookTicketRequestDto(
                    duplicateRequestId,
                    "SE-001",
                    "G02",
                    "Trần Khách Trùng Lặp"
            );

            // Lần gửi 1 (gốc)
            BookTicketResponseDto firstResponse = client.book(originalRequest).get(3, TimeUnit.SECONDS);
            assertNotNull(firstResponse);
            assertEquals("CONFIRMED", firstResponse.getOutcome());
            String initialTicketId = firstResponse.getTicketId();
            assertNotNull(initialTicketId);
            Long initialLogIndex = firstResponse.getAppliedLogIndex();

            LOGGER.info("[SMOKE-STEP-4] Lần 1: Thành công, ticketId=" + initialTicketId + ", appliedIndex=" + initialLogIndex);

            // Lần gửi 2 (duplicate lần 1): cùng requestId và cùng payload
            BookTicketResponseDto secondResponse = client.book(originalRequest).get(3, TimeUnit.SECONDS);
            assertNotNull(secondResponse);
            assertEquals("CONFIRMED", secondResponse.getOutcome());
            assertEquals(initialTicketId, secondResponse.getTicketId(), "Duplicate lần 1 phải trả về đúng ticketId cũ");
            assertEquals(initialLogIndex, secondResponse.getAppliedLogIndex(), "Duplicate lần 1 phải giữ nguyên appliedLogIndex");

            LOGGER.info("[SMOKE-STEP-4] Lần 2 (duplicate): Trả về đúng kết quả cũ!");

            // Lần gửi 3 đến 12 (vòng lặp 10 lần duplicate liên tiếp cùng requestId)
            for (int i = 3; i <= 12; i++) {
                BookTicketResponseDto loopResponse = client.book(originalRequest).get(3, TimeUnit.SECONDS);
                assertNotNull(loopResponse);
                assertEquals("CONFIRMED", loopResponse.getOutcome());
                assertEquals(initialTicketId, loopResponse.getTicketId(), "Lần " + i + " phải trả về đúng ticketId cũ");
                assertEquals(initialLogIndex, loopResponse.getAppliedLogIndex());
            }

            LOGGER.info("[SMOKE-STEP-4] Đã gửi lại 10 lần duplicate liên tiếp, tất cả đều trả về kết quả nhất quán!");

            // Nếu chạy trên Fake Server: Khẳng định chặt chẽ bất biến hệ thống phân tán
            if (!isRealNode && fakeServer != null) {
                // Đếm số vé ACTIVE: Đúng 1 vé cho G02
                int activeTickets = fakeServer.getActiveTicketCount("G02");
                assertEquals(1, activeTickets, "BẤT BIẾN: Sau 12 lần gửi cùng requestId, chỉ được có DUY NHẤT 1 vé ACTIVE trên G02");

                // Tổng số lần request tới server là 12
                int totalAttempts = fakeServer.getRequestAttemptCount(duplicateRequestId);
                assertEquals(12, totalAttempts, "Server phải nhận đúng 12 lần request với requestId=" + duplicateRequestId);

                // Kiểm tra mỗi lần gửi có messageId khác nhau nhưng requestId giữ nguyên
                List<MessageEnvelope<JsonNode>> allRequests = fakeServer.getReceivedRequests().stream()
                        .filter(env -> env.getPayload() != null && env.getPayload().hasNonNull("requestId")
                                && duplicateRequestId.equals(env.getPayload().get("requestId").asText()))
                        .toList();

                assertEquals(12, allRequests.size());

                // Khẳng định: mọi envelope đều có messageId riêng biệt (không trùng nhau)
                long distinctMessageIds = allRequests.stream()
                        .map(MessageEnvelope::getMessageId)
                        .distinct()
                        .count();
                assertEquals(12, distinctMessageIds, "Mỗi lần gửi phải sinh một messageId mới theo SPEC-03 Section 1 & Section 3");
            }

            LOGGER.info("[SMOKE-STEP-4-PASS] Duplicate idempotent kiểm tra thành công: 1 + 10 lần gửi = duy nhất 1 vé!");
        });
    }

    @Test
    @DisplayName("5. Conflict: Đổi payload cùng requestId -> REQUEST_ID_CONFLICT, bị từ chối, không được coi là thành công")
    void testSameRequestIdDifferentPayloadReturnsConflict() {
        assertTimeoutPreemptively(Duration.ofSeconds(6), () -> {
            String conflictRequestId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-5] Bắt đầu kiểm tra Conflict với requestId=" + conflictRequestId);

            // Gửi lần đầu: đặt ghế G02
            BookTicketRequestDto firstReq = new BookTicketRequestDto(
                    conflictRequestId,
                    "SE-001",
                    "G02",
                    "Khách Ban Đầu"
            );
            BookTicketResponseDto firstRes = client.book(firstReq).get(3, TimeUnit.SECONDS);
            assertEquals("CONFIRMED", firstRes.getOutcome());

            // Gửi lần 2: CÙNG conflictRequestId nhưng ĐỔI GHẾ sang G05
            BookTicketRequestDto conflictingReq = new BookTicketRequestDto(
                    conflictRequestId,
                    "SE-001",
                    "G05",
                    "Khách Xung Đột"
            );
            BookTicketResponseDto conflictRes = client.book(conflictingReq).get(3, TimeUnit.SECONDS);

            assertNotNull(conflictRes);
            assertEquals("REJECTED", conflictRes.getOutcome(), "Outcome phải là REJECTED");
            assertFalse(conflictRes.isConfirmed(), "Conflict không được coi là thành công");
            assertEquals("REQUEST_ID_CONFLICT", conflictRes.getReasonCode(), "Mã lỗi/lý do phải là REQUEST_ID_CONFLICT");

            LOGGER.info("[SMOKE-STEP-5-PASS] Xung đột REQUEST_ID_CONFLICT được phát hiện và từ chối chính xác!");
        });
    }

    @Test
    @DisplayName("6. Cờ MOCK: Kết quả từ SocketBookingClient không mang cờ MOCK (isMock() == false)")
    void testResultsDoNotCarryMockFlag() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            String mockCheckReqId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-6] Kiểm tra cờ MOCK với requestId=" + mockCheckReqId);

            BookTicketRequestDto req = new BookTicketRequestDto(mockCheckReqId, "SE-001", "G01", "Khách Kiểm Mock");
            BookTicketResponseDto res = client.book(req).get(3, TimeUnit.SECONDS);

            assertNotNull(res);
            assertFalse(res.isMock(), "BookTicketResponseDto từ socket phải có isMock() == false");

            // Kiểm tra ViewModel
            CompletableFuture<BookTicketResponseDto> vmFuture = viewModel.book(
                    new BookTicketRequestDto(UUID.randomUUID().toString(), "SE-001", "G03", "Khách VM")
            );
            BookTicketResponseDto vmRes = vmFuture.get(3, TimeUnit.SECONDS);
            assertNotNull(vmRes);
            assertFalse(vmRes.isMock());
            assertFalse(viewModel.isMock(), "BookingViewModel sau khi nhận phản hồi socket phải có isMock() == false");

            LOGGER.info("[SMOKE-STEP-6-PASS] Cờ isMock() == false được xác nhận trên cả DTO và ViewModel!");
        });
    }

    @Test
    @DisplayName("7. GET_REQUEST_RESULT: Ghi nhận tình trạng DTO từ TV3 theo quy tắc Bước 0")
    void testGetRequestResultStatus() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            String queryReqId = UUID.randomUUID().toString();
            LOGGER.info("[SMOKE-STEP-7] Kiểm tra getRequestResult với requestId=" + queryReqId);

            // Kiểm tra phương thức getRequestResult() của SocketBookingClient
            CompletableFuture<?> future = client.getRequestResult(queryReqId);
            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));

            assertTrue(ex.getCause() instanceof UnsupportedOperationException);
            assertTrue(ex.getCause().getMessage().contains("BLOCKED"),
                    "Phải thông báo BLOCKED do thiếu DTO GET_REQUEST_RESULT từ TV3");

            LOGGER.info("[SMOKE-STEP-7-PASS] getRequestResult phản ánh đúng trạng thái BLOCKED theo SPEC-03/D09!");
        });
    }
}
