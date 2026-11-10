package vn.nhom6.railway.client.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử đơn vị cho {@link MockBookingClient} (TASK D07).
 */
class MockBookingClientTest {

    private MockBookingClient client;

    @BeforeEach
    void setUp() {
        client = new MockBookingClient();
        client.setDelayMs(10); // Đặt delay ngắn (10ms) để test chạy nhanh
        client.resetDefaultData();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @DisplayName("Lấy danh sách 10 ghế G01-G10 kèm nhãn MOCK")
    void testGetSeatsReturnsTenSeatsWithMockFlag() throws Exception {
        GetSeatsResponseDto response = client.getSeats(new GetSeatsRequestDto("SE-001")).get(2, TimeUnit.SECONDS);

        assertNotNull(response);
        assertTrue(response.isMock(), "Cờ MOCK phải luôn bật");
        assertEquals(10, response.getSeats().size(), "Phải có đúng 10 ghế");
        assertEquals("SE-001", response.getTripId());
        assertEquals("node-1", response.getLeaderId());
    }

    @Test
    @DisplayName("Kịch bản SUCCESS: Đặt vé thành công, ghế chuyển sang BOOKED và mang cờ MOCK")
    void testBookSuccessScenario() throws Exception {
        client.setScenario(MockScenario.SUCCESS);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G03", "Hành khách test");

        BookTicketResponseDto result = client.book(req).get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isMock(), "Kết quả phải mang cờ MOCK");
        assertTrue(result.isConfirmed(), "Phải có outcome là CONFIRMED");
        assertEquals("G03", result.getSeatId());
        assertNotNull(result.getTicketId());
        assertNull(result.getReasonCode());

        // Kiểm tra tính nhất quán: G03 phải chuyển sang BOOKED
        GetSeatsResponseDto seats = client.getSeats(new GetSeatsRequestDto("SE-001")).get(2, TimeUnit.SECONDS);
        SeatInfoDto g03 = seats.getSeats().stream().filter(s -> "G03".equals(s.getSeatId())).findFirst().orElseThrow();
        assertEquals("BOOKED", g03.getStatus());
    }

    @Test
    @DisplayName("Kịch bản SEAT_ALREADY_BOOKED: Bị từ chối nghiệp vụ (REJECTED)")
    void testBookSeatAlreadyBookedScenario() throws Exception {
        client.setScenario(MockScenario.SEAT_ALREADY_BOOKED);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G05", "Khách Demo");

        BookTicketResponseDto result = client.book(req).get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isMock());
        assertTrue(result.isRejected(), "Phải có outcome là REJECTED");
        assertEquals("SEAT_ALREADY_BOOKED", result.getReasonCode());
        assertNull(result.getTicketId());
    }

    @Test
    @DisplayName("Kịch bản NOT_LEADER: Trả lỗi hạ tầng UNKNOWN kèm gợi ý Leader node-2")
    void testBookNotLeaderScenario() throws Exception {
        client.setScenario(MockScenario.NOT_LEADER);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Demo");

        BookTicketResponseDto result = client.book(req).get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isMock());
        assertTrue(result.isUnknown(), "Lỗi định tuyến Leader phải là UNKNOWN, không phải REJECTED");
        assertEquals("NOT_LEADER", result.getReasonCode());
        assertEquals("node-2", result.getLeaderHint());
    }

    @Test
    @DisplayName("Kịch bản NO_QUORUM: Trả lỗi thiếu Quorum UNKNOWN")
    void testBookNoQuorumScenario() throws Exception {
        client.setScenario(MockScenario.NO_QUORUM);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Demo");

        BookTicketResponseDto result = client.book(req).get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isMock());
        assertTrue(result.isUnknown());
        assertEquals("NO_QUORUM", result.getReasonCode());
    }

    @Test
    @DisplayName("Kịch bản UNKNOWN: Trả về TIMEOUT_UNKNOWN và có thể tra cứu lại sau đó")
    void testBookUnknownScenarioAndLookup() throws Exception {
        client.setScenario(MockScenario.UNKNOWN);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G04", "Khách Demo");

        BookTicketResponseDto result = client.book(req).get(2, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isMock());
        assertTrue(result.isUnknown());
        assertEquals("TIMEOUT_UNKNOWN", result.getReasonCode());

        // Tra cứu lại cùng requestId
        GetRequestResultResponseDto lookup = client.getRequestResult(reqId).get(2, TimeUnit.SECONDS);
        assertNotNull(lookup);
        assertTrue(lookup.isMock());
        assertEquals(reqId, lookup.getRequestId());
        assertEquals("PENDING", lookup.getState());
    }

    @Test
    @DisplayName("Kịch bản CONNECTION_LOST: Future hoàn thành có ngoại lệ (IOException)")
    void testBookConnectionLostScenario() {
        client.setScenario(MockScenario.CONNECTION_LOST);
        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto req = new BookTicketRequestDto(reqId, "SE-001", "G06", "Khách Demo");

        CompletableFuture<BookTicketResponseDto> future = client.book(req);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertTrue(ex.getCause().getCause() instanceof IOException, "Nguyên nhân gốc phải là IOException");
    }

    @Test
    @DisplayName("Hủy vé thành công giải phóng ghế sang AVAILABLE")
    void testCancelTicketSuccess() throws Exception {
        client.setScenario(MockScenario.SUCCESS);
        // G01 đã có vé mẫu TCK-SE001-G01
        CancelTicketRequestDto req = new CancelTicketRequestDto(UUID.randomUUID().toString(), "TCK-SE001-G01");

        CancelTicketResponseDto cancelResult = client.cancel(req).get(2, TimeUnit.SECONDS);

        assertNotNull(cancelResult);
        assertTrue(cancelResult.isMock());
        assertTrue(cancelResult.isConfirmed());

        // Kiểm tra ghế G01 đã được giải phóng
        GetSeatsResponseDto seats = client.getSeats(new GetSeatsRequestDto("SE-001")).get(2, TimeUnit.SECONDS);
        SeatInfoDto g01 = seats.getSeats().stream().filter(s -> "G01".equals(s.getSeatId())).findFirst().orElseThrow();
        assertEquals("AVAILABLE", g01.getStatus());
    }
}
