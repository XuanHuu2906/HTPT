package vn.nhom6.railway.client.network;

import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Cài đặt giả lập dịch vụ bán vé {@link BookingClient} (TASK D07).
 * <p>
 * Tính năng chính:
 * <ul>
 *   <li>Giả lập độ trễ mạng có cấu hình (mặc định 2000ms = 2s) bằng daemon executor riêng biệt, không khóa UI thread.</li>
 *   <li>Hỗ trợ 6 kịch bản kiểm thử ({@link MockScenario}): SUCCESS, SEAT_ALREADY_BOOKED, NOT_LEADER, NO_QUORUM, UNKNOWN, CONNECTION_LOST.</li>
 *   <li>Trạng thái ghế/vé trong bộ nhớ nhất quán và thread-safe qua {@link ConcurrentHashMap}.</li>
 *   <li>Tất cả kết quả trả về đều mang cờ/nhãn MOCK (isMock() == true).</li>
 *   <li>Phân biệt rõ ràng giữa REJECTED (từ chối nghiệp vụ), lỗi hạ tầng (NOT_LEADER, NO_QUORUM) và UNKNOWN.</li>
 * </ul>
 */
public class MockBookingClient implements BookingClient, AutoCloseable {

    public static final long DEFAULT_DELAY_MS = 2000L;
    private static volatile MockBookingClient defaultInstance;

    private final ScheduledExecutorService scheduler;
    private final AtomicLong delayMs = new AtomicLong(DEFAULT_DELAY_MS);
    private final AtomicReference<MockScenario> activeScenario = new AtomicReference<>(MockScenario.SUCCESS);
    private final AtomicReference<MockScenario> nextScenario = new AtomicReference<>(null);

    // Dữ liệu mock trong bộ nhớ: 10 ghế G01–G10
    private final Map<String, SeatInfoDto> seatMap = new ConcurrentHashMap<>();
    // Dữ liệu vé đã cấp: ticketId -> TicketDetails
    private final Map<String, MockTicketRecord> ticketMap = new ConcurrentHashMap<>();
    // Nhật ký kết quả yêu cầu: requestId -> GetRequestResultResponseDto
    private final Map<String, GetRequestResultResponseDto> requestResults = new ConcurrentHashMap<>();

    private final AtomicLong logIndexSequence = new AtomicLong(100L);

    public record MockTicketRecord(String ticketId, String seatId, String tripId,
                                   String passengerName, String status, String issuedAt) {}

    public MockBookingClient() {
        this(Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "mock-booking-worker");
            t.setDaemon(true);
            return t;
        }));
    }

    public MockBookingClient(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        resetDefaultData();
    }

    public static MockBookingClient getInstance() {
        if (defaultInstance == null) {
            synchronized (MockBookingClient.class) {
                if (defaultInstance == null) {
                    defaultInstance = new MockBookingClient();
                }
            }
        }
        return defaultInstance;
    }

    /**
     * Khởi tạo lại trạng thái ban đầu của 10 ghế G01–G10.
     */
    public void resetDefaultData() {
        seatMap.clear();
        ticketMap.clear();
        requestResults.clear();

        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            seatMap.put(seatId, new SeatInfoDto(seatId, "AVAILABLE", null));
        }

        // Mẫu 2 vé đã đặt sẵn ở G01, G02 để kiểm thử theo D01-D06
        markSeatBookedInternal("G01", "TCK-SE001-G01", "Nguyễn Văn Mẫu");
        markSeatBookedInternal("G02", "TCK-SE001-G02", "Trần Thị Demo");
    }

    private void markSeatBookedInternal(String seatId, String ticketId, String passengerName) {
        seatMap.put(seatId, new SeatInfoDto(seatId, "BOOKED", ticketId));
        ticketMap.put(ticketId, new MockTicketRecord(ticketId, seatId, "SE-001", passengerName, "CONFIRMED", "2026-11-08 10:00"));
    }

    public long getDelayMs() {
        return delayMs.get();
    }

    public void setDelayMs(long ms) {
        this.delayMs.set(Math.max(0, ms));
    }

    public MockScenario getScenario() {
        return activeScenario.get();
    }

    public void setScenario(MockScenario scenario) {
        this.activeScenario.set(scenario != null ? scenario : MockScenario.SUCCESS);
    }

    public void setNextScenario(MockScenario scenario) {
        this.nextScenario.set(scenario);
    }

    private MockScenario consumeScenario() {
        MockScenario next = nextScenario.getAndSet(null);
        return next != null ? next : activeScenario.get();
    }

    private <T> CompletableFuture<T> executeAsync(Supplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        long delay = delayMs.get();
        Runnable task = () -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        };

        if (delay <= 0) {
            scheduler.execute(task);
        } else {
            scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
        }
        return future;
    }

    @Override
    public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
        return executeAsync(() -> {
            MockScenario scenario = consumeScenario();
            if (scenario == MockScenario.CONNECTION_LOST) {
                throw new RuntimeException(new IOException("Mất kết nối tới cụm Node (Socket Connection Timeout) [MOCK]"));
            }

            List<SeatInfoDto> list = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                String seatId = String.format("G%02d", i);
                SeatInfoDto info = seatMap.getOrDefault(seatId, new SeatInfoDto(seatId, "AVAILABLE", null));
                list.add(info);
            }

            String leaderId = (scenario == MockScenario.NOT_LEADER) ? null : "node-1";
            return new GetSeatsResponseDto("SE-001", list, logIndexSequence.get(), leaderId, true);
        });
    }

    @Override
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        return executeAsync(() -> {
            if (request == null) {
                throw new IllegalArgumentException("BookTicketRequestDto không được để null");
            }

            MockScenario scenario = consumeScenario();
            String reqId = request.getRequestId();
            if (reqId == null || reqId.isBlank()) {
                reqId = UUID.randomUUID().toString();
            }
            String seatId = request.getSeatId();
            String passenger = request.getPassengerName();

            switch (scenario) {
                case CONNECTION_LOST -> {
                    throw new RuntimeException(new IOException("Mất kết nối mạng trong khi gửi yêu cầu đặt vé (Socket Reset) [MOCK]"));
                }
                case NOT_LEADER -> {
                    BookTicketResponseDto response = new BookTicketResponseDto(
                            reqId, "UNKNOWN", null, request.getTripId(), seatId,
                            "NOT_LEADER", null, true, "node-2",
                            "Node hiện tại không phải Leader. Vui lòng kết nối node-2 [MOCK]"
                    );
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "UNKNOWN", "BOOK_TICKET", null, "NOT_LEADER", null, true));
                    return response;
                }
                case NO_QUORUM -> {
                    BookTicketResponseDto response = new BookTicketResponseDto(
                            reqId, "UNKNOWN", null, request.getTripId(), seatId,
                            "NO_QUORUM", null, true, null,
                            "Cụm không đạt đa số (Quorum < 3/5), không thể ghi an toàn [MOCK]"
                    );
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "UNKNOWN", "BOOK_TICKET", null, "NO_QUORUM", null, true));
                    return response;
                }
                case UNKNOWN -> {
                    BookTicketResponseDto response = new BookTicketResponseDto(
                            reqId, "UNKNOWN", null, request.getTripId(), seatId,
                            "TIMEOUT_UNKNOWN", null, true, null,
                            "Hết thời gian chờ kết quả apply (TIMEOUT_UNKNOWN) [MOCK]"
                    );
                    // Lưu trạng thái PENDING để tra cứu sau
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "PENDING", "BOOK_TICKET", null, "TIMEOUT_UNKNOWN", null, true));
                    return response;
                }
                case SEAT_ALREADY_BOOKED -> {
                    long appliedIndex = logIndexSequence.incrementAndGet();
                    BookTicketResponseDto response = new BookTicketResponseDto(
                            reqId, "REJECTED", null, request.getTripId(), seatId,
                            "SEAT_ALREADY_BOOKED", appliedIndex, true, null,
                            "Ghế " + seatId + " đã có người đặt trước (Bị từ chối) [MOCK]"
                    );
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "APPLIED_REJECTED", "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", appliedIndex, true));
                    return response;
                }
                case SUCCESS -> {
                    // Kiểm tra tính nhất quán trong memory
                    SeatInfoDto currentSeat = seatMap.get(seatId);
                    if (currentSeat != null && "BOOKED".equalsIgnoreCase(currentSeat.getStatus())) {
                        long appliedIndex = logIndexSequence.incrementAndGet();
                        BookTicketResponseDto response = new BookTicketResponseDto(
                                reqId, "REJECTED", null, request.getTripId(), seatId,
                                "SEAT_ALREADY_BOOKED", appliedIndex, true, null,
                                "Ghế " + seatId + " hiện đã có vé hiệu lực [MOCK]"
                        );
                        requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "APPLIED_REJECTED", "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", appliedIndex, true));
                        return response;
                    }

                    // Đặt vé thành công
                    long appliedIndex = logIndexSequence.incrementAndGet();
                    String ticketId = "TCK-" + seatId + "-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
                    seatMap.put(seatId, new SeatInfoDto(seatId, "BOOKED", ticketId));
                    ticketMap.put(ticketId, new MockTicketRecord(ticketId, seatId, request.getTripId(), passenger, "CONFIRMED", "2026-11-08 12:00"));

                    BookTicketResponseDto response = new BookTicketResponseDto(
                            reqId, "CONFIRMED", ticketId, request.getTripId(), seatId,
                            null, appliedIndex, true, "node-1",
                            "Đặt vé thành công cho hành khách " + passenger + " [MOCK]"
                    );
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "BOOK_TICKET", ticketId, null, appliedIndex, true));
                    return response;
                }
                default -> throw new IllegalStateException("Kịch bản không được hỗ trợ: " + scenario);
            }
        });
    }

    @Override
    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        return executeAsync(() -> {
            if (request == null) {
                throw new IllegalArgumentException("CancelTicketRequestDto không được để null");
            }

            MockScenario scenario = consumeScenario();
            String reqId = request.getRequestId();
            String ticketId = request.getTicketId();

            switch (scenario) {
                case CONNECTION_LOST -> {
                    throw new RuntimeException(new IOException("Mất kết nối mạng khi gửi yêu cầu hủy vé [MOCK]"));
                }
                case NOT_LEADER -> {
                    return new CancelTicketResponseDto(
                            reqId, "UNKNOWN", ticketId, "NOT_LEADER", null, true, "node-2",
                            "Node không phải Leader, không thể hủy vé (Gợi ý: node-2) [MOCK]"
                    );
                }
                case NO_QUORUM -> {
                    return new CancelTicketResponseDto(
                            reqId, "UNKNOWN", ticketId, "NO_QUORUM", null, true, null,
                            "Cụm mất Quorum, không thể hủy vé an toàn [MOCK]"
                    );
                }
                case UNKNOWN -> {
                    return new CancelTicketResponseDto(
                            reqId, "UNKNOWN", ticketId, "TIMEOUT_UNKNOWN", null, true, null,
                            "Hết thời gian chờ phản hồi hủy vé (TIMEOUT_UNKNOWN) [MOCK]"
                    );
                }
                case SEAT_ALREADY_BOOKED -> {
                    // Mô phỏng trường hợp vé không thể hủy (ví dụ quá hạn hoặc đã hủy)
                    long appliedIndex = logIndexSequence.incrementAndGet();
                    return new CancelTicketResponseDto(
                            reqId, "REJECTED", ticketId, "TICKET_NOT_ACTIVE", appliedIndex, true, null,
                            "Vé không còn ở trạng thái hiệu lực để hủy [MOCK]"
                    );
                }
                case SUCCESS -> {
                    MockTicketRecord record = ticketMap.get(ticketId);
                    if (record == null || !"CONFIRMED".equalsIgnoreCase(record.status())) {
                        long appliedIndex = logIndexSequence.incrementAndGet();
                        return new CancelTicketResponseDto(
                                reqId, "REJECTED", ticketId, "TICKET_NOT_ACTIVE", appliedIndex, true, null,
                                "Không tìm thấy vé hoặc vé đã bị hủy trước đó [MOCK]"
                        );
                    }

                    // Hủy vé thành công: giải phóng ghế sang AVAILABLE
                    long appliedIndex = logIndexSequence.incrementAndGet();
                    String seatId = record.seatId();
                    seatMap.put(seatId, new SeatInfoDto(seatId, "AVAILABLE", null));
                    ticketMap.put(ticketId, new MockTicketRecord(ticketId, seatId, record.tripId(), record.passengerName(), "CANCELLED", record.issuedAt()));

                    CancelTicketResponseDto response = new CancelTicketResponseDto(
                            reqId, "CONFIRMED", ticketId, null, appliedIndex, true, "node-1",
                            "Hủy vé " + ticketId + " thành công. Ghế " + seatId + " đã được giải phóng [MOCK]"
                    );
                    requestResults.put(reqId, new GetRequestResultResponseDto(reqId, "APPLIED_SUCCESS", "CANCEL_TICKET", ticketId, null, appliedIndex, true));
                    return response;
                }
                default -> throw new IllegalStateException("Kịch bản không được hỗ trợ: " + scenario);
            }
        });
    }

    @Override
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        return executeAsync(() -> {
            GetRequestResultResponseDto result = requestResults.get(requestId);
            if (result != null) {
                return result;
            }
            return new GetRequestResultResponseDto(requestId, "UNKNOWN", null, null, null, null, true);
        });
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
