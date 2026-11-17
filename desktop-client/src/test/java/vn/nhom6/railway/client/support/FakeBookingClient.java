package vn.nhom6.railway.client.support;

import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.CancelTicketRequestDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.GetRequestResultResponseDto;
import vn.nhom6.railway.client.network.GetSeatsResponseDto;
import vn.nhom6.railway.client.network.SeatInfoDto;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client giả lập phục vụ kiểm thử đơn vị UI (TASK TV1-D17).
 * <p>
 * Nhãn nguồn bắt buộc: "FAKE, không phải Node thật" (mock = true).
 * Tuyệt đối không kết nối mạng và không tuyên bố là Node Raft thật.
 */
public class FakeBookingClient implements BookingClient, Closeable {

    public static final String SOURCE_LABEL = "FAKE, không phải Node thật";

    private final AtomicInteger bookCallCount = new AtomicInteger(0);
    private final AtomicInteger cancelCallCount = new AtomicInteger(0);
    private final AtomicInteger getSeatsCallCount = new AtomicInteger(0);
    private final AtomicInteger getRequestResultCallCount = new AtomicInteger(0);

    private final List<BookTicketRequestDto> recordedBookRequests = Collections.synchronizedList(new ArrayList<>());

    private volatile CompletableFuture<BookTicketResponseDto> manualBookFuture = null;
    private volatile BookTicketResponseDto configuredBookResponse = null;
    private volatile Throwable configuredBookException = null;

    private volatile ScheduledExecutorService delayScheduler = null;
    private volatile long simulatedDelayMs = 0;

    private volatile GetRequestResultResponseDto configuredResultResponse = null;

    public FakeBookingClient() {
    }

    public void setManualBookFuture(CompletableFuture<BookTicketResponseDto> manualFuture) {
        this.manualBookFuture = manualFuture;
    }

    public void setConfiguredBookResponse(BookTicketResponseDto response) {
        this.configuredBookResponse = response;
    }

    public void setConfiguredBookException(Throwable throwable) {
        this.configuredBookException = throwable;
    }

    public void setSimulatedDelay(long delayMs, ScheduledExecutorService scheduler) {
        this.simulatedDelayMs = delayMs;
        this.delayScheduler = scheduler;
    }

    public void setConfiguredResultResponse(GetRequestResultResponseDto response) {
        this.configuredResultResponse = response;
    }

    public int getBookCallCount() {
        return bookCallCount.get();
    }

    public int getGetRequestResultCallCount() {
        return getRequestResultCallCount.get();
    }

    public List<BookTicketRequestDto> getRecordedBookRequests() {
        return new ArrayList<>(recordedBookRequests);
    }

    public void reset() {
        bookCallCount.set(0);
        cancelCallCount.set(0);
        getSeatsCallCount.set(0);
        getRequestResultCallCount.set(0);
        recordedBookRequests.clear();
        manualBookFuture = null;
        configuredBookResponse = null;
        configuredBookException = null;
        simulatedDelayMs = 0;
        delayScheduler = null;
        configuredResultResponse = null;
    }

    @Override
    public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
        getSeatsCallCount.incrementAndGet();
        List<SeatInfoDto> seats = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String seatId = String.format("G%02d", i);
            seats.add(new SeatInfoDto(seatId, "AVAILABLE", null));
        }
        return CompletableFuture.completedFuture(new GetSeatsResponseDto("SE-001", seats, 100L, "node-1", true));
    }

    @Override
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        bookCallCount.incrementAndGet();
        recordedBookRequests.add(request);

        if (manualBookFuture != null) {
            return manualBookFuture;
        }

        CompletableFuture<BookTicketResponseDto> future = new CompletableFuture<>();

        Runnable action = () -> {
            if (configuredBookException != null) {
                future.completeExceptionally(configuredBookException);
            } else if (configuredBookResponse != null) {
                future.complete(configuredBookResponse);
            } else {
                BookTicketResponseDto defaultResponse = new BookTicketResponseDto(
                        request.getRequestId(),
                        "CONFIRMED",
                        "TCK-FAKE-" + request.getSeatId(),
                        request.getTripId() != null ? request.getTripId() : "SE-001",
                        request.getSeatId(),
                        null,
                        100L,
                        true,
                        "node-1",
                        "Đặt vé thành công [FAKE]"
                );
                future.complete(defaultResponse);
            }
        };

        if (simulatedDelayMs > 0 && delayScheduler != null) {
            delayScheduler.schedule(action, simulatedDelayMs, TimeUnit.MILLISECONDS);
        } else {
            action.run();
        }

        return future;
    }

    @Override
    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        cancelCallCount.incrementAndGet();
        return CompletableFuture.completedFuture(new CancelTicketResponseDto(
                request.getRequestId(),
                "CONFIRMED",
                request.getTicketId(),
                null,
                101L,
                true,
                "node-1",
                "Hủy vé thành công [FAKE]"
        ));
    }

    @Override
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        getRequestResultCallCount.incrementAndGet();
        if (configuredResultResponse != null) {
            return CompletableFuture.completedFuture(configuredResultResponse);
        }
        return CompletableFuture.completedFuture(new GetRequestResultResponseDto(
                requestId,
                "UNKNOWN",
                null,
                null,
                null,
                null,
                true
        ));
    }

    @Override
    public void close() throws IOException {
        reset();
    }
}
