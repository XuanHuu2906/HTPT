package vn.nhom6.railway.client.network;

import vn.nhom6.railway.client.viewmodel.SeatViewModel.MockSeatSource;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Triển khai {@link ReadAdapter} giả lập (Testbed Fake) hỗ trợ tiêm lỗi có kiểm soát
 * phục vụ kiểm thử tính an toàn khi đọc sơ đồ ghế (TASK TV1-D15).
 * <p>
 * <b>Quy tắc an toàn bất biến:</b>
 * <ul>
 *   <li>Luôn trả về nguồn dữ liệu là {@link DataSource#FAKE}.</li>
 *   <li><b>TUYỆT ĐỐI KHÔNG BAO GIỜ</b> trả về {@link VerificationLevel#VERIFIED} (bởi vì fake
 *   không phải là Raft consensus thật). Mọi snapshot đều là {@link VerificationLevel#UNVERIFIED}.</li>
 *   <li>{@link #isBarrierSupported()} luôn trả về {@code false}.</li>
 * </ul>
 * <p>
 * Hỗ trợ 7 kịch bản tiêm lỗi:
 * <ul>
 *   <li>{@link Scenario#OK}: Trả về snapshot hợp lệ 10 ghế (UNVERIFIED).</li>
 *   <li>{@link Scenario#READ_UNAVAILABLE}: Giả lập Leader không chứng minh được lease, trả lỗi READ_UNAVAILABLE và toàn bộ 10 ghế UNKNOWN.</li>
 *   <li>{@link Scenario#TIMEOUT}: Giả lập quá hạn chờ mạng/Node không phản hồi (TimeoutException).</li>
 *   <li>{@link Scenario#DISCONNECTED}: Giả lập đứt socket/mất kết nối TCP (IOException).</li>
 *   <li>{@link Scenario#STALE}: Trả về snapshot với timestamp quá khứ và readIndex cũ.</li>
 *   <li>{@link Scenario#DELAY}: Trả về snapshot sau khoảng thời gian delay cấu hình.</li>
 *   <li>{@link Scenario#OUT_OF_ORDER}: Trả về hai phản hồi đọc đảo ngược thứ tự để kiểm tra chống stale/drop của service.</li>
 * </ul>
 */
public class FakeReadAdapter implements ReadAdapter {

    public enum Scenario {
        OK,
        READ_UNAVAILABLE,
        TIMEOUT,
        DISCONNECTED,
        STALE,
        DELAY,
        OUT_OF_ORDER
    }

    private volatile Scenario scenario = Scenario.OK;
    private final Map<String, SeatStatus> seatStatuses = new LinkedHashMap<>();
    private volatile long delayMs = 0;
    private volatile Long readIndex = 42L;
    private volatile String leaderId = "node-1";
    private final AtomicInteger readInvocationCount = new AtomicInteger(0);

    // Dùng để điều khiển kịch bản OUT_OF_ORDER
    private volatile CompletableFuture<SeatsSnapshot> pendingFirstOutOfOrder = null;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fake-read-adapter-thread");
        t.setDaemon(true);
        return t;
    });

    public FakeReadAdapter() {
        resetDefaultSeats();
    }

    public synchronized void resetDefaultSeats() {
        seatStatuses.clear();
        for (int i = 1; i <= SeatsSnapshot.EXPECTED_SEAT_COUNT; i++) {
            String seatId = String.format("G%02d", i);
            if (MockSeatSource.INITIAL_BOOKED_SEATS.contains(seatId)) {
                seatStatuses.put(seatId, SeatStatus.BOOKED);
            } else {
                seatStatuses.put(seatId, SeatStatus.AVAILABLE);
            }
        }
    }

    @Override
    public CompletableFuture<SeatsSnapshot> readSeats(String tripId) {
        readInvocationCount.incrementAndGet();
        final String effectiveTripId = (tripId != null && !tripId.isBlank()) ? tripId : SeatsSnapshot.DEFAULT_TRIP_ID;

        switch (scenario) {
            case READ_UNAVAILABLE -> {
                SeatsSnapshot snapshot = SeatsSnapshot.readUnavailable(
                        effectiveTripId,
                        DataSource.FAKE,
                        leaderId,
                        "Fake: Leader không chứng minh được lease (READ_UNAVAILABLE)"
                );
                assertNotVerified(snapshot);
                return executeWithOptionalDelay(() -> snapshot);
            }

            case TIMEOUT -> {
                CompletableFuture<SeatsSnapshot> future = new CompletableFuture<>();
                long wait = delayMs > 0 ? delayMs : 50;
                scheduler.schedule(() -> future.completeExceptionally(
                        new TimeoutException("Fake: Quá thời gian chờ phản hồi sơ đồ ghế từ Node")
                ), wait, TimeUnit.MILLISECONDS);
                return future;
            }

            case DISCONNECTED -> {
                CompletableFuture<SeatsSnapshot> future = new CompletableFuture<>();
                long wait = delayMs > 0 ? delayMs : 10;
                scheduler.schedule(() -> future.completeExceptionally(
                        new IOException("Fake: Mất kết nối TCP socket tới cụm máy chủ")
                ), wait, TimeUnit.MILLISECONDS);
                return future;
            }

            case STALE -> {
                Map<String, SeatStatus> staleMap = getCopyOfSeats();
                // Dữ liệu cũ với timestamp 5 phút trước
                SeatsSnapshot snapshot = new SeatsSnapshot(
                        effectiveTripId,
                        staleMap,
                        VerificationLevel.UNVERIFIED,
                        DataSource.FAKE,
                        Instant.now().minusSeconds(300),
                        1L,
                        leaderId,
                        null,
                        null
                );
                assertNotVerified(snapshot);
                return executeWithOptionalDelay(() -> snapshot);
            }

            case DELAY -> {
                long wait = delayMs > 0 ? delayMs : 100;
                return scheduleSnapshot(effectiveTripId, wait);
            }

            case OUT_OF_ORDER -> {
                // Cuộc gọi thứ 1 sẽ bị hoãn lại, cuộc gọi thứ 2 sẽ hoàn thành trước
                if (pendingFirstOutOfOrder == null) {
                    CompletableFuture<SeatsSnapshot> first = new CompletableFuture<>();
                    pendingFirstOutOfOrder = first;
                    // Lần 1 giữ lại snapshot cũ với G01 AVAILABLE
                    Map<String, SeatStatus> oldSeats = getCopyOfSeats();
                    oldSeats.put("G01", SeatStatus.AVAILABLE);
                    SeatsSnapshot oldSnapshot = SeatsSnapshot.unverified(
                            effectiveTripId, oldSeats, DataSource.FAKE, 40L, leaderId
                    );
                    assertNotVerified(oldSnapshot);

                    // Hẹn giờ hoàn thành lần 1 sau 150ms
                    scheduler.schedule(() -> {
                        first.complete(oldSnapshot);
                        pendingFirstOutOfOrder = null;
                    }, 150, TimeUnit.MILLISECONDS);
                    return first;
                } else {
                    // Lần 2 hoàn thành ngay sau 20ms với dữ liệu mới hơn (G01 BOOKED)
                    CompletableFuture<SeatsSnapshot> second = new CompletableFuture<>();
                    Map<String, SeatStatus> newSeats = getCopyOfSeats();
                    newSeats.put("G01", SeatStatus.BOOKED);
                    SeatsSnapshot newSnapshot = SeatsSnapshot.unverified(
                            effectiveTripId, newSeats, DataSource.FAKE, 43L, leaderId
                    );
                    assertNotVerified(newSnapshot);

                    scheduler.schedule(() -> second.complete(newSnapshot), 20, TimeUnit.MILLISECONDS);
                    return second;
                }
            }

            case OK -> {
                return executeWithOptionalDelay(() -> {
                    SeatsSnapshot snapshot = SeatsSnapshot.unverified(
                            effectiveTripId,
                            getCopyOfSeats(),
                            DataSource.FAKE,
                            readIndex,
                            leaderId
                    );
                    assertNotVerified(snapshot);
                    return snapshot;
                });
            }

            default -> throw new IllegalStateException("Kịch bản không được hỗ trợ: " + scenario);
        }
    }

    private CompletableFuture<SeatsSnapshot> scheduleSnapshot(String tripId, long waitMs) {
        CompletableFuture<SeatsSnapshot> future = new CompletableFuture<>();
        scheduler.schedule(() -> {
            SeatsSnapshot snapshot = SeatsSnapshot.unverified(
                    tripId,
                    getCopyOfSeats(),
                    DataSource.FAKE,
                    readIndex,
                    leaderId
            );
            assertNotVerified(snapshot);
            future.complete(snapshot);
        }, waitMs, TimeUnit.MILLISECONDS);
        return future;
    }

    private CompletableFuture<SeatsSnapshot> executeWithOptionalDelay(java.util.function.Supplier<SeatsSnapshot> supplier) {
        if (delayMs <= 0) {
            return CompletableFuture.completedFuture(supplier.get());
        }
        CompletableFuture<SeatsSnapshot> future = new CompletableFuture<>();
        scheduler.schedule(() -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
        return future;
    }

    private synchronized Map<String, SeatStatus> getCopyOfSeats() {
        return new LinkedHashMap<>(seatStatuses);
    }

    /**
     * Khẳng định an toàn: Fake tuyệt đối không bao giờ được trả về VERIFIED.
     */
    private static void assertNotVerified(SeatsSnapshot snapshot) {
        if (snapshot.getVerification() == VerificationLevel.VERIFIED) {
            throw new IllegalStateException("INVARIANT VIOLATION: FakeReadAdapter tuyệt đối không bao giờ được trả về VERIFIED!");
        }
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.FAKE;
    }

    @Override
    public boolean isBarrierSupported() {
        return false;
    }

    public Scenario getScenario() {
        return scenario;
    }

    public void setScenario(Scenario scenario) {
        this.scenario = scenario != null ? scenario : Scenario.OK;
    }

    public synchronized void setSeatStatus(String seatId, SeatStatus status) {
        if (seatId != null && status != null) {
            seatStatuses.put(seatId, status);
        }
    }

    public synchronized void setSeats(Map<String, SeatStatus> newSeats) {
        if (newSeats != null) {
            seatStatuses.clear();
            seatStatuses.putAll(newSeats);
        }
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = Math.max(0, delayMs);
    }

    public Long getReadIndex() {
        return readIndex;
    }

    public void setReadIndex(Long readIndex) {
        this.readIndex = readIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getReadInvocationCount() {
        return readInvocationCount.get();
    }

    public void resetInvocationCount() {
        readInvocationCount.set(0);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
