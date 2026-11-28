package vn.nhom6.railway.client.network;

import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Adapter bọc {@link BookingClient} để cung cấp dữ liệu sơ đồ ghế qua giao diện {@link ReadAdapter} (TASK TV1-D15).
 * <p>
 * <b>Quy tắc an toàn bất biến:</b>
 * <ul>
 *   <li>Trước khi TV4-D19 (Raft Read Barrier) được nghiệm thu, mọi snapshot đọc được từ Node
 *   đều mang mức {@link VerificationLevel#UNVERIFIED}.</li>
 *   <li>{@link #isBarrierSupported()} mặc định là {@code false}.</li>
 *   <li>Nếu gặp {@link NotLeaderException} hoặc mã lỗi {@code READ_UNAVAILABLE},
 *   toàn bộ 10 ghế chuyển sang trạng thái {@link SeatStatus#UNKNOWN} kèm mã lỗi rõ ràng.</li>
 * </ul>
 */
public class BookingClientReadAdapter implements ReadAdapter {

    private final BookingClient bookingClient;

    public BookingClientReadAdapter(BookingClient bookingClient) {
        this.bookingClient = Objects.requireNonNull(bookingClient, "bookingClient không được null");
    }

    @Override
    public CompletableFuture<SeatsSnapshot> readSeats(String tripId) {
        final String effectiveTripId = (tripId != null && !tripId.isBlank()) ? tripId : SeatsSnapshot.DEFAULT_TRIP_ID;
        GetSeatsRequestDto request = new GetSeatsRequestDto(effectiveTripId);

        return bookingClient.getSeats(request)
                .thenApply(dto -> {
                    DataSource source = (dto.isMock() || DevSupportBridge.isDevMockClient(bookingClient))
                            ? DataSource.MOCK
                            : DataSource.REAL;

                    Map<String, SeatStatus> seatMap = new LinkedHashMap<>();
                    List<SeatInfoDto> seatList = dto.getSeats();
                    if (seatList != null) {
                        for (SeatInfoDto info : seatList) {
                            if (info != null && info.getSeatId() != null) {
                                String rawStatus = info.getStatus();
                                SeatStatus st;
                                if ("AVAILABLE".equalsIgnoreCase(rawStatus)) {
                                    st = SeatStatus.AVAILABLE;
                                } else if ("BOOKED".equalsIgnoreCase(rawStatus)) {
                                    st = SeatStatus.BOOKED;
                                } else {
                                    st = SeatStatus.UNKNOWN;
                                }
                                seatMap.put(info.getSeatId(), st);
                            }
                        }
                    }

                    // Đảm bảo đủ đúng 10 ghế G01 đến G10 theo SPEC-03
                    for (int i = 1; i <= SeatsSnapshot.EXPECTED_SEAT_COUNT; i++) {
                        seatMap.putIfAbsent(String.format("G%02d", i), SeatStatus.UNKNOWN);
                    }

                    // Invariant: Trước TV4-D19, verification luôn là UNVERIFIED
                    VerificationLevel verification = VerificationLevel.UNVERIFIED;

                    return new SeatsSnapshot(
                            dto.getTripId() != null ? dto.getTripId() : effectiveTripId,
                            seatMap,
                            verification,
                            source,
                            Instant.now(),
                            dto.getReadIndex(),
                            dto.getLeaderId(),
                            null,
                            null
                    );
                })
                .exceptionally(throwable -> {
                    Throwable cause = throwable;
                    while (cause.getCause() != null && cause != cause.getCause()) {
                        cause = cause.getCause();
                    }

                    DataSource source = DevSupportBridge.isDevMockClient(bookingClient)
                            ? DataSource.MOCK
                            : DataSource.REAL;

                    if (cause instanceof NotLeaderException nle) {
                        return SeatsSnapshot.readUnavailable(
                                effectiveTripId,
                                source,
                                nle.getLeaderHint(),
                                "Node không phải Leader (NOT_LEADER), leaderHint=" + nle.getLeaderHint()
                        );
                    }

                    String msg = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    if (msg.contains("READ_UNAVAILABLE")) {
                        return SeatsSnapshot.readUnavailable(
                                effectiveTripId,
                                source,
                                null,
                                msg
                        );
                    }

                    if (cause instanceof TimeoutException || msg.contains("Timeout") || msg.contains("thời gian")) {
                        return SeatsSnapshot.error(
                                effectiveTripId,
                                source,
                                "TIMEOUT_UNKNOWN",
                                "Quá thời gian chờ phản hồi sơ đồ ghế từ Node: " + msg
                        );
                    }

                    if (cause instanceof IOException || msg.contains("kết nối")) {
                        return SeatsSnapshot.error(
                                effectiveTripId,
                                source,
                                "DISCONNECTED",
                                "Mất kết nối tới cụm máy chủ: " + msg
                        );
                    }

                    return SeatsSnapshot.error(
                            effectiveTripId,
                            source,
                            "INTERNAL_ERROR",
                            "Lỗi khi đọc sơ đồ ghế: " + msg
                    );
                });
    }

    @Override
    public DataSource getDataSource() {
        return DevSupportBridge.isDevMockClient(bookingClient) ? DataSource.MOCK : DataSource.REAL;
    }

    @Override
    public boolean isBarrierSupported() {
        // Trước TV4-D19, cluster chưa hỗ trợ linearizable read barrier
        return false;
    }

    public BookingClient getBookingClient() {
        return bookingClient;
    }

    @Override
    public void close() {
        // BookingClient lifecycle được quản lý tập trung ở ClientFactory
    }
}
