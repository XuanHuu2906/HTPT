package vn.nhom6.railway.client.network;

import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bản sao bất biến (immutable snapshot) dữ liệu trạng thái 10 ghế (G01–G10)
 * kèm đầy đủ siêu dữ liệu về mức độ xác minh (VerificationLevel), nguồn dữ liệu (DataSource)
 * và thời điểm đọc (TASK TV1-D15).
 * <p>
 * Theo SPEC-03 và SPEC-04, dữ liệu đọc chưa qua Read Barrier luôn mang mức
 * {@link VerificationLevel#UNVERIFIED}.
 */
public final class SeatsSnapshot {

    public static final String DEFAULT_TRIP_ID = "SE-001";
    public static final int EXPECTED_SEAT_COUNT = 10;

    private final String tripId;
    private final Map<String, SeatStatus> seats;
    private final VerificationLevel verification;
    private final DataSource source;
    private final Instant asOf;
    private final Long readIndex;
    private final String leaderId;
    private final String errorCode;
    private final String errorMessage;

    public SeatsSnapshot(
            String tripId,
            Map<String, SeatStatus> seats,
            VerificationLevel verification,
            DataSource source,
            Instant asOf,
            Long readIndex,
            String leaderId,
            String errorCode,
            String errorMessage) {
        this.tripId = (tripId != null && !tripId.isBlank()) ? tripId : DEFAULT_TRIP_ID;
        this.verification = Objects.requireNonNullElse(verification, VerificationLevel.UNVERIFIED);
        this.source = Objects.requireNonNullElse(source, DataSource.NONE);
        this.asOf = Objects.requireNonNullElseGet(asOf, Instant::now);
        this.readIndex = readIndex;
        this.leaderId = leaderId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;

        Map<String, SeatStatus> map = new LinkedHashMap<>();
        if (seats != null) {
            map.putAll(seats);
        }
        // Đảm bảo đủ đúng 10 ghế G01 đến G10 theo SPEC-03
        for (int i = 1; i <= EXPECTED_SEAT_COUNT; i++) {
            String seatId = String.format("G%02d", i);
            map.putIfAbsent(seatId, SeatStatus.UNKNOWN);
        }
        this.seats = Collections.unmodifiableMap(map);
    }

    public static SeatsSnapshot unverified(
            String tripId,
            Map<String, SeatStatus> seats,
            DataSource source,
            Long readIndex,
            String leaderId) {
        return new SeatsSnapshot(
                tripId,
                seats,
                VerificationLevel.UNVERIFIED,
                source,
                Instant.now(),
                readIndex,
                leaderId,
                null,
                null
        );
    }

    public static SeatsSnapshot verified(
            String tripId,
            Map<String, SeatStatus> seats,
            DataSource source,
            Long readIndex,
            String leaderId) {
        return new SeatsSnapshot(
                tripId,
                seats,
                VerificationLevel.VERIFIED,
                source,
                Instant.now(),
                readIndex,
                leaderId,
                null,
                null
        );
    }

    public static SeatsSnapshot readUnavailable(
            String tripId,
            DataSource source,
            String leaderId,
            String message) {
        Map<String, SeatStatus> allUnknown = createAllUnknownMap();
        return new SeatsSnapshot(
                tripId,
                allUnknown,
                VerificationLevel.UNVERIFIED,
                source,
                Instant.now(),
                null,
                leaderId,
                "READ_UNAVAILABLE",
                message != null ? message : "Không thể chứng minh trạng thái ghế hiện hành từ máy chủ Leader"
        );
    }

    public static SeatsSnapshot error(
            String tripId,
            DataSource source,
            String errorCode,
            String errorMessage) {
        Map<String, SeatStatus> allUnknown = createAllUnknownMap();
        return new SeatsSnapshot(
                tripId,
                allUnknown,
                VerificationLevel.UNVERIFIED,
                source,
                Instant.now(),
                null,
                null,
                errorCode,
                errorMessage
        );
    }

    public static Map<String, SeatStatus> createAllUnknownMap() {
        Map<String, SeatStatus> map = new LinkedHashMap<>();
        for (int i = 1; i <= EXPECTED_SEAT_COUNT; i++) {
            map.put(String.format("G%02d", i), SeatStatus.UNKNOWN);
        }
        return map;
    }

    public String getTripId() {
        return tripId;
    }

    public Map<String, SeatStatus> getSeats() {
        return seats;
    }

    public SeatStatus getSeatStatus(String seatId) {
        return seats.getOrDefault(seatId, SeatStatus.UNKNOWN);
    }

    public VerificationLevel getVerification() {
        return verification;
    }

    public boolean isVerified() {
        return verification.isVerified();
    }

    public DataSource getSource() {
        return source;
    }

    public Instant getAsOf() {
        return asOf;
    }

    public Long getReadIndex() {
        return readIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isSuccess() {
        return errorCode == null;
    }

    public boolean isUnavailable() {
        return "READ_UNAVAILABLE".equalsIgnoreCase(errorCode);
    }

    @Override
    public String toString() {
        return "SeatsSnapshot{" +
                "tripId='" + tripId + '\'' +
                ", seatsCount=" + seats.size() +
                ", verification=" + verification +
                ", source=" + source +
                ", asOf=" + asOf +
                ", readIndex=" + readIndex +
                ", leaderId='" + leaderId + '\'' +
                ", errorCode='" + errorCode + '\'' +
                '}';
    }
}
