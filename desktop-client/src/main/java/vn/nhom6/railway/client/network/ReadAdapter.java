package vn.nhom6.railway.client.network;

import java.util.concurrent.CompletableFuture;

/**
 * Interface trừu tượng hóa việc đọc sơ đồ trạng thái 10 ghế (TASK TV1-D15).
 * <p>
 * Cho phép UI và {@link vn.nhom6.railway.client.SeatsRefreshService} giao tiếp với
 * các nguồn dữ liệu khác nhau:
 * <ul>
 *   <li>{@link FakeReadAdapter}: Giả lập tiêm lỗi cho kiểm thử tự động.</li>
 *   <li>{@link BookingClientReadAdapter}: Đọc qua TCP Socket/BookingClient thật.</li>
 * </ul>
 * Mọi thao tác đều trả về {@link CompletableFuture} bất đồng bộ, không bao giờ chặn UI thread.
 */
public interface ReadAdapter extends AutoCloseable {

    /**
     * Đọc snapshot 10 ghế cho một chuyến tàu cụ thể.
     *
     * @param tripId mã chuyến tàu (mặc định "SE-001")
     * @return CompletableFuture chứa {@link SeatsSnapshot}
     */
    CompletableFuture<SeatsSnapshot> readSeats(String tripId);

    /**
     * Đọc snapshot 10 ghế cho chuyến tàu mặc định ("SE-001").
     */
    default CompletableFuture<SeatsSnapshot> readSeats() {
        return readSeats(SeatsSnapshot.DEFAULT_TRIP_ID);
    }

    /**
     * Nguồn gốc dữ liệu của adapter này (FAKE, MOCK, REAL).
     */
    DataSource getDataSource();

    /**
     * Kiểm tra xem adapter và cụm backend hiện tại đã hỗ trợ Raft Read Barrier (TV4-D19) hay chưa.
     * Trước khi TV4-D19 được nghiệm thu, phương thức này luôn trả về false.
     */
    boolean isBarrierSupported();

    @Override
    default void close() throws Exception {
    }
}
