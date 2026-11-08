package vn.nhom6.railway.booking;

import java.util.Map;
import java.util.Optional;

/**
 * Biên lưu trữ cho {@link BookingStateMachine}; triển khai tương lai có thể dùng PostgreSQL/JDBC
 * mà không làm thay đổi logic domain.
 *
 * <p>Mỗi entry chỉ được apply sau khi Raft commit và theo đúng thứ tự {@code lastApplied + 1}.
 * Thay đổi nghiệp vụ, kết quả theo requestId, kết quả apply theo logIndex và lastApplied phải
 * được commit/rollback như một đơn vị nguyên tử. Nếu lỗi, lastApplied không tăng và entry phải
 * được retry trước khi apply entry tiếp theo. Apply tất định, không sinh UUID/thời gian/random.
 * Kết quả của index đã lưu phải được dùng lại khi redelivery, không apply lần hai.
 *
 * <p>{@link ApplyResult.Outcome#UNKNOWN} không thuộc kết quả lưu trữ/applier; UNKNOWN chỉ do tầng
 * Leader chờ timeout tạo ra. Hợp đồng chỉ dùng kiểu domain, không phụ thuộc wire DTO hay JDBC.
 */
public interface BookingRepository {

    /** Chỉ số entry cuối cùng đã apply bền vững; ban đầu là 0. */
    long lastApplied();

    /**
     * Apply nguyên tử một entry đã commit, trả kết quả theo state machine: BusinessResult,
     * NoOpApplied hoặc RequestIdConflict. Kết quả theo index được giữ để redelivery trả đúng kết
     * quả trước đó, kể cả khi entry không có kết quả nghiệp vụ.
     *
     * @throws StorageException nếu lưu trữ thất bại; khi đó toàn bộ transaction phải rollback
     */
    BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command);

    /** Tra kết quả nghiệp vụ bền vững theo requestId; UNKNOWN không bao giờ được trả về. */
    Optional<ApplyResult> findResultByRequestId(String requestId);

    /**
     * Đọc outcome đã apply theo logIndex để phục vụ redelivery mà không apply lần hai.
     * Outcome NoOpApplied/RequestIdConflict được lưu làm dấu vết apply, không phải kết quả nghiệp vụ.
     */
    Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex);

    /** Snapshot trạng thái 10 ghế G01–G10, dùng cho GET_SEATS; giá trị là AVAILABLE/BOOKED. */
    Map<String, String> readSeatSnapshot();

    /** Lỗi lưu trữ unchecked; transaction implementation phải bảo đảm rollback trước khi ném. */
    final class StorageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public StorageException(String message) {
            super(message);
        }

        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}