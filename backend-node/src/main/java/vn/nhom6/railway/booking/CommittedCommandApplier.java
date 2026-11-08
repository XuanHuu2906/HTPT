package vn.nhom6.railway.booking;

import java.util.Objects;
import java.util.Optional;

/**
 * Điểm nối domain được gọi khi Raft báo một entry đã commit. Lớp này không thực hiện Raft,
 * replication, persistence JDBC hay giao tiếp mạng.
 */
public final class CommittedCommandApplier {

    /**
     * Callback tạm tối giản cho phía Raft: mỗi lần gọi là một entry đã commit, theo index tăng.
     * Có thể truyền {@link #commitListener()} cho adapter Raft; callback không tự commit entry.
     */
    @FunctionalInterface
    public interface CommitListener {
        Optional<BookingStateMachine.ApplyOutcome> onCommitted(long logIndex, BookingCommand command);
    }

    /** Hook thông báo kết quả domain cho tầng chờ của Leader sau khi repository apply thành công. */
    @FunctionalInterface
    public interface ApplyResultListener {
        void onApplied(ApplyResult result);
    }

    private final BookingRepository repository;
    private final ApplyResultListener resultListener;

    public CommittedCommandApplier(BookingRepository repository) {
        this(repository, result -> { });
    }

    public CommittedCommandApplier(
            BookingRepository repository,
            ApplyResultListener resultListener) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.resultListener = Objects.requireNonNull(resultListener, "resultListener");
    }

    /** Raft-side adapter callback; it accepts committed entries only. */
    public CommitListener commitListener() {
        return this::applyCommitted;
    }

    /**
     * Apply một entry đã commit theo đúng thứ tự. Entry cũ được tra bằng logIndex và không apply
     * lần hai; NO_OP/conflict cũ trả Optional.empty(). Lỗ index fail-closed. Hook chỉ được gọi
     * sau khi repository hoàn tất apply nguyên tử.
     *
     * @return outcome của lần apply mới, hoặc outcome đã lưu cho redelivery có kết quả; rỗng nếu
     *         redelivery là NO_OP/conflict chưa có kết quả nghiệp vụ
     */
    public Optional<BookingStateMachine.ApplyOutcome> applyCommitted(
            long logIndex,
            BookingCommand command) {
        Objects.requireNonNull(command, "command");
        if (logIndex < 1) {
            throw new IllegalArgumentException("logIndex phải >= 1: " + logIndex);
        }

        long lastApplied = repository.lastApplied();
        if (logIndex <= lastApplied) {
            Optional<BookingStateMachine.ApplyOutcome> previous =
                    Objects.requireNonNull(repository.findAppliedOutcome(logIndex),
                            "findAppliedOutcome không được trả null");
            if (previous.isEmpty()) {
                throw new IllegalStateException(
                        "Thiếu outcome bền vững cho index đã apply: " + logIndex);
            }
            ensureNoUnknown(previous.orElseThrow());
            if (previous.orElseThrow() instanceof BookingStateMachine.ApplyOutcome.BusinessResult) {
                return previous;
            }
            return Optional.empty();
        }

        if (lastApplied == Long.MAX_VALUE || logIndex != lastApplied + 1) {
            throw new IllegalStateException(
                    "Committed index có lỗ: cần " + (lastApplied + 1)
                    + " nhưng nhận " + logIndex + " (lastApplied=" + lastApplied + ")");
        }

        BookingStateMachine.ApplyOutcome outcome = Objects.requireNonNull(
                repository.applyAtomically(logIndex, command),
                "applyAtomically không được trả null");
        ensureNoUnknown(outcome);

        if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult business) {
            // Chỉ phát kết quả sau khi applyAtomically đã trả về thành công.
            resultListener.onApplied(business.applyResult());
        }
        return Optional.of(outcome);
    }

    private static void ensureNoUnknown(BookingStateMachine.ApplyOutcome outcome) {
        if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult business
                && !business.applyResult().isPersistable()) {
            throw new IllegalStateException("UNKNOWN không phải kết quả của committed applier");
        }
    }
}