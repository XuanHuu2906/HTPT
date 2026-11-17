package vn.nhom6.railway.booking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cầu nối giữa callback "entry đã commit" của Raft ({@link RaftNode.CommitListener})
 * và {@link CommittedCommandApplier} / {@link JdbcCommittedCommandApplier}.
 *
 * <p>Tuân thủ SPEC-04 §5 (apply sau commit), §6 (thứ tự, replay, restart), §7 (read barrier NO_OP),
 * và SPEC-03 §4.4 (phân biệt PENDING / APPLIED).
 */
public final class RaftApplyAdapter implements RaftNode.CommitListener {

    private static final Logger LOGGER = Logger.getLogger(RaftApplyAdapter.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Functional interface cho việc áp dụng một entry đã commit.
     */
    @FunctionalInterface
    public interface EntryApplier {
        Optional<BookingStateMachine.ApplyOutcome> applyCommitted(long logIndex, BookingCommand command);
    }

    /**
     * Trạng thái của request theo SPEC-03 §4.4.
     */
    public enum RequestState {
        NONE,
        PENDING,
        APPLIED
    }

    /**
     * Ngoại lệ ném ra khi giải mã command từ RaftLogEntry thất bại (fail-closed).
     */
    public static class CommandDecodeException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CommandDecodeException(String message) {
            super(message);
        }

        public CommandDecodeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final EntryApplier applier;
    private final BookingRepository repository;
    private final LongSupplier commitIndexSupplier;
    private final Consumer<Long> onAppliedCallback;

    private long commitIndex;
    private long lastAppliedInMemory;

    private final ConcurrentMap<String, CompletableFuture<ApplyResult>> pendingRequests =
            new ConcurrentHashMap<>();
    private final Map<Long, List<CompletableFuture<Void>>> indexWaiters =
            new LinkedHashMap<>();

    public RaftApplyAdapter(
            EntryApplier applier,
            BookingRepository repository,
            LongSupplier commitIndexSupplier,
            Consumer<Long> onAppliedCallback) {
        this.applier = Objects.requireNonNull(applier, "applier");
        this.repository = repository;
        this.commitIndexSupplier = commitIndexSupplier;
        this.onAppliedCallback = onAppliedCallback;

        long initialLastApplied = 0L;
        if (repository != null) {
            try {
                initialLastApplied = repository.lastApplied();
            } catch (BookingRepository.StorageException exception) {
                LOGGER.log(Level.SEVERE, "Could not initialize lastApplied from repository", exception);
                throw exception;
            }
        }
        this.lastAppliedInMemory = initialLastApplied;
        this.commitIndex = initialLastApplied;
    }

    public RaftApplyAdapter(
            CommittedCommandApplier applier,
            BookingRepository repository) {
        this(applier::applyCommitted, repository, null, null);
    }

    public RaftApplyAdapter(
            CommittedCommandApplier applier,
            BookingRepository repository,
            LongSupplier commitIndexSupplier,
            Consumer<Long> onAppliedCallback) {
        this(applier::applyCommitted, repository, commitIndexSupplier, onAppliedCallback);
    }

    public RaftApplyAdapter(
            JdbcCommittedCommandApplier applier,
            BookingRepository repository) {
        this(applier::applyCommitted, repository, null, null);
    }

    public RaftApplyAdapter(
            JdbcCommittedCommandApplier applier,
            BookingRepository repository,
            LongSupplier commitIndexSupplier,
            Consumer<Long> onAppliedCallback) {
        this(applier::applyCommitted, repository, commitIndexSupplier, onAppliedCallback);
    }

    public RaftApplyAdapter(
            BookingStateMachine stateMachine,
            BookingRepository repository) {
        this((idx, cmd) -> Optional.of(stateMachine.apply(idx, cmd)), repository, null, null);
    }

    public RaftApplyAdapter(
            BookingStateMachine stateMachine,
            BookingRepository repository,
            LongSupplier commitIndexSupplier,
            Consumer<Long> onAppliedCallback) {
        this((idx, cmd) -> Optional.of(stateMachine.apply(idx, cmd)), repository, commitIndexSupplier, onAppliedCallback);
    }

    /**
     * Cập nhật commitIndex theo Raft. commitIndex chỉ được tăng, không bao giờ giảm.
     */
    public synchronized void advanceCommitIndex(long newCommitIndex) {
        if (newCommitIndex > this.commitIndex) {
            this.commitIndex = newCommitIndex;
        }
    }

    /**
     * Trả về commitIndex hiện tại được biết bởi adapter.
     */
    public synchronized long commitIndex() {
        if (commitIndexSupplier != null) {
            long supplied = commitIndexSupplier.getAsLong();
            if (supplied > this.commitIndex) {
                this.commitIndex = supplied;
            }
        }
        return this.commitIndex;
    }

    /**
     * Trả về lastApplied bền vững (từ DB) hoặc in-memory.
     */
    public long lastApplied() {
        return repository != null ? repository.lastApplied() : lastAppliedInMemory;
    }

    /**
     * Nhận callback entry đã commit từ Raft.
     *
     * <p>Chỉ chấp nhận entry có index <= commitIndex. Entry index > commitIndex bị từ chối
     * và ném {@link IllegalStateException}.
     */
    @Override
    public synchronized void onCommitted(RaftLogEntry entry) {
        Objects.requireNonNull(entry, "entry");

        long currentCommit = commitIndex();
        if (entry.index() > currentCommit) {
            throw new IllegalStateException(
                    "Entry at log index " + entry.index()
                            + " has not been committed (commitIndex=" + currentCommit + ")");
        }

        // Giải mã command: fail closed nếu payload lỗi hoặc loại lệnh lạ
        BookingCommand command;
        try {
            command = decodeCommand(entry);
        } catch (RuntimeException exception) {
            LOGGER.log(Level.SEVERE, "Failed to decode Raft log entry at index " + entry.index(), exception);
            throw exception;
        }

        long previousLastApplied = lastApplied();

        // Áp dụng tuần tự vào state machine / DB
        Optional<BookingStateMachine.ApplyOutcome> outcome;
        try {
            outcome = applier.applyCommitted(entry.index(), command);
        } catch (RuntimeException exception) {
            LOGGER.log(Level.SEVERE, "Apply failed for committed entry at index " + entry.index(), exception);
            throw exception;
        }

        long currentLastApplied = lastApplied();
        if (repository == null && entry.index() == previousLastApplied + 1) {
            lastAppliedInMemory = entry.index();
            currentLastApplied = entry.index();
        }

        // Báo tiến độ apply cho RaftNode (nếu có callback và là bước apply mới)
        if (currentLastApplied > previousLastApplied && onAppliedCallback != null) {
            try {
                onAppliedCallback.accept(currentLastApplied);
            } catch (RuntimeException exception) {
                LOGGER.log(Level.WARNING, "onAppliedCallback failed for index " + currentLastApplied, exception);
            }
        }

        // Hoàn tất future đang chờ cho Leader (chỉ hoàn tất SAU KHI DB apply thành công)
        String requestId = extractRequestId(command);
        if (requestId != null) {
            CompletableFuture<ApplyResult> future = pendingRequests.remove(requestId);
            if (future != null && outcome.isPresent()) {
                BookingStateMachine.ApplyOutcome applyOutcome = outcome.get();
                if (applyOutcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult biz) {
                    future.complete(biz.applyResult());
                } else if (applyOutcome instanceof BookingStateMachine.ApplyOutcome.RequestIdConflict) {
                    future.completeExceptionally(new BookingRequestHandler.RequestIdConflictException());
                }
            }
        }

        // Giải phóng các waiter theo log index (read barrier)
        notifyIndexWaiters(currentLastApplied);
    }

    /**
     * Đăng ký chờ kết quả cho một requestId ở phía Leader.
     * Nếu đã được apply trước đó, future hoàn tất ngay với kết quả đã lưu.
     */
    public synchronized CompletableFuture<ApplyResult> registerPending(String requestId) {
        BookingCommand.requireValidRequestId(requestId);
        if (repository != null) {
            Optional<ApplyResult> existing = repository.findResultByRequestId(requestId);
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(existing.get());
            }
        }
        return pendingRequests.computeIfAbsent(requestId, k -> new CompletableFuture<>());
    }

    /**
     * Tra cứu trạng thái của request (NONE, PENDING, APPLIED).
     */
    public RequestState requestState(String requestId) {
        return statusOf(requestId);
    }

    /**
     * Alias cho {@link #requestState(String)}.
     */
    public synchronized RequestState statusOf(String requestId) {
        BookingCommand.requireValidRequestId(requestId);
        if (pendingRequests.containsKey(requestId)) {
            return RequestState.PENDING;
        }
        if (repository != null) {
            try {
                if (repository.findResultByRequestId(requestId).isPresent()) {
                    return RequestState.APPLIED;
                }
            } catch (BookingRepository.StorageException exception) {
                LOGGER.log(Level.WARNING, "Failed to query repository for requestId " + requestId, exception);
            }
        }
        return RequestState.NONE;
    }

    /**
     * Tra cứu kết quả nghiệp vụ đã lưu theo requestId nếu đã APPLIED.
     */
    public Optional<ApplyResult> findResult(String requestId) {
        BookingCommand.requireValidRequestId(requestId);
        if (repository != null) {
            return repository.findResultByRequestId(requestId);
        }
        return Optional.empty();
    }

    /**
     * Read barrier: Trả về future hoàn tất khi {@code lastApplied() >= targetIndex}.
     */
    public synchronized CompletableFuture<Void> awaitIndex(long targetIndex) {
        if (targetIndex < 1) {
            throw new IllegalArgumentException("targetIndex must be >= 1 but was " + targetIndex);
        }
        if (lastApplied() >= targetIndex) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        indexWaiters.computeIfAbsent(targetIndex, k -> new ArrayList<>()).add(future);
        return future;
    }

    /**
     * Hủy tất cả future đang chờ bằng ngoại lệ (dùng khi step-down hoặc hết hạn chờ).
     * Tuyệt đối KHÔNG ghi nhận thất bại vào DB.
     */
    public synchronized void cancelPending(Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        for (CompletableFuture<ApplyResult> future : pendingRequests.values()) {
            future.completeExceptionally(cause);
        }
        pendingRequests.clear();

        for (List<CompletableFuture<Void>> waiters : indexWaiters.values()) {
            for (CompletableFuture<Void> waiter : waiters) {
                waiter.completeExceptionally(cause);
            }
        }
        indexWaiters.clear();
    }

    /**
     * Hủy một future đang chờ cụ thể theo requestId.
     */
    public synchronized boolean cancelPending(String requestId, Throwable cause) {
        BookingCommand.requireValidRequestId(requestId);
        Objects.requireNonNull(cause, "cause");
        CompletableFuture<ApplyResult> future = pendingRequests.remove(requestId);
        if (future != null) {
            future.completeExceptionally(cause);
            return true;
        }
        return false;
    }

    /**
     * Số lượng request đang PENDING trong RAM.
     */
    public int pendingCount() {
        return pendingRequests.size();
    }

    private synchronized void notifyIndexWaiters(long currentLastApplied) {
        Iterator<Map.Entry<Long, List<CompletableFuture<Void>>>> iterator = indexWaiters.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<CompletableFuture<Void>>> mapEntry = iterator.next();
            if (mapEntry.getKey() <= currentLastApplied) {
                for (CompletableFuture<Void> waiter : mapEntry.getValue()) {
                    waiter.complete(null);
                }
                iterator.remove();
            }
        }
    }

    private static String extractRequestId(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return book.requestId();
        } else if (command instanceof BookingCommand.CancelTicket cancel) {
            return cancel.requestId();
        }
        return null;
    }

    /**
     * Giải mã RaftLogEntry sang BookingCommand.
     */
    public static BookingCommand decodeCommand(RaftLogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        String type = entry.commandType();
        String payload = entry.commandPayload();
        try {
            return switch (type) {
                case RaftLogEntry.COMMAND_TYPE_BOOK_TICKET -> {
                    JsonNode node = MAPPER.readTree(payload);
                    yield new BookingCommand.BookTicket(
                            text(node, "requestId"),
                            text(node, "tripId"),
                            text(node, "seatId"),
                            text(node, "passengerName"),
                            text(node, "ticketId"),
                            parseInstant(text(node, "issuedAt"), "issuedAt"));
                }
                case RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET -> {
                    JsonNode node = MAPPER.readTree(payload);
                    yield new BookingCommand.CancelTicket(
                            text(node, "requestId"),
                            text(node, "ticketId"),
                            parseInstant(text(node, "cancelledAt"), "cancelledAt"));
                }
                case RaftLogEntry.COMMAND_TYPE_NO_OP -> new BookingCommand.NoOp();
                default -> throw new CommandDecodeException("Unsupported commandType: " + type);
            };
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            throw new CommandDecodeException(
                    "Failed to decode command of type '" + type + "' at log index " + entry.index(), exception);
        }
    }

    private static String text(JsonNode node, String fieldName) {
        if (node == null) {
            throw new IllegalArgumentException("JSON node is null");
        }
        JsonNode value = node.get(fieldName);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("Missing required text field: " + fieldName);
        }
        return value.asText();
    }

    private static Instant parseInstant(String text, String fieldName) {
        if (text == null) {
            throw new IllegalArgumentException("Missing timestamp field: " + fieldName);
        }
        try {
            return Instant.parse(text);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid ISO-8601 timestamp in field " + fieldName + ": " + text, exception);
        }
    }
}
