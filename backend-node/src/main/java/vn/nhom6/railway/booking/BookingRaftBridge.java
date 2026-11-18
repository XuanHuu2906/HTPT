package vn.nhom6.railway.booking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SPEC-04 §4 (propose/append, chỉ Leader), §5, §7 (read barrier), §9;
 * SPEC-03 §4.4 (kết quả, TIMEOUT_UNKNOWN, NOT_LEADER + leaderHint), §6;
 * SPEC-05 (idempotency, request_results).
 *
 * <p>Cầu nối giữa API Raft ({@link RaftNode} / {@link RaftPort}) và tầng xử lý Booking ({@link RaftApplyAdapter}).
 *
 * <p>Luồng xử lý một yêu cầu ghi (BookingCommand BOOK/CANCEL):
 * <ol>
 *   <li><b>Tra cứu idempotency trước:</b> nếu đã có requestId trong DB hoặc bộ nhớ → trả kết quả gốc (cùng fingerprint)
 *       hoặc ném {@link BookingRequestHandler.RequestIdConflictException} (khác fingerprint); không append lại.</li>
 *   <li><b>Tái sử dụng PENDING:</b> nếu requestId đang PENDING (cùng requestId + cùng fingerprint) → dùng lại future đang chờ,
 *       không append lần hai.</li>
 *   <li><b>Kiểm Leader:</b> nếu node không phải Leader → trả {@link BookingRequestHandler.NotLeaderException}
 *       kèm leaderHint (có thể null), KHÔNG append.</li>
 *   <li><b>Đăng ký PENDING trước khi append:</b> gọi {@link RaftApplyAdapter#registerPending(String)} để tránh race commit
 *       nhanh hơn đăng ký.</li>
 *   <li><b>Append:</b> gọi propose/append của RaftCore. Nếu lỗi NOT_LEADER lúc append → hủy pending và ném NOT_LEADER.
 *       Nếu thành công (Accepted) → lưu mapping {@code (index, term)} để kiểm soát log matching/cắt log.</li>
 *   <li><b>Chờ commit + apply:</b> chờ future của adapter với deadline hữu hạn (cấu hình được). Future chỉ hoàn tất
 *       sau khi DB apply transaction đã commit thành công.</li>
 *   <li><b>UNKNOWN khi commit chưa xác minh:</b> hết hạn chờ, step-down, mất quyền Leader, hoặc adapter hủy future →
 *       trả {@link ApplyResult.Outcome#UNKNOWN} với {@code reasonCode = TIMEOUT_UNKNOWN}, KHÔNG CONFIRMED, KHÔNG REJECTED,
 *       KHÔNG lưu UNKNOWN vào DB. Khi apply muộn, tra cứu theo requestId phải trả kết quả gốc.</li>
 *   <li><b>Ghi nhận term:</b> nếu entry tại index bị ghi đè bởi term khác → UNKNOWN cho yêu cầu này, không coi là REJECTED.</li>
 * </ol>
 */
public final class BookingRaftBridge implements BookingRequestHandler.CommandSubmitter, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(BookingRaftBridge.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Port trừu tượng đại diện cho RaftNode core, cho phép inject Raft thật hoặc FakeRaft trong kiểm thử.
     */
    public interface RaftPort {
        Role role();
        Optional<String> leaderId();
        long currentTerm();
        RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload);
    }

    /**
     * Tra cứu fingerprint của requestId đã lưu trước đó (phục vụ idempotency across restart / DB).
     */
    @FunctionalInterface
    public interface FingerprintLookup {
        Optional<String> findIntentFingerprint(String requestId);
    }

    private record EntryInfo(long index, long term, String requestId) {}
    private record InFlight(String fingerprint, CompletableFuture<ApplyResult> future, long index, long term) {}

    private final RaftPort raftPort;
    private final RaftApplyAdapter adapter;
    private final BookingRepository repository;
    private final FingerprintLookup fingerprintLookup;
    private final Duration timeout;

    private final ScheduledExecutorService timeoutScheduler;
    private final boolean ownsScheduler;

    private final ConcurrentMap<String, String> requestFingerprints = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InFlight> inFlightRequests = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, EntryInfo> appendedEntries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ApplyResult> appliedResults = new ConcurrentHashMap<>();

    public BookingRaftBridge(
            RaftPort raftPort,
            RaftApplyAdapter adapter,
            BookingRepository repository,
            FingerprintLookup fingerprintLookup,
            Duration timeout) {
        this.raftPort = Objects.requireNonNull(raftPort, "raftPort");
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.repository = repository;
        this.fingerprintLookup = fingerprintLookup;
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive and non-null");
        }
        this.timeout = timeout;
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "booking-raft-bridge-timeout");
            t.setDaemon(true);
            return t;
        });
        this.ownsScheduler = true;
    }

    public BookingRaftBridge(
            RaftPort raftPort,
            RaftApplyAdapter adapter,
            BookingRepository repository,
            Duration timeout) {
        this(raftPort, adapter, repository, null, timeout);
    }

    public BookingRaftBridge(
            RaftPort raftPort,
            RaftApplyAdapter adapter,
            BookingRepository repository) {
        this(raftPort, adapter, repository, null, DEFAULT_TIMEOUT);
    }

    public BookingRaftBridge(
            RaftNode raftNode,
            RaftApplyAdapter adapter,
            BookingRepository repository,
            Duration timeout) {
        this(fromRaftNode(raftNode), adapter, repository, null, timeout);
    }

    public BookingRaftBridge(
            RaftNode raftNode,
            RaftApplyAdapter adapter,
            BookingRepository repository) {
        this(fromRaftNode(raftNode), adapter, repository, null, DEFAULT_TIMEOUT);
    }

    /**
     * Tạo RaftPort bọc quanh {@link RaftNode} thật của TV4/TV5.
     */
    public static RaftPort fromRaftNode(RaftNode node) {
        Objects.requireNonNull(node, "node");
        return new RaftPort() {
            @Override
            public Role role() {
                return node.role();
            }

            @Override
            public Optional<String> leaderId() {
                return node.leaderId();
            }

            @Override
            public long currentTerm() {
                return node.currentTerm();
            }

            @Override
            public RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
                return node.appendCommand(commandType, commandPayload);
            }
        };
    }

    @Override
    public CompletionStage<ApplyResult> submit(BookingCommand.BookTicket command) throws Exception {
        return submitCommand(command);
    }

    /**
     * Gửi bất kỳ BookingCommand nào (BOOK, CANCEL) qua Raft bridge.
     */
    public CompletableFuture<ApplyResult> submitCommand(BookingCommand command) throws Exception {
        Objects.requireNonNull(command, "command");
        String requestId = extractRequestId(command);
        if (requestId == null) {
            throw new IllegalArgumentException("Command must have a non-null requestId");
        }
        String fingerprint = computeFingerprint(command);

        // 1. Tra cứu idempotency trước: nếu request_results đã có requestId
        Optional<ApplyResult> existingResult = findExistingResult(requestId);
        if (existingResult.isPresent()) {
            Optional<String> storedFingerprint = findStoredFingerprint(requestId);
            if (storedFingerprint.isPresent() && !storedFingerprint.get().equals(fingerprint)) {
                // Cùng requestId nhưng khác fingerprint -> REQUEST_ID_CONFLICT, không append
                throw new BookingRequestHandler.RequestIdConflictException();
            }
            // Cùng fingerprint -> trả kết quả gốc đã lưu
            return CompletableFuture.completedFuture(existingResult.get());
        }

        // 2. Tái sử dụng PENDING nếu cùng requestId gửi lại khi đang chờ commit
        InFlight inFlight = inFlightRequests.get(requestId);
        if (inFlight != null) {
            if (!inFlight.fingerprint().equals(fingerprint)) {
                throw new BookingRequestHandler.RequestIdConflictException();
            }
            // Cùng ý định -> dùng lại future đang chờ, KHÔNG append lần hai
            return inFlight.future();
        }

        // 3. Kiểm Leader: nếu node không phải Leader -> trả NOT_LEADER kèm leaderHint (có thể null), KHÔNG append
        if (raftPort.role() != Role.LEADER) {
            String leaderHint = raftPort.leaderId().orElse(null);
            throw new BookingRequestHandler.NotLeaderException(leaderHint);
        }

        // Ghi nhận fingerprint vào bộ nhớ
        requestFingerprints.put(requestId, fingerprint);

        // 4. Đăng ký PENDING trước khi append (tránh race commit nhanh hơn đăng ký)
        CompletableFuture<ApplyResult> adapterFuture = adapter.registerPending(requestId);
        CompletableFuture<ApplyResult> clientFuture = new CompletableFuture<>();

        // 5. Append: gọi propose/append của RaftCore
        String type = commandType(command);
        String payload = serializePayload(command);
        RaftNode.AppendOutcome outcome = raftPort.appendCommand(type, payload);

        if (outcome instanceof RaftNode.NotLeader notLeader) {
            // Lỗi NOT_LEADER lúc append -> hủy pending, ném NOT_LEADER, chưa có entry nên không phải UNKNOWN
            adapter.cancelPending(requestId, new BookingRequestHandler.NotLeaderException(notLeader.leaderHint()));
            inFlightRequests.remove(requestId);
            requestFingerprints.remove(requestId);
            throw new BookingRequestHandler.NotLeaderException(notLeader.leaderHint());
        } else if (outcome instanceof RaftNode.StorageFailure failure) {
            // Lỗi lưu trữ Raft
            BookingRepository.StorageException storageException =
                    new BookingRepository.StorageException("Raft storage failure during append", failure.cause());
            adapter.cancelPending(requestId, storageException);
            inFlightRequests.remove(requestId);
            requestFingerprints.remove(requestId);
            throw storageException;
        } else if (outcome instanceof RaftNode.Accepted accepted) {
            long index = accepted.index();
            long term = accepted.term();
            inFlightRequests.put(requestId, new InFlight(fingerprint, clientFuture, index, term));
            appendedEntries.put(index, new EntryInfo(index, term, requestId));

            // Nối adapterFuture vào clientFuture kèm timeout hữu hạn
            attachTimeoutAndCompletion(command, requestId, index, term, adapterFuture, clientFuture);
            return clientFuture;
        } else {
            throw new IllegalStateException("Unknown append outcome: " + outcome);
        }
    }

    /**
     * Phiên bản tiện ích chuyển ngoại lệ đồng bộ thành CompletableFuture.failedFuture.
     */
    public CompletableFuture<ApplyResult> submitAsFuture(BookingCommand command) {
        try {
            return submitCommand(command);
        } catch (Exception exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    /**
     * Báo cho bridge biết node bị step down khỏi quyền Leader.
     * Hoàn tất các yêu cầu in-flight chưa commit bằng TIMEOUT_UNKNOWN.
     */
    public synchronized void onStepDown() {
        for (Map.Entry<String, InFlight> entry : inFlightRequests.entrySet()) {
            String requestId = entry.getKey();
            InFlight inFlight = entry.getValue();
            ApplyResult unknownResult = ApplyResult.bookUnknown(requestId);
            inFlight.future().complete(unknownResult);
            adapter.cancelPending(requestId, new IllegalStateException("Leader stepped down"));
        }
        inFlightRequests.clear();
    }

    /**
     * Báo cho bridge biết entry tại index bị ghi đè bởi term khác (cắt log Raft).
     * Yêu cầu tại index này trở thành UNKNOWN, không phải REJECTED.
     */
    public synchronized void onEntryOverwritten(long index, long newTerm) {
        EntryInfo info = appendedEntries.get(index);
        if (info != null) {
            appendedEntries.remove(index);
            InFlight inFlight = inFlightRequests.remove(info.requestId());
            if (inFlight != null) {
                ApplyResult unknownResult = ApplyResult.bookUnknown(info.requestId());
                inFlight.future().complete(unknownResult);
                adapter.cancelPending(info.requestId(),
                        new IllegalStateException("Log entry overwritten at index " + index + " with term " + newTerm));
            }
        }
    }

    private void attachTimeoutAndCompletion(
            BookingCommand command,
            String requestId,
            long index,
            long term,
            CompletableFuture<ApplyResult> adapterFuture,
            CompletableFuture<ApplyResult> clientFuture) {

        ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
            if (!clientFuture.isDone()) {
                LOGGER.log(Level.WARNING, "Commit/apply timed out for requestId {0} at index {1}",
                        new Object[]{requestId, index});
                ApplyResult unknownResult = unknownResultFor(command, requestId);
                clientFuture.complete(unknownResult);
                adapter.cancelPending(requestId, new TimeoutException("Commit timeout"));
                inFlightRequests.remove(requestId);
            }
        }, timeout.toNanos(), TimeUnit.NANOSECONDS);

        adapterFuture.whenComplete((result, throwable) -> {
            timeoutTask.cancel(false);
            inFlightRequests.remove(requestId);
            if (throwable != null) {
                Throwable cause = throwable;
                if (cause instanceof BookingRequestHandler.RequestIdConflictException conflict) {
                    clientFuture.completeExceptionally(conflict);
                } else if (cause.getCause() instanceof BookingRequestHandler.RequestIdConflictException conflict) {
                    clientFuture.completeExceptionally(conflict);
                } else {
                    // Mọi lỗi huỷ / step-down / timeout -> trả UNKNOWN
                    clientFuture.complete(unknownResultFor(command, requestId));
                }
            } else {
                EntryInfo info = appendedEntries.get(index);
                if (info != null && info.term() != term) {
                    // Entry bị ghi đè bởi term khác
                    clientFuture.complete(unknownResultFor(command, requestId));
                } else {
                    if (result != null && result.isPersistable()) {
                        appliedResults.put(requestId, result);
                    }
                    clientFuture.complete(result);
                }
            }
        });
    }

    /**
     * Tra cứu kết quả đã lưu theo requestId nếu đã APPLIED.
     */
    public Optional<ApplyResult> findResult(String requestId) {
        return findExistingResult(requestId);
    }

    private Optional<ApplyResult> findExistingResult(String requestId) {
        if (repository != null) {
            try {
                Optional<ApplyResult> res = repository.findResultByRequestId(requestId);
                if (res.isPresent()) {
                    return res;
                }
            } catch (BookingRepository.StorageException exception) {
                LOGGER.log(Level.WARNING, "Failed to query repository for requestId " + requestId, exception);
            }
        }
        ApplyResult mem = appliedResults.get(requestId);
        if (mem != null) {
            return Optional.of(mem);
        }
        return adapter.findResult(requestId);
    }

    private Optional<String> findStoredFingerprint(String requestId) {
        String memFp = requestFingerprints.get(requestId);
        if (memFp != null) {
            return Optional.of(memFp);
        }
        if (fingerprintLookup != null) {
            try {
                return fingerprintLookup.findIntentFingerprint(requestId);
            } catch (RuntimeException exception) {
                LOGGER.log(Level.WARNING, "FingerprintLookup failed for requestId " + requestId, exception);
            }
        }
        return Optional.empty();
    }

    private static String extractRequestId(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return book.requestId();
        } else if (command instanceof BookingCommand.CancelTicket cancel) {
            return cancel.requestId();
        }
        return null;
    }

    private static ApplyResult unknownResultFor(BookingCommand command, String requestId) {
        if (command instanceof BookingCommand.CancelTicket cancel) {
            return ApplyResult.cancelUnknown(requestId, cancel.ticketId());
        }
        return ApplyResult.bookUnknown(requestId);
    }

    public static String commandType(BookingCommand command) {
        return switch (command.type()) {
            case BOOK_TICKET -> RaftLogEntry.COMMAND_TYPE_BOOK_TICKET;
            case CANCEL_TICKET -> RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET;
            case NO_OP -> RaftLogEntry.COMMAND_TYPE_NO_OP;
        };
    }

    public static String computeFingerprint(BookingCommand command) {
        if (command instanceof BookingCommand.BookTicket book) {
            return BookingStateMachine.computeBookFingerprint(
                    book.tripId(), book.seatId(), book.passengerName());
        } else if (command instanceof BookingCommand.CancelTicket cancel) {
            return BookingStateMachine.computeCancelFingerprint(cancel.ticketId());
        } else if (command instanceof BookingCommand.NoOp) {
            return "";
        }
        throw new IllegalArgumentException("Unsupported command type: " + command.getClass());
    }

    public static String serializePayload(BookingCommand command) {
        try {
            if (command instanceof BookingCommand.BookTicket book) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("requestId", book.requestId());
                node.put("tripId", book.tripId());
                node.put("seatId", book.seatId());
                node.put("passengerName", book.passengerName());
                node.put("ticketId", book.ticketId());
                node.put("issuedAt", book.issuedAt().toString());
                return MAPPER.writeValueAsString(node);
            } else if (command instanceof BookingCommand.CancelTicket cancel) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("requestId", cancel.requestId());
                node.put("ticketId", cancel.ticketId());
                node.put("cancelledAt", cancel.cancelledAt().toString());
                return MAPPER.writeValueAsString(node);
            } else if (command instanceof BookingCommand.NoOp) {
                return "{}";
            }
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize booking command", exception);
        }
        throw new IllegalArgumentException("Unsupported command type: " + command.getClass());
    }

    @Override
    public void close() {
        if (ownsScheduler) {
            timeoutScheduler.shutdownNow();
        }
    }
}
