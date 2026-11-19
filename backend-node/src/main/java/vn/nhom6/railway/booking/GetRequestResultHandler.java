package vn.nhom6.railway.booking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.persistence.JdbcRequestResultStore;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Xử lý yêu cầu tra cứu kết quả {@code GET_REQUEST_RESULT} theo SPEC-03 §4.4.
 *
 * <p>Quy tắc tuân thủ:
 * <ol>
 *   <li><b>Cổng đọc an toàn (ReadPolicy):</b> chỉ trả kết quả quyền uy khi ReadPolicy cho phép
 *       (Leader hiện tại + read barrier đã qua theo SPEC-04 §7). Không cho phép → fail closed,
 *       ném NOT_LEADER (kèm leaderHint) hoặc READ_UNAVAILABLE; tuyệt đối không đoán kết quả.</li>
 *   <li><b>Nguồn kết quả:</b>
 *       <ul>
 *         <li>Có hàng trong {@code request_results} → trả kết quả gốc: {@code APPLIED_SUCCESS}
 *             (kèm ticketId/trường theo SPEC) hoặc {@code APPLIED_REJECTED} (kèm reasonCode).</li>
 *         <li>Chưa có hàng nhưng {@link RaftApplyAdapter} đang giữ requestId ở {@code PENDING} →
 *             trả {@code PENDING} (chưa xác định), không phải REJECTED/UNKNOWN.</li>
 *         <li>Không biết gì về requestId → trả {@code UNKNOWN} theo SPEC-03 §4.4 (cả 4 trường phụ
 *             đều null), KHÔNG kết luận "thất bại" hay "ghế trống", KHÔNG lưu vào DB.</li>
 *       </ul>
 *   </li>
 *   <li><b>Follower/stale không tự trả AVAILABLE:</b> không suy diễn kết quả từ bảng {@code seats};
 *       chỉ căn cứ vào {@code request_results} hoặc trạng thái {@code PENDING}.</li>
 *   <li><b>Chỉ đọc:</b> không ghi DB, không append Raft, không đổi trạng thái adapter.
 *       Lỗi DB → ném {@link BookingRepository.StorageException}, fail closed.</li>
 * </ol>
 */
public final class GetRequestResultHandler {

    private static final Logger LOGGER = Logger.getLogger(GetRequestResultHandler.class.getName());

    /**
     * Cổng đọc an toàn đại diện cho cơ chế kiểm tra thẩm quyền Leader và read-barrier (SPEC-04 §7).
     * Được inject qua interface nhỏ để dễ dàng mock/test bằng FakeReadPolicy.
     */
    public interface ReadPolicy {
        /**
         * Kiểm tra điều kiện đọc an toàn (Leader hiện tại + read barrier đã hoàn tất).
         *
         * @throws BookingRequestHandler.NotLeaderException nếu node không phải là Leader
         * @throws BookingRequestHandler.ReadUnavailableException nếu không thể hoàn thành read barrier / timeout
         */
        void ensureAuthoritative()
                throws BookingRequestHandler.NotLeaderException, BookingRequestHandler.ReadUnavailableException;

        /**
         * Gợi ý Leader hiện tại (có thể null hoặc empty).
         */
        Optional<String> leaderHint();
    }

    /**
     * Nguồn tra cứu bản ghi kết quả bền vững từ {@code request_results}.
     */
    @FunctionalInterface
    public interface ResultStore {
        /**
         * Tra cứu bản ghi {@link JdbcRequestResultStore.StoredRequestResult} bền vững theo requestId.
         *
         * @throws BookingRepository.StorageException nếu truy vấn database thất bại
         */
        Optional<JdbcRequestResultStore.StoredRequestResult> find(String requestId);
    }

    /**
     * Nguồn tra cứu loại lệnh cho các request đang ở trạng thái PENDING.
     */
    @FunctionalInterface
    public interface PendingLookup {
        /**
         * Trả về loại lệnh ("BOOK_TICKET" / "CANCEL_TICKET") nếu request đang PENDING.
         */
        Optional<String> findPendingCommandType(String requestId);
    }

    /**
     * DTO kết quả payload theo đúng chuẩn SPEC-03 §4.4.
     */
    public record ResultPayload(
            String requestId,
            String state,
            String commandType,
            String ticketId,
            String reasonCode,
            Long appliedLogIndex) {

        public static final String STATE_APPLIED_SUCCESS = "APPLIED_SUCCESS";
        public static final String STATE_APPLIED_REJECTED = "APPLIED_REJECTED";
        public static final String STATE_PENDING = "PENDING";
        public static final String STATE_UNKNOWN = "UNKNOWN";

        public static ResultPayload appliedSuccess(
                String requestId, String commandType, String ticketId, long appliedLogIndex) {
            BookingCommand.requireValidRequestId(requestId);
            Objects.requireNonNull(commandType, "commandType");
            if (appliedLogIndex < 1) {
                throw new IllegalArgumentException("appliedLogIndex must be >= 1");
            }
            return new ResultPayload(requestId, STATE_APPLIED_SUCCESS, commandType, ticketId, null, appliedLogIndex);
        }

        public static ResultPayload appliedRejected(
                String requestId, String commandType, String reasonCode, long appliedLogIndex) {
            BookingCommand.requireValidRequestId(requestId);
            Objects.requireNonNull(commandType, "commandType");
            Objects.requireNonNull(reasonCode, "reasonCode");
            if (appliedLogIndex < 1) {
                throw new IllegalArgumentException("appliedLogIndex must be >= 1");
            }
            return new ResultPayload(requestId, STATE_APPLIED_REJECTED, commandType, null, reasonCode, appliedLogIndex);
        }

        public static ResultPayload pending(String requestId, String commandType) {
            BookingCommand.requireValidRequestId(requestId);
            return new ResultPayload(requestId, STATE_PENDING, commandType, null, null, null);
        }

        public static ResultPayload unknown(String requestId) {
            BookingCommand.requireValidRequestId(requestId);
            return new ResultPayload(requestId, STATE_UNKNOWN, null, null, null, null);
        }
    }

    /**
     * Sealed outcome đại diện cho các trường hợp kết quả hoặc lỗi phân loại.
     */
    public sealed interface LookupOutcome {
        record Found(ResultPayload payload) implements LookupOutcome {}
        record NotLeader(String leaderHint) implements LookupOutcome {}
        record ReadUnavailable(String message) implements LookupOutcome {}
        record Conflict(String requestId) implements LookupOutcome {}
        record StorageError(String message) implements LookupOutcome {}
    }

    private final ReadPolicy readPolicy;
    private final ResultStore resultStore;
    private final RaftApplyAdapter adapter;
    private final PendingLookup pendingLookup;

    public GetRequestResultHandler(
            ReadPolicy readPolicy,
            ResultStore resultStore,
            RaftApplyAdapter adapter,
            PendingLookup pendingLookup) {
        this.readPolicy = Objects.requireNonNull(readPolicy, "readPolicy");
        this.resultStore = Objects.requireNonNull(resultStore, "resultStore");
        this.adapter = adapter;
        this.pendingLookup = pendingLookup;
    }

    public GetRequestResultHandler(
            ReadPolicy readPolicy,
            ResultStore resultStore,
            RaftApplyAdapter adapter) {
        this(readPolicy, resultStore, adapter, null);
    }

    public GetRequestResultHandler(
            ReadPolicy readPolicy,
            ConnectionFactory connectionFactory,
            JdbcRequestResultStore jdbcStore,
            RaftApplyAdapter adapter) {
        this(readPolicy, fromJdbc(connectionFactory, jdbcStore), adapter, null);
    }

    public GetRequestResultHandler(
            ReadPolicy readPolicy,
            ConnectionFactory connectionFactory,
            JdbcRequestResultStore jdbcStore,
            RaftApplyAdapter adapter,
            PendingLookup pendingLookup) {
        this(readPolicy, fromJdbc(connectionFactory, jdbcStore), adapter, pendingLookup);
    }

    /**
     * Tạo {@link ResultStore} từ {@link ConnectionFactory} và {@link JdbcRequestResultStore}.
     * Mở connection chỉ đọc và đóng tự động qua try-with-resources.
     */
    public static ResultStore fromJdbc(ConnectionFactory connectionFactory, JdbcRequestResultStore jdbcStore) {
        Objects.requireNonNull(connectionFactory, "connectionFactory");
        Objects.requireNonNull(jdbcStore, "jdbcStore");
        return requestId -> {
            try (Connection connection = connectionFactory.getConnection()) {
                return jdbcStore.find(connection, requestId);
            } catch (SQLException exception) {
                throw new BookingRepository.StorageException(
                        "Failed to query request_results for requestId " + requestId, exception);
            }
        };
    }

    /**
     * Tra cứu kết quả quyền uy theo {@code requestId} (SPEC-03 §4.4).
     *
     * @param requestId UUID hợp lệ của yêu cầu
     * @return {@link ResultPayload} tương ứng (APPLIED_SUCCESS, APPLIED_REJECTED, PENDING, UNKNOWN)
     * @throws BookingRequestHandler.NotLeaderException nếu node không phải là Leader
     * @throws BookingRequestHandler.ReadUnavailableException nếu read barrier thất bại
     * @throws BookingRepository.StorageException nếu lỗi truy vấn cơ sở dữ liệu (fail-closed)
     */
    public ResultPayload lookup(String requestId)
            throws BookingRequestHandler.NotLeaderException,
                   BookingRequestHandler.ReadUnavailableException,
                   BookingRequestHandler.RequestIdConflictException {
        return lookup(requestId, null);
    }

    /**
     * Tra cứu kết quả quyền uy theo {@code requestId} kèm kiểm tra xung đột {@code expectedFingerprint}.
     *
     * @param requestId UUID hợp lệ của yêu cầu
     * @param expectedFingerprint fingerprint mong đợi (nếu có, dùng cho kiểm tra conflict)
     * @return {@link ResultPayload} tương ứng
     * @throws BookingRequestHandler.NotLeaderException nếu node không phải là Leader
     * @throws BookingRequestHandler.ReadUnavailableException nếu read barrier thất bại
     * @throws BookingRequestHandler.RequestIdConflictException nếu fingerprint đã lưu khác expectedFingerprint
     * @throws BookingRepository.StorageException nếu lỗi truy vấn cơ sở dữ liệu
     */
    public ResultPayload lookup(String requestId, String expectedFingerprint)
            throws BookingRequestHandler.NotLeaderException,
                   BookingRequestHandler.ReadUnavailableException,
                   BookingRequestHandler.RequestIdConflictException {
        BookingCommand.requireValidRequestId(requestId);

        // 1. Cổng đọc an toàn: ReadPolicy kiểm tra quyền uy trước (SPEC-04 §7)
        readPolicy.ensureAuthoritative();

        // 2. Tra cứu request_results bền vững (chỉ đọc)
        Optional<JdbcRequestResultStore.StoredRequestResult> storedOpt = resultStore.find(requestId);
        if (storedOpt.isPresent()) {
            JdbcRequestResultStore.StoredRequestResult stored = storedOpt.get();

            // Kiểm tra xung đột fingerprint nếu client cung cấp
            if (expectedFingerprint != null && !expectedFingerprint.isEmpty()
                    && !stored.intentFingerprint().equals(expectedFingerprint)) {
                throw new BookingRequestHandler.RequestIdConflictException();
            }

            if (ResultPayload.STATE_APPLIED_SUCCESS.equals(stored.state())) {
                return ResultPayload.appliedSuccess(
                        requestId, stored.commandType(), stored.ticketId(), stored.appliedLogIndex());
            } else if (ResultPayload.STATE_APPLIED_REJECTED.equals(stored.state())) {
                return ResultPayload.appliedRejected(
                        requestId, stored.commandType(), stored.reasonCode(), stored.appliedLogIndex());
            } else {
                throw new BookingRepository.StorageException(
                        "Corrupted request_results state for requestId " + requestId + ": " + stored.state());
            }
        }

        // 3. Nếu chưa có hàng trong DB, kiểm tra PENDING trong RaftApplyAdapter hoặc PendingLookup
        if (adapter != null && adapter.statusOf(requestId) == RaftApplyAdapter.RequestState.PENDING) {
            String cmdType = pendingLookup != null
                    ? pendingLookup.findPendingCommandType(requestId).orElse(null)
                    : null;
            return ResultPayload.pending(requestId, cmdType);
        }
        if (pendingLookup != null) {
            Optional<String> pendingType = pendingLookup.findPendingCommandType(requestId);
            if (pendingType.isPresent()) {
                return ResultPayload.pending(requestId, pendingType.get());
            }
        }

        // 4. Không biết gì về requestId -> trả UNKNOWN theo SPEC-03 §4.4
        // Cả 4 trường phụ đều null; không kết luận thất bại, không lưu vào DB, không suy từ seats
        return ResultPayload.unknown(requestId);
    }

    /**
     * Tra cứu và đóng gói kết quả thành {@link LookupOutcome} mà không ném ngoại lệ kiểm tra.
     */
    public LookupOutcome handle(String requestId) {
        return handle(requestId, null);
    }

    /**
     * Tra cứu và đóng gói kết quả thành {@link LookupOutcome} có kiểm tra fingerprint.
     */
    public LookupOutcome handle(String requestId, String expectedFingerprint) {
        try {
            ResultPayload payload = lookup(requestId, expectedFingerprint);
            return new LookupOutcome.Found(payload);
        } catch (BookingRequestHandler.NotLeaderException exception) {
            return new LookupOutcome.NotLeader(exception.leaderHint());
        } catch (BookingRequestHandler.ReadUnavailableException exception) {
            return new LookupOutcome.ReadUnavailable(exception.getMessage());
        } catch (BookingRequestHandler.RequestIdConflictException exception) {
            return new LookupOutcome.Conflict(requestId);
        } catch (BookingRepository.StorageException exception) {
            LOGGER.log(Level.SEVERE, "Storage failure during GET_REQUEST_RESULT for {0}", requestId);
            return new LookupOutcome.StorageError(exception.getMessage());
        }
    }

    /**
     * Xử lý envelope tin nhắn TCP ở tầng dispatcher.
     */
    public MessageEnvelope<?> handleEnvelope(MessageEnvelope<JsonNode> request, String senderId) {
        if (request == null) {
            return null;
        }
        JsonNode payload = request.getPayload();
        if (payload == null || !payload.has("requestId") || !payload.get("requestId").isTextual()) {
            return MessageCodec.createErrorResponse(
                    request.getMessageId(), "INVALID_REQUEST", "Missing or invalid requestId",
                    null, null, null, senderId);
        }
        String requestId = payload.get("requestId").asText();
        try {
            BookingCommand.requireValidRequestId(requestId);
        } catch (IllegalArgumentException exception) {
            return MessageCodec.createErrorResponse(
                    request.getMessageId(), "INVALID_REQUEST", "Invalid requestId UUID format",
                    null, null, requestId, senderId);
        }

        String expectedFingerprint = payload.has("intentFingerprint") && payload.get("intentFingerprint").isTextual()
                ? payload.get("intentFingerprint").asText() : null;

        LookupOutcome outcome = handle(requestId, expectedFingerprint);
        return switch (outcome) {
            case LookupOutcome.Found found -> {
                Map<String, Object> respMap = new LinkedHashMap<>();
                respMap.put("requestId", found.payload().requestId());
                respMap.put("state", found.payload().state());
                respMap.put("commandType", found.payload().commandType());
                respMap.put("ticketId", found.payload().ticketId());
                respMap.put("reasonCode", found.payload().reasonCode());
                respMap.put("appliedLogIndex", found.payload().appliedLogIndex());
                yield MessageEnvelope.createResponse(
                        request.getMessageId(), MessageType.GET_REQUEST_RESULT_RESPONSE, senderId, respMap);
            }
            case LookupOutcome.NotLeader notLeader -> MessageCodec.createErrorResponse(
                    request.getMessageId(), "NOT_LEADER", "This node is not the current leader",
                    notLeader.leaderHint(), null, requestId, senderId);
            case LookupOutcome.ReadUnavailable unavail -> MessageCodec.createErrorResponse(
                    request.getMessageId(), "READ_UNAVAILABLE", unavail.message(),
                    null, null, requestId, senderId);
            case LookupOutcome.Conflict conflict -> MessageCodec.createErrorResponse(
                    request.getMessageId(), "REQUEST_ID_CONFLICT",
                    "requestId was already used for a different booking intent",
                    null, null, requestId, senderId);
            case LookupOutcome.StorageError storage -> MessageCodec.createErrorResponse(
                    request.getMessageId(), "INTERNAL_ERROR",
                    "Booking storage operation failed",
                    null, null, requestId, senderId);
        };
    }
}
