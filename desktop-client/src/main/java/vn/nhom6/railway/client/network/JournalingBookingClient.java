package vn.nhom6.railway.client.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;
import vn.nhom6.railway.protocol.dto.GetSeatsRequestDto;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decorator bọc một {@link BookingClient} bất kỳ (Socket / LeaderAware / Mock)
 * để đảm bảo mọi yêu cầu ghi được lưu trữ bền vững vào {@link PersistentPendingJournal}
 * trước khi gửi qua mạng (TASK TV1-D14).
 * <p>
 * Bất biến & Quy tắc:
 * 1. BẮT BUỘC ghi journal thành công (flush + force) mới gửi lệnh đi. Nếu ghi journal thất bại -> fail-closed, không gửi!
 * 2. Khi gửi lại (retry): gửi lại CÙNG requestId và CÙNG payload nguyên văn (so khớp hash), messageId mới do transport cấp.
 * 3. Không tự nhận kết quả thành công: Chỉ phản hồi CONFIRMED hoặc REJECTED từ server (hoặc kết quả kiểm tra)
 *    mới được chuyển trạng thái sang RESOLVED.
 * 4. Quyền riêng tư: KHÔNG in nội dung payload ra log hệ thống.
 */
public class JournalingBookingClient implements BookingClient, Closeable {

    private static final Logger LOGGER = Logger.getLogger(JournalingBookingClient.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final BookingClient delegate;
    private final PersistentPendingJournal journal;

    public JournalingBookingClient(BookingClient delegate, PersistentPendingJournal journal) {
        this.delegate = Objects.requireNonNull(delegate, "delegate BookingClient không được null");
        this.journal = Objects.requireNonNull(journal, "journal không được null");
    }

    public BookingClient getDelegate() {
        return delegate;
    }

    public PersistentPendingJournal getJournal() {
        return journal;
    }

    @Override
    public CompletableFuture<GetSeatsResponseDto> getSeats(GetSeatsRequestDto request) {
        // GET_SEATS là thao tác đọc, không cần ghi journal
        return delegate.getSeats(request);
    }

    @Override
    public CompletableFuture<BookTicketResponseDto> book(BookTicketRequestDto request) {
        Objects.requireNonNull(request, "request không được null");

        String payload;
        try {
            payload = OBJECT_MAPPER.writeValueAsString(request);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Không thể serialize BookTicketRequestDto: " + e.getMessage(), e));
        }

        String hash = PendingEntry.computePayloadHash(payload);
        PendingEntry entry = new PendingEntry(
                request.getRequestId(),
                "BOOK_TICKET",
                payload,
                hash,
                PendingState.PREPARED,
                Instant.now(),
                null,
                0,
                null,
                null,
                1
        );

        // BƯỚC 1: Ghi bền vững trước khi gửi (FAIL-CLOSED)
        try {
            journal.recordPrepared(entry);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[JOURNAL-CLIENT] Ghi journal PREPARED thất bại cho requestId=" + request.getRequestId() + ". Hủy gửi yêu cầu!", e);
            return CompletableFuture.failedFuture(new IOException("Không thể ghi journal bền vững trước khi gửi: " + e.getMessage(), e));
        }

        // BƯỚC 2: Đánh dấu đã gửi (SENT_UNRESOLVED)
        try {
            journal.markSent(request.getRequestId());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Cảnh báo: không thể đánh dấu SENT_UNRESOLVED cho requestId=" + request.getRequestId(), e);
        }

        // BƯỚC 3: Gửi qua delegate client
        return delegate.book(request).whenComplete((response, throwable) -> {
            if (throwable != null) {
                // Lỗi mạng, timeout, hoặc ngắt kết nối: Giữ nguyên SENT_UNRESOLVED trong journal
                LOGGER.info(String.format("[JOURNAL-CLIENT] Gửi thất bại/timeout requestId=%s, giữ SENT_UNRESOLVED", request.getRequestId()));
                return;
            }

            if (response != null) {
                String outcome = response.getOutcome();
                if ("CONFIRMED".equals(outcome) || "REJECTED".equals(outcome)) {
                    try {
                        journal.markResolved(request.getRequestId(), outcome);
                        LOGGER.info(String.format("[JOURNAL-CLIENT] Đã nhận kết quả cuối requestId=%s, outcome=%s -> markResolved",
                                request.getRequestId(), outcome));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Không thể cập nhật trạng thái RESOLVED vào journal: " + e.getMessage(), e);
                    }
                } else {
                    // outcome == UNKNOWN (hoặc NOT_LEADER, NO_QUORUM): giữ nguyên SENT_UNRESOLVED
                    LOGGER.info(String.format("[JOURNAL-CLIENT] Phản hồi outcome=%s cho requestId=%s, giữ nguyên SENT_UNRESOLVED",
                            outcome, request.getRequestId()));
                }
            }
        });
    }

    /**
     * Gửi lại (retry) yêu cầu với CÙNG requestId và CÙNG payload nguyên văn từ journal.
     */
    public CompletableFuture<BookTicketResponseDto> retry(String requestId) {
        Objects.requireNonNull(requestId, "requestId không được null");

        Optional<PendingEntry> optEntry = journal.get(requestId);
        if (optEntry.isEmpty()) {
            return CompletableFuture.failedFuture(new NoSuchElementException("Không tìm thấy pending entry cho requestId: " + requestId));
        }

        PendingEntry entry = optEntry.get();
        if (entry.getState() == PendingState.CORRUPT || !entry.isPayloadValid()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Payload bị lỗi hoặc sai lệch SHA-256 hash, từ chối retry"));
        }

        if (entry.getState() == PendingState.RESOLVED) {
            return CompletableFuture.failedFuture(new IllegalStateException("Yêu cầu đã được xác nhận kết quả cuối (" +
                    entry.getFinalOutcome() + "), không cần gửi lại!"));
        }

        BookTicketRequestDto request;
        try {
            request = OBJECT_MAPPER.readValue(entry.getPayload(), BookTicketRequestDto.class);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new IllegalStateException("Không thể deserialize payload đã lưu trong journal: " + e.getMessage(), e));
        }

        // Đánh dấu lần thử gửi mới trong journal
        try {
            journal.markSent(requestId);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Không thể cập nhật số lần thử vào journal cho requestId=" + requestId, e);
        }

        LOGGER.info(String.format("[JOURNAL-CLIENT] Bắt đầu retry requestId=%s, lần thử thứ %d", requestId, entry.getAttemptCount() + 1));

        return delegate.book(request).whenComplete((response, throwable) -> {
            if (response != null) {
                String outcome = response.getOutcome();
                if ("CONFIRMED".equals(outcome) || "REJECTED".equals(outcome)) {
                    try {
                        journal.markResolved(requestId, outcome);
                        LOGGER.info(String.format("[JOURNAL-CLIENT] Retry thành công có kết quả cuối requestId=%s, outcome=%s", requestId, outcome));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Lỗi cập nhật RESOLVED sau retry: " + e.getMessage(), e);
                    }
                }
            }
        });
    }

    /**
     * Tra cứu kết quả yêu cầu theo SPEC-03 §4.4 (GET_REQUEST_RESULT).
     * Chỉ cập nhật RESOLVED nếu Node trả về kết quả cuối (APPLIED_SUCCESS hoặc APPLIED_REJECTED).
     */
    @Override
    public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
        Objects.requireNonNull(requestId, "requestId không được null");

        return delegate.getRequestResult(requestId).whenComplete((response, throwable) -> {
            if (throwable != null) {
                LOGGER.info(String.format("[JOURNAL-CLIENT] getRequestResult requestId=%s gặp lỗi/timeout, giữ pending", requestId));
                return;
            }

            if (response != null) {
                String state = response.getState();
                if ("APPLIED_SUCCESS".equals(state)) {
                    try {
                        journal.markResolved(requestId, "CONFIRMED");
                        LOGGER.info(String.format("[JOURNAL-CLIENT] Tra cứu thành công requestId=%s -> APPLIED_SUCCESS (CONFIRMED)", requestId));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Không thể markResolved: " + e.getMessage(), e);
                    }
                } else if ("APPLIED_REJECTED".equals(state)) {
                    try {
                        journal.markResolved(requestId, "REJECTED");
                        LOGGER.info(String.format("[JOURNAL-CLIENT] Tra cứu hoàn tất requestId=%s -> APPLIED_REJECTED (REJECTED)", requestId));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-CLIENT] Không thể markResolved: " + e.getMessage(), e);
                    }
                } else {
                    // PENDING hoặc UNKNOWN: giữ nguyên trạng thái pending trong journal
                    LOGGER.info(String.format("[JOURNAL-CLIENT] Tra cứu requestId=%s nhận state=%s, giữ nguyên pending", requestId, state));
                }
            }
        });
    }

    @Override
    public CompletableFuture<CancelTicketResponseDto> cancel(CancelTicketRequestDto request) {
        return delegate.cancel(request);
    }

    @Override
    public void close() throws IOException {
        if (delegate instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception e) {
                if (e instanceof IOException ioe) throw ioe;
                throw new IOException(e);
            }
        }
        journal.close();
    }
}
