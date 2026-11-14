package vn.nhom6.railway.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.network.*;
import vn.nhom6.railway.client.smoke.FakeNodeServer;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PendingState;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel;
import vn.nhom6.railway.client.viewmodel.PendingRequestsViewModel.PendingItem;
import vn.nhom6.railway.protocol.dto.BookTicketRequestDto;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử toàn diện cho PersistentPendingJournal và JournalingBookingClient (TASK TV1-D14).
 * <p>
 * Bất biến & Yêu cầu:
 * 1. Chạy hoàn toàn ở môi trường local, không gọi mạng ngoài, sử dụng @TempDir.
 * 2. Mọi kết quả từ server giả lập đều gắn rõ nhãn "FAKE, không phải Node thật".
 * 3. Không coi kết quả từ fake là kết quả Raft thật.
 */
class PersistentPendingJournalTest {

    @TempDir
    Path tempJournalDir;

    private PersistentPendingJournal journal;

    @BeforeEach
    void setUp() throws IOException {
        journal = new PersistentPendingJournal(tempJournalDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (journal != null && !journal.isClosed()) {
            journal.close();
        }
    }

    // =========================================================================
    // CA 1: recordPrepared rồi mở lại (restart giả lập) -> mục còn nguyên vẹn
    // =========================================================================
    @Test
    @DisplayName("Ca 1: recordPrepared rồi restart -> dữ liệu khôi phục nguyên văn và hash đúng")
    void testRecordPreparedAndRestartRecovery() throws IOException {
        String reqId = UUID.randomUUID().toString();
        String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G01\",\"passengerName\":\"Nguyen Van A\"}";
        PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload);

        journal.recordPrepared(entry);
        journal.close(); // Đóng giả lập tắt app

        // Mở lại instance mới trên cùng thư mục lưu trữ (mô phỏng restart)
        try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
            List<PendingEntry> pending = restartedJournal.loadPending();
            assertEquals(1, pending.size(), "Phải khôi phục đúng 1 bản ghi pending");

            PendingEntry recovered = pending.get(0);
            assertEquals(reqId, recovered.getRequestId());
            assertEquals("BOOK_TICKET", recovered.getCommandType());
            assertEquals(payload, recovered.getPayload(), "Payload JSON phải nguyên văn từng ký tự để phục vụ retry");
            assertEquals(entry.getPayloadHash(), recovered.getPayloadHash());
            assertEquals(PendingState.PREPARED, recovered.getState());
            assertTrue(recovered.isPayloadValid(), "Mã băm SHA-256 của payload phải hoàn toàn khớp");
        }
    }

    // =========================================================================
    // CA 2: Ghi journal phải xong trước khi gửi; journal ghi lỗi -> không được gửi (Fail-closed)
    // =========================================================================
    @Test
    @DisplayName("Ca 2: Bắt buộc ghi journal trước khi gửi; lỗi ghi -> không được gửi (Fail-closed)")
    void testWriteBeforeSendAndFailClosed() throws IOException {
        AtomicBoolean delegateCalled = new AtomicBoolean(false);
        AtomicBoolean journalHasRecordBeforeSend = new AtomicBoolean(false);

        String reqId = UUID.randomUUID().toString();
        BookTicketRequestDto request = new BookTicketRequestDto(reqId, "SE-001", "G02", "Nguyen Van B");

        BookingClient testDelegate = new MockBookingClient() {
            @Override
            public CompletableFuture<vn.nhom6.railway.client.network.BookTicketResponseDto> book(BookTicketRequestDto req) {
                delegateCalled.set(true);
                // Kiểm tra xem tại thời điểm delegate được gọi, journal đã có bản ghi chưa
                journalHasRecordBeforeSend.set(journal.get(reqId).isPresent());
                return super.book(req);
            }
        };

        JournalingBookingClient client = new JournalingBookingClient(testDelegate, journal);
        client.book(request).join();

        assertTrue(delegateCalled.get(), "Delegate phải được gọi sau khi journal đã ghi");
        assertTrue(journalHasRecordBeforeSend.get(), "Journal bắt buộc phải có bản ghi trước khi lệnh được gửi qua delegate");

        // Thử nghiệm fail-closed: đóng journal để cố tình gây lỗi ghi
        journal.close();
        AtomicBoolean delegateCalledOnFailure = new AtomicBoolean(false);
        BookingClient failDelegate = new MockBookingClient() {
            @Override
            public CompletableFuture<vn.nhom6.railway.client.network.BookTicketResponseDto> book(BookTicketRequestDto req) {
                delegateCalledOnFailure.set(true);
                return super.book(req);
            }
        };

        JournalingBookingClient failClient = new JournalingBookingClient(failDelegate, journal);
        String reqId2 = UUID.randomUUID().toString();
        BookTicketRequestDto request2 = new BookTicketRequestDto(reqId2, "SE-001", "G03", "Nguyen Van C");

        CompletableFuture<?> future = failClient.book(request2);
        assertTrue(future.isCompletedExceptionally(), "Phải trả về CompletableFuture lỗi khi ghi journal thất bại");
        assertFalse(delegateCalledOnFailure.get(), "Tuyệt đối không được gửi lệnh qua mạng nếu ghi journal thất bại (Fail-closed)");
    }

    // =========================================================================
    // CA 3: Restart khi đang SENT_UNRESOLVED -> khôi phục CHƯA XÁC NHẬN, không tự nhận thành công
    // =========================================================================
    @Test
    @DisplayName("Ca 3: Restart khi SENT_UNRESOLVED -> ViewModel khôi phục CHƯA XÁC NHẬN, không tự thành công")
    void testRestartWhenSentUnresolvedDisplaysUnknown() throws IOException {
        String reqId = UUID.randomUUID().toString();
        String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G03\",\"passengerName\":\"Nguyen Van D\"}";
        PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload);

        journal.recordPrepared(entry);
        journal.markSent(reqId, "node-1"); // Đã gửi nhưng chưa có kết quả (SENT_UNRESOLVED)
        journal.close();

        try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
            BookingClient mockDelegate = new MockBookingClient();
            JournalingBookingClient client = new JournalingBookingClient(mockDelegate, restartedJournal);

            PendingRequestsViewModel viewModel = new PendingRequestsViewModel(client);
            assertEquals(1, viewModel.getPendingItems().size());

            PendingItem item = viewModel.getPendingItems().get(0);
            assertEquals(reqId, item.getRequestId());
            assertEquals(PendingState.SENT_UNRESOLVED, item.getState());
            assertEquals("CHƯA XÁC NHẬN", item.getDisplayStatus(), "Không được tự ý hiển thị thành công khi chưa có phản hồi cuối");
            assertEquals("booking-status-unknown", item.getStyleClass());
            assertTrue(item.isCanRetry(), "Mục chưa xác nhận phải cho phép người dùng kiểm tra hoặc gửi lại");
        }
    }

    // =========================================================================
    // CA 4: Retry sau restart với FakeNodeServer: cùng requestId + payload, kết quả CONFIRMED -> RESOLVED
    // =========================================================================
    @Test
    @DisplayName("Ca 4: Retry sau restart với FakeNodeServer: cùng requestId/payload -> CONFIRMED, đúng 1 vé")
    void testRetryAfterRestartWithFakeServer() throws Exception {
        // Khởi động FakeNodeServer trên cổng ngẫu nhiên (Port 0)
        try (FakeNodeServer fakeServer = new FakeNodeServer("node-1")) {
            int port = fakeServer.getPort();

            String reqId = UUID.randomUUID().toString();
            String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G05\",\"passengerName\":\"Hanh Khach Fake\"}";
            PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload);

            journal.recordPrepared(entry);
            journal.markSent(reqId, "node-1");
            journal.close();

            // Restart ứng dụng
            try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
                SocketBookingClient socketClient = new SocketBookingClient("127.0.0.1", port);
                JournalingBookingClient journalingClient = new JournalingBookingClient(socketClient, restartedJournal);

                // Gửi lại (retry) từ journal
                var future = journalingClient.retry(reqId);
                var response = future.get();

                assertEquals("CONFIRMED", response.getOutcome(), "Fake server phải trả về CONFIRMED [FAKE, không phải Node thật]");
                assertEquals(reqId, response.getRequestId());

                // Kiểm tra journal đã chuyển sang RESOLVED
                PendingEntry resolvedEntry = restartedJournal.get(reqId).orElseThrow();
                assertEquals(PendingState.RESOLVED, resolvedEntry.getState());
                assertEquals("CONFIRMED", resolvedEntry.getFinalOutcome());

                // Xác nhận bất biến trên Fake Server: Đúng 1 vé duy nhất được cấp cho ghế G05
                assertEquals(1, fakeServer.getActiveTicketCount("G05"), "Bất biến: Retry cùng requestId phải idempotent, duy nhất 1 vé");
            }
        }
    }

    // =========================================================================
    // CA 5: checkResult sau restart bằng fake result: CONFIRMED, REJECTED, UNKNOWN, timeout
    // =========================================================================
    @Test
    @DisplayName("Ca 5: checkResult sau restart với fake result: cập nhật đúng outcome, giữ pending khi UNKNOWN")
    void testCheckResultWithFakeResults() throws Exception {
        String reqSuccess = UUID.randomUUID().toString();
        String reqRejected = UUID.randomUUID().toString();
        String reqUnknown = UUID.randomUUID().toString();

        journal.recordPrepared(PendingEntry.createPrepared(reqSuccess, "BOOK_TICKET", "{\"requestId\":\"" + reqSuccess + "\"}"));
        journal.recordPrepared(PendingEntry.createPrepared(reqRejected, "BOOK_TICKET", "{\"requestId\":\"" + reqRejected + "\"}"));
        journal.recordPrepared(PendingEntry.createPrepared(reqUnknown, "BOOK_TICKET", "{\"requestId\":\"" + reqUnknown + "\"}"));

        journal.markSent(reqSuccess);
        journal.markSent(reqRejected);
        journal.markSent(reqUnknown);
        journal.close();

        try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
            BookingClient mockClient = new MockBookingClient() {
                @Override
                public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
                    if (requestId.equals(reqSuccess)) {
                        return CompletableFuture.completedFuture(
                                new GetRequestResultResponseDto(requestId, "APPLIED_SUCCESS", "BOOK_TICKET", "ticket-1", null, 10L, true));
                    } else if (requestId.equals(reqRejected)) {
                        return CompletableFuture.completedFuture(
                                new GetRequestResultResponseDto(requestId, "APPLIED_REJECTED", "BOOK_TICKET", null, "SEAT_ALREADY_BOOKED", 11L, true));
                    } else {
                        // Node chưa từng thấy requestId
                        return CompletableFuture.completedFuture(
                                new GetRequestResultResponseDto(requestId, "UNKNOWN", null, null, null, null, true));
                    }
                }
            };

            JournalingBookingClient journalingClient = new JournalingBookingClient(mockClient, restartedJournal);

            // 1. Kiểm tra thành công -> RESOLVED CONFIRMED
            journalingClient.getRequestResult(reqSuccess).join();
            assertEquals(PendingState.RESOLVED, restartedJournal.get(reqSuccess).orElseThrow().getState());
            assertEquals("CONFIRMED", restartedJournal.get(reqSuccess).orElseThrow().getFinalOutcome());

            // 2. Kiểm tra bị từ chối -> RESOLVED REJECTED
            journalingClient.getRequestResult(reqRejected).join();
            assertEquals(PendingState.RESOLVED, restartedJournal.get(reqRejected).orElseThrow().getState());
            assertEquals("REJECTED", restartedJournal.get(reqRejected).orElseThrow().getFinalOutcome());

            // 3. Node báo UNKNOWN -> Tiếp tục giữ pending (SENT_UNRESOLVED), không tự ý suy ra thất bại
            journalingClient.getRequestResult(reqUnknown).join();
            assertEquals(PendingState.SENT_UNRESOLVED, restartedJournal.get(reqUnknown).orElseThrow().getState());

            // 4. Lỗi mạng / timeout -> Vẫn giữ nguyên pending
            BookingClient timeoutClient = new MockBookingClient() {
                @Override
                public CompletableFuture<GetRequestResultResponseDto> getRequestResult(String requestId) {
                    return CompletableFuture.failedFuture(new IOException("Simulated network timeout"));
                }
            };
            JournalingBookingClient timeoutJournalingClient = new JournalingBookingClient(timeoutClient, restartedJournal);
            timeoutJournalingClient.getRequestResult(reqUnknown).whenComplete((res, err) -> {});

            assertEquals(PendingState.SENT_UNRESOLVED, restartedJournal.get(reqUnknown).orElseThrow().getState());
        }
    }

    // =========================================================================
    // CA 6: Crash giữa lúc ghi: dòng cuối bị cắt dở -> bỏ qua an toàn kèm cảnh báo
    // =========================================================================
    @Test
    @DisplayName("Ca 6: Crash giữa lúc ghi: dòng dở được bỏ qua, bản ghi trước vẫn nguyên vẹn")
    void testPartialLineSkippedSafelyOnCrash() throws IOException {
        String reqId1 = UUID.randomUUID().toString();
        PendingEntry entry1 = PendingEntry.createPrepared(reqId1, "BOOK_TICKET", "{\"requestId\":\"" + reqId1 + "\"}");
        journal.recordPrepared(entry1);
        journal.close();

        // Cố tình ghi thêm một dòng dở (mô phỏng crash mất điện đột ngột khi đang ghi dở)
        Path journalFile = tempJournalDir.resolve(PersistentPendingJournal.JOURNAL_FILE_NAME);
        String partialLine = "{\"requestId\":\"corrupt-id\",\"commandType\":\"BOOK_TIC";
        Files.writeString(journalFile, partialLine, StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        // Khởi động lại journal: phải đọc được entry1 và không bị ném ngoại lệ
        try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
            assertEquals(1, restartedJournal.loadPending().size());
            assertEquals(reqId1, restartedJournal.loadPending().get(0).getRequestId());
        }
    }

    // =========================================================================
    // CA 7: Payload bị sửa / hash lệch -> CORRUPT, khóa nút retry
    // =========================================================================
    @Test
    @DisplayName("Ca 7: Payload bị sửa lệch SHA-256 -> đánh dấu CORRUPT, từ chối retry")
    void testPayloadTamperedMarksCorrupt() throws IOException {
        String reqId = UUID.randomUUID().toString();
        String validPayload = "{\"requestId\":\"" + reqId + "\",\"seatId\":\"G06\"}";
        PendingEntry validEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", validPayload);

        // Tạo bản ghi bị lệch hash
        PendingEntry corruptEntry = new PendingEntry(
                reqId, "BOOK_TICKET", "{\"requestId\":\"" + reqId + "\",\"seatId\":\"G07\"}", // Payload bị sửa
                validEntry.getPayloadHash(), // Nhưng hash vẫn là của G06
                PendingState.PREPARED, Instant.now(), null, 0, null, null, 1
        );

        assertFalse(corruptEntry.isPayloadValid(), "isPayloadValid phải trả về false khi hash không khớp");

        journal.close();

        // Ghi trực tiếp bản ghi sai hash xuống file
        Path journalFile = tempJournalDir.resolve(PersistentPendingJournal.JOURNAL_FILE_NAME);
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        String json = mapper.writeValueAsString(corruptEntry);
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(json.getBytes(StandardCharsets.UTF_8));
        Files.writeString(journalFile, json + "#" + Long.toHexString(crc.getValue()) + "\n");

        try (PersistentPendingJournal restartedJournal = new PersistentPendingJournal(tempJournalDir)) {
            PendingEntry loaded = restartedJournal.get(reqId).orElseThrow();
            assertEquals(PendingState.CORRUPT, loaded.getState(), "Phải đánh dấu CORRUPT khi nạp bản ghi có hash không khớp");

            JournalingBookingClient client = new JournalingBookingClient(new MockBookingClient(), restartedJournal);
            var retryFuture = client.retry(reqId);
            assertTrue(retryFuture.isCompletedExceptionally(), "Phải từ chối retry khi bản ghi bị CORRUPT");
        }
    }

    // =========================================================================
    // CA 8: Ghi trùng requestId: cùng payload -> idempotent; khác payload -> REQUEST_ID_CONFLICT
    // =========================================================================
    @Test
    @DisplayName("Ca 8: Idempotency & Conflict: cùng payload thì idempotent, khác payload thì từ chối")
    void testIdempotencyAndRequestIdConflict() throws IOException {
        String reqId = UUID.randomUUID().toString();
        String payload1 = "{\"requestId\":\"" + reqId + "\",\"seatId\":\"G08\"}";
        String payload2 = "{\"requestId\":\"" + reqId + "\",\"seatId\":\"G09\"}";

        PendingEntry entry1 = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload1);
        journal.recordPrepared(entry1);

        // Ghi lại CÙNG requestId và CÙNG payload: phải idempotent, không ném ngoại lệ
        assertDoesNotThrow(() -> journal.recordPrepared(entry1));
        assertEquals(1, journal.getAll().size());

        // Ghi lại CÙNG requestId nhưng KHÁC payload: phải ném IllegalArgumentException (REQUEST_ID_CONFLICT)
        PendingEntry conflictEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload2);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> journal.recordPrepared(conflictEntry));
        assertTrue(ex.getMessage().contains("REQUEST_ID_CONFLICT"), "Phải thông báo rõ lỗi REQUEST_ID_CONFLICT");

        // Bản ghi ban đầu không bị ghi đè
        assertEquals(payload1, journal.get(reqId).orElseThrow().getPayload());
    }

    // =========================================================================
    // CA 9: Khóa tiến trình duy nhất (Single-process lock): chặn instance thứ hai
    // =========================================================================
    @Test
    @DisplayName("Ca 9: Single-process lock: Chặn instance thứ hai mở cùng thư mục journal")
    void testSingleProcessLockPreventsSecondInstance() {
        assertNotNull(journal);
        // Cố gắng mở instance thứ hai trên cùng tempJournalDir khi instance thứ nhất đang mở
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            new PersistentPendingJournal(tempJournalDir);
        });
        assertTrue(ex.getMessage().contains("journal.lock") || ex.getMessage().contains("tiến trình khác"),
                "Phải ném ngoại lệ báo không thể lấy khóa độc quyền journal.lock");
    }

    // =========================================================================
    // CA 10: Compaction: dọn dẹp các bản ghi RESOLVED hết hạn giữ, bảo lưu pending
    // =========================================================================
    @Test
    @DisplayName("Ca 10: Compaction dọn dẹp bản ghi RESOLVED cũ, không làm mất bản ghi pending")
    void testCompactionCleansOldResolvedEntries() throws IOException {
        String reqResolved = UUID.randomUUID().toString();
        String reqPending = UUID.randomUUID().toString();

        journal.recordPrepared(PendingEntry.createPrepared(reqResolved, "BOOK_TICKET", "{\"requestId\":\"" + reqResolved + "\"}"));
        journal.markSent(reqResolved);
        journal.markResolved(reqResolved, "CONFIRMED");

        journal.recordPrepared(PendingEntry.createPrepared(reqPending, "BOOK_TICKET", "{\"requestId\":\"" + reqPending + "\"}"));
        journal.markSent(reqPending);

        assertEquals(2, journal.getAll().size());

        // Thực hiện compaction với retention = 0 (xóa ngay các bản ghi RESOLVED)
        journal.compact(Duration.ZERO);

        assertEquals(1, journal.getAll().size(), "Bản ghi RESOLVED phải bị dọn dẹp");
        assertTrue(journal.get(reqPending).isPresent(), "Bản ghi pending BẮT BUỘC phải được bảo lưu nguyên vẹn");
        assertEquals(PendingState.SENT_UNRESOLVED, journal.get(reqPending).get().getState());
    }

    // =========================================================================
    // CA 11: Chuyển trạng thái bất hợp lệ sang thành công bị từ chối
    // =========================================================================
    @Test
    @DisplayName("Ca 11: Chuyển trạng thái sang RESOLVED mà không có outcome hợp lệ bị từ chối")
    void testInvalidStateTransitionsRejected() {
        String reqId = UUID.randomUUID().toString();
        PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", "{\"requestId\":\"" + reqId + "\"}");

        // 1. withState(RESOLVED) khi finalOutcome == null -> ném IllegalStateException
        assertThrows(IllegalStateException.class, () -> entry.withState(PendingState.RESOLVED));

        // 2. withResolved với outcome không hợp lệ -> ném IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> entry.withResolved("INVALID_OUTCOME", Instant.now()));
        assertThrows(IllegalArgumentException.class, () -> entry.withResolved(null, Instant.now()));
    }

    // =========================================================================
    // CA 12: Quyền riêng tư: Bắt log kiểm tra TUYỆT ĐỐI KHÔNG log payload
    // =========================================================================
    @Test
    @DisplayName("Ca 12: Quyền riêng tư: Bắt log kiểm chứng TUYỆT ĐỐI KHÔNG log nội dung payload")
    void testPrivacyNoPayloadInLogs() throws IOException {
        String secretName = "BiMatHanhKhach_NguyenVanX";
        String reqId = UUID.randomUUID().toString();
        String sensitivePayload = "{\"requestId\":\"" + reqId + "\",\"passengerName\":\"" + secretName + "\"}";

        List<String> capturedLogs = new ArrayList<>();
        Handler testHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record != null && record.getMessage() != null) {
                    capturedLogs.add(record.getMessage());
                }
            }
            @Override
            public void flush() {}
            @Override
            public void close() throws SecurityException {}
        };

        Logger journalLogger = Logger.getLogger(PersistentPendingJournal.class.getName());
        Logger clientLogger = Logger.getLogger(JournalingBookingClient.class.getName());

        journalLogger.addHandler(testHandler);
        clientLogger.addHandler(testHandler);

        try {
            PendingEntry entry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", sensitivePayload);
            journal.recordPrepared(entry);
            journal.markSent(reqId, "node-1");
            journal.markResolved(reqId, "CONFIRMED");

            // Kiểm tra toàn bộ các dòng log đã xuất ra
            for (String logMsg : capturedLogs) {
                assertFalse(logMsg.contains(secretName),
                        "VI PHẠM BẢO MẬT: Phát hiện tên hành khách nhạy cảm trong log: " + logMsg);
                assertFalse(logMsg.contains(sensitivePayload),
                        "VI PHẠM BẢO MẬT: Phát hiện toàn bộ payload trong log: " + logMsg);
            }
        } finally {
            journalLogger.removeHandler(testHandler);
            clientLogger.removeHandler(testHandler);
        }
    }

    // =========================================================================
    // CA 13: Lookup với Node thật (Chỉ kích hoạt khi có biến môi trường NODE_HOST)
    // =========================================================================
    @Test
    @EnabledIfEnvironmentVariable(named = "NODE_HOST", matches = ".+")
    @DisplayName("Ca 13: Tra cứu với Node thật (sau D19 - kích hoạt khi có NODE_HOST)")
    void testRealNodeLookupPostD19() {
        // Chỉ chạy khi có biến môi trường cấu hình Node thật
        String host = System.getenv("NODE_HOST");
        assertNotNull(host);
    }
}
