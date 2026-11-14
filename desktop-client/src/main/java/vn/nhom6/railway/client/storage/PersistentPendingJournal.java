package vn.nhom6.railway.client.storage;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Journal lưu trữ yêu cầu đang chờ xử lý bền vững xuống đĩa cục bộ (TASK TV1-D14).
 * <p>
 * Bất biến & Quy tắc kỹ thuật:
 * 1. Lưu file cục bộ (append-only JSON Lines), KHÔNG dùng DB, KHÔNG dùng mạng.
 * 2. Ghi bền vững trước khi gửi: flush + {@link FileChannel#force(boolean)} xong mới gửi đi.
 * 3. Chống crash: Dòng cuối bị cắt dở (do mất điện/crash) được bỏ qua an toàn kèm cảnh báo,
 *    không làm hỏng các bản ghi hợp lệ trước đó.
 * 4. Khóa độc quyền (Single-process lock) qua {@link FileLock} trên file {@code journal.lock}.
 * 5. Quyền riêng tư: TUYỆT ĐỐI KHÔNG ghi nội dung {@code payload} ra log hệ thống
 *    (chỉ ghi requestId, state, attemptCount, payloadBytes).
 * 6. Idempotent: Ghi lại cùng requestId và cùng payload là idempotent;
 *    ghi trùng requestId nhưng khác payload sẽ ném {@link IllegalArgumentException} (REQUEST_ID_CONFLICT).
 */
public final class PersistentPendingJournal implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(PersistentPendingJournal.class.getName());

    public static final String LOCK_FILE_NAME = "journal.lock";
    public static final String JOURNAL_FILE_NAME = "journal.jsonl";
    public static final String TMP_JOURNAL_FILE_NAME = "journal.jsonl.tmp";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Path journalDir;
    private final Path lockFilePath;
    private final Path journalFilePath;

    private FileChannel lockChannel;
    private FileLock fileLock;
    private FileChannel journalChannel;

    private final Object writeLock = new Object();
    private final Map<String, PendingEntry> entries = new LinkedHashMap<>();
    private volatile boolean closed = false;

    /**
     * Khởi tạo Journal trong thư mục mặc định hoặc cấu hình hệ thống:
     * {@code -Drailway.journal.dir=...} hoặc {@code ~/.railway/journal}.
     */
    public static PersistentPendingJournal createDefault() {
        String dirProp = System.getProperty("railway.journal.dir");
        Path dir;
        if (dirProp != null && !dirProp.isBlank()) {
            dir = Paths.get(dirProp.trim());
        } else {
            String userHome = System.getProperty("user.home", ".");
            dir = Paths.get(userHome, ".railway", "journal");
        }
        try {
            return new PersistentPendingJournal(dir);
        } catch (IOException e) {
            throw new IllegalStateException("Không thể khởi tạo PersistentPendingJournal tại " + dir + ": " + e.getMessage(), e);
        }
    }

    /**
     * Khởi tạo Journal với thư mục cụ thể (phục vụ unit test với {@code @TempDir}).
     */
    public PersistentPendingJournal(Path journalDir) throws IOException {
        this.journalDir = Objects.requireNonNull(journalDir, "journalDir không được null").toAbsolutePath().normalize();
        Files.createDirectories(this.journalDir);

        this.lockFilePath = this.journalDir.resolve(LOCK_FILE_NAME);
        this.journalFilePath = this.journalDir.resolve(JOURNAL_FILE_NAME);

        acquireLock();
        openJournalChannel();
        recoverFromDisk();
    }

    /**
     * Chiếm khóa độc quyền trên file journal.lock.
     */
    private void acquireLock() throws IOException {
        this.lockChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            this.fileLock = this.lockChannel.tryLock();
        } catch (OverlappingFileLockException e) {
            this.lockChannel.close();
            throw new IllegalStateException("Không thể chiếm khóa journal.lock tại " + lockFilePath +
                    ". Đã có tiến trình khác đang chạy ứng dụng hoặc mở journal!", e);
        }
        if (this.fileLock == null) {
            this.lockChannel.close();
            throw new IllegalStateException("Không thể chiếm khóa journal.lock tại " + lockFilePath +
                    ". Đã có tiến trình khác đang chạy ứng dụng hoặc mở journal!");
        }
    }

    /**
     * Mở kênh ghi append vào file journal.jsonl.
     */
    private void openJournalChannel() throws IOException {
        this.journalChannel = FileChannel.open(journalFilePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /**
     * Nạp và khôi phục các bản ghi từ đĩa khi khởi động (TASK D14).
     */
    private void recoverFromDisk() {
        synchronized (writeLock) {
            entries.clear();
            if (!Files.exists(journalFilePath)) {
                return;
            }

            try (BufferedReader reader = Files.newBufferedReader(journalFilePath, StandardCharsets.UTF_8)) {
                String rawLine;
                long lineNumber = 0;
                while ((rawLine = reader.readLine()) != null) {
                    lineNumber++;
                    String line = rawLine.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    // Kiểm tra định dạng dòng: <json>#<crc32_hex>
                    int hashIdx = line.lastIndexOf('#');
                    if (hashIdx <= 0 || hashIdx >= line.length() - 1) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-RECOVERY] Bỏ qua dòng dở hoặc thiếu checksum ở dòng {0}", lineNumber);
                        continue;
                    }

                    String jsonPart = line.substring(0, hashIdx);
                    String expectedCrcHex = line.substring(hashIdx + 1);

                    CRC32 crc = new CRC32();
                    crc.update(jsonPart.getBytes(StandardCharsets.UTF_8));
                    String actualCrcHex = Long.toHexString(crc.getValue());

                    if (!expectedCrcHex.equalsIgnoreCase(actualCrcHex)) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-RECOVERY] Bỏ qua dòng bị sai lệch CRC32 ở dòng {0}", lineNumber);
                        continue;
                    }

                    PendingEntry entry;
                    try {
                        entry = OBJECT_MAPPER.readValue(jsonPart, PendingEntry.class);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-RECOVERY] Bỏ qua dòng JSON không hợp lệ ở dòng {0}: {1}",
                                new Object[]{lineNumber, e.getMessage()});
                        continue;
                    }

                    // Kiểm tra tính toàn vẹn của payload
                    if (!entry.isPayloadValid()) {
                        LOGGER.log(Level.WARNING, "[JOURNAL-RECOVERY] Phát hiện payload bị sai lệch SHA-256 cho requestId={0}, đánh dấu CORRUPT",
                                entry.getRequestId());
                        entry = entry.withState(PendingState.CORRUPT);
                    }

                    // Kiểm tra conflict payload nếu đã có requestId trước đó
                    PendingEntry existing = entries.get(entry.getRequestId());
                    if (existing != null && !existing.getPayload().equals(entry.getPayload())) {
                        LOGGER.log(Level.SEVERE, "[JOURNAL-RECOVERY] REQUEST_ID_CONFLICT khi nạp journal: requestId={0}", entry.getRequestId());
                        // Giữ nguyên bản ghi đầu tiên, không ghi đè bản ghi xung đột
                        continue;
                    }

                    entries.put(entry.getRequestId(), entry);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "[JOURNAL-RECOVERY] Gặp lỗi I/O khi đọc file journal: {0}", e.getMessage());
            }
        }
    }

    /**
     * Ghi một bản ghi xuống đĩa kèm flush + force(true).
     */
    private void appendLine(PendingEntry entry) throws IOException {
        checkNotClosed();
        String json = OBJECT_MAPPER.writeValueAsString(entry);
        CRC32 crc = new CRC32();
        crc.update(json.getBytes(StandardCharsets.UTF_8));
        String crcHex = Long.toHexString(crc.getValue());

        String formattedLine = json + "#" + crcHex + "\n";
        byte[] bytes = formattedLine.getBytes(StandardCharsets.UTF_8);

        journalChannel.write(ByteBuffer.wrap(bytes));
        // Đảm bảo dữ liệu được flush xuống lớp lưu trữ vật lý
        journalChannel.force(true);

        // Ghi log bảo mật: KHÔNG log payload, chỉ log metadata
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(String.format("[JOURNAL] Persisted entry requestId=%s, cmd=%s, state=%s, attempts=%d, payloadBytes=%d",
                    entry.getRequestId(), entry.getCommandType(), entry.getState(),
                    entry.getAttemptCount(), entry.getPayload() != null ? entry.getPayload().length() : 0));
        }
    }

    /**
     * Ghi nhận yêu cầu mới ở trạng thái PREPARED (bắt buộc hoàn tất trước khi gửi qua mạng).
     *
     * @param entry bản ghi yêu cầu (chứa payload nguyên văn và payloadHash)
     * @throws IOException nếu không thể ghi bền vững (fail-closed)
     * @throws IllegalArgumentException nếu phát hiện REQUEST_ID_CONFLICT
     */
    public void recordPrepared(PendingEntry entry) throws IOException {
        Objects.requireNonNull(entry, "entry không được null");
        synchronized (writeLock) {
            checkNotClosed();
            PendingEntry existing = entries.get(entry.getRequestId());
            if (existing != null) {
                if (existing.getPayload().equals(entry.getPayload())) {
                    // Idempotent: cùng requestId và cùng payload -> giữ nguyên
                    return;
                } else {
                    throw new IllegalArgumentException("REQUEST_ID_CONFLICT: requestId " + entry.getRequestId() +
                            " đã tồn tại trong journal với payload khác!");
                }
            }

            if (!entry.isPayloadValid()) {
                throw new IllegalArgumentException("Payload SHA-256 hash không hợp lệ trước khi ghi journal!");
            }

            appendLine(entry);
            entries.put(entry.getRequestId(), entry);
        }
    }

    /**
     * Đánh dấu yêu cầu đã được gửi hoặc bắt đầu gửi qua mạng (chuyển sang SENT_UNRESOLVED).
     */
    public void markSent(String requestId) throws IOException {
        markSent(requestId, null);
    }

    /**
     * Đánh dấu yêu cầu đã được gửi kèm nodeId (nếu biết).
     */
    public void markSent(String requestId, String nodeId) throws IOException {
        Objects.requireNonNull(requestId, "requestId không được null");
        synchronized (writeLock) {
            checkNotClosed();
            PendingEntry existing = entries.get(requestId);
            if (existing == null) {
                throw new NoSuchElementException("Không tìm thấy entry cho requestId: " + requestId);
            }
            if (existing.getState() == PendingState.RESOLVED) {
                // Đã RESOLVED thì không chuyển ngược lại SENT_UNRESOLVED
                return;
            }

            PendingEntry updated = existing.withAttempt(Instant.now(), nodeId);
            appendLine(updated);
            entries.put(requestId, updated);
        }
    }

    /**
     * Đánh dấu yêu cầu đã nhận được kết quả cuối cùng từ server (CONFIRMED hoặc REJECTED).
     */
    public void markResolved(String requestId, String outcome) throws IOException {
        Objects.requireNonNull(requestId, "requestId không được null");
        Objects.requireNonNull(outcome, "outcome không được null");
        synchronized (writeLock) {
            checkNotClosed();
            PendingEntry existing = entries.get(requestId);
            if (existing == null) {
                throw new NoSuchElementException("Không tìm thấy entry cho requestId: " + requestId);
            }

            PendingEntry updated = existing.withResolved(outcome, Instant.now());
            appendLine(updated);
            entries.put(requestId, updated);
        }
    }

    /**
     * Đánh dấu yêu cầu không thể gửi đi (lỗi tiền kiểm hoặc hủy trước khi chạm mạng).
     */
    public void markFailedNotSent(String requestId, String reason) throws IOException {
        Objects.requireNonNull(requestId, "requestId không được null");
        synchronized (writeLock) {
            checkNotClosed();
            PendingEntry existing = entries.get(requestId);
            if (existing == null) {
                throw new NoSuchElementException("Không tìm thấy entry cho requestId: " + requestId);
            }

            PendingEntry updated = existing.withState(PendingState.FAILED_NOT_SENT);
            appendLine(updated);
            entries.put(requestId, updated);
        }
    }

    /**
     * Lấy danh sách các yêu cầu đang chờ xử lý (PREPARED, SENT_UNRESOLVED, CORRUPT).
     */
    public List<PendingEntry> loadPending() {
        synchronized (writeLock) {
            List<PendingEntry> list = new ArrayList<>();
            for (PendingEntry e : entries.values()) {
                if (e.getState().isPending() || e.getState() == PendingState.CORRUPT) {
                    list.add(e);
                }
            }
            return Collections.unmodifiableList(list);
        }
    }

    /**
     * Lấy toàn bộ các bản ghi trong journal.
     */
    public List<PendingEntry> getAll() {
        synchronized (writeLock) {
            return Collections.unmodifiableList(new ArrayList<>(entries.values()));
        }
    }

    /**
     * Lấy bản ghi theo requestId.
     */
    public Optional<PendingEntry> get(String requestId) {
        synchronized (writeLock) {
            return Optional.ofNullable(entries.get(requestId));
        }
    }

    /**
     * Thu dọn định kỳ (Compaction):
     * Dọn dẹp các bản ghi đã RESOLVED quá thời hạn giữ (retentionDuration) bằng file tạm + ATOMIC_MOVE.
     * Các bản ghi đang pending (PREPARED / SENT_UNRESOLVED / CORRUPT) luôn được giữ nguyên vẹn.
     */
    public void compact(Duration retentionDuration) throws IOException {
        Objects.requireNonNull(retentionDuration, "retentionDuration không được null");
        synchronized (writeLock) {
            checkNotClosed();
            Instant cutoff = Instant.now().minus(retentionDuration);

            Path tmpPath = journalDir.resolve(TMP_JOURNAL_FILE_NAME);
            Files.deleteIfExists(tmpPath);

            Map<String, PendingEntry> survivingEntries = new LinkedHashMap<>();

            try (FileChannel tmpChannel = FileChannel.open(tmpPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                for (PendingEntry entry : entries.values()) {
                    boolean keep = true;
                    if (entry.getState() == PendingState.RESOLVED) {
                        Instant time = entry.getLastAttemptAt() != null ? entry.getLastAttemptAt() : entry.getCreatedAt();
                        if (time.isBefore(cutoff)) {
                            keep = false;
                        }
                    }

                    if (keep) {
                        survivingEntries.put(entry.getRequestId(), entry);
                        String json = OBJECT_MAPPER.writeValueAsString(entry);
                        CRC32 crc = new CRC32();
                        crc.update(json.getBytes(StandardCharsets.UTF_8));
                        String line = json + "#" + Long.toHexString(crc.getValue()) + "\n";
                        tmpChannel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
                    }
                }
                tmpChannel.force(true);
            }

            // Đóng kênh journal cũ
            if (journalChannel != null && journalChannel.isOpen()) {
                journalChannel.close();
            }

            // ATOMIC_MOVE file tạm sang file journal chính thức
            Files.move(tmpPath, journalFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            // Cập nhật lại bộ nhớ và mở lại kênh journal
            entries.clear();
            entries.putAll(survivingEntries);
            openJournalChannel();

            LOGGER.info(String.format("[JOURNAL] Compaction hoàn tất, còn lại %d bản ghi sống sót", entries.size()));
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("PersistentPendingJournal đã bị đóng");
        }
    }

    public Path getJournalDir() {
        return journalDir;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        if (journalChannel != null && journalChannel.isOpen()) {
            try {
                journalChannel.force(true);
                journalChannel.close();
            } catch (IOException ignored) {}
        }

        if (fileLock != null && fileLock.isValid()) {
            try {
                fileLock.release();
            } catch (IOException ignored) {}
        }

        if (lockChannel != null && lockChannel.isOpen()) {
            try {
                lockChannel.close();
            } catch (IOException ignored) {}
        }

        LOGGER.info("[JOURNAL] Đã giải phóng fileLock và đóng journal thành công.");
    }
}
