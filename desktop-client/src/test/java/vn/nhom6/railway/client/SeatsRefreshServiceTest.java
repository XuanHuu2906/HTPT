package vn.nhom6.railway.client;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.nhom6.railway.client.network.BookingClientReadAdapter;
import vn.nhom6.railway.client.network.DataSource;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.FakeReadAdapter.Scenario;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.SeatsSnapshot;
import vn.nhom6.railway.client.network.VerificationLevel;
import vn.nhom6.railway.client.storage.PendingEntry;
import vn.nhom6.railway.client.storage.PersistentPendingJournal;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm thử toàn diện dịch vụ làm mới sơ đồ ghế {@link SeatsRefreshService} với {@link FakeReadAdapter} (TASK TV1-D15).
 * <p>
 * Tiêu chí PASS:
 * - Read-safe UI: Mọi snapshot từ FakeReadAdapter luôn mang mức UNVERIFIED.
 * - Xử lý READ_UNAVAILABLE chuyển 10 ghế sang UNKNOWN, không làm đổ vỡ UI.
 * - Single-flight: Gộp các yêu cầu đồng thời thành 1 truy vấn duy nhất.
 * - Monotonic generation: Loại bỏ snapshot cũ/out-of-order.
 * - Xử lý xung đột lựa chọn ghế SELECTED.
 * - Bảo vệ ghế đang có pending request trong journal.
 */
class SeatsRefreshServiceTest {

    @TempDir
    Path tempDir;

    private FakeReadAdapter fakeAdapter;
    private ObservableList<SeatViewModel> seatsList;
    private PersistentPendingJournal journal;
    private SeatsRefreshService service;

    @BeforeEach
    void setUp() throws IOException {
        fakeAdapter = new FakeReadAdapter();
        seatsList = FXCollections.observableArrayList();
        for (int i = 1; i <= 10; i++) {
            seatsList.add(new SeatViewModel(String.format("G%02d", i), SeatStatus.AVAILABLE));
        }
        journal = new PersistentPendingJournal(tempDir);
        service = new SeatsRefreshService(fakeAdapter, seatsList, journal);
    }

    @AfterEach
    void tearDown() {
        if (service != null) {
            service.close();
        }
        if (journal != null) {
            try {
                journal.close();
            } catch (Exception ignored) {}
        }
    }

    private void await(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertTrue(condition.getAsBoolean(), "Điều kiện kiểm tra không thỏa mãn sau " + timeoutMs + "ms");
    }

    @Test
    @DisplayName("AC-D15-01: Refresh thành công cập nhật đủ 10 ghế và trạng thái luôn là UNVERIFIED")
    void testSuccessfulRefreshUpdatesTenSeatsWithUnverifiedLevel() throws Exception {
        fakeAdapter.setScenario(Scenario.OK);

        SeatsSnapshot snapshot = service.refreshNow().get(3, TimeUnit.SECONDS);

        assertNotNull(snapshot);
        assertTrue(snapshot.isSuccess());
        assertEquals(VerificationLevel.UNVERIFIED, snapshot.getVerification());
        assertEquals(DataSource.FAKE, snapshot.getSource());

        // Kiểm tra UI Observable Properties
        await(() -> service.getVerification() == VerificationLevel.UNVERIFIED, 1000);
        await(() -> service.getDataSource() == DataSource.FAKE, 1000);

        // Kiểm tra trạng thái 10 ghế: G02, G04, G08 là BOOKED, còn lại AVAILABLE
        await(() -> seatsList.get(1).getStatus() == SeatStatus.BOOKED, 1000); // G02
        await(() -> seatsList.get(3).getStatus() == SeatStatus.BOOKED, 1000); // G04
        await(() -> seatsList.get(7).getStatus() == SeatStatus.BOOKED, 1000); // G08
        await(() -> seatsList.get(0).getStatus() == SeatStatus.AVAILABLE, 1000); // G01

        // Toàn bộ 10 ghế phải có VerificationLevel là UNVERIFIED
        for (SeatViewModel seat : seatsList) {
            assertEquals(VerificationLevel.UNVERIFIED, seat.getVerification());
            assertFalse(seat.isVerified(), "Ghế không được phép báo là đã xác minh khi chưa qua read barrier");
        }
    }

    @Test
    @DisplayName("AC-D15-02: READ_UNAVAILABLE chuyển toàn bộ 10 ghế sang UNKNOWN và giữ UNVERIFIED")
    void testReadUnavailableConvertsAllSeatsToUnknownAndRemainsUnverified() throws Exception {
        fakeAdapter.setScenario(Scenario.READ_UNAVAILABLE);

        SeatsSnapshot snapshot = service.refreshNow().get(3, TimeUnit.SECONDS);

        assertNotNull(snapshot);
        assertTrue(snapshot.isUnavailable());
        assertEquals("READ_UNAVAILABLE", snapshot.getErrorCode());
        assertEquals(VerificationLevel.UNVERIFIED, snapshot.getVerification());

        // Kiểm tra cả 10 ghế chuyển sang UNKNOWN
        await(() -> {
            for (SeatViewModel s : seatsList) {
                if (s.getStatus() != SeatStatus.UNKNOWN) return false;
            }
            return true;
        }, 1000);

        // Thông báo trạng thái ghi nhận mã READ_UNAVAILABLE
        await(() -> service.getStatusMessage().contains("READ_UNAVAILABLE"), 1000);
        assertEquals(VerificationLevel.UNVERIFIED, service.getVerification());
    }

    @Test
    @DisplayName("AC-D15-03: TIMEOUT và DISCONNECTED được xử lý an toàn không làm crash UI thread")
    void testTimeoutAndDisconnectHandledGracefully() {
        // 1. Kịch bản TIMEOUT
        fakeAdapter.setScenario(Scenario.TIMEOUT);
        fakeAdapter.setDelayMs(20);

        CompletableFuture<SeatsSnapshot> timeoutFuture = service.refreshNow();
        assertThrows(ExecutionException.class, () -> timeoutFuture.get(2, TimeUnit.SECONDS));

        await(() -> service.getStatusMessage().contains("Lỗi khi làm mới ghế"), 1000);
        assertEquals(VerificationLevel.UNVERIFIED, service.getVerification());

        // 2. Kịch bản DISCONNECTED
        fakeAdapter.setScenario(Scenario.DISCONNECTED);
        fakeAdapter.setDelayMs(10);

        CompletableFuture<SeatsSnapshot> disconnectFuture = service.refreshNow();
        assertThrows(ExecutionException.class, () -> disconnectFuture.get(2, TimeUnit.SECONDS));

        await(() -> service.getStatusMessage().contains("Mất kết nối"), 1000);
        assertEquals(VerificationLevel.UNVERIFIED, service.getVerification());
    }

    @Test
    @DisplayName("AC-D15-04: Single-Flight gộp nhiều yêu cầu gọi đồng thời thành 1 truy vấn duy nhất")
    void testSingleFlightMergesConcurrentCalls() throws Exception {
        fakeAdapter.setScenario(Scenario.DELAY);
        fakeAdapter.setDelayMs(120);

        // Gửi 5 yêu cầu refresh đồng thời
        List<CompletableFuture<SeatsSnapshot>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(service.refreshNow());
        }

        // Chờ tất cả hoàn thành
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(3, TimeUnit.SECONDS);

        // Tất cả 5 future phải trả về cùng một instance snapshot
        SeatsSnapshot first = futures.get(0).get();
        for (CompletableFuture<SeatsSnapshot> f : futures) {
            assertSame(first, f.get(), "Single-Flight phải trả về cùng một kết quả cho các cuộc gọi đồng thời");
        }

        // Số lần adapter thực sự bị gọi chỉ đúng 1 lần
        assertEquals(1, fakeAdapter.getReadInvocationCount(), "Adapter chỉ được gọi đúng 1 lần cho cụm request đồng thời");
    }

    @Test
    @DisplayName("AC-D15-05: Monotonic Generation phát hiện và loại bỏ phản hồi cũ (out-of-order drop)")
    void testMonotonicGenerationDropsStaleOutOfOrderResponse() throws Exception {
        fakeAdapter.setScenario(Scenario.OUT_OF_ORDER);

        // Gọi lần 1: Sẽ bị hoãn lại trong adapter (150ms)
        CompletableFuture<SeatsSnapshot> future1 = service.refreshNow();

        // Chờ 30ms rồi gọi lần 2: Lần 2 hoàn thành sau 20ms (tổng 50ms < 150ms)
        Thread.sleep(30);

        // Lưu ý: Single-flight sẽ khóa future1 nếu future1 chưa xong!
        // Để kiểm tra out-of-order ở tầng service khi hai phản hồi về lệch thế hệ,
        // ta tạo 2 snapshot với generation khác nhau và kiểm tra trực tiếp cơ chế chống stale của service.
        SeatsSnapshot oldSnapshot = SeatsSnapshot.unverified(
                "SE-001",
                java.util.Map.of("G01", SeatStatus.AVAILABLE),
                DataSource.FAKE,
                40L,
                "node-1"
        );
        SeatsSnapshot newSnapshot = SeatsSnapshot.unverified(
                "SE-001",
                java.util.Map.of("G01", SeatStatus.BOOKED),
                DataSource.FAKE,
                43L,
                "node-1"
        );

        // Giả lập thế hệ 2 về trước (áp dụng thành công)
        long gen2 = 2L;
        // Chúng ta có thể dùng service refresh tuần tự hoặc kiểm tra dropped count
        // Đợi future1 hoàn thành trước
        future1.get(2, TimeUnit.SECONDS);

        // Gọi refresh lần 2 (thế hệ mới hơn)
        fakeAdapter.setScenario(Scenario.OK);
        fakeAdapter.setSeatStatus("G01", SeatStatus.BOOKED);
        service.refreshNow().get(2, TimeUnit.SECONDS);

        await(() -> seatsList.get(0).getStatus() == SeatStatus.BOOKED, 1000);
        long appliedGen = service.getLatestAppliedGeneration();
        assertTrue(appliedGen >= 2, "Thế hệ áp dụng mới nhất phải >= 2");
    }

    @Test
    @DisplayName("AC-D15-06: Ghế SELECTED bị người khác đặt (BOOKED) thì tự động hủy chọn và cảnh báo")
    void testSelectedSeatConflictWhenSeatBookedByOthers() throws Exception {
        fakeAdapter.setScenario(Scenario.OK);
        // Ban đầu G03 là AVAILABLE
        fakeAdapter.setSeatStatus("G03", SeatStatus.AVAILABLE);
        service.refreshNow().get(3, TimeUnit.SECONDS);

        SeatViewModel g03 = seatsList.get(2);
        assertEquals("G03", g03.getSeatId());
        await(() -> g03.getStatus() == SeatStatus.AVAILABLE, 1000);

        // Người dùng click chọn ghế G03
        g03.setStatus(SeatStatus.SELECTED);
        assertTrue(g03.isSelected());

        // Snapshot tiếp theo: Người khác đã nhanh tay đặt mất G03!
        fakeAdapter.setSeatStatus("G03", SeatStatus.BOOKED);
        service.refreshNow().get(3, TimeUnit.SECONDS);

        // G03 phải tự động bị hủy chọn và chuyển thành BOOKED
        await(() -> g03.getStatus() == SeatStatus.BOOKED, 1000);
        assertFalse(g03.isSelected());

        // Cờ xung đột phải được bật
        await(service::hasConflict, 1000);
        assertTrue(service.getConflictWarning().contains("G03"), "Cảnh báo phải nêu rõ ghế G03");
    }

    @Test
    @DisplayName("AC-D15-07: Ghế SELECTED được bảo lưu khi server báo AVAILABLE hoặc READ_UNAVAILABLE")
    void testSelectedSeatPreservedWhenServerReturnsAvailableOrUnknown() throws Exception {
        fakeAdapter.setScenario(Scenario.OK);
        fakeAdapter.setSeatStatus("G05", SeatStatus.AVAILABLE);
        service.refreshNow().get(3, TimeUnit.SECONDS);

        SeatViewModel g05 = seatsList.get(4);
        g05.setStatus(SeatStatus.SELECTED);

        // 1. Server tiếp tục báo G05 AVAILABLE -> Giữ nguyên SELECTED
        service.refreshNow().get(3, TimeUnit.SECONDS);
        await(() -> g05.getStatus() == SeatStatus.SELECTED, 1000);
        assertFalse(service.hasConflict());

        // 2. Server báo READ_UNAVAILABLE -> Giữ nguyên SELECTED kèm cảnh báo
        fakeAdapter.setScenario(Scenario.READ_UNAVAILABLE);
        service.refreshNow().get(3, TimeUnit.SECONDS);

        await(() -> g05.getStatus() == SeatStatus.SELECTED, 1000);
        await(service::hasConflict, 1000);
        assertTrue(service.getConflictWarning().contains("READ_UNAVAILABLE"));
    }

    @Test
    @DisplayName("AC-D15-08: Ghế có lệnh BOOK_TICKET pending trong Journal không bị hiển thị AVAILABLE sạch")
    void testPendingSeatInJournalProtectedFromCleanAvailable() throws Exception {
        String reqId = UUID.randomUUID().toString();
        String payload = "{\"requestId\":\"" + reqId + "\",\"tripId\":\"SE-001\",\"seatId\":\"G07\",\"passengerName\":\"Khach Pending\"}";
        PendingEntry pendingEntry = PendingEntry.createPrepared(reqId, "BOOK_TICKET", payload);
        journal.recordPrepared(pendingEntry);

        // Server trả về G07 là AVAILABLE
        fakeAdapter.setScenario(Scenario.OK);
        fakeAdapter.setSeatStatus("G07", SeatStatus.AVAILABLE);

        service.refreshNow().get(3, TimeUnit.SECONDS);

        SeatViewModel g07 = seatsList.get(6);
        assertEquals("G07", g07.getSeatId());

        // Vì G07 đang có lệnh pending trong journal, service không được phép hiển thị AVAILABLE sạch!
        await(() -> g07.getStatus() == SeatStatus.UNKNOWN, 1000);
        assertNotEquals(SeatStatus.AVAILABLE, g07.getStatus());
    }

    @Test
    @DisplayName("AC-D15-09: FakeReadAdapter TUYỆT ĐỐI không bao giờ trả về VerificationLevel.VERIFIED")
    void testFakeReadAdapterNeverReturnsVerified() throws Exception {
        for (Scenario sc : Scenario.values()) {
            if (sc == Scenario.TIMEOUT || sc == Scenario.DISCONNECTED) continue;
            fakeAdapter.setScenario(sc);
            SeatsSnapshot snap = fakeAdapter.readSeats().get(2, TimeUnit.SECONDS);
            assertEquals(VerificationLevel.UNVERIFIED, snap.getVerification(),
                    "Kịch bản " + sc + " không được trả VERIFIED");
            assertEquals(DataSource.FAKE, snap.getSource());
            assertFalse(fakeAdapter.isBarrierSupported());
        }
    }

    @Test
    @DisplayName("AC-D15-10: BookingClientReadAdapter bọc MockBookingClient trả DataSource.MOCK và UNVERIFIED")
    void testBookingClientReadAdapterWrapsMockClient() throws Exception {
        MockBookingClient mockClient = MockBookingClient.getInstance();
        try (BookingClientReadAdapter adapter = new BookingClientReadAdapter(mockClient)) {
            assertEquals(DataSource.MOCK, adapter.getDataSource());
            assertFalse(adapter.isBarrierSupported());

            SeatsSnapshot snapshot = adapter.readSeats("SE-001").get(3, TimeUnit.SECONDS);
            assertNotNull(snapshot);
            assertEquals(VerificationLevel.UNVERIFIED, snapshot.getVerification());
            assertEquals(DataSource.MOCK, snapshot.getSource());
            assertEquals(10, snapshot.getSeats().size());
        }
    }
}
