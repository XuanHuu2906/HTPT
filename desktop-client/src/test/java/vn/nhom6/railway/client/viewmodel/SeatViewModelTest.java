package vn.nhom6.railway.client.viewmodel;

import javafx.collections.ObservableList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.MockSeatSource;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenStateModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SeatViewModel & ScreenStateModel Tests (D04/D05)")
class SeatViewModelTest {

    @Test
    @DisplayName("SeatStatus - 4 trạng thái ghế có tên hiển thị và style class CSS chính xác")
    void testSeatStatusStyleClassesAndNames() {
        assertEquals("seat-available", SeatStatus.AVAILABLE.getStyleClass());
        assertEquals("Trống", SeatStatus.AVAILABLE.getDisplayName());
        assertEquals("✓", SeatStatus.AVAILABLE.getBadgeSymbol());

        assertEquals("seat-booked", SeatStatus.BOOKED.getStyleClass());
        assertEquals("Đã đặt", SeatStatus.BOOKED.getDisplayName());
        assertEquals("✗", SeatStatus.BOOKED.getBadgeSymbol());

        assertEquals("seat-selected", SeatStatus.SELECTED.getStyleClass());
        assertEquals("Đang chọn", SeatStatus.SELECTED.getDisplayName());
        assertEquals("★", SeatStatus.SELECTED.getBadgeSymbol());

        assertEquals("seat-unknown", SeatStatus.UNKNOWN.getStyleClass());
        assertEquals("Chưa rõ", SeatStatus.UNKNOWN.getDisplayName());
        assertEquals("⚠", SeatStatus.UNKNOWN.getBadgeSymbol());
    }

    @Test
    @DisplayName("SeatStatus - formatButtonText định dạng chuẩn mã ghế và nhãn phụ trợ accessibility")
    void testFormatButtonText() {
        String textAvailable = SeatStatus.AVAILABLE.formatButtonText("G01");
        assertTrue(textAvailable.contains("G01"));
        assertTrue(textAvailable.contains("✓"));
        assertTrue(textAvailable.contains("Trống"));

        String textUnknown = SeatStatus.UNKNOWN.formatButtonText("G04");
        assertTrue(textUnknown.contains("G04"));
        assertTrue(textUnknown.contains("⚠"));
        assertTrue(textUnknown.contains("Chưa rõ"));
    }

    @Test
    @DisplayName("SeatViewModel - Khởi tạo mặc định và khởi tạo với trạng thái tùy chọn")
    void testSeatViewModelInitialization() {
        SeatViewModel vmDefault = new SeatViewModel("G01");
        assertEquals("G01", vmDefault.getSeatId());
        assertEquals(SeatStatus.AVAILABLE, vmDefault.getStatus());
        assertEquals("seat-available", vmDefault.getStyleClass());
        assertTrue(vmDefault.isAvailable());
        assertFalse(vmDefault.isBooked());
        assertFalse(vmDefault.isSelected());
        assertFalse(vmDefault.isUnknown());

        SeatViewModel vmBooked = new SeatViewModel("G02", SeatStatus.BOOKED);
        assertEquals("G02", vmBooked.getSeatId());
        assertEquals(SeatStatus.BOOKED, vmBooked.getStatus());
        assertEquals("seat-booked", vmBooked.getStyleClass());
        assertTrue(vmBooked.isBooked());
        assertFalse(vmBooked.isAvailable());
    }

    @Test
    @DisplayName("SeatViewModel - Cập nhật trạng thái tự động đồng bộ displayText, booked, selected và styleClass properties")
    void testSeatViewModelPropertySynchronization() {
        SeatViewModel vm = new SeatViewModel("G03", SeatStatus.AVAILABLE);
        assertTrue(vm.isAvailable());
        assertFalse(vm.isBooked());
        assertFalse(vm.isSelected());
        assertTrue(vm.displayTextProperty().get().contains("Trống"));
        assertEquals("seat-available", vm.styleClassProperty().get());

        // Đổi sang SELECTED
        vm.setStatus(SeatStatus.SELECTED);
        assertTrue(vm.isSelected());
        assertFalse(vm.isAvailable());
        assertFalse(vm.isBooked());
        assertTrue(vm.displayTextProperty().get().contains("Đang chọn"));
        assertEquals("seat-selected", vm.styleClassProperty().get());

        // Đổi sang BOOKED
        vm.setStatus(SeatStatus.BOOKED);
        assertTrue(vm.isBooked());
        assertFalse(vm.isSelected());
        assertTrue(vm.displayTextProperty().get().contains("Đã đặt"));
        assertEquals("seat-booked", vm.styleClassProperty().get());

        // Đổi sang UNKNOWN
        vm.setStatus(SeatStatus.UNKNOWN);
        assertTrue(vm.isUnknown());
        assertFalse(vm.isBooked());
        assertTrue(vm.displayTextProperty().get().contains("Chưa rõ"));
        assertEquals("seat-unknown", vm.styleClassProperty().get());
    }

    @Test
    @DisplayName("MockSeatSource - Sinh 10 ghế MOCK chuẩn, G02/G04/G08 BOOKED, các ghế khác AVAILABLE")
    void testMockSeatSourceCreation() {
        ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();
        assertNotNull(seats);
        assertEquals(10, seats.size());

        for (int i = 1; i <= 10; i++) {
            SeatViewModel seat = seats.get(i - 1);
            String expectedId = String.format("G%02d", i);
            assertEquals(expectedId, seat.getSeatId());
            if (i == 2 || i == 4 || i == 8) {
                assertEquals(SeatStatus.BOOKED, seat.getStatus());
                assertTrue(seat.isBooked());
            } else {
                assertEquals(SeatStatus.AVAILABLE, seat.getStatus());
                assertTrue(seat.isAvailable());
            }
        }
    }

    @Test
    @DisplayName("MockSeatSource - ObservableList có extractor kích hoạt sự kiện update khi status ghế thay đổi")
    void testObservableListExtractorUpdates() {
        ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();
        AtomicInteger updateEventsCount = new AtomicInteger(0);

        seats.addListener((javafx.collections.ListChangeListener<SeatViewModel>) change -> {
            while (change.next()) {
                if (change.wasUpdated()) {
                    updateEventsCount.incrementAndGet();
                }
            }
        });

        // Đổi trạng thái G01 từ AVAILABLE sang SELECTED
        seats.get(0).setStatus(SeatStatus.SELECTED);
        assertEquals(1, updateEventsCount.get());

        // Đổi trạng thái G03 từ AVAILABLE sang BOOKED
        seats.get(2).setStatus(SeatStatus.BOOKED);
        assertEquals(2, updateEventsCount.get());
    }

    @Test
    @DisplayName("MockSeatSource.applySnapshot - Cập nhật trạng thái ghế mà không tạo lại đối tượng")
    void testApplySnapshotPreservesInstancesAndUpdatesState() {
        ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();
        SeatViewModel seatG01 = seats.get(0);
        SeatViewModel seatG03 = seats.get(2);
        SeatViewModel seatG04 = seats.get(3);

        assertEquals("G01", seatG01.getSeatId());
        assertEquals(SeatStatus.AVAILABLE, seatG01.getStatus());
        assertEquals("G03", seatG03.getSeatId());
        assertEquals(SeatStatus.AVAILABLE, seatG03.getStatus());
        assertEquals("G04", seatG04.getSeatId());
        assertEquals(SeatStatus.BOOKED, seatG04.getStatus());

        // Snapshot thay đổi: G03 AVAILABLE -> BOOKED, G04 BOOKED -> AVAILABLE
        java.util.Map<String, SeatStatus> snapshot = new java.util.HashMap<>();
        snapshot.put("G03", SeatStatus.BOOKED);
        snapshot.put("G04", SeatStatus.AVAILABLE);

        SeatViewModel.applySnapshot(seats, snapshot);

        // Đảm bảo đối tượng trong list giữ nguyên tham chiếu (không tạo mới đối tượng)
        assertSame(seatG01, seats.get(0));
        assertSame(seatG03, seats.get(2));
        assertSame(seatG04, seats.get(3));

        // Trạng thái ghế được cập nhật chính xác
        assertEquals(SeatStatus.AVAILABLE, seatG01.getStatus());
        assertEquals(SeatStatus.BOOKED, seatG03.getStatus());
        assertTrue(seatG03.isBooked());
        assertEquals(SeatStatus.AVAILABLE, seatG04.getStatus());
        assertTrue(seatG04.isAvailable());
    }

    @Test
    @DisplayName("MockSeatSource - createRefreshedMockSnapshot sinh snapshot hợp lệ và cập nhật được qua applySnapshot")
    void testRefreshedMockSnapshot() {
        ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();
        java.util.Map<String, SeatStatus> refreshedSnapshot = MockSeatSource.createRefreshedMockSnapshot();

        assertNotNull(refreshedSnapshot);
        assertEquals(10, refreshedSnapshot.size());
        assertEquals(SeatStatus.BOOKED, refreshedSnapshot.get("G01"));
        assertEquals(SeatStatus.AVAILABLE, refreshedSnapshot.get("G04"));
        assertEquals(SeatStatus.UNKNOWN, refreshedSnapshot.get("G09"));

        SeatViewModel.applySnapshot(seats, refreshedSnapshot);

        assertEquals(SeatStatus.BOOKED, seats.get(0).getStatus()); // G01
        assertEquals(SeatStatus.AVAILABLE, seats.get(3).getStatus()); // G04
        assertEquals(SeatStatus.UNKNOWN, seats.get(8).getStatus()); // G09
    }

    @Test
    @DisplayName("ScreenState - 4 trạng thái màn hình có style class CSS và thông điệp chuẩn")
    void testScreenStateEnums() {
        assertEquals("status-idle", ScreenState.IDLE.getStyleClass());
        assertNotNull(ScreenState.IDLE.getDefaultMessage());

        assertEquals("status-loading", ScreenState.LOADING.getStyleClass());
        assertNotNull(ScreenState.LOADING.getDefaultMessage());

        assertEquals("status-error", ScreenState.ERROR.getStyleClass());
        assertNotNull(ScreenState.ERROR.getDefaultMessage());

        assertEquals("status-disconnected", ScreenState.DISCONNECTED.getStyleClass());
        assertNotNull(ScreenState.DISCONNECTED.getDefaultMessage());
    }

    @Test
    @DisplayName("ScreenStateModel - Chuyển trạng thái và liên kết dữ liệu message")
    void testScreenStateModelTransitions() {
        ScreenStateModel model = new ScreenStateModel();

        // Ban đầu là IDLE
        assertEquals(ScreenState.IDLE, model.getState());
        assertEquals(ScreenState.IDLE.getDefaultMessage(), model.getMessage());
        assertEquals("status-idle", model.getStyleClass());

        // Đổi sang LOADING
        model.setState(ScreenState.LOADING);
        assertEquals(ScreenState.LOADING, model.getState());
        assertEquals(ScreenState.LOADING.getDefaultMessage(), model.getMessage());
        assertEquals("status-loading", model.getStyleClass());

        // Đổi sang ERROR kèm custom message
        model.setState(ScreenState.ERROR, "Lỗi kết nối cụm Raft Node-02");
        assertEquals(ScreenState.ERROR, model.getState());
        assertEquals("Lỗi kết nối cụm Raft Node-02", model.getMessage());

        // Đổi sang DISCONNECTED
        model.setState(ScreenState.DISCONNECTED);
        assertEquals(ScreenState.DISCONNECTED, model.getState());
        assertEquals("status-disconnected", model.getStyleClass());
    }

    @Test
    @DisplayName("Kiểm tra kịch bản Reset gốc: chọn G06, G07 rồi bấm Reset gốc")
    void testResetDefaultScenario() {
        ObservableList<SeatViewModel> seats = MockSeatSource.createMockSeats();
        javafx.collections.transformation.FilteredList<SeatViewModel> selectedSeatsList = new javafx.collections.transformation.FilteredList<>(
                seats, SeatViewModel::isSelected);

        javafx.beans.binding.StringBinding binding = javafx.beans.binding.Bindings.createStringBinding(() -> {
            if (selectedSeatsList.isEmpty()) {
                return "Ghế đang chọn: (Chưa chọn ghế nào)";
            }
            java.util.List<String> sortedIds = selectedSeatsList.stream()
                    .map(SeatViewModel::getSeatId)
                    .sorted()
                    .toList();
            return "Ghế đang chọn: " + String.join(", ", sortedIds) + " (" + sortedIds.size() + " ghế)";
        }, selectedSeatsList);

        javafx.beans.binding.BooleanBinding disableBinding = javafx.beans.binding.Bindings.isEmpty(selectedSeatsList);

        // Ban đầu
        assertEquals("Ghế đang chọn: (Chưa chọn ghế nào)", binding.get());
        assertTrue(disableBinding.get());

        // 1. Người dùng click G06, G07
        SeatViewModel g06 = seats.get(5);
        SeatViewModel g07 = seats.get(6);
        g06.setStatus(SeatStatus.SELECTED);
        g07.setStatus(SeatStatus.SELECTED);

        assertEquals(2, selectedSeatsList.size());
        assertEquals("Ghế đang chọn: G06, G07 (2 ghế)", binding.get());
        assertFalse(disableBinding.get());

        // 2. Người dùng nhấn nút "Reset gốc" (chạy onMockResetDefault)
        // clearSelection():
        for (SeatViewModel vm : seats) {
            if (vm.isSelected()) {
                vm.setStatus(SeatStatus.AVAILABLE);
            }
        }
        // applySnapshot:
        SeatViewModel.applySnapshot(seats, MockSeatSource.createInitialSnapshot());

        assertEquals(0, selectedSeatsList.size());
        assertEquals("Ghế đang chọn: (Chưa chọn ghế nào)", binding.get());
        assertTrue(disableBinding.get());
    }
}
