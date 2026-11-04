package vn.nhom6.railway.client.viewmodel;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenState;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.ScreenStateModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SeatViewModel & ScreenStateModel Tests (D04)")
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

        SeatViewModel vmBooked = new SeatViewModel("G02", SeatStatus.BOOKED);
        assertEquals("G02", vmBooked.getSeatId());
        assertEquals(SeatStatus.BOOKED, vmBooked.getStatus());
        assertEquals("seat-booked", vmBooked.getStyleClass());
    }

    @Test
    @DisplayName("SeatViewModel - Cập nhật trạng thái kích hoạt property listener phản ứng tức thì")
    void testSeatViewModelPropertyListener() {
        SeatViewModel vm = new SeatViewModel("G03", SeatStatus.AVAILABLE);

        AtomicReference<SeatStatus> observedNewStatus = new AtomicReference<>();
        AtomicBoolean listenerFired = new AtomicBoolean(false);

        vm.statusProperty().addListener((obs, oldVal, newVal) -> {
            listenerFired.set(true);
            observedNewStatus.set(newVal);
        });

        vm.setStatus(SeatStatus.SELECTED);
        assertTrue(listenerFired.get());
        assertEquals(SeatStatus.SELECTED, observedNewStatus.get());
        assertEquals(SeatStatus.SELECTED, vm.getStatus());
        assertEquals("seat-selected", vm.getStyleClass());

        // Chuyển sang UNKNOWN
        listenerFired.set(false);
        vm.setStatus(SeatStatus.UNKNOWN);
        assertTrue(listenerFired.get());
        assertEquals(SeatStatus.UNKNOWN, observedNewStatus.get());
        assertEquals("seat-unknown", vm.getStyleClass());
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
}

