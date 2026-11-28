package vn.nhom6.railway.client.dev;

import javafx.collections.ObservableList;
import javafx.geometry.Pos;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import vn.nhom6.railway.client.network.BookingClient;
import vn.nhom6.railway.client.network.ClusterStatusClient;
import vn.nhom6.railway.client.network.DevSupportSpi;
import vn.nhom6.railway.client.network.FakeReadAdapter;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockClusterStatusClient;
import vn.nhom6.railway.client.network.MockScenario;
import vn.nhom6.railway.client.network.ReadAdapter;
import vn.nhom6.railway.client.viewmodel.SeatViewModel;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.MockSeatSource;
import vn.nhom6.railway.client.viewmodel.SeatViewModel.SeatStatus;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.MockTicketSource;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Cài đặt dành riêng cho chế độ phát triển / kiểm thử cục bộ ({@code release.mode=false}).
 * <p>
 * Gói {@code vn.nhom6.railway.client.dev} này bị loại hoàn toàn khỏi artifact JAR
 * khi đóng gói với Maven profile {@code -Prelease} (TASK TV1-D28).
 */
public final class DevModeSupport implements DevSupportSpi {

    @Override
    public BookingClient createDevBookingClient() {
        return MockBookingClient.getInstance();
    }

    @Override
    public ClusterStatusClient createDevClusterStatusClient() {
        return MockClusterStatusClient.getInstance();
    }

    @Override
    public ReadAdapter createDevReadAdapter() {
        return new FakeReadAdapter();
    }

    @Override
    public boolean isDevMockBookingClient(BookingClient client) {
        return client instanceof MockBookingClient;
    }

    @Override
    public String getMockSeatBadgeLabel() {
        return MockSeatSource.MOCK_LABEL;
    }

    @Override
    public String getMockTicketNoticeLabel() {
        return MockTicketSource.MOCK_NOTICE;
    }

    @Override
    public ObservableList<SeatViewModel> createDevMockSeats() {
        return MockSeatSource.createMockSeats();
    }

    @Override
    public Map<String, SeatStatus> createInitialMockSnapshot() {
        return MockSeatSource.createInitialSnapshot();
    }

    @Override
    public Map<String, SeatStatus> createRefreshedMockSnapshot() {
        return MockSeatSource.createRefreshedMockSnapshot();
    }

    @Override
    public ObservableList<TicketViewModel> getSharedDevTickets() {
        return MockTicketSource.getSharedTickets();
    }

    @Override
    public void addDevTicket(TicketViewModel ticket) {
        MockTicketSource.addTicket(ticket);
    }

    @Override
    public void syncDevSeatStatusChange(String seatId, SeatStatus status) {
        MockTicketSource.syncSeatStatusChange(seatId, status);
    }

    @Override
    public void syncDevSnapshot(Map<String, SeatStatus> snapshot) {
        MockTicketSource.syncSnapshot(snapshot);
    }

    @Override
    public void resetDevTicketsToDefault() {
        MockTicketSource.resetToDefault();
    }

    @Override
    public void attachDevScenarioRow(VBox mockPanel, Consumer<String> statusUpdater) {
        if (mockPanel == null) {
            return;
        }
        HBox d07Row = new HBox(8);
        d07Row.setAlignment(Pos.CENTER_LEFT);
        d07Row.getStyleClass().add("mock-d07-row");

        Label scenarioLabel = new Label("Kịch bản MOCK:");
        scenarioLabel.getStyleClass().add("mock-control-label");

        ComboBox<MockScenario> scenarioCombo = new ComboBox<>();
        scenarioCombo.getItems().setAll(MockScenario.values());
        scenarioCombo.setValue(MockBookingClient.getInstance().getScenario());
        scenarioCombo.getStyleClass().add("mock-combobox");
        scenarioCombo.setPrefWidth(210);
        scenarioCombo.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                MockBookingClient.getInstance().setScenario(newVal);
                if (statusUpdater != null) {
                    statusUpdater.accept("MOCK: Đã chọn kịch bản " + newVal.name() + " cho lần đặt vé tiếp theo.");
                }
            }
        });

        Label delayLabel = new Label("Delay (ms):");
        delayLabel.getStyleClass().add("mock-control-label");

        TextField delayField = new TextField(String.valueOf(MockBookingClient.getInstance().getDelayMs()));
        delayField.setPrefWidth(65);
        delayField.getStyleClass().add("mock-combobox");
        delayField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                try {
                    long ms = Long.parseLong(newVal.trim());
                    MockBookingClient.getInstance().setDelayMs(ms);
                } catch (NumberFormatException ignored) {
                }
            }
        });

        Label devBadge = new Label("MOCK DEV");
        devBadge.getStyleClass().add("badge-mock");

        d07Row.getChildren().addAll(scenarioLabel, scenarioCombo, delayLabel, delayField, devBadge);
        mockPanel.getChildren().add(d07Row);
    }

    @Override
    public void configureNextDevCancelOutcome(String outcomeKind) {
        if ("CANCELLED_SUCCESS".equals(outcomeKind)) {
            MockBookingClient.getInstance().setNextScenario(MockScenario.SUCCESS);
        } else if ("REJECTED_TICKET_NOT_ACTIVE".equals(outcomeKind)) {
            MockBookingClient.getInstance().setNextScenario(MockScenario.SEAT_ALREADY_BOOKED);
        } else {
            MockBookingClient.getInstance().setNextScenario(MockScenario.UNKNOWN);
        }
    }
}
