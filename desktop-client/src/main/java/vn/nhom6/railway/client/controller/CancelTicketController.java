package vn.nhom6.railway.client.controller;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.network.MockBookingClient;
import vn.nhom6.railway.client.network.MockScenario;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

/**
 * Controller cho hộp thoại xác nhận hủy vé mô phỏng (TASK D06).
 * <p>
 * Tính năng chính:
 * - Hiển thị chi tiết vé cần hủy và cảnh báo MOCK rõ ràng (không ghi DB).
 * - Yêu cầu xác nhận (Checkbox) trước khi nút "Xác nhận hủy vé" được bật.
 * - Hỗ trợ bộ điều khiển MOCK giả lập 3 kết quả phản hồi:
 *   + CANCELLED_SUCCESS: Hủy thành công, cập nhật vé sang CANCELLED trong model.
 *   + REJECTED_NOT_CANCELLABLE: Bị từ chối (NOT_CANCELLABLE), vé giữ nguyên CONFIRMED.
 *   + UNKNOWN_TIMEOUT: Mất kết nối/timeout, vé giữ nguyên CONFIRMED (không tự đổi sang CANCELLED theo SPEC-06).
 */
public class CancelTicketController {

    public enum CancelMockOutcome {
        CANCELLED_SUCCESS("✓ CANCELLED — Thành công (Vé chuyển sang ĐÃ HỦY)"),
        REJECTED_NOT_CANCELLABLE("⛔ REJECTED — Bị từ chối (Mã: NOT_CANCELLABLE - Quá hạn hủy)"),
        UNKNOWN_TIMEOUT("⚠ UNKNOWN — Timeout / Mất kết nối (Chưa rõ kết quả, vé GIỮ NGUYÊN)");

        private final String label;

        CancelMockOutcome(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    @FXML
    private Label ticketIdLabel;

    @FXML
    private Label requestIdLabel;

    @FXML
    private Label tripLabel;

    @FXML
    private Label seatIdLabel;

    @FXML
    private Label passengerLabel;

    @FXML
    private Label refundPriceLabel;

    @FXML
    private ComboBox<CancelMockOutcome> mockOutcomeCombo;

    @FXML
    private CheckBox confirmCheckBox;

    @FXML
    private VBox resultBox;

    @FXML
    private ProgressIndicator resultProgress;

    @FXML
    private Label resultTitleLabel;

    @FXML
    private Label resultDetailLabel;

    @FXML
    private Button closeBtn;

    @FXML
    private Button confirmCancelBtn;

    private Stage dialogStage;
    private TicketViewModel ticket;

    @FXML
    public void initialize() {
        if (mockOutcomeCombo != null) {
            mockOutcomeCombo.getItems().setAll(CancelMockOutcome.values());
            mockOutcomeCombo.setValue(CancelMockOutcome.CANCELLED_SUCCESS);
        }

        if (confirmCancelBtn != null && confirmCheckBox != null) {
            confirmCancelBtn.disableProperty().bind(confirmCheckBox.selectedProperty().not());
        }
    }

    public void setDialogStage(Stage dialogStage) {
        this.dialogStage = dialogStage;
    }

    public void setTicket(TicketViewModel ticket) {
        this.ticket = ticket;
        if (ticket == null) {
            return;
        }

        if (ticketIdLabel != null) ticketIdLabel.setText(ticket.getTicketId());
        if (requestIdLabel != null) requestIdLabel.setText(ticket.getRequestId());
        if (tripLabel != null) tripLabel.setText(ticket.getTripCode() + " · Toa 01 VIP");
        if (seatIdLabel != null) seatIdLabel.setText(ticket.getSeatId());
        if (passengerLabel != null) passengerLabel.setText(ticket.getPassengerName());
        if (refundPriceLabel != null) refundPriceLabel.setText(ticket.getPriceFormatted() + " (Hoàn 100% theo chính sách)");
    }

    @FXML
    public void onConfirmCancelClicked() {
        if (ticket == null) {
            return;
        }

        // Khóa các điều khiển trong khi giả lập xử lý
        confirmCancelBtn.disableProperty().unbind();
        confirmCancelBtn.setDisable(true);
        confirmCheckBox.setDisable(true);
        if (mockOutcomeCombo != null) {
            mockOutcomeCombo.setDisable(true);
        }

        // Hiển thị trạng thái đang xử lý
        if (resultBox != null) {
            resultBox.setVisible(true);
            resultBox.setManaged(true);
            setBoxStyle(resultBox, "cancel-box-info");
        }
        if (resultProgress != null) {
            resultProgress.setVisible(true);
            resultProgress.setManaged(true);
        }
        if (resultTitleLabel != null) {
            resultTitleLabel.setText("Đang gửi yêu cầu hủy vé tới cụm máy chủ (Mô phỏng)...");
            setLabelStyle(resultTitleLabel, "cancel-title-info");
        }
        if (resultDetailLabel != null) {
            resultDetailLabel.setText("Đang chờ phản hồi...");
            setLabelStyle(resultDetailLabel, "cancel-detail-info");
        }

        if (dialogStage != null) {
            dialogStage.sizeToScene();
        }

        CancelMockOutcome outcome = (mockOutcomeCombo != null && mockOutcomeCombo.getValue() != null)
                ? mockOutcomeCombo.getValue()
                : CancelMockOutcome.CANCELLED_SUCCESS;

        // Cấu hình kịch bản tiếp theo cho MockBookingClient
        switch (outcome) {
            case CANCELLED_SUCCESS -> MockBookingClient.getInstance().setNextScenario(MockScenario.SUCCESS);
            case REJECTED_NOT_CANCELLABLE -> MockBookingClient.getInstance().setNextScenario(MockScenario.SEAT_ALREADY_BOOKED);
            case UNKNOWN_TIMEOUT -> MockBookingClient.getInstance().setNextScenario(MockScenario.UNKNOWN);
        }

        // Gọi bất đồng bộ qua BookingViewModel (TASK D07 - Không treo UI)
        ClientFactory.getBookingViewModel().cancel(ticket.getTicketId())
                .whenComplete((res, ex) -> Platform.runLater(() -> {
                    if (ex != null) {
                        applyCancelOutcome(CancelMockOutcome.UNKNOWN_TIMEOUT);
                    } else if (res.isConfirmed()) {
                        applyCancelOutcome(CancelMockOutcome.CANCELLED_SUCCESS);
                    } else if (res.isRejected()) {
                        applyCancelOutcome(CancelMockOutcome.REJECTED_NOT_CANCELLABLE);
                    } else {
                        applyCancelOutcome(CancelMockOutcome.UNKNOWN_TIMEOUT);
                    }
                }));
    }

    public void applyCancelOutcome(CancelMockOutcome outcome) {
        if (resultProgress != null) {
            resultProgress.setVisible(false);
            resultProgress.setManaged(false);
        }

        switch (outcome) {
            case CANCELLED_SUCCESS -> {
                // Hủy thành công: cập nhật model sang CANCELLED, UI tự động đồng bộ qua binding
                ticket.setStatus(TicketStatus.CANCELLED);
                ticket.setReasonCode("HỦY BỞI KHÁCH HÀNG (MOCK)");

                setBoxStyle(resultBox, "cancel-box-success");
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText("✓ HỦY VÉ THÀNH CÔNG (MOCK D08)!");
                    setLabelStyle(resultTitleLabel, "cancel-title-success");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText("Vé " + ticket.getTicketId() + " (Ghế " + ticket.getSeatId() + ") đã chuyển sang trạng thái ĐÃ HỦY. Ghế được giải phóng trên sơ đồ (MOCK).");
                    setLabelStyle(resultDetailLabel, "cancel-detail-success");
                }
            }
            case REJECTED_NOT_CANCELLABLE -> {
                // Từ chối hủy: vé giữ nguyên CONFIRMED
                ErrorPresentation p = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TICKET_NOT_ACTIVE, true);
                setBoxStyle(resultBox, "cancel-box-rejected");
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText("⛔ TỪ CHỐI HỦY VÉ (REJECTED: " + p.rawCode() + ")!");
                    setLabelStyle(resultTitleLabel, "cancel-title-rejected");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText("Lý do: " + p.message() + " Trạng thái vé giữ nguyên ĐÃ XÁC NHẬN.");
                    setLabelStyle(resultDetailLabel, "cancel-detail-rejected");
                }
            }
            case UNKNOWN_TIMEOUT -> {
                // SPEC-06: UNKNOWN/timeout không được hiển thị như đã hủy thành công và không được tự đổi trạng thái vé thành CANCELLED
                ErrorPresentation p = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, true);
                setBoxStyle(resultBox, "cancel-box-unknown");
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText("⚠ " + p.title().toUpperCase() + "!");
                    setLabelStyle(resultTitleLabel, "cancel-title-unknown");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText(p.message() + " Vui lòng tra cứu lại theo mã Request ID!");
                    setLabelStyle(resultDetailLabel, "cancel-detail-unknown");
                }
            }
        }

        if (closeBtn != null) {
            closeBtn.setText("Đóng");
            closeBtn.getStyleClass().clear();
            closeBtn.getStyleClass().add("btn-primary");
        }

        if (dialogStage != null) {
            dialogStage.sizeToScene();
        }
    }

    private void setBoxStyle(VBox box, String styleClass) {
        if (box != null) {
            box.setStyle(null);
            box.getStyleClass().removeAll("cancel-box-info", "cancel-box-success", "cancel-box-rejected", "cancel-box-unknown");
            box.getStyleClass().add(styleClass);
        }
    }

    private void setLabelStyle(Label label, String styleClass) {
        if (label != null) {
            label.setStyle(null);
            label.getStyleClass().removeAll(
                    "cancel-title-info", "cancel-title-success", "cancel-title-rejected", "cancel-title-unknown",
                    "cancel-detail-info", "cancel-detail-success", "cancel-detail-rejected", "cancel-detail-unknown"
            );
            label.getStyleClass().add(styleClass);
        }
    }

    @FXML
    public void onCloseClicked() {
        if (dialogStage != null) {
            dialogStage.close();
        }
    }

    public Stage getDialogStage() {
        return dialogStage;
    }

    public TicketViewModel getTicket() {
        return ticket;
    }

    public ComboBox<CancelMockOutcome> getMockOutcomeCombo() {
        return mockOutcomeCombo;
    }

    public CheckBox getConfirmCheckBox() {
        return confirmCheckBox;
    }

    public Button getConfirmCancelBtn() {
        return confirmCancelBtn;
    }
}
