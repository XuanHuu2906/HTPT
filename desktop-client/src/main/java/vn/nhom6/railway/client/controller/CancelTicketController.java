package vn.nhom6.railway.client.controller;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import vn.nhom6.railway.client.ClientFactory;
import vn.nhom6.railway.client.network.ClientConfig;
import vn.nhom6.railway.client.network.DevSupportBridge;
import vn.nhom6.railway.client.network.FailedNotSentException;
import vn.nhom6.railway.client.viewmodel.TicketViewModel;
import vn.nhom6.railway.client.viewmodel.TicketViewModel.TicketStatus;

import java.util.concurrent.CompletionException;

/**
 * Controller cho hộp thoại xác nhận hủy vé (TASK D06..D28).
 * <p>
 * Bất biến chế độ RELEASE / SOCKET:
 * <ul>
 *   <li>Không chứa bất kỳ import hay tham chiếu biên dịch nào tới các lớp giả lập.</li>
 *   <li>Gỡ hoàn toàn khung kịch bản kiểm thử cục bộ ({@code cancelMockPanel}) khỏi cây giao diện khi chạy Socket/Release.</li>
 *   <li>Chỉ đổi trạng thái vé sang {@code CANCELLED} khi nhận phản hồi cấu trúc {@code CONFIRMED} từ máy chủ;
 *       timeout/mất kết nối giữ nguyên vé {@code CONFIRMED} và báo chưa rõ kết quả mà không chứa từ cấm.</li>
 * </ul>
 */
public class CancelTicketController {

    public enum CancelOutcomeOption {
        CANCELLED_SUCCESS("✓ CANCELLED — Thành công (Vé chuyển sang ĐÃ HỦY)"),
        REJECTED_TICKET_NOT_ACTIVE("⛔ REJECTED — Bị từ chối (Mã: TICKET_NOT_ACTIVE - Vé không còn hiệu lực)"),
        UNKNOWN_TIMEOUT("⚠ UNKNOWN — Timeout / Mất kết nối (Chưa rõ kết quả, vé GIỮ NGUYÊN)");

        private final String label;

        CancelOutcomeOption(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    @FXML
    private Label cancelModeBadgeLabel;

    @FXML
    private VBox cancelMockPanel;

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
    private ComboBox<CancelOutcomeOption> mockOutcomeCombo;

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
    private volatile String lastResultTitle = "";
    private volatile String lastResultDetail = "";

    @FXML
    public void initialize() {
        ClientConfig cfg = ClientConfig.loadDefault();
        boolean realFlow = cfg.isReleaseMode() || cfg.isSocketMode();

        if (realFlow) {
            removeNodeFromParent(cancelMockPanel);
            if (cancelModeBadgeLabel != null) {
                cancelModeBadgeLabel.getStyleClass().removeAll("badge-cluster-mode", "badge-release-mode", "badge-mock");
                if (cfg.isReleaseMode()) {
                    cancelModeBadgeLabel.setText("RELEASE");
                    cancelModeBadgeLabel.getStyleClass().add("badge-release-mode");
                } else {
                    cancelModeBadgeLabel.setText("SE-001");
                    cancelModeBadgeLabel.getStyleClass().add("badge-cluster-mode");
                }
            }
        } else if (mockOutcomeCombo != null) {
            mockOutcomeCombo.getItems().setAll(CancelOutcomeOption.values());
            mockOutcomeCombo.setValue(CancelOutcomeOption.CANCELLED_SUCCESS);
        }

        if (confirmCancelBtn != null && confirmCheckBox != null) {
            confirmCancelBtn.disableProperty().bind(confirmCheckBox.selectedProperty().not());
        }
    }

    private static void removeNodeFromParent(Node node) {
        if (node != null && node.getParent() instanceof Pane parentPane) {
            parentPane.getChildren().remove(node);
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

        if (confirmCancelBtn != null) {
            confirmCancelBtn.disableProperty().unbind();
            confirmCancelBtn.setDisable(true);
        }
        if (confirmCheckBox != null) {
            confirmCheckBox.setDisable(true);
        }
        if (mockOutcomeCombo != null) {
            mockOutcomeCombo.setDisable(true);
        }

        boolean socketOrRelease = ClientConfig.isReleaseModeActive();
        if (!socketOrRelease) {
            try {
                ClientConfig cfg = ClientConfig.loadDefault();
                socketOrRelease = (cfg.isReleaseMode() || cfg.isSocketMode())
                        && !ClientFactory.getBookingViewModel().isMock();
            } catch (Exception ignored) {
                socketOrRelease = true;
            }
        }
        final boolean isRealSocket = socketOrRelease;

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
            resultTitleLabel.setText(isRealSocket
                    ? "Đang gửi yêu cầu hủy vé tới cụm máy chủ..."
                    : "Đang gửi yêu cầu hủy vé cục bộ...");
            setLabelStyle(resultTitleLabel, "cancel-title-info");
        }
        if (resultDetailLabel != null) {
            resultDetailLabel.setText("Đang chờ phản hồi...");
            setLabelStyle(resultDetailLabel, "cancel-detail-info");
        }

        if (dialogStage != null) {
            dialogStage.sizeToScene();
        }

        if (!isRealSocket && !ClientConfig.isReleaseModeActive()) {
            CancelOutcomeOption outcome = (mockOutcomeCombo != null && mockOutcomeCombo.getValue() != null)
                    ? mockOutcomeCombo.getValue()
                    : CancelOutcomeOption.CANCELLED_SUCCESS;
            DevSupportBridge.get().configureNextDevCancelOutcome(outcome.name());
        }

        ClientFactory.getBookingViewModel().cancel(ticket.getTicketId())
                .whenComplete((res, ex) -> runOnFxOrInline(() -> {
                    if (ex != null) {
                        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                                ? ex.getCause()
                                : ex;
                        if (cause instanceof IllegalStateException
                                || cause instanceof IllegalArgumentException
                                || cause instanceof FailedNotSentException) {
                            applyPreSendCancelError(cause);
                        } else {
                            applyCancelOutcome(CancelOutcomeOption.UNKNOWN_TIMEOUT, isRealSocket);
                        }
                    } else if (res.isConfirmed()) {
                        applyCancelOutcome(CancelOutcomeOption.CANCELLED_SUCCESS, isRealSocket);
                    } else if (res.isRejected()) {
                        applyCancelOutcome(CancelOutcomeOption.REJECTED_TICKET_NOT_ACTIVE, isRealSocket);
                    } else {
                        applyCancelOutcome(CancelOutcomeOption.UNKNOWN_TIMEOUT, isRealSocket);
                    }
                }));
    }

    private static void runOnFxOrInline(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
        } else {
            try {
                Platform.runLater(action);
            } catch (IllegalStateException e) {
                action.run();
            }
        }
    }

    private void applyPreSendCancelError(Throwable cause) {
        if (resultProgress != null) {
            resultProgress.setVisible(false);
            resultProgress.setManaged(false);
        }
        setBoxStyle(resultBox, "cancel-box-rejected");
        String title = "⛔ KHÔNG THỂ GỬI YÊU CẦU HỦY VÉ";
        this.lastResultTitle = title;
        if (resultTitleLabel != null) {
            resultTitleLabel.setText(title);
            setLabelStyle(resultTitleLabel, "cancel-title-rejected");
        }
        String msg = (cause != null && cause.getMessage() != null && !cause.getMessage().isBlank())
                ? cause.getMessage()
                : "Yêu cầu chưa được gửi đi do điều kiện tiền kiểm tra không đạt.";
        this.lastResultDetail = msg;
        if (resultDetailLabel != null) {
            resultDetailLabel.setText(msg);
            setLabelStyle(resultDetailLabel, "cancel-detail-rejected");
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

    public void applyCancelOutcome(CancelOutcomeOption outcome) {
        boolean socketOrRelease = ClientConfig.isReleaseModeActive();
        if (!socketOrRelease) {
            try {
                ClientConfig cfg = ClientConfig.loadDefault();
                socketOrRelease = (cfg.isReleaseMode() || cfg.isSocketMode())
                        && !ClientFactory.getBookingViewModel().isMock();
            } catch (Exception ignored) {
                socketOrRelease = true;
            }
        }
        applyCancelOutcome(outcome, socketOrRelease);
    }

    private void applyCancelOutcome(CancelOutcomeOption outcome, boolean isRealSocket) {
        if (resultProgress != null) {
            resultProgress.setVisible(false);
            resultProgress.setManaged(false);
        }

        switch (outcome) {
            case CANCELLED_SUCCESS -> {
                ticket.setStatus(TicketStatus.CANCELLED);
                ticket.setReasonCode("HỦY ĐÃ XÁC NHẬN");

                setBoxStyle(resultBox, "cancel-box-success");
                String t = "✓ HỦY VÉ THÀNH CÔNG!";
                String d = "Vé " + ticket.getTicketId() + " (Ghế " + ticket.getSeatId()
                        + ") đã chuyển sang trạng thái ĐÃ HỦY. Ghế đã được giải phóng trên sơ đồ.";
                this.lastResultTitle = t;
                this.lastResultDetail = d;
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText(t);
                    setLabelStyle(resultTitleLabel, "cancel-title-success");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText(d);
                    setLabelStyle(resultDetailLabel, "cancel-detail-success");
                }
            }
            case REJECTED_TICKET_NOT_ACTIVE -> {
                ErrorPresentation p = ErrorPresentationMapper.mapCode(
                        ErrorPresentationMapper.CODE_TICKET_NOT_ACTIVE, !isRealSocket);
                setBoxStyle(resultBox, "cancel-box-rejected");
                String t = "⛔ TỪ CHỐI HỦY VÉ (REJECTED: " + p.rawCode() + ")!";
                String d = "Lý do: " + p.message() + " Trạng thái vé giữ nguyên ĐÃ XÁC NHẬN.";
                this.lastResultTitle = t;
                this.lastResultDetail = d;
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText(t);
                    setLabelStyle(resultTitleLabel, "cancel-title-rejected");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText(d);
                    setLabelStyle(resultDetailLabel, "cancel-detail-rejected");
                }
            }
            case UNKNOWN_TIMEOUT -> {
                ErrorPresentation p = ErrorPresentationMapper.mapCode(
                        ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, !isRealSocket);
                setBoxStyle(resultBox, "cancel-box-unknown");
                String t = "⚠ " + p.title().toUpperCase() + "!";
                String d = p.message() + " Vui lòng tra cứu lại theo mã Request ID!";
                this.lastResultTitle = t;
                this.lastResultDetail = d;
                if (resultTitleLabel != null) {
                    resultTitleLabel.setText(t);
                    setLabelStyle(resultTitleLabel, "cancel-title-unknown");
                }
                if (resultDetailLabel != null) {
                    resultDetailLabel.setText(d);
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

    public ComboBox<CancelOutcomeOption> getMockOutcomeCombo() {
        return mockOutcomeCombo;
    }

    public CheckBox getConfirmCheckBox() {
        return confirmCheckBox;
    }

    public Button getConfirmCancelBtn() {
        return confirmCancelBtn;
    }

    public String getLastResultTitle() {
        return lastResultTitle;
    }

    public String getLastResultDetail() {
        return lastResultDetail;
    }
}
