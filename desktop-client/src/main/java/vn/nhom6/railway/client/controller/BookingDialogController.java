package vn.nhom6.railway.client.controller;

import javafx.animation.PauseTransition;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import javafx.util.Duration;
import vn.nhom6.railway.client.BookingFormValidator;
import vn.nhom6.railway.client.BookingOutcome;
import vn.nhom6.railway.client.MockBookingService;
import vn.nhom6.railway.client.MockBookingServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Controller điều khiển hộp thoại Xác nhận đặt vé (TASK D03/D04).
 * <p>
 * Tính năng:
 * - Tiếp nhận danh sách ghế đã chọn từ sơ đồ ghế D02/D04.
 * - Lắng nghe thay đổi trên ô nhập họ tên, kiểm tra tính hợp lệ và cập nhật bộ đếm ký tự.
 * - Gọi {@link MockBookingService} chạy ngầm qua {@link Task} để không khóa UI thread.
 * - Chặn bấm xác nhận nhiều lần khi đang xử lý.
 * - Cung cấp {@link #getOutcome()} để D02/D04 lấy kết quả sau khi đóng hộp thoại.
 * - Định dạng giao diện hoàn toàn qua CSS classes (railway.css), không hardcode màu trong Java.
 */
public class BookingDialogController {

    @FXML
    private Label selectedSeatsSummaryLabel;

    @FXML
    private TextField passengerNameField;

    @FXML
    private Label charCountLabel;

    @FXML
    private Label errorLabel;

    @FXML
    private Button cancelBtn;

    @FXML
    private Button confirmBtn;

    @FXML
    private Label statusLabel;

    private MockBookingService bookingService = new MockBookingServiceImpl();
    private List<String> selectedSeats = new ArrayList<>();
    private BookingOutcome outcome = null;
    private Stage dialogStage;
    private boolean isSubmitting = false;

    @FXML
    public void initialize() {
        if (errorLabel != null) {
            errorLabel.setText("");
            errorLabel.setVisible(false);
            errorLabel.setManaged(false);
        }

        if (charCountLabel != null) {
            charCountLabel.setText("0/" + BookingFormValidator.NAME_MAX_LENGTH);
            setCharCountStyleClass("char-count-normal");
        }

        if (confirmBtn != null) {
            confirmBtn.setDisable(true);
        }

        if (statusLabel != null) {
            statusLabel.setText("");
        }

        // Lắng nghe thay đổi trên ô nhập họ tên
        if (passengerNameField != null) {
            passengerNameField.textProperty().addListener((obs, oldVal, newVal) -> handleNameChanged(newVal));
        }
    }

    /**
     * Cung cấp MockBookingService tùy biến (phục vụ unit test / mock dependency injection).
     */
    public void setBookingService(MockBookingService bookingService) {
        if (bookingService != null) {
            this.bookingService = bookingService;
        }
    }

    /**
     * Gắn tham chiếu Stage của hộp thoại để quản lý đóng mở và sự kiện nút X.
     */
    public void setDialogStage(Stage dialogStage) {
        this.dialogStage = dialogStage;
        if (dialogStage != null) {
            // Đóng bằng nút X có cùng hành vi như nhấn nút Hủy
            dialogStage.setOnCloseRequest(event -> onCancel());
        }
    }

    /**
     * Thiết lập danh sách ghế đã chọn từ D02/D04.
     *
     * @param seats danh sách mã ghế
     */
    public void setSelectedSeats(List<String> seats) {
        this.selectedSeats = (seats != null) ? new ArrayList<>(seats) : new ArrayList<>();
        if (selectedSeatsSummaryLabel != null) {
            selectedSeatsSummaryLabel.getStyleClass().removeAll("summary-seats-empty", "summary-seats-selected");
            if (this.selectedSeats.isEmpty()) {
                selectedSeatsSummaryLabel.setText("(Chưa có ghế nào)");
                selectedSeatsSummaryLabel.getStyleClass().add("summary-seats-empty");
            } else {
                selectedSeatsSummaryLabel.setText(
                        String.join(", ", this.selectedSeats) + " (Tổng: " + this.selectedSeats.size() + " ghế)");
                selectedSeatsSummaryLabel.getStyleClass().add("summary-seats-selected");
            }
        }
        // Tái đánh giá tính hợp lệ của form
        String currentName = (passengerNameField != null) ? passengerNameField.getText() : "";
        handleNameChanged(currentName);
    }

    /**
     * Xử lý kiểm tra tính hợp lệ khi người dùng gõ vào ô họ tên.
     */
    private void handleNameChanged(String rawName) {
        if (isSubmitting) {
            return;
        }

        int len = (rawName == null) ? 0 : rawName.length();
        if (charCountLabel != null) {
            charCountLabel.setText(len + "/" + BookingFormValidator.NAME_MAX_LENGTH);
            if (len > BookingFormValidator.NAME_MAX_LENGTH) {
                setCharCountStyleClass("char-count-error");
            } else {
                setCharCountStyleClass("char-count-normal");
            }
        }

        // Kiểm tra họ tên
        BookingFormValidator.ValidationResult nameResult = BookingFormValidator.validateName(rawName);
        // Kiểm tra danh sách ghế
        BookingFormValidator.ValidationResult seatsResult = BookingFormValidator.validateSeats(selectedSeats);

        if (!nameResult.isValid()) {
            showError(nameResult.message());
            if (confirmBtn != null) {
                confirmBtn.setDisable(true);
            }
        } else if (!seatsResult.isValid()) {
            showError(seatsResult.message());
            if (confirmBtn != null) {
                confirmBtn.setDisable(true);
            }
        } else {
            hideError();
            if (confirmBtn != null) {
                confirmBtn.setDisable(false);
            }
        }
    }

    private void setCharCountStyleClass(String styleClass) {
        if (charCountLabel != null) {
            charCountLabel.getStyleClass().removeAll("char-count-normal", "char-count-error");
            charCountLabel.getStyleClass().add(styleClass);
        }
    }

    private void showError(String message) {
        if (errorLabel != null) {
            errorLabel.setText(message);
            errorLabel.setVisible(true);
            errorLabel.setManaged(true);
        }
    }

    private void hideError() {
        if (errorLabel != null) {
            errorLabel.setText("");
            errorLabel.setVisible(false);
            errorLabel.setManaged(false);
        }
    }

    /**
     * Xử lý khi nhấn nút "Xác nhận đặt vé".
     */
    @FXML
    public void onConfirm() {
        if (isSubmitting) {
            return;
        }

        String rawName = (passengerNameField != null) ? passengerNameField.getText() : "";
        BookingFormValidator.ValidationResult nameResult = BookingFormValidator.validateName(rawName);
        BookingFormValidator.ValidationResult seatsResult = BookingFormValidator.validateSeats(selectedSeats);

        if (!nameResult.isValid()) {
            showError(nameResult.message());
            return;
        }
        if (!seatsResult.isValid()) {
            showError(seatsResult.message());
            return;
        }

        // Khóa giao diện chống double-click
        isSubmitting = true;
        if (confirmBtn != null)
            confirmBtn.setDisable(true);
        if (cancelBtn != null)
            cancelBtn.setDisable(true);
        if (passengerNameField != null)
            passengerNameField.setDisable(true);

        if (statusLabel != null) {
            statusLabel.getStyleClass().removeAll(
                    "booking-status-submitting", "booking-status-confirmed", "booking-status-rejected", "booking-status-unknown"
            );
            statusLabel.getStyleClass().add("booking-status-submitting");
            statusLabel.setText("Đang gửi yêu cầu đặt vé...");
        }

        String normalizedName = BookingFormValidator.normalizeName(rawName);
        List<String> seatsToBook = new ArrayList<>(selectedSeats);

        // Chạy service ngầm trên background Task (không chặn UI thread)
        Task<BookingOutcome> bookingTask = new Task<>() {
            @Override
            protected BookingOutcome call() {
                return bookingService.submit(normalizedName, seatsToBook);
            }
        };

        bookingTask.setOnSucceeded(event -> {
            BookingOutcome result = bookingTask.getValue();
            this.outcome = result;
            showOutcomeAndAutoClose(result);
        });

        bookingTask.setOnFailed(event -> {
            Throwable err = bookingTask.getException();
            this.outcome = BookingOutcome
                    .unknown("Lỗi hệ thống: " + (err != null ? err.getMessage() : "không rõ nguyên nhân"));
            showOutcomeAndAutoClose(this.outcome);
        });

        new Thread(bookingTask, "mock-booking-worker").start();
    }

    /**
     * Hiển thị trạng thái kết quả trong dialog và tự động đóng sau một khoảng ngắn
     * để D02/D04 nhận kết quả.
     */
    private void showOutcomeAndAutoClose(BookingOutcome result) {
        if (statusLabel != null) {
            statusLabel.getStyleClass().removeAll(
                    "booking-status-submitting", "booking-status-confirmed", "booking-status-rejected", "booking-status-unknown"
            );

            switch (result.getStatus()) {
                case CONFIRMED -> {
                    statusLabel.getStyleClass().add("booking-status-confirmed");
                    statusLabel.setText("✓ Đặt vé thành công! Mã: " + result.getReferenceCode());
                }
                case REJECTED -> {
                    statusLabel.getStyleClass().add("booking-status-rejected");
                    statusLabel.setText("✗ Ghế đã có người đặt, vui lòng chọn ghế khác");
                }
                case UNKNOWN -> {
                    statusLabel.getStyleClass().add("booking-status-unknown");
                    statusLabel.setText("⚠ Chưa xác định được kết quả, vui lòng kiểm tra lại");
                }
            }
        }

        // Tự động đóng dialog sau 650ms để người dùng kịp đọc phản hồi
        PauseTransition delay = new PauseTransition(Duration.millis(650));
        delay.setOnFinished(e -> closeDialogStage());
        delay.play();
    }

    /**
     * Xử lý khi nhấn nút "Hủy".
     * Đóng dialog, không trả kết quả và không đổi trạng thái ghế.
     */
    @FXML
    public void onCancel() {
        this.outcome = null;
        closeDialogStage();
    }

    private void closeDialogStage() {
        if (dialogStage != null) {
            dialogStage.close();
        }
    }

    /**
     * Cung cấp kết quả đặt vé cho D02/D04 sau khi hộp thoại đóng.
     *
     * @return {@link Optional} chứa {@link BookingOutcome} nếu đã xác nhận, hoặc rỗng nếu người dùng hủy
     */
    public Optional<BookingOutcome> getOutcome() {
        return Optional.ofNullable(outcome);
    }
}
