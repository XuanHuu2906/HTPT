package vn.nhom6.railway.client;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Lớp kiểm tra tính hợp lệ của dữ liệu biểu mẫu đặt vé (TASK D03).
 * <p>
 * Lưu ý: Lớp này là thuần Java (POJO), KHÔNG import bất kỳ thành phần nào của JavaFX.
 */
public final class BookingFormValidator {

    /**
     * Giới hạn độ dài tối đa của họ tên hành khách theo yêu cầu giao diện D03.
     */
    public static final int NAME_MAX_LENGTH = 50;

    /**
     * Mẫu biểu thức chính quy cho phép chữ cái Unicode (tiếng Việt có dấu), khoảng trắng, dấu nháy đơn (') và dấu gạch nối (-).
     * Không cho phép số, emoji, hoặc ký tự đặc biệt khác.
     */
    public static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[\\p{L}\\s'-]+$");

    /**
     * Kết quả kiểm tra tính hợp lệ dữ liệu.
     */
    public static final class ValidationResult {
        private final boolean valid;
        private final String message;

        public ValidationResult(boolean valid, String message) {
            this.valid = valid;
            this.message = (message != null) ? message : "";
        }

        public static ValidationResult valid() {
            return new ValidationResult(true, "");
        }

        public static ValidationResult invalid(String message) {
            return new ValidationResult(false, message);
        }

        public boolean isValid() {
            return valid;
        }

        public String message() {
            return message;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ValidationResult that = (ValidationResult) o;
            return valid == that.valid && Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(valid, message);
        }

        @Override
        public String toString() {
            return "ValidationResult{valid=" + valid + ", message='" + message + "'}";
        }
    }

    private BookingFormValidator() {
        // Utility class
    }

    /**
     * Kiểm tra tính hợp lệ của họ tên hành khách.
     * <p>
     * Quy tắc:
     * 1. Trim trước khi kiểm tra.
     * 2. Rỗng / null / chỉ chứa khoảng trắng -> "Vui lòng nhập họ tên"
     * 3. Dài hơn 50 ký tự -> "Họ tên tối đa 50 ký tự"
     * 4. Chứa ký tự không hợp lệ (số, emoji, ký tự đặc biệt ngoài khoảng trắng, ' và -) -> "Họ tên chứa ký tự không hợp lệ"
     *
     * @param rawName chuỗi họ tên thô người dùng nhập
     * @return {@link ValidationResult} tương ứng
     */
    public static ValidationResult validateName(String rawName) {
        if (rawName == null) {
            return ValidationResult.invalid("Vui lòng nhập họ tên");
        }

        String trimmed = rawName.trim();
        if (trimmed.isEmpty()) {
            return ValidationResult.invalid("Vui lòng nhập họ tên");
        }

        if (trimmed.length() > NAME_MAX_LENGTH) {
            return ValidationResult.invalid("Họ tên tối đa 50 ký tự");
        }

        if (!VALID_NAME_PATTERN.matcher(trimmed).matches()) {
            return ValidationResult.invalid("Họ tên chứa ký tự không hợp lệ");
        }

        return ValidationResult.valid();
    }

    /**
     * Kiểm tra danh sách ghế được chọn.
     * <p>
     * Chặn danh sách ghế rỗng hoặc null.
     *
     * @param seats danh sách ghế cần kiểm tra
     * @return {@link ValidationResult} tương ứng
     */
    public static ValidationResult validateSeats(List<?> seats) {
        if (seats == null || seats.isEmpty()) {
            return ValidationResult.invalid("Danh sách ghế không được để trống");
        }
        return ValidationResult.valid();
    }

    /**
     * Chuẩn hóa họ tên hành khách: loại bỏ khoảng trắng thừa ở hai đầu
     * và gộp nhiều khoảng trắng liên tiếp ở giữa thành một khoảng trắng đơn.
     *
     * @param rawName chuỗi họ tên thô
     * @return chuỗi họ tên đã chuẩn hóa
     */
    public static String normalizeName(String rawName) {
        if (rawName == null) {
            return "";
        }
        String trimmed = rawName.trim();
        return trimmed.replaceAll("\\s+", " ");
    }
}
