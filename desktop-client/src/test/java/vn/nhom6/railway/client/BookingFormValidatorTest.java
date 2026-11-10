package vn.nhom6.railway.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Kiểm thử bộ xác thực biểu mẫu đặt vé (BookingFormValidatorTest)")
class BookingFormValidatorTest {

    @Test
    @DisplayName("Chặn họ tên null")
    void testValidateName_Null() {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(null);
        assertFalse(result.isValid());
        assertEquals("Vui lòng nhập họ tên", result.getMessage());
    }

    @Test
    @DisplayName("Chặn họ tên rỗng")
    void testValidateName_Empty() {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName("");
        assertFalse(result.isValid());
        assertEquals("Vui lòng nhập họ tên", result.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "   ", "\t", "\n", " \t \n "})
    @DisplayName("Chặn họ tên chỉ chứa khoảng trắng")
    void testValidateName_WhitespaceOnly(String input) {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(input);
        assertFalse(result.isValid());
        assertEquals("Vui lòng nhập họ tên", result.getMessage());
    }

    @Test
    @DisplayName("Họ tên đúng 50 ký tự -> hợp lệ (vượt qua)")
    void testValidateName_Exactly50Chars() {
        String name50 = "A".repeat(50);
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(name50);
        assertTrue(result.isValid());
        assertEquals("", result.getMessage());
    }

    @Test
    @DisplayName("Họ tên 51 ký tự -> không hợp lệ (bị chặn)")
    void testValidateName_51Chars() {
        String name51 = "A".repeat(51);
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(name51);
        assertFalse(result.isValid());
        assertEquals("Họ tên tối đa 50 ký tự", result.getMessage());
    }

    @Test
    @DisplayName("Họ tên tiếng Việt có dấu đầy đủ và dấu gạch nối/nháy đơn -> hợp lệ")
    void testValidateName_VietnameseAccented() {
        String name = "Nguyễn Thị Ánh-Viên O'Connor";
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(name);
        assertTrue(result.isValid());
        assertEquals("", result.getMessage());

        // Kiểm tra nguyên âm có dấu tiếng Việt (độ dài <= 50)
        String vowelsPart1 = "ÀÁẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬĐÈÉẺẼẸÊẾỀỂỄỆ";
        assertTrue(vowelsPart1.length() <= 50);
        assertTrue(BookingFormValidator.validateName(vowelsPart1).isValid());

        String vowelsPart2 = "ÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴ";
        assertTrue(vowelsPart2.length() <= 50);
        assertTrue(BookingFormValidator.validateName(vowelsPart2).isValid());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Nguyễn Văn A 123", "User99", "123456", "Trần 1"})
    @DisplayName("Chặn họ tên chứa chữ số")
    void testValidateName_ContainsDigits(String input) {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(input);
        assertFalse(result.isValid());
        assertEquals("Họ tên chứa ký tự không hợp lệ", result.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Nguyễn Văn A 😀", "Trần 🎉", "VIP 🔥"})
    @DisplayName("Chặn họ tên chứa emoji")
    void testValidateName_ContainsEmoji(String input) {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(input);
        assertFalse(result.isValid());
        assertEquals("Họ tên chứa ký tự không hợp lệ", result.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Nguyễn @ Văn A", "Trần #", "Lê $", "Hoàng %", "Phạm &", "Đỗ *", "Vũ (", "Bùi )"})
    @DisplayName("Chặn họ tên chứa ký tự đặc biệt khác ngoài ' và -")
    void testValidateName_SpecialCharacters(String input) {
        BookingFormValidator.ValidationResult result = BookingFormValidator.validateName(input);
        assertFalse(result.isValid());
        assertEquals("Họ tên chứa ký tự không hợp lệ", result.getMessage());
    }

    @Test
    @DisplayName("Chuẩn hóa tên nhập: trim và gộp nhiều khoảng trắng liên tiếp")
    void testNormalizeName() {
        assertEquals("Nguyễn Văn A", BookingFormValidator.normalizeName("   Nguyễn   Văn    A   "));
        assertEquals("Trần Thị B", BookingFormValidator.normalizeName("Trần\t\tThị \n B"));
        assertEquals("", BookingFormValidator.normalizeName("   "));
        assertEquals("", BookingFormValidator.normalizeName(null));
    }

    @Test
    @DisplayName("Chặn danh sách ghế rỗng hoặc null")
    void testValidateSeats() {
        BookingFormValidator.ValidationResult nullResult = BookingFormValidator.validateSeats(null);
        assertFalse(nullResult.isValid());
        assertEquals("Danh sách ghế không được để trống", nullResult.getMessage());

        BookingFormValidator.ValidationResult emptyResult = BookingFormValidator.validateSeats(Collections.emptyList());
        assertFalse(emptyResult.isValid());
        assertEquals("Danh sách ghế không được để trống", emptyResult.getMessage());

        BookingFormValidator.ValidationResult validResult = BookingFormValidator.validateSeats(List.of("G01", "G03"));
        assertTrue(validResult.isValid());
    }
}
