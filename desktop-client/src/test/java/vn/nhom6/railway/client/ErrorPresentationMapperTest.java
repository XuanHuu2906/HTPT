package vn.nhom6.railway.client;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import vn.nhom6.railway.client.controller.ClientErrorKind;
import vn.nhom6.railway.client.controller.ErrorPresentation;
import vn.nhom6.railway.client.controller.ErrorPresentationMapper;
import vn.nhom6.railway.client.controller.PresentationKind;
import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bộ kiểm thử toàn diện cho {@link ErrorPresentationMapper} (TASK TV1-D13).
 * <p>
 * Kiểm thử thuần đơn vị: không phụ thuộc JavaFX Application Thread, không I/O mạng, không HTTP.
 */
class ErrorPresentationMapperTest {

    private static Set<String> cssClassesInRailwayCss = new HashSet<>();

    @BeforeAll
    static void loadRailwayCssClasses() throws IOException {
        try (InputStream is = ErrorPresentationMapperTest.class.getResourceAsStream("/css/railway.css")) {
            assertNotNull(is, "File /css/railway.css phải tồn tại trong classpath desktop-client");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                Pattern pattern = Pattern.compile("\\.([a-zA-Z0-9_-]+)");
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher m = pattern.matcher(line);
                    while (m.find()) {
                        cssClassesInRailwayCss.add(m.group(1));
                    }
                }
            }
        }
        assertFalse(cssClassesInRailwayCss.isEmpty(), "Tập lớp CSS từ railway.css không được rỗng");
    }

    // =========================================================================
    // CA 1: Từng mã trong danh sách -> đúng kind, styleClass, retryable, needsSeatRefresh, thông báo
    // =========================================================================

    @Test
    @DisplayName("Ca 1.1: SEAT_ALREADY_BOOKED trong BookTicketResponseDto (REJECTED)")
    void testSeatAlreadyBookedInBookingResponse() {
        BookTicketResponseDto dto = new BookTicketResponseDto(
                "req-1", "REJECTED", null, "SE-001", "G05",
                ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, 43L, false, null, "Ghế đã có người mua");

        ErrorPresentation p = ErrorPresentationMapper.mapBookingResponse(dto);

        assertEquals(PresentationKind.REJECTED, p.kind());
        assertEquals(ErrorPresentationMapper.STYLE_REJECTED, p.styleClass());
        assertFalse(p.retryable(), "Ghế đã có chủ thì không retry cùng ghế");
        assertTrue(p.needsSeatRefresh(), "Cần làm mới sơ đồ ghế để đánh dấu BOOKED");
        assertFalse(p.showRequestId());
        assertTrue(p.message().contains("đã có người đặt"));
        assertEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.2: INVALID_REQUEST trong ErrorResponseDto")
    void testInvalidRequestInErrorResponse() {
        ErrorResponseDto dto = new ErrorResponseDto("INVALID_REQUEST", "Schema không hợp lệ", null, null, "req-1");
        ErrorPresentation p = ErrorPresentationMapper.mapErrorResponse(dto);

        assertEquals(PresentationKind.ERROR, p.kind());
        assertEquals(ErrorPresentationMapper.STYLE_ERROR, p.styleClass());
        assertFalse(p.retryable(), "INVALID_REQUEST không được retry nguyên trạng vô hạn");
        assertFalse(p.needsSeatRefresh());
        assertFalse(p.showRequestId());
        assertTrue(p.message().contains("không hợp lệ"));
        assertEquals("INVALID_REQUEST", p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.3: NO_QUORUM trong ErrorResponseDto / BookTicketResponseDto")
    void testNoQuorumMapping() {
        ErrorResponseDto dto = new ErrorResponseDto("NO_QUORUM", "Thiếu quorum 3/5", null, 2L, "req-2");
        ErrorPresentation p = ErrorPresentationMapper.mapErrorResponse(dto);

        assertEquals(PresentationKind.ERROR, p.kind());
        assertEquals(ErrorPresentationMapper.STYLE_ERROR, p.styleClass());
        assertTrue(p.retryable(), "NO_QUORUM có thể thử lại sau cùng requestId");
        assertFalse(p.needsSeatRefresh());
        assertTrue(p.showRequestId(), "NO_QUORUM phải hiển thị requestId để người dùng thử lại cùng ý định");
        assertTrue(p.message().contains("Không đủ đa số") || p.message().contains("quorum"));
        assertEquals("NO_QUORUM", p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.4: NOT_LEADER trong ErrorResponseDto")
    void testNotLeaderMapping() {
        ErrorResponseDto dto = new ErrorResponseDto("NOT_LEADER", "Node-1 là Follower", "node-2", 3L, "req-3");
        ErrorPresentation p = ErrorPresentationMapper.mapErrorResponse(dto);

        assertEquals(PresentationKind.ERROR, p.kind());
        assertEquals(ErrorPresentationMapper.STYLE_ERROR, p.styleClass());
        assertTrue(p.retryable(), "NOT_LEADER có thể kết nối lại theo leaderHint");
        assertFalse(p.needsSeatRefresh());
        assertTrue(p.showRequestId());
        assertTrue(p.message().contains("Node không phải Leader"));
        assertEquals("NOT_LEADER", p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.5: READ_UNAVAILABLE trong ErrorResponseDto")
    void testReadUnavailableMapping() {
        ErrorResponseDto dto = new ErrorResponseDto("READ_UNAVAILABLE", "Read barrier failed", null, 1L, null);
        ErrorPresentation p = ErrorPresentationMapper.mapErrorResponse(dto);

        assertEquals(PresentationKind.UNKNOWN, p.kind(), "READ_UNAVAILABLE phải hiển thị trạng thái UNKNOWN theo SPEC-03 §4.4");
        assertEquals(ErrorPresentationMapper.STYLE_UNKNOWN, p.styleClass());
        assertTrue(p.retryable());
        assertTrue(p.needsSeatRefresh(), "READ_UNAVAILABLE cần làm mới lại sơ đồ ghế");
        assertFalse(p.showRequestId());
        assertTrue(p.message().contains("chưa rõ") || p.message().contains("READ_UNAVAILABLE"));
        assertEquals("READ_UNAVAILABLE", p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.6: TIMEOUT_UNKNOWN trong BookTicketResponseDto (UNKNOWN outcome)")
    void testTimeoutUnknownInBookingResponse() {
        BookTicketResponseDto dto = new BookTicketResponseDto(
                "req-4", "UNKNOWN", null, "SE-001", "G01",
                ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, null, false, null, "Hết thời gian chờ");

        ErrorPresentation p = ErrorPresentationMapper.mapBookingResponse(dto);

        assertEquals(PresentationKind.UNKNOWN, p.kind());
        assertEquals(ErrorPresentationMapper.STYLE_UNKNOWN, p.styleClass());
        assertTrue(p.retryable(), "TIMEOUT_UNKNOWN có thể tra cứu hoặc retry cùng requestId");
        assertFalse(p.needsSeatRefresh(), "UNKNOWN không được tự ý đổi ghế thành BOOKED");
        assertTrue(p.showRequestId(), "Phải hiển thị requestId để tra cứu");
        assertTrue(p.message().contains("Chưa xác định kết quả"));
        assertEquals("TIMEOUT_UNKNOWN", p.rawCode());
    }

    @Test
    @DisplayName("Ca 1.7: UNSUPPORTED_VERSION, REQUEST_ID_CONFLICT, INTERNAL_ERROR, TICKET_NOT_ACTIVE")
    void testOtherSpec03Codes() {
        // UNSUPPORTED_VERSION
        ErrorPresentation p1 = ErrorPresentationMapper.mapCode("UNSUPPORTED_VERSION", false);
        assertEquals(PresentationKind.ERROR, p1.kind());
        assertFalse(p1.retryable());

        // REQUEST_ID_CONFLICT
        ErrorPresentation p2 = ErrorPresentationMapper.mapCode("REQUEST_ID_CONFLICT", false);
        assertEquals(PresentationKind.ERROR, p2.kind());
        assertFalse(p2.retryable(), "REQUEST_ID_CONFLICT dừng, không tự thay ID");
        assertTrue(p2.showRequestId());

        // INTERNAL_ERROR
        ErrorPresentation p3 = ErrorPresentationMapper.mapCode("INTERNAL_ERROR", false);
        assertEquals(PresentationKind.ERROR, p3.kind());
        assertTrue(p3.retryable());

        // TICKET_NOT_ACTIVE
        ErrorPresentation p4 = ErrorPresentationMapper.mapCode("TICKET_NOT_ACTIVE", false);
        assertEquals(PresentationKind.REJECTED, p4.kind());
        assertFalse(p4.retryable());
        assertTrue(p4.needsSeatRefresh());
    }

    // =========================================================================
    // CA 2: UNKNOWN (TIMEOUT_UNKNOWN) KHÁC BIỆT HOÀN TOÀN VỚI REJECTED (SEAT_ALREADY_BOOKED)
    // =========================================================================

    @Test
    @DisplayName("Ca 2: Phân biệt rõ rệt giữa UNKNOWN (TIMEOUT_UNKNOWN) và REJECTED (SEAT_ALREADY_BOOKED)")
    void testUnknownDiffersFromRejected() {
        ErrorPresentation unknownP = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN, false);
        ErrorPresentation rejectedP = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, false);

        assertNotEquals(unknownP.kind(), rejectedP.kind(), "kind phải khác nhau (UNKNOWN vs REJECTED)");
        assertEquals(PresentationKind.UNKNOWN, unknownP.kind());
        assertEquals(PresentationKind.REJECTED, rejectedP.kind());

        assertNotEquals(unknownP.styleClass(), rejectedP.styleClass(), "styleClass phải khác nhau");
        assertEquals(ErrorPresentationMapper.STYLE_UNKNOWN, unknownP.styleClass());
        assertEquals(ErrorPresentationMapper.STYLE_REJECTED, rejectedP.styleClass());

        assertNotEquals(unknownP.title(), rejectedP.title(), "Tiêu đề phải khác nhau");
        assertNotEquals(unknownP.message(), rejectedP.message(), "Thông báo phải khác nhau");
        assertNotEquals(unknownP.suggestedAction(), rejectedP.suggestedAction(), "Gợi ý hành động phải khác nhau");

        assertTrue(unknownP.retryable(), "UNKNOWN là retryable");
        assertFalse(rejectedP.retryable(), "REJECTED SEAT_ALREADY_BOOKED không được retry");

        assertFalse(unknownP.needsSeatRefresh(), "UNKNOWN không được làm mới ghế sang BOOKED");
        assertTrue(rejectedP.needsSeatRefresh(), "REJECTED phải làm mới sơ đồ ghế");

        assertTrue(unknownP.showRequestId(), "UNKNOWN phải hiển thị requestId");
        assertFalse(rejectedP.showRequestId(), "REJECTED không hiển thị requestId để retry");
    }

    // =========================================================================
    // CA 3: Cùng errorCode với message khác nhau -> Presentation giống hệt (Không parse chuỗi)
    // =========================================================================

    @Test
    @DisplayName("Ca 3: Cùng errorCode nhưng message rỗng / khác / gây nhiễu -> Presentation logic giống hệt")
    void testSameErrorCodeWithDifferentMessagesYieldsSameLogic() {
        ErrorPresentation p1 = ErrorPresentationMapper.mapCode("NO_QUORUM", "", false);
        ErrorPresentation p2 = ErrorPresentationMapper.mapCode("NO_QUORUM", "Thong bao bat ky tu server", false);
        ErrorPresentation p3 = ErrorPresentationMapper.mapCode("NO_QUORUM", "SEAT_ALREADY_BOOKED - chuỗi gây nhiễu", false);

        // Logic không bị thay đổi bởi nội dung message
        assertEquals(p1.kind(), p2.kind());
        assertEquals(p1.kind(), p3.kind());
        assertEquals(p1.styleClass(), p2.styleClass());
        assertEquals(p1.styleClass(), p3.styleClass());
        assertEquals(p1.title(), p2.title());
        assertEquals(p1.message(), p2.message());
        assertEquals(p1.message(), p3.message());
        assertEquals(p1.suggestedAction(), p2.suggestedAction());
        assertEquals(p1.retryable(), p2.retryable());
        assertEquals(p1.needsSeatRefresh(), p2.needsSeatRefresh());

        // Ngoại lệ chứa chữ "SEAT_ALREADY_BOOKED" nhưng errorCode là INVALID_REQUEST -> vẫn là INVALID_REQUEST
        ErrorPresentation pInvalid = ErrorPresentationMapper.mapCode("INVALID_REQUEST", "Lỗi SEAT_ALREADY_BOOKED trong text", false);
        assertEquals(PresentationKind.ERROR, pInvalid.kind());
        assertNotEquals(PresentationKind.REJECTED, pInvalid.kind());
        assertEquals("INVALID_REQUEST", pInvalid.rawCode());
    }

    // =========================================================================
    // CA 4: Mã không nhận diện được / null / rỗng / sai hoa thường -> lỗi không xác định mã, không REJECTED
    // =========================================================================

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN_RANDOM_CODE", "HACK_OR_ATTEMPT", "seat_already_booked", "no_quorum", "Invalid_Request", " "})
    @DisplayName("Ca 4.1: Các mã lạ hoặc sai quy tắc hoa/thường rơi vào lỗi không xác định mã")
    void testUnrecognizedCodes(String badCode) {
        ErrorPresentation p = ErrorPresentationMapper.mapCode(badCode, false);

        assertNotNull(p);
        assertEquals(PresentationKind.ERROR, p.kind(), "Không được coi là REJECTED");
        assertNotEquals(PresentationKind.REJECTED, p.kind());
        assertNotEquals(PresentationKind.CONFIRMED, p.kind());
        assertEquals("Lỗi không xác định mã", p.title());
        assertTrue(p.message().contains("không nhận diện được"));
        assertFalse(p.retryable());
        assertFalse(p.needsSeatRefresh());
    }

    @Test
    @DisplayName("Ca 4.2: Mã null và rỗng")
    void testNullAndEmptyCodes() {
        ErrorPresentation pNull = ErrorPresentationMapper.mapCode(null, false);
        assertNotNull(pNull);
        assertEquals(PresentationKind.ERROR, pNull.kind());
        assertEquals("Lỗi không xác định mã", pNull.title());

        ErrorPresentation pEmpty = ErrorPresentationMapper.mapCode("", false);
        assertNotNull(pEmpty);
        assertEquals(PresentationKind.ERROR, pEmpty.kind());
        assertEquals("Lỗi không xác định mã", pEmpty.title());
    }

    // =========================================================================
    // CA 5: Mã SEAT_SOLD (cấm dùng) -> rơi vào nhánh không nhận diện được
    // =========================================================================

    @Test
    @DisplayName("Ca 5: Chuỗi SEAT_SOLD rơi vào lỗi không nhận diện, tuyệt đối không ánh xạ thành ghế đã đặt")
    void testSeatSoldIsNotRecognized() {
        String forbiddenCode = "SEAT_SOLD";
        ErrorPresentation p = ErrorPresentationMapper.mapCode(forbiddenCode, false);

        assertEquals(PresentationKind.ERROR, p.kind());
        assertNotEquals(PresentationKind.REJECTED, p.kind(), "SEAT_SOLD không được ánh xạ thành REJECTED");
        assertEquals("Lỗi không xác định mã", p.title());
        assertFalse(p.message().contains("Ghế đã có người đặt"));
        assertTrue(p.message().contains("SEAT_SOLD"));
        assertEquals("SEAT_SOLD", p.rawCode());
    }

    // =========================================================================
    // CA 6: Quét toàn bộ mã chuẩn trong SPEC-03 / shared
    // =========================================================================

    @Test
    @DisplayName("Ca 6: Mọi mã chuẩn trong SPEC-03 đều được xử lý rõ ràng, không rơi vào nhánh không xác định mã")
    void testAllKnownSpec03CodesCovered() {
        List<String> knownCodes = List.of(
                ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                ErrorPresentationMapper.CODE_INVALID_REQUEST,
                ErrorPresentationMapper.CODE_NO_QUORUM,
                ErrorPresentationMapper.CODE_NOT_LEADER,
                ErrorPresentationMapper.CODE_READ_UNAVAILABLE,
                ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN,
                ErrorPresentationMapper.CODE_UNSUPPORTED_VERSION,
                ErrorPresentationMapper.CODE_REQUEST_ID_CONFLICT,
                ErrorPresentationMapper.CODE_INTERNAL_ERROR,
                ErrorPresentationMapper.CODE_TICKET_NOT_ACTIVE
        );

        for (String code : knownCodes) {
            ErrorPresentation p = ErrorPresentationMapper.mapCode(code, false);
            assertNotEquals("Lỗi không xác định mã", p.title(), "Mã chuẩn " + code + " không được rơi vào fallback không xác định");
            assertEquals(code, p.rawCode());
        }
    }

    // =========================================================================
    // CA 7: Lỗi Client (ClientErrorKind) tách biệt, không đóng giả errorCode SPEC-03
    // =========================================================================

    @Test
    @DisplayName("Ca 7: ClientErrorKind tách biệt hoàn toàn, mang nhãn lỗi phía ứng dụng")
    void testClientErrorsDistinctFromSpec03() {
        for (ClientErrorKind clientError : ClientErrorKind.values()) {
            ErrorPresentation p = ErrorPresentationMapper.mapClientError(clientError, "Chi tiết kỹ thuật", false);

            assertNotNull(p.clientErrorKind());
            assertEquals(clientError, p.clientErrorKind());
            assertTrue(p.title().contains("[Lỗi kết nối]"));
            assertTrue(p.message().contains("[Lỗi ứng dụng/kết nối]") || p.message().contains("Mất kết nối"));
            assertNotEquals(PresentationKind.REJECTED, p.kind(), "Lỗi client không bao giờ là REJECTED nghiệp vụ");
            assertNotEquals(PresentationKind.CONFIRMED, p.kind());

            // Tên lỗi client không trùng với bất kỳ mã SPEC-03 nào
            assertNotEquals(ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED, clientError.name());
            assertNotEquals(ErrorPresentationMapper.CODE_NO_QUORUM, clientError.name());
            assertNotEquals(ErrorPresentationMapper.CODE_NOT_LEADER, clientError.name());
        }

        // Kiểm tra qua mapThrowable
        ErrorPresentation pTimeout = ErrorPresentationMapper.mapThrowable(new SocketTimeoutException("Read timed out"), false);
        assertEquals(ClientErrorKind.READ_TIMEOUT, pTimeout.clientErrorKind());
        assertEquals(PresentationKind.UNKNOWN, pTimeout.kind());

        ErrorPresentation pConnect = ErrorPresentationMapper.mapThrowable(new ConnectException("Connection refused"), false);
        assertEquals(ClientErrorKind.CONNECTION_FAILED, pConnect.clientErrorKind());
        assertEquals(PresentationKind.DISCONNECTED, pConnect.kind());

        ErrorPresentation pIo = ErrorPresentationMapper.mapThrowable(new IOException("Connection reset"), false);
        assertEquals(ClientErrorKind.CONNECTION_LOST, pIo.clientErrorKind());
        assertEquals(PresentationKind.DISCONNECTED, pIo.kind());
    }

    // =========================================================================
    // CA 8: Mọi styleClass trả về đều tồn tại trong railway.css
    // =========================================================================

    @Test
    @DisplayName("Ca 8: Tất cả styleClass và getBookingStatusClass trả về đều tồn tại thực sự trong railway.css")
    void testAllStyleClassesExistInRailwayCss() {
        List<String> codesToTest = List.of(
                ErrorPresentationMapper.CODE_SEAT_ALREADY_BOOKED,
                ErrorPresentationMapper.CODE_INVALID_REQUEST,
                ErrorPresentationMapper.CODE_NO_QUORUM,
                ErrorPresentationMapper.CODE_NOT_LEADER,
                ErrorPresentationMapper.CODE_READ_UNAVAILABLE,
                ErrorPresentationMapper.CODE_TIMEOUT_UNKNOWN,
                ErrorPresentationMapper.CODE_UNSUPPORTED_VERSION,
                ErrorPresentationMapper.CODE_REQUEST_ID_CONFLICT,
                ErrorPresentationMapper.CODE_INTERNAL_ERROR,
                ErrorPresentationMapper.CODE_TICKET_NOT_ACTIVE,
                "UNKNOWN_ARBITRARY_CODE"
        );

        for (String code : codesToTest) {
            ErrorPresentation p = ErrorPresentationMapper.mapCode(code, false);
            assertTrue(cssClassesInRailwayCss.contains(p.styleClass()),
                    "Lớp CSS " + p.styleClass() + " của mã " + code + " phải có trong railway.css");
            assertTrue(cssClassesInRailwayCss.contains(p.getBookingStatusClass()),
                    "Lớp CSS booking " + p.getBookingStatusClass() + " của mã " + code + " phải có trong railway.css");
        }

        for (ClientErrorKind clientError : ClientErrorKind.values()) {
            ErrorPresentation p = ErrorPresentationMapper.mapClientError(clientError, null, false);
            assertTrue(cssClassesInRailwayCss.contains(p.styleClass()),
                    "Lớp CSS " + p.styleClass() + " của client error " + clientError + " phải có trong railway.css");
            assertTrue(cssClassesInRailwayCss.contains(p.getBookingStatusClass()),
                    "Lớp CSS booking " + p.getBookingStatusClass() + " của client error " + clientError + " phải có trong railway.css");
        }
    }

    // =========================================================================
    // CA 9: Cờ MOCK được giữ nguyên qua mapper
    // =========================================================================

    @Test
    @DisplayName("Ca 9: Cờ MOCK được bảo lưu chính xác, thêm nhãn [MOCK] và giữ isMock() = true")
    void testMockFlagPreserved() {
        // Mock = true
        ErrorPresentation pMock = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_NO_QUORUM, true);
        assertTrue(pMock.isMock(), "isMock() phải là true khi truyền mock=true");
        assertTrue(pMock.message().contains("[MOCK]"), "Thông báo phải có nhãn [MOCK]");

        // Mock = false (Real)
        ErrorPresentation pReal = ErrorPresentationMapper.mapCode(ErrorPresentationMapper.CODE_NO_QUORUM, false);
        assertFalse(pReal.isMock(), "isMock() phải là false khi truyền mock=false");
        assertFalse(pReal.message().contains("[MOCK]"), "Thông báo chế độ thực không được có nhãn [MOCK]");

        // Qua BookTicketResponseDto
        BookTicketResponseDto mockDto = new BookTicketResponseDto(
                "req-mock", "CONFIRMED", "TCK-1", "SE-001", "G01", null, 10L, true, null, "Thành công");
        ErrorPresentation pDtoMock = ErrorPresentationMapper.mapBookingResponse(mockDto);
        assertTrue(pDtoMock.isMock());
        assertTrue(pDtoMock.message().contains("[MOCK]"));

        BookTicketResponseDto realDto = new BookTicketResponseDto(
                "req-real", "CONFIRMED", "TCK-2", "SE-001", "G02", null, 11L, false, null, "Thành công");
        ErrorPresentation pDtoReal = ErrorPresentationMapper.mapBookingResponse(realDto);
        assertFalse(pDtoReal.isMock());
        assertFalse(pDtoReal.message().contains("[MOCK]"));
    }
}

