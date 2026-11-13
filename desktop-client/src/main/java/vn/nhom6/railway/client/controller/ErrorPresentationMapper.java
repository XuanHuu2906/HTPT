package vn.nhom6.railway.client.controller;

import vn.nhom6.railway.client.network.BookTicketResponseDto;
import vn.nhom6.railway.client.network.CancelTicketResponseDto;
import vn.nhom6.railway.client.network.NotLeaderException;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * Bộ ánh xạ trạng thái và mã lỗi chuẩn hóa theo hợp đồng SPEC-03 (TASK D13).
 * <p>
 * Nguyên tắc bất biến:
 * 1. Logic ánh xạ dựa HOÀN TOÀN vào mã có cấu trúc (outcome, errorCode, reasonCode).
 * 2. TUYỆT ĐỐI KHÔNG parse, so sánh chuỗi hoặc dùng regex trên nội dung {@code message} từ server.
 * 3. UNKNOWN luôn khác biệt hoàn toàn với REJECTED (về kind, styleClass, gợi ý xử lý và làm mới sơ đồ).
 * 4. Không dùng chuỗi tự chế SEAT_SOLD. Mã không nhận diện được rơi vào nhánh lỗi chưa xác định mã.
 * 5. Lỗi client được phân loại qua {@link ClientErrorKind}, không đóng giả làm mã SPEC-03.
 */
public final class ErrorPresentationMapper {

    // === Hằng số errorCode và reasonCode chuẩn hóa theo SPEC-03 Section 7 ===
    public static final String CODE_SEAT_ALREADY_BOOKED = "SEAT_ALREADY_BOOKED";
    public static final String CODE_INVALID_REQUEST = "INVALID_REQUEST";
    public static final String CODE_NO_QUORUM = "NO_QUORUM";
    public static final String CODE_NOT_LEADER = "NOT_LEADER";
    public static final String CODE_READ_UNAVAILABLE = "READ_UNAVAILABLE";
    public static final String CODE_TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN";
    public static final String CODE_UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION";
    public static final String CODE_REQUEST_ID_CONFLICT = "REQUEST_ID_CONFLICT";
    public static final String CODE_INTERNAL_ERROR = "INTERNAL_ERROR";
    public static final String CODE_TICKET_NOT_ACTIVE = "TICKET_NOT_ACTIVE";

    // CSS style classes đã có từ railway.css (D04/D07)
    public static final String STYLE_CONFIRMED = "booking-status-confirmed";
    public static final String STYLE_REJECTED = "booking-status-rejected";
    public static final String STYLE_UNKNOWN = "booking-status-unknown";
    public static final String STYLE_ERROR = "status-error";
    public static final String STYLE_DISCONNECTED = "status-disconnected";

    private ErrorPresentationMapper() {
        // Utility class
    }

    /**
     * Ánh xạ kết quả đặt vé {@link BookTicketResponseDto}.
     */
    public static ErrorPresentation mapBookingResponse(BookTicketResponseDto dto) {
        if (dto == null) {
            return mapUnrecognizedCode(null, null, false);
        }

        boolean mock = dto.isMock();
        String serverMsg = dto.getMessage();
        String outcome = dto.getOutcome();

        if ("CONFIRMED".equalsIgnoreCase(outcome)) {
            String seatInfo = (dto.getSeatId() != null && !dto.getSeatId().isBlank()) ? " (Ghế " + dto.getSeatId() + ")" : "";
            String ticketInfo = (dto.getTicketId() != null && !dto.getTicketId().isBlank()) ? " Mã vé: " + dto.getTicketId() : "";
            String msg = "✓ Đặt vé thành công!" + ticketInfo + seatInfo + (mock ? " [MOCK]" : "");
            return new ErrorPresentation(
                    PresentationKind.CONFIRMED,
                    "Đặt vé thành công",
                    msg,
                    STYLE_CONFIRMED,
                    false, // không retry khi đã thành công
                    false, // không cần refresh theo hướng lỗi
                    false,
                    "Xem thông tin vé trong mục 'Vé của tôi'",
                    "CONFIRMED",
                    serverMsg,
                    mock,
                    null
            );
        }

        if ("REJECTED".equalsIgnoreCase(outcome)) {
            String reason = dto.getReasonCode();
            if (Objects.equals(CODE_SEAT_ALREADY_BOOKED, reason)) {
                String seatInfo = (dto.getSeatId() != null && !dto.getSeatId().isBlank()) ? " " + dto.getSeatId() : "";
                String msg = "✗ Ghế" + seatInfo + " đã có người đặt (" + CODE_SEAT_ALREADY_BOOKED + ")" + (mock ? " [MOCK]" : "");
                return new ErrorPresentation(
                        PresentationKind.REJECTED,
                        "Từ chối đặt vé",
                        msg,
                        STYLE_REJECTED,
                        false, // Ghế đã có chủ: không retry cùng ghế
                        true,  // Cần làm mới sơ đồ để chuyển ghế sang BOOKED
                        false,
                        "Chọn ghế khác trên sơ đồ",
                        CODE_SEAT_ALREADY_BOOKED,
                        serverMsg,
                        mock,
                        null
                );
            } else if (Objects.equals(CODE_INVALID_REQUEST, reason)) {
                return mapKnownErrorCode(CODE_INVALID_REQUEST, serverMsg, mock);
            } else {
                return mapUnrecognizedCode(reason, serverMsg, mock);
            }
        }

        if ("UNKNOWN".equalsIgnoreCase(outcome)) {
            String reason = dto.getReasonCode();
            if (Objects.equals(CODE_NO_QUORUM, reason)) {
                return mapKnownErrorCode(CODE_NO_QUORUM, serverMsg, mock);
            } else if (Objects.equals(CODE_NOT_LEADER, reason)) {
                return mapKnownErrorCode(CODE_NOT_LEADER, serverMsg, mock);
            } else if (Objects.equals(CODE_READ_UNAVAILABLE, reason)) {
                return mapKnownErrorCode(CODE_READ_UNAVAILABLE, serverMsg, mock);
            } else if (Objects.equals(CODE_TIMEOUT_UNKNOWN, reason) || reason == null || reason.isBlank()) {
                return mapKnownErrorCode(CODE_TIMEOUT_UNKNOWN, serverMsg, mock);
            } else {
                // Nếu là mã lỗi chuẩn khác trả trong UNKNOWN outcome
                return mapCode(reason, serverMsg, mock);
            }
        }

        return mapUnrecognizedCode(outcome, serverMsg, mock);
    }

    /**
     * Ánh xạ thông điệp lỗi ERROR_RESPONSE từ máy chủ theo SPEC-03 Section 7.
     */
    public static ErrorPresentation mapErrorResponse(ErrorResponseDto dto) {
        if (dto == null) {
            return mapUnrecognizedCode(null, null, false);
        }
        return mapCode(dto.getErrorCode(), dto.getMessage(), false);
    }

    /**
     * Ánh xạ kết quả hủy vé {@link CancelTicketResponseDto} (P1).
     */
    public static ErrorPresentation mapCancelResponse(CancelTicketResponseDto dto) {
        if (dto == null) {
            return mapUnrecognizedCode(null, null, false);
        }

        boolean mock = dto.isMock();
        String serverMsg = null;
        String outcome = dto.getOutcome();

        if ("CONFIRMED".equalsIgnoreCase(outcome)) {
            String ticketInfo = (dto.getTicketId() != null && !dto.getTicketId().isBlank()) ? " " + dto.getTicketId() : "";
            String msg = "✓ Hủy vé" + ticketInfo + " thành công! Ghế đã được giải phóng." + (mock ? " [MOCK]" : "");
            return new ErrorPresentation(
                    PresentationKind.CONFIRMED,
                    "Hủy vé thành công",
                    msg,
                    STYLE_CONFIRMED,
                    false,
                    true, // Giải phóng ghế trên sơ đồ
                    false,
                    "Kiểm tra danh sách vé trong mục 'Vé của tôi'",
                    "CONFIRMED",
                    serverMsg,
                    mock,
                    null
            );
        }

        if ("REJECTED".equalsIgnoreCase(outcome)) {
            String reason = dto.getReasonCode();
            if (Objects.equals(CODE_TICKET_NOT_ACTIVE, reason)) {
                return mapKnownErrorCode(CODE_TICKET_NOT_ACTIVE, serverMsg, mock);
            } else if (Objects.equals(CODE_INVALID_REQUEST, reason)) {
                return mapKnownErrorCode(CODE_INVALID_REQUEST, serverMsg, mock);
            } else {
                return mapUnrecognizedCode(reason, serverMsg, mock);
            }
        }

        if ("UNKNOWN".equalsIgnoreCase(outcome)) {
            return mapKnownErrorCode(CODE_TIMEOUT_UNKNOWN, serverMsg, mock);
        }

        return mapUnrecognizedCode(outcome, serverMsg, mock);
    }

    /**
     * Ánh xạ mã lỗi / mã lý do thuần túy theo SPEC-03.
     */
    public static ErrorPresentation mapCode(String rawCode, boolean mock) {
        return mapCode(rawCode, null, mock);
    }

    /**
     * Ánh xạ mã lỗi / mã lý do thuần túy kèm thông điệp phụ trợ từ máy chủ.
     * <p>
     * Phép so khớp TUYỆT ĐỐI chính xác theo dạng chuỗi hằng số, không so sánh lỏng lẻo hay bỏ qua hoa thường.
     */
    public static ErrorPresentation mapCode(String rawCode, String serverMessage, boolean mock) {
        if (rawCode == null || rawCode.isBlank()) {
            return mapUnrecognizedCode(rawCode, serverMessage, mock);
        }

        return switch (rawCode) {
            case CODE_SEAT_ALREADY_BOOKED -> mapKnownErrorCode(CODE_SEAT_ALREADY_BOOKED, serverMessage, mock);
            case CODE_INVALID_REQUEST -> mapKnownErrorCode(CODE_INVALID_REQUEST, serverMessage, mock);
            case CODE_NO_QUORUM -> mapKnownErrorCode(CODE_NO_QUORUM, serverMessage, mock);
            case CODE_NOT_LEADER -> mapKnownErrorCode(CODE_NOT_LEADER, serverMessage, mock);
            case CODE_READ_UNAVAILABLE -> mapKnownErrorCode(CODE_READ_UNAVAILABLE, serverMessage, mock);
            case CODE_TIMEOUT_UNKNOWN -> mapKnownErrorCode(CODE_TIMEOUT_UNKNOWN, serverMessage, mock);
            case CODE_UNSUPPORTED_VERSION -> mapKnownErrorCode(CODE_UNSUPPORTED_VERSION, serverMessage, mock);
            case CODE_REQUEST_ID_CONFLICT -> mapKnownErrorCode(CODE_REQUEST_ID_CONFLICT, serverMessage, mock);
            case CODE_INTERNAL_ERROR -> mapKnownErrorCode(CODE_INTERNAL_ERROR, serverMessage, mock);
            case CODE_TICKET_NOT_ACTIVE -> mapKnownErrorCode(CODE_TICKET_NOT_ACTIVE, serverMessage, mock);
            default -> mapUnrecognizedCode(rawCode, serverMessage, mock);
        };
    }

    /**
     * Ánh xạ lỗi phát sinh từ ngoại lệ Exception / Throwable phía client.
     */
    public static ErrorPresentation mapThrowable(Throwable throwable, boolean mock) {
        if (throwable == null) {
            return mapClientError(ClientErrorKind.INTERNAL_CLIENT_ERROR, "Lỗi không xác định", mock);
        }

        Throwable current = throwable;
        Throwable unwrapped = throwable;
        while (current != null) {
            String simpleName = current.getClass().getSimpleName();
            if (current instanceof NotLeaderException nle) {
                return mapKnownErrorCode(CODE_NOT_LEADER, nle.getMessage(), mock);
            }
            if ("SocketTimeoutException".equals(simpleName)) {
                return mapClientError(ClientErrorKind.READ_TIMEOUT, current.getMessage(), mock);
            }
            if ("ConnectException".equals(simpleName)) {
                return mapClientError(ClientErrorKind.CONNECTION_FAILED, current.getMessage(), mock);
            }
            if (current instanceof IOException) {
                return mapClientError(ClientErrorKind.CONNECTION_LOST, current.getMessage(), mock);
            }
            unwrapped = current;
            current = current.getCause();
        }

        return mapClientError(ClientErrorKind.INTERNAL_CLIENT_ERROR, unwrapped.getMessage(), mock);
    }

    /**
     * Ánh xạ lỗi phía Client với phân loại rõ ràng trong {@link ClientErrorKind}.
     */
    public static ErrorPresentation mapClientError(ClientErrorKind clientError, String detailMessage, boolean mock) {
        if (clientError == null) {
            clientError = ClientErrorKind.INTERNAL_CLIENT_ERROR;
        }

        PresentationKind kind = (clientError == ClientErrorKind.CONNECTION_LOST || clientError == ClientErrorKind.CONNECTION_FAILED)
                ? PresentationKind.DISCONNECTED
                : (clientError == ClientErrorKind.READ_TIMEOUT ? PresentationKind.UNKNOWN : PresentationKind.CLIENT_ERROR);

        String desc = clientError.getDescription();
        String msg = (kind == PresentationKind.DISCONNECTED)
                ? "Mất kết nối tới cụm máy chủ: " + desc + (mock ? " [MOCK]" : "")
                : "[Lỗi ứng dụng/kết nối] " + desc + (mock ? " [MOCK]" : "");
        String title = "[Lỗi kết nối] " + clientError.name();

        return new ErrorPresentation(
                kind,
                title,
                msg,
                clientError.getStyleClass(),
                clientError.isRetryable(),
                false,
                clientError == ClientErrorKind.READ_TIMEOUT,
                "Kiểm tra kết nối mạng và thử lại sau ít phút",
                clientError.name(),
                detailMessage,
                mock,
                clientError
        );
    }

    // === Các hàm phụ trợ ánh xạ mã cụ thể theo SPEC-03 ===

    private static ErrorPresentation mapKnownErrorCode(String code, String serverMessage, boolean mock) {
        return switch (code) {
            case CODE_SEAT_ALREADY_BOOKED -> new ErrorPresentation(
                    PresentationKind.REJECTED,
                    "Ghế đã được đặt",
                    "Ghế đã có người đặt trước (" + CODE_SEAT_ALREADY_BOOKED + "), vui lòng chọn ghế khác." + (mock ? " [MOCK]" : ""),
                    STYLE_REJECTED,
                    false, // không retry cùng ghế
                    true,  // cần làm mới sơ đồ
                    false,
                    "Chọn ghế khác trên sơ đồ",
                    CODE_SEAT_ALREADY_BOOKED,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_INVALID_REQUEST -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Yêu cầu không hợp lệ",
                    "Dữ liệu yêu cầu không hợp lệ (" + CODE_INVALID_REQUEST + "), vui lòng kiểm tra lại thông tin." + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    false, // không retry nguyên trạng vô hạn
                    false,
                    false,
                    "Kiểm tra và sửa lại dữ liệu đầu vào",
                    CODE_INVALID_REQUEST,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_NO_QUORUM -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Cụm máy chủ thiếu đa số (NO_QUORUM)",
                    "⚡ Lỗi cụm: Không đủ đa số (Quorum 3/5) để cam kết an toàn! (" + CODE_NO_QUORUM + ")" + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    true, // có thể thử lại sau cùng requestId
                    false,
                    true, // hiển thị requestId để tra cứu/thử lại
                    "Thử lại sau ít phút với cùng mã yêu cầu (requestId)",
                    CODE_NO_QUORUM,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_NOT_LEADER -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Node không phải Leader (NOT_LEADER)",
                    "⚡ Lỗi định tuyến: Node không phải Leader! Chưa có giao dịch nào được xác nhận." + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    true, // dùng leaderHint để kết nối lại
                    false,
                    true,
                    "Chuyển kết nối tới Node Leader và thử lại",
                    CODE_NOT_LEADER,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_READ_UNAVAILABLE -> new ErrorPresentation(
                    PresentationKind.UNKNOWN,
                    "Không đọc được sơ đồ ghế (READ_UNAVAILABLE)",
                    "Không thể chứng minh trạng thái ghế hiện hành từ máy chủ Leader (" + CODE_READ_UNAVAILABLE + "). Sơ đồ ghế tạm thời chuyển sang trạng thái chưa rõ." + (mock ? " [MOCK]" : ""),
                    STYLE_UNKNOWN,
                    true,
                    true, // làm mới sơ đồ ghế khi Leader sẵn sàng
                    false,
                    "Làm mới lại sơ đồ ghế sau giây lát",
                    CODE_READ_UNAVAILABLE,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_TIMEOUT_UNKNOWN -> new ErrorPresentation(
                    PresentationKind.UNKNOWN,
                    "Chưa xác định kết quả (TIMEOUT_UNKNOWN)",
                    "⚠ Chưa xác định kết quả giao dịch (" + CODE_TIMEOUT_UNKNOWN + ")! Vui lòng tra cứu lại với requestId. Tuyệt đối không tự ý đặt lại bằng yêu cầu mới." + (mock ? " [MOCK]" : ""),
                    STYLE_UNKNOWN,
                    true, // tra cứu hoặc retry cùng requestId
                    false, // tuyệt đối không tự đổi trạng thái ghế sang BOOKED/AVAILABLE
                    true,  // hiển thị requestId để người dùng tra cứu
                    "Kiểm tra kết quả qua chức năng Tra cứu vé với cùng Request ID",
                    CODE_TIMEOUT_UNKNOWN,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_UNSUPPORTED_VERSION -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Phiên bản không hỗ trợ (UNSUPPORTED_VERSION)",
                    "Phiên bản giao thức không được hỗ trợ bởi máy chủ (" + CODE_UNSUPPORTED_VERSION + ")." + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    false,
                    false,
                    false,
                    "Cập nhật phiên bản phần mềm ứng dụng",
                    CODE_UNSUPPORTED_VERSION,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_REQUEST_ID_CONFLICT -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Xung đột Request ID (REQUEST_ID_CONFLICT)",
                    "Xung đột mã yêu cầu: payload khác nhau đã được gửi cho cùng Request ID (" + CODE_REQUEST_ID_CONFLICT + ")." + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    false, // không tự thay ID và gửi lại cùng ý định
                    false,
                    true,
                    "Tạo giao dịch mới với Request ID mới nếu thay đổi thông tin",
                    CODE_REQUEST_ID_CONFLICT,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_INTERNAL_ERROR -> new ErrorPresentation(
                    PresentationKind.ERROR,
                    "Lỗi nội bộ hệ thống (INTERNAL_ERROR)",
                    "Hệ thống gặp lỗi xử lý nội bộ chưa phân loại (" + CODE_INTERNAL_ERROR + "). Không tự coi là giao dịch thất bại." + (mock ? " [MOCK]" : ""),
                    STYLE_ERROR,
                    true,
                    false,
                    true,
                    "Tra cứu lại kết quả bằng Request ID trước khi thử lại",
                    CODE_INTERNAL_ERROR,
                    serverMessage,
                    mock,
                    null
            );
            case CODE_TICKET_NOT_ACTIVE -> new ErrorPresentation(
                    PresentationKind.REJECTED,
                    "Vé không còn hiệu lực (TICKET_NOT_ACTIVE)",
                    "Vé không tồn tại hoặc đã bị hủy trước đó (" + CODE_TICKET_NOT_ACTIVE + "), không thể tiếp tục thao tác." + (mock ? " [MOCK]" : ""),
                    STYLE_REJECTED,
                    false,
                    true,
                    false,
                    "Kiểm tra lại danh sách vé hợp lệ",
                    CODE_TICKET_NOT_ACTIVE,
                    serverMessage,
                    mock,
                    null
            );
            default -> mapUnrecognizedCode(code, serverMessage, mock);
        };
    }

    private static ErrorPresentation mapUnrecognizedCode(String rawCode, String serverMessage, boolean mock) {
        String displayCode = (rawCode == null || rawCode.isBlank()) ? "<rỗng>" : rawCode;
        return new ErrorPresentation(
                PresentationKind.ERROR,
                "Lỗi không xác định mã",
                "Hệ thống trả về mã lỗi không nhận diện được: " + displayCode + (mock ? " [MOCK]" : ""),
                STYLE_ERROR,
                false,
                false,
                false,
                "Liên hệ quản trị viên hệ thống để kiểm tra mã phản hồi",
                rawCode,
                serverMessage,
                mock,
                null
        );
    }
}
