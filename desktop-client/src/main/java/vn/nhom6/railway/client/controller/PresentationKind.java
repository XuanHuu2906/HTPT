package vn.nhom6.railway.client.controller;

/**
 * Phân loại kết quả và lỗi giao diện người dùng theo SPEC-03 và kiến trúc UI (D04/D07).
 */
public enum PresentationKind {
    /**
     * Giao dịch thành công (đã commit và apply bền vững).
     */
    CONFIRMED,

    /**
     * Từ chối nghiệp vụ (đã commit và apply quyết định từ chối: ghế đã đặt, vé không hiệu lực).
     */
    REJECTED,

    /**
     * Lỗi hạ tầng / giao thức / dữ liệu (sai schema, thiếu quorum, không phải leader, lỗi nội bộ).
     */
    ERROR,

    /**
     * Chưa rõ kết quả giao dịch (timeout, không đọc được sơ đồ ghế hiện hành).
     * Tuyệt đối không tự suy diễn thành REJECTED hay CONFIRMED.
     */
    UNKNOWN,

    /**
     * Mất kết nối mạng hoặc không thể kết nối tới máy chủ.
     */
    DISCONNECTED,

    /**
     * Lỗi phát sinh phía ứng dụng client (khung hỏng, timeout nội bộ client).
     */
    CLIENT_ERROR
}

