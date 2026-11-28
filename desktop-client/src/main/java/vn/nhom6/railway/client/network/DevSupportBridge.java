package vn.nhom6.railway.client.network;

/**
 * Trình nạp có bảo vệ cho {@link DevSupportSpi} (TASK TV1-D28).
 * <p>
 * Khi {@code release.mode=true}, mọi truy cập tới cầu nối này đều bị từ chối fail-fast
 * bằng {@link IllegalStateException}. Khi chạy dev/test ({@code release.mode=false}),
 * lớp cài đặt {@code vn.nhom6.railway.client.dev.DevModeSupport} được nạp động để các
 * lớp trong luồng release không giữ bất kỳ tham chiếu biên dịch nào tới {@code Mock*}/{@code Fake*}.
 */
public final class DevSupportBridge {

    private static final String IMPL_CLASS_NAME = "vn.nhom6.railway.client.dev.DevModeSupport";
    private static volatile DevSupportSpi cachedInstance;

    private DevSupportBridge() {
    }

    public static DevSupportSpi get() {
        if (ClientConfig.isReleaseModeActive()) {
            throw new IllegalStateException(
                    "Lỗi cấu hình RELEASE (release.mode=true): Tuyệt đối không được nạp thành phần giả lập phát triển!");
        }
        DevSupportSpi local = cachedInstance;
        if (local != null) {
            return local;
        }
        synchronized (DevSupportBridge.class) {
            if (cachedInstance == null) {
                try {
                    Class<?> clazz = Class.forName(IMPL_CLASS_NAME);
                    cachedInstance = (DevSupportSpi) clazz.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(
                            "Không tìm thấy bộ hỗ trợ phát triển (" + IMPL_CLASS_NAME
                                    + "). Bản đóng gói hiện tại là bản RELEASE chỉ hỗ trợ SocketBookingClient thật!",
                            e);
                }
            }
            return cachedInstance;
        }
    }

    public static boolean isDevMockClient(BookingClient client) {
        if (client == null || ClientConfig.isReleaseModeActive()) {
            return false;
        }
        BookingClient unwrapped = (client instanceof JournalingBookingClient jbc) ? jbc.getDelegate() : client;
        if (unwrapped instanceof SocketBookingClient || unwrapped instanceof LeaderAwareBookingClient) {
            return false;
        }
        String simpleName = unwrapped.getClass().getSimpleName();
        return simpleName.contains("Mock");
    }
}
