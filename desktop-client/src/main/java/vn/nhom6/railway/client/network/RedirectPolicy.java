package vn.nhom6.railway.client.network;

import java.util.Properties;

/**
 * Chính sách chuyển hướng (redirect) và thời gian chờ (timeout) cho {@link LeaderAwareBookingClient} (TASK TV1-D12).
 * <p>
 * Cấu hình được từ file cấu hình của cụm (SPEC-06 §4 & SPEC-08 §3.1).
 *
 * @param maxRedirects   số lần redirect tối đa khi nhận NOT_LEADER (mặc định: 3)
 * @param stepTimeoutMs  thời gian chờ tối đa cho một bước gửi/nhận (mặc định: 5000ms)
 * @param totalTimeoutMs thời gian chờ tối đa cho toàn bộ chu kỳ lệnh bao gồm các lần redirect (mặc định: 15000ms)
 */
public record RedirectPolicy(int maxRedirects, long stepTimeoutMs, long totalTimeoutMs) {

    public static final int DEFAULT_MAX_REDIRECTS = 3;
    public static final long DEFAULT_STEP_TIMEOUT_MS = 5000L;
    public static final long DEFAULT_TOTAL_TIMEOUT_MS = 15000L;

    public RedirectPolicy {
        if (maxRedirects < 0) {
            throw new IllegalArgumentException("maxRedirects không được âm, nhận được: " + maxRedirects);
        }
        if (stepTimeoutMs <= 0) {
            throw new IllegalArgumentException("stepTimeoutMs phải > 0, nhận được: " + stepTimeoutMs);
        }
        if (totalTimeoutMs <= 0) {
            throw new IllegalArgumentException("totalTimeoutMs phải > 0, nhận được: " + totalTimeoutMs);
        }
    }

    public static RedirectPolicy defaultPolicy() {
        return new RedirectPolicy(DEFAULT_MAX_REDIRECTS, DEFAULT_STEP_TIMEOUT_MS, DEFAULT_TOTAL_TIMEOUT_MS);
    }

    /**
     * Nạp chính sách từ {@link Properties} cấu hình.
     */
    public static RedirectPolicy loadFromProperties(Properties props) {
        if (props == null) {
            return defaultPolicy();
        }
        int maxAttempts = DEFAULT_MAX_REDIRECTS;
        String maxAttemptsStr = props.getProperty("client.retry.max-attempts");
        if (maxAttemptsStr != null && !maxAttemptsStr.isBlank()) {
            try {
                maxAttempts = Integer.parseInt(maxAttemptsStr.trim());
            } catch (NumberFormatException ignored) {}
        }

        long reqTimeout = DEFAULT_STEP_TIMEOUT_MS;
        String reqTimeoutStr = props.getProperty("client.request-timeout-ms");
        if (reqTimeoutStr != null && !reqTimeoutStr.isBlank()) {
            try {
                reqTimeout = Long.parseLong(reqTimeoutStr.trim());
            } catch (NumberFormatException ignored) {}
        }

        long totalTimeout = Math.max(reqTimeout * (maxAttempts + 1), DEFAULT_TOTAL_TIMEOUT_MS);
        String totalTimeoutStr = props.getProperty("client.total-timeout-ms");
        if (totalTimeoutStr != null && !totalTimeoutStr.isBlank()) {
            try {
                totalTimeout = Long.parseLong(totalTimeoutStr.trim());
            } catch (NumberFormatException ignored) {}
        }

        return new RedirectPolicy(maxAttempts, reqTimeout, totalTimeout);
    }
}
