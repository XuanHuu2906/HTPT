package vn.nhom6.railway.networking;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Encapsulates timeout limits, backoff rules, and retry eligibility for network connections (TASK TV3-D14).
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li>Distinguishes transient connection-establishment errors from in-flight read/write timeouts.</li>
 *   <li>Computes deterministic exponential backoff bounded by {@code maxBackoffMs}.</li>
 *   <li>Enforces positive socket connect timeout and read timeout ({@code SO_TIMEOUT}).</li>
 * </ul>
 */
public record RetryPolicy(
        int maxAttempts,
        long initialBackoffMs,
        long maxBackoffMs,
        double backoffMultiplier,
        int connectTimeoutMs,
        int readTimeoutMs
) {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_MS = 100L;
    public static final long DEFAULT_MAX_BACKOFF_MS = 1_000L;
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 3_000;
    public static final int DEFAULT_READ_TIMEOUT_MS = 5_000;

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got: " + maxAttempts);
        }
        if (initialBackoffMs < 0) {
            throw new IllegalArgumentException("initialBackoffMs must not be negative, got: " + initialBackoffMs);
        }
        if (maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException("maxBackoffMs (" + maxBackoffMs + ") must be >= initialBackoffMs (" + initialBackoffMs + ")");
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0, got: " + backoffMultiplier);
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be > 0, got: " + connectTimeoutMs);
        }
        if (readTimeoutMs <= 0) {
            throw new IllegalArgumentException("readTimeoutMs must be > 0, got: " + readTimeoutMs);
        }
    }

    /**
     * Default policy for LAN cluster: 3 attempts, 100ms initial backoff, 1s max, 3s connect timeout, 5s read timeout.
     */
    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(
                DEFAULT_MAX_ATTEMPTS,
                DEFAULT_INITIAL_BACKOFF_MS,
                DEFAULT_MAX_BACKOFF_MS,
                DEFAULT_BACKOFF_MULTIPLIER,
                DEFAULT_CONNECT_TIMEOUT_MS,
                DEFAULT_READ_TIMEOUT_MS
        );
    }

    /**
     * Single-attempt policy with zero retry, retaining standard timeouts.
     */
    public static RetryPolicy noRetry() {
        return new RetryPolicy(
                1,
                0L,
                0L,
                1.0,
                DEFAULT_CONNECT_TIMEOUT_MS,
                DEFAULT_READ_TIMEOUT_MS
        );
    }

    /**
     * Custom builder helper for quick policy construction.
     */
    public static RetryPolicy withTimeouts(int connectTimeoutMs, int readTimeoutMs) {
        return new RetryPolicy(
                DEFAULT_MAX_ATTEMPTS,
                DEFAULT_INITIAL_BACKOFF_MS,
                DEFAULT_MAX_BACKOFF_MS,
                DEFAULT_BACKOFF_MULTIPLIER,
                connectTimeoutMs,
                readTimeoutMs
        );
    }

    /**
     * Checks if the given attempt index (0-based) can still be retried.
     */
    public boolean canRetry(int attempt) {
        return attempt + 1 < maxAttempts;
    }

    /**
     * Computes the exponential backoff duration in milliseconds for the given 0-based attempt index.
     */
    public long computeBackoffMs(int attempt) {
        if (attempt <= 0 || initialBackoffMs == 0) {
            return initialBackoffMs;
        }
        double backoff = initialBackoffMs * Math.pow(backoffMultiplier, attempt);
        return Math.min(maxBackoffMs, Math.round(backoff));
    }

    /**
     * Determines whether an error is a transient connection-establishment error safe to retry before dispatch.
     */
    public static boolean isTransientConnectionError(Throwable ex) {
        if (ex == null) {
            return false;
        }
        if (ex instanceof ConnectException
                || ex instanceof NoRouteToHostException
                || ex instanceof PortUnreachableException) {
            return true;
        }
        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
        if (ex instanceof SocketException && (msg.contains("refused") || msg.contains("reset") || msg.contains("unreachable"))) {
            return true;
        }
        return false;
    }

    /**
     * Determines whether an error is a socket or read/write timeout.
     */
    public static boolean isTimeout(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof SocketTimeoutException || current instanceof TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
