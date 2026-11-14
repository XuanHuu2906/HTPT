package vn.nhom6.railway.networking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Unit tests for RetryPolicy (TASK TV3-D14)")
public class RetryPolicyTest {

    @Test
    @DisplayName("Should initialize default policy with valid constants")
    public void testDefaultPolicy() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        assertEquals(3, policy.maxAttempts());
        assertEquals(100L, policy.initialBackoffMs());
        assertEquals(1000L, policy.maxBackoffMs());
        assertEquals(2.0, policy.backoffMultiplier());
        assertEquals(3000, policy.connectTimeoutMs());
        assertEquals(5000, policy.readTimeoutMs());
    }

    @Test
    @DisplayName("Should initialize noRetry policy with 1 attempt")
    public void testNoRetryPolicy() {
        RetryPolicy policy = RetryPolicy.noRetry();
        assertEquals(1, policy.maxAttempts());
        assertFalse(policy.canRetry(0));
        assertEquals(0L, policy.computeBackoffMs(0));
    }

    @Test
    @DisplayName("Should enforce strict parameter validations")
    public void testParameterValidations() {
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(0, 100, 1000, 2.0, 3000, 5000));
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(3, -1, 1000, 2.0, 3000, 5000));
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(3, 2000, 1000, 2.0, 3000, 5000));
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(3, 100, 1000, 0.5, 3000, 5000));
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(3, 100, 1000, 2.0, 0, 5000));
        assertThrows(IllegalArgumentException.class, () ->
                new RetryPolicy(3, 100, 1000, 2.0, 3000, 0));
    }

    @Test
    @DisplayName("Should correctly compute exponential backoff bounded by maxBackoffMs")
    public void testComputeBackoff() {
        RetryPolicy policy = new RetryPolicy(5, 100L, 1000L, 2.0, 3000, 5000);
        assertEquals(100L, policy.computeBackoffMs(0));
        assertEquals(200L, policy.computeBackoffMs(1));
        assertEquals(400L, policy.computeBackoffMs(2));
        assertEquals(800L, policy.computeBackoffMs(3));
        assertEquals(1000L, policy.computeBackoffMs(4), "Backoff must be capped at maxBackoffMs");
        assertEquals(1000L, policy.computeBackoffMs(5));
    }

    @Test
    @DisplayName("Should check canRetry correctly")
    public void testCanRetry() {
        RetryPolicy policy = new RetryPolicy(3, 100L, 1000L, 2.0, 3000, 5000);
        assertTrue(policy.canRetry(0));
        assertTrue(policy.canRetry(1));
        assertFalse(policy.canRetry(2));
        assertFalse(policy.canRetry(3));
    }

    @Test
    @DisplayName("Should correctly classify transient connection errors")
    public void testTransientConnectionErrors() {
        assertTrue(RetryPolicy.isTransientConnectionError(new ConnectException("Connection refused")));
        assertTrue(RetryPolicy.isTransientConnectionError(new NoRouteToHostException("No route to host")));
        assertTrue(RetryPolicy.isTransientConnectionError(new PortUnreachableException("Port unreachable")));
        assertTrue(RetryPolicy.isTransientConnectionError(new SocketException("Connection reset by peer")));
        assertTrue(RetryPolicy.isTransientConnectionError(new SocketException("connection refused")));

        // Read timeout is NOT a transient connection-establishment error
        assertFalse(RetryPolicy.isTransientConnectionError(new SocketTimeoutException("Read timed out")));
        assertFalse(RetryPolicy.isTransientConnectionError(new NullPointerException("NPE")));
        assertFalse(RetryPolicy.isTransientConnectionError(null));
    }

    @Test
    @DisplayName("Should correctly classify socket timeouts")
    public void testTimeoutClassification() {
        assertTrue(RetryPolicy.isTimeout(new SocketTimeoutException("Read timed out")));
        assertTrue(RetryPolicy.isTimeout(new TimeoutException("Operation timed out")));
        assertTrue(RetryPolicy.isTimeout(new RuntimeException(new SocketTimeoutException("Nested timeout"))));

        assertFalse(RetryPolicy.isTimeout(new ConnectException("Refused")));
        assertFalse(RetryPolicy.isTimeout(new IllegalArgumentException("Invalid")));
        assertFalse(RetryPolicy.isTimeout(null));
    }
}
