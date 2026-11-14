package vn.nhom6.railway.networking;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock/stub implementation of {@link ResultLookup} for independent testing of timeout and
 * recovery pathways prior to TV2's real D19 lookup delivery (TASK TV3-D14).
 */
public class FakeResultLookup implements ResultLookup {

    private final Map<String, LookupResult> store = new ConcurrentHashMap<>();
    private final AtomicInteger lookupCallCount = new AtomicInteger(0);
    private volatile boolean throwException = false;
    private volatile Exception simulatedException;

    @Override
    public LookupResult lookup(String requestId) throws Exception {
        lookupCallCount.incrementAndGet();
        if (throwException) {
            if (simulatedException != null) {
                throw simulatedException;
            }
            throw new java.io.IOException("Simulated lookup failure for requestId: " + requestId);
        }
        LookupResult result = store.get(requestId);
        if (result != null) {
            return result;
        }
        return LookupResult.unknown(requestId);
    }

    public void registerSuccess(String requestId, String commandType, String ticketId, long appliedIndex) {
        store.put(requestId, LookupResult.appliedSuccess(requestId, commandType, ticketId, appliedIndex));
    }

    public void registerRejected(String requestId, String commandType, String reasonCode, long appliedIndex) {
        store.put(requestId, LookupResult.appliedRejected(requestId, commandType, reasonCode, appliedIndex));
    }

    public void registerPending(String requestId, String commandType) {
        store.put(requestId, LookupResult.pending(requestId, commandType));
    }

    public void registerUnknown(String requestId) {
        store.put(requestId, LookupResult.unknown(requestId));
    }

    public void setThrowException(boolean throwException, Exception ex) {
        this.throwException = throwException;
        this.simulatedException = ex;
    }

    public int getLookupCallCount() {
        return lookupCallCount.get();
    }

    public void reset() {
        store.clear();
        lookupCallCount.set(0);
        throwException = false;
        simulatedException = null;
    }
}
