package vn.nhom6.railway.networking;

import com.fasterxml.jackson.databind.JsonNode;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.ErrorResponseDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe correlation registry pairing outgoing asynchronous Raft RPC requests
 * with incoming responses via {@code correlationId}.
 *
 * <p>Key capabilities:
 * <ul>
 *   <li>Asynchronous completion via {@link CompletableFuture}.</li>
 *   <li>Per-peer backpressure control (rejects requests when in-flight limit is reached).</li>
 *   <li>Scheduled deadline timeouts completing with {@link TimeoutException}.</li>
 *   <li>Bulk failure of pending requests when a peer connection is severed.</li>
 *   <li>Safe discarding of stale, late, or unregistered correlation responses.</li>
 * </ul>
 */
public class RpcCorrelationRegistry implements AutoCloseable {

    public static final int DEFAULT_MAX_IN_FLIGHT_PER_PEER = 64;

    private final int maxInFlightPerPeer;
    private final ConcurrentMap<String, PendingRpc<?>> pendingRpcs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> inFlightPerPeer = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timeoutScheduler;
    private volatile boolean closed = false;

    public RpcCorrelationRegistry() {
        this(DEFAULT_MAX_IN_FLIGHT_PER_PEER);
    }

    public RpcCorrelationRegistry(int maxInFlightPerPeer) {
        if (maxInFlightPerPeer <= 0) {
            throw new IllegalArgumentException("maxInFlightPerPeer must be positive: " + maxInFlightPerPeer);
        }
        this.maxInFlightPerPeer = maxInFlightPerPeer;
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-rpc-timeout-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Registers a pending asynchronous RPC expectation.
     *
     * @param correlationId Expected correlation ID (must match request's messageId)
     * @param peerId Target peer ID
     * @param timeout Request deadline
     * @param responsePayloadClass Expected response payload DTO class
     * @param <T> Response payload type
     * @return CompletableFuture completing when response is received or failing on timeout/backpressure
     * @throws RpcBackpressureException if the in-flight limit for the peer is reached
     */
    public <T> CompletableFuture<MessageEnvelope<T>> register(
            String correlationId,
            String peerId,
            Duration timeout,
            Class<T> responsePayloadClass) throws RpcBackpressureException {

        if (closed) {
            throw new IllegalStateException("RpcCorrelationRegistry is closed");
        }
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(peerId, "peerId must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        Objects.requireNonNull(responsePayloadClass, "responsePayloadClass must not be null");

        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be strictly positive");
        }

        // 1. Backpressure check with atomic CAS
        AtomicInteger counter = inFlightPerPeer.computeIfAbsent(peerId, k -> new AtomicInteger(0));
        int prev;
        do {
            prev = counter.get();
            if (prev >= maxInFlightPerPeer) {
                throw new RpcBackpressureException(peerId, prev, maxInFlightPerPeer);
            }
        } while (!counter.compareAndSet(prev, prev + 1));

        // 2. Create future and schedule timeout task
        CompletableFuture<MessageEnvelope<T>> future = new CompletableFuture<>();
        ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
            onTimeout(correlationId, peerId, timeout);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        PendingRpc<T> pending = new PendingRpc<>(correlationId, peerId, responsePayloadClass, future, timeoutTask);
        pendingRpcs.put(correlationId, pending);

        // Clean up counter if caller manually cancels future
        future.whenComplete((res, err) -> {
            if (future.isCancelled()) {
                PendingRpc<?> removed = pendingRpcs.remove(correlationId);
                if (removed != null) {
                    removed.timeoutTask.cancel(false);
                    decrementInFlight(peerId);
                }
            }
        });

        return future;
    }

    /**
     * Completes a pending RPC when an incoming response envelope arrives.
     *
     * @param correlationId Response correlationId
     * @param responseEnvelope Incoming response envelope
     * @return {@code true} if a matching pending RPC was found and completed; {@code false} if stale/unknown
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean complete(String correlationId, MessageEnvelope<?> responseEnvelope) {
        if (correlationId == null || responseEnvelope == null) {
            return false;
        }

        PendingRpc<?> pending = pendingRpcs.remove(correlationId);
        if (pending == null) {
            // Stale or already timed out response - safe to ignore
            return false;
        }

        pending.timeoutTask.cancel(false);
        decrementInFlight(pending.peerId);

        try {
            // Check if response is an error response
            if (responseEnvelope.getType() == MessageType.ERROR_RESPONSE) {
                String errorMsg = "RPC failed with ERROR_RESPONSE";
                if (responseEnvelope.getPayload() != null) {
                    if (responseEnvelope.getPayload() instanceof ErrorResponseDto errDto) {
                        errorMsg = "RPC failed: " + errDto.getErrorCode() + " - " + errDto.getMessage();
                    } else if (responseEnvelope.getPayload() instanceof JsonNode jsonNode) {
                        ErrorResponseDto errDto = MessageCodec.getMapper().treeToValue(jsonNode, ErrorResponseDto.class);
                        errorMsg = "RPC failed: " + errDto.getErrorCode() + " - " + errDto.getMessage();
                    }
                }
                pending.future.completeExceptionally(new IOException(errorMsg));
                return true;
            }

            // Convert / cast payload to expected responsePayloadClass
            Object payload = responseEnvelope.getPayload();
            Object typedPayload;
            if (payload == null) {
                typedPayload = null;
            } else if (pending.responsePayloadClass.isInstance(payload)) {
                typedPayload = payload;
            } else if (payload instanceof JsonNode jsonNode) {
                typedPayload = MessageCodec.getMapper().treeToValue(jsonNode, pending.responsePayloadClass);
            } else {
                // Try converting arbitrary object via mapper
                typedPayload = MessageCodec.getMapper().convertValue(payload, pending.responsePayloadClass);
            }

            MessageEnvelope<?> typedEnvelope = new MessageEnvelope<>(
                    responseEnvelope.getProtocolVersion(),
                    responseEnvelope.getMessageId(),
                    responseEnvelope.getCorrelationId(),
                    responseEnvelope.getType(),
                    responseEnvelope.getSenderId(),
                    responseEnvelope.getSentAt(),
                    typedPayload
            );

            ((CompletableFuture) pending.future).complete(typedEnvelope);
            return true;
        } catch (Exception e) {
            pending.future.completeExceptionally(new IOException("Failed to deserialize response payload: " + e.getMessage(), e));
            return true;
        }
    }

    /**
     * Fails a specific pending RPC.
     */
    public boolean fail(String correlationId, Throwable cause) {
        if (correlationId == null) {
            return false;
        }
        PendingRpc<?> pending = pendingRpcs.remove(correlationId);
        if (pending == null) {
            return false;
        }
        pending.timeoutTask.cancel(false);
        decrementInFlight(pending.peerId);
        pending.future.completeExceptionally(cause);
        return true;
    }

    /**
     * Fails all pending RPCs for a given peer (e.g. when peer connection closes or resets).
     */
    public void failAll(String peerId, Throwable cause) {
        if (peerId == null) {
            return;
        }
        pendingRpcs.entrySet().removeIf(entry -> {
            PendingRpc<?> pending = entry.getValue();
            if (pending.peerId.equals(peerId)) {
                pending.timeoutTask.cancel(false);
                decrementInFlight(peerId);
                pending.future.completeExceptionally(cause);
                return true;
            }
            return false;
        });
    }

    private void onTimeout(String correlationId, String peerId, Duration timeout) {
        PendingRpc<?> pending = pendingRpcs.remove(correlationId);
        if (pending != null) {
            decrementInFlight(peerId);
            pending.future.completeExceptionally(new TimeoutException(
                    String.format("RPC correlationId=%s to peer=%s timed out after %d ms",
                            correlationId, peerId, timeout.toMillis())));
        }
    }

    private void decrementInFlight(String peerId) {
        AtomicInteger counter = inFlightPerPeer.get(peerId);
        if (counter != null) {
            counter.updateAndGet(c -> Math.max(0, c - 1));
        }
    }

    public int getInFlightCount(String peerId) {
        AtomicInteger counter = inFlightPerPeer.get(peerId);
        return counter != null ? counter.get() : 0;
    }

    public int getTotalInFlightCount() {
        return pendingRpcs.size();
    }

    public boolean hasPending(String correlationId) {
        return correlationId != null && pendingRpcs.containsKey(correlationId);
    }

    public int getMaxInFlightPerPeer() {
        return maxInFlightPerPeer;
    }

    @Override
    public void close() {
        closed = true;
        timeoutScheduler.shutdownNow();
        CancellationException ex = new CancellationException("RpcCorrelationRegistry has been closed");
        pendingRpcs.forEach((k, v) -> {
            v.timeoutTask.cancel(false);
            v.future.completeExceptionally(ex);
        });
        pendingRpcs.clear();
        inFlightPerPeer.clear();
    }

    private record PendingRpc<T>(
            String correlationId,
            String peerId,
            Class<T> responsePayloadClass,
            CompletableFuture<MessageEnvelope<T>> future,
            ScheduledFuture<?> timeoutTask
    ) {
    }
}
