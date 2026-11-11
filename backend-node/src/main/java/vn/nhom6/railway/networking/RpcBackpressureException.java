package vn.nhom6.railway.networking;

import java.io.IOException;

/**
 * Exception thrown when the number of concurrent in-flight RPCs for a peer exceeds the backpressure limit.
 */
public class RpcBackpressureException extends IOException {

    private final String peerId;
    private final int inFlightCount;
    private final int maxInFlight;

    public RpcBackpressureException(String peerId, int inFlightCount, int maxInFlight) {
        super(String.format("RPC backpressure triggered for peer '%s': current in-flight %d reached limit %d",
                peerId, inFlightCount, maxInFlight));
        this.peerId = peerId;
        this.inFlightCount = inFlightCount;
        this.maxInFlight = maxInFlight;
    }

    public String getPeerId() {
        return peerId;
    }

    public int getInFlightCount() {
        return inFlightCount;
    }

    public int getMaxInFlight() {
        return maxInFlight;
    }
}
