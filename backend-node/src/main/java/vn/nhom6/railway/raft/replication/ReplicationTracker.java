package vn.nhom6.railway.raft.replication;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Leader replication progress operations backed by the authoritative volatile indices in
 * {@link RaftState} (SPEC-04 section 4).
 *
 * <p>This type does not cache peer progress or calculate a commit index. All mutations must run on
 * the serialized Raft event loop.
 */
public final class ReplicationTracker {

    private final RaftState state;
    private final Set<String> peers;

    public ReplicationTracker(RaftState state, Collection<String> peers) {
        this.state = Objects.requireNonNull(state, "state");
        Objects.requireNonNull(peers, "peers");

        Set<String> configuredPeers = new TreeSet<>();
        for (String peer : peers) {
            if (peer == null || peer.isBlank()) {
                throw new IllegalArgumentException("peer ids must not be blank");
            }
            if (!configuredPeers.add(peer)) {
                throw new IllegalArgumentException("peer list contains duplicate node id: " + peer);
            }
        }

        Set<String> statePeers = state.nextIndex().keySet();
        if (!configuredPeers.equals(statePeers) || !statePeers.equals(state.matchIndex().keySet())) {
            throw new IllegalArgumentException("tracker peers must exactly match RaftState peer indices: "
                    + configuredPeers + " != " + statePeers);
        }
        this.peers = Set.copyOf(configuredPeers);
    }

    /**
     * Advances a peer's verified match index. Equal or older successes are ignored.
     *
     * @return {@code true} only when {@code matchIndex} advanced
     * @throws IllegalStateException when the local node is not Leader
     * @throws IllegalArgumentException when the peer is unknown, the index is negative or exceeds
     *                                  the local log
     */
    public boolean recordSuccess(String peer, long verifiedIndex) {
        requireLeader();
        requirePeer(peer);
        if (verifiedIndex < RaftState.EMPTY_LOG_INDEX || verifiedIndex > state.lastLogIndex()) {
            throw new IllegalArgumentException("verifiedIndex must be within [0, lastLogIndex="
                    + state.lastLogIndex() + "] but was " + verifiedIndex);
        }
        if (verifiedIndex <= state.matchIndexOf(peer)) {
            return false;
        }
        state.advanceMatchIndex(peer, verifiedIndex);
        return true;
    }

    /**
     * Compare-and-sets a peer's {@code nextIndex} to a lower value, so a stale rejection cannot
     * overwrite progress made by a newer reply.
     *
     * @return {@code true} when the expected current value matched and the index was lowered;
     *         {@code false} when the current value no longer equals {@code expectedCurrent}
     * @throws IllegalStateException when the local node is not Leader
     * @throws IllegalArgumentException when the peer is unknown or an index is outside
     *                                  {@code [matchIndex + 1, expectedCurrent - 1]}
     */
    public boolean lowerNextIndex(String peer, long expectedCurrent, long newNext) {
        requireLeader();
        requirePeer(peer);
        long minimum = Math.addExact(state.matchIndexOf(peer), 1L);
        long maximum = Math.addExact(state.lastLogIndex(), 1L);
        if (expectedCurrent < 1L || newNext < minimum || newNext < 1L
                || newNext >= expectedCurrent || newNext > maximum) {
            throw new IllegalArgumentException("newNext must be in [" + minimum + ", "
                    + Math.min(expectedCurrent - 1L, maximum) + "] but was " + newNext);
        }
        if (state.nextIndexOf(peer) != expectedCurrent) {
            return false;
        }
        state.updateNextIndex(peer, newNext);
        return true;
    }

    public long nextIndexOf(String peer) {
        requirePeer(peer);
        return state.nextIndexOf(peer);
    }

    public long matchIndexOf(String peer) {
        requirePeer(peer);
        return state.matchIndexOf(peer);
    }

    /** Returns an immutable snapshot of the current Leader peer indices. */
    public Map<String, Progress> snapshot() {
        return peers.stream().collect(Collectors.toUnmodifiableMap(
                peer -> peer,
                peer -> new Progress(state.nextIndexOf(peer), state.matchIndexOf(peer))));
    }

    private void requireLeader() {
        if (state.role() != Role.LEADER) {
            throw new IllegalStateException("replication progress is Leader-only; role=" + state.role());
        }
    }

    private void requirePeer(String peer) {
        if (peer == null || !peers.contains(peer)) {
            throw new IllegalArgumentException("unknown peer '" + peer + "'; allowed peers=" + peers);
        }
    }

    public record Progress(long nextIndex, long matchIndex) {
    }
}
