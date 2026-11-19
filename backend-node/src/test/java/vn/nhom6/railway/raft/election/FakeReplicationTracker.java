package vn.nhom6.railway.raft.election;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

/**
 * TV5-D19: Test double for replication tracking and quorum commit simulations.
 *
 * <p>Enables deterministic testing of:
 * <ul>
 *   <li>3/5 majority quorum commit vs 2/5 minority commit failure</li>
 *   <li>Simulated peer ACKs, isolated peers, network partitions</li>
 *   <li>Write availability evaluation and timeout resolution without real network sockets</li>
 * </ul>
 */
public final class FakeReplicationTracker {

    private final String leaderId;
    private final RaftState state;
    private final Set<String> peers;
    private final ReplicationTracker realTracker;
    private final CommitAdvancer commitAdvancer;
    private final WriteAvailabilityPolicy policy;
    private final Set<String> isolatedPeers = new TreeSet<>();
    private final Map<String, Long> recordedAcks = new LinkedHashMap<>();

    public FakeReplicationTracker(String leaderId, RaftState state, Collection<String> peers) {
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        this.state = Objects.requireNonNull(state, "state");
        Objects.requireNonNull(peers, "peers");
        this.peers = Collections.unmodifiableSet(new TreeSet<>(peers));
        this.realTracker = new ReplicationTracker(state, this.peers);
        this.commitAdvancer = new CommitAdvancer(state, realTracker);
        this.policy = new WriteAvailabilityPolicy(leaderId, state, this.peers, realTracker, commitAdvancer);
    }

    public ReplicationTracker tracker() {
        return realTracker;
    }

    public CommitAdvancer commitAdvancer() {
        return commitAdvancer;
    }

    public WriteAvailabilityPolicy policy() {
        return policy;
    }

    public Set<String> peers() {
        return peers;
    }

    public RaftState state() {
        return state;
    }

    public String leaderId() {
        return leaderId;
    }

    /**
     * Simulates receiving an AppendEntries ACK from a peer.
     *
     * @return {@code true} if ACK was accepted and matchIndex advanced
     */
    public boolean acknowledge(String peer, long matchIndex, Instant timestamp) {
        if (isolatedPeers.contains(peer)) {
            return false;
        }
        boolean advanced = policy.tracker().recordSuccess(peer, matchIndex);
        policy.recordPeerAck(peer, matchIndex, timestamp);
        recordedAcks.put(peer, matchIndex);
        return advanced;
    }

    public boolean acknowledge(String peer, long matchIndex) {
        return acknowledge(peer, matchIndex, Instant.now());
    }

    /**
     * Simulates receiving a simple ping from a peer (no matchIndex advancement).
     */
    public void ping(String peer, Instant timestamp) {
        if (!isolatedPeers.contains(peer)) {
            policy.recordPeerPing(peer, timestamp);
        }
    }

    public void ping(String peer) {
        ping(peer, Instant.now());
    }

    /**
     * Isolates a peer from communicating with the leader.
     */
    public void isolate(String peer) {
        isolatedPeers.add(peer);
        policy.markPeerUnresponsive(peer);
    }

    /**
     * Reconnects a previously isolated peer.
     */
    public void heal(String peer) {
        isolatedPeers.remove(peer);
    }

    /**
     * Reconnects all isolated peers.
     */
    public void healAll() {
        isolatedPeers.clear();
    }

    public boolean isIsolated(String peer) {
        return isolatedPeers.contains(peer);
    }

    /**
     * Partitions the cluster: isolates all peers belonging to {@code otherPartition}.
     */
    public void partition(Set<String> thisPartition, Set<String> otherPartition) {
        for (String peer : peers) {
            if (otherPartition.contains(peer)) {
                isolate(peer);
            }
        }
    }

    /**
     * Simulates verified ACKs from exactly {@code k} available peers.
     */
    public int acknowledgeKPeers(int k, long matchIndex, Instant timestamp) {
        int count = 0;
        for (String peer : peers) {
            if (count >= k) {
                break;
            }
            if (!isolatedPeers.contains(peer)) {
                acknowledge(peer, matchIndex, timestamp);
                count++;
            }
        }
        return count;
    }

    public int acknowledgeKPeers(int k, long matchIndex) {
        return acknowledgeKPeers(k, matchIndex, Instant.now());
    }

    /**
     * Simulates verified ACKs from enough peers to reach quorum (for 5 nodes: 2 peers + leader = 3).
     */
    public void acknowledgeQuorum(long matchIndex, Instant timestamp) {
        int peersNeeded = policy.quorumSize() - 1;
        acknowledgeKPeers(peersNeeded, matchIndex, timestamp);
    }

    public void acknowledgeQuorum(long matchIndex) {
        acknowledgeQuorum(matchIndex, Instant.now());
    }

    /**
     * Simulates minority ACKs (for 5 nodes: exactly 1 peer + leader = 2 nodes &lt; 3).
     */
    public void acknowledgeMinority(long matchIndex, Instant timestamp) {
        int minorityPeers = Math.max(1, policy.quorumSize() - 2);
        acknowledgeKPeers(minorityPeers, matchIndex, timestamp);
    }

    public void acknowledgeMinority(long matchIndex) {
        acknowledgeMinority(matchIndex, Instant.now());
    }

    /**
     * Simulates verified ACKs from all non-isolated peers.
     */
    public void acknowledgeAll(long matchIndex, Instant timestamp) {
        for (String peer : peers) {
            if (!isolatedPeers.contains(peer)) {
                acknowledge(peer, matchIndex, timestamp);
            }
        }
    }

    public void acknowledgeAll(long matchIndex) {
        acknowledgeAll(matchIndex, Instant.now());
    }

    /**
     * Advances commit index using CommitAdvancer.
     */
    public OptionalLong tryAdvanceCommit() {
        return policy.tryAdvanceCommit();
    }

    public Map<String, Long> recordedAcks() {
        return Map.copyOf(recordedAcks);
    }
}
