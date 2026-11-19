package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

/**
 * TV5-D19: Write availability and quorum commit capability policy.
 *
 * <p><strong>SPEC-04 Sections 1, 5, 8 &amp; SPEC-03 Section 7 Rules:</strong>
 * <ul>
 *   <li><strong>Theo dõi khả năng cam kết chứ không dựa vào ping đơn:</strong>
 *       In a 5-node cluster, a single TCP ping to 1 peer only proves 2 nodes are alive (leader + peer).
 *       A 2/5 minority cannot commit writes. Quorum requires at least 3 nodes (majority = {@code N / 2 + 1}).
 *       Write availability is based on verified consensus capability in the current term, not single ping.</li>
 *   <li><strong>Timeout ghi trả UNKNOWN/NO_QUORUM theo tình huống xác minh:</strong>
 *       When a write times out waiting for replication:
 *       <ul>
 *         <li>If verified that quorum cannot be reached (e.g. minority partition 2/5, unreachable peers &lt; quorum):
 *             resolves to {@link TimeoutOutcome#NO_QUORUM} (error code {@code NO_QUORUM}).</li>
 *         <li>If indeterminate (ambiguous delivery, late response, network hiccup):
 *             resolves to {@link TimeoutOutcome#UNKNOWN} (error code {@code TIMEOUT_UNKNOWN}).</li>
 *         <li>If the entry was actually committed by majority despite timeout before reply:
 *             resolves to {@link TimeoutOutcome#COMMITTED}.</li>
 *       </ul>
 *   </li>
 *   <li><strong>Tránh kết luận rollback entry chưa commit:</strong>
 *       In Raft, an uncommitted entry on a leader MUST NEVER be rolled back, truncated, or reported as
 *       {@code REJECTED} upon timeout. The uncommitted entry must remain in the log until either:
 *       (a) it commits when the network heals, or
 *       (b) it is overwritten by an AppendEntries RPC from a newer term leader.
 *       Premature rollback violates linearizability and creates split-brain state.</li>
 * </ul>
 *
 * <p>Integrates directly with {@link CommitAdvancer} (TV4-D16) and {@link LeadershipGuard} (TV5-D16);
 * does not wait for TV4-D19 ReadBarrier.
 */
public final class WriteAvailabilityPolicy {

    /** Default time window during which a peer acknowledgment is considered fresh for quorum. */
    public static final Duration DEFAULT_FRESHNESS_WINDOW = Duration.ofMillis(1500);

    private final String localId;
    private final RaftState state;
    private final Set<String> peers;
    private final ReplicationTracker tracker;
    private final CommitAdvancer commitAdvancer;
    private final LeadershipGuard guard;
    private final Duration freshnessWindow;

    private final Map<String, Instant> peerLastAckTimes = new ConcurrentHashMap<>();
    private final Map<String, Instant> peerLastPingTimes = new ConcurrentHashMap<>();

    /**
     * Write availability evaluation statuses.
     */
    public enum AvailabilityStatus {
        /** Leader has an active quorum of peers and is capable of committing writes. */
        AVAILABLE,
        /** Leader lacks quorum (e.g. 2/5 minority partition, isolated leader). Cannot commit writes. */
        NO_QUORUM,
        /** Node is not leader (FOLLOWER or CANDIDATE). Must redirect to leaderHint. */
        NOT_LEADER
    }

    /**
     * Situations observed when verifying a write timeout.
     */
    public enum TimeoutVerificationSituation {
        /**
         * Quorum loss was definitively verified (e.g. minority partition, unreachable peers &lt; quorum,
         * verified that fewer than 3 nodes are acknowledging in a 5-node cluster).
         */
        VERIFIED_NO_QUORUM,

        /**
         * Indeterminate situation: entry was appended and sent, but responses were delayed, lost, or
         * ambiguous. It is unknown whether the entry was committed by peers or not.
         */
        INDETERMINATE,

        /**
         * Entry was verified to have been successfully committed by majority quorum despite timeout.
         */
        VERIFIED_COMMITTED
    }

    /**
     * Outcome of resolving a write timeout.
     */
    public enum TimeoutOutcome {
        /** Definitively failed to commit due to lack of quorum; returns NO_QUORUM error. */
        NO_QUORUM,
        /** Indeterminate consensus outcome; returns TIMEOUT_UNKNOWN error / UNKNOWN outcome. */
        UNKNOWN,
        /** Entry actually reached majority commit; safe to proceed with business apply. */
        COMMITTED
    }

    /**
     * Structured result of a write availability check.
     */
    public record AvailabilityResult(
            AvailabilityStatus status,
            String leaderHint,
            Long currentTerm,
            int activeVoters,
            int clusterSize,
            int quorumSize,
            String message) {

        public boolean isAvailable() {
            return status == AvailabilityStatus.AVAILABLE;
        }

        public boolean isNoQuorum() {
            return status == AvailabilityStatus.NO_QUORUM;
        }

        public boolean isNotLeader() {
            return status == AvailabilityStatus.NOT_LEADER;
        }
    }

    /**
     * Structured resolution of a write timeout.
     */
    public record WriteTimeoutResolution(
            TimeoutOutcome outcome,
            String errorCode,
            String message,
            long logIndex,
            long term,
            boolean uncommittedEntryRetained) {

        /**
         * Returns whether this resolution represents a definitive lack of quorum.
         */
        public boolean isNoQuorum() {
            return outcome == TimeoutOutcome.NO_QUORUM;
        }

        /**
         * Returns whether this resolution represents an unknown / indeterminate outcome.
         */
        public boolean isUnknown() {
            return outcome == TimeoutOutcome.UNKNOWN;
        }

        /**
         * Returns whether this write was actually committed.
         */
        public boolean isCommitted() {
            return outcome == TimeoutOutcome.COMMITTED;
        }
    }

    /**
     * Creates a new write availability policy with default freshness window.
     */
    public WriteAvailabilityPolicy(
            String localId,
            RaftState state,
            Collection<String> peers,
            ReplicationTracker tracker,
            CommitAdvancer commitAdvancer) {
        this(localId, state, peers, tracker, commitAdvancer, DEFAULT_FRESHNESS_WINDOW);
    }

    /**
     * Creates a new write availability policy with custom freshness window.
     */
    public WriteAvailabilityPolicy(
            String localId,
            RaftState state,
            Collection<String> peers,
            ReplicationTracker tracker,
            CommitAdvancer commitAdvancer,
            Duration freshnessWindow) {
        this.localId = requireNonBlank(localId, "localId");
        this.state = Objects.requireNonNull(state, "state");
        Objects.requireNonNull(peers, "peers");
        Set<String> validatedPeers = new TreeSet<>();
        for (String peer : peers) {
            validatedPeers.add(requireNonBlank(peer, "peer"));
        }
        if (validatedPeers.contains(localId)) {
            throw new IllegalArgumentException("peer set must not contain localId: " + localId);
        }
        this.peers = Collections.unmodifiableSet(validatedPeers);
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.commitAdvancer = Objects.requireNonNull(commitAdvancer, "commitAdvancer");
        this.guard = new LeadershipGuard(state);
        this.freshnessWindow = Objects.requireNonNull(freshnessWindow, "freshnessWindow");
    }

    // -------------------------------------------------------------------------------------------
    // Quorum & Topology Calculations
    // -------------------------------------------------------------------------------------------

    /**
     * Total number of voting nodes in the cluster (local node + configured peers).
     */
    public int clusterSize() {
        return peers.size() + 1;
    }

    /**
     * Majority quorum size (e.g. 5 nodes -&gt; 3, 3 nodes -&gt; 2).
     */
    public int quorumSize() {
        return clusterSize() / 2 + 1;
    }

    public String localId() {
        return localId;
    }

    public Set<String> peers() {
        return peers;
    }

    public RaftState state() {
        return state;
    }

    public ReplicationTracker tracker() {
        return tracker;
    }

    public CommitAdvancer commitAdvancer() {
        return commitAdvancer;
    }

    public LeadershipGuard guard() {
        return guard;
    }

    public Duration freshnessWindow() {
        return freshnessWindow;
    }

    // -------------------------------------------------------------------------------------------
    // Peer Activity & Ping Tracking ("Theo dõi khả năng cam kết chứ không dựa vào ping đơn")
    // -------------------------------------------------------------------------------------------

    /**
     * Records a verified replication or heartbeat acknowledgment from a peer.
     *
     * <p>This acknowledgment actively contributes to quorum commit capability because the peer
     * is actively processing and persisting Raft entries in the current term.
     *
     * @param peerId     the peer ID
     * @param matchIndex the peer's verified match index
     * @param timestamp  timestamp of the acknowledgment
     */
    public void recordPeerAck(String peerId, long matchIndex, Instant timestamp) {
        requireKnownPeer(peerId);
        Objects.requireNonNull(timestamp, "timestamp");
        peerLastAckTimes.put(peerId, timestamp);
        peerLastPingTimes.put(peerId, timestamp);

        if (state.role() == Role.LEADER) {
            tracker.recordSuccess(peerId, matchIndex);
        }
    }

    /**
     * Overload recording peer acknowledgment at the current instant.
     */
    public void recordPeerAck(String peerId, long matchIndex) {
        recordPeerAck(peerId, matchIndex, Instant.now());
    }

    /**
     * Records a simple network ping or transport probe from a peer.
     *
     * <p><strong>CRITICAL SPEC RULE:</strong> A ping alone only proves network reachability to a single peer.
     * In a 5-node cluster:
     * <ul>
     *   <li>Leader + 1 Pinged Peer = 2 nodes = MINORITY (2/5).</li>
     *   <li>Minority CANNOT commit writes!</li>
     * </ul>
     * Therefore, receiving a ping from one node does NOT establish commit quorum.
     *
     * @param peerId    the peer ID
     * @param timestamp timestamp of the ping
     */
    public void recordPeerPing(String peerId, Instant timestamp) {
        requireKnownPeer(peerId);
        Objects.requireNonNull(timestamp, "timestamp");
        peerLastPingTimes.put(peerId, timestamp);
    }

    /**
     * Marks a peer as unresponsive / isolated.
     */
    public void markPeerUnresponsive(String peerId) {
        requireKnownPeer(peerId);
        peerLastAckTimes.remove(peerId);
        peerLastPingTimes.remove(peerId);
    }

    /**
     * Clears all peer acknowledgment history (e.g. on role change or term step-down).
     */
    public void clearPeerActivity() {
        peerLastAckTimes.clear();
        peerLastPingTimes.clear();
    }

    /**
     * Counts active voting nodes participating in commit replication within {@code freshnessWindow}
     * relative to {@code now}.
     *
     * <p>Leader counts as 1 (when {@code role == Role.LEADER}). Followers count only if their
     * verified ACK is fresh within the window.
     */
    public int activeVoterCount(Instant now) {
        if (state.role() != Role.LEADER) {
            return 0;
        }
        int count = 1; // local leader
        for (String peer : peers) {
            Instant lastAck = peerLastAckTimes.get(peer);
            if (lastAck != null && !Duration.between(lastAck, now).isNegative()
                    && Duration.between(lastAck, now).compareTo(freshnessWindow) <= 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Convenience method checking active voter count against current wall clock.
     */
    public int activeVoterCount() {
        return activeVoterCount(Instant.now());
    }

    /**
     * Counts peers reachable only via simple ping (diagnostic comparison).
     */
    public int activePingCount(Instant now) {
        if (state.role() != Role.LEADER) {
            return 0;
        }
        int count = 1; // local leader
        for (String peer : peers) {
            Instant lastPing = peerLastPingTimes.get(peer);
            if (lastPing != null && !Duration.between(lastPing, now).isNegative()
                    && Duration.between(lastPing, now).compareTo(freshnessWindow) <= 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns whether the leader currently maintains an active quorum for commit.
     */
    public boolean hasCommitQuorum(Instant now) {
        return state.role() == Role.LEADER && activeVoterCount(now) >= quorumSize();
    }

    public boolean hasCommitQuorum() {
        return hasCommitQuorum(Instant.now());
    }

    // -------------------------------------------------------------------------------------------
    // Write Availability Assessment
    // -------------------------------------------------------------------------------------------

    /**
     * Checks if the cluster is currently capable of committing writes.
     *
     * @param now the current reference timestamp
     * @return {@link AvailabilityResult} detailing whether writes can be accepted
     */
    public AvailabilityResult checkWriteAvailability(Instant now) {
        if (state.role() != Role.LEADER) {
            String leaderHint = state.leaderId();
            return new AvailabilityResult(
                    AvailabilityStatus.NOT_LEADER,
                    leaderHint,
                    state.currentTerm(),
                    0,
                    clusterSize(),
                    quorumSize(),
                    "Node " + localId + " is not the leader (role=" + state.role()
                            + ", currentTerm=" + state.currentTerm() + ")");
        }

        int activeVoters = activeVoterCount(now);
        int quorum = quorumSize();

        if (activeVoters < quorum) {
            return new AvailabilityResult(
                    AvailabilityStatus.NO_QUORUM,
                    null,
                    state.currentTerm(),
                    activeVoters,
                    clusterSize(),
                    quorum,
                    "Write quorum unavailable: active voters (" + activeVoters
                            + "/" + clusterSize() + ") < majority quorum (" + quorum + ")");
        }

        return new AvailabilityResult(
                AvailabilityStatus.AVAILABLE,
                localId,
                state.currentTerm(),
                activeVoters,
                clusterSize(),
                quorum,
                "Write quorum available: " + activeVoters + "/" + clusterSize() + " voters active");
    }

    public AvailabilityResult checkWriteAvailability() {
        return checkWriteAvailability(Instant.now());
    }

    /**
     * Fast boolean check for whether writes can be accepted at reference timestamp.
     */
    public boolean canCommitWrites(Instant now) {
        return checkWriteAvailability(now).isAvailable();
    }

    /**
     * Fast boolean check for whether writes can be accepted against wall clock.
     */
    public boolean canCommitWrites() {
        return canCommitWrites(Instant.now());
    }

    /**
     * Evaluates whether a specific candidate log index has been replicated to a quorum and
     * can be committed under current-term rules (SPEC-04 Section 5).
     *
     * @param candidateIndex candidate log index
     * @return {@code true} if at least quorum nodes hold the index and the entry is current term
     */
    public boolean canCommitIndex(long candidateIndex) {
        if (state.role() != Role.LEADER) {
            return false;
        }
        if (candidateIndex <= state.commitIndex()) {
            return true; // already committed
        }
        if (candidateIndex > state.lastLogIndex()) {
            return false;
        }
        // SPEC-04 Section 5 Figure 8 rule: Leader cannot commit old-term entries directly
        if (state.termAt(candidateIndex) != state.currentTerm()) {
            return false;
        }

        // Count nodes holding this index (leader itself holds it)
        int matchedCount = 1;
        for (ReplicationTracker.Progress progress : tracker.snapshot().values()) {
            if (progress.matchIndex() >= candidateIndex) {
                matchedCount++;
            }
        }
        return matchedCount >= quorumSize();
    }

    /**
     * Attempts to advance commitIndex via {@link CommitAdvancer} if commit quorum is present.
     *
     * @return newly committed index, or empty if unable to advance
     */
    public OptionalLong tryAdvanceCommit() {
        if (state.role() != Role.LEADER) {
            return OptionalLong.empty();
        }
        return commitAdvancer.advance();
    }

    // -------------------------------------------------------------------------------------------
    // Write Timeout Resolution ("timeout ghi trả UNKNOWN/NO_QUORUM theo tình huống xác minh")
    // -------------------------------------------------------------------------------------------

    /**
     * Resolves the outcome of a timed-out write request based on verified consensus context.
     *
     * @param logIndex         the log index of the command (or 0 if not appended)
     * @param entryTerm        the term when the command was submitted
     * @param appendedLocally  whether the command was persisted to the local Raft log
     * @param situation        the verified situation at timeout
     * @return {@link WriteTimeoutResolution} with exact error code, outcome, and invariant guarantees
     */
    public WriteTimeoutResolution resolveTimeout(
            long logIndex,
            long entryTerm,
            boolean appendedLocally,
            TimeoutVerificationSituation situation) {

        // 1. Check if the entry was actually committed despite timeout
        if (logIndex > 0L && state.commitIndex() >= logIndex) {
            return new WriteTimeoutResolution(
                    TimeoutOutcome.COMMITTED,
                    null,
                    "The command at index " + logIndex + " was successfully committed by quorum",
                    logIndex,
                    entryTerm,
                    true);
        }

        // 2. Verified NO_QUORUM situation:
        // Quorum loss was definitively confirmed (e.g. 2/5 minority partition, unreachable peers).
        // Or write was rejected before append because active voter count < quorum.
        if (situation == TimeoutVerificationSituation.VERIFIED_NO_QUORUM || !appendedLocally) {
            return new WriteTimeoutResolution(
                    TimeoutOutcome.NO_QUORUM,
                    "NO_QUORUM",
                    "The command could not be committed by a quorum",
                    logIndex,
                    entryTerm,
                    true); // SAFETY: uncommitted entry is NOT rolled back!
        }

        // 3. Indeterminate situation:
        // Entry was appended and sent, but responses were delayed, lost, or leader stepped down.
        // It is unknown whether the entry reached quorum on other nodes.
        return new WriteTimeoutResolution(
                TimeoutOutcome.UNKNOWN,
                "TIMEOUT_UNKNOWN",
                "Write timed out with indeterminate consensus outcome; query or retry with same requestId",
                logIndex,
                entryTerm,
                true); // SAFETY: uncommitted entry is NOT rolled back!
    }

    /**
     * Convenience overload for binary verifiedNoQuorum flag.
     */
    public WriteTimeoutResolution resolveTimeout(
            long logIndex,
            long entryTerm,
            boolean appendedLocally,
            boolean verifiedNoQuorum) {
        TimeoutVerificationSituation situation = verifiedNoQuorum
                ? TimeoutVerificationSituation.VERIFIED_NO_QUORUM
                : TimeoutVerificationSituation.INDETERMINATE;
        return resolveTimeout(logIndex, entryTerm, appendedLocally, situation);
    }

    // -------------------------------------------------------------------------------------------
    // Rollback Prevention Safety Invariants ("tránh kết luận rollback entry chưa commit")
    // -------------------------------------------------------------------------------------------

    /**
     * Invariant verification: In Raft, an uncommitted entry on a leader MUST NEVER be rolled back
     * or discarded locally upon client timeout.
     *
     * <p><strong>SPEC-04 Raft Safety:</strong>
     * An uncommitted entry must remain in the leader's log. If the partition heals, the leader may
     * still commit it. If a new leader is elected, that new leader will safely overwrite it via
     * standard AppendEntries consistency rules. Local premature rollback violates linearizability.
     *
     * @param logIndex the index of the uncommitted entry
     * @return {@code false} always; rollback of uncommitted entries is strictly forbidden
     */
    public boolean canRollbackUncommitted(long logIndex) {
        return false;
    }

    /**
     * Asserts that an uncommitted log entry has NOT been truncated or rolled back from local state.
     *
     * @param logIndex the log index to verify
     * @throws IllegalStateException if the entry was illegally truncated from the log
     */
    public void assertUncommittedEntryNotRolledBack(long logIndex) {
        if (logIndex <= 0L) {
            return; // was not appended
        }
        if (state.lastLogIndex() < logIndex) {
            throw new IllegalStateException("CRITICAL RAFT SAFETY VIOLATION: Uncommitted entry at index "
                    + logIndex + " was illegally rolled back or truncated! lastLogIndex=" + state.lastLogIndex());
        }
    }

    /**
     * Invariant check: A timeout or quorum failure must NEVER report {@code REJECTED} (e.g.
     * {@code SEAT_ALREADY_BOOKED} or business rejection) for a command that simply failed to reach quorum.
     *
     * <p>It must only report {@code NO_QUORUM} or {@code TIMEOUT_UNKNOWN}.
     *
     * @return {@code false} always
     */
    public boolean canReportRejectedOnTimeout() {
        return false;
    }

    // -------------------------------------------------------------------------------------------
    // Validation Helpers
    // -------------------------------------------------------------------------------------------

    private void requireKnownPeer(String peerId) {
        if (!peers.contains(requireNonBlank(peerId, "peerId"))) {
            throw new IllegalArgumentException("unknown peer: " + peerId + "; configured peers: " + peers);
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        return value;
    }
}
