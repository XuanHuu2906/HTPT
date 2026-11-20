package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityResult;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityStatus;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutOutcome;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutVerificationSituation;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.WriteTimeoutResolution;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

/**
 * TV5-D20: High-level Raft Election, Recovery, and Quorum Write Availability Service.
 *
 * <p>Coordinates election campaigns, split-vote recovery, leader re-election, higher-term step-down,
 * and quorum write availability verification (3/5 majority vs 2/5 minority) per SPEC-04 and SPEC-03.
 *
 * <p><strong>Key Capabilities (TV5-D20 before G5):</strong>
 * <ul>
 *   <li><strong>Election and Candidate Lifecycle:</strong> Implements {@link ElectionService} with durable
 *       term/vote transitions, candidate campaigning, and majority vote counting.</li>
 *   <li><strong>Split Vote &amp; Re-election:</strong> Handles split votes in campaigns with retry logic across terms.</li>
 *   <li><strong>Leader Re-election &amp; Step-Down:</strong> Detects higher terms from peer traffic or delayed responses,
 *       durably records new terms with cleared votes, steps down from LEADER/CANDIDATE to FOLLOWER,
 *       and guards callbacks against stale terms via {@link LeadershipGuard}.</li>
 *   <li><strong>Quorum Write Availability Integration (TV5-D19):</strong> Integrates with
 *       {@link WriteAvailabilityPolicy} to enforce that only leaders with verified 3/5 active commit capability
 *       can accept and commit writes; 2/5 minority partitions report {@code NO_QUORUM} without advancing commitIndex.</li>
 *   <li><strong>Durable Recovery:</strong> Restores persistent election state (term, vote, log) safely upon node restart.</li>
 *   <li><strong>G5 Fault Scenario Preparation:</strong> Prepares fault simulation hooks for leader crashes,
 *       follower partitions (3-2 split), and network healing.</li>
 * </ul>
 */
public final class RaftElectionService implements ElectionService {

    public static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    public static final int CLUSTER_SIZE = 5;
    public static final int REQUIRED_VOTES = 3; // strict majority: 5 / 2 + 1 = 3

    private final String localId;
    private final RaftNode node;
    private final RaftState state;
    private final RaftStorage storage;
    private final Set<String> peers;
    private final CandidateElection candidateElection;
    private final PersistentElectionState persistence;
    private final LeadershipGuard leadershipGuard;
    private final Runnable onLeader;
    private final Runnable onStepDown;
    private final CandidateElection.VoteTransport transport;

    private volatile WriteAvailabilityPolicy writeAvailabilityPolicy;
    private final Set<String> currentGrants = new CopyOnWriteArraySet<>();
    private final Map<String, Instant> peerAckTimes = new ConcurrentHashMap<>();
    private final Map<String, Instant> peerPingTimes = new ConcurrentHashMap<>();
    private final Set<String> partitionIsolatedPeers = new CopyOnWriteArraySet<>();
    private volatile boolean failed;

    /**
     * Primary constructor with all dependencies.
     */
    public RaftElectionService(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            WriteAvailabilityPolicy writeAvailabilityPolicy) {
        this.localId = Objects.requireNonNull(localId, "localId");
        if (!VOTERS.contains(localId)) {
            throw new IllegalArgumentException("localId must be one of the five configured voters: " + VOTERS);
        }
        this.node = Objects.requireNonNull(node, "node");
        this.state = Objects.requireNonNull(state, "state");
        this.storage = storage;

        Set<String> remotePeers = new TreeSet<>(VOTERS);
        remotePeers.remove(localId);
        this.peers = Collections.unmodifiableSet(remotePeers);

        this.onLeader = onLeader != null ? onLeader : () -> {};
        this.onStepDown = onStepDown != null ? onStepDown : () -> {};
        this.transport = transport != null ? transport : (peer, req, reply) -> {};
        this.writeAvailabilityPolicy = writeAvailabilityPolicy;

        this.persistence = storage != null
                ? (node instanceof PersistentRaftNode p ? p.persistence() : new PersistentElectionState(state, storage))
                : null;
        this.leadershipGuard = new LeadershipGuard(state);

        CandidateElection.HigherTermHandler termHandler = higherTermHandler != null
                ? higherTermHandler
                : term -> observeHigherTerm(term);

        this.candidateElection = new CandidateElection(
                localId,
                node,
                state,
                (peer, req, reply) -> {
                    if (partitionIsolatedPeers.contains(peer)) {
                        // Drop RPC across simulated partition boundary
                        return;
                    }
                    this.transport.requestVote(peer, req, response -> {
                        if (response != null && response.voteGranted() && response.term() == this.state.currentTerm()) {
                            currentGrants.add(peer);
                        }
                        reply.receive(response);
                    });
                },
                termHandler,
                () -> {
                    this.onLeader.run();
                }
        );
    }

    /**
     * Overload without initial WriteAvailabilityPolicy.
     */
    public RaftElectionService(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown) {
        this(localId, node, state, storage, transport, higherTermHandler, onLeader, onStepDown, null);
    }

    /**
     * Overload with standard election callbacks.
     */
    public RaftElectionService(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            Runnable onLeader) {
        this(localId, node, state, storage, transport, null, onLeader, () -> {}, null);
    }

    /**
     * Minimal constructor for test doubles or components where transport is attached later.
     */
    public RaftElectionService(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage) {
        this(localId, node, state, storage, (peer, req, reply) -> {}, null, () -> {}, () -> {}, null);
    }

    // -------------------------------------------------------------------------------------------
    // ElectionService Interface Implementation
    // -------------------------------------------------------------------------------------------

    @Override
    public String localId() {
        return localId;
    }

    @Override
    public RaftNode node() {
        return node;
    }

    @Override
    public RaftState state() {
        return state;
    }

    @Override
    public List<String> voters() {
        return VOTERS;
    }

    @Override
    public boolean isVoter(String nodeId) {
        return nodeId != null && VOTERS.contains(nodeId);
    }

    @Override
    public int requiredVotes() {
        return REQUIRED_VOTES;
    }

    @Override
    public long startElection() throws RaftStorageException {
        requireHealthy();
        currentGrants.clear();
        currentGrants.add(localId);
        return candidateElection.startElection();
    }

    @Override
    public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
        return candidateElection.voteRequestFor(candidateTerm);
    }

    @Override
    public RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) throws RaftStorageException {
        requireHealthy();
        Objects.requireNonNull(request, "request").validate();
        if (partitionIsolatedPeers.contains(request.getCandidateId())) {
            // Drop or deny vote request from isolated candidate
            return new RequestVoteResponse(state.currentTerm(), false);
        }
        Role prevRole = state.role();
        RequestVoteResponse response = candidateElection.handleRequestVote(request);
        if (prevRole == Role.LEADER && state.role() == Role.FOLLOWER) {
            onStepDown.run();
        }
        if (response.term() > state.currentTerm()) {
            observeHigherTerm(response.term());
        }
        return response;
    }

    @Override
    public ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted) {
        requireHealthy();
        if (partitionIsolatedPeers.contains(voterId)) {
            return new ElectionService.IgnoredVote("peer isolated in partition");
        }
        ElectionOutcome outcome = candidateElection.recordVoteResponse(voterId, term, granted);
        if (outcome instanceof ElectionService.Won) {
            currentGrants.add(voterId);
        } else if (outcome instanceof ElectionService.VoteProgress) {
            currentGrants.add(voterId);
        } else if (outcome instanceof ElectionService.SteppedDown steppedDown) {
            currentGrants.clear();
            try {
                observeHigherTerm(steppedDown.observedTerm());
            } catch (RaftStorageException e) {
                failClosed();
                throw new RuntimeException("Durable step down failed", e);
            }
        }
        return outcome;
    }

    @Override
    public void promoteToLeader(Set<String> leaderPeers) {
        requireHealthy();
        candidateElection.promoteToLeader(leaderPeers);
    }

    // -------------------------------------------------------------------------------------------
    // TV5-D20 Split Vote & Re-election Management
    // -------------------------------------------------------------------------------------------

    /**
     * Retries an election in a new term when a split vote occurred or a campaign timed out.
     *
     * @return the new term campaigned for
     * @throws RaftStorageException if persisting new term fails
     */
    public long retryElection() throws RaftStorageException {
        requireHealthy();
        if (state.role() == Role.LEADER) {
            throw new IllegalStateException("Leader cannot retry election");
        }
        return startElection();
    }

    /**
     * Current count of votes granted for the active campaign (including self vote).
     */
    public int grantedVoteCount() {
        return currentGrants.size();
    }

    /**
     * Unmodifiable set of voters that granted votes in the current campaign.
     */
    public Set<String> grantedVoters() {
        return Collections.unmodifiableSet(new TreeSet<>(currentGrants));
    }

    /**
     * Returns true if the active campaign has reached majority quorum (>= 3 votes).
     */
    public boolean hasWonElection() {
        return currentGrants.size() >= REQUIRED_VOTES;
    }

    /**
     * Steps down to FOLLOWER when observing a higher term from peer communication.
     */
    public void observeHigherTerm(long higherTerm) throws RaftStorageException {
        requireHealthy();
        if (higherTerm < 0) {
            throw new IllegalArgumentException("term must be nonnegative: " + higherTerm);
        }
        if (higherTerm > state.currentTerm()) {
            currentGrants.clear();
            onStepDown.run();
            if (node instanceof PersistentRaftNode durable) {
                durable.observeHigherTerm(higherTerm);
            } else if (persistence != null) {
                persistence.observeHigherTerm(higherTerm);
            } else {
                state.becomeFollower(higherTerm);
            }
        }
    }

    /**
     * Explicit step-down to FOLLOWER.
     */
    public void stepDown(long higherTerm) throws RaftStorageException {
        observeHigherTerm(higherTerm);
    }

    // -------------------------------------------------------------------------------------------
    // TV5-D19 / TV5-D20 Quorum Write Availability Integration
    // -------------------------------------------------------------------------------------------

    /**
     * Attaches a {@link WriteAvailabilityPolicy} to this election service.
     */
    public void attachWriteAvailabilityPolicy(WriteAvailabilityPolicy policy) {
        this.writeAvailabilityPolicy = Objects.requireNonNull(policy, "policy");
    }

    /**
     * Configures a {@link WriteAvailabilityPolicy} using replication tracking and commit advancer.
     */
    public void configureWriteAvailability(ReplicationTracker tracker, CommitAdvancer advancer) {
        this.writeAvailabilityPolicy = new WriteAvailabilityPolicy(localId, state, peers, tracker, advancer);
    }

    /**
     * Returns the attached {@link WriteAvailabilityPolicy}, or null if not yet attached.
     */
    public WriteAvailabilityPolicy writeAvailabilityPolicy() {
        return writeAvailabilityPolicy;
    }

    /**
     * Evaluates write availability:
     * - Returns NOT_LEADER if local node is not leader.
     * - Returns NO_QUORUM if active voters < 3 (e.g. 2/5 minority).
     * - Returns AVAILABLE if active voters >= 3.
     */
    public AvailabilityResult checkWriteAvailability(Instant now) {
        if (state.role() != Role.LEADER) {
            return new AvailabilityResult(
                    AvailabilityStatus.NOT_LEADER,
                    state.leaderId(),
                    state.currentTerm(),
                    0,
                    CLUSTER_SIZE,
                    REQUIRED_VOTES,
                    "Node " + localId + " is not leader (role=" + state.role() + ")"
            );
        }
        if (writeAvailabilityPolicy != null) {
            return writeAvailabilityPolicy.checkWriteAvailability(now);
        }
        int active = activeVoterCountFallback(now);
        boolean hasQuorum = active >= REQUIRED_VOTES;
        return new AvailabilityResult(
                hasQuorum ? AvailabilityStatus.AVAILABLE : AvailabilityStatus.NO_QUORUM,
                localId,
                state.currentTerm(),
                active,
                CLUSTER_SIZE,
                REQUIRED_VOTES,
                hasQuorum
                        ? "Write quorum available (" + active + "/" + CLUSTER_SIZE + " nodes)"
                        : "Write quorum unavailable: active voters " + active + "/" + CLUSTER_SIZE + " < quorum " + REQUIRED_VOTES
        );
    }

    /**
     * Returns true only when local node is Leader and has verified commit quorum capability (>= 3/5).
     */
    public boolean canCommitWrites(Instant now) {
        if (state.role() != Role.LEADER) {
            return false;
        }
        if (writeAvailabilityPolicy != null) {
            return writeAvailabilityPolicy.canCommitWrites(now);
        }
        return activeVoterCountFallback(now) >= REQUIRED_VOTES;
    }

    /**
     * Returns true if active voters count >= 3.
     */
    public boolean hasCommitQuorum(Instant now) {
        if (writeAvailabilityPolicy != null) {
            return writeAvailabilityPolicy.hasCommitQuorum(now);
        }
        return activeVoterCountFallback(now) >= REQUIRED_VOTES;
    }

    /**
     * Counts active voters (Leader + peers that sent verified commit ACKs within freshness window).
     */
    public int activeVoterCount(Instant now) {
        if (writeAvailabilityPolicy != null) {
            return writeAvailabilityPolicy.activeVoterCount(now);
        }
        return activeVoterCountFallback(now);
    }

    /**
     * Records a verified AppendEntries commit acknowledgment from a peer.
     */
    public void recordPeerAck(String peerId, long matchIndex, Instant timestamp) {
        peerAckTimes.put(peerId, timestamp);
        if (writeAvailabilityPolicy != null) {
            writeAvailabilityPolicy.recordPeerAck(peerId, matchIndex, timestamp);
        }
    }

    /**
     * Records a peer ping (proves peer connectivity, but NOT commit ACK).
     */
    public void recordPeerPing(String peerId, Instant timestamp) {
        peerPingTimes.put(peerId, timestamp);
        if (writeAvailabilityPolicy != null) {
            writeAvailabilityPolicy.recordPeerPing(peerId, timestamp);
        }
    }

    /**
     * Resolves write timeout with deterministic error code:
     * - VERIFIED_NO_QUORUM / not appended: returns NO_QUORUM
     * - INDETERMINATE: returns TIMEOUT_UNKNOWN
     * - COMMITTED: returns COMMITTED
     * In all cases, uncommitted entry is NOT rolled back.
     */
    public WriteTimeoutResolution resolveWriteTimeout(
            long logIndex,
            long term,
            boolean appendedLocally,
            TimeoutVerificationSituation situation) {
        if (writeAvailabilityPolicy != null) {
            return writeAvailabilityPolicy.resolveTimeout(logIndex, term, appendedLocally, situation);
        }
        if (logIndex > 0L && state.commitIndex() >= logIndex) {
            return new WriteTimeoutResolution(
                    TimeoutOutcome.COMMITTED,
                    null,
                    "The command at index " + logIndex + " was successfully committed by quorum",
                    logIndex,
                    term,
                    true);
        }
        if (situation == TimeoutVerificationSituation.VERIFIED_NO_QUORUM || !appendedLocally) {
            return new WriteTimeoutResolution(
                    TimeoutOutcome.NO_QUORUM,
                    "NO_QUORUM",
                    "The command could not be committed by a quorum",
                    logIndex,
                    term,
                    true);
        }
        return new WriteTimeoutResolution(
                TimeoutOutcome.UNKNOWN,
                "TIMEOUT_UNKNOWN",
                "Write timed out with indeterminate consensus outcome; query or retry with same requestId",
                logIndex,
                term,
                true);
    }

    /**
     * Verifies that an uncommitted entry was not discarded or rolled back upon timeout.
     */
    public void assertUncommittedEntryNotRolledBack(long logIndex) {
        if (writeAvailabilityPolicy != null) {
            writeAvailabilityPolicy.assertUncommittedEntryNotRolledBack(logIndex);
        }
    }

    private int activeVoterCountFallback(Instant now) {
        if (state.role() != Role.LEADER) {
            return 0;
        }
        int count = 1; // local leader
        Duration window = WriteAvailabilityPolicy.DEFAULT_FRESHNESS_WINDOW;
        for (String peer : peers) {
            if (partitionIsolatedPeers.contains(peer)) {
                continue;
            }
            Instant ack = peerAckTimes.get(peer);
            if (ack != null && !ack.isBefore(now.minus(window))) {
                count++;
            }
        }
        return count;
    }

    // -------------------------------------------------------------------------------------------
    // Durable Storage Recovery
    // -------------------------------------------------------------------------------------------

    /**
     * Reloads and validates durable state from storage on node recovery/restart.
     */
    public PersistentState recoverFromStorage() throws RaftStorageException {
        if (storage == null) {
            throw new IllegalStateException("no durable storage configured for recovery");
        }
        PersistentState loaded = storage.load();
        if (loaded.currentTerm() < state.currentTerm()) {
            throw new RaftStorageException("term regression in storage: stored="
                    + loaded.currentTerm() + ", state=" + state.currentTerm());
        }
        return loaded;
    }

    // -------------------------------------------------------------------------------------------
    // G5 Fault Scenario Simulation Helpers
    // -------------------------------------------------------------------------------------------

    /**
     * Isolates a peer from local communication (simulating link failure or partition).
     */
    public void isolatePeer(String peer) {
        if (peers.contains(peer)) {
            partitionIsolatedPeers.add(peer);
        }
    }

    /**
     * Heals communication with an isolated peer.
     */
    public void healPeer(String peer) {
        partitionIsolatedPeers.remove(peer);
    }

    /**
     * Configures a simulated partition, isolating the given set of peers.
     */
    public void partition(Set<String> isolatedPeers) {
        partitionIsolatedPeers.clear();
        for (String p : isolatedPeers) {
            if (peers.contains(p)) {
                partitionIsolatedPeers.add(p);
            }
        }
    }

    /**
     * Heals all simulated partitions.
     */
    public void healAll() {
        partitionIsolatedPeers.clear();
    }

    /**
     * Set of currently isolated peers.
     */
    public Set<String> isolatedPeers() {
        return Collections.unmodifiableSet(new TreeSet<>(partitionIsolatedPeers));
    }

    // -------------------------------------------------------------------------------------------
    // State Inspection & Guards
    // -------------------------------------------------------------------------------------------

    public Set<String> peers() {
        return peers;
    }

    public int quorumSize() {
        return REQUIRED_VOTES;
    }

    public int clusterSize() {
        return CLUSTER_SIZE;
    }

    public Role role() {
        return state.role();
    }

    public boolean isLeader() {
        return state.role() == Role.LEADER;
    }

    public boolean isCandidate() {
        return state.role() == Role.CANDIDATE;
    }

    public boolean isFollower() {
        return state.role() == Role.FOLLOWER;
    }

    public LeadershipGuard leadershipGuard() {
        return leadershipGuard;
    }

    public PersistentElectionState persistence() {
        return persistence;
    }

    public RaftStorage storage() {
        return storage;
    }

    private void requireHealthy() {
        if (failed) {
            throw new IllegalStateException("RaftElectionService failed closed; reload storage before reuse");
        }
    }

    private void failClosed() {
        failed = true;
        currentGrants.clear();
    }
}
