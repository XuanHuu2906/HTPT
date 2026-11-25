package vn.nhom6.railway.raft.election;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import vn.nhom6.railway.protocol.dto.NodeStatusDto;
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

    // TV5-D25 structured term/failover event types (SPEC-02 §8 & SPEC-04 §10)
    public static final String EVENT_ELECTION_STARTED = "ELECTION_STARTED";
    public static final String EVENT_VOTE_GRANTED = "VOTE_GRANTED";
    public static final String EVENT_LEADER_ELECTED = "LEADER_ELECTED";
    public static final String EVENT_HIGHER_TERM_STEP_DOWN = "HIGHER_TERM_STEP_DOWN";
    public static final String EVENT_STOP_LEADER_PREPARED = "STOP_LEADER_PREPARED";
    public static final String EVENT_STOP_LEADER_EXECUTED = "STOP_LEADER_EXECUTED";
    public static final String EVENT_NODE_RESTARTED = "NODE_RESTARTED";
    public static final String EVENT_PARTITION_APPLIED = "PARTITION_APPLIED";
    public static final String EVENT_PARTITION_HEALED = "PARTITION_HEALED";
    public static final String EVENT_RECOVERY_RESTORED = "RECOVERY_RESTORED";

    /**
     * Structured term/role transition log entry for LAN failover auditing (TV5-D25, SPEC-02 §8).
     */
    public record TermLogEntry(
            Instant timestamp,
            String nodeId,
            String eventType,
            long previousTerm,
            long currentTerm,
            Role previousRole,
            Role currentRole,
            String leaderId,
            String votedFor,
            long commitIndex,
            long lastLogIndex,
            String detail) {

        public String toLogLine() {
            return timestamp + " [" + nodeId + "] event=" + eventType
                    + " term=" + previousTerm + "->" + currentTerm
                    + " role=" + previousRole + "->" + currentRole
                    + " leader=" + (leaderId != null ? leaderId : "NONE")
                    + " votedFor=" + (votedFor != null ? votedFor : "NONE")
                    + " commit=" + commitIndex
                    + " lastLog=" + lastLogIndex
                    + (detail != null && !detail.isBlank() ? " detail=\"" + detail + "\"" : "");
        }
    }

    /**
     * Prepared or executed stop-leader failover action (TV5-D25).
     * Never hardcodes node-1; always resolves the current Leader dynamically.
     */
    public record StopLeaderAction(
            String targetNodeId,
            long observedTerm,
            long commitIndex,
            long lastLogIndex,
            String clientEndpoint,
            String commandHint,
            String reason,
            boolean dryRun,
            Instant preparedAt) {

        public StopLeaderAction {
            Objects.requireNonNull(targetNodeId, "targetNodeId");
            if (!VOTERS.contains(targetNodeId)) {
                throw new IllegalArgumentException("targetNodeId must be one of the 5 voters: " + targetNodeId);
            }
            if (observedTerm < 0) {
                throw new IllegalArgumentException("observedTerm must be >= 0: " + observedTerm);
            }
            Objects.requireNonNull(commandHint, "commandHint");
            Objects.requireNonNull(preparedAt, "preparedAt");
        }
    }

    /**
     * Audit result over a sequence of term log entries or recorded NODE_STATUS trace steps (TV5-D25).
     */
    public record TraceAuditReport(
            boolean passed,
            int stepCount,
            Map<Long, String> leaderByTerm,
            Map<String, Long> maxTermByNode,
            List<String> violations) {

        public TraceAuditReport {
            leaderByTerm = Collections.unmodifiableMap(new LinkedHashMap<>(leaderByTerm));
            maxTermByNode = Collections.unmodifiableMap(new LinkedHashMap<>(maxTermByNode));
            violations = List.copyOf(violations);
        }
    }

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
    private volatile Clock clock = Clock.systemUTC();
    private final Set<String> currentGrants = new CopyOnWriteArraySet<>();
    private final Map<String, Instant> peerAckTimes = new ConcurrentHashMap<>();
    private final Map<String, Instant> peerPingTimes = new ConcurrentHashMap<>();
    private final Set<String> partitionIsolatedPeers = new CopyOnWriteArraySet<>();
    private final CopyOnWriteArrayList<TermLogEntry> termLog = new CopyOnWriteArrayList<>();
    private volatile boolean failed;
    private volatile boolean stopped;

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
                    recordTermEvent(
                            EVENT_LEADER_ELECTED,
                            this.state.currentTerm(),
                            Role.CANDIDATE,
                            "Elected leader with " + currentGrants.size() + "/" + CLUSTER_SIZE + " votes");
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
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        currentGrants.clear();
        currentGrants.add(localId);
        long newTerm = candidateElection.startElection();
        recordTermEvent(EVENT_ELECTION_STARTED, prevTerm, prevRole, "Started election campaign for term " + newTerm);
        return newTerm;
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
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        ElectionOutcome outcome = candidateElection.recordVoteResponse(voterId, term, granted);
        if (outcome instanceof ElectionService.Won) {
            currentGrants.add(voterId);
            recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                    "Vote granted by " + voterId + " (quorum reached: " + currentGrants.size() + "/" + CLUSTER_SIZE + ")");
        } else if (outcome instanceof ElectionService.VoteProgress) {
            currentGrants.add(voterId);
            recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                    "Vote granted by " + voterId + " (" + currentGrants.size() + "/" + CLUSTER_SIZE + ")");
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
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        candidateElection.promoteToLeader(leaderPeers);
        recordTermEvent(EVENT_LEADER_ELECTED, prevTerm, prevRole,
                "Promoted to leader with " + currentGrants.size() + "/" + CLUSTER_SIZE + " votes");
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
            long prevTerm = state.currentTerm();
            Role prevRole = state.role();
            currentGrants.clear();
            onStepDown.run();
            if (node instanceof PersistentRaftNode durable) {
                durable.observeHigherTerm(higherTerm);
            } else if (persistence != null) {
                persistence.observeHigherTerm(higherTerm);
            } else {
                state.becomeFollower(higherTerm);
            }
            recordTermEvent(EVENT_HIGHER_TERM_STEP_DOWN, prevTerm, prevRole,
                    "Observed higher term " + higherTerm + " -> stepped down to FOLLOWER");
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
        if (stopped || state.role() != Role.LEADER) {
            return new AvailabilityResult(
                    AvailabilityStatus.NOT_LEADER,
                    stopped ? null : state.leaderId(),
                    state.currentTerm(),
                    0,
                    CLUSTER_SIZE,
                    REQUIRED_VOTES,
                    stopped
                            ? "Node " + localId + " is stopped for failover simulation"
                            : "Node " + localId + " is not leader (role=" + state.role() + ")"
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
        if (stopped || state.role() != Role.LEADER) {
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
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        PersistentState loaded = storage.load();
        if (loaded.currentTerm() < state.currentTerm()) {
            throw new RaftStorageException("term regression in storage: stored="
                    + loaded.currentTerm() + ", state=" + state.currentTerm());
        }
        recordTermEvent(EVENT_RECOVERY_RESTORED, prevTerm, prevRole,
                "Recovered durable state term=" + loaded.currentTerm()
                        + ", votedFor=" + loaded.votedFor()
                        + ", logEntries=" + loaded.log().size());
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
            recordTermEvent(EVENT_PARTITION_APPLIED, state.currentTerm(), state.role(),
                    "Isolated peer " + peer + " (isolated=" + isolatedPeers() + ")");
        }
    }

    /**
     * Heals communication with an isolated peer.
     */
    public void healPeer(String peer) {
        if (partitionIsolatedPeers.remove(peer)) {
            recordTermEvent(EVENT_PARTITION_HEALED, state.currentTerm(), state.role(),
                    "Healed peer " + peer + " (remaining isolated=" + isolatedPeers() + ")");
        }
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
        recordTermEvent(EVENT_PARTITION_APPLIED, state.currentTerm(), state.role(),
                "Applied partition isolating peers=" + isolatedPeers());
    }

    /**
     * Heals all simulated partitions.
     */
    public void healAll() {
        partitionIsolatedPeers.clear();
        recordTermEvent(EVENT_PARTITION_HEALED, state.currentTerm(), state.role(),
                "Healed all simulated partitions");
    }

    /**
     * Set of currently isolated peers.
     */
    public Set<String> isolatedPeers() {
        return Collections.unmodifiableSet(new TreeSet<>(partitionIsolatedPeers));
    }

    // -------------------------------------------------------------------------------------------
    // TV5-D25: LAN Failover Stop-Leader Actions, Term Logging & Trace Auditing
    // -------------------------------------------------------------------------------------------

    /**
     * Configures a custom clock for deterministic term-log timestamps in tests.
     */
    public void setClock(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Returns an immutable snapshot of all structured term/failover events recorded by this service.
     */
    public List<TermLogEntry> termLog() {
        return List.copyOf(termLog);
    }

    /**
     * Clears the in-memory term log (useful between independent test scenarios).
     */
    public void clearTermLog() {
        termLog.clear();
    }

    /**
     * Appends a structured term/role transition entry to this node's audit log.
     */
    public TermLogEntry recordTermEvent(String eventType, long previousTerm, Role previousRole, String detail) {
        TermLogEntry entry = new TermLogEntry(
                clock.instant(),
                localId,
                Objects.requireNonNull(eventType, "eventType"),
                previousTerm,
                state.currentTerm(),
                previousRole,
                state.role(),
                state.leaderId(),
                state.votedFor(),
                state.commitIndex(),
                state.lastLogIndex(),
                detail);
        termLog.add(entry);
        return entry;
    }

    /**
     * Asserts that all recorded term transitions on this node are monotonically non-decreasing.
     */
    public void assertTermMonotonicity() {
        long highest = -1L;
        for (TermLogEntry entry : termLog) {
            if (entry.currentTerm() < highest) {
                throw new IllegalStateException("Term regression on " + localId + ": "
                        + entry.currentTerm() + " < " + highest + " at event " + entry.eventType());
            }
            highest = entry.currentTerm();
        }
    }

    /**
     * Prepares a dry-run stop-leader action for this node after verifying it currently holds LEADER role.
     */
    public StopLeaderAction prepareLocalStopLeaderAction(String clientEndpoint, String reason) {
        requireHealthy();
        if (state.role() != Role.LEADER) {
            throw new IllegalStateException("Cannot prepare stop-leader action: " + localId
                    + " is not LEADER (role=" + state.role() + ")");
        }
        String effectiveReason = (reason != null && !reason.isBlank()) ? reason : "LAN failover drill";
        String commandHint = "docker stop -t 10 railway-backend-" + localId
                + " # or scripts/stop-node.ps1 -TargetLeader";
        StopLeaderAction action = new StopLeaderAction(
                localId,
                state.currentTerm(),
                state.commitIndex(),
                state.lastLogIndex(),
                clientEndpoint,
                commandHint,
                effectiveReason,
                true,
                clock.instant());
        recordTermEvent(EVENT_STOP_LEADER_PREPARED, state.currentTerm(), state.role(),
                "Prepared stop-leader action for " + localId + " at term=" + state.currentTerm()
                        + " endpoint=" + clientEndpoint + " reason=" + effectiveReason);
        return action;
    }

    /**
     * Simulates stopping this node while it is Leader without erasing durable Raft storage.
     * Isolates all peers, stops local leadership activity, and logs STOP_LEADER_EXECUTED.
     */
    public StopLeaderAction simulateStopLeader(String clientEndpoint, String reason) {
        StopLeaderAction prepared = prepareLocalStopLeaderAction(clientEndpoint, reason);
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        stopped = true;
        partitionIsolatedPeers.addAll(peers);
        currentGrants.clear();
        onStepDown.run();
        recordTermEvent(EVENT_STOP_LEADER_EXECUTED, prevTerm, prevRole,
                "Simulated stop-leader executed on " + localId + " at term=" + prevTerm
                        + " (durable storage preserved)");
        return new StopLeaderAction(
                prepared.targetNodeId(),
                prepared.observedTerm(),
                prepared.commitIndex(),
                prepared.lastLogIndex(),
                prepared.clientEndpoint(),
                prepared.commandHint(),
                prepared.reason(),
                false,
                clock.instant());
    }

    /**
     * Restarts a previously stopped node, validating durable storage (if attached) without regressing term.
     */
    public PersistentState simulateRestartAfterStop() throws RaftStorageException {
        if (!stopped) {
            throw new IllegalStateException("Node " + localId + " is not stopped");
        }
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        PersistentState loaded = null;
        if (storage != null) {
            loaded = storage.load();
            if (loaded.currentTerm() < prevTerm) {
                throw new RaftStorageException("Term regression detected on restart for " + localId
                        + ": stored=" + loaded.currentTerm() + " < previous=" + prevTerm);
            }
        }
        long restoredTerm = loaded != null ? loaded.currentTerm() : prevTerm;
        state.becomeFollower(restoredTerm);
        partitionIsolatedPeers.clear();
        currentGrants.clear();
        stopped = false;
        recordTermEvent(EVENT_NODE_RESTARTED, prevTerm, prevRole,
                "Node " + localId + " restarted as FOLLOWER at term=" + state.currentTerm()
                        + " votedFor=" + state.votedFor());
        return loaded;
    }

    /**
     * Returns true if this service has been halted via {@link #simulateStopLeader(String, String)}.
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * Dynamically discovers the unique healthy LEADER from SPEC-03 {@link NodeStatusDto} observations
     * (from live TCP probes, fake endpoints, or recorded traces) and prepares a {@link StopLeaderAction}.
     * Fails closed when 0 leaders, multiple leaders, or unhealthy leader storage are observed.
     */
    public static StopLeaderAction prepareStopLeaderFromStatuses(
            Collection<NodeStatusDto> statuses,
            Map<String, String> clientEndpoints,
            String reason) {
        Objects.requireNonNull(statuses, "statuses");
        List<NodeStatusDto> leaders = new ArrayList<>();
        for (NodeStatusDto status : statuses) {
            if (status == null) {
                continue;
            }
            status.validate();
            if (status.isLeader()) {
                if (!status.isStorageHealthy()) {
                    throw new IllegalStateException("UNHEALTHY_LEADER: Leader " + status.getNodeId()
                            + " reports storageHealthy=false");
                }
                if (status.getTerm() == null) {
                    throw new IllegalStateException("LEADER_MISSING_TERM: Leader " + status.getNodeId()
                            + " has null term");
                }
                leaders.add(status);
            }
        }
        if (leaders.isEmpty()) {
            throw new IllegalStateException("NO_LEADER_FOUND: 0 healthy LEADER nodes observed in status snapshot");
        }
        if (leaders.size() > 1) {
            List<String> ids = leaders.stream()
                    .map(l -> l.getNodeId() + "@term=" + l.getTerm())
                    .toList();
            throw new IllegalStateException("MULTIPLE_LEADERS_DETECTED: fail-closed with leaders=" + ids);
        }
        NodeStatusDto leader = leaders.get(0);
        String targetId = leader.getNodeId();
        String endpoint = clientEndpoints != null ? clientEndpoints.get(targetId) : null;
        String effectiveReason = (reason != null && !reason.isBlank()) ? reason : "Dynamic LAN failover stop-leader";
        String commandHint = "docker stop -t 10 railway-backend-" + targetId
                + " # or scripts/stop-node.ps1 -TargetLeader";
        return new StopLeaderAction(
                targetId,
                leader.getTerm(),
                leader.getCommitIndex() != null ? leader.getCommitIndex() : 0L,
                leader.getLastLogIndex() != null ? leader.getLastLogIndex() : 0L,
                endpoint,
                commandHint,
                effectiveReason,
                true,
                Instant.now());
    }

    /**
     * Audits a combined list of {@link TermLogEntry} records for per-node term monotonicity
     * and Election Safety (at most one distinct LEADER per term).
     */
    public static TraceAuditReport auditTermLog(List<TermLogEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        Map<String, Long> maxTermByNode = new LinkedHashMap<>();
        Map<Long, String> leaderByTerm = new LinkedHashMap<>();
        List<String> violations = new ArrayList<>();

        for (int i = 0; i < entries.size(); i++) {
            TermLogEntry entry = entries.get(i);
            Long prev = maxTermByNode.get(entry.nodeId());
            if (prev != null && entry.currentTerm() < prev) {
                violations.add("Entry " + i + ": term regression on " + entry.nodeId()
                        + " (" + prev + " -> " + entry.currentTerm() + ")");
            } else {
                maxTermByNode.put(entry.nodeId(), entry.currentTerm());
            }

            if (EVENT_LEADER_ELECTED.equals(entry.eventType()) || entry.currentRole() == Role.LEADER) {
                String existing = leaderByTerm.putIfAbsent(entry.currentTerm(), entry.nodeId());
                if (existing != null && !existing.equals(entry.nodeId())) {
                    violations.add("Entry " + i + ": Election Safety violated at term " + entry.currentTerm()
                            + " (leaders=" + existing + " and " + entry.nodeId() + ")");
                }
            }
        }
        return new TraceAuditReport(violations.isEmpty(), entries.size(), leaderByTerm, maxTermByNode, violations);
    }

    /**
     * Audits a recorded multi-step cluster {@link NodeStatusDto} trace against Raft Election Safety:
     * <ul>
     *   <li>Every status observation validates against SPEC-03 §5.</li>
     *   <li>Per-node {@code currentTerm} is monotonically non-decreasing across steps.</li>
     *   <li>At most one distinct {@code LEADER} exists for any given term across all steps.</li>
     *   <li>Minority steps (&lt; 3 online nodes) never elect a new LEADER in a term unseen while quorum existed.</li>
     *   <li>Commit/applied invariants: {@code 0 <= lastApplied <= commitIndex <= lastLogIndex}.</li>
     * </ul>
     */
    public static TraceAuditReport auditRecordedStatusTrace(List<Map<String, NodeStatusDto>> traceSteps) {
        Objects.requireNonNull(traceSteps, "traceSteps");
        Map<String, Long> maxTermByNode = new LinkedHashMap<>();
        Map<Long, String> leaderByTerm = new LinkedHashMap<>();
        List<String> violations = new ArrayList<>();

        for (int stepIdx = 0; stepIdx < traceSteps.size(); stepIdx++) {
            Map<String, NodeStatusDto> step = traceSteps.get(stepIdx);
            if (step == null || step.isEmpty()) {
                violations.add("Step " + stepIdx + ": empty status snapshot");
                continue;
            }
            int onlineCount = step.size();
            for (Map.Entry<String, NodeStatusDto> kv : step.entrySet()) {
                NodeStatusDto dto = kv.getValue();
                if (dto == null) {
                    violations.add("Step " + stepIdx + ": null status for " + kv.getKey());
                    continue;
                }
                try {
                    dto.validate();
                } catch (IllegalArgumentException ex) {
                    violations.add("Step " + stepIdx + ": invalid NodeStatusDto for " + kv.getKey()
                            + " (" + ex.getMessage() + ")");
                    continue;
                }
                if (!VOTERS.contains(dto.getNodeId())) {
                    violations.add("Step " + stepIdx + ": non-voter nodeId in trace: " + dto.getNodeId());
                }
                if (dto.getTerm() != null) {
                    Long prevTerm = maxTermByNode.get(dto.getNodeId());
                    if (prevTerm != null && dto.getTerm() < prevTerm) {
                        violations.add("Step " + stepIdx + ": term regression on " + dto.getNodeId()
                                + " (" + prevTerm + " -> " + dto.getTerm() + ")");
                    } else {
                        maxTermByNode.put(dto.getNodeId(), dto.getTerm());
                    }
                }
                if (dto.getLastApplied() != null && dto.getCommitIndex() != null
                        && dto.getLastApplied() > dto.getCommitIndex()) {
                    violations.add("Step " + stepIdx + ": lastApplied (" + dto.getLastApplied()
                            + ") > commitIndex (" + dto.getCommitIndex() + ") on " + dto.getNodeId());
                }
                if (dto.getCommitIndex() != null && dto.getLastLogIndex() != null
                        && dto.getCommitIndex() > dto.getLastLogIndex()) {
                    violations.add("Step " + stepIdx + ": commitIndex (" + dto.getCommitIndex()
                            + ") > lastLogIndex (" + dto.getLastLogIndex() + ") on " + dto.getNodeId());
                }
                if (dto.isLeader() && dto.getTerm() != null) {
                    boolean knownTerm = leaderByTerm.containsKey(dto.getTerm());
                    String existing = leaderByTerm.putIfAbsent(dto.getTerm(), dto.getNodeId());
                    if (existing != null && !existing.equals(dto.getNodeId())) {
                        violations.add("Step " + stepIdx + ": Election Safety violated at term " + dto.getTerm()
                                + " (leaders=" + existing + " and " + dto.getNodeId() + ")");
                    }
                    if (!knownTerm && onlineCount < REQUIRED_VOTES) {
                        violations.add("Step " + stepIdx + ": Minority step (" + onlineCount + "/" + CLUSTER_SIZE
                                + " online) illegally elected new leader " + dto.getNodeId()
                                + " at term " + dto.getTerm());
                    }
                }
            }
        }
        return new TraceAuditReport(violations.isEmpty(), traceSteps.size(), leaderByTerm, maxTermByNode, violations);
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
        return !stopped && state.role() == Role.LEADER;
    }

    public boolean isCandidate() {
        return !stopped && state.role() == Role.CANDIDATE;
    }

    public boolean isFollower() {
        return !stopped && state.role() == Role.FOLLOWER;
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
        if (stopped) {
            throw new IllegalStateException("RaftElectionService is stopped for failover simulation; restart before reuse");
        }
    }

    private void failClosed() {
        failed = true;
        currentGrants.clear();
    }
}
