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
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger LOGGER = Logger.getLogger(RaftElectionService.class.getName());

    public static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    public static final int CLUSTER_SIZE = 5;
    public static final int REQUIRED_VOTES = 3; // strict majority: 5 / 2 + 1 = 3

    // TV5-D28 runtime mode & timeout configuration keys (SPEC-04 §3, SPEC-08 §3.1)
    public static final String ENV_RAFT_MODE = "RAFT_MODE";
    public static final String ENV_RAILWAY_MODE = "RAILWAY_MODE";
    public static final String PROP_RAFT_MODE = "railway.raft.mode";
    public static final String ENV_HEARTBEAT_INTERVAL_MS = "RAFT_HEARTBEAT_INTERVAL_MS";
    public static final String ENV_ELECTION_TIMEOUT_MIN_MS = "RAFT_ELECTION_TIMEOUT_MIN_MS";
    public static final String ENV_ELECTION_TIMEOUT_MAX_MS = "RAFT_ELECTION_TIMEOUT_MAX_MS";
    public static final String ENV_RPC_TIMEOUT_MS = "RAFT_RPC_TIMEOUT_MS";
    public static final String ENV_TCP_RESPONSE_DEADLINE_MS = "TCP_RESPONSE_DEADLINE_MS";
    public static final String ENV_QUORUM_FRESHNESS_MS = "RAFT_QUORUM_FRESHNESS_MS";

    private static final CandidateElection.VoteTransport NO_OP_TRANSPORT = (peer, req, reply) -> {};

    /**
     * Execution mode for {@link RaftElectionService} (TV5-D28).
     * <ul>
     *   <li>{@link #PRODUCTION}: Enforces real durable storage, persistent Raft core, real vote transport,
     *       quorum availability policy, and validated Raft timeouts; forbids all mock/simulation helpers.</li>
     *   <li>{@link #TEST_SIMULATION}: Permitted only in deterministic unit/harness tests for partition,
     *       stop-leader simulation, and fake-transport drills.</li>
     * </ul>
     */
    public enum RuntimeMode {
        PRODUCTION,
        TEST_SIMULATION
    }

    /**
     * Validated Raft Election &amp; Quorum timing configuration (TV5-D28, SPEC-04 §3 &amp; SPEC-08 §3.1).
     */
    public record TimeoutConfig(
            ElectionConfig electionConfig,
            Duration rpcTimeout,
            Duration quorumFreshnessWindow) {

        public TimeoutConfig {
            Objects.requireNonNull(electionConfig, "electionConfig");
            Objects.requireNonNull(rpcTimeout, "rpcTimeout");
            Objects.requireNonNull(quorumFreshnessWindow, "quorumFreshnessWindow");
            if (rpcTimeout.isZero() || rpcTimeout.isNegative()) {
                throw new IllegalArgumentException("rpcTimeout must be positive: " + rpcTimeout);
            }
            if (quorumFreshnessWindow.isZero() || quorumFreshnessWindow.isNegative()) {
                throw new IllegalArgumentException(
                        "quorumFreshnessWindow must be positive: " + quorumFreshnessWindow);
            }
        }

        public static TimeoutConfig defaults() {
            ElectionConfig cfg = ElectionConfig.defaults();
            return new TimeoutConfig(
                    cfg,
                    Duration.ofMillis(1_000L),
                    Duration.ofMillis(Math.max(1_000L, cfg.heartbeatIntervalMillis() * 6L)));
        }

        public static TimeoutConfig lanProductionDefaults() {
            ElectionConfig cfg = ElectionConfig.defaults();
            return new TimeoutConfig(
                    cfg,
                    Duration.ofMillis(500L),
                    WriteAvailabilityPolicy.DEFAULT_FRESHNESS_WINDOW);
        }

        public static TimeoutConfig fromEnvironment(Map<String, String> env, Properties props) {
            Map<String, String> safeEnv = env != null ? env : Map.of();
            Properties safeProps = props != null ? props : new Properties();
            long heartbeat = readConfigLong(
                    safeEnv, safeProps, ENV_HEARTBEAT_INTERVAL_MS, "raft.heartbeat.interval.ms", 150L);
            long minElection = readConfigLong(
                    safeEnv, safeProps, ENV_ELECTION_TIMEOUT_MIN_MS, "raft.election.timeout.min.ms", 500L);
            long maxElection = readConfigLong(
                    safeEnv, safeProps, ENV_ELECTION_TIMEOUT_MAX_MS, "raft.election.timeout.max.ms", 1_000L);
            ElectionConfig electionConfig = new ElectionConfig(heartbeat, minElection, maxElection);

            long rpcMs;
            if (hasConfigValue(safeEnv, safeProps, ENV_RPC_TIMEOUT_MS, "raft.rpc.timeout.ms")) {
                rpcMs = readConfigLong(safeEnv, safeProps, ENV_RPC_TIMEOUT_MS, "raft.rpc.timeout.ms", 500L);
            } else {
                long tcpDeadline = readConfigLong(
                        safeEnv, safeProps, ENV_TCP_RESPONSE_DEADLINE_MS, "tcp.response.deadline.ms", 1_000L);
                rpcMs = Math.max(200L, Math.min(tcpDeadline, maxElection));
            }
            long defaultFreshness = Math.max(1_000L, heartbeat * 6L);
            long freshnessMs = readConfigLong(
                    safeEnv, safeProps, ENV_QUORUM_FRESHNESS_MS, "raft.quorum.freshness.ms", defaultFreshness);
            return new TimeoutConfig(
                    electionConfig,
                    Duration.ofMillis(rpcMs),
                    Duration.ofMillis(freshnessMs));
        }

        /**
         * Validates strict Raft liveness/safety timing margins required in PRODUCTION mode (SPEC-04 §3).
         */
        public void validateForProduction() {
            long hb = electionConfig.heartbeatIntervalMillis();
            long min = electionConfig.electionTimeoutMinMillis();
            long max = electionConfig.electionTimeoutMaxMillis();
            if (min < hb * 3L) {
                throw new IllegalArgumentException(
                        "Production electionTimeoutMinMillis (" + min
                                + "ms) must be >= 3 * heartbeatIntervalMillis (" + (hb * 3L) + "ms)");
            }
            if (max - min < hb) {
                throw new IllegalArgumentException(
                        "Production randomized election window (" + min + ".." + max
                                + "ms) spread must be >= heartbeatIntervalMillis (" + hb + "ms)");
            }
            if (rpcTimeout.toMillis() > max) {
                throw new IllegalArgumentException(
                        "Production rpcTimeout (" + rpcTimeout.toMillis()
                                + "ms) must not exceed electionTimeoutMaxMillis (" + max + "ms)");
            }
            if (quorumFreshnessWindow.toMillis() < hb * 3L) {
                throw new IllegalArgumentException(
                        "Production quorumFreshnessWindow (" + quorumFreshnessWindow.toMillis()
                                + "ms) must be >= 3 * heartbeatIntervalMillis (" + (hb * 3L) + "ms)");
            }
        }
    }

    // TV5-D25 / TV5-D28 structured term/failover event types (SPEC-02 §8 & SPEC-04 §10)
    public static final String EVENT_NODE_STARTED = "NODE_STARTED";
    public static final String EVENT_TIMEOUT_CONFIGURED = "TIMEOUT_CONFIGURED";
    public static final String EVENT_ELECTION_STARTED = "ELECTION_STARTED";
    public static final String EVENT_VOTE_GRANTED = "VOTE_GRANTED";
    public static final String EVENT_VOTE_DENIED = "VOTE_DENIED";
    public static final String EVENT_LEADER_ELECTED = "LEADER_ELECTED";
    public static final String EVENT_HIGHER_TERM_STEP_DOWN = "HIGHER_TERM_STEP_DOWN";
    public static final String EVENT_STEP_DOWN = "STEP_DOWN";
    public static final String EVENT_STOP_LEADER_PREPARED = "STOP_LEADER_PREPARED";
    public static final String EVENT_STOP_LEADER_EXECUTED = "STOP_LEADER_EXECUTED";
    public static final String EVENT_NODE_RESTARTED = "NODE_RESTARTED";
    public static final String EVENT_PARTITION_APPLIED = "PARTITION_APPLIED";
    public static final String EVENT_PARTITION_HEALED = "PARTITION_HEALED";
    public static final String EVENT_RECOVERY_RESTORED = "RECOVERY_RESTORED";
    public static final String EVENT_RECOVERY_FAILED = "RECOVERY_FAILED";
    public static final String EVENT_FAIL_CLOSED = "FAIL_CLOSED";

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

        public String toDetailedLogLine(long lastApplied, RuntimeMode mode) {
            return timestamp + " [" + nodeId + "] mode=" + mode
                    + " event=" + eventType
                    + " term=" + previousTerm + "->" + currentTerm
                    + " role=" + previousRole + "->" + currentRole
                    + " leader=" + (leaderId != null ? leaderId : "NONE")
                    + " votedFor=" + (votedFor != null ? votedFor : "NONE")
                    + " commitIndex=" + commitIndex
                    + " lastApplied=" + lastApplied
                    + " lastLogIndex=" + lastLogIndex
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
    private final RuntimeMode runtimeMode;
    private final TimeoutConfig timeoutConfig;
    private final ElectionTimeoutPolicy timeoutPolicy;
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

    private record TermedAck(long term, Instant timestamp) { }

    private volatile WriteAvailabilityPolicy writeAvailabilityPolicy;
    private volatile Clock clock = Clock.systemUTC();
    private final Set<String> currentGrants = new CopyOnWriteArraySet<>();
    private final Map<String, TermedAck> peerAckTimes = new ConcurrentHashMap<>();
    private final Map<String, Instant> peerPingTimes = new ConcurrentHashMap<>();
    private final Set<String> partitionIsolatedPeers = new CopyOnWriteArraySet<>();
    private final CopyOnWriteArrayList<TermLogEntry> termLog = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<TermLogEntry>> eventListeners = new CopyOnWriteArrayList<>();
    private volatile boolean failed;
    private volatile boolean stopped;

    /**
     * Production factory: enforces real durable storage, {@link PersistentRaftNode}, real vote transport,
     * and validated production timeout intervals; disables all mock/simulation hooks.
     * Automatically wires a real {@link WriteAvailabilityPolicy} backed by {@link ReplicationTracker}
     * and {@link CommitAdvancer} upon promotion to {@link Role#LEADER}.
     */
    public static RaftElectionService production(
            String localId,
            PersistentRaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            TimeoutConfig timeoutConfig) {
        return new RaftElectionService(
                RuntimeMode.PRODUCTION,
                timeoutConfig != null ? timeoutConfig : TimeoutConfig.lanProductionDefaults(),
                localId,
                Objects.requireNonNull(node, "Production mode requires PersistentRaftNode"),
                state,
                Objects.requireNonNull(storage, "Production mode requires durable RaftStorage"),
                Objects.requireNonNull(transport, "Production mode requires real VoteTransport"),
                higherTermHandler,
                onLeader,
                onStepDown,
                null);
    }

    /**
     * Production factory overload with an explicit {@link WriteAvailabilityPolicy}.
     */
    public static RaftElectionService production(
            String localId,
            PersistentRaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            WriteAvailabilityPolicy writeAvailabilityPolicy,
            TimeoutConfig timeoutConfig) {
        return new RaftElectionService(
                RuntimeMode.PRODUCTION,
                timeoutConfig != null ? timeoutConfig : TimeoutConfig.lanProductionDefaults(),
                localId,
                Objects.requireNonNull(node, "Production mode requires PersistentRaftNode"),
                state,
                Objects.requireNonNull(storage, "Production mode requires durable RaftStorage"),
                Objects.requireNonNull(transport, "Production mode requires real VoteTransport"),
                higherTermHandler,
                onLeader,
                onStepDown,
                Objects.requireNonNull(writeAvailabilityPolicy, "writeAvailabilityPolicy"));
    }

    /**
     * Production factory overload constructing {@link WriteAvailabilityPolicy} from replication dependencies
     * with the configured {@link TimeoutConfig#quorumFreshnessWindow()}.
     */
    public static RaftElectionService production(
            String localId,
            PersistentRaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            ReplicationTracker tracker,
            CommitAdvancer advancer,
            TimeoutConfig timeoutConfig) {
        Objects.requireNonNull(localId, "localId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(tracker, "tracker");
        Objects.requireNonNull(advancer, "advancer");
        TimeoutConfig effectiveTimeout = timeoutConfig != null ? timeoutConfig : TimeoutConfig.lanProductionDefaults();
        Set<String> remotePeers = new TreeSet<>(VOTERS);
        remotePeers.remove(localId);
        WriteAvailabilityPolicy policy = new WriteAvailabilityPolicy(
                localId, state, remotePeers, tracker, advancer, effectiveTimeout.quorumFreshnessWindow());
        return production(
                localId, node, state, storage, transport, higherTermHandler,
                onLeader, onStepDown, policy, effectiveTimeout);
    }

    /**
     * Resolves {@link RuntimeMode} and {@link TimeoutConfig} from environment and properties.
     * Defaults to {@link RuntimeMode#PRODUCTION} unless an explicit test/simulation mode is specified.
     */
    public static RaftElectionService fromEnvironment(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            WriteAvailabilityPolicy writeAvailabilityPolicy,
            Map<String, String> env,
            Properties props) {
        RuntimeMode mode = resolveRuntimeMode(env, props);
        TimeoutConfig timeouts = TimeoutConfig.fromEnvironment(env, props);
        return new RaftElectionService(
                mode, timeouts, localId, node, state, storage, transport,
                higherTermHandler, onLeader, onStepDown, writeAvailabilityPolicy);
    }

    /**
     * Resolves {@link RuntimeMode} from environment variables and properties, defaulting to
     * {@link RuntimeMode#PRODUCTION} (fail-closed against mock election in production).
     */
    public static RuntimeMode resolveRuntimeMode(Map<String, String> env, Properties props) {
        String raw = getFirstNonBlank(
                env != null ? env.get(ENV_RAFT_MODE) : null,
                env != null ? env.get(ENV_RAILWAY_MODE) : null,
                System.getProperty(PROP_RAFT_MODE),
                props != null ? props.getProperty(PROP_RAFT_MODE) : null,
                props != null ? props.getProperty("raft.mode") : null);
        if (raw == null) {
            return RuntimeMode.PRODUCTION;
        }
        String normalized = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "production", "prod", "lan", "release" -> RuntimeMode.PRODUCTION;
            case "test", "simulation", "sim", "test_simulation" -> RuntimeMode.TEST_SIMULATION;
            default -> throw new IllegalArgumentException(
                    "Invalid Raft runtime mode '" + raw + "'; expected PRODUCTION or TEST_SIMULATION");
        };
    }

    /**
     * Returns true when system properties or process environment explicitly enforce PRODUCTION mode.
     */
    public static boolean isProductionModeConfigured() {
        String raw = getFirstNonBlank(
                System.getProperty(PROP_RAFT_MODE),
                System.getenv(ENV_RAFT_MODE),
                System.getenv(ENV_RAILWAY_MODE));
        if (raw == null) {
            return false;
        }
        String normalized = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return "production".equals(normalized)
                || "prod".equals(normalized)
                || "lan".equals(normalized)
                || "release".equals(normalized);
    }

    /**
     * Primary constructor with all dependencies (defaults to {@link RuntimeMode#TEST_SIMULATION}
     * unless PRODUCTION mode is configured in system properties or environment).
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
        this(
                isProductionModeConfigured() ? RuntimeMode.PRODUCTION : RuntimeMode.TEST_SIMULATION,
                TimeoutConfig.defaults(),
                localId,
                node,
                state,
                storage,
                transport,
                higherTermHandler,
                onLeader,
                onStepDown,
                writeAvailabilityPolicy);
    }

    /**
     * Full mode- and timeout-aware constructor.
     */
    public RaftElectionService(
            RuntimeMode runtimeMode,
            TimeoutConfig timeoutConfig,
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage,
            CandidateElection.VoteTransport transport,
            CandidateElection.HigherTermHandler higherTermHandler,
            Runnable onLeader,
            Runnable onStepDown,
            WriteAvailabilityPolicy writeAvailabilityPolicy) {
        this.runtimeMode = Objects.requireNonNull(runtimeMode, "runtimeMode");
        this.timeoutConfig = Objects.requireNonNull(timeoutConfig, "timeoutConfig");
        this.timeoutPolicy = ElectionTimeoutPolicy.randomized(timeoutConfig.electionConfig());

        this.localId = Objects.requireNonNull(localId, "localId");
        if (!VOTERS.contains(localId)) {
            throw new IllegalArgumentException("localId must be one of the five configured voters: " + VOTERS);
        }
        this.node = Objects.requireNonNull(node, "node");
        this.state = Objects.requireNonNull(state, "state");

        if (runtimeMode == RuntimeMode.PRODUCTION) {
            if (storage == null) {
                throw new IllegalArgumentException("Production mode requires non-null durable RaftStorage");
            }
            if (!(node instanceof PersistentRaftNode)) {
                throw new IllegalArgumentException(
                        "Production mode requires PersistentRaftNode (mock/in-memory RaftNode forbidden)");
            }
            if (transport == null || transport == NO_OP_TRANSPORT) {
                throw new IllegalArgumentException(
                        "Production mode requires a real VoteTransport (mock/no-op election transport forbidden)");
            }
            timeoutConfig.validateForProduction();
        }

        this.storage = storage;

        Set<String> remotePeers = new TreeSet<>(VOTERS);
        remotePeers.remove(localId);
        this.peers = Collections.unmodifiableSet(remotePeers);

        this.onLeader = onLeader != null ? onLeader : () -> {};
        this.onStepDown = onStepDown != null ? onStepDown : () -> {};
        this.transport = transport != null ? transport : NO_OP_TRANSPORT;
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
                    if (failed || stopped || partitionIsolatedPeers.contains(peer)) {
                        // Drop RPC across simulated partition boundary or inactive node
                        return;
                    }
                    this.transport.requestVote(peer, req, response -> {
                        if (failed || stopped || partitionIsolatedPeers.contains(peer)) {
                            return;
                        }
                        long prevTerm = this.state.currentTerm();
                        Role prevRole = this.state.role();
                        int prevGrants = currentGrants.size();
                        int insertIdx = termLog.size();
                        boolean sameCampaign = prevRole == Role.CANDIDATE && prevTerm == req.getTerm();
                        try {
                            reply.receive(response);
                            syncGrantsFromCandidate();
                            if (sameCampaign && response.term() == req.getTerm()) {
                                if (response.voteGranted()) {
                                    if (this.state.role() == Role.LEADER && prevRole == Role.CANDIDATE) {
                                        TermLogEntry voteEntry = new TermLogEntry(
                                                clock.instant(),
                                                this.localId,
                                                EVENT_VOTE_GRANTED,
                                                prevTerm,
                                                prevTerm,
                                                Role.CANDIDATE,
                                                Role.CANDIDATE,
                                                null,
                                                this.state.votedFor(),
                                                this.state.commitIndex(),
                                                this.state.lastLogIndex(),
                                                "Peer " + peer + " granted vote (quorum reached for term " + prevTerm + ")");
                                        termLog.add(Math.min(insertIdx, termLog.size()), voteEntry);
                                        emitStructuredLog(voteEntry);
                                        notifyEventListeners(voteEntry);
                                    } else if (currentGrants.size() > prevGrants) {
                                        recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                                                "Peer " + peer + " granted vote (" + currentGrants.size()
                                                        + "/" + CLUSTER_SIZE + ")");
                                    }
                                } else {
                                    recordTermEvent(EVENT_VOTE_DENIED, prevTerm, prevRole,
                                            "Peer " + peer + " denied vote for term " + req.getTerm());
                                }
                            }
                        } catch (RaftStorageException | RuntimeException failure) {
                            failClosed();
                            throw failure;
                        }
                    });
                },
                termHandler,
                () -> {
                    syncGrantsFromCandidate();
                    clearPeerAcks();
                    if (runtimeMode == RuntimeMode.PRODUCTION && this.writeAvailabilityPolicy == null
                            && this.state.role() == Role.LEADER) {
                        ReplicationTracker tracker = new ReplicationTracker(this.state, this.peers);
                        CommitAdvancer advancer = new CommitAdvancer(this.state, tracker);
                        this.writeAvailabilityPolicy = new WriteAvailabilityPolicy(
                                this.localId, this.state, this.peers, tracker, advancer,
                                this.timeoutConfig.quorumFreshnessWindow());
                    }
                    recordTermEvent(
                            EVENT_LEADER_ELECTED,
                            this.state.currentTerm(),
                            Role.CANDIDATE,
                            "Elected leader with " + currentGrants.size() + "/" + CLUSTER_SIZE + " votes");
                    this.onLeader.run();
                }
        );

        recordTermEvent(
                EVENT_NODE_STARTED,
                state.currentTerm(),
                state.role(),
                "Initialized RaftElectionService in mode=" + runtimeMode
                        + " storage=" + (storage != null ? "DURABLE" : "NONE"));
        recordTermEvent(
                EVENT_TIMEOUT_CONFIGURED,
                state.currentTerm(),
                state.role(),
                "Configured timeouts heartbeat=" + timeoutConfig.electionConfig().heartbeatIntervalMillis() + "ms"
                        + " election=" + timeoutConfig.electionConfig().electionTimeoutMinMillis()
                        + ".." + timeoutConfig.electionConfig().electionTimeoutMaxMillis() + "ms"
                        + " rpc=" + timeoutConfig.rpcTimeout().toMillis() + "ms"
                        + " quorumFreshness=" + timeoutConfig.quorumFreshnessWindow().toMillis() + "ms");
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
     * Minimal test-only constructor for test doubles or components where transport is attached later.
     * Forbidden when PRODUCTION mode is enforced.
     */
    public RaftElectionService(
            String localId,
            RaftNode node,
            RaftState state,
            RaftStorage storage) {
        this(
                rejectNoOpTransportInProduction(),
                TimeoutConfig.defaults(),
                localId,
                node,
                state,
                storage,
                NO_OP_TRANSPORT,
                null,
                () -> {},
                () -> {},
                null);
    }

    private static RuntimeMode rejectNoOpTransportInProduction() {
        if (isProductionModeConfigured()) {
            throw new IllegalStateException(
                    "Mock/no-op election constructor is forbidden when PRODUCTION mode is configured");
        }
        return RuntimeMode.TEST_SIMULATION;
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
        if (state.role() == Role.LEADER) {
            throw new IllegalStateException("a leader cannot start an election");
        }
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        int insertIndex = termLog.size();
        clearPeerAcks();
        currentGrants.clear();
        try {
            long newTerm = candidateElection.startElection();
            syncGrantsFromCandidate();
            TermLogEntry startedEntry = new TermLogEntry(
                    clock.instant(),
                    localId,
                    EVENT_ELECTION_STARTED,
                    prevTerm,
                    newTerm,
                    prevRole,
                    Role.CANDIDATE,
                    null,
                    localId,
                    state.commitIndex(),
                    state.lastLogIndex(),
                    "Started election campaign for term " + newTerm);
            termLog.add(Math.min(insertIndex, termLog.size()), startedEntry);
            return newTerm;
        } catch (RaftStorageException | RuntimeException failure) {
            failClosed();
            throw failure;
        }
    }

    @Override
    public Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm) {
        return candidateElection.voteRequestFor(candidateTerm);
    }

    @Override
    public RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) throws RaftStorageException {
        requireHealthy();
        Objects.requireNonNull(request, "request").validate();
        if (!isVoter(request.getCandidateId())) {
            throw new IllegalArgumentException("candidate is not a voter");
        }
        if (partitionIsolatedPeers.contains(request.getCandidateId())) {
            // Drop or deny vote request from isolated candidate (test/simulation only)
            recordTermEvent(EVENT_VOTE_DENIED, state.currentTerm(), state.role(),
                    "Denied vote to isolated candidate " + request.getCandidateId()
                            + " for term " + request.getTerm());
            return new RequestVoteResponse(state.currentTerm(), false);
        }
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        RequestVoteResponse response;
        try {
            response = candidateElection.handleRequestVote(request);
        } catch (RaftStorageException failure) {
            failClosed();
            throw failure;
        }
        boolean steppedDownHigherTerm = state.currentTerm() > prevTerm;
        if (steppedDownHigherTerm) {
            currentGrants.clear();
            clearPeerAcks();
            if (prevRole == Role.LEADER || prevRole == Role.CANDIDATE) {
                onStepDown.run();
            }
            recordTermEvent(EVENT_HIGHER_TERM_STEP_DOWN, prevTerm, prevRole,
                    "Observed higher term " + state.currentTerm() + " from RequestVote("
                            + request.getCandidateId() + ") -> stepped down to FOLLOWER");
        } else if (prevRole == Role.LEADER && state.role() == Role.FOLLOWER) {
            currentGrants.clear();
            clearPeerAcks();
            onStepDown.run();
            recordTermEvent(EVENT_STEP_DOWN, prevTerm, prevRole,
                    "Stepped down from LEADER to FOLLOWER on RequestVote(" + request.getCandidateId() + ")");
        }
        if (response.voteGranted()) {
            recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                    "Granted vote to " + request.getCandidateId() + " for term " + state.currentTerm());
        } else {
            recordTermEvent(EVENT_VOTE_DENIED, prevTerm, prevRole,
                    "Denied vote to " + request.getCandidateId() + " for requestTerm=" + request.getTerm()
                            + " (currentTerm=" + state.currentTerm() + ", votedFor=" + state.votedFor() + ")");
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
            recordTermEvent(EVENT_VOTE_DENIED, state.currentTerm(), state.role(),
                    "Ignored vote response from isolated peer " + voterId + " for term " + term);
            return new ElectionService.IgnoredVote("peer isolated in partition");
        }
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        ElectionOutcome outcome = candidateElection.recordVoteResponse(voterId, term, granted);
        syncGrantsFromCandidate();
        if (outcome instanceof ElectionService.Won) {
            recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                    "Vote granted by " + voterId + " (quorum reached: " + currentGrants.size() + "/" + CLUSTER_SIZE + ")");
        } else if (outcome instanceof ElectionService.VoteProgress) {
            recordTermEvent(EVENT_VOTE_GRANTED, prevTerm, prevRole,
                    "Vote granted by " + voterId + " (" + currentGrants.size() + "/" + CLUSTER_SIZE + ")");
        } else if (outcome instanceof ElectionService.SteppedDown steppedDown) {
            currentGrants.clear();
            clearPeerAcks();
            try {
                observeHigherTerm(steppedDown.observedTerm());
            } catch (RaftStorageException e) {
                failClosed();
                throw new RuntimeException("Durable step down failed", e);
            }
        } else if (outcome instanceof ElectionService.IgnoredVote ignored) {
            recordTermEvent(EVENT_VOTE_DENIED, prevTerm, prevRole,
                    "Vote response from " + voterId + " (term=" + term + ", granted=" + granted
                            + ") ignored: " + ignored.reason());
        }
        return outcome;
    }

    @Override
    public void promoteToLeader(Set<String> leaderPeers) {
        requireHealthy();
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        clearPeerAcks();
        candidateElection.promoteToLeader(leaderPeers);
        syncGrantsFromCandidate();
        if (runtimeMode == RuntimeMode.PRODUCTION && this.writeAvailabilityPolicy == null
                && this.state.role() == Role.LEADER) {
            ReplicationTracker tracker = new ReplicationTracker(this.state, this.peers);
            CommitAdvancer advancer = new CommitAdvancer(this.state, tracker);
            this.writeAvailabilityPolicy = new WriteAvailabilityPolicy(
                    this.localId, this.state, this.peers, tracker, advancer,
                    this.timeoutConfig.quorumFreshnessWindow());
        }
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
        if (failed || stopped || state.role() == Role.FOLLOWER) {
            return 0;
        }
        syncGrantsFromCandidate();
        return currentGrants.size();
    }

    /**
     * Unmodifiable set of voters that granted votes in the current campaign.
     */
    public Set<String> grantedVoters() {
        if (failed || stopped || state.role() == Role.FOLLOWER) {
            return Set.of();
        }
        syncGrantsFromCandidate();
        return Collections.unmodifiableSet(new TreeSet<>(currentGrants));
    }

    /**
     * Returns true if the active campaign has reached majority quorum (>= 3 votes).
     */
    public boolean hasWonElection() {
        return !failed && !stopped && state.role() != Role.FOLLOWER
                && grantedVoteCount() >= REQUIRED_VOTES;
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
            candidateElection.clearCampaignOnStepDown();
            clearPeerAcks();
            onStepDown.run();
            try {
                if (node instanceof PersistentRaftNode durable) {
                    durable.observeHigherTerm(higherTerm);
                } else if (persistence != null) {
                    persistence.observeHigherTerm(higherTerm);
                } else {
                    state.becomeFollower(higherTerm);
                }
            } catch (RaftStorageException | RuntimeException failure) {
                failClosed();
                throw failure;
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
        this.writeAvailabilityPolicy = new WriteAvailabilityPolicy(
                localId, state, peers, tracker, advancer, timeoutConfig.quorumFreshnessWindow());
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
        if (peerId == null || !peers.contains(peerId)) {
            throw new IllegalArgumentException("unknown remote peer: " + peerId);
        }
        Objects.requireNonNull(timestamp, "timestamp");
        if (failed || stopped || state.role() != Role.LEADER || partitionIsolatedPeers.contains(peerId)) {
            return;
        }
        peerAckTimes.put(peerId, new TermedAck(state.currentTerm(), timestamp));
        if (writeAvailabilityPolicy != null) {
            writeAvailabilityPolicy.recordPeerAck(peerId, matchIndex, timestamp);
        }
    }

    /**
     * Records a peer ping (proves peer connectivity, but NOT commit ACK).
     */
    public void recordPeerPing(String peerId, Instant timestamp) {
        if (peerId == null || !peers.contains(peerId)) {
            throw new IllegalArgumentException("unknown remote peer: " + peerId);
        }
        Objects.requireNonNull(timestamp, "timestamp");
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
        if (runtimeMode == RuntimeMode.PRODUCTION) {
            throw new IllegalStateException(
                    "Production mode requires WriteAvailabilityPolicy; fallback mock counting is forbidden");
        }
        if (failed || stopped || state.role() != Role.LEADER) {
            return 0;
        }
        long currentTerm = state.currentTerm();
        int count = 1; // local leader
        Duration window = timeoutConfig.quorumFreshnessWindow();
        for (String peer : peers) {
            if (partitionIsolatedPeers.contains(peer)) {
                continue;
            }
            TermedAck ack = peerAckTimes.get(peer);
            if (ack != null && ack.term() == currentTerm
                    && !ack.timestamp().isAfter(now)
                    && !ack.timestamp().isBefore(now.minus(window))) {
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
        try {
            PersistentState loaded = storage.load();
            validateDurableState(loaded, state.currentTerm());
            recordTermEvent(EVENT_RECOVERY_RESTORED, prevTerm, prevRole,
                    "Recovered durable state term=" + loaded.currentTerm()
                            + ", votedFor=" + loaded.votedFor()
                            + ", lastApplied=" + loaded.lastApplied()
                            + ", logEntries=" + loaded.log().size());
            return loaded;
        } catch (RaftStorageException | RuntimeException failure) {
            recordTermEvent(EVENT_RECOVERY_FAILED, prevTerm, prevRole,
                    "Durable recovery failed on " + localId + ": " + failure.getMessage());
            throw failure;
        }
    }

    // -------------------------------------------------------------------------------------------
    // G5 Fault Scenario Simulation Helpers (TEST_SIMULATION mode only; forbidden in PRODUCTION)
    // -------------------------------------------------------------------------------------------

    /**
     * Isolates a peer from local communication (simulating link failure or partition).
     */
    public void isolatePeer(String peer) {
        requireSimulationMode("isolatePeer");
        if (peers.contains(peer)) {
            partitionIsolatedPeers.add(peer);
            peerAckTimes.remove(peer);
            if (writeAvailabilityPolicy != null) {
                writeAvailabilityPolicy.markPeerUnresponsive(peer);
            }
            recordTermEvent(EVENT_PARTITION_APPLIED, state.currentTerm(), state.role(),
                    "Isolated peer " + peer + " (isolated=" + isolatedPeers() + ")");
        }
    }

    /**
     * Heals communication with an isolated peer.
     */
    public void healPeer(String peer) {
        requireSimulationMode("healPeer");
        if (partitionIsolatedPeers.remove(peer)) {
            recordTermEvent(EVENT_PARTITION_HEALED, state.currentTerm(), state.role(),
                    "Healed peer " + peer + " (remaining isolated=" + isolatedPeers() + ")");
        }
    }

    /**
     * Configures a simulated partition, isolating the given set of peers.
     */
    public void partition(Set<String> isolatedPeers) {
        requireSimulationMode("partition");
        partitionIsolatedPeers.clear();
        for (String p : isolatedPeers) {
            if (peers.contains(p)) {
                partitionIsolatedPeers.add(p);
                peerAckTimes.remove(p);
                if (writeAvailabilityPolicy != null) {
                    writeAvailabilityPolicy.markPeerUnresponsive(p);
                }
            }
        }
        recordTermEvent(EVENT_PARTITION_APPLIED, state.currentTerm(), state.role(),
                "Applied partition isolating peers=" + isolatedPeers());
    }

    /**
     * Heals all simulated partitions.
     */
    public void healAll() {
        requireSimulationMode("healAll");
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
    // TV5-D25 / TV5-D28: LAN Failover Stop-Leader Actions, Event Logging & Trace Auditing
    // -------------------------------------------------------------------------------------------

    /**
     * Configures a custom clock for deterministic term-log timestamps in tests.
     * Forbidden in PRODUCTION mode.
     */
    public void setClock(Clock clock) {
        requireSimulationMode("setClock");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Registers a listener notified synchronously whenever a structured {@link TermLogEntry} is recorded.
     */
    public void addEventListener(Consumer<TermLogEntry> listener) {
        eventListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Removes a previously registered event log listener.
     */
    public void removeEventListener(Consumer<TermLogEntry> listener) {
        eventListeners.remove(listener);
    }

    /**
     * Returns an immutable snapshot of all structured term/failover events recorded by this service.
     */
    public List<TermLogEntry> termLog() {
        return List.copyOf(termLog);
    }

    /**
     * Exports all recorded structured election/recovery events as formatted single-line log records.
     */
    public List<String> exportEventLogs() {
        List<String> lines = new ArrayList<>(termLog.size());
        long applied = state.lastApplied();
        for (TermLogEntry entry : termLog) {
            lines.add(entry.toDetailedLogLine(applied, runtimeMode));
        }
        return Collections.unmodifiableList(lines);
    }

    /**
     * Clears the in-memory term log (useful between independent test scenarios).
     * Forbidden in PRODUCTION mode so audit history cannot be wiped.
     */
    public void clearTermLog() {
        requireSimulationMode("clearTermLog");
        termLog.clear();
    }

    /**
     * Appends a structured term/role transition entry to this node's audit log, emits a runtime
     * {@link Logger} record, and notifies registered event listeners.
     */
    public TermLogEntry recordTermEvent(String eventType, long previousTerm, Role previousRole, String detail) {
        String effectiveLeader = state.role() == Role.LEADER ? localId : state.leaderId();
        TermLogEntry entry = new TermLogEntry(
                clock.instant(),
                localId,
                Objects.requireNonNull(eventType, "eventType"),
                previousTerm,
                state.currentTerm(),
                previousRole,
                state.role(),
                effectiveLeader,
                state.votedFor(),
                state.commitIndex(),
                state.lastLogIndex(),
                detail);
        termLog.add(entry);
        emitStructuredLog(entry);
        notifyEventListeners(entry);
        return entry;
    }

    private void notifyEventListeners(TermLogEntry entry) {
        for (Consumer<TermLogEntry> listener : eventListeners) {
            try {
                listener.accept(entry);
            } catch (RuntimeException listenerError) {
                LOGGER.log(Level.WARNING,
                        "[" + localId + "] Event listener failed for " + entry.eventType() + ": "
                                + listenerError.getMessage(),
                        listenerError);
            }
        }
    }

    private void emitStructuredLog(TermLogEntry entry) {
        Level level = switch (entry.eventType()) {
            case EVENT_FAIL_CLOSED, EVENT_RECOVERY_FAILED -> Level.SEVERE;
            case EVENT_HIGHER_TERM_STEP_DOWN, EVENT_STEP_DOWN, EVENT_VOTE_DENIED, EVENT_PARTITION_APPLIED -> Level.WARNING;
            default -> Level.INFO;
        };
        if (LOGGER.isLoggable(level)) {
            LOGGER.log(level, () -> "[RAFT-ELECTION] " + entry.toDetailedLogLine(state.lastApplied(), runtimeMode));
        }
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
        requireSimulationMode("simulateStopLeader");
        StopLeaderAction prepared = prepareLocalStopLeaderAction(clientEndpoint, reason);
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        stopped = true;
        partitionIsolatedPeers.addAll(peers);
        currentGrants.clear();
        candidateElection.clearCampaignOnStepDown();
        clearPeerAcks();
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
     * Restarts a previously stopped node, validating durable storage (if attached) without regressing term
     * or wiping the persisted {@code votedFor} record.
     */
    public PersistentState simulateRestartAfterStop() throws RaftStorageException {
        requireSimulationMode("simulateRestartAfterStop");
        if (!stopped) {
            throw new IllegalStateException("Node " + localId + " is not stopped");
        }
        long prevTerm = state.currentTerm();
        Role prevRole = state.role();
        PersistentState loaded = null;
        if (storage != null) {
            loaded = storage.load();
            validateDurableState(loaded, prevTerm);
            long restoredTerm = loaded.currentTerm();
            String persistedVote = loaded.votedFor();
            if (restoredTerm > state.currentTerm()) {
                state.becomeFollower(restoredTerm);
                if (persistedVote != null) {
                    boolean restored = state.grantVote(
                            restoredTerm, persistedVote, state.lastLogIndex(), state.lastLogTerm());
                    if (!restored || !persistedVote.equals(state.votedFor())) {
                        failClosed();
                        throw new RaftStorageException("Failed to restore persisted votedFor=" + persistedVote
                                + " for term " + restoredTerm + " on " + localId);
                    }
                }
            } else {
                state.becomeFollower(restoredTerm);
                if (persistedVote != null) {
                    if (state.votedFor() == null) {
                        boolean restored = state.grantVote(
                                restoredTerm, persistedVote, state.lastLogIndex(), state.lastLogTerm());
                        if (!restored || !persistedVote.equals(state.votedFor())) {
                            failClosed();
                            throw new RaftStorageException("Failed to restore persisted votedFor=" + persistedVote
                                    + " for term " + restoredTerm + " on " + localId);
                        }
                    } else if (!persistedVote.equals(state.votedFor())) {
                        failClosed();
                        throw new RaftStorageException("Divergent vote on restart for " + localId
                                + " at term " + restoredTerm + ": memory=" + state.votedFor()
                                + ", storage=" + persistedVote);
                    }
                } else if (state.votedFor() != null) {
                    failClosed();
                    throw new RaftStorageException("Unpersisted in-memory vote detected on restart for " + localId
                            + " at term " + restoredTerm + ": memory=" + state.votedFor() + ", storage=null");
                }
            }
        } else {
            state.becomeFollower(prevTerm);
        }
        partitionIsolatedPeers.clear();
        currentGrants.clear();
        candidateElection.clearCampaignOnStepDown();
        clearPeerAcks();
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
        Map<String, String> voteByNodeTerm = new LinkedHashMap<>();
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

            if (entry.votedFor() != null) {
                String nodeTermKey = entry.nodeId() + "@" + entry.currentTerm();
                String priorVote = voteByNodeTerm.putIfAbsent(nodeTermKey, entry.votedFor());
                if (priorVote != null && !priorVote.equals(entry.votedFor())) {
                    violations.add("Entry " + i + ": Vote-once-per-term violated on " + entry.nodeId()
                            + " at term " + entry.currentTerm()
                            + " (votedFor=" + priorVote + " then " + entry.votedFor() + ")");
                }
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

    public RuntimeMode runtimeMode() {
        return runtimeMode;
    }

    public boolean isProductionMode() {
        return runtimeMode == RuntimeMode.PRODUCTION;
    }

    public TimeoutConfig timeoutConfig() {
        return timeoutConfig;
    }

    public ElectionConfig electionConfig() {
        return timeoutConfig.electionConfig();
    }

    public long nextElectionTimeoutMillis() {
        return timeoutPolicy.nextTimeoutMillis();
    }

    public Duration rpcTimeout() {
        return timeoutConfig.rpcTimeout();
    }

    public Duration quorumFreshnessWindow() {
        return timeoutConfig.quorumFreshnessWindow();
    }

    private void syncGrantsFromCandidate() {
        Set<String> snapshot = candidateElection.grantedVotersSnapshot();
        currentGrants.retainAll(snapshot);
        currentGrants.addAll(snapshot);
    }

    private void clearPeerAcks() {
        peerAckTimes.clear();
        if (writeAvailabilityPolicy != null) {
            writeAvailabilityPolicy.clearPeerActivity();
        }
    }

    private void validateDurableState(PersistentState loaded, long minTerm) throws RaftStorageException {
        if (loaded == null) {
            failClosed();
            throw new RaftStorageException("storage returned null PersistentState for " + localId);
        }
        if (loaded.currentTerm() < minTerm) {
            failClosed();
            throw new RaftStorageException("term regression in storage: stored="
                    + loaded.currentTerm() + ", expected>=" + minTerm);
        }
        if (loaded.currentTerm() == 0L && loaded.votedFor() != null) {
            failClosed();
            throw new RaftStorageException("invalid durable state: votedFor=" + loaded.votedFor()
                    + " cannot exist at term 0 on " + localId);
        }
        if (loaded.votedFor() != null && !isVoter(loaded.votedFor())) {
            failClosed();
            throw new RaftStorageException("invalid durable state: non-voter votedFor=" + loaded.votedFor()
                    + " on " + localId);
        }
        try {
            RaftState.restore(loaded.currentTerm(), loaded.votedFor(), loaded.log(), loaded.lastApplied());
        } catch (IllegalArgumentException corrupt) {
            failClosed();
            throw new RaftStorageException("corrupt durable state for " + localId + ": " + corrupt.getMessage(), corrupt);
        }
    }

    private void requireSimulationMode(String operation) {
        if (runtimeMode == RuntimeMode.PRODUCTION) {
            LOGGER.log(Level.WARNING,
                    "[{0}] Blocked mock/simulation operation ''{1}'' in PRODUCTION mode",
                    new Object[] { localId, operation });
            throw new UnsupportedOperationException(
                    "Mock/simulation operation '" + operation + "' is forbidden in PRODUCTION mode");
        }
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
        boolean firstFailure = !failed;
        failed = true;
        currentGrants.clear();
        candidateElection.clearCampaignOnStepDown();
        clearPeerAcks();
        if (firstFailure) {
            recordTermEvent(EVENT_FAIL_CLOSED, state.currentTerm(), state.role(),
                    "RaftElectionService transitioned to FAIL_CLOSED state on " + localId);
        }
    }

    private static boolean hasConfigValue(
            Map<String, String> env, Properties props, String envKey, String propKey) {
        return getFirstNonBlank(
                env.get(envKey),
                System.getProperty(propKey),
                props.getProperty(propKey)) != null;
    }

    private static long readConfigLong(
            Map<String, String> env,
            Properties props,
            String envKey,
            String propKey,
            long fallback) {
        String raw = getFirstNonBlank(
                env.get(envKey),
                System.getProperty(propKey),
                props.getProperty(propKey));
        if (raw == null) {
            return fallback;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    "Invalid numeric config for " + envKey + "/" + propKey + ": '" + raw + "'", ex);
        }
    }

    private static String getFirstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }
}
