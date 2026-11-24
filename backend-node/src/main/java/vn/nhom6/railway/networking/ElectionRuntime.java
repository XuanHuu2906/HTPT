package vn.nhom6.railway.networking;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingRaftBridge;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.booking.GetRequestResultHandler;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.persistence.JdbcRequestResultStore;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.ElectionConfig;
import vn.nhom6.railway.raft.election.RaftElectionComponent;
import vn.nhom6.railway.raft.election.VoteRpcAdapter;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy;
import vn.nhom6.railway.raft.replication.AppendRetryCoordinator;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.RaftReadBarrier;
import vn.nhom6.railway.raft.replication.RaftReplicationService;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

/**
 * Full per-node Raft runtime composition: real TCP election, heartbeat, log replication,
 * majority current-term commit (Figure 8), NO_OP read barrier, and atomic PostgreSQL apply.
 */
public final class ElectionRuntime implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ElectionRuntime.class.getName());
    private static final int REPLICATION_BATCH_MAX = 64;
    private static final int MAX_BACKTRACK_ATTEMPTS = 1000;

    private final String localId;
    private final List<String> remotePeers;
    private final RaftRpcClient rpc;
    private final RaftElectionComponent election;
    private final Duration rpcDeadline;
    private final Duration freshnessWindow;
    private final Duration readBarrierTimeout;
    private final ScheduledThreadPoolExecutor deadlines =
            new ScheduledThreadPoolExecutor(2, task -> {
                Thread thread = new Thread(task, "raft-rpc-deadlines");
                thread.setDaemon(true);
                return thread;
            });

    private final ConcurrentHashMap<String, Boolean> peerConnectivity = new ConcurrentHashMap<>();
    private final Consumer<Exception> failureCallback;

    // Initialized during configureWiring on the serialized Raft event loop
    private RaftState state;
    private PersistentRaftNode core;
    private ScheduledThreadPoolExecutor raftLoop;
    private RaftApplyAdapter applyAdapter;

    // Leader-scoped active replication/quorum components (mutated on the Raft event loop)
    private volatile RaftReplicationService activeReplication;
    private volatile WriteAvailabilityPolicy activeAvailability;

    // Client-facing booking/read consensus bridges
    private final BookingRaftBridge bookingBridge;
    private final RaftReadBarrier readBarrier;
    private final BookingRequestHandler.CommandSubmitter commandSubmitter;
    private final GetRequestResultHandler getRequestResultHandler;

    public ElectionRuntime(NodeConfig config, Map<String, String> env, Consumer<Exception> failure)
            throws Exception {
        this(config, env, createJdbcComponents(config), failure);
    }

    private record StorageBundle(
            RaftStorage raftStorage,
            BookingRepository bookingRepository,
            RaftApplyAdapter.EntryApplier entryApplier,
            GetRequestResultHandler.ResultStore resultStore,
            BookingRaftBridge.FingerprintLookup fingerprintLookup) { }

    private static StorageBundle createJdbcComponents(NodeConfig config) throws Exception {
        var factory = new ConnectionFactory(Objects.requireNonNull(
                Objects.requireNonNull(config, "config").dbConfig(),
                "Election runtime requires PostgreSQL"));
        var storage = new JdbcRaftStorage(factory);
        var bookingRepository = new JdbcBookingRepository(factory);
        var requestResultStore = new JdbcRequestResultStore();

        // Validate D13 before constructing core, timers or any TCP listener.
        new DbStateLoader(storage, bookingRepository, factory).load();

        var committedApplier = new JdbcCommittedCommandApplier(factory);
        BookingRaftBridge.FingerprintLookup fingerprintLookup = requestId -> {
            try (Connection connection = factory.getConnection()) {
                return requestResultStore.find(connection, requestId)
                        .map(JdbcRequestResultStore.StoredRequestResult::intentFingerprint);
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to query stored fingerprint for " + requestId, e);
                return Optional.empty();
            }
        };
        return new StorageBundle(
                storage,
                bookingRepository,
                committedApplier::applyCommitted,
                GetRequestResultHandler.fromJdbc(factory, requestResultStore),
                fingerprintLookup);
    }

    /** Package-private test constructor exercising the exact same Raft/TCP/Booking composition. */
    ElectionRuntime(
            NodeConfig config,
            Map<String, String> env,
            RaftStorage raftStorage,
            BookingRepository bookingRepository,
            RaftApplyAdapter.EntryApplier entryApplier,
            GetRequestResultHandler.ResultStore resultStore,
            BookingRaftBridge.FingerprintLookup fingerprintLookup,
            Consumer<Exception> failure) throws Exception {
        this(config, env, new StorageBundle(
                Objects.requireNonNull(raftStorage, "raftStorage"),
                Objects.requireNonNull(bookingRepository, "bookingRepository"),
                Objects.requireNonNull(entryApplier, "entryApplier"),
                Objects.requireNonNull(resultStore, "resultStore"),
                fingerprintLookup), failure);
    }

    private ElectionRuntime(NodeConfig config, Map<String, String> env, StorageBundle bundle, Consumer<Exception> failure)
            throws Exception {
        this.localId = Objects.requireNonNull(config, "config").nodeId();
        this.failureCallback = Objects.requireNonNull(failure, "failure");
        var endpoints = parsePeers(env.get("NODE_PEERS"));
        this.remotePeers = endpoints.keySet().stream()
                .filter(id -> !id.equals(localId))
                .toList();
        for (String peer : remotePeers) {
            peerConnectivity.put(peer, Boolean.FALSE);
        }

        var storage = bundle.raftStorage();
        var bookingRepository = bundle.bookingRepository();

        long rpcDeadlineMs = Math.min(number(env, "TCP_RESPONSE_DEADLINE_MS", 1000), 1000L);
        this.rpcDeadline = Duration.ofMillis(Math.max(200L, rpcDeadlineMs));
        long clientDeadlineMs = number(env, "TCP_RESPONSE_DEADLINE_MS", 3000);
        Duration bridgeTimeout = Duration.ofMillis(Math.max(1000L, Math.min(2200L, clientDeadlineMs - 500L)));
        this.readBarrierTimeout = Duration.ofMillis(Math.max(1000L, Math.min(2500L, clientDeadlineMs - 300L)));

        var timers = new ElectionConfig(
                number(env, "RAFT_HEARTBEAT_INTERVAL_MS", 150),
                number(env, "RAFT_ELECTION_TIMEOUT_MIN_MS", 500),
                number(env, "RAFT_ELECTION_TIMEOUT_MAX_MS", 1000));
        this.freshnessWindow = Duration.ofMillis(Math.max(1000L, timers.heartbeatIntervalMillis() * 6L));

        rpc = new RaftRpcClient(localId, endpoints, 8,
                Math.toIntExact(number(env, "TCP_CONNECT_TIMEOUT_MS", 300)),
                rpcDeadline.multipliedBy(2));

        var reference = new AtomicReference<RaftElectionComponent>();
        java.util.concurrent.Executor loop = task -> {
            RaftElectionComponent comp = reference.get();
            if (comp != null) {
                comp.execute(task);
            }
        };

        try {
            var votes = new VoteRpcAdapter(localId, rpc::exchange, rpcDeadline, loop, failure);
            election = new RaftElectionComponent(localId, storage,
                    (restoredState, durable) -> new PersistentRaftNode(localId, restoredState, durable),
                    votes, timers, failure, () -> {});
            reference.set(election);
            deadlines.setRemoveOnCancelPolicy(true);

            // Wire commit listener & replication lifecycle onto the serialized Raft event loop.
            election.configureWiring(ctx -> {
                this.state = ctx.state();
                this.core = (PersistentRaftNode) ctx.core();
                this.raftLoop = ctx.loop();
                this.applyAdapter = new RaftApplyAdapter(
                        bundle.entryApplier(),
                        bookingRepository,
                        core::commitIndex,
                        core::onApplied);
                this.core.addCommitListener(this.applyAdapter);
            }, this::onBecomeLeaderOnLoop, this::onStepDownOnLoop);

            // Delegate periodic heartbeats to the active replication service so every peer has
            // strictly at most one in-flight APPEND_ENTRIES RPC carrying up-to-date entries/leaderCommit.
            election.configureHeartbeat((peer, request, reply) -> {
                RaftReplicationService replication = activeReplication;
                if (replication != null) {
                    replication.replicate(peer);
                }
            });

            // Build client-facing BookingRaftBridge (all RaftState/core calls serialized on loop).
            BookingRaftBridge.RaftPort raftPort = createLoopSerializedRaftPort();
            this.bookingBridge = new BookingRaftBridge(
                    raftPort, applyAdapter, bookingRepository, bundle.fingerprintLookup(), bridgeTimeout);

            // Build leader-side NO_OP ReadBarrier (SPEC-04 §7).
            RaftReplicationService.Scheduler deadlineScheduler = (task, delayMillis) -> {
                ScheduledFuture<?> future = deadlines.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
                return () -> future.cancel(false);
            };
            this.readBarrier = new RaftReadBarrier(
                    createLoopSerializedReadBarrierPort(),
                    applyAdapter::awaitIndex,
                    deadlineScheduler);

            // Wrap BookingRaftBridge for BookingRequestHandler, classifying verified minority NO_QUORUM
            // versus indeterminate TIMEOUT_UNKNOWN without ever rolling back uncommitted entries.
            this.commandSubmitter = command -> {
                CompletionStage<ApplyResult> stage = bookingBridge.submit(command);
                return stage.thenCompose(result -> {
                    if (result != null && result.isPersistable()) {
                        return CompletableFuture.completedFuture(result);
                    }
                    if (isVerifiedNoQuorumOnLeader()) {
                        return CompletableFuture.failedFuture(new BookingRequestHandler.NoQuorumException());
                    }
                    return CompletableFuture.failedFuture(
                            new TimeoutException("Booking command did not commit and apply within deadline"));
                });
            };

            // Build authoritative GET_REQUEST_RESULT handler backed by NO_OP ReadBarrier + store.
            GetRequestResultHandler.ReadPolicy readPolicy = new GetRequestResultHandler.ReadPolicy() {
                @Override
                public void ensureAuthoritative()
                        throws BookingRequestHandler.NotLeaderException,
                               BookingRequestHandler.ReadUnavailableException {
                    try {
                        BookingRequestHandler.ReadBarrierResult barrierResult = readBarrier
                                .appendAndAwaitNoOp(readBarrierTimeout)
                                .toCompletableFuture()
                                .get(readBarrierTimeout.toNanos(), TimeUnit.NANOSECONDS);
                        if (barrierResult == null || barrierResult.readIndex() < 1) {
                            throw new BookingRequestHandler.ReadUnavailableException(
                                    "Read barrier returned invalid readIndex");
                        }
                    } catch (ExecutionException | CompletionException ex) {
                        Throwable cause = ex.getCause();
                        if (cause instanceof BookingRequestHandler.NotLeaderException nle) {
                            throw nle;
                        }
                        if (cause instanceof BookingRequestHandler.ReadUnavailableException rue) {
                            throw rue;
                        }
                        throw new BookingRequestHandler.ReadUnavailableException(
                                "Read barrier unavailable: " + (cause != null ? cause.getMessage() : ex.getMessage()));
                    } catch (TimeoutException ex) {
                        throw new BookingRequestHandler.ReadUnavailableException(
                                "Read barrier timed out before quorum apply");
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new BookingRequestHandler.ReadUnavailableException("Read barrier interrupted");
                    }
                }

                @Override
                public Optional<String> leaderHint() {
                    try {
                        return Optional.ofNullable(election.detailedStatus().leaderId());
                    } catch (Exception ignored) {
                        return Optional.empty();
                    }
                }
            };
            this.getRequestResultHandler = new GetRequestResultHandler(
                    readPolicy, bundle.resultStore(), applyAdapter);
        } catch (Exception startup) {
            closeResourcesQuietly();
            throw startup;
        }
    }

    /** Called on the serialized Raft event loop when this node transitions to LEADER. */
    private void onBecomeLeaderOnLoop() {
        stopActiveReplicationOnLoop();
        if (state.role() != Role.LEADER) {
            return;
        }

        // Fresh RaftTransportAdapter per leadership term avoids any stale in-flight peer slot lock.
        var termTransport = new RaftTransportAdapter(
                localId, rpc::exchange, rpcDeadline, election::execute, deadlines, failureCallback);

        var tracker = new ReplicationTracker(state, remotePeers);
        var commitAdvancer = new CommitAdvancer(state, tracker);
        var availability = new WriteAvailabilityPolicy(
                localId, state, remotePeers, tracker, commitAdvancer, freshnessWindow);

        RaftReplicationService.Scheduler loopScheduler = (task, delayMillis) -> {
            ScheduledFuture<?> future = raftLoop.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
            return () -> future.cancel(false);
        };

        var replicationRef = new AtomicReference<RaftReplicationService>();
        var hooks = new RaftReplicationService.Hooks() {
            @Override
            public void onHigherTerm(long term) {
                election.observeHigherTermFromReplication(term);
            }

            @Override
            public void onMatchIndexAdvanced(String peerId, long matchIndex) {
                peerConnectivity.put(peerId, Boolean.TRUE);
                if (state.role() != Role.LEADER) {
                    return;
                }
                availability.recordPeerAck(peerId, matchIndex, Instant.now());
                OptionalLong advanced = commitAdvancer.advance();
                if (advanced.isPresent()) {
                    long newCommit = advanced.getAsLong();
                    applyAdapter.advanceCommitIndex(newCommit);
                    election.onCommitIndexAdvanced(newCommit);
                    RaftReplicationService current = replicationRef.get();
                    if (current != null && state.role() == Role.LEADER) {
                        current.replicateAll();
                    }
                    core.deliverCommittedEntries();
                }
            }

            @Override
            public void onRejected(String peerId, RaftNode.AppendEntriesRequest request) {
                peerConnectivity.put(peerId, Boolean.TRUE);
            }

            @Override
            public void onFailure(String peerId, Throwable failure) {
                peerConnectivity.put(peerId, Boolean.FALSE);
                LOGGER.log(Level.WARNING, "[" + localId + "] Replication hook failure for " + peerId, failure);
            }
        };

        var replication = new RaftReplicationService(
                localId,
                state,
                termTransport,
                remotePeers,
                election::execute,
                loopScheduler,
                REPLICATION_BATCH_MAX,
                new RaftReplicationService.Backoff(50L, 500L),
                hooks);
        replicationRef.set(replication);

        new AppendRetryCoordinator(
                state,
                tracker,
                replication,
                election::execute,
                loopScheduler,
                new RaftReplicationService.Backoff(25L, 250L),
                MAX_BACKTRACK_ATTEMPTS,
                new AppendRetryCoordinator.Hooks() {
                    @Override
                    public void onHigherTerm(long term) {
                        election.observeHigherTermFromReplication(term);
                    }

                    @Override
                    public void onGiveUp(String peerId, String reason) {
                        LOGGER.warning("[" + localId + "] Replication backtrack gave up for "
                                + peerId + ": " + reason);
                    }
                });

        this.activeReplication = replication;
        this.activeAvailability = availability;

        // Per Raft Figure 8: if uncommitted entries from a previous term exist on this newly elected
        // Leader, append one current-term NO_OP so they can be committed once replicated to a quorum.
        if (state.lastLogIndex() > state.commitIndex()) {
            core.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        }
    }

    /** Called on the serialized Raft event loop on any step-down or failure. */
    private void onStepDownOnLoop() {
        boolean hadActiveLeaderState = (activeReplication != null || activeAvailability != null);
        stopActiveReplicationOnLoop();
        if (hadActiveLeaderState) {
            if (bookingBridge != null) {
                bookingBridge.onStepDown();
            }
            if (readBarrier != null && readBarrier.pendingCount() > 0) {
                readBarrier.onLeadershipLost();
            }
        }
    }

    private void stopActiveReplicationOnLoop() {
        RaftReplicationService prevReplication = activeReplication;
        activeReplication = null;
        if (prevReplication != null) {
            prevReplication.stop();
        }
        WriteAvailabilityPolicy prevAvailability = activeAvailability;
        activeAvailability = null;
        if (prevAvailability != null) {
            prevAvailability.clearPeerActivity();
        }
    }

    private boolean isVerifiedNoQuorumOnLeader() {
        try {
            return election.submitOnLoop(() -> {
                if (state.role() != Role.LEADER) {
                    return false;
                }
                WriteAvailabilityPolicy availability = activeAvailability;
                return availability != null && !availability.hasCommitQuorum(Instant.now());
            });
        } catch (Exception ignored) {
            return false;
        }
    }

    private BookingRaftBridge.RaftPort createLoopSerializedRaftPort() {
        return new BookingRaftBridge.RaftPort() {
            @Override
            public Role role() {
                return callOnLoopUnchecked(() -> state.role());
            }

            @Override
            public Optional<String> leaderId() {
                return callOnLoopUnchecked(() -> Optional.ofNullable(
                        state.role() == Role.LEADER ? localId : state.leaderId()));
            }

            @Override
            public long currentTerm() {
                return callOnLoopUnchecked(() -> state.currentTerm());
            }

            @Override
            public RaftNode.AppendOutcome appendCommand(String commandType, String commandPayload) {
                return callOnLoopUnchecked(() -> {
                    RaftNode.AppendOutcome outcome = core.appendCommand(commandType, commandPayload);
                    if (outcome instanceof RaftNode.Accepted && activeReplication != null) {
                        activeReplication.replicateAll();
                    }
                    return outcome;
                });
            }
        };
    }

    private RaftReadBarrier.Port createLoopSerializedReadBarrierPort() {
        return new RaftReadBarrier.Port() {
            @Override
            public String localId() {
                return localId;
            }

            @Override
            public boolean isLeader() {
                return callOnLoopUnchecked(() -> state.role() == Role.LEADER);
            }

            @Override
            public long currentTerm() {
                return callOnLoopUnchecked(() -> state.currentTerm());
            }

            @Override
            public Optional<String> leaderId() {
                return callOnLoopUnchecked(() -> Optional.ofNullable(
                        state.role() == Role.LEADER ? localId : state.leaderId()));
            }

            @Override
            public RaftNode.AppendOutcome appendNoOp() {
                return callOnLoopUnchecked(() -> {
                    RaftNode.AppendOutcome outcome = core.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
                    if (outcome instanceof RaftNode.Accepted && activeReplication != null) {
                        activeReplication.replicateAll();
                    }
                    return outcome;
                });
            }

            @Override
            public long commitIndex() {
                return callOnLoopUnchecked(() -> state.commitIndex());
            }

            @Override
            public OptionalLong termAt(long index) {
                return callOnLoopUnchecked(() -> {
                    if (index < RaftLogEntry.FIRST_INDEX || index > state.lastLogIndex()) {
                        return OptionalLong.empty();
                    }
                    return OptionalLong.of(state.termAt(index));
                });
            }
        };
    }

    private <T> T callOnLoopUnchecked(java.util.concurrent.Callable<T> task) {
        try {
            return election.submitOnLoop(task);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ex) {
            throw new IllegalStateException("Raft event loop invocation failed", ex);
        }
    }

    public RaftElectionComponent election() {
        return election;
    }

    public BookingRequestHandler.CommandSubmitter commandSubmitter() {
        return commandSubmitter;
    }

    public BookingRequestHandler.ReadBarrier readBarrier() {
        return readBarrier;
    }

    public GetRequestResultHandler getRequestResultHandler() {
        return getRequestResultHandler;
    }

    public Map<String, Boolean> peerConnectivitySnapshot() {
        Map<String, Boolean> snapshot = new LinkedHashMap<>();
        for (String peer : remotePeers) {
            snapshot.put(peer, Boolean.TRUE.equals(peerConnectivity.get(peer)));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    public static Map<String, InetSocketAddress> parsePeers(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("NODE_PEERS is required");
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (String token : value.split(",", -1)) {
            String[] pair = token.trim().split("@", -1);
            if (pair.length != 2 || !pair[0].matches("node-[1-5]")) {
                throw new IllegalArgumentException("NODE_PEERS expects node-N@host:port");
            }
            int colon = pair[1].lastIndexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("peer host:port required");
            int port = Integer.parseInt(pair[1].substring(colon + 1));
            if (port < 1 || port > 65535) throw new IllegalArgumentException("peer port out of range");
            if (peers.put(pair[0], InetSocketAddress.createUnresolved(pair[1].substring(0, colon), port)) != null) {
                throw new IllegalArgumentException("duplicate peer id");
            }
        }
        if (!peers.keySet().equals(Set.of("node-1", "node-2", "node-3", "node-4", "node-5"))) {
            throw new IllegalArgumentException("exactly five configured voters required");
        }
        return Map.copyOf(peers);
    }

    private static long number(Map<String, String> env, String key, long fallback) {
        return env.containsKey(key) ? Long.parseLong(env.get(key)) : fallback;
    }

    private void closeResourcesQuietly() {
        try {
            if (bookingBridge != null) bookingBridge.close();
        } catch (Exception ignored) { }
        try {
            if (election != null) election.close();
        } catch (Exception ignored) { }
        try {
            if (rpc != null) rpc.close();
        } catch (Exception ignored) { }
        deadlines.shutdownNow();
    }

    @Override
    public void close() {
        closeResourcesQuietly();
    }
}
