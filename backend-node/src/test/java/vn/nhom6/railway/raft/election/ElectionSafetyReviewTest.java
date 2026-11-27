package vn.nhom6.railway.raft.election;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.NodeStatusDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.RaftElectionService.TermLogEntry;
import vn.nhom6.railway.raft.election.RaftElectionService.TraceAuditReport;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityResult;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityStatus;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutOutcome;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutVerificationSituation;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D27: Independent Raft Election Safety Review &amp; P0/P1 Regression Suite.
 *
 * <p>Covers all six required election safety properties from SPEC-04 (§2, §3, §5, §6, §8–§10)
 * and SPEC-07 ({@code TC-RAFT-01}, {@code TC-RAFT-02}, {@code TC-RAFT-06}, {@code TC-RAFT-08},
 * {@code TC-RAFT-09}) with automated assertions and explicit property-named verification logs:
 * <ol>
 *   <li>{@code PROP-01 VOTE_ONCE_PER_TERM}</li>
 *   <li>{@code PROP-02 STALE_LOG_ELIGIBILITY}</li>
 *   <li>{@code PROP-03 HIGHER_TERM_STEPDOWN}</li>
 *   <li>{@code PROP-04 TIMER_RACES}</li>
 *   <li>{@code PROP-05 QUORUM_SAFETY}</li>
 *   <li>{@code PROP-06 RESTART_RECOVERY}</li>
 * </ol>
 */
@DisplayName("TV5-D27: Raft Election Safety Review (6 Properties + P0/P1 Fixes)")
class ElectionSafetyReviewTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Set<String> PEERS_OF_NODE1 = Set.of("node-2", "node-3", "node-4", "node-5");
    private static final Instant T0 = Instant.parse("2026-11-27T10:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(T0, ZoneOffset.UTC);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static void logPropertyPass(String propId, String propName, String detail) {
        System.out.println("[SAFETY-PROPERTY][" + propId + "][" + propName + "] PASS - " + detail);
    }

    private static RaftLogEntry entry(long index, long term, String req) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                "{\"requestId\":\"" + req + "\"}");
    }

    // ===========================================================================================
    // PROP-01: VOTE_ONCE_PER_TERM
    // ===========================================================================================

    @Test
    @DisplayName("PROP-01A [VOTE_ONCE_PER_TERM]: Tối đa 1 phiếu mỗi term, idempotent retry, chặn non-voter, auditTermLog phát hiện double-vote")
    void prop01_voteOncePerTerm_singleVoteIdempotentRetryAndAuditDetection() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(4L, null, List.of(entry(1, 3, "r1")), 1L);
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        RaftElectionService svc = new RaftElectionService("node-1", core, state, storage);
        svc.setClock(FIXED_CLOCK);

        // 1. Grant vote to node-2 in term 4
        RequestVoteResponse first = svc.handleRequestVote(new RequestVoteRequestDto(4L, "node-2", 1L, 3L));
        assertTrue(first.voteGranted());
        assertEquals(4L, first.term());
        assertEquals("node-2", storage.load().votedFor());

        // 2. Rival candidate node-3 in same term 4 (even with longer log) MUST be rejected
        RequestVoteResponse rival = svc.handleRequestVote(new RequestVoteRequestDto(4L, "node-3", 50L, 4L));
        assertFalse(rival.voteGranted());
        assertEquals(4L, rival.term());
        assertEquals("node-2", storage.load().votedFor());

        // 3. Idempotent retry by node-2 in term 4 MUST succeed without changing vote
        RequestVoteResponse retry = svc.handleRequestVote(new RequestVoteRequestDto(4L, "node-2", 1L, 3L));
        assertTrue(retry.voteGranted());
        assertEquals("node-2", storage.load().votedFor());

        // 4. Non-voter candidate MUST be rejected without mutating state
        assertThrows(IllegalArgumentException.class,
                () -> svc.handleRequestVote(new RequestVoteRequestDto(9L, "monitor-1", 10L, 9L)));
        assertEquals(4L, state.currentTerm());
        assertEquals("node-2", state.votedFor());

        // 5. Valid term log passes auditTermLog
        TraceAuditReport cleanAudit = RaftElectionService.auditTermLog(svc.termLog());
        assertTrue(cleanAudit.passed(), () -> "Expected clean audit, violations=" + cleanAudit.violations());

        // 6. Synthetic double-vote log in same term MUST fail closed in auditTermLog (P1 fix)
        List<TermLogEntry> doubleVoteTrace = List.of(
                new TermLogEntry(T0, "node-1", RaftElectionService.EVENT_VOTE_GRANTED,
                        4L, 4L, Role.FOLLOWER, Role.FOLLOWER, null, "node-2", 1L, 1L, "first"),
                new TermLogEntry(T0.plusMillis(5), "node-1", RaftElectionService.EVENT_VOTE_GRANTED,
                        4L, 4L, Role.FOLLOWER, Role.FOLLOWER, null, "node-3", 1L, 1L, "illegal second"));
        TraceAuditReport badAudit = RaftElectionService.auditTermLog(doubleVoteTrace);
        assertFalse(badAudit.passed());
        assertTrue(badAudit.violations().stream().anyMatch(v -> v.contains("Vote-once-per-term violated")));

        logPropertyPass("PROP-01A", "VOTE_ONCE_PER_TERM",
                "single vote enforced across retries/rivals/non-voters; auditTermLog rejects same-term double vote");
    }

    @Test
    @DisplayName("PROP-01B [VOTE_ONCE_PER_TERM]: Trùng phiếu, phiếu trễ từ term cũ, phiếu sau step-down hoặc sau partition không làm sai đếm phiếu (P0-2)")
    void prop01_voteOncePerTerm_duplicateDelayedPostStepDownAndPartitionedRepliesCannotPolluteGrants() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        Map<String, CandidateElection.VoteReply> term1Replies = new LinkedHashMap<>();
        Map<String, CandidateElection.VoteReply> term2Replies = new LinkedHashMap<>();

        RaftElectionService svc = new RaftElectionService("node-1", core, state, storage,
                (peer, req, reply) -> {
                    if (req.getTerm() == 1L) term1Replies.put(peer, reply);
                    else if (req.getTerm() == 2L) term2Replies.put(peer, reply);
                },
                null, () -> {}, () -> {}, null);

        // Start term 1 campaign
        assertEquals(1L, svc.startElection());
        assertEquals(1, svc.grantedVoteCount());

        // Duplicate grant from node-2 three times -> STILL only 2 votes (self + node-2)
        term1Replies.get("node-2").receive(new RequestVoteResponse(1L, true));
        term1Replies.get("node-2").receive(new RequestVoteResponse(1L, true));
        term1Replies.get("node-2").receive(new RequestVoteResponse(1L, true));
        assertEquals(2, svc.grantedVoteCount());
        assertFalse(svc.hasWonElection());
        assertEquals(Role.CANDIDATE, svc.role());

        // Retry into term 2
        assertEquals(2L, svc.retryElection());
        assertEquals(1, svc.grantedVoteCount());

        // Stale delayed reply from term-1 request carrying term=2 MUST NOT pollute term-2 grants (P0-2)
        term1Replies.get("node-3").receive(new RequestVoteResponse(2L, true));
        assertEquals(1, svc.grantedVoteCount(), "Delayed reply from old campaign must be ignored");
        assertFalse(svc.grantedVoters().contains("node-3"));

        // Isolate node-4 after request was sent -> late reply from isolated peer MUST be dropped (P0-2)
        svc.isolatePeer("node-4");
        term2Replies.get("node-4").receive(new RequestVoteResponse(2L, true));
        assertEquals(1, svc.grantedVoteCount(), "Reply from partition-isolated peer must be dropped");
        assertFalse(svc.hasWonElection());

        // Step down to FOLLOWER via same-term AppendEntries directly on state -> late reply MUST NOT pollute grants
        state.becomeFollower(2L, "node-5");
        term2Replies.get("node-2").receive(new RequestVoteResponse(2L, true));
        assertEquals(0, svc.grantedVoteCount(), "Stepped-down follower must report 0 active campaign grants");
        assertFalse(svc.hasWonElection());

        logPropertyPass("PROP-01B", "VOTE_ONCE_PER_TERM",
                "duplicate voter, delayed old-campaign reply, post-partition reply, and post-stepdown reply all blocked");
    }

    // ===========================================================================================
    // PROP-02: STALE_LOG_ELIGIBILITY
    // ===========================================================================================

    @Test
    @DisplayName("PROP-02 [STALE_LOG_ELIGIBILITY]: So sánh (lastLogTerm, lastLogIndex) kể cả uncommitted tail; candidate log cũ không thể đắc cử (TC-RAFT-02)")
    void prop02_staleLogEligibility_preventsStaleCandidateFromWinningAndLogsHigherTermStepDown() throws Exception {
        // Cluster of 5 nodes:
        // node-1, node-2, node-3 hold [ (idx=1,term=1), (idx=2,term=3), (idx=3,term=3) ] with lastApplied=1 (indices 2..3 uncommitted)
        // node-4, node-5 have stale logs: node-4 has [(1,1),(2,2),(3,2),(4,2)], node-5 has [(1,1),(2,3)]
        List<RaftLogEntry> freshLog = List.of(entry(1, 1, "r1"), entry(2, 3, "r2"), entry(3, 3, "r3"));
        List<RaftLogEntry> lowerTermLongerLog = List.of(
                entry(1, 1, "r1"), entry(2, 2, "s2"), entry(3, 2, "s3"), entry(4, 2, "s4"));
        List<RaftLogEntry> sameTermShorterLog = List.of(entry(1, 1, "r1"), entry(2, 3, "r2"));

        MiniCluster cluster = new MiniCluster();
        cluster.seedNode("node-1", 3L, null, freshLog, 1L);
        cluster.seedNode("node-2", 3L, null, freshLog, 1L);
        cluster.seedNode("node-3", 3L, null, freshLog, 1L);
        cluster.seedNode("node-4", 3L, null, lowerTermLongerLog, 1L);
        cluster.seedNode("node-5", 3L, null, sameTermShorterLog, 1L);

        // Make node-1 LEADER in term 4 first to verify higher-term stale-log RequestVote still steps it down
        cluster.promoteDirectLeader("node-1", 4L);
        AtomicInteger node1StepDowns = cluster.stepDownCounts.get("node-1");

        // Stale candidate node-4 (lastLogTerm=2, lastLogIndex=4) starts election for term 5
        cluster.state("node-4").becomeFollower(4L);
        cluster.storage("node-4").saveTermAndVote(4L, null);
        assertEquals(5L, cluster.service("node-4").startElection());
        cluster.deliverAll();

        // node-4 MUST NOT win: node-1,2,3 (lastLogTerm=3 > 2) and node-5 (lastLogTerm=3 > 2) ALL reject it!
        assertEquals(Role.CANDIDATE, cluster.service("node-4").role());
        assertEquals(1, cluster.service("node-4").grantedVoteCount(), "Stale-term candidate only has self-vote");
        assertFalse(cluster.service("node-4").hasWonElection());

        // Yet node-1 (former Leader at term 4) MUST have stepped down to FOLLOWER at term 5 with votedFor=null!
        assertEquals(Role.FOLLOWER, cluster.service("node-1").role());
        assertEquals(5L, cluster.state("node-1").currentTerm());
        assertNull(cluster.storage("node-1").load().votedFor());
        assertEquals(0, cluster.service("node-1").grantedVoteCount());
        assertFalse(cluster.service("node-1").hasWonElection());
        assertTrue(node1StepDowns.get() >= 1, "Leader must fire onStepDown on higher-term stale-log RequestVote");
        assertTrue(cluster.service("node-1").termLog().stream()
                .anyMatch(e -> RaftElectionService.EVENT_HIGHER_TERM_STEP_DOWN.equals(e.eventType())));

        // Stale shorter-log candidate node-5 (lastLogTerm=3, lastLogIndex=2) campaigns at term 6
        assertEquals(6L, cluster.service("node-5").startElection());
        cluster.deliverAll();

        // node-5 gets vote ONLY from node-4 (term 2 < 3) + self = 2/5, rejected by node-1,2,3 (index 3 > 2)!
        assertEquals(2, cluster.service("node-5").grantedVoteCount(), "Shorter-log candidate reaches at most 2/5");
        assertFalse(cluster.service("node-5").hasWonElection());
        assertEquals(Role.CANDIDATE, cluster.service("node-5").role());

        // Fresh candidate node-2 (lastLogTerm=3, lastLogIndex=3) campaigns at term 7 -> wins 5/5 (quorum >= 3)!
        assertEquals(7L, cluster.service("node-2").startElection());
        cluster.deliverAll();
        assertEquals(Role.LEADER, cluster.service("node-2").role());
        assertTrue(cluster.service("node-2").hasWonElection());
        assertEquals(1, cluster.leaderCount());

        logPropertyPass("PROP-02", "STALE_LOG_ELIGIBILITY",
                "lower-term longer log (1/5) and equal-term shorter log (2/5) denied; fresh log node-2 elected at term 7");
    }

    // ===========================================================================================
    // PROP-03: HIGHER_TERM_STEPDOWN
    // ===========================================================================================

    @Test
    @DisplayName("PROP-03A [HIGHER_TERM_STEPDOWN]: Step-down từ mọi nguồn RPC (RequestVote/AppendEntries/VoteReply/HeartbeatReply) + fail-closed khi storage lỗi")
    void prop03_higherTermStepDown_allRpcPathsAndStorageFailClosed() throws Exception {
        // Path 1 & 2: PersistentRaftNode + AppendEntriesHandler + HigherTermHandler + LeaderHeartbeatScheduler
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, List.of(), 0L);
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        VirtualScheduler clock = new VirtualScheduler();
        List<Exception> failures = new ArrayList<>();

        AtomicInteger heartbeatStops = new AtomicInteger();
        AtomicReference<ElectionTimer> timerRef = new AtomicReference<>();
        HigherTermHandler higherTermHandler = new HigherTermHandler(
                state,
                core::observeHigherTerm,
                heartbeatStops::incrementAndGet,
                () -> timerRef.get().start());

        AtomicReference<java.util.function.Consumer<AppendEntriesResponse>> hbReply = new AtomicReference<>();
        LeaderHeartbeatScheduler hbScheduler = new LeaderHeartbeatScheduler(
                "node-1", state, clock, ElectionConfig.defaults(),
                (peer, req, reply) -> hbReply.set(reply),
                higherTermHandler, failures::add);

        CandidateElection election = new CandidateElection("node-1", core, state,
                (peer, req, reply) -> {}, higherTermHandler, () -> {});
        ElectionTimer timer = new ElectionTimer(election, clock, () -> 500L, failures::add);
        timerRef.set(timer);
        core.bindLifecycle(timer, () -> {
            hbScheduler.stop();
            heartbeatStops.incrementAndGet();
        });

        // Promote node-1 to LEADER at term 3
        state.startElection("node-1");
        storage.saveTermAndVote(3L, "node-1");
        state.becomeLeader(PEERS_OF_NODE1);
        long term3Token = core.leadershipGuard().captureTerm();
        assertTrue(core.leadershipGuard().isCurrent(term3Token));
        hbScheduler.start();

        // Path 1: Delayed AppendEntriesResponse carrying term 7 stops heartbeats, persists (7, null), steps down & rearms timer
        hbReply.get().accept(new AppendEntriesResponse(7L, false, 0L));
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(7L, state.currentTerm());
        assertNull(state.votedFor());
        assertEquals(7L, storage.load().currentTerm());
        assertNull(storage.load().votedFor());
        assertFalse(core.leadershipGuard().isCurrent(term3Token));
        assertTrue(heartbeatStops.get() >= 1);
        assertEquals(1, clock.activeTaskCount(), "Election timer must be re-armed after Leader step-down");

        // Path 2: Higher-term AppendEntries (term 9) persists (9, null) and records leader hint
        AppendEntriesResponse aeResp = core.handleAppendEntries(
                new AppendEntriesRequest(9L, "node-3", 0L, 0L, List.of(), 0L));
        assertTrue(aeResp.success());
        assertEquals(9L, aeResp.term());
        assertEquals(9L, storage.load().currentTerm());
        assertNull(storage.load().votedFor());
        assertEquals("node-3", state.leaderId());

        // Path 3: Storage failure during RaftElectionService.observeHigherTerm MUST fail closed (P0-4)
        FakeRaftStorage failStorage = new FakeRaftStorage();
        failStorage.seedState(5L, "node-1", List.of(), 0L);
        RaftState failState = restoreFrom(failStorage);
        PersistentRaftNode failCore = new PersistentRaftNode("node-1", failState, failStorage);
        RaftElectionService failSvc = new RaftElectionService("node-1", failCore, failState, failStorage);
        failStorage.failNext(new RaftStorageException("disk io failure"));
        assertThrows(RaftStorageException.class, () -> failSvc.observeHigherTerm(10L));
        assertThrows(IllegalStateException.class, () -> failSvc.observeHigherTerm(11L),
                "RaftElectionService must fail closed after storage failure during step-down");
        assertThrows(IllegalStateException.class, failSvc::startElection);
        assertEquals(0, failSvc.grantedVoteCount());
        assertFalse(failSvc.hasWonElection());

        // Path 4: Storage failure during handleRequestVote MUST also fail closed (P0-4)
        FakeRaftStorage rvFailStorage = new FakeRaftStorage();
        RaftState rvFailState = restoreFrom(rvFailStorage);
        PersistentRaftNode rvFailCore = new PersistentRaftNode("node-1", rvFailState, rvFailStorage);
        RaftElectionService rvFailSvc = new RaftElectionService("node-1", rvFailCore, rvFailState, rvFailStorage);
        rvFailStorage.failNext(new RaftStorageException("vote persist failed"));
        assertThrows(RaftStorageException.class,
                () -> rvFailSvc.handleRequestVote(new RequestVoteRequestDto(2L, "node-2", 0L, 0L)));
        assertThrows(IllegalStateException.class, rvFailSvc::startElection,
                "RaftElectionService must fail closed after RequestVote storage failure");

        timer.close();
        hbScheduler.close();
        assertTrue(failures.isEmpty());

        logPropertyPass("PROP-03A", "HIGHER_TERM_STEPDOWN",
                "AppendEntries/HeartbeatReply/observeHigherTerm/handleRequestVote step down durably and fail closed on storage error");
    }

    @Test
    @DisplayName("PROP-03B [HIGHER_TERM_STEPDOWN]: Phản hồi đồng bộ (higher term hoặc thắng cử ngay) giữa vòng lặp startElection không gây crash/failClosed (P0-3)")
    void prop03_higherTermStepDown_synchronousReplyMidStartElectionLoopIsSafe() throws Exception {
        // Case 1: First peer synchronously replies with higher term 8 during startElection() loop
        FakeRaftStorage s1 = new FakeRaftStorage();
        RaftState st1 = restoreFrom(s1);
        PersistentRaftNode c1 = new PersistentRaftNode("node-1", st1, s1);
        List<String> contacted1 = new ArrayList<>();
        RaftElectionService svc1 = new RaftElectionService("node-1", c1, st1, s1,
                (peer, req, reply) -> {
                    contacted1.add(peer);
                    try {
                        reply.receive(new RequestVoteResponse(8L, false));
                    } catch (RaftStorageException e) {
                        throw new RuntimeException(e);
                    }
                },
                null, () -> {}, () -> {}, null);

        assertEquals(1L, svc1.startElection());
        assertEquals(Role.FOLLOWER, svc1.role(), "Node must cleanly step down to FOLLOWER on sync higher-term reply");
        assertEquals(8L, st1.currentTerm());
        assertNull(st1.votedFor());
        assertEquals(1, contacted1.size(), "Must stop sending stale RequestVotes after stepping down mid-loop");
        // Crucially: node MUST NOT be failed closed — it can still handle subsequent valid RPCs!
        assertTrue(svc1.handleRequestVote(new RequestVoteRequestDto(8L, "node-2", 0L, 0L)).voteGranted());

        // Case 2: First two peers synchronously grant votes during startElection() loop -> immediate Leader promotion
        FakeRaftStorage s2 = new FakeRaftStorage();
        RaftState st2 = restoreFrom(s2);
        PersistentRaftNode c2 = new PersistentRaftNode("node-1", st2, s2);
        List<String> contacted2 = new ArrayList<>();
        RaftElectionService svc2 = new RaftElectionService("node-1", c2, st2, s2,
                (peer, req, reply) -> {
                    contacted2.add(peer);
                    try {
                        reply.receive(new RequestVoteResponse(req.getTerm(), true));
                    } catch (RaftStorageException e) {
                        throw new RuntimeException(e);
                    }
                },
                null, () -> {}, () -> {}, null);

        assertEquals(1L, svc2.startElection());
        assertEquals(Role.LEADER, svc2.role(), "Sync quorum grants must promote cleanly without crashing");
        assertEquals(3, svc2.grantedVoteCount());
        assertTrue(svc2.hasWonElection());
        assertEquals(2, contacted2.size(), "Must stop sending unnecessary RequestVotes once promoted");
        // Verify log ordering: ELECTION_STARTED precedes LEADER_ELECTED
        List<String> eventTypes = svc2.termLog().stream().map(TermLogEntry::eventType).toList();
        assertTrue(eventTypes.indexOf(RaftElectionService.EVENT_ELECTION_STARTED)
                < eventTypes.indexOf(RaftElectionService.EVENT_LEADER_ELECTED),
                "ELECTION_STARTED must precede LEADER_ELECTED in termLog: " + eventTypes);
        assertTrue(RaftElectionService.auditTermLog(svc2.termLog()).passed());

        logPropertyPass("PROP-03B", "HIGHER_TERM_STEPDOWN",
                "synchronous higher-term step-down and immediate sync quorum win mid-startElection loop stay healthy");
    }

    // ===========================================================================================
    // PROP-04: TIMER_RACES
    // ===========================================================================================

    @Test
    @DisplayName("PROP-04A [TIMER_RACES]: Token chống stale timer callback, không double-arm khi sync reset, stale/denied không đẩy lùi deadline")
    void prop04_timerRaces_staleCallbacksResetRulesDoubleArmGuardAndHeartbeatGeneration() throws Exception {
        VirtualScheduler clock = new VirtualScheduler();
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        List<Exception> failures = new ArrayList<>();
        AtomicReference<ElectionTimer> timerRef = new AtomicReference<>();
        AtomicBoolean injectSyncAppendDuringElection = new AtomicBoolean(false);

        CandidateElection election = new CandidateElection("node-1", core, state,
                (peer, req, reply) -> {
                    if (injectSyncAppendDuringElection.compareAndSet(true, false)) {
                        // Simulate same-term valid AppendEntries arriving inline during startElection
                        state.becomeFollower(req.getTerm(), "node-2");
                        timerRef.get().onValidAppendEntries(req.getTerm());
                    }
                },
                term -> {
                    core.observeHigherTerm(term);
                    timerRef.get().start();
                },
                () -> timerRef.get().stop());

        ElectionTimer timer = new ElectionTimer(election, clock, () -> 500L, failures::add);
        timerRef.set(timer);
        core.bindLifecycle(timer, () -> {});

        // 1. Start arms 1 deadline; repeated start does not postpone
        timer.start();
        ScheduledVirtualTask initialTask = clock.peekOldest();
        clock.advance(300);
        timer.start();
        assertEquals(1, clock.scheduledTotal);

        // 2. Stale AppendEntries and denied vote do NOT reset timer
        timer.onValidAppendEntries(-1);
        timer.onVoteDecision(0, false);
        assertEquals(1, clock.scheduledTotal);

        // 3. Valid AppendEntries (even log mismatch) resets timer & invalidates old callback
        AppendEntriesResponse mismatch = core.handleAppendEntries(
                new AppendEntriesRequest(1L, "node-2", 5L, 1L, List.of(), 0L));
        assertFalse(mismatch.success());
        assertTrue(initialTask.isCancelled());
        // Simulate race: already-dequeued stale callback runs after reset -> MUST be ignored!
        initialTask.command.run();
        assertEquals(1L, state.currentTerm(), "Stale timer callback must NOT increment term");
        assertEquals(Role.FOLLOWER, state.role());

        // 4. Sync valid AppendEntries inside startElection() arms fresh deadline; expire() MUST NOT double-arm (P1 fix)
        int scheduledBeforeSyncRace = clock.scheduledTotal;
        injectSyncAppendDuringElection.set(true);
        clock.advance(500);
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(2L, state.currentTerm());
        assertEquals(scheduledBeforeSyncRace + 1, clock.scheduledTotal,
                "expire() must NOT double-arm when synchronous onValidAppendEntries already armed a new generation");

        // 5. Heartbeat scheduler stale tick from term 3 cannot fire after stepping down and re-electing in term 4
        List<Long> heartbeatTermsSent = new ArrayList<>();
        LeaderHeartbeatScheduler hb = new LeaderHeartbeatScheduler("node-1", state, clock,
                new ElectionConfig(100, 500, 1000),
                (peer, req, reply) -> heartbeatTermsSent.add(req.getTerm()),
                core::observeHigherTerm, failures::add);

        state.startElection("node-1"); // term 3
        state.becomeLeader(PEERS_OF_NODE1);
        hb.start();
        ScheduledVirtualTask term3HbTick = clock.lastScheduled();
        assertEquals(4, heartbeatTermsSent.size());
        assertTrue(heartbeatTermsSent.stream().allMatch(t -> t == 3L));

        // Step down and later become leader in term 4
        hb.stop();
        state.becomeFollower(3L);
        state.startElection("node-1"); // term 4
        state.becomeLeader(PEERS_OF_NODE1);
        hb.start();
        int countAfterTerm4Start = heartbeatTermsSent.size();

        // Fire stale term-3 heartbeat tick -> MUST be ignored by generation + leaderTerm guard!
        term3HbTick.command.run();
        assertEquals(countAfterTerm4Start, heartbeatTermsSent.size(),
                "Stale term-3 heartbeat tick must not send heartbeats in term 4");

        timer.close();
        hb.close();
        assertTrue(failures.isEmpty());

        logPropertyPass("PROP-04A", "TIMER_RACES",
                "stale election/heartbeat callbacks blocked by generation tokens; sync double-arm race fixed; reset matrix verified");
    }

    @Test
    @DisplayName("PROP-04B [TIMER_RACES]: Shutdown RaftElectionComponent đồng thời với late vote reply + VoteRpcAdapter timeout vs higher-term race")
    void prop04_timerRaces_componentShutdownAndVoteRpcAdapterTimeoutVsHigherTerm() throws Exception {
        // Part 1: RaftElectionComponent close() concurrent with late vote reply MUST NOT report failure (P1 fix)
        FakeRaftStorage storage = new FakeRaftStorage();
        List<Exception> failures = new CopyOnWriteArrayList<>();
        CountDownLatch sentLatch = new CountDownLatch(1);
        AtomicReference<CandidateElection.VoteReply> capturedReply = new AtomicReference<>();

        RaftElectionComponent component = new RaftElectionComponent(
                "node-1",
                storage,
                (restoredState, durable) -> new PersistentRaftNode("node-1", restoredState, durable),
                (peer, req, reply) -> {
                    capturedReply.compareAndSet(null, reply);
                    sentLatch.countDown();
                },
                new ElectionConfig(10, 30, 60),
                failures::add,
                () -> {});
        component.start();
        assertTrue(sentLatch.await(2, TimeUnit.SECONDS), "Election timer must fire and send RequestVote");
        component.close();

        // Deliver late vote reply AFTER component closed -> must be ignored without RejectedExecutionException!
        assertDoesNotThrow(() -> capturedReply.get().receive(new RequestVoteResponse(5L, true)));
        assertTrue(failures.isEmpty(), () -> "Late reply after close must not invoke onFailure: " + failures);

        // Part 2: VoteRpcAdapter timeout drops late same-term grant, but STILL delivers late higher-term response!
        List<RequestVoteResponse> delivered = new CopyOnWriteArrayList<>();
        List<Exception> adapterFailures = new CopyOnWriteArrayList<>();
        CompletableFuture<MessageEnvelope<JsonNode>> sameTermLate = new CompletableFuture<>();
        CompletableFuture<MessageEnvelope<JsonNode>> higherTermLate = new CompletableFuture<>();
        AtomicReference<String> msgIdSame = new AtomicReference<>();
        AtomicReference<String> msgIdHigher = new AtomicReference<>();

        VoteRpcAdapter adapter = new VoteRpcAdapter(
                "node-1",
                (peer, env) -> {
                    if ("node-2".equals(peer)) {
                        msgIdSame.set(env.getMessageId());
                        return sameTermLate;
                    }
                    msgIdHigher.set(env.getMessageId());
                    return higherTermLate;
                },
                Duration.ofMillis(30),
                Runnable::run,
                adapterFailures::add);

        adapter.requestVote("node-2", new RequestVoteRequestDto(2L, "node-1", 0L, 0L), delivered::add);
        adapter.requestVote("node-3", new RequestVoteRequestDto(2L, "node-1", 0L, 0L), delivered::add);

        Thread.sleep(80); // Both RPCs have now timed out

        // Complete late same-term grant -> MUST be dropped!
        sameTermLate.complete(voteResponseEnvelope(msgIdSame.get(), "node-2", 2L, true));
        assertTrue(delivered.isEmpty(), "Timed-out same-term grant must be dropped");

        // Complete late higher-term denial -> MUST STILL be delivered to trigger step-down!
        higherTermLate.complete(voteResponseEnvelope(msgIdHigher.get(), "node-3", 9L, false));
        assertEquals(1, delivered.size());
        assertEquals(new RequestVoteResponse(9L, false), delivered.getFirst());
        assertTrue(adapterFailures.isEmpty());

        logPropertyPass("PROP-04B", "TIMER_RACES",
                "component shutdown race cleanly ignores late vote reply; VoteRpcAdapter drops timed-out grant but delivers late higher term");
    }

    // ===========================================================================================
    // PROP-05: QUORUM_SAFETY
    // ===========================================================================================

    @Test
    @DisplayName("PROP-05 [QUORUM_SAFETY]: Strict majority 3/5, single ping != quorum, stale-term ACK và peer bị partition không cấp quorum giả (P0-5)")
    void prop05_quorumSafety_strictMajorityNoStaleTermAcksAndPartitionPropagation() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        RaftState state = restoreFrom(storage);
        PersistentRaftNode core = new PersistentRaftNode("node-1", state, storage);
        RaftElectionService svc = new RaftElectionService(
                "node-1", core, state, storage, (p, req, rep) -> {}, null, () -> {}, () -> {}, null);

        // 1. Fixed 5 voters, majority = 3, non-voter rejected
        assertEquals(5, svc.clusterSize());
        assertEquals(3, svc.quorumSize());
        assertThrows(IllegalArgumentException.class, () -> svc.recordPeerAck("monitor-1", 0L, T0));

        // 2. Follower cannot accumulate leader ACKs
        svc.recordPeerAck("node-2", 0L, T0);
        svc.recordPeerAck("node-3", 0L, T0);
        assertEquals(AvailabilityStatus.NOT_LEADER, svc.checkWriteAvailability(T0).status());

        // 3. Elect node-1 as Leader in term 1 -> must NOT reuse follower-era ACKs!
        assertEquals(1L, svc.startElection());
        assertInstanceOf(ElectionService.VoteProgress.class, svc.recordVoteResponse("node-2", 1L, true));
        assertInstanceOf(ElectionService.Won.class, svc.recordVoteResponse("node-3", 1L, true));
        svc.promoteToLeader(PEERS_OF_NODE1);
        assertEquals(1, svc.activeVoterCount(T0), "Newly elected Leader starts with only itself until term-1 ACKs arrive");
        assertEquals(AvailabilityStatus.NO_QUORUM, svc.checkWriteAvailability(T0).status());

        // Attach WriteAvailabilityPolicy backed by ReplicationTracker + CommitAdvancer now that node-1 is LEADER
        ReplicationTracker tracker = new ReplicationTracker(state, PEERS_OF_NODE1);
        CommitAdvancer advancer = new CommitAdvancer(state, tracker);
        WriteAvailabilityPolicy policy = new WriteAvailabilityPolicy("node-1", state, PEERS_OF_NODE1, tracker, advancer);
        svc.attachWriteAvailabilityPolicy(policy);

        // 4. Single ping does NOT grant quorum
        svc.recordPeerPing("node-2", T0);
        assertEquals(1, svc.activeVoterCount(T0));
        assertFalse(svc.canCommitWrites(T0));

        // 5. Verified ACKs from 2 peers in term 1 -> 3/5 AVAILABLE
        svc.recordPeerAck("node-2", 0L, T0);
        svc.recordPeerAck("node-3", 0L, T0);
        assertEquals(3, svc.activeVoterCount(T0));
        assertTrue(svc.canCommitWrites(T0));
        assertEquals(AvailabilityStatus.AVAILABLE, svc.checkWriteAvailability(T0).status());

        // 6. Partition isolating {node-3, node-4, node-5} MUST propagate to WriteAvailabilityPolicy immediately (P0-5)
        svc.partition(Set.of("node-3", "node-4", "node-5"));
        assertEquals(2, svc.activeVoterCount(T0), "Isolated node-3 must no longer count toward active quorum");
        assertFalse(svc.canCommitWrites(T0), "2/5 minority partition must NOT commit writes");
        assertEquals(AvailabilityStatus.NO_QUORUM, svc.checkWriteAvailability(T0).status());
        // ACK from isolated peer MUST be ignored
        svc.recordPeerAck("node-3", 0L, T0.plusMillis(10));
        assertEquals(2, svc.activeVoterCount(T0.plusMillis(10)));

        // Uncommitted entry retained on NO_QUORUM timeout
        state.append(entry(1, 1, "uncommitted-req"));
        var timeoutRes = svc.resolveWriteTimeout(1L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, timeoutRes.outcome());
        assertEquals("NO_QUORUM", timeoutRes.errorCode());
        assertTrue(timeoutRes.uncommittedEntryRetained());
        svc.assertUncommittedEntryNotRolledBack(1L);

        // 7. Heal, step down to term 2, re-elect in term 3 within freshness window -> term-1 ACKs MUST NOT carry over (P0-5)
        svc.healAll();
        svc.recordPeerAck("node-2", 1L, T0.plusMillis(100));
        svc.recordPeerAck("node-3", 1L, T0.plusMillis(100));
        assertEquals(3, svc.activeVoterCount(T0.plusMillis(100)));

        // Even if someone directly mutates RaftState to term 3 without calling observeHigherTerm, WriteAvailabilityPolicy
        // itself scopes ACKs by term!
        state.becomeFollower(2L);
        state.startElection("node-1"); // term 3
        state.becomeLeader(PEERS_OF_NODE1);
        assertEquals(1, policy.activeVoterCount(T0.plusMillis(200)),
                "WriteAvailabilityPolicy must ignore stale term-1 ACKs in term 3");
        assertEquals(AvailabilityStatus.NO_QUORUM, policy.checkWriteAvailability(T0.plusMillis(200)).status());

        // 8. Future-timestamp ACK cannot inflate fallback quorum count
        RaftElectionService fallbackSvc = new RaftElectionService("node-1", core, state, storage);
        fallbackSvc.recordPeerAck("node-2", 1L, T0.plusSeconds(60));
        fallbackSvc.recordPeerAck("node-3", 1L, T0.plusSeconds(60));
        assertEquals(1, fallbackSvc.activeVoterCount(T0), "Future-dated ACKs must not count as active at T0");

        // 9. Trace audit rejects minority-elected leader
        Map<String, NodeStatusDto> illegalMinorityStep = Map.of(
                "node-1", statusDto("node-1", "LEADER", 4L, "node-1", 1L, 1L, 1L),
                "node-2", statusDto("node-2", "FOLLOWER", 4L, "node-1", 1L, 1L, 1L));
        TraceAuditReport minorityAudit = RaftElectionService.auditRecordedStatusTrace(List.of(illegalMinorityStep));
        assertFalse(minorityAudit.passed());
        assertTrue(minorityAudit.violations().stream().anyMatch(v -> v.contains("Minority step")));

        logPropertyPass("PROP-05", "QUORUM_SAFETY",
                "3/5 strict majority enforced; single ping rejected; partition & term-scoped ACK isolation verified; minority leader audit failed closed");
    }

    // ===========================================================================================
    // PROP-06: RESTART_RECOVERY
    // ===========================================================================================

    @Test
    @DisplayName("PROP-06A [RESTART_RECOVERY]: simulateRestartAfterStop bảo toàn votedFor bền vững (không xóa về null) và fail-closed khi storage hỏng (P0-1)")
    void prop06_restartRecovery_preservesPersistedVoteAndFailsClosedOnCorruptStorage() throws Exception {
        // Case 1: Leader stops at term 3 (votedFor="node-1"), restarts -> MUST preserve votedFor="node-1"
        FakeRaftStorage storage1 = new FakeRaftStorage();
        storage1.seedState(2L, null, List.of(entry(1, 1, "r1"), entry(2, 2, "r2")), 1L);
        RaftState state1 = restoreFrom(storage1);
        PersistentRaftNode core1 = new PersistentRaftNode("node-1", state1, storage1);
        RaftElectionService svc1 = new RaftElectionService("node-1", core1, state1, storage1);

        assertEquals(3L, svc1.startElection());
        svc1.recordVoteResponse("node-2", 3L, true);
        svc1.recordVoteResponse("node-3", 3L, true);
        svc1.promoteToLeader(PEERS_OF_NODE1);
        assertEquals(Role.LEADER, svc1.role());

        // Attempting startElection while LEADER throws IllegalStateException WITHOUT corrupting currentGrants!
        assertThrows(IllegalStateException.class, svc1::startElection);
        assertEquals(3, svc1.grantedVoteCount());
        assertTrue(svc1.hasWonElection());

        // Stop leader and restart -> storage still holds (term=3, votedFor="node-1")
        svc1.simulateStopLeader("127.0.0.1:9101", "D27 restart safety test");
        assertTrue(svc1.isStopped());
        assertEquals(0, svc1.grantedVoteCount());
        assertFalse(svc1.hasWonElection());

        PersistentState loaded1 = svc1.simulateRestartAfterStop();
        assertEquals(3L, loaded1.currentTerm());
        assertEquals("node-1", loaded1.votedFor());
        assertEquals(Role.FOLLOWER, svc1.role());
        assertEquals(3L, state1.currentTerm());
        assertEquals("node-1", state1.votedFor(), "Restart must preserve persisted self-vote");
        // Rival candidate in term 3 MUST be denied (prevents double vote after restart!)
        assertFalse(svc1.handleRequestVote(new RequestVoteRequestDto(3L, "node-4", 2L, 2L)).voteGranted());

        // Case 2: Storage advanced ahead of memory with a persisted votedFor (term=5, votedFor="node-3") ->
        // simulateRestartAfterStop MUST restore BOTH term=5 AND votedFor="node-3", NOT wipe votedFor to null! (P0-1)
        FakeRaftStorage storage2 = new FakeRaftStorage();
        storage2.seedState(3L, "node-1", List.of(entry(1, 1, "r1")), 1L);
        RaftState state2 = restoreFrom(storage2);
        PersistentRaftNode core2 = new PersistentRaftNode("node-1", state2, storage2);
        RaftElectionService svc2 = new RaftElectionService("node-1", core2, state2, storage2);

        // Promote to leader at term 4, then stop
        assertEquals(4L, svc2.startElection());
        svc2.recordVoteResponse("node-2", 4L, true);
        svc2.recordVoteResponse("node-3", 4L, true);
        svc2.promoteToLeader(PEERS_OF_NODE1);
        svc2.simulateStopLeader("127.0.0.1:9101", "D27 vote preservation across higher stored term");

        // Simulate durable storage already holding (term=5, votedFor="node-3")
        storage2.saveTermAndVote(5L, "node-3");
        svc2.simulateRestartAfterStop();
        assertEquals(Role.FOLLOWER, svc2.role());
        assertEquals(5L, state2.currentTerm());
        assertEquals("node-3", state2.votedFor(),
                "P0-1: simulateRestartAfterStop MUST NOT wipe persisted votedFor when storage term > previous memory term");
        // Rival node-2 in term 5 MUST be rejected; original node-3 retry MUST be accepted!
        assertFalse(svc2.handleRequestVote(new RequestVoteRequestDto(5L, "node-2", 1L, 1L)).voteGranted());
        assertTrue(svc2.handleRequestVote(new RequestVoteRequestDto(5L, "node-3", 1L, 1L)).voteGranted());

        // Case 3: Corrupt durable storage (lastApplied > lastLogIndex, vote in term 0, non-voter vote, log gap)
        // MUST fail closed in recoverFromStorage() (P0-1)
        assertCorruptRecoveryFailsClosed(new PersistentState(0L, "node-2", List.of(), 0L), "term 0 vote");
        assertCorruptRecoveryFailsClosed(new PersistentState(2L, "monitor-1", List.of(), 0L), "non-voter vote");
        assertCorruptRecoveryFailsClosed(new PersistentState(2L, null, List.of(entry(1, 1, "r1")), 2L),
                "lastApplied > lastLogIndex");
        assertCorruptRecoveryFailsClosed(
                new PersistentState(3L, null, List.of(entry(1, 2, "r1"), entry(2, 1, "r2")), 0L),
                "decreasing log term");
        assertCorruptRecoveryFailsClosed(
                new PersistentState(2L, null, List.of(entry(1, 3, "r1")), 0L),
                "entry term > currentTerm");

        logPropertyPass("PROP-06A", "RESTART_RECOVERY",
                "simulateRestartAfterStop preserves votedFor across same/higher stored term; corrupt storage fails closed");
    }

    @Test
    @DisplayName("PROP-06B [RESTART_RECOVERY]: Restart thực qua RaftElectionComponent khởi tạo commitIndex=lastApplied, giữ uncommitted tail, chặn double-vote")
    void prop06_restartRecovery_fullComponentRestartPreservesLogAndCheckpoints() throws Exception {
        List<RaftLogEntry> logWithUncommittedTail = List.of(
                entry(1, 1, "committed-1"),
                entry(2, 2, "committed-2"),
                entry(3, 4, "uncommitted-3"));
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(4L, "node-2", logWithUncommittedTail, 2L);

        List<Exception> failures = new CopyOnWriteArrayList<>();
        try (RaftElectionComponent reopened = new RaftElectionComponent(
                "node-1",
                storage,
                (restoredState, durable) -> new PersistentRaftNode("node-1", restoredState, durable),
                (peer, req, reply) -> {},
                new ElectionConfig(50, 500, 1000),
                failures::add,
                () -> {})) {
            RaftElectionComponent.DetailedStatus status = reopened.detailedStatus();
            assertEquals("FOLLOWER", status.role());
            assertEquals(4L, status.currentTerm());
            assertNull(status.leaderId());
            assertEquals(3L, status.lastLogIndex(), "Uncommitted tail must be preserved on restart");
            assertEquals(2L, status.commitIndex(), "commitIndex must initialize to lastApplied, NOT lastLogIndex");
            assertEquals(2L, status.lastApplied());
            assertTrue(status.storageHealthy());
            assertTrue(failures.isEmpty());
        }

        // Fresh PersistentRaftNode restored from same storage rejects rival in term 4 & stale log in term 5
        RaftState restoredState = restoreFrom(storage);
        PersistentRaftNode restoredCore = new PersistentRaftNode("node-1", restoredState, storage);
        assertFalse(restoredCore.handleRequestVote(new RequestVoteRequest(4L, "node-3", 3L, 4L)).voteGranted());
        assertFalse(restoredCore.handleRequestVote(new RequestVoteRequest(5L, "node-3", 2L, 4L)).voteGranted());
        assertTrue(restoredCore.handleRequestVote(new RequestVoteRequest(5L, "node-4", 3L, 4L)).voteGranted());
        assertEquals(5L, storage.load().currentTerm());
        assertEquals("node-4", storage.load().votedFor());

        logPropertyPass("PROP-06B", "RESTART_RECOVERY",
                "RaftElectionComponent restart restores FOLLOWER, commitIndex=lastApplied<lastLogIndex, single-vote & log freshness intact");
    }

    // ===========================================================================================
    // Helpers
    // ===========================================================================================

    private static void assertCorruptRecoveryFailsClosed(PersistentState corruptState, String label) throws Exception {
        RaftStorage corruptStorage = new RaftStorage() {
            @Override public PersistentState load() { return corruptState; }
            @Override public void saveTermAndVote(long term, String votedFor) { }
            @Override public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) { }
            @Override public long loadLastApplied() { return corruptState.lastApplied(); }
        };
        RaftState fresh = new RaftState();
        FakeRaftStorage backing = new FakeRaftStorage();
        PersistentRaftNode core = new PersistentRaftNode("node-1", fresh, backing);
        RaftElectionService svc = new RaftElectionService("node-1", core, fresh, corruptStorage);
        assertThrows(RaftStorageException.class, svc::recoverFromStorage,
                "Expected fail-closed RaftStorageException for corrupt storage: " + label);
        assertThrows(IllegalStateException.class, svc::startElection,
                "Service must remain failed closed after corrupt recovery: " + label);
    }

    private static RaftState restoreFrom(RaftStorage storage) throws RaftStorageException {
        PersistentState p = storage.load();
        return RaftState.restore(p.currentTerm(), p.votedFor(), p.log(), p.lastApplied());
    }

    private static NodeStatusDto statusDto(
            String nodeId, String role, long term, String leaderId,
            long lastLogIndex, long commitIndex, long lastApplied) {
        return new NodeStatusDto(
                nodeId, role, term, leaderId, lastLogIndex, commitIndex, lastApplied,
                true, Map.of(), T0.toString());
    }

    private static MessageEnvelope<JsonNode> voteResponseEnvelope(
            String correlationId, String senderId, long term, boolean voteGranted) {
        JsonNode payload = MAPPER.createObjectNode()
                .put("term", term)
                .put("voteGranted", voteGranted);
        return MessageEnvelope.createResponse(correlationId, MessageType.REQUEST_VOTE_RESPONSE, senderId, payload);
    }

    private static final class MiniCluster {
        record PendingRpc(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) { }

        final Map<String, FakeRaftStorage> storages = new LinkedHashMap<>();
        final Map<String, RaftState> states = new LinkedHashMap<>();
        final Map<String, PersistentRaftNode> cores = new LinkedHashMap<>();
        final Map<String, RaftElectionService> services = new LinkedHashMap<>();
        final Map<String, AtomicInteger> stepDownCounts = new LinkedHashMap<>();
        final Deque<PendingRpc> queue = new ArrayDeque<>();

        void seedNode(String id, long term, String votedFor, List<RaftLogEntry> log, long lastApplied) throws Exception {
            FakeRaftStorage storage = new FakeRaftStorage();
            storage.seedState(term, votedFor, log, lastApplied);
            RaftState state = restoreFrom(storage);
            PersistentRaftNode core = new PersistentRaftNode(id, state, storage);
            AtomicInteger stepDowns = new AtomicInteger();
            RaftElectionService svc = new RaftElectionService(
                    id, core, state, storage,
                    (peer, req, reply) -> queue.add(new PendingRpc(id, peer, req, reply)),
                    null, () -> {}, stepDowns::incrementAndGet, null);
            svc.setClock(FIXED_CLOCK);
            storages.put(id, storage);
            states.put(id, state);
            cores.put(id, core);
            services.put(id, svc);
            stepDownCounts.put(id, stepDowns);
        }

        void promoteDirectLeader(String id, long term) throws Exception {
            RaftState st = states.get(id);
            if (st.currentTerm() < term - 1) {
                st.becomeFollower(term - 1);
            }
            if (st.currentTerm() == term) {
                // Start from term-1 so startElection lands on term
                FakeRaftStorage s = storages.get(id);
                List<RaftLogEntry> existing = st.entries();
                long applied = st.lastApplied();
                seedNode(id, term - 1, null, existing, applied);
                st = states.get(id);
            }
            st.startElection(id);
            storages.get(id).saveTermAndVote(term, id);
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(id);
            st.becomeLeader(peers);
        }

        void deliverAll() throws Exception {
            while (!queue.isEmpty()) {
                PendingRpc rpc = queue.removeFirst();
                RequestVoteResponse resp = services.get(rpc.to()).handleRequestVote(rpc.req());
                rpc.reply().receive(resp);
            }
        }

        RaftElectionService service(String id) { return services.get(id); }
        RaftState state(String id) { return states.get(id); }
        FakeRaftStorage storage(String id) { return storages.get(id); }
        long leaderCount() { return services.values().stream().filter(RaftElectionService::isLeader).count(); }
    }

    private static final class VirtualScheduler extends AbstractExecutorService implements ScheduledExecutorService {
        private final PriorityQueue<ScheduledVirtualTask> tasks = new PriorityQueue<>();
        private long now;
        private boolean shutdown;
        int scheduledTotal;
        private ScheduledVirtualTask last;

        void advance(long millis) {
            long target = now + millis;
            while (!tasks.isEmpty() && tasks.peek().deadline <= target) {
                ScheduledVirtualTask next = tasks.remove();
                now = next.deadline;
                next.run();
                if (!next.isCancelled()) {
                    assertDoesNotThrow(() -> next.get());
                }
            }
            now = target;
        }

        ScheduledVirtualTask peekOldest() { return tasks.peek(); }
        ScheduledVirtualTask lastScheduled() { return last; }
        long activeTaskCount() { return tasks.stream().filter(t -> !t.isCancelled()).count(); }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (shutdown) throw new RejectedExecutionException("virtual scheduler stopped");
            scheduledTotal++;
            ScheduledVirtualTask task = new ScheduledVirtualTask(command, now + unit.toMillis(delay), this);
            last = task;
            tasks.add(task);
            return task;
        }

        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && tasks.isEmpty(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return isTerminated(); }
        @Override public void execute(Runnable command) { command.run(); }
        @Override public <V> ScheduledFuture<V> schedule(Callable<V> task, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
        @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initial, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
        @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long initial, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ScheduledVirtualTask extends FutureTask<Void> implements ScheduledFuture<Void> {
        final Runnable command;
        final long deadline;
        final VirtualScheduler clock;

        ScheduledVirtualTask(Runnable command, long deadline, VirtualScheduler clock) {
            super(command, null);
            this.command = command;
            this.deadline = deadline;
            this.clock = clock;
        }

        @Override public long getDelay(TimeUnit unit) { return unit.convert(deadline - clock.now, TimeUnit.MILLISECONDS); }
        @Override public int compareTo(Delayed other) {
            return Long.compare(deadline, ((ScheduledVirtualTask) other).deadline);
        }
    }
}
