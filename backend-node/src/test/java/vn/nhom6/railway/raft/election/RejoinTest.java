package vn.nhom6.railway.raft.election;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRequestHandler;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.networking.BookingService;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.networking.TestNetworkFilter;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.SeatDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.AppendEntriesHandler;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.LogConflictResolver;
import vn.nhom6.railway.raft.replication.PersistentLogAppender;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D23: Old Leader Rejoin, Step-Down &amp; Uncommitted Suffix Repair Test Suite.
 *
 * <p><strong>Scope &amp; Integration (TV5-D23, SPEC-04, SPEC-07):</strong>
 * <ul>
 *   <li><strong>Heal partition &amp; Old Leader Rejoin:</strong> Sau khi phân vùng 3–2 được hàn gắn qua
 *       {@link TestNetworkFilter}, Leader cũ ở term thấp nhận heartbeat hoặc {@code APPEND_ENTRIES}
 *       từ Leader mới ở term cao hơn (hoặc nhận phản hồi term cao hơn) phải lập tức giáng chức.</li>
 *   <li><strong>Step-Down (TV5-D14):</strong> Tích hợp trực tiếp {@link HigherTermHandler},
 *       {@link PersistentElectionState}, {@link LeadershipGuard}, {@link PersistentRaftNode} và
 *       {@link RaftElectionComponent} — đảm bảo dừng heartbeat cũ, persist {@code (higherTerm, votedFor=null)}
 *       trước khi chạm log, re-arm election timer, vô hiệu hóa callback/write cũ và fail-closed khi lỗi storage.</li>
 *   <li><strong>Phối hợp TV4 sửa suffix chưa commit:</strong> Mỗi node trong FakeNetwork sử dụng trực tiếp
 *       {@link PersistentRaftNode} + {@link AppendEntriesHandler} + {@link LogConflictResolver} +
 *       {@link PersistentLogAppender} + {@link ReplicationTracker} + {@link CommitAdvancer} đã có của TV4
 *       (không chờ TV4-D23) để kiểm chứng backtrack {@code nextIndex}, cắt bỏ bền vững suffix chưa commit,
 *       bảo vệ tiền tố đã commit {@code [1, commitIndex]} và hội tụ 5/5 node.</li>
 *   <li><strong>Ranh giới kiểm thử:</strong> Suite này chứng minh rejoin/step-down/log-repair trên
 *       {@code FakeNetwork} + {@link TestNetworkFilter} (PASS); kiểm thử rejoin &amp; log repair trên cụm
 *       5 Node TCP + PostgreSQL thực tế thực hiện tại <strong>Gate G6</strong>.</li>
 * </ul>
 */
@DisplayName("TV5-D23: Old Leader Rejoin, Step-Down (TV5-D14) & Uncommitted Suffix Repair (TV4) on FakeNetwork")
class RejoinTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();

    private static String reqId(int num) {
        return String.format("%08d-0000-0000-0000-%012d", num, num);
    }

    private TestNetworkFilter filter;
    private FakeNetwork network;
    private RejoinCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        filter = new TestNetworkFilter();
        network = new FakeNetwork(filter);
        cluster = new RejoinCluster(network);
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Heal Partition -> Higher-Term Heartbeat -> Immediate Step-Down, Persist & Timer Arm
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Heal partition: Leader cũ nhận heartbeat term mới -> step-down, persist term/vote=null, dừng heartbeat và arm timer")
    void rejoinAfterHeal_oldLeaderReceivesHigherTermHeartbeat_stepsDownPersistsAndArmsTimer() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        ApplyResult g01 = oldLeader.bookTicket(reqId(201), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, g01.outcome());
        cluster.syncCluster();

        // Partition 3–2: minority={node-1, node-2}, majority={node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Majority elects node-3 at term 2
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, newLeader.state.role());
        assertEquals(2L, newLeader.state.currentTerm());

        // Old leader still thinks it is LEADER at term 1 (timer stopped, heartbeat active)
        assertEquals(Role.LEADER, oldLeader.state.role());
        assertEquals(1L, oldLeader.state.currentTerm());
        assertTrue(oldLeader.leadershipGuard().isCurrent(1L));
        int stepDownsBefore = oldLeader.stepDownCount;
        int timerSchedulesBefore = oldLeader.scheduler.scheduledCount();

        // Heal partition -> newLeader sends heartbeat (empty entries) to all peers
        network.heal();
        newLeader.sendHeartbeat();
        network.deliverAllAppends();

        // Old leader immediately steps down to FOLLOWER at term 2 with leaderHint="node-3"
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());
        assertNull(oldLeader.state.votedFor());
        assertEquals("node-3", oldLeader.state.leaderId());

        // TV5-D14 durable persistence & lifecycle hooks verified
        assertEquals(2L, oldLeader.storage.term);
        assertNull(oldLeader.storage.vote);
        assertTrue(oldLeader.stepDownCount > stepDownsBefore, "onStepDown hook phải được gọi để dừng heartbeat cũ");
        assertTrue(oldLeader.scheduler.scheduledCount() > timerSchedulesBefore,
                "Election timer trên Leader cũ phải được re-arm sau khi step-down");
        assertFalse(oldLeader.leadershipGuard().isCurrent(1L),
                "LeadershipGuard phải vô hiệu hóa mọi callback của term 1 cũ");
        assertEquals(1, cluster.leaderCount());
        assertEquals("node-3", cluster.currentLeaderId().orElseThrow());

        // Write attempt to deposed old leader is rejected with leaderHint="node-3"
        BookingRequestHandler.NotLeaderException notLeader = assertThrows(
                BookingRequestHandler.NotLeaderException.class,
                () -> oldLeader.bookTicket(reqId(202), "SE-001", "G02", "Bob"));
        assertEquals("node-3", notLeader.leaderHint());
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Old Leader Sends Stale AppendEntries After Heal -> Steps Down via HigherTermHandler
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Heal partition: Leader cũ gửi AppendEntries term cũ -> bị từ chối và step-down qua TV5-D14 HigherTermHandler")
    void rejoinAfterHeal_oldLeaderSendsStaleAppendToMajority_stepsDownViaHigherTermHandlerOnResponse() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(211), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Old leader accepts an uncommitted ghost write in minority (2/5)
        CompletableFuture<ApplyResult> ghostWrite =
                oldLeader.submitBooking(reqId(212), "SE-001", "G02", "Ghost Minority");
        network.deliverAllAppends();
        assertEquals(1L, oldLeader.state.commitIndex());
        assertFalse(ghostWrite.isDone());

        // Majority elects node-3 at term 2
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, newLeader.state.role());
        newLeader.sendHeartbeat();
        network.deliverAllAppends();

        // Record majority follower node-4 timer count before stale RPC
        TestNode node4 = cluster.node("node-4");
        int node4TimerBefore = node4.scheduler.scheduledCount();

        // Heal partition: old leader sends stale term-1 AppendEntries first!
        network.heal();
        int oldLeaderStepDownsBefore = oldLeader.stepDownCount;
        int oldLeaderTimersBefore = oldLeader.scheduler.scheduledCount();
        oldLeader.replicateToPeers();
        network.deliverAllAppends();

        // Majority nodes rejected stale term-1 AppendEntries without resetting election timer (SPEC-04 §3.1)
        assertEquals(2L, node4.state.currentTerm());
        assertEquals(1L, node4.state.lastLogIndex());
        assertEquals(node4TimerBefore, node4.scheduler.scheduledCount(),
                "Stale-term AppendEntries không được phép reset election timer trên follower term mới");

        // Old leader received AppendEntriesResponse(term=2, success=false) -> HigherTermHandler stepped it down
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());
        assertNull(oldLeader.state.votedFor());
        assertEquals(2L, oldLeader.storage.term);
        assertNull(oldLeader.storage.vote);
        assertTrue(oldLeader.stepDownCount > oldLeaderStepDownsBefore);
        assertTrue(oldLeader.scheduler.scheduledCount() > oldLeaderTimersBefore);
        assertFalse(oldLeader.leadershipGuard().isCurrent(1L));
        assertFalse(ghostWrite.isDone(), "Pending write từ term 1 không bao giờ được hoàn thành sau step-down");
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: TC-RAFT-07 — Heartbeat Does Not Truncate Suffix & Never Advances Commit Past Match
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. TC-RAFT-07: Heartbeat term mới gây step-down & reset timer nhưng KHÔNG xóa suffix và KHÔNG advance commitIndex quá matchIndex")
    void rejoinAfterHeal_heartbeatAloneDoesNotTruncateUnmatchedSuffix_andDoesNotAdvanceCommitPastMatchedIndex() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(221), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Old leader appends 2 uncommitted ghost writes at indices 2, 3 (term 1)
        oldLeader.submitBooking(reqId(222), "SE-001", "G02", "Ghost-A");
        oldLeader.submitBooking(reqId(223), "SE-001", "G03", "Ghost-B");
        network.deliverAllAppends();
        assertEquals(3L, oldLeader.state.lastLogIndex());
        assertEquals(1L, oldLeader.state.commitIndex());

        // Majority elects node-3 at term 2 and commits 3 entries (indices 2, 3, 4 -> leaderCommit=4)
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(ApplyResult.Outcome.CONFIRMED, newLeader.bookTicket(reqId(224), "SE-001", "G02", "Dave").outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, newLeader.bookTicket(reqId(225), "SE-001", "G03", "Eve").outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, newLeader.bookTicket(reqId(226), "SE-001", "G04", "Frank").outcome());
        assertEquals(4L, newLeader.state.commitIndex());

        // Heal partition
        network.heal();

        // Step A: Direct heartbeat from newLeader at prevLogIndex=4, prevLogTerm=2 (mismatches oldLeader's log)
        int timersBefore = oldLeader.scheduler.scheduledCount();
        AppendEntriesResponse mismatchResp = oldLeader.core.handleAppendEntries(
                new AppendEntriesRequest(2L, "node-3", 4L, 2L, List.of(), 4L));
        assertFalse(mismatchResp.success(), "Heartbeat với prevLogIndex=4 phải trả success=false vì log divergence");
        assertEquals(2L, mismatchResp.term());
        assertEquals(Role.FOLLOWER, oldLeader.state.role(), "Dù log mismatch, Leader cũ vẫn phải step-down ngay sang term 2");
        assertEquals(2L, oldLeader.state.currentTerm());
        assertEquals("node-3", oldLeader.state.leaderId());
        assertTrue(oldLeader.scheduler.scheduledCount() > timersBefore,
                "SPEC-04 §3.1: AppendEntries hợp lệ (dù log mismatch) phải arm/reset election timer");
        assertEquals(3L, oldLeader.state.lastLogIndex(), "Log mismatch không được xóa suffix cục bộ");
        assertEquals(1L, oldLeader.state.commitIndex());

        // Step B: Direct heartbeat at matching prefix prevLogIndex=1, prevLogTerm=1, leaderCommit=4
        AppendEntriesResponse matchHeartbeat = oldLeader.core.handleAppendEntries(
                new AppendEntriesRequest(2L, "node-3", 1L, 1L, List.of(), 4L));
        assertTrue(matchHeartbeat.success());
        assertEquals(1L, matchHeartbeat.matchIndex());
        // SPEC-04 §4 / TC-RAFT-07:
        // 1. Empty heartbeat MUST NOT truncate the uncommitted suffix [2, 3]
        assertEquals(3L, oldLeader.state.lastLogIndex(),
                "Heartbeat rỗng không được phép cắt suffix chưa khớp khi chưa có entry xung đột");
        // 2. commitIndex MUST be min(leaderCommit=4, matchIndex=1) = 1, NEVER local lastLogIndex=3!
        assertEquals(1L, oldLeader.state.commitIndex(),
                "commitIndex không được vượt quá matchIndex=1 của heartbeat dù lastLogIndex=3 và leaderCommit=4");
        assertEquals(1L, oldLeader.state.lastApplied());
        assertEquals("AVAILABLE", oldLeader.stateMachine.seatStatus("G02").orElse("AVAILABLE"),
                "Không được apply entry mồ côi khi chỉ mới nhận heartbeat");
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Single Uncommitted Ghost Write Truncated Durably & Replaced via TV4 LogConflictResolver
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Phối hợp TV4: 1 entry mồ côi chưa commit trên Leader cũ bị truncate bền vững và thay thế bởi entry term mới")
    void rejoinAfterHeal_singleUncommittedGhostWrite_truncatedDurablyAndReplacedByNewLeader() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(231), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Minority appends ghost entry at index 2 (term 1) for G02
        CompletableFuture<ApplyResult> ghostFuture =
                oldLeader.submitBooking(reqId(232), "SE-001", "G02", "Ghost Passenger");
        network.deliverAllAppends();
        assertEquals(2L, oldLeader.state.lastLogIndex());
        assertEquals(1L, oldLeader.state.entryAt(2L).term());
        assertEquals(2, oldLeader.storage.log.size());

        // Majority elects node-3 at term 2 and commits G02 for Dave at index 2 (term 2)
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult daveTicket = newLeader.bookTicket(reqId(233), "SE-001", "G02", "Dave");
        assertEquals(ApplyResult.Outcome.CONFIRMED, daveTicket.outcome());
        assertEquals(2L, newLeader.state.commitIndex());

        // Heal partition and run backtracking replication rounds from newLeader
        network.heal();
        cluster.convergeFromLeader("node-3");

        // Verify oldLeader stepped down and TV4 LogConflictResolver replaced index 2 durably
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());
        assertEquals(2L, oldLeader.state.lastLogIndex());
        assertEquals(2L, oldLeader.state.commitIndex());
        assertEquals(2L, oldLeader.state.lastApplied());
        assertEquals(2L, oldLeader.state.entryAt(2L).term());
        assertEquals(newLeader.state.entries(), oldLeader.state.entries());
        assertEquals(newLeader.storage.log, oldLeader.storage.log,
                "Durable RaftStorage trên Leader cũ phải đã truncate và ghi bền entry term 2");
        assertTrue(oldLeader.storage.replaceSuffixCalls > 0);

        // BookingStateMachine converged on Dave's ticket, ghost write never confirmed
        assertEquals("BOOKED", oldLeader.stateMachine.seatStatus("G02").orElse(null));
        assertEquals(daveTicket.ticketId(), oldLeader.stateMachine.seatCurrentTicketId("G02").orElse(null));
        assertFalse(ghostFuture.isDone());
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: Long Divergent Uncommitted Suffix (Longer Than New Leader Log — Raft Fig. 7)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Phối hợp TV4 (Raft Fig. 7): Suffix chưa commit DÀI HƠN log Leader mới (5 vs 3) bị cắt sạch về đúng độ dài 3")
    void rejoinAfterHeal_longDivergentUncommittedSuffix_longerThanNewLeaderLog_truncatedAndRepaired() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(241), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Minority appends 4 uncommitted ghost entries (indices 2, 3, 4, 5 at term 1)
        oldLeader.submitBooking(reqId(242), "SE-001", "G02", "Ghost-1");
        oldLeader.submitBooking(reqId(243), "SE-001", "G03", "Ghost-2");
        oldLeader.submitBooking(reqId(244), "SE-001", "G04", "Ghost-3");
        oldLeader.submitBooking(reqId(245), "SE-001", "G05", "Ghost-4");
        network.deliverAllAppends();

        // Both minority nodes now have lastLogIndex = 5, commitIndex = 1
        assertEquals(5L, oldLeader.state.lastLogIndex());
        assertEquals(5L, cluster.node("node-2").state.lastLogIndex());
        assertEquals(1L, oldLeader.state.commitIndex());
        assertEquals(1L, cluster.node("node-2").state.commitIndex());

        // Majority elects node-3 at term 2 and commits ONLY 2 entries (indices 2, 3 -> lastLogIndex = 3)
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult dave = newLeader.bookTicket(reqId(246), "SE-001", "G02", "Dave");
        ApplyResult eve = newLeader.bookTicket(reqId(247), "SE-001", "G06", "Eve");
        assertEquals(ApplyResult.Outcome.CONFIRMED, dave.outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, eve.outcome());
        assertEquals(3L, newLeader.state.lastLogIndex());
        assertEquals(3L, newLeader.state.commitIndex());

        // Heal partition and converge all 5 nodes
        network.heal();
        cluster.convergeFromLeader("node-3");

        // Entire long suffix [2, 3, 4, 5] must be truncated; final log length on all 5 nodes must be 3!
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(2L, n.state.currentTerm(), "term trên " + id);
            assertEquals(3L, n.state.lastLogIndex(), "lastLogIndex trên " + id + " phải giảm từ 5 về 3");
            assertEquals(3L, n.state.commitIndex(), "commitIndex trên " + id);
            assertEquals(3L, n.state.lastApplied(), "lastApplied trên " + id);
            assertEquals(newLeader.state.entries(), n.state.entries(), "log state trên " + id);
            assertEquals(newLeader.storage.log, n.storage.log, "durable storage log trên " + id);
            assertEquals("BOOKED", n.stateMachine.seatStatus("G01").orElse(null));
            assertEquals(dave.ticketId(), n.stateMachine.seatCurrentTicketId("G02").orElse(null));
            assertEquals(eve.ticketId(), n.stateMachine.seatCurrentTicketId("G06").orElse(null));
            assertEquals("AVAILABLE", n.stateMachine.seatStatus("G03").orElse("AVAILABLE"));
            assertEquals("AVAILABLE", n.stateMachine.seatStatus("G04").orElse("AVAILABLE"));
            assertEquals("AVAILABLE", n.stateMachine.seatStatus("G05").orElse("AVAILABLE"));
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Multi-Term Cascading Divergence -> Full Backtracking Across Terms 1, 2, 3
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Phối hợp TV4: Phân kỳ đa term (term 1 vs term 2 -> term 3) backtrack chính xác qua ReplicationTracker và hội tụ 5 node")
    void rejoinAfterHeal_multiTermCascadingDivergence_oldLeaderWithMultipleStaleTermsBacktracksAndRepairs() throws Exception {
        // Term 1: node-1 commits index 1 (G01)
        cluster.electLeader("node-1");
        TestNode node1 = cluster.node("node-1");
        node1.bookTicket(reqId(251), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        // Partition 3–2: minority={node-1, node-2}, majority={node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Minority leader node-1 writes 2 uncommitted entries at indices 2, 3 (term 1)
        node1.submitBooking(reqId(252), "SE-001", "G02", "Ghost-T1-A");
        node1.submitBooking(reqId(253), "SE-001", "G03", "Ghost-T1-B");
        network.deliverAllAppends();

        // Majority Term 2: node-3 elected and commits index 2 (G02)
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult t2Ticket = node3.bookTicket(reqId(254), "SE-001", "G02", "Term2-Winner");
        assertEquals(ApplyResult.Outcome.CONFIRMED, t2Ticket.outcome());
        node3.sendHeartbeat();
        network.deliverAllAppends();

        // Majority Term 3: node-4 takes over as Leader at term 3 and commits indices 3 (G03) and 4 (G04)
        TestNode node4 = cluster.node("node-4");
        node4.startElection();
        network.deliverVote("node-4", "node-3", 3L);
        network.deliverVote("node-4", "node-5", 3L);
        assertEquals(Role.LEADER, node4.state.role());
        assertEquals(3L, node4.state.currentTerm());
        ApplyResult t3TicketA = node4.bookTicket(reqId(255), "SE-001", "G03", "Term3-A");
        ApplyResult t3TicketB = node4.bookTicket(reqId(256), "SE-001", "G04", "Term3-B");
        assertEquals(ApplyResult.Outcome.CONFIRMED, t3TicketA.outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, t3TicketB.outcome());
        assertEquals(4L, node4.state.commitIndex());

        // Heal partition: node-4 (term 3, log terms [1, 2, 3, 3]) repairs node-1 & node-2 (term 1, log terms [1, 1, 1])
        network.heal();
        cluster.convergeFromLeader("node-4");

        // Verify backtracking resolved all conflicting indices cleanly across all 5 nodes
        assertEquals(1, cluster.leaderCount());
        assertEquals("node-4", cluster.currentLeaderId().orElseThrow());
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(3L, n.state.currentTerm(), "term trên " + id);
            assertEquals(4L, n.state.lastLogIndex(), "lastLogIndex trên " + id);
            assertEquals(4L, n.state.commitIndex(), "commitIndex trên " + id);
            assertEquals(4L, n.state.lastApplied(), "lastApplied trên " + id);
            assertEquals(List.of(1L, 2L, 3L, 3L),
                    n.state.entries().stream().map(RaftLogEntry::term).toList(),
                    "dãy term trên " + id);
            assertEquals(node4.state.entries(), n.state.entries());
            assertEquals(node4.storage.log, n.storage.log);
            assertEquals(t2Ticket.ticketId(), n.stateMachine.seatCurrentTicketId("G02").orElse(null));
            assertEquals(t3TicketA.ticketId(), n.stateMachine.seatCurrentTicketId("G03").orElse(null));
            assertEquals(t3TicketB.ticketId(), n.stateMachine.seatCurrentTicketId("G04").orElse(null));
        }
        assertEquals(4L, node4.tracker.matchIndexOf("node-1"));
        assertEquals(4L, node4.tracker.matchIndexOf("node-2"));
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Both Minority Nodes Have Divergent Suffixes -> Full 5-Node Convergence
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Toàn cụm 5 node: Cả 2 node phía thiểu số (node-1 & node-2) sửa sạch suffix mồ côi và đồng bộ 10 ghế")
    void rejoinAfterHeal_bothMinorityNodesHaveDivergentSuffixes_allFiveNodesConvergeExactly() throws Exception {
        cluster.electLeader("node-1");
        TestNode node1 = cluster.node("node-1");
        node1.bookTicket(reqId(261), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Both node-1 and node-2 durably receive 3 uncommitted ghost entries
        node1.submitBooking(reqId(262), "SE-001", "G07", "Ghost-7");
        node1.submitBooking(reqId(263), "SE-001", "G08", "Ghost-8");
        node1.submitBooking(reqId(264), "SE-001", "G09", "Ghost-9");
        network.deliverAllAppends();
        assertEquals(4L, node1.state.lastLogIndex());
        assertEquals(4L, cluster.node("node-2").state.lastLogIndex());

        // Majority commits 2 different seats (G02, G03)
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        node3.bookTicket(reqId(265), "SE-001", "G02", "Bob");
        node3.bookTicket(reqId(266), "SE-001", "G03", "Charlie");

        network.heal();
        cluster.convergeFromLeader("node-3");

        BookingService.SeatSnapshot leaderSeats = node3.getSeats("SE-001");
        assertEquals(10, leaderSeats.seats().size());
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(id.equals("node-3") ? Role.LEADER : Role.FOLLOWER, n.state.role());
            assertEquals(2L, n.state.currentTerm());
            assertEquals(3L, n.state.commitIndex());
            assertEquals(3L, n.state.lastApplied());
            assertEquals(3L, n.state.lastLogIndex());
            assertEquals(node3.state.entries(), n.state.entries());
            assertEquals(node3.storage.log, n.storage.log);
            assertEquals(leaderSeats.seats(), n.getSeats("SE-001").seats(),
                    "Bảng 10 ghế G01–G10 trên " + id + " phải khớp tuyệt đối với Leader");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: TC-RAFT-09 — Old Leader Restarts From Durable Storage With Uncommitted Suffix
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. TC-RAFT-09: Leader cũ crash & restart từ storage có suffix chưa commit -> commitIndex=lastApplied, sửa suffix không double-apply")
    void rejoinAfterHeal_oldLeaderRestartsFromDurableStorageWithUncommittedSuffix_repairsWithoutDoubleApply() throws Exception {
        cluster.electLeader("node-1");
        TestNode node1 = cluster.node("node-1");
        ApplyResult g01 = node1.bookTicket(reqId(271), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, g01.outcome());
        cluster.syncCluster();
        assertEquals(1L, node1.storage.loadLastApplied());

        // Partition 3–2 and let old leader durably write 2 uncommitted entries (indices 2, 3)
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        node1.submitBooking(reqId(272), "SE-001", "G02", "Ghost-Before-Crash-1");
        node1.submitBooking(reqId(273), "SE-001", "G03", "Ghost-Before-Crash-2");
        network.deliverAllAppends();
        assertEquals(3, node1.storage.log.size());
        assertEquals(1L, node1.storage.loadLastApplied());

        // Majority elects node-3 term 2 and commits indices 2, 3
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult dave = node3.bookTicket(reqId(274), "SE-001", "G02", "Dave");
        ApplyResult eve = node3.bookTicket(reqId(275), "SE-001", "G03", "Eve");
        assertEquals(ApplyResult.Outcome.CONFIRMED, dave.outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, eve.outcome());

        // Old leader restarts from its durable storage (preserving term=1, log=[1,2,3], lastApplied=1)
        TestNode restartedNode1 = cluster.restartNodeFromStorage("node-1");

        // SPEC-04 §6 / TC-RAFT-09: After restart, role=FOLLOWER, commitIndex == lastApplied == 1 (NOT lastLogIndex=3!)
        assertEquals(Role.FOLLOWER, restartedNode1.state.role());
        assertEquals(1L, restartedNode1.state.currentTerm());
        assertEquals(3L, restartedNode1.state.lastLogIndex());
        assertEquals(1L, restartedNode1.state.commitIndex(),
                "Sau restart commitIndex phải khởi tạo từ lastApplied=1, tuyệt đối không suy từ lastLogIndex=3");
        assertEquals(1L, restartedNode1.state.lastApplied());
        assertTrue(restartedNode1.appliedLogIndices.isEmpty(),
                "Không được replay lại prefix [1..lastApplied] ngay lúc restart");

        // Heal partition -> restarted old leader repairs uncommitted suffix [2, 3] and applies ONLY indices 2 and 3
        network.heal();
        cluster.convergeFromLeader("node-3");

        assertEquals(2L, restartedNode1.state.currentTerm());
        assertEquals(3L, restartedNode1.state.commitIndex());
        assertEquals(3L, restartedNode1.state.lastApplied());
        assertEquals(List.of(2L, 3L), restartedNode1.appliedLogIndices,
                "Node sau restart chỉ được apply đúng các entry mới [2, 3], không double-apply index 1");
        assertEquals(node3.state.entries(), restartedNode1.state.entries());
        assertEquals(node3.storage.log, restartedNode1.storage.log);
        assertEquals(g01.ticketId(), restartedNode1.stateMachine.seatCurrentTicketId("G01").orElse(null));
        assertEquals(dave.ticketId(), restartedNode1.stateMachine.seatCurrentTicketId("G02").orElse(null));
        assertEquals(eve.ticketId(), restartedNode1.stateMachine.seatCurrentTicketId("G03").orElse(null));
    }

    // -------------------------------------------------------------------------------------------
    // Test 9: TC-RAFT-08 — Delayed Stale-Term Responses Ignored by LeadershipGuard After Step-Down
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. TC-RAFT-08: Response AppendEntries trễ từ term 1 cũ bị LeadershipGuard bỏ qua sau khi Leader cũ đã step-down")
    void rejoinAfterHeal_staleDelayedResponsesFromOldTerm_ignoredByLeadershipGuardAndNeverCommit() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(281), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        // Partition 3–2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Old leader appends uncommitted index 2 (term 1) and enqueues AppendEntries RPCs (NOT delivered yet!)
        CompletableFuture<ApplyResult> stalePending =
                oldLeader.submitBooking(reqId(282), "SE-001", "G02", "Delayed Ghost");
        List<SentAppendRpc> capturedStaleRpcs = network.drainPendingAppends();
        assertFalse(capturedStaleRpcs.isEmpty());

        // Majority elects node-3 term 2
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, newLeader.state.role());

        // Heal partition and step down oldLeader via heartbeat
        network.heal();
        newLeader.sendHeartbeat();
        network.deliverAllAppends();
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());

        // Now deliver delayed/reordered term-1 success responses straight to oldLeader's response callback!
        for (SentAppendRpc staleRpc : capturedStaleRpcs) {
            staleRpc.reply.accept(new AppendEntriesResponse(1L, true, 2L));
        }

        // LeadershipGuard fenced all stale callbacks: no commit advance, no CONFIRMED, stays FOLLOWER at term 2
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());
        assertEquals(1L, oldLeader.state.commitIndex());
        assertFalse(stalePending.isDone(), "Callback trễ term 1 không bao giờ được hoàn thành lệnh ghi cũ");
    }

    // -------------------------------------------------------------------------------------------
    // Test 10: Committed Prefix Protection — Conflict at Committed Index Fails Closed (SPEC-04 §4)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Bảo vệ tiền tố đã commit: Xung đột tại index <= commitIndex fail-closed, không ghi đè dữ liệu đã commit")
    void rejoinAfterHeal_committedPrefixProtection_conflictAtCommittedIndexFailsClosed() throws Exception {
        cluster.electLeader("node-1");
        TestNode node1 = cluster.node("node-1");
        ApplyResult g01 = node1.bookTicket(reqId(291), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, g01.outcome());
        cluster.syncCluster();

        RaftLogEntry committedEntry1 = node1.state.entryAt(1L);
        List<RaftLogEntry> storageLogBefore = List.copyOf(node1.storage.log);

        // Adversarial/buggy RPC attempting to overwrite committed index 1 with a different term (term 2)
        RaftLogEntry conflictingAtIndex1 = new RaftLogEntry(
                1L, 2L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                "{\"requestId\":\"" + reqId(299) + "\",\"tripId\":\"SE-001\",\"seatId\":\"G01\"}");
        AppendEntriesRequest illegalOverwrite = new AppendEntriesRequest(
                2L, "node-3", 0L, 0L, List.of(conflictingAtIndex1), 1L);

        // TV4 LogConflictResolver + AppendEntriesHandler + PersistentRaftNode MUST fail closed
        assertThrows(IllegalStateException.class,
                () -> node1.core.handleAppendEntries(illegalOverwrite),
                "Ghi đè entry đã commit phải bị chặn fail-closed với IllegalStateException");

        // Committed prefix [1..1] in both RaftState and RaftStorage is untouched!
        assertEquals(committedEntry1, node1.state.entryAt(1L));
        assertEquals(storageLogBefore, node1.storage.log);
        assertEquals(1L, node1.state.commitIndex());

        // Subsequent operations on the poisoned node fail closed until storage recovery
        assertThrows(IllegalStateException.class,
                () -> node1.core.handleAppendEntries(
                        new AppendEntriesRequest(2L, "node-3", 1L, 1L, List.of(), 1L)));
        assertThrows(IllegalStateException.class,
                () -> node1.core.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"));
    }

    // -------------------------------------------------------------------------------------------
    // Test 11: Storage Failure During Higher-Term Step-Down Stops Leader & Fails Closed (TV5-D14)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. TV5-D14 Safety: Lỗi RaftStorage khi persist term mới lúc step-down -> dừng Leader và fail-closed")
    void rejoinAfterHeal_storageFailureDuringHigherTermStepDown_stopsLeaderAndFailsClosed() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        oldLeader.bookTicket(reqId(301), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        // Inject durable storage failure on next saveTermAndVote
        oldLeader.storage.failNextSaveTermAndVote(new RaftStorageException("PG1 disk unavailable"));
        int stepDownsBefore = oldLeader.stepDownCount;
        int timersBefore = oldLeader.scheduler.scheduledCount();

        // Higher-term heartbeat arrives from new leader node-3
        assertThrows(RaftStorageException.class,
                () -> oldLeader.core.handleAppendEntries(
                        new AppendEntriesRequest(2L, "node-3", 1L, 1L, List.of(), 1L)));

        // Leader activity stopped, timer was NOT re-armed, storage term stayed 1, core failed closed
        assertTrue(oldLeader.stepDownCount > stepDownsBefore,
                "onStepDown phải được gọi ngay cả khi persist term thất bại");
        assertEquals(timersBefore, oldLeader.scheduler.scheduledCount(),
                "Không được re-arm election timer khi persist term thất bại");
        assertEquals(1L, oldLeader.storage.term);
        assertThrows(IllegalStateException.class,
                () -> oldLeader.core.appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}"),
                "Core fail-closed phải từ chối mọi lệnh ghi mới");
    }

    // -------------------------------------------------------------------------------------------
    // Test 12: Real RaftElectionComponent + PersistentRaftNode Lifecycle Rejoin & Suffix Repair
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Tích hợp RaftElectionComponent thật (TV5-D14 + TV4): Nhận APPEND_ENTRIES DTO term 2 -> dừng HeartbeatScheduler & sửa suffix bền vững")
    void rejoinAfterHeal_realRaftElectionComponentLifecycle_stopsHeartbeatSchedulerAndRepairsSuffix() throws Exception {
        DurableMemoryStorage storage = new DurableMemoryStorage();
        // Pre-seed term 1 committed prefix at index 1 so both sides share common index 1
        RaftLogEntry committed1 = new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        storage.term = 1L;
        storage.vote = "node-1";
        storage.log.add(committed1);
        storage.lastApplied = 1L;

        AtomicInteger heartbeatSends = new AtomicInteger();
        AtomicReference<PersistentRaftNode> coreRef = new AtomicReference<>();
        List<Exception> failures = new CopyOnWriteArrayList<>();

        try (RaftElectionComponent component = new RaftElectionComponent(
                "node-1",
                storage,
                (state, durable) -> {
                    PersistentRaftNode node = new PersistentRaftNode("node-1", state, durable);
                    coreRef.set(node);
                    return node;
                },
                (peer, request, reply) -> {
                    try {
                        reply.receive(new RequestVoteResponse(request.getTerm(), true));
                    } catch (RaftStorageException e) {
                        throw new IllegalStateException(e);
                    }
                },
                new ElectionConfig(15, 300, 600),
                failures::add,
                () -> {})) {

            HandlerRegistry peerRegistry = new HandlerRegistry();
            component.register(peerRegistry);
            component.configureHeartbeat((peer, request, reply) -> heartbeatSends.incrementAndGet());

            // Elect node-1 as Leader (at term 2 since storage started at term 1)
            new PersistentElectionRuntimeTest().awaitLeader(component);
            RaftElectionComponent.Status leaderStatus = component.status();
            assertEquals("LEADER", leaderStatus.role());
            long oldLeaderTerm = leaderStatus.currentTerm();
            assertEquals(2L, oldLeaderTerm);

            // Append 2 uncommitted ghost entries on the real PersistentRaftNode (indices 2 and 3, term 2)
            AtomicReference<RaftNode.AppendOutcome> ghostA = new AtomicReference<>();
            AtomicReference<RaftNode.AppendOutcome> ghostB = new AtomicReference<>();
            component.execute(() -> {
                ghostA.set(coreRef.get().appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{\"ghost\":1}"));
                ghostB.set(coreRef.get().appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{\"ghost\":2}"));
            });
            component.status(); // event-loop barrier
            assertInstanceOf(RaftNode.Accepted.class, ghostA.get());
            assertInstanceOf(RaftNode.Accepted.class, ghostB.get());
            assertEquals(3, storage.log.size());

            // Simulate heal partition: new leader node-3 at term 3 sends APPEND_ENTRIES DTO via peer HandlerRegistry
            // replacing the 2 uncommitted entries [2, 3] with 1 committed entry [2] at term 3
            AppendEntriesRequestDto repairDto = new AppendEntriesRequestDto(
                    3L,
                    "node-3",
                    1L,
                    1L,
                    List.of(new AppendEntriesRequestDto.LogEntryDto(
                            2L, 3L, RaftLogEntry.COMMAND_TYPE_NO_OP, Map.of("winner", "node-3"))),
                    2L);
            MessageEnvelope<JsonNode> wireRequest = MessageEnvelope.createRequest(
                    MessageType.APPEND_ENTRIES, "node-3", MAPPER.valueToTree(repairDto));

            MessageEnvelope<?> wireResponse =
                    peerRegistry.getHandler(MessageType.APPEND_ENTRIES).handle(wireRequest);
            assertEquals(MessageType.APPEND_ENTRIES_RESPONSE, wireResponse.getType());
            AppendEntriesResponse response = assertInstanceOf(AppendEntriesResponse.class, wireResponse.getPayload());
            assertTrue(response.success(), "AppendEntries sửa suffix phải trả success=true");
            assertEquals(3L, response.term());
            assertEquals(2L, response.matchIndex());

            // Verify component stepped down, heartbeat scheduler stopped, and suffix [2, 3] replaced by [2(t3)]
            RaftElectionComponent.Status afterRejoin = component.status();
            assertEquals("FOLLOWER", afterRejoin.role());
            assertEquals(3L, afterRejoin.currentTerm());
            assertEquals(2L, afterRejoin.commitIndex());
            assertTrue(afterRejoin.storageHealthy());

            int sendsAfterStepDown = heartbeatSends.get();
            Thread.sleep(70); // > 4 heartbeat intervals, well before election timeout (300ms)
            assertEquals(sendsAfterStepDown, heartbeatSends.get(),
                    "LeaderHeartbeatScheduler phải ngừng phát heartbeat term cũ sau khi step-down");

            // Further writes on deposed old leader are rejected as NotLeader("node-3")
            AtomicReference<RaftNode.AppendOutcome> rejectedWrite = new AtomicReference<>();
            component.execute(() -> rejectedWrite.set(coreRef.get().appendCommand(RaftLogEntry.COMMAND_TYPE_NO_OP, "{}")));
            component.status();
            RaftNode.NotLeader notLeader = assertInstanceOf(RaftNode.NotLeader.class, rejectedWrite.get());
            assertEquals("node-3", notLeader.leaderHint());

            // Durable storage persisted term=3, votedFor=null, and repaired 2-entry log [1(t1), 2(t3)]
            PersistentState reloaded = storage.load();
            assertEquals(3L, reloaded.currentTerm());
            assertNull(reloaded.votedFor());
            assertEquals(2, reloaded.log().size());
            assertEquals(1L, reloaded.log().get(0).term());
            assertEquals(3L, reloaded.log().get(1).term());
            assertEquals("{\"winner\":\"node-3\"}", reloaded.log().get(1).commandPayload());
            assertTrue(failures.isEmpty(), "Không có ngoại lệ lỗi nội bộ trên RaftElectionComponent");
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 13: Post-Rejoin Client Redirection, Idempotency & Business Safety (SEAT_ALREADY_BOOKED)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("13. Sau rejoin: Redirect Client từ Leader cũ sang Leader mới, idempotency term 1 giữ nguyên, retry ghost write bị SEAT_ALREADY_BOOKED")
    void rejoinAfterHeal_oldLeaderClientRedirectionAndIdempotentRetry_succeedsOnNewLeader() throws Exception {
        cluster.electLeader("node-1");
        TestNode oldLeader = cluster.node("node-1");
        ApplyResult aliceG01 = oldLeader.bookTicket(reqId(311), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, aliceG01.outcome());
        cluster.syncCluster();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Minority ghost request reqId(312) for G02 (never committed)
        CompletableFuture<ApplyResult> ghostG02 =
                oldLeader.submitBooking(reqId(312), "SE-001", "G02", "Minority Ghost");
        network.deliverAllAppends();

        // Majority elects node-3 term 2 and commits reqId(313) for G02 -> Dave wins G02
        TestNode newLeader = cluster.node("node-3");
        newLeader.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult daveG02 = newLeader.bookTicket(reqId(313), "SE-001", "G02", "Dave");
        assertEquals(ApplyResult.Outcome.CONFIRMED, daveG02.outcome());

        // Heal partition & converge
        network.heal();
        cluster.convergeFromLeader("node-3");
        assertFalse(ghostG02.isDone());

        // 1. Client sends new booking G04 to oldLeader -> redirected via leaderHint="node-3" -> CONFIRMED
        BookingRequestHandler.NotLeaderException redirect = assertThrows(
                BookingRequestHandler.NotLeaderException.class,
                () -> oldLeader.bookTicket(reqId(314), "SE-001", "G04", "Frank"));
        assertEquals("node-3", redirect.leaderHint());
        ApplyResult frankG04 = cluster.node(redirect.leaderHint())
                .bookTicket(reqId(314), "SE-001", "G04", "Frank");
        assertEquals(ApplyResult.Outcome.CONFIRMED, frankG04.outcome());

        // 2. Client retries already-committed term-1 reqId(311) on newLeader -> idempotent cached result
        ApplyResult idempotentAlice = newLeader.bookTicket(reqId(311), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, idempotentAlice.outcome());
        assertEquals(aliceG01.ticketId(), idempotentAlice.ticketId());

        // 3. Client retries uncommitted ghost reqId(312) for G02 on newLeader -> REJECTED(SEAT_ALREADY_BOOKED)
        ApplyResult ghostRetry = newLeader.bookTicket(reqId(312), "SE-001", "G02", "Minority Ghost");
        assertEquals(ApplyResult.Outcome.REJECTED, ghostRetry.outcome());
        assertEquals("SEAT_ALREADY_BOOKED", ghostRetry.reasonCode());
        assertNull(ghostRetry.ticketId());

        // Propagate and verify all 5 nodes agree on G01 (Alice), G02 (Dave), G04 (Frank)
        cluster.convergeFromLeader("node-3");
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(aliceG01.ticketId(), n.stateMachine.seatCurrentTicketId("G01").orElse(null));
            assertEquals(daveG02.ticketId(), n.stateMachine.seatCurrentTicketId("G02").orElse(null));
            assertEquals(frankG04.ticketId(), n.stateMachine.seatCurrentTicketId("G04").orElse(null));
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 14: Rejoined Old Leader Can Be Re-Elected Healthy Leader in a Later Term After Catch-Up
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("14. Phục hồi hoàn toàn: Leader cũ sau khi step-down và sửa suffix có thể tái đắc cử ở term 3 khi Leader mới gặp sự cố")
    void rejoinAfterHeal_oldLeaderCanBeReelectedInLaterTermAfterCatchUp() throws Exception {
        // Term 1: node-1 leads, commits G01
        cluster.electLeader("node-1");
        TestNode node1 = cluster.node("node-1");
        node1.bookTicket(reqId(321), "SE-001", "G01", "Alice");
        cluster.syncCluster();

        // Partition 3–2: node-1 writes ghost G02; majority elects node-3 term 2 and commits G02 + G03
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        node1.submitBooking(reqId(322), "SE-001", "G02", "Ghost");
        network.deliverAllAppends();

        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult daveG02 = node3.bookTicket(reqId(323), "SE-001", "G02", "Dave");
        ApplyResult eveG03 = node3.bookTicket(reqId(324), "SE-001", "G03", "Eve");
        assertEquals(ApplyResult.Outcome.CONFIRMED, daveG02.outcome());
        assertEquals(ApplyResult.Outcome.CONFIRMED, eveG03.outcome());

        // Heal partition -> node-1 steps down, repairs suffix, catches up to (lastLogIndex=3, lastLogTerm=2)
        network.heal();
        cluster.convergeFromLeader("node-3");
        assertEquals(Role.FOLLOWER, node1.state.role());
        assertEquals(3L, node1.state.lastLogIndex());
        assertEquals(2L, node1.state.lastLogTerm());

        // Next fault: isolate node-3 -> node-1 campaigns at term 3 and wins 3/5 majority!
        filter.isolate("node-3");
        node1.startElection();
        network.deliverVote("node-1", "node-2", 3L);
        network.deliverVote("node-1", "node-4", 3L);
        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(3L, node1.state.currentTerm());

        // Re-elected node-1 commits G05 at index 4 (term 3) cleanly
        ApplyResult graceG05 = node1.bookTicket(reqId(325), "SE-001", "G05", "Grace");
        assertEquals(ApplyResult.Outcome.CONFIRMED, graceG05.outcome());
        assertEquals(4L, node1.state.commitIndex());

        // De-isolate node-3 -> node-3 steps down from term 2 to term 3 and catches up index 4
        filter.deisolate("node-3");
        cluster.convergeFromLeader("node-1");

        assertEquals(Role.FOLLOWER, node3.state.role());
        assertEquals(3L, node3.state.currentTerm());
        assertEquals(4L, node3.state.commitIndex());
        assertEquals(graceG05.ticketId(), node3.stateMachine.seatCurrentTicketId("G05").orElse(null));
        assertEquals(daveG02.ticketId(), node1.stateMachine.seatCurrentTicketId("G02").orElse(null));
        assertEquals(eveG03.ticketId(), node1.stateMachine.seatCurrentTicketId("G03").orElse(null));
    }

    // ===========================================================================================
    // Real TV5 (PersistentRaftNode + HigherTermHandler + RaftElectionService)
    //  + Real TV4 (AppendEntriesHandler + LogConflictResolver + PersistentLogAppender
    //              + ReplicationTracker + CommitAdvancer)
    //  + Real TV3 (TestNetworkFilter) & TV2 (BookingStateMachine) FakeNetwork Test Harness
    // ===========================================================================================

    private static final class RejoinCluster {
        final FakeNetwork network;
        final Map<String, TestNode> nodes = new LinkedHashMap<>();

        RejoinCluster(FakeNetwork network) throws Exception {
            this.network = network;
            for (String id : VOTERS) {
                TestNode node = new TestNode(id, this, new DurableMemoryStorage(), new BookingStateMachine());
                nodes.put(id, node);
                network.registerNode(node);
            }
        }

        TestNode node(String id) {
            return nodes.get(id);
        }

        TestNode restartNodeFromStorage(String id) throws Exception {
            TestNode old = nodes.get(id);
            old.timer.close();
            // Preserve durable storage and durable business state machine checkpoint across restart
            TestNode restarted = new TestNode(id, this, old.storage, old.stateMachine);
            nodes.put(id, restarted);
            network.registerNode(restarted);
            return restarted;
        }

        void electLeader(String leaderId) throws Exception {
            TestNode candidate = nodes.get(leaderId);
            candidate.startElection();
            network.deliverAllVotes();
            assertEquals(Role.LEADER, candidate.state.role(),
                    "Candidate " + leaderId + " phải đắc cử LEADER");
            candidate.sendHeartbeat();
            network.deliverAllAppends();
        }

        void syncCluster() {
            currentLeaderId().ifPresent(this::convergeFromLeader);
        }

        /**
         * Drives deterministic AppendEntries rounds from the given Leader until all reachable peers
         * backtrack past any divergent suffix, replace uncommitted entries, and match commitIndex.
         */
        void convergeFromLeader(String leaderId) {
            TestNode leader = nodes.get(leaderId);
            for (int round = 0; round < 20; round++) {
                if (leader.state.role() != Role.LEADER) {
                    return;
                }
                leader.replicateToPeers();
                network.deliverAllAppends();
            }
        }

        int leaderCount() {
            return (int) nodes.values().stream()
                    .filter(n -> n.state.role() == Role.LEADER)
                    .count();
        }

        Optional<String> currentLeaderId() {
            return nodes.values().stream()
                    .filter(n -> n.state.role() == Role.LEADER)
                    .map(n -> n.id)
                    .findFirst();
        }

        Set<String> peersOf(String localId) {
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(localId);
            return peers;
        }
    }

    private static final class TestNode implements BookingService {
        final String id;
        final RejoinCluster cluster;
        final DurableMemoryStorage storage;
        final RaftState state;
        final PersistentRaftNode core;
        final ManualScheduledExecutor scheduler = new ManualScheduledExecutor();
        final ElectionTimer timer;
        final HigherTermHandler higherTermHandler;
        final RaftElectionService electionService;
        final BookingStateMachine stateMachine;
        final List<ApplyResult> appliedEvents = new ArrayList<>();
        final List<Long> appliedLogIndices = new ArrayList<>();
        final Map<Long, PendingWrite> pendingWrites = new LinkedHashMap<>();

        ReplicationTracker tracker;
        CommitAdvancer commitAdvancer;
        int stepDownCount;

        TestNode(String id, RejoinCluster cluster, DurableMemoryStorage storage, BookingStateMachine stateMachine)
                throws RaftStorageException {
            this.id = id;
            this.cluster = cluster;
            this.storage = storage;
            this.stateMachine = stateMachine;

            PersistentState persisted = storage.load();
            this.state = RaftState.restore(
                    persisted.currentTerm(),
                    persisted.votedFor(),
                    persisted.log(),
                    persisted.lastApplied());
            this.core = new PersistentRaftNode(id, state, storage);

            Runnable stopHeartbeatHook = () -> stepDownCount++;

            this.electionService = new RaftElectionService(
                    id,
                    core,
                    state,
                    storage,
                    (peer, req, reply) -> cluster.network.sendVote(id, peer, req, reply),
                    term -> observeHigherTermViaHandler(term),
                    this::onPromotedToLeader,
                    stopHeartbeatHook,
                    null
            );

            this.timer = new ElectionTimer(
                    electionService,
                    scheduler,
                    () -> 500L,
                    failure -> {
                        throw new AssertionError("Unexpected timer failure on " + id, failure);
                    });

            // TV5-D14 HigherTermHandler: stop heartbeat -> durable step-down -> re-arm election timer
            this.higherTermHandler = new HigherTermHandler(
                    state,
                    core::observeHigherTerm,
                    stopHeartbeatHook,
                    timer::start);

            // Wire TV4 AppendEntriesHandler (using LogConflictResolver + PersistentLogAppender) inside PersistentRaftNode
            this.core.bindLifecycle(timer, stopHeartbeatHook);
            this.core.addCommitListener(entry -> {
                if (entry.index() > state.lastApplied()) {
                    applyEntry(entry);
                }
            });

            // Followers start with election timer armed
            this.timer.start();
        }

        private void observeHigherTermViaHandler(long term) throws RaftStorageException {
            higherTermHandler.observe(term);
        }

        private void onPromotedToLeader() {
            timer.stop();
            Set<String> peers = cluster.peersOf(id);
            this.tracker = new ReplicationTracker(state, peers);
            this.commitAdvancer = new CommitAdvancer(state, tracker);
            this.electionService.configureWriteAvailability(tracker, commitAdvancer);
        }

        LeadershipGuard leadershipGuard() {
            return core.leadershipGuard();
        }

        long startElection() throws RaftStorageException {
            return electionService.startElection();
        }

        @Override
        public ApplyResult bookTicket(String requestId, String tripId, String seatId, String passengerName) throws Exception {
            CompletableFuture<ApplyResult> future = submitBooking(requestId, tripId, seatId, passengerName);
            cluster.network.deliverAll();
            try {
                return future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof Exception ex) {
                    throw ex;
                }
                throw e;
            }
        }

        CompletableFuture<ApplyResult> submitBooking(
                String requestId, String tripId, String seatId, String passengerName) {
            if (state.role() != Role.LEADER) {
                return CompletableFuture.failedFuture(
                        new BookingRequestHandler.NotLeaderException(state.leaderId()));
            }

            Optional<BookingCommand.BookTicket> logged = findLoggedBooking(requestId);
            if (logged.isPresent()) {
                BookingCommand.BookTicket prev = logged.get();
                if (!prev.tripId().equals(tripId) || !prev.seatId().equals(seatId)
                        || !prev.passengerName().equals(passengerName)) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("REQUEST_ID_CONFLICT"));
                }
                Optional<ApplyResult> existing = stateMachine.storedResult(requestId);
                if (existing.isPresent()) {
                    return CompletableFuture.completedFuture(existing.get());
                }
            }

            long term = leadershipGuard().captureTerm();
            long nextIndex = state.lastLogIndex() + 1L;
            String ticketId = "TKT-" + id.toUpperCase() + "-" + term + "-" + nextIndex;
            Instant issuedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                    requestId, tripId, seatId, passengerName, ticketId, issuedAt);
            String payload = serializeCommand(command);

            // Durable leader append via PersistentRaftNode -> PersistentLogAppender -> RaftStorage + RaftState
            RaftNode.AppendOutcome outcome = core.appendCommand(RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
            if (outcome instanceof RaftNode.NotLeader nl) {
                return CompletableFuture.failedFuture(new BookingRequestHandler.NotLeaderException(nl.leaderHint()));
            }
            if (outcome instanceof RaftNode.StorageFailure sf) {
                return CompletableFuture.failedFuture(sf.cause());
            }
            RaftNode.Accepted accepted = (RaftNode.Accepted) outcome;

            CompletableFuture<ApplyResult> future = new CompletableFuture<>();
            pendingWrites.put(accepted.index(), new PendingWrite(requestId, term, accepted.index(), command, future));

            replicateToPeers();
            return future;
        }

        void replicateToPeers() {
            if (state.role() != Role.LEADER) {
                return;
            }
            long currentTerm = state.currentTerm();
            for (String peer : cluster.peersOf(id)) {
                long nextIndex = tracker.nextIndexOf(peer);
                long prevLogIndex = Math.max(0L, nextIndex - 1L);
                long prevLogTerm = state.termAt(prevLogIndex);
                List<RaftLogEntry> entries = entriesFrom(nextIndex);
                AppendEntriesRequest request = new AppendEntriesRequest(
                        currentTerm, id, prevLogIndex, prevLogTerm, entries, state.commitIndex());
                cluster.network.sendAppend(id, peer, request, response ->
                        handleAppendResponse(peer, currentTerm, nextIndex, response));
            }
        }

        void sendHeartbeat() {
            if (state.role() != Role.LEADER) {
                return;
            }
            long currentTerm = state.currentTerm();
            for (String peer : cluster.peersOf(id)) {
                long nextIndex = tracker.nextIndexOf(peer);
                long prevLogIndex = Math.max(0L, nextIndex - 1L);
                long prevLogTerm = state.termAt(prevLogIndex);
                AppendEntriesRequest heartbeat = new AppendEntriesRequest(
                        currentTerm, id, prevLogIndex, prevLogTerm, List.of(), state.commitIndex());
                cluster.network.sendAppend(id, peer, heartbeat, response ->
                        handleAppendResponse(peer, currentTerm, nextIndex, response));
            }
        }

        AppendEntriesResponse handleAppendRequest(AppendEntriesRequest request) {
            try {
                // Delegates directly to TV5 PersistentRaftNode + TV4 AppendEntriesHandler + LogConflictResolver!
                return core.handleAppendEntries(request);
            } catch (RaftStorageException e) {
                throw new RuntimeException(e);
            }
        }

        void handleAppendResponse(String peer, long requestTerm, long sentNextIndex, AppendEntriesResponse response) {
            // TV5-D14: Higher-term response triggers durable step-down via HigherTermHandler
            if (response.term() > state.currentTerm()) {
                try {
                    higherTermHandler.observe(response.term());
                } catch (RaftStorageException e) {
                    throw new RuntimeException(e);
                }
                return;
            }

            // TV5-D14: LeadershipGuard fences stale callbacks after step-down or re-election
            if (!leadershipGuard().isCurrent(requestTerm)) {
                return;
            }

            if (response.success()) {
                tracker.recordSuccess(peer, response.matchIndex());
                electionService.recordPeerAck(peer, response.matchIndex(), Instant.now());

                OptionalLong newCommit = commitAdvancer.advance();
                if (newCommit.isPresent()) {
                    applyCommittedLeader(newCommit.getAsLong());
                }
            } else {
                // TV4 ReplicationTracker CAS backtrack on prevLog mismatch
                if (sentNextIndex > 1L) {
                    long minAllowed = tracker.matchIndexOf(peer) + 1L;
                    long newNext = Math.max(minAllowed, sentNextIndex - 1L);
                    if (newNext < sentNextIndex) {
                        tracker.lowerNextIndex(peer, sentNextIndex, newNext);
                    }
                }
            }
        }

        private void applyCommittedLeader(long throughIndex) {
            while (state.lastApplied() < throughIndex) {
                long next = state.lastApplied() + 1L;
                applyEntry(state.entryAt(next));
            }
        }

        private void applyEntry(RaftLogEntry entry) {
            BookingCommand cmd = deserializeCommand(entry);
            if (cmd != null) {
                BookingStateMachine.ApplyOutcome outcome = stateMachine.apply(entry.index(), cmd);
                if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult br) {
                    appliedEvents.add(br.applyResult());
                    PendingWrite pw = pendingWrites.remove(entry.index());
                    if (pw != null) {
                        leadershipGuard().runIfCurrent(pw.term, () -> pw.future.complete(br.applyResult()));
                    }
                }
            }
            core.onApplied(entry.index());
            storage.lastApplied = entry.index();
            appliedLogIndices.add(entry.index());
        }

        private List<RaftLogEntry> entriesFrom(long nextIndex) {
            long last = state.lastLogIndex();
            if (nextIndex > last) {
                return List.of();
            }
            List<RaftLogEntry> result = new ArrayList<>();
            for (long idx = nextIndex; idx <= last; idx++) {
                result.add(state.entryAt(idx));
            }
            return result;
        }

        private Optional<BookingCommand.BookTicket> findLoggedBooking(String requestId) {
            for (long i = 1; i <= state.lastLogIndex(); i++) {
                BookingCommand cmd = deserializeCommand(state.entryAt(i));
                if (cmd instanceof BookingCommand.BookTicket book && book.requestId().equals(requestId)) {
                    return Optional.of(book);
                }
            }
            return Optional.empty();
        }

        @Override
        public SeatSnapshot getSeats(String tripId) {
            if (!"SE-001".equals(tripId)) {
                throw new IllegalArgumentException("Invalid tripId: " + tripId);
            }
            List<SeatDto> seats = new ArrayList<>(10);
            for (int i = 1; i <= 10; i++) {
                String seatId = String.format("G%02d", i);
                String status = stateMachine.seatStatus(seatId).orElse("AVAILABLE");
                String ticketId = stateMachine.seatCurrentTicketId(seatId).orElse(null);
                seats.add(new SeatDto(seatId, status, ticketId));
            }
            return new SeatSnapshot(tripId, seats, stateMachine.lastApplied(), state.leaderId());
        }

        @Override
        public boolean isLeader() {
            return state.role() == Role.LEADER;
        }

        @Override
        public String getLeaderHint() {
            return state.leaderId();
        }
    }

    private record PendingWrite(
            String requestId,
            long term,
            long logIndex,
            BookingCommand.BookTicket command,
            CompletableFuture<ApplyResult> future
    ) {}

    private static final class SentVoteRpc {
        final String from;
        final String to;
        final RequestVoteRequestDto request;
        final CandidateElection.VoteReply reply;

        SentVoteRpc(String from, String to, RequestVoteRequestDto request, CandidateElection.VoteReply reply) {
            this.from = from;
            this.to = to;
            this.request = request;
            this.reply = reply;
        }
    }

    private static final class SentAppendRpc {
        final String from;
        final String to;
        final AppendEntriesRequest request;
        final Consumer<AppendEntriesResponse> reply;

        SentAppendRpc(String from, String to, AppendEntriesRequest request, Consumer<AppendEntriesResponse> reply) {
            this.from = from;
            this.to = to;
            this.request = request;
            this.reply = reply;
        }
    }

    /** Deterministic FakeNetwork backed directly by TV3-D21 {@link TestNetworkFilter}. */
    private static final class FakeNetwork {
        final Map<String, TestNode> nodes = new LinkedHashMap<>();
        final Deque<SentVoteRpc> pendingVotes = new ArrayDeque<>();
        final Deque<SentAppendRpc> pendingAppends = new ArrayDeque<>();
        final TestNetworkFilter filter;

        FakeNetwork(TestNetworkFilter filter) {
            this.filter = Objects.requireNonNull(filter, "filter");
        }

        void registerNode(TestNode node) {
            nodes.put(node.id, node);
        }

        void partition(Set<String> partA, Set<String> partB) {
            filter.partition(partA, partB);
        }

        void heal() {
            filter.heal();
        }

        boolean canDeliverRequest(String from, String to, MessageType type, Object payload) {
            MessageEnvelope<?> envelope = MessageEnvelope.createRequest(type, from, payload);
            return filter.shouldDeliver(from, to, envelope);
        }

        boolean canDeliverResponse(String from, String to, String correlationId, MessageType type, Object payload) {
            MessageEnvelope<?> envelope = MessageEnvelope.createResponse(correlationId, type, from, payload);
            return filter.shouldDeliver(from, to, envelope);
        }

        void sendVote(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) {
            if (canDeliverRequest(from, to, MessageType.REQUEST_VOTE, req)) {
                pendingVotes.add(new SentVoteRpc(from, to, req, reply));
            }
        }

        void sendAppend(String from, String to, AppendEntriesRequest req, Consumer<AppendEntriesResponse> reply) {
            AppendEntriesRequestDto dto = new AppendEntriesRequestDto(
                    req.term(), req.leaderId(), req.prevLogIndex(), req.prevLogTerm(), List.of(), req.leaderCommit());
            if (canDeliverRequest(from, to, MessageType.APPEND_ENTRIES, dto)) {
                pendingAppends.add(new SentAppendRpc(from, to, req, reply));
            }
        }

        List<SentAppendRpc> drainPendingAppends() {
            List<SentAppendRpc> drained = new ArrayList<>(pendingAppends);
            pendingAppends.clear();
            return drained;
        }

        void deliverVote(String from, String to, long term) throws Exception {
            SentVoteRpc target = pendingVotes.stream()
                    .filter(r -> r.from.equals(from) && r.to.equals(to) && r.request.getTerm() == term)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No pending vote RPC from " + from + " to " + to + " term=" + term));
            pendingVotes.remove(target);
            MessageEnvelope<?> reqEnvelope = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, from, target.request);
            if (filter.shouldDeliver(from, to, reqEnvelope)) {
                RequestVoteResponse resp = nodes.get(to).electionService.handleRequestVote(target.request);
                if (canDeliverResponse(to, from, reqEnvelope.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, resp)) {
                    target.reply.receive(resp);
                }
            }
        }

        void deliverAllVotes() throws Exception {
            while (!pendingVotes.isEmpty()) {
                SentVoteRpc rpc = pendingVotes.removeFirst();
                MessageEnvelope<?> reqEnvelope = MessageEnvelope.createRequest(MessageType.REQUEST_VOTE, rpc.from, rpc.request);
                if (filter.shouldDeliver(rpc.from, rpc.to, reqEnvelope)) {
                    RequestVoteResponse resp = nodes.get(rpc.to).electionService.handleRequestVote(rpc.request);
                    if (canDeliverResponse(rpc.to, rpc.from, reqEnvelope.getMessageId(), MessageType.REQUEST_VOTE_RESPONSE, resp)) {
                        rpc.reply.receive(resp);
                    }
                }
            }
        }

        void deliverAllAppends() {
            while (!pendingAppends.isEmpty()) {
                SentAppendRpc rpc = pendingAppends.removeFirst();
                AppendEntriesRequestDto dto = new AppendEntriesRequestDto(
                        rpc.request.term(), rpc.request.leaderId(), rpc.request.prevLogIndex(),
                        rpc.request.prevLogTerm(), List.of(), rpc.request.leaderCommit());
                MessageEnvelope<?> reqEnvelope = MessageEnvelope.createRequest(MessageType.APPEND_ENTRIES, rpc.from, dto);
                if (filter.shouldDeliver(rpc.from, rpc.to, reqEnvelope)) {
                    AppendEntriesResponse resp = nodes.get(rpc.to).handleAppendRequest(rpc.request);
                    if (canDeliverResponse(rpc.to, rpc.from, reqEnvelope.getMessageId(), MessageType.APPEND_ENTRIES_RESPONSE, resp)) {
                        rpc.reply.accept(resp);
                    }
                }
            }
        }

        void deliverAll() throws Exception {
            boolean progress;
            do {
                progress = false;
                if (!pendingVotes.isEmpty()) {
                    deliverAllVotes();
                    progress = true;
                }
                if (!pendingAppends.isEmpty()) {
                    deliverAllAppends();
                    progress = true;
                }
            } while (progress);
        }
    }

    /** In-memory durable RaftStorage supporting failure injection and call counters. */
    private static final class DurableMemoryStorage implements RaftStorage {
        long term;
        String vote;
        final List<RaftLogEntry> log = new ArrayList<>();
        long lastApplied;
        int replaceSuffixCalls;
        RaftStorageException nextSaveTermFailure;

        void failNextSaveTermAndVote(RaftStorageException failure) {
            this.nextSaveTermFailure = failure;
        }

        @Override
        public PersistentState load() {
            return new PersistentState(term, vote, List.copyOf(log), lastApplied);
        }

        @Override
        public void saveTermAndVote(long newTerm, String newVote) throws RaftStorageException {
            if (nextSaveTermFailure != null) {
                RaftStorageException failure = nextSaveTermFailure;
                nextSaveTermFailure = null;
                throw failure;
            }
            if (newTerm < this.term) {
                throw new RaftStorageException("term regression: " + newTerm + " < " + this.term);
            }
            this.term = newTerm;
            this.vote = newVote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException {
            if (fromIndex < RaftLogEntry.FIRST_INDEX) {
                throw new IllegalArgumentException("fromIndex must be >= 1");
            }
            replaceSuffixCalls++;
            while (log.size() >= fromIndex && !log.isEmpty()) {
                log.remove(log.size() - 1);
            }
            log.addAll(entries);
        }

        @Override
        public long loadLastApplied() {
            return lastApplied;
        }
    }

    /** Deterministic no-thread ScheduledExecutorService for recording ElectionTimer arm/reset calls. */
    private static final class ManualScheduledExecutor implements ScheduledExecutorService {
        private int scheduledCount;
        private boolean shutdown;

        int scheduledCount() {
            return scheduledCount;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduledCount++;
            return new CancelledTrackingFuture();
        }

        @Override public <V> ScheduledFuture<V> schedule(Callable<V> c, long d, TimeUnit u) { throw new UnsupportedOperationException(); }
        @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable c, long i, long p, TimeUnit u) { throw new UnsupportedOperationException(); }
        @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable c, long i, long d, TimeUnit u) { throw new UnsupportedOperationException(); }
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        @Override public <T> Future<T> submit(Callable<T> task) { throw new UnsupportedOperationException(); }
        @Override public <T> Future<T> submit(Runnable task, T result) { throw new UnsupportedOperationException(); }
        @Override public Future<?> submit(Runnable task) { throw new UnsupportedOperationException(); }
        @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) { throw new UnsupportedOperationException(); }
        @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long t, TimeUnit u) { throw new UnsupportedOperationException(); }
        @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) { throw new UnsupportedOperationException(); }
        @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long t, TimeUnit u) { throw new UnsupportedOperationException(); }
        @Override public void execute(Runnable command) { command.run(); }

        private static final class CancelledTrackingFuture implements ScheduledFuture<Void> {
            private boolean cancelled;
            @Override public long getDelay(TimeUnit unit) { return 0; }
            @Override public int compareTo(Delayed o) { return 0; }
            @Override public boolean cancel(boolean mayInterruptIfRunning) { cancelled = true; return true; }
            @Override public boolean isCancelled() { return cancelled; }
            @Override public boolean isDone() { return cancelled; }
            @Override public Void get() { return null; }
            @Override public Void get(long timeout, TimeUnit unit) { return null; }
        }
    }

    private static String serializeCommand(BookingCommand.BookTicket cmd) {
        return "{\"requestId\":\"" + cmd.requestId()
                + "\",\"tripId\":\"" + cmd.tripId()
                + "\",\"seatId\":\"" + cmd.seatId()
                + "\",\"passengerName\":\"" + cmd.passengerName()
                + "\",\"ticketId\":\"" + cmd.ticketId()
                + "\",\"issuedAt\":\"" + cmd.issuedAt() + "\"}";
    }

    private static BookingCommand deserializeCommand(RaftLogEntry entry) {
        if (!RaftLogEntry.COMMAND_TYPE_BOOK_TICKET.equals(entry.commandType())) {
            return null;
        }
        try {
            JsonNode n = MAPPER.readTree(entry.commandPayload());
            return new BookingCommand.BookTicket(
                    n.get("requestId").asText(),
                    n.get("tripId").asText(),
                    n.get("seatId").asText(),
                    n.get("passengerName").asText(),
                    n.get("ticketId").asText(),
                    Instant.parse(n.get("issuedAt").asText())
            );
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot deserialize command", e);
        }
    }
}
