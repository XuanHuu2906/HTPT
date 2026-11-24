package vn.nhom6.railway.raft.election;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.ConnectionFactory;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.networking.HandlerRegistry;
import vn.nhom6.railway.networking.MessageDispatcher;
import vn.nhom6.railway.persistence.DbConfig;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.persistence.DbStateLoader.BootstrapType;
import vn.nhom6.railway.persistence.DbStateLoader.NodeStartupState;
import vn.nhom6.railway.persistence.JdbcBookingRepository;
import vn.nhom6.railway.persistence.JdbcCommittedCommandApplier;
import vn.nhom6.railway.persistence.JdbcRaftStorage;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.PersistentRaftNode;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesRequest;
import vn.nhom6.railway.raft.core.RaftNode.AppendEntriesResponse;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;
import vn.nhom6.railway.raft.replication.LogReplayCoordinator;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D24: Raft Election + Recovery — Term/Vote Recovery &amp; Timer Reset Across Node Restarts.
 *
 * <p><strong>Scope &amp; Integration (TV5-D24, SPEC-04 §2–§3, §6, §8–§10, SPEC-07):</strong>
 * <ul>
 *   <li><strong>Shutdown &amp; Restart of Follower, Candidate, and Leader:</strong> Verifies that
 *       restarting a Node from any Raft role always recovers as {@link Role#FOLLOWER} with
 *       {@code leaderId == null}, empty Leader-volatile indices, stopped old heartbeats, and a
 *       freshly armed {@link ElectionTimer}.</li>
 *   <li><strong>TV2 D13 Persistent Loader ({@link DbStateLoader}):</strong> Validates startup
 *       inspection ({@link BootstrapType#GENESIS} vs {@link BootstrapType#RESTART}), durable
 *       {@code currentTerm}, {@code votedFor}, {@code lastLogIndex}, {@code lastLogTerm},
 *       {@code lastApplied}, {@code initialCommitIndex == lastApplied}, and fail-closed rejection
 *       of corrupt storage/checkpoint mismatches.</li>
 *   <li><strong>TV4 D22 Startup Replay ({@link LogReplayCoordinator}):</strong> Reconciles
 *       {@link RaftState} with the durable {@link BookingRepository} checkpoint via
 *       {@link RaftStateMachineApplier} and {@link CommittedCommandApplier} without deleting or
 *       applying uncommitted log tails, and verifies {@link LogReplayCoordinator#restoreAligned}.</li>
 *   <li><strong>Single-Vote-Per-Term &amp; Up-to-Date Log Rules Across Restarts:</strong>
 *       Guarantees no Node ever votes for two different candidates in the same term across
 *       restarts (including mid-campaign Candidate crashes, ambiguous storage writes, and 5-node
 *       split-vote restarts), never decreases {@code currentTerm}, and never deletes old log
 *       entries on restart.</li>
 *   <li><strong>Honesty Boundary:</strong> Uses in-memory {@link FakeRaftStorage}, in-memory
 *       {@link BookingRepository}, deterministic schedulers/transport, and real
 *       {@link RaftElectionComponent}/{@link PersistentRaftNode}/{@link DbStateLoader}/
 *       {@link LogReplayCoordinator}/{@link BookingStateMachine}. Real OS process kills, TCP
 *       sockets, and PostgreSQL restarts across 5 nodes are verified at Gate G6.</li>
 * </ul>
 */
@DisplayName("TV5-D24: Election Restart Recovery (Follower/Candidate/Leader, TV2-D13 DbStateLoader, TV4-D22 LogReplayCoordinator)")
class ElectionRestartTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Instant BASE_TIME = Instant.parse("2026-11-24T09:00:00Z");

    private static String reqId(int seq) {
        return String.format("%08d-0000-0000-0000-%012d", seq, seq);
    }

    private static RaftLogEntry bookEntry(long index, long term, int seq, String seatId, String passenger) {
        String ticketId = "TKT-" + seq;
        String issuedAt = BASE_TIME.plusSeconds(seq).toString();
        String payload = String.format(
                "{\"issuedAt\":\"%s\",\"passengerName\":\"%s\",\"requestId\":\"%s\",\"seatId\":\"%s\",\"ticketId\":\"%s\",\"tripId\":\"SE-001\"}",
                issuedAt, passenger, reqId(seq), seatId, ticketId);
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);
    }

    private static RaftLogEntry cancelEntry(long index, long term, int seq, String ticketId) {
        String cancelledAt = BASE_TIME.plusSeconds(seq).toString();
        String payload = String.format(
                "{\"cancelledAt\":\"%s\",\"requestId\":\"%s\",\"ticketId\":\"%s\"}",
                cancelledAt, reqId(seq), ticketId);
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_CANCEL_TICKET, payload);
    }

    private static RaftLogEntry noOpEntry(long index, long term) {
        return new RaftLogEntry(index, term, RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Follower Restart — Read term/votedFor (TV2 D13), No Double Vote in Same Term
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Follower restart: đọc term/votedFor qua DbStateLoader, không vote 2 lần trong cùng term, không xóa log cũ")
    void followerRestart_readsTermAndVote_preventsDoubleVoteAndPreservesOldLog() throws Exception {
        List<RaftLogEntry> initialLog = List.of(
                bookEntry(1, 2, 101, "G01", "Alice"),
                bookEntry(2, 3, 102, "G02", "Bob"),
                bookEntry(3, 5, 103, "G03", "Charlie"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(5L, null, initialLog, 2L);
        InMemoryBookingRepository repo = repositoryWithPrefix(initialLog, 2);

        // Boot 1: Follower node-1 grants vote to node-2 in term 5
        RestartableNode boot1 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, boot1.startupState.bootstrapType());
        assertEquals(Role.FOLLOWER, boot1.state.role());
        assertEquals(2L, boot1.state.commitIndex());

        RequestVoteResponse grant = boot1.receiveVote(new RequestVoteRequest(5L, "node-2", 3L, 5L));
        assertTrue(grant.voteGranted());
        assertEquals(5L, grant.term());
        assertEquals("node-2", storage.load().votedFor());

        // Shutdown boot 1 -> election timer cancelled cleanly
        boot1.shutdown();
        assertTrue(boot1.scheduler.allDeadlinesCancelled(), "Shutdown phải hủy toàn bộ election timer cũ");

        int replaceCallsBeforeRestart = storage.replaceSuffixCalls();

        // Boot 2: Restart Follower from durable storage + TV2 D13 DbStateLoader + TV4 D22 replay
        RestartableNode boot2 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, boot2.startupState.bootstrapType());
        assertEquals(5L, boot2.startupState.currentTerm());
        assertEquals("node-2", boot2.startupState.votedFor());
        assertEquals(3L, boot2.startupState.lastLogIndex());
        assertEquals(5L, boot2.startupState.lastLogTerm());
        assertEquals(2L, boot2.startupState.lastApplied());
        assertEquals(2L, boot2.startupState.initialCommitIndex(),
                "commitIndex sau restart phải lấy từ lastApplied, không suy ra từ lastLogIndex");

        assertEquals(Role.FOLLOWER, boot2.state.role());
        assertNull(boot2.state.leaderId());
        assertEquals(5L, boot2.state.currentTerm());
        assertEquals("node-2", boot2.state.votedFor());
        assertEquals(2L, boot2.state.commitIndex());
        assertEquals(2L, boot2.state.lastApplied());
        assertEquals(initialLog, boot2.state.entries(), "Restart không được xóa log cũ");
        assertEquals(initialLog, storage.storedLog());
        assertEquals(replaceCallsBeforeRestart, storage.replaceSuffixCalls(), "Restart không được gọi replaceSuffix");
        assertEquals(1, boot2.scheduler.scheduledCount(), "Restart phải arm election timer mới đúng 1 lần");
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, boot2.replayReport.outcome());

        // Different candidate in same term 5 -> MUST REJECT, timer MUST NOT reset
        int timerBeforeDeny = boot2.scheduler.scheduledCount();
        RequestVoteResponse denyOther = boot2.receiveVote(new RequestVoteRequest(5L, "node-3", 3L, 5L));
        assertFalse(denyOther.voteGranted(), "Không được vote cho candidate thứ hai trong cùng term 5 sau restart");
        assertEquals(5L, denyOther.term());
        assertEquals("node-2", boot2.state.votedFor());
        assertEquals("node-2", storage.load().votedFor());
        assertEquals(timerBeforeDeny, boot2.scheduler.scheduledCount(),
                "Từ chối vote không được reset election timer (SPEC-04 §3.1)");

        // Stale term 4 -> MUST REJECT, term never decreases, timer MUST NOT reset
        RequestVoteResponse denyStale = boot2.receiveVote(new RequestVoteRequest(4L, "node-4", 10L, 4L));
        assertFalse(denyStale.voteGranted());
        assertEquals(5L, denyStale.term());
        assertEquals(5L, boot2.state.currentTerm());
        assertEquals(timerBeforeDeny, boot2.scheduler.scheduledCount());

        // Same candidate retry in term 5 -> idempotent grant + resets timer, no duplicate storage write
        int storageCallsBeforeRetry = storage.calls().size();
        RequestVoteResponse retrySame = boot2.receiveVote(new RequestVoteRequest(5L, "node-2", 3L, 5L));
        assertTrue(retrySame.voteGranted(), "Retry cùng candidate đã bầu ở term 5 phải được đồng ý idempotent");
        assertEquals(5L, retrySame.term());
        assertEquals(storageCallsBeforeRetry, storage.calls().size(), "Retry không đổi term/vote không cần ghi lại storage");
        assertEquals(timerBeforeDeny + 1, boot2.scheduler.scheduledCount(), "Vote được cấp phải reset election timer");

        boot2.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Candidate Restart — Mid-Campaign Crash -> Restarts as Follower, Keeps Self-Vote
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Candidate restart: tắt giữa chiến dịch -> về Follower giữ votedFor=self, chặn vote cùng term, timeout mới lên term+1")
    void candidateRestart_restartsAsFollower_keepsSelfVoteAndStartsFreshTermOnNextTimeout() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 1, 201, "G01", "Alice"),
                bookEntry(2, 3, 202, "G02", "Bob"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, null, log, 2L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 2);

        // Boot 1: node-1 times out and becomes CANDIDATE in term 4, persists (4, "node-1")
        RestartableNode boot1 = RestartableNode.boot("node-1", storage, repo);
        boot1.scheduler.fireNextDeadline();

        assertEquals(Role.CANDIDATE, boot1.state.role());
        assertEquals(4L, boot1.state.currentTerm());
        assertEquals("node-1", boot1.state.votedFor());
        assertEquals(4L, storage.load().currentTerm());
        assertEquals("node-1", storage.load().votedFor());
        assertEquals(4, boot1.outboundVotes.size(), "Candidate gửi REQUEST_VOTE tới 4 peer");

        // Receives only 1 peer vote (total 2/5 — still CANDIDATE, not yet Leader)
        boot1.outboundVotes.getFirst().reply().receive(new RequestVoteResponse(4L, true));
        assertEquals(Role.CANDIDATE, boot1.state.role());

        // Crash Candidate mid-campaign!
        boot1.shutdown();
        assertTrue(boot1.scheduler.allElectionDeadlinesCancelled(), "Shutdown phải hủy mọi election deadline còn chờ");
        assertTrue(boot1.scheduler.tasks.getLast().isCancelled(), "Retry deadline đang chờ của Candidate phải bị hủy khi shutdown");

        // Boot 2: Restart from durable storage
        RestartableNode boot2 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, boot2.startupState.bootstrapType());
        assertEquals(4L, boot2.startupState.currentTerm());
        assertEquals("node-1", boot2.startupState.votedFor());
        assertEquals(Role.FOLLOWER, boot2.state.role(), "Candidate sau restart phải khởi động lại ở role FOLLOWER");
        assertEquals(4L, boot2.state.currentTerm());
        assertEquals("node-1", boot2.state.votedFor());
        assertEquals(log, boot2.state.entries(), "Candidate restart không được xóa log cũ");
        assertEquals(0, storage.replaceSuffixCalls());
        assertEquals(1, boot2.scheduler.scheduledCount(), "Election timer phải được arm sạch sau restart");

        // Another candidate in term 4 with even longer log MUST be denied (already self-voted in term 4)
        int timerBeforeDeny = boot2.scheduler.scheduledCount();
        RequestVoteResponse denyTerm4 = boot2.receiveVote(new RequestVoteRequest(4L, "node-3", 5L, 4L));
        assertFalse(denyTerm4.voteGranted(), "Candidate sau restart đã self-vote ở term 4 nên phải từ chối node-3");
        assertEquals(4L, denyTerm4.term());
        assertEquals("node-1", boot2.state.votedFor());
        assertEquals(timerBeforeDeny, boot2.scheduler.scheduledCount());

        // Firing the new election timer starts a fresh campaign at term 5 (monotonic term increment)
        boot2.scheduler.fireNextDeadline();
        assertEquals(Role.CANDIDATE, boot2.state.role());
        assertEquals(5L, boot2.state.currentTerm());
        assertEquals("node-1", boot2.state.votedFor());
        assertEquals(5L, storage.load().currentTerm());
        assertEquals("node-1", storage.load().votedFor());
        assertEquals(4, boot2.outboundVotes.size());
        assertTrue(boot2.outboundVotes.stream().allMatch(r -> r.dto().getTerm() == 5L
                && r.dto().getLastLogIndex() == 2L && r.dto().getLastLogTerm() == 3L));

        boot2.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Leader Restart — Drops Leadership, Stops Heartbeats, Preserves Committed + Uncommitted Log
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Leader restart: tắt Leader -> về Follower, xóa volatile Leader state, bật lại election timer, giữ nguyên log cũ")
    void leaderRestart_losesLeadership_armsElectionTimer_preservesCommittedAndUncommittedLog() throws Exception {
        List<RaftLogEntry> committedPrefix = List.of(
                bookEntry(1, 1, 301, "G01", "Alice"),
                bookEntry(2, 1, 302, "G02", "Bob"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, "node-1", committedPrefix, 2L);
        InMemoryBookingRepository repo = repositoryWithPrefix(committedPrefix, 2);

        // Boot 1: node-1 campaigns and wins leadership in term 2
        RestartableNode boot1 = RestartableNode.boot("node-1", storage, repo);
        boot1.scheduler.fireNextDeadline(); // term 2, CANDIDATE
        assertEquals(2L, boot1.state.currentTerm());
        boot1.outboundVotes.get(0).reply().receive(new RequestVoteResponse(2L, true));
        boot1.outboundVotes.get(1).reply().receive(new RequestVoteResponse(2L, true));
        assertEquals(Role.LEADER, boot1.state.role());
        assertEquals(4, boot1.state.nextIndex().size());
        assertEquals(4, boot1.state.matchIndex().size());
        assertTrue(boot1.heartbeatSentCount > 0, "Leader phải phát heartbeat khi đắc cử");
        assertTrue(boot1.scheduler.allElectionDeadlinesCancelled(), "Election timer phải dừng khi là Leader");

        // Leader appends a 3rd entry locally (uncommitted tail, not yet replicated/applied)
        RaftLogEntry uncommitted = bookEntry(3, 2, 303, "G03", "Uncommitted Charlie");
        RaftNode.AppendOutcome appendOutcome = boot1.core.appendCommand(
                uncommitted.commandType(), uncommitted.commandPayload());
        RaftNode.Accepted accepted = assertInstanceOf(RaftNode.Accepted.class, appendOutcome);
        assertEquals(3L, accepted.index());
        assertEquals(2L, accepted.term());
        assertEquals(3L, boot1.state.lastLogIndex());
        assertEquals(2L, boot1.state.commitIndex());
        assertEquals(2L, boot1.state.lastApplied());

        // Leader crashes / shuts down!
        boot1.shutdown();
        int heartbeatsAtShutdown = boot1.heartbeatSentCount;
        assertTrue(boot1.heartbeatScheduler.allDeadlinesCancelled(), "Heartbeat scheduler phải dừng khi tắt Leader");
        assertEquals(heartbeatsAtShutdown, boot1.heartbeatSentCount);

        // Boot 2: Restart former Leader from the exact same storage + repo
        RestartableNode boot2 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, boot2.startupState.bootstrapType());
        assertEquals(2L, boot2.startupState.currentTerm());
        assertEquals("node-1", boot2.startupState.votedFor());
        assertEquals(3L, boot2.startupState.lastLogIndex());
        assertEquals(2L, boot2.startupState.lastLogTerm());
        assertEquals(2L, boot2.startupState.lastApplied());
        assertEquals(2L, boot2.startupState.initialCommitIndex());

        // Must be FOLLOWER with cleared Leader volatile state & freshly armed election timer
        assertEquals(Role.FOLLOWER, boot2.state.role(), "Leader cũ sau restart phải khởi động là FOLLOWER");
        assertNull(boot2.state.leaderId());
        assertTrue(boot2.state.nextIndex().isEmpty(), "nextIndex phải rỗng sau restart");
        assertTrue(boot2.state.matchIndex().isEmpty(), "matchIndex phải rỗng sau restart");
        assertEquals(0, boot2.heartbeatSentCount, "Node mới restart không được tự phát heartbeat");
        assertEquals(1, boot2.scheduler.scheduledCount(), "Election timer phải được arm lại sau khi Leader restart");
        assertFalse(boot2.scheduler.allDeadlinesCancelled(), "Deadline bầu cử phải đang hoạt động");

        // Former leader rejects client writes because it is now FOLLOWER
        RaftNode.AppendOutcome writeAfterRestart = boot2.core.appendCommand(
                RaftLogEntry.COMMAND_TYPE_NO_OP, "{}");
        assertInstanceOf(RaftNode.NotLeader.class, writeAfterRestart);

        // Both committed prefix [1..2] and uncommitted tail [3] are preserved untouched!
        assertEquals(3L, boot2.state.lastLogIndex());
        assertEquals(2L, boot2.state.commitIndex());
        assertEquals(2L, boot2.state.lastApplied());
        assertEquals(uncommitted, boot2.state.entryAt(3));
        assertEquals(committedPrefix.get(0), boot2.state.entryAt(1));
        assertEquals(committedPrefix.get(1), boot2.state.entryAt(2));
        assertEquals("AVAILABLE", repo.readSeatSnapshot().get("G03"), "Uncommitted tail không được tự apply khi restart");

        // Vote safety in term 2: rejects another candidate because votedFor == "node-1"
        assertFalse(boot2.receiveVote(new RequestVoteRequest(2L, "node-2", 3L, 2L)).voteGranted());
        // Higher term 3 with up-to-date log: clears old vote, grants vote, resets election timer
        int timersBeforeTerm3 = boot2.scheduler.scheduledCount();
        RequestVoteResponse grantTerm3 = boot2.receiveVote(new RequestVoteRequest(3L, "node-3", 3L, 2L));
        assertTrue(grantTerm3.voteGranted());
        assertEquals(3L, grantTerm3.term());
        assertEquals("node-3", storage.load().votedFor());
        assertEquals(timersBeforeTerm3 + 1, boot2.scheduler.scheduledCount());

        boot2.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Election Timer Reset Rules After Restart (SPEC-04 §3.1)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Timer reset sau restart: chỉ reset khi AppendEntries hợp lệ (kể cả mismatch) hoặc grantVote; không reset khi stale/denied")
    void electionTimerResetRules_afterRestart_followSpec04Section31() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 3, 401, "G01", "Alice"),
                bookEntry(2, 5, 402, "G02", "Bob"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(5L, null, log, 2L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 2);

        RestartableNode node = RestartableNode.boot("node-1", storage, repo);
        assertEquals(1, node.scheduler.scheduledCount(), "start() arm đúng 1 deadline ban đầu");

        // Repeated start() is idempotent and must NOT postpone deadline
        node.timer.start();
        assertEquals(1, node.scheduler.scheduledCount(), "Gọi timer.start() lặp lại không được đẩy lùi deadline");

        // 1. Stale AppendEntries (term 4 < 5) -> rejected, NO timer reset
        AppendEntriesResponse staleAppend = node.receiveAppend(
                new AppendEntriesRequest(4L, "node-2", 2L, 5L, List.of(), 2L));
        assertFalse(staleAppend.success());
        assertEquals(5L, staleAppend.term());
        assertEquals(1, node.scheduler.scheduledCount(), "AppendEntries term cũ không được reset timer");

        // 2. Denied RequestVote (higher term 6, but candidate log is stale: lastLogTerm=4 < 5)
        //    Persists (6, null) but MUST NOT reset timer!
        RequestVoteResponse deniedHigherTerm = node.receiveVote(
                new RequestVoteRequest(6L, "node-2", 2L, 4L));
        assertFalse(deniedHigherTerm.voteGranted());
        assertEquals(6L, deniedHigherTerm.term());
        assertEquals(6L, storage.load().currentTerm());
        assertNull(storage.load().votedFor());
        assertEquals(1, node.scheduler.scheduledCount(),
                "RequestVote bị từ chối (kể cả khi tăng term) không được reset timer");

        // 3. Granted RequestVote at term 6 -> persists (6, "node-3") BEFORE resetting timer
        AtomicInteger persistObservedBeforeArm = new AtomicInteger();
        node.scheduler.beforeSchedule = () -> {
            try {
                PersistentState snap = storage.load();
                if (snap.currentTerm() == 6L && "node-3".equals(snap.votedFor())) {
                    persistObservedBeforeArm.incrementAndGet();
                }
            } catch (RaftStorageException e) {
                throw new RuntimeException(e);
            }
        };
        RequestVoteResponse grantedTerm6 = node.receiveVote(
                new RequestVoteRequest(6L, "node-3", 2L, 5L));
        node.scheduler.beforeSchedule = () -> {};
        assertTrue(grantedTerm6.voteGranted());
        assertEquals(1, persistObservedBeforeArm.get(), "Phải persist term/vote trước khi reset election timer");
        assertEquals(2, node.scheduler.scheduledCount(), "Vote được cấp phải reset timer");
        assertTrue(node.scheduler.tasks.get(0).isCancelled(), "Deadline cũ phải bị hủy khi reset");

        // 4. Valid AppendEntries at same term 6 with log mismatch (prevLogIndex=5 > lastLogIndex=2)
        //    MUST still reset timer (SPEC-04 §3.1)
        AppendEntriesResponse mismatchAppend = node.receiveAppend(
                new AppendEntriesRequest(6L, "node-3", 5L, 6L, List.of(), 2L));
        assertFalse(mismatchAppend.success());
        assertEquals(6L, mismatchAppend.term());
        assertEquals("node-3", node.state.leaderId());
        assertEquals(3, node.scheduler.scheduledCount(),
                "AppendEntries hợp lệ term không cũ phải reset timer kể cả khi prevLog mismatch");
        assertTrue(node.scheduler.tasks.get(1).isCancelled());

        // 5. Higher-term AppendEntries (term 7) -> persists (7, null), updates leaderId, resets timer
        AppendEntriesResponse higherAppend = node.receiveAppend(
                new AppendEntriesRequest(7L, "node-4", 2L, 5L, List.of(), 2L));
        assertTrue(higherAppend.success());
        assertEquals(7L, higherAppend.term());
        assertEquals(7L, storage.load().currentTerm());
        assertNull(storage.load().votedFor());
        assertEquals("node-4", node.state.leaderId());
        assertEquals(4, node.scheduler.scheduledCount());
        assertTrue(node.scheduler.tasks.get(2).isCancelled());

        node.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: TC-RAFT-02 — Up-to-Date Log Vote Rule Preserved After Restart (Old Log Not Deleted)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. TC-RAFT-02: Sau restart không xóa log cũ -> vẫn từ chối candidate có log cũ hơn theo luật (lastLogTerm, lastLogIndex)")
    void restartPreservesLog_upToDateVoteRuleStillRejectsStaleCandidates() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 1, 501, "G01", "Alice"),
                bookEntry(2, 3, 502, "G02", "Bob"),
                bookEntry(3, 3, 503, "G03", "Charlie"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(4L, null, log, 1L); // index 2..3 uncommitted, still part of log freshness!
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 1);

        RestartableNode node = RestartableNode.boot("node-1", storage, repo);
        assertEquals(3L, node.state.lastLogIndex());
        assertEquals(3L, node.state.lastLogTerm());
        assertEquals(1L, node.state.commitIndex());

        // Candidate with LOWER lastLogTerm (2 < 3) even with longer log (index 10) -> DENIED
        RequestVoteResponse lowerTermLog = node.receiveVote(new RequestVoteRequest(5L, "node-2", 10L, 2L));
        assertFalse(lowerTermLog.voteGranted(), "Candidate có lastLogTerm thấp hơn phải bị từ chối");
        assertEquals(5L, lowerTermLog.term());
        assertNull(storage.load().votedFor(), "Bị từ chối vì log cũ vẫn persist (term=5, votedFor=null)");

        // Candidate with SAME lastLogTerm (3) but SHORTER log (2 < 3) -> DENIED
        RequestVoteResponse shorterLog = node.receiveVote(new RequestVoteRequest(5L, "node-3", 2L, 3L));
        assertFalse(shorterLog.voteGranted(), "Candidate cùng lastLogTerm nhưng ngắn hơn phải bị từ chối");
        assertNull(storage.load().votedFor());

        // Candidate with EQUAL (lastLogTerm=3, lastLogIndex=3) -> GRANTED
        RequestVoteResponse upToDate = node.receiveVote(new RequestVoteRequest(5L, "node-4", 3L, 3L));
        assertTrue(upToDate.voteGranted(), "Candidate có log mới bằng phải được cấp phiếu");
        assertEquals("node-4", storage.load().votedFor());

        // Verify uncommitted log entries were never deleted across the entire restart + vote sequence
        assertEquals(log, node.state.entries());
        assertEquals(log, storage.storedLog());
        assertEquals(0, storage.replaceSuffixCalls());

        node.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Multi-Restart Cycle — Monotonic Term, Higher Term Clears Old Vote, Single Vote Per Term
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Nhiều chu kỳ restart liên tiếp: term đơn điệu tăng, term mới xóa votedFor cũ, mỗi term tối đa 1 phiếu")
    void multipleRestartCycles_monotonicTermAndSingleVotePerTerm() throws Exception {
        List<RaftLogEntry> log = List.of(bookEntry(1, 1, 601, "G01", "Alice"));
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(2L, null, log, 1L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 1);

        // Cycle 1: vote node-2 at term 2 -> shutdown
        RestartableNode b1 = RestartableNode.boot("node-1", storage, repo);
        assertTrue(b1.receiveVote(new RequestVoteRequest(2L, "node-2", 1L, 1L)).voteGranted());
        b1.shutdown();

        // Cycle 2: restart -> rejects node-3 at term 2 -> grants node-3 at term 3 -> shutdown
        RestartableNode b2 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(2L, b2.state.currentTerm());
        assertEquals("node-2", b2.state.votedFor());
        assertFalse(b2.receiveVote(new RequestVoteRequest(2L, "node-3", 1L, 1L)).voteGranted());
        assertTrue(b2.receiveVote(new RequestVoteRequest(3L, "node-3", 1L, 1L)).voteGranted());
        assertEquals(3L, storage.load().currentTerm());
        assertEquals("node-3", storage.load().votedFor());
        b2.shutdown();

        // Cycle 3: restart -> rejects node-4 at term 3 -> receives term-4 heartbeat from node-5 (clears vote) -> shutdown
        RestartableNode b3 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(3L, b3.state.currentTerm());
        assertEquals("node-3", b3.state.votedFor());
        assertFalse(b3.receiveVote(new RequestVoteRequest(3L, "node-4", 1L, 1L)).voteGranted());
        assertFalse(b3.receiveVote(new RequestVoteRequest(2L, "node-2", 1L, 1L)).voteGranted());
        AppendEntriesResponse hb4 = b3.receiveAppend(new AppendEntriesRequest(4L, "node-5", 1L, 1L, List.of(), 1L));
        assertTrue(hb4.success());
        assertEquals(4L, storage.load().currentTerm());
        assertNull(storage.load().votedFor());
        b3.shutdown();

        // Cycle 4: restart at (term=4, votedFor=null) -> can vote once in term 4 -> rejects second candidate in term 4
        RestartableNode b4 = RestartableNode.boot("node-1", storage, repo);
        assertEquals(4L, b4.state.currentTerm());
        assertNull(b4.state.votedFor());
        assertTrue(b4.receiveVote(new RequestVoteRequest(4L, "node-4", 1L, 1L)).voteGranted());
        assertFalse(b4.receiveVote(new RequestVoteRequest(4L, "node-5", 1L, 1L)).voteGranted());
        assertEquals(log, b4.state.entries(), "Log gốc phải còn nguyên sau 4 chu kỳ restart");
        assertEquals(0, storage.replaceSuffixCalls());
        b4.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: TC-RAFT-09 — TV2 D13 DbStateLoader + TV4 D22 LogReplayCoordinator on Clean Restart
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. TC-RAFT-09: TV2 D13 DbStateLoader + TV4 D22 LogReplayCoordinator giữ nguyên uncommitted tail và apply tiếp khi Leader báo commit")
    void dbStateLoaderAndLogReplayCoordinator_cleanRestartLeavesUncommittedTailUntilLeaderCommits() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 1, 701, "G01", "Alice"),
                noOpEntry(2, 1),
                bookEntry(3, 2, 702, "G02", "Bob"),
                cancelEntry(4, 2, 703, "TKT-701"),
                bookEntry(5, 3, 704, "G03", "Charlie"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, "node-2", log, 3L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 3);
        int applyCallsBeforeRestart = repo.applyCalls();

        RestartableNode node = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, node.startupState.bootstrapType());
        assertEquals(3L, node.startupState.currentTerm());
        assertEquals("node-2", node.startupState.votedFor());
        assertEquals(3L, node.startupState.lastApplied());
        assertEquals(3L, node.startupState.initialCommitIndex());
        assertEquals(5L, node.startupState.lastLogIndex());

        // TV4 D22 replay report: clean restart -> nothing replayed, 2 uncommitted entries preserved
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, node.replayReport.outcome());
        assertEquals(3L, node.replayReport.commitFloor());
        assertEquals(2L, node.replayReport.uncommittedSuffixLength());
        assertEquals(applyCallsBeforeRestart, repo.applyCalls(), "Không được apply lại prefix 1..3");
        assertEquals("BOOKED", repo.readSeatSnapshot().get("G01"));
        assertEquals("BOOKED", repo.readSeatSnapshot().get("G02"));
        assertEquals("AVAILABLE", repo.readSeatSnapshot().get("G03"), "Uncommitted entry 5 chưa được apply");
        assertEquals(log, node.state.entries(), "Uncommitted entries 4..5 vẫn giữ nguyên trong log");

        // Leader node-2 at term 3 later confirms commitIndex=5 via heartbeat AppendEntries
        AppendEntriesResponse commitAdvance = node.receiveAppend(
                new AppendEntriesRequest(3L, "node-2", 5L, 3L, List.of(), 5L));
        assertTrue(commitAdvance.success());
        assertEquals(5L, node.state.commitIndex());
        assertEquals(5L, node.state.lastApplied());
        assertEquals(5L, repo.lastApplied());
        assertEquals(applyCallsBeforeRestart + 2, repo.applyCalls(), "Chỉ apply thêm đúng 2 entry 4 và 5");
        assertEquals("AVAILABLE", repo.readSeatSnapshot().get("G01"), "Entry 4 hủy vé G01 thành công");
        assertEquals("BOOKED", repo.readSeatSnapshot().get("G03"), "Entry 5 đặt G03 thành công");

        node.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: TC-RAFT-05 / TC-RAFT-09 — Crash Mid-Apply Replay + Election Vote Safety
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. TC-RAFT-05: Crash sau durable DB apply trước Raft markApplied -> LogReplayCoordinator redeliver idempotent và giữ nguyên luật phiếu")
    void crashMidApply_logReplayCoordinatorReconcilesIdempotentlyAndPreservesElectionSafety() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 1, 801, "G01", "Alice"),
                bookEntry(2, 2, 802, "G02", "Bob"),
                noOpEntry(3, 2),
                bookEntry(4, 3, 803, "G03", "Charlie"),
                bookEntry(5, 4, 804, "G04", "Uncommitted Dave"),
                bookEntry(6, 4, 805, "G05", "Uncommitted Eve"));

        // Durable DB already applied index 4, but Raft storage checkpoint lagged at 3
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(4L, "node-2", log, 3L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 4);
        Map<String, String> seatsBeforeRestart = new TreeMap<>(repo.readSeatSnapshot());
        int applyCallsBeforeRestart = repo.applyCalls();

        // DbStateLoader with storage+repo detects the checkpoint discrepancy fail-closed
        DbStateLoader strictLoader = new DbStateLoader(storage, repo, (DbStateLoader.ConnectionSupplier) null);
        BookingRepository.StorageException mismatch = assertThrows(
                BookingRepository.StorageException.class, strictLoader::load);
        assertTrue(mismatch.getMessage().contains("Checkpoint mismatch"));

        // Storage-only loader succeeds, then TV4 D22 LogReplayCoordinator reconciles [4..4] idempotently
        RestartableNode node = RestartableNode.bootAllowingDurableAhead("node-1", storage, repo);
        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, node.replayReport.outcome());
        assertEquals(4L, node.replayReport.commitFloor());
        assertEquals(4L, node.replayReport.replayedFrom());
        assertEquals(4L, node.replayReport.replayedTo());
        assertEquals(1, node.replayReport.redelivered());
        assertEquals(2L, node.replayReport.uncommittedSuffixLength());
        assertEquals(4L, node.state.commitIndex());
        assertEquals(4L, node.state.lastApplied());
        assertEquals(applyCallsBeforeRestart, repo.applyCalls(),
                "Redelivery index 4 đã durable không được gọi applyAtomically lần 2");
        assertTrue(node.appliedBusinessResults.isEmpty(),
                "Redelivery không được phát sự kiện nghiệp vụ trùng lặp");
        assertEquals(seatsBeforeRestart, new TreeMap<>(repo.readSeatSnapshot()));
        assertEquals(log, node.state.entries(), "Uncommitted suffix [5, 6] không bị xóa");

        // Election vote safety still intact after replay
        assertEquals(4L, node.state.currentTerm());
        assertEquals("node-2", node.state.votedFor());
        assertFalse(node.receiveVote(new RequestVoteRequest(4L, "node-3", 6L, 4L)).voteGranted());
        assertTrue(node.receiveVote(new RequestVoteRequest(4L, "node-2", 6L, 4L)).voteGranted());
        // Candidate in term 5 with log shorter than 6 is still rejected because uncommitted tail was preserved!
        assertFalse(node.receiveVote(new RequestVoteRequest(5L, "node-4", 5L, 4L)).voteGranted());

        node.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 9: TV4 D22 restoreAligned When Raft Checkpoint Ahead of DB + Vote Preservation
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Raft checkpoint vượt durable DB -> fail-closed, phục hồi bằng LogReplayCoordinator.restoreAligned giữ nguyên term/vote/log")
    void raftAheadOfDurableDb_failsClosedThenRestoresAlignedWithoutLosingVoteOrLog() throws Exception {
        List<RaftLogEntry> log = List.of(
                bookEntry(1, 1, 901, "G01", "Alice"),
                bookEntry(2, 2, 902, "G02", "Bob"),
                bookEntry(3, 3, 903, "G03", "Charlie"),
                bookEntry(4, 3, 904, "G04", "Dave"));

        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(3L, "node-5", log, 4L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 2);

        // 1. Unaligned restore fails closed inside LogReplayCoordinator
        PersistentState persisted = storage.load();
        RaftState unaligned = RaftState.restore(
                persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
        CommittedCommandApplier unalignedSink = new CommittedCommandApplier(repo);
        RaftStateMachineApplier unalignedApplier = new RaftStateMachineApplier(
                unaligned, RaftApplyAdapter::decodeCommand, unalignedSink.commitListener());
        LogReplayCoordinator brokenCoordinator = new LogReplayCoordinator(
                unaligned, unalignedApplier, repo::lastApplied);
        LogReplayCoordinator.ReplayReport failedReport = brokenCoordinator.replayOnStartup();
        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, failedReport.outcome());
        assertTrue(brokenCoordinator.failed());

        // 2. Recover with TV4 D22 LogReplayCoordinator.restoreAligned(persisted, durable=2)
        RaftState aligned = LogReplayCoordinator.restoreAligned(persisted, repo.lastApplied());
        assertEquals(Role.FOLLOWER, aligned.role());
        assertEquals(3L, aligned.currentTerm());
        assertEquals("node-5", aligned.votedFor());
        assertEquals(2L, aligned.lastApplied());
        assertEquals(2L, aligned.commitIndex());
        assertEquals(log, aligned.entries(), "restoreAligned không được xóa log cũ");

        // Align durable storage checkpoint so PersistentRaftNode matches aligned state
        storage.seedLastApplied(2L);
        RestartableNode recovered = RestartableNode.bootFromState("node-1", aligned, storage, repo);
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, recovered.replayReport.outcome());

        // Vote rule preserved at term 3
        assertFalse(recovered.receiveVote(new RequestVoteRequest(3L, "node-2", 4L, 3L)).voteGranted());
        assertTrue(recovered.receiveVote(new RequestVoteRequest(3L, "node-5", 4L, 3L)).voteGranted());

        // Leader confirms commitIndex=4 -> entries 3 and 4 applied exactly once
        AppendEntriesResponse committed = recovered.receiveAppend(
                new AppendEntriesRequest(3L, "node-5", 4L, 3L, List.of(), 4L));
        assertTrue(committed.success());
        assertEquals(4L, recovered.state.lastApplied());
        assertEquals(4L, repo.lastApplied());
        assertEquals("BOOKED", repo.readSeatSnapshot().get("G03"));
        assertEquals("BOOKED", repo.readSeatSnapshot().get("G04"));

        recovered.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 10: Fail-Closed Validation & Ambiguous Write Recovery Across Restart
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Fail-closed khi storage hỏng (checkpoint > log, gap, invalid voter) và bảo toàn vote sau ambiguous write crash")
    void failClosedOnCorruptStorage_andAmbiguousWritePreservesVoteOnRestart() throws Exception {
        // 10a. Checkpoint beyond log -> DbStateLoader & RaftState.restore fail closed
        FakeRaftStorage beyondLog = new FakeRaftStorage();
        beyondLog.seedState(3L, "node-2", List.of(bookEntry(1, 2, 1001, "G01", "Alice")), 2L);
        assertThrows(BookingRepository.StorageException.class,
                () -> new DbStateLoader(beyondLog, null, (DbStateLoader.ConnectionSupplier) null).load());
        assertThrows(IllegalArgumentException.class,
                () -> RaftState.restore(3L, "node-2", beyondLog.storedLog(), 2L));

        // 10b. Log gap (index 1, 3 missing 2) -> DbStateLoader & RaftState.restore fail closed
        FakeRaftStorage gappedLog = new FakeRaftStorage();
        gappedLog.seedState(3L, null, List.of(
                bookEntry(1, 1, 1002, "G01", "Alice"),
                bookEntry(3, 2, 1003, "G03", "Charlie")), 0L);
        assertThrows(BookingRepository.StorageException.class,
                () -> new DbStateLoader(gappedLog, null, (DbStateLoader.ConnectionSupplier) null).load());
        assertThrows(IllegalArgumentException.class,
                () -> RaftState.restore(3L, null, gappedLog.storedLog(), 0L));

        // 10c. Entry term exceeds currentTerm -> DbStateLoader & RaftState.restore fail closed
        FakeRaftStorage badTermLog = new FakeRaftStorage();
        badTermLog.seedState(2L, null, List.of(bookEntry(1, 5, 1004, "G01", "Alice")), 0L);
        assertThrows(BookingRepository.StorageException.class,
                () -> new DbStateLoader(badTermLog, null, (DbStateLoader.ConnectionSupplier) null).load());
        assertThrows(IllegalArgumentException.class,
                () -> RaftState.restore(2L, null, badTermLog.storedLog(), 0L));

        // 10d. Invalid non-voter in votedFor -> DbStateLoader fails closed
        FakeRaftStorage invalidVoter = new FakeRaftStorage();
        invalidVoter.seedState(2L, "monitor-1", List.of(), 0L);
        assertThrows(BookingRepository.StorageException.class,
                () -> new DbStateLoader(invalidVoter, null, (DbStateLoader.ConnectionSupplier) null).load());

        // 10e. Ambiguous write crash: saveTermAndVote(5, "node-2") wrote to disk then threw IO error
        List<RaftLogEntry> log = List.of(bookEntry(1, 3, 1005, "G01", "Alice"));
        FakeRaftStorage ambiguousStorage = new FakeRaftStorage();
        ambiguousStorage.seedState(5L, null, log, 1L);
        InMemoryBookingRepository repo = repositoryWithPrefix(log, 1);

        RestartableNode crashed = RestartableNode.boot("node-1", ambiguousStorage, repo);
        ambiguousStorage.persistTermAndVoteThenFailNext();
        assertThrows(RaftStorageException.class,
                () -> crashed.receiveVote(new RequestVoteRequest(5L, "node-2", 1L, 3L)));
        // Old instance is poisoned (fail-closed)
        assertThrows(IllegalStateException.class,
                () -> crashed.receiveVote(new RequestVoteRequest(5L, "node-3", 1L, 3L)));
        crashed.shutdown();

        // Restart reads the vote that actually landed on disk -> MUST reject node-3 in term 5!
        RestartableNode recovered = RestartableNode.boot("node-1", ambiguousStorage, repo);
        assertEquals(5L, recovered.startupState.currentTerm());
        assertEquals("node-2", recovered.startupState.votedFor());
        assertFalse(recovered.receiveVote(new RequestVoteRequest(5L, "node-3", 1L, 3L)).voteGranted(),
                "Sau ambiguous write crash, restart phải tôn trọng vote đã xuống disk");
        assertTrue(recovered.receiveVote(new RequestVoteRequest(5L, "node-2", 1L, 3L)).voteGranted());
        assertEquals(log, recovered.state.entries());
        recovered.shutdown();
    }

    // -------------------------------------------------------------------------------------------
    // Test 11: Real RaftElectionComponent + PersistentRaftNode + LeaderHeartbeatScheduler Restart
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. Vòng đời thật RaftElectionComponent: tắt Leader dừng heartbeat, restart về Follower, giữ vote & log, timer timeout bầu term mới")
    void realRaftElectionComponent_leaderShutdownAndRestart_preservesVoteLogAndRearmsTimer() throws Exception {
        List<RaftLogEntry> seedLog = List.of(bookEntry(1, 1, 1101, "G01", "Alice"));
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(1L, null, seedLog, 1L);

        AtomicInteger boot1Heartbeats = new AtomicInteger();
        List<Exception> boot1Failures = new CopyOnWriteArrayList<>();

        // Boot 1: Real RaftElectionComponent wins election at term 2, appends entry 2, sends heartbeats
        AtomicReference<PersistentRaftNode> boot1CoreRef = new AtomicReference<>();
        try (RaftElectionComponent comp1 = new RaftElectionComponent(
                "node-1",
                storage,
                (state, durable) -> {
                    PersistentRaftNode core = new PersistentRaftNode("node-1", state, durable);
                    boot1CoreRef.set(core);
                    return core;
                },
                (peer, req, reply) -> {
                    try {
                        reply.receive(new RequestVoteResponse(req.getTerm(), true));
                    } catch (RaftStorageException e) {
                        throw new IllegalStateException(e);
                    }
                },
                new ElectionConfig(20, 60, 120),
                boot1Failures::add,
                () -> {})) {

            comp1.configureHeartbeat((peer, req, reply) -> boot1Heartbeats.incrementAndGet());
            comp1.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!"LEADER".equals(comp1.status().role()) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals("LEADER", comp1.status().role());
            assertEquals(2L, comp1.status().currentTerm());
            assertTrue(boot1Heartbeats.get() > 0, "Leader thật phải phát heartbeat");

            // Append an uncommitted entry on the Raft loop before shutdown
            RaftLogEntry entry2 = bookEntry(2, 2, 1102, "G02", "Bob");
            RaftNode.AppendOutcome outcome = comp1.submitOnLoop(
                    () -> boot1CoreRef.get().appendCommand(entry2.commandType(), entry2.commandPayload()));
            assertInstanceOf(RaftNode.Accepted.class, outcome);
            assertEquals(2L, comp1.detailedStatus().lastLogIndex());
        } // comp1.close() executes here!

        // Verify boot 1 heartbeats completely stopped after close()
        int heartbeatsAfterClose = boot1Heartbeats.get();
        Thread.sleep(80);
        assertEquals(heartbeatsAfterClose, boot1Heartbeats.get(),
                "Heartbeat từ RaftElectionComponent cũ phải dừng hẳn sau close()");
        assertTrue(boot1Failures.isEmpty());

        // Validate persisted state via TV2 D13 DbStateLoader before Boot 2
        NodeStartupState startup2 = new DbStateLoader(
                storage, null, (DbStateLoader.ConnectionSupplier) null).load();
        assertEquals(BootstrapType.RESTART, startup2.bootstrapType());
        assertEquals(2L, startup2.currentTerm());
        assertEquals("node-1", startup2.votedFor());
        assertEquals(2L, startup2.lastLogIndex());
        assertEquals(2L, startup2.lastLogTerm());
        assertEquals(1L, startup2.lastApplied());
        assertEquals(1L, startup2.initialCommitIndex());

        // Boot 2: Fresh RaftElectionComponent on the exact same storage, transport denies votes (no quorum)
        AtomicInteger boot2Heartbeats = new AtomicInteger();
        AtomicInteger boot2OutboundVoteTerms = new AtomicInteger();
        List<Exception> boot2Failures = new CopyOnWriteArrayList<>();

        try (RaftElectionComponent comp2 = new RaftElectionComponent(
                "node-1",
                storage,
                (state, durable) -> new PersistentRaftNode("node-1", state, durable),
                (peer, req, reply) -> boot2OutboundVoteTerms.set((int) req.getTerm()),
                new ElectionConfig(20, 180, 260),
                boot2Failures::add,
                () -> {})) {

            comp2.configureHeartbeat((peer, req, reply) -> boot2Heartbeats.incrementAndGet());

            HandlerRegistry peerRegistry = new HandlerRegistry();
            comp2.register(peerRegistry);
            MessageDispatcher dispatcher = new MessageDispatcher(
                    peerRegistry, MessageDispatcher.ChannelType.PEER, "node-1");

            comp2.start();

            // Immediately after restart: FOLLOWER, term=2, lastLogIndex=2, commitIndex=1, no heartbeats
            RaftElectionComponent.DetailedStatus status = comp2.detailedStatus();
            assertEquals("FOLLOWER", status.role());
            assertEquals(2L, status.currentTerm());
            assertNull(status.leaderId());
            assertEquals(2L, status.lastLogIndex());
            assertEquals(1L, status.commitIndex());
            assertEquals(1L, status.lastApplied());
            assertTrue(status.storageHealthy());
            assertEquals(0, boot2Heartbeats.get());

            // Wire REQUEST_VOTE for term 2 from node-2 MUST be rejected (already self-voted in term 2!)
            MessageEnvelope<?> rvReq = MessageEnvelope.createRequest(
                    MessageType.REQUEST_VOTE, "node-2",
                    new RequestVoteRequestDto(2L, "node-2", 2L, 2L));
            MessageEnvelope<?> rvResp = dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(rvReq)));
            assertFalse(MessageCodec.getMapper().valueToTree(rvResp.getPayload()).get("voteGranted").asBoolean(),
                    "Node restart không được cấp phiếu lần 2 ở term 2 qua wire REQUEST_VOTE");
            assertEquals(2L, MessageCodec.getMapper().valueToTree(rvResp.getPayload()).get("term").asLong());

            // Wire APPEND_ENTRIES heartbeat at term 3 keeps node-1 as FOLLOWER and resets timer
            MessageEnvelope<?> aeReq = MessageEnvelope.createRequest(
                    MessageType.APPEND_ENTRIES, "node-3",
                    AppendEntriesRequestDto.createHeartbeat(3L, "node-3", 2L, 2L, 1L));
            MessageEnvelope<?> aeResp = dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(aeReq)));
            assertTrue(MessageCodec.getMapper().valueToTree(aeResp.getPayload()).get("success").asBoolean());
            assertEquals("FOLLOWER", comp2.detailedStatus().role());
            assertEquals(3L, comp2.detailedStatus().currentTerm());
            assertEquals("node-3", comp2.detailedStatus().leaderId());
            assertEquals(2L, comp2.detailedStatus().lastLogIndex(), "Log cũ vẫn còn nguyên vẹn");

            // Now let the restarted election timer naturally expire (no more heartbeats from node-3)
            long electionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (boot2OutboundVoteTerms.get() < 4 && System.nanoTime() < electionDeadline) {
                Thread.sleep(10);
            }
            assertTrue(boot2OutboundVoteTerms.get() >= 4,
                    "Election timer mới sau restart phải timeout và phát động election ở term >= 4");
            assertTrue(storage.load().currentTerm() >= 4L);
            assertEquals("node-1", storage.load().votedFor());
            assertEquals(2, storage.storedLog().size(), "Log cũ vẫn còn nguyên sau khi election timer timeout");
            assertTrue(boot2Failures.isEmpty());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 12: 5-Node Cluster — Leader Crash & Restart, New Leader Elects & Commits, Old Leader Catches Up
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Cụm 5 node (AC07/AC08): Leader tắt với entry chưa commit -> cụm bầu Leader mới -> Leader cũ restart, tuân luật phiếu, sửa suffix và hội tụ 5/5")
    void fiveNodeCluster_leaderCrashAndRestart_preservesVoteRulesThenRepairsAndConverges() throws Exception {
        RestartCluster cluster = new RestartCluster();

        // 1. Elect node-1 at term 1, commit & apply G01 across all 5 nodes
        cluster.electLeader("node-1");
        ClusterNode n1 = cluster.node("node-1");
        assertEquals(1L, n1.state.currentTerm());

        cluster.leaderAppendAndReplicateAll("node-1", bookEntry(1, 1, 1201, "G01", "Alice"));
        for (String id : VOTERS) {
            assertEquals(1L, cluster.node(id).state.commitIndex());
            assertEquals(1L, cluster.node(id).state.lastApplied());
            assertEquals("BOOKED", cluster.node(id).repo.readSeatSnapshot().get("G01"));
        }

        // 2. Leader node-1 appends entry 2 locally (uncommitted) and CRASHES before replicating!
        RaftLogEntry ghostEntry = bookEntry(2, 1, 1202, "G02", "Ghost Uncommitted");
        assertInstanceOf(RaftNode.Accepted.class,
                n1.core.appendCommand(ghostEntry.commandType(), ghostEntry.commandPayload()));
        assertEquals(2L, n1.state.lastLogIndex());
        assertEquals(1L, n1.state.commitIndex());
        cluster.stopNode("node-1");

        // 3. Remaining 4 nodes elect node-2 at term 2 and commit G02 (Bob) + G03 (Charlie)
        cluster.electLeader("node-2");
        assertEquals(2L, cluster.node("node-2").state.currentTerm());
        cluster.leaderAppendAndReplicateOnline("node-2", bookEntry(2, 2, 1203, "G02", "Bob"));
        cluster.leaderAppendAndReplicateOnline("node-2", bookEntry(3, 2, 1204, "G03", "Charlie"));
        assertEquals(3L, cluster.node("node-2").state.commitIndex());
        assertEquals(3L, cluster.node("node-2").state.lastApplied());

        // 4. Restart node-1 from durable storage (via TV2 D13 DbStateLoader + TV4 D22 LogReplayCoordinator)
        ClusterNode n1Restarted = cluster.restartNode("node-1");
        assertEquals(BootstrapType.RESTART, n1Restarted.startupState.bootstrapType());
        assertEquals(1L, n1Restarted.startupState.currentTerm());
        assertEquals("node-1", n1Restarted.startupState.votedFor());
        assertEquals(2L, n1Restarted.startupState.lastLogIndex());
        assertEquals(1L, n1Restarted.startupState.lastApplied());
        assertEquals(1L, n1Restarted.startupState.initialCommitIndex());
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, n1Restarted.replayReport.outcome());

        // Before hearing from new Leader: FOLLOWER, timer armed, old log preserved, rejects same-term vote
        assertEquals(Role.FOLLOWER, n1Restarted.state.role());
        assertEquals(1, n1Restarted.scheduler.scheduledCount());
        assertEquals(ghostEntry, n1Restarted.state.entryAt(2), "Restart không tự ý xóa entry chưa commit");
        assertFalse(n1Restarted.receiveVote(new RequestVoteRequest(1L, "node-3", 2L, 1L)).voteGranted(),
                "Không vote lại lần 2 ở term 1 sau restart");

        // 5. New Leader node-2 (term 2) catches up restarted node-1 via TV4 AppendEntries backtracking
        cluster.catchUpFollower("node-2", "node-1");

        // All 5 nodes converge identically on term, log, commitIndex, lastApplied, and BookingStateMachine
        List<RaftLogEntry> expectedLog = cluster.node("node-2").state.entries();
        Map<String, String> expectedSeats = cluster.node("node-2").repo.readSeatSnapshot();
        assertEquals(3, expectedLog.size());
        assertEquals("BOOKED", expectedSeats.get("G01"));
        assertEquals("BOOKED", expectedSeats.get("G02"));
        assertEquals("BOOKED", expectedSeats.get("G03"));

        for (String id : VOTERS) {
            ClusterNode peer = cluster.node(id);
            assertEquals(2L, peer.state.currentTerm(), "term mismatch on " + id);
            assertEquals(3L, peer.state.commitIndex(), "commitIndex mismatch on " + id);
            assertEquals(3L, peer.state.lastApplied(), "lastApplied mismatch on " + id);
            assertEquals(expectedLog, peer.state.entries(), "log mismatch on " + id);
            assertEquals(expectedSeats, peer.repo.readSeatSnapshot(), "seat snapshot mismatch on " + id);
        }
        assertEquals("Bob", extractPassenger(n1Restarted.state.entryAt(2).commandPayload()));
        cluster.shutdownAll();
    }

    // -------------------------------------------------------------------------------------------
    // Test 13: 5-Node Cluster — Follower Crashes & Restarts Mid-Split-Vote -> Prevents Dual Leaders!
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("13. Cụm 5 node: Follower vote cho node-2 ở term 2 rồi tắt/restart giữa split vote -> từ chối node-3 cùng term, ngăn 2 Leader")
    void fiveNodeCluster_followerRestartsMidSplitVote_preventsTwoLeadersInSameTerm() throws Exception {
        RestartCluster cluster = new RestartCluster();

        // Cluster starts at term 1 with 1 committed entry
        cluster.electLeader("node-1");
        cluster.leaderAppendAndReplicateAll("node-1", bookEntry(1, 1, 1301, "G01", "Alice"));
        cluster.stopNode("node-1"); // Leader crashes

        // Split vote at term 2: both node-2 and node-3 start campaigning at term 2
        ClusterNode n2 = cluster.node("node-2");
        ClusterNode n3 = cluster.node("node-3");
        ClusterNode n4 = cluster.node("node-4");
        ClusterNode n5 = cluster.node("node-5");

        n2.scheduler.fireNextDeadline(); // term 2, self-vote (1/5)
        n3.scheduler.fireNextDeadline(); // term 2, self-vote (1/5)
        assertEquals(2L, n2.state.currentTerm());
        assertEquals(2L, n3.state.currentTerm());

        // node-5 votes for node-3 -> node-3 now has 2/5 votes (needs only 1 more to win!)
        assertTrue(cluster.deliverVoteRequest("node-3", "node-5"));
        assertEquals(Role.CANDIDATE, n3.state.role());

        // node-4 votes for node-2 -> node-2 now has 2/5 votes; node-4 persists (term=2, votedFor="node-2")
        assertTrue(cluster.deliverVoteRequest("node-2", "node-4"));
        assertEquals(Role.CANDIDATE, n2.state.role());
        assertEquals(2L, n4.storage.load().currentTerm());
        assertEquals("node-2", n4.storage.load().votedFor());

        // node-4 CRASHES AND RESTARTS right before node-3 asks it for a vote in term 2!
        cluster.stopNode("node-4");
        ClusterNode n4Restarted = cluster.restartNode("node-4");
        assertEquals(2L, n4Restarted.startupState.currentTerm());
        assertEquals("node-2", n4Restarted.startupState.votedFor());
        assertEquals(Role.FOLLOWER, n4Restarted.state.role());
        assertEquals(1L, n4Restarted.state.lastLogIndex(), "Log cũ của node-4 không bị xóa");

        // node-3 asks restarted node-4 for vote in term 2 -> MUST BE DENIED!
        // If node-4 had forgotten its vote on restart, node-3 would win 3/5 at the same time as node-2!
        assertFalse(cluster.deliverVoteRequest("node-3", "node-4"),
                "node-4 sau restart phải từ chối node-3 ở term 2 vì đã bầu cho node-2");
        assertEquals(Role.CANDIDATE, n3.state.role(), "node-3 không thể đắc cử khi bị từ chối phiếu");

        // Now node-1 restarts at term 1 and votes for node-2 at term 2 -> node-2 wins uniquely with 3/5 (n2, n4, n1)
        cluster.restartNode("node-1");
        assertTrue(cluster.deliverVoteRequest("node-2", "node-1"));
        assertEquals(Role.LEADER, n2.state.role(), "node-2 đắc cử Leader duy nhất ở term 2");
        assertEquals(Role.CANDIDATE, n3.state.role());

        // Even if node-2 retries REQUEST_VOTE to restarted node-4, node-4 replies granted (idempotent)
        assertTrue(n4Restarted.receiveVote(new RequestVoteRequest(2L, "node-2", 1L, 1L)).voteGranted());

        // Verify at most one Leader in term 2 across all 5 nodes
        long term2Leaders = VOTERS.stream()
                .map(cluster::node)
                .filter(n -> n.state.role() == Role.LEADER && n.state.currentTerm() == 2L)
                .count();
        assertEquals(1L, term2Leaders, "Không bao giờ có quá 1 Leader trong cùng term 2");
        cluster.shutdownAll();
    }

    // -------------------------------------------------------------------------------------------
    // Test 14: TV2 D13 GENESIS vs RESTART Classification With Empty Log
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("14. DbStateLoader phân biệt GENESIS và RESTART ngay cả khi log rỗng nhưng đã có term/votedFor")
    void dbStateLoader_distinguishesGenesisAndEmptyLogRestartWithVote() throws Exception {
        FakeRaftStorage storage = new FakeRaftStorage();
        InMemoryBookingRepository repo = new InMemoryBookingRepository();

        // Completely fresh storage -> GENESIS
        RestartableNode genesis = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.GENESIS, genesis.startupState.bootstrapType());
        assertEquals(0L, genesis.startupState.currentTerm());
        assertNull(genesis.startupState.votedFor());
        assertEquals(0L, genesis.startupState.lastLogIndex());

        // Node votes in term 1 while log is still empty -> shutdown
        assertTrue(genesis.receiveVote(new RequestVoteRequest(1L, "node-2", 0L, 0L)).voteGranted());
        genesis.shutdown();

        // Restart with empty log but (term=1, votedFor="node-2") -> MUST classify as RESTART and enforce vote rule!
        RestartableNode restarted = RestartableNode.boot("node-1", storage, repo);
        assertEquals(BootstrapType.RESTART, restarted.startupState.bootstrapType());
        assertEquals(1L, restarted.startupState.currentTerm());
        assertEquals("node-2", restarted.startupState.votedFor());
        assertEquals(0L, restarted.startupState.lastLogIndex());
        assertEquals(0L, restarted.startupState.initialCommitIndex());
        assertFalse(restarted.receiveVote(new RequestVoteRequest(1L, "node-3", 0L, 0L)).voteGranted(),
                "Dù log rỗng, restart vẫn phải nhớ vote ở term 1 và từ chối candidate khác");
        assertTrue(restarted.receiveVote(new RequestVoteRequest(1L, "node-2", 0L, 0L)).voteGranted());
        restarted.shutdown();
    }

    // ===========================================================================================
    // Test Infrastructure: Real Production Components + Deterministic Schedulers & Storage
    // ===========================================================================================

    private static String extractPassenger(String payload) throws Exception {
        return MessageCodec.getMapper().readTree(payload).get("passengerName").asText();
    }

    private static InMemoryBookingRepository repositoryWithPrefix(List<RaftLogEntry> log, int count) {
        InMemoryBookingRepository repo = new InMemoryBookingRepository();
        CommittedCommandApplier applier = new CommittedCommandApplier(repo);
        for (int i = 0; i < count; i++) {
            RaftLogEntry entry = log.get(i);
            applier.applyCommitted(entry.index(), RaftApplyAdapter.decodeCommand(entry));
        }
        return repo;
    }

    private record OutboundVote(String peer, RequestVoteRequestDto dto, CandidateElection.VoteReply reply) { }

    /**
     * Single-node lifecycle harness composed exclusively of real production TV2/TV4/TV5 classes:
     * {@link DbStateLoader}, {@link RaftState}, {@link PersistentRaftNode}, {@link CandidateElection},
     * {@link ElectionTimer}, {@link HigherTermHandler}, {@link LeaderHeartbeatScheduler},
     * {@link RaftStateMachineApplier}, {@link CommittedCommandApplier}, and {@link LogReplayCoordinator}.
     */
    private static class RestartableNode {
        final String id;
        final FakeRaftStorage storage;
        final InMemoryBookingRepository repo;
        final NodeStartupState startupState;
        final RaftState state;
        final PersistentRaftNode core;
        final RecordingScheduler scheduler = new RecordingScheduler();
        final RecordingScheduler heartbeatScheduler = new RecordingScheduler();
        final ElectionTimer timer;
        final HigherTermHandler higherTermHandler;
        final CandidateElection election;
        final LeaderHeartbeatScheduler heartbeats;
        final RaftStateMachineApplier applier;
        final LogReplayCoordinator replayCoordinator;
        final LogReplayCoordinator.ReplayReport replayReport;
        final List<OutboundVote> outboundVotes = new ArrayList<>();
        final List<ApplyResult> appliedBusinessResults = new ArrayList<>();
        int heartbeatSentCount;
        boolean shutDown;

        private RestartableNode(
                String id,
                NodeStartupState startupState,
                RaftState state,
                FakeRaftStorage storage,
                InMemoryBookingRepository repo) throws Exception {
            this.id = id;
            this.startupState = startupState;
            this.storage = storage;
            this.repo = repo;
            this.state = state;
            this.core = new PersistentRaftNode(id, state, storage);

            CommittedCommandApplier committedApplier = new CommittedCommandApplier(
                    repo, appliedBusinessResults::add);
            this.applier = new RaftStateMachineApplier(
                    state, RaftApplyAdapter::decodeCommand, committedApplier.commitListener());
            this.core.addCommitListener(applier);

            // TV4 D22 startup replay before starting election timers
            this.replayCoordinator = new LogReplayCoordinator(state, applier, repo::lastApplied);
            this.replayReport = replayCoordinator.replayOnStartup();

            AtomicReference<ElectionTimer> timerRef = new AtomicReference<>();
            AtomicReference<LeaderHeartbeatScheduler> hbRef = new AtomicReference<>();

            this.higherTermHandler = new HigherTermHandler(
                    state,
                    core::observeHigherTerm,
                    () -> {
                        LeaderHeartbeatScheduler hb = hbRef.get();
                        if (hb != null) {
                            hb.stop();
                        }
                    },
                    () -> {
                        ElectionTimer t = timerRef.get();
                        if (t != null) {
                            t.start();
                        }
                    });

            this.heartbeats = new LeaderHeartbeatScheduler(
                    id,
                    state,
                    heartbeatScheduler,
                    new ElectionConfig(150, 500, 1000),
                    (peer, req, reply) -> heartbeatSentCount++,
                    higherTermHandler,
                    failure -> {
                        throw new AssertionError("Unexpected heartbeat failure on " + id, failure);
                    });
            hbRef.set(this.heartbeats);

            this.election = new CandidateElection(
                    id,
                    core,
                    state,
                    (peer, dto, reply) -> outboundVotes.add(new OutboundVote(peer, dto, reply)),
                    higherTermHandler,
                    () -> {
                        timerRef.get().stop();
                        heartbeats.start();
                    });

            this.timer = new ElectionTimer(
                    election,
                    scheduler,
                    () -> 500L,
                    failure -> {
                        throw new AssertionError("Unexpected timer failure on " + id, failure);
                    });
            timerRef.set(this.timer);

            this.core.bindLifecycle(timer, heartbeats::stop);
            this.timer.start();
        }

        static RestartableNode boot(String id, FakeRaftStorage storage, InMemoryBookingRepository repo)
                throws Exception {
            NodeStartupState startup = new DbStateLoader(
                    storage, repo, (DbStateLoader.ConnectionSupplier) null).load();
            PersistentState persisted = storage.load();
            RaftState restored = RaftState.restore(
                    persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
            return new RestartableNode(id, startup, restored, storage, repo);
        }

        static RestartableNode bootAllowingDurableAhead(
                String id, FakeRaftStorage storage, InMemoryBookingRepository repo) throws Exception {
            NodeStartupState startup = new DbStateLoader(
                    storage, null, (DbStateLoader.ConnectionSupplier) null).load();
            PersistentState persisted = storage.load();
            RaftState restored = RaftState.restore(
                    persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
            return new RestartableNode(id, startup, restored, storage, repo);
        }

        static RestartableNode bootFromState(
                String id, RaftState restored, FakeRaftStorage storage, InMemoryBookingRepository repo)
                throws Exception {
            NodeStartupState startup = new DbStateLoader(
                    storage, repo, (DbStateLoader.ConnectionSupplier) null).load();
            return new RestartableNode(id, startup, restored, storage, repo);
        }

        RequestVoteResponse receiveVote(RequestVoteRequest request) throws RaftStorageException {
            RequestVoteResponse response = core.handleRequestVote(request);
            timer.onVoteDecision(request.term(), response.voteGranted());
            if (state.role() == Role.FOLLOWER) {
                timer.start();
            }
            return response;
        }

        AppendEntriesResponse receiveAppend(AppendEntriesRequest request) throws RaftStorageException {
            return core.handleAppendEntries(request);
        }

        void shutdown() {
            if (shutDown) {
                return;
            }
            shutDown = true;
            timer.close();
            heartbeats.close();
            scheduler.shutdownNow();
            heartbeatScheduler.shutdownNow();
        }
    }

    /** Cluster node extending {@link RestartableNode} with per-node durable storage &amp; repo. */
    private static final class ClusterNode extends RestartableNode {
        boolean online = true;

        private ClusterNode(
                String id,
                NodeStartupState startupState,
                RaftState state,
                FakeRaftStorage storage,
                InMemoryBookingRepository repo) throws Exception {
            super(id, startupState, state, storage, repo);
        }

        static ClusterNode bootClusterNode(String id, FakeRaftStorage storage, InMemoryBookingRepository repo)
                throws Exception {
            NodeStartupState startup = new DbStateLoader(
                    storage, repo, (DbStateLoader.ConnectionSupplier) null).load();
            PersistentState persisted = storage.load();
            RaftState restored = RaftState.restore(
                    persisted.currentTerm(), persisted.votedFor(), persisted.log(), persisted.lastApplied());
            return new ClusterNode(id, startup, restored, storage, repo);
        }
    }

    /** Deterministic 5-node cluster harness with durable per-node storage surviving stop/restart. */
    private static final class RestartCluster {
        private final Map<String, FakeRaftStorage> storages = new LinkedHashMap<>();
        private final Map<String, InMemoryBookingRepository> repos = new LinkedHashMap<>();
        private final Map<String, ClusterNode> nodes = new LinkedHashMap<>();

        RestartCluster() throws Exception {
            for (String id : VOTERS) {
                FakeRaftStorage storage = new FakeRaftStorage();
                InMemoryBookingRepository repo = new InMemoryBookingRepository();
                storages.put(id, storage);
                repos.put(id, repo);
                nodes.put(id, ClusterNode.bootClusterNode(id, storage, repo));
            }
        }

        ClusterNode node(String id) {
            return Objects.requireNonNull(nodes.get(id), "unknown node " + id);
        }

        void stopNode(String id) {
            ClusterNode n = node(id);
            n.online = false;
            n.shutdown();
        }

        ClusterNode restartNode(String id) throws Exception {
            FakeRaftStorage storage = storages.get(id);
            InMemoryBookingRepository repo = repos.get(id);
            // Synchronize durable Raft checkpoint with repo's committed durable lastApplied on clean restart
            storage.seedLastApplied(repo.lastApplied());
            ClusterNode restarted = ClusterNode.bootClusterNode(id, storage, repo);
            restarted.online = true;
            nodes.put(id, restarted);
            return restarted;
        }

        void electLeader(String candidateId) throws Exception {
            ClusterNode candidate = node(candidateId);
            candidate.outboundVotes.clear();
            candidate.scheduler.fireNextDeadline();
            for (String peerId : VOTERS) {
                if (peerId.equals(candidateId)) {
                    continue;
                }
                if (node(peerId).online) {
                    deliverVoteRequest(candidateId, peerId);
                    if (candidate.state.role() == Role.LEADER) {
                        break;
                    }
                }
            }
            assertEquals(Role.LEADER, candidate.state.role(), candidateId + " must win election");
        }

        boolean deliverVoteRequest(String candidateId, String voterId) throws Exception {
            ClusterNode candidate = node(candidateId);
            ClusterNode voter = node(voterId);
            OutboundVote matching = candidate.outboundVotes.stream()
                    .filter(v -> v.peer().equals(voterId))
                    .reduce((first, second) -> second)
                    .orElseThrow(() -> new AssertionError("No vote request from " + candidateId + " to " + voterId));
            RequestVoteRequest req = new RequestVoteRequest(
                    matching.dto().getTerm(),
                    matching.dto().getCandidateId(),
                    matching.dto().getLastLogIndex(),
                    matching.dto().getLastLogTerm());
            RequestVoteResponse resp = voter.receiveVote(req);
            matching.reply().receive(resp);
            return resp.voteGranted();
        }

        void leaderAppendAndReplicateAll(String leaderId, RaftLogEntry entry) throws Exception {
            leaderAppendAndReplicateOnline(leaderId, entry);
        }

        void leaderAppendAndReplicateOnline(String leaderId, RaftLogEntry entry) throws Exception {
            ClusterNode leader = node(leaderId);
            assertEquals(Role.LEADER, leader.state.role());
            RaftNode.AppendOutcome outcome = leader.core.appendCommand(entry.commandType(), entry.commandPayload());
            assertInstanceOf(RaftNode.Accepted.class, outcome);

            ReplicationTracker tracker = new ReplicationTracker(leader.state, leader.state.nextIndex().keySet());
            CommitAdvancer advancer = new CommitAdvancer(leader.state, tracker);

            for (String peerId : VOTERS) {
                if (peerId.equals(leaderId) || !node(peerId).online) {
                    continue;
                }
                catchUpPeerOnce(leader, node(peerId), tracker);
            }

            if (advancer.advance().isPresent()) {
                leader.core.deliverCommittedEntries();
            }

            // Propagate leaderCommit to all online followers
            for (String peerId : VOTERS) {
                if (peerId.equals(leaderId) || !node(peerId).online) {
                    continue;
                }
                ClusterNode follower = node(peerId);
                long prevIdx = leader.state.lastLogIndex();
                long prevTerm = leader.state.termAt(prevIdx);
                follower.receiveAppend(new AppendEntriesRequest(
                        leader.state.currentTerm(), leaderId, prevIdx, prevTerm, List.of(),
                        leader.state.commitIndex()));
            }
        }

        void catchUpFollower(String leaderId, String followerId) throws Exception {
            ClusterNode leader = node(leaderId);
            ClusterNode follower = node(followerId);
            ReplicationTracker tracker = new ReplicationTracker(leader.state, leader.state.nextIndex().keySet());
            // Backtrack nextIndex until matched, then replicate and propagate leaderCommit
            int guard = 0;
            while (guard++ < 32) {
                long nextIdx = leader.state.nextIndexOf(followerId);
                long prevIdx = nextIdx - 1;
                long prevTerm = leader.state.termAt(prevIdx);
                List<RaftLogEntry> batch = new ArrayList<>();
                for (long i = nextIdx; i <= leader.state.lastLogIndex(); i++) {
                    batch.add(leader.state.entryAt(i));
                }
                AppendEntriesResponse resp = follower.receiveAppend(new AppendEntriesRequest(
                        leader.state.currentTerm(), leaderId, prevIdx, prevTerm, batch,
                        leader.state.commitIndex()));
                if (resp.success()) {
                    tracker.recordSuccess(followerId, resp.matchIndex());
                    if (follower.state.lastLogIndex() == leader.state.lastLogIndex()
                            && follower.state.commitIndex() == leader.state.commitIndex()) {
                        return;
                    }
                } else {
                    tracker.lowerNextIndex(followerId, nextIdx, Math.max(1L, nextIdx - 1));
                }
            }
            throw new AssertionError("Follower " + followerId + " failed to catch up from " + leaderId);
        }

        private void catchUpPeerOnce(ClusterNode leader, ClusterNode follower, ReplicationTracker tracker)
                throws Exception {
            long nextIdx = leader.state.nextIndexOf(follower.id);
            long prevIdx = nextIdx - 1;
            long prevTerm = leader.state.termAt(prevIdx);
            List<RaftLogEntry> batch = new ArrayList<>();
            for (long i = nextIdx; i <= leader.state.lastLogIndex(); i++) {
                batch.add(leader.state.entryAt(i));
            }
            AppendEntriesResponse resp = follower.receiveAppend(new AppendEntriesRequest(
                    leader.state.currentTerm(), leader.id, prevIdx, prevTerm, batch,
                    leader.state.commitIndex()));
            if (resp.success()) {
                tracker.recordSuccess(follower.id, resp.matchIndex());
            }
        }

        void shutdownAll() {
            for (ClusterNode n : nodes.values()) {
                n.shutdown();
            }
        }
    }

    /** Deterministic in-memory {@link BookingRepository} backed by the real {@link BookingStateMachine}. */
    private static final class InMemoryBookingRepository implements BookingRepository {
        private final BookingStateMachine machine = new BookingStateMachine();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomesByIndex = new LinkedHashMap<>();
        private int applyCalls;

        @Override
        public long lastApplied() {
            return machine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            applyCalls++;
            BookingStateMachine.ApplyOutcome outcome = machine.apply(logIndex, command);
            outcomesByIndex.put(logIndex, outcome);
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return machine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomesByIndex.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return machine.allSeatStatuses();
        }

        int applyCalls() {
            return applyCalls;
        }
    }

    /** Deterministic single-thread scheduler that records deadlines without spawning threads. */
    private static final class RecordingScheduler extends ScheduledThreadPoolExecutor {
        final List<RecordedDeadline> tasks = new ArrayList<>();
        final Deque<RecordedDeadline> queue = new ArrayDeque<>();
        Runnable beforeSchedule = () -> {};

        RecordingScheduler() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            beforeSchedule.run();
            RecordedDeadline deadline = new RecordedDeadline(command, unit.toMillis(delay));
            tasks.add(deadline);
            queue.addLast(deadline);
            return deadline;
        }

        int scheduledCount() {
            return tasks.size();
        }

        boolean allDeadlinesCancelled() {
            return tasks.stream().allMatch(RecordedDeadline::isCancelled);
        }

        boolean allElectionDeadlinesCancelled() {
            return tasks.stream().allMatch(d -> d.isCancelled() || d.isDone());
        }

        void fireNextDeadline() {
            while (!queue.isEmpty()) {
                RecordedDeadline next = queue.removeFirst();
                if (!next.isCancelled()) {
                    next.run();
                    return;
                }
            }
            throw new AssertionError("No active election deadline to fire");
        }
    }

    private static final class RecordedDeadline extends FutureTask<Void> implements ScheduledFuture<Void> {
        private final long delayMillis;

        RecordedDeadline(Runnable command, long delayMillis) {
            super(command, null);
            this.delayMillis = delayMillis;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    // ===========================================================================================
    // Real PostgreSQL JDBC Restart Suite (TV2-D13 DbStateLoader + TV4-D22 LogReplayCoordinator +
    // JdbcRaftStorage + JdbcBookingRepository + RaftElectionComponent)
    // ===========================================================================================

    @Nested
    @DisplayName("Real PostgreSQL JDBC Election Restart Tests (gated by RAILWAY_TEST_*)")
    class RealPostgreSqlElectionRestartTests {

        private String jdbcUrl;
        private String dbUser;
        private String dbPassword;
        private String schema;
        private String host;
        private int port;
        private String dbName;
        private ConnectionFactory connectionFactory;
        private JdbcRaftStorage jdbcStorage;
        private JdbcBookingRepository jdbcRepo;

        @BeforeEach
        void setUpPostgres() throws Exception {
            jdbcUrl = System.getenv("RAILWAY_TEST_JDBC_URL");
            dbUser = System.getenv("RAILWAY_TEST_DB_USER");
            dbPassword = System.getenv("RAILWAY_TEST_DB_PASSWORD");

            if (Boolean.getBoolean("railway.requirePostgres")) {
                assertTrue(hasText(jdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                        "PostgreSQL is mandatory when -Drailway.requirePostgres=true");
            }
            Assumptions.assumeTrue(
                    hasText(jdbcUrl) && hasText(dbUser) && hasText(dbPassword),
                    "RAILWAY_TEST_JDBC_URL, RAILWAY_TEST_DB_USER, and RAILWAY_TEST_DB_PASSWORD required");

            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            host = uri.getHost();
            port = uri.getPort() < 0 ? 5432 : uri.getPort();
            dbName = uri.getPath().substring(1);

            schema = "railway_d24_" + UUID.randomUUID().toString().replace("-", "");
            try (Connection conn = rawConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA \"" + schema + "\"");
            }
            try {
                runMigration("V1__schema.sql");
                runMigration("V2__seed.sql");
                String schemaDbName = dbName + "?currentSchema=" + schema;
                connectionFactory = new ConnectionFactory(new DbConfig(host, port, schemaDbName, dbUser, dbPassword));
                jdbcStorage = new JdbcRaftStorage(connectionFactory);
                jdbcRepo = new JdbcBookingRepository(connectionFactory);
            } catch (Exception ex) {
                dropSchema();
                throw ex;
            }
        }

        @AfterEach
        void tearDownPostgres() throws SQLException {
            dropSchema();
        }

        @Test
        @DisplayName("JDBC-1. Follower restart trên PostgreSQL thật: DbStateLoader + JdbcRaftStorage giữ nguyên vote & log, chặn double-vote")
        void jdbcFollowerRestart_preservesTermVoteAndLogAcrossPostgresReload() throws Exception {
            List<RaftLogEntry> entries = List.of(
                    bookEntry(1, 2, 2001, "G01", "Alice"),
                    bookEntry(2, 3, 2002, "G02", "Bob"),
                    bookEntry(3, 5, 2003, "G03", "Uncommitted Charlie"));

            jdbcStorage.saveTermAndVote(5L, null);
            jdbcStorage.replaceSuffix(1L, entries);
            JdbcCommittedCommandApplier seedApplier = new JdbcCommittedCommandApplier(connectionFactory);
            seedApplier.applyCommitted(1L, RaftApplyAdapter.decodeCommand(entries.get(0)));
            seedApplier.applyCommitted(2L, RaftApplyAdapter.decodeCommand(entries.get(1)));

            // Boot 1 on real PostgreSQL: Follower grants vote to node-2 at term 5
            DbStateLoader loader1 = new DbStateLoader(jdbcStorage, jdbcRepo, connectionFactory);
            NodeStartupState startup1 = loader1.load();
            assertEquals(BootstrapType.RESTART, startup1.bootstrapType());
            assertEquals(5L, startup1.currentTerm());
            assertNull(startup1.votedFor());
            assertEquals(3L, startup1.lastLogIndex());
            assertEquals(2L, startup1.lastApplied());
            assertEquals(2L, startup1.initialCommitIndex());

            PersistentState persisted1 = jdbcStorage.load();
            RaftState state1 = RaftState.restore(
                    persisted1.currentTerm(), persisted1.votedFor(), persisted1.log(), persisted1.lastApplied());
            PersistentRaftNode core1 = new PersistentRaftNode("node-1", state1, jdbcStorage);
            RecordingScheduler sched1 = new RecordingScheduler();
            CandidateElection election1 = new CandidateElection(
                    "node-1", core1, state1, (p, d, r) -> {}, core1::observeHigherTerm, () -> {});
            ElectionTimer timer1 = new ElectionTimer(election1, sched1, () -> 500L, f -> {
                throw new AssertionError(f);
            });
            core1.bindLifecycle(timer1, () -> {});
            timer1.start();

            RequestVoteResponse granted = core1.handleRequestVote(new RequestVoteRequest(5L, "node-2", 3L, 5L));
            timer1.onVoteDecision(5L, granted.voteGranted());
            assertTrue(granted.voteGranted());
            timer1.close();
            sched1.shutdownNow();

            // Boot 2: Brand-new JdbcRaftStorage + JdbcBookingRepository + DbStateLoader + LogReplayCoordinator
            JdbcRaftStorage storage2 = new JdbcRaftStorage(connectionFactory);
            JdbcBookingRepository repo2 = new JdbcBookingRepository(connectionFactory);
            NodeStartupState startup2 = new DbStateLoader(storage2, repo2, connectionFactory).load();
            assertEquals(BootstrapType.RESTART, startup2.bootstrapType());
            assertEquals(5L, startup2.currentTerm());
            assertEquals("node-2", startup2.votedFor());
            assertEquals(3L, startup2.lastLogIndex());
            assertEquals(5L, startup2.lastLogTerm());
            assertEquals(2L, startup2.lastApplied());
            assertEquals(2L, startup2.initialCommitIndex());

            PersistentState persisted2 = storage2.load();
            RaftState state2 = RaftState.restore(
                    persisted2.currentTerm(), persisted2.votedFor(), persisted2.log(), persisted2.lastApplied());
            PersistentRaftNode core2 = new PersistentRaftNode("node-1", state2, storage2);
            JdbcCommittedCommandApplier jdbcApplier2 = new JdbcCommittedCommandApplier(connectionFactory);
            RaftStateMachineApplier smApplier2 = new RaftStateMachineApplier(
                    state2, RaftApplyAdapter::decodeCommand, jdbcApplier2.commitListener());
            LogReplayCoordinator replay2 = new LogReplayCoordinator(state2, smApplier2, repo2::lastApplied);
            LogReplayCoordinator.ReplayReport report2 = replay2.replayOnStartup();
            assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report2.outcome());
            assertEquals(1L, report2.uncommittedSuffixLength());

            RecordingScheduler sched2 = new RecordingScheduler();
            CandidateElection election2 = new CandidateElection(
                    "node-1", core2, state2, (p, d, r) -> {}, core2::observeHigherTerm, () -> {});
            ElectionTimer timer2 = new ElectionTimer(election2, sched2, () -> 500L, f -> {
                throw new AssertionError(f);
            });
            core2.bindLifecycle(timer2, () -> {});
            timer2.start();

            // Must reject different candidate in term 5 (no double vote across real PostgreSQL restart)
            int timersBeforeDeny = sched2.scheduledCount();
            RequestVoteResponse denyNode3 = core2.handleRequestVote(new RequestVoteRequest(5L, "node-3", 3L, 5L));
            timer2.onVoteDecision(5L, denyNode3.voteGranted());
            assertFalse(denyNode3.voteGranted());
            assertEquals(timersBeforeDeny, sched2.scheduledCount());

            // Must allow idempotent retry from node-2
            RequestVoteResponse retryNode2 = core2.handleRequestVote(new RequestVoteRequest(5L, "node-2", 3L, 5L));
            timer2.onVoteDecision(5L, retryNode2.voteGranted());
            assertTrue(retryNode2.voteGranted());
            assertEquals(timersBeforeDeny + 1, sched2.scheduledCount());

            // Verify PostgreSQL tables directly: log intact (3 rows), tickets intact (2 rows), meta intact
            assertEquals(3, queryInt("SELECT COUNT(*) FROM raft_log"));
            assertEquals(2, queryInt("SELECT COUNT(*) FROM tickets WHERE status = 'ACTIVE'"));
            assertEquals(2L, queryLong("SELECT last_applied FROM raft_apply_progress WHERE singleton = 1"));
            assertEquals(5L, queryLong("SELECT current_term FROM raft_meta WHERE singleton = 1"));
            assertEquals("node-2", queryString("SELECT voted_for FROM raft_meta WHERE singleton = 1"));

            timer2.close();
            sched2.shutdownNow();
        }

        @Test
        @DisplayName("JDBC-2. Candidate restart trên PostgreSQL thật: giữ self-vote sau crash, timeout mới tăng term lên PostgreSQL")
        void jdbcCandidateRestart_keepsSelfVoteAndAdvancesTermOnNextTimeout() throws Exception {
            List<RaftLogEntry> entries = List.of(bookEntry(1, 3, 2101, "G01", "Alice"));
            jdbcStorage.saveTermAndVote(3L, null);
            jdbcStorage.replaceSuffix(1L, entries);
            new JdbcCommittedCommandApplier(connectionFactory)
                    .applyCommitted(1L, RaftApplyAdapter.decodeCommand(entries.get(0)));

            // Boot 1: times out into CANDIDATE at term 4, persists (4, "node-1") into PostgreSQL raft_meta
            PersistentState p1 = jdbcStorage.load();
            RaftState s1 = RaftState.restore(p1.currentTerm(), p1.votedFor(), p1.log(), p1.lastApplied());
            PersistentRaftNode c1 = new PersistentRaftNode("node-1", s1, jdbcStorage);
            RecordingScheduler sched1 = new RecordingScheduler();
            CandidateElection e1 = new CandidateElection("node-1", c1, s1, (p, d, r) -> {}, c1::observeHigherTerm, () -> {});
            ElectionTimer t1 = new ElectionTimer(e1, sched1, () -> 500L, f -> {
                throw new AssertionError(f);
            });
            c1.bindLifecycle(t1, () -> {});
            t1.start();
            sched1.fireNextDeadline();
            assertEquals(Role.CANDIDATE, s1.role());
            assertEquals(4L, queryLong("SELECT current_term FROM raft_meta WHERE singleton = 1"));
            assertEquals("node-1", queryString("SELECT voted_for FROM raft_meta WHERE singleton = 1"));
            t1.close();
            sched1.shutdownNow();

            // Boot 2: Reload from PostgreSQL via DbStateLoader -> starts as FOLLOWER keeping votedFor="node-1"
            JdbcRaftStorage storage2 = new JdbcRaftStorage(connectionFactory);
            JdbcBookingRepository repo2 = new JdbcBookingRepository(connectionFactory);
            NodeStartupState startup2 = new DbStateLoader(storage2, repo2, connectionFactory).load();
            assertEquals(BootstrapType.RESTART, startup2.bootstrapType());
            assertEquals(4L, startup2.currentTerm());
            assertEquals("node-1", startup2.votedFor());

            PersistentState p2 = storage2.load();
            RaftState s2 = RaftState.restore(p2.currentTerm(), p2.votedFor(), p2.log(), p2.lastApplied());
            PersistentRaftNode c2 = new PersistentRaftNode("node-1", s2, storage2);
            RecordingScheduler sched2 = new RecordingScheduler();
            CandidateElection e2 = new CandidateElection("node-1", c2, s2, (p, d, r) -> {}, c2::observeHigherTerm, () -> {});
            ElectionTimer t2 = new ElectionTimer(e2, sched2, () -> 500L, f -> {
                throw new AssertionError(f);
            });
            c2.bindLifecycle(t2, () -> {});
            t2.start();

            assertEquals(Role.FOLLOWER, s2.role());
            assertFalse(c2.handleRequestVote(new RequestVoteRequest(4L, "node-3", 1L, 3L)).voteGranted());

            // Next election timeout campaigns in term 5 and persists to PostgreSQL
            sched2.fireNextDeadline();
            assertEquals(Role.CANDIDATE, s2.role());
            assertEquals(5L, queryLong("SELECT current_term FROM raft_meta WHERE singleton = 1"));
            assertEquals("node-1", queryString("SELECT voted_for FROM raft_meta WHERE singleton = 1"));
            assertEquals(1, queryInt("SELECT COUNT(*) FROM raft_log"));

            t2.close();
            sched2.shutdownNow();
        }

        @Test
        @DisplayName("JDBC-3. Leader restart với RaftElectionComponent thật + PostgreSQL thật: dừng heartbeat, về Follower, chặn vote cùng term, giữ log")
        void jdbcRealRaftElectionComponent_leaderRestartPreservesVoteLogAndRearmsTimer() throws Exception {
            List<RaftLogEntry> seed = List.of(bookEntry(1, 1, 2201, "G01", "Alice"));
            jdbcStorage.saveTermAndVote(1L, null);
            jdbcStorage.replaceSuffix(1L, seed);
            new JdbcCommittedCommandApplier(connectionFactory)
                    .applyCommitted(1L, RaftApplyAdapter.decodeCommand(seed.get(0)));

            AtomicInteger boot1Heartbeats = new AtomicInteger();
            AtomicReference<PersistentRaftNode> boot1Core = new AtomicReference<>();
            List<Exception> boot1Failures = new CopyOnWriteArrayList<>();

            // Boot 1: real RaftElectionComponent backed by real PostgreSQL JdbcRaftStorage
            try (RaftElectionComponent comp1 = new RaftElectionComponent(
                    "node-1",
                    jdbcStorage,
                    (state, durable) -> {
                        PersistentRaftNode core = new PersistentRaftNode("node-1", state, durable);
                        boot1Core.set(core);
                        return core;
                    },
                    (peer, req, reply) -> {
                        try {
                            reply.receive(new RequestVoteResponse(req.getTerm(), true));
                        } catch (RaftStorageException e) {
                            throw new IllegalStateException(e);
                        }
                    },
                    new ElectionConfig(20, 60, 120),
                    boot1Failures::add,
                    () -> {})) {

                comp1.configureHeartbeat((peer, req, reply) -> boot1Heartbeats.incrementAndGet());
                comp1.start();

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (!"LEADER".equals(comp1.status().role()) && System.nanoTime() < deadline) {
                    Thread.sleep(5);
                }
                assertEquals("LEADER", comp1.status().role());
                assertEquals(2L, comp1.status().currentTerm());
                assertTrue(boot1Heartbeats.get() > 0);

                // Append uncommitted entry 2 to PostgreSQL raft_log
                RaftLogEntry entry2 = bookEntry(2, 2, 2202, "G02", "Uncommitted Bob");
                RaftNode.AppendOutcome outcome = comp1.submitOnLoop(
                        () -> boot1Core.get().appendCommand(entry2.commandType(), entry2.commandPayload()));
                assertInstanceOf(RaftNode.Accepted.class, outcome);
            }

            int hbAfterClose = boot1Heartbeats.get();
            Thread.sleep(80);
            assertEquals(hbAfterClose, boot1Heartbeats.get());
            assertTrue(boot1Failures.isEmpty());
            assertEquals(2, queryInt("SELECT COUNT(*) FROM raft_log"));

            // Boot 2: Validate with real DbStateLoader (with SQL business checks) then restart RaftElectionComponent
            JdbcRaftStorage storage2 = new JdbcRaftStorage(connectionFactory);
            JdbcBookingRepository repo2 = new JdbcBookingRepository(connectionFactory);
            NodeStartupState startup2 = new DbStateLoader(storage2, repo2, connectionFactory).load();
            assertEquals(BootstrapType.RESTART, startup2.bootstrapType());
            assertEquals(2L, startup2.currentTerm());
            assertEquals("node-1", startup2.votedFor());
            assertEquals(2L, startup2.lastLogIndex());
            assertEquals(1L, startup2.lastApplied());
            assertEquals(1L, startup2.initialCommitIndex());

            AtomicInteger boot2VoteTerm = new AtomicInteger();
            List<Exception> boot2Failures = new CopyOnWriteArrayList<>();
            try (RaftElectionComponent comp2 = new RaftElectionComponent(
                    "node-1",
                    storage2,
                    (state, durable) -> new PersistentRaftNode("node-1", state, durable),
                    (peer, req, reply) -> boot2VoteTerm.set((int) req.getTerm()),
                    new ElectionConfig(20, 150, 220),
                    boot2Failures::add,
                    () -> {})) {

                HandlerRegistry registry = new HandlerRegistry();
                comp2.register(registry);
                MessageDispatcher dispatcher = new MessageDispatcher(
                        registry, MessageDispatcher.ChannelType.PEER, "node-1");

                comp2.start();
                RaftElectionComponent.DetailedStatus st = comp2.detailedStatus();
                assertEquals("FOLLOWER", st.role());
                assertEquals(2L, st.currentTerm());
                assertEquals(2L, st.lastLogIndex());
                assertEquals(1L, st.commitIndex());
                assertEquals(1L, st.lastApplied());

                // Reject wire REQUEST_VOTE at term 2 from node-2
                MessageEnvelope<?> rv = MessageEnvelope.createRequest(
                        MessageType.REQUEST_VOTE, "node-2",
                        new RequestVoteRequestDto(2L, "node-2", 2L, 2L));
                MessageEnvelope<?> resp = dispatcher.dispatch(MessageCodec.decode(MessageCodec.encode(rv)));
                assertFalse(MessageCodec.getMapper().valueToTree(resp.getPayload()).get("voteGranted").asBoolean());

                // Wait for restarted election timer to fire term >= 3 and persist to PostgreSQL
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (boot2VoteTerm.get() < 3 && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertTrue(boot2VoteTerm.get() >= 3);
                assertTrue(queryLong("SELECT current_term FROM raft_meta WHERE singleton = 1") >= 3L);
                assertEquals("node-1", queryString("SELECT voted_for FROM raft_meta WHERE singleton = 1"));
                assertEquals(2, queryInt("SELECT COUNT(*) FROM raft_log"), "PostgreSQL raft_log cũ không được bị xóa");
                assertTrue(boot2Failures.isEmpty());
            }
        }

        @Test
        @DisplayName("JDBC-4. Crash mid-apply trên PostgreSQL thật: LogReplayCoordinator redeliver idempotent (0 vé trùng) + giữ luật phiếu & log")
        void jdbcMidApplyReplay_reconcilesIdempotentlyAndEnforcesVoteAndLogRules() throws Exception {
            List<RaftLogEntry> entries = List.of(
                    bookEntry(1, 1, 2301, "G01", "Alice"),
                    bookEntry(2, 2, 2302, "G02", "Bob"),
                    bookEntry(3, 3, 2303, "G03", "Uncommitted Charlie"));

            jdbcStorage.saveTermAndVote(3L, "node-4");
            jdbcStorage.replaceSuffix(1L, entries);
            JdbcCommittedCommandApplier seedApplier = new JdbcCommittedCommandApplier(connectionFactory);
            seedApplier.applyCommitted(1L, RaftApplyAdapter.decodeCommand(entries.get(0)));
            seedApplier.applyCommitted(2L, RaftApplyAdapter.decodeCommand(entries.get(1)));
            assertEquals(2, queryInt("SELECT COUNT(*) FROM tickets"));
            assertEquals(2, queryInt("SELECT COUNT(*) FROM business_events"));

            // Simulate Raft state waking up with stale lastApplied=1 while PostgreSQL raft_apply_progress is already 2
            PersistentState persisted = jdbcStorage.load();
            RaftState behind = RaftState.restore(
                    persisted.currentTerm(), persisted.votedFor(), persisted.log(), 1L);
            List<ApplyResult> redeliveredEvents = new ArrayList<>();
            JdbcCommittedCommandApplier applier2 = new JdbcCommittedCommandApplier(
                    connectionFactory, redeliveredEvents::add);
            RaftStateMachineApplier smApplier = new RaftStateMachineApplier(
                    behind, RaftApplyAdapter::decodeCommand, applier2.commitListener());
            LogReplayCoordinator coordinator = new LogReplayCoordinator(
                    behind, smApplier, jdbcRepo::lastApplied);

            LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();
            assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
            assertEquals(2L, report.commitFloor());
            assertEquals(1, report.redelivered());
            assertEquals(1L, report.uncommittedSuffixLength());
            assertEquals(2L, behind.lastApplied());
            assertEquals(2L, behind.commitIndex());
            assertTrue(redeliveredEvents.isEmpty(), "Redelivery trên PostgreSQL không được phát event thứ hai");
            assertEquals(2, queryInt("SELECT COUNT(*) FROM tickets"), "Không sinh vé trùng khi replay");
            assertEquals(2, queryInt("SELECT COUNT(*) FROM business_events"), "Không sinh business_event trùng");
            assertEquals(3, queryInt("SELECT COUNT(*) FROM raft_log"), "Uncommitted entry 3 vẫn còn trong raft_log");

            // Vote rules after replay: reject node-2 in term 3, reject stale-log node-2 in term 4, grant up-to-date node-2 in term 4
            PersistentElectionState pes = new PersistentElectionState(behind, jdbcStorage);
            assertFalse(pes.grantVote(new RequestVoteRequest(3L, "node-2", 3L, 3L)));
            assertFalse(pes.grantVote(new RequestVoteRequest(4L, "node-2", 2L, 2L)));
            assertTrue(pes.grantVote(new RequestVoteRequest(4L, "node-2", 3L, 3L)));
            assertEquals(4L, queryLong("SELECT current_term FROM raft_meta WHERE singleton = 1"));
            assertEquals("node-2", queryString("SELECT voted_for FROM raft_meta WHERE singleton = 1"));
        }

        private Connection rawConnection() throws SQLException {
            return DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
        }

        private Connection schemaConnection() throws SQLException {
            Connection conn = rawConnection();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET search_path TO \"" + schema + "\"");
            }
            return conn;
        }

        private void runMigration(String fileName) throws Exception {
            Path path = resolveMigration(fileName);
            String sqlScript = Files.readString(path, StandardCharsets.UTF_8);
            try (Connection conn = schemaConnection(); Statement stmt = conn.createStatement()) {
                for (String sql : sqlScript.split(";")) {
                    if (!sql.isBlank()) {
                        stmt.execute(sql);
                    }
                }
            }
        }

        private Path resolveMigration(String fileName) throws IOException {
            Path direct = Path.of("database/migration", fileName);
            if (Files.exists(direct)) {
                return direct;
            }
            Path parent = Path.of("..", "database/migration", fileName);
            if (Files.exists(parent)) {
                return parent;
            }
            throw new IOException("Cannot locate migration file " + fileName);
        }

        private void dropSchema() throws SQLException {
            if (schema != null && hasText(jdbcUrl) && hasText(dbUser) && hasText(dbPassword)) {
                try (Connection conn = rawConnection(); Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
                }
            }
        }

        private int queryInt(String sql) throws SQLException {
            try (Connection conn = schemaConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                return rs.getInt(1);
            }
        }

        private long queryLong(String sql) throws SQLException {
            try (Connection conn = schemaConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                return rs.getLong(1);
            }
        }

        private String queryString(String sql) throws SQLException {
            try (Connection conn = schemaConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();
                return rs.getString(1);
            }
        }

        private static boolean hasText(String value) {
            return value != null && !value.isBlank();
        }
    }
}
