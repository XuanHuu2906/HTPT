package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.protocol.dto.SeatDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.PersistentState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D21: Leader Failover and Recovery over FakeNetwork.
 *
 * <p>Validates the complete lifecycle of Leader failure and automatic failover in a 5-node cluster:
 * <ul>
 *   <li>Initial leader election, durable term/vote recording, and successful BOOK command commit.</li>
 *   <li>Simulated leader failure (kill/stop), halting heartbeats and rejecting new BOOK commands on dead leader.</li>
 *   <li>Follower election timeout detection, candidate promotion, and majority (3/5) re-election in a higher term.</li>
 *   <li>Durable recording of higher term and granted votes in persistent storage across all participating nodes.</li>
 *   <li>Testing BOOK operations via fake interface on the newly elected Leader (replicated, committed, applied).</li>
 *   <li>Rejection and redirection (NOT_LEADER with leaderHint) when attempting BOOK on followers.</li>
 *   <li>Idempotency preservation across failovers with identical requestId re-submission.</li>
 *   <li>Deterministic business validation (e.g., SEAT_ALREADY_BOOKED rejection) under the new Leader.</li>
 *   <li>Safe recovery of the deposed old Leader (storage restoration, step-down, log synchronization, split-brain prevention).</li>
 *   <li>Minority partition failover where uncommitted ghost entries are overwritten upon network healing.</li>
 * </ul>
 *
 * <p><em>Note:</em> Real multi-container Docker Leader failover testing is performed at Gate G6;
 * this test suite provides deterministic verification of consensus, failover, and booking invariants.
 */
@DisplayName("TV5-D21: Leader Failover, Re-election & Fake Booking Interface Test Suite")
class LeaderFailoverTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();
    private static final Instant T0 = Instant.parse("2026-11-21T10:00:00.000Z");

    private static String reqId(int num) {
        return String.format("%08d-0000-0000-0000-%012d", num, num);
    }

    private FakeNetwork network;
    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        network = new FakeNetwork();
        cluster = new TestCluster(network);
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Initial Leader Election, Durable Term/Vote Recording & First Booking
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Bầu Leader ban đầu (node-1 term 1), ghi term/vote bền vững và BOOK thành công vé đầu tiên")
    void initialLeaderElected_persistsTermAndVotes_andBooksTicketSuccessfully() throws Exception {
        // Bầu node-1 làm Leader ở term 1 với đa số phiếu (node-1, node-2, node-3)
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");
        TestNode node3 = cluster.node("node-3");

        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(1L, node1.state.currentTerm());
        assertEquals(1, cluster.leaderCount());
        assertEquals("node-1", cluster.currentLeaderId().orElseThrow());

        // Kiểm tra ghi nhận term/vote bền vững trong storage
        assertEquals(1L, node1.storage.term);
        assertEquals("node-1", node1.storage.vote);
        assertEquals(1L, node2.storage.term);
        assertEquals("node-1", node2.storage.vote);
        assertEquals(1L, node3.storage.term);
        assertEquals("node-1", node3.storage.vote);

        // Thử BOOK qua fake interface của Leader node-1
        String reqId = reqId(101);
        ApplyResult result = node1.bookTicket(reqId, "SE-001", "G01", "Alice");

        assertNotNull(result);
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(reqId, result.requestId());
        assertNotNull(result.ticketId());
        assertEquals(1L, result.appliedLogIndex());
        assertEquals(1L, node1.state.commitIndex());

        // Đồng bộ tiếp commitIndex sang các follower qua heartbeat
        cluster.replicateAll();
        assertEquals(1L, node2.state.commitIndex());
        assertEquals(1L, node3.state.commitIndex());

        // StateMachine trên Leader và Follower đều ghi nhận ghế G01 đã BOOKED
        assertEquals("BOOKED", node1.stateMachine.seatStatus("G01").orElseThrow());
        assertEquals("BOOKED", node2.stateMachine.seatStatus("G01").orElseThrow());
        assertEquals(1, node1.stateMachine.totalTicketCount());
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Kill Simulated Leader & Verification of Inability to Process Writes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Dừng Leader mô phỏng (kill node-1): từ chối lệnh BOOK trên node chết và ngừng gửi heartbeat")
    void killSimulatedLeader_rejectsBookOnKilledLeader_andStopsHeartbeats() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");

        // Dừng simulated leader node-1
        network.killNode("node-1");
        assertTrue(cluster.isKilled("node-1"));
        assertFalse(node1.isLeader(), "Node đã chết không được coi là active Leader");

        // Thử BOOK trên leader đã bị kill -> ném ngoại lệ dừng/hỏng
        assertThrows(IllegalStateException.class, () ->
                node1.bookTicket(reqId(201), "SE-001", "G02", "Bob"));

        // Kiểm tra node-1 không thể gửi thêm bất kỳ RPC hay heartbeat nào vào mạng
        node1.sendHeartbeat();
        node1.replicateToPeers();
        assertEquals(0, network.pendingAppendsCount(), "Node chết không được phát tán tin nhắn mạng");
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Failover Re-election in Higher Term
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Bầu cử failover: Candidate (node-2) phát động bầu cử term 2 và đắc cử Leader với đa số 3/5")
    void failoverReelection_candidateGathersMajorityVotesInNewTerm_becomesNewLeader() throws Exception {
        cluster.electLeader("node-1", 1L);
        network.killNode("node-1");

        // Follower node-2 phát hiện mất heartbeat (election timeout) -> Khởi động bầu cử term 2
        TestNode node2 = cluster.node("node-2");
        long campaignTerm = node2.startElection();
        assertEquals(2L, campaignTerm);
        assertEquals(Role.CANDIDATE, node2.state.role());
        assertEquals(1, node2.electionService.grantedVoteCount(), "Self vote = 1");

        // Giao phiếu tới node-3 và node-4 (node-1 đã chết không nhận được)
        network.deliverVote("node-2", "node-3", 2L);
        assertEquals(2, node2.electionService.grantedVoteCount());
        assertFalse(node2.isLeader(), "2 phiếu chưa đủ đa số 3/5");

        network.deliverVote("node-2", "node-4", 2L);
        assertEquals(3, node2.electionService.grantedVoteCount(), "Đạt 3 phiếu (node-2, node-3, node-4)");

        // Hoàn tất bầu cử -> Node-2 trở thành LEADER mới ở term 2!
        assertTrue(node2.electionService.hasWonElection());
        assertEquals(Role.LEADER, node2.state.role());
        assertEquals(2L, node2.state.currentTerm());
        assertEquals(1, cluster.leaderCount());
        assertEquals("node-2", cluster.currentLeaderId().orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Term & Votes Persistence Invariants Across Failover
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Bền vững Term & Vote: Term đơn điệu tăng, lưu vào storage và chặn vote 2 lần trong cùng term")
    void termAndVotesPersistence_strictlyMonotonic_andRejectsDoubleVotingInSameTerm() throws Exception {
        cluster.electLeader("node-1", 1L);
        network.killNode("node-1");

        TestNode node2 = cluster.node("node-2");
        TestNode node3 = cluster.node("node-3");
        TestNode node4 = cluster.node("node-4");
        TestNode node5 = cluster.node("node-5");

        // Node-2 bầu cử term 2
        node2.startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);

        // Xác thực storage bền vững: term 2 và vote được ghi nhận chính xác
        assertEquals(2L, node2.storage.term);
        assertEquals("node-2", node2.storage.vote);
        assertEquals(2L, node3.storage.term);
        assertEquals("node-2", node3.storage.vote);
        assertEquals(2L, node4.storage.term);
        assertEquals("node-2", node4.storage.vote);

        // Invariant: Node không được vote 2 lần trong cùng 1 term
        // Giả sử node-5 cũng phát động bầu cử ở term 2 và xin phiếu từ node-3
        RequestVoteRequestDto competingReq = new RequestVoteRequestDto(2L, "node-5", 0L, 0L);
        RequestVoteResponse rejectResp = node3.electionService.handleRequestVote(competingReq);
        assertFalse(rejectResp.voteGranted(), "Node-3 đã vote cho node-2 ở term 2 -> PHẢI TỪ CHỐI node-5");
        assertEquals("node-2", node3.storage.vote, "Phiếu bầu bền vững của node-3 không bị thay đổi");

        // Invariant: Term thoái lui bị storage từ chối nghiêm ngặt
        assertThrows(RaftStorageException.class, () -> node3.storage.saveTermAndVote(1L, "node-1"));
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: BOOK via Fake Interface on Newly Elected Leader
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Thử BOOK qua fake interface trên Leader mới (node-2): commit quorum 3/5 và ghi log thành công")
    void newLeaderAcceptsAndCommitsBooking_replicatesToAliveFollowers() throws Exception {
        // 1. Term 1: node-1 là leader, book G01
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(501), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // 2. Kill node-1 và failover sang node-2 (term 2)
        network.killNode("node-1");
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);

        TestNode newLeader = cluster.node("node-2");
        assertEquals(Role.LEADER, newLeader.state.role());
        assertEquals(2L, newLeader.state.currentTerm());

        // 3. Client gửi yêu cầu BOOK ghế G02 tới newLeader
        String req2 = reqId(502);
        ApplyResult result = newLeader.bookTicket(req2, "SE-001", "G02", "Bob");

        assertNotNull(result);
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(req2, result.requestId());
        assertNotNull(result.ticketId());
        assertEquals(2L, result.appliedLogIndex(), "Log index tăng lên 2 trên Leader mới");
        assertEquals(2L, newLeader.state.commitIndex());

        // 4. Kiểm tra sao chép tới các follower còn sống (node-3, node-4, node-5)
        cluster.replicateAll();
        for (String id : List.of("node-3", "node-4", "node-5")) {
            TestNode follower = cluster.node(id);
            assertEquals(2L, follower.state.commitIndex());
            assertEquals("BOOKED", follower.stateMachine.seatStatus("G01").orElseThrow());
            assertEquals("BOOKED", follower.stateMachine.seatStatus("G02").orElseThrow());
        }

        // Snapshot ghế trên Leader mới hiển thị chính xác cả 2 vé đã đặt
        BookingService.SeatSnapshot snapshot = newLeader.getSeats("SE-001");
        assertEquals(10, snapshot.seats().size());
        assertEquals("BOOKED", snapshot.seats().get(0).getStatus()); // G01
        assertEquals("BOOKED", snapshot.seats().get(1).getStatus()); // G02
        assertEquals("AVAILABLE", snapshot.seats().get(2).getStatus()); // G03
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Follower Rejection with NOT_LEADER and Leader Hint
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Thử BOOK trên Follower (node-3): bị từ chối NOT_LEADER kèm leaderHint trỏ về Leader mới")
    void followerRejectsBooking_returnsNotLeaderWithLeaderHint() throws Exception {
        cluster.electLeader("node-1", 1L);
        network.killNode("node-1");

        // Failover sang node-2
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);

        // Leader mới phát heartbeat tới node-3 để cập nhật leader hint
        cluster.node("node-2").sendHeartbeat();
        network.deliverAllAppends();

        TestNode followerNode3 = cluster.node("node-3");
        assertFalse(followerNode3.isLeader());

        // Client thử gửi BOOK_TICKET trực tiếp tới node-3
        BookingRequestHandler.NotLeaderException ex = assertThrows(
                BookingRequestHandler.NotLeaderException.class,
                () -> followerNode3.bookTicket(reqId(601), "SE-001", "G03", "Charlie")
        );

        // Xác thực leaderHint chính xác là node-2
        assertEquals("node-2", ex.leaderHint());

        // Client theo leaderHint gửi lại yêu cầu tới node-2 -> Thành công!
        TestNode actualLeader = cluster.node(ex.leaderHint());
        ApplyResult successResult = actualLeader.bookTicket(reqId(601), "SE-001", "G03", "Charlie");
        assertEquals(ApplyResult.Outcome.CONFIRMED, successResult.outcome());
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Idempotency Across Leader Failover
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Tính Idempotent sau failover: gửi lại cùng requestId trả kết quả cũ, không sinh vé trùng")
    void idempotentBookingAcrossFailover_sameRequestIdReturnsCachedResult() throws Exception {
        // 1. Leader cũ node-1 commit req-init-1 (ghế G01, Alice)
        cluster.electLeader("node-1", 1L);
        String idemReq = reqId(701);
        ApplyResult origResult = cluster.node("node-1").bookTicket(idemReq, "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, origResult.outcome());
        cluster.replicateAll();

        // 2. Node-1 chết, failover sang node-2
        network.killNode("node-1");
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);
        TestNode newLeader = cluster.node("node-2");

        // 3. Client bị timeout nghi ngờ, gửi lại đúng idemReq tới newLeader
        ApplyResult retryResult = newLeader.bookTicket(idemReq, "SE-001", "G01", "Alice");

        assertEquals(ApplyResult.Outcome.CONFIRMED, retryResult.outcome());
        assertEquals(origResult.ticketId(), retryResult.ticketId(), "ticketId trả về phải đồng nhất tuyệt đối");
        assertEquals(1L, retryResult.appliedLogIndex());
        assertEquals(1, newLeader.stateMachine.totalTicketCount(), "Không được tạo thêm vé thứ hai trong StateMachine");

        // 4. Nếu gửi cùng requestId nhưng đổi ý định (ghế khác) -> Báo lỗi REQUEST_ID_CONFLICT
        assertThrows(IllegalArgumentException.class, () ->
                newLeader.bookTicket(idemReq, "SE-001", "G05", "Alice"));
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: Business Validation (Double Booking Rejection) on New Leader
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Nghiệp vụ nhất quán: Leader mới từ chối đặt ghế đã có người đặt (SEAT_ALREADY_BOOKED)")
    void businessRuleValidation_rejectsDoubleBookingOnNewLeader() throws Exception {
        // G01 đã được đặt ở term 1
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(801), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // Failover sang node-2 term 2
        network.killNode("node-1");
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);
        TestNode newLeader = cluster.node("node-2");

        // Khách hàng khác cố gắng đặt lại ghế G01 trên Leader mới
        ApplyResult rejectResult = newLeader.bookTicket(reqId(802), "SE-001", "G01", "Bob");

        assertNotNull(rejectResult);
        assertEquals(ApplyResult.Outcome.REJECTED, rejectResult.outcome());
        assertEquals(ApplyResult.SEAT_ALREADY_BOOKED, rejectResult.reasonCode());
        assertNull(rejectResult.ticketId(), "BOOK REJECTED tuyệt đối không mang ticketId");
        assertEquals(2L, rejectResult.appliedLogIndex(), "Lệnh REJECTED vẫn được apply tuần tự qua log");

        // Chủ nhân vé G01 vẫn là Alice
        String ticketIdG01 = newLeader.stateMachine.seatCurrentTicketId("G01").orElseThrow();
        assertEquals("TKT-NODE-1-1", ticketIdG01);
        assertEquals("ACTIVE", newLeader.stateMachine.ticketStatus(ticketIdG01).orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // Test 9: Recovered Old Leader Restoration, Step-Down & Log Catch-Up
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Phục hồi Leader cũ: nạp lại storage, giáng chức thành Follower và đồng bộ log từ Leader mới")
    void recoveredOldLeader_restoresStorage_stepsDownToFollower_andSyncsWithNewLeader() throws Exception {
        // 1. Ban đầu node-1 là Leader term 1, book G01
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(901), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // 2. Kill node-1, failover sang node-2, book G02
        network.killNode("node-1");
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);
        TestNode newLeader = cluster.node("node-2");
        newLeader.bookTicket(reqId(902), "SE-001", "G02", "Bob");
        cluster.replicateAll();

        assertEquals(2L, newLeader.state.commitIndex());

        // 3. Node-1 phục hồi / restart sau sự cố
        network.recoverNode("node-1");
        TestNode recoveredNode1 = cluster.node("node-1");

        // Phục hồi từ storage bền vững
        PersistentState stateLoaded = recoveredNode1.electionService.recoverFromStorage();
        assertEquals(1L, stateLoaded.currentTerm());
        assertEquals("node-1", stateLoaded.votedFor());
        assertEquals(1, stateLoaded.log().size());

        // 4. Leader mới node-2 gửi AppendEntries (term 2, commitIndex 2) tới node-1
        newLeader.replicateToPeers();
        network.deliverAllAppends();

        // 5. Invariant nghiệm thu:
        //    a. Node-1 nhận term 2 > term 1 -> Giáng chức thành FOLLOWER
        assertEquals(Role.FOLLOWER, recoveredNode1.state.role());
        assertEquals(2L, recoveredNode1.state.currentTerm());
        assertEquals("node-2", recoveredNode1.state.leaderId());

        //    b. LeadershipGuard của node-1 vô hiệu hóa term 1 cũ
        assertFalse(recoveredNode1.guard.isCurrent(1L));

        //    c. Log và commitIndex của node-1 bắt kịp Leader mới
        assertEquals(2L, recoveredNode1.state.lastLogIndex());
        assertEquals(2L, recoveredNode1.state.commitIndex());
        assertEquals(2, recoveredNode1.stateMachine.totalTicketCount());
        assertEquals("BOOKED", recoveredNode1.stateMachine.seatStatus("G02").orElseThrow());

        //    d. Client gọi BOOK vào node-1 bị redirect sang node-2
        assertThrows(BookingRequestHandler.NotLeaderException.class, () ->
                recoveredNode1.bookTicket(reqId(903), "SE-001", "G03", "Charlie"));
    }

    // -------------------------------------------------------------------------------------------
    // Test 10: Partition Failover & Safe Overwriting of Minority Uncommitted Entry
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Phân vùng 3-2: Lệnh ghi dở dang phía 2/5 bị ghi đè an toàn bởi log Leader term mới khi hàn gắn mạng")
    void minorityPartitionFailover_ghostWriteOverwrittenSafelyUponHealing() throws Exception {
        // 1. Bầu node-1 term 1, book G01
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(1001), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // 2. Mạng phân vùng 3-2:
        //    Thiểu số 2/5: {node-1, node-2} (chứa leader cũ)
        //    Đa số 3/5: {node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode oldLeader = cluster.node("node-1");

        // 3. Phía 2/5 nhận lệnh book G02 (ghost write chưa commit)
        CompletableFuture<ApplyResult> minorityFuture =
                oldLeader.submitBooking(reqId(1002), "SE-001", "G02", "Ghost Passenger");
        network.deliverAllAppends();

        // Phía 2/5 chỉ có 2 voter -> KHÔNG THỂ commit (commitIndex vẫn là 1)
        assertEquals(1L, oldLeader.state.commitIndex());
        assertFalse(minorityFuture.isDone(), "Lệnh ghi phía 2/5 không được hoàn thành");
        assertEquals(2L, oldLeader.state.lastLogIndex(), "Node-1 có entry uncommitted tại index 2");

        // 4. Phía 3/5 đa số bầu node-3 làm Leader term 2
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        assertEquals(Role.LEADER, node3.state.role());
        assertEquals(2L, node3.state.currentTerm());

        // Phía 3/5 book ghế G02 cho khách hợp lệ Dave
        ApplyResult daveResult = node3.bookTicket(reqId(1003), "SE-001", "G02", "Dave");
        assertEquals(ApplyResult.Outcome.CONFIRMED, daveResult.outcome());
        assertEquals(2L, node3.state.commitIndex());

        // 5. Hàn gắn mạng (Heal partition)
        network.heal();

        // Node-3 phát thông điệp đồng bộ sang node-1 và node-2
        node3.replicateToPeers();
        network.deliverAllAppends();

        // 6. Invariants nghiệm thu sau hàn gắn:
        //    a. Leader cũ node-1 giáng chức thành FOLLOWER
        assertEquals(Role.FOLLOWER, oldLeader.state.role());
        assertEquals(2L, oldLeader.state.currentTerm());

        //    b. Entry mồ côi term 1 tại index 2 trên node-1 bị GHI ĐÈ bởi entry term 2 của Dave
        RaftLogEntry entryAtIndex2 = oldLeader.state.entryAt(2L);
        assertEquals(2L, entryAtIndex2.term(), "Entry index 2 trên node-1 phải mang term 2 của Leader mới");
        assertTrue(entryAtIndex2.commandPayload().contains("Dave"), "Payload index 2 phải là Dave, không phải Ghost");

        //    c. commitIndex trên node-1 tiến lên 2 và áp dụng Dave
        assertEquals(2L, oldLeader.state.commitIndex());
        String ticketIdDave = oldLeader.stateMachine.seatCurrentTicketId("G02").orElseThrow();
        assertEquals("ACTIVE", oldLeader.stateMachine.ticketStatus(ticketIdDave).orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // Test 11: Cascading Leader Failover Across Consecutive Terms
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. Failover liên tiếp qua nhiều term: node-1 chết -> node-2 lên, node-2 chết -> node-3 lên")
    void cascadingLeaderFailover_multipleConsecutiveLeaderKillsAndReelections() throws Exception {
        // Term 1: node-1 là leader, book G01
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(1101), "SE-001", "G01", "Passenger 1");
        cluster.replicateAll();

        // Failover lần 1: node-1 chết -> node-2 đắc cử term 2, book G02
        network.killNode("node-1");
        cluster.node("node-2").startElection();
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);
        TestNode leader2 = cluster.node("node-2");
        leader2.bookTicket(reqId(1102), "SE-001", "G02", "Passenger 2");
        cluster.replicateAll();

        assertEquals(Role.LEADER, leader2.state.role());
        assertEquals(2L, leader2.state.currentTerm());

        // Failover lần 2: node-2 chết -> node-3 đắc cử term 3, book G03
        network.killNode("node-2");
        cluster.node("node-3").startElection();
        network.deliverVote("node-3", "node-4", 3L);
        network.deliverVote("node-3", "node-5", 3L);
        TestNode leader3 = cluster.node("node-3");
        leader3.bookTicket(reqId(1103), "SE-001", "G03", "Passenger 3");
        cluster.replicateAll();

        assertEquals(Role.LEADER, leader3.state.role());
        assertEquals(3L, leader3.state.currentTerm());
        assertEquals(3L, leader3.state.commitIndex());

        // Kiểm tra tính toàn vẹn trên các node còn sống (node-3, node-4, node-5)
        for (String id : List.of("node-3", "node-4", "node-5")) {
            TestNode aliveNode = cluster.node(id);
            assertEquals(3L, aliveNode.state.commitIndex());
            assertEquals("BOOKED", aliveNode.stateMachine.seatStatus("G01").orElseThrow());
            assertEquals("BOOKED", aliveNode.stateMachine.seatStatus("G02").orElseThrow());
            assertEquals("BOOKED", aliveNode.stateMachine.seatStatus("G03").orElseThrow());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 12: Loss of Quorum (Majority Down) Pauses Writes Safely
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Mất Quorum: Khi quá bán node chết (3/5 chết, chỉ còn 2/5 sống), cụm không thể bầu Leader và dừng ghi an toàn")
    void lossOfQuorum_whenMajorityDead_pausesWritesAndPreventsElection() throws Exception {
        cluster.electLeader("node-1", 1L);

        // 3 node bị chết (node-1, node-2, node-3) -> Cụm chỉ còn 2 node sống (node-4, node-5)
        network.killNode("node-1");
        network.killNode("node-2");
        network.killNode("node-3");

        assertEquals(0, cluster.leaderCount());

        // Node-4 cố gắng ứng cử term 2
        TestNode node4 = cluster.node("node-4");
        node4.startElection();
        network.deliverVote("node-4", "node-5", 2L);

        // Chỉ nhận được tối đa 2 phiếu (node-4 + node-5) < quorum 3
        assertEquals(2, node4.electionService.grantedVoteCount());
        assertFalse(node4.electionService.hasWonElection());
        assertEquals(Role.CANDIDATE, node4.state.role(), "Không đủ 3/5 phiếu -> KHÔNG THỂ trở thành LEADER");

        // Cố gắng ghi qua node-4 bị từ chối
        assertThrows(BookingRequestHandler.NotLeaderException.class, () ->
                node4.bookTicket(reqId(1201), "SE-001", "G01", "Passenger Deadlock"));
    }

    // ===========================================================================================
    // Test Cluster, TestNode & FakeNetwork Implementations
    // ===========================================================================================

    private static final class TestCluster {
        final FakeNetwork network;
        final Map<String, TestNode> nodes = new LinkedHashMap<>();

        TestCluster(FakeNetwork network) {
            this.network = network;
            for (String id : VOTERS) {
                TestNode node = new TestNode(id, this);
                nodes.put(id, node);
                network.registerNode(node);
            }
        }

        TestNode node(String id) {
            return nodes.get(id);
        }

        boolean isKilled(String id) {
            return network.deadNodes.contains(id);
        }

        int leaderCount() {
            return (int) nodes.values().stream()
                    .filter(n -> !isKilled(n.id) && n.state.role() == Role.LEADER)
                    .count();
        }

        Optional<String> currentLeaderId() {
            return nodes.values().stream()
                    .filter(n -> !isKilled(n.id) && n.state.role() == Role.LEADER)
                    .map(n -> n.id)
                    .findFirst();
        }

        void electLeader(String leaderId, long term) throws Exception {
            for (TestNode n : nodes.values()) {
                if (!n.id.equals(leaderId)) {
                    n.state.becomeFollower(term, leaderId);
                    n.storage.saveTermAndVote(term, leaderId);
                }
            }
            TestNode leader = nodes.get(leaderId);
            if (term > 1L && leader.state.currentTerm() < term - 1L) {
                leader.state.becomeFollower(term - 1L);
            }
            leader.state.startElection(leaderId);
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(leaderId);
            leader.state.becomeLeader(peers);
            leader.storage.saveTermAndVote(term, leaderId);
            leader.initLeaderReplication();
        }

        void replicateAll() {
            currentLeaderId().ifPresent(id -> {
                nodes.get(id).replicateToPeers();
                network.deliverAllAppends();
            });
        }

        Set<String> peersOf(String localId) {
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(localId);
            return peers;
        }
    }

    private static final class TestNode implements BookingService {
        final String id;
        final TestCluster cluster;
        final RaftState state = new RaftState();
        final DurableMemoryStorage storage = new DurableMemoryStorage();
        final LeadershipGuard guard;
        final PersistentElectionState persistence;
        final BookingStateMachine stateMachine = new BookingStateMachine();
        final List<ApplyResult> appliedEvents = new ArrayList<>();
        final Map<Long, PendingWrite> pendingWrites = new LinkedHashMap<>();

        RaftElectionService electionService;
        ReplicationTracker tracker;
        CommitAdvancer commitAdvancer;

        TestNode(String id, TestCluster cluster) {
            this.id = id;
            this.cluster = cluster;
            this.guard = new LeadershipGuard(state);
            this.persistence = new PersistentElectionState(state, storage);

            FakeCoreNode core = new FakeCoreNode(id, state, persistence);
            this.electionService = new RaftElectionService(
                    id,
                    core,
                    state,
                    storage,
                    (peer, req, reply) -> cluster.network.sendVote(id, peer, req, reply),
                    term -> {
                        state.becomeFollower(term);
                        try {
                            storage.saveTermAndVote(term, null);
                        } catch (RaftStorageException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    this::initLeaderReplication,
                    () -> {
                        // On step down
                    },
                    null
            );
        }

        void initLeaderReplication() {
            Set<String> peers = cluster.peersOf(id);
            this.tracker = new ReplicationTracker(state, peers);
            this.commitAdvancer = new CommitAdvancer(state, tracker);
            this.electionService.configureWriteAvailability(tracker, commitAdvancer);
        }

        long startElection() throws RaftStorageException {
            if (cluster.isKilled(id)) {
                throw new IllegalStateException("Node " + id + " is stopped/killed");
            }
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

        Optional<BookingCommand.BookTicket> findLoggedBooking(String requestId) {
            for (long i = 1; i <= state.lastLogIndex(); i++) {
                RaftLogEntry entry = state.entryAt(i);
                BookingCommand cmd = deserializeCommand(entry);
                if (cmd instanceof BookingCommand.BookTicket book && book.requestId().equals(requestId)) {
                    return Optional.of(book);
                }
            }
            return Optional.empty();
        }

        public CompletableFuture<ApplyResult> submitBooking(
                String requestId, String tripId, String seatId, String passengerName) {
            if (cluster.isKilled(id)) {
                throw new IllegalStateException("Node " + id + " is stopped/killed");
            }
            if (state.role() != Role.LEADER) {
                return CompletableFuture.failedFuture(
                        new BookingRequestHandler.NotLeaderException(state.leaderId()));
            }

            // Kiểm tra tính Idempotent qua Raft log đã sao chép
            Optional<BookingCommand.BookTicket> logged = findLoggedBooking(requestId);
            if (logged.isPresent()) {
                BookingCommand.BookTicket prev = logged.get();
                if (!prev.tripId().equals(tripId) || !prev.seatId().equals(seatId) || !prev.passengerName().equals(passengerName)) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("REQUEST_ID_CONFLICT"));
                }
                Optional<ApplyResult> existing = stateMachine.storedResult(requestId);
                if (existing.isPresent()) {
                    return CompletableFuture.completedFuture(existing.get());
                }
            }

            long term = guard.captureTerm();
            long logIndex = state.lastLogIndex() + 1L;
            String ticketId = "TKT-" + id.toUpperCase() + "-" + logIndex;
            Instant issuedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                    requestId, tripId, seatId, passengerName, ticketId, issuedAt);
            String payload = serializeCommand(command);
            RaftLogEntry entry = new RaftLogEntry(logIndex, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);

            state.append(entry);
            storage.append(entry);

            CompletableFuture<ApplyResult> future = new CompletableFuture<>();
            pendingWrites.put(logIndex, new PendingWrite(requestId, term, logIndex, command, future));

            replicateToPeers();
            return future;
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
            return !cluster.isKilled(id) && state.role() == Role.LEADER;
        }

        @Override
        public String getLeaderHint() {
            return state.leaderId();
        }

        void replicateToPeers() {
            if (cluster.isKilled(id) || state.role() != Role.LEADER) {
                return;
            }
            for (String peer : cluster.peersOf(id)) {
                long nextIndex = state.nextIndexOf(peer);
                long prevLogIndex = Math.max(0L, nextIndex - 1L);
                long prevLogTerm = state.termAt(prevLogIndex);
                List<RaftLogEntry> entries = entriesFrom(nextIndex);
                RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                        state.currentTerm(), id, prevLogIndex, prevLogTerm, entries, state.commitIndex());
                cluster.network.sendAppend(id, peer, request, response ->
                        handleAppendResponse(peer, request.term(), response));
            }
        }

        void sendHeartbeat() {
            replicateToPeers();
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

        RaftNode.AppendEntriesResponse handleAppendRequest(RaftNode.AppendEntriesRequest request) {
            if (cluster.isKilled(id)) {
                return new RaftNode.AppendEntriesResponse(state.currentTerm(), false, state.lastLogIndex());
            }
            if (request.term() < state.currentTerm()) {
                return new RaftNode.AppendEntriesResponse(state.currentTerm(), false, state.lastLogIndex());
            }
            if (request.term() > state.currentTerm()) {
                state.becomeFollower(request.term(), request.leaderId());
                try {
                    storage.saveTermAndVote(request.term(), null);
                } catch (RaftStorageException e) {
                    throw new RuntimeException(e);
                }
            } else if (state.role() == Role.CANDIDATE) {
                state.becomeFollower(request.term(), request.leaderId());
            } else {
                state.becomeFollower(state.currentTerm(), request.leaderId());
            }

            // Log consistency check
            if (request.prevLogIndex() > 0L) {
                if (state.lastLogIndex() < request.prevLogIndex()
                        || state.termAt(request.prevLogIndex()) != request.prevLogTerm()) {
                    return new RaftNode.AppendEntriesResponse(state.currentTerm(), false, state.lastLogIndex());
                }
            }

            // Ghi đè log xung đột và thêm các entry mới
            if (!request.entries().isEmpty()) {
                if (state.lastLogIndex() > request.prevLogIndex()) {
                    state.truncateSuffixFrom(request.prevLogIndex() + 1L);
                }
                for (RaftLogEntry e : request.entries()) {
                    state.append(e);
                }
                storage.replaceSuffix(request.prevLogIndex() + 1L, request.entries());
            }

            // Tiến commitIndex theo leaderCommit
            if (request.leaderCommit() > state.commitIndex()) {
                long newCommit = Math.min(request.leaderCommit(), state.lastLogIndex());
                if (state.advanceCommitIndex(newCommit)) {
                    applyCommitted(newCommit);
                }
            }

            return new RaftNode.AppendEntriesResponse(state.currentTerm(), true, state.lastLogIndex());
        }

        void handleAppendResponse(String peer, long requestTerm, RaftNode.AppendEntriesResponse response) {
            if (cluster.isKilled(id)) {
                return;
            }
            if (response.term() > state.currentTerm()) {
                state.becomeFollower(response.term(), null);
                try {
                    storage.saveTermAndVote(response.term(), null);
                } catch (RaftStorageException e) {
                    throw new RuntimeException(e);
                }
                return;
            }

            if (!guard.isCurrent(requestTerm)) {
                return;
            }

            if (response.success()) {
                state.advanceMatchIndex(peer, response.matchIndex());
                state.updateNextIndex(peer, response.matchIndex() + 1L);

                electionService.recordPeerAck(peer, response.matchIndex(), Instant.now());

                if (commitAdvancer != null) {
                    OptionalLong newCommit = commitAdvancer.advance();
                    if (newCommit.isPresent()) {
                        applyCommitted(newCommit.getAsLong());
                    }
                }
            } else {
                long currentNext = state.nextIndexOf(peer);
                if (currentNext > 1L) {
                    state.updateNextIndex(peer, currentNext - 1L);
                }
            }
        }

        void applyCommitted(long throughIndex) {
            while (state.lastApplied() < throughIndex) {
                long next = state.lastApplied() + 1L;
                RaftLogEntry entry = state.entryAt(next);
                BookingCommand cmd = deserializeCommand(entry);
                if (cmd != null) {
                    BookingStateMachine.ApplyOutcome outcome = stateMachine.apply(next, cmd);
                    if (outcome instanceof BookingStateMachine.ApplyOutcome.BusinessResult br) {
                        appliedEvents.add(br.applyResult());
                        PendingWrite pw = pendingWrites.remove(next);
                        if (pw != null) {
                            guard.runIfCurrent(pw.term, () -> pw.future.complete(br.applyResult()));
                        }
                    }
                }
                state.markApplied(next);
            }
        }
    }

    private record PendingWrite(
            String requestId,
            long term,
            long logIndex,
            BookingCommand.BookTicket command,
            CompletableFuture<ApplyResult> future
    ) {}

    private static final class FakeCoreNode implements RaftNode {
        final String id;
        final RaftState state;
        final PersistentElectionState persistence;

        FakeCoreNode(String id, RaftState state, PersistentElectionState persistence) {
            this.id = id;
            this.state = state;
            this.persistence = persistence;
        }

        @Override public Role role() { return state.role(); }
        @Override public long currentTerm() { return state.currentTerm(); }
        @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
        @Override public long commitIndex() { return state.commitIndex(); }
        @Override public long lastApplied() { return state.lastApplied(); }
        @Override public void onApplied(long index) { state.markApplied(index); }
        @Override public long lastLogIndex() { return state.lastLogIndex(); }
        @Override public AppendOutcome appendCommand(String commandType, String commandPayload) { throw new UnsupportedOperationException(); }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) { throw new UnsupportedOperationException(); }
        @Override public void addCommitListener(CommitListener listener) { throw new UnsupportedOperationException(); }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws RaftStorageException {
            boolean granted = persistence.grantVote(request);
            return new RequestVoteResponse(state.currentTerm(), granted);
        }

        @Override
        public void onElectionTimeout() throws RaftStorageException {
            persistence.startElection(id);
        }
    }

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
        final RaftNode.AppendEntriesRequest request;
        final Consumer<RaftNode.AppendEntriesResponse> reply;

        SentAppendRpc(String from, String to, RaftNode.AppendEntriesRequest request, Consumer<RaftNode.AppendEntriesResponse> reply) {
            this.from = from;
            this.to = to;
            this.request = request;
            this.reply = reply;
        }
    }

    private static final class FakeNetwork {
        final Map<String, TestNode> nodes = new LinkedHashMap<>();
        final Set<String> deadNodes = new TreeSet<>();
        final Deque<SentVoteRpc> pendingVotes = new ArrayDeque<>();
        final Deque<SentAppendRpc> pendingAppends = new ArrayDeque<>();
        Set<String> partitionA = Set.of();
        Set<String> partitionB = Set.of();
        boolean partitioned = false;

        void registerNode(TestNode node) {
            nodes.put(node.id, node);
        }

        void killNode(String id) {
            deadNodes.add(id);
            if (nodes.containsKey(id)) {
                nodes.get(id).state.becomeFollower(nodes.get(id).state.currentTerm(), null);
            }
        }

        void recoverNode(String id) {
            deadNodes.remove(id);
            if (nodes.containsKey(id)) {
                nodes.get(id).state.becomeFollower(nodes.get(id).state.currentTerm(), null);
            }
        }

        void partition(Set<String> partA, Set<String> partB) {
            this.partitionA = Set.copyOf(partA);
            this.partitionB = Set.copyOf(partB);
            this.partitioned = true;
            for (String a : partA) {
                if (nodes.containsKey(a)) {
                    nodes.get(a).electionService.partition(partB);
                }
            }
            for (String b : partB) {
                if (nodes.containsKey(b)) {
                    nodes.get(b).electionService.partition(partA);
                }
            }
        }

        void heal() {
            this.partitioned = false;
            this.partitionA = Set.of();
            this.partitionB = Set.of();
            for (TestNode n : nodes.values()) {
                n.electionService.healAll();
            }
        }

        boolean canCommunicate(String from, String to) {
            if (deadNodes.contains(from) || deadNodes.contains(to)) {
                return false;
            }
            if (!partitioned) {
                return true;
            }
            return (partitionA.contains(from) && partitionA.contains(to))
                    || (partitionB.contains(from) && partitionB.contains(to));
        }

        void sendVote(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) {
            if (canCommunicate(from, to)) {
                pendingVotes.add(new SentVoteRpc(from, to, req, reply));
            }
        }

        void sendAppend(String from, String to, RaftNode.AppendEntriesRequest req, Consumer<RaftNode.AppendEntriesResponse> reply) {
            if (canCommunicate(from, to)) {
                pendingAppends.add(new SentAppendRpc(from, to, req, reply));
            }
        }

        int pendingAppendsCount() {
            return pendingAppends.size();
        }

        void deliverVote(String from, String to, long term) throws Exception {
            SentVoteRpc target = pendingVotes.stream()
                    .filter(r -> r.from.equals(from) && r.to.equals(to) && r.request.getTerm() == term)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No pending vote RPC from " + from + " to " + to + " term=" + term));
            pendingVotes.remove(target);
            if (canCommunicate(from, to)) {
                RequestVoteResponse resp = nodes.get(to).electionService.handleRequestVote(target.request);
                target.reply.receive(resp);
            }
        }

        void deliverAllVotes() throws Exception {
            while (!pendingVotes.isEmpty()) {
                SentVoteRpc rpc = pendingVotes.removeFirst();
                if (canCommunicate(rpc.from, rpc.to)) {
                    RequestVoteResponse resp = nodes.get(rpc.to).electionService.handleRequestVote(rpc.request);
                    rpc.reply.receive(resp);
                }
            }
        }

        void deliverAllAppends() {
            while (!pendingAppends.isEmpty()) {
                SentAppendRpc rpc = pendingAppends.removeFirst();
                if (canCommunicate(rpc.from, rpc.to)) {
                    RaftNode.AppendEntriesResponse resp = nodes.get(rpc.to).handleAppendRequest(rpc.request);
                    if (canCommunicate(rpc.to, rpc.from)) {
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

    private static final class DurableMemoryStorage implements RaftStorage {
        long term;
        String vote;
        final List<RaftLogEntry> log = new ArrayList<>();
        long lastApplied = 0;

        @Override
        public PersistentState load() {
            return new PersistentState(term, vote, List.copyOf(log), lastApplied);
        }

        @Override
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (term < this.term) {
                throw new RaftStorageException("term regression: " + term + " < " + this.term);
            }
            this.term = term;
            this.vote = vote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            while (log.size() >= fromIndex && !log.isEmpty()) {
                log.remove(log.size() - 1);
            }
            log.addAll(entries);
        }

        @Override
        public long loadLastApplied() {
            return lastApplied;
        }

        void append(RaftLogEntry entry) {
            log.add(entry);
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
