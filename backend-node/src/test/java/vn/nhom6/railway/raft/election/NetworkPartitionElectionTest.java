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
import java.util.UUID;
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
import vn.nhom6.railway.networking.TestNetworkFilter;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.MessageEnvelope;
import vn.nhom6.railway.protocol.dto.MessageType;
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
 * TV5-D22: 3–2 Network Partition Election & Safety Test Suite.
 *
 * <p><strong>Key Requirements & Invariants (TV5-D22 & SPEC-04):</strong>
 * <ul>
 *   <li>Phân vùng mạng 3–2 trong cụm 5 voter mà không kill process (giữ tiến trình sống để phân biệt partition với crash).</li>
 *   <li>Tích hợp trực tiếp với {@link TestNetworkFilter} đã bàn giao tại TV3-D21 và {@link FakeNetwork} của TV5-D21.</li>
 *   <li><strong>Leader phía thiểu số (2/5):</strong> có thể chưa biết mình mất quyền nhưng KHÔNG ĐƯỢC commit writes
 *       (không đạt quorum 3/5); không sinh BOOK_CONFIRMED cho client.</li>
 *   <li><strong>Candidate phía thiểu số (2/5):</strong> phát động bầu cử không thể đắc cử vì chỉ thu thập tối đa 2/5 phiếu.</li>
 *   <li><strong>Phía đa số (3/5):</strong> phát hiện timeout, chuyển CANDIDATE ở term cao hơn, bầu LEADER mới với 3/5 phiếu,
 *       tiếp nhận và commit lệnh ghi bình thường (đạt quorum 3/5).</li>
 *   <li><strong>Follower phía đa số:</strong> từ chối ghi và trả NotLeaderException kèm {@code leaderHint} trỏ về Leader mới.</li>
 *   <li><strong>Hàn gắn mạng (Heal partition):</strong> Leader cũ nhận heartbeat/AppendEntries term mới lập tức giáng chức
 *       xuống FOLLOWER; entry mồ côi (ghost write) chưa commit bị ghi đè an toàn; toàn cụm 5 node hội tụ log và state machine.</li>
 *   <li><strong>Không chờ harness TV6-D22:</strong> kiểm thử phân vùng mạng 3–2 bằng FakeNetwork/TestNetworkFilter đạt PASS 100%;
 *       kiểm thử phân vùng trên môi trường 5 laptop thật diễn tập tại Gate G6.</li>
 * </ul>
 */
@DisplayName("TV5-D22: 3–2 Network Partition Election & Safety Test Suite (TestNetworkFilter + FakeNetwork)")
class NetworkPartitionElectionTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final ObjectMapper MAPPER = MessageCodec.getMapper();

    private static String reqId(int num) {
        return String.format("%08d-0000-0000-0000-%012d", num, num);
    }

    private TestNetworkFilter filter;
    private FakeNetwork network;
    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        filter = new TestNetworkFilter();
        network = new FakeNetwork(filter);
        cluster = new TestCluster(network);
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Initial Leader Election & First Booking Commit
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Bầu Leader ban đầu (node-1 term 1), ghi term/vote bền vững và BOOK thành công vé đầu tiên")
    void initialClusterElection_electsNode1AtTerm1_andBooksFirstTicket() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");

        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(1L, node1.state.currentTerm());
        assertEquals(1, cluster.leaderCount());
        assertEquals("node-1", cluster.currentLeaderId().orElseThrow());

        // Ghi term và vote bền vững
        assertEquals(1L, node1.storage.term);
        assertEquals("node-1", node1.storage.vote);

        // Đặt vé đầu tiên G01
        ApplyResult result = node1.bookTicket(reqId(101), "SE-001", "G01", "Alice");
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertNotNull(result.ticketId());
        assertEquals(1L, node1.state.commitIndex());

        // Đồng bộ toàn cụm
        cluster.replicateAll();
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(1L, n.state.commitIndex());
            assertEquals("BOOKED", n.stateMachine.seatStatus("G01").orElse(null));
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Leader in 2/5 Minority Partition Cannot Commit Writes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Phân vùng 3–2: Leader cũ phía thiểu số (2/5) không thể commit write và không tạo BOOK_CONFIRMED")
    void minorityPartition_isolatedLeaderCannotCommitWrites_producesNoConfirmedBookings() throws Exception {
        // 1. Khởi tạo cụm và đặt vé G01
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(101), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // 2. Chia mạng 3–2 qua TestNetworkFilter:
        //    Thiểu số A (2/5): {node-1, node-2} (chứa leader cũ)
        //    Đa số B (3/5): {node-3, node-4, node-5}
        Set<String> minority = Set.of("node-1", "node-2");
        Set<String> majority = Set.of("node-3", "node-4", "node-5");
        network.partition(minority, majority);

        TestNode oldLeader = cluster.node("node-1");

        // 3. Client gửi lệnh BOOK G02 tới leader cũ phía 2/5
        CompletableFuture<ApplyResult> minorityFuture =
                oldLeader.submitBooking(reqId(102), "SE-001", "G02", "Minority Ghost");

        // Chuyển giao các bản tin AppendEntries
        network.deliverAllAppends();

        // 4. Kiểm tra các bất biến phía thiểu số:
        //    - Node-1 chỉ nhận ack từ node-2 (tổng 2/5 matchIndex >= 2)
        //    - Quorum yêu cầu 3/5 -> commitIndex KHÔNG THỂ tiến (vẫn là 1)
        assertEquals(1L, oldLeader.state.commitIndex(), "commitIndex trên Leader thiểu số phải giữ nguyên");
        assertFalse(minorityFuture.isDone(), "Lệnh ghi phía thiểu số không được phép hoàn thành");
        assertEquals(2L, oldLeader.state.lastLogIndex(), "Node-1 có entry uncommitted tại index 2");

        // StateMachine trên node-1 KHÔNG apply ghế G02
        assertEquals("AVAILABLE", oldLeader.stateMachine.seatStatus("G02").orElse("AVAILABLE"),
                "Ghế G02 không được phép chuyển trạng thái BOOKED trên Leader thiểu số");

        // Không có sự kiện BOOK_CONFIRMED mới
        assertEquals(1, oldLeader.appliedEvents.size(), "Chỉ có duy nhất 1 sự kiện commit từ trước khi phân vùng");
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Candidate in 2/5 Minority Partition Cannot Win Election
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Phân vùng 3–2: Candidate phía thiểu số (2/5) không thể đắc cử Leader do thiếu Quorum")
    void minorityPartition_candidateCannotWinElection_withoutQuorum() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        // Phân vùng 3–2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Follower node-2 ở phía thiểu số cố gắng tranh cử ở term 2
        TestNode node2 = cluster.node("node-2");
        node2.startElection();
        assertEquals(Role.CANDIDATE, node2.state.role());
        assertEquals(2L, node2.state.currentTerm());

        // Bản tin vote gửi sang node-3, node-4, node-5 bị TestNetworkFilter chặn
        // Chỉ có node-1 ở cùng phân vùng nhận được và bỏ phiếu cho node-2
        network.deliverAllVotes();

        // Node-2 chỉ có 2 phiếu (self-vote + node-1) < 3 phiếu đa số
        assertNotEquals(Role.LEADER, node2.state.role(), "Candidate phía 2/5 không được trở thành LEADER");
        assertEquals(Role.CANDIDATE, node2.state.role());
        assertFalse(node2.isLeader());
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Majority (3/5) Partition Elects New Leader in Higher Term
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Phân vùng 3–2: Phía đa số (3/5) timeout, bầu LEADER mới ở term 2 với đủ 3/5 phiếu")
    void majorityPartition_electsNewLeaderInHigherTerm_withThreeVotes() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        // Phân vùng 3–2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Node-3 ở phía đa số timeout và phát động bầu cử term 2
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        assertEquals(Role.CANDIDATE, node3.state.role());
        assertEquals(2L, node3.state.currentTerm());

        // TestNetworkFilter cho phép giao tiếp giữa {node-3, node-4, node-5}
        // và chặn toàn bộ giao tiếp tới {node-1, node-2}
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        // Node-3 thu thập đủ 3 phiếu (node-3 + node-4 + node-5) -> Đắc cử LEADER term 2
        assertEquals(Role.LEADER, node3.state.role(), "Node-3 phải đắc cử LEADER ở term 2");
        assertEquals(2L, node3.state.currentTerm());
        assertTrue(node3.isLeader());

        // Bền vững Term và Vote trong RaftStorage
        assertEquals(2L, node3.storage.term);
        assertEquals("node-3", node3.storage.vote);
        assertEquals(2L, cluster.node("node-4").storage.term);
        assertEquals("node-3", cluster.node("node-4").storage.vote);
        assertEquals(2L, cluster.node("node-5").storage.term);
        assertEquals("node-3", cluster.node("node-5").storage.vote);
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: Majority (3/5) Partition Commits Writes Successfully
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Phân vùng 3–2: Leader mới phía đa số tiếp nhận và commit ghi thành công (đạt quorum 3/5)")
    void majorityPartition_commitsWritesSuccessfully_whilePartitioned() throws Exception {
        // Khởi tạo cụm và phân vùng 3–2
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(101), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Bầu node-3 làm Leader term 2 phía đa số
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, node3.state.role());

        // Client đặt vé G02 cho Dave trên Leader mới node-3
        CompletableFuture<ApplyResult> future =
                node3.submitBooking(reqId(103), "SE-001", "G02", "Dave");

        // Sao chép AppendEntries tới node-4 và node-5
        network.deliverAllAppends();

        // Leader node-3 đạt quorum (matchIndex >= 2 trên node-3, node-4, node-5 = 3 nodes)
        assertTrue(future.isDone(), "Lệnh ghi phía đa số phải hoàn thành thành công");
        ApplyResult result = future.get();
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(2L, node3.state.commitIndex());
        assertEquals("BOOKED", node3.stateMachine.seatStatus("G02").orElse(null));

        // Phát heartbeat để follower trong đa số nhận commitIndex=2
        node3.sendHeartbeat();
        network.deliverAllAppends();

        assertEquals(2L, cluster.node("node-4").state.commitIndex());
        assertEquals("BOOKED", cluster.node("node-4").stateMachine.seatStatus("G02").orElse(null));
        assertEquals(2L, cluster.node("node-5").state.commitIndex());
        assertEquals("BOOKED", cluster.node("node-5").stateMachine.seatStatus("G02").orElse(null));
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Follower in Majority Partition Rejects Booking with LeaderHint
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. Phân vùng 3–2: Follower phía đa số từ chối ghi và trả leaderHint trỏ về Leader mới")
    void majorityPartition_followerRejectsBooking_withLeaderHint() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        // Phát heartbeat từ node-3 sang node-4 để node-4 cập nhật leaderId
        node3.sendHeartbeat();
        network.deliverAllAppends();

        TestNode node4 = cluster.node("node-4");
        assertEquals("node-3", node4.getLeaderHint());

        // Thử đặt vé qua follower node-4
        BookingRequestHandler.NotLeaderException notLeader = assertThrows(
                BookingRequestHandler.NotLeaderException.class,
                () -> node4.bookTicket(reqId(104), "SE-001", "G04", "Charlie"));
        assertEquals("node-3", notLeader.leaderHint());
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Business Rules Consistency on New Leader (Seat Conflict)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7. Phân vùng 3–2: Leader mới từ chối đặt ghế đã có chủ từ term 1 (SEAT_ALREADY_BOOKED)")
    void businessRuleValidation_majorityLeaderRejectsDoubleBooking() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(101), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        // Khách hàng cố đặt lại ghế G01 (đã thuộc về Alice từ trước phân vùng)
        ApplyResult rejectedResult = node3.bookTicket(reqId(105), "SE-001", "G01", "Eve");
        assertEquals(ApplyResult.Outcome.REJECTED, rejectedResult.outcome());
        assertEquals("SEAT_ALREADY_BOOKED", rejectedResult.reasonCode());
        assertNull(rejectedResult.ticketId());

        // Quyền sở hữu ghế G01 vẫn thuộc về Alice
        assertEquals("BOOKED", node3.stateMachine.seatStatus("G01").orElse(null));
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: Network Heal - Old Leader Steps Down Immediately Upon Heartbeat
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Hàn gắn mạng: Leader cũ nhận heartbeat term 2 từ Leader mới lập tức giáng chức xuống FOLLOWER")
    void networkHeal_oldLeaderReceivesNewLeaderHeartbeat_stepsDownImmediately() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, node3.state.role());

        TestNode oldLeader = cluster.node("node-1");
        assertEquals(Role.LEADER, oldLeader.state.role()); // Chưa biết mình mất quyền

        // Hàn gắn phân vùng mạng qua TestNetworkFilter
        network.heal();

        // Node-3 phát heartbeat toàn cụm
        node3.sendHeartbeat();
        network.deliverAllAppends();

        // Leader cũ node-1 thấy term 2 > term 1 -> Giáng chức ngay lập tức
        assertEquals(Role.FOLLOWER, oldLeader.state.role(), "Leader cũ phải giáng chức xuống FOLLOWER");
        assertEquals(2L, oldLeader.state.currentTerm());
        assertEquals("node-3", oldLeader.state.leaderId());
        assertEquals(2L, oldLeader.storage.term);
        assertEquals(1, cluster.leaderCount(), "Toàn cụm chỉ tồn tại duy nhất 1 Leader");
        assertEquals("node-3", cluster.currentLeaderId().orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // Test 9: Network Heal - Uncommitted Ghost Writes on Old Leader Overwritten
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Hàn gắn mạng: Entry mồ côi (ghost write) chưa commit trên Leader cũ bị ghi đè an toàn")
    void networkHeal_uncommittedGhostWritesOnOldLeader_safelyOverwritten() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(101), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        // Phân vùng 3–2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode oldLeader = cluster.node("node-1");

        // Phía 2/5 nhận write mồ côi cho ghế G02
        CompletableFuture<ApplyResult> ghostFuture =
                oldLeader.submitBooking(reqId(102), "SE-001", "G02", "Minority Ghost");
        network.deliverAllAppends();
        assertEquals(1L, oldLeader.state.commitIndex());
        assertEquals(2L, oldLeader.state.lastLogIndex());

        // Phía 3/5 bầu node-3 term 2 và commit ghế G02 cho Dave
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        ApplyResult daveResult = node3.bookTicket(reqId(103), "SE-001", "G02", "Dave");
        assertEquals(ApplyResult.Outcome.CONFIRMED, daveResult.outcome());

        // Hàn gắn mạng
        network.heal();

        // Node-3 đồng bộ AppendEntries sang node-1 và node-2
        node3.replicateToPeers();
        network.deliverAllAppends();

        // Node-1 ghi đè entry index 2
        RaftLogEntry overwrittenEntry = oldLeader.state.entryAt(2L);
        assertEquals(2L, overwrittenEntry.term(), "Entry index 2 trên node-1 phải mang term 2 của Leader mới");
        assertEquals(2L, oldLeader.state.commitIndex());

        // StateMachine trên node-1 cập nhật vé của Dave
        assertEquals("BOOKED", oldLeader.stateMachine.seatStatus("G02").orElse(null));
        assertEquals(daveResult.ticketId(), oldLeader.stateMachine.seatCurrentTicketId("G02").orElse(null));

        // Write mồ côi không sinh CONFIRMED
        assertFalse(ghostFuture.isDone());
    }

    // -------------------------------------------------------------------------------------------
    // Test 10: Full Cluster Log and State Machine Convergence Across All 5 Nodes
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. Hàn gắn mạng: Toàn bộ 5 node hội tụ đồng nhất về Term, Log, CommitIndex và StateMachine")
    void networkHeal_fullClusterConvergenceAcrossAllFiveNodes() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(101), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Phía 2/5 ghi dở dang
        cluster.node("node-1").submitBooking(reqId(102), "SE-001", "G02", "Minority Ghost");
        network.deliverAllAppends();

        // Phía 3/5 bầu node-3 term 2 và commit G02 cho Dave
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        node3.bookTicket(reqId(103), "SE-001", "G02", "Dave");

        // Hàn gắn và đồng bộ
        network.heal();
        node3.replicateToPeers();
        network.deliverAllAppends();
        node3.sendHeartbeat();
        network.deliverAllAppends();

        // Kiểm tra hội tụ trên tất cả 5 node
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(2L, n.state.currentTerm(), "currentTerm trên " + id + " phải là 2");
            assertEquals(2L, n.state.commitIndex(), "commitIndex trên " + id + " phải là 2");
            assertEquals(2L, n.state.lastApplied(), "lastApplied trên " + id + " phải là 2");
            assertEquals(2L, n.state.lastLogIndex(), "lastLogIndex trên " + id + " phải là 2");

            // Log entries tại index 1 và 2 giống nhau trên toàn cụm
            assertEquals(1L, n.state.entryAt(1L).term());
            assertEquals(2L, n.state.entryAt(2L).term());

            // StateMachine nhất quán
            assertEquals("BOOKED", n.stateMachine.seatStatus("G01").orElse(null));
            assertEquals("BOOKED", n.stateMachine.seatStatus("G02").orElse(null));
            for (int s = 3; s <= 10; s++) {
                String seat = String.format("G%02d", s);
                assertEquals("AVAILABLE", n.stateMachine.seatStatus(seat).orElse("AVAILABLE"));
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 11: Follower Redirection on Recovered Minority Nodes After Healing
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. Hàn gắn mạng: Client gửi yêu cầu vào node-1 hoặc node-2 được chuyển tiếp chính xác về node-3")
    void followerRedirectionAcrossPartitionsAfterHeal() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        network.heal();
        node3.sendHeartbeat();
        network.deliverAllAppends();

        // Thử gửi lệnh BOOK vào node-1 (cựu Leader)
        BookingRequestHandler.NotLeaderException ex1 = assertThrows(
                BookingRequestHandler.NotLeaderException.class,
                () -> cluster.node("node-1").bookTicket(reqId(111), "SE-001", "G03", "Frank"));
        assertEquals("node-3", ex1.leaderHint());

        // Chuyển tiếp theo hint tới node-3 -> Thành công
        ApplyResult result = node3.bookTicket(reqId(111), "SE-001", "G03", "Frank");
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
    }

    // -------------------------------------------------------------------------------------------
    // Test 12: Direct TestNetworkFilter Telemetry & Drop Verification
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("12. Kiểm thử TestNetworkFilter: Chặn đúng 12 directed link phân vùng 3–2 và ghi nhận metrics")
    void testNetworkFilter_metricsAndDropVerification() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        Set<String> partA = Set.of("node-1", "node-2");
        Set<String> partB = Set.of("node-3", "node-4", "node-5");

        // Thiết lập phân vùng qua TestNetworkFilter
        network.partition(partA, partB);

        // 2 node trong A và 3 node trong B -> 2 * 3 * 2 = 12 directed links bị chặn
        assertEquals(12, filter.getBlockedLinks().size());
        assertTrue(filter.getBlockedLinks().contains(new TestNetworkFilter.DirectedLink("node-1", "node-3")));
        assertTrue(filter.getBlockedLinks().contains(new TestNetworkFilter.DirectedLink("node-3", "node-1")));
        assertTrue(filter.getBlockedLinks().contains(new TestNetworkFilter.DirectedLink("node-2", "node-5")));
        assertFalse(filter.getBlockedLinks().contains(new TestNetworkFilter.DirectedLink("node-1", "node-2")));
        assertFalse(filter.getBlockedLinks().contains(new TestNetworkFilter.DirectedLink("node-3", "node-4")));

        // Bầu cử term 2 phía đa số
        cluster.node("node-3").startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        assertEquals(Role.LEADER, cluster.node("node-3").state.role());

        // TestNetworkFilter ghi nhận dropped messages xuyên phân vùng
        assertTrue(filter.getDroppedCount() > 0, "Phải có gói tin bị drop khi qua phân vùng");
        assertTrue(filter.getDroppedCount("node-3", "node-1") > 0 || filter.getDroppedCount("node-3", "node-2") > 0);

        // Hàn gắn mạng
        network.heal();
        assertEquals(0, filter.getBlockedLinks().size(), "Hàn gắn mạng phải xóa sạch blocked links");
        long droppedBeforeSync = filter.getDroppedCount();
        long deliveredBeforeSync = filter.getDeliveredCount();

        cluster.node("node-3").sendHeartbeat();
        network.deliverAllAppends();

        assertEquals(droppedBeforeSync, filter.getDroppedCount(), "Không có gói tin nào bị drop sau khi hàn gắn");
        assertTrue(filter.getDeliveredCount() > deliveredBeforeSync, "Số gói delivered phải tăng sau hàn gắn");
    }

    // -------------------------------------------------------------------------------------------
    // Test 13: Selective Drop Rule with Custom Predicate (Drop Only REQUEST_VOTE)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("13. Bộ lọc có chọn lọc: TestNetworkFilter drop duy nhất REQUEST_VOTE, AppendEntries vẫn lưu thông")
    void selectiveDropRule_withCustomPredicate_blocksOnlyRequestVote() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        // Thêm rule vào TestNetworkFilter: drop toàn bộ REQUEST_VOTE
        filter.dropMessageType(MessageType.REQUEST_VOTE);

        // AppendEntries/heartbeat vẫn lưu thông bình thường
        TestNode node1 = cluster.node("node-1");
        ApplyResult result = node1.bookTicket(reqId(131), "SE-001", "G06", "Grace");
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(1L, node1.state.commitIndex());

        // Bất kỳ node nào cố gắng startElection đều bị drop toàn bộ vote requests
        TestNode node2 = cluster.node("node-2");
        node2.startElection();
        network.deliverAllVotes();

        // Node-2 không nhận được bất kỳ vote reply nào từ peer
        assertEquals(Role.CANDIDATE, node2.state.role());
        assertFalse(node2.isLeader());

        // Gỡ bỏ bộ lọc và hàn gắn
        filter.heal();
    }

    // -------------------------------------------------------------------------------------------
    // Test 14: Asymmetric Partition - Isolated Node with 4-Node Functional Quorum
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("14. Phân vùng bất đối xứng: 1 node bị cô lập hoàn toàn, 4 node còn lại vẫn bầu cử và commit bình thường")
    void asymmetricPartition_isolatedNode_fourNodeQuorumRemainsFunctional() throws Exception {
        cluster.electLeader("node-1", 1L);
        cluster.replicateAll();

        // Cô lập hoàn toàn node-5 qua TestNetworkFilter
        filter.isolate("node-5");
        assertTrue(filter.getIsolatedNodes().contains("node-5"));

        // 4 node còn lại {node-1, node-2, node-3, node-4} đủ quorum (3/5)
        TestNode node1 = cluster.node("node-1");
        ApplyResult result = node1.bookTicket(reqId(141), "SE-001", "G07", "Hannah");
        assertEquals(ApplyResult.Outcome.CONFIRMED, result.outcome());
        assertEquals(1L, node1.state.commitIndex());

        // Node-5 bị cô lập không nhận được commit
        TestNode node5 = cluster.node("node-5");
        assertEquals(0L, node5.state.commitIndex());

        // Tái hòa nhập node-5 (de-isolate)
        filter.deisolate("node-5");
        assertFalse(filter.getIsolatedNodes().contains("node-5"));

        node1.replicateToPeers();
        network.deliverAllAppends();

        assertEquals(1L, node5.state.commitIndex());
        assertEquals("BOOKED", node5.stateMachine.seatStatus("G07").orElse(null));
    }

    // -------------------------------------------------------------------------------------------
    // Test 15: Successive Partitions with Alternating Minority and Majority
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("15. Phân vùng liên tiếp đổi bên: 3–2 chu kỳ 1 (node-3 lên) -> heal -> 3–2 chu kỳ 2 (node-1 lên)")
    void successivePartitions_alternatingMinorityAndMajority() throws Exception {
        // Chu kỳ 1: Node-1 term 1 -> Phân vùng {1,2} vs {3,4,5} -> Node-3 đắc cử term 2 -> Heal
        cluster.electLeader("node-1", 1L);
        cluster.node("node-1").bookTicket(reqId(151), "SE-001", "G01", "Alice");
        cluster.replicateAll();

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        TestNode node3 = cluster.node("node-3");
        node3.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        node3.bookTicket(reqId(152), "SE-001", "G02", "Dave");

        network.heal();
        node3.replicateToPeers();
        network.deliverAllAppends();
        assertEquals(2L, cluster.node("node-1").state.commitIndex());

        // Chu kỳ 2: Đổi phân vùng: Thiểu số {node-3, node-4} vs Đa số {node-1, node-2, node-5}
        // Lúc này leader đương nhiệm node-3 rơi vào phía thiểu số!
        network.partition(Set.of("node-3", "node-4"), Set.of("node-1", "node-2", "node-5"));

        // Leader cũ node-3 cố ghi phía 2/5 -> Không thể commit
        CompletableFuture<ApplyResult> stuckFuture =
                node3.submitBooking(reqId(153), "SE-001", "G03", "Stuck Passenger");
        network.deliverAllAppends();
        assertEquals(2L, node3.state.commitIndex());
        assertFalse(stuckFuture.isDone());

        // Phía đa số mới {node-1, node-2, node-5} bầu node-1 làm Leader ở term 3
        TestNode node1 = cluster.node("node-1");
        node1.startElection();
        network.deliverVote("node-1", "node-2", 3L);
        network.deliverVote("node-1", "node-5", 3L);
        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(3L, node1.state.currentTerm());

        // Node-1 commit thành công ghế G03 cho Ian
        ApplyResult ianResult = node1.bookTicket(reqId(154), "SE-001", "G03", "Ian");
        assertEquals(ApplyResult.Outcome.CONFIRMED, ianResult.outcome());
        assertEquals(3L, node1.state.commitIndex());

        // Hàn gắn mạng chu kỳ 2
        network.heal();
        node1.replicateToPeers();
        network.deliverAllAppends();

        // Node-3 giáng chức, ghi đè entry mồ côi
        assertEquals(Role.FOLLOWER, node3.state.role());
        assertEquals(3L, node3.state.currentTerm());
        assertEquals(3L, node3.state.commitIndex());
        assertEquals("BOOKED", node3.stateMachine.seatStatus("G03").orElse(null));
        assertEquals(ianResult.ticketId(), node3.stateMachine.seatCurrentTicketId("G03").orElse(null));
        assertFalse(stuckFuture.isDone());
    }

    // -------------------------------------------------------------------------------------------
    // Test 16: Durable Persistence Invariants Across Partitions
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("16. Bền vững lưu trữ RaftStorage: Term đơn điệu tăng và từ chối bỏ phiếu hai lần trong cùng term")
    void durablePersistenceAcrossPartitions_termMonotonicityAndVoteSafety() throws Exception {
        DurableMemoryStorage storage = new DurableMemoryStorage();
        storage.saveTermAndVote(1L, "node-1");
        assertEquals(1L, storage.term);
        assertEquals("node-1", storage.vote);

        // Term thoái lui bị ném RaftStorageException
        assertThrows(RaftStorageException.class, () -> storage.saveTermAndVote(0L, "node-2"));

        // Cùng term không được đổi vote
        RaftState state = RaftState.restore(1L, "node-1", List.of(), 0L);
        PersistentElectionState persistence = new PersistentElectionState(state, storage);

        assertFalse(persistence.grantVote(new RequestVoteRequest(1L, "node-2", 0L, 0L)),
                "Không được bỏ phiếu cho candidate khác trong cùng term đã vote");

        // Term cao hơn được cấp vote và lưu trữ cập nhật
        assertTrue(persistence.grantVote(new RequestVoteRequest(2L, "node-3", 0L, 0L)));
        assertEquals(2L, storage.term);
        assertEquals("node-3", storage.vote);

        // Khôi phục từ storage đọc đúng giá trị
        PersistentState loaded = storage.load();
        assertEquals(2L, loaded.currentTerm());
        assertEquals("node-3", loaded.votedFor());
    }

    // ===========================================================================================
    // Test Cluster & Fake Network Infrastructure Driven by TestNetworkFilter
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
            Set<String> peers = peersOf(leaderId);
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
            if (state.role() != Role.LEADER) {
                return CompletableFuture.failedFuture(
                        new BookingRequestHandler.NotLeaderException(state.leaderId()));
            }

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
            return state.role() == Role.LEADER;
        }

        @Override
        public String getLeaderHint() {
            return state.leaderId();
        }

        void replicateToPeers() {
            if (state.role() != Role.LEADER) {
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

            if (request.prevLogIndex() > 0L) {
                if (state.lastLogIndex() < request.prevLogIndex()
                        || state.termAt(request.prevLogIndex()) != request.prevLogTerm()) {
                    return new RaftNode.AppendEntriesResponse(state.currentTerm(), false, state.lastLogIndex());
                }
            }

            if (!request.entries().isEmpty()) {
                if (state.lastLogIndex() > request.prevLogIndex()) {
                    state.truncateSuffixFrom(request.prevLogIndex() + 1L);
                }
                for (RaftLogEntry e : request.entries()) {
                    state.append(e);
                }
                storage.replaceSuffix(request.prevLogIndex() + 1L, request.entries());
            }

            if (request.leaderCommit() > state.commitIndex()) {
                long newCommit = Math.min(request.leaderCommit(), state.lastLogIndex());
                if (state.advanceCommitIndex(newCommit)) {
                    applyCommitted(newCommit);
                }
            }

            return new RaftNode.AppendEntriesResponse(state.currentTerm(), true, state.lastLogIndex());
        }

        void handleAppendResponse(String peer, long requestTerm, RaftNode.AppendEntriesResponse response) {
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

    /**
     * Deterministic FakeNetwork directly integrated with {@link TestNetworkFilter}.
     */
    private static final class FakeNetwork {
        final Map<String, TestNode> nodes = new LinkedHashMap<>();
        final Deque<SentVoteRpc> pendingVotes = new ArrayDeque<>();
        final Deque<SentAppendRpc> pendingAppends = new ArrayDeque<>();
        final TestNetworkFilter filter;

        FakeNetwork(TestNetworkFilter filter) {
            this.filter = Objects.requireNonNull(filter, "filter must not be null");
        }

        void registerNode(TestNode node) {
            nodes.put(node.id, node);
        }

        void partition(Set<String> partA, Set<String> partB) {
            // Apply partition rules directly to TestNetworkFilter (TV3-D21)
            filter.partition(partA, partB);
        }

        void heal() {
            // Clear all drop rules in TestNetworkFilter (TV3-D21)
            filter.heal();
        }

        boolean canCommunicate(String from, String to, MessageType type, Object payload) {
            MessageEnvelope<?> envelope = MessageEnvelope.createRequest(type, from, payload);
            return filter.shouldDeliver(from, to, envelope);
        }

        void sendVote(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) {
            if (canCommunicate(from, to, MessageType.REQUEST_VOTE, req)) {
                pendingVotes.add(new SentVoteRpc(from, to, req, reply));
            }
        }

        void sendAppend(String from, String to, RaftNode.AppendEntriesRequest req, Consumer<RaftNode.AppendEntriesResponse> reply) {
            AppendEntriesRequestDto dto = new AppendEntriesRequestDto(
                    req.term(), req.leaderId(), req.prevLogIndex(), req.prevLogTerm(), List.of(), req.leaderCommit());
            if (canCommunicate(from, to, MessageType.APPEND_ENTRIES, dto)) {
                pendingAppends.add(new SentAppendRpc(from, to, req, reply));
            }
        }

        void deliverVote(String from, String to, long term) throws Exception {
            SentVoteRpc target = pendingVotes.stream()
                    .filter(r -> r.from.equals(from) && r.to.equals(to) && r.request.getTerm() == term)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No pending vote RPC from " + from + " to " + to + " term=" + term));
            pendingVotes.remove(target);
            if (canCommunicate(from, to, MessageType.REQUEST_VOTE, target.request)) {
                RequestVoteResponse resp = nodes.get(to).electionService.handleRequestVote(target.request);
                target.reply.receive(resp);
            }
        }

        void deliverAllVotes() throws Exception {
            while (!pendingVotes.isEmpty()) {
                SentVoteRpc rpc = pendingVotes.removeFirst();
                if (canCommunicate(rpc.from, rpc.to, MessageType.REQUEST_VOTE, rpc.request)) {
                    RequestVoteResponse resp = nodes.get(rpc.to).electionService.handleRequestVote(rpc.request);
                    rpc.reply.receive(resp);
                }
            }
        }

        void deliverAllAppends() {
            while (!pendingAppends.isEmpty()) {
                SentAppendRpc rpc = pendingAppends.removeFirst();
                AppendEntriesRequestDto dto = new AppendEntriesRequestDto(
                        rpc.request.term(), rpc.request.leaderId(), rpc.request.prevLogIndex(),
                        rpc.request.prevLogTerm(), List.of(), rpc.request.leaderCommit());
                if (canCommunicate(rpc.from, rpc.to, MessageType.APPEND_ENTRIES, dto)) {
                    RaftNode.AppendEntriesResponse resp = nodes.get(rpc.to).handleAppendRequest(rpc.request);
                    if (canCommunicate(rpc.to, rpc.from, MessageType.APPEND_ENTRIES, resp)) {
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
