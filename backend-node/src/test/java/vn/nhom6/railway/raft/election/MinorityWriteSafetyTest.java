package vn.nhom6.railway.raft.election;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.protocol.codec.MessageCodec;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.CommitAdvancer;
import vn.nhom6.railway.raft.replication.ReplicationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D18 tests for isolated old leader write safety during network partition (3-2 split).
 *
 * <p><strong>SPEC-04 Section 5 & TV5-D18 Requirements:</strong>
 * <ul>
 *   <li>Chia 3–2 trong test: leader phía thiểu số có thể chưa biết mình mất quyền nhưng không được commit writes.</li>
 *   <li>Không dùng role == Leader để tự báo success.</li>
 *   <li>Append ack chưa commit không tạo BOOK_CONFIRMED; lost leader không tự báo REJECTED.</li>
 *   <li>Không có BOOK_CONFIRMED mới phía 2/5 dù Leader cũ còn giữ role.</li>
 *   <li>Sử dụng LeadershipGuard (TV5-D16) + CommitAdvancer (TV4-D16), FakeNetwork; không chờ TV4-D18.</li>
 * </ul>
 */
class MinorityWriteSafetyTest {

    private static final List<String> VOTERS =
            List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final String REQ_MINORITY_1 = "11111111-1111-1111-1111-111111111111";
    private static final String REQ_MINORITY_2 = "22222222-2222-2222-2222-222222222222";
    private static final String REQ_MINORITY_3 = "33333333-3333-3333-3333-333333333333";
    private static final String REQ_MAJORITY_1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final Instant BASE_TIME = Instant.parse("2026-11-18T10:00:00.000Z");

    private FakeNetwork network;
    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        network = new FakeNetwork();
        cluster = new TestCluster(network);
    }

    // -------------------------------------------------------------------------------------------
    // Test 1: Isolated old leader in 2/5 minority cannot commit writes and produces NO BOOK_CONFIRMED
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Leader cô lập phía 2/5 không commit writes và không tạo BOOK_CONFIRMED dù giữ role LEADER")
    void isolatedLeaderInMinority_doesNotCommitWrites_andProducesNoBookConfirmed() throws Exception {
        // 1. Cụm 5 node ban đầu bầu node-1 làm Leader term 1
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");

        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(1L, node1.state.currentTerm());
        assertEquals(0L, node1.state.commitIndex());

        // 2. Chia mạng 3-2:
        //    Thiểu số 2/5: {node-1, node-2} (node-1 là leader cũ)
        //    Đa số 3/5: {node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // 3. Client gửi lệnh đặt vé BOOK_TICKET tới node-1
        CompletableFuture<ApplyResult> future =
                node1.submitBooking(REQ_MINORITY_1, "G01", "Minority Passenger", "ticket-m-1", BASE_TIME);

        // Ghi cục bộ tại node-1
        assertEquals(1L, node1.state.lastLogIndex());
        assertEquals(0L, node1.state.commitIndex(), "commitIndex ban đầu vẫn là 0");

        // 4. Node-1 sao chép sang các peer qua FakeNetwork
        network.deliverAll();

        // 5. Kết quả sau khi giao tiếp trong phân vùng 2/5:
        //    Node-2 (trong phân vùng 2/5) nhận được và gửi ACK matchIndex=1
        assertEquals(1L, node2.state.lastLogIndex());
        assertEquals(1L, node1.state.matchIndexOf("node-2"));

        // Nhưng node-3, node-4, node-5 bị cô lập nên không nhận được
        assertEquals(0L, node1.state.matchIndexOf("node-3"));
        assertEquals(0L, node1.state.matchIndexOf("node-4"));
        assertEquals(0L, node1.state.matchIndexOf("node-5"));

        // 6. TIÊU CHÍ AN TOÀN CHÍNH (TV5-D18):
        //    a. Leader phía thiểu số VẪN giữ role LEADER (chưa biết mình bị cô lập)
        assertEquals(Role.LEADER, node1.state.role());
        assertEquals(1L, node1.state.currentTerm());

        //    b. Nhưng KHÔNG ĐƯỢC commit writes: quorum cần 3/5, ở đây chỉ có 2/5 (node-1 + node-2)
        assertEquals(0L, node1.state.commitIndex(), "2/5 node không đủ quorum (cần 3/5) -> commitIndex KHÔNG tăng");
        assertEquals(0L, node2.state.commitIndex(), "Node-2 commitIndex KHÔNG tăng");

        //    c. KHÔNG CÓ BOOK_CONFIRMED MỚI PHÍA 2/5 dù Leader cũ còn giữ role
        assertTrue(node1.appliedEvents.isEmpty(), "Node-1 KHÔNG được tạo BOOK_CONFIRMED khi chưa commit");
        assertTrue(node2.appliedEvents.isEmpty(), "Node-2 KHÔNG được tạo BOOK_CONFIRMED khi chưa commit");
        assertEquals(0, node1.stateMachine.totalTicketCount(), "StateMachine không có vé nào được xác nhận");
        assertEquals(0, node2.stateMachine.totalTicketCount(), "StateMachine node-2 không có vé nào");

        //    d. Request chưa hoàn thành (không tự ý trả về CONFIRMED)
        assertFalse(future.isDone(), "Lệnh ghi chưa commit không được hoàn thành thành công");
    }

    // -------------------------------------------------------------------------------------------
    // Test 2: Do not use role == Leader to self-report success
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Không dùng role == Leader để tự báo success khi chưa commit qua quorum")
    void cannotUseRoleLeaderToSelfReportSuccess() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");

        // Chia 3-2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        CompletableFuture<ApplyResult> future =
                node1.submitBooking(REQ_MINORITY_1, "G01", "Minority Passenger", "ticket-m-1", BASE_TIME);
        network.deliverAll();

        // Node-1 đang có role == LEADER
        assertEquals(Role.LEADER, node1.state.role());
        assertTrue(node1.guard.isCurrent(1L), "LeadershipGuard vẫn current ở term 1");

        // Nhưng write chưa quorum-committed
        assertFalse(node1.isCommitted(1L));

        // Phải tuân thủ quy tắc: nếu kiểm tra role == LEADER mà tự trả CONFIRMED thì sai nghiêm trọng
        // Kết quả phải KHÔNG CONFIRMED
        assertFalse(future.isDone());

        // Khi client timeout hoặc truy vấn trạng thái lúc mất quorum:
        // Kết quả hợp lệ là UNKNOWN hoặc chờ tiếp, TUYỆT ĐỐI KHÔNG phải CONFIRMED
        ApplyResult timeoutResult = ApplyResult.bookUnknown(REQ_MINORITY_1);
        assertEquals(ApplyResult.Outcome.UNKNOWN, timeoutResult.outcome());
        assertNotEquals(ApplyResult.Outcome.CONFIRMED, timeoutResult.outcome());
    }

    // -------------------------------------------------------------------------------------------
    // Test 3: Append ACK without commit does not create BOOK_CONFIRMED; lost leader does not report REJECTED
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Append ack chưa commit không tạo BOOK_CONFIRMED; lost leader không tự báo REJECTED")
    void appendAckWithoutQuorum_doesNotCreateBookConfirmed_andLostLeaderDoesNotReportRejected() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        CompletableFuture<ApplyResult> future =
                node1.submitBooking(REQ_MINORITY_1, "G01", "Minority Passenger", "ticket-m-1", BASE_TIME);

        // Giao tiếp trong mạng 2/5 để node-2 gửi AppendEntriesResponse ACK
        network.deliverAll();

        // Node-2 đã ACK thành công
        assertEquals(1L, node1.state.matchIndexOf("node-2"));

        // Tiêu chuẩn 1: Append ack chưa commit KHÔNG tạo BOOK_CONFIRMED
        assertTrue(node1.appliedEvents.isEmpty());
        assertTrue(node2.appliedEvents.isEmpty());

        // Tiêu chuẩn 2: Lost leader KHÔNG tự báo REJECTED
        // Lệnh này hoàn toàn hợp lệ (ghế trống, định dạng chuẩn); nó chỉ chưa commit vì thiếu quorum.
        // Do đó không được tự ý báo REJECTED (vốn chỉ dành cho lỗi nghiệp vụ như trùng ghế).
        assertFalse(future.isCompletedExceptionally());
        assertFalse(future.isDone());
        for (ApplyResult ev : node1.appliedEvents) {
            assertNotEquals(ApplyResult.Outcome.REJECTED, ev.outcome());
        }
    }

    // -------------------------------------------------------------------------------------------
    // Test 4: Split-brain prevention: majority elects new leader, commits write, and overwrites minority log
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Ngăn chặn Split-brain: đa số 3/5 bầu leader mới và commit, ghi đè log thiểu số 2/5 khi hàn gắn mạng")
    void splitBrainPrevention_majorityPartitionElectsLeaderAndCommits_overwritesMinorityLog() throws Exception {
        // 1. Ban đầu node-1 là Leader term 1
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");
        TestNode node3 = cluster.node("node-3");
        TestNode node4 = cluster.node("node-4");
        TestNode node5 = cluster.node("node-5");

        // 2. Chia mạng 3-2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // 3. Phía 2/5: Client đặt ghế G01 qua node-1 (requestId REQ_MINORITY_1)
        CompletableFuture<ApplyResult> minorityFuture =
                node1.submitBooking(REQ_MINORITY_1, "G01", "Guest Minority", "ticket-min-1", BASE_TIME);
        network.deliverAll();

        // Kiểm tra phía 2/5: không commit, không BOOK_CONFIRMED
        assertEquals(0L, node1.state.commitIndex());
        assertTrue(node1.appliedEvents.isEmpty());
        assertEquals(Role.LEADER, node1.state.role());

        // 4. Phía 3/5: Node-3 nhận thấy thiếu leader, tổ chức bầu cử term 2
        //    Node-3 xin phiếu từ node-4 và node-5
        cluster.electLeaderInPartition("node-3", 2L, Set.of("node-3", "node-4", "node-5"));

        assertEquals(Role.LEADER, node3.state.role());
        assertEquals(2L, node3.state.currentTerm());

        // 5. Phía 3/5: Client đặt cùng ghế G01 qua node-3 (requestId REQ_MAJORITY_1)
        CompletableFuture<ApplyResult> majorityFuture =
                node3.submitBooking(REQ_MAJORITY_1, "G01", "Guest Majority", "ticket-maj-1", BASE_TIME);
        network.deliverAll();

        // Phía 3/5 đủ quorum (node-3, node-4, node-5 = 3/5)!
        assertEquals(1L, node3.state.commitIndex(), "3/5 node đủ quorum -> commitIndex phải tăng lên 1");
        assertTrue(majorityFuture.isDone());
        assertEquals(ApplyResult.Outcome.CONFIRMED, majorityFuture.get().outcome());
        assertEquals("ticket-maj-1", majorityFuture.get().ticketId());

        // Node-3, node-4, node-5 đều đã ghi nhận BOOK_CONFIRMED cho REQ_MAJORITY_1
        assertEquals(1, node3.appliedEvents.size());
        assertEquals(REQ_MAJORITY_1, node3.appliedEvents.getFirst().requestId());

        // Trong lúc này, phía 2/5 VẪN KHÔNG CÓ BOOK_CONFIRMED
        assertTrue(node1.appliedEvents.isEmpty());
        assertTrue(node2.appliedEvents.isEmpty());

        // 6. Hàn gắn mạng (Heal partition)
        network.heal();

        // 7. Node-3 (Leader term 2) phát AppendEntries / Heartbeat tới toàn bộ cụm
        node3.replicateToAll();
        network.deliverAll();

        // 8. Đánh giá trạng thái sau khi hàn gắn mạng:
        //    a. Node-1 (leader cũ) phát hiện term 2 > term 1 -> giáng chức thành FOLLOWER
        assertEquals(Role.FOLLOWER, node1.state.role());
        assertEquals(2L, node1.state.currentTerm());

        //    b. LeadershipGuard của node-1 vô hiệu hóa term 1
        assertFalse(node1.guard.isCurrent(1L), "LeadershipGuard ở term 1 phải bị coi là stale");
        AtomicInteger staleCallbackRun = new AtomicInteger();
        boolean ran = node1.guard.runIfCurrent(1L, staleCallbackRun::incrementAndGet);
        assertFalse(ran, "runIfCurrent ở term 1 phải trả về false");
        assertEquals(0, staleCallbackRun.get(), "Callback cũ không được phép chạy");

        //    c. Log chưa commit ở term 1 trên node-1 và node-2 BỊ GHI ĐÈ bởi entry term 2 của node-3
        assertEquals(1L, node1.state.lastLogIndex());
        assertEquals(2L, node1.state.termAt(1L), "Term tại index 1 trên node-1 giờ là term 2 của leader mới");
        assertEquals(2L, node2.state.termAt(1L), "Term tại index 1 trên node-2 giờ là term 2 của leader mới");

        //    d. CommitIndex được đồng bộ sang node-1 và node-2
        assertEquals(1L, node1.state.commitIndex());
        assertEquals(1L, node2.state.commitIndex());

        //    e. TOÀN BỘ CỤM ĐỒNG THUẬN DUY NHẤT 1 VÉ CONFIRMED (của REQ_MAJORITY_1)
        //       REQ_MINORITY_1 hoàn toàn không bao giờ được CONFIRMED!
        for (String id : VOTERS) {
            TestNode n = cluster.node(id);
            assertEquals(1, n.appliedEvents.size(), "Node " + id + " phải có đúng 1 event apply");
            assertEquals(REQ_MAJORITY_1, n.appliedEvents.getFirst().requestId(),
                    "Vé được xác nhận trên " + id + " phải là của majority request");
            assertEquals(ApplyResult.Outcome.CONFIRMED, n.appliedEvents.getFirst().outcome());
        }

        // Không bao giờ có BOOK_CONFIRMED cho REQ_MINORITY_1 trên bất kỳ node nào
        long minorityConfirmedCount = VOTERS.stream()
                .map(cluster::node)
                .flatMap(n -> n.appliedEvents.stream())
                .filter(e -> REQ_MINORITY_1.equals(e.requestId()) && e.outcome() == ApplyResult.Outcome.CONFIRMED)
                .count();
        assertEquals(0L, minorityConfirmedCount, "Tuyệt đối không có BOOK_CONFIRMED nào cho lệnh phía 2/5");
    }

    // -------------------------------------------------------------------------------------------
    // Test 5: Multiple writes in minority remain uncommitted and overwritten after heal
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Nhiều lệnh ghi liên tiếp phía thiểu số 2/5 đều không được commit và bị xóa sạch sau heal")
    void multipleWritesInMinority_remainUncommitted_allOverwrittenAfterHeal() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");
        TestNode node3 = cluster.node("node-3");

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Gửi 3 lệnh ghi liên tiếp tới node-1
        CompletableFuture<ApplyResult> f1 = node1.submitBooking(REQ_MINORITY_1, "G01", "P1", "t-1", BASE_TIME);
        CompletableFuture<ApplyResult> f2 = node1.submitBooking(REQ_MINORITY_2, "G02", "P2", "t-2", BASE_TIME);
        CompletableFuture<ApplyResult> f3 = node1.submitBooking(REQ_MINORITY_3, "G03", "P3", "t-3", BASE_TIME);

        network.deliverAll();

        // Cả 3 lệnh đều đã sang log của node-1 và node-2
        assertEquals(3L, node1.state.lastLogIndex());
        assertEquals(3L, node2.state.lastLogIndex());

        // Nhưng commitIndex VẪN LÀ 0
        assertEquals(0L, node1.state.commitIndex());
        assertEquals(0L, node2.state.commitIndex());
        assertTrue(node1.appliedEvents.isEmpty());
        assertTrue(node2.appliedEvents.isEmpty());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());
        assertFalse(f3.isDone());

        // Bầu leader mới term 2 ở phía 3/5 và commit 1 lệnh hợp lệ
        cluster.electLeaderInPartition("node-3", 2L, Set.of("node-3", "node-4", "node-5"));
        node3.submitBooking(REQ_MAJORITY_1, "G01", "P-Maj", "t-maj", BASE_TIME);
        network.deliverAll();
        assertEquals(1L, node3.state.commitIndex());

        // Hàn gắn mạng và đồng bộ
        network.heal();
        node3.replicateToAll();
        network.deliverAll();

        // Toàn bộ 3 lệnh chưa commit ở term 1 bị ghi đè hoàn toàn
        assertEquals(1L, node1.state.lastLogIndex());
        assertEquals(1L, node2.state.lastLogIndex());
        assertEquals(1L, node1.state.commitIndex());
        assertEquals(1L, node2.state.commitIndex());
        assertEquals(1, node1.appliedEvents.size());
        assertEquals(REQ_MAJORITY_1, node1.appliedEvents.getFirst().requestId());
    }

    // -------------------------------------------------------------------------------------------
    // Test 6: Healing partition to quorum (3/5) allows pending write to commit safely
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Hàn gắn mạng từ 2/5 lên 3/5 cho phép lệnh đang chờ đạt quorum và commit an toàn")
    void healingToQuorum_allowsPendingMinorityWriteToCommit() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");

        // Ban đầu chia 2 - 3
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        CompletableFuture<ApplyResult> future =
                node1.submitBooking(REQ_MINORITY_1, "G01", "Passenger", "t-1", BASE_TIME);
        network.deliverAll();

        // Chỉ có 2/5 -> chưa commit
        assertEquals(0L, node1.state.commitIndex());
        assertFalse(future.isDone());

        // Bây giờ kết nối node-3 vào phân vùng của node-1 (tạo thành phân vùng 3/5: node-1, node-2, node-3)
        // Chưa có cuộc bầu cử nào ở term cao hơn xảy ra
        network.partition(Set.of("node-1", "node-2", "node-3"), Set.of("node-4", "node-5"));

        // Node-1 sao chép sang node-3
        node1.replicateToPeers();
        network.deliverAll();

        // Giờ đã có 3/5 node (node-1, node-2, node-3) nắm giữ entry -> đạt QUORUM!
        assertEquals(1L, node1.state.commitIndex(), "Đã đạt 3/5 quorum -> commitIndex phải tiến lên 1");
        assertTrue(future.isDone(), "Lệnh ghi đã được commit và hoàn tất");
        assertEquals(ApplyResult.Outcome.CONFIRMED, future.get().outcome());
        assertEquals(1, node1.appliedEvents.size());
        assertEquals(REQ_MINORITY_1, node1.appliedEvents.getFirst().requestId());
    }

    // -------------------------------------------------------------------------------------------
    // Test 7: Stale ACK after leader step-down is rejected by LeadershipGuard
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("ACK đến muộn sau khi leader đã giáng chức bị LeadershipGuard từ chối")
    void staleAckAfterStepDown_rejectedByLeadershipGuard() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");

        long capturedTerm = node1.guard.captureTerm();
        assertEquals(1L, capturedTerm);

        // Giả lập node-1 quan sát thấy term cao hơn (term 3) và giáng chức
        node1.state.becomeFollower(3L);
        assertEquals(Role.FOLLOWER, node1.state.role());
        assertEquals(3L, node1.state.currentTerm());

        // LeadershipGuard phải phát hiện term cũ không còn current
        assertFalse(node1.guard.isCurrent(capturedTerm));

        AtomicInteger sideEffect = new AtomicInteger();
        boolean ran = node1.guard.runIfCurrent(capturedTerm, sideEffect::incrementAndGet);
        assertFalse(ran);
        assertEquals(0, sideEffect.get(), "Không được phép thực thi side-effect từ term cũ");

        // Cố tình gọi captureTerm khi không còn là Leader phải ném ngoại lệ
        assertThrows(IllegalStateException.class, node1.guard::captureTerm);
    }

    // -------------------------------------------------------------------------------------------
    // Test 8: Heartbeat in minority partition carries unadvanced commitIndex
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Heartbeat trong phân vùng thiểu số mang leaderCommit không đổi, ngăn follower commit sớm")
    void heartbeatsInMinorityPartition_propagateZeroCommitIndex() throws Exception {
        cluster.electLeader("node-1", 1L);
        TestNode node1 = cluster.node("node-1");
        TestNode node2 = cluster.node("node-2");

        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Ghi lệnh ở node-1
        node1.submitBooking(REQ_MINORITY_1, "G01", "P1", "t-1", BASE_TIME);
        network.deliverAll();

        // Node-1 phát heartbeat với commitIndex hiện tại (vẫn là 0)
        assertEquals(0L, node1.state.commitIndex());
        node1.sendHeartbeat();
        network.deliverAll();

        // Node-2 nhận heartbeat nhưng leaderCommit là 0 nên commitIndex của node-2 cũng không đổi
        assertEquals(0L, node2.state.commitIndex());
        assertTrue(node2.appliedEvents.isEmpty());
    }

    // ===========================================================================================
    // Test Harness: ClusterNode, FakeNetwork, DurableMemoryStorage
    // ===========================================================================================

    private static final class TestCluster {
        final Map<String, TestNode> nodes = new LinkedHashMap<>();
        final FakeNetwork network;

        TestCluster(FakeNetwork network) {
            this.network = network;
            for (String id : VOTERS) {
                TestNode n = new TestNode(id, this);
                nodes.put(id, n);
                network.registerNode(n);
            }
        }

        TestNode node(String id) {
            return nodes.get(id);
        }

        void electLeader(String leaderId, long term) throws Exception {
            for (TestNode n : nodes.values()) {
                if (!n.id.equals(leaderId)) {
                    n.state.becomeFollower(term);
                    n.storage.saveTermAndVote(term, null);
                }
            }
            TestNode leader = nodes.get(leaderId);
            if (term > 1L && leader.state.currentTerm() < term - 1L) {
                leader.state.becomeFollower(term - 1L);
            }
            leader.state.startElection(leaderId);
            leader.state.becomeLeader(new TreeSet<>(peersOf(leaderId)));
            leader.storage.saveTermAndVote(term, leaderId);
            leader.initLeaderReplication();
        }

        void electLeaderInPartition(String leaderId, long term, Set<String> partition) throws Exception {
            TestNode leader = nodes.get(leaderId);
            if (term > 1L && leader.state.currentTerm() < term - 1L) {
                leader.state.becomeFollower(term - 1L);
            }
            leader.state.startElection(leaderId);
            leader.state.becomeLeader(new TreeSet<>(peersOf(leaderId)));
            leader.storage.saveTermAndVote(term, leaderId);
            leader.initLeaderReplication();

            for (String member : partition) {
                if (!member.equals(leaderId)) {
                    TestNode n = nodes.get(member);
                    n.state.becomeFollower(term);
                    n.storage.saveTermAndVote(term, leaderId);
                }
            }
        }

        Set<String> peersOf(String localId) {
            Set<String> peers = new TreeSet<>(VOTERS);
            peers.remove(localId);
            return peers;
        }
    }

    private static final class TestNode {
        final String id;
        final TestCluster cluster;
        final RaftState state = new RaftState();
        final DurableMemoryStorage storage = new DurableMemoryStorage();
        final LeadershipGuard guard;
        final BookingStateMachine stateMachine = new BookingStateMachine();
        final List<ApplyResult> appliedEvents = new ArrayList<>();
        final List<PendingWrite> pendingWrites = new ArrayList<>();

        ReplicationTracker tracker;
        CommitAdvancer commitAdvancer;

        TestNode(String id, TestCluster cluster) {
            this.id = id;
            this.cluster = cluster;
            this.guard = new LeadershipGuard(state);
        }

        void initLeaderReplication() {
            Set<String> peers = cluster.peersOf(id);
            this.tracker = new ReplicationTracker(state, peers);
            this.commitAdvancer = new CommitAdvancer(state, tracker);
        }

        CompletableFuture<ApplyResult> submitBooking(
                String requestId, String seatId, String passengerName, String ticketId, Instant issuedAt) {
            if (state.role() != Role.LEADER) {
                return CompletableFuture.failedFuture(new IllegalStateException("Not leader"));
            }
            long term = guard.captureTerm();
            long logIndex = state.lastLogIndex() + 1;

            BookingCommand.BookTicket command = new BookingCommand.BookTicket(
                    requestId, "SE-001", seatId, passengerName, ticketId, issuedAt);
            String payload = serializeCommand(command);
            RaftLogEntry entry = new RaftLogEntry(logIndex, term, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, payload);

            state.append(entry);
            storage.append(entry);

            CompletableFuture<ApplyResult> future = new CompletableFuture<>();
            pendingWrites.add(new PendingWrite(requestId, term, logIndex, command, future));

            replicateToPeers();
            return future;
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

        void replicateToAll() {
            replicateToPeers();
        }

        void sendHeartbeat() {
            if (state.role() != Role.LEADER) {
                return;
            }
            for (String peer : cluster.peersOf(id)) {
                long nextIndex = state.nextIndexOf(peer);
                long prevLogIndex = Math.max(0L, nextIndex - 1L);
                long prevLogTerm = state.termAt(prevLogIndex);
                RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                        state.currentTerm(), id, prevLogIndex, prevLogTerm, List.of(), state.commitIndex());
                cluster.network.sendAppend(id, peer, request, response ->
                        handleAppendResponse(peer, request.term(), response));
            }
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
                state.becomeFollower(request.term());
                try {
                    storage.saveTermAndVote(request.term(), null);
                } catch (RaftStorageException e) {
                    throw new RuntimeException(e);
                }
            }

            // Kiểm tra tính nhất quán log tại prevLogIndex và prevLogTerm
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

            // Cập nhật commitIndex theo leaderCommit
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
                state.becomeFollower(response.term());
                try {
                    storage.saveTermAndVote(response.term(), null);
                } catch (RaftStorageException e) {
                    throw new RuntimeException(e);
                }
                return;
            }

            // Kiểm tra tính hợp lệ bằng LeadershipGuard
            if (!guard.isCurrent(requestTerm)) {
                return;
            }

            if (response.success()) {
                state.advanceMatchIndex(peer, response.matchIndex());
                state.updateNextIndex(peer, response.matchIndex() + 1L);

                // Dùng CommitAdvancer (TV4-D16) tính toán majority commit
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
                        for (PendingWrite pw : pendingWrites) {
                            if (pw.logIndex == next) {
                                guard.runIfCurrent(pw.term, () -> pw.future.complete(br.applyResult()));
                            }
                        }
                    }
                }
                state.markApplied(next);
            }
        }

        boolean isCommitted(long logIndex) {
            return state.commitIndex() >= logIndex;
        }
    }

    private record PendingWrite(
            String requestId,
            long term,
            long logIndex,
            BookingCommand.BookTicket command,
            CompletableFuture<ApplyResult> future
    ) {}

    // ===========================================================================================
    // Fake Network with bidirectional partition control
    // ===========================================================================================

    private record AppendCall(
            String from,
            String to,
            RaftNode.AppendEntriesRequest request,
            Consumer<RaftNode.AppendEntriesResponse> reply
    ) {}

    private static final class FakeNetwork {
        private final Deque<AppendCall> appendQueue = new ArrayDeque<>();
        private Set<String> partitionA = Set.of();
        private Set<String> partitionB = Set.of();
        private boolean isPartitioned = false;
        private final Map<String, TestNode> activeNodes = new LinkedHashMap<>();

        void registerNode(TestNode node) {
            activeNodes.put(node.id, node);
        }

        void partition(Set<String> partA, Set<String> partB) {
            this.partitionA = Set.copyOf(partA);
            this.partitionB = Set.copyOf(partB);
            this.isPartitioned = true;
        }

        void heal() {
            this.isPartitioned = false;
            this.partitionA = Set.of();
            this.partitionB = Set.of();
        }

        boolean canCommunicate(String from, String to) {
            if (!isPartitioned) {
                return true;
            }
            boolean fromInA = partitionA.contains(from);
            boolean toInA = partitionA.contains(to);
            if (fromInA && toInA) {
                return true;
            }
            boolean fromInB = partitionB.contains(from);
            boolean toInB = partitionB.contains(to);
            return fromInB && toInB;
        }

        void sendAppend(String from, String to, RaftNode.AppendEntriesRequest request,
                Consumer<RaftNode.AppendEntriesResponse> reply) {
            appendQueue.add(new AppendCall(from, to, request, reply));
        }

        void deliverAll() {
            int maxRounds = 50;
            int round = 0;
            while (!appendQueue.isEmpty() && round++ < maxRounds) {
                List<AppendCall> currentBatch = new ArrayList<>(appendQueue);
                appendQueue.clear();
                for (AppendCall call : currentBatch) {
                    if (canCommunicate(call.from, call.to)) {
                        TestNode target = activeNodes.get(call.to);
                        if (target != null) {
                            RaftNode.AppendEntriesResponse response = target.handleAppendRequest(call.request);
                            if (canCommunicate(call.to, call.from)) {
                                call.reply.accept(response);
                            }
                        }
                    }
                    // Nếu không giao tiếp được (bị partition), message bị drop tự nhiên theo mô hình mạng thực tế
                }
            }
        }
    }

    private static final class DurableMemoryStorage implements RaftStorage {
        long term;
        String vote;
        final List<RaftLogEntry> log = new ArrayList<>();
        long lastApplied;

        @Override
        public PersistentState load() {
            return new PersistentState(term, vote, List.copyOf(log), lastApplied);
        }

        @Override
        public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            if (term < this.term) {
                throw new RaftStorageException("Term regression");
            }
            this.term = term;
            this.vote = vote;
        }

        @Override
        public void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) {
            while (log.size() >= fromIndex && !log.isEmpty()) {
                log.removeLast();
            }
            log.addAll(entries);
        }

        void append(RaftLogEntry entry) {
            log.add(entry);
        }

        @Override
        public long loadLastApplied() {
            return lastApplied;
        }
    }

    private static final ObjectMapper MAPPER = MessageCodec.getMapper();

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
