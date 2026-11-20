package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D20: Five-peer fake network tests for Raft Election, Recovery, Split Vote,
 * and Quorum (3/5 vs 2/5) Write Availability before Gate G5.
 *
 * <p>Validates:
 * <ul>
 *   <li>Split vote handling and re-election in subsequent terms.</li>
 *   <li>Minority 2/5 partition cannot commit writes; reports NO_QUORUM via WriteAvailabilityPolicy.</li>
 *   <li>Majority 3/5 partition re-elects a new leader and safely commits writes.</li>
 *   <li>Leader step-down on higher term and log reconciliation upon network healing.</li>
 *   <li>Timeout resolution with deterministic error codes (NO_QUORUM vs TIMEOUT_UNKNOWN vs COMMITTED).</li>
 *   <li>Prepared live fault scenarios for Gate G5.</li>
 * </ul>
 */
class RaftElectionServiceTest {

    private static final List<String> VOTERS = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
    private static final Instant T0 = Instant.parse("2026-11-20T10:00:00.000Z");

    private FakeClusterNetwork network;

    @BeforeEach
    void setUp() {
        network = new FakeClusterNetwork();
    }

    // -------------------------------------------------------------------------------------------
    // 1. Cluster Topology & Invariants
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Cấu hình cụm 5 voter có quorum = 3 (3/5) và phân định chính xác voter/non-voter")
    void clusterTopology_andVoterMembership() {
        TestClusterNode node1 = network.node("node-1");
        RaftElectionService svc = node1.service;

        assertEquals(5, svc.clusterSize());
        assertEquals(3, svc.quorumSize());
        assertEquals("node-1", svc.localId());
        assertEquals(Set.of("node-2", "node-3", "node-4", "node-5"), svc.peers());

        assertTrue(svc.isVoter("node-1"));
        assertTrue(svc.isVoter("node-5"));
        assertFalse(svc.isVoter("node-6"));
        assertFalse(svc.isVoter("client-1"));
        assertFalse(svc.isVoter(null));

        assertEquals(Role.FOLLOWER, svc.role());
        assertFalse(svc.isLeader());
        assertTrue(svc.isFollower());
    }

    // -------------------------------------------------------------------------------------------
    // 2. Split Vote & Re-election across Terms
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Chia phiếu (Split Vote): không ai đạt 3/5 -> retry term mới và bầu thành công Leader với 3/5 phiếu")
    void splitVote_retriesInNewTerm_andElectsLeader() throws Exception {
        // Cả 5 node đều khởi động chiến dịch ở term 1 cùng lúc
        for (String id : VOTERS) {
            network.node(id).service.startElection();
        }

        // Mỗi node tự bầu cho chính mình ở term 1
        for (String id : VOTERS) {
            assertEquals(1L, network.node(id).state.currentTerm());
            assertEquals(id, network.node(id).storage.vote);
        }

        // Giao toàn bộ RequestVote của term 1
        network.deliverAll();

        // Do mọi node đã tự vote, tất cả các RequestVote term 1 tới peer đều bị từ chối
        // Không ai đạt majority 3/5 -> Leader count = 0
        assertEquals(0, network.leaderCount(), "Split vote: không có candidate nào đạt 3 phiếu");
        for (String id : VOTERS) {
            assertFalse(network.node(id).service.isLeader());
            assertFalse(network.node(id).service.hasWonElection());
        }

        // Timeout phân giải split vote: node-1 retry bầu cử ở term 2
        long newTerm = network.node("node-1").service.retryElection();
        assertEquals(2L, newTerm);
        assertEquals(Role.CANDIDATE, network.node("node-1").service.role());
        assertEquals(1, network.node("node-1").service.grantedVoteCount(), "Self vote = 1");

        // Giao phiếu từ node-1 tới node-2
        network.deliverVote("node-1", "node-2", 2L);
        assertEquals(2, network.node("node-1").service.grantedVoteCount());
        assertFalse(network.node("node-1").service.isLeader(), "2 phiếu chưa đủ quorum 3/5");

        // Giao phiếu từ node-1 tới node-3 -> Đạt 3 phiếu (majority 3/5)!
        network.deliverVote("node-1", "node-3", 2L);

        assertEquals(3, network.node("node-1").service.grantedVoteCount());
        assertTrue(network.node("node-1").service.hasWonElection());
        assertEquals(Role.LEADER, network.node("node-1").service.role(), "Đạt 3/5 phiếu -> trở thành LEADER");
        assertEquals(2L, network.node("node-1").state.currentTerm());
        assertEquals(1, network.leaderCount());

        // Giao nốt các tin nhắn còn lại trong mạng
        network.deliverAll();
        assertEquals(1, network.leaderCount(), "Tin nhắn trễ không tạo thêm leader");
        assertEquals("node-1", network.currentLeaderId().orElseThrow());
    }

    // -------------------------------------------------------------------------------------------
    // 3. Quorum Commit Capability (3/5 vs 2/5 Minority No-Commit)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Leader trong phân vùng 2/5 không thể commit writes: WriteAvailability báo NO_QUORUM và commitIndex giữ nguyên")
    void minorityPartition2of5_cannotCommitWrites_writeAvailabilityReportsNoQuorum() throws Exception {
        // 1. Bầu node-1 làm Leader term 1
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");
        TestClusterNode node2 = network.node("node-2");

        assertTrue(node1.service.isLeader());
        assertEquals(1L, node1.state.currentTerm());

        // 2. Chia mạng 3-2:
        //    Thiểu số 2/5: {node-1, node-2} (chứa leader cũ)
        //    Đa số 3/5: {node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // 3. Leader node-1 nhận lệnh ghi, ghi vào log cục bộ
        RaftLogEntry entry1 = new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-m-1\"}");
        node1.appendLocal(entry1);

        // 4. Node-1 sao chép sang node-2 trong phân vùng 2/5
        node1.service.recordPeerAck("node-2", 1L, T0);
        node2.appendLocal(entry1);

        // 5. Đánh giá Quorum Write Availability:
        //    Chỉ có 2 voter active (node-1 + node-2 = 2/5 < 3/5 quorum)
        assertEquals(2, node1.service.activeVoterCount(T0));
        assertFalse(node1.service.canCommitWrites(T0), "2/5 node không thể commit writes");
        assertFalse(node1.service.hasCommitQuorum(T0));

        AvailabilityResult avail = node1.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NO_QUORUM, avail.status());
        assertTrue(avail.isNoQuorum());
        assertFalse(avail.isAvailable());

        // 6. Invariant kiểm thử: commitIndex của node-1 và node-2 KHÔNG THỂ tăng
        assertEquals(0L, node1.state.commitIndex(), "2/5 thiểu số KHÔNG ĐƯỢC commit");
        assertEquals(0L, node2.state.commitIndex(), "Follower trong 2/5 KHÔNG ĐƯỢC commit");

        // 7. Timeout ghi trả về mã NO_QUORUM và uncommitted entry không bị xóa
        WriteTimeoutResolution timeoutRes = node1.service.resolveWriteTimeout(
                1L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, timeoutRes.outcome());
        assertEquals("NO_QUORUM", timeoutRes.errorCode());
        assertTrue(timeoutRes.uncommittedEntryRetained());
        node1.service.assertUncommittedEntryNotRolledBack(1L);
    }

    // -------------------------------------------------------------------------------------------
    // 4. Leader Re-election in Majority Partition (3/5)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Phân vùng đa số 3/5 bầu Leader mới thành công và commit lệnh ghi đạt quorum")
    void majorityPartition3of5_electsNewLeader_andCommitsWrites() throws Exception {
        // Ban đầu node-1 là leader term 1
        network.electLeader("node-1", 1L);

        // Chia 3-2: {node-1, node-2} vs {node-3, node-4, node-5}
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        TestClusterNode node3 = network.node("node-3");
        TestClusterNode node4 = network.node("node-4");
        TestClusterNode node5 = network.node("node-5");

        // Phía 3/5 phát hiện mất heartbeat từ leader cũ -> node-3 timeout và khởi động bầu cử term 2
        long term2 = node3.service.startElection();
        assertEquals(2L, term2);
        assertEquals(Role.CANDIDATE, node3.service.role());

        // Node-3 xin phiếu từ node-4 và node-5
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);

        // Node-3 nhận 3 phiếu (node-3, node-4, node-5) -> Trở thành Leader term 2!
        assertTrue(node3.service.hasWonElection());
        assertEquals(Role.LEADER, node3.service.role());
        assertEquals(2L, node3.state.currentTerm());

        // Phía 3/5 đạt Write Quorum AVAILABLE
        node3.service.recordPeerAck("node-4", 0L, T0);
        node3.service.recordPeerAck("node-5", 0L, T0);

        assertEquals(3, node3.service.activeVoterCount(T0));
        assertTrue(node3.service.canCommitWrites(T0));
        AvailabilityResult avail = node3.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.AVAILABLE, avail.status());
        assertTrue(avail.isAvailable());

        // Node-3 nhận lệnh ghi ở term 2, sao chép sang node-4 và node-5
        RaftLogEntry majEntry = new RaftLogEntry(1L, 2L, "BOOK_TICKET", "{\"requestId\":\"req-maj-1\"}");
        node3.appendLocal(majEntry);
        node4.appendLocal(majEntry);
        node5.appendLocal(majEntry);

        node3.service.recordPeerAck("node-4", 1L, T0);
        node3.service.recordPeerAck("node-5", 1L, T0);

        // Nâng commitIndex trên node-3 thành 1 (đủ 3/5 node xác nhận)
        node3.state.advanceCommitIndex(1L);
        assertEquals(1L, node3.state.commitIndex(), "3/5 quorum commit thành công!");

        // Timeout check cho entry đã commit trả về COMMITTED
        WriteTimeoutResolution res = node3.service.resolveWriteTimeout(
                1L, 2L, true, TimeoutVerificationSituation.VERIFIED_COMMITTED);
        assertEquals(TimeoutOutcome.COMMITTED, res.outcome());
        assertNull(res.errorCode());
    }

    // -------------------------------------------------------------------------------------------
    // 5. Network Healing, Step-Down & Split-Brain Prevention
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Hàn gắn mạng: Leader cũ bị cô lập phát hiện term cao hơn, giáng chức và log được đồng nhất")
    void networkHeal_stepsDownOldLeader_andOverwritesMinorityUncommittedEntry() throws Exception {
        // Node-1 là leader term 1
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");
        TestClusterNode node2 = network.node("node-2");

        // Chia 3-2
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Lệnh ghi chưa commit ở term 1 trên node-1 và node-2
        RaftLogEntry uncommitted = new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"uncommitted-1\"}");
        node1.appendLocal(uncommitted);
        node2.appendLocal(uncommitted);
        assertEquals(0L, node1.state.commitIndex());

        // Phía 3/5 bầu node-3 làm Leader term 2 và commit 1 lệnh hợp lệ
        network.node("node-3").service.startElection();
        network.deliverVote("node-3", "node-4", 2L);
        network.deliverVote("node-3", "node-5", 2L);
        TestClusterNode node3 = network.node("node-3");
        assertEquals(Role.LEADER, node3.service.role());

        RaftLogEntry committedEntry = new RaftLogEntry(1L, 2L, "BOOK_TICKET", "{\"requestId\":\"committed-2\"}");
        node3.appendLocal(committedEntry);
        node3.state.advanceCommitIndex(1L);

        // Hàn gắn mạng toàn cụm
        network.heal();

        // Node-3 gửi AppendEntries mang term 2 tới node-1
        long termBefore = node1.state.currentTerm();
        assertEquals(1L, termBefore);
        assertTrue(node1.service.isLeader());

        // Node-1 quan sát thấy term 2 > term 1
        node1.service.observeHigherTerm(2L);

        // Node-1 lập tức giáng chức thành FOLLOWER, xóa role LEADER và term cập nhật lên 2
        assertEquals(Role.FOLLOWER, node1.service.role());
        assertEquals(2L, node1.state.currentTerm());
        assertFalse(node1.service.isLeader());
        assertTrue(node1.service.isFollower());
        assertNull(node1.state.votedFor());

        // LeadershipGuard của node-1 vô hiệu hóa term 1 cũ
        assertFalse(node1.service.leadershipGuard().isCurrent(1L));
        AtomicBoolean staleCallbackRan = new AtomicBoolean(false);
        boolean executed = node1.service.leadershipGuard().runIfCurrent(1L, () -> staleCallbackRan.set(true));
        assertFalse(executed);
        assertFalse(staleCallbackRan.get(), "Callback cũ từ term 1 tuyệt đối không được chạy");

        // Ghi đè log chưa commit ở term 1 bằng log của Leader term 2
        node1.state.truncateSuffixFrom(1L);
        node1.state.append(committedEntry);
        node1.state.advanceCommitIndex(1L);

        assertEquals(1L, node1.state.commitIndex());
        assertEquals(committedEntry, node1.state.entryAt(1L));
        assertEquals(2L, node1.state.termAt(1L));
    }

    // -------------------------------------------------------------------------------------------
    // 6. Timeout Resolution Invariants (SPEC-03 §7 & SPEC-04 §8)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Phân giải timeout ghi: VERIFIED_NO_QUORUM trả NO_QUORUM, INDETERMINATE trả TIMEOUT_UNKNOWN, không rollback")
    void writeTimeoutResolution_verifiedNoQuorumVsUnknownVsCommitted() {
        TestClusterNode node1 = network.node("node-1");
        RaftElectionService svc = node1.service;

        // 1. Tình huống VERIFIED_NO_QUORUM: đã xác minh mất quorum
        WriteTimeoutResolution r1 = svc.resolveWriteTimeout(
                5L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, r1.outcome());
        assertEquals("NO_QUORUM", r1.errorCode());
        assertTrue(r1.isNoQuorum());
        assertFalse(r1.isUnknown());
        assertFalse(r1.isCommitted());
        assertTrue(r1.uncommittedEntryRetained(), "Entry uncommitted không bao giờ bị rollback");

        // 2. Tình huống INDETERMINATE: không rõ trạng thái commit phía peer
        WriteTimeoutResolution r2 = svc.resolveWriteTimeout(
                6L, 1L, true, TimeoutVerificationSituation.INDETERMINATE);
        assertEquals(TimeoutOutcome.UNKNOWN, r2.outcome());
        assertEquals("TIMEOUT_UNKNOWN", r2.errorCode());
        assertTrue(r2.isUnknown());
        assertFalse(r2.isNoQuorum());
        assertFalse(r2.isCommitted());
        assertTrue(r2.uncommittedEntryRetained());

        // 3. Tình huống lệnh bị từ chối trước khi append do thiếu quorum
        WriteTimeoutResolution r3 = svc.resolveWriteTimeout(
                0L, 1L, false, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, r3.outcome());
        assertEquals("NO_QUORUM", r3.errorCode());

        // 4. Tình huống VERIFIED_COMMITTED: entry đã commit qua quorum
        node1.state.becomeFollower(1L);
        node1.state.append(new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{\"requestId\":\"req-1\"}"));
        node1.state.advanceCommitIndex(1L);

        WriteTimeoutResolution r4 = svc.resolveWriteTimeout(
                1L, 1L, true, TimeoutVerificationSituation.VERIFIED_COMMITTED);
        assertEquals(TimeoutOutcome.COMMITTED, r4.outcome());
        assertNull(r4.errorCode());
        assertTrue(r4.isCommitted());
    }

    // -------------------------------------------------------------------------------------------
    // 7. Ping vs Commit Quorum ("Theo dõi khả năng cam kết chứ không dựa vào ping đơn")
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Ping đơn tới 1 peer không tạo thành quorum ghi; cần verified commit ACKs từ 2 peer (tổng 3/5)")
    void pingOnly_doesNotEstablishCommitQuorum() throws Exception {
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");

        // Ghi nhận ping từ node-2
        node1.service.recordPeerPing("node-2", T0);

        // Ping đơn chỉ chứng minh 2 node sống, KHÔNG PHẢI commit ACK
        // Active voter count chỉ tính local node = 1
        assertEquals(1, node1.service.activeVoterCount(T0));
        assertFalse(node1.service.canCommitWrites(T0));
        assertFalse(node1.service.hasCommitQuorum(T0));

        AvailabilityResult avail1 = node1.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NO_QUORUM, avail1.status());

        // Giờ ghi nhận verified commit ACKs từ node-2 và node-3
        node1.service.recordPeerAck("node-2", 1L, T0);
        node1.service.recordPeerAck("node-3", 1L, T0);

        // Đạt 3/5 node active commit ACKs!
        assertEquals(3, node1.service.activeVoterCount(T0));
        assertTrue(node1.service.canCommitWrites(T0));
        assertTrue(node1.service.hasCommitQuorum(T0));

        AvailabilityResult avail2 = node1.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.AVAILABLE, avail2.status());
    }

    // -------------------------------------------------------------------------------------------
    // 8. Storage Recovery & Term Monotonicity
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Phục hồi bền vững sau sự cố: Term không giảm, giữ nguyên vote và log đã lưu")
    void storageRecovery_restoresTermAndVote_ensuresMonotonicTerm() throws Exception {
        TestClusterNode node1 = network.node("node-1");
        node1.storage.saveTermAndVote(3L, "node-2");

        PersistentState restored = node1.service.recoverFromStorage();
        assertEquals(3L, restored.currentTerm());
        assertEquals("node-2", restored.votedFor());

        // Giả lập storage bị thoái lui term ném ngoại lệ an toàn
        node1.storage.term = 1L;
        node1.state.becomeFollower(3L);
        assertThrows(RaftStorageException.class, () -> node1.service.recoverFromStorage());
    }

    // -------------------------------------------------------------------------------------------
    // 9. G5 Fault Scenarios (Live Cluster Pre-validation)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("G5 Kịch bản 1: Leader chết -> 4 node còn lại bầu Leader mới thành công")
    void g5FaultScenario1_leaderFailure_majorityReelectsNewLeader() throws Exception {
        network.electLeader("node-1", 1L);
        assertEquals("node-1", network.currentLeaderId().orElseThrow());

        // Leader node-1 bị cô lập / crash hoàn toàn khỏi 4 node còn lại
        network.crashNode("node-1");
        network.partition(Set.of("node-1"), Set.of("node-2", "node-3", "node-4", "node-5"));

        // Node-2 timeout và ứng cử term 2
        TestClusterNode node2 = network.node("node-2");
        node2.service.startElection();
        assertEquals(Role.CANDIDATE, node2.service.role());
        assertEquals(2L, node2.state.currentTerm());

        // Xin phiếu từ node-3, node-4
        network.deliverVote("node-2", "node-3", 2L);
        network.deliverVote("node-2", "node-4", 2L);

        // Node-2 trở thành Leader mới (3/5 phiếu)
        assertTrue(node2.service.hasWonElection());
        assertEquals(Role.LEADER, node2.service.role());
        assertEquals("node-2", network.currentLeaderId().orElseThrow());
    }

    @Test
    @DisplayName("G5 Kịch bản 2: 2 Follower chết -> Cụm còn 3/5 node sống vẫn duy trì quorum ghi")
    void g5FaultScenario2_twoFollowersDown_leaderRetainsQuorumAndCommits() throws Exception {
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");

        // 2 follower (node-4, node-5) bị down / ngắt kết nối
        network.partition(Set.of("node-1", "node-2", "node-3"), Set.of("node-4", "node-5"));

        // Node-1 vẫn nhận ACK từ node-2 và node-3
        node1.service.recordPeerAck("node-2", 1L, T0);
        node1.service.recordPeerAck("node-3", 1L, T0);

        // Cụm còn đúng 3/5 node sống -> QUORUM DUY TRÌ!
        assertEquals(3, node1.service.activeVoterCount(T0));
        assertTrue(node1.service.canCommitWrites(T0));
        AvailabilityResult avail = node1.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.AVAILABLE, avail.status());
    }

    @Test
    @DisplayName("G5 Kịch bản 3: 3 Follower chết -> Cụm chỉ còn 2/5 node sống -> MẤT QUORUM ghi, dừng commit")
    void g5FaultScenario3_threeFollowersDown_losesQuorum_writesPause() throws Exception {
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");

        // 3 follower (node-3, node-4, node-5) chết -> Chỉ còn node-1 và node-2 (2/5)
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        node1.service.recordPeerAck("node-2", 1L, T0);

        // Active voter count = 2/5 < 3/5
        assertEquals(2, node1.service.activeVoterCount(T0));
        assertFalse(node1.service.canCommitWrites(T0));
        assertFalse(node1.service.hasCommitQuorum(T0));

        AvailabilityResult avail = node1.service.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NO_QUORUM, avail.status());
        assertTrue(avail.isNoQuorum());
    }

    @Test
    @DisplayName("G5 Kịch bản 4: 1 Follower hồi phục -> Cụm từ 2/5 lên 3/5 -> HỒI PHỤC QUORUM ghi, tiếp tục commit")
    void g5FaultScenario4_recoveredFollower_restoresWriteQuorum() throws Exception {
        network.electLeader("node-1", 1L);
        TestClusterNode node1 = network.node("node-1");

        // Ban đầu chỉ có node-1 và node-2 (2/5) -> NO_QUORUM
        network.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        node1.service.recordPeerAck("node-2", 1L, T0);
        assertFalse(node1.service.canCommitWrites(T0));

        // Node-3 hồi phục và kết nối lại với Leader
        network.partition(Set.of("node-1", "node-2", "node-3"), Set.of("node-4", "node-5"));
        node1.service.recordPeerAck("node-3", 1L, T0.plusMillis(100));

        // Quorum được khôi phục thành công (3/5 active nodes)!
        assertEquals(3, node1.service.activeVoterCount(T0.plusMillis(100)));
        assertTrue(node1.service.canCommitWrites(T0.plusMillis(100)));
        AvailabilityResult avail = node1.service.checkWriteAvailability(T0.plusMillis(100));
        assertEquals(AvailabilityStatus.AVAILABLE, avail.status());
        assertTrue(avail.isAvailable());
    }

    // ===========================================================================================
    // Test Harness: FakeClusterNetwork, TestClusterNode, DurableMemoryStorage
    // ===========================================================================================

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

    private static final class FakeClusterNetwork {
        final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();
        final Deque<SentVoteRpc> pendingVotes = new ArrayDeque<>();
        final List<SentVoteRpc> deliveredVotes = new ArrayList<>();
        Set<String> partitionA = Set.of();
        Set<String> partitionB = Set.of();
        boolean partitioned = false;

        FakeClusterNetwork() {
            for (String id : VOTERS) {
                TestClusterNode node = new TestClusterNode(id, this);
                nodes.put(id, node);
            }
        }

        TestClusterNode node(String id) {
            return nodes.get(id);
        }

        int leaderCount() {
            return (int) nodes.values().stream().filter(n -> n.service.isLeader()).count();
        }

        Optional<String> currentLeaderId() {
            return nodes.values().stream()
                    .filter(n -> n.service.isLeader())
                    .map(n -> n.id)
                    .findFirst();
        }

        void crashNode(String id) {
            nodes.get(id).state.becomeFollower(nodes.get(id).state.currentTerm());
        }

        void electLeader(String leaderId, long term) throws Exception {
            for (TestClusterNode n : nodes.values()) {
                if (!n.id.equals(leaderId)) {
                    n.state.becomeFollower(term);
                    n.storage.saveTermAndVote(term, null);
                }
            }
            TestClusterNode leader = nodes.get(leaderId);
            if (term > 1L && leader.state.currentTerm() < term - 1L) {
                leader.state.becomeFollower(term - 1L);
            }
            leader.state.startElection(leaderId);
            leader.state.becomeLeader(new TreeSet<>(leader.service.peers()));
            leader.storage.saveTermAndVote(term, leaderId);
        }

        void partition(Set<String> partA, Set<String> partB) {
            this.partitionA = Set.copyOf(partA);
            this.partitionB = Set.copyOf(partB);
            this.partitioned = true;
            for (String a : partA) {
                nodes.get(a).service.partition(partB);
            }
            for (String b : partB) {
                nodes.get(b).service.partition(partA);
            }
        }

        void heal() {
            this.partitioned = false;
            this.partitionA = Set.of();
            this.partitionB = Set.of();
            for (TestClusterNode n : nodes.values()) {
                n.service.healAll();
            }
        }

        boolean canCommunicate(String from, String to) {
            if (!partitioned) {
                return true;
            }
            return (partitionA.contains(from) && partitionA.contains(to))
                    || (partitionB.contains(from) && partitionB.contains(to));
        }

        void sendVote(String from, String to, RequestVoteRequestDto req, CandidateElection.VoteReply reply) {
            SentVoteRpc rpc = new SentVoteRpc(from, to, req, reply);
            if (canCommunicate(from, to)) {
                pendingVotes.add(rpc);
            }
        }

        void deliverVote(String from, String to, long term) throws Exception {
            SentVoteRpc target = pendingVotes.stream()
                    .filter(r -> r.from.equals(from) && r.to.equals(to) && r.request.getTerm() == term)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No pending vote RPC from " + from + " to " + to + " term=" + term));
            pendingVotes.remove(target);
            RequestVoteResponse resp = nodes.get(to).service.handleRequestVote(target.request);
            target.reply.receive(resp);
            deliveredVotes.add(target);
        }

        void deliverAll() throws Exception {
            while (!pendingVotes.isEmpty()) {
                SentVoteRpc rpc = pendingVotes.removeFirst();
                if (canCommunicate(rpc.from, rpc.to)) {
                    RequestVoteResponse resp = nodes.get(rpc.to).service.handleRequestVote(rpc.request);
                    rpc.reply.receive(resp);
                    deliveredVotes.add(rpc);
                }
            }
        }
    }

    private static final class TestClusterNode {
        final String id;
        final RaftState state = new RaftState();
        final DurableMemoryStorage storage = new DurableMemoryStorage();
        final RaftElectionService service;
        final FakeCoreNode core;

        TestClusterNode(String id, FakeClusterNetwork network) {
            this.id = id;
            this.core = new FakeCoreNode(id, state, storage);
            this.service = new RaftElectionService(
                    id,
                    core,
                    state,
                    storage,
                    (peer, req, reply) -> network.sendVote(id, peer, req, reply),
                    null,
                    () -> {
                        // On leader promote: becomeLeader already called on state
                    },
                    () -> {
                        // On step down
                    },
                    null
            );
        }

        void appendLocal(RaftLogEntry entry) {
            state.append(entry);
            storage.append(entry);
        }
    }

    private static final class FakeCoreNode implements RaftNode {
        final String id;
        final RaftState state;
        final PersistentElectionState persistence;

        FakeCoreNode(String id, RaftState state, DurableMemoryStorage storage) {
            this.id = id;
            this.state = state;
            this.persistence = new PersistentElectionState(state, storage);
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

    private static final class DurableMemoryStorage implements RaftStorage {
        long term;
        String vote;
        List<RaftLogEntry> log = new ArrayList<>();

        @Override
        public PersistentState load() {
            return new PersistentState(term, vote, List.copyOf(log), 0);
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
            return 0;
        }

        void append(RaftLogEntry entry) {
            log.add(entry);
        }
    }
}
