package vn.nhom6.railway.raft.election;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.AvailabilityStatus;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutOutcome;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.TimeoutVerificationSituation;
import vn.nhom6.railway.raft.election.WriteAvailabilityPolicy.WriteTimeoutResolution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV5-D19: Unit and invariant test suite for {@link WriteAvailabilityPolicy}.
 *
 * <p><strong>Key Requirements Verified:</strong>
 * <ul>
 *   <li>Theo dõi khả năng cam kết chứ không dựa vào ping đơn (Single ping is NOT quorum).</li>
 *   <li>Write availability under 3/5 majority vs 2/5 minority partitions.</li>
 *   <li>Timeout ghi trả UNKNOWN/NO_QUORUM theo tình huống xác minh.</li>
 *   <li>Tránh kết luận rollback entry chưa commit: uncommitted entries must NEVER be discarded or reported as REJECTED.</li>
 *   <li>CommitAdvancer TV4-D16 + FakeReplicationTracker integration without waiting for TV4-D19 ReadBarrier.</li>
 * </ul>
 */
class WriteAvailabilityPolicyTest {

    private static final String LEADER_ID = "node-1";
    private static final Set<String> PEERS = Set.of("node-2", "node-3", "node-4", "node-5");
    private static final Instant T0 = Instant.parse("2026-11-19T10:00:00.000Z");

    private RaftState state;
    private FakeReplicationTracker tracker;
    private WriteAvailabilityPolicy policy;

    @BeforeEach
    void setUp() {
        state = new RaftState();
        state.startElection(LEADER_ID);
        state.becomeLeader(PEERS);

        tracker = new FakeReplicationTracker(LEADER_ID, state, PEERS);
        policy = tracker.policy();
    }

    // -------------------------------------------------------------------------------------------
    // 1. Quorum & Topology Tests
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Cụm 5 voter có majority quorum = 3 (3/5)")
    void clusterTopology_fiveVotersRequiresThreeForQuorum() {
        assertEquals(5, policy.clusterSize(), "5 voters in total");
        assertEquals(3, policy.quorumSize(), "Majority quorum is 3 (3/5)");
        assertEquals(LEADER_ID, policy.localId());
        assertEquals(PEERS, policy.peers());
    }

    // -------------------------------------------------------------------------------------------
    // 2. Ping vs Commit Capability ("Theo dõi khả năng cam kết chứ không dựa vào ping đơn")
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Ping đơn tới 1 peer không tạo thành quorum ghi (chỉ có 2/5 node -> NO_QUORUM)")
    void singlePingToPeer_doesNotEstablishWriteQuorum() {
        // Leader nhận ping từ node-2
        tracker.ping("node-2", T0);

        // Ping count = 2 (leader + node-2), nhưng active commit ACKs = 1 (chỉ leader)
        assertEquals(2, policy.activePingCount(T0), "Leader + 1 pinged peer = 2");
        assertEquals(1, policy.activeVoterCount(T0), "Không có verified commit ACK -> active voter = 1");

        // Quorum cần 3/5: ping đơn KHÔNG đủ để cho phép commit writes
        assertFalse(policy.hasCommitQuorum(T0));
        assertFalse(policy.canCommitWrites(T0));

        WriteAvailabilityPolicy.AvailabilityResult result = policy.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NO_QUORUM, result.status());
        assertTrue(result.isNoQuorum());
        assertFalse(result.isAvailable());
        assertTrue(result.message().contains("Write quorum unavailable"));
    }

    @Test
    @DisplayName("Nhận đủ verified commit ACKs từ 2 peer (tổng 3/5 node) mới có quorum AVAILABLE")
    void verifiedAcksFromTwoPeers_establishesWriteQuorum() {
        // node-2 và node-3 gửi verified commit ACK
        tracker.acknowledge("node-2", 0L, T0);
        tracker.acknowledge("node-3", 0L, T0);

        // Leader (1) + 2 peers = 3 voters active >= quorum (3)
        assertEquals(3, policy.activeVoterCount(T0));
        assertTrue(policy.hasCommitQuorum(T0));
        assertTrue(policy.canCommitWrites(T0));

        WriteAvailabilityPolicy.AvailabilityResult result = policy.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.AVAILABLE, result.status());
        assertTrue(result.isAvailable());
        assertFalse(result.isNoQuorum());
        assertEquals(LEADER_ID, result.leaderHint());
    }

    // -------------------------------------------------------------------------------------------
    // 3. Minority (2/5) Partition vs Majority (3/5) Write Replication
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Phân vùng thiểu số 2/5 không thể commit write; phân vùng 3/5 commit thành công")
    void minorityPartition_cannotCommitWrites_majorityCommits() {
        // Leader append entry index 1 tại term 1
        RaftLogEntry entry1 = new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}");
        state.append(entry1);

        // Phân vùng mạng 3-2:
        // Thiểu số: {node-1, node-2}
        // Đa số: {node-3, node-4, node-5}
        tracker.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));

        // Chỉ node-2 trong phân vùng thiểu số nhận và ACK matchIndex=1
        tracker.acknowledge("node-2", 1L, T0);

        // Kiểm tra khả năng commit tại index 1:
        // Chỉ có 2 node (node-1 + node-2) nắm entry 1 -> < 3 -> canCommitIndex = false
        assertFalse(policy.canCommitIndex(1L));

        // CommitAdvancer không thể nâng commitIndex
        OptionalLong advanced = tracker.tryAdvanceCommit();
        assertTrue(advanced.isEmpty(), "CommitAdvancer không nâng commit khi chỉ có 2/5");
        assertEquals(0L, state.commitIndex());

        // write availability báo NO_QUORUM
        WriteAvailabilityPolicy.AvailabilityResult avail = policy.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NO_QUORUM, avail.status());

        // Hàn gắn mạng: node-3 kết nối lại và ACK matchIndex=1
        tracker.heal("node-3");
        tracker.acknowledge("node-3", 1L, T0);

        // Bây giờ 3 node (node-1, node-2, node-3) nắm entry 1 -> đạt quorum 3/5
        assertTrue(policy.canCommitIndex(1L));

        // CommitAdvancer nâng commitIndex lên 1
        OptionalLong commitNew = tracker.tryAdvanceCommit();
        assertTrue(commitNew.isPresent());
        assertEquals(1L, commitNew.getAsLong());
        assertEquals(1L, state.commitIndex());
        assertEquals(AvailabilityStatus.AVAILABLE, policy.checkWriteAvailability(T0).status());
    }

    // -------------------------------------------------------------------------------------------
    // 4. Timeout Resolution ("timeout ghi trả UNKNOWN/NO_QUORUM theo tình huống xác minh")
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Timeout khi xác minh mất quorum trả về NO_QUORUM")
    void timeoutWithVerifiedNoQuorum_returnsNoQuorumResolution() {
        // Appended locally index 1
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));

        // Khi timeout xảy ra trong phân vùng 2/5 (xác minh mất quorum)
        WriteTimeoutResolution resolution = policy.resolveTimeout(
                1L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);

        assertEquals(TimeoutOutcome.NO_QUORUM, resolution.outcome());
        assertEquals("NO_QUORUM", resolution.errorCode());
        assertEquals("The command could not be committed by a quorum", resolution.message());
        assertTrue(resolution.isNoQuorum());
        assertFalse(resolution.isUnknown());
        assertFalse(resolution.isCommitted());

        // QUY TẮC AN TOÀN: uncommitted entry vẫn được giữ lại, KHÔNG bị rollback
        assertTrue(resolution.uncommittedEntryRetained());
        assertEquals(1L, state.lastLogIndex(), "Entry vẫn nằm trong log cục bộ");
    }

    @Test
    @DisplayName("Timeout tình huống không xác định (indeterminate/mất phản hồi) trả về UNKNOWN (TIMEOUT_UNKNOWN)")
    void timeoutIndeterminate_returnsUnknownResolution() {
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));

        // Entry đã gửi đi nhưng mất phản hồi / mạng trễ -> tình huống INDETERMINATE
        WriteTimeoutResolution resolution = policy.resolveTimeout(
                1L, 1L, true, TimeoutVerificationSituation.INDETERMINATE);

        assertEquals(TimeoutOutcome.UNKNOWN, resolution.outcome());
        assertEquals("TIMEOUT_UNKNOWN", resolution.errorCode());
        assertTrue(resolution.message().contains("query or retry with same requestId"));
        assertTrue(resolution.isUnknown());
        assertFalse(resolution.isNoQuorum());

        // uncommitted entry vẫn được giữ lại
        assertTrue(resolution.uncommittedEntryRetained());
        assertEquals(1L, state.lastLogIndex());
    }

    @Test
    @DisplayName("Timeout khi entry đã kịp commit trả về COMMITTED")
    void timeoutWhenEntryActuallyCommitted_returnsCommittedResolution() {
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));
        tracker.acknowledgeQuorum(1L, T0);
        tracker.tryAdvanceCommit();
        assertEquals(1L, state.commitIndex(), "Entry đã được commit bởi quorum");

        // Xử lý timeout khi entry đã commit
        WriteTimeoutResolution resolution = policy.resolveTimeout(
                1L, 1L, true, TimeoutVerificationSituation.INDETERMINATE);

        assertEquals(TimeoutOutcome.COMMITTED, resolution.outcome());
        assertNull(resolution.errorCode());
        assertTrue(resolution.isCommitted());
        assertTrue(resolution.message().contains("successfully committed"));
    }

    @Test
    @DisplayName("Lệnh bị từ chối trước khi append do mất quorum trả về NO_QUORUM")
    void writeRejectedPreAppend_returnsNoQuorum() {
        // Chưa append vào log (logIndex = 0, appendedLocally = false)
        WriteTimeoutResolution resolution = policy.resolveTimeout(
                0L, 1L, false, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);

        assertEquals(TimeoutOutcome.NO_QUORUM, resolution.outcome());
        assertEquals("NO_QUORUM", resolution.errorCode());
    }

    // -------------------------------------------------------------------------------------------
    // 5. Rollback Prevention ("tránh kết luận rollback entry chưa commit")
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Tuyệt đối KHÔNG rollback entry chưa commit khi timeout hoặc mất quorum")
    void uncommittedEntriesMustNeverBeRolledBackOnTimeout() {
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));
        state.append(new RaftLogEntry(2L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-2\"}"));

        // Invariant 1: canRollbackUncommitted luôn là false
        assertFalse(policy.canRollbackUncommitted(1L));
        assertFalse(policy.canRollbackUncommitted(2L));

        // Invariant 2: canReportRejectedOnTimeout luôn là false (không báo REJECTED cho consensus timeout)
        assertFalse(policy.canReportRejectedOnTimeout());

        // Invariant 3: assertUncommittedEntryNotRolledBack chứng minh 2 entry vẫn nguyên vẹn
        policy.assertUncommittedEntryNotRolledBack(1L);
        policy.assertUncommittedEntryNotRolledBack(2L);
        assertEquals(2L, state.lastLogIndex());

        // Timeout resolution cho entry 2
        WriteTimeoutResolution resolution = policy.resolveTimeout(
                2L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);

        assertEquals("NO_QUORUM", resolution.errorCode());
        assertTrue(resolution.uncommittedEntryRetained());

        // Log entry 2 VẪN NẰM TRONG LOG, không bị xóa
        assertEquals(2L, state.lastLogIndex());
        assertEquals("BOOK_TICKET", state.entryAt(2L).commandType());
    }

    @Test
    @DisplayName("assertUncommittedEntryNotRolledBack ném ngoại lệ nếu entry bị xóa trái phép")
    void assertUncommittedEntryNotRolledBack_throwsOnIllegalTruncation() {
        // Chỉ có 1 entry trong log
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));

        // Kiểm tra entry index 2 (không có trong log)
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                policy.assertUncommittedEntryNotRolledBack(2L));
        assertTrue(ex.getMessage().contains("CRITICAL RAFT SAFETY VIOLATION"));
    }

    // -------------------------------------------------------------------------------------------
    // 6. Role & Step Down Handling
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Node không phải LEADER (FOLLOWER hoặc CANDIDATE) trả về NOT_LEADER với leaderHint")
    void nonLeaderRole_returnsNotLeaderWithHint() {
        state.becomeFollower(2L);

        WriteAvailabilityPolicy.AvailabilityResult result = policy.checkWriteAvailability(T0);
        assertEquals(AvailabilityStatus.NOT_LEADER, result.status());
        assertTrue(result.isNotLeader());
        assertFalse(result.isAvailable());
        assertFalse(policy.canCommitWrites());
        assertEquals(0, policy.activeVoterCount(T0));
    }

    // -------------------------------------------------------------------------------------------
    // 7. Freshness Window Expiration
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("ACK quá hạn so với freshness window làm mất quorum write availability")
    void staleAcksBeyondFreshnessWindow_causeQuorumLoss() {
        // T0: node-2 và node-3 gửi ACK -> AVAILABLE
        tracker.acknowledge("node-2", 0L, T0);
        tracker.acknowledge("node-3", 0L, T0);
        assertTrue(policy.checkWriteAvailability(T0).isAvailable());

        // 2 giây sau (vượt quá freshness window 1500ms):
        Instant tLate = T0.plus(Duration.ofMillis(2000));

        // ACKs đã hết hạn -> active voters chỉ còn lại Leader (1)
        assertEquals(1, policy.activeVoterCount(tLate));
        assertFalse(policy.hasCommitQuorum(tLate));
        assertEquals(AvailabilityStatus.NO_QUORUM, policy.checkWriteAvailability(tLate).status());

        // Nhận lại ACK tươi mới tại tLate -> hồi phục AVAILABLE
        tracker.acknowledge("node-2", 0L, tLate);
        tracker.acknowledge("node-3", 0L, tLate);
        assertEquals(3, policy.activeVoterCount(tLate));
        assertTrue(policy.checkWriteAvailability(tLate).isAvailable());
    }

    // -------------------------------------------------------------------------------------------
    // 8. Figure 8 Safety: Old Term Entry Cannot Be Committed Directly
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Entry thuộc term cũ không được commit trực tiếp dù có 3 node nắm giữ (Figure 8)")
    void oldTermEntry_cannotBeCommittedDirectlyEvenWithQuorum() {
        // Giả lập log entry 1 thuộc term 1
        state.append(new RaftLogEntry(1L, 1L, RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{}"));

        // Giả sử Leader hiện tại đang ở term 2
        // Khi Leader ở term 2, entry 1 (term 1) dù có 3 node nắm giữ cũng không được commit trực tiếp
        // (chỉ được commit gián tiếp khi có 1 entry của term 2 được commit)
        // Để kiểm tra rule này: tăng term lên 2 trong RaftState
        state.becomeFollower(1L);
        state.startElection(LEADER_ID);
        state.becomeLeader(PEERS); // term 2

        tracker.acknowledge("node-2", 1L, T0);
        tracker.acknowledge("node-3", 1L, T0);

        // canCommitIndex(1L) phải trả về false vì termAt(1) = 1 != currentTerm(2)
        assertFalse(policy.canCommitIndex(1L), "Entry term cũ không được commit trực tiếp");
        assertTrue(tracker.tryAdvanceCommit().isEmpty());
    }

    // -------------------------------------------------------------------------------------------
    // 9. Post-Timeout Healing: Uncommitted Entry Commits on Quorum Restoration
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Sau timeout ghi, mạng hàn gắn cho phép entry chưa commit được commit an toàn")
    void afterTimeout_healingAllowsUncommittedEntryToCommitSafely() {
        // 1. Entry 1 được append
        state.append(new RaftLogEntry(1L, 1L, "BOOK_TICKET", "{\"requestId\":\"req-1\"}"));

        // 2. Chia mạng 3-2, chỉ node-2 ACK
        tracker.partition(Set.of("node-1", "node-2"), Set.of("node-3", "node-4", "node-5"));
        tracker.acknowledge("node-2", 1L, T0);

        // 3. Timeout xảy ra -> trả NO_QUORUM, nhưng entry KHÔNG bị rollback
        WriteTimeoutResolution timeoutRes = policy.resolveTimeout(
                1L, 1L, true, TimeoutVerificationSituation.VERIFIED_NO_QUORUM);
        assertEquals(TimeoutOutcome.NO_QUORUM, timeoutRes.outcome());
        assertTrue(timeoutRes.uncommittedEntryRetained());
        assertEquals(1L, state.lastLogIndex());

        // 4. Mạng hàn gắn, node-4 và node-5 kết nối lại và ACK entry 1
        tracker.healAll();
        tracker.acknowledge("node-4", 1L, T0);
        tracker.acknowledge("node-5", 1L, T0);

        // 5. Bây giờ 4 node nắm entry 1 -> commit index nâng lên 1
        OptionalLong committed = tracker.tryAdvanceCommit();
        assertTrue(committed.isPresent());
        assertEquals(1L, committed.getAsLong());
        assertEquals(1L, state.commitIndex());

        // 6. Client retry cùng requestId: kết quả bây giờ là COMMITTED
        WriteTimeoutResolution retryRes = policy.resolveTimeout(
                1L, 1L, true, TimeoutVerificationSituation.INDETERMINATE);
        assertEquals(TimeoutOutcome.COMMITTED, retryRes.outcome());
        assertTrue(retryRes.isCommitted());
    }
}
