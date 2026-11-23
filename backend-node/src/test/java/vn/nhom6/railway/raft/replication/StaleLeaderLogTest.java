package vn.nhom6.railway.raft.replication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D23 stale-old-Leader suffix correction tests.
 *
 * <p>Uses {@link ReplicationCommitInvariantSuiteTest.Harness}, its deterministic five-Node
 * FakeNetwork, real in-harness RaftState/PersistentRaftNode/ReplicationService/apply pipeline,
 * and manual schedulers. No TCP, PostgreSQL, real process, wall-clock election or G6 LAN is used.
 * Production Raft and harness files are not modified.
 */
class StaleLeaderLogTest {

    private static final List<Long> SEEDS = ReplicationCommitInvariantSuiteTest.SEEDS;

    @Test
    @DisplayName("1. isolated old Leader suffix is present, then replaced with the new Leader entry")
    void isolatedOldLeaderSuffixIsReplaced() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.appendCommand(h.commandFor(1));
                h.settle(400, "D23-1 initial");

                long committedBefore = h.leader.state.commitIndex();
                h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                BookingCommand staleCommand = h.commandFor(90);
                long staleIndex = h.appendCommand(staleCommand);
                h.runRounds(40, "D23-1 old Leader suffix");

                RaftLogEntry stale = h.leader.state.entryAt(staleIndex);
                assertEquals(staleIndex, stale.index());
                assertEquals(h.leader.state.currentTerm(), stale.term());
                assertTrue(h.leader.state.entries().contains(stale));
                for (String followerId : List.of("node-2", "node-3", "node-4", "node-5")) {
                    ReplicationCommitInvariantSuiteTest.Node follower = h.node(followerId);
                    assertFalse(follower.state.entries().contains(stale),
                            "stale suffix unexpectedly reached " + followerId);
                }
                assertEquals(committedBefore, h.leader.state.commitIndex());

                h.promote("node-2");
                h.network.unblockAll();
                BookingCommand replacementCommand = h.commandFor(91);
                h.appendCommand(replacementCommand);
                h.propagate("D23-1 rejoin");
                h.assertInvariants("D23-1");

                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertEquals(h.leader.state.entries(), node.state.entries());
                    assertEquals(h.leader.state.commitIndex(), node.state.commitIndex());
                    assertTrue(node.state.lastApplied() >= committedBefore);
                    assertFalse(node.repo.findResultByRequestId(
                            ReplicationCommitInvariantSuiteTest.requestIdOf(staleCommand)).isPresent());
                }
                RaftLogEntry replacement = h.leader.state.entryAt(staleIndex);
                assertNotEquals(stale, replacement);
                assertEquals(h.leader.state.currentTerm(), replacement.term());
                assertEquals(ReplicationCommitInvariantSuiteTest.requestIdOf(replacementCommand),
                        ReplicationCommitInvariantSuiteTest.requestIdOf(replacement));
                assertEquals(committedBefore,
                        h.leader.state.entryAt(committedBefore).index());
                assertEquals(Role.FOLLOWER, h.node("node-1").state.role());
                assertEquals(h.leader.state.currentTerm(), h.node("node-1").state.currentTerm());
            }
        }
    }

    @Test
    @DisplayName("2. exactly one follower gets the stale suffix; rejoin removes it and converges")
    void exactlyOneFollowerStaleSuffixIsRemoved() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.settle(300, "D23-2 prefix");
                long committed = h.leader.state.commitIndex();

                h.network.blockPeers(Set.of("node-3", "node-4", "node-5"));
                BookingCommand staleCommand = h.commandFor(92);
                long staleIndex = h.appendCommand(staleCommand);
                h.service.replicate("node-2");
                h.runRounds(20, "D23-2 one-follower suffix");

                RaftLogEntry stale = h.leader.state.entryAt(staleIndex);
                assertEquals(h.leader.state.entries().subList(0, (int) committed),
                        h.node("node-2").state.entries().subList(0, (int) committed));
                assertTrue(h.node("node-2").state.entries().contains(stale));
                assertEquals(2, h.nodes.values().stream()
                        .filter(node -> node.state.entries().contains(stale)).count(),
                        "stale entry must exist only on Leader and one follower");
                assertTrue(h.leader.state.commitIndex() == committed,
                        "two stale copies (Leader + one follower) must not reach majority");

                h.promote("node-3");
                h.network.unblockAll();
                h.appendCommand(h.commandFor(93));
                h.propagate("D23-2 rejoin");

                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertEquals(h.leader.state.entries(), node.state.entries());
                    assertFalse(node.state.entries().contains(stale));
                    assertFalse(node.repo.findResultByRequestId(
                            ReplicationCommitInvariantSuiteTest.requestIdOf(staleCommand)).isPresent());
                }
                h.assertInvariants("D23-2");
            }
        }
    }

    @Test
    @DisplayName("3. controlled suffix lengths cut from the first mismatch and preserve the prefix")
    void controlledSuffixLengthsCutAtFirstMismatch() throws Exception {
        List<int[]> pairs = List.of(new int[]{4, 2}, new int[]{2, 2}, new int[]{2, 4});
        for (long seed : SEEDS) {
            for (int[] pair : pairs) {
                int staleCount = pair[0];
                int replacementCount = pair[1];
                try (ReplicationCommitInvariantSuiteTest.Harness h =
                             ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                    h.appendCommand(h.commandFor(0));
                    h.appendCommand(h.commandFor(1));
                    h.settle(300, "D23-3 prefix");
                    long committed = h.leader.state.commitIndex();
                    List<RaftLogEntry> committedPrefix =
                            new ArrayList<>(h.leader.state.entries().subList(0, (int) committed));

                    h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                    for (int i = 0; i < staleCount; i++) {
                        h.appendCommand(h.commandFor(100 + i));
                    }
                    long firstMismatch = committed + 1;
                    h.runRounds(20, "D23-3 stale");

                    h.promote("node-2");
                    h.network.unblockAll();
                    for (int i = 0; i < replacementCount; i++) {
                        h.appendCommand(h.commandFor(200 + i));
                    }
                    h.propagate("D23-3 replacement");
                    h.assertInvariants("D23-3 k=" + staleCount + ",m=" + replacementCount);

                    assertEquals(committedPrefix,
                            h.leader.state.entries().subList(0, (int) committed));
                    assertEquals(h.leader.state.entries(), h.node("node-2").state.entries());
                    assertTrue(h.leader.state.lastLogIndex() >= firstMismatch);
                    assertEquals(h.leader.state.commitIndex(), h.leader.state.lastApplied());
                }
            }
        }
    }

    @Test
    @DisplayName("4. Figure 8 term-old entry survives then commits indirectly")
    void figureEightEntryCommitsIndirectlyAtExactIndex() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.oldTermPrefix(seed)) {
                h.service.replicateAll();
                h.settle(400, "D23-4 old-term replicated");

                long oldEntryIndex = 2L;
                RaftLogEntry oldEntry = h.leader.state.entryAt(oldEntryIndex);
                assertEquals(1L, oldEntry.term());
                assertTrue(h.leader.state.commitIndex() < oldEntryIndex);
                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertEquals(oldEntry, node.state.entryAt(oldEntryIndex));
                    assertTrue(node.state.commitIndex() < oldEntryIndex);
                }

                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.settle(500, "D23-4 current-term commit");

                long expectedCommit = 3L;
                assertEquals(expectedCommit, h.leader.state.commitIndex());
                assertEquals(h.leader.state.currentTerm(),
                        h.leader.state.termAt(h.leader.state.commitIndex()));
                assertEquals(oldEntry, h.leader.state.entryAt(oldEntryIndex));
                h.propagate("D23-4 final");
                h.assertInvariants("D23-4");
            }
        }
    }

    @Test
    @DisplayName("5. stale Leader receives a higher-term response through real replication and steps down")
    void staleLeaderStepsDownViaReplicationResponse() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(17L)) {
            h.appendCommand(h.commandFor(0));
            h.settle(200, "D23-5 before");

            ReplicationCommitInvariantSuiteTest.Node oldLeader = h.leader;
            long oldTerm = oldLeader.state.currentTerm();
            h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));

            ReplicationCommitInvariantSuiteTest.Node newLeader = h.node("node-2");
            newLeader.state.becomeFollower(oldTerm);
            newLeader.state.startElection("node-2");
            long newTerm = newLeader.state.currentTerm();
            assertEquals(oldTerm + 1L, newTerm);
            newLeader.state.becomeLeader(new TreeSet<>(Set.of("node-1", "node-3", "node-4", "node-5")));
            newLeader.rebindPersistent();

            h.network.setLeader("node-2");
            h.network.unblockAll();
            RaftNode.AppendEntriesRequest request = new RaftNode.AppendEntriesRequest(
                    oldTerm, oldLeader.id, 0L, 0L, List.of(), 0L);
            RaftNode.AppendEntriesResponse response =
                    new RaftNode.AppendEntriesResponse(newTerm, false, 0L);
            h.service.handleAppendResult("node-2", request,
                    new vn.nhom6.railway.raft.core.RaftTransportAdapter.Replied(
                            "node-2", request, response));

            assertTrue(newTerm > oldLeader.state.currentTerm()
                    || oldLeader.state.currentTerm() == newTerm);
            assertEquals(Role.FOLLOWER, oldLeader.state.role());
            assertEquals(newTerm, oldLeader.state.currentTerm());

            RaftNode.AppendOutcome outcome = oldLeader.raftNode.appendCommand(
                    RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                    "{\"requestId\":\"after-stepdown\"}");
            assertInstanceOf(RaftNode.NotLeader.class, outcome);
        }
    }

    @Test
    @DisplayName("6. NO_OP barrier waiting on stale Leader fails when it loses leadership")
    void staleLeaderBarrierAndPendingWriteNeverSucceed() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(23L)) {
            h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
            ReplicationCommitInvariantSuiteTest.Harness.BarrierHandle handle =
                    h.issueBarrier(Duration.ofMillis(500).toMillis());

            h.barrier.onLeadershipLost();
            h.runRounds(20, "D23-6 leadership lost");

            assertTrue(handle.stage().toCompletableFuture().isDone());
            assertTrue(handle.stage().toCompletableFuture().isCompletedExceptionally());
            assertEquals(0, h.barrier.pendingCount());
            h.assertInvariants("D23-6");
        }
    }

    @Test
    @DisplayName("7. conflict inside the committed prefix does not move matchIndex or state")
    void conflictInsideCommittedPrefixFailsClosed() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(19L)) {
            h.appendCommand(h.commandFor(0));
            h.appendCommand(h.commandFor(1));
            h.appendCommand(h.commandFor(2));
            h.runRounds(300, "D23-7");
            h.propagate("D23-7");

            long committed = h.leader.state.commitIndex();
            assertTrue(committed >= 2L);
            ReplicationCommitInvariantSuiteTest.Node follower = h.node("node-2");
            long conflictIndex = committed - 1L;
            RaftLogEntry original = follower.state.entryAt(conflictIndex);
            long matchBefore = h.leader.state.matchIndexOf("node-2");
            long commitBefore = follower.state.commitIndex();

            AppendEntriesRequestDto conflict = new AppendEntriesRequestDto(
                    follower.state.currentTerm(), h.leader.id,
                    conflictIndex - 1L, follower.state.termAt(conflictIndex - 1L),
                    List.of(new AppendEntriesRequestDto.LogEntryDto(
                            conflictIndex, original.term(), original.commandType(),
                            java.util.Map.of("requestId", "D23-committed-conflict"))),
                    committed);
            assertThrows(IllegalStateException.class,
                    () -> follower.raftNode.handleAppendEntries(conflict, h.leader.id));

            assertEquals(matchBefore, h.leader.state.matchIndexOf("node-2"));
            assertEquals(commitBefore, follower.state.commitIndex());
            assertEquals(original, follower.state.entryAt(conflictIndex));
            h.assertInvariants("D23-7");
        }
    }

    @Test
    @DisplayName("8. rejoin backtracks mismatched prevLog through one RPC at a time")
    void rejoinBacktracksPrevLogAtMostOneInFlight() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.divergentTail(seed)) {
                h.runDivergentTailWorkload();
                h.assertInvariants("D23-8");
                assertTrue(h.network.pendingCount() == 0);
                assertTrue(h.network.maxActivePerPeer() <= 1);
            }
        }
    }

    @Test
    @DisplayName("9. each flip-flop step converges and preserves committed prefix")
    void flipFlopPreservesCommittedPrefixEveryStep() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.settle(250, "D23-9 initial");
                long committedPrefix = h.leader.state.commitIndex();

                for (int flip = 0; flip < 4; flip++) {
                    h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                    BookingCommand stale = h.commandFor(300 + flip);
                    long staleIndex = h.appendCommand(stale);
                    h.runRounds(15, "D23-9 stale " + flip);
                    String target = flip % 2 == 0 ? "node-2" : "node-3";

                    h.promote(target);
                    h.network.unblockAll();
                    h.appendCommand(h.commandFor(400 + flip));
                    h.propagate("D23-9 flip " + flip);
                    h.assertInvariants("D23-9 flip " + flip);

                    assertTrue(h.leader.state.commitIndex() >= committedPrefix);
                    assertTrue(h.leader.state.currentTerm() >= 1L);
                    assertFalse(h.leader.state.entries().stream()
                            .anyMatch(entry -> entry.index() == staleIndex
                                    && ReplicationCommitInvariantSuiteTest.requestIdOf(entry)
                                    .equals(ReplicationCommitInvariantSuiteTest.requestIdOf(stale))));
                    for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                        assertEquals(h.leader.state.entries(), node.state.entries());
                        assertFalse(node.repo.findResultByRequestId(
                                ReplicationCommitInvariantSuiteTest.requestIdOf(stale)).isPresent());
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("10. final convergence includes a stale old-Leader suffix, not only a follower partition")
    void finalConvergenceWithOldLeaderSuffix() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.appendCommand(h.commandFor(1));
                h.settle(400, "D23-10 prefix");

                long committed = h.leader.state.commitIndex();
                h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                BookingCommand stale = h.commandFor(500);
                long staleIndex = h.appendCommand(stale);
                h.runRounds(20, "D23-10 stale old Leader");

                h.promote("node-2");
                h.network.unblockAll();
                h.appendCommand(h.commandFor(501));
                h.propagate("D23-10 final");
                h.assertInvariants("D23-10");

                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertEquals(h.leader.state.entries(), node.state.entries());
                    assertEquals(h.leader.state.commitIndex(), node.state.commitIndex());
                    assertEquals(h.leader.state.lastApplied(), node.state.lastApplied());
                    assertFalse(node.repo.findResultByRequestId(
                            ReplicationCommitInvariantSuiteTest.requestIdOf(stale)).isPresent());
                }
                assertFalse(h.leader.state.entries().stream()
                        .anyMatch(entry -> entry.index() == staleIndex
                                && ReplicationCommitInvariantSuiteTest.requestIdOf(entry)
                                .equals(ReplicationCommitInvariantSuiteTest.requestIdOf(stale))));
                assertTrue(h.leader.state.commitIndex() >= committed);
            }
        }
    }
}
