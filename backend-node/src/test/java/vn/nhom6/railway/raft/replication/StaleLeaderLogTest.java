package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D23 stale-old-Leader suffix scenarios.
 *
 * <p>These tests use the deterministic five-Node harness in
 * {@link ReplicationCommitInvariantSuiteTest}. Transport, storage, apply and elections are
 * simulated in-memory; the harness contains no TCP, Docker or real process/failover. Production
 * Raft files are not modified by this class.
 */
class StaleLeaderLogTest {

    private static final List<Long> SEEDS = ReplicationCommitInvariantSuiteTest.SEEDS;

    @Test
    @DisplayName("1. isolated old Leader rejoins and replaces only its uncommitted suffix")
    void isolatedOldLeaderRejoinsAndTruncatesSuffix() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.appendCommand(h.commandFor(1));
                h.settle(400, "D23-1 initial");
                long committed = h.leader.state.commitIndex();
                h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                BookingCommand stale = h.commandFor(90);
                long staleIndex = h.appendCommand(stale);
                h.runRounds(40, "D23-1 stale isolated");
                assertEquals(committed, h.leader.state.commitIndex());
                assertFalse(h.leader.state.lastApplied() > committed);

                h.promote("node-2");
                h.network.unblockAll();
                h.appendCommand(h.commandFor(91));
                h.propagate("D23-1 rejoin");
                h.assertInvariants("D23-1");
                assertTrue(h.leader.state.entries().size() >= committed);
                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertFalse(node.repo.findResultByRequestId(
                            ReplicationCommitInvariantSuiteTest.requestIdOf(stale)).isPresent(),
                            "stale entry must never be applied");
                }
                assertTrue(staleIndex >= 1L);
            }
        }
    }

    @Test
    @DisplayName("2. partially replicated minority suffix is removed from every follower")
    void minoritySuffixIsRemovedOnRejoin() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.divergentTail(seed)) {
                h.runDivergentTailWorkload();
                h.assertInvariants("D23-2");
            }
        }
    }

    @Test
    @DisplayName("3. suffixes longer, shorter and equal to the new Leader converge")
    void staleSuffixLengthVariantsConverge() throws Exception {
        for (long seed : SEEDS) {
            for (int staleEntries : List.of(1, 2, 4)) {
                try (ReplicationCommitInvariantSuiteTest.Harness h =
                             ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                    h.appendCommand(h.commandFor(0));
                    h.appendCommand(h.commandFor(1));
                    h.settle(300, "D23-3 prefix");
                    long committed = h.leader.state.commitIndex();
                    h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                    for (int i = 0; i < staleEntries; i++) {
                        h.appendCommand(h.commandFor(100 + i));
                    }
                    h.runRounds(20, "D23-3 stale suffix");
                    h.promote("node-2");
                    h.network.unblockAll();
                    h.appendCommand(h.commandFor(200));
                    h.propagate("D23-3 rejoin");
                    h.assertInvariants("D23-3 length=" + staleEntries);
                    assertTrue(h.leader.state.commitIndex() >= committed);
                }
            }
        }
    }

    @Test
    @DisplayName("4. old-term Figure 8 entry survives until an indirect current-term commit")
    void oldTermFigureEightEntryIsNotDirectlyCommitted() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.oldTermPrefix(seed)) {
                h.runOldTermWorkload();
                h.assertInvariants("D23-4");
            }
        }
    }

    @Test
    @DisplayName("5. old Leader still thinking leader is refused and steps down")
    void oldLeaderAppendIsRefusedAndStepDown() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(17L)) {
            h.appendCommand(h.commandFor(0));
            h.settle(200, "D23-5 before partition");
            ReplicationCommitInvariantSuiteTest.Node old = h.leader;
            h.promote("node-2");
            h.network.unblockAll();
            h.runRounds(20, "D23-5 new Leader");
            long oldTerm = h.leader.state.currentTerm();

            RaftNode.AppendOutcome outcome = old.raftNode.appendCommand(
                    RaftLogEntry.COMMAND_TYPE_BOOK_TICKET,
                    "{\"requestId\":\"old-leader-after-stepdown\"}");
            assertTrue(outcome instanceof RaftNode.NotLeader,
                    "old Leader must refuse a new write");

            old.state.becomeFollower(oldTerm);
            assertEquals(Role.FOLLOWER, old.state.role());
            h.barrier.onLeadershipLost();
            assertEquals(0, h.barrier.pendingCount());
            h.assertInvariants("D23-5");
        }
    }

    @Test
    @DisplayName("6. write waiting in a truncated entry never gets CONFIRMED")
    void pendingWriteInTruncatedEntryNeverConfirms() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                BookingCommand stale = h.commandFor(77);
                h.appendCommand(stale);
                h.runRounds(20, "D23-6 waiting stale entry");
                long staleIndex = h.leader.state.lastLogIndex();
                h.promote("node-2");
                h.network.unblockAll();
                h.appendCommand(h.commandFor(78));
                h.propagate("D23-6 rejoin");
                h.assertInvariants("D23-6");
                assertFalse(h.leader.state.entries().stream()
                        .anyMatch(entry -> entry.index() == staleIndex
                                && ReplicationCommitInvariantSuiteTest.requestIdOf(entry)
                                .equals(ReplicationCommitInvariantSuiteTest.requestIdOf(stale))));
            }
        }
    }

    @Test
    @DisplayName("7. conflict in the middle of a committed prefix fails closed")
    void conflictBeforeCommitIndexFailsClosed() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(19L)) {
            h.appendCommand(h.commandFor(0));
            h.appendCommand(h.commandFor(1));
            h.appendCommand(h.commandFor(2));
            h.runRounds(300, "D23-7 committed");
            h.propagate("D23-7");
            long committed = h.leader.state.commitIndex();
            assertTrue(committed >= 2L);
            ReplicationCommitInvariantSuiteTest.Node follower = h.node("node-2");
            long conflictIndex = committed - 1L;
            RaftLogEntry matchedBefore = follower.state.entryAt(conflictIndex);

            AppendEntriesRequestDto conflict = new AppendEntriesRequestDto(
                    follower.state.currentTerm(),
                    h.leader.id,
                    conflictIndex - 1L,
                    follower.state.termAt(conflictIndex - 1L),
                    List.of(new AppendEntriesRequestDto.LogEntryDto(
                            conflictIndex,
                            matchedBefore.term(),
                            matchedBefore.commandType(),
                            java.util.Map.of("requestId", "D23-committed-conflict"))),
                    committed);
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> follower.raftNode.handleAppendEntries(conflict, h.leader.id));
            assertEquals(committed, follower.state.commitIndex());
            assertEquals(matchedBefore, follower.state.entryAt(conflictIndex));
        }
    }

    @Test
    @DisplayName("8. heartbeat rejoin backtracks mismatched prevLog before reconnecting")
    void heartbeatBacktracksThenReconnects() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.divergentTail(seed)) {
                h.runDivergentTailWorkload();
                h.assertInvariants("D23-8");
            }
        }
    }

    @Test
    @DisplayName("9. repeated old-Leader flip-flop preserves safety invariants")
    void repeatedFlipFlopPreservesSafety() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.settle(250, "D23-9 initial");
                for (int flip = 0; flip < 4; flip++) {
                    h.network.blockPeers(Set.of("node-2", "node-3", "node-4", "node-5"));
                    h.appendCommand(h.commandFor(300 + flip));
                    h.runRounds(15, "D23-9 stale flip=" + flip);
                    String target = flip % 2 == 0 ? "node-2" : "node-3";
                    h.promote(target);
                    h.network.unblockAll();
                    h.appendCommand(h.commandFor(400 + flip));
                    h.propagate("D23-9 flip=" + flip);
                    h.assertInvariants("D23-9 flip=" + flip);
                }
            }
        }
    }

    @Test
    @DisplayName("10. final log, commit and apply sequence converge on all five Nodes")
    void finalLogsAndApplySequenceConverge() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.appendCommand(h.commandFor(0));
                h.appendCommand(h.commandFor(1));
                h.settle(400, "D23-10 prefix");
                h.network.blockPeers(Set.of("node-2", "node-3"));
                h.appendCommand(h.commandFor(2));
                h.runRounds(40, "D23-10 partition");
                h.network.unblockAll();
                h.propagate("D23-10 final");
                h.assertInvariants("D23-10");
                List<RaftLogEntry> canonical = new ArrayList<>(h.leader.state.entries());
                long commit = h.leader.state.commitIndex();
                for (ReplicationCommitInvariantSuiteTest.Node node : h.nodes.values()) {
                    assertEquals(commit, node.state.commitIndex());
                    assertEquals(canonical, node.state.entries());
                    assertEquals(commit, node.state.lastApplied());
                }
            }
        }
    }
}
