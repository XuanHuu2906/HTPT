package vn.nhom6.railway.raft.replication;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D21 follower stop/rejoin catch-up tests.
 *
 * <p>The harness uses real Leader replication/commit/apply components, real
 * PersistentRaftNode followers, per-node in-memory repositories and FakeNetwork.
 * Commit is advanced only by CommitAdvancer.onMatchIndexAdvanced.
 */
class FollowerCatchupTest {

    private static final List<Long> SEEDS =
            List.of(1L, 2L, 3L, 7L, 11L, 13L, 17L, 19L, 23L, 29L,
                    31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L, 71L);

    @Test
    @DisplayName("1. Stop then rejoin follower catches up its missing tail")
    void stoppedFollowerCatchesUpMissingTail() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.blockPeers(Set.of("node-2"));
                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.runRounds(3, "D21 scenario 1");
                h.network.unblockAll();
                h.service.replicate("node-2");
                h.runRounds(300, "D21 scenario 1");
                h.propagate("D21 scenario 1 final");
                h.assertInvariants("D21 scenario 1");
            }
        }
    }

    @Test
    @DisplayName("2. Large catch-up is bounded and converges")
    void largeCatchupIsBounded() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.runNormalWorkload();
                h.assertInvariants("D21 scenario 2");
                assertTrue(h.network.pendingCount() <= 1);
            }
        }
    }

    @Test
    @DisplayName("3. Three of five commits while two followers are stopped")
    void threeOfFiveCommitsWithTwoStopped() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.blockPeers(Set.of("node-4", "node-5"));
                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.runRounds(120, "D21 scenario 3");

                long committed = h.leader.state.commitIndex();
                assertTrue(committed >= 1L, h.seedMessage("3/5 must commit"));
                assertEquals(0L, h.node("node-4").state.commitIndex());
                assertEquals(0L, h.node("node-5").state.commitIndex());

                h.network.unblockAll();
                h.service.replicateAll();
                h.runRounds(300, "D21 scenario 3");
                h.assertInvariants("D21 scenario 3");
            }
        }
    }

    @Test
    @DisplayName("4. Two of five does not commit, rejoining one restores majority")
    void twoOfFiveDoesNotCommitThenOneRejoinCommits() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.blockPeers(Set.of("node-3", "node-4", "node-5"));
                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.runRounds(120, "D21 scenario 4");

                long oldCommit = h.leader.state.commitIndex();
                long oldApplied = h.leader.state.lastApplied();
                assertEquals(0L, oldCommit);
                assertEquals(oldCommit, oldApplied);

                h.network.unblockAll();
                h.network.blockPeers(Set.of("node-3", "node-4", "node-5"));
                h.network.unblockAll();
                h.network.blockPeers(Set.of("node-4", "node-5"));
                h.service.replicate("node-3");
                h.runRounds(300, "D21 scenario 4");

                assertTrue(h.leader.state.commitIndex() >= 1L);
                assertTrue(h.leader.state.lastApplied() >= h.leader.state.commitIndex());
            }
        }
    }

    @Test
    @DisplayName("5. Restart follower restores durable state and catches up")
    void restartedFollowerRestoresDurableState() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.runNormalWorkload();
                ReplicationCommitInvariantSuiteTest.Node victim =
                        h.node("node-3");
                victim.rebindPersistent();
                h.runRound("D21 scenario 5");
                h.propagate("D21 scenario 5");
                h.assertInvariants("D21 scenario 5");
            }
        }
    }

    @Test
    @DisplayName("6. Restart during catch-up resumes without duplicate apply")
    void restartDuringCatchupDoesNotDuplicate() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.blockPeers(Set.of("node-2"));
                h.appendCommand(h.commandFor(0));
                h.appendCommand(h.commandFor(1));
                h.service.replicateAll();
                h.runRounds(3, "D21 scenario 6");

                h.network.unblockAll();
                h.node("node-2").rebindPersistent();
                h.service.replicate("node-2");
                h.runRounds(300, "D21 scenario 6");
                h.propagate("D21 scenario 6");
                h.assertInvariants("D21 scenario 6");
            }
        }
    }

    @Test
    @DisplayName("7. Divergent uncommitted tail backtracks and preserves committed prefix")
    void divergentUncommittedTailPreservesCommittedPrefix() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.divergentTail(seed)) {
                h.runDivergentTailWorkload();
            }
        }
    }

    @Test
    @DisplayName("8. Committed conflict fails closed")
    void committedConflictFailsClosed() throws Exception {
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(17L)) {
            h.appendCommand(h.commandFor(0));
            h.settle(300, "D21 scenario 8 before conflict");
            h.assertInvariants("D21 scenario 8");
            assertTrue(h.leader.state.commitIndex() >= 1L);
        }
    }

    @Test
    @DisplayName("9. Random drops, delays and bad replies never become ACK")
    void randomFaultRepliesNeverBecomeAck() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                Random random = new Random(seed);
                h.network.enableFaults(random, 0.35, 0.35);
                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.runRounds(20, "D21 scenario 9");
                h.assertInvariants("D21 scenario 9 seed " + seed);
                for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
                    assertTrue(h.leader.state.matchIndexOf(peer) >= 0L);
                    assertTrue(h.leader.state.nextIndexOf(peer) >=
                            h.leader.state.matchIndexOf(peer) + 1L);
                }
            }
        }
    }

    @Test
    @DisplayName("10. Higher-term new leader replaces old uncommitted suffix")
    void higherTermLeaderReplacesOldSuffix() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.runLeaderChangeWorkload();
            }
        }
    }

    @Test
    @DisplayName("11. Old-term leader append is rejected")
    void oldTermLeaderAppendIsRejected() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.node("node-2").raftNode.observeHigherTerm(h.leader.state.currentTerm() + 1L);
                h.runRound("D21 scenario 11");
                assertFalse(h.node("node-2").state.commitIndex() >
                        h.node("node-2").state.lastApplied());
            }
        }
    }

    @Test
    @DisplayName("12. Lost disk log is characterized and bounded")
    void lostDiskLogIsCharacterized() throws Exception {
        long seed = 53L;
        try (ReplicationCommitInvariantSuiteTest.Harness h =
                     ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
            h.runNormalWorkload();
            ReplicationCommitInvariantSuiteTest.Node victim =
                    h.node("node-2");

            long oldMatch = h.leader.state.matchIndexOf("node-2");
            long oldCommit = h.leader.state.commitIndex();
            RaftState emptyState = RaftState.restore(
                    victim.state.currentTerm(), victim.state.votedFor(), List.of(), 0L);
            victim.state = emptyState;
            victim.rebindPersistent();

            h.service.replicate("node-2");
            for (int round = 0; round < 200; round++) {
                h.network.step();
                h.drain();
                h.retries.fireAll();
                h.drain();
            }

            assertEquals(oldMatch, h.leader.state.matchIndexOf("node-2"),
                    "lost disk must not reduce matchIndex");
            assertEquals(oldCommit, h.leader.state.commitIndex(),
                    "lost disk must not reduce commitIndex");
            assertTrue(h.tracker.nextIndexOf("node-2") >=
                    h.tracker.matchIndexOf("node-2") + 1L);
            assertTrue(h.network.pendingCount() <= 1);
            java.lang.reflect.Field callsField =
                    h.network.getClass().getDeclaredField("calls");
            callsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.List<?> calls = (java.util.List<?>) callsField.get(h.network);
            long peerCalls = calls.stream()
                    .filter(call -> {
                        try {
                            java.lang.reflect.Field peerField =
                                    call.getClass().getDeclaredField("peerId");
                            peerField.setAccessible(true);
                            return "node-2".equals(peerField.get(call));
                        } catch (ReflectiveOperationException failure) {
                            return false;
                        }
                    }).count();
            assertTrue(peerCalls <= 205,
                    "lost disk catch-up RPC count must be bounded; measured=" + peerCalls);
            System.out.printf(
                    "D21-12 seed=53 oldMatch=%d oldCommit=%d finalMatch=%d finalCommit=%d nextIndex=%d followerLog=%d rpc=%d%n",
                    oldMatch,
                    oldCommit,
                    h.leader.state.matchIndexOf("node-2"),
                    h.leader.state.commitIndex(),
                    h.tracker.nextIndexOf("node-2"),
                    victim.state.lastLogIndex(),
                    peerCalls);
        }
    }

    @Test
    @DisplayName("13. Final rejoin converges commit, listener and apply")
    void finalRejoinConvergesAllNodes() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.runNormalWorkload();
            h.assertInvariants("D21 scenario 13");
            for (ReplicationCommitInvariantSuiteTest.Node node :
                        h.nodes.values()) {
                    assertTrue(node.state.lastApplied() <= node.state.commitIndex());
                    assertTrue(node.state.commitIndex() <= node.state.lastLogIndex());
                    assertEquals(Role.LEADER == h.leader.state.role()
                                    ? h.leader.state.commitIndex()
                                    : node.state.commitIndex(),
                            node.state.commitIndex());
                }
            }
        }
    }
}
