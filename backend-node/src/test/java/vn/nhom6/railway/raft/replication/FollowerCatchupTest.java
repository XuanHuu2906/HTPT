package vn.nhom6.railway.raft.replication;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.protocol.dto.AppendEntriesRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftTransportAdapter;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                h.network.blockPeers(Set.of("node-2"));
                for (int step = 0; step < 200; step++) {
                    h.appendCommand(h.commandFor(step));
                }
                h.service.replicateAll();
                h.runRounds(3, "D21 scenario 2 blocked");
                h.network.unblockAll();
                h.service.replicate("node-2");
                h.runRounds(600, "D21 scenario 2 rejoin");
                h.propagate("D21 scenario 2 final");

                int batchMax = readBatchMax(h.service);
                int maxActive = readMaxActivePerPeer(h.network);
                long rpcCount = countPeerCalls(h.network, "node-2");
                for (Object call : readCalls(h.network)) {
                    if (callPeer(call).equals("node-2")) {
                        assertTrue(callRequestEntries(call) <= batchMax,
                                "AppendEntries batch exceeds batchMax=" + batchMax);
                    }
                }
                assertTrue(maxActive <= 1,
                        "more than one AppendEntries was in flight for node-2");
                assertEquals(h.leader.state.lastLogIndex(),
                        h.leader.state.matchIndexOf("node-2"));
                assertEquals(h.leader.state.commitIndex(),
                        h.node("node-2").state.commitIndex());
                assertEquals(h.leader.state.entries(),
                        h.node("node-2").state.entries());
                h.assertInvariants("D21 scenario 2");
                assertTrue(rpcCount <= 205,
                        "RPC count not bounded; measured=" + rpcCount);
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
                assertTrue(committed >= 1L);
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
                assertEquals(0L, h.leader.state.commitIndex());
                assertEquals(h.leader.state.commitIndex(), h.leader.state.lastApplied());

                h.network.unblockAll();
                h.network.blockPeers(Set.of("node-4", "node-5"));
                h.service.replicate("node-3");
                h.runRounds(300, "D21 scenario 4");
                assertTrue(h.leader.state.commitIndex() >= 1L);
                assertEquals(h.leader.state.commitIndex(), h.leader.state.lastApplied());
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
                h.node("node-3").rebindPersistent();
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
            h.appendCommand(h.commandFor(1));
            h.appendCommand(h.commandFor(2));
            h.runRounds(300, "D21 scenario 8");
            h.propagate("D21 scenario 8 final");

            long committedIndex = h.leader.state.commitIndex();
            assertTrue(committedIndex >= 1L);
            ReplicationCommitInvariantSuiteTest.Node follower = h.node("node-2");
            assertEquals(committedIndex, follower.state.commitIndex());

            RaftLogEntry original = follower.state.entryAt(committedIndex);
            long matchBefore = h.leader.state.matchIndexOf("node-2");
            long commitBefore = follower.state.commitIndex();
            List<RaftLogEntry> logBefore = follower.state.entries();

            AppendEntriesRequestDto conflict = new AppendEntriesRequestDto(
                    follower.state.currentTerm(),
                    h.leader.id,
                    committedIndex - 1L,
                    follower.state.termAt(committedIndex - 1L),
                    List.of(new AppendEntriesRequestDto.LogEntryDto(
                            committedIndex,
                            original.term(),
                            original.commandType(),
                            java.util.Map.of("requestId",
                                    "committed-conflict-at-index-" + committedIndex))),
                    committedIndex);

            IllegalStateException failure = assertThrows(
                    IllegalStateException.class,
                    () -> follower.raftNode.handleAppendEntries(conflict, h.leader.id));
            assertFalse(failure.getMessage().isBlank());
            assertEquals(logBefore, follower.state.entries());
            assertEquals(commitBefore, follower.state.commitIndex());
            assertEquals(original, follower.state.entryAt(committedIndex));
            assertEquals(matchBefore, h.leader.state.matchIndexOf("node-2"));
            assertTrue(follower.repo.lastApplied() <= follower.state.commitIndex());
            assertThrows(IllegalStateException.class,
                    () -> follower.raftNode.handleAppendEntries(
                            AppendEntriesRequestDto.createHeartbeat(
                                    follower.state.currentTerm(),
                                    h.leader.id,
                                    committedIndex,
                                    follower.state.termAt(committedIndex),
                                    committedIndex),
                            h.leader.id));
        }
    }

    @Test
    @DisplayName("9. Random drops, delays and bad replies never become ACK")
    void randomFaultRepliesNeverBecomeAck() throws Exception {
        for (long seed : SEEDS) {
            try (ReplicationCommitInvariantSuiteTest.Harness h =
                         ReplicationCommitInvariantSuiteTest.Harness.normal(seed)) {
                h.network.enableFaults(new Random(seed), 0.35, 0.35);
                h.appendCommand(h.commandFor(0));
                h.service.replicateAll();
                h.runRounds(20, "D21 scenario 9");

                java.util.Map<String, Long> matchBefore = new java.util.LinkedHashMap<>();
                for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
                    matchBefore.put(peer, h.leader.state.matchIndexOf(peer));
                }
                RaftNode.AppendEntriesRequest synthetic =
                        new RaftNode.AppendEntriesRequest(
                                h.leader.state.currentTerm(), h.leader.id,
                                0L, 0L, List.of(),
                                h.leader.state.commitIndex());
                h.service.handleAppendResult("node-2", synthetic,
                        new RaftTransportAdapter.Failed(
                                "node-2", synthetic, RaftTransportAdapter.FailureKind.TIMEOUT));
                h.service.handleAppendResult("node-3", synthetic,
                        new RaftTransportAdapter.Replied(
                                "node-3", synthetic,
                                new RaftNode.AppendEntriesResponse(
                                        synthetic.term(), true, 1L)));
                h.service.handleAppendResult("node-4", synthetic,
                        new RaftTransportAdapter.Failed(
                                "node-4", synthetic, RaftTransportAdapter.FailureKind.PEER_ERROR));
                h.service.handleAppendResult("node-5", synthetic,
                        new RaftTransportAdapter.Failed(
                                "node-5", synthetic, RaftTransportAdapter.FailureKind.TRANSPORT_ERROR));
                h.runRound("D21 scenario 9 bad replies");

                for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
                    assertEquals(matchBefore.get(peer),
                            h.leader.state.matchIndexOf(peer));
                    assertTrue(h.leader.state.matchIndexOf(peer) <=
                            h.node(peer).state.lastLogIndex());
                    assertTrue(h.leader.state.nextIndexOf(peer) >=
                            h.leader.state.matchIndexOf(peer) + 1L);
                }

                h.network.disableFaultsAndHeal();
                h.service.replicateAll();
                h.runRounds(300, "D21 scenario 9 heal");
                h.propagate("D21 scenario 9 final");
                h.assertInvariants("D21 scenario 9 heal " + seed);
                for (String peer : List.of("node-2", "node-3", "node-4", "node-5")) {
                    assertEquals(h.leader.state.lastLogIndex(),
                            h.leader.state.matchIndexOf(peer));
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
                ReplicationCommitInvariantSuiteTest.Node follower =
                        h.node("node-2");
                long oldTerm = h.leader.state.currentTerm();
                long higherTerm = oldTerm + 1L;
                follower.raftNode.observeHigherTerm(higherTerm);
                h.runRound("D21 scenario 11");

                long oldCommit = follower.state.commitIndex();
                long oldApplied = follower.state.lastApplied();
                List<RaftLogEntry> oldLog = follower.state.entries();
                String oldVote = follower.state.votedFor();

                RaftNode.AppendEntriesRequest oldRequest =
                        new RaftNode.AppendEntriesRequest(
                                oldTerm,
                                h.leader.id,
                                follower.state.lastLogIndex(),
                                follower.state.termAt(follower.state.lastLogIndex()),
                                List.of(new RaftLogEntry(
                                        follower.state.lastLogIndex() + 1L,
                                        oldTerm,
                                        RaftLogEntry.COMMAND_TYPE_NO_OP,
                                        "{}")),
                                follower.state.commitIndex() + 100L);
                AppendEntriesRequestDto oldDto =
                        new AppendEntriesRequestDto(
                                oldRequest.term(),
                                oldRequest.leaderId(),
                                oldRequest.prevLogIndex(),
                                oldRequest.prevLogTerm(),
                                List.of(new AppendEntriesRequestDto.LogEntryDto(
                                        follower.state.lastLogIndex() + 1L,
                                        oldTerm,
                                        RaftLogEntry.COMMAND_TYPE_NO_OP,
                                        java.util.Map.of())),
                                oldRequest.leaderCommit());

                RaftNode.AppendEntriesResponse response =
                        follower.raftNode.handleAppendEntries(oldDto, h.leader.id);
                assertFalse(response.success());
                assertEquals(higherTerm, response.term());
                assertEquals(higherTerm, follower.state.currentTerm());
                assertEquals(oldCommit, follower.state.commitIndex());
                assertEquals(oldApplied, follower.state.lastApplied());
                assertEquals(oldLog, follower.state.entries());
                assertEquals(oldVote, follower.state.votedFor());

                h.service.handleAppendResult(
                        "node-2",
                        oldRequest,
                        new RaftTransportAdapter.Replied(
                                "node-2", oldRequest, response));
                h.runRound("D21 scenario 11 old Leader response");
                assertEquals(Role.FOLLOWER, h.leader.state.role());
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
            ReplicationCommitInvariantSuiteTest.Node victim = h.node("node-2");
            long oldMatch = h.leader.state.matchIndexOf("node-2");
            long oldCommit = h.leader.state.commitIndex();

            RaftState emptyState =
                    RaftState.restore(victim.state.currentTerm(),
                            victim.state.votedFor(), List.of(), 0L);
            victim.state = emptyState;
            victim.rebindPersistent();
            h.service.replicate("node-2");
            for (int round = 0; round < 200; round++) {
                h.network.step();
                h.drain();
                h.retries.fireAll();
                h.drain();
            }

            assertEquals(oldMatch, h.leader.state.matchIndexOf("node-2"));
            assertEquals(oldCommit, h.leader.state.commitIndex());
            assertTrue(h.tracker.nextIndexOf("node-2") >=
                    h.tracker.matchIndexOf("node-2") + 1L);
            assertTrue(h.network.pendingCount() <= 1);

            long peerCalls = countPeerCalls(h.network, "node-2");
            assertTrue(peerCalls <= 205,
                    "lost disk catch-up RPC count not bounded; measured=" + peerCalls);
            System.out.printf(
                    "D21-12 seed=53 oldMatch=%d oldCommit=%d finalMatch=%d finalCommit=%d nextIndex=%d followerLog=%d rpc=%d%n",
                    oldMatch, oldCommit,
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
                h.network.blockPeers(Set.of("node-2", "node-3"));
                for (int step = 0; step < 30; step++) {
                    h.appendCommand(h.commandFor(step));
                }
                h.service.replicateAll();
                h.runRounds(120, "D21 scenario 13 blocked");
                h.network.unblockAll();
                h.service.replicateAll();
                h.runRounds(600, "D21 scenario 13 rejoin");
                h.propagate("D21 scenario 13 final");
                h.assertInvariants("D21 scenario 13");

                long expectedCommit = h.leader.state.commitIndex();
                long expectedLog = h.leader.state.lastLogIndex();
                List<RaftLogEntry> expectedEntries = h.leader.state.entries();
                java.util.Map<String, String> expectedSeats =
                        h.leader.repo.readSeatSnapshot();
                for (ReplicationCommitInvariantSuiteTest.Node node :
                        h.nodes.values()) {
                    assertEquals(expectedCommit, node.state.commitIndex());
                    assertEquals(expectedLog, node.state.lastLogIndex());
                    assertEquals(expectedEntries, node.state.entries());
                    assertEquals(expectedCommit, node.state.lastApplied());
                    assertEquals(expectedCommit, node.repo.lastApplied());
                    assertEquals(expectedSeats,
                            new java.util.TreeMap<>(node.repo.readSeatSnapshot()));
                }
            }
        }
    }

    private static java.util.List<?> readCalls(
            ReplicationCommitInvariantSuiteTest.FakeNetwork network) throws Exception {
        java.lang.reflect.Field field = network.getClass().getDeclaredField("calls");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<?> calls = (java.util.List<?>) field.get(network);
        return calls;
    }

    private static long countPeerCalls(
            ReplicationCommitInvariantSuiteTest.FakeNetwork network,
            String peerId) throws Exception {
        return readCalls(network).stream()
                .filter(call -> callPeer(call).equals(peerId))
                .count();
    }

    private static String callPeer(Object call) {
        try {
            java.lang.reflect.Field field = call.getClass().getDeclaredField("peerId");
            field.setAccessible(true);
            return (String) field.get(call);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("cannot inspect FakeNetwork call peerId", failure);
        }
    }

    private static int callRequestEntries(Object call) {
        try {
            java.lang.reflect.Field field = call.getClass().getDeclaredField("request");
            field.setAccessible(true);
            vn.nhom6.railway.protocol.dto.MessageEnvelope<?> envelope =
                    (vn.nhom6.railway.protocol.dto.MessageEnvelope<?>) field.get(call);
            Object payload = envelope.getPayload();
            java.lang.reflect.Method method = payload.getClass().getMethod("getEntries");
            @SuppressWarnings("unchecked")
            java.util.List<?> entries = (java.util.List<?>) method.invoke(payload);
            return entries.size();
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("cannot inspect AppendEntries batch size", failure);
        }
    }

    private static int readBatchMax(RaftReplicationService service) throws Exception {
        java.lang.reflect.Field field =
                RaftReplicationService.class.getDeclaredField("batchMax");
        field.setAccessible(true);
        return field.getInt(service);
    }

    private static int readMaxActivePerPeer(
            ReplicationCommitInvariantSuiteTest.FakeNetwork network) throws Exception {
        java.lang.reflect.Field field =
                network.getClass().getDeclaredField("maxActivePerPeer");
        field.setAccessible(true);
        return field.getInt(network);
    }
}
