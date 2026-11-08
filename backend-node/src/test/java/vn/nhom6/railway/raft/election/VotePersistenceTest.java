package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;
import vn.nhom6.railway.raft.replication.FakeRaftStorage;

import static org.junit.jupiter.api.Assertions.*;

/** Contract tests only: TV4's memory fake, real state/handler, no JDBC/process restart. */
class VotePersistenceTest {
    private static final List<RaftLogEntry> LOG = List.of(new RaftLogEntry(1, 3, "NO_OP", "{}"));

    @Test
    void grantPersistsThroughSharedBoundaryBeforeTimerResetAndReply() throws Exception {
        try (Fixture f = new Fixture()) {
            f.clock.beforeArm = () -> {
                assertEquals(List.of("load", "saveTermAndVote(5, node-2)"), f.storage.calls());
            };
            assertEquals(new RequestVoteResponse(5, true), f.vote(5, "node-2", 1, 3));
            assertSame(f.state, f.persistence.state());
            assertEquals(1, f.replies);
            assertEquals(2, f.clock.tasks.size());
            assertEquals("node-2", f.storage.load().votedFor());
            assertEquals(LOG, f.storage.storedLog());
            assertEquals(1, f.state.commitIndex());
            assertEquals(1, f.state.lastApplied());
            assertEquals(0, f.storage.replaceSuffixCalls());
        }
    }

    @Test
    void unchangedRetriesAndDenialsCannotSaveAnotherVoteInSameTerm() throws Exception {
        try (Fixture f = new Fixture()) {
            assertTrue(f.vote(5, "node-2", 1, 3).voteGranted());
            assertTrue(f.vote(5, "node-2", 1, 3).voteGranted());
            assertFalse(f.vote(5, "node-3", 1, 3).voteGranted());
            assertFalse(f.vote(4, "node-3", 1, 3).voteGranted());
            assertEquals(List.of("load", "saveTermAndVote(5, node-2)"), f.storage.calls());
            assertEquals(3, f.clock.tasks.size(), "only successful grants reset the deadline");
        }
    }

    @Test
    void higherTermWithOldLogStillPersistsClearedVoteBeforeDenial() throws Exception {
        try (Fixture f = new Fixture()) {
            f.persistence.startElection("node-1");
            assertEquals(new RequestVoteResponse(9, false), f.vote(9, "node-2", 1, 2));
            assertEquals(List.of("load", "saveTermAndVote(6, node-1)",
                    "saveTermAndVote(9, null)"), f.storage.calls());
            assertEquals(Role.FOLLOWER, f.state.role());
            assertNull(f.state.votedFor());
            assertEquals(1, f.clock.tasks.size(), "denied vote must not reset timer");
        }
    }

    @Test
    void failureBeforeWriteReturnsNoGrantAndRequiresFreshStateFromStorage() throws Exception {
        try (Fixture f = new Fixture()) {
            var failure = new RaftStorageException("injected before-write failure");
            f.storage.failNext(failure);
            assertSame(failure, assertThrows(RaftStorageException.class,
                    () -> f.vote(5, "node-2", 1, 3)));
            assertFailedClosed(f);
            assertNull(f.storage.load().votedFor(), "fake write did not happen");
            // Healthy storage alone must not permit reusing the dirty in-memory vote.
            assertThrows(IllegalStateException.class, () -> f.vote(5, "node-2", 1, 3));
            try (Fixture recovered = new Fixture(f.storage)) {
                assertTrue(recovered.vote(5, "node-3", 1, 3).voteGranted());
            }
        }
    }

    @Test
    void ambiguousWriteReturnsNoGrantButReloadPreservesTheStoredVote() throws Exception {
        try (Fixture f = new Fixture()) {
            f.storage.persistTermAndVoteThenFailNext();
            assertThrows(RaftStorageException.class, () -> f.vote(5, "node-2", 1, 3));
            assertFailedClosed(f);
            assertEquals("node-2", f.storage.load().votedFor());
            try (Fixture recovered = new Fixture(f.storage)) {
                assertFalse(recovered.vote(5, "node-3", 1, 3).voteGranted());
                assertTrue(recovered.vote(5, "node-2", 1, 3).voteGranted());
            }
        }
    }

    @Test
    void failedPersistenceOfHigherTermDenialDoesNotReturnAResponse() throws Exception {
        try (Fixture f = new Fixture()) {
            f.storage.failNext(new RaftStorageException("cannot store higher term"));
            assertThrows(RaftStorageException.class, () -> f.vote(9, "node-2", 1, 2));
            assertFailedClosed(f);
            assertEquals(9, f.state.currentTerm());
            assertEquals(5, f.storage.load().currentTerm(), "memory must not be mistaken for storage");
        }
    }

    @Test
    void selfVoteIsSavedBeforeCampaignCanContinueAndFailurePoisonsBoundary() throws Exception {
        try (Fixture f = new Fixture()) {
            assertEquals(6, f.persistence.startElection("node-1"));
            assertEquals(List.of("load", "saveTermAndVote(6, node-1)"), f.storage.calls());
            f.storage.failNext(new RaftStorageException("self-vote failed"));
            assertThrows(RaftStorageException.class, () -> f.persistence.startElection("node-1"));
            assertThrows(IllegalStateException.class, () -> f.persistence.startElection("node-1"));
            assertThrows(IllegalStateException.class, () -> f.persistence.observeHigherTerm(10));
            assertThrows(IllegalStateException.class, () -> f.vote(8, "node-3", 1, 3));
            assertEquals(0, f.replies);
            assertEquals(6, f.storage.load().currentTerm());
        }
    }

    @Test
    void observingHigherTermSavesNullVoteAndNeverDecreasesTerm() throws Exception {
        try (Fixture f = new Fixture()) {
            f.persistence.startElection("node-1");
            f.persistence.observeHigherTerm(9);
            f.persistence.observeHigherTerm(9);
            f.persistence.observeHigherTerm(7);
            assertEquals(List.of("load", "saveTermAndVote(6, node-1)",
                    "saveTermAndVote(9, null)"), f.storage.calls());
            assertEquals(Role.FOLLOWER, f.state.role());
            assertNull(f.state.votedFor());
            assertEquals(9, f.storage.load().currentTerm());
        }
    }

    @Test
    void failedHigherTermObservationBlocksAllLaterElectionTransitions() throws Exception {
        try (Fixture f = new Fixture()) {
            f.storage.setFailing(true);
            assertThrows(RaftStorageException.class, () -> f.persistence.observeHigherTerm(9));
            f.storage.setFailing(false);
            assertThrows(IllegalStateException.class, () -> f.persistence.observeHigherTerm(10));
            assertThrows(IllegalStateException.class, () -> f.persistence.startElection("node-1"));
            assertThrows(IllegalStateException.class, () -> f.vote(10, "node-3", 1, 3));
            assertEquals(List.of("load", "saveTermAndVote(9, null)"), f.storage.calls());
            assertEquals(0, f.replies);
        }
    }

    @Test
    void invalidInputDoesNotWriteOrPoisonThePersistenceBoundary() throws Exception {
        try (Fixture f = new Fixture()) {
            assertThrows(IllegalArgumentException.class, () -> f.vote(9, "monitor-1", 1, 3));
            assertThrows(IllegalArgumentException.class, () -> f.vote(9, "node-2", 0, 3));
            assertThrows(IllegalArgumentException.class, () -> f.persistence.startElection("node-6"));
            assertThrows(IllegalArgumentException.class, () -> f.persistence.observeHigherTerm(-1));
            assertEquals(List.of("load"), f.storage.calls());
            assertEquals(5, f.state.currentTerm());
            assertEquals(1, f.clock.tasks.size());
            assertTrue(f.vote(5, "node-2", 1, 3).voteGranted());
        }
    }

    private static void assertFailedClosed(Fixture f) {
        assertEquals(0, f.replies, "storage failure cannot emit even a denial response");
        assertEquals(1, f.clock.tasks.size(), "failed write cannot reset the timer");
        assertTrue(f.clock.tasks.getFirst().isCancelled());
        assertThrows(IllegalStateException.class, f.timer::start);
        assertThrows(IllegalStateException.class, () -> f.vote(10, "node-3", 1, 3));
        assertThrows(IllegalStateException.class, () -> f.persistence.startElection("node-1"));
        assertThrows(IllegalStateException.class, () -> f.persistence.observeHigherTerm(10));
        assertEquals(2, f.storage.calls().size(), "no more storage calls after poisoning");
    }

    private static final class Fixture implements AutoCloseable {
        final FakeRaftStorage storage;
        final RaftState state;
        final PersistentElectionState persistence;
        final RecordingScheduler clock = new RecordingScheduler();
        final ElectionTimer timer;
        final RequestVoteHandler handler;
        int replies;

        Fixture() throws Exception { this(seededStorage()); }
        Fixture(FakeRaftStorage storage) throws Exception {
            this.storage = storage;
            var loaded = storage.load();
            state = RaftState.restore(loaded.currentTerm(), loaded.votedFor(), loaded.log(), loaded.lastApplied());
            persistence = new PersistentElectionState(state, storage);
            var election = new CandidateElection("node-1", new TestCore(), state,
                    (peer, request, reply) -> fail("unexpected outbound RPC"),
                    persistence::observeHigherTerm, () -> fail("unexpected leader"));
            timer = new ElectionTimer(election, clock, () -> 500, failure -> fail(failure));
            handler = new RequestVoteHandler(persistence, timer);
            timer.start();
        }
        static FakeRaftStorage seededStorage() {
            var storage = new FakeRaftStorage();
            storage.seedState(5, null, LOG, 1);
            return storage;
        }
        RequestVoteResponse vote(long term, String candidate, long index, long logTerm) throws Exception {
            var response = handler.handle(new RequestVoteRequest(term, candidate, index, logTerm));
            replies++;
            return response;
        }
        @Override public void close() { timer.close(); clock.shutdownNow(); }

        private final class TestCore implements RaftNode {
            @Override public void onElectionTimeout() throws RaftStorageException {
                persistence.startElection("node-1");
            }
            @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
                    throws RaftStorageException { return handler.handle(request); }
            @Override public Role role() { return state.role(); }
            @Override public long currentTerm() { return state.currentTerm(); }
            @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
            @Override public long commitIndex() { return state.commitIndex(); }
            @Override public long lastApplied() { return state.lastApplied(); }
            @Override public long lastLogIndex() { return state.lastLogIndex(); }
            @Override public void onApplied(long index) { throw new UnsupportedOperationException(); }
            @Override public AppendOutcome appendCommand(String type, String payload) {
                throw new UnsupportedOperationException();
            }
            @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
                throw new UnsupportedOperationException();
            }
            @Override public void addCommitListener(CommitListener listener) {
                throw new UnsupportedOperationException();
            }
        }
    }

    /** Records deadlines without launching worker threads. */
    private static final class RecordingScheduler extends ScheduledThreadPoolExecutor {
        final List<Deadline> tasks = new ArrayList<>();
        Runnable beforeArm = () -> { };
        RecordingScheduler() { super(1); }
        @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            beforeArm.run();
            Deadline task = new Deadline(command);
            tasks.add(task);
            return task;
        }
    }

    private static final class Deadline extends FutureTask<Void> implements ScheduledFuture<Void> {
        Deadline(Runnable command) { super(command, null); }
        @Override public long getDelay(TimeUnit unit) { return unit.convert(500, TimeUnit.MILLISECONDS); }
        @Override public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
