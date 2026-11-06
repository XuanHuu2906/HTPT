package vn.nhom6.railway.raft.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteRequest;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStorage;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.Role;

import static org.junit.jupiter.api.Assertions.*;

/** Real handler/rules/timer, test-only core wiring and FakeStorage: no JDBC or live cluster. */
class VoteSafetyTest {
    private static final Set<String> PEERS = Set.of("node-2", "node-3", "node-4", "node-5");
    private static final List<RaftLogEntry> LOG = List.of(
            new RaftLogEntry(1, 1, "NO_OP", "{}"),
            new RaftLogEntry(2, 3, "NO_OP", "{}"),
            new RaftLogEntry(3, 3, "NO_OP", "{}"));

    @Test
    void firstVoteIsStoredBeforeTimerResetAndResponse() throws Exception {
        Fixture f = new Fixture(new FakeStorage(0, null, List.of(), 0));
        f.storage.beforeSave = () -> assertEquals(List.of("arm"), f.events);

        assertEquals(new RequestVoteResponse(1, true), f.vote(1, "node-2", 0, 0));

        assertEquals(List.of("arm", "save:1:node-2", "arm", "reply:1:true"), f.events);
        assertEquals(1, f.storage.term);
        assertEquals("node-2", f.storage.vote);
        assertTrue(f.clock.tasks.getFirst().isCancelled());
    }

    @ParameterizedTest
    @CsvSource({"0,0,false", "100,2,false", "2,3,false", "3,3,true", "4,3,true",
        "1,4,true", "9223372036854775807,3,true"})
    void comparesLogTermBeforeIndexAndLeavesLogAndCheckpointsUnchanged(
            long index, long logTerm, boolean grant) throws Exception {
        Fixture f = fixture(null);
        assertEquals(new RequestVoteResponse(5, grant), f.vote(5, "node-2", index, logTerm));
        assertEquals(grant ? "node-2" : null, f.storage.vote);
        assertEquals(grant ? 1 : 0, f.storage.saves);
        assertEquals(grant ? 2 : 1, f.clock.tasks.size());
        assertEquals(LOG, f.state.entries());
        assertEquals(LOG, f.storage.log);
        assertEquals(1, f.state.commitIndex());
        assertEquals(1, f.state.lastApplied());
        assertEquals(1, f.storage.applied);
    }

    @Test
    void staleRequestReturnsLocalTermWithoutStorageOrTimerChanges() throws Exception {
        Fixture f = fixture("node-4");
        f.state.becomeFollower(5, "node-4");
        assertEquals(new RequestVoteResponse(5, false), f.vote(4, "node-2", 100, 4));
        assertEquals("node-4", f.state.votedFor());
        assertEquals("node-4", f.state.leaderId());
        assertEquals(0, f.storage.saves);
        assertEquals(1, f.clock.tasks.size());
        assertFalse(f.clock.tasks.getFirst().isCancelled());
    }

    @Test
    void repeatedGrantIsIdempotentAndAnotherCandidateCannotTakeTheVote() throws Exception {
        Fixture f = fixture(null);
        assertTrue(f.vote(5, "node-2", 3, 3).voteGranted());
        assertTrue(f.vote(5, new String("node-2"), 3, 3).voteGranted());
        assertFalse(f.vote(5, "node-3", 100, 5).voteGranted());
        assertFalse(f.vote(5, "node-2", 2, 3).voteGranted(), "retry still needs a fresh log");
        assertEquals(1, f.storage.saves);
        assertEquals("node-2", f.storage.vote);
        assertEquals(3, f.clock.tasks.size(), "only the two grants reset the timer");
    }

    @Test
    void rejectedLogDoesNotConsumeVoteAndNewTermAllowsAnotherCandidate() throws Exception {
        Fixture f = fixture(null);
        assertFalse(f.vote(5, "node-2", 100, 2).voteGranted());
        assertTrue(f.vote(5, "node-3", 3, 3).voteGranted());
        assertTrue(f.vote(6, "node-4", 3, 3).voteGranted());
        assertEquals(new RequestVoteResponse(6, false), f.vote(5, "node-3", 100, 5));
        assertEquals(6, f.storage.term);
        assertEquals("node-4", f.storage.vote);
    }

    @ParameterizedTest
    @EnumSource(Role.class)
    void higherTermDenialStoresClearedVoteAndStepsDownFromEveryRole(Role role) throws Exception {
        Fixture f = fixture("node-1");
        f.setRole(role);
        int before = f.clock.tasks.size();

        assertEquals(new RequestVoteResponse(9, false), f.vote(9, "node-2", 100, 2));

        assertEquals(9, f.storage.term);
        assertNull(f.storage.vote);
        assertEquals(Role.FOLLOWER, f.state.role());
        assertNull(f.state.leaderId());
        assertTrue(f.state.nextIndex().isEmpty());
        assertTrue(f.state.matchIndex().isEmpty());
        assertEquals(before + (role == Role.LEADER ? 1 : 0), f.clock.tasks.size());
        assertFalse(f.clock.tasks.getLast().isCancelled());
        assertTrue(f.vote(9, "node-3", 3, 3).voteGranted());
    }

    @ParameterizedTest
    @EnumSource(Role.class)
    void higherTermGrantStoresNewVoteAndArmsTimerOnce(Role role) throws Exception {
        Fixture f = fixture("node-1");
        f.setRole(role);
        int before = f.clock.tasks.size();
        assertTrue(f.vote(9, "node-2", 3, 3).voteGranted());
        assertEquals(Role.FOLLOWER, f.state.role());
        assertEquals(9, f.storage.term);
        assertEquals("node-2", f.storage.vote);
        assertEquals(before + 1, f.clock.tasks.size());
    }

    @ParameterizedTest
    @EnumSource(value = Role.class, names = {"CANDIDATE", "LEADER"})
    void sameTermRequestCannotStealSelfVoteOrChangeRole(Role role) throws Exception {
        Fixture f = fixture(null);
        f.setRole(role);
        int before = f.clock.tasks.size();
        assertFalse(f.vote(6, "node-2", 100, 6).voteGranted());
        assertEquals(role, f.state.role());
        assertEquals("node-1", f.state.votedFor());
        assertEquals(0, f.storage.saves);
        assertEquals(before, f.clock.tasks.size());
    }

    @Test
    void restoringFromFakeStoragePreservesVoteWithoutClaimingProcessDurability() throws Exception {
        Fixture original = fixture(null);
        original.vote(5, "node-2", 3, 3);
        original.timer.close();
        Fixture restored = new Fixture(original.storage);
        assertFalse(restored.vote(5, "node-3", 100, 5).voteGranted());
        assertTrue(restored.vote(5, "node-2", 3, 3).voteGranted());
        assertEquals(1, restored.storage.saves);
        assertEquals(5, restored.state.currentTerm());
    }

    @ParameterizedTest
    @CsvSource({"5,3,3,false", "9,100,2,false", "9,3,3,false",
        "5,3,3,true", "9,100,2,true", "9,3,3,true"})
    void storageFailureBeforeOrAfterWriteReturnsNoReplyAndPermanentlyClosesHandler(
            long term, long index, long logTerm, boolean afterWrite) throws Exception {
        Fixture f = fixture(null);
        f.storage.failure = new RaftStorageException("injected fake storage failure");
        f.storage.failAfterWrite = afterWrite;
        assertSame(f.storage.failure, assertThrows(RaftStorageException.class,
                () -> f.vote(term, "node-2", index, logTerm)));
        assertTrue(f.clock.tasks.getLast().isCancelled());
        assertEquals(1, f.clock.tasks.size(), "no grant reset on failed persistence");
        assertTrue(f.events.stream().noneMatch(e -> e.startsWith("reply:")));
        assertThrows(IllegalStateException.class, f.timer::start);
        f.storage.failure = null;
        assertThrows(IllegalStateException.class, () -> f.vote(term, "node-2", index, logTerm));
        assertThrows(IllegalStateException.class, () -> f.vote(term + 1, "node-3", 3, 3));
        assertEquals(1, f.storage.saves, "retry cannot use an unpersisted in-memory vote");
        Fixture restored = new Fixture(f.storage);
        if (afterWrite && logTerm == 3) {
            assertFalse(restored.vote(term, "node-3", 3, 3).voteGranted());
        }
    }

    @ParameterizedTest
    @CsvSource({"-1,0,0", "0,0,0", "9,-1,1", "9,1,-1", "9,0,1", "9,1,0", "9,1,10"})
    void malformedMetadataCannotMutateStateOrResetTimer(long term, long index, long logTerm)
            throws Exception {
        Fixture f = fixture("node-4");
        assertThrows(IllegalArgumentException.class, () -> f.vote(term, "node-2", index, logTerm));
        assertUnchangedAfterInvalidRequest(f);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "node-6", "monitor-1", "client-1"})
    void nonVoterCannotAdvanceTerm(String candidate) throws Exception {
        Fixture f = fixture("node-4");
        assertThrows(IllegalArgumentException.class, () -> f.vote(9, candidate, 3, 3));
        assertUnchangedAfterInvalidRequest(f);
    }

    @Test
    void missingRequestAndMaximumTermAreHandledWithoutOverflow() throws Exception {
        Fixture f = fixture("node-4");
        assertThrows(NullPointerException.class, () -> f.handler.handle(null));
        assertUnchangedAfterInvalidRequest(f);
        assertEquals(new RequestVoteResponse(Long.MAX_VALUE, true),
                f.vote(Long.MAX_VALUE, "node-2", 1, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, f.storage.term);
    }

    @Test
    void electionServiceDelegatesThroughCoreToRealHandlerAndPropagatesFailure() throws Exception {
        Fixture f = fixture(null);
        assertTrue(f.election.handleRequestVote(new RequestVoteRequestDto(6, "node-2", 3, 3))
                .voteGranted());
        assertEquals(6, f.storage.term);
        assertEquals("node-2", f.storage.vote);
        f.storage.failure = new RaftStorageException("fake write failure");
        assertThrows(RaftStorageException.class, () -> f.election.handleRequestVote(
                new RequestVoteRequestDto(7, "node-3", 3, 3)));
        assertThrows(IllegalStateException.class, f.election::startElection);
    }

    private static void assertUnchangedAfterInvalidRequest(Fixture f) throws Exception {
        assertEquals(5, f.state.currentTerm());
        assertEquals("node-4", f.state.votedFor());
        assertEquals(0, f.storage.saves);
        assertEquals(1, f.clock.tasks.size());
        assertFalse(f.clock.tasks.getFirst().isCancelled());
        assertTrue(f.vote(6, "node-2", 3, 3).voteGranted(), "validation does not poison handler");
    }

    private static Fixture fixture(String vote) throws Exception {
        return new Fixture(new FakeStorage(5, vote, LOG, 1));
    }

    private static final class Fixture {
        final List<String> events = new ArrayList<>();
        final RecordingScheduler clock = new RecordingScheduler(events);
        final FakeStorage storage;
        final RaftState state;
        final CandidateElection election;
        final ElectionTimer timer;
        final RequestVoteHandler handler;

        Fixture(FakeStorage storage) throws Exception {
            this.storage = storage;
            storage.events = events;
            var loaded = storage.load();
            state = RaftState.restore(loaded.currentTerm(), loaded.votedFor(), loaded.log(), loaded.lastApplied());
            RaftNode core = new TestCore();
            election = new CandidateElection("node-1", core, state,
                    (peer, request, reply) -> fail("unexpected outbound RPC"),
                    term -> fail("unexpected higher-term response"),
                    () -> fail("unexpected promotion"));
            timer = new ElectionTimer(election, clock, () -> 500, failure -> fail(failure));
            handler = new RequestVoteHandler(state, storage, timer);
            timer.start();
        }

        RequestVoteResponse vote(long term, String candidate, long index, long logTerm) throws Exception {
            var response = handler.handle(new RequestVoteRequest(term, candidate, index, logTerm));
            assertEquals(state.currentTerm(), storage.term, "persist term before response");
            assertEquals(state.votedFor(), storage.vote, "persist vote before response");
            events.add("reply:" + response.term() + ":" + response.voteGranted());
            return response;
        }

        void setRole(Role role) {
            if (role == Role.FOLLOWER) {
                state.becomeFollower(5, "node-4");
            } else {
                state.startElection("node-1");
                // Seed completed self-vote; this fixture does not test campaign persistence.
                storage.term = state.currentTerm();
                storage.vote = state.votedFor();
                if (role == Role.LEADER) {
                    state.becomeLeader(PEERS);
                    timer.stop();
                }
            }
        }

        /** Test wiring of the existing core boundary, not a production RaftNode. */
        private final class TestCore implements RaftNode {
            @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request)
                    throws RaftStorageException { return handler.handle(request); }
            @Override public Role role() { return state.role(); }
            @Override public long currentTerm() { return state.currentTerm(); }
            @Override public Optional<String> leaderId() { return Optional.ofNullable(state.leaderId()); }
            @Override public long commitIndex() { return state.commitIndex(); }
            @Override public long lastApplied() { return state.lastApplied(); }
            @Override public long lastLogIndex() { return state.lastLogIndex(); }
            @Override public void onElectionTimeout() { throw new UnsupportedOperationException(); }
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

    /** Memory only, including restart simulation and ambiguous-write failure injection. */
    private static final class FakeStorage implements RaftStorage {
        long term;
        String vote;
        final List<RaftLogEntry> log;
        final long applied;
        int saves;
        List<String> events;
        Runnable beforeSave = () -> { };
        RaftStorageException failure;
        boolean failAfterWrite;

        FakeStorage(long term, String vote, List<RaftLogEntry> log, long applied) {
            this.term = term;
            this.vote = vote;
            this.log = List.copyOf(log);
            this.applied = applied;
        }

        @Override public PersistentState load() { return new PersistentState(term, vote, log, applied); }
        @Override public long loadLastApplied() { return applied; }
        @Override public void replaceSuffix(long from, List<RaftLogEntry> entries) {
            fail("RequestVote must never write the log");
        }
        @Override public void saveTermAndVote(long term, String vote) throws RaftStorageException {
            saves++;
            beforeSave.run();
            if (failure != null && !failAfterWrite) throw failure;
            this.term = term;
            this.vote = vote;
            events.add("save:" + term + ":" + vote);
            if (failure != null) throw failure;
        }
    }

    /** Captures one-shot deadlines without creating threads or using wall-clock sleeps. */
    private static final class RecordingScheduler extends ScheduledThreadPoolExecutor {
        final List<Deadline> tasks = new ArrayList<>();
        final List<String> events;

        RecordingScheduler(List<String> events) { super(1); this.events = events; }

        @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            assertEquals(500, unit.toMillis(delay));
            Deadline task = new Deadline(command);
            tasks.add(task);
            events.add("arm");
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
