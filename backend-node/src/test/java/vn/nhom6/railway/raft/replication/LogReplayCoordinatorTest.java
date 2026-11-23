package vn.nhom6.railway.raft.replication;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import vn.nhom6.railway.booking.ApplyResult;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.RaftApplyAdapter;
import vn.nhom6.railway.persistence.DbStateLoader;
import vn.nhom6.railway.raft.core.RaftLogEntry;
import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.RaftStateMachineApplier;
import vn.nhom6.railway.raft.core.RaftStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TV4-D22 in-memory suite for {@link LogReplayCoordinator}.
 *
 * <p><strong>Honesty boundary.</strong> Every Node is an in-memory object in this JVM. Storage is
 * {@link FakeRaftStorage} (never durable, never JDBC). "Crash" means rebuilding {@link RaftState}
 * from that fake storage while keeping the in-memory {@link BookingRepository}, exactly as
 * requested. The apply sink is a recording wrapper around the real
 * {@link CommittedCommandApplier}; the state machine is the real {@link BookingStateMachine}. No
 * thread, socket, sleep, random seed for timing, Docker container or real process is killed. The
 * gated {@code LogReplayCoordinatorJdbcTest} covers the same restart on one PostgreSQL schema when
 * the three {@code RAILWAY_TEST_*} variables are set; it is not run by this class.
 *
 * <p><strong>What this proves.</strong> The commit floor ({@code max(commitIndex, durable)}), the
 * no-apply-above-floor rule, redelivery without a second business effect, fail-closed for
 * {@code durable > lastLogIndex} and Raft-ahead-of-DB, transient retry at the same index, and
 * equality with a no-crash baseline for every crash point of a mixed BOOK/NO_OP/CANCEL/duplicate
 * workload.
 */
class LogReplayCoordinatorTest {

    private static final String TRIP = BookingCommand.VALID_TRIP_ID;
    private static final Instant BASE = Instant.parse("2026-11-22T08:00:00Z");

    private static final List<Long> SEEDS =
            List.of(1L, 2L, 3L, 7L, 11L, 13L, 17L, 19L, 23L, 29L,
                    31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L, 71L);

    // -------------------------------------------------------------------------------------------
    // 1. Clean restart: checkpoints agree, the uncommitted tail is not applied
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("1. Clean restart replays nothing and leaves the uncommitted tail untouched")
    void cleanRestartWithUncommittedTailReplaysNothing() {
        List<RaftLogEntry> log = mixedLog(7L);
        InMemoryBookingRepository repository = repositoryWithPrefix(log, 3);
        Map<String, String> seatsBefore = new TreeMap<>(repository.readSeatSnapshot());

        FakeRaftStorage storage = storageOf(log, 3L);
        Fixture fixture = restart(storage, repository);
        assertEquals(3L, fixture.state.commitIndex(), "restore must set commitIndex = lastApplied");

        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);
        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report.outcome(), report.reason());
        assertFalse(coordinator.failed());
        assertEquals(3L, report.raftLastApplied());
        assertEquals(3L, report.durableLastApplied());
        assertEquals(3L, report.commitFloor());
        assertEquals(log.size(), report.lastLogIndex());
        assertEquals(log.size() - 3L, report.uncommittedSuffixLength());
        assertEquals(0, report.redelivered());
        assertTrue(fixture.sink.indices().isEmpty(), "no entry may be delivered on a clean restart");
        assertEquals(seatsBefore, new TreeMap<>(repository.readSeatSnapshot()));
        assertEquals(log, fixture.state.entries(), "the uncommitted tail must stay in the log");
    }

    // -------------------------------------------------------------------------------------------
    // 2. Crash between the durable sink and markApplied: redelivery, no second effect
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("2. Crash after the sink but before markApplied re-delivers <= durable only")
    void crashBetweenSinkAndMarkAppliedRedeliversWithoutSecondBusinessEffect() {
        List<RaftLogEntry> log = mixedLog(13L);
        // The repository already contains entries 1..4; Raft only recorded 3.
        InMemoryBookingRepository repository = repositoryWithPrefix(log, 4);
        Map<String, String> seatsBefore = new TreeMap<>(repository.readSeatSnapshot());
        List<Optional<ApplyResult>> resultsBefore = storedResults(repository, log);
        int applyCallsBefore = repository.applyCalls();

        FakeRaftStorage storage = storageOf(log, 3L);
        Fixture fixture = restart(storage, repository);
        assertEquals(3L, fixture.state.commitIndex());
        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
        assertFalse(coordinator.failed());
        assertEquals(4L, report.commitFloor(), "the durable checkpoint is the commit floor");
        assertEquals(4L, report.replayedFrom());
        assertEquals(4L, report.replayedTo());
        assertEquals(1, report.redelivered());
        assertEquals(log.size() - 4L, report.uncommittedSuffixLength());
        assertEquals(4L, fixture.state.lastApplied());
        assertEquals(4L, repository.lastApplied());
        assertEquals(0, repository.applyCalls() - applyCallsBefore,
                "index 4 was already durable; redelivery must not run applyAtomically again");
        assertEquals(List.of(4L), fixture.sink.indices(), "only the checkpoint gap is re-delivered");
        assertTrue(fixture.results.isEmpty(),
                "redelivery must not publish a second business event");
        assertEquals(seatsBefore, new TreeMap<>(repository.readSeatSnapshot()));
        assertEquals(resultsBefore, storedResults(repository, log));
    }

    // -------------------------------------------------------------------------------------------
    // 3. Raft checkpoint ahead of the durable state machine: fail closed, then restoreAligned
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("3. Raft ahead of DB fails closed; restoreAligned replays each missing entry once")
    void raftCheckpointAheadOfDatabaseFailsClosedUntilRestoreAligned() {
        List<RaftLogEntry> log = mixedLog(17L);
        InMemoryBookingRepository repository = repositoryWithPrefix(log, 2);
        RaftStorage.PersistentState persisted = persistedOf(storageOf(log, 4L));

        RaftState stale = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        Fixture broken = new Fixture(stale, repository);
        LogReplayCoordinator coordinator = coordinator(broken, repository::lastApplied);

        LogReplayCoordinator.ReplayReport failure = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, failure.outcome());
        assertTrue(coordinator.failed());
        assertTrue(failure.reason().contains("restoreAligned"),
                "the reason must point at the documented recovery: " + failure.reason());
        assertEquals(4L, stale.lastApplied(), "a fail-closed replay must not touch apply progress");
        assertEquals(2L, repository.lastApplied());
        assertTrue(broken.sink.indices().isEmpty(), "nothing may be applied on the fail-closed path");
        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, coordinator.replayOnStartup().outcome(),
                "a latched coordinator stays failed closed");

        // Recover exactly as documented: rebuild from storage using the durable checkpoint.
        RaftState rebuilt = LogReplayCoordinator.restoreAligned(persisted, 2L);
        assertEquals(2L, rebuilt.lastApplied());
        assertEquals(2L, rebuilt.commitIndex());
        Fixture fixture = new Fixture(rebuilt, repository);
        LogReplayCoordinator recovered = coordinator(fixture, repository::lastApplied);

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY,
                recovered.replayOnStartup().outcome());

        rebuilt.advanceCommitIndex(6L); // the Leader relearns the commit knowledge
        LogReplayCoordinator.ReplayReport report = recovered.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
        assertEquals(0, report.redelivered(), "indices 3..6 were never applied before the crash");
        assertEquals(6L, rebuilt.lastApplied());
        assertEquals(6L, repository.lastApplied());
        assertEquals(List.of(3L, 4L, 5L, 6L), fixture.sink.indices(),
                "every missing entry is applied exactly once, in order");

        Baseline baseline = baseline(log.subList(0, 6));
        assertMatchesBaseline(repository, baseline, 6, "restoreAligned recovery");
    }

    // -------------------------------------------------------------------------------------------
    // 4. Durable checkpoint beyond the log: fail closed and refuse to align
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("4. Durable checkpoint beyond the log fails closed and restoreAligned rejects it")
    void durableCheckpointBeyondLogFailsClosed() {
        List<RaftLogEntry> log = mixedLog(19L);
        RaftStorage.PersistentState persisted = persistedOf(storageOf(log, 0L));
        RaftState state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        Fixture fixture = new Fixture(state, repository);

        LogReplayCoordinator coordinator = coordinator(fixture, () -> log.size() + 1L);
        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, report.outcome());
        assertTrue(coordinator.failed());
        assertTrue(report.reason().contains("beyond lastLogIndex"), report.reason());
        assertEquals(0L, state.lastApplied());
        assertEquals(0L, repository.lastApplied());
        assertTrue(fixture.sink.indices().isEmpty());
        assertThrows(IllegalArgumentException.class,
                () -> LogReplayCoordinator.restoreAligned(persisted, log.size() + 1L));

        LogReplayCoordinator negative = coordinator(new Fixture(
                RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                        persisted.log(), persisted.lastApplied()),
                new InMemoryBookingRepository()), () -> -1L);
        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, negative.replayOnStartup().outcome());
        assertTrue(negative.failed());
    }

    // -------------------------------------------------------------------------------------------
    // 5. Every crash point vs the no-crash baseline (the core D22 requirement)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("5. Every crash point converges to the same state as the no-crash baseline")
    void everyCrashPointMatchesTheBaselineRun() {
        for (long seed : SEEDS) {
            List<RaftLogEntry> log = mixedLog(seed);
            Baseline baseline = baseline(log);
            for (int applied = 0; applied <= log.size(); applied++) {
                // Raft and the repository both recorded `applied` entries.
                assertCrashPoint(log, baseline, applied, applied, seed,
                        "clean crash at k=" + applied);
                if (applied < log.size()) {
                    // The sink committed entry applied+1 but Raft did not mark it applied.
                    assertCrashPoint(log, baseline, applied, applied + 1, seed,
                            "crash after sink before markApplied at k=" + applied);
                }
            }
        }
    }

    private static void assertCrashPoint(
            List<RaftLogEntry> log,
            Baseline baseline,
            int raftApplied,
            int repositoryApplied,
            long seed,
            String label) {
        String context = "seed=" + seed + " " + label;
        InMemoryBookingRepository repository = repositoryWithPrefix(log, repositoryApplied);
        List<ApplyResult> preCrashEvents = eventsOfPrefix(log, repositoryApplied);
        assertEquals(repositoryApplied, repository.lastApplied(), context);

        FakeRaftStorage storage = storageOf(log, raftApplied);
        Fixture fixture = restart(storage, repository);
        assertEquals(raftApplied, fixture.state.lastApplied(), context);
        assertEquals(raftApplied, fixture.state.commitIndex(), context);

        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);
        LogReplayCoordinator.ReplayReport first = coordinator.replayOnStartup();

        assertFalse(coordinator.failed(), context + ": " + first.reason());
        assertEquals(expectedOutcome(raftApplied, repositoryApplied), first.outcome(), context);
        assertEquals(repositoryApplied, first.durableLastApplied(), context);
        assertEquals(repositoryApplied, first.commitFloor(), context);
        assertEquals(log.size() - repositoryApplied, first.uncommittedSuffixLength(), context);
        assertEquals(repositoryApplied, fixture.state.lastApplied(), context);
        assertEquals(repositoryApplied, repository.lastApplied(), context);
        assertEquals(repositoryApplied - raftApplied, first.redelivered(), context);
        if (repositoryApplied > raftApplied) {
            assertEquals(raftApplied + 1L, first.replayedFrom(), context);
            assertEquals(repositoryApplied, first.replayedTo(), context);
        } else {
            assertEquals(0L, first.replayedFrom(), context);
            assertEquals(0L, first.replayedTo(), context);
        }
        assertTrue(fixture.results.isEmpty(),
                context + ": redelivery must not emit a business event");

        fixture.state.advanceCommitIndex(log.size());
        LogReplayCoordinator.ReplayReport second = coordinator.replayOnStartup();

        if (repositoryApplied < log.size()) {
            assertEquals(LogReplayCoordinator.Outcome.REPLAYED, second.outcome(), context);
            assertEquals(0, second.redelivered(), context);
        } else {
            assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, second.outcome(), context);
        }
        assertEquals(log.size(), fixture.state.lastApplied(), context);
        assertEquals(log.size(), repository.lastApplied(), context);

        List<Long> expectedSinkCalls = LongStream
                .rangeClosed(raftApplied + 1L, log.size() - 1L + 1L)
                .boxed()
                .toList();
        assertEquals(expectedSinkCalls, fixture.sink.indices(),
                context + ": every entry above the Raft checkpoint is delivered exactly once");

        assertMatchesBaseline(repository, baseline, log.size(), context);
        List<ApplyResult> allEvents = new ArrayList<>(preCrashEvents);
        allEvents.addAll(fixture.results);
        assertEquals(baseline.events(), allEvents,
                context + ": pre-crash plus post-restart business events");
    }

    private static LogReplayCoordinator.Outcome expectedOutcome(int raftApplied, int repositoryApplied) {
        return repositoryApplied > raftApplied
                ? LogReplayCoordinator.Outcome.REPLAYED
                : LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY;
    }

    // -------------------------------------------------------------------------------------------
    // 6. Never apply a different-term / different-payload uncommitted tail
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("6. An uncommitted conflicting tail is never applied and stays in the log")
    void uncommittedConflictingTailIsNeverApplied() {
        RaftLogEntry committed = entry(1L, 1L, book(101L, "G01", "Passenger-101"));
        RaftLogEntry conflictingTail = entry(2L, 3L, book(102L, "G05", "Passenger-102"));
        List<RaftLogEntry> log = List.of(committed, conflictingTail);

        InMemoryBookingRepository repository = repositoryWithPrefix(log, 1);
        Fixture fixture = restart(storageOf(3L, log, 1L), repository);
        assertEquals(3L, fixture.state.currentTerm());
        // The persisted term is newer than the tail's term but the tail is still uncommitted.
        fixture.state.becomeFollower(5L);

        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);
        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report.outcome(), report.reason());
        assertEquals(1L, report.commitFloor());
        assertEquals(1L, report.uncommittedSuffixLength());
        assertEquals(log, fixture.state.entries(), "the conflicting tail must not be truncated here");
        assertEquals(conflictingTail, fixture.state.entryAt(2L));
        assertEquals(1L, fixture.state.commitIndex());
        assertEquals(1L, repository.lastApplied());
        assertTrue(fixture.sink.indices().isEmpty(), "an uncommitted entry must never reach the sink");
    }

    // -------------------------------------------------------------------------------------------
    // 7. Transient storage failure vs fatal decode failure
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("7a. A transient storage failure retries the same index and loses no entry")
    void transientStorageFailureRetriesTheSameIndex() {
        List<RaftLogEntry> log = mixedLog(23L).subList(0, 5);
        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        Fixture fixture = restart(storageOf(log, 0L), repository);
        fixture.state.advanceCommitIndex(5L);
        fixture.sink.failNext(new BookingRepository.StorageException("fixture: transient apply failure"));
        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);

        LogReplayCoordinator.ReplayReport first = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.RETRYABLE_FAILURE, first.outcome(), first.reason());
        assertFalse(coordinator.failed(), "a StorageException must stay retryable");
        assertEquals(0L, fixture.state.lastApplied(), "index 1 was not marked applied");
        assertEquals(0L, repository.lastApplied());
        assertEquals(List.of(1L), fixture.sink.indices(), "index 1 was attempted once");

        LogReplayCoordinator.ReplayReport second = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, second.outcome(), second.reason());
        assertEquals(0, second.redelivered(), "the durable checkpoint never moved");
        assertEquals(5L, fixture.state.lastApplied());
        assertEquals(5L, repository.lastApplied());
        assertEquals(List.of(1L, 1L, 2L, 3L, 4L, 5L), fixture.sink.indices(),
                "the retry repeats index 1 and continues; no index is skipped");
        assertEquals(5, repository.applyCalls(), "each entry really applied exactly once");
        assertMatchesBaseline(repository, baseline(log), log.size(), "transient retry");
    }

    @Test
    @DisplayName("7b. A fatal decode failure latches failed closed and applies nothing more")
    void fatalDecodeFailureLatchesClosed() {
        RaftLogEntry valid = entry(1L, 1L, book(201L, "G01", "Passenger-201"));
        RaftLogEntry corrupt = new RaftLogEntry(2L, 1L,
                RaftLogEntry.COMMAND_TYPE_BOOK_TICKET, "{not valid json");
        List<RaftLogEntry> log = List.of(valid, corrupt);

        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        Fixture fixture = restart(storageOf(log, 0L), repository);
        fixture.state.advanceCommitIndex(2L);
        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);

        LogReplayCoordinator.ReplayReport first = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, first.outcome());
        assertTrue(coordinator.failed());
        assertTrue(first.reason().contains("fatal"), first.reason());
        assertEquals(1L, fixture.state.lastApplied());
        assertEquals(1L, repository.lastApplied());
        assertEquals(List.of(1L), fixture.sink.indices(),
                "index 2 is decoded before the sink, so a corrupt payload never reaches it");

        LogReplayCoordinator.ReplayReport second = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.FAILED_CLOSED, second.outcome());
        assertEquals(List.of(1L), fixture.sink.indices(),
                "a latched coordinator must not call the sink again");
        assertEquals(1, repository.applyCalls());
    }

    // -------------------------------------------------------------------------------------------
    // 8. Idempotent repeated calls and an ignored reentrant call
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("8. Repeated and nested calls are idempotent and never recurse")
    void repeatedAndNestedCallsAreIdempotent() {
        List<RaftLogEntry> log = mixedLog(29L).subList(0, 3);
        InMemoryBookingRepository repository = repositoryWithPrefix(log, 3);
        Fixture fixture = restart(storageOf(log, 3L), repository);
        LogReplayCoordinator coordinator = coordinator(fixture, repository::lastApplied);

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY,
                coordinator.replayOnStartup().outcome());
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY,
                coordinator.replayOnStartup().outcome());
        assertTrue(fixture.sink.indices().isEmpty());

        InMemoryBookingRepository replayRepository = new InMemoryBookingRepository();
        AtomicReference<LogReplayCoordinator> reference = new AtomicReference<>();
        AtomicReference<LogReplayCoordinator.ReplayReport> nested = new AtomicReference<>();
        LogReplayCoordinator.DurableApplyCheckpoint reentrantCheckpoint = () -> {
            if (nested.get() == null && reference.get() != null) {
                nested.set(reference.get().replayOnStartup());
            }
            return replayRepository.lastApplied();
        };
        Fixture replayFixture = restart(storageOf(log, 0L), replayRepository);
        replayFixture.state.advanceCommitIndex(3L);
        LogReplayCoordinator replayCoordinator =
                new LogReplayCoordinator(replayFixture.state, replayFixture.applier, reentrantCheckpoint);
        reference.set(replayCoordinator);

        LogReplayCoordinator.ReplayReport outer = replayCoordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, outer.outcome(), outer.reason());
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, nested.get().outcome());
        assertTrue(nested.get().reason().contains("reentrant"), nested.get().reason());
        assertEquals(List.of(1L, 2L, 3L), replayFixture.sink.indices(),
                "the nested call must not apply anything on its own");
        assertEquals(3L, replayFixture.state.lastApplied());
    }

    // -------------------------------------------------------------------------------------------
    // 9. Genesis with an empty log
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("9. Genesis with an empty log has nothing to replay")
    void genesisEmptyLogHasNothingToReplay() {
        Fixture fixture = new Fixture(new RaftState(), new InMemoryBookingRepository());
        LogReplayCoordinator coordinator = coordinator(fixture, fixture.repository::lastApplied);

        LogReplayCoordinator.ReplayReport report = coordinator.replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report.outcome());
        assertFalse(coordinator.failed());
        assertEquals(0L, report.raftLastApplied());
        assertEquals(0L, report.durableLastApplied());
        assertEquals(0L, report.commitFloor());
        assertEquals(0L, report.lastLogIndex());
        assertEquals(0L, report.uncommittedSuffixLength());
        assertTrue(fixture.sink.indices().isEmpty());
    }

    // -------------------------------------------------------------------------------------------
    // 10. DbStateLoader startup state agrees with the commit floor
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("10. DbStateLoader initialCommitIndex equals the coordinator commit floor")
    void dbStateLoaderStartupStateMatchesCommitFloor() {
        List<RaftLogEntry> log = mixedLog(31L);
        long durable = 4L;
        FakeRaftStorage storage = storageOf(log, durable);
        InMemoryBookingRepository repository = repositoryWithPrefix(log, (int) durable);

        DbStateLoader loader = new DbStateLoader(storage, repository, (DbStateLoader.ConnectionSupplier) null);
        DbStateLoader.NodeStartupState startup = loader.load();

        assertEquals(durable, startup.lastApplied());
        assertEquals(durable, startup.initialCommitIndex(),
                "SPEC-04 section 6: the initial commit index is lastApplied, not lastLogIndex");
        assertEquals(DbStateLoader.BootstrapType.RESTART, startup.bootstrapType());

        RaftState state = RaftState.restore(startup.currentTerm(), startup.votedFor(),
                storage.storedLog(), startup.lastApplied());
        Fixture fixture = new Fixture(state, repository);
        LogReplayCoordinator.ReplayReport report =
                coordinator(fixture, repository::lastApplied).replayOnStartup();

        assertEquals(startup.initialCommitIndex(), report.commitFloor());
        assertEquals(LogReplayCoordinator.Outcome.NOTHING_TO_REPLAY, report.outcome(), report.reason());
        assertEquals(startup.lastLogIndex(), report.lastLogIndex());
        assertEquals(startup.lastLogIndex() - durable, report.uncommittedSuffixLength());
    }

    // -------------------------------------------------------------------------------------------
    // 11. Explicit check of the redelivery contract (D22 requirement 2.3 #11)
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("11. CommittedCommandApplier redelivery does not call ApplyResultListener again")
    void redeliveryDoesNotReplayBusinessEvents() {
        List<RaftLogEntry> log = mixedLog(37L).subList(0, 4);
        InMemoryBookingRepository repository = repositoryWithPrefix(log, 4);
        int applyCallsBefore = repository.applyCalls();

        Fixture fixture = restart(storageOf(log, 1L), repository);
        LogReplayCoordinator.ReplayReport report =
                coordinator(fixture, repository::lastApplied).replayOnStartup();

        assertEquals(LogReplayCoordinator.Outcome.REPLAYED, report.outcome(), report.reason());
        assertEquals(3, report.redelivered(), "indices 2, 3 and 4 are already durable");
        assertEquals(List.of(2L, 3L, 4L), fixture.sink.indices(),
                "every index in the checkpoint gap is re-delivered, including the durable one");
        assertTrue(fixture.results.isEmpty(),
                "CommittedCommandApplier.applyCommitted redelivery returns the stored outcome without"
                        + " invoking ApplyResultListener; this test asserts that real behaviour");
        assertEquals(0, repository.applyCalls() - applyCallsBefore,
                "a pure redelivery must not reach applyAtomically again");
    }

    // -------------------------------------------------------------------------------------------
    // Crash-point plumbing
    // -------------------------------------------------------------------------------------------

    private static Fixture restart(FakeRaftStorage storage, InMemoryBookingRepository repository) {
        RaftStorage.PersistentState persisted = persistedOf(storage);
        RaftState state = RaftState.restore(persisted.currentTerm(), persisted.votedFor(),
                persisted.log(), persisted.lastApplied());
        return new Fixture(state, repository);
    }

    private static RaftStorage.PersistentState persistedOf(FakeRaftStorage storage) {
        try {
            return storage.load();
        } catch (RaftStorage.RaftStorageException failure) {
            throw new AssertionError("fake storage must not fail in this fixture", failure);
        }
    }

    private static FakeRaftStorage storageOf(List<RaftLogEntry> log, long applied) {
        return storageOf(1L, log, applied);
    }

    private static FakeRaftStorage storageOf(long term, List<RaftLogEntry> log, long applied) {
        FakeRaftStorage storage = new FakeRaftStorage();
        storage.seedState(term, null, log, applied);
        return storage;
    }

    private static LogReplayCoordinator coordinator(
            Fixture fixture, LogReplayCoordinator.DurableApplyCheckpoint checkpoint) {
        return new LogReplayCoordinator(fixture.state, fixture.applier, checkpoint);
    }

    private static InMemoryBookingRepository repositoryWithPrefix(List<RaftLogEntry> log, int count) {
        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        CommittedCommandApplier committed = new CommittedCommandApplier(repository);
        for (int position = 0; position < count; position++) {
            RaftLogEntry entry = log.get(position);
            committed.applyCommitted(entry.index(), RaftApplyAdapter.decodeCommand(entry));
        }
        assertEquals(count, repository.lastApplied());
        return repository;
    }

    private static List<ApplyResult> eventsOfPrefix(List<RaftLogEntry> log, int count) {
        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        List<ApplyResult> events = new ArrayList<>();
        CommittedCommandApplier committed = new CommittedCommandApplier(repository, events::add);
        for (int position = 0; position < count; position++) {
            RaftLogEntry entry = log.get(position);
            committed.applyCommitted(entry.index(), RaftApplyAdapter.decodeCommand(entry));
        }
        return events;
    }

    private static Baseline baseline(List<RaftLogEntry> log) {
        InMemoryBookingRepository repository = new InMemoryBookingRepository();
        List<ApplyResult> events = new ArrayList<>();
        CommittedCommandApplier committed = new CommittedCommandApplier(repository, events::add);
        for (RaftLogEntry entry : log) {
            committed.applyCommitted(entry.index(), RaftApplyAdapter.decodeCommand(entry));
        }
        Map<Long, Optional<BookingStateMachine.ApplyOutcome>> outcomes = new LinkedHashMap<>();
        for (long index = 1L; index <= log.size(); index++) {
            outcomes.put(index, repository.findAppliedOutcome(index));
        }
        Map<String, Optional<ApplyResult>> results = new TreeMap<>();
        for (RaftLogEntry entry : log) {
            if (RaftLogEntry.COMMAND_TYPE_NO_OP.equals(entry.commandType())) {
                continue;
            }
            String requestId = requestIdOf(RaftApplyAdapter.decodeCommand(entry));
            results.put(requestId, repository.findResultByRequestId(requestId));
        }
        return new Baseline(repository.readSeatSnapshot(), List.copyOf(events),
                Map.copyOf(outcomes), Map.copyOf(results));
    }

    private static void assertMatchesBaseline(
            InMemoryBookingRepository repository, Baseline baseline, int entries, String context) {
        assertEquals(new TreeMap<>(baseline.seats()), new TreeMap<>(repository.readSeatSnapshot()),
                context + ": seat snapshot");
        for (long index = 1L; index <= entries; index++) {
            assertEquals(baseline.outcomes().get(index), repository.findAppliedOutcome(index),
                    context + ": durable outcome at index " + index);
        }
        for (Map.Entry<String, Optional<ApplyResult>> expected : baseline.results().entrySet()) {
            assertEquals(expected.getValue(),
                    repository.findResultByRequestId(expected.getKey()),
                    context + ": durable result for requestId " + expected.getKey());
        }
    }

    private static List<Optional<ApplyResult>> storedResults(
            InMemoryBookingRepository repository, List<RaftLogEntry> log) {
        List<Optional<ApplyResult>> results = new ArrayList<>();
        for (RaftLogEntry entry : log) {
            if (RaftLogEntry.COMMAND_TYPE_NO_OP.equals(entry.commandType())) {
                continue;
            }
            results.add(repository.findResultByRequestId(requestIdOf(RaftApplyAdapter.decodeCommand(entry))));
        }
        return results;
    }

    private record Baseline(
            Map<String, String> seats,
            List<ApplyResult> events,
            Map<Long, Optional<BookingStateMachine.ApplyOutcome>> outcomes,
            Map<String, Optional<ApplyResult>> results) {
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private static final class Fixture {

        final RaftState state;
        final InMemoryBookingRepository repository;
        final RecordingSink sink;
        final List<ApplyResult> results = new ArrayList<>();
        final RaftStateMachineApplier applier;

        Fixture(RaftState state, InMemoryBookingRepository repository) {
            this.state = state;
            this.repository = repository;
            CommittedCommandApplier committed = new CommittedCommandApplier(repository, results::add);
            this.sink = new RecordingSink(committed);
            this.applier = new RaftStateMachineApplier(state, RaftApplyAdapter::decodeCommand, sink);
        }
    }

    /** Recording sink around the real committed applier; can inject one transient failure. */
    private static final class RecordingSink implements CommittedCommandApplier.CommitListener {

        private final CommittedCommandApplier delegate;
        private final List<Long> indices = new ArrayList<>();
        private final List<BookingCommand> commands = new ArrayList<>();
        private RuntimeException nextFailure;

        RecordingSink(CommittedCommandApplier delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> onCommitted(long logIndex, BookingCommand command) {
            indices.add(logIndex);
            commands.add(command);
            if (nextFailure != null) {
                RuntimeException failure = nextFailure;
                nextFailure = null;
                throw failure;
            }
            return delegate.applyCommitted(logIndex, command);
        }

        List<Long> indices() {
            return List.copyOf(indices);
        }

        void failNext(RuntimeException failure) {
            this.nextFailure = failure;
        }
    }

    /**
     * Test-only repository with transactional semantics: changes are staged on a rebuilt state
     * machine and published only on success. Not JDBC and not durable across processes.
     */
    private static final class InMemoryBookingRepository implements BookingRepository {

        private record AppliedEntry(long index, BookingCommand command) {
        }

        private BookingStateMachine machine = new BookingStateMachine();
        private final List<AppliedEntry> applied = new ArrayList<>();
        private final Map<Long, BookingStateMachine.ApplyOutcome> outcomes = new LinkedHashMap<>();
        private int applyCalls;

        @Override
        public long lastApplied() {
            return machine.lastApplied();
        }

        @Override
        public BookingStateMachine.ApplyOutcome applyAtomically(long logIndex, BookingCommand command) {
            applyCalls++;
            BookingStateMachine staged = new BookingStateMachine();
            for (AppliedEntry entry : applied) {
                staged.apply(entry.index(), entry.command());
            }
            BookingStateMachine.ApplyOutcome outcome = staged.apply(logIndex, command);
            machine = staged;
            applied.add(new AppliedEntry(logIndex, command));
            outcomes.put(logIndex, outcome);
            return outcome;
        }

        @Override
        public Optional<ApplyResult> findResultByRequestId(String requestId) {
            return machine.storedResult(requestId);
        }

        @Override
        public Optional<BookingStateMachine.ApplyOutcome> findAppliedOutcome(long logIndex) {
            return Optional.ofNullable(outcomes.get(logIndex));
        }

        @Override
        public Map<String, String> readSeatSnapshot() {
            return machine.allSeatStatuses();
        }

        int applyCalls() {
            return applyCalls;
        }
    }

    // -------------------------------------------------------------------------------------------
    // Deterministic entries: BOOK duplicate seat, duplicate requestId, CANCEL and NO_OP
    // -------------------------------------------------------------------------------------------

    static List<RaftLogEntry> mixedLog(long seed) {
        int offset = (int) Math.floorMod(seed, 10L);
        String seatA = seat(offset);
        String seatB = seat(offset + 1);

        BookingCommand firstBook = book(seed * 100L + 1L, seatA, "Passenger-A");
        BookingCommand sameSeatBook = book(seed * 100L + 2L, seatA, "Passenger-B");
        BookingCommand cancelFirst = cancel(seed * 100L + 3L, ticket(seed, 1));
        BookingCommand noOp = new BookingCommand.NoOp();
        BookingCommand rebook = book(seed * 100L + 4L, seatA, "Passenger-C");
        BookingCommand duplicateRequestDifferentIntent =
                book(seed * 100L + 4L, seatB, "Passenger-D");
        BookingCommand replaySameIntent = book(seed * 100L + 2L, seatA, "Passenger-B");
        BookingCommand cancelMissing = cancel(seed * 100L + 5L, ticket(seed, 999));

        return List.of(
                entry(1L, 1L, firstBook),
                entry(2L, 1L, sameSeatBook),
                entry(3L, 1L, cancelFirst),
                entry(4L, 1L, noOp),
                entry(5L, 1L, rebook),
                entry(6L, 1L, duplicateRequestDifferentIntent),
                entry(7L, 1L, replaySameIntent),
                entry(8L, 1L, cancelMissing));
    }

    private static String seat(int value) {
        return String.format(Locale.ROOT, "G%02d", Math.floorMod(value, 10) + 1);
    }

    static BookingCommand.BookTicket book(long sequence, String seatId, String passengerName) {
        return new BookingCommand.BookTicket(
                requestId(sequence), TRIP, seatId, passengerName, ticket(sequence, 0),
                BASE.plusMillis(sequence));
    }

    static BookingCommand.CancelTicket cancel(long sequence, String ticketId) {
        return new BookingCommand.CancelTicket(
                requestId(sequence), ticketId, BASE.plusMillis(sequence));
    }

    static RaftLogEntry entry(long index, long term, BookingCommand command) {
        return new RaftLogEntry(index, term, command.type().name(), payload(command));
    }

    static String payload(BookingCommand command) {
        return switch (command) {
            case BookingCommand.BookTicket book -> "{\"requestId\":\"" + book.requestId()
                    + "\",\"tripId\":\"" + book.tripId()
                    + "\",\"seatId\":\"" + book.seatId()
                    + "\",\"passengerName\":\"" + book.passengerName()
                    + "\",\"ticketId\":\"" + book.ticketId()
                    + "\",\"issuedAt\":\"" + book.issuedAt() + "\"}";
            case BookingCommand.CancelTicket cancel -> "{\"requestId\":\"" + cancel.requestId()
                    + "\",\"ticketId\":\"" + cancel.ticketId()
                    + "\",\"cancelledAt\":\"" + cancel.cancelledAt() + "\"}";
            case BookingCommand.NoOp ignored -> "{}";
        };
    }

    static String requestId(long sequence) {
        return String.format(Locale.ROOT, "%08x-0000-4000-8000-%012x", sequence, sequence);
    }

    static String requestIdOf(BookingCommand command) {
        return switch (command) {
            case BookingCommand.BookTicket book -> book.requestId();
            case BookingCommand.CancelTicket cancel -> cancel.requestId();
            case BookingCommand.NoOp ignored -> "";
        };
    }

    private static String ticket(long seed, int number) {
        return "tk-" + seed + "-" + number;
    }
}
