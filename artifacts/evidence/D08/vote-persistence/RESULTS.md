# TV2 D8 — Persistent election state contract

## Changes and SPEC

- PersistentElectionState operates on the core-owned RaftState and delegates atomic
  term/vote writes to RaftStorage.saveTermAndVote. It does not own a second term/vote.
- RequestVoteHandler uses that boundary before resetting the timer or returning a
  response. Existing (RaftState, RaftStorage, ElectionTimer) constructor remains available;
  a new constructor accepts the core's shared PersistentElectionState instance.
- Persistence failures poison the boundary. The handler closes the timer and propagates
  the error with no grant/denial response. Validation errors do not poison the instance.
- FakeRaftStorage gains a dedicated persistTermAndVoteThenFailNext switch; existing
  replacement failure injection is unchanged. Fake is test-only, no JDBC dependency.
- Tests cover SPEC-04 sections 3/9, TC-RAFT-01, plus term-response safety relevant to
  TC-RAFT-08. Supports FR07; no system AC is claimed as passed.
- No wire DTO or RaftStorage API changes. startElection/observeHigherTerm are core
  integration methods, not a second candidate/timer. Existing production core wiring is
  not supplied here; TestCore explicitly demonstrates their use.

## Executed verification

1. `python3 artifacts/evidence/D08/vote-persistence/run-standalone.py`
   Exit 0; PASS for 10 D8 scenarios and 41 existing D6 VoteSafetyTest scenarios.
   Evidence: standalone.log. Compiles current Raft sources and FakeRaftStorage using
   JDK 25 against the existing shared/target/classes DTO bytecode. The runner converts
   test methods to temporary Java classes with plain assertion helpers. It does not
   install/emulate JUnit or execute Maven. D6 parameter values are explicitly enumerated.
2. `python3 artifacts/evidence/D07/split-vote/run-standalone.py`
   Exit 0; PASS for four D7 scenarios against D8 sources; d7-regression.log.
3. `mvn -o -B -ntp -pl backend-node -am '-Dtest=VotePersistenceTest,VoteSafetyTest,SplitVoteTest,TimerTest,ElectionTest,RaftStorageAdapterTest' -Dsurefire.failIfNoSpecifiedTests=false test`
   BLOCKED, exit 127: mvn is not installed; maven.log. Actual JUnit suite NOT_RUN.
4. `git diff --check -- backend-node/src/main/java/vn/nhom6/railway/raft/election/RequestVoteHandler.java backend-node/src/test/java/vn/nhom6/railway/raft/replication/FakeRaftStorage.java`
   Exit 0.

## D8 scenarios

- Changed term/vote stored before timer reset and grant response; shared state identity,
  log, commitIndex and lastApplied unchanged by voting.
- Duplicate grant does not write again; stale/competing candidates cannot take the vote.
- Higher-term stale-log denial persists the new term and null vote without timer reset.
- Save failure before write: no response, timer closed, all subsequent transitions refused.
- Ambiguous save after write: no response; simulated reload preserves the stored vote.
- Failed persistence of higher-term denial returns no response.
- Self-vote persists before campaign can continue; failure prevents reuse.
- Higher-term observation persists null vote, ignores equal/older observations.
- Failed higher-term observation poisons subsequent transitions even when storage recovers.
- Invalid metadata/non-voters cause no mutation/write and do not poison the adapter.

## Limits / next milestone

Fake-storage failure contract checks PASS only in the standalone runner above.
DB persistence is NOT_VERIFIED: no PostgreSQL, fsync, real process restart or TCP
cluster test was run. Reconstructing state from the same memory fake is a simulated
reload, not process durability. JDBC restart tests remain for TV2-D12 and later.
After any persistence error, node lifecycle must stop every Raft path and recover
from storage; closing this handler's timer alone does not implement that lifecycle.
Do not retry a failed instance or construct a new adapter around its dirty state.
Run actual Maven/JUnit regressions when Maven/dependencies become available.
Existing workspace edits were preserved; no commit created.
