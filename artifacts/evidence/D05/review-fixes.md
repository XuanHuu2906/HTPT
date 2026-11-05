# D05 review fixes — issues 1 and 2 only

- Date/environment: 2026-11-05, Windows 11 amd64, Adoptium JDK 25.0.5, Maven 3.10.0.
- Base commit: `02a5e5fd1b0216ee091445620b1d38e404bd482a` (`D05`), branch `Huu`.
- Tested the working-tree changes on that commit. Source fingerprints: [review-fixes-sources.csv](review-fixes-sources.csv).
- Scope/owners: TV4 log conflict checking; TV5 election timer/candidate composition.
- User explicitly excluded issues 3 (full Raft runtime) and 4 (promotion ownership).
  No runtime implementation or promotion API was added; CandidateElection still uses
  the existing RaftState.becomeLeader transition.

## Changes and assertions

1. `LogConsistencyChecker.findDivergence` remembers the first term conflict and continues
   checking every overlapping entry against the original log. A later same-index/term
   command mismatch throws before truncation or append. A valid batch extending beyond
   the local tail still truncates from the first divergence.
   - Reproduced the reported index 2 term divergence followed by index 3 payload violation.
   - The new regression failed before the fix and passed afterward, asserting unchanged log.
2. `ElectionTimer` takes `ElectionService` instead of `RaftNode` in both constructors.
   Its only timeout action is `election.startElection()`. CandidateElection then invokes
   the core's durable transition once and sends four RequestVote messages.
   - Updated every constructor call in the repository.
   - Timer tests now compose the real timer and CandidateElection with fake core/storage,
     queued transport replies and a virtual scheduler.
   - Assertions: one timeout -> one term increment -> one request per remote voter;
     split vote retry -> exactly one next term; repeated expired callback -> no new campaign;
     majority promotion stops the timer; persistence failure -> no vote requests.
   - RaftNode/ElectionService/CandidateElection Javadocs describe this single path.

SPEC mapping: SPEC-04 sections 3–4; TC-RAFT-08 election subset and log invariant safety;
supports FR07 and the AC07/AC08 prerequisites. Wire DTOs, protocol version, membership and
quorum are unchanged. The timer constructor is an internal Java source API change:
callers now pass an ElectionService, not a raw RaftNode.

## Commands actually run

1. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=ConflictTests" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D05/review-conflict-before.log test`
   - Exit 1, no tests executed: test discovery failed with `NoClassDefFoundError: InMemoryRaftLog`
     from existing generated classes. This attempt is not evidence of the reported log bug.
   - [Initial log](review-conflict-before.log).
2. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=ConflictTests" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D05/review-conflict-before-clean.log clean test`
   - Exit 1, expected regression FAIL: 31 tests, 1 assertion failure, 0 errors/skips.
   - `laterSameTermPayloadConflictAfterDivergenceLeavesEntireLogUntouched` expected rejection
     but the original checker accepted the batch.
   - [Reproduction log](review-conflict-before-clean.log).
3. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=ConflictTests,TimerTest,ElectionTest" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D05/review-fixes-targeted.log clean test`
   - Exit 0, PASS: ConflictTests 31, TimerTest 20, ElectionTest 24; total 75,
     no failures/errors/skips.
   - [Targeted log](review-fixes-targeted.log).
4. `rtk proxy mvn -B -ntp -pl backend-node -am -l artifacts/evidence/D05/review-fixes-verify.log verify`
   - Exit 0, BUILD SUCCESS. Backend: 367 tests, 0 failures/errors, 1 skipped.
     Shared: 28 tests, 0 failures/errors/skips.
   - PostgreSQL migration integration remains NOT_RUN without its test DB configuration.
   - [Regression log](review-fixes-verify.log).
5. `rtk proxy git diff --check`: exit 0; only line-ending conversion warnings.

These are unit/component tests with fakes, not evidence of a functioning five-node
TCP/PostgreSQL cluster. Issues 3 and 4 remain intentionally outside this change.
