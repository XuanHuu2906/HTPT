# TV5-D06 — RequestVote grant conditions

- Date: 2026-11-06, Asia/Saigon.
- Owner: TV5; integration/review dependencies: TV3 (peer validation/DTO), TV4 (core lifecycle).
- Base commit: `ac9161097bb9e3e43bfe8ab98a8fb3000ffe9fc1` plus the uncommitted handler/test in this task. This is not a test of the base commit alone.
- Tested source fingerprints: [source-sha256.txt](source-sha256.txt).
- Environment observed with `rtk proxy java -version` and `rtk proxy mvn -version` (exit 0): Windows 11 amd64, Eclipse Temurin JDK 25.0.5+8 LTS, Apache Maven 3.10.0; Java release 25 from the existing POM. Working directory: `D:\HTPT\railway-distributed-system`.
- Scope: real RequestVoteHandler, VoteRules, RaftState and ElectionTimer; test-only RaftNode delegation, FakeStorage and a scheduler that captures deadlines without starting threads. No PostgreSQL or live cluster.

## Changes and contracts

- Added `backend-node/src/main/java/vn/nhom6/railway/raft/election/RequestVoteHandler.java`: core-owned delegate using the existing internal RequestVote request/response types. It checks membership, delegates all vote/log rules to VoteRules, saves changed term/vote before returning, and notifies the existing timer after persistence.
- Higher-term rejection still stores the new term and cleared vote. Repeated grants to the same candidate reuse the stored vote, while other candidates and stale logs are rejected.
- Storage failure, including an ambiguous completed write, returns no response, closes the election timer and permanently blocks the handler. The owning Node must propagate this failure to its whole lifecycle and recover from storage.
- A running timer is not reset on denial. A stopped Leader timer is started after higher-term step-down, including denial; this does not postpone an existing Follower/Candidate deadline.
- Added `backend-node/src/test/java/vn/nhom6/railway/raft/election/VoteSafetyTest.java` with 41 unit cases.
- SPEC-03 wire contracts, core interfaces, VoteRules, RaftState and ElectionTimer are unchanged. No response DTO was invented; the existing RaftNode.RequestVoteResponse remains the internal result.

## Commands actually executed

1. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=VoteSafetyTest,VoteRulesTest" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D06/request-vote/vote-tests.log test`
   - Exit 1; BLOCKED before test execution by sandbox access to the Maven cache lock under `C:\Users\huung\.m2\repository\.locks`.
   - Evidence: [vote-tests.log](vote-tests.log). This is not a failing vote assertion.
2. Same targeted tests, rerun with approved elevated execution:
   `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=VoteSafetyTest,VoteRulesTest" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D06/request-vote/vote-tests-approved.log test`
   - Exit 0; PASS: 74 tests, 0 failures, 0 errors, 0 skipped (41 VoteSafetyTest + 33 VoteRulesTest).
   - Evidence: [vote-tests-approved.log](vote-tests-approved.log).
3. `rtk proxy mvn -B -ntp -pl backend-node -am -l artifacts/evidence/D06/request-vote/backend-verify.log verify`
   - Approved elevated execution; exit 0, BUILD SUCCESS.
   - Shared: 28 tests, 0 failures/errors/skips. Backend: 433 tests, 0 failures/errors, 1 skipped; 432 executed successfully.
   - The skipped SchemaMigrationTest requires `RAILWAY_TEST_JDBC_URL`, `RAILWAY_TEST_DB_USER`, and `RAILWAY_TEST_DB_PASSWORD`; no DB migration assertion is claimed as passed.
   - Evidence: [backend-verify.log](backend-verify.log), [VoteSafetyTest.txt](VoteSafetyTest.txt), [VoteRulesTest.txt](VoteRulesTest.txt). The copied test summaries are from this verify run.

## Assertions and SPEC coverage

| Scope | Expected and actual | Status |
|---|---|---|
| SPEC-04 section 3; TC-RAFT-02 unit portion | Reject older lastLogTerm regardless of longer index; compare index when terms equal; accept equal/newer logs | PASS with FakeStorage |
| SPEC-04 sections 3/10; TC-RAFT-01 unit portion | At most one candidate per term; same-candidate retry allowed only with fresh log; fake restore preserves vote | PASS in memory only |
| Higher term | Clear old vote, step down from Follower/Candidate/Leader, persist on denial as well as grant, return current local term | PASS |
| SPEC-04 section 9 ordering | Save before timer reset/response; failed or ambiguous save returns no response and cannot reuse handler | PASS with injected fake failures |
| Timer coordination | Denied/stale/invalid votes do not reset a running timer; successful grants reset; Leader step-down starts stopped timer | PASS with captured deadlines |
| Validation and invariants | Reject non-voters/malformed metadata before mutation; preserve log/commit/apply state; maximum long term does not overflow | PASS |
| Existing ElectionService boundary | CandidateElection delegates via test core to real handler; storage failure disables subsequent campaign | PASS, test-only wiring |
| FR07 / AC07 | These unit assertions support valid election; actual Leader failover needs a live five-Node cluster | NOT_RUN system acceptance |
| PostgreSQL vote persistence / process crash recovery | Needs JDBC storage and real restart tests after D12 | NOT_RUN |

## Remaining integration work

- There is no production RaftNode implementation wired to this handler in this task. TV4 must compose it with the shared state/storage/timer on one serialized event loop and stop all other Raft activity on storage failure.
- TV3 must validate peer channel, sender identity, schema and correlation at the networking boundary; this handler checks candidate membership only. It does not implement TCP dispatch or authentication.
- No five-Node test, PostgreSQL vote persistence, real process restart, booking consensus, or full AC07 acceptance is claimed. FakeStorage restore is not evidence of durable PostgreSQL recovery.
