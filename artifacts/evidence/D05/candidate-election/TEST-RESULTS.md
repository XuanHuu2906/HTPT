# D5 Candidate transition — TV5, review/integration TV4

- Date: 2026-11-05, Asia/Saigon.
- Environment: Windows 11 amd64; Eclipse Adoptium JDK 25.0.5; Maven 3.10.0.
- Base HEAD: `04fcf6d2f1c534af8519ebb8ed1c0862b1a3bed5`; tested working tree was dirty.
- Pre-existing staged ElectionService / ElectionCoreContractTest / D02 evidence and deleted
  shared .gitkeep files were left unchanged. This implementation depends on that staged interface.
- Source fingerprints: [source-hashes.txt](source-hashes.txt).
- Scope: real CandidateElection and shared RaftState, test-only fake core persistence and
  queued RequestVote transport. No PostgreSQL, sockets or five-process cluster in these tests.

## Commands actually executed

1. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=ElectionTest" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D05/candidate-election/election-test.log test`
   - Exit 1; BLOCKED by sandbox access to the Maven cache lock under `.m2/repository/.locks`.
   - No tests ran in this attempt. Log: [election-test.log](election-test.log).
2. `rtk proxy mvn -B -ntp -pl backend-node -am "-Dtest=ElectionTest" "-Dsurefire.failIfNoSpecifiedTests=false" -l artifacts/evidence/D05/candidate-election/election-test-approved.log test`
   - Re-run with approved cache access; exit 0, PASS: 24 tests, 0 failures/errors/skips.
   - Log: [election-test-approved.log](election-test-approved.log).
3. `rtk proxy mvn -B -ntp -pl backend-node -am -l artifacts/evidence/D05/candidate-election/backend-verify.log verify`
   - Approved cache access; exit 0, BUILD SUCCESS.
   - Shared: 28 tests, 0 failures/errors/skips.
   - Backend: 310 tests, 0 failures/errors, 1 skipped; includes the same 24 ElectionTest cases.
   - Combined: 337 passed, 1 skipped. PostgreSQL migration integration test was NOT_RUN:
     required `RAILWAY_TEST_JDBC_URL`, `RAILWAY_TEST_DB_USER`, `RAILWAY_TEST_DB_PASSWORD`
     configuration was absent. No credentials are recorded here.
   - Log: [backend-verify.log](backend-verify.log); candidate report: [ElectionTest.xml](ElectionTest.xml).
4. `rtk git diff --check`: exit 0 (tracked working-tree diff).

## Assertions and SPEC mapping

All rows below are unit-test results, not system acceptance results.

| Assertion | Result | Evidence in ElectionTest |
|---|---|---|
| Increment term, self-vote, persistence before all four sends, log metadata from shared state | PASS | persistsIncrementedTermAndSelfVoteBeforeSendingToAllFourPeers |
| Three distinct votes including self elect Leader; all five local IDs supported | PASS | threeDistinctVotesElectAnyNodeAndInitializeAllPeerIndices (5 cases) |
| Two votes never elect Leader; no majority reduction for offline peers | PASS | twoVotesAndOfflinePeersNeverElectLeader |
| Duplicate/self/non-voter/denied votes cannot inflate quorum | PASS | duplicateSelfNonVoterAndRefusedResponsesCannotMakeQuorum |
| Reset grants on new campaign; ignore old response terms | PASS | oldTermGrantsNeverWinAndNewCampaignDiscardsEarlierVotes |
| Response to an old request cannot count even if its response term is current | PASS | responseToOldRequestCannotVoteInNewCampaignEvenWithCurrentResponseTerm |
| Higher term clears durable vote and steps down, including late replies after promotion | PASS | higherTermGrantOrDenialPersistsClearedVoteAndStepsDown; delayedResponseCanStepDownAnAlreadyElectedLeader; higherTermFromEarlierCampaignStillUpdatesCore |
| Core term/role changes invalidate pending campaign | PASS | sameTermLeaderContactThroughCorePreventsLateVotesFromWinning; sharedCoreChangingTermInvalidatesCampaign |
| Storage failures prevent send/promotion/reuse | PASS | failedSelfVotePersistenceSendsNothingAndCannotBeReused; failedHigherTermPersistenceBlocksEverySubsequentVoteAndCampaign; failedIncomingVotePersistenceClosesElection |
| Guard promotion, delegate incoming votes, validate membership, reject term overflow | PASS | Remaining ElectionTest cases in XML report |

Supports FR07/FR08, SPEC-04 section 3, and the election subset of TC-RAFT-08.
AC05/AC06/AC07/AC09 system acceptance: NOT_RUN. Election alone proves neither commit
safety nor failover availability. Existing wire DTOs, core interfaces and SPEC are unchanged.

## Integration limits / handoff

- RaftNode currently has no production implementation or higher-term-response method.
  CandidateElection delegates durable election start and incoming votes through it;
  the required HigherTermHandler hook belongs to the core/event loop. It must persist
  `(newTerm, null)`, step down, and resume the timer if necessary. Unit tests fake it.
- The existing ElectionService permits the volatile promotion via RaftState.becomeLeader.
  CandidateElection requires three current-term grants and exactly four peers first.
  Its required onLeader hook runs after peer indices are initialized and must stop the
  timer/start TV4 heartbeat replication; tests only verify hook invocation and ordering.
- D4 ElectionTimer calls RaftNode.onElectionTimeout. Future runtime wiring must route
  this event through the coordinator once, with a core transition delegate that neither
  recursively calls the coordinator nor sends duplicate RequestVote messages.
- Transport must validate peer/channel/type/correlation and enqueue replies on the same
  event loop, never invoke inline. Network failure yields no vote. Callback storage
  failures must stop the node lifecycle; this component latches its own failed state.
- No runtime wiring, real heartbeat transport, PostgreSQL durability/restart, recovery
  catch-up, or five-node AC verification is claimed by this D5 task.

## Reverification after integrating main

- Date: 2026-11-05; same Windows/JDK/Maven environment as above.
- Base HEAD: `966c9b7d8ee5a308af188a4f0e35cdda501fca4f` on `Huu`; CandidateElection,
  ElectionTest and the existing staged interface remain uncommitted working-tree changes.
- Command: `rtk proxy mvn -B -ntp -pl backend-node -am -l artifacts/evidence/D05/candidate-election/post-main-verify.log verify`
- Actual: exit 0, BUILD SUCCESS. ElectionTest: 24 passed, 0 failures/errors/skips.
  Backend total: 364 tests, 0 failures/errors, 1 PostgreSQL integration test skipped.
- Evidence: [post-main-verify.log](post-main-verify.log).
- Review conclusion: the requested D05 Candidate transition acceptance cases are complete
  at the fake-transport unit scope: increment/self-vote/send, term-scoped distinct grants,
  3/5 promotion, 2/5 remaining Candidate, and rejection of old-term votes.
  No additional production wiring or five-node acceptance is inferred.
