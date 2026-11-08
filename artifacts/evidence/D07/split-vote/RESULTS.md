# D7 — Election split vote / timeout retry

Scope: user-assigned TV4 D7, dependencies D5 Candidate and D6 RequestVoteHandler.
SPEC-04 section 3; FR07/FR08; unit-level support for AC07/AC09 only.
ElectionTimer keeps its constructors and public lifecycle/RPC methods. Deadline ownership
moves into ElectionRetryCoordinator; CandidateElection and RequestVoteHandler retain
term/vote authority. No wire DTO or quorum changes. One coordinator per node.

## Executed checks

- `javac --release 25 -cp shared/target/classes -d /tmp/raft-d7/classes backend-node/src/main/java/vn/nhom6/railway/raft/core/*.java backend-node/src/main/java/vn/nhom6/railway/raft/election/*.java`
  Exit 0; `javac.log`. Compiles current Raft sources against existing compiled shared DTO.
- `python3 artifacts/evidence/D07/split-vote/run-standalone.py`
  Exit 0; `standalone.log`. Four scenarios pass with plain Java assertion helpers.
  The runner translates the JUnit test source into a temporary standalone Java class;
  it does not execute JUnit or modify production sources. Uses existing shared DTO bytecode.
- `mvn -o -B -ntp -pl backend-node -am '-Dtest=SplitVoteTest,TimerTest,ElectionTest,VoteSafetyTest,VoteRulesTest,ElectionTimeoutPolicyTest' -Dsurefire.failIfNoSpecifiedTests=false test`
  BLOCKED, exit 127: Maven not installed (`maven.log`). JUnit regression NOT_RUN.
- `git diff --check -- backend-node/src/main/java/vn/nhom6/railway/raft/election/ElectionTimer.java`
  Exit 0.

## Scenarios

1. Two candidates enter term 1 before delivering RPCs; votes go through actual
   RequestVoteHandler and VoteRules. Duplicate/reordered responses cannot produce two winners.
2. Split vote 2 versus 2 with fifth voter unreachable; distinct fresh deadlines trigger
   term 2. Late term-1 grants do not combine with term-2 votes; quorum elects one leader.
3. Seeded production random timeout policy: each retry follows the next sampled
   500–1000 ms interval for five terms.
4. Only two voters communicate: no leader; no work between deadlines; exactly four
   sends per campaign and one pending deadline. This checks bounded scheduling,
   not measured CPU utilization.

## Limits

Fake core wiring, memory storage and virtual scheduler; no TCP, PostgreSQL, process
restart or five-process integration. AC07/AC09 system acceptance remains NOT_RUN.
Run the Maven command above when Maven/dependencies are available before handoff.
Existing workspace changes were retained. No commit created.
