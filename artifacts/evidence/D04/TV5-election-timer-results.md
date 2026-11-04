# TV5 — Election timer (TV4 D4 review)

- Date: 2026-11-04, Asia/Saigon.
- Base commit: `d2922f7cc454c114a46edab127486a8f7cc89966`; tests ran against the working tree, including pre-existing uncommitted ElectionService/ElectionCoreContractTest. No commit created.
- Environment: Windows 11 amd64, Eclipse Temurin JDK 25.0.5+8, Maven 3.10.0; see `environment.log`.
- Scope: `ElectionTimer.java`, `TimerTest.java`, and evidence in this directory. Existing user changes preserved.
- SPEC: SPEC-04 section 3; SPEC-07 unit timer tests; supports FR07 / AC07 without claiming system acceptance. Core API and wire contracts unchanged.
- Owner: TV5; reviewer/dependency: TV4 State API. No human review is claimed.

## Executed checks

| Command | Actual result | Evidence |
|---|---|---|
| `rtk proxy mvn -B -ntp -pl backend-node -am -Dtest=TimerTest -Dsurefire.failIfNoSpecifiedTests=false test` | PASS, exit 0; 19 tests, 0 failures/errors/skips | `timer-unit.log` |
| `rtk proxy mvn -B -ntp -pl backend-node -am test` | PASS, exit 0; shared 13 + backend 254 = 267 tests, 0 failures/errors/skips | `timer-regression.log` |
| `rtk git diff --check` | PASS, exit 0; tracked diff check only (new files are untracked) | Recorded in task tool output |

Reproduce the two Maven commands with `rtk proxy powershell -NoProfile -File artifacts/evidence/D04/run-election-timer-tests.ps1`.

The first ad-hoc command failed before tests because nested PowerShell quoting split a Maven property (`TV5-election-timer-test.log`). The script corrected argument handling. The initial script attempt also stopped on Java version output to stderr; it now uses `java --version`. Maven then hit a sandbox write restriction on `.m2/repository/.locks`; rerunning the script with elevated sandbox permission produced the successful logs above. These setup failures are not test passes.

## Assertions covered

- Virtual timeout transitions through the existing RaftNode callback to Candidate; subsequent timeout starts another term with a fresh sampled interval.
- Valid current/higher-term leader contact resets after core processing, including log mismatch and same-term Candidate step-down.
- Stale contact, stale grant notification, rejected vote (including higher-term rejection) and unprocessed higher term do not reset.
- Granted vote resets; repeated start does not postpone the deadline.
- Cancelled callbacks delivered late cannot trigger elections after reset, stop, restart or close.
- Leader does not campaign; stop is restartable, close is terminal and does not shut down the borrowed executor.
- Storage failure, invalid delay and scheduling rejection fail closed; asynchronous failures reach the supplied failure handler.
- Production constructor draws within the configured randomized range; deterministic fake time tests use no sleep or wall clock.

## Integration boundary / remaining work

The production Raft runtime/event loop and handlers are not implemented by this task. Wire this timer to the same single-thread ScheduledExecutorService as RaftNode; invoke lifecycle and notification methods on that loop. Validate schema, peer membership and sender identity before `onValidAppendEntries`; persist term/vote and perform role transitions before notifications. Do not notify on malformed/non-voter RPCs. The timer deliberately reuses that validation boundary rather than parsing RPCs again.

Call stop on Leader promotion, start after step-down, and close on the event loop before executor shutdown. Use remove-on-cancel for a ScheduledThreadPoolExecutor. The supplied failure handler must log the failure and stop voter/writes when storage fails; this component itself closes the timer.

Persistence and network are faked in TimerTest. AC07 failover on 5 actual Nodes, PostgreSQL durability, recovery/catch-up and other cluster ACs: NOT_RUN by this task. Passing these unit tests does not establish distributed election safety/liveness.
