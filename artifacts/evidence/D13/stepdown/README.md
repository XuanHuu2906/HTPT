# D13 — Higher-term response step-down

- Date: 2026-11-14, Asia/Ho_Chi_Minh.
- Base commit: f2eb190797650f47be699e4473876fa74b777529; tests ran against working-tree changes.
- Environment: Linux, OpenJDK 25.0.4.1 (Red Hat), Maven.
- Command: `mvn -B -ntp -pl backend-node -am -Dtest=StepDownTest,PersistentElectionRuntimeTest,RaftElectionComponentTest,ElectionTest,LeaderHeartbeatSchedulerTest -Dsurefire.failIfNoSpecifiedTests=false test`
- Exit code: 0. Result: 44 tests, 0 failures, 0 errors, 0 skipped.
- Output: [maven-test.log](maven-test.log).
- `git diff --check`: exit code 0.

The five StepDownTest cases cover delayed RequestVote/AppendEntries responses after
promotion to leader, heartbeat cancellation, subsequent write rejection, term/vote
restoration, equal/lower term no-op, failed persistence and ambiguous persisted writes.
The component shares one response handler using the existing durable transition;
wire DTOs, TCP contracts and JDBC implementation remain unchanged.

SPEC-04 sections 3/9; supports FR07/FR09 and AC07–AC09. Storage and transport in these
tests are isolated fixtures. PostgreSQL restart and five-node acceptance scenarios
were NOT_RUN by this command; this evidence does not establish those ACs as passed.

## Follow-up environment attempt

Command: `mvn -B -ntp -pl backend-node -am -Dtest=JdbcRaftStorageTest,StepDownTest -Dsurefire.failIfNoSpecifiedTests=false test`

Exit 0, but PostgreSQL tests did **not** pass: 12 JDBC cases were skipped because
RAILWAY_TEST_JDBC_URL/DB_USER/DB_PASSWORD were unset. Five StepDownTest cases passed.
See [jdbc-attempt.log](jdbc-attempt.log).

Docker preflight failed with permission denied on `/var/run/docker.sock`; no local
postgres/psql executable was found. See [environment-check.log](environment-check.log).
PostgreSQL server restart and five-node Docker execution are BLOCKED by this environment.
No containers or databases were changed.
