# TV3 D10 — Election lifecycle integration (partial)

Environment: OpenJDK 25.0.4.1, Linux sandbox, 2026-11-10.

- `mvn -B -ntp -o -pl backend-node -am test-compile`: exit 0.
- JUnit Launcher 6.0.3, `python3 /tmp/run-raft-d10.py`: exit 0, 57/57 tests.
  Runner sources are saved here. Compile RaftD10Runner.java with the classpath assembled
  in run-raft-d10.py and place the class in /tmp before running the script from repo root.
- Default Maven Surefire 3.5.2 invocation was blocked by a missing cached dependency and
  read-only ~/.m2. Explicit cached Surefire 3.5.6 could not resolve the reactor shared JAR.
- Initial runner also included NodeMainTest: TCP attempts failed with socket creation
  forbidden by sandbox. That log also records the initial fake-core query omission,
  subsequently fixed before the successful unit run. This is not a TCP PASS.

Scope implemented: injectable RaftElectionComponent, restored state, per-node scheduler,
peer RequestVote dispatcher registration, sender validation, serialized replies,
monitor snapshot, cancellation and startup cleanup. Existing CandidateElection owns
3/5 quorum and persistence-before-send semantics.

Blockers: repository has only the RaftNode and RaftStorage interfaces, no production core
or JdbcRaftStorage, and no correlated asynchronous peer transport. Their injected
adapters are required. Default main() runs diagnostic mode and does not join a real
cluster. AppendEntries/heartbeat/replication wiring remains with core/transport owners.
No PostgreSQL restart, five-process election, AC07 or AC08 completion is claimed.
