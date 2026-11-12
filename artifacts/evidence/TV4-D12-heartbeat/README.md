# TV4-D12 — heartbeat FakeRPC

Command: `mvn -B -ntp -pl backend-node -am '-Dtest=vn.nhom6.railway.raft.election.*Test' -Dsurefire.failIfNoSpecifiedTests=false test`

Result: exit 0; 198 tests, 0 failures/errors/skips. Includes 8 heartbeat FakeRPC tests and existing election timer tests.

Environment: OpenJDK 25.0.4.1, Linux. Base commit: `11dd33f935ca5fa7f149feaf5c00d669c2c10d12`; tested with uncommitted task changes.

SPEC: FR07/AC07 support, SPEC-04 sections 3–4; wire DTO unchanged.

Integration: construct scheduler with the existing RaftState and serialized event loop. CandidateElection.onLeader starts it after peer indices initialize. Step-down stops it; shutdown closes it before executor shutdown. HigherTermHandler must persist the new term/cleared vote, step down and arm ElectionTimer. Transport must enqueue validated, correlated replies on that loop. Normal peer timeouts produce no response.

No real RPC server added. Real five-node G4/AC07: NOT_RUN; these fake tests do not certify cluster acceptance. Heartbeat responses deliberately do not update replication indices or commit state; replication remains TV4's responsibility.
