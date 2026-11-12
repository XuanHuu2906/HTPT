# TV5-D11 — RequestVote RPC adapter

- Scope: FR07 / AC07 support, TC-RAFT-08 unit coverage. Owner TV5; TV3 supplies peer transport after D11; TV6 reviews evidence.
- Added VoteRpcAdapter implementing CandidateElection.VoteTransport. Uses shared request DTO and v1 envelope, validates sender/type/correlation and strict term/voteGranted payload, queues callbacks on supplied Raft executor. No wire contract changes.
- RaftRpcClient is a local injection seam pending TV3 transport, not a claim that TV3-D11 TCP exists. Response is mapped to existing core RequestVoteResponse because shared response DTO is absent.
- Timeout/disconnection yields no vote; a late response after the RPC deadline is discarded. Valid higher-term replies within the deadline reach CandidateElection even when voteGranted=false. Storage callback errors reach lifecycle failure handler.
- Runtime wiring/TCP integration NOT_RUN, deferred to D12. No five-node AC07 or PostgreSQL durability claim.

## Actual validation

Environment: Linux, OpenJDK 25.0.4.1, cached Jackson 2.19.2 and JUnit 6.0.3. Base HEAD 4aec682c9671df53e6080dcd5eedeb7c2d21dca5 with uncommitted D10 runtime changes already present.

1. `mvn -B -ntp -pl backend-node -am -Dtest=VoteRpcAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test`: BLOCKED, exit 1; read-only Maven cache prevents dependency resolution. See test.log. Standard declared JUnit 5.11.4/Maven build remains unverified.
2. `python3 artifacts/evidence/D11/vote-rpc/run.py`: PASS, exit 0. Compiles shared/backend main sources and election tests, runs 190 election tests including 5 new adapter tests using cached JUnit 6.0.3. See direct-junit.log. Runner and exact compilation recipe included here.
3. `git diff --check`: exit 0.

New tests cover DTO snapshot + queued delivery, timeout/disconnect/late response, sender/correlation/schema rejection, higher-term election step-down through fake persistence hook, and storage callback failure propagation.
