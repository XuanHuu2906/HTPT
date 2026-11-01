# TV5-D01 — Election configuration and timeout policy

- Date: 2026-11-01; owner: TV5; core integration reviewer: TV4 (pending).
- Base commit: `d3ae584798a72772b2b1bdffbb1212fe12cdd3f7`, with uncommitted task changes.
- Scope: SPEC-04 section 3; SPEC-03 section 3 local monotonic timeout rule.
- Related requirements: FR07/FR08, supporting AC05/AC07. These system ACs are NOT_RUN.

## Implementation

- `ElectionConfig`: immutable millisecond intervals, default heartbeat 150 and inclusive election range 500–1000; positive heartbeat, minimum greater than heartbeat, maximum greater than minimum.
- `ElectionTimeoutPolicy`: injectable functional interface; randomized factory accepts `RandomGenerator` for deterministic tests, or creates its own randomly seeded source. Each call draws a new interval; independent nodes may still randomly collide.
- Policy returns an interval only. The future timer must measure elapsed time locally with a monotonic clock and apply SPEC-04 reset rules. No scheduler, RPC handling, term/vote persistence, or role transitions are implemented here.
- JUnit added only to backend-node test scope; existing managed version is retained. Wire contracts and SPEC are unchanged.

## Verification actually executed

Final command (via `TV5-D01-run-tests.ps1`, which captures environment, base SHA, command and exit code):

```powershell
rtk proxy powershell -NoProfile -ExecutionPolicy Bypass -File artifacts/evidence/D01/TV5-D01-run-tests.ps1 -MavenPath C:/Users/huung/.m2/wrapper/dists/apache-maven-3.9.16/56ba1f9f/bin/mvn.cmd -JavaHome C:/Users/huung/.vscode/extensions/redhat.java-1.58.0-win32-x64/jre/25.0.4.1-win32-x86_64
```

Underlying Maven arguments:

```text
-B -ntp -o -pl backend-node -am -Dtest=ElectionTimeoutPolicyTest -Dsurefire.failIfNoSpecifiedTests=false clean test
```

- Environment: Windows 11 amd64, Maven 3.9.16, Eclipse Adoptium JDK 25.0.4.1; offline cached dependencies; no DB or cluster needed.
- Result: PASS, exit 0, 19 tests, 0 failures/errors/skips. Production and test sources compiled with release 25 from clean targets.
- Assertions: default intervals, ten invalid configurations, tunable intervals, both inclusive endpoints, resampling each call, minimum range, long overflow boundary, deterministic seeded replay, independent injected sources, production range and null rejection.
- Evidence: [final build output](TV5-D01-test-jdk25.txt).
- Earlier environment attempts: [default release on JDK 24](TV5-D01-test-default-release.txt) failed with unsupported release 25; [release-24 override](TV5-D01-test-jdk24-override.txt) failed loading existing release-25 classes. The final clean build with JDK 25 resolves these environment failures without changing the project release.
- An earlier inline PowerShell attempt failed argument parsing before compilation; output retained in `TV5-D01-maven-test-jdk24-default.txt`. The script above avoids that quoting issue.

## TV4 handoff / remaining work

Inspection found only `.gitkeep` in `raft/core`; there is no existing RaftState/role implementation or TV4 confirmation in this checkout. Confirmation remains PENDING, not claimed complete.

For TV4 review: election and replication must share a single state/role in `raft/core`, with FOLLOWER/CANDIDATE/LEADER per SPEC-04. This task introduces no competing state or role type and does not depend on the final core API. Review the actual TV4-D01 model when available before wiring election handlers.

Five-node election/failover, timer reset integration and AC05/AC07 remain NOT_RUN; these unit tests do not establish distributed Raft correctness.
