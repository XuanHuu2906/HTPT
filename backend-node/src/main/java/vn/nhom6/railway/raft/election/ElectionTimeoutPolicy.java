package vn.nhom6.railway.raft.election;

import java.util.Objects;
import java.util.Random;
import java.util.random.RandomGenerator;

/**
 * Supplies a fresh local waiting interval when the election timer is armed/reset.
 * The caller measures elapsed time using a local monotonic clock (for example
 * System.nanoTime()), never wall-clock time or a peer's sentAt timestamp.
 *
 * <p>This policy does not own a timer, Raft state, or a role. Election and
 * replication must use the single raft/core state owned jointly by TV4/TV5.
 * The election handler decides whether an RPC permits a reset under SPEC-04;
 * sampling a timeout does not itself grant a vote or change a term/role.
 */
@FunctionalInterface
public interface ElectionTimeoutPolicy {

    long nextTimeoutMillis();

    /**
     * Creates a policy with its own randomly seeded source. Nodes must not use
     * the same fixed seed in production. Random draws can still collide.
     */
    static ElectionTimeoutPolicy randomized(ElectionConfig config) {
        return randomized(config, new Random());
    }

    /**
     * Inject a source for reproducible tests. Invoke from the serialized Raft
     * event loop; an injected source need not be thread-safe.
     * Each invocation samples uniformly from the configured inclusive range.
     */
    static ElectionTimeoutPolicy randomized(ElectionConfig config, RandomGenerator random) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(random, "random");
        // min > 0, so the inclusive width fits in a long even when max == MAX_VALUE.
        long width = config.electionTimeoutMaxMillis() - config.electionTimeoutMinMillis() + 1;
        return () -> config.electionTimeoutMinMillis() + random.nextLong(width);
    }
}
