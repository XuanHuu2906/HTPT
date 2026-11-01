package vn.nhom6.railway.raft.election;

import java.util.Random;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElectionTimeoutPolicyTest {

    @Test
    void defaultsFollowSpecDemoIntervals() {
        ElectionConfig config = ElectionConfig.defaults();
        assertEquals(150, config.heartbeatIntervalMillis());
        assertEquals(500, config.electionTimeoutMinMillis());
        assertEquals(1_000, config.electionTimeoutMaxMillis());
    }

    @ParameterizedTest
    @CsvSource({
        "0, 500, 1000", "-1, 500, 1000",
        "150, -1, 1000", "150, 0, 1000", "150, 149, 1000", "150, 150, 1000",
        "150, 500, -1", "150, 500, 0", "150, 500, 499", "150, 500, 500"
    })
    void rejectsInvalidIntervals(long heartbeat, long minimum, long maximum) {
        assertThrows(IllegalArgumentException.class, () -> new ElectionConfig(heartbeat, minimum, maximum));
    }

    @Test
    void demoIntervalsAreTunableRatherThanHardLimits() {
        ElectionConfig config = new ElectionConfig(300, 1_500, 3_000);
        ElectionTimeoutPolicy policy = ElectionTimeoutPolicy.randomized(config, new Random(42));
        for (int i = 0; i < 1_000; i++) {
            long timeout = policy.nextTimeoutMillis();
            assertTrue(timeout >= 1_500 && timeout <= 3_000);
        }
    }

    @Test
    void samplesAgainOnEveryCallAndIncludesBothEndpoints() {
        EndpointRandom random = new EndpointRandom(501);
        ElectionTimeoutPolicy policy = ElectionTimeoutPolicy.randomized(ElectionConfig.defaults(), random);
        assertEquals(500, policy.nextTimeoutMillis());
        assertEquals(1_000, policy.nextTimeoutMillis());
        assertEquals(500, policy.nextTimeoutMillis());
        assertEquals(3, random.calls);
    }

    @Test
    void supportsSmallestRandomizedRange() {
        ElectionTimeoutPolicy policy = ElectionTimeoutPolicy.randomized(
                new ElectionConfig(1, 2, 3), new EndpointRandom(2));
        assertEquals(2, policy.nextTimeoutMillis());
        assertEquals(3, policy.nextTimeoutMillis());
    }

    @Test
    void inclusiveUpperBoundDoesNotOverflow() {
        ElectionTimeoutPolicy policy = ElectionTimeoutPolicy.randomized(
                new ElectionConfig(1, 2, Long.MAX_VALUE), new EndpointRandom(Long.MAX_VALUE - 1));
        assertEquals(2, policy.nextTimeoutMillis());
        assertEquals(Long.MAX_VALUE, policy.nextTimeoutMillis());
    }

    @Test
    void injectedSeedReproducesTimeoutsWithoutSleepingOrReadingAClock() {
        ElectionTimeoutPolicy first = ElectionTimeoutPolicy.randomized(ElectionConfig.defaults(), new Random(73));
        ElectionTimeoutPolicy second = ElectionTimeoutPolicy.randomized(ElectionConfig.defaults(), new Random(73));
        for (int i = 0; i < 1_000; i++) {
            long timeout = first.nextTimeoutMillis();
            assertTrue(timeout >= 500 && timeout <= 1_000);
            assertEquals(timeout, second.nextTimeoutMillis());
        }
    }

    @Test
    void separatePoliciesDoNotShareInjectedRandomState() {
        ElectionTimeoutPolicy first = ElectionTimeoutPolicy.randomized(
                ElectionConfig.defaults(), new EndpointRandom(501));
        ElectionTimeoutPolicy second = ElectionTimeoutPolicy.randomized(
                ElectionConfig.defaults(), new EndpointRandom(501));
        assertEquals(500, first.nextTimeoutMillis());
        assertEquals(1_000, first.nextTimeoutMillis());
        assertEquals(500, second.nextTimeoutMillis());
    }

    @Test
    void productionFactoryReturnsTimeoutsWithinConfiguredRange() {
        ElectionTimeoutPolicy policy = ElectionTimeoutPolicy.randomized(ElectionConfig.defaults());
        for (int i = 0; i < 1_000; i++) {
            long timeout = policy.nextTimeoutMillis();
            assertTrue(timeout >= 500 && timeout <= 1_000);
        }
    }

    @Test
    void rejectsMissingConfigurationOrRandomSource() {
        assertThrows(NullPointerException.class, () -> ElectionTimeoutPolicy.randomized(null));
        assertThrows(NullPointerException.class, () -> ElectionTimeoutPolicy.randomized(null, new Random(1)));
        assertThrows(NullPointerException.class,
                () -> ElectionTimeoutPolicy.randomized(ElectionConfig.defaults(), null));
    }

    /** Deterministic boundary source; no probabilistic coverage assertions. */
    private static final class EndpointRandom implements RandomGenerator {
        private final long expectedBound;
        private int calls;

        private EndpointRandom(long expectedBound) {
            this.expectedBound = expectedBound;
        }

        @Override
        public long nextLong(long bound) {
            assertEquals(expectedBound, bound);
            return calls++ % 2 == 0 ? 0 : bound - 1;
        }

        @Override
        public long nextLong() {
            throw new AssertionError("Expected a bounded random draw");
        }
    }
}
