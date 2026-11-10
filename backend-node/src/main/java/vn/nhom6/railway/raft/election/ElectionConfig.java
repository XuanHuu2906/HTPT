package vn.nhom6.railway.raft.election;

/**
 * Local timer intervals in milliseconds (SPEC-04 section 3).
 * Demo defaults may be tuned for the deployment; they are not Raft constants.
 * The election range includes both endpoints and must permit randomization.
 */
public record ElectionConfig(
        long heartbeatIntervalMillis,
        long electionTimeoutMinMillis,
        long electionTimeoutMaxMillis) {

    public ElectionConfig {
        if (heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMillis must be positive");
        }
        if (electionTimeoutMinMillis <= heartbeatIntervalMillis) {
            throw new IllegalArgumentException("electionTimeoutMinMillis must exceed heartbeatIntervalMillis");
        }
        if (electionTimeoutMaxMillis <= electionTimeoutMinMillis) {
            throw new IllegalArgumentException("electionTimeoutMaxMillis must exceed electionTimeoutMinMillis");
        }
    }

    public static ElectionConfig defaults() {
        return new ElectionConfig(150, 500, 1_000);
    }
}
