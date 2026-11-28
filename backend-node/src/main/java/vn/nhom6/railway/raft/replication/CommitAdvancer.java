package vn.nhom6.railway.raft.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import vn.nhom6.railway.raft.core.RaftState;
import vn.nhom6.railway.raft.core.Role;

/**
 * Leader-side majority commit rule of SPEC-04 section 5.
 *
 * <p>For a cluster of {@code 2f+1} Nodes, the Leader may raise {@code commitIndex} to {@code N}
 * only when at least a quorum of Nodes (the Leader itself plus peers whose verified
 * {@code matchIndex} is at least {@code N}) holds the log through {@code N}, and the entry at
 * {@code N} belongs to the Leader's <strong>current term</strong>. An old-term entry can only be
 * committed indirectly, by committing a later current-term entry.
 *
 * <p>Peer progress is read from {@link ReplicationTracker}, which only advances {@code matchIndex}
 * after a verified ACK. A failed, timed-out or malformed reply therefore never contributes to the
 * quorum. This class does not apply the state machine, does not call JDBC and does not notify a
 * commit listener. It is not thread-safe: run it on the serialized Raft event loop.
 *
 * <p><strong>Idempotent and monotonic.</strong> Repeated calls after progress stops return empty,
 * {@code commitIndex} never decreases and can never exceed {@code lastLogIndex}.
 */
public final class CommitAdvancer {

    private static final Logger LOGGER = Logger.getLogger(CommitAdvancer.class.getName());

    private final RaftState state;
    private final ReplicationTracker tracker;

    public CommitAdvancer(RaftState state, ReplicationTracker tracker) {
        this.state = Objects.requireNonNull(state, "state");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
    }

    /**
     * Computes and applies one commit advance according to the SPEC-04 section 5 rule.
     *
     * @return the new commit index when it advanced, or empty when the Leader must not commit
     */
    public OptionalLong advance() {
        if (state.role() != Role.LEADER) {
            return OptionalLong.empty();
        }

        List<Long> matched = new ArrayList<>();
        matched.add(state.lastLogIndex());
        for (ReplicationTracker.Progress progress : tracker.snapshot().values()) {
            matched.add(progress.matchIndex());
        }

        int clusterSize = matched.size();
        int quorum = clusterSize / 2 + 1;
        matched.sort((left, right) -> Long.compare(right, left));
        long candidate = matched.get(quorum - 1);

        if (candidate <= state.commitIndex()) {
            return OptionalLong.empty();
        }
        // Only an entry of the current term may be committed directly. Log terms never decrease,
        // so every lower index also belongs to an older term and there is nothing to search for.
        if (state.termAt(candidate) != state.currentTerm()) {
            return OptionalLong.empty();
        }
        if (!state.advanceCommitIndex(candidate)) {
            return OptionalLong.empty();
        }
        LOGGER.log(Level.INFO,
                "RAFT-COMMIT|node={0} term={1} event=ADVANCE commitIndex={2}",
                new Object[]{state.leaderId() == null ? "LEADER" : state.leaderId(),
                        state.currentTerm(), candidate});
        return OptionalLong.of(candidate);
    }
}
