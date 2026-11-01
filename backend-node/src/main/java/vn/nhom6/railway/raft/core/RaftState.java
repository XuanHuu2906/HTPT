package vn.nhom6.railway.raft.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * In-memory Raft state of one Node: term, vote, log, commit/apply progress and leader-volatile
 * per-peer indices (SPEC-04 sections 2, 3, 4 and 6).
 *
 * <p>This class is a pure model. It performs no I/O, no persistence, no RPC, no timers and no
 * majority/current-term commit computation. Persisting {@code currentTerm}, {@code votedFor}
 * and log entries <em>before</em> answering a Raft RPC is the responsibility of the durable log
 * and transport layers (SPEC-04 section 9); a mutation here is only valid once that persistence
 * succeeds. Electing a leader, counting votes, advancing {@code commitIndex} from
 * {@code matchIndex} and applying the state machine belong to the election (TV5) and
 * replication (TV4) handlers.
 *
 * <p><strong>Not thread-safe.</strong> Every method must be called from the single serialized
 * Raft event loop of the Node; this class deliberately uses no locks. Returned log and peer maps
 * are unmodifiable snapshots, so callers cannot bypass validation.
 *
 * <p>Invariants kept by this type (SPEC-04 section 10):
 * {@code 0 <= lastApplied <= commitIndex <= lastLogIndex}, the term never decreases, at most one
 * candidate is voted for per term, the log is contiguous from index 1 with non-decreasing terms,
 * and committed entries are never truncated or overwritten.
 */
public final class RaftState {

    /** Index reported for an empty log; entry 0 is never stored. */
    public static final long EMPTY_LOG_INDEX = 0L;

    /** Term reported for an empty log; entry term 0 never exists. */
    public static final long EMPTY_LOG_TERM = 0L;

    private long currentTerm;
    private String votedFor;
    private final List<RaftLogEntry> log = new ArrayList<>();
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex = EMPTY_LOG_INDEX;
    private long lastApplied = EMPTY_LOG_INDEX;
    private final Map<String, Long> nextIndexByPeer = new LinkedHashMap<>();
    private final Map<String, Long> matchIndexByPeer = new LinkedHashMap<>();

    /** Creates the initial state of a Node that has never voted: term 0, empty log, follower. */
    public RaftState() {
        // Nothing to persist yet; role and indices already hold their documented defaults.
    }

    private RaftState(long currentTerm, String votedFor, List<RaftLogEntry> entries, long lastApplied) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log.addAll(entries);
        this.commitIndex = lastApplied;
        this.lastApplied = lastApplied;
    }

    // ---------------------------------------------------------------------------------------
    // Queries
    // ---------------------------------------------------------------------------------------

    /** Current term, never decreasing during the lifetime of this object. */
    public long currentTerm() {
        return currentTerm;
    }

    /** Candidate voted for in {@link #currentTerm()}, or {@code null} when no vote was cast. */
    public String votedFor() {
        return votedFor;
    }

    public Role role() {
        return role;
    }

    /** Leader hint for clients and redirects; may be {@code null} or stale (SPEC-04 section 2). */
    public String leaderId() {
        return leaderId;
    }

    /** Highest index known to be committed by a Raft quorum. */
    public long commitIndex() {
        return commitIndex;
    }

    /** Highest index already applied by the state machine. */
    public long lastApplied() {
        return lastApplied;
    }

    /** Index of the last log entry, or {@link #EMPTY_LOG_INDEX} for an empty log. */
    public long lastLogIndex() {
        return log.isEmpty() ? EMPTY_LOG_INDEX : log.get(log.size() - 1).index();
    }

    /** Term of the last log entry, or {@link #EMPTY_LOG_TERM} for an empty log. */
    public long lastLogTerm() {
        return log.isEmpty() ? EMPTY_LOG_TERM : log.get(log.size() - 1).term();
    }

    /** Unmodifiable snapshot of the log, ordered by index. */
    public List<RaftLogEntry> entries() {
        return List.copyOf(log);
    }

    /**
     * Returns the entry at {@code index}.
     *
     * @throws IndexOutOfBoundsException when {@code index} is 0 (no stored entry), negative or
     *                                   beyond {@link #lastLogIndex()}
     */
    public RaftLogEntry entryAt(long index) {
        if (index < RaftLogEntry.FIRST_INDEX) {
            throw new IndexOutOfBoundsException(
                    "no log entry at index " + index + "; an empty log is (lastLogIndex=0, lastLogTerm=0)");
        }
        if (index > lastLogIndex()) {
            throw new IndexOutOfBoundsException("index " + index + " is beyond lastLogIndex " + lastLogIndex());
        }
        return log.get((int) Math.toIntExact(index - 1));
    }

    /**
     * Returns the term of the entry at {@code index}, with {@code termAt(0) == 0} for the empty
     * prefix so that {@code (prevLogIndex = 0, prevLogTerm = 0)} checks work without a sentinel
     * entry (SPEC-03 section 6).
     */
    public long termAt(long index) {
        if (index < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException("index must be >= " + EMPTY_LOG_INDEX + " but was " + index);
        }
        if (index == EMPTY_LOG_INDEX) {
            return EMPTY_LOG_TERM;
        }
        return entryAt(index).term();
    }

    /** Unmodifiable snapshot of {@code nextIndex} per peer; empty unless {@link Role#LEADER}. */
    public Map<String, Long> nextIndex() {
        return Map.copyOf(nextIndexByPeer);
    }

    /** Unmodifiable snapshot of {@code matchIndex} per peer; empty unless {@link Role#LEADER}. */
    public Map<String, Long> matchIndex() {
        return Map.copyOf(matchIndexByPeer);
    }

    /** {@code nextIndex} of a known peer. */
    public long nextIndexOf(String peerId) {
        return nextIndexByPeer.get(requireKnownPeer(peerId));
    }

    /** {@code matchIndex} of a known peer. */
    public long matchIndexOf(String peerId) {
        return matchIndexByPeer.get(requireKnownPeer(peerId));
    }

    // ---------------------------------------------------------------------------------------
    // Term and role
    // ---------------------------------------------------------------------------------------

    /**
     * Steps down to {@link Role#FOLLOWER} in {@code newTerm} without a leader hint.
     *
     * @see #becomeFollower(long, String)
     */
    public void becomeFollower(long newTerm) {
        becomeFollower(newTerm, null);
    }

    /**
     * Applies a higher (or equal) term and becomes {@link Role#FOLLOWER} (SPEC-04 section 3.5).
     *
     * <p>A strictly higher term clears {@link #votedFor()} because votes are per term. An equal
     * term keeps the existing vote, so a candidate that sees a valid Leader for its own term
     * steps down without silently voting twice. Terms never decrease.
     *
     * @throws IllegalArgumentException when {@code newTerm} is negative or lower than the current
     *                                  term, or when the hint is blank
     */
    public void becomeFollower(long newTerm, String newLeaderId) {
        if (newTerm < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException("term must be >= 0 but was " + newTerm);
        }
        if (newTerm < currentTerm) {
            throw new IllegalArgumentException(
                    "term must not decrease: currentTerm=" + currentTerm + ", newTerm=" + newTerm);
        }
        String hint = newLeaderId == null ? null : requireNonBlank(newLeaderId, "leaderId");
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        leaderId = hint;
        clearLeaderVolatileState();
    }

    /**
     * Starts an election locally: increments the term, votes for itself and becomes
     * {@link Role#CANDIDATE} (SPEC-04 section 3.2). Persisting the new term and vote before
     * sending {@code REQUEST_VOTE} happens outside this model.
     *
     * @throws ArithmeticException      when the term would overflow {@link Long#MAX_VALUE}
     * @throws IllegalStateException    when the local Node is still {@link Role#LEADER}
     */
    public void startElection(String selfId) {
        String candidateId = requireNonBlank(selfId, "selfId");
        if (role == Role.LEADER) {
            throw new IllegalStateException("a leader must step down before starting a new election");
        }
        currentTerm = Math.addExact(currentTerm, 1L);
        votedFor = candidateId;
        role = Role.CANDIDATE;
        leaderId = null;
        clearLeaderVolatileState();
    }

    /**
     * Applies the {@code REQUEST_VOTE} rules of SPEC-04 section 3.3 and records the grant.
     *
     * <p>The vote is granted only when the candidate term is not stale, this Node has not voted
     * for a different candidate in that term, and the candidate log is at least as up to date
     * (last log term first, then last log index). A request with a strictly higher term first
     * makes this Node a follower and clears the old vote (SPEC-04 section 3.5); an equal term
     * keeps the current role, so a candidate that already voted for itself simply refuses. This
     * method only mutates the model; it does not send a response and does not persist the vote.
     *
     * @param candidateTerm        term carried by the vote request, {@code >= 1}
     * @param candidateId          voter-visible id of the candidate
     * @param candidateLastLogIndex last log index advertised by the candidate, {@code >= 0}
     * @param candidateLastLogTerm  last log term advertised by the candidate, {@code >= 0}
     *                              and never greater than {@code candidateTerm}
     * @return {@code true} when the vote was granted and recorded
     */
    public boolean grantVote(
            long candidateTerm, String candidateId, long candidateLastLogIndex, long candidateLastLogTerm) {
        String candidate = requireNonBlank(candidateId, "candidateId");
        validateCandidateLog(candidateTerm, candidateLastLogIndex, candidateLastLogTerm);
        if (candidateTerm < currentTerm) {
            return false;
        }
        if (candidateTerm > currentTerm) {
            currentTerm = candidateTerm;
            votedFor = null;
            role = Role.FOLLOWER;
            leaderId = null;
            clearLeaderVolatileState();
        }
        if (votedFor != null && !votedFor.equals(candidate)) {
            return false;
        }
        if (!isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm)) {
            return false;
        }
        votedFor = candidate;
        return true;
    }

    /**
     * Transitions {@link Role#CANDIDATE} to {@link Role#LEADER} and initializes the leader-volatile
     * indices: {@code nextIndex = lastLogIndex + 1} and {@code matchIndex = 0} for every peer
     * (SPEC-04 section 3.4). No Node is privileged; this only reflects a completed election.
     *
     * @throws IllegalStateException    when the local role is not {@link Role#CANDIDATE}
     * @throws IllegalArgumentException when the peer set is missing or contains a blank id
     */
    public void becomeLeader(Set<String> peers) {
        Objects.requireNonNull(peers, "peers");
        if (role != Role.CANDIDATE) {
            throw new IllegalStateException("only a candidate can become leader; role=" + role);
        }
        Set<String> validatedPeers = new TreeSet<>();
        for (String peer : peers) {
            validatedPeers.add(requireNonBlank(peer, "peer"));
        }
        clearLeaderVolatileState();
        long initialNextIndex = Math.addExact(lastLogIndex(), 1L);
        for (String peer : validatedPeers) {
            nextIndexByPeer.put(peer, initialNextIndex);
            matchIndexByPeer.put(peer, EMPTY_LOG_INDEX);
        }
        role = Role.LEADER;
        leaderId = null;
    }

    // ---------------------------------------------------------------------------------------
    // Log
    // ---------------------------------------------------------------------------------------

    /**
     * Appends the next entry of the local log.
     *
     * <p>The entry must extend the log by exactly one index, carry a term that does not exceed
     * {@link #currentTerm()} and does not decrease relative to the previous entry (SPEC-03
     * section 6). Persisting the entry before acknowledging replication is the caller's duty.
     *
     * @throws IllegalArgumentException when the index is not {@code lastLogIndex + 1}, or the term
     *                                  regresses, or it is above the current term
     */
    public void append(RaftLogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        long expectedIndex = Math.addExact(lastLogIndex(), 1L);
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException(
                    "entry index must be lastLogIndex + 1 (" + expectedIndex + ") but was " + entry.index());
        }
        if (entry.term() > currentTerm) {
            throw new IllegalArgumentException(
                    "entry term " + entry.term() + " must not exceed currentTerm " + currentTerm);
        }
        if (!log.isEmpty() && entry.term() < lastLogTerm()) {
            throw new IllegalArgumentException(
                    "entry term " + entry.term() + " must not be lower than previous term " + lastLogTerm());
        }
        log.add(entry);
    }

    /**
     * Removes every entry from {@code index} to the end of the log (uncommitted suffix only,
     * SPEC-04 section 4).
     *
     * @throws IllegalStateException    when {@code index <= commitIndex}: committed entries must
     *                                  never be truncated, so this fails closed instead of
     *                                  silently losing acknowledged data
     * @throws IllegalArgumentException when {@code index} is not an existing log position
     */
    public void truncateSuffixFrom(long index) {
        if (index < RaftLogEntry.FIRST_INDEX) {
            throw new IllegalArgumentException("index must be >= " + RaftLogEntry.FIRST_INDEX + " but was " + index);
        }
        if (index > lastLogIndex()) {
            throw new IllegalArgumentException(
                    "cannot truncate from index " + index + " beyond lastLogIndex " + lastLogIndex());
        }
        if (index <= commitIndex) {
            throw new IllegalStateException(
                    "refusing to truncate committed entries: index=" + index + ", commitIndex=" + commitIndex);
        }
        log.subList((int) Math.toIntExact(index - 1), log.size()).clear();
    }

    /**
     * Fail-closed guard for the SPEC-04 section 4 invariant: one {@code (index, term)} pair can
     * only ever hold one command.
     *
     * <p>Replication calls this with an incoming entry before truncating or appending. If the
     * local log already holds that index and term with different content, the cluster would be
     * diverging on an agreed entry, so this throws and leaves the local log untouched. A local
     * entry with the same index but a different term is a normal conflict that the caller may
     * resolve by truncating the uncommitted suffix; if that index is already committed, it is
     * itself a fatal violation.
     *
     * @throws IllegalStateException when the same {@code (index, term)} has different content, or
     *                              when an incoming entry would rewrite a committed index
     */
    public void verifyIncomingEntry(RaftLogEntry incoming) {
        Objects.requireNonNull(incoming, "incoming");
        if (incoming.index() > lastLogIndex()) {
            return;
        }
        RaftLogEntry local = log.get((int) Math.toIntExact(incoming.index() - 1));
        if (local.term() != incoming.term()) {
            if (incoming.index() <= commitIndex) {
                throw new IllegalStateException("leader entry at committed index " + incoming.index()
                        + " has term " + incoming.term() + " but the committed term is " + local.term());
            }
            return;
        }
        if (!local.equals(incoming)) {
            throw new IllegalStateException("same (index, term) with different command: index=" + incoming.index()
                    + ", term=" + incoming.term());
        }
    }

    // ---------------------------------------------------------------------------------------
    // Commit and apply progress
    // ---------------------------------------------------------------------------------------

    /**
     * Raises {@link #commitIndex()} to {@code newIndex}.
     *
     * <p>Only a monotonic advance is applied: a stale or duplicate value is ignored rather than
     * lowering the index, and an index beyond {@link #lastLogIndex()} is rejected. Computing the
     * index from a majority of {@code matchIndex} including the current-term rule is replication
     * logic and stays outside this model (SPEC-04 section 5).
     *
     * @return {@code true} when the commit index advanced
     * @throws IllegalArgumentException when {@code newIndex} is negative or beyond the local log
     */
    public boolean advanceCommitIndex(long newIndex) {
        if (newIndex < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException("commitIndex must be >= 0 but was " + newIndex);
        }
        if (newIndex > lastLogIndex()) {
            throw new IllegalArgumentException(
                    "commitIndex " + newIndex + " must not exceed lastLogIndex " + lastLogIndex());
        }
        if (newIndex <= commitIndex) {
            return false;
        }
        commitIndex = newIndex;
        return true;
    }

    /**
     * Records that the state machine applied exactly the next entry.
     *
     * <p>Applies must be contiguous and only within the committed prefix (SPEC-04 section 6):
     * {@code index == lastApplied + 1} and {@code index <= commitIndex}. A skipped index or an
     * index beyond the commit point fails closed instead of guessing.
     *
     * @throws IllegalArgumentException when the index is not the next committed entry
     */
    public void markApplied(long index) {
        if (index < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException("lastApplied must be >= 0 but was " + index);
        }
        long expected = Math.addExact(lastApplied, 1L);
        if (index != expected) {
            throw new IllegalArgumentException(
                    "index must be lastApplied + 1 (" + expected + ") but was " + index);
        }
        if (index > commitIndex) {
            throw new IllegalArgumentException(
                    "cannot apply index " + index + " beyond commitIndex " + commitIndex);
        }
        lastApplied = index;
    }

    // ---------------------------------------------------------------------------------------
    // Leader-volatile per-peer indices
    // ---------------------------------------------------------------------------------------

    /**
     * Lowers or raises {@code nextIndex} for a known peer while this Node is Leader.
     *
     * <p>{@code nextIndex} must stay within {@code [matchIndex + 1, lastLogIndex + 1]} so that a
     * stale or bogus value can never re-send already matched entries or skip the log tail.
     *
     * @throws IllegalStateException    when this Node is not {@link Role#LEADER}
     * @throws IllegalArgumentException when the peer is unknown or the value is out of range
     */
    public void updateNextIndex(String peerId, long newNextIndex) {
        requireLeader("nextIndex");
        String peer = requireKnownPeer(peerId);
        long minimum = Math.addExact(matchIndexByPeer.get(peer), 1L);
        if (newNextIndex < minimum) {
            throw new IllegalArgumentException(
                    "nextIndex must be >= matchIndex + 1 (" + minimum + ") but was " + newNextIndex);
        }
        long maximum = Math.addExact(lastLogIndex(), 1L);
        if (newNextIndex > maximum) {
            throw new IllegalArgumentException(
                    "nextIndex must be <= lastLogIndex + 1 (" + maximum + ") but was " + newNextIndex);
        }
        nextIndexByPeer.put(peer, newNextIndex);
    }

    /**
     * Raises {@code matchIndex} for a known peer while this Node is Leader (SPEC-04 section 4).
     *
     * <p>{@code matchIndex} never decreases and never exceeds {@link #lastLogIndex()}. If it
     * overtakes {@code nextIndex - 1}, {@code nextIndex} is raised with it so the two stay
     * consistent.
     *
     * @throws IllegalStateException    when this Node is not {@link Role#LEADER}
     * @throws IllegalArgumentException when the peer is unknown, the value decreases, or it
     *                                  exceeds the local log
     */
    public void advanceMatchIndex(String peerId, long newMatchIndex) {
        requireLeader("matchIndex");
        String peer = requireKnownPeer(peerId);
        if (newMatchIndex < matchIndexByPeer.get(peer)) {
            throw new IllegalArgumentException("matchIndex must not decrease: current="
                    + matchIndexByPeer.get(peer) + ", new=" + newMatchIndex);
        }
        if (newMatchIndex > lastLogIndex()) {
            throw new IllegalArgumentException(
                    "matchIndex " + newMatchIndex + " must not exceed lastLogIndex " + lastLogIndex());
        }
        matchIndexByPeer.put(peer, newMatchIndex);
        long minimumNext = Math.addExact(newMatchIndex, 1L);
        if (nextIndexByPeer.get(peer) < minimumNext) {
            nextIndexByPeer.put(peer, minimumNext);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Restart
    // ---------------------------------------------------------------------------------------

    /**
     * Rebuilds the state after a restart from durably stored data (SPEC-04 section 6).
     *
     * <p>Validates that the stored log is contiguous from index 1 with non-decreasing terms that
     * never exceed {@code currentTerm}, and that the apply checkpoint does not run past the stored
     * log. Because a durable apply checkpoint proves that prefix was already committed,
     * {@code commitIndex} is initialized to {@code lastApplied} — deliberately not to
     * {@code lastLogIndex}. The result is always a {@link Role#FOLLOWER} with no leader hint, and
     * the applied prefix is not replayed.
     *
     * @throws IllegalArgumentException when the persisted data is inconsistent (fail closed)
     */
    public static RaftState restore(
            long currentTerm, String votedFor, List<RaftLogEntry> entries, long lastApplied) {
        if (currentTerm < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException("currentTerm must be >= 0 but was " + currentTerm);
        }
        Objects.requireNonNull(entries, "entries");
        if (votedFor != null) {
            requireNonBlank(votedFor, "votedFor");
        }
        List<RaftLogEntry> restoredLog = new ArrayList<>(entries.size());
        long expectedIndex = RaftLogEntry.FIRST_INDEX;
        long previousTerm = EMPTY_LOG_TERM;
        for (RaftLogEntry entry : entries) {
            Objects.requireNonNull(entry, "entries must not contain null");
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException(
                        "persisted log is not contiguous: expected index " + expectedIndex
                                + " but found " + entry.index());
            }
            if (entry.term() < previousTerm) {
                throw new IllegalArgumentException("persisted log term decreases at index " + entry.index()
                        + ": " + previousTerm + " -> " + entry.term());
            }
            if (entry.term() > currentTerm) {
                throw new IllegalArgumentException("persisted entry term " + entry.term() + " at index "
                        + entry.index() + " exceeds currentTerm " + currentTerm);
            }
            previousTerm = entry.term();
            expectedIndex = Math.addExact(expectedIndex, 1L);
            restoredLog.add(entry);
        }
        long lastLogIndex = restoredLog.size();
        if (lastApplied < EMPTY_LOG_INDEX || lastApplied > lastLogIndex) {
            throw new IllegalArgumentException("lastApplied " + lastApplied
                    + " must be within [0, persisted lastLogIndex=" + lastLogIndex + "]");
        }
        return new RaftState(currentTerm, votedFor, restoredLog, lastApplied);
    }

    // ---------------------------------------------------------------------------------------
    // Validation helpers
    // ---------------------------------------------------------------------------------------

    private void clearLeaderVolatileState() {
        nextIndexByPeer.clear();
        matchIndexByPeer.clear();
    }

    private void requireLeader(String field) {
        if (role != Role.LEADER) {
            throw new IllegalStateException(field + " is leader-volatile state; role=" + role);
        }
    }

    private String requireKnownPeer(String peerId) {
        String peer = requireNonBlank(peerId, "peerId");
        if (!nextIndexByPeer.containsKey(peer)) {
            throw new IllegalArgumentException("unknown peer '" + peer + "'");
        }
        return peer;
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static void validateCandidateLog(long candidateTerm, long candidateLastLogIndex, long candidateLastLogTerm) {
        if (candidateTerm < RaftLogEntry.FIRST_TERM) {
            // A candidate increments its term before sending REQUEST_VOTE (SPEC-04 section 3.2),
            // so a conformant peer never sends term 0. Term 0 only describes a fresh Node that
            // has not taken part in any election yet and must never be granted a vote.
            throw new IllegalArgumentException(
                    "candidateTerm must be >= " + RaftLogEntry.FIRST_TERM + " but was " + candidateTerm);
        }
        if (candidateLastLogTerm < EMPTY_LOG_TERM) {
            throw new IllegalArgumentException(
                    "candidateLastLogTerm must be >= 0 but was " + candidateLastLogTerm);
        }
        if (candidateLastLogIndex < EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "candidateLastLogIndex must be >= 0 but was " + candidateLastLogIndex);
        }
        if (candidateLastLogTerm == EMPTY_LOG_TERM && candidateLastLogIndex != EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "an empty candidate log is (lastLogIndex=0, lastLogTerm=0) but was ("
                            + candidateLastLogIndex + ", " + candidateLastLogTerm + ")");
        }
        if (candidateLastLogTerm > EMPTY_LOG_TERM && candidateLastLogIndex == EMPTY_LOG_INDEX) {
            throw new IllegalArgumentException(
                    "candidateLastLogIndex 0 cannot have term " + candidateLastLogTerm);
        }
        if (candidateLastLogTerm > candidateTerm) {
            throw new IllegalArgumentException(
                    "candidateLastLogTerm " + candidateLastLogTerm + " must not exceed candidateTerm " + candidateTerm);
        }
    }

    /** True when the candidate log is at least as up to date (term first, then index). */
    private boolean isLogUpToDate(long candidateLastLogIndex, long candidateLastLogTerm) {
        if (candidateLastLogTerm != lastLogTerm()) {
            return candidateLastLogTerm > lastLogTerm();
        }
        return candidateLastLogIndex >= lastLogIndex();
    }
}
