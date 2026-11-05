package vn.nhom6.railway.raft.election;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import vn.nhom6.railway.protocol.dto.RequestVoteRequestDto;
import vn.nhom6.railway.raft.core.RaftNode;
import vn.nhom6.railway.raft.core.RaftNode.RequestVoteResponse;
import vn.nhom6.railway.raft.core.RaftStorage.RaftStorageException;
import vn.nhom6.railway.raft.core.RaftState;

/**
 * Election boundary for one Raft Node (SPEC-04 section 3).
 *
 * <p>This is glue only. It owns no term, vote or log: the single copy lives in the
 * {@link RaftState} owned by TV4's {@link RaftNode}, the grant decision is
 * {@link VoteRules}, and the wire payload is TV3's {@link RequestVoteRequestDto}.
 * Implementations must never keep a second {@code currentTerm}/{@code votedFor}.
 *
 * <p>A durable mutation is always delegated to {@link RaftNode} so that election code
 * never persists or rewrites core state itself: {@link #startElection()} uses
 * {@link RaftNode#onElectionTimeout()} and {@link #handleRequestVote(RequestVoteRequestDto)}
 * uses {@link RaftNode#handleRequestVote}. That delegation is also what makes the
 * persist-before-reply rule (SPEC-04 section 3.2 and 3.3) testable in one place.
 *
 * <p>The only completion step TV4's {@link RaftNode} does not expose yet is the
 * Candidate-to-Leader transition, so {@link #promoteToLeader(Set)} performs it through
 * {@link RaftState#becomeLeader(Set)}. That is still the single core transition, not a
 * second election mechanism; when TV4 adds it to {@link RaftNode}, this method delegates
 * to it instead.
 *
 * <p>Every method is invoked on the one serialized Raft event loop, so implementations
 * need no locks. A {@link RaftStorageException} means the Node must fail closed and stop
 * confirming writes (SPEC-04 section 9).
 */
public interface ElectionService {

    /** Id of the local Node in the fixed voter set. */
    String localId();

    /** The shared Raft event-loop boundary supplied by TV4. */
    RaftNode node();

    /**
     * The same {@link RaftState} instance held by {@link #node()}.
     *
     * <p>Exposed so election, replication and recovery can prove they share one term,
     * vote and log instead of keeping parallel copies. Mutate it only through the
     * {@link RaftNode} boundary and only on the Raft event loop.
     */
    RaftState state();

    /** Ids of the voters of this cluster, in stable order; the local Node is included. */
    List<String> voters();

    /** Whether {@code nodeId} belongs to the fixed voter set; {@code null} is not a voter. */
    boolean isVoter(String nodeId);

    /** Minimum distinct votes needed to win: a strict majority of {@link #voters()}. */
    int requiredVotes();

    /**
     * Starts a local election after the election timer expires.
     *
     * <p>Delegates to {@link RaftNode#onElectionTimeout()}, which increments the term,
     * records the self-vote and durably stores both before this method returns
     * (SPEC-04 section 3.2). The campaign vote count is reset.
     *
     * @return the term this Node now campaigns for, equal to {@code state().currentTerm()}
     * @throws IllegalStateException    when the local Node is not a configured voter or is
     *                                  still a Leader
     * @throws RaftStorageException     when the new term and self-vote cannot be stored
     */
    long startElection() throws RaftStorageException;

    /**
     * Builds the {@code REQUEST_VOTE} payload for the term this Node campaigns for.
     *
     * <p>The advertised log position is read from {@link RaftState#lastLogIndex()} and
     * {@link RaftState#lastLogTerm()}, so the payload always matches the local log.
     * Empty when the Node is not a candidate in {@code candidateTerm}, which happens
     * after a stale timeout fires or after the Node steps down.
     *
     * @param candidateTerm term returned by {@link #startElection()}
     * @return the shared wire DTO to send, or empty when no campaign is pending
     */
    Optional<RequestVoteRequestDto> voteRequestFor(long candidateTerm);

    /**
     * Applies a {@code REQUEST_VOTE} received from a peer.
     *
     * <p>Validates the payload with {@link RequestVoteRequestDto#validate()}, refuses
     * non-voters before any state change, then delegates to
     * {@link RaftNode#handleRequestVote(RaftNode.RequestVoteRequest)}. That call applies
     * {@link VoteRules} and persists the resulting term and vote before returning, so a
     * granted vote is never reported without durable storage. A higher term that rejects
     * the log still persists the new term with a cleared vote.
     *
     * <p>While TV3 has not published a response DTO, the result is TV4's
     * {@link RequestVoteResponse}.
     *
     * @param request payload received over the wire
     * @return grant flag plus the term to echo in the response
     * @throws IllegalArgumentException when the payload violates SPEC-03 field rules
     * @throws RaftStorageException     when the resulting term and vote cannot be stored
     */
    RequestVoteResponse handleRequestVote(RequestVoteRequestDto request) throws RaftStorageException;

    /**
     * Counts one {@code REQUEST_VOTE_RESPONSE} for the current campaign.
     *
     * <p>Only a response carrying the current term counts, each voter counts at most once,
     * and the self-vote is already included, so a 5-voter cluster wins at
     * {@link #requiredVotes()} = 3 distinct grants. A response with a higher term ends the
     * campaign: this method reports {@link SteppedDown} and ignores every later response
     * of that term, because persisting the higher term and stepping down belong to the
     * Raft event loop. This method never mutates term or vote.
     *
     * @param voterId id of the peer that answered
     * @param term    term carried by the response
     * @param granted whether the peer granted its vote
     * @return progress, victory, step-down or an ignored response
     * @throws IllegalArgumentException when {@code voterId} is null or blank
     */
    ElectionOutcome recordVoteResponse(String voterId, long term, boolean granted);

    /**
     * Completes a won campaign by transitioning to {@link Role#LEADER}.
     *
     * <p>Uses {@link RaftState#becomeLeader(Set)}, which initializes
     * {@code nextIndex = lastLogIndex + 1} and {@code matchIndex = 0} for each peer and
     * clears the leader hint. Call only after {@link #recordVoteResponse(String, long,
     * boolean)} returned {@link Won} for the current term, then send heartbeats.
     *
     * @param peers the non-local voters that will receive {@code APPEND_ENTRIES}
     * @throws IllegalStateException    when this Node is not a {@link Role#CANDIDATE}
     * @throws IllegalArgumentException when a peer is blank or not a voter
     */
    void promoteToLeader(Set<String> peers);

    /** Election progress of a campaigning Node. */
    sealed interface ElectionOutcome permits IgnoredVote, VoteProgress, Won, SteppedDown {
    }

    /**
     * The response carried no information for the current campaign: stale term, duplicate
     * voter, self response, non-voter, refused vote, or an already-abandoned campaign.
     *
     * @param reason classification for logs; not a protocol code
     */
    record IgnoredVote(String reason) implements ElectionOutcome {
    }

    /**
     * The vote was counted but the threshold is not reached yet.
     *
     * @param votesGranted  distinct votes for the current term, including the self-vote
     * @param votesRequired votes needed to win
     */
    record VoteProgress(int votesGranted, int votesRequired) implements ElectionOutcome {
    }

    /**
     * The threshold was reached in the current term. This is a vote count only; the Raft
     * event loop still performs the Candidate-to-Leader transition.
     *
     * @param votesGranted distinct votes for the current term, including the self-vote
     */
    record Won(int votesGranted) implements ElectionOutcome {
    }

    /**
     * A peer reported a higher term, so this campaign is over.
     *
     * @param observedTerm the higher term reported by the peer
     */
    record SteppedDown(long observedTerm) implements ElectionOutcome {
    }
}
