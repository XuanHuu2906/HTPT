package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response payload for REQUEST_VOTE Raft RPC according to SPEC-03 Section 6.
 *
 * <p>Wire JSON payload format:
 * <pre>{@code
 * { "term": 8, "voteGranted": true }
 * }</pre>
 */
public class RequestVoteResponseDto {

    @JsonProperty("term")
    private long term;

    @JsonProperty("voteGranted")
    private boolean voteGranted;

    public RequestVoteResponseDto() {
    }

    @JsonCreator
    public RequestVoteResponseDto(
            @JsonProperty("term") long term,
            @JsonProperty("voteGranted") boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    /**
     * Factory method for granted vote.
     */
    public static RequestVoteResponseDto granted(long term) {
        return new RequestVoteResponseDto(term, true);
    }

    /**
     * Factory method for rejected vote.
     */
    public static RequestVoteResponseDto rejected(long term) {
        return new RequestVoteResponseDto(term, false);
    }

    /**
     * Validates fields according to SPEC-03 Section 6.
     */
    public void validate() {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0, got: " + term);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestVoteResponseDto that = (RequestVoteResponseDto) o;
        return term == that.term && voteGranted == that.voteGranted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, voteGranted);
    }

    @Override
    public String toString() {
        return "RequestVoteResponseDto{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
