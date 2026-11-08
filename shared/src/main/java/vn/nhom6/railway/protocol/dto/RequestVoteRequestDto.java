package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Request payload for REQUEST_VOTE Raft RPC according to SPEC-03 Section 6.
 */
public class RequestVoteRequestDto {

    @JsonProperty("term")
    private long term;

    @JsonProperty("candidateId")
    private String candidateId;

    @JsonProperty("lastLogIndex")
    private long lastLogIndex;

    @JsonProperty("lastLogTerm")
    private long lastLogTerm;

    public RequestVoteRequestDto() {
    }

    @JsonCreator
    public RequestVoteRequestDto(
            @JsonProperty("term") long term,
            @JsonProperty("candidateId") String candidateId,
            @JsonProperty("lastLogIndex") long lastLogIndex,
            @JsonProperty("lastLogTerm") long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * Validates fields according to SPEC-03 Section 3.0 and Section 6.
     */
    public void validate() {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0, got: " + term);
        }
        if (candidateId == null || candidateId.isBlank()) {
            throw new IllegalArgumentException("candidateId must not be null or blank");
        }
        if (lastLogIndex < 0) {
            throw new IllegalArgumentException("lastLogIndex must be >= 0, got: " + lastLogIndex);
        }
        if (lastLogTerm < 0) {
            throw new IllegalArgumentException("lastLogTerm must be >= 0, got: " + lastLogTerm);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestVoteRequestDto that = (RequestVoteRequestDto) o;
        return term == that.term &&
                lastLogIndex == that.lastLogIndex &&
                lastLogTerm == that.lastLogTerm &&
                Objects.equals(candidateId, that.candidateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public String toString() {
        return "RequestVoteRequestDto{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
