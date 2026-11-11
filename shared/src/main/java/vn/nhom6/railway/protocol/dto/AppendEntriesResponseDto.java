package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response payload for APPEND_ENTRIES Raft RPC according to SPEC-03 Section 6.
 *
 * <p>Wire JSON payload format:
 * <pre>{@code
 * { "term": 8, "success": true, "matchIndex": 43 }
 * }</pre>
 *
 * <p>Rules per SPEC-03 Section 6:
 * <ul>
 *   <li>When {@code success = true}, {@code matchIndex = prevLogIndex + entries.size()} of this RPC.</li>
 *   <li>When {@code success = false}, {@code matchIndex = 0}.</li>
 *   <li>Response {@code term} is always the currentTerm of the receiver after handling.</li>
 * </ul>
 */
public class AppendEntriesResponseDto {

    @JsonProperty("term")
    private long term;

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("matchIndex")
    private long matchIndex;

    public AppendEntriesResponseDto() {
    }

    @JsonCreator
    public AppendEntriesResponseDto(
            @JsonProperty("term") long term,
            @JsonProperty("success") boolean success,
            @JsonProperty("matchIndex") long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    /**
     * Factory method for successful append / heartbeat response.
     */
    public static AppendEntriesResponseDto success(long term, long matchIndex) {
        return new AppendEntriesResponseDto(term, true, matchIndex);
    }

    /**
     * Factory method for unsuccessful append / heartbeat response (matchIndex = 0).
     */
    public static AppendEntriesResponseDto failure(long term) {
        return new AppendEntriesResponseDto(term, false, 0L);
    }

    /**
     * Validates fields according to SPEC-03 Section 6.
     */
    public void validate() {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0, got: " + term);
        }
        if (matchIndex < 0) {
            throw new IllegalArgumentException("matchIndex must be >= 0, got: " + matchIndex);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppendEntriesResponseDto that = (AppendEntriesResponseDto) o;
        return term == that.term &&
                success == that.success &&
                matchIndex == that.matchIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, success, matchIndex);
    }

    @Override
    public String toString() {
        return "AppendEntriesResponseDto{" +
                "term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
