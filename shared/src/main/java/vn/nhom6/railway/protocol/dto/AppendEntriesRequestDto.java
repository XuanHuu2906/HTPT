package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request payload for APPEND_ENTRIES Raft RPC according to SPEC-03 Section 6 and Section 6.1.
 * Also used for Leader heartbeats when {@code entries} is empty.
 */
public class AppendEntriesRequestDto {

    @JsonProperty("term")
    private long term;

    @JsonProperty("leaderId")
    private String leaderId;

    @JsonProperty("prevLogIndex")
    private long prevLogIndex;

    @JsonProperty("prevLogTerm")
    private long prevLogTerm;

    @JsonProperty("entries")
    private List<LogEntryDto> entries = new ArrayList<>();

    @JsonProperty("leaderCommit")
    private long leaderCommit;

    public AppendEntriesRequestDto() {
    }

    @JsonCreator
    public AppendEntriesRequestDto(
            @JsonProperty("term") long term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("prevLogIndex") long prevLogIndex,
            @JsonProperty("prevLogTerm") long prevLogTerm,
            @JsonProperty("entries") List<LogEntryDto> entries,
            @JsonProperty("leaderCommit") long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? entries : new ArrayList<>();
        this.leaderCommit = leaderCommit;
    }

    /**
     * Factory method to build a heartbeat request with empty entries list.
     */
    public static AppendEntriesRequestDto createHeartbeat(
            long term, String leaderId, long prevLogIndex, long prevLogTerm, long leaderCommit) {
        return new AppendEntriesRequestDto(term, leaderId, prevLogIndex, prevLogTerm, new ArrayList<>(), leaderCommit);
    }

    /**
     * Checks if this RPC is a heartbeat (empty entries list).
     */
    @JsonIgnore
    public boolean isHeartbeat() {
        return entries == null || entries.isEmpty();
    }

    /**
     * Validates fields according to SPEC-03 Section 3.0 and Section 6.1.
     */
    public void validate() {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0, got: " + term);
        }
        if (leaderId == null || leaderId.isBlank()) {
            throw new IllegalArgumentException("leaderId must not be null or blank");
        }
        if (prevLogIndex < 0) {
            throw new IllegalArgumentException("prevLogIndex must be >= 0, got: " + prevLogIndex);
        }
        if (prevLogTerm < 0) {
            throw new IllegalArgumentException("prevLogTerm must be >= 0, got: " + prevLogTerm);
        }
        if (prevLogIndex == 0 && prevLogTerm != 0) {
            throw new IllegalArgumentException("(prevLogIndex=0) requires prevLogTerm=0 according to SPEC-03 Section 6.1, got: " + prevLogTerm);
        }
        if (entries == null) {
            throw new IllegalArgumentException("entries must not be null (use empty list for heartbeat)");
        }
        for (LogEntryDto entry : entries) {
            if (entry == null) {
                throw new IllegalArgumentException("Log entry in entries must not be null");
            }
            entry.validate();
        }
        if (leaderCommit < 0) {
            throw new IllegalArgumentException("leaderCommit must be >= 0, got: " + leaderCommit);
        }
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<LogEntryDto> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntryDto> entries) {
        this.entries = entries != null ? entries : new ArrayList<>();
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppendEntriesRequestDto that = (AppendEntriesRequestDto) o;
        return term == that.term &&
                prevLogIndex == that.prevLogIndex &&
                prevLogTerm == that.prevLogTerm &&
                leaderCommit == that.leaderCommit &&
                Objects.equals(leaderId, that.leaderId) &&
                Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public String toString() {
        return "AppendEntriesRequestDto{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entriesCount=" + (entries != null ? entries.size() : 0) +
                ", leaderCommit=" + leaderCommit +
                '}';
    }

    /**
     * Represents a single Raft log entry inside APPEND_ENTRIES RPC.
     */
    public static class LogEntryDto {

        @JsonProperty("index")
        private long index;

        @JsonProperty("term")
        private long term;

        @JsonProperty("commandType")
        private String commandType;

        @JsonProperty("command")
        private Object command;

        public LogEntryDto() {
        }

        @JsonCreator
        public LogEntryDto(
                @JsonProperty("index") long index,
                @JsonProperty("term") long term,
                @JsonProperty("commandType") String commandType,
                @JsonProperty("command") Object command) {
            this.index = index;
            this.term = term;
            this.commandType = commandType;
            this.command = command;
        }

        public void validate() {
            if (index < 1) {
                throw new IllegalArgumentException("log entry index must be >= 1, got: " + index);
            }
            if (term < 1) {
                throw new IllegalArgumentException("log entry term must be >= 1, got: " + term);
            }
            if (commandType == null || commandType.isBlank()) {
                throw new IllegalArgumentException("commandType must not be null or blank");
            }
            if (command == null) {
                throw new IllegalArgumentException("command must not be null");
            }
        }

        public long getIndex() {
            return index;
        }

        public void setIndex(long index) {
            this.index = index;
        }

        public long getTerm() {
            return term;
        }

        public void setTerm(long term) {
            this.term = term;
        }

        public String getCommandType() {
            return commandType;
        }

        public void setCommandType(String commandType) {
            this.commandType = commandType;
        }

        public Object getCommand() {
            return command;
        }

        public void setCommand(Object command) {
            this.command = command;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LogEntryDto that = (LogEntryDto) o;
            return index == that.index &&
                    term == that.term &&
                    Objects.equals(commandType, that.commandType) &&
                    Objects.equals(command, that.command);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, term, commandType, command);
        }

        @Override
        public String toString() {
            return "LogEntryDto{" +
                    "index=" + index +
                    ", term=" + term +
                    ", commandType='" + commandType + '\'' +
                    '}';
        }
    }
}
