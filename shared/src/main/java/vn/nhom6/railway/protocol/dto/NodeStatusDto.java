package vn.nhom6.railway.protocol.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Payload DTO for diagnostic {@code NODE_STATUS_RESPONSE} according to SPEC-03 Section 5.
 *
 * <p>Key Invariants:
 * <ul>
 *   <li><b>Local Observation:</b> Reflects only the local node's perspective at {@code observedAt}.
 *       May lag behind cluster reality and is not a consensus-wide snapshot.</li>
 *   <li><b>No Fabricated State:</b> If Raft or storage is not initialized, {@code role} and {@code term}
 *       may be {@code null}. Implementations must not fabricate {@code FOLLOWER} or term 0 to hide faults.</li>
 *   <li><b>Peer Connectivity:</b> For a 5-node cluster, contains the other 4 nodes with {@code true} (recent RPC success),
 *       {@code false} (failed/timeout), or {@code null} (unobserved).</li>
 * </ul>
 */
public class NodeStatusDto {

    public static final String ROLE_LEADER = "LEADER";
    public static final String ROLE_FOLLOWER = "FOLLOWER";
    public static final String ROLE_CANDIDATE = "CANDIDATE";

    public static final Set<String> VALID_ROLES = Set.of(ROLE_LEADER, ROLE_FOLLOWER, ROLE_CANDIDATE);
    private static final Pattern NODE_ID_PATTERN = Pattern.compile("^node-[a-zA-Z0-9_-]+$");

    @JsonProperty("nodeId")
    private String nodeId;

    @JsonProperty("role")
    private String role;

    @JsonProperty("term")
    private Long term;

    @JsonProperty("leaderId")
    private String leaderId;

    @JsonProperty("lastLogIndex")
    private Long lastLogIndex;

    @JsonProperty("commitIndex")
    private Long commitIndex;

    @JsonProperty("lastApplied")
    private Long lastApplied;

    @JsonProperty("storageHealthy")
    private Boolean storageHealthy;

    @JsonProperty("peerConnectivity")
    private Map<String, Boolean> peerConnectivity;

    @JsonProperty("observedAt")
    private String observedAt;

    public NodeStatusDto() {
        // Default constructor for Jackson
    }

    @JsonCreator
    public NodeStatusDto(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("role") String role,
            @JsonProperty("term") Long term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("lastLogIndex") Long lastLogIndex,
            @JsonProperty("commitIndex") Long commitIndex,
            @JsonProperty("lastApplied") Long lastApplied,
            @JsonProperty("storageHealthy") Boolean storageHealthy,
            @JsonProperty("peerConnectivity") Map<String, Boolean> peerConnectivity,
            @JsonProperty("observedAt") String observedAt) {
        this.nodeId = nodeId;
        this.role = role;
        this.term = term;
        this.leaderId = leaderId;
        this.lastLogIndex = lastLogIndex;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.storageHealthy = storageHealthy;
        this.peerConnectivity = peerConnectivity != null ? Collections.unmodifiableMap(peerConnectivity) : Collections.emptyMap();
        this.observedAt = observedAt != null ? observedAt : Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
    }

    /**
     * Validates fields according to SPEC-03 Section 5 rules.
     */
    public void validate() {
        if (nodeId == null || !NODE_ID_PATTERN.matcher(nodeId).matches()) {
            throw new IllegalArgumentException("nodeId must match node-*, got: " + nodeId);
        }
        if (role != null && !VALID_ROLES.contains(role)) {
            throw new IllegalArgumentException("Invalid role: " + role + ", must be one of " + VALID_ROLES + " or null");
        }
        if (term != null && term < 0) {
            throw new IllegalArgumentException("term must be >= 0 when present, got: " + term);
        }
        if (leaderId != null && !NODE_ID_PATTERN.matcher(leaderId).matches()) {
            throw new IllegalArgumentException("leaderId must match node-* when present, got: " + leaderId);
        }
        if (lastLogIndex != null && lastLogIndex < 0) {
            throw new IllegalArgumentException("lastLogIndex must be >= 0 when present, got: " + lastLogIndex);
        }
        if (commitIndex != null && commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must be >= 0 when present, got: " + commitIndex);
        }
        if (lastApplied != null && lastApplied < 0) {
            throw new IllegalArgumentException("lastApplied must be >= 0 when present, got: " + lastApplied);
        }
        if (storageHealthy == null) {
            throw new IllegalArgumentException("storageHealthy must not be null");
        }
        if (peerConnectivity == null) {
            throw new IllegalArgumentException("peerConnectivity must not be null");
        }
        if (observedAt == null || observedAt.isBlank()) {
            throw new IllegalArgumentException("observedAt must not be null or blank");
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Long getTerm() {
        return term;
    }

    public void setTerm(Long term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public Long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(Long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public Long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(Long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public Long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(Long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public Boolean getStorageHealthy() {
        return storageHealthy;
    }

    public void setStorageHealthy(Boolean storageHealthy) {
        this.storageHealthy = storageHealthy;
    }

    public Map<String, Boolean> getPeerConnectivity() {
        return peerConnectivity;
    }

    public void setPeerConnectivity(Map<String, Boolean> peerConnectivity) {
        this.peerConnectivity = peerConnectivity != null ? Collections.unmodifiableMap(peerConnectivity) : Collections.emptyMap();
    }

    public String getObservedAt() {
        return observedAt;
    }

    public void setObservedAt(String observedAt) {
        this.observedAt = observedAt;
    }

    @JsonIgnore
    public boolean isLeader() {
        return ROLE_LEADER.equalsIgnoreCase(role);
    }

    @JsonIgnore
    public boolean isFollower() {
        return ROLE_FOLLOWER.equalsIgnoreCase(role);
    }

    @JsonIgnore
    public boolean isCandidate() {
        return ROLE_CANDIDATE.equalsIgnoreCase(role);
    }

    @JsonIgnore
    public boolean isStorageHealthy() {
        return Boolean.TRUE.equals(storageHealthy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeStatusDto that = (NodeStatusDto) o;
        return Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(role, that.role) &&
                Objects.equals(term, that.term) &&
                Objects.equals(leaderId, that.leaderId) &&
                Objects.equals(lastLogIndex, that.lastLogIndex) &&
                Objects.equals(commitIndex, that.commitIndex) &&
                Objects.equals(lastApplied, that.lastApplied) &&
                Objects.equals(storageHealthy, that.storageHealthy) &&
                Objects.equals(peerConnectivity, that.peerConnectivity) &&
                Objects.equals(observedAt, that.observedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, role, term, leaderId, lastLogIndex, commitIndex, lastApplied, storageHealthy, peerConnectivity, observedAt);
    }

    @Override
    public String toString() {
        return "NodeStatusDto{" +
                "nodeId='" + nodeId + '\'' +
                ", role='" + role + '\'' +
                ", term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", commitIndex=" + commitIndex +
                ", lastApplied=" + lastApplied +
                ", storageHealthy=" + storageHealthy +
                ", peerConnectivity=" + peerConnectivity +
                ", observedAt='" + observedAt + '\'' +
                '}';
    }
}
