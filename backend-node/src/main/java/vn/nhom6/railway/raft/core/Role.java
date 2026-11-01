package vn.nhom6.railway.raft.core;

/**
 * Raft role of the local Node (SPEC-04 section 2).
 *
 * <p>Volatile state only: it is never persisted and is rebuilt as {@link #FOLLOWER} on
 * restart. No Node or laptop is hardcoded as leader; leadership is only the result of a
 * completed election. Shared by election (TV5) and replication (TV4) so that both sides use
 * one role type instead of two competing definitions.
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
