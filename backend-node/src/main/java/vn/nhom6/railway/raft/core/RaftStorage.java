package vn.nhom6.railway.raft.core;

import java.util.List;
import java.util.Objects;

/**
 * Durable local storage boundary for one Raft Node's term, vote, and log.
 *
 * <p>This contract is invoked sequentially by the Raft event loop and is not thread-safe.
 * It performs no timing, randomness, or identifier generation. A successful persistence
 * operation means its data is durable; it does not mean that an entry is Raft-committed
 * or has been applied by the state machine.
 */
public interface RaftStorage {

    /**
     * Loads the durable state needed by {@link RaftState#restore(long, String, List, long)}.
     *
     * <p>The returned log must be contiguous from index 1. A gap in the log or a
     * {@code lastApplied} value beyond the last log index is a storage error and must fail
     * closed. On restart, Raft reconstructs {@code commitIndex} as {@code lastApplied};
     * it must not infer the commit index from the last log index (SPEC-04 section 6).
     *
     * @return the durable term, vote, log, and state-machine apply checkpoint
     * @throws RaftStorageException if the state cannot be read or is inconsistent
     */
    PersistentState load() throws RaftStorageException;

    /**
     * Durably stores the current term and vote as one state update.
     *
     * <p>{@code currentTerm} must not decrease. {@code votedFor} may be {@code null}, in
     * particular when the Node enters a newer term and clears its vote. This method must
     * return only after both values are durable. It must be called before replying to a
     * granted vote or sending {@code REQUEST_VOTE} for a new election (SPEC-04 sections 3.2
     * and 9).
     *
     * @param currentTerm current persistent term
     * @param votedFor candidate voted for in this term, or {@code null}
     * @throws RaftStorageException if the update cannot be made durable
     */
    void saveTermAndVote(long currentTerm, String votedFor) throws RaftStorageException;

    /**
     * Atomically truncates the log suffix starting at {@code fromIndex} and appends
     * {@code entries}.
     *
     * <p>{@code entries} must be contiguous and, when non-empty, start at
     * {@code fromIndex}. The caller ({@link RaftNode}) must reject
     * {@code fromIndex <= commitIndex}; storage must never silently overwrite a committed
     * entry. This method returns only after the replacement is durable, before an
     * {@code APPEND_ENTRIES_RESPONSE} with {@code success=true} is sent (SPEC-04 sections 4
     * and 9). An empty list means truncate the suffix from {@code fromIndex} onward.
     *
     * @param fromIndex first index to replace; must be at least 1
     * @param entries replacement entries in contiguous index order
     * @throws IllegalArgumentException if the index or replacement sequence is invalid
     * @throws RaftStorageException if the replacement cannot be made durable or would
     *                              overwrite committed data
     */
    void replaceSuffix(long fromIndex, List<RaftLogEntry> entries) throws RaftStorageException;

    /**
     * Reads the durable state-machine apply checkpoint.
     *
     * <p>This is read-only. There is intentionally no storage method to set
     * {@code lastApplied}: TV2's state-machine apply transaction must update the business
     * state and checkpoint atomically (SPEC-04 section 6 and SPEC-05 section 5).
     *
     * @return the highest log index durably applied
     * @throws RaftStorageException if the checkpoint cannot be read
     */
    long loadLastApplied() throws RaftStorageException;

    /**
     * Durable state required to restore the in-memory state of one Raft Node.
     *
     * @param currentTerm persisted current term
     * @param votedFor persisted vote for the current term, or {@code null}
     * @param log persisted entries in ascending contiguous index order
     * @param lastApplied durable state-machine apply checkpoint
     */
    record PersistentState(long currentTerm, String votedFor, List<RaftLogEntry> log, long lastApplied) {
        public PersistentState {
            log = List.copyOf(Objects.requireNonNull(log, "log"));
        }
    }

    /**
     * Checked failure from local durable storage.
     *
     * <p>On any storage failure, the Node must fail closed, stop confirming writes, and log
     * the operational failure (SPEC-04 section 9).
     */
    class RaftStorageException extends Exception {

        private static final long serialVersionUID = 1L;

        public RaftStorageException(String message) {
            super(message);
        }

        public RaftStorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
