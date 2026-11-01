package vn.nhom6.railway.raft.core;

import java.util.Objects;
import java.util.Set;

/**
 * One immutable entry of the Raft log owned by a single Node (SPEC-04 section 2 and
 * SPEC-05 {@code raft_log}).
 *
 * <p>The command is carried as an opaque JSON string. This type must not depend on the
 * Booking command DTOs and must not parse or inspect the command: the same persisted
 * bytes are replicated to every Node and applied deterministically by the state machine
 * (SPEC-03 section 6.1). {@code commandType} stays a {@link String} on purpose so that
 * {@code raft/core} keeps no dependency on the {@code booking} or {@code shared} packages.
 *
 * <p>Entry index 0 never exists; the empty log is represented by
 * {@code (lastLogIndex = 0, lastLogTerm = 0)} (SPEC-03). A log therefore starts at index 1
 * and is contiguous; {@link RaftState} enforces both properties.
 *
 * @param index         1-based position in the log, {@code >= 1}
 * @param term          term of the Leader that created the entry, {@code >= 1}
 * @param commandType   one of {@link #COMMAND_TYPE_BOOK_TICKET},
 *                      {@link #COMMAND_TYPE_CANCEL_TICKET}, {@link #COMMAND_TYPE_NO_OP};
 *                      matches the {@code CHECK} constraint of {@code raft_log.command_type}
 * @param commandPayload opaque JSON text of the command, never {@code null} or blank
 */
public record RaftLogEntry(long index, long term, String commandType, String commandPayload) {

    /** First valid log index. There is no stored entry at index 0. */
    public static final long FIRST_INDEX = 1L;

    /** Lowest valid entry term; term 0 only describes an empty log. */
    public static final long FIRST_TERM = 1L;

    public static final String COMMAND_TYPE_BOOK_TICKET = "BOOK_TICKET";
    public static final String COMMAND_TYPE_CANCEL_TICKET = "CANCEL_TICKET";
    public static final String COMMAND_TYPE_NO_OP = "NO_OP";

    /** Command types accepted by {@code raft_log.command_type} (SPEC-05 section 4). */
    public static final Set<String> SUPPORTED_COMMAND_TYPES =
            Set.of(COMMAND_TYPE_BOOK_TICKET, COMMAND_TYPE_CANCEL_TICKET, COMMAND_TYPE_NO_OP);

    public RaftLogEntry {
        if (index < FIRST_INDEX) {
            throw new IllegalArgumentException("index must be >= " + FIRST_INDEX + " but was " + index);
        }
        if (term < FIRST_TERM) {
            throw new IllegalArgumentException("term must be >= " + FIRST_TERM + " but was " + term);
        }
        Objects.requireNonNull(commandType, "commandType");
        if (!SUPPORTED_COMMAND_TYPES.contains(commandType)) {
            throw new IllegalArgumentException(
                    "unsupported commandType '" + commandType + "', expected one of " + SUPPORTED_COMMAND_TYPES);
        }
        Objects.requireNonNull(commandPayload, "commandPayload");
        if (commandPayload.isBlank()) {
            throw new IllegalArgumentException("commandPayload must not be blank");
        }
    }

    /**
     * Returns {@code true} when the other entry occupies the same log position in the same
     * term. This is the Raft log matching check (SPEC-04 section 4); it intentionally ignores
     * the command payload, whereas {@link #equals(Object)} compares every field.
     */
    public boolean sameIndexAndTerm(RaftLogEntry other) {
        Objects.requireNonNull(other, "other");
        return index == other.index && term == other.term;
    }

    /** Convenience factory for a {@code NO_OP} read-barrier entry (SPEC-04 section 7). */
    public static RaftLogEntry noOp(long index, long term) {
        return new RaftLogEntry(index, term, COMMAND_TYPE_NO_OP, "{}");
    }
}
