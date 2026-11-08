package vn.nhom6.railway.persistence;

import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import vn.nhom6.railway.booking.BookingCommand;
import vn.nhom6.railway.booking.BookingRepository;
import vn.nhom6.railway.booking.BookingStateMachine;
import vn.nhom6.railway.booking.CommittedCommandApplier;
import vn.nhom6.railway.booking.ConnectionFactory;

/** Bridges committed Raft entries to this node's PostgreSQL booking repository. */
public final class JdbcCommittedCommandApplier {
    private static final Logger LOGGER =
            Logger.getLogger(JdbcCommittedCommandApplier.class.getName());

    private final BookingRepository repository;
    private final CommittedCommandApplier delegate;
    private final long initialCommitIndex;

    public JdbcCommittedCommandApplier(ConnectionFactory connectionFactory) {
        this(connectionFactory, result -> { });
    }

    public JdbcCommittedCommandApplier(
            ConnectionFactory connectionFactory,
            CommittedCommandApplier.ApplyResultListener resultListener) {
        this(new JdbcBookingRepository(Objects.requireNonNull(
                connectionFactory, "connectionFactory")), resultListener);
    }

    JdbcCommittedCommandApplier(
            JdbcBookingRepository repository,
            CommittedCommandApplier.ApplyResultListener resultListener) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.delegate = new CommittedCommandApplier(
                repository, Objects.requireNonNull(resultListener, "resultListener"));
        try {
            this.initialCommitIndex = repository.lastApplied();
        } catch (BookingRepository.StorageException exception) {
            LOGGER.log(Level.SEVERE, "Could not read persistent lastApplied during startup");
            throw exception;
        }
    }

    /**
     * Persistent checkpoint to initialize Raft's commitIndex after restart.
     * It is not inferred from the last log index.
     */
    public long initialCommitIndex() {
        return initialCommitIndex;
    }

    /** Reads the current durable apply checkpoint. */
    public long lastApplied() {
        try {
            return repository.lastApplied();
        } catch (BookingRepository.StorageException exception) {
            LOGGER.log(Level.SEVERE, "Could not read persistent lastApplied");
            throw exception;
        }
    }

    /** Raft-side callback for entries that have already committed, in log-index order. */
    public CommittedCommandApplier.CommitListener commitListener() {
        return this::applyCommitted;
    }

    /**
     * Applies one committed entry. Repository completion includes the database commit, so the
     * delegated result listener cannot observe a result before durable state is committed.
     */
    public Optional<BookingStateMachine.ApplyOutcome> applyCommitted(
            long logIndex, BookingCommand command) {
        try {
            return delegate.applyCommitted(logIndex, command);
        } catch (BookingRepository.StorageException exception) {
            LOGGER.log(Level.SEVERE, "PostgreSQL committed-entry apply failed at index {0}",
                    logIndex);
            throw exception;
        }
    }
}
