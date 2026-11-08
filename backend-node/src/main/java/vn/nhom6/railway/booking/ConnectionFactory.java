package vn.nhom6.railway.booking;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import vn.nhom6.railway.persistence.DbConfig;

/**
 * Opens PostgreSQL connections for this node without opening a connection in the constructor.
 *
 * <p>The caller must close each returned connection, preferably with try-with-resources.
 * Connections retain JDBC's default {@code autoCommit=true}; the caller manages transactions
 * (including disabling auto-commit and committing or rolling back each apply atomically as
 * required by SPEC-05 §5).
 */
public final class ConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(ConnectionFactory.class.getName());
    private static final int DEFAULT_TIMEOUT_SECONDS = 5;

    private final DbConfig config;
    private final int connectTimeoutSeconds;
    private final int loginTimeoutSeconds;

    public ConnectionFactory(DbConfig config) {
        this(config, DEFAULT_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_SECONDS);
    }

    public ConnectionFactory(DbConfig config, int connectTimeoutSeconds, int loginTimeoutSeconds) {
        this.config = Objects.requireNonNull(config, "config");
        if (connectTimeoutSeconds < 1 || loginTimeoutSeconds < 1) {
            throw new IllegalArgumentException("Connection timeouts must be positive finite seconds");
        }
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.loginTimeoutSeconds = loginTimeoutSeconds;
    }

    /**
     * Opens a connection. The caller owns and must close the returned connection.
     *
     * @throws BookingRepository.StorageException if PostgreSQL cannot be reached
     */
    public Connection getConnection() {
        Properties properties = new Properties();
        properties.setProperty("user", config.user());
        properties.setProperty("password", config.password());
        properties.setProperty("connectTimeout", Integer.toString(connectTimeoutSeconds));
        properties.setProperty("loginTimeout", Integer.toString(loginTimeoutSeconds));

        try {
            return DriverManager.getConnection(config.jdbcUrl(), properties);
        } catch (SQLException exception) {
            throw storageFailure("connection", exception);
        }
    }

    /**
     * Checks PostgreSQL availability with {@code SELECT 1}; failures are propagated rather than
     * treated as healthy or retried.
     */
    public boolean isHealthy() {
        try (Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT 1");
                ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next() && resultSet.getInt(1) == 1) {
                return true;
            }
            throw new SQLException("Health check returned an unexpected result");
        } catch (SQLException exception) {
            throw storageFailure("health check", exception);
        }
    }

    private BookingRepository.StorageException storageFailure(String operation, SQLException cause) {
        String sqlState = cause.getSQLState();
        String driverMessage = cause.getMessage();
        String safeDriverMessage = driverMessage == null ? "unknown"
                : driverMessage.replace(config.password(), "***");
        String endpoint = config.host() + ":" + config.port() + "/" + config.dbName();

        LOGGER.log(Level.SEVERE,
                "PostgreSQL {0} failed for {1}; SQLState={2}, vendorCode={3}, driverMessage={4}",
                new Object[] {operation, endpoint, sqlState, cause.getErrorCode(), safeDriverMessage});

        return new BookingRepository.StorageException(
                "PostgreSQL " + operation + " failed for " + endpoint
                        + " (SQLState=" + sqlState + ", vendorCode=" + cause.getErrorCode() + ")",
                cause);
    }
}
