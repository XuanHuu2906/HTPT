package vn.nhom6.railway.persistence;

import java.util.Map;
import java.util.Objects;

/** Immutable PostgreSQL connection configuration loaded from the process environment. */
public record DbConfig(String host, int port, String dbName, String user, String password) {
    private static final String PASSWORD_PLACEHOLDER = "CHANGE_ME_LOCAL_ONLY";

    public DbConfig {
        requireNonBlank(host, "DB_HOST");
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("DB_PORT must be an integer from 1 to 65535");
        }
        requireNonBlank(dbName, "DB_NAME");
        requireNonBlank(user, "DB_USER");
        requireNonBlank(password, "DB_PASSWORD");
        if (PASSWORD_PLACEHOLDER.equals(password)) {
            throw new IllegalArgumentException("DB_PASSWORD must not use the example placeholder");
        }
    }

    /** Loads configuration from the process environment. */
    public static DbConfig fromEnvironment() {
        return fromMap(System.getenv());
    }

    /**
     * Loads configuration from a map using the same validation as environment configuration.
     * DB_PORT defaults to 5432; all other DB_* values are required.
     */
    public static DbConfig fromMap(Map<String, String> values) {
        Objects.requireNonNull(values, "values");
        String host = required(values, "DB_HOST");
        String portValue = values.get("DB_PORT");
        int port = portValue == null ? 5432 : parsePort(portValue);
        String dbName = required(values, "DB_NAME");
        String user = required(values, "DB_USER");
        String password = required(values, "DB_PASSWORD");
        return new DbConfig(host, port, dbName, user, password);
    }

    /** Returns a JDBC URL without embedding credentials. */
    public String jdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    }

    @Override
    public String toString() {
        return "DbConfig[host=" + host + ", port=" + port + ", dbName=" + dbName
                + ", user=" + user + ", password=***]";
    }

    private static String required(Map<String, String> values, String name) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static int parsePort(String value) {
        final int port;
        try {
            port = Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("DB_PORT must be an integer from 1 to 65535");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("DB_PORT must be an integer from 1 to 65535");
        }
        return port;
    }
}
