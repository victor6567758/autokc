package com.zv.kcmanager.source.postgresql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.util.Strings;


public final class ZvSqlConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvSqlConfig.class);

    /** Name of the zv SQL resources configuration key. */
    public static final String SQL_RESOURCES_PROPERTY_NAME = "zv.sql.resources";

    /**
     * Comma-separated classpath resources with the SQL statements to run. Optional: when empty
     * the zv feature is disabled and the connector behaves exactly like the stock connector.
     */
    public static final Field SQL_RESOURCES = Field.create(SQL_RESOURCES_PROPERTY_NAME)
            .withDisplayName("SQL resources")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Comma-separated classpath resources, packaged inside the connector plugin archive, "
                    + "with the SQL statements to run. Statements within a resource are separated by semicolons; "
                    + "line and block comments are ignored. Connector configuration values are available in "
                    + "statements via ${key} placeholders, e.g. ${slot.name}. Whenever the connector configuration "
                    + "is validated, every statement is parsed by the database server (prepared, never executed).");

    /** Interval at which the zv SQL statements run on the connector task. */
    public static final Field INTERVAL_MS = Field.create("zv.sql.interval.ms")
            .withDisplayName("SQL run interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(30_000L)
            .withValidation(Field::isPositiveLong)
            .withDescription("How often the statements of the zv SQL resources run on the connector "
                    + "task's single-thread executor, in milliseconds.");

    /** Server-side timeout of every zv SQL statement. */
    public static final Field QUERY_TIMEOUT_S = Field.create("zv.sql.query.timeout.s")
            .withDisplayName("SQL query timeout (s)")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(10L)
            .withValidation(Field::isPositiveLong)
            .withDescription("Server-side timeout of every zv SQL statement, in seconds.");

    /** All zv-specific fields. */
    public static final Field.Set ALL_FIELDS = Field.setOf(SQL_RESOURCES, INTERVAL_MS, QUERY_TIMEOUT_S);

    private static final String JDBC_URL_PREFIX = "jdbc:postgresql://";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_DBNAME = "postgres";

    /** {@code database.*} keys consumed by the URL itself, not handed to the driver as properties. */
    private static final Set<String> URL_KEYS = Set.of("hostname", "port", "dbname");

    private static volatile boolean driverLoaded;
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)\\}");

    private final Configuration config;
    private final List<String> resources;
    private final long intervalMs;
    private final int queryTimeoutSeconds;
    private final String hostname;
    private final String port;
    private final String dbname;
    private final Properties driverProperties;

    private ZvSqlConfig(Configuration config) {
        this.config = config;
        this.resources = parseResources(config.getString(SQL_RESOURCES_PROPERTY_NAME));
        this.intervalMs = orDefaultLong(config.getLong(INTERVAL_MS.name()), INTERVAL_MS);
        this.queryTimeoutSeconds = (int) orDefaultLong(config.getLong(QUERY_TIMEOUT_S.name()), QUERY_TIMEOUT_S);
        Configuration database = config.subset("database.", true);
        this.hostname = orDefault(database.getString("hostname"), DEFAULT_HOSTNAME);
        this.port = orDefault(database.getString("port"), DEFAULT_PORT);
        this.dbname = orDefault(database.getString("dbname"), DEFAULT_DBNAME);
        this.driverProperties = new Properties();
        database.asMap().forEach((key, value) -> {
            if (!URL_KEYS.contains(key) && !Strings.isNullOrEmpty(value)) {
                driverProperties.setProperty(key, value);
            }
        });
    }

    /**
     * Pulls the zv SQL configuration out of the given (raw) connector configuration.
     *
     * @param config the connector configuration as handed to {@code Connector#validate(Map)}
     * @return never {@code null}
     */
    public static ZvSqlConfig from(Configuration config) {
        return new ZvSqlConfig(config);
    }

    /** Whether the zv SQL feature is configured at all. */
    public boolean enabled() {
        return !resources.isEmpty();
    }

    /** The configured resource paths, in order; empty when the feature is disabled. */
    public List<String> resources() {
        return resources;
    }

    /** How often the statements run, in milliseconds; {@link #INTERVAL_MS}'s default when unset. */
    public long intervalMs() {
        return intervalMs;
    }

    /** Server-side timeout of every statement, in seconds; {@link #QUERY_TIMEOUT_S}'s default when unset. */
    public int queryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    /** JDBC URL derived from the {@code database.*} properties. */
    public String jdbcUrl() {
        return JDBC_URL_PREFIX + hostname + ":" + port + "/" + dbname;
    }

    /** Driver connection properties (user, password and every extra {@code database.*} key). */
    public Properties driverProperties() {
        Properties copy = new Properties();
        copy.putAll(driverProperties);
        return copy;
    }

    /**
     * Opens a fresh plain JDBC connection for the zv feature. Deliberately separate from
     * Debezium's replication connection: zv statements run on a plain autocommit connection.
     *
     * @return an open connection; callers are responsible for closing it
     * @throws SQLException if the driver is missing or the connection cannot be established
     */
    public Connection openConnection() throws SQLException {
        loadDriver();
        return DriverManager.getConnection(jdbcUrl(), driverProperties());
    }

    /**
     * Reads a SQL resource from the connector plugin classpath.
     *
     * @param resource the resource path, as configured (no leading slash)
     * @return the resource content, never {@code null}
     * @throws IOException if the resource does not exist or cannot be read
     */
    public String loadResource(String resource) throws IOException {
        ClassLoader classLoader = ZvSqlConfig.class.getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IOException("SQL resource '" + resource + "' not found on the connector plugin classpath");
            }
            StringBuilder text = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    text.append(line).append('\n');
                }
            }
            return text.toString();
        }
    }

    /**
     * Loads every configured resource and splits it into individual statements.
     *
     * @return resource path to its statements, in configured order
     * @throws IOException if any resource cannot be read
     */
    public Map<String, List<String>> loadStatements() throws IOException {
        Map<String, List<String>> statements = new LinkedHashMap<>();
        for (String resource : resources) {
            List<String> resolved = new ArrayList<>();
            for (String statement : splitStatements(loadResource(resource))) {
                resolved.add(resolvePlaceholders(statement));
            }
            statements.put(resource, List.copyOf(resolved));
        }
        return statements;
    }

    private String resolvePlaceholders(String statement) {
        if (!statement.contains("${")) {
            return statement;
        }
        Matcher matcher = PLACEHOLDER.matcher(statement);
        StringBuilder resolved = new StringBuilder();
        while (matcher.find()) {
            String value = placeholderValue(matcher.group(1).trim());
            if (value != null) {
                matcher.appendReplacement(resolved, Matcher.quoteReplacement(value));
            }
        }
        matcher.appendTail(resolved);
        return resolved.toString();
    }

    private String placeholderValue(String key) {
        String value = config.getString(key);
        if (value == null && PostgresConnectorConfig.SLOT_NAME.name().equals(key)) {
            value = PostgresConnectorConfig.SLOT_NAME.defaultValueAsString();
        }
        return value;
    }

    /**
     * Splits SQL text into statements on semicolons, dropping line and block comments.
     * Semicolons inside single-quoted literals (including escaped quotes) are preserved.
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean inSingleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
            }
            else if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                    current.append(' ');
                }
            }
            else if (inSingleQuote) {
                current.append(c);
                if (c == '\'') {
                    inSingleQuote = false;
                }
            }
            else if (c == '-' && next == '-') {
                inLineComment = true;
                i++;
            }
            else if (c == '/' && next == '*') {
                inBlockComment = true;
                i++;
            }
            else if (c == '\'') {
                inSingleQuote = true;
                current.append(c);
            }
            else if (c == ';') {
                addIfNotBlank(statements, current);
                current.setLength(0);
            }
            else {
                current.append(c);
            }
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    private static void loadDriver() throws SQLException {
        if (driverLoaded) {
            return;
        }
        synchronized (ZvSqlConfig.class) {
            if (driverLoaded) {
                return;
            }
            try {
                // Under Kafka Connect plugin classloading the driver jar is invisible to the
                // DriverManager's ServiceLoader scan of the system classloader; load it
                // explicitly through the plugin classloader so it self-registers.
                Class.forName("org.postgresql.Driver", true, ZvSqlConfig.class.getClassLoader());
                driverLoaded = true;
            }
            catch (ClassNotFoundException e) {
                throw new SQLException("PostgreSQL JDBC driver not found on the connector plugin classpath", e);
            }
        }
    }

    private static List<String> parseResources(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return List.of();
        }
        List<String> resources = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                resources.add(trimmed);
            }
        }
        return List.copyOf(resources);
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder statement) {
        String trimmed = statement.toString().trim();
        if (!trimmed.isEmpty()) {
            statements.add(trimmed);
        }
    }

    private static String orDefault(String value, String fallback) {
        return Strings.isNullOrEmpty(value) ? fallback : value;
    }

    private static long orDefaultLong(Long value, Field field) {
        return value != null ? value : (Long) field.defaultValue();
    }
}
