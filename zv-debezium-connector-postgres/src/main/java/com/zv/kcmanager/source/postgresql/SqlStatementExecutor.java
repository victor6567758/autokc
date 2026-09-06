package com.zv.kcmanager.source.postgresql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zv.kcmanager.common.metrics.MetricsManager;

import io.debezium.config.Configuration;

/**
 * Periodically runs the statements of the configured zv SQL resources on a dedicated JDBC
 * connection (never Debezium's) and reports success, duration and numeric results to
 * {@link MetricsManager}.
 */
final class SqlStatementExecutor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlStatementExecutor.class);

    private static final String CONNECTOR_TYPE = "zv-postgres-source";
    private static final String CONTEXT = "sql";
    private static final int TASK = -1;
    private static final String TASK_PARTITION = "n/a";

    private final ZvSqlConfig config;
    private final String connectorName;
    private final MetricsManager metrics = MetricsManager.instance();
    private final Map<String, List<String>> statements;
    private final ScheduledExecutorService executor;
    private volatile Connection connection;

    private SqlStatementExecutor(ZvSqlConfig config, String connectorName) {
        this.config = config;
        this.connectorName = connectorName;
        try {
            this.statements = config.loadStatements();
        }
        catch (IOException e) {
            throw new ConnectException("Failed to load zv SQL resources of connector " + connectorName, e);
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "zv-sql-" + connectorName);
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::runOnce, 0, config.intervalMs(), TimeUnit.MILLISECONDS);
        LOGGER.info("zv SQL statements of connector {} scheduled every {} ms", connectorName, config.intervalMs());
    }

    static SqlStatementExecutor startIfEnabled(Configuration rawConfig) {
        ZvSqlConfig config = ZvSqlConfig.from(rawConfig);
        if (!config.enabled()) {
            return null;
        }
        String name = rawConfig.getString("name");
        return new SqlStatementExecutor(config, name == null ? "unknown" : name);
    }

    private void runOnce() {
        try {
            runStatements();
        }
        catch (Throwable t) {
            // an exception escaping the runnable would silently cancel the scheduling
            LOGGER.warn("zv SQL run of connector {} failed", connectorName, t);
        }
    }

    private void runStatements() throws SQLException {
        try(Connection connection = connection()) {
            for (Map.Entry<String, List<String>> resource : statements.entrySet()) {
                List<String> resourceStatements = resource.getValue();
                for (int i = 0; i < resourceStatements.size(); i++) {
                    String statementId = statementId(resource.getKey(), i + 1);
                    long startedAt = System.nanoTime();
                    try (PreparedStatement statement = connection.prepareStatement(resourceStatements.get(i))) {
                        statement.setQueryTimeout(config.queryTimeoutSeconds());
                        try (ResultSet resultSet = statement.executeQuery()) {
                            report(statementId, (System.nanoTime() - startedAt) / 1_000_000L, resultSet);
                        }
                    } catch (SQLException e) {
                        LOGGER.warn("zv SQL statement {} of connector {} failed: {}", statementId,
                            connectorName, e.getMessage());
                        metrics.setGauge(CONNECTOR_TYPE, connectorName, CONTEXT, TASK,
                            TASK_PARTITION, statementId + ".success", 0);
                        metrics.incrementCounter(CONNECTOR_TYPE, connectorName, CONTEXT, TASK,
                            TASK_PARTITION, "failures");
                        dropConnection();
                        return;
                    }
                }
            }
        }
        metrics.incrementCounter(CONNECTOR_TYPE, connectorName, CONTEXT, TASK, TASK_PARTITION, "runs");
    }

    private void report(String statementId, long durationMs, ResultSet resultSet) throws SQLException {
        metrics.setGauge(CONNECTOR_TYPE, connectorName, CONTEXT, TASK, TASK_PARTITION, statementId + ".success", 1);
        metrics.updateHistogram(CONNECTOR_TYPE, connectorName, CONTEXT, TASK, TASK_PARTITION, statementId + ".durationMs", durationMs);
        if (!resultSet.next()) {
            return;
        }
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            Object value = resultSet.getObject(column);
            if (value instanceof Number number) {
                metrics.setGauge(CONNECTOR_TYPE, connectorName, CONTEXT, TASK, TASK_PARTITION,
                        statementId + "." + metricName(metaData.getColumnLabel(column)), number.longValue());
            }
        }
    }

    private static String metricName(String columnLabel) {
        return columnLabel.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private Connection connection() throws SQLException {
        Connection lConnection = connection;
        if (lConnection == null || lConnection.isClosed()) {
            lConnection = config.openConnection();
            lConnection.setAutoCommit(true);
            connection = lConnection;
        }
        return lConnection;
    }

    private void dropConnection() {
        Connection lConnection = connection;
        connection = null;
        if (lConnection != null) {
            try {
                lConnection.close();
            }
            catch (SQLException ignored) {
            }
        }
    }

    private static String statementId(String resource, int index) {
        String base = resource.substring(resource.lastIndexOf('/') + 1);
        if (base.endsWith(".sql")) {
            base = base.substring(0, base.length() - 4);
        }
        return base + "." + index;
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        dropConnection();
    }
}
