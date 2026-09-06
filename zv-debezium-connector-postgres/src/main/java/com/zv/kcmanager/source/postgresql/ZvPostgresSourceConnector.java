package com.zv.kcmanager.source.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;


public class ZvPostgresSourceConnector extends PostgresConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvPostgresSourceConnector.class);

    /** Config-UI group of the zv additions, appended after the stock Debezium groups. */
    private static final String ZV_GROUP = "zv";
    private static final int ZV_GROUP_ORDER = 1000;

    /**
     * Stock Debezium Postgres ConfigDef plus the zv additions. The copy-constructor merge keeps
     * the fork's definitions (defaults, recommended values, groups) untouched.
     */
    @Override
    public ConfigDef config() {
        ConfigDef merged = new ConfigDef(super.config());
        merged.define(ZvSqlConfig.SQL_RESOURCES.name(), ConfigDef.Type.LIST, null, null,
                ConfigDef.Importance.LOW, ZvSqlConfig.SQL_RESOURCES.description(),
                ZV_GROUP, ZV_GROUP_ORDER, ConfigDef.Width.LONG,
                ZvSqlConfig.SQL_RESOURCES.displayName());
        merged.define(ZvSqlConfig.INTERVAL_MS.name(), ConfigDef.Type.LONG, (Long) ZvSqlConfig.INTERVAL_MS.defaultValue(), null,
                ConfigDef.Importance.LOW, ZvSqlConfig.INTERVAL_MS.description(),
                ZV_GROUP, ZV_GROUP_ORDER + 1, ConfigDef.Width.MEDIUM, ZvSqlConfig.INTERVAL_MS.displayName());
        merged.define(ZvSqlConfig.QUERY_TIMEOUT_S.name(), ConfigDef.Type.LONG, (Long) ZvSqlConfig.QUERY_TIMEOUT_S.defaultValue(), null,
                ConfigDef.Importance.LOW, ZvSqlConfig.QUERY_TIMEOUT_S.description(),
                ZV_GROUP, ZV_GROUP_ORDER + 2, ConfigDef.Width.SHORT, ZvSqlConfig.QUERY_TIMEOUT_S.displayName());
        return merged;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ZvPostgresSourceConnectorTask.class;
    }

    /**
     * Field-level validation of the zv additions. The raw properties are already available at
     * this early stage; every configured {@code zv.sql.resources} entry must resolve on the
     * connector plugin classpath. A resource that does not resolve is reported on the
     * {@code zv.sql.resources} field and prevents the connection check below from running.
     */
    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        Map<String, ConfigValue> results = super.validateAllFields(config);
        results.putAll(config.validate(ZvSqlConfig.ALL_FIELDS));
        validateResourcesResolve(config, results);
        return results;
    }

    /**
     * Connection validation: the fork's Postgres connectivity check first, then - only when zv
     * SQL resources are configured and the configuration is otherwise clean - every statement
     * is parsed by the database server via {@code prepareStatement()} (never executed). Parse
     * errors are reported on the {@code zv.sql.resources} field of the validate output.
     */
    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        super.validateConnection(configValues, config);
        validateSqlResourcesAgainstDatabase(configValues, config);
    }

    private void validateResourcesResolve(Configuration config, Map<String, ConfigValue> results) {
        ZvSqlConfig zvSqlConfig = ZvSqlConfig.from(config);
        if (!zvSqlConfig.enabled()) {
            return;
        }
        ConfigValue resourcesValue = results.computeIfAbsent(ZvSqlConfig.SQL_RESOURCES.name(), ConfigValue::new);
        for (String resource : zvSqlConfig.resources()) {
            try {
                zvSqlConfig.loadResource(resource);
            }
            catch (Exception e) {
                LOGGER.debug("SQL resource '{}' cannot be read", resource, e);
                resourcesValue.addErrorMessage(String.format(
                        "Cannot read SQL resource '%s' from the connector plugin classpath: %s", resource, e.getMessage()));
            }
        }
    }

    private void validateSqlResourcesAgainstDatabase(Map<String, ConfigValue> configValues, Configuration config) {
        ZvSqlConfig zvSqlConfig = ZvSqlConfig.from(config);
        if (!zvSqlConfig.enabled()) {
            return;
        }
        ConfigValue resourcesValue = configValues.get(ZvSqlConfig.SQL_RESOURCES.name());
        if (resourcesValue == null || !resourcesValue.errorMessages().isEmpty()) {
            return; // broken resource configuration - already reported at field level
        }
        // The fork's connection check has succeeded (validateConnection is only invoked by the
        // pipeline when no field has errors), so the same database.* properties are usable here.
        try (Connection connection = zvSqlConfig.openConnection()) {
            for (Map.Entry<String, List<String>> entry : zvSqlConfig.loadStatements().entrySet()) {
                for (String statement : entry.getValue()) {
                    LOGGER.debug("Preparing statement of SQL resource '{}': {}", entry.getKey(), statement);
                    // Server-side parse only: the try-with-resources PreparedStatement is closed
                    // immediately and never executed.
                    try (PreparedStatement ignored = connection.prepareStatement(statement)) {
                    }
                    catch (Exception e) {
                        resourcesValue.addErrorMessage(String.format(
                                "SQL resource '%s' contains a statement the database cannot parse: %s (statement: %s)",
                                entry.getKey(), e.getMessage(), statement));
                    }
                }
            }
            LOGGER.info("Validated SQL resources {} against {}", zvSqlConfig.resources(), zvSqlConfig.jdbcUrl());
        }
        catch (Exception e) {
            resourcesValue.addErrorMessage(String.format("Cannot validate SQL resources against %s: %s",
                    zvSqlConfig.jdbcUrl(), e.getMessage()));
        }
    }
}
