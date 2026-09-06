package com.zv.kcmanager.source.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link ZvSqlConfig} parsing and for the zv field-validation stage of
 * {@link ZvPostgresSourceConnector}. No database required.
 */
public class ZvSqlConfigTest {

    @Test
    public void shouldDeriveJdbcUrlFromDatabaseProperties() {
        Configuration config = configWith(
                "database.hostname", "db1",
                "database.port", "5555",
                "database.dbname", "appdb");
        assertThat(ZvSqlConfig.from(config).jdbcUrl()).isEqualTo("jdbc:postgresql://db1:5555/appdb");
    }

    @Test
    public void shouldFallBackToDefaultPort() {
        Configuration config = configWith(
                "database.hostname", "db1",
                "database.dbname", "appdb");
        assertThat(ZvSqlConfig.from(config).jdbcUrl()).isEqualTo("jdbc:postgresql://db1:5432/appdb");
    }

    @Test
    public void shouldPassDatabaseExtrasToDriverButNotUrlKeys() {
        Configuration config = configWith(
                "database.hostname", "db1",
                "database.port", "5432",
                "database.dbname", "appdb",
                "database.user", "monitor",
                "database.password", "secret",
                "database.sslmode", "require");
        Properties properties = ZvSqlConfig.from(config).driverProperties();
        assertThat(properties.getProperty("user")).isEqualTo("monitor");
        assertThat(properties.getProperty("password")).isEqualTo("secret");
        assertThat(properties.getProperty("sslmode")).isEqualTo("require");
        assertThat(properties).doesNotContainKeys("hostname", "port", "dbname");
    }

    @Test
    public void shouldParseResourcesFromCommaSeparatedList() {
        Configuration config = configWith("zv.sql.resources", " a.sql , b.sql ,, ");
        assertThat(ZvSqlConfig.from(config).resources()).containsExactly("a.sql", "b.sql");
        assertThat(ZvSqlConfig.from(config).enabled()).isTrue();

        assertThat(ZvSqlConfig.from(Configuration.empty()).resources()).isEmpty();
        assertThat(ZvSqlConfig.from(Configuration.empty()).enabled()).isFalse();
    }

    @Test
    public void shouldSplitStatementsIgnoringCommentsAndQuotedSemicolons() {
        String sql = "-- leading comment\n"
                + "SELECT 1; /* mid\n comment */ SELECT ';' AS semicolon;\n\n"
                + "SELECT 3";
        assertThat(ZvSqlConfig.splitStatements(sql)).containsExactly(
                "SELECT 1",
                "SELECT ';' AS semicolon",
                "SELECT 3");

        assertThat(ZvSqlConfig.splitStatements("SELECT 'it''s;ok'"))
                .containsExactly("SELECT 'it''s;ok'");
        assertThat(ZvSqlConfig.splitStatements("-- only comments\n/* nothing else */")).isEmpty();
    }

    @Test
    public void shouldLoadShippedHealthResource() throws IOException {
        ZvSqlConfig zvSqlConfig = ZvSqlConfig.from(configWith("zv.sql.resources", "zv-sql/health.sql"));
        Map<String, List<String>> statements = zvSqlConfig.loadStatements();
        assertThat(statements).containsKey("zv-sql/health.sql");
        assertThat(statements.get("zv-sql/health.sql")).singleElement().asString()
                .contains("pg_wal_lsn_diff")
                .contains("slot_name = 'debezium'");
    }

    @Test
    public void shouldResolveConfigPlaceholdersInStatements() throws IOException {
        Configuration config = configWith(
                "zv.sql.resources", "zv-sql/health.sql",
                "slot.name", "streamkap_slot");
        Map<String, List<String>> statements = ZvSqlConfig.from(config).loadStatements();
        assertThat(statements.get("zv-sql/health.sql").get(0)).contains("slot_name = 'streamkap_slot'");
    }

    @Test
    public void shouldFailToLoadMissingResource() {
        ZvSqlConfig zvSqlConfig = ZvSqlConfig.from(configWith("zv.sql.resources", "zv-sql/missing.sql"));
        try {
            zvSqlConfig.loadStatements();
            assertThat(false).as("expected IOException").isTrue();
        }
        catch (IOException expected) {
            assertThat(expected.getMessage()).contains("zv-sql/missing.sql");
        }
    }

    @Test
    public void shouldFlagUnreadableResourceDuringFieldValidation() {
        Map<String, ConfigValue> results = new ZvPostgresSourceConnector().validateAllFields(
                configWith("zv.sql.resources", "zv-sql/missing.sql"));
        ConfigValue value = results.get(ZvSqlConfig.SQL_RESOURCES_PROPERTY_NAME);
        assertThat(value).isNotNull();
        assertThat(value.errorMessages()).hasSize(1);
        assertThat(value.errorMessages().get(0)).contains("zv-sql/missing.sql");
    }

    @Test
    public void shouldAcceptShippedResourceDuringFieldValidation() {
        Map<String, ConfigValue> results = new ZvPostgresSourceConnector().validateAllFields(
                configWith("zv.sql.resources", "zv-sql/health.sql"));
        ConfigValue value = results.get(ZvSqlConfig.SQL_RESOURCES_PROPERTY_NAME);
        assertThat(value).isNotNull();
        assertThat(value.errorMessages()).isEmpty();
    }

    private static Configuration configWith(String... keyValues) {
        Configuration.Builder builder = Configuration.create();
        for (int i = 0; i < keyValues.length; i += 2) {
            builder = builder.with(keyValues[i], keyValues[i + 1]);
        }
        return builder.build();
    }
}
