package com.zv.kcmanager.source.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zv.kcmanager.common.test.ZvDebeziumITBase;
import com.zv.kcmanager.common.util.TestUtils;

/**
 * End-to-end Postgres source test: deploys a {@code ZvPostgresSourceConnector} via the
 * Connect REST API of the shared stack, mutates the source DB with SQL and
 * asserts the change events on Kafka.
 */
public class ITPostgresSource extends ZvDebeziumITBase {

    private static final String CONNECTOR_CLASS = "com.zv.kcmanager.source.postgresql.ZvPostgresSourceConnector";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration CONNECTOR_TIMEOUT = Duration.ofMinutes(2);

    private String connectorName;

    @AfterEach
    public void removeConnector() {
        if (connectorName != null) {
            deleteConnector(connectorName);
        }
    }

    @Test
    public void shouldStreamInitialSnapshot() throws Exception {
        createCustomersTable();
        insertCustomer(1, "Ada", "Lovelace", "ada@example.com");
        insertCustomer(2, "Grace", "Hopper", "grace@example.com");

        deployConnector(CONNECTOR_CLASS, "zvit_snapshot", "zvit_slot_snapshot", Map.of());
        String topic = "zvit_snapshot.public.customers";
        awaitTopic(topic, CONNECTOR_TIMEOUT);

        List<ConsumerRecord<String, String>> records = waitForRecords(topic, 2, CONNECTOR_TIMEOUT,
                recs -> countWithOp(recs, "r") >= 2);

        List<JsonNode> snapshots = records.stream().map(this::payloadOf).filter(p -> "r".equals(p.path("op").asText())).toList();
        assertThat(snapshots).hasSize(2);
        JsonNode first = snapshots.stream().filter(p -> p.path("after").path("id").asInt() == 1).findFirst().orElseThrow();
        assertThat(first.path("after").path("first_name").asText()).isEqualTo("Ada");
        assertThat(first.path("after").path("last_name").asText()).isEqualTo("Lovelace");
        assertThat(first.path("after").path("email").asText()).isEqualTo("ada@example.com");
        assertThat(first.path("source").path("snapshot").asText()).as("snapshot record").isIn("true", "first", "last");
    }

    @Test
    public void shouldStreamInserts() throws Exception {
        createCustomersTable();
        deployConnector(CONNECTOR_CLASS, "zvit_insert", "zvit_slot_insert", Map.of());

        insertCustomer(3, "Linus", "Torvalds", "linus@example.com");

        List<ConsumerRecord<String, String>> records = waitForRecords("zvit_insert.public.customers", 1, CONNECTOR_TIMEOUT,
                recs -> recs.stream().map(this::payloadOf).anyMatch(p -> "c".equals(p.path("op").asText())
                        && p.path("after").path("id").asInt() == 3));

        JsonNode create = records.stream().map(this::payloadOf)
                .filter(p -> "c".equals(p.path("op").asText()) && p.path("after").path("id").asInt() == 3)
                .findFirst().orElseThrow();
        assertThat(create.path("after").path("first_name").asText()).isEqualTo("Linus");
        assertThat(create.path("after").path("email").asText()).isEqualTo("linus@example.com");
        assertThat(create.path("before").isNull()).isTrue();
    }

    @Test
    public void shouldStreamUpdates() throws Exception {
        createCustomersTable();
        deployConnector(CONNECTOR_CLASS, "zvit_update", "zvit_slot_update", Map.of());

        insertCustomer(10, "Edsger", "Dijkstra", "edsger@example.com");
        String topic = "zvit_update.public.customers";
        waitForRecords(topic, 1, CONNECTOR_TIMEOUT);

        try (Connection connection = openDbConnection()) {
            TestUtils.runSQL(connection, "UPDATE customers SET email = 'dijkstra@example.com' WHERE id = 10");
        }

        List<ConsumerRecord<String, String>> records = waitForRecords(topic, 2, CONNECTOR_TIMEOUT,
                recs -> recs.stream().map(this::payloadOf).anyMatch(p -> "u".equals(p.path("op").asText())
                        && "dijkstra@example.com".equals(p.path("after").path("email").asText())));

        JsonNode update = records.stream().map(this::payloadOf)
                .filter(p -> "u".equals(p.path("op").asText()))
                .findFirst().orElseThrow();
        assertThat(update.path("before").path("email").asText()).isEqualTo("edsger@example.com");
        assertThat(update.path("after").path("email").asText()).isEqualTo("dijkstra@example.com");
    }

    @Test
    public void shouldStreamDeletes() throws Exception {
        createCustomersTable();
        deployConnector(CONNECTOR_CLASS, "zvit_delete", "zvit_slot_delete", Map.of());

        insertCustomer(20, "Barbara", "Liskov", "barbara@example.com");
        String topic = "zvit_delete.public.customers";
        waitForRecords(topic, 1, CONNECTOR_TIMEOUT);

        try (Connection connection = openDbConnection()) {
            TestUtils.runSQL(connection, "DELETE FROM customers WHERE id = 20");
        }

        List<ConsumerRecord<String, String>> records = waitForRecords(topic, 2, CONNECTOR_TIMEOUT,
                recs -> recs.stream().map(this::payloadOf).anyMatch(p -> "d".equals(p.path("op").asText())
                        && p.path("before").path("id").asInt() == 20));

        JsonNode delete = records.stream().map(this::payloadOf)
                .filter(p -> "d".equals(p.path("op").asText()))
                .findFirst().orElseThrow();
        assertThat(delete.path("before").path("first_name").asText()).isEqualTo("Barbara");
        assertThat(delete.path("after").isNull()).isTrue();

        // Debezium follows delete events with a tombstone (null value) record.
        assertThat(records.stream().anyMatch(record -> record.value() == null))
                .as("tombstone after delete event").isTrue();

        try (Connection connection = openDbConnection()) {
            TestUtils.assertSQL(connection, "SELECT count(*) FROM customers WHERE id = 20", new String[]{ "0" });
        }
    }

    @Test
    public void shouldValidateZvSqlResourcesViaValidateEndpoint() throws Exception {
        Map<String, String> good = baseConnectorConfig(CONNECTOR_CLASS, "zvit_validate", "zvit_slot_validate",
                Map.of("zv.sql.resources", "zv-sql/health.sql"));
        JsonNode goodResponse = validateConnectorConfig(CONNECTOR_CLASS, good);
        assertThat(configValue(goodResponse, "zv.sql.resources").path("errors").size())
                .as("shipped health.sql resource must validate cleanly").isZero();
        assertThat(goodResponse.path("error_count").asInt())
                .as("whole configuration must validate cleanly").isZero();

        Map<String, String> bad = baseConnectorConfig(CONNECTOR_CLASS, "zvit_validate", "zvit_slot_validate",
                Map.of("zv.sql.resources", "zv-sql/missing.sql"));
        JsonNode badValue = configValue(validateConnectorConfig(CONNECTOR_CLASS, bad), "zv.sql.resources");
        assertThat(badValue.path("errors").size()).isPositive();
        assertThat(badValue.path("errors").toString()).contains("zv-sql/missing.sql");
    }

    @Test
    public void shouldStreamInsertsWithZvConnector() throws Exception {
        createCustomersTable();
        deployConnector(CONNECTOR_CLASS, "zvit_zv", "zvit_slot_zv", Map.of("zv.sql.resources", "zv-sql/health.sql"));

        insertCustomer(31, "Zvi", "Sql", "zvi@example.com");

        List<ConsumerRecord<String, String>> records = waitForRecords("zvit_zv.public.customers", 1, CONNECTOR_TIMEOUT,
                recs -> recs.stream().map(this::payloadOf).anyMatch(p -> "c".equals(p.path("op").asText())
                        && p.path("after").path("id").asInt() == 31));

        JsonNode create = records.stream().map(this::payloadOf)
                .filter(p -> "c".equals(p.path("op").asText()) && p.path("after").path("id").asInt() == 31)
                .findFirst().orElseThrow();
        assertThat(create.path("after").path("first_name").asText()).isEqualTo("Zvi");
        assertThat(create.path("after").path("email").asText()).isEqualTo("zvi@example.com");
    }


    private void deployConnector(String connectorClass, String topicPrefix, String slotName, Map<String, String> extraConfig) {
        connectorName = topicPrefix;
        createConnector(connectorName, baseConnectorConfig(connectorClass, topicPrefix, slotName, extraConfig));
        waitForConnectorRunning(connectorName, CONNECTOR_TIMEOUT);
    }

    private Map<String, String> baseConnectorConfig(String connectorClass, String topicPrefix, String slotName,
                                                    Map<String, String> extraConfig) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", connectorClass);
        config.put("database.hostname", POSTGRES_ALIAS);
        config.put("database.port", "5432");
        config.put("database.user", DB_USER);
        config.put("database.password", DB_PASSWORD);
        config.put("database.dbname", DB_NAME);
        config.put("topic.prefix", topicPrefix);
        config.put("plugin.name", "pgoutput");
        config.put("slot.name", slotName);
        config.put("publication.name", topicPrefix + "_pub");
        config.put("publication.autocreate.mode", "filtered");
        config.put("table.include.list", "public.customers");
        config.put("snapshot.mode", "initial");
        config.put("tombstones.on.delete", "true");
        config.putAll(extraConfig);
        return config;
    }

    /** PUT /connector-plugins/{class}/config/validate - the pre-deployment validation entry point. */
    private JsonNode validateConnectorConfig(String connectorClass, Map<String, String> config) throws Exception {
        Map<String, String> flatConfig = new HashMap<>(config);
        flatConfig.putIfAbsent("name", "zvit_validate");
        String body = MAPPER.writeValueAsString(flatConfig);
        HttpRequest request = HttpRequest.newBuilder(
                URI.create(connectRestUrl() + "/connector-plugins/" + connectorClass + "/config/validate"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = java.net.http.HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new AssertionError("Validate endpoint returned " + response.statusCode()
                    + "\nbody: " + response.body()
                    + "\n--- connect worker log tail ---\n" + tail(kafkaConnect.getLogs()));
        }
        return MAPPER.readTree(response.body());
    }

    private static String tail(String logs) {
        return logs.length() <= 8000 ? logs : logs.substring(logs.length() - 8000);
    }

    private static JsonNode configValue(JsonNode validateResponse, String name) {
        // Kafka 4.x serializes the values as 'configs'; stay lenient about the field name.
        for (String arrayField : new String[]{ "configs", "config_values" }) {
            for (JsonNode entry : validateResponse.path(arrayField)) {
                if (name.equals(entry.path("definition").path("name").asText())) {
                    return entry.path("value");
                }
            }
        }
        throw new AssertionError("No config value '" + name + "' in validate response: " + validateResponse);
    }

    private void createCustomersTable() throws SQLException {
        try (Connection connection = openDbConnection()) {
            TestUtils.runSQL(connection, "DROP TABLE IF EXISTS customers");
            TestUtils.runSQL(connection,
                    "CREATE TABLE customers ("
                            + "id SERIAL PRIMARY KEY, "
                            + "first_name VARCHAR(64) NOT NULL, "
                            + "last_name VARCHAR(64), "
                            + "email VARCHAR(128) NOT NULL UNIQUE)");
            // Full identity so update/delete events carry complete before-images.
            TestUtils.runSQL(connection, "ALTER TABLE customers REPLICA IDENTITY FULL");
        }
    }

    private void insertCustomer(int id, String firstName, String lastName, String email) throws SQLException {
        try (Connection connection = openDbConnection()) {
            TestUtils.runSQL(connection, "INSERT INTO customers (id, first_name, last_name, email) VALUES ("
                    + id + ", '" + firstName + "', '" + lastName + "', '" + email + "')");
        }
    }

    private JsonNode payloadOf(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            return MAPPER.createObjectNode();
        }
        try {
            return MAPPER.readTree(record.value()).path("payload");
        }
        catch (Exception e) {
            throw new IllegalStateException("Cannot parse change event: " + record.value(), e);
        }
    }

    private static long countWithOp(List<ConsumerRecord<String, String>> records, String op) {
        return records.stream()
                .filter(record -> record.value() != null)
                .filter(record -> op.equals(payloadOp(record)))
                .count();
    }

    private static String payloadOp(ConsumerRecord<String, String> record) {
        try {
            return new ObjectMapper().readTree(record.value()).path("payload").path("op").asText();
        }
        catch (Exception e) {
            return "";
        }
    }
}
