package com.zv.kcmanager.sink.postgresql;

import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zv.kcmanager.common.test.ZvDebeziumITBase;
import com.zv.kcmanager.common.util.TestUtils;

/**
 * End-to-end JDBC sink test: produces Debezium-shaped change events onto
 * Kafka via the shared stack and asserts the rows replicated into the sink
 * Postgres.
 */
public class ITPostgresSink extends ZvDebeziumITBase {

    private static final String CONNECTOR_CLASS = "io.debezium.connector.jdbc.JdbcSinkConnector";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration CONNECTOR_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration DB_TIMEOUT = Duration.ofSeconds(90);

    private String connectorName;

    @AfterEach
    public void removeConnector() {
        if (connectorName != null) {
            deleteConnector(connectorName);
        }
    }

    @Test
    public void shouldApplyInsertEvents() throws Exception {
        String topic = "customers_inserts";
        deploySinkConnector("sink-inserts", topic);

        sendChangeEvent(topic, 1, "c", null, customer(1, "Ada", "Lovelace", "ada@example.com"));
        sendChangeEvent(topic, 2, "c", null, customer(2, "Grace", "Hopper", "grace@example.com"));

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "inserted rows to appear in the sink",
                    () -> TestUtils.querySQL(connection, "SELECT count(*) FROM customers_inserts").get(0)[0].equals("2"));
            TestUtils.assertSQLRows(connection, "SELECT id, first_name, last_name, email FROM customers_inserts ORDER BY id",
                    row(1, "Ada", "Lovelace", "ada@example.com"),
                    row(2, "Grace", "Hopper", "grace@example.com"));
        }
    }

    @Test
    public void shouldApplyUpdateEvents() throws Exception {
        String topic = "customers_updates";
        deploySinkConnector("sink-updates", topic);

        sendChangeEvent(topic, 1, "c", null, customer(1, "Ada", "Lovelace", "ada@example.com"));
        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "created row to appear in the sink",
                    () -> !TestUtils.querySQL(connection, "SELECT id FROM customers_updates WHERE id = 1").isEmpty());
        }

        sendChangeEvent(topic, 1, "u",
                customer(1, "Ada", "Lovelace", "ada@example.com"),
                customer(1, "Ada", "Lovelace", "ada@updated.example.com"));

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "updated email to reach the sink",
                    () -> TestUtils.querySQL(connection, "SELECT count(*) FROM customers_updates WHERE email = 'ada@updated.example.com'")
                            .get(0)[0].equals("1"));
            TestUtils.assertSQLRows(connection, "SELECT id, email FROM customers_updates",
                    new String[]{ "1", "ada@updated.example.com" });
        }
    }

    @Test
    public void shouldApplyDeleteTombstones() throws Exception {
        String topic = "customers_deletes";
        deploySinkConnector("sink-deletes", topic);

        sendChangeEvent(topic, 30, "c", null, customer(30, "Barbara", "Liskov", "barbara@example.com"));
        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "created row to appear in the sink",
                    () -> !TestUtils.querySQL(connection, "SELECT id FROM customers_deletes WHERE id = 30").isEmpty());
        }

        // A real Debezium pipeline emits the delete event followed by a tombstone.
        sendChangeEvent(topic, 30, "d", customer(30, "Barbara", "Liskov", "barbara@example.com"), null);
        sendTombstone(topic, 30);

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "deleted row to disappear from the sink",
                    () -> TestUtils.querySQL(connection, "SELECT count(*) FROM customers_deletes").get(0)[0].equals("0"));
        }
    }

    @Test
    public void shouldAutoCreateTableWithColumns() throws Exception {
        String topic = "autocreate_customers";
        deploySinkConnector("sink-autocreate", topic);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 7);
        row.put("first_name", "Linus");
        row.put("active", Boolean.TRUE);
        row.put("score", 42L);
        sendChangeEvent(topic, 7, "c", null, row,
                Arrays.asList(field("id", "int32", false), field("first_name", "string", false),
                        field("active", "boolean", false), field("score", "int64", true)));

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "auto-created table to appear in the sink",
                    () -> TestUtils.querySQL(connection,
                            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'autocreate_customers'")
                            .get(0)[0].equals("1"));
            TestUtils.assertSQLRows(connection, "SELECT id, first_name, active, score FROM autocreate_customers",
                    new String[]{ "7", "Linus", "t", "42" });
        }
    }

    @Test
    public void shouldUpsertWithoutDuplicates() throws Exception {
        String topic = "customers_upserts";
        deploySinkConnector("sink-upserts", topic);

        // Same primary key twice - upsert mode must keep exactly one row.
        sendChangeEvent(topic, 5, "c", null, customer(5, "One", "First", "one@example.com"));
        sendChangeEvent(topic, 5, "c", customer(5, "One", "First", "one@example.com"),
                customer(5, "Two", "Second", "two@example.com"));

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "upserted row to appear in the sink",
                    () -> TestUtils.querySQL(connection, "SELECT count(*) FROM customers_upserts WHERE first_name = 'Two'")
                            .get(0)[0].equals("1"));
            TestUtils.assertSQLRows(connection, "SELECT id, first_name, email FROM customers_upserts",
                    new String[]{ "5", "Two", "two@example.com" });
        }
    }

    @Test
    public void shouldEvolveSchemaWithNewColumn() throws Exception {
        String topic = "customers_evolved";
        deploySinkConnector("sink-evolved", topic);

        sendChangeEvent(topic, 9, "c", null, customer(9, "Edsger", "Dijkstra", "edsger@example.com"));
        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "created row to appear in the sink",
                    () -> !TestUtils.querySQL(connection, "SELECT id FROM customers_evolved WHERE id = 9").isEmpty());
        }

        Map<String, Object> after = new LinkedHashMap<>();
        after.put("id", 9);
        after.put("first_name", "Edsger");
        after.put("last_name", "Dijkstra");
        after.put("email", "edsger@example.com");
        after.put("nickname", "Ed");
        sendChangeEvent(topic, 9, "u", customer(9, "Edsger", "Dijkstra", "edsger@example.com"), after,
                Arrays.asList(field("id", "int32", false), field("first_name", "string", false),
                        field("last_name", "string", true), field("email", "string", false),
                        field("nickname", "string", true)));

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "evolved column to appear in the sink",
                    () -> TestUtils.querySQL(connection,
                            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'customers_evolved' AND column_name = 'nickname'")
                            .get(0)[0].equals("1"));
            TestUtils.assertSQLRows(connection, "SELECT id, nickname FROM customers_evolved",
                    new String[]{ "9", "Ed" });
        }
    }

    @Test
    public void shouldApplyBatchOfEvents() throws Exception {
        String topic = "customers_batch";
        deploySinkConnector("sink-batch", topic);

        for (int id = 101; id <= 105; id++) {
            sendChangeEvent(topic, id, "c", null, customer(id, "User" + id, "Batch", "user" + id + "@example.com"));
        }

        try (Connection connection = openDbConnection()) {
            TestUtils.await(DB_TIMEOUT, "batched rows to appear in the sink",
                    () -> TestUtils.querySQL(connection, "SELECT count(*) FROM customers_batch").get(0)[0].equals("5"));
            TestUtils.assertSQL(connection, "SELECT count(*) FROM customers_batch", new String[]{ "5" });
        }
    }

    // ------------------------------------------------------------- helpers

    private void deploySinkConnector(String name, String topic) {
        connectorName = name;
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("topics", topic);
        config.put("connection.url", dbNetworkUrl());
        config.put("connection.username", DB_USER);
        config.put("connection.password", DB_PASSWORD);
        config.put("insert.mode", "upsert");
        config.put("delete.enabled", "true");
        config.put("primary.key.mode", "record_key");
        config.put("primary.key.fields", "id");
        config.put("schema.evolution", "basic");

        createConnector(name, config);
        waitForConnectorRunning(name, CONNECTOR_TIMEOUT);
    }

    /** The default customers columns shared by most tests. */
    private static List<JsonNode> columnFields() {
        return Arrays.asList(
                field("id", "int32", false),
                field("first_name", "string", false),
                field("last_name", "string", true),
                field("email", "string", false));
    }

    private static Map<String, Object> customer(int id, String firstName, String lastName, String email) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("first_name", firstName);
        values.put("last_name", lastName);
        values.put("email", email);
        return values;
    }

    private static String[] row(int id, String firstName, String lastName, String email) {
        return new String[]{ String.valueOf(id), firstName, lastName, email };
    }

    private void sendChangeEvent(String topic, int id, String op, Map<String, Object> before, Map<String, Object> after)
            throws Exception {
        sendChangeEvent(topic, id, op, before, after, columnFields());
    }

    /** Sends one Debezium-style change event (struct key + schema-ful envelope). */
    private void sendChangeEvent(String topic, int id, String op, Map<String, Object> before, Map<String, Object> after,
            List<JsonNode> columnFields) throws Exception {
        send(topic, JSON.writeValueAsString(key(id, topic)), JSON.writeValueAsString(envelope(op, before, after, columnFields, topic)));
    }

    private void sendTombstone(String topic, int id) throws Exception {
        send(topic, JSON.writeValueAsString(key(id, topic)), null);
    }

    private static ObjectNode key(int id, String topic) {
        ObjectNode key = JSON.createObjectNode();
        ObjectNode schema = JSON.createObjectNode();
        schema.put("type", "struct");
        schema.put("name", topic + ".Key");
        schema.put("optional", false);
        ArrayNode fields = JSON.createArrayNode();
        fields.add(field("id", "int32", false));
        schema.set("fields", fields);
        key.set("schema", schema);
        ObjectNode payload = JSON.createObjectNode();
        payload.put("id", id);
        key.set("payload", payload);
        return key;
    }

    private static ObjectNode envelope(String op, Map<String, Object> before, Map<String, Object> after,
            List<JsonNode> columnFields, String topic) {
        ObjectNode event = JSON.createObjectNode();

        ObjectNode schema = JSON.createObjectNode();
        schema.put("type", "struct");
        schema.put("name", topic + ".Envelope");
        schema.put("optional", true);
        ArrayNode envelopeFields = JSON.createArrayNode();
        envelopeFields.add(structField("before", topic + ".Value", columnFields, true));
        envelopeFields.add(structField("after", topic + ".Value", columnFields, true));
        envelopeFields.add(structField("source", "io.debezium.connector.postgresql.Source",
                Arrays.asList(field("lsn", "int64", true), field("ts_ms", "int64", false)), false));
        envelopeFields.add(field("op", "string", false));
        envelopeFields.add(field("ts_ms", "int64", false));
        schema.set("fields", envelopeFields);
        event.set("schema", schema);

        ObjectNode payload = JSON.createObjectNode();
        payload.set("before", toNode(before));
        payload.set("after", toNode(after));
        ObjectNode source = JSON.createObjectNode();
        source.put("lsn", 1L);
        source.put("ts_ms", System.currentTimeMillis());
        payload.set("source", source);
        payload.put("op", op);
        payload.put("ts_ms", System.currentTimeMillis());
        event.set("payload", payload);
        return event;
    }

    private static JsonNode field(String name, String type, boolean optional) {
        ObjectNode field = JSON.createObjectNode();
        field.put("field", name);
        field.put("type", type);
        field.put("optional", optional);
        return field;
    }

    private static JsonNode structField(String name, String structName, List<JsonNode> fields, boolean optional) {
        ObjectNode field = JSON.createObjectNode();
        field.put("field", name);
        field.put("type", "struct");
        field.put("name", structName);
        field.put("optional", optional);
        ArrayNode nested = JSON.createArrayNode();
        columnFieldsOr(fields).forEach(nested::add);
        field.set("fields", nested);
        return field;
    }

    private static ArrayNode columnFieldsOr(List<JsonNode> fields) {
        ArrayNode array = JSON.createArrayNode();
        fields.forEach(array::add);
        return array;
    }

    private static JsonNode toNode(Map<String, Object> values) {
        if (values == null) {
            return null;
        }
        ObjectNode node = JSON.createObjectNode();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Integer intValue) {
                node.put(entry.getKey(), intValue);
            }
            else if (value instanceof Long longValue) {
                node.put(entry.getKey(), longValue);
            }
            else if (value instanceof Boolean boolValue) {
                node.put(entry.getKey(), boolValue);
            }
            else {
                node.put(entry.getKey(), String.valueOf(value));
            }
        }
        return node;
    }
}
