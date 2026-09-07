package com.zv.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZvMonitorConfigTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private static JsonNode yaml(String content) {
        try {
            return YAML.readTree(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void shippedYamlParsesWithLocalDefaults() {
        JsonNode root;
        try (InputStream in = ZvMonitorConfig.class.getResourceAsStream("/zv-monitor.yml")) {
            assertNotNull(in, "zv-monitor.yml must ship on the classpath");
            root = yaml(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ZvMonitorConfig config = ZvMonitorConfig.of(root, Map.of());

        assertEquals("http://localhost:8083", config.connectRestUrl());
        assertTrue(config.connectorNames().isEmpty(), "shipped config auto-discovers connectors");
        assertEquals(5000, config.pollIntervalMs());
        assertTrue(config.logEventsEnabled());
        assertEquals("http://localhost:3100", config.lokiUrl());
        assertEquals(10000, config.logPollIntervalMs());
        assertEquals(30, config.logLookbackSeconds());
    }

    @Test
    void emptyOrMissingYamlFallsBackToCodeDefaults() {
        ZvMonitorConfig config = ZvMonitorConfig.of(yaml("{}"), Map.of());

        assertEquals("http://localhost:8083", config.connectRestUrl());
        assertEquals(5000, config.pollIntervalMs());
        assertTrue(config.logEventsEnabled());
        assertEquals("http://localhost:3100", config.lokiUrl());
    }

    @Test
    void yamlWinsOverCodeDefaults() {
        ZvMonitorConfig config = ZvMonitorConfig.of(yaml("""
                connect:
                  restUrl: http://kafka-connect:8083
                  connectorNames: [inventory-source, customers-sink]
                  pollIntervalMs: 250
                logs:
                  enabled: false
                  lokiUrl: http://loki:3100
                  pollIntervalMs: 1500
                  lookbackSeconds: 90
                """), Map.of());

        assertEquals("http://kafka-connect:8083", config.connectRestUrl());
        assertEquals(List.of("inventory-source", "customers-sink"), config.connectorNames());
        assertEquals(250, config.pollIntervalMs());
        assertFalse(config.logEventsEnabled());
        assertEquals("http://loki:3100", config.lokiUrl());
        assertEquals(1500, config.logPollIntervalMs());
        assertEquals(90, config.logLookbackSeconds());
    }

    @Test
    void envWinsOverYaml() {
        ZvMonitorConfig config = ZvMonitorConfig.of(yaml("""
                connect:
                  restUrl: http://ignored:8083
                  connectorNames: [ignored]
                  pollIntervalMs: 123
                logs:
                  lokiUrl: http://ignored:3100
                """), Map.of(
                "CONNECT_REST_URL", "http://kafka-connect:8083",
                "CONNECTOR_NAMES", "inventory-source, customers-sink",
                "POLL_INTERVAL_MS", "7000",
                "LOKI_URL", "http://loki:3100"));

        assertEquals("http://kafka-connect:8083", config.connectRestUrl());
        assertEquals(List.of("inventory-source", "customers-sink"), config.connectorNames());
        assertEquals(7000, config.pollIntervalMs());
        assertEquals("http://loki:3100", config.lokiUrl());
    }

    @Test
    void connectorNamesAlsoAcceptedAsCommaSeparatedStringInYaml() {
        ZvMonitorConfig config = ZvMonitorConfig.of(yaml("""
                connect:
                  connectorNames: "one,two"
                """), Map.of());

        assertEquals(List.of("one", "two"), config.connectorNames());
    }

    @Test
    void blankUrlOrNonPositiveIntervalFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> ZvMonitorConfig.of(yaml("connect: {restUrl: \"\"}"), Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> ZvMonitorConfig.of(yaml("connect: {pollIntervalMs: 0}"), Map.of()));
    }
}
