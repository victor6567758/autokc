package com.zv.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * zv-monitor's runtime configuration, resolved in three layers:
 * code defaults &lt; classpath zv-monitor.yml &lt; environment variables.
 * The env layer keeps container wiring (docker-compose service DNS names,
 * intervals) working without rebuilding the jar.
 *
 * <p>Catalog file paths ({@code logs.patternsFile}, {@code metrics.patternsFile})
 * are additionally resolved against the working directory with a
 * {@code zv-monitor/} fallback, so the shipped relative defaults work for the
 * container (CWD {@code /app}), module-dir runs and repo-root local runs
 * ({@code make up-dev}) alike.
 */
public record ZvMonitorConfig(
        String connectRestUrl,
        List<String> connectorNames,
        long pollIntervalMs,
        boolean logEventsEnabled,
        String lokiUrl,
        long logPollIntervalMs,
        long logLookbackSeconds,
        String logPatternsFile,
        boolean metricEventsEnabled,
        String prometheusUrl,
        long metricPollIntervalMs,
        String metricPatternsFile) {

    private static final String RESOURCE = "/zv-monitor.yml";

    public static ZvMonitorConfig load() {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        JsonNode root;
        try (InputStream in = ZvMonitorConfig.class.getResourceAsStream(RESOURCE)) {
            JsonNode parsed = in == null ? null : yaml.readTree(in);
            root = parsed == null ? yaml.createObjectNode() : parsed;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + RESOURCE, e);
        }
        return of(root, System.getenv());
    }

    static ZvMonitorConfig of(JsonNode root, Map<String, String> env) {
        return new ZvMonitorConfig(
                envText(env, "CONNECT_REST_URL", text(root.at("/connect/restUrl"), "http://localhost:8083")),
                envNames(env, root),
                envLong(env, "POLL_INTERVAL_MS", longValue(root.at("/connect/pollIntervalMs"), 5000)),
                envBool(env, "LOG_EVENTS_ENABLED", boolValue(root.at("/logs/enabled"), true)),
                envText(env, "LOKI_URL", text(root.at("/logs/lokiUrl"), "http://localhost:3100")),
                envLong(env, "LOG_POLL_INTERVAL_MS", longValue(root.at("/logs/pollIntervalMs"), 10000)),
                envLong(env, "LOG_LOOKBACK_SECONDS", longValue(root.at("/logs/lookbackSeconds"), 30)),
                resolveCatalogPath(envText(env, "LOG_PATTERNS_FILE",
                        text(root.at("/logs/patternsFile"), "analysis/loki-log-patterns.yaml"))),
                envBool(env, "METRICS_EVENTS_ENABLED", boolValue(root.at("/metrics/enabled"), true)),
                envText(env, "PROMETHEUS_URL", text(root.at("/metrics/prometheusUrl"), "http://localhost:9090")),
                envLong(env, "METRICS_POLL_INTERVAL_MS", longValue(root.at("/metrics/pollIntervalMs"), 15000)),
                resolveCatalogPath(envText(env, "METRIC_PATTERNS_FILE",
                        text(root.at("/metrics/patternsFile"), "analysis/prometheus-metrics.yaml"))));
    }

    public ZvMonitorConfig {
        if (connectRestUrl == null || connectRestUrl.isBlank()) {
            throw new IllegalArgumentException("connect.restUrl / CONNECT_REST_URL must not be blank");
        }
        if (lokiUrl == null || lokiUrl.isBlank()) {
            throw new IllegalArgumentException("logs.lokiUrl / LOKI_URL must not be blank");
        }
        if (prometheusUrl == null || prometheusUrl.isBlank()) {
            throw new IllegalArgumentException("metrics.prometheusUrl / PROMETHEUS_URL must not be blank");
        }
        if (logPatternsFile == null || logPatternsFile.isBlank()) {
            throw new IllegalArgumentException("logs.patternsFile / LOG_PATTERNS_FILE must not be blank");
        }
        if (metricPatternsFile == null || metricPatternsFile.isBlank()) {
            throw new IllegalArgumentException("metrics.patternsFile / METRIC_PATTERNS_FILE must not be blank");
        }
        if (pollIntervalMs <= 0 || logPollIntervalMs <= 0 || metricPollIntervalMs <= 0 || logLookbackSeconds < 0) {
            throw new IllegalArgumentException(
                    "poll intervals must be > 0 and lookbackSeconds >= 0, got: " + pollIntervalMs
                            + "/" + logPollIntervalMs + "/" + metricPollIntervalMs + "/" + logLookbackSeconds);
        }
        connectorNames = connectorNames == null ? List.of() : List.copyOf(connectorNames);
    }

    /**
     * Resolves a catalog file location against the current working directory.
     * The configured value wins when it exists (container: CWD {@code /app};
     * module-dir runs); otherwise a {@code zv-monitor/}-prefixed copy is tried so
     * the shipped defaults also work when the jar is launched from the repo root
     * ({@code make up-dev} prints exactly such a command). When nothing matches,
     * the configured value is returned unchanged so the startup error still
     * names the path the user configured.
     */
    static String resolveCatalogPath(String configured) {
        return resolveCatalogPath(configured, Path.of(""));
    }

    static String resolveCatalogPath(String configured, Path workingDir) {
        if (configured == null || configured.isBlank()) {
            return configured;
        }
        Path asConfigured = workingDir.resolve(configured).normalize();
        if (Files.isRegularFile(asConfigured)) {
            return configured;
        }
        if (!Path.of(configured).isAbsolute()) {
            Path fromRepoRoot = workingDir.resolve(Path.of("zv-monitor", configured)).normalize();
            if (Files.isRegularFile(fromRepoRoot)) {
                return fromRepoRoot.toString();
            }
        }
        return configured;
    }

    private static String text(JsonNode node, String fallback) {
        return node.isTextual() ? node.asText() : fallback;
    }

    private static long longValue(JsonNode node, long fallback) {
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            return Long.parseLong(node.asText().trim());
        }
        return fallback;
    }

    private static boolean boolValue(JsonNode node, boolean fallback) {
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isTextual()) {
            return Boolean.parseBoolean(node.asText());
        }
        return fallback;
    }

    private static List<String> envNames(Map<String, String> env, JsonNode root) {
        String fromEnv = env.get("CONNECTOR_NAMES");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return splitNames(fromEnv);
        }
        JsonNode node = root.at("/connect/connectorNames");
        if (node.isArray()) {
            List<String> names = new ArrayList<>();
            node.forEach(n -> names.add(n.asText().trim()));
            names.removeIf(String::isBlank);
            return names;
        }
        return node.isTextual() ? splitNames(node.asText()) : List.of();
    }

    private static List<String> splitNames(String commaSeparated) {
        List<String> names = new ArrayList<>();
        for (String part : commaSeparated.split(",")) {
            if (!part.isBlank()) {
                names.add(part.trim());
            }
        }
        return names;
    }

    private static String envText(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static long envLong(Map<String, String> env, String key, long fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }

    private static boolean envBool(Map<String, String> env, String key, boolean fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : Boolean.parseBoolean(value);
    }
}
