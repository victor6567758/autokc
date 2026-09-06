package com.zv;

import com.zv.connect.ConnectClient;
import com.zv.connect.ConnectorStatus;
import com.zv.jmx.ConnectorHealth;
import com.zv.remediation.LoggingRemediationHandler;
import com.zv.remediation.RemediationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls the Kafka Connect REST API on a fixed interval for a configured set of
 * connectors, publishes their health as JMX metrics, and invokes a
 * RemediationHandler whenever a connector goes unhealthy or recovers.
 *
 * Config via environment variables:
 *   CONNECT_REST_URL     default: http://localhost:8083
 *   CONNECTOR_NAMES      comma-separated, e.g. "inventory-source,jdbc-sink"
 *                        if unset, auto-discovers via GET /connectors on startup
 *   POLL_INTERVAL_MS     default: 5000
 */
public class ZvMonitorApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvMonitorApp.class);

    public static void main(String[] args) throws Exception {
        String connectUrl = env("CONNECT_REST_URL", "http://localhost:8083");
        long pollIntervalMs = Long.parseLong(env("POLL_INTERVAL_MS", "5000"));
        String connectorNamesEnv = System.getenv("CONNECTOR_NAMES");

        ConnectClient client = new ConnectClient(connectUrl);
        RemediationHandler remediationHandler = new LoggingRemediationHandler();

        List<String> connectorNames;
        if (connectorNamesEnv == null || connectorNamesEnv.isBlank()) {
            LOGGER.info("CONNECTOR_NAMES not set, auto-discovering from {}", connectUrl);
            connectorNames = client.listConnectors();
        } else {
            connectorNames = List.of(connectorNamesEnv.split(","));
        }
        LOGGER.info("Monitoring connectors: {}", connectorNames);

        Map<String, ConnectorHealth> healthByConnector = new HashMap<>();
        for (String name : connectorNames) {
            ConnectorHealth health = new ConnectorHealth(name.trim());
            health.register();
            healthByConnector.put(name.trim(), health);
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> pollOnce(client, healthByConnector, remediationHandler),
                0, pollIntervalMs, TimeUnit.MILLISECONDS);

        LOGGER.info("Kafka Connector Monitor (zv-monitor) started. Polling every {}ms against {}", pollIntervalMs, connectUrl);

        // Keep the JVM alive; JMX is exposed via the platform MBean server / JMX exporter agent.
        Thread.currentThread().join();
    }

    private static void pollOnce(ConnectClient client, Map<String, ConnectorHealth> healthByConnector,
                                  RemediationHandler remediationHandler) {
        for (Map.Entry<String, ConnectorHealth> entry : healthByConnector.entrySet()) {
            String name = entry.getKey();
            ConnectorHealth health = entry.getValue();
            try {
                ConnectorStatus status = client.getConnectorStatus(name);
                boolean wasHealthy = health.getHealthy() == 1;
                health.update(status);

                if (status.isUnhealthy()) {
                    remediationHandler.handleUnhealthy(status);
                } else if (!wasHealthy) {
                    remediationHandler.handleRecovered(status);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to poll status for connector '{}': {}", name, e.getMessage());
            }
        }
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
