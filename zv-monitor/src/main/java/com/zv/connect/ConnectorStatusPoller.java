package com.zv.connect;

import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventSeverity;
import com.zv.jmx.ConnectorHealth;
import com.zv.remediation.RemediationHandler;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns every Kafka Connect REST interaction on the metrics path: connector
 * discovery, periodic status polling, the per-connector ConnectorHealth JMX
 * MBeans, remediation hooks and connector-unhealthy/-recovered events.
 *
 * Same shape as the other pollers (LogEventPoller, MetricsEventPoller): build
 * one (via {@link #create}), then {@link #start} it on the shared scheduler.
 * ZvMonitorApp only wires it up.
 */
@RequiredArgsConstructor
public class ConnectorStatusPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorStatusPoller.class);

    private final ConnectClient client;
    private final Map<String, ConnectorHealth> healthByConnector;
    private final RemediationHandler remediationHandler;
    private final EventBus eventBus;

    /**
     * Resolves which connectors to monitor - the configured list if non-empty,
     * otherwise auto-discovered via GET /connectors - and registers a
     * ConnectorHealth MBean per connector.
     */
    public static ConnectorStatusPoller create(ConnectClient client, List<String> configuredConnectorNames,
                                               RemediationHandler remediationHandler, EventBus eventBus)
            throws IOException, InterruptedException {
        List<String> connectorNames = configuredConnectorNames.isEmpty()
                ? client.listConnectors()
                : configuredConnectorNames;
        LOGGER.info("Monitoring connectors: {}", connectorNames);

        Map<String, ConnectorHealth> healthByConnector = new LinkedHashMap<>();
        for (String name : connectorNames) {
            ConnectorHealth health = new ConnectorHealth(name.trim());
            health.register();
            healthByConnector.put(name.trim(), health);
        }
        return new ConnectorStatusPoller(client, healthByConnector, remediationHandler, eventBus);
    }

    public void start(ScheduledExecutorService scheduler, long pollIntervalMs) {
        scheduler.scheduleAtFixedRate(this::pollOnce, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Unregisters the MBeans registered by {@link #create}; safe to repeat. */
    public void unregisterMBeans() {
        healthByConnector.values().forEach(ConnectorHealth::unregister);
    }

    /** Per-connector health, mainly for tests / direct status reads. */
    ConnectorHealth healthFor(String connectorName) {
        return healthByConnector.get(connectorName);
    }

    void pollOnce() {
        for (Map.Entry<String, ConnectorHealth> entry : healthByConnector.entrySet()) {
            String name = entry.getKey();
            ConnectorHealth health = entry.getValue();
            try {
                ConnectorStatus status = client.getConnectorStatus(name);
                boolean wasHealthy = health.getHealthy() == 1;
                health.update(status);
                LOGGER.debug("poll: connector '{}' state={} tasks={} failedTasks={} healthy={}",
                        name, status.connectorState(), status.tasks().size(),
                        status.tasks().stream().filter(TaskStatus::isFailed).count(), health.getHealthy());

                if (status.isUnhealthy()) {
                    remediationHandler.handleUnhealthy(status);
                    eventBus.publish(Event.metric("connector-unhealthy", EventSeverity.CRITICAL, name,
                            describeUnhealthy(status)));
                } else if (!wasHealthy) {
                    remediationHandler.handleRecovered(status);
                    eventBus.publish(Event.metric("connector-recovered", EventSeverity.INFO, name,
                            "connector and all tasks RUNNING"));
                }
            } catch (Exception e) {
                // log the full exception: transport failures (worker container
                // down, DNS gone, ...) often carry a null message - e.getMessage()
                // alone printed just "null" and hid the root cause
                LOGGER.error("Failed to poll status for connector '{}'", name, e);
            }
        }
    }

    private static String describeUnhealthy(ConnectorStatus status) {
        long failedTasks = status.tasks().stream().filter(TaskStatus::isFailed).count();
        String errorMessage = status.errorMessage();
        return "reachability=" + status.reachability() + " state=" + status.connectorState()
                + " failedTasks=" + failedTasks + (errorMessage != null ? " error=" + errorMessage : "");
    }
}
