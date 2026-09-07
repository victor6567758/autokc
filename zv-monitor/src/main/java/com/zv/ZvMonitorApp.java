package com.zv;

import com.zv.config.ZvMonitorConfig;
import com.zv.connect.ConnectClient;
import com.zv.connect.ConnectorStatus;
import com.zv.connect.TaskStatus;
import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventCounterRegistry;
import com.zv.event.EventSeverity;
import com.zv.event.LoggingEventHandler;
import com.zv.jmx.ConnectorHealth;
import com.zv.logs.LogEventPoller;
import com.zv.logs.LogPattern;
import com.zv.logs.LogPatternCatalog;
import com.zv.logs.LokiClient;
import com.zv.metrics.MetricPattern;
import com.zv.metrics.MetricPatternCatalog;
import com.zv.metrics.MetricsEventPoller;
import com.zv.metrics.PrometheusClient;
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

public class ZvMonitorApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvMonitorApp.class);

    public static void main(String[] args) throws Exception {
        ZvMonitorConfig config = ZvMonitorConfig.load();
        LOGGER.info("zv-monitor config: {}", config);

        // -------------------------------------------------------------- events --
        // Single event bus: connector/task-status transitions (metrics path) and
        // filtered log lines (log path) both land here as Events, so any
        // consumer - logging, JMX counters, and eventually an AI remediation
        // module - only has to deal with one shape of thing.
        EventBus eventBus = new EventBus();
        eventBus.register(new LoggingEventHandler());
        eventBus.register(new EventCounterRegistry());

        // ------------------------------------------------------- metrics path --
        ConnectClient client = new ConnectClient(config.connectRestUrl());
        RemediationHandler remediationHandler = new LoggingRemediationHandler();

        List<String> connectorNames = config.connectorNames().isEmpty()
                ? client.listConnectors()
                : config.connectorNames();
        LOGGER.info("Monitoring connectors: {}", connectorNames);

        Map<String, ConnectorHealth> healthByConnector = new HashMap<>();
        for (String name : connectorNames) {
            ConnectorHealth health = new ConnectorHealth(name.trim());
            health.register();
            healthByConnector.put(name.trim(), health);
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(() -> pollOnce(client, healthByConnector, remediationHandler, eventBus),
                0, config.pollIntervalMs(), TimeUnit.MILLISECONDS);

        // ---------------------------------------------------------- logs path --
        if (config.logEventsEnabled()) {
            LokiClient lokiClient = new LokiClient(config.lokiUrl());
            List<LogPattern> patterns = LogPatternCatalog.load(config.logPatternsFile());
            LOGGER.info("Watching {} log pattern(s) from {} via Loki at {}",
                    patterns.size(), config.logPatternsFile(), config.lokiUrl());
            LogEventPoller logEventPoller = new LogEventPoller(lokiClient, patterns, eventBus,
                    TimeUnit.SECONDS.toNanos(config.logLookbackSeconds()));
            logEventPoller.start(scheduler, config.logPollIntervalMs());
        } else {
            LOGGER.info("log events disabled (logs.enabled=false) - skipping Loki-based log event detection");
        }

        if (config.metricEventsEnabled()) {
            PrometheusClient prometheusClient = new PrometheusClient(config.prometheusUrl());
            List<MetricPattern> metricPatterns = MetricPatternCatalog.load(config.metricPatternsFile());
            LOGGER.info("Watching {} metric pattern(s) from {} via Prometheus at {}", metricPatterns.size(),
                    config.metricPatternsFile(), config.prometheusUrl());
            MetricsEventPoller metricsEventPoller = new MetricsEventPoller(prometheusClient, metricPatterns, eventBus);
            metricsEventPoller.start(scheduler, config.metricPollIntervalMs());
        } else {
            LOGGER.info("metric events disabled (metrics.enabled=false) - skipping Prometheus-based metric events");
        }

        LOGGER.info("Kafka Connector Monitor (zv-monitor) started. Polling every {}ms against {}",
                config.pollIntervalMs(), config.connectRestUrl());

        // Keep the JVM alive; JMX is exposed via the platform MBean server / JMX exporter agent.
        Thread.currentThread().join();
    }

    private static void pollOnce(ConnectClient client, Map<String, ConnectorHealth> healthByConnector,
                                  RemediationHandler remediationHandler, EventBus eventBus) {
        for (Map.Entry<String, ConnectorHealth> entry : healthByConnector.entrySet()) {
            String name = entry.getKey();
            ConnectorHealth health = entry.getValue();
            try {
                ConnectorStatus status = client.getConnectorStatus(name);
                boolean wasHealthy = health.getHealthy() == 1;
                health.update(status);

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
                LOGGER.error("Failed to poll status for connector '{}': {}", name, e.getMessage());
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
