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
import com.zv.lifecycle.GracefulShutdown;
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

    /** How long in-flight polls may finish before being interrupted at shutdown. */
    private static final long SHUTDOWN_GRACE_MS = 10_000;
    /** Extra wait after the interrupt, before giving up on the scheduler. */
    private static final long SHUTDOWN_FORCE_GRACE_MS = 5_000;

    public static void main(String[] args) throws Exception {
        ZvMonitorConfig config = ZvMonitorConfig.load();
        LOGGER.info("zv-monitor config: {}", config);

        // ---------------------------------------------------------- shutdown --
        // GracefulShutdown parks main() and runs the cleanup steps below on
        // SIGTERM (docker stop) / SIGINT (Ctrl-C) via the JVM shutdown hook -
        // or when anything calls initiate() programmatically later (admin
        // endpoint, self-check). Steps run in registration order: pollers
        // stop before the MBeans they feed are unregistered.
        GracefulShutdown shutdown = new GracefulShutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown::initiate, "zv-monitor-shutdown"));

        // -------------------------------------------------------------- events --
        // Single event bus: connector/task-status transitions (metrics path) and
        // filtered log lines (log path) both land here as Events, so any
        // consumer - logging, JMX counters, and eventually an AI remediation
        // module - only has to deal with one shape of thing.
        EventBus eventBus = new EventBus();
        EventCounterRegistry counterRegistry = new EventCounterRegistry();
        eventBus.register(new LoggingEventHandler());
        eventBus.register(counterRegistry);

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
        shutdown.addStep("stop-pollers",
                () -> stopScheduler(scheduler, SHUTDOWN_GRACE_MS, SHUTDOWN_FORCE_GRACE_MS));
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

        shutdown.addStep("unregister-jmx-mbeans", () -> {
            healthByConnector.values().forEach(ConnectorHealth::unregister);
            counterRegistry.unregisterAll();
        });

        LOGGER.info("Kafka Connector Monitor (zv-monitor) started. Polling every {}ms against {}",
                config.pollIntervalMs(), config.connectRestUrl());

        // Park until the shutdown hook (or a programmatic initiate()) ran the
        // steps above; the JVM then exits normally once main returns.
        shutdown.await();
        LOGGER.info("zv-monitor stopped");
    }

    /**
     * Drains the shared poller scheduler: in-flight ticks (each an interruptible
     * HttpClient call with 5-10s timeouts) get {@code graceMs} to finish, then
     * {@code shutdownNow()} interrupts them and waits up to {@code forceGraceMs}
     * more before giving up.
     */
    static void stopScheduler(ScheduledExecutorService scheduler, long graceMs, long forceGraceMs) {
        scheduler.shutdown();
        try {
            if (scheduler.awaitTermination(graceMs, TimeUnit.MILLISECONDS)) {
                return;
            }
            LOGGER.warn("Polls did not finish within {} ms - interrupting", graceMs);
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(forceGraceMs, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Scheduler did not terminate after interrupt; giving up");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
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
