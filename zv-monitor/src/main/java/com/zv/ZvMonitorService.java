package com.zv;

import com.zv.config.ZvMonitorConfig;
import com.zv.connect.ConnectClient;
import com.zv.connect.ConnectStatusTopicConsumer;
import com.zv.connect.ConnectorStatusPoller;
import com.zv.event.EventBus;
import com.zv.event.EventCounterRegistry;
import com.zv.event.LoggingEventHandler;
import com.zv.utils.GracefulShutdown;
import com.zv.logs.LogEventPoller;
import com.zv.logs.LogPattern;
import com.zv.logs.LogPatternCatalog;
import com.zv.logs.LokiClient;
import com.zv.metrics.MetricPattern;
import com.zv.metrics.MetricPatternCatalog;
import com.zv.metrics.MetricsEventPoller;
import com.zv.metrics.PrometheusClient;
import com.zv.remediation.LoggingRemediationHandler;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class ZvMonitorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZvMonitorService.class);

    /** How long in-flight polls may finish before being interrupted at shutdown. */
    private static final long SHUTDOWN_GRACE_MS = 10_000;
    /** Extra wait after the interrupt, before giving up on the scheduler. */
    private static final long SHUTDOWN_FORCE_GRACE_MS = 5_000;

    private final ZvMonitorConfig config;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final GracefulShutdown shutdown = new GracefulShutdown();


    public void start() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown::initiate, "zv-monitor-shutdown"));
        LOGGER.info("zv-monitor config: {}", config);

        EventBus eventBus = new EventBus();
        EventCounterRegistry counterRegistry = new EventCounterRegistry();
        eventBus.register(new LoggingEventHandler());
        eventBus.register(counterRegistry);

        shutdown.addStep("stop-pollers",
                () -> stopScheduler(scheduler, SHUTDOWN_GRACE_MS, SHUTDOWN_FORCE_GRACE_MS));

        List<Runnable> mbeanUnregisters = new ArrayList<>();
        if (config.statusTopicEnabled()) {
            ConnectStatusTopicConsumer statusTopicConsumer = new ConnectStatusTopicConsumer(
                    config.statusBootstrapServers(), config.statusTopic(), config.statusGroupId(),
                    new LoggingRemediationHandler(), eventBus);
            shutdown.addStep("stop-status-topic-consumer", statusTopicConsumer::close);
            statusTopicConsumer.start();
            mbeanUnregisters.add(statusTopicConsumer::unregisterMBeans);
        }
        ConnectorStatusPoller connectorStatusPoller = ConnectorStatusPoller.create(
                new ConnectClient(config.connectRestUrl()), config.connectorNames(),
                new LoggingRemediationHandler(), eventBus);
        connectorStatusPoller.start(scheduler, config.pollIntervalMs());
        mbeanUnregisters.add(connectorStatusPoller::unregisterMBeans);

        LokiClient lokiClient = new LokiClient(config.lokiUrl());
        List<LogPattern> patterns = LogPatternCatalog.load(config.logPatternsFile());
        LOGGER.info("Watching {} log pattern(s) from {} via Loki at {}",
                patterns.size(), config.logPatternsFile(), config.lokiUrl());
        LogEventPoller logEventPoller = new LogEventPoller(lokiClient, patterns, eventBus,
                TimeUnit.SECONDS.toNanos(config.logLookbackSeconds()));
        logEventPoller.start(scheduler, config.logPollIntervalMs());

        PrometheusClient prometheusClient = new PrometheusClient(config.prometheusUrl());
        List<MetricPattern> metricPatterns = MetricPatternCatalog.load(config.metricPatternsFile());
        LOGGER.info("Watching {} metric pattern(s) from {} via Prometheus at {}", metricPatterns.size(),
                config.metricPatternsFile(), config.prometheusUrl());
        MetricsEventPoller metricsEventPoller = new MetricsEventPoller(prometheusClient, metricPatterns, eventBus);
        metricsEventPoller.start(scheduler, config.metricPollIntervalMs());

        shutdown.addStep("unregister-jmx-mbeans", () -> {
            mbeanUnregisters.forEach(Runnable::run);
            counterRegistry.unregisterAll();
        });

        String statusSource = (config.statusTopicEnabled()
                ? "status topic '" + config.statusTopic() + "' and "
                : "") + "REST polling every " + config.pollIntervalMs() + "ms against " + config.connectRestUrl();
        LOGGER.info("Kafka Connector Monitor (zv-monitor) started. Status source: {}", statusSource);
    }

    public void await() throws InterruptedException {
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
}
