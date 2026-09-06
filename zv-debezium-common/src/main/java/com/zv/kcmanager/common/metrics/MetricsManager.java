package com.zv.kcmanager.common.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Slf4jReporter.LoggingLevel;

/**
 * Codahale (Dropwizard 3.x) metrics facade shared by the zv connectors.
 * Counters, histograms and gauges are keyed by
 * {@code connectorType.connectorName.context.task.taskPartition.metricName}
 * and reported both to SLF4J (DEBUG every 5s, INFO every 60s) and JMX. Use
 * the process-wide {@link #instance()} singleton, or instantiate your own.
 */
public class MetricsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);
    final MetricRegistry registry = new MetricRegistry();
    private final Slf4jReporter slf4jReporterDEBUG;
    private final Slf4jReporter slf4jReporterINFO;
    private final JmxReporter jmxReporter;
    private static final MetricsManager defaultMetricsManager = new MetricsManager();
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    static {
        LOGGER.info("Starting defaultMetricsManager {}", defaultMetricsManager);
        defaultMetricsManager.start();
    }

    public MetricsManager() {
        slf4jReporterDEBUG = Slf4jReporter.forRegistry(registry)
                                         .outputTo(LoggerFactory.getLogger(MetricsManager.class))
                                         .convertRatesTo(TimeUnit.SECONDS)
                                         .convertDurationsTo(TimeUnit.MILLISECONDS)
                                         .withLoggingLevel(LoggingLevel.DEBUG)
                                         .build();
        slf4jReporterINFO = Slf4jReporter.forRegistry(registry)
                                         .outputTo(LoggerFactory.getLogger(MetricsManager.class))
                                         .convertRatesTo(TimeUnit.SECONDS)
                                         .convertDurationsTo(TimeUnit.MILLISECONDS)
                                         .withLoggingLevel(LoggingLevel.INFO)
                                         .build();
        jmxReporter = JmxReporter.forRegistry(registry).build();
    }

    protected void start() {
        LOGGER.info("Starting metrics manager {}", this);
        slf4jReporterDEBUG.start(5, TimeUnit.SECONDS);
        slf4jReporterINFO.start(1, TimeUnit.MINUTES);
        jmxReporter.start();
    }

    protected void stop() {
        slf4jReporterDEBUG.stop();
        slf4jReporterINFO.stop();
        jmxReporter.stop();
    }

    private String metricName(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName) {
        return connectorType + "." + connectorName + "." + context + "." + task + "." + taskPartition + "." + metricName;
    }

    public String fullMetricName(String connectorName, String metricName){
        return this.metricName("n/a", connectorName, "n/a", -1, "n/a", metricName);
    }

    public void setGauge(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName, long value) {
        String gaugeName = metricName(connectorType, connectorName, context, task, taskPartition, metricName);
        gauges.computeIfAbsent(gaugeName, name -> {
            registry.gauge(name, () -> (Gauge<Long>) () -> gauges.get(name).get());
            return new AtomicLong(0L);
        }).set(value);
    }

    public void updateHistogram(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName, long n) {
        registry.histogram(metricName(connectorType, connectorName, context, task, taskPartition, metricName)).update(n);
    }

    public void incrementCounter(String connectorName, String metricName) {
        this.incrementCounter("n/a", connectorName, "n/a", -1, "n/a", metricName, 1);
    }

    public void incrementCounter(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName) {
        this.incrementCounter(connectorType, connectorName, context, task, taskPartition, metricName, 1);
    }

    public void incrementCounter(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName, long n) {
        registry.counter(metricName(connectorType, connectorName, context, task, taskPartition, metricName)).inc(n);
    }

    public long getCounter(String connectorName, String name) {
        return getCounter("n/a", connectorName, "n/a", -1, "n/a", name);
    }

    public long getCounter(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName) {
        return registry.counter(metricName(connectorType, connectorName, context, task, taskPartition, metricName)).getCount();
    }

    public long getGauge(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName) {
        String gaugeName = metricName(connectorType, connectorName, context, task, taskPartition, metricName);
        if (!gauges.containsKey(gaugeName)) {
            throw new IllegalArgumentException("Gauge does not exist: " + gaugeName);
        }
        Gauge<?> gauge = registry.getGauges().get(gaugeName);
        return (Long) gauge.getValue();
    }

    public long getHistogram(String connectorType, String connectorName, String context, int task, String taskPartition, String metricName) {
        return registry.histogram(metricName(connectorType, connectorName, context, task, taskPartition, metricName)).getCount();
    }

    public void clearAll() {
        registry.removeMatching((name, metric) -> true);
        // Keep the gauge backing map in sync: a stale entry would stop
        // setGauge() from re-registering the gauge after a clearAll().
        gauges.clear();
    }

    public static MetricsManager instance() {
        return defaultMetricsManager;
    }
}
