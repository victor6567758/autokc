package com.zv.kcmanager.common.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the metric name scheme and the counter/gauge/histogram
 * accessors of the default {@link MetricsManager} singleton. Each test uses
 * its own metric names (the singleton is shared, clearAll() runs after each).
 */
class MetricsManagerTest {

    private final MetricsManager metrics = MetricsManager.instance();

    @AfterEach
    void clearMetrics() {
        metrics.clearAll();
    }

    @Test
    void shouldIncrementAndReadCounters() {
        metrics.incrementCounter("orders-source", "records-sent");
        metrics.incrementCounter("orders-source", "records-sent");
        metrics.incrementCounter("postgres", "orders-source", "n/a", -1, "n/a", "records-sent", 5);

        assertThat(metrics.getCounter("orders-source", "records-sent")).isEqualTo(2);
        assertThat(metrics.getCounter("postgres", "orders-source", "n/a", -1, "n/a", "records-sent")).isEqualTo(5);
    }

    @Test
    void shouldSetAndReadGauges() {
        metrics.setGauge("jdbc", "orders-sink", "n/a", 0, "n/a", "queue-depth", 42);
        assertThat(metrics.getGauge("jdbc", "orders-sink", "n/a", 0, "n/a", "queue-depth")).isEqualTo(42);

        metrics.setGauge("jdbc", "orders-sink", "n/a", 0, "n/a", "queue-depth", 7);
        assertThat(metrics.getGauge("jdbc", "orders-sink", "n/a", 0, "n/a", "queue-depth")).isEqualTo(7);
    }

    @Test
    void shouldRejectUnknownGaugeReads() {
        assertThatThrownBy(() -> metrics.getGauge("jdbc", "nope", "n/a", 0, "n/a", "missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Gauge does not exist");
    }

    @Test
    void shouldTrackHistogramCounts() {
        metrics.updateHistogram("jdbc", "orders-sink", "writes", 1, "p0", "batch-size", 10);
        metrics.updateHistogram("jdbc", "orders-sink", "writes", 1, "p0", "batch-size", 20);

        assertThat(metrics.getHistogram("jdbc", "orders-sink", "writes", 1, "p0", "batch-size")).isEqualTo(2);
    }

    @Test
    void clearAllShouldAllowReregisteringGauges() {
        metrics.setGauge("jdbc", "clear-sink", "n/a", 0, "n/a", "lag", 100);
        metrics.clearAll();

        metrics.setGauge("jdbc", "clear-sink", "n/a", 0, "n/a", "lag", 5);
        assertThat(metrics.getGauge("jdbc", "clear-sink", "n/a", 0, "n/a", "lag")).isEqualTo(5);
    }
}
