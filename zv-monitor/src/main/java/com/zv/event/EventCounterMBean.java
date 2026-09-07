package com.zv.event;

/**
 * MBean contract exposed per (source, patternId) pair under:
 *   com.zv:type=EventCounter,source=<log|metric>,pattern=<patternId>
 *
 * Scraped by the Prometheus JMX exporter (see zv-monitor-jmx.yml) so
 * "how many NPEs in the last hour" is just another Grafana panel next to
 * connector_health_* and kafka_connect_*.
 */
public interface EventCounterMBean {

    String getPatternId();

    String getSource();

    /** Total hits since this MBean was first registered (i.e. since zv-monitor started). */
    long getCount();

    long getLastEpochMillis();

    String getLastContainer();

    /** Last matched message, truncated. Skipped by the exporter (not numeric) but handy in JConsole. */
    String getLastMessage();
}
