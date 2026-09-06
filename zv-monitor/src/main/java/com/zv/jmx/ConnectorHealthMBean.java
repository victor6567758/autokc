package com.zv.jmx;

/**
 * MBean contract exposed per-connector under:
 *   com.zv:type=ConnectorHealth,name=<connectorName>
 *
 * Scraped via the Prometheus JMX exporter javaagent (see development/monitoring/jmx-exporter/)
 * so these show up in Grafana next to Kafka Connect's own JMX metrics.
 */
public interface ConnectorHealthMBean {

    String getConnectorName();

    /** RUNNING, FAILED, PAUSED, UNASSIGNED, UNKNOWN, NOT_FOUND, UNREACHABLE */
    String getConnectorState();

    /** 1 if connector + all tasks are RUNNING, 0 otherwise. Easiest single number to alert on. */
    int getHealthy();

    /** Number of tasks currently in FAILED state. */
    int getFailedTaskCount();

    /** Total number of tasks last observed. */
    int getTotalTaskCount();

    /** Consecutive unhealthy polls in a row - resets to 0 as soon as it's healthy again. */
    int getConsecutiveFailures();

    /** Epoch millis of the last successful poll against the Connect REST API. */
    long getLastCheckedEpochMillis();

    /** Last known error/trace text, if any (truncated). Empty string when healthy. */
    String getLastErrorMessage();
}
