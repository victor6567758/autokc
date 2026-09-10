package com.zv.connect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregates the per-key records of the Kafka Connect status topic into whole
 * {@link ConnectorStatus} snapshots, mirroring what the Connect worker itself
 * persists: one record per {@code status-connector-<name>} key, one per
 * {@code status-task-<connector>-<task>} key, tombstones (null values)
 * removing them.
 *
 * <p>Not thread-safe by design: owned exclusively by the single
 * status-consumer thread (or a test thread). Connector- and task-level keys
 * hash to different partitions, so records for the same connector can arrive
 * in any order - a task record may build a connector entry before its
 * connector-level record shows up (connector state stays {@code UNKNOWN}
 * until then, which {@link ConnectorStatus#isUnhealthy()} reports honestly).
 */
class ConnectStatusStore {

    /**
     * One applied record = previous (may be {@code null} if the connector was
     * unknown before) and current snapshot of that connector.
     */
    record Change(ConnectorStatus previous, ConnectorStatus current) {
    }

    private static final class Entry {
        private String connectorState = "UNKNOWN";
        private String workerId;
        private String trace;
        private final TreeMap<Integer, TaskStatus> tasks = new TreeMap<>();
    }

    private final Map<String, Entry> entries = new LinkedHashMap<>();

    Change applyConnectorStatus(String connector, String state, String workerId, String trace) {
        boolean existed = entries.containsKey(connector);
        Entry entry = entries.computeIfAbsent(connector, c -> new Entry());
        ConnectorStatus previous = existed ? snapshot(connector, entry) : null;
        entry.connectorState = state;
        entry.workerId = workerId;
        entry.trace = trace;
        return new Change(previous, snapshot(connector, entry));
    }

    Change applyTaskStatus(String connector, int taskId, String state, String workerId, String trace) {
        boolean existed = entries.containsKey(connector);
        Entry entry = entries.computeIfAbsent(connector, c -> new Entry());
        ConnectorStatus previous = existed ? snapshot(connector, entry) : null;
        entry.tasks.put(taskId, new TaskStatus(taskId, state, workerId, trace));
        return new Change(previous, snapshot(connector, entry));
    }

    /**
     * @return the change around the removal, or {@code null} when the task was
     *         not tracked (idempotent for replayed tombstones)
     */
    Change removeTask(String connector, int taskId) {
        Entry entry = entries.get(connector);
        if (entry == null || !entry.tasks.containsKey(taskId)) {
            return null;
        }
        ConnectorStatus previous = snapshot(connector, entry);
        entry.tasks.remove(taskId);
        return new Change(previous, snapshot(connector, entry));
    }

    /**
     * @return the connector's last snapshot if it was tracked, else {@code null}
     *         (idempotent for replayed tombstones)
     */
    ConnectorStatus removeConnector(String connector) {
        Entry entry = entries.remove(connector);
        return entry != null ? snapshot(connector, entry) : null;
    }

    Map<String, ConnectorStatus> snapshots() {
        Map<String, ConnectorStatus> result = new LinkedHashMap<>();
        entries.forEach((name, entry) -> result.put(name, snapshot(name, entry)));
        return result;
    }

    int connectorCount() {
        return entries.size();
    }

    private static ConnectorStatus snapshot(String connector, Entry entry) {
        return ConnectorStatus.healthy(connector, entry.connectorState, entry.workerId, entry.trace,
                List.copyOf(entry.tasks.values()));
    }
}
