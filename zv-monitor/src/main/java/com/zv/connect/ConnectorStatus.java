package com.zv.connect;

import java.util.List;

/** Snapshot of a connector's status as reported by the Kafka Connect REST API. */
public class ConnectorStatus {

    public enum Reachability { OK, NOT_FOUND, UNREACHABLE }

    private final String connectorName;
    private final Reachability reachability;
    private final String connectorState;
    private final String workerId;
    private final String trace;
    private final List<TaskStatus> tasks;
    private final String errorMessage;

    private ConnectorStatus(String connectorName, Reachability reachability, String connectorState,
                             String workerId, String trace, List<TaskStatus> tasks, String errorMessage) {
        this.connectorName = connectorName;
        this.reachability = reachability;
        this.connectorState = connectorState;
        this.workerId = workerId;
        this.trace = trace;
        this.tasks = tasks == null ? List.of() : tasks;
        this.errorMessage = errorMessage;
    }

    public static ConnectorStatus healthy(String name, String state, String workerId, String trace, List<TaskStatus> tasks) {
        return new ConnectorStatus(name, Reachability.OK, state, workerId, trace, tasks, null);
    }

    public static ConnectorStatus notFound(String name) {
        return new ConnectorStatus(name, Reachability.NOT_FOUND, "UNKNOWN", null, null, List.of(),
                "Connector not registered with Kafka Connect");
    }

    public static ConnectorStatus unreachable(String name, String errorMessage) {
        return new ConnectorStatus(name, Reachability.UNREACHABLE, "UNKNOWN", null, null, List.of(), errorMessage);
    }

    public String connectorName() { return connectorName; }
    public Reachability reachability() { return reachability; }
    public String connectorState() { return connectorState; }
    public String workerId() { return workerId; }
    public String trace() { return trace; }
    public List<TaskStatus> tasks() { return tasks; }
    public String errorMessage() { return errorMessage; }

    public boolean isConnectorFailed() {
        return "FAILED".equalsIgnoreCase(connectorState);
    }

    public boolean anyTaskFailed() {
        return tasks.stream().anyMatch(TaskStatus::isFailed);
    }

    /** True if either the connector itself or any of its tasks are in a FAILED state. */
    public boolean isUnhealthy() {
        return reachability != Reachability.OK || isConnectorFailed() || anyTaskFailed();
    }
}
