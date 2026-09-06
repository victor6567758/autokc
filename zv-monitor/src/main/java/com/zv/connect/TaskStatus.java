package com.zv.connect;

/** Snapshot of a single Kafka Connect task's status. */
public record TaskStatus(int id, String state, String workerId, String trace) {

    public boolean isFailed() {
        return "FAILED".equalsIgnoreCase(state);
    }
}
