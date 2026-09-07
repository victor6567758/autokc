package com.zv.event;

public enum EventSource {
    /** Derived from a Kafka Connect REST status poll (connector/task state). */
    METRIC,
    /** Derived from a filtered log line (via Loki). */
    LOG
}
