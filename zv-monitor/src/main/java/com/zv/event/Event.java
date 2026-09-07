package com.zv.event;

import java.time.Instant;

/**
 * A single "something happened" fact, regardless of whether it was noticed
 * via a metric threshold (connector/task status) or a filtered log line.
 * This is the one shape every EventHandler deals with - see EventBus.
 */
public record Event(
        EventSource source,
        String patternId,
        EventSeverity severity,
        String container,
        String message,
        Instant timestamp
) {

    /** A log-origin event, e.g. a matched Loki line. */
    public static Event log(String patternId, EventSeverity severity, String container, String message,
                             Instant timestamp) {
        return new Event(EventSource.LOG, patternId, severity, container, message, timestamp);
    }

    /** A metric-origin event, e.g. a connector/task status transition. Always "now". */
    public static Event metric(String patternId, EventSeverity severity, String container, String message) {
        return new Event(EventSource.METRIC, patternId, severity, container, message, Instant.now());
    }
}
