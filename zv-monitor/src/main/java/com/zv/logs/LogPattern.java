package com.zv.logs;

import com.zv.event.EventSeverity;

/**
 * One line-matching rule turned into a LogQL query against Loki.
 *
 * @param id                     short stable id, becomes the EventCounter's "pattern" JMX/Prometheus label
 * @param description            human-readable, for logs/docs only
 * @param severity               mapped onto the resulting Event
 * @param containerSelectorRegex regex matched against Promtail's "service" label
 *                                (docker-compose service name, e.g. "kafka-connect")
 * @param lineMatchRegex         case-sensitivity is up to the regex itself; use "(?i)" for
 *                                case-insensitive matches
 */
public record LogPattern(
        String id,
        String description,
        EventSeverity severity,
        String containerSelectorRegex,
        String lineMatchRegex
) {

    /** Builds the LogQL query for this pattern. Backticks avoid escaping the regex's own quotes. */
    public String toLogQl() {
        return "{service=~\"" + containerSelectorRegex + "\"} |~ `" + lineMatchRegex + "`";
    }
}
