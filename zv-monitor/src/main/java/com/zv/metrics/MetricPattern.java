package com.zv.metrics;

import com.zv.event.EventSeverity;

public record MetricPattern(
        String id,
        String description,
        EventSeverity severity,
        String container,
        String promql
) {
}
