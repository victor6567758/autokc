package com.zv.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.zv.event.EventSeverity;
import com.zv.kcmanager.common.util.YamlCatalogReader;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MetricPatternCatalog {

    public static List<MetricPattern> load(String location) {
        YamlCatalogReader reader = new YamlCatalogReader(location, "metric pattern",
                "set metrics.patternsFile / METRIC_PATTERNS_FILE to the right path");
        JsonNode entries = reader.readArray("subscribe");
        List<MetricPattern> patterns = new ArrayList<>();
        for (JsonNode entry : entries) {
            String id = reader.requiredText(entry, "id");
            patterns.add(new MetricPattern(
                    id,
                    reader.firstText(entry, "fires", reader.firstText(entry, "description", id)),
                    EventSeverity.valueOf(reader.requiredText(entry, "severity").trim().toUpperCase()),
                    reader.requiredText(entry, "container"),
                    reader.requiredText(entry, "promql")));
        }
        return List.copyOf(patterns);
    }
}
