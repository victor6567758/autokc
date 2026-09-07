package com.zv.logs;

import com.fasterxml.jackson.databind.JsonNode;
import com.zv.event.EventSeverity;
import com.zv.kcmanager.common.util.YamlCatalogReader;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LogPatternCatalog {

    public static List<LogPattern> load(String location) {
        YamlCatalogReader reader = new YamlCatalogReader(location, "log pattern",
                "set logs.patternsFile / LOG_PATTERNS_FILE to the right path");
        JsonNode entries = reader.readArray("patterns");
        List<LogPattern> patterns = new ArrayList<>();
        for (JsonNode entry : entries) {
            String id = reader.requiredText(entry, "id");
            patterns.add(new LogPattern(
                    id,
                    reader.firstText(entry, "description", id),
                    EventSeverity.valueOf(reader.requiredText(entry, "severity").trim().toUpperCase()),
                    reader.requiredText(entry, "containers"),
                    reader.requiredText(entry, "regex")));
        }
        return List.copyOf(patterns);
    }
}
