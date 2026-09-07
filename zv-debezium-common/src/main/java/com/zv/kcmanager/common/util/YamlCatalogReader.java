package com.zv.kcmanager.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@RequiredArgsConstructor
public final class YamlCatalogReader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final String location;
    private final String entryNoun;
    private final String configHint;

    public JsonNode readArray(String arrayField) {
        Path file = Path.of(location);
        JsonNode root;
        try {
            root = YAML.readTree(Files.newBufferedReader(file));
        } catch (IOException e) {
            throw new IllegalStateException(entryNoun + "s file not readable: " + location
                    + " (" + configHint + ")", e);
        }
        JsonNode entries = root.path(arrayField);
        if (!entries.isArray() || entries.isEmpty()) {
            throw new IllegalStateException("no '" + arrayField + ":' entries in " + location);
        }
        return entries;
    }

    public String requiredText(JsonNode entry, String field) {
        JsonNode node = entry.path(field);
        if (!node.isTextual() || node.asText().isBlank()) {
            throw new IllegalStateException(entryNoun + " '" + entry.path("id").asText("<unnamed>")
                    + "' is missing required text field '" + field + "' in " + location);
        }
        return node.asText();
    }

    public String firstText(JsonNode entry, String field, String fallback) {
        JsonNode node = entry.path(field);
        return node.isTextual() && !node.asText().isBlank() ? node.asText() : fallback;
    }
}
