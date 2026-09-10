package com.zv.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses record values of the Kafka Connect status topic. Ground truth from
 * the live dev-stack topic: plain JSON objects
 * {@code {"state": "...", "trace": null | "<stacktrace>", "worker_id": "...",
 * "generation": N}} - note task values carry <em>no</em> {@code id} field; the
 * task number lives in the record key, not the value. The worker writes these
 * with its internal JSON converter; we read them as plain strings and parse
 * here, keeping the Kafka layer deserializer-free beyond strings.
 */
final class StatusValueParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    record StatusValue(String state, String workerId, String trace) {
    }

    private StatusValueParser() {
    }

    /**
     * @return the parsed value, or {@code null} when the payload is not a
     *         status object with a usable {@code state} (callers log-and-skip);
     *         never throws - a malformed record must not kill the consumer
     */
    static StatusValue parse(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            if (!root.isObject()) {
                return null;
            }
            JsonNode stateNode = root.get("state");
            if (stateNode == null || !stateNode.isTextual() || stateNode.asText().isBlank()) {
                return null;
            }
            return new StatusValue(stateNode.asText(), textualOrNull(root.get("worker_id")),
                    textualOrNull(root.get("trace")));
        } catch (Exception e) {
            return null;
        }
    }

    private static String textualOrNull(JsonNode node) {
        return node != null && node.isTextual() ? node.asText() : null;
    }
}
