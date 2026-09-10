package com.zv.connect;

/**
 * Parses record keys of the Kafka Connect status topic (the worker's
 * {@code status.storage.topic} - {@code connect-status} in this stack).
 *
 * <p>Ground truth from the live dev-stack topic:
 * <ul>
 *   <li>{@code status-connector-<name>} - the connector-level status record</li>
 *   <li>{@code status-task-<connector>-<taskNum>} - one record per task; the
 *       task number is the segment after the <em>last</em> dash, because
 *       connector names themselves contain dashes
 *       ({@code status-task-inventory-source-0})</li>
 *   <li>{@code status-topic-<...>} (connector topic/count statuses) and
 *       anything else is not health-relevant and ignored</li>
 * </ul>
 */
final class StatusKeyParser {

    private static final String CONNECTOR_PREFIX = "status-connector-";
    private static final String TASK_PREFIX = "status-task-";

    enum KeyType { CONNECTOR, TASK }

    /**
     * @param taskId only set for {@link KeyType#TASK}; {@code -1} for connector keys
     */
    record ParsedKey(KeyType type, String connector, int taskId) {
    }

    private StatusKeyParser() {
    }

    /**
     * @return the parsed key, or {@code null} when the key does not address a
     *         connector/task status (callers log-and-skip those)
     */
    static ParsedKey parse(String key) {
        if (key == null) {
            return null;
        }
        if (key.startsWith(CONNECTOR_PREFIX)) {
            String connector = key.substring(CONNECTOR_PREFIX.length());
            return connector.isBlank() ? null : new ParsedKey(KeyType.CONNECTOR, connector, -1);
        }
        if (key.startsWith(TASK_PREFIX)) {
            // task number = segment after the LAST dash (dashed connector names)
            String rest = key.substring(TASK_PREFIX.length());
            int lastDash = rest.lastIndexOf('-');
            if (lastDash <= 0) {
                return null; // no "<connector>-<taskNum>" split possible
            }
            Integer taskId = tryParseInt(rest.substring(lastDash + 1));
            return taskId == null ? null : new ParsedKey(KeyType.TASK, rest.substring(0, lastDash), taskId);
        }
        return null;
    }

    private static Integer tryParseInt(String s) {
        try {
            return Integer.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
