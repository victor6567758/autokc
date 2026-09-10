package com.zv.connect;

import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventSeverity;
import com.zv.jmx.ConnectorHealth;
import com.zv.remediation.RemediationHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Consumes the Connect worker's status topic, keeping JMX MBeans,
 * remediation hooks and health events in sync. Runs alongside the REST
 * {@link ConnectorStatusPoller} feed.
 *
 * <p>Semantics of the topic (ground truth from the live dev stack): keys are
 * plain strings {@code status-connector-<name>} /
 * {@code status-task-<connector>-<taskNum>}; values are plain-string JSON
 * {@code {"state","trace","worker_id","generation"}} (task values have no id
 * field); a null value is a tombstone meaning the connector/task was removed.
 * {@code status-topic-*} keys are ignored.
 *
 * <p>On startup the topic is replayed from the very beginning
 * ({@code auto.offset.reset=earliest}, offsets never committed): replaying
 * the compacted topic IS the state snapshot, so there is no REST bootstrap.
 * During that initial replay the ConnectorHealth MBeans are already updated
 * (the dashboard works from second one), but events and remediation are
 * suppressed - historical failures must not page anyone. Records consumed
 * after the replay catches up with the head are live and fire edge-triggered
 * connector-unhealthy/-recovered events, with the same patternIds/severities
 * the REST poller emits so downstream consumers cannot tell the sources apart.
 */
public class ConnectStatusTopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectStatusTopicConsumer.class);

    /** Poll cadence of the consume loop; also bounds shutdown latency (wakeup + one poll). */
    private static final long POLL_TIMEOUT_MS = 500;

    /** Backoff after a failed poll (broker down, rebalance in progress, ...) - the loop never exits on errors. */
    private static final long POLL_ERROR_BACKOFF_MS = 1_000;

    /** How long close() waits for the consume thread to end (one poll cycle usually suffices). */
    private static final long CLOSE_JOIN_TIMEOUT_MS = 3_000;

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final RemediationHandler remediationHandler;
    private final EventBus eventBus;

    /** Guarded by: the consume thread, or a test thread calling handleRecord directly. */
    private final ConnectStatusStore store = new ConnectStatusStore();
    private final Map<String, ConnectorHealth> healthByConnector = new LinkedHashMap<>();
    private long recordsApplied;

    private final AtomicBoolean running = new AtomicBoolean(false);

    // created by start(), stopped by close(); visibility via the synchronized
    // start/close pair
    private KafkaConsumer<String, String> consumer;
    private Thread thread;

    /** False until the initial replay of the compacted topic caught up with the head. */
    private volatile boolean caughtUp;

    public ConnectStatusTopicConsumer(String bootstrapServers, String topic, String groupId,
                                      RemediationHandler remediationHandler, EventBus eventBus) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.remediationHandler = remediationHandler;
        this.eventBus = eventBus;
    }

    /** Starts the (daemon) consume thread. Safe to call once; no-op afterwards. */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.consumer = new KafkaConsumer<>(consumerProps(), new StringDeserializer(), new StringDeserializer());
        this.thread = new Thread(this::runLoop, "connect-status-consumer");
        thread.setDaemon(true);
        thread.start();
        LOGGER.info("consuming Kafka Connect status topic '{}' on {} (group {}) - primary status source",
                topic, bootstrapServers, groupId);
    }

    /**
     * Stops the consume thread and closes the underlying Kafka consumer.
     * Idempotent; registered as the stop-status-topic-consumer shutdown step.
     */
    public synchronized void close() {
        running.set(false);
        KafkaConsumer<String, String> toClose = consumer;
        if (toClose != null) {
            toClose.wakeup(); // interrupts an in-flight poll()
        }
        Thread t = thread;
        if (t != null) {
            try {
                t.join(CLOSE_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (toClose != null) {
            try {
                toClose.close();
            } catch (Exception e) {
                LOGGER.warn("closing the status topic consumer failed: {}", e.getMessage());
            }
        }
        consumer = null;
        thread = null;
    }

    /** Unregisters every ConnectorHealth MBean this consumer registered; safe to repeat. */
    public void unregisterMBeans() {
        // copy: this runs on the shutdown thread, the (by then stopped) consume
        // thread is the only other toucher of the map
        for (ConnectorHealth health : List.copyOf(healthByConnector.values())) {
            try {
                health.unregister();
            } catch (RuntimeException e) {
                LOGGER.warn("unregistering ConnectorHealth MBean failed: {}", e.getMessage());
            }
        }
        healthByConnector.clear();
    }

    /** Connector count currently tracked (seen on the topic, not yet tombstoned). */
    public int trackedConnectorCount() {
        return store.connectorCount();
    }

    /** Per-connector health, mainly for tests / direct status reads. */
    ConnectorHealth healthFor(String connectorName) {
        return healthByConnector.get(connectorName);
    }

    // ------------------------------------------------------------------ loop

    private void runLoop() {
        // local reference: close() nulls the field only after this thread ended,
        // but a join timeout must not NPE the finally block below
        KafkaConsumer<String, String> c = consumer;
        Map<TopicPartition, Long> replayEndOffsets = null;
        try {
            c.subscribe(List.of(topic));
            while (running.get()) {
                ConsumerRecords<String, String> batch;
                try {
                    batch = c.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                } catch (WakeupException we) {
                    break; // close() woke us up
                } catch (Exception e) {
                    LOGGER.error("status topic poll failed: {} - retrying in {}ms",
                            e.getMessage(), POLL_ERROR_BACKOFF_MS);
                    sleep(POLL_ERROR_BACKOFF_MS);
                    continue;
                }
                boolean live = caughtUp;
                for (ConsumerRecord<String, String> record : batch) {
                    handleRecord(record.key(), record.value(), live);
                }
                if (!caughtUp) {
                    replayEndOffsets = checkCaughtUp(c, replayEndOffsets);
                }
            }
        } finally {
            try {
                c.unsubscribe();
            } catch (Exception ignored) {
                // consumer already closed / never assigned - nothing to do
            }
        }
    }

    /**
     * Flips {@link #caughtUp} once every assigned partition's position reached
     * the end offset captured when the assignment first appeared.
     */
    private Map<TopicPartition, Long> checkCaughtUp(KafkaConsumer<String, String> c,
                                                    Map<TopicPartition, Long> endOffsets) {
        Set<TopicPartition> assignment = c.assignment();
        if (assignment.isEmpty()) {
            return endOffsets;
        }
        if (endOffsets == null) {
            endOffsets = new HashMap<>(c.endOffsets(assignment));
        }
        for (TopicPartition partition : assignment) {
            if (c.position(partition) < endOffsets.getOrDefault(partition, 0L)) {
                return endOffsets; // still replaying
            }
        }
        caughtUp = true;
        Map<String, ConnectorStatus> snapshots = store.snapshots();
        String summary = snapshots.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().connectorState()
                        + "(" + e.getValue().tasks().size() + " task(s))")
                .collect(Collectors.joining(", "));
        LOGGER.info("connect-status topic replay finished after {} record(s) - tracking {} connector(s){}",
                recordsApplied, snapshots.size(), summary.isEmpty() ? "" : ": " + summary);
        return null;
    }

    // --------------------------------------------------------------- records

    /**
     * Applies one raw topic record. Package-private so tests can drive the
     * exact replay/live behavior without a broker.
     *
     * @param live false during the initial replay (events/remediation suppressed)
     */
    void handleRecord(String key, String value, boolean live) {
        applyRecord(key, value, live);
    }

    private void applyRecord(String key, String value, boolean live) {
        recordsApplied++;
        StatusKeyParser.ParsedKey parsed = StatusKeyParser.parse(key);
        if (parsed == null) {
            LOGGER.debug("ignoring status record with key '{}'", key);
            return;
        }
        if (value == null) {
            handleTombstone(parsed, live);
            return;
        }
        StatusValueParser.StatusValue parsedValue = StatusValueParser.parse(value);
        if (parsedValue == null) {
            LOGGER.warn("ignoring unparseable status value for key '{}': {}", key, excerpt(value));
            return;
        }
        LOGGER.debug("status record: key='{}' state={} worker={} live={}",
                key, parsedValue.state(), parsedValue.workerId(), live);
        ConnectStatusStore.Change change = parsed.type() == StatusKeyParser.KeyType.CONNECTOR
                ? store.applyConnectorStatus(parsed.connector(), parsedValue.state(),
                        parsedValue.workerId(), parsedValue.trace())
                : store.applyTaskStatus(parsed.connector(), parsed.taskId(), parsedValue.state(),
                        parsedValue.workerId(), parsedValue.trace());
        onSnapshot(change, live);
    }

    private void handleTombstone(StatusKeyParser.ParsedKey parsed, boolean live) {
        if (parsed.type() == StatusKeyParser.KeyType.CONNECTOR) {
            ConnectorStatus last = store.removeConnector(parsed.connector());
            if (last == null) {
                return; // tombstone for a connector we never tracked (or already removed)
            }
            LOGGER.info("connector '{}' removed (tombstone on the status topic){}",
                    parsed.connector(), live ? "" : " [replay]");
            ConnectorHealth health = healthByConnector.remove(parsed.connector());
            if (health != null) {
                try {
                    health.unregister();
                } catch (RuntimeException e) {
                    LOGGER.warn("unregistering ConnectorHealth MBean for '{}' failed: {}",
                            parsed.connector(), e.getMessage());
                }
            }
        } else {
            onSnapshot(store.removeTask(parsed.connector(), parsed.taskId()), live);
        }
    }

    /**
     * Applies one changed snapshot: the ConnectorHealth MBean always follows
     * the latest state (replay included - that replay IS the snapshot);
     * events/remediation are edge-triggered and only fire for live records.
     */
    private void onSnapshot(ConnectStatusStore.Change change, boolean live) {
        if (change == null) {
            return;
        }
        ConnectorStatus current = change.current();
        ConnectorHealth health = healthForCreate(current.connectorName());
        health.update(current);

        if (!live) {
            return; // initial replay: snapshot only, no events from history
        }
        boolean wasUnhealthy = change.previous() != null && change.previous().isUnhealthy();
        if (current.isUnhealthy() && !wasUnhealthy) {
            remediationHandler.handleUnhealthy(current);
            eventBus.publish(Event.metric("connector-unhealthy", EventSeverity.CRITICAL,
                    current.connectorName(), describeUnhealthy(current)));
        } else if (!current.isUnhealthy() && wasUnhealthy) {
            remediationHandler.handleRecovered(current);
            eventBus.publish(Event.metric("connector-recovered", EventSeverity.INFO,
                    current.connectorName(), "connector and all tasks RUNNING"));
        }
    }

    private ConnectorHealth healthForCreate(String connectorName) {
        return healthByConnector.computeIfAbsent(connectorName, name -> {
            ConnectorHealth health = new ConnectorHealth(name);
            try {
                health.register();
            } catch (RuntimeException e) {
                LOGGER.error("could not register ConnectorHealth MBean for '{}': {}", name, e.getMessage());
            }
            return health;
        });
    }

    /** Same shape the REST poller emits, so events look identical regardless of source. */
    private static String describeUnhealthy(ConnectorStatus status) {
        long failedTasks = status.tasks().stream().filter(TaskStatus::isFailed).count();
        String error = status.isConnectorFailed()
                ? status.trace()
                : status.tasks().stream()
                        .filter(TaskStatus::isFailed)
                        .map(TaskStatus::trace)
                        .filter(t -> t != null && !t.isBlank())
                        .findFirst()
                        .orElse(status.errorMessage());
        return "reachability=" + status.reachability() + " state=" + status.connectorState()
                + " failedTasks=" + failedTasks + (error != null ? " error=" + excerpt(error) : "");
    }

    /** First non-blank line of a trace (the exception headline) - full stacktraces don't belong in event messages. */
    private static String excerpt(String text) {
        String firstLine = text.lines().filter(s -> !s.isBlank()).findFirst().orElse("");
        return firstLine.length() > 300 ? firstLine.substring(0, 300) + "..." : firstLine;
    }

    // ----------------------------------------------------------------- kafka

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "zv-monitor-status-consumer");
        // The compacted status topic is the source of truth: every start replays
        // it from the beginning (that replay IS the state snapshot - no REST
        // bootstrap), so nothing is ever committed and auto.offset.reset is
        // earliest.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
