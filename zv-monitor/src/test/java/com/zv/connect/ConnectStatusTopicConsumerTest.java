package com.zv.connect;

import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventSeverity;
import com.zv.remediation.RemediationHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link ConnectStatusTopicConsumer#handleRecord} directly - the
 * replay/live distinction is exactly what these tests pin down, no broker
 * needed. The consume loop itself is verified against the dev stack.
 */
class ConnectStatusTopicConsumerTest {

    private final RecordingRemediation remediation = new RecordingRemediation();
    private final List<Event> events = new ArrayList<>();
    private final List<ConnectStatusTopicConsumer> consumers = new ArrayList<>();

    @AfterEach
    void unregisterMbeans() {
        consumers.forEach(ConnectStatusTopicConsumer::unregisterMBeans);
    }

    @Test
    void replayBuildsSnapshotAndMBeansWithoutEventsOrRemediation() {
        ConnectStatusTopicConsumer consumer = newConsumer();
        // replay ends on an UNHEALTHY state on purpose - history must not page anyone
        consumer.handleRecord("status-connector-topic-orders", running(null), false);
        consumer.handleRecord("status-task-topic-orders-0", running(null), false);
        consumer.handleRecord("status-task-topic-orders-0", failed("task boom"), false);

        assertThat(consumer.trackedConnectorCount()).isEqualTo(1);
        assertThat(consumer.healthFor("topic-orders").getHealthy()).isZero(); // MBean follows the replay
        assertThat(events).isEmpty();
        assertThat(remediation.unhealthy).isEmpty();
        assertThat(remediation.recovered).isEmpty();
    }

    @Test
    void liveFailureFiresUnhealthyAndLiveRecoveryFiresRecovered() {
        ConnectStatusTopicConsumer consumer = newConsumer();
        replayHealthy(consumer, "topic-live");

        consumer.handleRecord("status-task-topic-live-0", failed("task boom"), true);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).patternId()).isEqualTo("connector-unhealthy");
        assertThat(events.get(0).severity()).isEqualTo(EventSeverity.CRITICAL);
        assertThat(events.get(0).message()).contains("failedTasks=1").contains("task boom");
        assertThat(remediation.unhealthy).extracting(ConnectorStatus::connectorName)
                .containsExactly("topic-live");

        consumer.handleRecord("status-task-topic-live-0", running(null), true);

        assertThat(events).hasSize(2);
        assertThat(events.get(1).patternId()).isEqualTo("connector-recovered");
        assertThat(events.get(1).severity()).isEqualTo(EventSeverity.INFO);
        assertThat(remediation.recovered).extracting(ConnectorStatus::connectorName)
                .containsExactly("topic-live");
        assertThat(consumer.healthFor("topic-live").getHealthy()).isEqualTo(1);
    }

    @Test
    void liveUnhealthyIsEdgeTriggeredPerConnector() {
        ConnectStatusTopicConsumer consumer = newConsumer();
        replayHealthy(consumer, "topic-edge");

        consumer.handleRecord("status-task-topic-edge-0", failed("boom-0"), true); // healthy -> unhealthy
        consumer.handleRecord("status-task-topic-edge-1", failed("boom-1"), true); // still unhealthy -> silent
        consumer.handleRecord("status-task-topic-edge-1", running(null), true); // still unhealthy -> silent
        consumer.handleRecord("status-task-topic-edge-0", running(null), true); // unhealthy -> recovered

        assertThat(events).hasSize(2);
        assertThat(events.get(0).patternId()).isEqualTo("connector-unhealthy");
        assertThat(events.get(1).patternId()).isEqualTo("connector-recovered");
        assertThat(remediation.unhealthy).hasSize(1);
        assertThat(remediation.recovered).hasSize(1);
    }

    @Test
    void connectorFirstSeenLiveInUnhealthyStateStillFires() {
        ConnectStatusTopicConsumer consumer = newConsumer();

        // created while we are already live: previous=null counts as healthy - no missed first incident
        consumer.handleRecord("status-connector-topic-new", failed("connector boom"), true);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).patternId()).isEqualTo("connector-unhealthy");
        assertThat(events.get(0).message()).contains("state=FAILED");
    }

    @Test
    void tombstonesRemoveTasksThenConnectorAndItsMBean() {
        ConnectStatusTopicConsumer consumer = newConsumer();
        replayHealthy(consumer, "topic-gone");

        consumer.handleRecord("status-task-topic-gone-0", null, true); // task tombstone
        assertThat(consumer.trackedConnectorCount()).isEqualTo(1);
        assertThat(consumer.healthFor("topic-gone").getTotalTaskCount()).isEqualTo(1);

        consumer.handleRecord("status-connector-topic-gone", null, true); // connector tombstone
        assertThat(consumer.trackedConnectorCount()).isZero();
        assertThat(consumer.healthFor("topic-gone")).isNull(); // MBean unregistered + map entry gone
    }

    @Test
    void unparseableAndIrrelevantRecordsAreIgnored() {
        ConnectStatusTopicConsumer consumer = newConsumer();

        consumer.handleRecord("status-connector-topic-junk", "not json", true);
        consumer.handleRecord("status-topic-topic-junk-0", running(null), true);

        assertThat(consumer.trackedConnectorCount()).isZero();
        assertThat(events).isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    private ConnectStatusTopicConsumer newConsumer() {
        EventBus eventBus = new EventBus();
        eventBus.register(events::add);
        ConnectStatusTopicConsumer consumer = new ConnectStatusTopicConsumer(
                "localhost:9092", "connect-status", "test", remediation, eventBus);
        consumers.add(consumer);
        return consumer;
    }

    private static void replayHealthy(ConnectStatusTopicConsumer consumer, String connector) {
        consumer.handleRecord("status-connector-" + connector, running(null), false);
        consumer.handleRecord("status-task-" + connector + "-0", running(null), false);
        consumer.handleRecord("status-task-" + connector + "-1", running(null), false);
    }

    private static String running(String trace) {
        return "{\"state\":\"RUNNING\",\"worker_id\":\"worker-1:8083\",\"trace\":" + json(trace) + ",\"generation\":1}";
    }

    private static String failed(String trace) {
        return "{\"state\":\"FAILED\",\"worker_id\":\"worker-1:8083\",\"trace\":" + json(trace) + ",\"generation\":2}";
    }

    private static String json(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static final class RecordingRemediation implements RemediationHandler {
        final List<ConnectorStatus> unhealthy = new ArrayList<>();
        final List<ConnectorStatus> recovered = new ArrayList<>();

        @Override
        public void handleUnhealthy(ConnectorStatus status) {
            unhealthy.add(status);
        }

        @Override
        public void handleRecovered(ConnectorStatus status) {
            recovered.add(status);
        }
    }
}
