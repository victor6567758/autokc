package com.zv.connect;

import com.zv.event.Event;
import com.zv.event.EventBus;
import com.zv.event.EventSeverity;
import com.zv.jmx.ConnectorHealth;
import com.zv.remediation.RemediationHandler;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorStatusPollerTest {

    @Test
    void failedTaskMarksUnhealthyPublishesCriticalEventAndNotifiesRemediation() {
        StubConnectClient client = new StubConnectClient(List.of());
        client.statuses.put("orders-src", statusWithFailedTask());
        RecordingRemediation remediation = new RecordingRemediation();
        List<Event> events = new ArrayList<>();
        ConnectorStatusPoller poller = newPoller(client, remediation, events, "orders-src");

        poller.pollOnce();

        assertThat(poller.healthFor("orders-src").getHealthy()).isZero();
        assertThat(poller.healthFor("orders-src").getConsecutiveFailures()).isEqualTo(1);
        assertThat(remediation.unhealthy)
                .extracting(ConnectorStatus::connectorName)
                .containsExactly("orders-src");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).patternId()).isEqualTo("connector-unhealthy");
        assertThat(events.get(0).severity()).isEqualTo(EventSeverity.CRITICAL);
        assertThat(events.get(0).message()).contains("failedTasks=1");
    }

    @Test
    void recoveryAfterFailurePublishesRecoveredEventAndResetsCounters() {
        StubConnectClient client = new StubConnectClient(List.of());
        client.statuses.put("orders-src", statusWithFailedTask());
        RecordingRemediation remediation = new RecordingRemediation();
        List<Event> events = new ArrayList<>();
        ConnectorStatusPoller poller = newPoller(client, remediation, events, "orders-src");

        poller.pollOnce();
        client.statuses.put("orders-src", allRunning("orders-src"));
        poller.pollOnce();

        assertThat(poller.healthFor("orders-src").getHealthy()).isEqualTo(1);
        assertThat(poller.healthFor("orders-src").getConsecutiveFailures()).isZero();
        assertThat(remediation.recovered)
                .extracting(ConnectorStatus::connectorName)
                .containsExactly("orders-src");
        assertThat(events).hasSize(2);
        assertThat(events.get(1).patternId()).isEqualTo("connector-recovered");
        assertThat(events.get(1).severity()).isEqualTo(EventSeverity.INFO);
    }

    @Test
    void restFailureForOneConnectorDoesNotStopOthers() {
        StubConnectClient client = new StubConnectClient(List.of());
        client.failFor = "bad-connector";
        client.statuses.put("good-connector", allRunning("good-connector"));
        List<Event> events = new ArrayList<>();
        ConnectorStatusPoller poller = newPoller(client, new RecordingRemediation(), events,
                "bad-connector", "good-connector");

        poller.pollOnce();

        assertThat(poller.healthFor("good-connector").getHealthy()).isEqualTo(1);
        // first healthy poll after the initial UNKNOWN(healthy=0) state counts as
        // a recovery transition - same behavior the app had before the refactor
        assertThat(events).hasSize(1);
        assertThat(events.get(0).patternId()).isEqualTo("connector-recovered");
        assertThat(events.get(0).container()).isEqualTo("good-connector");
    }

    @Test
    void createDiscoversConnectorsViaRestAndRegistersAndUnregistersMBeans() throws Exception {
        StubConnectClient client = new StubConnectClient(List.of("create-disco-a", " create-disco-b "));
        List<Event> events = new ArrayList<>();
        ConnectorStatusPoller poller = ConnectorStatusPoller.create(client, List.of(),
                new RecordingRemediation(), busFor(events));

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        assertThat(mbeanServer.isRegistered(objectName("create-disco-a"))).isTrue();
        // configured/discovered names must be trimmed before MBean registration
        assertThat(mbeanServer.isRegistered(objectName("create-disco-b"))).isTrue();
        assertThat(poller.healthFor("create-disco-a").getConnectorName()).isEqualTo("create-disco-a");

        poller.unregisterMBeans();
        poller.unregisterMBeans(); // idempotent
        assertThat(mbeanServer.isRegistered(objectName("create-disco-a"))).isFalse();
        assertThat(mbeanServer.isRegistered(objectName("create-disco-b"))).isFalse();
    }

    // ------------------------------------------------------------------ helpers

    private static ConnectorStatusPoller newPoller(ConnectClient client, RemediationHandler remediation,
                                                   List<Event> events, String... connectorNames) {
        Map<String, ConnectorHealth> healthByConnector = new LinkedHashMap<>();
        for (String name : connectorNames) {
            healthByConnector.put(name, new ConnectorHealth(name)); // deliberately not JMX-registered
        }
        EventBus eventBus = busFor(events);
        return new ConnectorStatusPoller(client, healthByConnector, remediation, eventBus);
    }

    private static EventBus busFor(List<Event> events) {
        EventBus eventBus = new EventBus();
        eventBus.register(events::add);
        return eventBus;
    }

    private static ConnectorStatus statusWithFailedTask() {
        return ConnectorStatus.healthy("orders-src", "RUNNING", "worker-1:8083", null,
                List.of(new TaskStatus(0, "FAILED", "worker-1:8083", "task boom")));
    }

    private static ConnectorStatus allRunning(String name) {
        return ConnectorStatus.healthy(name, "RUNNING", "worker-1:8083", null,
                List.of(new TaskStatus(0, "RUNNING", "worker-1:8083", null)));
    }

    private static javax.management.ObjectName objectName(String connectorName) throws Exception {
        return new javax.management.ObjectName("com.zv:type=ConnectorHealth,name=" + connectorName);
    }

    /** Answers getConnectorStatus()/listConnectors() from a canned map - no HTTP. */
    private static final class StubConnectClient extends ConnectClient {
        final Map<String, ConnectorStatus> statuses = new LinkedHashMap<>();
        final List<String> discovered;
        String failFor;

        StubConnectClient(List<String> discovered) {
            super("http://localhost:1");
            this.discovered = discovered;
        }

        @Override
        public ConnectorStatus getConnectorStatus(String connectorName) {
            if (connectorName.equals(failFor)) {
                throw new RuntimeException("connection refused (stub)");
            }
            ConnectorStatus status = statuses.get(connectorName);
            if (status == null) {
                throw new IllegalStateException("no stub status for connector " + connectorName);
            }
            return status;
        }

        @Override
        public List<String> listConnectors() {
            return discovered;
        }
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
