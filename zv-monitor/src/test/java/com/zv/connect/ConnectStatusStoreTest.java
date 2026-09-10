package com.zv.connect;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectStatusStoreTest {

    @Test
    void connectorRecordBuildsSnapshot() {
        ConnectStatusStore store = new ConnectStatusStore();

        ConnectStatusStore.Change change =
                store.applyConnectorStatus("orders-src", "RUNNING", "worker-1:8083", null);

        assertThat(change.previous()).isNull();
        assertThat(change.current().connectorName()).isEqualTo("orders-src");
        assertThat(change.current().connectorState()).isEqualTo("RUNNING");
        assertThat(change.current().workerId()).isEqualTo("worker-1:8083");
        assertThat(change.current().tasks()).isEmpty();
    }

    @Test
    void taskRecordBeforeConnectorRecordStartsWithUnknownConnectorState() {
        // connector- and task-level keys hash to different partitions - order is not guaranteed
        ConnectStatusStore store = new ConnectStatusStore();

        store.applyTaskStatus("orders-src", 0, "RUNNING", "worker-1:8083", null);

        ConnectorStatus snapshot = store.snapshots().get("orders-src");
        assertThat(snapshot.connectorState()).isEqualTo("UNKNOWN");
        assertThat(snapshot.tasks()).hasSize(1);
        assertThat(snapshot.tasks().get(0).id()).isZero();
        assertThat(snapshot.tasks().get(0).state()).isEqualTo("RUNNING");
        // connector state UNKNOWN alone is NOT unhealthy (no FAILED anywhere)
        assertThat(snapshot.isUnhealthy()).isFalse();
    }

    @Test
    void taskUpdateCarriesPreviousAndCurrent() {
        ConnectStatusStore store = new ConnectStatusStore();
        store.applyTaskStatus("orders-src", 0, "RUNNING", "worker-1:8083", null);

        ConnectStatusStore.Change change =
                store.applyTaskStatus("orders-src", 0, "FAILED", "worker-1:8083", "task boom");

        assertThat(change.previous().tasks().get(0).state()).isEqualTo("RUNNING");
        assertThat(change.current().tasks().get(0).state()).isEqualTo("FAILED");
        assertThat(change.previous().isUnhealthy()).isFalse();
        assertThat(change.current().isUnhealthy()).isTrue();
    }

    @Test
    void tasksAreSortedByIdAndTombstonesRemove() {
        ConnectStatusStore store = new ConnectStatusStore();
        store.applyTaskStatus("orders-src", 1, "RUNNING", "w", null);
        store.applyTaskStatus("orders-src", 0, "RUNNING", "w", null);

        List<TaskStatus> tasks = store.snapshots().get("orders-src").tasks();
        assertThat(tasks).extracting(TaskStatus::id).containsExactly(0, 1);

        ConnectStatusStore.Change removal = store.removeTask("orders-src", 0);
        assertThat(removal.current().tasks()).extracting(TaskStatus::id).containsExactly(1);
        assertThat(store.removeTask("orders-src", 0)).isNull(); // idempotent

        ConnectorStatus last = store.removeConnector("orders-src");
        assertThat(last.tasks()).hasSize(1);
        assertThat(store.removeConnector("orders-src")).isNull(); // idempotent
        assertThat(store.connectorCount()).isZero();
    }

    @Test
    void snapshotsCoverAllConnectors() {
        ConnectStatusStore store = new ConnectStatusStore();
        store.applyConnectorStatus("a", "RUNNING", "w", null);
        store.applyConnectorStatus("b", "PAUSED", "w", null);

        Map<String, ConnectorStatus> snapshots = store.snapshots();

        assertThat(snapshots).containsOnlyKeys("a", "b");
        assertThat(snapshots.get("b").connectorState()).isEqualTo("PAUSED");
    }
}
