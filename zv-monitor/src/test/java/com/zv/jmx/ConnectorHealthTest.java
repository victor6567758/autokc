package com.zv.jmx;

import com.zv.connect.ConnectorStatus;
import com.zv.connect.TaskStatus;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorHealthTest {

    @Test
    void healthyStatusReportsHealthyOne() {
        ConnectorHealth health = new ConnectorHealth("test-connector");
        ConnectorStatus status = ConnectorStatus.healthy("test-connector", "RUNNING", "worker-1", null,
                List.of(new TaskStatus(0, "RUNNING", "worker-1", null)));

        health.update(status);

        assertEquals(1, health.getHealthy());
        assertEquals(0, health.getFailedTaskCount());
        assertEquals(0, health.getConsecutiveFailures());
    }

    @Test
    void failedTaskMarksUnhealthyAndIncrementsFailures() {
        ConnectorHealth health = new ConnectorHealth("test-connector");
        ConnectorStatus status = ConnectorStatus.healthy("test-connector", "RUNNING", "worker-1", null,
                List.of(new TaskStatus(0, "FAILED", "worker-1", "boom")));

        health.update(status);
        health.update(status);

        assertEquals(0, health.getHealthy());
        assertEquals(1, health.getFailedTaskCount());
        assertEquals(2, health.getConsecutiveFailures());
    }

    @Test
    void recoveryResetsConsecutiveFailures() {
        ConnectorHealth health = new ConnectorHealth("test-connector");
        ConnectorStatus failed = ConnectorStatus.healthy("test-connector", "RUNNING", "worker-1", null,
                List.of(new TaskStatus(0, "FAILED", "worker-1", "boom")));
        ConnectorStatus recovered = ConnectorStatus.healthy("test-connector", "RUNNING", "worker-1", null,
                List.of(new TaskStatus(0, "RUNNING", "worker-1", null)));

        health.update(failed);
        health.update(recovered);

        assertEquals(1, health.getHealthy());
        assertEquals(0, health.getConsecutiveFailures());
    }

    @Test
    void unregisterRemovesTheMBeanAndIsSafeToRepeat() throws Exception {
        ConnectorHealth health = new ConnectorHealth("test-unregister-target");
        health.register();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.zv:type=ConnectorHealth,name=test-unregister-target");
        assertTrue(server.isRegistered(name));

        health.unregister();
        health.unregister();

        assertFalse(server.isRegistered(name));
    }
}
