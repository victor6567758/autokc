package com.zv.jmx;

import com.zv.connect.ConnectorStatus;
import lombok.RequiredArgsConstructor;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RequiredArgsConstructor
public class ConnectorHealth implements ConnectorHealthMBean {

    private final String connectorName;
    private final AtomicReference<String> connectorState = new AtomicReference<>("UNKNOWN");
    private final AtomicInteger healthy = new AtomicInteger(0);
    private final AtomicInteger failedTaskCount = new AtomicInteger(0);
    private final AtomicInteger totalTaskCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong lastCheckedEpochMillis = new AtomicLong(0);
    private final AtomicReference<String> lastErrorMessage = new AtomicReference<>("");

    /** Registers this instance as an MBean under com.zv:type=ConnectorHealth,name=<connectorName> */
    public void register() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("com.zv:type=ConnectorHealth,name=" + connectorName);
            if (!server.isRegistered(name)) {
                server.registerMBean(this, name);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register JMX MBean for " + connectorName, e);
        }
    }

    /** Called after each poll of the Connect REST API to refresh the exposed metrics. */
    public void update(ConnectorStatus status) {
        connectorState.set(status.connectorState());
        totalTaskCount.set(status.tasks().size());
        failedTaskCount.set((int) status.tasks().stream().filter(t -> t.isFailed()).count());
        lastCheckedEpochMillis.set(System.currentTimeMillis());

        if (status.isUnhealthy()) {
            healthy.set(0);
            consecutiveFailures.incrementAndGet();
            String msg = status.errorMessage() != null ? status.errorMessage() : status.trace();
            lastErrorMessage.set(truncate(msg));
        } else {
            healthy.set(1);
            consecutiveFailures.set(0);
            lastErrorMessage.set("");
        }
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() > 500 ? s.substring(0, 500) + "..." : s;
    }

    @Override public String getConnectorName() { return connectorName; }
    @Override public String getConnectorState() { return connectorState.get(); }
    @Override public int getHealthy() { return healthy.get(); }
    @Override public int getFailedTaskCount() { return failedTaskCount.get(); }
    @Override public int getTotalTaskCount() { return totalTaskCount.get(); }
    @Override public int getConsecutiveFailures() { return consecutiveFailures.get(); }
    @Override public long getLastCheckedEpochMillis() { return lastCheckedEpochMillis.get(); }
    @Override public String getLastErrorMessage() { return lastErrorMessage.get(); }
}
