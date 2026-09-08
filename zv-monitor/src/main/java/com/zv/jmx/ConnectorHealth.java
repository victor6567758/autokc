package com.zv.jmx;

import com.zv.connect.ConnectorStatus;
import com.zv.connect.TaskStatus;
import java.lang.management.ManagementFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConnectorHealth implements ConnectorHealthMBean {

  private final String connectorName;

  /**
   * Guards the mutable fields below. The only writer is the single Connect poller thread
   * ({@link #update}); readers are JMX scrape threads (the Prometheus JMX exporter javaagent). At
   * that concurrency a plain read/write lock is plenty - and unlike independent atomics it keeps
   * each getter call on one poll's consistent snapshot instead of a mix of two.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private String connectorState = "UNKNOWN";
  private int healthy;
  private int failedTaskCount;
  private int totalTaskCount;
  private int consecutiveFailures;
  private long lastCheckedEpochMillis;
  private String lastErrorMessage = "";

  /**
   * Registers this instance as an MBean under com.zv:type=ConnectorHealth,name=<connectorName>
   */
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

  /**
   * Undoes {@link #register()}; safe to call when not (or no longer) registered.
   */
  public void unregister() {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = new ObjectName("com.zv:type=ConnectorHealth,name=" + connectorName);
      if (server.isRegistered(name)) {
        server.unregisterMBean(name);
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to unregister JMX MBean for " + connectorName, e);
    }
  }

  /**
   * Called after each poll of the Connect REST API to refresh the exposed metrics.
   */
  public void update(ConnectorStatus status) {
    lock.writeLock().lock();
    try {
      connectorState = status.connectorState();
      totalTaskCount = status.tasks().size();
      failedTaskCount = (int) status.tasks().stream().filter(TaskStatus::isFailed).count();
      lastCheckedEpochMillis = System.currentTimeMillis();

      if (status.isUnhealthy()) {
        healthy = 0;
        consecutiveFailures++;
        String msg = status.errorMessage() != null ? status.errorMessage() : status.trace();
        lastErrorMessage = truncate(msg);
      } else {
        healthy = 1;
        consecutiveFailures = 0;
        lastErrorMessage = "";
      }
    } finally {
      lock.writeLock().unlock();
    }
  }


  @Override
  public String getConnectorName() {
    return connectorName;
  }

  @Override
  public String getConnectorState() {
    return readLocked(() -> connectorState);
  }

  @Override
  public int getHealthy() {
    return readLocked(() -> healthy);
  }

  @Override
  public int getFailedTaskCount() {
    return readLocked(() -> failedTaskCount);
  }

  @Override
  public int getTotalTaskCount() {
    return readLocked(() -> totalTaskCount);
  }

  @Override
  public int getConsecutiveFailures() {
    return readLocked(() -> consecutiveFailures);
  }

  @Override
  public long getLastCheckedEpochMillis() {
    return readLocked(() -> lastCheckedEpochMillis);
  }

  @Override
  public String getLastErrorMessage() {
    return readLocked(() -> lastErrorMessage);
  }

  /**
   * One getter = one read lock acquisition = one poll's consistent snapshot.
   */
  private <T> T readLocked(Supplier<T> read) {
    lock.readLock().lock();
    try {
      return read.get();
    } finally {
      lock.readLock().unlock();
    }
  }

  private static String truncate(String s) {
    if (s == null) {
      return "";
    }
    return s.length() > 500 ? s.substring(0, 500) + "..." : s;
  }
}
