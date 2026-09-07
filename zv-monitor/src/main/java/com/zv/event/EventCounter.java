package com.zv.event;

import java.lang.management.ManagementFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Deliberately lock-guarded instead of atomic: {@link #recordHit(Event)} is the only mutator and
 * takes the write lock, which is all {@code count++} needs. The getters backing the JMX /
 * Prometheus exporter read path take the shared read lock, so readers always see a consistent
 * snapshot of the last hit (never a torn count/message pair) while still running concurrently with
 * each other. Immutable source/patternId stay plain Lombok getters.
 */
@RequiredArgsConstructor
public class EventCounter implements EventCounterMBean {

  @Getter
  private final String source;
  @Getter
  private final String patternId;

  private long count;
  private long lastEpochMillis;
  private String lastContainer = "";
  private String lastMessage = "";

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  @Override
  public long getCount() {
    lock.readLock().lock();
    try {
      return count;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long getLastEpochMillis() {
    lock.readLock().lock();
    try {
      return lastEpochMillis;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getLastContainer() {
    lock.readLock().lock();
    try {
      return lastContainer;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getLastMessage() {
    lock.readLock().lock();
    try {
      return lastMessage;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Registers this instance as an MBean under
   * com.zv:type=EventCounter,source=<source>,pattern=<patternId>
   */
  void register() {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = new ObjectName(
          "com.zv:type=EventCounter,source=" + source + ",pattern=" + patternId);
      if (!server.isRegistered(name)) {
        server.registerMBean(this, name);
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to register EventCounter MBean for " + patternId, e);
    }
  }

  void recordHit(Event event) {
    lock.writeLock().lock();
    try {
      count++;
      lastEpochMillis = event.timestamp().toEpochMilli();
      lastContainer = event.container() == null ? "" : event.container();
      lastMessage = truncate(event.message());
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static String truncate(String s) {
      if (s == null) {
          return "";
      }
    return s.length() > 300 ? s.substring(0, 300) + "..." : s;
  }


}
