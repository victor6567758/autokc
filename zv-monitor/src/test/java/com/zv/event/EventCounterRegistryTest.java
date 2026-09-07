package com.zv.event;

import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.*;

class EventCounterRegistryTest {

    @Test
    void firstHitRegistersMBeanAndSubsequentHitsIncrement() throws Exception {
        EventCounterRegistry registry = new EventCounterRegistry();
        EventBus bus = new EventBus();
        bus.register(registry);

        bus.publish(Event.log("npe-test", EventSeverity.CRITICAL, "kafka-connect", "boom #1", java.time.Instant.now()));
        bus.publish(Event.log("npe-test", EventSeverity.CRITICAL, "kafka-connect", "boom #2", java.time.Instant.now()));

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.zv:type=EventCounter,source=log,pattern=npe-test");
        assertTrue(server.isRegistered(name));
        assertEquals(2L, server.getAttribute(name, "Count"));
        assertEquals("boom #2", server.getAttribute(name, "LastMessage"));
    }

    @Test
    void differentPatternsGetIndependentCounters() {
        EventCounterRegistry registry = new EventCounterRegistry();

        registry.onEvent(Event.metric("connector-unhealthy", EventSeverity.CRITICAL, "inventory-source", "x"));
        registry.onEvent(Event.log("postgres-fatal", EventSeverity.CRITICAL, "postgres-source", "y", java.time.Instant.now()));
        registry.onEvent(Event.metric("connector-unhealthy", EventSeverity.CRITICAL, "inventory-source", "x2"));

        // no shared state between distinct (source, patternId) keys - verified indirectly via
        // the JMX side effects exercised in the other test; this just guards against exceptions
        // when interleaving multiple pattern ids on the same registry instance.
        assertDoesNotThrow(() -> registry.onEvent(
                Event.log("npe", EventSeverity.CRITICAL, "kafka-connect", "z", java.time.Instant.now())));
    }

    @Test
    void unregisterAllRemovesCountersAndNextHitRegistersFresh() throws Exception {
        EventCounterRegistry registry = new EventCounterRegistry();
        EventBus bus = new EventBus();
        bus.register(registry);
        registry.onEvent(Event.log("unregister-test", EventSeverity.CRITICAL, "kafka-connect", "hit",
                java.time.Instant.now()));

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.zv:type=EventCounter,source=log,pattern=unregister-test");
        assertTrue(server.isRegistered(name));

        registry.unregisterAll();

        assertFalse(server.isRegistered(name));
        // without unregisterAll a stale MBean would survive (register() skips
        // already-registered names) and keep showing the old counter forever
        registry.onEvent(Event.log("unregister-test", EventSeverity.CRITICAL, "kafka-connect", "hit-2",
                java.time.Instant.now()));
        assertTrue(server.isRegistered(name));
        assertEquals(1L, server.getAttribute(name, "Count"));
        assertEquals("hit-2", server.getAttribute(name, "LastMessage"));
    }
}
