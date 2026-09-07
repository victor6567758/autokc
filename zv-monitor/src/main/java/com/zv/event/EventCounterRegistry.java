package com.zv.event;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Same "computeIfAbsent -> register MBean -> update" idiom as
 * MetricsManager.setGauge in zv-debezium-common, applied to events instead
 * of raw numeric gauges: the first time a given (source, patternId) is seen,
 * an EventCounter MBean is registered for it; every hit after that just
 * increments the existing one.
 */
public class EventCounterRegistry implements EventHandler {

    private final ConcurrentHashMap<String, EventCounter> counters = new ConcurrentHashMap<>();

    @Override
    public void onEvent(Event event) {
        String source = event.source().name().toLowerCase();
        String key = source + ":" + event.patternId();
        EventCounter counter = counters.computeIfAbsent(key, k -> {
            EventCounter c = new EventCounter(source, event.patternId());
            c.register();
            return c;
        });
        counter.recordHit(event);
    }
}
