package com.zv.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Deliberately the simplest thing that works for a single-process sidecar:
 * an in-memory, synchronous fan-out. No queue, no persistence, no network -
 * if zv-monitor restarts, in-flight events are gone, which is fine since the
 * metrics/log sources are themselves polled and re-observed on the next tick.
 *
 * If this ever needs to survive restarts or fan out to another process,
 * swap the publish() body for a Kafka topic / outbox - the EventHandler
 * contract at the edges wouldn't need to change.
 */
public class EventBus {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventBus.class);

    private final List<EventHandler> handlers = new CopyOnWriteArrayList<>();

    public void register(EventHandler handler) {
        handlers.add(handler);
    }

    public void publish(Event event) {
        for (EventHandler handler : handlers) {
            try {
                handler.onEvent(event);
            } catch (Exception e) {
                LOGGER.error("EventHandler {} threw while handling {}: {}",
                        handler.getClass().getSimpleName(), event, e.getMessage(), e);
            }
        }
    }
}
