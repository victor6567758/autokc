package com.zv.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baseline handler: makes every event visible in zv-monitor's own logs.
 * This is also what today stands in for the "React on event" box in the
 * design - swap in/add a RemediationHandler-style AI consumer later without
 * touching the producers (EventCounterRegistry, LogEventPoller, ZvMonitorApp).
 */
public class LoggingEventHandler implements EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventHandler.class);

    @Override
    public void onEvent(Event event) {
        String line = String.format("-->   source=%s pattern=%s container=%s :: %s",
                event.source(), event.patternId(), event.container(), event.message());
        switch (event.severity()) {
            case CRITICAL -> LOGGER.error(line);
            case WARNING -> LOGGER.warn(line);
            case INFO -> LOGGER.info(line);
        }
    }
}
