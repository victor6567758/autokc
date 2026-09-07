package com.zv.event;

/**
 * Reacts to Events published on the EventBus. Keep implementations fast and
 * non-blocking - they run inline on the polling thread that produced the
 * event. Do slow work (LLM calls, notifications, tickets) on a separate
 * executor from within the implementation, the way RemediationHandler's
 * eventual AI module is expected to.
 */
public interface EventHandler {
    void onEvent(Event event);
}
