package com.zv.remediation;

import com.zv.connect.ConnectorStatus;

/**
 * Called whenever a connector transitions into an unhealthy state.
 *
 * This is the intended plug point for the future AI module: an implementation
 * here could send the failure trace to an LLM, get back a diagnosis + suggested
 * action (e.g. "restart task 0", "the sink table is missing a column", "widen
 * the connection pool"), and either surface it for a human to approve or,
 * once trusted, call ConnectClient.restartConnector/restartTask directly.
 *
 * Keep implementations non-blocking / fast - they're called from the polling
 * loop. Do slow work (LLM calls, ticket creation, etc.) on a separate executor.
 */
public interface RemediationHandler {

    void handleUnhealthy(ConnectorStatus status);

    /** Called once the connector recovers on its own, so you can clear any open alerts. */
    default void handleRecovered(ConnectorStatus status) {
        // no-op by default
    }
}
