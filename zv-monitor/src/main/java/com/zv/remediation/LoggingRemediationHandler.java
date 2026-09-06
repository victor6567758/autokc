package com.zv.remediation;

import com.zv.connect.ConnectorStatus;
import com.zv.connect.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default handler: just logs loudly. Swap this out (or wrap it) once the
 * AI-driven remediation module exists - see RemediationHandler for the intended shape.
 */
public class LoggingRemediationHandler implements RemediationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingRemediationHandler.class);

    @Override
    public void handleUnhealthy(ConnectorStatus status) {
        LOGGER.warn("UNHEALTHY connector='{}' reachability={} state={} error={}",
                status.connectorName(), status.reachability(), status.connectorState(), status.errorMessage());
        for (TaskStatus task : status.tasks()) {
            if (task.isFailed()) {
                LOGGER.warn("  task={} state={} trace={}", task.id(), task.state(), task.trace());
            }
        }
        // TODO(AI module): dispatch status here for automatic diagnosis / fix suggestion.
    }

    @Override
    public void handleRecovered(ConnectorStatus status) {
        LOGGER.info("RECOVERED connector='{}' state={}", status.connectorName(), status.connectorState());
    }
}
