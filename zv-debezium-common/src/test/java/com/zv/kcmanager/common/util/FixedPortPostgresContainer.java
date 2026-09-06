package com.zv.kcmanager.common.util;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A {@link PostgreSQLContainer} bound to a fixed host port - useful when the
 * code under test needs a stable, pre-announced port (e.g. connector configs
 * that reference the database before the container is started).
 *
 * <p>Mirrors streamkap's {@code FixedPortPostgresContainer}.</p>
 */
public class FixedPortPostgresContainer extends PostgreSQLContainer<FixedPortPostgresContainer> {

    private final int exposedPort;

    public FixedPortPostgresContainer(DockerImageName dockerImageName, int exposedPort) {
        super(dockerImageName);
        this.exposedPort = exposedPort;
    }

    @Override
    protected void configure() {
        super.configure();
        this.addFixedExposedPort(exposedPort, PostgreSQLContainer.POSTGRESQL_PORT);
    }
}
