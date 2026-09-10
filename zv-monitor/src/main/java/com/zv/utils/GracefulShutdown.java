package com.zv.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ordered, idempotent shutdown for {@code ZvMonitorApp.main}.
 *
 * <p>Steps run in <em>registration</em> order, so a producer's stop is
 * registered before the cleanup of what it feeds (e.g. "stop the poller
 * scheduler" is added before "unregister the JMX MBeans the pollers write
 * to"). Each step is guarded: a failing step is logged and never blocks the
 * remaining ones.
 *
 * <p>{@code ZvMonitorApp} installs a JVM shutdown hook that calls
 * {@link #initiate()}, so SIGTERM ({@code docker stop}) and SIGINT (Ctrl-C)
 * run exactly the same path as a programmatic shutdown (a future admin
 * endpoint just calls {@code initiate()} directly). {@code main} parks on
 * {@link #await()} instead of {@code Thread.currentThread().join()}: once
 * {@code initiate()} finishes, {@code main} returns and the JVM exits
 * normally.
 */
public final class GracefulShutdown {

    private static final Logger LOGGER = LoggerFactory.getLogger(GracefulShutdown.class);

    private record Step(String name, Runnable action) {}

    private final List<Step> steps = new ArrayList<>();
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicBoolean initiated = new AtomicBoolean(false);

    /** Adds a cleanup step; see the class javadoc for ordering. */
    public synchronized void addStep(String name, Runnable action) {
        if (initiated.get()) {
            throw new IllegalStateException("shutdown already initiated");
        }
        synchronized (this) {
            steps.add(new Step(name, action));
        }
    }

    /**
     * Runs every registered step exactly once - further calls are no-ops - and
     * then releases {@link #await()}.
     *
     * @return whether this call performed the shutdown
     */
    public boolean initiate() {
        if (!initiated.compareAndSet(false, true)) {
            return false;
        }
        List<Step> toRun;
        synchronized (this) {
            toRun = List.copyOf(steps);
        }
        LOGGER.info("Shutting down: {} step(s)", toRun.size());
        for (Step step : toRun) {
            try {
                step.action().run();
            } catch (Throwable t) {
                LOGGER.warn("Shutdown step '{}' failed: {}", step.name(), t.getMessage(), t);
            }
        }
        completed.countDown();
        return true;
    }

    /** Blocks until {@link #initiate()} has run all steps. */
    public void await() throws InterruptedException {
        completed.await();
    }
}
