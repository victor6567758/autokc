package com.zv;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ZvMonitorServiceTest {

    @Test
    void stopSchedulerInterruptsStuckTaskAfterGraceInsteadOfWaitingItOut() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        CountDownLatch stuck = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        scheduler.submit(() -> {
            stuck.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        });
        assertTrue(stuck.await(5, TimeUnit.SECONDS));

        // a plain shutdown() would block on the 60s sleep; after graceMs the
        // interrupt path must cut through, and the call return shortly after
        long start = System.nanoTime();
        ZvMonitorService.stopScheduler(scheduler, 3_000, 2_000);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(scheduler.isTerminated(), "scheduler should be terminated");
        assertTrue(interrupted.await(0, TimeUnit.SECONDS), "stuck task should have been interrupted");
        assertTrue(elapsedMs >= 3_000 && elapsedMs < 5_500,
                "expected ~graceMs before the interrupt, got " + elapsedMs + " ms");
    }
}
