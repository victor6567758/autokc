package com.zv.lifecycle;

import com.zv.utils.GracefulShutdown;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GracefulShutdownTest {

    @Test
    void runsStepsInRegistrationOrder() {
        List<String> order = new ArrayList<>();
        GracefulShutdown shutdown = new GracefulShutdown();
        shutdown.addStep("first", () -> order.add("first"));
        shutdown.addStep("second", () -> order.add("second"));

        assertTrue(shutdown.initiate());

        assertEquals(List.of("first", "second"), order);
    }

    @Test
    void initiateRunsStepsExactlyOnce() {
        List<String> order = new ArrayList<>();
        GracefulShutdown shutdown = new GracefulShutdown();
        shutdown.addStep("only-once", () -> order.add("only-once"));

        assertTrue(shutdown.initiate());
        assertFalse(shutdown.initiate());

        assertEquals(List.of("only-once"), order);
    }

    @Test
    void failingStepDoesNotBlockLaterStepsOrAwait() {
        List<String> order = new ArrayList<>();
        GracefulShutdown shutdown = new GracefulShutdown();
        shutdown.addStep("boom", () -> {
            throw new IllegalStateException("boom");
        });
        shutdown.addStep("after", () -> order.add("after"));

        assertTrue(shutdown.initiate());

        assertEquals(List.of("after"), order);
        assertDoesNotThrow(shutdown::await);
    }

    @Test
    void addStepAfterInitiateIsRejected() {
        GracefulShutdown shutdown = new GracefulShutdown();
        shutdown.initiate();

        assertThrows(IllegalStateException.class, () -> shutdown.addStep("late", () -> {
        }));
    }
}
