package io.github.hectorvent.floci.services.lambda;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaEnvironmentLimiterTest {

    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();

    private static LambdaEnvironmentLimiter.Status status(int limit, int total, int active, int idle, int retiring,
                                                          int queued, int available) {
        return new LambdaEnvironmentLimiter.Status(limit, total, active, idle, retiring, queued, available);
    }

    @Test
    void withoutACapEveryAdmissionLaunchesAndIsOnlyCounted() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter();

        LambdaEnvironmentLimiter.Slot first = limiter.admit(() -> null).slot();
        LambdaEnvironmentLimiter.Slot second = limiter.admit(() -> null).slot();
        limiter.markIdle(second);

        assertFalse(limiter.bounded());
        assertEquals(status(0, 2, 1, 1, 0, 0, LambdaEnvironmentLimiter.UNLIMITED), limiter.status());
        first.close();
        second.close();
        assertEquals(status(0, 0, 0, 0, 0, 0, LambdaEnvironmentLimiter.UNLIMITED), limiter.status());
    }

    @Test
    void aReusedWarmEnvironmentKeepsItsSlotAndTakesNoNewOne() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 5);
        LambdaEnvironmentLimiter.Slot slot = limiter.admit(() -> null).slot();
        limiter.markIdle(slot);

        LambdaEnvironmentLimiter.Admission<String> admission = limiter.admit(() -> "warm");

        assertEquals("warm", admission.reused());
        assertNull(admission.slot());
        assertEquals(status(1, 1, 0, 1, 0, 0, 0), limiter.status());
    }

    @Test
    void aSlotIsCountedThroughEveryStateUntilItCloses() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(2, 5);
        LambdaEnvironmentLimiter.Slot slot = limiter.admit(() -> null).slot();
        assertEquals(status(2, 1, 1, 0, 0, 0, 1), limiter.status());

        limiter.markIdle(slot);
        assertEquals(status(2, 1, 0, 1, 0, 0, 1), limiter.status());
        limiter.markActive(slot);
        assertEquals(status(2, 1, 1, 0, 0, 0, 1), limiter.status());
        limiter.markRetiring(slot);
        assertEquals(status(2, 1, 0, 0, 1, 0, 1), limiter.status());

        slot.close();
        slot.close();
        limiter.markIdle(slot);
        assertEquals(status(2, 0, 0, 0, 0, 0, 2), limiter.status());
    }

    @Test
    void aFullCapQueuesAdmissionsInArrivalOrder() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Slot held = limiter.admit(() -> null).slot();

        Future<LambdaEnvironmentLimiter.Slot> first = threads.submit(() -> limiter.admit(() -> null).slot());
        await().atMost(5, TimeUnit.SECONDS).until(() -> limiter.status().queued() == 1);
        Future<LambdaEnvironmentLimiter.Slot> second = threads.submit(() -> limiter.admit(() -> null).slot());
        await().atMost(5, TimeUnit.SECONDS).until(() -> limiter.status().queued() == 2);

        held.close();

        LambdaEnvironmentLimiter.Slot firstSlot = first.get(5, TimeUnit.SECONDS);
        assertNotNull(firstSlot);
        assertFalse(second.isDone());
        assertEquals(status(1, 1, 1, 0, 0, 1, 0), limiter.status());

        firstSlot.close();
        assertNotNull(second.get(5, TimeUnit.SECONDS));
        assertEquals(0, limiter.status().queued());
    }

    @Test
    void theOldestWaiterRetiresAnIdleEnvironmentToMakeRoom() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Slot idle = limiter.admit(() -> null).slot();
        limiter.markIdle(idle);
        AtomicInteger retirements = new AtomicInteger();
        limiter.retireIdleWith(() -> {
            retirements.incrementAndGet();
            limiter.markRetiring(idle);
            idle.close();
            return true;
        });

        LambdaEnvironmentLimiter.Slot launched = limiter.admit(() -> null).slot();

        assertNotNull(launched);
        assertEquals(1, retirements.get());
        assertEquals(status(1, 1, 1, 0, 0, 0, 0), limiter.status());
    }

    @Test
    void aWaiterTakesUpAWarmEnvironmentReleasedWhileItWaits() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Slot busy = limiter.admit(() -> null).slot();
        AtomicReference<String> pool = new AtomicReference<>();

        CompletableFuture<LambdaEnvironmentLimiter.Admission<String>> waiter = CompletableFuture.supplyAsync(
                () -> limiter.admit(() -> pool.getAndSet(null)), threads);
        await().atMost(5, TimeUnit.SECONDS).until(() -> limiter.status().queued() == 1);

        pool.set("warm");
        limiter.markIdle(busy);

        assertEquals("warm", waiter.get(5, TimeUnit.SECONDS).reused());
        assertEquals(status(1, 1, 0, 1, 0, 0, 0), limiter.status());
    }

    @Test
    void anAdmissionStillWaitingAtItsDeadlineTimesOutAndLeavesTheQueue() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 0);
        limiter.admit(() -> null);

        assertThrows(LambdaEnvironmentLimiter.AdmissionTimeoutException.class, () -> limiter.admit(() -> null));
        assertEquals(status(1, 1, 1, 0, 0, 0, 0), limiter.status());
    }

    @Test
    void anInterruptedWaiterLeavesTheQueueWithItsInterruptRestored() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 30);
        limiter.admit(() -> null);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                limiter.admit(() -> null);
            } catch (RuntimeException e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        await().atMost(5, TimeUnit.SECONDS).until(() -> limiter.status().queued() == 1);

        waiter.interrupt();
        waiter.join(5000);

        assertTrue(failure.get() instanceof LambdaEnvironmentLimiter.AdmissionInterruptedException);
        assertTrue(interrupted.get());
        assertEquals(0, limiter.status().queued());
    }

    @Test
    void recoveredContainersCountAsRetiringEvenPastTheCap() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 0);
        LambdaEnvironmentLimiter.Slot first = limiter.reserveRecovered();
        LambdaEnvironmentLimiter.Slot second = limiter.reserveRecovered();

        assertEquals(status(1, 2, 0, 0, 2, 0, 0), limiter.status());
        assertThrows(LambdaEnvironmentLimiter.AdmissionTimeoutException.class, () -> limiter.admit(() -> null));

        first.close();
        second.close();
        assertSame(LambdaEnvironmentLimiter.Slot.class, limiter.admit(() -> null).slot().getClass());
    }
}
