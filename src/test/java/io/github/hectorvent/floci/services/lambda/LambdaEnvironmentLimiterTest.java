package io.github.hectorvent.floci.services.lambda;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaEnvironmentLimiterTest {

    @Test
    void twentyLogicalInvocationsShareFourPhysicalSlots() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(4, 10);
        List<LambdaEnvironmentLimiter.Permit> held = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            held.add(limiter.acquire("arn:aws:lambda:us-east-1:000000000000:function:fn:" + i));
        }

        ExecutorService workers = Executors.newFixedThreadPool(16);
        CountDownLatch started = new CountDownLatch(16);
        List<Future<LambdaEnvironmentLimiter.Permit>> waiting = new ArrayList<>();
        try {
            for (int i = 0; i < 16; i++) {
                waiting.add(workers.submit(() -> {
                    started.countDown();
                    return limiter.acquire("arn:aws:lambda:us-east-1:000000000000:function:fn:burst");
                }));
            }
            assertTrue(started.await(2, TimeUnit.SECONDS), "all logical invocations did not start");
            awaitQueued(limiter, 16);
            assertEquals(4, limiter.status().total());
            assertEquals(4, limiter.status().active());
            assertEquals(0, limiter.status().idle());
            assertEquals(16, limiter.status().queued());
            assertEquals(0, limiter.status().available());

            held.forEach(LambdaEnvironmentLimiter.Permit::close);
            held.clear();
            for (Future<LambdaEnvironmentLimiter.Permit> future : waiting) {
                LambdaEnvironmentLimiter.Permit permit = future.get(5, TimeUnit.SECONDS);
                assertNotNull(permit);
                permit.close();
            }
            assertEquals(20, limiter.status().granted());
            assertEquals(0, limiter.status().total());
            assertEquals(0, limiter.status().queued());
        } finally {
            held.forEach(LambdaEnvironmentLimiter.Permit::close);
            workers.shutdownNow();
        }
        assertEquals(0, limiter.status().total());
        assertEquals(0, limiter.status().queued());
    }

    @Test
    void waitersAreGrantedInFifoOrder() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Permit holder = limiter.acquire("holder");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch firstGranted = new CountDownLatch(1);
        CountDownLatch allowFirstToFinish = new CountDownLatch(1);
        List<String> order = new ArrayList<>();
        try {
            Future<?> first = workers.submit(() -> {
                try (LambdaEnvironmentLimiter.Permit ignored = limiter.acquireInterruptibly("first")) {
                    synchronized (order) {
                        order.add("first");
                    }
                    firstGranted.countDown();
                    assertTrue(allowFirstToFinish.await(5, TimeUnit.SECONDS));
                }
                return null;
            });
            awaitQueued(limiter, 1);

            Future<?> second = workers.submit(() -> {
                try (LambdaEnvironmentLimiter.Permit ignored = limiter.acquireInterruptibly("second")) {
                    synchronized (order) {
                        order.add("second");
                    }
                }
                return null;
            });
            awaitQueued(limiter, 2);

            holder.close();
            assertTrue(firstGranted.await(2, TimeUnit.SECONDS), "first waiter was not granted");
            assertFalse(second.isDone(), "second waiter bypassed the FIFO waiter");
            synchronized (order) {
                assertEquals(List.of("first"), order);
            }
            allowFirstToFinish.countDown();
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);
            synchronized (order) {
                assertEquals(List.of("first", "second"), order);
            }
        } finally {
            holder.close();
            workers.shutdownNow();
        }
    }

    @Test
    void earlierDifferentKeyWaiterCannotBeBypassedByWarmReuse() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Permit holder = limiter.acquire("holder");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch firstGranted = new CountDownLatch(1);
        CountDownLatch allowFirstToFinish = new CountDownLatch(1);
        AtomicBoolean secondWarmCallbackCalled = new AtomicBoolean();
        try {
            Future<?> first = workers.submit(() -> {
                try (LambdaEnvironmentLimiter.Permit ignored = limiter.acquireInterruptibly("first")) {
                    firstGranted.countDown();
                    assertTrue(allowFirstToFinish.await(5, TimeUnit.SECONDS));
                }
                return null;
            });
            awaitQueued(limiter, 1);

            Future<LambdaEnvironmentLimiter.Admission<String>> second = workers.submit(() ->
                    limiter.acquireInterruptibly("second", () -> {
                        secondWarmCallbackCalled.set(true);
                        return "warm-second";
                    }));
            awaitQueued(limiter, 2);

            assertFalse(secondWarmCallbackCalled.get(),
                    "later waiter's warm callback ran before the FIFO head completed");

            holder.close();
            assertTrue(firstGranted.await(2, TimeUnit.SECONDS), "first waiter was not granted");

            allowFirstToFinish.countDown();
            first.get(2, TimeUnit.SECONDS);
            assertEquals("warm-second", second.get(2, TimeUnit.SECONDS).reusable());
        } finally {
            allowFirstToFinish.countDown();
            holder.close();
            workers.shutdownNow();
        }
    }

    @Test
    void lockAcquisitionConsumesAdmissionDeadline() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 1);
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        ExecutorService workers = Executors.newSingleThreadExecutor();
        try {
            Future<LambdaEnvironmentLimiter.Admission<String>> slow = workers.submit(() ->
                    limiter.acquireInterruptibly("slow", () -> {
                        callbackStarted.countDown();
                        try {
                            assertTrue(releaseCallback.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                        return null;
                    }));
            assertTrue(callbackStarted.await(2, TimeUnit.SECONDS), "limiter callback did not start");

            LambdaEnvironmentLimiter.AdmissionTimeoutException timeout = assertThrows(
                    LambdaEnvironmentLimiter.AdmissionTimeoutException.class,
                    () -> limiter.acquireInterruptibly("blocked", () -> null));
            assertEquals("blocked", timeout.environmentKey());
            releaseCallback.countDown();
            slow.get(2, TimeUnit.SECONDS).permit().close();
            assertEquals(0, limiter.status().queued(), "lock wait must not leave a queue entry");
            assertEquals(1, limiter.status().timedOut());
        } finally {
            releaseCallback.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void timeoutRemovesWaiterAndDoesNotLeakPermit() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 0);
        LambdaEnvironmentLimiter.Permit holder = limiter.acquire("held");
        try {
            LambdaEnvironmentLimiter.AdmissionTimeoutException exception = assertThrows(
                    LambdaEnvironmentLimiter.AdmissionTimeoutException.class,
                    () -> limiter.acquireInterruptibly("timed-out"));
            assertEquals("timed-out", exception.environmentKey());
            assertEquals(0, limiter.status().queued());
            assertEquals(1, limiter.status().timedOut());
            assertEquals(1, limiter.status().total());
        } finally {
            holder.close();
        }
        assertEquals(0, limiter.status().total());
    }

    @Test
    void interruptionRemovesWaiterAndRestoresInterruptAtPublicBoundary() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Permit holder = limiter.acquire("held");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                limiter.acquire("interrupted");
                failure.set(new AssertionError("acquire unexpectedly succeeded"));
            } catch (LambdaEnvironmentLimiter.AdmissionInterruptedException expected) {
                interrupted.set(Thread.currentThread().isInterrupted());
            } catch (Throwable unexpected) {
                failure.set(unexpected);
            }
        });
        waiter.start();
        awaitQueued(limiter, 1);
        waiter.interrupt();
        waiter.join(2_000);
        try {
            assertFalse(waiter.isAlive(), "interrupted waiter did not exit");
            assertNull(failure.get());
            assertEquals(Boolean.TRUE, interrupted.get());
            assertEquals(0, limiter.status().queued());
            assertEquals(1, limiter.status().interrupted());
        } finally {
            holder.close();
        }
    }

    @Test
    void closeWakesAndRejectsQueuedAcquisition() throws Exception {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 10);
        LambdaEnvironmentLimiter.Permit holder = limiter.acquire("held");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                limiter.acquire("closed");
                failure.set(new AssertionError("acquire unexpectedly succeeded"));
            } catch (LambdaEnvironmentLimiter.AdmissionClosedException expected) {
                // expected
            } catch (Throwable unexpected) {
                failure.set(unexpected);
            }
        });
        waiter.start();
        awaitQueued(limiter, 1);
        limiter.close();
        waiter.join(2_000);
        try {
            assertFalse(waiter.isAlive(), "closed waiter did not exit");
            assertNull(failure.get());
            assertEquals(0, limiter.status().queued());
            assertTrue(limiter.status().closed());
        } finally {
            holder.close();
        }
    }

    @Test
    void statusIsolatedByExecutionEnvironmentIdentity() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(2, 1);
        LambdaEnvironmentLimiter.Permit first = limiter.acquire("account-a/function-a:$LATEST");
        LambdaEnvironmentLimiter.Permit second = limiter.acquire("account-b/function-a:$LATEST");
        try {
            assertEquals(1, limiter.status("account-a/function-a:$LATEST").total());
            assertEquals(1, limiter.status("account-a/function-a:$LATEST").active());
            assertEquals(0, limiter.status("account-a/function-a:$LATEST").idle());
            assertEquals(1, limiter.status("account-b/function-a:$LATEST").total());
            assertEquals(0, limiter.status("account-c/function-a:$LATEST").total());
            assertEquals(2, limiter.status().total());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void staleIdleTransitionCannotDemoteAReactivatedEnvironment() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 1);
        LambdaEnvironmentLimiter.Permit permit = limiter.acquire("function");
        try {
            long firstGeneration = limiter.markActive(permit);
            assertTrue(limiter.markIdle(permit, firstGeneration));

            long secondGeneration = limiter.markActive(permit);
            assertTrue(secondGeneration > firstGeneration);
            assertFalse(limiter.markIdle(permit, firstGeneration));
            assertEquals(1, limiter.status().total());
            assertEquals(1, limiter.status().active());
            assertEquals(0, limiter.status().idle());
        } finally {
            permit.close();
        }
        assertEquals(0, limiter.status().total());
    }

    @Test
    void retiringPermitRemainsInPhysicalTotalButNotIdle() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 1);
        LambdaEnvironmentLimiter.Permit permit = limiter.acquire("function");
        limiter.markIdle(permit);

        try {
            assertTrue(limiter.markRetiring(permit));
            assertEquals(1, limiter.status().total());
            assertEquals(0, limiter.status().active());
            assertEquals(0, limiter.status().idle());
            assertEquals(1, limiter.status().retiring());
            assertEquals(1, limiter.status("function").retiring());
            assertEquals(0, limiter.status().available());
        } finally {
            permit.close();
        }

        assertEquals(0, limiter.status().total());
        assertEquals(0, limiter.status().retiring());
    }

    @Test
    void recoveredEnvironmentIsCountedUntilRetirementIsConfirmed() {
        LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(1, 1);
        LambdaEnvironmentLimiter.Permit permit = limiter.reserveRecovered("recovered");

        try {
            assertEquals(1, limiter.status().total());
            assertEquals(0, limiter.status().active());
            assertEquals(0, limiter.status().idle());
            assertEquals(1, limiter.status().retiring());
            assertEquals(0, limiter.status().available());
            assertEquals(1, limiter.status("recovered").total());
            assertEquals(1, limiter.status("recovered").retiring());
        } finally {
            permit.close();
        }

        assertEquals(0, limiter.status().total());
        assertEquals(0, limiter.status().retiring());
    }

    private static void awaitQueued(LambdaEnvironmentLimiter limiter, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (limiter.queuedCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(expected, limiter.queuedCount(), "expected waiters were not queued");
    }
}
